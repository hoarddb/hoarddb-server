package server

import (
	"context"
	"fmt"
	"github.com/hoarddb/hoarddb-server/storage"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/storage/kvfault"
	"github.com/hoarddb/hoarddb-server/transport"
)

// A drop database on a target while a migration pull of one of the
// database's buckets is in flight there. The drop runs in the "" fence and the
// materialise in the bucket name's, so the two never exclude each other; the
// drop's pending record is deleted by the expander long before a slow pull
// reaches its catalog commit. Both tests hold the pull at that commit, let the
// drop commit and the expander delete its record — the migrate-stream spec's
// T-MS-6b shape — and only then let the pull resume.

// eventually reports whether cond holds within 15 s.
func eventually(cond func() bool) bool {
	deadline := time.Now().Add(15 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(10 * time.Millisecond)
	}
	return true
}

// pollUntil waits up to 15 s for cond, failing the test with what otherwise.
func pollUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	if !eventually(cond) {
		t.Fatalf("timed out waiting until %s", what)
	}
}

// heldMaterialize holds the first materialise that reaches its catalog commit
// until release is called, and lets every other one through.
type heldMaterialize struct {
	reached chan uint64
	release func()
}

func holdFirstMaterializeCatalog(t *testing.T) heldMaterialize {
	t.Helper()
	held := heldMaterialize{reached: make(chan uint64, 1)}
	released := make(chan struct{})
	var releaseOnce, holdOnce sync.Once
	held.release = func() { releaseOnce.Do(func() { close(released) }) }
	hook := func(id uint64) error {
		first := false
		holdOnce.Do(func() { first = true })
		if first {
			held.reached <- id
			<-released
		}
		return nil
	}
	materializeCatalogHook.Store(&hook)
	t.Cleanup(func() {
		held.release()
		materializeCatalogHook.Store(nil)
	})
	return held
}

func (h heldMaterialize) awaitReserved(t *testing.T) uint64 {
	t.Helper()
	select {
	case id := <-h.reached:
		return id
	case <-time.After(20 * time.Second):
		t.Fatal("the pull never reached its catalog commit")
		return 0
	}
}

// assertDatabaseGone fails unless nothing on s — in memory or in the store —
// says database or its bucket exists.
func assertDatabaseGone(t *testing.T, s *Server, database, bucket, when string) {
	t.Helper()
	if s.serverDataAccessor().getDB(database) != nil {
		t.Errorf("(%s): database %q, dropped while a pull of %q was in flight, is back in memory", when, database, bucket)
	}
	if lookupBucket(s, database, bucket) != nil {
		t.Errorf("(%s): bucket %q of dropped database %q is served", when, bucket, database)
	}
	if _, catalogued, err := s.readCatalogEntry(database, bucket); err != nil || catalogued {
		t.Errorf("(%s): the pulled bucket %q of dropped database %q is in the catalog (catalogued %v, err %v)", when, bucket, database, catalogued, err)
	}
	if missing, err := s.databaseMarkerMissing(database); err != nil || !missing {
		t.Errorf("(%s): dropped database %q has a marker again (missing %v, err %v)", when, database, missing, err)
	}
}

// materializeRecordPresent reports whether id still has a materialise record.
func materializeRecordPresent(t *testing.T, s *Server, id uint64) bool {
	t.Helper()
	_, present, err := s.readMaterializeRecord(id)
	if err != nil {
		t.Fatalf("reading the materialise record of id %d: %v", id, err)
	}
	return present
}

// TestDatabaseDropDuringStreamDoesNotResurrect is the acceptance test
// (the migrate-stream spec's T-MS-6b, design-intact arm). A pull onto the
// target is held at its catalog commit with its id reserved and its keys
// written; the target drops the database; the expander deletes the drop
// record; the pull resumes. The database must not come back — in memory, in
// the store, or after a restart — and the pull's reserved range must be
// reclaimed, not stranded.
func TestDatabaseDropDuringStreamDoesNotResurrect(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: testBucket, Key: "k", Data: map[string]interface{}{"n": 1}})

	owner, err := tc.nodes[0].server.core.cluster.Owner(placementOf(testDatabase, testBucket))
	if err != nil {
		t.Fatalf("owner: %v", err)
	}
	targetIndex := -1
	for i, addr := range tc.addrs {
		if addr != owner.ID {
			targetIndex = i
			break
		}
	}
	target := tc.nodes[targetIndex].server.core
	if target.serverDataAccessor().getDB(testDatabase) == nil {
		t.Fatalf("precondition: the target does not know database %q, so there is nothing for it to drop", testDatabase)
	}

	held := holdFirstMaterializeCatalog(t)
	pullDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		pullDone <- target.cluster.pullBucket(ctx, testDatabase, testBucket, mustNode(t, target.cluster, owner.ID))
	}()
	reservedID := held.awaitReserved(t)
	slot := slotForCreate(testDatabase, testBucket)
	databaseID := storage.LegacyDatabaseID(testDatabase)
	if keys := liveKeysIn(t, target.kv, slot, databaseID, reservedID); keys == 0 {
		t.Fatalf("precondition: the held pull wrote no keys under {slot %d, id %d}", slot, reservedID)
	}

	// The drop, sent to the target as an operator's drop is, so it is in the
	// target's log and a restart's replay knows it. On the target it hides the
	// database and then waits for the held pull's bucket lock to mark the
	// bucket closed, so it runs beside the test.
	dropper := tc.dial(targetIndex)
	defer func() { _ = dropper.Close() }()
	dropPayload := mustMarshalBSON(t, DeleteDatabaseRequest{Name: testDatabase})
	dropDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		frame, err := dropper.SendRequest(ctx, transport.OpDeleteDB, dropPayload)
		if err == nil && frame.Opcode == transport.OpError {
			err = fmt.Errorf("drop database answered with an error: %s", frame.Payload)
		}
		dropDone <- err
	}()
	pollUntil(t, "the target has committed the drop and hidden the database", func() bool {
		missing, err := target.databaseMarkerMissing(testDatabase)
		return err == nil && missing && target.serverDataAccessor().getDB(testDatabase) == nil
	})
	// T-MS-6b: the pull resumes only once the expander has deleted the drop
	// record, so nothing durable that expires names the drop any more.
	pollUntil(t, "the expander has deleted the database's drop record", func() bool {
		target.wakeReclaimer()
		_, pending, err := target.pendingDatabaseDrop(testDatabase)
		return err == nil && !pending
	})

	held.release()
	pullErr := <-pullDone
	if dropErr := <-dropDone; dropErr != nil {
		t.Fatalf("drop database on the target: %v", dropErr)
	}
	if pullErr == nil {
		t.Errorf("a pull whose database was dropped while it was in flight reported success")
	}
	assertDatabaseGone(t, target, testDatabase, testBucket, "after the pull resumed")

	// The discarded pull's range is reclaimed, not stranded (not stranded by an abandoned pull).
	reclaimed := eventually(func() bool {
		target.wakeReclaimer()
		return !materializeRecordPresent(t, target, reservedID) && liveKeysIn(t, target.kv, slot, databaseID, reservedID) == 0 &&
			pendingReclaimKeys(t, target.kv) == 0
	})
	if !reclaimed {
		t.Errorf("the discarded pull's reserved range {slot %d, id %d} was not reclaimed within 15 s: %d keys, materialise record present %v, %d reclaim records pending",
			slot, reservedID, liveKeysIn(t, target.kv, slot, databaseID, reservedID), materializeRecordPresent(t, target, reservedID), pendingReclaimKeys(t, target.kv))
	}

	reloaded := tc.reloadCatalogOnly(targetIndex)
	assertDatabaseGone(t, reloaded, testDatabase, testBucket, "after a restart's catalog load")
}

// TestDatabaseDropCancelsPullOfNewDatabase is the case the witness
// cannot see, so the in-memory cancel is the only thing that holds it: the
// target has never had the database, so its marker is absent when the pull
// reserves its id and absent again at the catalog commit. The database exists
// on the target only as the pull's own in-memory struct, which is enough for a
// drop there to run. Without the cancel the pull writes the marker the drop
// never saw and the dropped database is back.
func TestDatabaseDropCancelsPullOfNewDatabase(t *testing.T) {
	const database, bucket = "fresh", "pulled"
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()
	owner, err := tc.nodes[0].server.core.cluster.Owner(placementOf(database, bucket))
	if err != nil {
		t.Fatalf("owner: %v", err)
	}
	ownerIndex := nodeIndexByAddr(t, tc.addrs, owner.ID)
	targetIndex := (ownerIndex + 1) % len(tc.nodes)
	// Created on the owner alone, outside the log, so no other node has ever
	// had the database.
	source := tc.nodes[ownerIndex].server.core
	if resp, err := source.CreateBucket(CreateBucketRequest{Database: database, Name: bucket, Type: BucketTypeHash}); err != nil || !resp.Success {
		t.Fatalf("create %s/%s on the owner: %+v, %v", database, bucket, resp, err)
	}
	if resp, err := source.Insert(InsertRequest{Database: database, Bucket: bucket, Key: "k", Data: map[string]interface{}{"n": 1}}); err != nil || !resp.Success {
		t.Fatalf("insert on the owner: %+v, %v", resp, err)
	}
	target := tc.nodes[targetIndex].server.core
	if missing, err := target.databaseMarkerMissing(database); err != nil || !missing || target.serverDataAccessor().getDB(database) != nil {
		t.Fatalf("precondition: the target already knows database %q (marker missing %v, err %v)", database, missing, err)
	}

	held := holdFirstMaterializeCatalog(t)
	pullDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		pullDone <- target.cluster.pullBucket(ctx, database, bucket, mustNode(t, target.cluster, owner.ID))
	}()
	reservedID := held.awaitReserved(t)
	slot := slotForCreate(database, bucket)
	databaseID := storage.LegacyDatabaseID(database)

	// A drop on the target alone: the database is known nowhere else.
	dropDone := make(chan error, 1)
	go func() {
		resp, err := target.DeleteDatabase(DeleteDatabaseRequest{Name: database})
		if err == nil && !resp.Success {
			err = fmt.Errorf("drop database: %s", resp.Message)
		}
		dropDone <- err
	}()
	// The pull's struct made the database known here, so its absence means
	// the drop has committed and removed it.
	if target.serverDataAccessor().getDB(database) == nil {
		t.Fatalf("precondition: the held pull did not make database %q known on the target", database)
	}
	pollUntil(t, "the target has committed the drop and hidden the database", func() bool {
		return target.serverDataAccessor().getDB(database) == nil
	})
	pollUntil(t, "the expander has deleted the database's drop record", func() bool {
		target.wakeReclaimer()
		_, pending, err := target.pendingDatabaseDrop(database)
		return err == nil && !pending
	})

	held.release()
	pullErr := <-pullDone
	if dropErr := <-dropDone; dropErr != nil {
		t.Fatalf("drop database on the target: %v", dropErr)
	}
	if pullErr == nil {
		t.Errorf("a pull of a database new to this node, dropped here while the pull was in flight, reported success")
	}
	assertDatabaseGone(t, target, database, bucket, "after the pull resumed")
	if !eventually(func() bool {
		target.wakeReclaimer()
		return !materializeRecordPresent(t, target, reservedID) && liveKeysIn(t, target.kv, slot, databaseID, reservedID) == 0 &&
			pendingReclaimKeys(t, target.kv) == 0
	}) {
		t.Errorf("the discarded pull's reserved range {slot %d, id %d} was not reclaimed within 15 s", slot, reservedID)
	}
	assertDatabaseGone(t, tc.reloadCatalogOnly(targetIndex), database, bucket, "after a restart's catalog load")
}

// reloadCatalogOnly stops node index and loads its catalog into a fresh
// service over the same data directory without starting it, so no log replay
// runs: what it registers is what the node's store alone says exists. A full
// restart is not used because its replay is not this test's subject — at
// 5121067 a node whose log holds a create database and its drop, with the drop
// record already expanded, refuses to start (the replay gate finds the
// database live, re-created by the replayed create, with no record to check),
// with or without a pull.
func (tc *testCluster) reloadCatalogOnly(index int) *Server {
	tc.t.Helper()
	cfg := tc.nodes[index].cfg
	if tc.nodes[index].client != nil {
		_ = tc.nodes[index].client.Close()
		tc.nodes[index].client = nil
	}
	if err := tc.nodes[index].server.Stop(); err != nil {
		tc.t.Fatalf("stop node %d: %v", index+1, err)
	}
	tc.nodes[index].server = nil
	reloaded, err := NewService(cfg, quietLogger())
	if err != nil {
		tc.t.Fatalf("reopening node %d: %v", index+1, err)
	}
	tc.t.Cleanup(func() { _ = reloaded.Stop() })
	if err := reloaded.LoadCatalog(); err != nil {
		tc.t.Fatalf("loading node %d's catalog: %v", index+1, err)
	}
	return reloaded.core
}

// TestMaterializeWitnessRefusesDroppedDatabase is the backstop, alone:
// a standalone node has no cluster and so no pull record for a drop to
// cancel. A materialise into a database that existed when its id was
// reserved, and whose marker is gone by its catalog commit, must discard —
// even once the drop record that said so has been expanded away — and leave
// its range to the reclaimer.
func TestMaterializeWitnessRefusesDroppedDatabase(t *testing.T) {
	const database, bucket = "moved", "pulled"
	useReclaimClock(t, newFakeReclaimClock())
	kv := kvfault.New()
	s := newKVFaultServer(t.TempDir(), kv)
	source := newMigrationSource(t, s, "source")
	if resp, err := s.CreateDatabase(database); err != nil || !resp.Success {
		t.Fatalf("create database %q: %+v, %v", database, resp, err)
	}

	// A pulled copy carries the generation its source created it in, which is
	// what the materialise's last check is made against since generations
	// (SA-DG-12): the database's one live generation here.
	live, err := s.liveGenerations(database)
	if err != nil || len(live) != 1 {
		t.Fatalf("live generations of %q: %v, %v", database, live, err)
	}
	held := holdFirstMaterializeCatalog(t)
	materialized := make(chan error, 1)
	go func() {
		materialized <- s.materializeRange(migratedBucket{database: database, name: bucket, storeType: BucketTypeBlob, generation: live[0]}, source.stream, 0)
	}()
	reservedID := held.awaitReserved(t)
	slot := slotForCreate(database, bucket)
	databaseID := storage.LegacyDatabaseID(database)
	residueFiles := payloadFilesOf(t, s.blobRoot(), reservedID)
	if liveKeysIn(t, kv, slot, databaseID, reservedID) == 0 || len(residueFiles) == 0 {
		t.Fatalf("precondition: the held materialise wrote no keys or no payload files under {slot %d, id %d}", slot, reservedID)
	}

	dropped := make(chan error, 1)
	go func() {
		resp, err := s.DeleteDatabase(DeleteDatabaseRequest{Name: database})
		if err == nil && !resp.Success {
			err = fmt.Errorf("drop database: %s", resp.Message)
		}
		dropped <- err
	}()
	pollUntil(t, "the drop has committed and hidden the database", func() bool {
		missing, err := s.databaseMarkerMissing(database)
		return err == nil && missing && s.serverDataAccessor().getDB(database) == nil
	})
	lapUntilDrained(t, s)
	if _, pending, err := s.pendingDatabaseDrop(database); err != nil || pending {
		t.Fatalf("precondition: the drop record of %q is still pending after the reclaimer drained (%v, %v)", database, pending, err)
	}

	held.release()
	materializeErr := <-materialized
	if err := <-dropped; err != nil {
		t.Fatalf("drop database %q: %v", database, err)
	}
	if materializeErr == nil {
		t.Errorf("a materialise whose database was dropped before its catalog commit succeeded")
	}
	assertDatabaseGone(t, s, database, bucket, "after the materialise resumed")

	lapUntilDrained(t, s)
	orphanTickOverFiles(t, s, residueFiles)
	if materializeRecordPresent(t, s, reservedID) {
		t.Errorf("the discarded materialise's record under id %d was never taken by the reclaimer", reservedID)
	}
	if keys := liveKeysIn(t, kv, slot, databaseID, reservedID); keys != 0 {
		t.Errorf("%d keys of the discarded materialise survive under {slot %d, id %d}: its range is stranded", keys, slot, reservedID)
	}
	for _, path := range residueFiles {
		if fileExists(t, path) {
			t.Errorf("payload file %s of the discarded materialise (id %d) survives: it is stranded", path, reservedID)
		}
	}
}
