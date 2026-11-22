package server

import (
	"context"
	"errors"
	"fmt"
	"github.com/hoarddb/hoarddb-server/storage"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/storage/kvfault"
)

// BUG-0527: a drop puts its reclaim record in the store (step 2) before it
// unlinks the name (step 3), and the node's reclaimer laps on its own — on any
// drop's wake and once a second while idle — so a lap landing between the two
// steps reclaimed a range whose name was still linked. A reader parked between
// its lookup and its snapshot (it holds db.mu for reading, which is also what
// holds the drop's unlink back) then took its snapshot over an emptied range
// and read the bucket as found with no documents: the reader rule of spec
// §5.2.2 broken by the real reclaimer, where TestReaderAcrossADropSeesAllOrNothing
// stands one in at the hand-off.

// fillWindowBucket creates database/bucket on s and puts whole documents in it.
func fillWindowBucket(t *testing.T, s *Server, database, bucket string, whole int) {
	t.Helper()
	createTestBucket(t, s, database, bucket, BucketTypeHash)
	docs := make([]map[string]interface{}, whole)
	for i := range docs {
		docs[i] = map[string]interface{}{"_id_": fmt.Sprintf("k%03d", i)}
	}
	if resp, err := s.BatchInsert(BatchInsertRequest{Database: database, Bucket: bucket, Data: docs}); err != nil || !resp.Success {
		t.Fatalf("filling bucket %q: %+v, %v", bucket, resp, err)
	}
}

// parkReader starts a scan of database/bucket and returns once it has looked
// the name up and is held before its snapshot, inside its db.mu read hold.
// The scan's outcome arrives on the returned channel after release is called.
func parkReader(t *testing.T, s *Server, database, bucket string) (outcome <-chan scanOutcome, release func()) {
	t.Helper()
	resolved, gate := make(chan struct{}), make(chan struct{})
	var resolvedOnce, releaseOnce sync.Once
	hold := func() {
		resolvedOnce.Do(func() { close(resolved) })
		<-gate
	}
	betweenResolveAndSnapshotHook.Store(&hold)
	release = func() { releaseOnce.Do(func() { close(gate) }) }
	// A failed test must not leave the reader, and the drop queued behind it,
	// parked for the rest of the package's run.
	t.Cleanup(release)
	scanned := make(chan scanOutcome, 1)
	go func() { scanned <- scanWhole(s, database, bucket) }()
	waitFor(t, resolved, "the reader to resolve bucket "+bucket)
	betweenResolveAndSnapshotHook.Store(nil)
	return scanned, release
}

// dropCommittedFor reports whether the catalog entry of database/bucket is
// gone from the store: a bucket drop deletes it in the batch that puts its
// reclaim record, and nothing puts it back while the name stays linked, so
// unlike the record, which a reclaimer may finish, it stays true once true.
func dropCommittedFor(t *testing.T, s *Server, database, bucket string) bool {
	t.Helper()
	_, catalogued, err := s.readCatalogEntry(database, bucket)
	if err != nil {
		t.Fatalf("reading the catalog entry of %s/%s: %v", database, bucket, err)
	}
	return !catalogued
}

// awaitLapAfter wakes s's own reclaimer and returns once a lap that began
// after the call has run to its end: it waits for a lap to start, wakes the
// loop again, and waits for the next start, which the loop reaches only past
// the end of the first. The wake is buffered, so the second start comes from
// it whether the first lap left work pending or went idle.
func awaitLapAfter(t *testing.T, s *Server) {
	t.Helper()
	started := make(chan struct{}, 1)
	signal := func() {
		select {
		case started <- struct{}{}:
		default:
		}
	}
	reclaimLapHook.Store(&signal)
	defer reclaimLapHook.Store(nil)
	for lap := 1; lap <= 2; lap++ {
		s.wakeReclaimer()
		select {
		case <-started:
		case <-time.After(10 * time.Second):
			t.Fatalf("the reclaimer did not start lap %d of 2 within 10s of a wake", lap)
		}
	}
}

// TestALiveReclaimerWaitsForTheDropsUnlink holds a reader between its lookup
// and its snapshot, runs a drop up to the point where its record is in the
// store and its unlink waits on the reader, and lets the node's own reclaimer
// run a whole lap in that window before the reader is let go. The reader must
// see the whole bucket or no bucket, and the range must still be reclaimed
// once the drop has unlinked it.
func TestALiveReclaimerWaitsForTheDropsUnlink(t *testing.T) {
	const database, whole = "app", 100
	t.Cleanup(func() {
		betweenResolveAndSnapshotHook.Store(nil)
		reclaimLapHook.Store(nil)
	})

	t.Run("bucket drop", func(t *testing.T) {
		s := newTestServer(t)
		fillWindowBucket(t, s, database, "held", whole)
		dropped := lookupBucket(s, database, "held")
		id, slot, databaseID := dropped.id.Load(), dropped.slot, dropped.databaseID

		outcome, release := parkReader(t, s, database, "held")
		dropDone := make(chan error, 1)
		go func() {
			resp, err := s.DeleteBucket(DeleteBucketRequest{Database: database, Name: "held"})
			if err == nil && !resp.Success {
				err = fmt.Errorf("drop refused: %s", resp.Message)
			}
			dropDone <- err
		}()
		pollUntil(t, "the drop's batch to commit", func() bool {
			return dropCommittedFor(t, s, database, "held")
		})
		awaitLapAfter(t, s)
		release()

		checkAllOrNothing(t, <-outcome, whole)
		if err := <-dropDone; err != nil {
			t.Fatalf("drop: %v", err)
		}
		waitReclaimed(t, s.kv, 10*time.Second)
		if live := liveKeysIn(t, s.kv, slot, databaseID, id); live != 0 {
			t.Fatalf("after the drop's record was reclaimed, %d key(s) of bucket %d are still live", live, id)
		}
	})

	t.Run("database drop", func(t *testing.T) {
		s := newTestServer(t)
		fillWindowBucket(t, s, database, "held", whole)
		dropped := lookupBucket(s, database, "held")
		id, slot, databaseID := dropped.id.Load(), dropped.slot, dropped.databaseID

		outcome, release := parkReader(t, s, database, "held")
		dropDone := make(chan error, 1)
		go func() {
			resp, err := s.DeleteDatabase(DeleteDatabaseRequest{Name: database})
			if err == nil && !resp.Success {
				err = fmt.Errorf("drop refused: %s", resp.Message)
			}
			dropDone <- err
		}()
		pollUntil(t, "the database drop's batch to commit", func() bool {
			_, pending, err := s.pendingDatabaseDrop(database)
			if err != nil {
				t.Fatalf("reading the database's drop record: %v", err)
			}
			// The expander can turn the record into the bucket's reclaim
			// record, deleting the catalog entry, and then delete the record,
			// between two polls; one of the two is true from the commit on.
			return pending || dropCommittedFor(t, s, database, "held")
		})
		awaitLapAfter(t, s)
		release()

		checkAllOrNothing(t, <-outcome, whole)
		if err := <-dropDone; err != nil {
			t.Fatalf("drop: %v", err)
		}
		pollUntil(t, "the dropped database's range to be reclaimed", func() bool {
			s.wakeReclaimer()
			_, pending, err := s.pendingDatabaseDrop(database)
			return err == nil && !pending && pendingReclaimKeys(t, s.kv) == 0
		})
		if live := liveKeysIn(t, s.kv, slot, databaseID, id); live != 0 {
			t.Fatalf("after the dropped database was reclaimed, %d key(s) of bucket %d are still live", live, id)
		}
	})
}

// A crash cut into a drop. Each test drops on a bare server over the fault
// store, freezes the drop at one point for the rest of the test — the crashed
// process never runs again — cuts the power, and starts a second server on
// what survived. Every point must recover as BUG-0527's table says: before the
// commit the bucket is whole and there is nothing to reclaim; from the commit
// on the name is gone and the range is reclaimed, with nothing left in the
// unlink window, because a restart links no name a committed drop removed.

// crashWindowDocs is how many documents the crash tests' bucket holds.
const crashWindowDocs = 20

// crashWindowServer is a bare server over a fresh fault store holding bucket
// app/gone with crashWindowDocs documents, synced.
func crashWindowServer(t *testing.T) (s *Server, kv *kvfault.Store, dataDir string) {
	t.Helper()
	dataDir = t.TempDir()
	kv = kvfault.New()
	s = newKVFaultServer(dataDir, kv)
	if resp, err := s.CreateBucket(CreateBucketRequest{Database: "app", Name: "gone", Type: BucketTypeHash}); err != nil || !resp.Success {
		t.Fatalf("create: %+v, %v", resp, err)
	}
	for i := range crashWindowDocs {
		key := fmt.Sprintf("k%02d", i)
		if resp, err := s.Insert(InsertRequest{Database: "app", Bucket: "gone", Key: key, Data: map[string]interface{}{"v": i}}); err != nil || !resp.Success {
			t.Fatalf("insert %s: %+v, %v", key, resp, err)
		}
	}
	if err := kv.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
	return s, kv, dataDir
}

// freezeAt returns a hook body that, on its first call, signals reached and
// then blocks until the test ends, as a process that crashed there would never
// go on, and inBackground, which runs the drop to be frozen. The rest of the
// drop runs at cleanup, after every assertion, into a store the test no
// longer reads, and the cleanup waits for it: let go unwaited, it would run
// on into the next test and set off that test's hook in its place.
func freezeAt(t *testing.T) (freeze func(), reached <-chan struct{}, inBackground func(drop func())) {
	t.Helper()
	signal, gate, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	freeze = func() {
		first := false
		once.Do(func() { first = true; close(signal) })
		if first {
			<-gate
		}
	}
	inBackground = func(drop func()) {
		go func() {
			defer close(finished)
			drop()
		}()
	}
	t.Cleanup(func() {
		close(gate)
		waitFor(t, finished, "the frozen drop to finish once released")
	})
	return freeze, signal, inBackground
}

// restartAfterPowerLoss cuts the power and loads a second server on what is
// left.
func restartAfterPowerLoss(t *testing.T, kv *kvfault.Store, dataDir string) *Server {
	t.Helper()
	kv.PowerLoss()
	restarted := newKVFaultServer(dataDir, kv)
	if err := restarted.LoadCatalog(); err != nil {
		t.Fatalf("LoadCatalog after the crash: %v", err)
	}
	return restarted
}

// requireReclaimedAfterRestart checks a restart after a committed drop: the
// name does not resolve, the range survived the crash, and the restarted
// node's reclaimer, whose unlink window is empty, empties the range and
// deletes the record.
func requireReclaimedAfterRestart(t *testing.T, restarted *Server, kv *kvfault.Store, ref storage.BucketRef) {
	slot, databaseID, id := ref.Slot, ref.DatabaseID, ref.BucketID
	t.Helper()
	if _, err := restarted.resolveBucket("app", "gone"); !errors.Is(err, ErrBucketNotFound) {
		t.Fatalf("after the crash the dropped name resolves again (err %v)", err)
	}
	if live := liveKeysIn(t, kv, slot, databaseID, id); live == 0 {
		t.Fatal("after the crash the dropped range is already empty: the test shows nothing about the restart's reclaim")
	}
	lapUntilDrained(t, restarted)
	if live := liveKeysIn(t, kv, slot, databaseID, id); live != 0 {
		t.Fatalf("after the restart's reclaim %d key(s) of dropped bucket %d are still live", live, id)
	}
	if pending := pendingReclaimKeys(t, kv); pending != 0 {
		t.Fatalf("after the restart's reclaim %d reclaim record(s) are left", pending)
	}
}

// dropFrozenAt runs a drop of app/gone in the background, frozen at phase,
// and returns once it is there.
func dropFrozenAt(t *testing.T, s *Server, phase dropPhase) {
	t.Helper()
	freeze, reached, inBackground := freezeAt(t)
	hook := func(at dropPhase, _, _ string) {
		if at == phase {
			freeze()
		}
	}
	dropPhaseHook.Store(&hook)
	t.Cleanup(func() { dropPhaseHook.Store(nil) })
	inBackground(func() { _, _ = s.DeleteBucket(DeleteBucketRequest{Database: "app", Name: "gone"}) })
	waitFor(t, reached, "the drop to reach the crash point")
}

// TestADropCrashedAtEachPointRecovers is BUG-0527's crash table for a bucket
// drop, one subtest per point.
func TestADropCrashedAtEachPointRecovers(t *testing.T) {
	t.Cleanup(func() {
		dropPhaseHook.Store(nil)
		afterDropHiddenHook.Store(nil)
	})

	t.Run("before the drop batch commits", func(t *testing.T) {
		s, kv, dataDir := crashWindowServer(t)
		dropFrozenAt(t, s, dropBeforeCommit)

		restarted := restartAfterPowerLoss(t, kv, dataDir)
		if seen := scanWhole(restarted, "app", "gone"); seen.err != nil || !seen.found || seen.documents != crashWindowDocs {
			t.Fatalf("after a crash before the drop committed, the bucket reads %+v, want all %d documents", seen, crashWindowDocs)
		}
		if pending := pendingReclaimKeys(t, kv); pending != 0 {
			t.Fatalf("after a crash before the drop committed, %d reclaim record(s) exist", pending)
		}
		lapUntilDrained(t, restarted)
		if seen := scanWhole(restarted, "app", "gone"); !seen.found || seen.documents != crashWindowDocs {
			t.Fatalf("a lap after the restart took documents from the undropped bucket: %+v", seen)
		}
	})

	t.Run("after the commit, before the unlink", func(t *testing.T) {
		s, kv, dataDir := crashWindowServer(t)
		dropped := lookupBucket(s, "app", "gone")
		id, slot, databaseID := dropped.id.Load(), dropped.slot, dropped.databaseID
		whole := liveKeysIn(t, kv, slot, databaseID, id)
		dropFrozenAt(t, s, dropCommitted)

		// The live node first: its name is still linked, so a lap leaves the
		// range whole and the record pending.
		if _, err := newReclaimer(s).lap(context.Background()); err != nil {
			t.Fatalf("a lap on the live node inside the window: %v", err)
		}
		if live := liveKeysIn(t, kv, slot, databaseID, id); live != whole {
			t.Fatalf("a lap between the drop's commit and its unlink left %d of the bucket's %d keys: it reclaimed a linked name", live, whole)
		}
		if _, found := readStoreKey(t, kv, reclaimKey(id)); !found {
			t.Fatal("a lap inside the window deleted the drop's reclaim record")
		}

		requireReclaimedAfterRestart(t, restartAfterPowerLoss(t, kv, dataDir), kv, storage.BucketRef{Slot: slot, DatabaseID: databaseID, BucketID: id})
	})

	t.Run("after the unlink, before the hand-off", func(t *testing.T) {
		s, kv, dataDir := crashWindowServer(t)
		dropped := lookupBucket(s, "app", "gone")
		id, slot, databaseID := dropped.id.Load(), dropped.slot, dropped.databaseID
		freeze, reached, inBackground := freezeAt(t)
		hook := func(_, _ string, _ uint64, _ uint16) { freeze() }
		afterDropHiddenHook.Store(&hook)
		t.Cleanup(func() { afterDropHiddenHook.Store(nil) })
		inBackground(func() { _, _ = s.DeleteBucket(DeleteBucketRequest{Database: "app", Name: "gone"}) })
		waitFor(t, reached, "the drop to reach the crash point")

		requireReclaimedAfterRestart(t, restartAfterPowerLoss(t, kv, dataDir), kv, storage.BucketRef{Slot: slot, DatabaseID: databaseID, BucketID: id})
	})

	t.Run("after the drop returned, before any reclaim", func(t *testing.T) {
		s, kv, dataDir := crashWindowServer(t)
		dropped := lookupBucket(s, "app", "gone")
		id, slot, databaseID := dropped.id.Load(), dropped.slot, dropped.databaseID
		dropOrFail(t, s, "app", "gone")

		requireReclaimedAfterRestart(t, restartAfterPowerLoss(t, kv, dataDir), kv, storage.BucketRef{Slot: slot, DatabaseID: databaseID, BucketID: id})
	})
}

// TestADatabaseDropCrashedInItsWindowRecovers is the same for a database drop
// cut between its committed drop record and the detach of its buckets: the
// live node's lap leaves the range whole, and a restart reclaims it. A drop
// whose commit fails leaves the window, so a later drop of the database is
// reclaimed on the same node.
func TestADatabaseDropCrashedInItsWindowRecovers(t *testing.T) {
	t.Cleanup(func() {
		afterDatabaseDropCommittedHook.Store(nil)
		generationDropCommitHook.Store(nil)
	})

	t.Run("after the commit, before the detach", func(t *testing.T) {
		s, kv, dataDir := crashWindowServer(t)
		dropped := lookupBucket(s, "app", "gone")
		id, slot, databaseID := dropped.id.Load(), dropped.slot, dropped.databaseID
		whole := liveKeysIn(t, kv, slot, databaseID, id)
		freeze, reached, inBackground := freezeAt(t)
		hook := func(string) { freeze() }
		afterDatabaseDropCommittedHook.Store(&hook)
		t.Cleanup(func() { afterDatabaseDropCommittedHook.Store(nil) })
		inBackground(func() { _, _ = s.DeleteDatabase(DeleteDatabaseRequest{Name: "app"}) })
		waitFor(t, reached, "the database drop to reach the crash point")

		if _, err := newReclaimer(s).lap(context.Background()); err != nil {
			t.Fatalf("a lap on the live node inside the window: %v", err)
		}
		if live := liveKeysIn(t, kv, slot, databaseID, id); live != whole {
			t.Fatalf("a lap between the database drop's commit and its detach left %d of the bucket's %d keys: it reclaimed a linked name", live, whole)
		}

		restarted := restartAfterPowerLoss(t, kv, dataDir)
		if lookupBucket(restarted, "app", "gone") != nil {
			t.Fatal("after the crash the dropped database's bucket is served again")
		}
		lapUntilDrained(t, restarted)
		if live := liveKeysIn(t, kv, slot, databaseID, id); live != 0 {
			t.Fatalf("after the restart's reclaim %d key(s) of the dropped database's bucket are still live", live)
		}
		if _, pending, err := restarted.pendingDatabaseDrop("app"); err != nil || pending {
			t.Fatalf("after the restart's reclaim the database's drop record is pending (%v, %v)", pending, err)
		}
	})

	t.Run("a failed commit leaves the window", func(t *testing.T) {
		s, kv, _ := crashWindowServer(t)
		dropped := lookupBucket(s, "app", "gone")
		id, slot, databaseID := dropped.id.Load(), dropped.slot, dropped.databaseID
		fail := func(string) error { return errors.New("simulated failure of the drop's commit") }
		generationDropCommitHook.Store(&fail)
		resp, err := s.DeleteDatabase(DeleteDatabaseRequest{Name: "app"})
		generationDropCommitHook.Store(nil)
		if err != nil || resp.Success {
			t.Fatalf("the drop with a failing commit answered %+v, %v; want a refusal", resp, err)
		}

		if resp, err := s.DeleteDatabase(DeleteDatabaseRequest{Name: "app"}); err != nil || !resp.Success {
			t.Fatalf("the second drop: %+v, %v", resp, err)
		}
		lapUntilDrained(t, s)
		if live := liveKeysIn(t, kv, slot, databaseID, id); live != 0 {
			t.Fatalf("after a failed drop and a successful one, %d key(s) of the database's bucket are still live: the failed drop left the database in the unlink window", live)
		}
	})
}
