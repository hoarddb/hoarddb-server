package server

import (
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
)

const familyBuckets = "hoarddb_node_buckets"

// TestBucketCounterPredicate (build doc BT-12) pins the counter's two writers
// directly: an attach counts only the 0 → id move of a record, and a close
// counts only the false → true move of a record with an id. Together they
// keep "records with an id that are not closed". An attach of a closed record
// cannot happen: every attach checks, under the record's lock, that its
// record is current (heldRecord.current), and a drop closes only under it.
func TestBucketCounterPredicate(t *testing.T) {
	s := newTestServer(t)
	if resp, err := s.CreateDatabase("app"); err != nil || !resp.Success {
		t.Fatalf("creating the database: %+v, %v", resp, err)
	}
	db := s.serverDataAccessor().getDB("app")
	attach := func(rec *bucketRec, id uint64) {
		t.Helper()
		rec.mu.Lock()
		defer rec.mu.Unlock()
		handle := s.handleLocked("app", heldRecord{rec: rec, epoch: rec.epoch.Load()})
		if err := s.attachSharedEngineLocked(handle, 7, id); err != nil {
			t.Fatalf("attachSharedEngineLocked: %v", err)
		}
	}
	counted := func() int64 { return s.bucketsAttached.Load() }

	start := counted()
	fresh := claimForTest(t, db, "fresh", BucketTypeHash)
	attach(fresh, 900_001)
	if got := counted() - start; got != 1 {
		t.Fatalf("(1) attaching a fresh record moved the counter by %d, want 1", got)
	}
	if !closeRecordForTest(s, fresh) {
		t.Fatal("(2) closing an attached record reported it unattached")
	}
	if closeRecordForTest(s, fresh) {
		t.Fatal("(3) closing the same record again reported it attached")
	}
	if closeRecordForTest(s, claimForTest(t, db, "never", BucketTypeHash)) {
		t.Fatal("(4) closing a record that was never attached reported it attached")
	}
}

// bucketsGauge takes a sample on s's running sampler and reads
// hoarddb_node_buckets.
func bucketsGauge(t *testing.T, s *Server) float64 {
	t.Helper()
	s.capacity.sampleNow()
	value, ok := seriesValue(gatherCapacity(t, s), familyBuckets, "", "")
	if !ok {
		t.Fatal("hoarddb_node_buckets is absent after a sample")
	}
	return value
}

// TestNodeBucketsGaugeFollowsAttachAndDrop (spec T-2) follows the gauge
// through creates, a bucket drop, a database drop and a restart. The bucket
// whose attach fails at load is made with the encryption refusal in
// prepareBucketLocked: a blob bucket's catalog entry on a node at
// mode=require, which refuses blob buckets, so the reload leaves it out of
// the registry with its entry kept.
func TestNodeBucketsGaugeFollowsAttachAndDrop(t *testing.T) {
	dataDir := t.TempDir()
	cfg := sealedServiceConfig(dataDir, makeKeyFile(t, t.TempDir(), 0x51), "require")
	start := func() *Service {
		t.Helper()
		qs, err := NewService(cfg, quietLogger())
		if err != nil {
			t.Fatalf("NewService: %v", err)
		}
		if err := qs.LoadCatalog(); err != nil {
			t.Fatalf("LoadCatalog: %v", err)
		}
		return qs
	}
	qs := start()
	s := qs.core
	waitFirstSample(t, s.capacity)
	expect := func(when string, want float64) {
		t.Helper()
		if got := bucketsGauge(t, s); got != want {
			t.Fatalf("%s: hoarddb_node_buckets = %v, want %v", when, got, want)
		}
	}

	for _, name := range []string{"a", "b", "c"} {
		createTestBucket(t, s, "app", name, BucketTypeHash)
	}
	expect("after creating 3 buckets", 3)
	if resp, err := s.DeleteBucket(DeleteBucketRequest{Database: "app", Name: "a"}); err != nil || !resp.Success {
		t.Fatalf("drop bucket: %+v, %v", resp, err)
	}
	expect("after dropping 1", 2)
	createTestBucket(t, s, "other", "x", BucketTypeHash)
	expect("after creating a bucket in a second database", 3)
	if resp, err := s.DeleteDatabase(DeleteDatabaseRequest{Name: "app"}); err != nil || !resp.Success {
		t.Fatalf("drop database: %+v, %v", resp, err)
	}
	expect("after dropping the database that held 2", 1)

	writeUnattachableEntry(t, s, "other", "y")
	if err := qs.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	qs = start()
	t.Cleanup(func() { _ = qs.Stop() })
	s = qs.core
	waitFirstSample(t, s.capacity)
	if s.bucketStoreOpen("other", "y") {
		t.Fatal("precondition: the bucket with an unknown store engine attached; the load failure did not happen")
	}
	_, entries, err := s.readCatalog(nil)
	if err != nil || len(entries) != 2 {
		t.Fatalf("precondition: the catalog holds %d entries (%v), want x and y", len(entries), err)
	}
	expect("after a restart with one of 2 catalogued buckets failing to attach", 1)
}

// writeUnattachableEntry catalogues a hash bucket in s's store whose recorded
// store field names an engine this server does not have, so attaching it is
// refused at the next start: the way a catalog written by a build with another
// engine set would look.
func writeUnattachableEntry(t *testing.T, s *Server, database, bucket string) {
	t.Helper()
	entry := catalogEntry{
		ID: 900_100, Slot: int32(storage.SlotOf(storage.PlacementKey(database, bucket, nil))), Type: BucketTypeHash, CreatedAt: time.Now().UTC(),
		// whole: a hash entry with no placement stops the load (B6).
		Placement: placementWhole,
		Stores:    []catalogStore{{Name: "Extra", ID: 1, Engine: "nosuchengine"}}, NextStoreID: 2,
	}
	encoded, err := bson.Marshal(entry)
	if err != nil {
		t.Fatalf("encode the catalog entry: %v", err)
	}
	batch := s.kv.NewBatch()
	if err := batch.Set(catalogKey(database, bucket), encoded); err != nil {
		t.Fatalf("set the catalog entry: %v", err)
	}
	if err := batch.Commit(true); err != nil {
		t.Fatalf("commit the catalog entry: %v", err)
	}
}

// walkAttachedBuckets counts the registry's buckets with a non-zero id, under
// the house lock order: data.mu, then db.mu, then b.mu.
func walkAttachedBuckets(s *Server) int64 {
	data := s.serverDataAccessor()
	data.mu.RLock()
	defer data.mu.RUnlock()
	var attached int64
	for _, db := range data.dbs {
		db.mu.RLock()
		db.table.each(func(_ string, held heldRecord) {
			held.rec.mu.RLock()
			if held.rec.id.Load() != 0 {
				attached++
			}
			held.rec.mu.RUnlock()
		})
		db.mu.RUnlock()
	}
	return attached
}

// TestNodeBucketsGaugeReconcilesWithRegistry (spec T-3) runs a seeded random
// sequence of creates, reads, drops, database drops, reloads and migration
// releases on one node of the two-node harness, and after every step checks
// the counter, and the gauge it publishes, against a walk of the registry. A
// detach path that bypasses the counter, or an attach counted twice (a read
// view counted in buildEngines, say), shows up as a difference. The reads
// are what make the buildEngines mistake visible: without them buildEngines
// runs only on attach, where counting there is indistinguishable.
//
// Every operation is local to node 2 and synchronous, so nothing is in flight
// when the walk runs. Before a reload, every copy node 2 holds outside its
// replica set is released, so the relocation that runs at start finds
// nothing to move in the background.
func TestNodeBucketsGaugeReconcilesWithRegistry(t *testing.T) {
	tc := newTestCluster(t, 2, 1)
	defer tc.stop()
	const node = 1
	rng := rand.New(rand.NewPCG(20260926, 3))
	databases := []string{"recon0", "recon1"}
	var created []string // "database/bucket" of every bucket ever created here
	released, reads := 0, 0
	serial := 0

	check := func(step string) {
		t.Helper()
		s := tc.nodes[node].server.core
		walked := walkAttachedBuckets(s)
		if counted := s.bucketsAttached.Load(); counted != walked {
			t.Fatalf("after %s: the counter holds %d, the registry walk counts %d", step, counted, walked)
		}
		if gauge := bucketsGauge(t, s); gauge != float64(walked) {
			t.Fatalf("after %s: hoarddb_node_buckets = %v, the registry walk counts %d", step, gauge, walked)
		}
	}
	existing := func() []string {
		s := tc.nodes[node].server.core
		var names []string
		for _, qualified := range created {
			database, bucket := splitQualified(qualified)
			if s.bucketStoreOpen(database, bucket) {
				names = append(names, qualified)
			}
		}
		return names
	}
	// A copy outside its set made here never reached its owner, so the set
	// never covers it (step 10b.7); the release path a re-seed and a take-over
	// use, which detaches the copy the same way, releases it.
	releaseOne := func(qualified string) {
		t.Helper()
		c := tc.nodes[node].server.core.cluster
		database, bucket := splitQualified(qualified)
		if c.Hosts(placementOf(database, bucket)) {
			return
		}
		if err := c.releaseLocalCopy(database, bucket, "outside its set"); err != nil {
			t.Fatalf("releasing %s: %v", qualified, err)
		}
		released++
	}

	waitFirstSample(t, tc.nodes[node].server.core.capacity)
	check("start")
	reloads := 0
	reload := func(step string) {
		t.Helper()
		reloads++
		for _, qualified := range existing() {
			releaseOne(qualified)
		}
		tc.restartNode(node)
		waitFirstSample(t, tc.nodes[node].server.core.capacity)
		check(step)
	}
	for step := range 60 {
		s := tc.nodes[node].server.core
		roll := rng.IntN(100)
		switch {
		case roll < 35 || len(existing()) == 0:
			database := databases[rng.IntN(len(databases))]
			serial++
			bucket := fmt.Sprintf("b%03d", serial)
			createTestBucket(t, s, database, bucket, BucketTypeHash)
			created = append(created, database+"/"+bucket)
			check(fmt.Sprintf("step %d: create %s/%s", step, database, bucket))
		case roll < 50:
			names := existing()
			database, bucket := splitQualified(names[rng.IntN(len(names))])
			if resp, err := s.DeleteBucket(DeleteBucketRequest{Database: database, Name: bucket}); err != nil || !resp.Success {
				t.Fatalf("step %d: drop %s/%s: %+v, %v", step, database, bucket, resp, err)
			}
			check(fmt.Sprintf("step %d: drop %s/%s", step, database, bucket))
		case roll < 58:
			database := databases[rng.IntN(len(databases))]
			if s.serverDataAccessor().getDB(database) == nil {
				continue
			}
			if resp, err := s.DeleteDatabase(DeleteDatabaseRequest{Name: database}); err != nil || !resp.Success {
				t.Fatalf("step %d: drop database %s: %+v, %v", step, database, resp, err)
			}
			check(fmt.Sprintf("step %d: drop database %s", step, database))
		case roll < 74:
			// A read builds the bucket's engines over a snapshot (a read view)
			// and attaches nothing: it must not move the counter.
			names := existing()
			database, bucket := splitQualified(names[rng.IntN(len(names))])
			if _, err := s.Find(FindRequest{Database: database, Bucket: bucket, Key: "absent"}); err != nil {
				t.Fatalf("step %d: read %s/%s: %v", step, database, bucket, err)
			}
			reads++
			check(fmt.Sprintf("step %d: read %s/%s", step, database, bucket))
		case roll < 90:
			names := existing()
			releaseOne(names[rng.IntN(len(names))])
			check(fmt.Sprintf("step %d: release", step))
		default:
			if reloads >= 3 {
				continue
			}
			reload(fmt.Sprintf("step %d: reload", step))
		}
	}

	// The sequence must exercise the paths it claims to: at least one real
	// release of a copy outside the set, and at least one reload.
	if reloads == 0 {
		reload("a reload the seeded sequence did not roll")
	}
	s := tc.nodes[node].server.core
	for serial < 10_000 && released == 0 {
		serial++
		database, bucket := databases[0], fmt.Sprintf("r%03d", serial)
		if s.cluster.Hosts(placementOf(database, bucket)) {
			continue
		}
		createTestBucket(t, s, database, bucket, BucketTypeHash)
		created = append(created, database+"/"+bucket)
		check("create a copy outside the replica set")
		releaseOne(database + "/" + bucket)
		check("release it")
	}
	if released == 0 || reloads == 0 || reads == 0 {
		t.Fatalf("the sequence released %d copies, reloaded %d times and read %d times; it must do all three", released, reloads, reads)
	}
	t.Logf("the sequence created %d buckets, read %d times, released %d copies outside the replica set and reloaded %d times; %d buckets remain attached",
		len(created), reads, released, reloads, walkAttachedBuckets(s))
}

// splitQualified splits "database/bucket".
func splitQualified(qualified string) (database, bucket string) {
	database, bucket, _ = strings.Cut(qualified, "/")
	return database, bucket
}
