package server

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/storage/kvfault"
	"github.com/hoarddb/hoarddb-server/transport"
)

// waitFor fails the test if ch is not closed within a generous bound: a hook
// that never runs is a test that proves nothing, and it must say so rather
// than hang.
func waitFor(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// deleteRangeInBatches stands in for the reclaimer (Step 15): it deletes every
// key of bucket id in slot with the store's plain Delete, a few keys a batch,
// so a reader that looks at the range mid-way sees part of it.
func deleteRangeInBatches(t *testing.T, kv storage.KV, slot uint16, databaseID, id uint64) {
	t.Helper()
	snap := kv.NewSnapshot()
	it := snap.NewIterator(storage.IterOptions{Lower: storage.Scope(slot, databaseID, id), Upper: storage.Scope(slot, databaseID, id+1)})
	var keys [][]byte
	for it.Seek(nil); it.Valid(); it.Next() {
		keys = append(keys, append([]byte(nil), it.Key()...))
	}
	it.Close()
	snap.Discard()
	for start := 0; start < len(keys); start += 10 {
		batch := kv.NewBatch()
		for _, key := range keys[start:min(start+10, len(keys))] {
			if err := batch.Delete(key); err != nil {
				t.Errorf("deleting %x: %v", key, err)
			}
		}
		if err := batch.Commit(false); err != nil {
			t.Errorf("committing a reclaim batch: %v", err)
		}
		batch.Discard()
	}
}

// recreatedAndMaterialised is the reproduction, which is also T-27(b)'s
// setup. X is written, dropped and re-created through the cluster, so all of
// it is in node 1's log; node 2's X is given a document outside the log; node
// 1 drops X locally and pulls it from node 2. It returns the cluster and the
// id node 1 serves X under before any restart.
func recreatedAndMaterialised(t *testing.T, bucket string) (*testCluster, uint64) {
	t.Helper()
	tc := newTestCluster(t, 2, 2)
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	for _, key := range []string{"old1", "old2", "old3"} {
		tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: key, Data: map[string]interface{}{"v": key}})
	}
	tc.ok(0, transport.OpDeleteBucket, DeleteBucketRequest{Database: testDatabase, Name: bucket})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})

	source := tc.nodes[1].server.core
	if resp, err := source.Insert(InsertRequest{Database: testDatabase, Bucket: bucket, Key: "seeded", Data: map[string]interface{}{"v": "seeded"}}); err != nil || !resp.Success {
		t.Fatalf("seeding node 2 outside the log: %+v, %v", resp, err)
	}

	target := tc.nodes[0].server.core
	if resp, err := target.DeleteBucket(DeleteBucketRequest{Database: testDatabase, Name: bucket}); err != nil || !resp.Success {
		t.Fatalf("dropping X locally on node 1: %+v, %v", resp, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// A node that owns X and holds no copy copies it in as its take-over
	// does (step 10b.5); any holder serves that pull.
	owns := target.cluster.ownsBucket(placementOf(testDatabase, bucket))
	if err := target.cluster.pullBucketWith(ctx, pullPlan{database: testDatabase, bucket: bucket,
		source: mustNode(t, target.cluster, tc.addrs[1]), takeOver: owns}); err != nil {
		t.Fatalf("pulling X from node 2: %v", err)
	}
	found, err := target.Find(FindRequest{Database: testDatabase, Bucket: bucket, Key: "seeded"})
	if err != nil || !found.Found {
		t.Fatalf("before the restart the materialised copy does not hold the seeded key: %+v, %v", found, err)
	}
	return tc, lookupBucket(target, testDatabase, bucket).id.Load()
}

// TestReplayIntoARecreatedName is T-27(b): the replayed drop and create of an
// earlier X are at or below the materialised X's applied_seq, so replay skips
// them. The new X keeps its id, holds none of the old entries, and keeps the
// key that reached it outside the log. It fails on Step 13's tree, whose
// replay dispatches every DDL entry.
func TestReplayIntoARecreatedName(t *testing.T) {
	tc, idBefore := recreatedAndMaterialised(t, "recreated")
	defer tc.stop()

	restarted := tc.restartNode(0).server.core
	served := lookupBucket(restarted, testDatabase, "recreated")
	if served == nil {
		t.Fatal("after the restart node 1 does not serve X at all")
	}
	if served.id.Load() != idBefore {
		t.Fatalf("after the restart X is served under id %d, before it was %d: replay dropped and re-created it", served.id.Load(), idBefore)
	}
	for _, key := range []string{"old1", "old2", "old3"} {
		if found, err := restarted.Find(FindRequest{Database: testDatabase, Bucket: "recreated", Key: key}); err != nil || found.Found {
			t.Fatalf("after the restart X holds the dropped X's key %s: %+v, %v", key, found, err)
		}
	}
	if found, err := restarted.Find(FindRequest{Database: testDatabase, Bucket: "recreated", Key: "seeded"}); err != nil || !found.Found {
		t.Fatalf("after the restart the seeded key is gone: %+v, %v", found, err)
	}
}

// newKVFaultServer is a bare standalone server over kv, so a test can cut its
// power and start another on what survived.
func newKVFaultServer(dataDir string, kv storage.KV) *Server {
	return &Server{
		config: &config.Config{
			Storage:    config.StorageConfig{DataDir: dataDir},
			Encryption: config.EncryptionConfig{Mode: "off", Keystore: "off"},
		},
		logger:      discardLogger(),
		dataStore:   &serverData{dbs: make(map[string]*inMemoryDB)},
		kv:          kv,
		authReqSeen: make(map[string]time.Time),
		knownNodes:  make(map[string]*NodeInfo),
	}
}

// TestHideIsSyncedBeforeReturn is N-05: the drop's batch is synced before the
// drop returns. On the fault store a power loss discards every batch
// committed without a sync; after one, the name still does not resolve and
// the reclaim record — with the slot the reclaimer walks by — is there.
func TestHideIsSyncedBeforeReturn(t *testing.T) {
	dataDir := t.TempDir()
	kv := kvfault.New()
	s := newKVFaultServer(dataDir, kv)
	if resp, err := s.CreateBucket(CreateBucketRequest{Database: "app", Name: "gone", Type: BucketTypeHash}); err != nil || !resp.Success {
		t.Fatalf("create: %+v, %v", resp, err)
	}
	if resp, err := s.Insert(InsertRequest{Database: "app", Bucket: "gone", Key: "k", Data: map[string]interface{}{"v": 1}}); err != nil || !resp.Success {
		t.Fatalf("insert: %+v, %v", resp, err)
	}
	if err := kv.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
	dropped := lookupBucket(s, "app", "gone")
	id, slot := dropped.id.Load(), dropped.slot
	if resp, err := s.DeleteBucket(DeleteBucketRequest{Database: "app", Name: "gone"}); err != nil || !resp.Success {
		t.Fatalf("drop: %+v, %v", resp, err)
	}

	kv.PowerLoss()
	restarted := newKVFaultServer(dataDir, kv)
	if err := restarted.LoadCatalog(); err != nil {
		t.Fatalf("LoadCatalog after the power loss: %v", err)
	}
	if _, err := restarted.resolveBucket("app", "gone"); !errors.Is(err, ErrBucketNotFound) {
		t.Fatalf("after a power loss the dropped name resolves (err %v): the hide was not synced", err)
	}
	encoded, found := readStoreKey(t, kv, reclaimKey(id))
	if !found {
		t.Fatalf("after a power loss bucket %d has no reclaim record: the hide was not synced, or never written", id)
	}
	var record reclaimRecord
	if err := bson.Unmarshal(encoded, &record); err != nil {
		t.Fatalf("decoding the reclaim record: %v", err)
	}
	want := reclaimRecord{ID: int64(id), Slot: int32(slot), Type: BucketTypeHash, Database: "app", Name: "gone"}
	if record.ID != want.ID || record.Slot != want.Slot || record.Type != want.Type || record.Database != want.Database || record.Name != want.Name {
		t.Fatalf("reclaim record = %+v, want %+v", record, want)
	}
}

// scanOutcome is what one reader saw.
type scanOutcome struct {
	found     bool
	documents int
	message   string
	err       error
}

func scanWhole(s *Server, database, bucket string) scanOutcome {
	resp, err := s.Scan(ScanRequest{Database: database, Bucket: bucket, Limit: 10_000})
	if err != nil {
		return scanOutcome{err: err}
	}
	return scanOutcome{found: resp.Found, documents: len(resp.Documents), message: resp.Message}
}

// checkAllOrNothing fails unless a read saw the whole pre-drop bucket or no
// bucket at all. A failure prints the whole outcome, so found with the wrong
// count (a real partial read) reads differently from an error or a timeout.
func checkAllOrNothing(t *testing.T, outcome scanOutcome, whole int) {
	t.Helper()
	if outcome.err != nil {
		t.Errorf("a read across the drop failed: found=%v documents=%d message=%q err=%v",
			outcome.found, outcome.documents, outcome.message, outcome.err)
		return
	}
	if outcome.found && outcome.documents != whole {
		t.Errorf("a read across the drop returned %d of the bucket's %d documents: found=%v documents=%d message=%q err=%v; it saw part of a reclaimed range",
			outcome.documents, whole, outcome.found, outcome.documents, outcome.message, outcome.err)
	}
}

// TestReaderAcrossADropSeesAllOrNothing is the reader rule (spec §5.2.2): a
// read resolves the name and opens its snapshot under one db.mu read hold, so
// it reads the whole bucket as of before the drop, or does not find it — never
// part of a range the reclaimer is deleting. The reclaimer is stood in for by
// deleting the range at the drop's hand-off.
func TestReaderAcrossADropSeesAllOrNothing(t *testing.T) {
	const database, whole = "app", 100
	setup := func(t *testing.T, bucket string) *Server {
		s := newTestServer(t)
		createTestBucket(t, s, database, bucket, BucketTypeHash)
		docs := make([]map[string]interface{}, whole)
		for i := range docs {
			docs[i] = map[string]interface{}{"_id_": fmt.Sprintf("k%03d", i)}
		}
		if resp, err := s.BatchInsert(BatchInsertRequest{Database: database, Bucket: bucket, Data: docs}); err != nil || !resp.Success {
			t.Fatalf("filling the bucket: %+v, %v", resp, err)
		}
		return s
	}
	t.Cleanup(func() {
		betweenResolveAndSnapshotHook.Store(nil)
		dropPhaseHook.Store(nil)
		afterDropHiddenHook.Store(nil)
	})

	// A reader held between its lookup and its snapshot while the drop runs
	// and the range is reclaimed: the one interleaving that separates the rule
	// from resolve-then-snapshot, forced rather than hoped for.
	t.Run("held between resolve and snapshot", func(t *testing.T) {
		s := setup(t, "held")
		resolved, gate := make(chan struct{}), make(chan struct{})
		var resolvedOnce, releaseOnce sync.Once
		holdReader := func() {
			resolvedOnce.Do(func() { close(resolved) })
			<-gate
		}
		release := func() { releaseOnce.Do(func() { close(gate) }) }
		betweenResolveAndSnapshotHook.Store(&holdReader)
		t.Cleanup(release)
		outcome := make(chan scanOutcome, 1)
		go func() { outcome <- scanWhole(s, database, "held") }()
		waitFor(t, resolved, "the reader to resolve the bucket")
		betweenResolveAndSnapshotHook.Store(nil)

		// The reader is released at the drop's commit, the hand-off the old
		// 200 ms stood in for: the reclaim record is durable and the reclaimer
		// could act, and the reader still holds the db.mu read hold the unlink
		// waits on.
		commit := func(phase dropPhase, _, _ string) {
			if phase == dropCommitted {
				release()
			}
		}
		dropPhaseHook.Store(&commit)
		t.Cleanup(func() { dropPhaseHook.Store(nil) })
		reclaim := func(database, _ string, id uint64, slot uint16) {
			deleteRangeInBatches(t, s.kv, slot, storage.LegacyDatabaseID(database), id)
		}
		afterDropHiddenHook.Store(&reclaim)
		if resp, err := s.DeleteBucket(DeleteBucketRequest{Database: database, Name: "held"}); err != nil || !resp.Success {
			t.Fatalf("drop: %+v, %v", resp, err)
		}
		afterDropHiddenHook.Store(nil)
		checkAllOrNothing(t, <-outcome, whole)
	})

	// Readers racing the drop with no hook, for the race detector.
	t.Run("racing readers", func(t *testing.T) {
		s := setup(t, "raced")
		reclaim := func(database, _ string, id uint64, slot uint16) {
			deleteRangeInBatches(t, s.kv, slot, storage.LegacyDatabaseID(database), id)
		}
		afterDropHiddenHook.Store(&reclaim)
		defer afterDropHiddenHook.Store(nil)

		stop := make(chan struct{})
		var readers sync.WaitGroup
		var scans atomic.Int64
		outcomes := make(chan scanOutcome, 4096)
		for range 8 {
			readers.Add(1)
			go func() {
				defer readers.Done()
				for {
					select {
					case <-stop:
						return
					default:
					}
					outcome := scanWhole(s, database, "raced")
					scans.Add(1)
					select {
					case outcomes <- outcome:
					default:
					}
				}
			}()
		}
		// The fixed 20 ms is a reader count: every reader is in its scan loop
		// before the drop races it.
		pollUntil(t, "the readers to be scanning", func() bool { return scans.Load() >= 8 })
		if resp, err := s.DeleteBucket(DeleteBucketRequest{Database: database, Name: "raced"}); err != nil || !resp.Success {
			t.Fatalf("drop: %+v, %v", resp, err)
		}
		// DeleteBucket returns once the drop's hand-off is done; each reader
		// then takes one more scan against the dropped name, a reader count in
		// place of the second 20 ms.
		before := scans.Load()
		pollUntil(t, "the readers to observe the drop", func() bool { return scans.Load() >= before+8 })
		close(stop)
		readers.Wait()
		close(outcomes)
		for outcome := range outcomes {
			checkAllOrNothing(t, outcome, whole)
		}
	})
}

// TestNoIDOutlivesTheFence is the build doc's §1 trap 2: a writer that
// resolved the bucket before a drop, and reaches it only after the drop has
// removed the name, fails with ErrBucketNotFound instead of writing into the
// range the drop handed to the reclaimer.
func TestNoIDOutlivesTheFence(t *testing.T) {
	const database, bucket = "app", "cached"
	s := newTestServer(t)
	createTestBucket(t, s, database, bucket, BucketTypeHash)
	dropped := lookupBucket(s, database, bucket)
	id, slot, databaseID := dropped.id.Load(), dropped.slot, dropped.databaseID

	resolved, release := make(chan struct{}), make(chan struct{})
	var resolvedOnce sync.Once
	holdWriter := func(_, name string) {
		if name != bucket {
			return
		}
		resolvedOnce.Do(func() { close(resolved) })
		<-release
	}
	afterResolveHook.Store(&holdWriter)
	t.Cleanup(func() {
		afterResolveHook.Store(nil)
		afterDropHiddenHook.Store(nil)
	})

	writeErr := make(chan error, 1)
	go func() {
		_, err := s.InsertCtx(context.Background(), InsertRequest{Database: database, Bucket: bucket, Key: "late", Data: map[string]interface{}{"v": 1}})
		writeErr <- err
	}()
	waitFor(t, resolved, "the writer to resolve the bucket")
	afterResolveHook.Store(nil)

	var lateErr error
	releaseAfterHide := func(_, _ string, _ uint64, _ uint16) {
		close(release)
		lateErr = <-writeErr
	}
	afterDropHiddenHook.Store(&releaseAfterHide)
	if resp, err := s.DeleteBucket(DeleteBucketRequest{Database: database, Name: bucket}); err != nil || !resp.Success {
		t.Fatalf("drop: %+v, %v", resp, err)
	}

	if !errors.Is(lateErr, ErrBucketNotFound) {
		t.Fatalf("a writer that resolved before the drop and wrote after it returned %v, want ErrBucketNotFound", lateErr)
	}
	if _, found := readStoreKey(t, s.kv, storage.Key(slot, databaseID, id, storage.NSDocuments, []byte("late"))); found {
		t.Fatal("the late write landed in the dropped bucket's range")
	}
}
