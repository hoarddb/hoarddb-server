package server

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Build doc Step 16: drop database is one record, expanded in the background;
// catalog load reads the pending database drops first; a create over an entry
// not yet expanded converts it. T-28(a), both halves, and N-10.

// holdExpansion makes the expander refuse every bucket from the n-th on, as a
// crash would stop it there, and reports when it first refused. Clear it with
// the returned function; it is cleared at cleanup too.
func holdExpansion(t *testing.T, afterExpanding int64) (stopped <-chan struct{}, clear func()) {
	t.Helper()
	reached := make(chan struct{})
	var once sync.Once
	var expanded atomic.Int64
	hook := func(_, _ string) error {
		if expanded.Load() >= afterExpanding {
			once.Do(func() { close(reached) })
			return errReclaimCrash
		}
		expanded.Add(1)
		return nil
	}
	expandBucketHook.Store(&hook)
	clear = func() { expandBucketHook.Store(nil) }
	t.Cleanup(clear)
	return reached, clear
}

// catalogEntriesOf counts the bucket catalog entries stored for database,
// whatever their ids.
func catalogEntriesOf(t *testing.T, kv storage.KV, database string) int {
	t.Helper()
	lower := catalogKey(database, "")
	upper := append([]byte(nil), lower...)
	upper[len(upper)-1]++
	snap := kv.NewSnapshot()
	defer snap.Discard()
	it := snap.NewIterator(storage.IterOptions{Lower: lower, Upper: upper})
	defer it.Close()
	count := 0
	for it.Seek(nil); it.Valid(); it.Next() {
		if len(it.Key()) > len(lower) {
			count++
		}
	}
	return count
}

// pendingDatabaseDrops counts the database-drop records.
func pendingDatabaseDrops(t *testing.T, kv storage.KV) int {
	t.Helper()
	snap := kv.NewSnapshot()
	defer snap.Discard()
	it := snap.NewIterator(storage.IterOptions{Lower: reclaimKey(storage.DatabaseDropID), Upper: storage.NodeKey(storage.NSReclaim + 1)})
	defer it.Close()
	count := 0
	for it.Seek(nil); it.Valid(); it.Next() {
		count++
	}
	return count
}

// isGone reports whether err is what a name in a dropped database resolves
// to: ErrBucketNotFound, or — when the database itself no longer exists on
// this node — its not-found error, which names the database rather than the
// bucket.
func isGone(err error, database string) bool {
	return errors.Is(err, ErrBucketNotFound) ||
		(err != nil && strings.Contains(err.Error(), fmt.Sprintf("database %q not found", database)))
}

type placedBucket struct {
	name       string
	slot       uint16
	databaseID uint64
	id         uint64
}

// TestDropDatabaseSurvivesACrashMidExpansion is T-28(a)'s first half: a
// database of 10³ buckets is dropped, the expander is stopped half-way, and the
// node restarts. Every bucket is not found, no write to one is accepted, and a
// database re-created under the name is empty and served. Then the expander
// finishes: every old range is emptied, and the database record goes.
func TestDropDatabaseSurvivesACrashMidExpansion(t *testing.T) {
	const database, buckets = "app", 1_000
	dataDir := t.TempDir()
	first := startCatalogServerAt(t, dataDir)
	if resp, err := first.core.CreateDatabase(database); err != nil || !resp.Success {
		t.Fatalf("create database: %+v, %v", resp, err)
	}
	var placed []placedBucket
	for i := 0; i < buckets; i++ {
		name := fmt.Sprintf("b%04d", i)
		createTestBucket(t, first.core, database, name, BucketTypeHash)
		if resp, err := first.core.Insert(InsertRequest{Database: database, Bucket: name, Key: "k", Data: map[string]interface{}{"n": i}}); err != nil || !resp.Success {
			t.Fatalf("insert into %s: %+v, %v", name, resp, err)
		}
		b := lookupBucket(first.core, database, name)
		placed = append(placed, placedBucket{name: name, slot: b.slot, databaseID: b.databaseID, id: b.id.Load()})
	}

	halfway, clear := holdExpansion(t, buckets/2)
	if resp, err := first.core.DeleteDatabase(DeleteDatabaseRequest{Name: database}); err != nil || !resp.Success {
		t.Fatalf("drop database: %+v, %v", resp, err)
	}
	waitFor(t, halfway, "the expander to stop half-way")
	second := restartCatalog(t, first, dataDir)
	s := second.core

	if left := catalogEntriesOf(t, s.kv, database); left == 0 || left == buckets {
		t.Fatalf("%d of %d catalog entries are left at the restart; the test needs the expansion half done", left, buckets)
	}
	for _, b := range placed {
		if _, err := s.resolveBucket(database, b.name); !isGone(err, database) {
			t.Fatalf("after the restart %s resolves (err %v): the dropped database's unexpanded entries were served", b.name, err)
		}
		if found, err := s.Find(FindRequest{Database: database, Bucket: b.name, Key: "k"}); err == nil && found.Found {
			t.Fatalf("after the restart %s serves its document", b.name)
		}
	}
	if resp, err := s.Insert(InsertRequest{Database: database, Bucket: placed[0].name, Key: "late", Data: map[string]interface{}{"n": 1}}); err == nil && resp.Success {
		t.Fatalf("a write to a bucket of the dropped database was accepted after the restart")
	}

	if resp, err := s.CreateDatabase(database); err != nil || !resp.Success {
		t.Fatalf("re-creating the database with its expansion pending: %+v, %v", resp, err)
	}
	listed, err := s.ListBuckets(ListBucketsRequest{Database: database})
	if err != nil || len(listed.Buckets) != 0 {
		t.Fatalf("the re-created database lists %+v (%v), want no bucket", listed, err)
	}
	createTestBucket(t, s, database, "fresh", BucketTypeHash)
	if resp, err := s.Insert(InsertRequest{Database: database, Bucket: "fresh", Key: "k", Data: map[string]interface{}{"n": 1}}); err != nil || !resp.Success {
		t.Fatalf("writing to the re-created database: %+v, %v", resp, err)
	}
	if found, err := s.Find(FindRequest{Database: database, Bucket: "fresh", Key: "k"}); err != nil || !found.Found {
		t.Fatalf("reading from the re-created database: %+v, %v", found, err)
	}

	clear()
	s.wakeReclaimer()
	waitReclaimed(t, s.kv, time.Minute)
	deadline := time.Now().Add(time.Minute)
	for pendingDatabaseDrops(t, s.kv) != 0 {
		if time.Now().After(deadline) {
			t.Fatal("the database-drop record was not deleted after its expansion")
		}
		time.Sleep(10 * time.Millisecond)
		waitReclaimed(t, s.kv, time.Minute)
	}
	for _, b := range placed {
		if n := liveKeysIn(t, s.kv, b.slot, b.databaseID, b.id); n != 0 {
			t.Fatalf("after the drain the dropped %s still holds %d live keys", b.name, n)
		}
	}
	if n := catalogEntriesOf(t, s.kv, database); n != 1 {
		t.Fatalf("after the drain the database's catalog holds %d bucket entries, want the re-created one", n)
	}
	if found, err := s.Find(FindRequest{Database: database, Bucket: "fresh", Key: "k"}); err != nil || !found.Found {
		t.Fatalf("the re-created database lost its bucket to the expansion: %+v, %v", found, err)
	}
}

// TestCreateOverAnUnexpandedEntryConverts is T-28(a)'s second half on one
// node: with the expansion held, re-creating the database succeeds, and a
// create bucket over a name whose old entry the expander has not reached is
// empty — and once the reclaimer runs, the old id's range is reclaimed rather
// than left with nothing pointing at it.
func TestCreateOverAnUnexpandedEntryConverts(t *testing.T) {
	const database = "app"
	s := newTestServer(t)
	createTestBucket(t, s, database, "x", BucketTypeHash)
	insertDocuments(t, s, "x", 10)
	old := lookupBucket(s, database, "x")
	oldSlot, oldDatabaseID, oldID := old.slot, old.databaseID, old.id.Load()

	_, clear := holdExpansion(t, 0)
	if resp, err := s.DeleteDatabase(DeleteDatabaseRequest{Name: database}); err != nil || !resp.Success {
		t.Fatalf("drop database: %+v, %v", resp, err)
	}
	if resp, err := s.CreateDatabase(database); err != nil || !resp.Success {
		t.Fatalf("re-creating the database with its expansion held: %+v, %v", resp, err)
	}
	createTestBucket(t, s, database, "x", BucketTypeHash)
	recreated := lookupBucket(s, database, "x")
	if recreated.id.Load() <= oldID {
		t.Fatalf("the re-created x has id %d; the dropped one had %d", recreated.id.Load(), oldID)
	}
	if found, err := s.Find(FindRequest{Database: database, Bucket: "x", Key: "k000"}); err != nil || found.Found {
		t.Fatalf("the re-created x serves the dropped x's document: %+v, %v", found, err)
	}
	if n, err := s.Count(CountRequest{Database: database, Bucket: "x"}); err != nil || n.Count != 0 {
		t.Fatalf("the re-created x counts %+v (%v), want 0", n, err)
	}

	clear()
	s.wakeReclaimer()
	waitReclaimed(t, s.kv, time.Minute)
	if n := liveKeysIn(t, s.kv, oldSlot, oldDatabaseID, oldID); n != 0 {
		t.Fatalf("after the drain the dropped x's range still holds %d live keys: its entry was overwritten and nothing reclaims it", n)
	}
	if found, err := s.Find(FindRequest{Database: database, Bucket: "x", Key: "k000"}); err != nil || found.Found {
		t.Fatalf("after the drain the re-created x serves an old document: %+v, %v", found, err)
	}
}

// TestRecreateDatabaseAcrossAClusterWithExpansionHeld is T-28(a)'s second
// half on a 3-node cluster: with every node's expansion held, create database
// succeeds on every node, and a create bucket over the not-yet-expanded name
// is empty everywhere and, once expansion resumes, every node reclaims the old
// id's range.
func TestRecreateDatabaseAcrossAClusterWithExpansionHeld(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: testBucket, Key: "old", Data: map[string]interface{}{"n": 1}})
	var olds []placedBucket
	for _, n := range tc.nodes {
		waitForBucket(t, n, testDatabase, testBucket)
		b := lookupBucket(n.server.core, testDatabase, testBucket)
		olds = append(olds, placedBucket{name: testBucket, slot: b.slot, databaseID: b.databaseID, id: b.id.Load()})
	}

	_, clear := holdExpansion(t, 0)
	tc.ok(0, transport.OpDeleteDB, DeleteDatabaseRequest{Name: testDatabase})
	for i, n := range tc.nodes {
		deadline := time.Now().Add(clusterSettleTimeout(t))
		for n.server.core.serverDataAccessor().getDB(testDatabase) != nil {
			if time.Now().After(deadline) {
				t.Fatalf("node %d did not apply the database drop", i+1)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	created := tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	if !strings.Contains(string(created.Payload), "created") {
		t.Fatalf("create database with the expansion pending answered %q", created.Payload)
	}
	for i, n := range tc.nodes {
		deadline := time.Now().Add(clusterSettleTimeout(t))
		for n.server.core.serverDataAccessor().getDB(testDatabase) == nil {
			if time.Now().After(deadline) {
				t.Fatalf("node %d did not re-create the database while its expansion was pending", i+1)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	for i, n := range tc.nodes {
		waitForBucket(t, n, testDatabase, testBucket)
		core := n.server.core
		if id := lookupBucket(core, testDatabase, testBucket).id.Load(); id <= olds[i].id {
			t.Fatalf("node %d re-created the bucket under id %d; the dropped one had %d", i+1, id, olds[i].id)
		}
		if found, err := core.Find(FindRequest{Database: testDatabase, Bucket: testBucket, Key: "old"}); err != nil || found.Found {
			t.Fatalf("node %d's re-created bucket serves the dropped one's document: %+v, %v", i+1, found, err)
		}
	}

	clear()
	for i, n := range tc.nodes {
		core := n.server.core
		core.wakeReclaimer()
		waitReclaimed(t, core.kv, time.Minute)
		if left := liveKeysIn(t, core.kv, olds[i].slot, olds[i].databaseID, olds[i].id); left != 0 {
			t.Fatalf("after the drain node %d's dropped range still holds %d live keys", i+1, left)
		}
	}
}

// TestStaleDatabasePointerSeesTheDrop is N-10: a reader that fetched the
// database before it was dropped, and takes its lock after, finds the bucket
// gone rather than reading a range the expander may already have handed to
// the reclaimer.
func TestStaleDatabasePointerSeesTheDrop(t *testing.T) {
	const database = "app"
	s := newTestServer(t)
	createTestBucket(t, s, database, "users", BucketTypeHash)
	insertDocuments(t, s, "users", 3)

	stale := s.serverDataAccessor().getDB(database)
	if resp, err := s.DeleteDatabase(DeleteDatabaseRequest{Name: database}); err != nil || !resp.Success {
		t.Fatalf("drop database: %+v, %v", resp, err)
	}
	view, snap, err := s.resolveForReadIn(stale, database, "users")
	if err == nil {
		snap.Discard()
		t.Fatalf("a reader holding the dropped database resolved its bucket (id %d)", view.id.Load())
	}
	if !errors.Is(err, ErrBucketNotFound) {
		t.Fatalf("a reader holding the dropped database got %v, want ErrBucketNotFound", err)
	}
}
