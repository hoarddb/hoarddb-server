package server

import (
	"context"
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/store"
)

// These tables pin today's behaviour of the hash, btree and blob engines at the
// server level (shared-storage spec T-03). They are written against Server's
// verbs, not against store.HashStore or storage.BlobStore, because the shared
// store rewrites those types: a test written against a type being deleted is a
// test deleted with it. Every engine step of the shared-storage build re-runs
// them unmodified.

// engineOp names one verb of a behaviour table.
type engineOp string

const (
	opPut      engineOp = "put"
	opGet      engineOp = "get"
	opDelete   engineOp = "delete"
	opRange    engineOp = "range"
	opFirst    engineOp = "first"
	opLast     engineOp = "last"
	opBlobPut  engineOp = "blob-put"
	opBlobRead engineOp = "blob-read"
)

// engineStep is one verb and everything the server must answer to it. After
// every step the bucket's count is checked against wantCount.
type engineStep struct {
	name string
	op   engineOp
	key  string
	// tag is the value written by a put, and the value a get expects.
	tag string
	// wantFound is a get's or blob read's Found, and a delete's Success.
	wantFound bool
	// low and high bound a range; wantKeys is what a range, first or last returns.
	low, high string
	wantKeys  []string
	// payload is a blob's bytes, written by blob-put and expected by blob-read.
	payload   []byte
	wantCount int64
}

// keyedTable is the put/get/overwrite/delete/delete-absent sequence every keyed
// engine shares. Deleting an absent key succeeds today on hash and btree; the
// table pins that, and pins that the count does not move.
func keyedTable() []engineStep {
	return []engineStep{
		{name: "put k1", op: opPut, key: "k1", tag: "v1", wantCount: 1},
		{name: "get k1", op: opGet, key: "k1", tag: "v1", wantFound: true, wantCount: 1},
		{name: "put k2", op: opPut, key: "k2", tag: "v2", wantCount: 2},
		{name: "overwrite k1", op: opPut, key: "k1", tag: "v1b", wantCount: 2},
		{name: "get overwritten k1", op: opGet, key: "k1", tag: "v1b", wantFound: true, wantCount: 2},
		{name: "delete k2", op: opDelete, key: "k2", wantFound: true, wantCount: 1},
		{name: "get deleted k2", op: opGet, key: "k2", wantFound: false, wantCount: 1},
		{name: "delete absent k9", op: opDelete, key: "k9", wantFound: false, wantCount: 1},
		{name: "get absent k9", op: opGet, key: "k9", wantFound: false, wantCount: 1},
	}
}

// afterRestart is what a keyed bucket holds once keyedTable has run: the
// overwrite survives and the delete stays deleted.
func keyedAfterRestart() []engineStep {
	return []engineStep{
		{name: "k1 survives restart with its overwrite", op: opGet, key: "k1", tag: "v1b", wantFound: true, wantCount: 1},
		{name: "deleted k2 stays deleted", op: opGet, key: "k2", wantFound: false, wantCount: 1},
	}
}

func TestHashEngineBehaviour(t *testing.T) {
	runEngineTable(t, BucketTypeHash, keyedTable(), keyedAfterRestart())
}

func TestBTreeEngineBehaviour(t *testing.T) {
	steps := []engineStep{
		{name: "first of an empty bucket", op: opFirst, wantKeys: nil},
		{name: "last of an empty bucket", op: opLast, wantKeys: nil},
	}
	steps = append(steps, keyedTable()...)
	steps = append(steps,
		engineStep{name: "put k3", op: opPut, key: "k3", tag: "v3", wantCount: 2},
		engineStep{name: "put k4", op: opPut, key: "k4", tag: "v4", wantCount: 3},
		engineStep{name: "put k5", op: opPut, key: "k5", tag: "v5", wantCount: 4},
		engineStep{name: "range is inclusive on both bounds", op: opRange, low: "k3", high: "k5", wantKeys: []string{"k3", "k4", "k5"}, wantCount: 4},
		engineStep{name: "range with an unbounded low", op: opRange, high: "k3", wantKeys: []string{"k1", "k3"}, wantCount: 4},
		engineStep{name: "range with an unbounded high", op: opRange, low: "k4", wantKeys: []string{"k4", "k5"}, wantCount: 4},
		engineStep{name: "empty range", op: opRange, low: "x", high: "z", wantKeys: nil, wantCount: 4},
		engineStep{name: "first", op: opFirst, wantKeys: []string{"k1"}, wantCount: 4},
		engineStep{name: "last", op: opLast, wantKeys: []string{"k5"}, wantCount: 4},
	)
	afterRestart := []engineStep{
		{name: "k1 survives restart with its overwrite", op: opGet, key: "k1", tag: "v1b", wantFound: true, wantCount: 4},
		{name: "deleted k2 stays deleted", op: opGet, key: "k2", wantFound: false, wantCount: 4},
		{name: "range after restart", op: opRange, low: "k1", high: "k5", wantKeys: []string{"k1", "k3", "k4", "k5"}, wantCount: 4},
		{name: "first after restart", op: opFirst, wantKeys: []string{"k1"}, wantCount: 4},
		{name: "last after restart", op: opLast, wantKeys: []string{"k5"}, wantCount: 4},
	}
	runEngineTable(t, BucketTypeBTree, steps, afterRestart)
}

func TestBlobEngineBehaviour(t *testing.T) {
	steps := []engineStep{
		{name: "put b1", op: opBlobPut, key: "b1", tag: "m1", payload: []byte("payload one"), wantCount: 1},
		{name: "read b1: bytes and metadata", op: opBlobRead, key: "b1", tag: "m1", payload: []byte("payload one"), wantFound: true, wantCount: 1},
		{name: "put b2", op: opBlobPut, key: "b2", tag: "m2", payload: []byte("payload two"), wantCount: 2},
		{name: "overwrite b1", op: opBlobPut, key: "b1", tag: "m1b", payload: []byte("payload one, rewritten"), wantCount: 2},
		{name: "read overwritten b1", op: opBlobRead, key: "b1", tag: "m1b", payload: []byte("payload one, rewritten"), wantFound: true, wantCount: 2},
		{name: "delete b2", op: opDelete, key: "b2", wantFound: true, wantCount: 1},
		{name: "read deleted b2", op: opBlobRead, key: "b2", wantFound: false, wantCount: 1},
		// A blob delete of an absent key reports failure today, unlike hash and btree.
		{name: "delete absent b9", op: opDelete, key: "b9", wantFound: false, wantCount: 1},
	}
	afterRestart := []engineStep{
		{name: "b1 survives restart with its overwrite", op: opBlobRead, key: "b1", tag: "m1b", payload: []byte("payload one, rewritten"), wantFound: true, wantCount: 1},
		{name: "deleted b2 stays deleted", op: opBlobRead, key: "b2", wantFound: false, wantCount: 1},
	}
	runEngineTable(t, BucketTypeBlob, steps, afterRestart)
}

// runEngineTable runs steps against a fresh bucket of btype, restarts the
// server over the same data directory, and runs afterRestart.
func runEngineTable(t *testing.T, btype BucketType, steps, afterRestart []engineStep) {
	t.Helper()
	const database, bucket = "app", "engine"

	dataDir := t.TempDir()
	first := startCatalogServerAt(t, dataDir)
	createTestBucket(t, first.core, database, bucket, btype)
	for _, step := range steps {
		runEngineStep(t, first.core, database, bucket, step)
	}

	second := restartCatalog(t, first, dataDir)
	for _, step := range afterRestart {
		runEngineStep(t, second.core, database, bucket, step)
	}
}

func runEngineStep(t *testing.T, s *Server, database, bucket string, step engineStep) {
	t.Helper()
	switch step.op {
	case opPut:
		resp, err := s.Insert(InsertRequest{Database: database, Bucket: bucket, Key: step.key, Data: map[string]interface{}{"tag": step.tag}})
		if err != nil || !resp.Success {
			t.Fatalf("%s: insert = %+v, %v", step.name, resp, err)
		}
	case opGet:
		resp, err := s.Find(FindRequest{Database: database, Bucket: bucket, Key: step.key})
		if err != nil {
			t.Fatalf("%s: find: %v", step.name, err)
		}
		if resp.Found != step.wantFound {
			t.Fatalf("%s: find found = %v, want %v (%s)", step.name, resp.Found, step.wantFound, resp.Message)
		}
		if step.wantFound {
			var doc bson.M
			if err := bson.Unmarshal(resp.Data, &doc); err != nil {
				t.Fatalf("%s: decode found document: %v", step.name, err)
			}
			if doc["tag"] != step.tag {
				t.Fatalf("%s: tag = %v, want %q", step.name, doc["tag"], step.tag)
			}
			if doc["_id_"] != step.key {
				t.Fatalf("%s: _id_ = %v, want %q", step.name, doc["_id_"], step.key)
			}
		}
	case opDelete:
		resp, err := s.Delete(DeleteRequest{Database: database, Bucket: bucket, Key: step.key})
		if err != nil {
			t.Fatalf("%s: delete: %v", step.name, err)
		}
		if resp.Success != step.wantFound {
			t.Fatalf("%s: delete success = %v, want %v (%s)", step.name, resp.Success, step.wantFound, resp.Message)
		}
	case opRange:
		resp, err := s.RangeQuery(BTreeRangeRequest{Database: database, Bucket: bucket, Low: step.low, High: step.high})
		if err != nil {
			t.Fatalf("%s: range: %v", step.name, err)
		}
		assertEntryKeys(t, step, resp.Entries)
	case opFirst, opLast:
		resp, err := s.Edge(database, bucket, step.op == opLast)
		if err != nil {
			t.Fatalf("%s: edge: %v", step.name, err)
		}
		assertEntryKeys(t, step, resp.Entries)
	case opBlobPut:
		resp, err := s.BlobWrite(BlobWriteRequest{Database: database, Bucket: bucket, Key: step.key, Data: step.payload, Metadata: map[string]interface{}{"tag": step.tag}})
		if err != nil || !resp.Success {
			t.Fatalf("%s: blob write = %+v, %v", step.name, resp, err)
		}
	case opBlobRead:
		resp, err := s.BlobRead(BlobReadRequest{Database: database, Bucket: bucket, Key: step.key})
		if err != nil {
			t.Fatalf("%s: blob read: %v", step.name, err)
		}
		if resp.Found != step.wantFound {
			t.Fatalf("%s: blob read found = %v, want %v (%s)", step.name, resp.Found, step.wantFound, resp.Message)
		}
		if step.wantFound {
			if string(resp.Data) != string(step.payload) {
				t.Fatalf("%s: blob bytes = %q, want %q", step.name, resp.Data, step.payload)
			}
			if resp.Metadata["tag"] != step.tag {
				t.Fatalf("%s: blob metadata tag = %v, want %q", step.name, resp.Metadata["tag"], step.tag)
			}
		}
	default:
		t.Fatalf("%s: unknown engine op %q", step.name, step.op)
	}

	count, err := s.Count(CountRequest{Database: database, Bucket: bucket})
	if err != nil {
		t.Fatalf("%s: count: %v", step.name, err)
	}
	if count.Count != step.wantCount {
		t.Fatalf("%s: count = %d, want %d", step.name, count.Count, step.wantCount)
	}
}

func assertEntryKeys(t *testing.T, step engineStep, entries []BTreeEntry) {
	t.Helper()
	var gotKeys []string
	for _, entry := range entries {
		gotKeys = append(gotKeys, entry.Key)
	}
	if !reflect.DeepEqual(gotKeys, step.wantKeys) {
		t.Fatalf("%s: keys = %v, want %v", step.name, gotKeys, step.wantKeys)
	}
}

// TestHashCountSurvivesRestartWithoutRecount checks on the shared store that
// the count is persisted in the bucket's state document in the batch of every
// write, so a restart reports it exactly without walking the bucket
// (HashStore.Recount is the audit path, never the open path — spec §3.2).
func TestHashCountSurvivesRestartWithoutRecount(t *testing.T) {
	const database, bucket = "app", "counted"
	const documents, perBatch = 100_000, 1_000

	var recounts atomic.Int64
	countRecount := func() { recounts.Add(1) }
	store.HashRecountHook.Store(&countRecount)
	t.Cleanup(func() { store.HashRecountHook.Store(nil) })

	dataDir := t.TempDir()
	first := startCatalogServerAt(t, dataDir)
	createTestBucket(t, first.core, database, bucket, BucketTypeHash)
	for batch := 0; batch < documents/perBatch; batch++ {
		docs := make([]map[string]interface{}, 0, perBatch)
		for i := 0; i < perBatch; i++ {
			docs = append(docs, map[string]interface{}{"_id_": fmt.Sprintf("doc-%06d", batch*perBatch+i)})
		}
		if resp, err := first.core.BatchInsert(BatchInsertRequest{Database: database, Bucket: bucket, Data: docs}); err != nil || !resp.Success {
			t.Fatalf("batch %d: %+v, %v", batch, resp, err)
		}
	}

	second := restartCatalog(t, first, dataDir)
	counted, err := second.core.Count(CountRequest{Database: database, Bucket: bucket})
	if err != nil {
		t.Fatalf("count after restart: %v", err)
	}
	if counted.Count != documents {
		t.Fatalf("count after restart = %d, want %d", counted.Count, documents)
	}
	if n := recounts.Load(); n != 0 {
		t.Fatalf("HashStore.Recount ran %d time(s) across the restart; the open path must not walk the bucket", n)
	}
}

// TestHashReplayIsGated is spec §4.4 rule 2 for hash: a replayed put at or
// below the bucket's applied_seq changes nothing — not the document, not the
// count, not the mark.
func TestHashReplayIsGated(t *testing.T) {
	const database, bucket = "app", "gated"
	s := newTestServer(t)
	createTestBucket(t, s, database, bucket, BucketTypeHash)

	put := func(ctx context.Context, value string) {
		t.Helper()
		resp, err := s.InsertCtx(ctx, InsertRequest{Database: database, Bucket: bucket, Key: "k", Data: map[string]interface{}{"v": value}})
		if err != nil || !resp.Success {
			t.Fatalf("put %s: %+v, %v", value, resp, err)
		}
	}
	put(withApplySeq(context.Background(), 10), "live")
	for _, seq := range []uint64{5, 10} {
		put(withReplay(withApplySeq(context.Background(), seq)), fmt.Sprintf("replayed-at-%d", seq))
	}

	found, err := s.Find(FindRequest{Database: database, Bucket: bucket, Key: "k"})
	if err != nil || !found.Found {
		t.Fatalf("find k: %+v, %v", found, err)
	}
	var doc struct {
		V string `bson:"v"`
	}
	if err := bson.Unmarshal(found.Data, &doc); err != nil {
		t.Fatalf("decode k: %v", err)
	}
	if doc.V != "live" {
		t.Fatalf("k = %q after replays at or below applied_seq 10, want the live write's value", doc.V)
	}
	st := bucketStateOf(t, s, database, bucket)
	if st.AppliedSeq != 10 || st.Entries != 1 {
		t.Fatalf("state after gated replays: applied_seq %d, entries %d; want 10 and 1", st.AppliedSeq, st.Entries)
	}
}

// TestRecreatedHashBucketReadsNoOldKey is T-04 with the dropped range still
// in the store: the reclaimer is held, so the re-created bucket is read while
// every key of the dropped one is there, and a bucket re-created under the same
// name gets a new id, so no read of it returns a key of the dropped one. Once
// the reclaimer is released, the dropped range is emptied (Step 15; until then
// the test read the interim list of ids Step 8 kept for it).
func TestRecreatedHashBucketReadsNoOldKey(t *testing.T) {
	const database, bucket = "app", "recreated"
	s := newTestServer(t)
	createTestBucket(t, s, database, bucket, BucketTypeHash)
	if resp, err := s.Insert(InsertRequest{Database: database, Bucket: bucket, Key: "old", Data: map[string]interface{}{"v": 1}}); err != nil || !resp.Success {
		t.Fatalf("insert: %+v, %v", resp, err)
	}
	dropped := lookupBucket(s, database, bucket)
	droppedID, droppedSlot, droppedDatabaseID := dropped.id.Load(), dropped.slot, dropped.databaseID
	release := holdReclaimer(t)
	if resp, err := s.DeleteBucket(DeleteBucketRequest{Database: database, Name: bucket}); err != nil || !resp.Success {
		t.Fatalf("drop: %+v, %v", resp, err)
	}
	if n := liveKeysIn(t, s.kv, droppedSlot, droppedDatabaseID, droppedID); n == 0 {
		t.Fatal("the dropped range is already empty with the reclaimer held, so the reads below prove nothing")
	}

	createTestBucket(t, s, database, bucket, BucketTypeHash)
	if id := lookupBucket(s, database, bucket).id.Load(); id == droppedID {
		t.Fatalf("the re-created bucket was given the dropped bucket's id %d", id)
	}
	found, err := s.Find(FindRequest{Database: database, Bucket: bucket, Key: "old"})
	if err != nil {
		t.Fatalf("find: %v", err)
	}
	if found.Found {
		t.Fatal("the re-created bucket returned a key of the dropped one")
	}
	if n, err := s.Count(CountRequest{Database: database, Bucket: bucket}); err != nil || n.Count != 0 {
		t.Fatalf("the re-created bucket counts %v (%v), want 0", n, err)
	}

	release()
	waitReclaimed(t, s.kv, time.Minute)
	if n := liveKeysIn(t, s.kv, droppedSlot, droppedDatabaseID, droppedID); n != 0 {
		t.Fatalf("after the reclaim the dropped bucket %d still holds %d live keys", droppedID, n)
	}
}
