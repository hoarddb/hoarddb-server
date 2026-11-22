package server

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"
	dto "github.com/prometheus/client_model/go"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Owner ruling 2026-09-30: a TTL expiry is a real delete. It removes the
// document's field stores and unlinks its blob payloads exactly as a user's
// delete does, on every node that applies it, and a document whose stores are
// too large to delete in one batch is skipped whole rather than half-deleted.

// createExpiringPost creates app/Post declaring the given store fields and a
// one-hour TTL index on CreatedAt.
func createExpiringPost(t *testing.T, s *Server, declared ...StoreDeclaration) {
	t.Helper()
	options := storesOption(declared...)
	options["indices"] = []interface{}{ttlDeclaration("CreatedAt", 3600)}
	createTestBucketWithOptions(t, s, "app", "Post", BucketTypeHash, options)
}

func insertPost(t *testing.T, s *Server, key string, createdAt time.Time) {
	t.Helper()
	mustInsert(t, s, "app", "Post", key, map[string]interface{}{"CreatedAt": primitive.NewDateTimeFromTime(createdAt)})
}

// fieldStoreKeysOf counts every key of a bucket's field-store range in a
// node's own store: elements and instance states alike.
func fieldStoreKeysOf(t *testing.T, s *Server, database, bucket string) int {
	t.Helper()
	b := lookupBucket(s, database, bucket)
	if b == nil {
		t.Fatalf("bucket %s/%s is not served", database, bucket)
	}
	snap := s.kv.NewSnapshot()
	defer snap.Discard()
	it := snap.NewIterator(storage.IterOptions{
		Lower: storage.Key(b.slot, b.databaseID, b.id.Load(), storage.NSFieldStore),
		Upper: storage.Key(b.slot, b.databaseID, b.id.Load(), storage.NSFieldStore+1),
	})
	defer it.Close()
	keys := 0
	for it.Seek(nil); it.Valid(); it.Next() {
		keys++
	}
	return keys
}

// TestTTLExpiryRemovesFieldStores: an expired document takes its field stores
// with it. Left behind, they are adopted by the next document with its key
// (nested buckets spec §4.6) — a new session inheriting an old one's elements.
func TestTTLExpiryRemovesFieldStores(t *testing.T) {
	s := newTestServer(t)
	now := time.Now()
	createExpiringPost(t, s, StoreDeclaration{Name: "Comments", Engine: "fifoStore"}, StoreDeclaration{Name: "Tags", Engine: "hashStore"})
	insertPost(t, s, "p1", now.Add(-2*time.Hour))
	for n := range 3 {
		fieldOK(t)(s.FieldPushCtx(context.Background(), FieldRequest{Database: "app", Bucket: "Post", Key: "p1", Field: "Comments", Data: map[string]interface{}{"n": n}}))
	}
	fieldOK(t)(s.FieldPutCtx(context.Background(), FieldRequest{Database: "app", Bucket: "Post", Key: "p1", Field: "Tags", Element: "t1", Data: map[string]interface{}{"n": 1}}))
	if keys := fieldStoreKeysOf(t, s, "app", "Post"); keys == 0 {
		t.Fatal("the fixture wrote no field-store key: the test proves nothing")
	}

	pass := s.sweepTTL(context.Background(), now)
	if documentOf(t, s, "app", "Post", "p1") != nil {
		t.Fatalf("the sweep (%+v) did not expire p1", pass)
	}
	if keys := fieldStoreKeysOf(t, s, "app", "Post"); keys != 0 {
		t.Errorf("expiring p1 left %d field-store keys behind", keys)
	}
	insertPost(t, s, "p1", now)
	for _, field := range []string{"Comments", "Tags"} {
		if got := fieldLen(t, s, "p1", field); got != 0 {
			t.Errorf("a new p1 holds %d %s it never wrote: it adopted the expired document's", got, field)
		}
	}
}

// TestTTLExpiryUnlinksBlobPayloads: an expired document's blobStore elements
// lose their metadata in the expiry's batch and their payload files after it.
func TestTTLExpiryUnlinksBlobPayloads(t *testing.T) {
	s := newTestServer(t)
	now := time.Now()
	createExpiringPost(t, s, StoreDeclaration{Name: "Photos", Engine: "blobStore"})
	insertPost(t, s, "p1", now.Add(-2*time.Hour))
	putPhoto(t, s, "cover", largePhoto)
	payload := onlyPayload(t, s)

	pass := s.sweepTTL(context.Background(), now)
	if documentOf(t, s, "app", "Post", "p1") != nil {
		t.Fatalf("the sweep (%+v) did not expire p1", pass)
	}
	if fileExists(t, payload) {
		t.Errorf("expiring p1 left its Photos payload %s", payload)
	}
	b := lookupBucket(s, "app", "Post")
	snap := s.kv.NewSnapshot()
	defer snap.Discard()
	it := snap.NewIterator(storage.IterOptions{
		Lower: storage.Key(b.slot, b.databaseID, b.id.Load(), storage.NSBlobMeta),
		Upper: storage.Key(b.slot, b.databaseID, b.id.Load(), storage.NSBlobMeta+1),
	})
	defer it.Close()
	if it.Seek(nil); it.Valid() {
		t.Errorf("expiring p1 left blob metadata at %x", it.Key())
	}
}

// overfillComments pushes more Comments onto key than one batch can delete,
// and returns how many.
func overfillComments(t *testing.T, s *Server, key string) int {
	t.Helper()
	return overfillCommentsIn(t, s, "app", "Post", key)
}

// overfillCommentsIn is overfillComments for any bucket that declares
// Comments as a fifoStore, written into s's own store and nowhere else.
func overfillCommentsIn(t *testing.T, s *Server, database, bucket, key string) int {
	t.Helper()
	b := servedBucket(t, s, database, bucket)
	owner, err := store.FieldOwnerPrefix(b.slot, b.databaseID, b.id.Load(), key)
	if err != nil {
		t.Fatalf("owner: %v", err)
	}
	comments, declared := b.storeFields().find("Comments", 0)
	if !declared {
		t.Fatalf("%s declares no Comments", bucket)
	}
	ref := store.FieldRef{Owner: owner, FieldID: uint16(comments.ID), Engine: store.FieldFIFO}
	element, _ := bson.Marshal(map[string]interface{}{"n": 1})
	// A delete is charged its key alone, so the cascade needs more elements
	// than the bound has bytes per key.
	// b.mu is what every writer of the bucket holds; without it this races the
	// node's own TTL sweeper, which applies to the same state.
	b.rec.mu.Lock()
	defer b.rec.mu.Unlock()
	total := int(s.kv.MaxBatchBytes()/40) + 1
	// One element per store.Apply took 58 s of this test under -race (BUG-0534):
	// every Apply is a snapshot, a state load and a commit. FieldPush reads its
	// cursor from the snapshot, so several pushes in one batch would all take the
	// same position; the chunk writes the keys FieldPush writes and the cursor
	// once, so the stored result is the one total pushes leave. The bound being
	// proved is the delete's, not the seeding's, so a chunk stays far below it.
	const chunkElements = 2000
	prefix := ref.StorePrefix()
	stateKey := store.FieldStateKey(prefix)
	for first := 0; first < total; first += chunkElements {
		last := min(first+chunkElements, total)
		cursor := store.FieldInstanceState{Tail: uint64(last - 1), Next: uint64(last), Count: uint64(last)}
		_, err := store.Apply(store.ApplyRequest{KV: s.kv, Slot: b.slot, DatabaseID: b.databaseID, ID: b.id.Load(), Owed: b.hashStore.Carry(),
			Op: func(snap storage.KVSnapshot, st *store.BucketState, batch storage.KVBatch) error {
				encodedCursor := store.EncodeFieldInstanceState(cursor)
				if first == 0 {
					st.LogicalBytes += int64(len(stateKey) + len(encodedCursor))
				}
				for position := first; position < last; position++ {
					key := store.FieldElementKey(prefix, storage.NSQueue, store.PositionRemainder(uint64(position)))
					if err := batch.Set(key, element); err != nil {
						return err
					}
					st.LogicalBytes += int64(len(key) + len(element))
				}
				return batch.Set(stateKey, encodedCursor)
			}})
		if err != nil {
			t.Fatalf("pushing elements %d to %d: %v", first, last-1, err)
		}
	}
	return total
}

// skippedExpiries reads hoarddb_ttl_sweep_documents_total{outcome="skipped"}.
func skippedExpiries(t *testing.T) float64 {
	t.Helper()
	var metric dto.Metric
	if err := ttlSweepDocuments.WithLabelValues("skipped").Write(&metric); err != nil {
		t.Fatalf("reading the skipped counter: %v", err)
	}
	return metric.GetCounter().GetValue()
}

// sweptDocuments sums hoarddb_ttl_sweep_documents_total over every outcome:
// each document a sweep pass touched, whichever way it ended.
func sweptDocuments(t *testing.T) float64 {
	t.Helper()
	var total float64
	for _, outcome := range []string{"deleted", "spared", "skipped", "failed"} {
		var metric dto.Metric
		if err := ttlSweepDocuments.WithLabelValues(outcome).Write(&metric); err != nil {
			t.Fatalf("reading the %s counter: %v", outcome, err)
		}
		total += metric.GetCounter().GetValue()
	}
	return total
}

// TestTTLExpirySkipsCascadeTooLarge: an expired document whose stores cannot
// be deleted in one batch stays, whole — never the document without its
// elements, which the next document with its key would adopt — and the pass
// goes on to expire the others. The skip is counted every pass that retries
// it and logged once, with the way out.
func TestTTLExpirySkipsCascadeTooLarge(t *testing.T) {
	// The fixture pushes MaxBatchBytes()/40 elements; at the 128 MiB default
	// that is 500,000 and takes minutes under the race detector, long enough
	// for the node's own sweeper to run a pass of its own and move the counter
	// this test reads. The floor keeps it to 125,000.
	s := newTestServerFloorMemtable(t)
	logger, captured := newLogCapture()
	s.logger = logger
	skippedBefore := skippedExpiries(t)
	now := time.Now()
	createExpiringPost(t, s, StoreDeclaration{Name: "Comments", Engine: "fifoStore"})
	for _, key := range []string{"a-small", "big", "z-small"} {
		insertPost(t, s, key, now.Add(-2*time.Hour))
	}
	insertPost(t, s, "live", now)
	for _, key := range []string{"a-small", "z-small"} {
		fieldOK(t)(s.FieldPushCtx(context.Background(), FieldRequest{Database: "app", Bucket: "Post", Key: key, Field: "Comments", Data: map[string]interface{}{"n": 1}}))
	}
	total := overfillComments(t, s, "big")

	pass := s.sweepTTL(context.Background(), now)
	if documentOf(t, s, "app", "Post", "big") == nil {
		t.Fatalf("the sweep (%+v) deleted big, whose %d Comments cannot go in its batch: they are left for the next big to adopt", pass, total)
	}
	if got := fieldLen(t, s, "big", "Comments"); got != int64(total) {
		t.Errorf("after the sweep big holds %d Comments, want all %d: a skipped document keeps its stores", got, total)
	}
	for _, key := range []string{"a-small", "z-small"} {
		if documentOf(t, s, "app", "Post", key) != nil {
			t.Errorf("the sweep (%+v) left %s: a skipped document stalled the ones around it", pass, key)
		}
	}
	if documentOf(t, s, "app", "Post", "live") == nil {
		t.Error("the sweep deleted the live document")
	}
	if pass.skipped != 1 || pass.deleted != 2 || pass.failed != 0 || pass.exhausted {
		t.Errorf("the pass reports %+v, want 1 skipped, 2 deleted, none failed and its budget not exhausted", pass)
	}
	if got := skippedExpiries(t) - skippedBefore; got != 1 {
		t.Errorf("the skipped counter moved by %v in one pass, want 1", got)
	}

	// The next pass retries it: counted again, and not logged again inside
	// the interval.
	again := s.sweepTTL(context.Background(), now)
	if again.skipped != 1 || documentOf(t, s, "app", "Post", "big") == nil {
		t.Errorf("the second pass reports %+v, want big skipped again and still present", again)
	}
	if got := skippedExpiries(t) - skippedBefore; got != 2 {
		t.Errorf("the skipped counter moved by %v over two passes, want 2", got)
	}
	wanted := []string{
		`Document "big" in bucket "Post" of database "app" has expired and was not deleted`,
		fmt.Sprintf("(Comments: %s)", groupThousands(total)),
		`db.Post["big"].Comments.pop() until len() is 0`,
		"delete the document yourself",
	}
	if lines := captured.messages(slog.LevelWarn, "TTL sweep skipped a document"); len(lines) != 1 || !containsAll(lines[0], wanted) {
		t.Errorf("two passes inside the log interval wrote %d skip lines, want one naming the document and the way out %q:\n%q", len(lines), wanted, lines)
	}
}

// TestTTLSkipLogIsRateLimited: one line per interval over every bucket, and
// the line after a quiet interval says how many skips went unlogged.
func TestTTLSkipLogIsRateLimited(t *testing.T) {
	var skips ttlSkipLog
	start := time.Now()
	if logged, unlogged := skips.due(start); !logged || unlogged != 0 {
		t.Fatalf("the first skip: logged %v with %d unlogged, want logged with 0", logged, unlogged)
	}
	for second := 1; second <= 3; second++ {
		if logged, _ := skips.due(start.Add(time.Duration(second) * time.Second)); logged {
			t.Fatalf("a skip %ds after a logged one was logged: the interval is %s", second, ttlSkipLogInterval)
		}
	}
	if logged, unlogged := skips.due(start.Add(ttlSkipLogInterval)); !logged || unlogged != 3 {
		t.Fatalf("the first skip after the interval: logged %v with %d unlogged, want logged with 3", logged, unlogged)
	}
}

// TestTTLExpirySparesARefreshedDocumentAndItsStores: the cascade runs only
// after the condition holds against the stored document. A document
// refreshed between the sweep's scan and its delete keeps itself and every
// element (S-7's guarantee, with stores under it).
func TestTTLExpirySparesARefreshedDocumentAndItsStores(t *testing.T) {
	s := newTestServer(t)
	now := time.Now()
	createExpiringPost(t, s, StoreDeclaration{Name: "Comments", Engine: "fifoStore"})
	insertPost(t, s, "p1", now.Add(-2*time.Hour))
	for n := range 3 {
		fieldOK(t)(s.FieldPushCtx(context.Background(), FieldRequest{Database: "app", Bucket: "Post", Key: "p1", Field: "Comments", Data: map[string]interface{}{"n": n}}))
	}
	// The sweep would have scanned p1 as expired; then a client refreshes it.
	insertPost(t, s, "p1", now)
	answer := dispatchDelete(t, s, expireIfPayload(t, "app", "Post", "p1", "CreatedAt", now.Add(-time.Hour)))
	if spared, _ := answer.Lookup("spared").BooleanOK(); !spared {
		t.Fatalf("the expiry of a refreshed document answered %s, want spared", answer)
	}
	if documentOf(t, s, "app", "Post", "p1") == nil {
		t.Fatal("the expiry deleted a refreshed document")
	}
	if got := fieldLen(t, s, "p1", "Comments"); got != 3 {
		t.Fatalf("a spared document holds %d Comments, want its 3: the cascade ran before the condition", got)
	}
}

// TestTTLExpiryCascadesOnEveryReplica: the expiry's cascade is applied by
// each node that applies the entry. An owner that cascades over replicas that
// do not is the adoption bug again, on the node a failover promotes.
func TestTTLExpiryCascadesOnEveryReplica(t *testing.T) {
	now := time.Now()
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	options := storesOption(StoreDeclaration{Name: "Comments", Engine: "fifoStore"})
	options["indices"] = []interface{}{ttlDeclaration("CreatedAt", 3600)}
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: "sessions", Type: BucketTypeHash, Options: options})
	put := func(key string, at time.Time) {
		tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: "sessions", Key: key,
			Data: map[string]interface{}{"CreatedAt": primitive.NewDateTimeFromTime(at)}, WriteConcern: "all"})
	}
	put("old", now.Add(-2*time.Hour))
	put("live", now)
	for n := range 3 {
		fieldCall(t, tc, 0, transport.OpFieldPush, FieldRequest{Database: testDatabase, Bucket: "sessions", Key: "old", Field: "Comments",
			Data: map[string]interface{}{"n": n}, WriteConcern: "all"})
	}
	for index := range tc.nodes {
		if keys := fieldStoreKeysOf(t, tc.nodes[index].server.core, testDatabase, "sessions"); keys == 0 {
			t.Fatalf("node %d holds no field-store key before the sweep: the test proves nothing there", index+1)
		}
	}

	owner := ownerIndexOf(t, tc, "sessions")
	tc.nodes[owner].server.core.sweepTTL(context.Background(), now)
	waitForSessions(t, tc, []int{0, 1, 2}, "live")
	// Every node is given the same deadline and reported on its own, so a
	// failure says whether the owner, the replicas or both kept the keys.
	keysOn := func(index int) int {
		return fieldStoreKeysOf(t, tc.nodes[index].server.core, testDatabase, "sessions")
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && keysOn(0)+keysOn(1)+keysOn(2) != 0 {
		time.Sleep(20 * time.Millisecond)
	}
	for index := range tc.nodes {
		role := "replica"
		if index == owner {
			role = "owner"
		}
		if keys := keysOn(index); keys != 0 {
			t.Errorf("node %d (%s) still holds %d field-store keys of the expired document: the next document with its key adopts them there",
				index+1, role, keys)
		}
	}
}

// TestTTLExpirySkipsCascadeTooLargeOnEveryReplica: the owner and
// each replica must refuse an expiry whose cascade does not fit one batch, so
// the three copies stay identical: the document and every element on all
// three, and nothing left owed by a refused apply.
func TestTTLExpirySkipsCascadeTooLargeOnEveryReplica(t *testing.T) {
	now := time.Now()
	sweptAtStart := sweptDocuments(t)
	// The floor, for the reason TestTTLExpirySkipsCascadeTooLarge gives: three
	// nodes each seeded with MaxBatchBytes()/40 elements.
	// TTLSweepSeconds 0: the test calls sweepTTL itself, and the nodes' own
	// 60 s sweeper must not expire "big" while it seeds (BUG-0253).
	tc := newTestClusterWith(t, 3, 3, func(_ int, cfg *config.Config) {
		cfg.Storage.MemtableMB = config.MinMemtableMB
		cfg.Server.TTLSweepSeconds = 0
	})
	defer tc.stop()
	options := storesOption(StoreDeclaration{Name: "Comments", Engine: "fifoStore"})
	options["indices"] = []interface{}{ttlDeclaration("CreatedAt", 3600)}
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: "sessions", Type: BucketTypeHash, Options: options})
	tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: "sessions", Key: "big",
		Data: map[string]interface{}{"CreatedAt": primitive.NewDateTimeFromTime(now.Add(-2 * time.Hour))}, WriteConcern: "all"})
	// The elements go into each node's own store, not over the wire, so all
	// three hold the same oversize cascade without replication in between.
	var total int
	keysBefore := make([]int, len(tc.nodes))
	for index := range tc.nodes {
		total = overfillCommentsIn(t, tc.nodes[index].server.core, testDatabase, "sessions", "big")
		keysBefore[index] = fieldStoreKeysOf(t, tc.nodes[index].server.core, testDatabase, "sessions")
		if keysBefore[index] < total {
			t.Fatalf("node %d holds %d field-store keys after seeding %d elements: the fixture proves nothing", index+1, keysBefore[index], total)
		}
	}

	// No background pass may have touched a document yet: one that ran during
	// the seeding would have deleted "big" before the test's own pass (BUG-0253).
	if swept := sweptDocuments(t) - sweptAtStart; swept != 0 {
		t.Fatalf("a background TTL pass touched %v documents before the test's own pass: the nodes' sweeper must be off", swept)
	}
	owner := ownerIndexOf(t, tc, "sessions")
	pass := tc.nodes[owner].server.core.sweepTTL(context.Background(), now)
	if pass.skipped != 1 {
		t.Fatalf("the owner's pass reports %+v, want big skipped", pass)
	}
	// The refusal is only tested on a replica once the replica has consumed
	// the expiry's entry: the owner logs it before applying, so wait for it in
	// every node's log, then for the node's applied mark to reach it (a refused
	// apply still advances the mark). The deadline only bounds a hang.
	deadline := time.Now().Add(60 * time.Second)
	for index := range tc.nodes {
		node := tc.nodes[index].server.core
		for walOps(t, node, "sessions")[transport.OpDelete] == 0 {
			if time.Now().After(deadline) {
				t.Fatalf("node %d never received the expiry's entry: the replica skip is not exercised", index+1)
			}
			time.Sleep(20 * time.Millisecond)
		}
		expirySeq := loggedSequence(t, node, transport.OpDelete, "sessions")
		for bucketStateOf(t, node, testDatabase, "sessions").AppliedSeq < expirySeq {
			if time.Now().After(deadline) {
				t.Fatalf("node %d logged the expiry at sequence %d and never applied or refused it", index+1, expirySeq)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	for index := range tc.nodes {
		node := tc.nodes[index].server.core
		if documentOf(t, node, testDatabase, "sessions", "big") == nil {
			t.Errorf("node %d lost the document whose %d elements cannot be deleted in one batch", index+1, total)
		}
		if keys := fieldStoreKeysOf(t, node, testDatabase, "sessions"); keys != keysBefore[index] {
			t.Errorf("node %d holds %d field-store keys, want the %d it held before the sweep", index+1, keys, keysBefore[index])
		}
		if owed := node.owed.count(); owed != 0 {
			t.Errorf("node %d owes %d applies after a refused expiry, want none", index+1, owed)
		}
	}
}
