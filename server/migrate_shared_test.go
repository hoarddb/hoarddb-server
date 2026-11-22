package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Build doc Step 18: migrate and seeding carry every engine, blob payloads
// included. T-12, with its Part A slot clause under SA-26's forced source slot.

// migrationPair is one bucket's source (its RF 1 owner) and a target that
// does not hold it.
type migrationPair struct {
	source, target int
}

func migrationPairFor(t *testing.T, tc *testCluster, bucket string) migrationPair {
	t.Helper()
	owner, err := tc.nodes[0].server.core.cluster.Owner(placementOf(testDatabase, bucket))
	if err != nil {
		t.Fatalf("owner of %s: %v", bucket, err)
	}
	source := nodeIndexByAddr(t, tc.addrs, owner.ID)
	return migrationPair{source: source, target: (source + 1) % len(tc.addrs)}
}

func (p migrationPair) pull(t *testing.T, tc *testCluster, bucket string) {
	t.Helper()
	target := tc.nodes[p.target].server.core.cluster
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := target.pullBucket(ctx, testDatabase, bucket, mustNode(t, target, tc.addrs[p.source])); err != nil {
		t.Fatalf("migrating %s from node %d to node %d: %v", bucket, p.source, p.target, err)
	}
}

// drainQueue pops a queue bucket on one node, locally, until it is empty, and
// returns each element's "n".
func drainQueue(t *testing.T, s *Server, bucket string) []int32 {
	t.Helper()
	var order []int32
	for {
		popped, err := s.Pop(PopRequest{Database: testDatabase, Bucket: bucket})
		if err != nil {
			t.Fatalf("pop %s: %v", bucket, err)
		}
		if !popped.Found {
			return order
		}
		var element struct {
			N int32 `bson:"n"`
		}
		if err := bson.Unmarshal(popped.Items[0], &element); err != nil {
			t.Fatalf("decoding an element of %s: %v", bucket, err)
		}
		order = append(order, element.N)
	}
}

// queueAfterPushesAndAPop pushes 1..5 onto a queue through its owner, pops
// one — so its head cursor is past zero — and returns the order the rest must
// leave in.
func queueAfterPushesAndAPop(t *testing.T, tc *testCluster, owner int, bucket string, push, pop transport.Opcode, lifo bool) []int32 {
	t.Helper()
	for n := 1; n <= 5; n++ {
		tc.ok(owner, push, PushRequest{Database: testDatabase, Bucket: bucket, Data: map[string]interface{}{"n": n}})
	}
	tc.ok(owner, pop, PopRequest{Database: testDatabase, Bucket: bucket})
	if lifo {
		return []int32{4, 3, 2, 1}
	}
	return []int32{2, 3, 4, 5}
}

// firstFreeSlot returns a slot that none of names hashes to, and that is not
// zero: the forced source slot must be one the target could not have written
// for any bucket in the test.
func firstFreeSlot(names ...string) uint16 {
	taken := make(map[uint16]bool, len(names))
	for _, name := range names {
		taken[storage.SlotOf(storage.PlacementKey(testDatabase, name, nil))] = true
	}
	for slot := uint16(1); ; slot++ {
		if !taken[slot] {
			return slot
		}
	}
}

// keysWithSlot counts the keys in a node's store that begin with slot.
func keysWithSlot(t *testing.T, kv storage.KV, slot uint16) int {
	t.Helper()
	lower := binary.BigEndian.AppendUint16(nil, slot)
	upper := binary.BigEndian.AppendUint16(nil, slot+1)
	snap := kv.NewSnapshot()
	defer snap.Discard()
	it := snap.NewIterator(storage.IterOptions{Lower: lower, Upper: upper})
	defer it.Close()
	count := 0
	for it.Seek(nil); it.Valid(); it.Next() {
		count++
	}
	return count
}

func payloadDigests(t *testing.T, paths []string) map[[sha256.Size]byte]bool {
	t.Helper()
	digests := make(map[[sha256.Size]byte]bool, len(paths))
	for _, path := range paths {
		content, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("reading payload %s: %v", path, err)
		}
		digests[sha256.Sum256(content)] = true
	}
	return digests
}

// TestMigrateCarriesQueuesAndBlobPayloads is T-12: a fifo, a lifo and a blob
// bucket — two payloads above 64 KiB, one inline — migrate, and the target
// serves all three: each queue in its order from its moved head, each blob
// byte-identical, and every payload file's SHA-256 on the target equal to the
// source's. The slot clause (spec §5.3, build doc SA-26): the blob bucket's
// slot is forced on the source, so the target — which records its own — can
// be told apart from one that wrote the source's prefix verbatim.
func TestMigrateCarriesQueuesAndBlobPayloads(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})

	const fifo, lifo, blob = "mq_fifo", "mq_lifo", "mq_blob"
	fifoPair, lifoPair, blobPair := migrationPairFor(t, tc, fifo), migrationPairFor(t, tc, lifo), migrationPairFor(t, tc, blob)

	tc.ok(fifoPair.source, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: fifo, Type: BucketTypeFIFO})
	tc.ok(lifoPair.source, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: lifo, Type: BucketTypeLIFO})
	fifoWant := queueAfterPushesAndAPop(t, tc, fifoPair.source, fifo, transport.OpFIFOPush, transport.OpFIFOPop, false)
	lifoWant := queueAfterPushesAndAPop(t, tc, lifoPair.source, lifo, transport.OpLIFOPush, transport.OpLIFOPop, true)

	// SA-26: the blob bucket's source slot is forced, and the hook cleared
	// before the migration so the target computes the real one. RF 1 keeps the
	// create off every other node while the hook is set.
	forced := firstFreeSlot(fifo, lifo, blob)
	clearForced := forceSlot(t, forced, blob)
	tc.ok(blobPair.source, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: blob, Type: BucketTypeBlob})
	clearForced()
	blobSource := tc.nodes[blobPair.source].server.core
	sourceBucket := lookupBucket(blobSource, testDatabase, blob)
	if sourceBucket.slot != forced || forced == storage.SlotOf(storage.PlacementKey(testDatabase, blob, nil)) {
		t.Fatalf("precondition: the source recorded slot %d for %s, want the forced %d, which differs from its SlotOf %d",
			sourceBucket.slot, blob, forced, storage.SlotOf(storage.PlacementKey(testDatabase, blob, nil)))
	}
	contents := map[string][]byte{
		"large-a": bytes.Repeat([]byte("A"), store.InlineThreshold+10),
		"large-b": bytes.Repeat([]byte("Bb"), store.InlineThreshold),
		"inline":  []byte("a small blob kept inline in its metadata"),
	}
	for key, content := range contents {
		tc.ok(blobPair.source, transport.OpBlobWrite, BlobWriteRequest{Database: testDatabase, Bucket: blob, Key: key, Data: content})
	}
	sourceFiles := payloadFilesOf(t, blobSource.blobRoot(), sourceBucket.id.Load())
	if len(sourceFiles) != 2 {
		t.Fatalf("precondition: the source holds %d payload files for %s, want 2", len(sourceFiles), blob)
	}
	sourceKeys := liveKeysIn(t, blobSource.kv, sourceBucket.slot, sourceBucket.databaseID, sourceBucket.id.Load())

	fifoPair.pull(t, tc, fifo)
	lifoPair.pull(t, tc, lifo)
	blobPair.pull(t, tc, blob)

	// The slot clause first: it is the structural check, and a target that
	// wrote the stream under the source's prefix fails every read below too.
	blobTarget := tc.nodes[blobPair.target].server.core
	targetBucket := lookupBucket(blobTarget, testDatabase, blob)
	if targetBucket.slot == forced || targetBucket.slot != storage.SlotOf(storage.PlacementKey(testDatabase, blob, nil)) {
		t.Fatalf("the target recorded slot %d for %s, want its own SlotOf %d, not the source's forced %d",
			targetBucket.slot, blob, storage.SlotOf(storage.PlacementKey(testDatabase, blob, nil)), forced)
	}
	if n := keysWithSlot(t, blobTarget.kv, forced); n != 0 {
		t.Fatalf("the target's store holds %d keys under the source's forced slot %d: the stream was written with the source's prefix", n, forced)
	}
	if n := liveKeysIn(t, blobTarget.kv, targetBucket.slot, targetBucket.databaseID, targetBucket.id.Load()); n != sourceKeys {
		t.Fatalf("the target holds %d keys under its own slot %d and id %d, the source %d under its own", n, targetBucket.slot, targetBucket.id.Load(), sourceKeys)
	}
	targetFiles := payloadFilesOf(t, blobTarget.blobRoot(), targetBucket.id.Load())
	for _, path := range targetFiles {
		parsed, ok := store.ParsePayloadFileName(filepath.Base(path))
		if !ok || parsed.Slot != targetBucket.slot || parsed.BucketID != targetBucket.id.Load() {
			t.Fatalf("the payload file %s on the target does not name the target's slot %d and id %d", path, targetBucket.slot, targetBucket.id.Load())
		}
	}

	if got := drainQueue(t, tc.nodes[fifoPair.target].server.core, fifo); !equalInt32s(got, fifoWant) {
		t.Fatalf("the migrated fifo pops %v, want %v", got, fifoWant)
	}
	if got := drainQueue(t, tc.nodes[lifoPair.target].server.core, lifo); !equalInt32s(got, lifoWant) {
		t.Fatalf("the migrated lifo pops %v, want %v", got, lifoWant)
	}

	sourceDigests, targetDigests := payloadDigests(t, sourceFiles), payloadDigests(t, targetFiles)
	if len(targetFiles) != len(sourceFiles) {
		t.Fatalf("the target holds %d payload files for %s, the source %d: the payloads did not arrive", len(targetFiles), blob, len(sourceFiles))
	}
	for digest := range sourceDigests {
		if !targetDigests[digest] {
			t.Fatalf("a payload's SHA-256 on the source (%x) has no match on the target", digest)
		}
	}
	for key, content := range contents {
		read, err := blobTarget.BlobRead(BlobReadRequest{Database: testDatabase, Bucket: blob, Key: key})
		if err != nil || !read.Found || !bytes.Equal(read.Data, content) {
			t.Fatalf("the migrated blob %s is not served byte-identical: %s", key, describeBlobRead(read, err))
		}
	}
}

// searchNames searches a bucket on one node, locally, and returns the "name"
// of every hit, sorted, with the response's message.
func searchNames(t *testing.T, s *Server, bucket string, filter map[string]interface{}) ([]string, string) {
	t.Helper()
	hits, err := s.Search(SearchRequest{Database: testDatabase, Bucket: bucket, Filter: filter})
	if err != nil {
		t.Fatalf("search %s by %v: %v", bucket, filter, err)
	}
	names := make([]string, 0, len(hits.Results))
	for _, doc := range decodedDocuments(t, hits.Results) {
		name, _ := doc["name"].(string)
		names = append(names, name)
	}
	sort.Strings(names)
	return names, hits.Message
}

// TestMigratedIndexedBucketSearchesLikeItsSource: an indexed hash
// bucket migrated to a node that did not hold it is searchable there by the
// same fields, with the same hits, and the target's catalog entry records the
// same declaration as the source's, the record Server.attachBucketLocked
// re-attaches the indexes from at restart. Before
// the fix the target's entry had no indices and its search answered "no
// secondary indexes defined for this bucket".
func TestMigratedIndexedBucketSearchesLikeItsSource(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})

	const users = "indexed_users"
	pair := migrationPairFor(t, tc, users)
	tc.ok(pair.source, transport.OpCreateBucket, CreateBucketRequest{
		Database: testDatabase, Name: users, Type: BucketTypeHash,
		Options: map[string]interface{}{"indices": []interface{}{
			"email",
			map[string]interface{}{"field": "age", "type": "range"},
		}},
	})
	people := []map[string]interface{}{
		{"name": "ada", "email": "shared@example.com", "age": 36},
		{"name": "brian", "email": "shared@example.com", "age": 41},
		{"name": "cleo", "email": "cleo@example.com", "age": 29},
	}
	for _, person := range people {
		tc.ok(pair.source, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: users, Key: person["name"].(string), Data: person})
	}

	source, target := tc.nodes[pair.source].server.core, tc.nodes[pair.target].server.core
	byEmail := map[string]interface{}{"email": "shared@example.com"}
	sourceBefore, message := searchNames(t, source, users, byEmail)
	if message != "" || len(sourceBefore) != 2 {
		t.Fatalf("precondition: the source's search by email returned %v (message %q), want ada and brian", sourceBefore, message)
	}
	sourceEntry, catalogued, err := source.readCatalogEntry(testDatabase, users)
	if err != nil || !catalogued || len(sourceEntry.Indices) != 2 {
		t.Fatalf("precondition: the source's catalog entry for %s is %+v (catalogued %v, err %v), want two indices", users, sourceEntry, catalogued, err)
	}

	pair.pull(t, tc, users)

	targetEntry, catalogued, err := target.readCatalogEntry(testDatabase, users)
	if err != nil || !catalogued {
		t.Fatalf("the target has no catalog entry for %s after the migration (err %v)", users, err)
	}
	if !reflect.DeepEqual(targetEntry.Indices, sourceEntry.Indices) {
		// Not fatal: the search below shows what the missing declaration costs.
		t.Errorf("the target's catalog entry records indices %+v, the source's %+v: the declaration did not migrate", targetEntry.Indices, sourceEntry.Indices)
	}
	targetHits, message := searchNames(t, target, users, byEmail)
	if message != "" {
		t.Fatalf("the migrated copy's search by email answered %q, want the source's hits %v", message, sourceBefore)
	}
	if !reflect.DeepEqual(targetHits, sourceBefore) {
		t.Fatalf("the migrated copy's search by email returned %v, the source's %v", targetHits, sourceBefore)
	}
	// The source keeps its copy (it is still the RF 1 owner) and its index.
	if sourceAfter, message := searchNames(t, source, users, byEmail); message != "" || !reflect.DeepEqual(sourceAfter, sourceBefore) {
		t.Fatalf("after the migration the source's search by email returned %v (message %q), before it %v", sourceAfter, message, sourceBefore)
	}
}

func equalInt32s(got, want []int32) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
