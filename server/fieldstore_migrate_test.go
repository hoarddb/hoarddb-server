package server

import (
	"reflect"
	"testing"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// TestFieldStoresSurviveParentMigration is shared-storage spec T-22, moved
// here by shared-storage-build.md SA-6.
func TestFieldStoresSurviveParentMigration(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	const bucket = "posts_mig"
	pair := migrationPairFor(t, tc, bucket)
	tc.ok(pair.source, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash, Options: storesOption(postStores...)})
	documents := []string{"p1", "p2", "p3"}
	for _, key := range documents {
		tc.ok(pair.source, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: key, Data: map[string]interface{}{"title": key}})
	}
	for n := range 30 {
		fieldCall(t, tc, pair.source, transport.OpFieldPush, FieldRequest{Database: testDatabase, Bucket: bucket, Key: documents[n%3], Field: "Comments", Data: map[string]interface{}{"n": n}})
	}
	source := tc.nodes[pair.source].server.core
	pair.pull(t, tc, bucket)
	target := tc.nodes[pair.target].server.core

	if got, want := lookupBucket(target, testDatabase, bucket).storeFields(), lookupBucket(source, testDatabase, bucket).storeFields(); !reflect.DeepEqual(got, want) {
		t.Fatalf("the target declares %+v, the source %+v: the declaration or its ids did not travel", got, want)
	}
	entry, _, err := target.readCatalogEntry(testDatabase, bucket)
	if err != nil || !reflect.DeepEqual(entry.Stores, catalogStores(lookupBucket(source, testDatabase, bucket))) {
		t.Fatalf("the target's catalog records %+v (%v)", entry.Stores, err)
	}
	for i, key := range documents {
		req := FieldRequest{Database: testDatabase, Bucket: bucket, Key: key, Field: "Comments"}
		length, err := target.FieldLen(req)
		if err != nil || length.Len != 10 {
			t.Fatalf("%s's Comments on the target: len %+v, %v; want 10", key, length, err)
		}
		for want := i; want < 30; want += 3 {
			popped, err := target.FieldPopCtx(t.Context(), FieldPopRequest{FieldRequest: req})
			if err != nil || !popped.Found || int(popped.Items[0].Element.Lookup("n").Int32()) != want {
				t.Fatalf("%s's Comments on the target popped %+v (%v), want n %d: the order did not travel", key, popped, err, want)
			}
		}
	}
	// A document delete on the target cascades: the migrated keys are its own.
	// Through the target's core, as the pops above: the ring still places the
	// bucket on the source, which a client write would be forwarded to.
	if resp, err := target.DeleteCtx(t.Context(), DeleteRequest{Database: testDatabase, Bucket: bucket, Key: "p1"}); err != nil || !resp.Success {
		t.Fatalf("deleting p1 on the target: %+v, %v", resp, err)
	}
	targetBucket := lookupBucket(target, testDatabase, bucket)
	owner, err := store.FieldOwnerPrefix(targetBucket.slot, targetBucket.databaseID, targetBucket.id.Load(), "p1")
	if err != nil {
		t.Fatalf("owner: %v", err)
	}
	snap := target.kv.NewSnapshot()
	defer snap.Discard()
	it := snap.NewIterator(storage.IterOptions{Lower: owner, Upper: append(append([]byte(nil), owner[:len(owner)-1]...), owner[len(owner)-1]+1)})
	defer it.Close()
	if it.Seek(nil); it.Valid() {
		t.Fatalf("deleting p1 on the target left its field-store key %x", it.Key())
	}
}

func TestMigratedDeclarationPreservesIds(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	const bucket = "posts_ids"
	pair := migrationPairFor(t, tc, bucket)
	tc.ok(pair.source, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	source := tc.nodes[pair.source].server.core
	gapped := []catalogStore{{Name: "A", ID: 1, Engine: "fifo"}, {Name: "C", ID: 3, Engine: "hash"}}
	// A source whose B (id 2) was never there, recorded as a restart would
	// have adopted it.
	writeCatalogStores(t, source, testDatabase, bucket, gapped, 4)
	recorded := lookupBucket(source, testDatabase, bucket)
	recorded.rec.mu.Lock()
	source.setStoreFields(recorded, &storeFieldSet{stores: gapped, next: 4})
	recorded.rec.mu.Unlock()

	pair.pull(t, tc, bucket)
	got := lookupBucket(tc.nodes[pair.target].server.core, testDatabase, bucket).storeFields()
	if !reflect.DeepEqual(got.stores, gapped) || got.next != 4 {
		t.Fatalf("the target declares %+v, next %d; want exactly %+v, next 4: C must keep id 3", got.stores, got.next, gapped)
	}
}
