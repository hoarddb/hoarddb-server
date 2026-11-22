package server

import (
	"context"
	"fmt"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Step 11.4a (dispatch 11): a delete bounded by a version, evaluated in the
// apply, which a consistent get's repair of a deleted key sends.

// boundedBucket is one standalone bucket of a type, declaring a fifoStore
// field, for the bounded delete's tests.
type boundedBucket struct {
	s      *Server
	name   string
	bucket *inMemoryBucket
}

func newBoundedBucket(t *testing.T, s *Server, bucketType BucketType) boundedBucket {
	t.Helper()
	name := fmt.Sprintf("Bounded%s", bucketType)
	resp, err := s.CreateBucket(CreateBucketRequest{Database: "app", Name: name, Type: bucketType,
		Options: storesOption(StoreDeclaration{Name: "Comments", Engine: "fifoStore"})})
	if err != nil || !resp.Success {
		t.Fatalf("creating %s: %+v, %v", name, resp, err)
	}
	return boundedBucket{s: s, name: name, bucket: lookupBucket(s, "app", name)}
}

// write puts key at version, with one element in its Comments store; version
// 0 leaves the key with no version side-key.
func (b boundedBucket) write(t *testing.T, key string, version uint64) {
	t.Helper()
	b.bucket.rec.mu.Lock()
	var err error
	if b.bucket.bucketType == BucketTypeBTree {
		err = b.bucket.btreeStore.Put(store.ApplyInfo{}, key, map[string]interface{}{"title": key}, version)
	} else {
		err = b.bucket.hashStore.Put(store.ApplyInfo{}, key, map[string]interface{}{"title": key}, version)
	}
	b.bucket.rec.mu.Unlock()
	if err != nil {
		t.Fatalf("writing %s at version %d: %v", key, version, err)
	}
	ref := b.comments(t, key)
	element, _ := bson.Marshal(map[string]interface{}{"c": 1})
	owed := b.bucket.btreeStore.Carry
	if b.bucket.bucketType == BucketTypeHash {
		owed = b.bucket.hashStore.Carry
	}
	if _, err := store.Apply(store.ApplyRequest{KV: b.s.kv, Slot: b.bucket.slot, DatabaseID: b.bucket.databaseID, ID: b.bucket.id.Load(), Owed: owed(),
		Op: func(snap storage.KVSnapshot, st *store.BucketState, batch storage.KVBatch) error {
			_, err := store.FieldPush(store.FieldWrite{Snap: snap, State: st, Batch: batch}, ref, element)
			return err
		}}); err != nil {
		t.Fatalf("pushing a comment onto %s: %v", key, err)
	}
}

func (b boundedBucket) comments(t *testing.T, key string) store.FieldRef {
	t.Helper()
	owner, err := store.FieldOwnerPrefix(b.bucket.slot, b.bucket.databaseID, b.bucket.id.Load(), key)
	if err != nil {
		t.Fatalf("the field owner of %s: %v", key, err)
	}
	declared, _ := b.bucket.storeFields().find("Comments", 0)
	return store.FieldRef{Owner: owner, FieldID: uint16(declared.ID), Engine: store.FieldFIFO}
}

// deleteBelow is the store's bounded delete of key.
func (b boundedBucket) deleteBelow(t *testing.T, key string, below uint64) bool {
	t.Helper()
	b.bucket.rec.mu.Lock()
	defer b.bucket.rec.mu.Unlock()
	var (
		spared bool
		err    error
	)
	if b.bucket.bucketType == BucketTypeBTree {
		spared, _, err = b.bucket.btreeStore.DeleteDocumentBelow(store.ApplyInfo{}, key, b.bucket.fieldLayout(), below)
	} else {
		spared, _, err = b.bucket.hashStore.DeleteDocumentBelow(store.ApplyInfo{}, key, b.bucket.fieldLayout(), below)
	}
	if err != nil {
		t.Fatalf("deleting %s below %d: %v", key, below, err)
	}
	return spared
}

// held says whether key's document, its version side-key and its field
// store's element are there.
func (b boundedBucket) held(t *testing.T, key string) (document bool, version uint64, comments uint64) {
	t.Helper()
	found, _, version, err := b.s.versionedFind("app", b.name, key)
	if err != nil {
		t.Fatalf("reading %s: %v", key, err)
	}
	snap := b.s.kv.NewSnapshot()
	defer snap.Discard()
	comments, err = store.FieldLen(snap, b.comments(t, key))
	if err != nil {
		t.Fatalf("counting %s's comments: %v", key, err)
	}
	return found, version, comments
}

// TestADeleteBoundedByAVersionSparesANewerDocument: under a bound at its
// version a document is kept whole — its side-key and its field store too —
// and the delete reports spared with no error; one above deletes it with both.
// The bound is exclusive: a bound of 1 keeps a document at a version and
// deletes one with no side-key, and 0 is no bound. An absent key under a bound
// deletes nothing and is not spared. Hash and btree alike. Guards: ignore the
// bound; compare inclusively; skip after the field-store cascade.
func TestADeleteBoundedByAVersionSparesANewerDocument(t *testing.T) {
	s := benchServer(t)
	if _, err := s.CreateDatabase("app"); err != nil {
		t.Fatal(err)
	}
	const v = uint64(1)<<44 | 7
	for _, bucketType := range []BucketType{BucketTypeHash, BucketTypeBTree} {
		b := newBoundedBucket(t, s, bucketType)
		b.write(t, "kept", v)
		if spared := b.deleteBelow(t, "kept", v); !spared {
			t.Errorf("%s: a delete bounded at the document's version did not report spared", bucketType)
		}
		if found, version, comments := b.held(t, "kept"); !found || version != v || comments != 1 {
			t.Errorf("%s: a delete bounded at the document's version left found=%v version=%d comments=%d; want it whole", bucketType, found, version, comments)
		}
		if spared := b.deleteBelow(t, "kept", v+1); spared {
			t.Errorf("%s: a delete bounded above the document's version reported spared", bucketType)
		}
		if found, version, comments := b.held(t, "kept"); found || version != 0 || comments != 0 {
			t.Errorf("%s: a delete bounded above the version left found=%v version=%d comments=%d", bucketType, found, version, comments)
		}

		b.write(t, "versioned", v)
		b.write(t, "unversioned", 0)
		if spared := b.deleteBelow(t, "versioned", 1); !spared {
			t.Errorf("%s: bound 1 did not spare a document at version %d", bucketType, v)
		}
		b.deleteBelow(t, "unversioned", 1)
		if found, _, _ := b.held(t, "versioned"); !found {
			t.Errorf("%s: bound 1 deleted a document at version %d", bucketType, v)
		}
		if found, _, comments := b.held(t, "unversioned"); found || comments != 0 {
			t.Errorf("%s: bound 1 left a document with no version (found=%v comments=%d)", bucketType, found, comments)
		}
		b.write(t, "unversioned", 0)
		b.deleteBelow(t, "versioned", 0)
		b.deleteBelow(t, "unversioned", 0)
		for _, key := range []string{"versioned", "unversioned"} {
			if found, _, _ := b.held(t, key); found {
				t.Errorf("%s: an unbounded delete left %s", bucketType, key)
			}
		}

		if spared := b.deleteBelow(t, "absent", v); spared {
			t.Errorf("%s: a bounded delete of an absent key reported spared", bucketType)
		}
	}
}

// TestASparedRepairDeleteIsAnsweredSpared: a bounded delete that finds the
// document newer than its bound is answered Success and Spared, as a spared
// expiry is, and one that deletes it "document deleted". Guard: decide the
// answer from the existence read alone (the spared case is answered "document
// deleted").
func TestASparedRepairDeleteIsAnsweredSpared(t *testing.T) {
	s := benchServer(t)
	if _, err := s.CreateDatabase("app"); err != nil {
		t.Fatal(err)
	}
	const v = uint64(1)<<44 | 9
	for _, bucketType := range []BucketType{BucketTypeHash, BucketTypeBTree} {
		b := newBoundedBucket(t, s, bucketType)
		b.write(t, "k", v)
		spared, err := s.DeleteCtx(context.Background(), DeleteRequest{Database: "app", Bucket: b.name, Key: "k", RepairBound: v})
		if err != nil || !spared.Success || !spared.Spared {
			t.Errorf("%s: a delete bounded at the stored version was answered %+v, %v; want Success and Spared", bucketType, spared, err)
		}
		deleted, err := s.DeleteCtx(context.Background(), DeleteRequest{Database: "app", Bucket: b.name, Key: "k", RepairBound: v + 1})
		if err != nil || !deleted.Success || deleted.Spared || deleted.Message != "document deleted" {
			t.Errorf("%s: a delete bounded above the stored version was answered %+v, %v; want document deleted", bucketType, deleted, err)
		}
	}
}

// TestAClientCannotSendARepairBound: a client's delete carrying repair_bound
// is refused at the client port, INVALID_ARGUMENT, naming the field, and the
// key is still there. Guard: no refusal (the key is deleted).
func TestAClientCannotSendARepairBound(t *testing.T) {
	tc := newTestCluster(t, 1, 1)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: testBucket, Key: "k", Data: map[string]interface{}{"n": 1}})
	frame := tc.frame(0, transport.OpDelete, bson.M{"database": testDatabase, "bucket": testBucket, "key": "k", "repair_bound": int64(1)})
	var refusal ErrorResponse
	if frame.Opcode != transport.OpError || bson.Unmarshal(frame.Payload, &refusal) != nil {
		t.Fatalf("a client delete carrying repair_bound was answered %s: %s", frame.Opcode, frame.Payload)
	}
	if transport.ErrorCode(refusal.Code) != transport.ErrInvalidArgument || refusal.Message !=
		`The field "repair_bound" is internal to hoardDB's own repair path and cannot be set by a client. Remove it from the request.` {
		t.Errorf("the refusal has code %d and text %q", refusal.Code, refusal.Message)
	}
	if found, _, _, err := tc.nodes[0].server.core.versionedFind(testDatabase, testBucket, "k"); err != nil || !found {
		t.Errorf("the refused delete removed k (found=%v, %v)", found, err)
	}
}
