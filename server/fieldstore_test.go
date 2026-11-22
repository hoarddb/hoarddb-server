package server

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// writeCatalogStores records stores in the bucket's catalog entry the way a
// declare does: read, modify, write — so every other field survives.
func writeCatalogStores(t *testing.T, s *Server, database, bucket string, stores []catalogStore, next int32) {
	t.Helper()
	entry, found, err := s.readCatalogEntry(database, bucket)
	if err != nil || !found {
		t.Fatalf("reading the catalog entry of %s/%s: found %v, %v", database, bucket, found, err)
	}
	entry.Stores, entry.NextStoreID = stores, next
	encoded, err := bson.Marshal(entry)
	if err != nil {
		t.Fatalf("encoding the catalog entry: %v", err)
	}
	batch := s.kv.NewBatch()
	defer batch.Discard()
	if err := batch.Set(catalogKey(database, bucket), encoded); err != nil {
		t.Fatalf("writing the catalog entry: %v", err)
	}
	if err := batch.Commit(true); err != nil {
		t.Fatalf("committing the catalog entry: %v", err)
	}
}

func servedBucket(t *testing.T, s *Server, database, bucket string) *inMemoryBucket {
	t.Helper()
	b, err := s.resolveBucket(database, bucket)
	if err != nil || b.id.Load() == 0 {
		t.Fatalf("bucket %s/%s is not served: %v", database, bucket, err)
	}
	return b
}

var twoStores = []catalogStore{{Name: "Comments", ID: 1, Engine: "fifo"}, {Name: "Photos", ID: 2, Engine: "blob"}}

// TestCatalogRecordsStoreFieldsWithStableIds is spec T-14, reshaped: there is
// no catalog version set, and an entry without stores is every existing
// bucket.
func TestCatalogRecordsStoreFieldsWithStableIds(t *testing.T) {
	dataDir := t.TempDir()
	cs := startCatalogServerAt(t, dataDir)
	createTestBucket(t, cs.core, "app", "Post", BucketTypeHash)
	createTestBucket(t, cs.core, "app", "Plain", BucketTypeHash)
	writeCatalogStores(t, cs.core, "app", "Post", twoStores, 3)

	entry, _, err := cs.core.readCatalogEntry("app", "Post")
	if err != nil || !reflect.DeepEqual(entry.Stores, twoStores) || entry.NextStoreID != 3 {
		t.Fatalf("the catalog entry read back stores %+v, next %d (%v); want %+v, 3", entry.Stores, entry.NextStoreID, err, twoStores)
	}

	cs = restartCatalog(t, cs, dataDir)
	post := servedBucket(t, cs.core, "app", "Post")
	if got := post.storeFields(); !reflect.DeepEqual(got.stores, twoStores) || got.next != 3 {
		t.Fatalf("after a restart Post declares %+v, next %d; want %+v, 3: the attach did not adopt the recorded declaration", got.stores, got.next, twoStores)
	}
	if got := servedBucket(t, cs.core, "app", "Plain").storeFields(); len(got.stores) != 0 || got.next != 0 {
		t.Fatalf("a bucket whose entry has no stores loads declaring %+v, next %d; want none", got.stores, got.next)
	}
}

func TestStoreIdsAreNeverReused(t *testing.T) {
	// A declared B (id 2) that is no longer in the list, with next still 3.
	set := &storeFieldSet{stores: []catalogStore{{Name: "A", ID: 1, Engine: "fifo"}}, next: 3}
	grown, err := set.withDeclared([]StoreDeclaration{{Name: "C", Engine: "hashStore"}})
	if err != nil {
		t.Fatalf("declaring C: %v", err)
	}
	c := grown.stores[len(grown.stores)-1]
	if c.Name != "C" || c.ID != 3 || grown.next != 4 {
		t.Fatalf("C was given id %d with next %d; want id 3, next 4: id 2 is B's forever, and C would adopt B's keys", c.ID, grown.next)
	}
	if len(set.stores) != 1 {
		t.Fatalf("withDeclared changed the set it was called on: %+v", set.stores)
	}

	fresh, err := noStoreFields.withDeclared([]StoreDeclaration{{Name: "Comments", Engine: "fifoStore", Fields: []StoreDeclaration{{Name: "Flags", Engine: "fifoStore"}}}, {Name: "Tags", Engine: "hash"}})
	if err != nil {
		t.Fatalf("declaring on an empty set: %v", err)
	}
	want := []catalogStore{{Name: "Comments", ID: 1, Engine: "fifo"}, {Name: "Flags", ID: 2, Engine: "fifo", Parent: 1}, {Name: "Tags", ID: 3, Engine: "hash"}}
	if !reflect.DeepEqual(fresh.stores, want) || fresh.next != 4 {
		t.Fatalf("a first declaration assigned %+v, next %d; want %+v, 4 (ids from 1, id 0 never)", fresh.stores, fresh.next, want)
	}

	full := &storeFieldSet{next: maxStoreID + 1}
	if _, err := full.withDeclared([]StoreDeclaration{{Name: "Z", Engine: "fifo"}}); err == nil {
		t.Fatalf("a set that has assigned every id accepted another declaration")
	}
}

// TestDropBucketForgetsStoreDeclarations is spec T-15, a pin: drop already
// deletes the catalog key, and bucket ids are never reused.
func TestDropBucketForgetsStoreDeclarations(t *testing.T) {
	dataDir := t.TempDir()
	cs := startCatalogServerAt(t, dataDir)
	createTestBucket(t, cs.core, "app", "Post", BucketTypeHash)
	writeCatalogStores(t, cs.core, "app", "Post", twoStores, 3)
	cs = restartCatalog(t, cs, dataDir)

	if resp, err := cs.core.DeleteBucket(DeleteBucketRequest{Database: "app", Name: "Post"}); err != nil || !resp.Success {
		t.Fatalf("dropping Post: %+v, %v", resp, err)
	}
	createTestBucket(t, cs.core, "app", "Post", BucketTypeHash)
	entry, _, err := cs.core.readCatalogEntry("app", "Post")
	if err != nil || len(entry.Stores) != 0 || entry.NextStoreID != 0 {
		t.Fatalf("the re-created Post's catalog entry has stores %+v, next %d (%v); want none", entry.Stores, entry.NextStoreID, err)
	}
	if got := servedBucket(t, cs.core, "app", "Post").storeFields(); len(got.stores) != 0 {
		t.Fatalf("the re-created Post declares %+v; want none", got.stores)
	}
}

// storesOption is the create option a client sends: {name, engine, fields}.
func storesOption(declared ...StoreDeclaration) map[string]interface{} {
	var list []interface{}
	var build func(StoreDeclaration) map[string]interface{}
	build = func(d StoreDeclaration) map[string]interface{} {
		entry := map[string]interface{}{"name": d.Name, "engine": d.Engine}
		if len(d.Fields) > 0 {
			var fields []interface{}
			for _, field := range d.Fields {
				fields = append(fields, build(field))
			}
			entry["fields"] = fields
		}
		return entry
	}
	for _, d := range declared {
		list = append(list, build(d))
	}
	return map[string]interface{}{"stores": list}
}

func createWithStores(t *testing.T, s *Server, database, bucket string, btype BucketType, declared ...StoreDeclaration) *CreateBucketResponse {
	t.Helper()
	if _, err := s.CreateDatabase(database); err != nil {
		t.Fatalf("CreateDatabase(%q): %v", database, err)
	}
	resp, err := s.CreateBucket(CreateBucketRequest{Database: database, Name: bucket, Type: btype, Options: storesOption(declared...)})
	if err != nil {
		t.Fatalf("CreateBucket(%q): %v", bucket, err)
	}
	return resp
}

func TestCreateRecordsDeclaredStoresWithIdsFromOne(t *testing.T) {
	s := newTestServer(t)
	resp := createWithStores(t, s, "app", "Post", BucketTypeHash,
		StoreDeclaration{Name: "Comments", Engine: "fifoStore"}, StoreDeclaration{Name: "Photos", Engine: "BlobStore"})
	if !resp.Success {
		t.Fatalf("creating Post with two store fields: %s", resp.Message)
	}
	entry, _, err := s.readCatalogEntry("app", "Post")
	if err != nil || !reflect.DeepEqual(entry.Stores, twoStores) || entry.NextStoreID != 3 {
		t.Fatalf("the catalog records stores %+v, next %d (%v); want %+v, 3", entry.Stores, entry.NextStoreID, err, twoStores)
	}
	if got := servedBucket(t, s, "app", "Post").storeFields(); !reflect.DeepEqual(got.stores, twoStores) {
		t.Fatalf("the served bucket declares %+v, want %+v", got.stores, twoStores)
	}
}

// TestFieldNameValidatorHasOneOwner is spec T-11, the server half: the
// validator and the create path refuse the same names with the same words,
// because the create path calls the validator.
func TestFieldNameValidatorHasOneOwner(t *testing.T) {
	s := newTestServer(t)
	names := append([]string{"", "a.b", "a\x00b", "$where"}, reservedStoreFieldNames...)
	for i, name := range names {
		validated := ValidateStoreDeclarations(StoreValidation{
			Database: "app", Bucket: fmt.Sprintf("B%d", i), BucketType: BucketTypeHash,
			Added: []StoreDeclaration{{Name: name, Engine: "fifoStore"}},
		})
		if !errors.Is(validated, ErrStoreDeclarationInvalid) {
			t.Errorf("ValidateStoreDeclarations accepted the store field name %q", name)
			continue
		}
		resp := createWithStores(t, s, "app", fmt.Sprintf("B%d", i), BucketTypeHash, StoreDeclaration{Name: name, Engine: "fifoStore"})
		if resp.Success {
			t.Errorf("create accepted the store field name %q that the validator refuses", name)
		} else if resp.Message != validated.Error() {
			t.Errorf("create refused %q with %q; the validator says %q", name, resp.Message, validated.Error())
		}
	}
	if err := ValidateStoreDeclarations(StoreValidation{BucketType: BucketTypeHash, Added: []StoreDeclaration{{Name: "Comments", Engine: "fifoStore"}}}); err != nil {
		t.Fatalf("an ordinary name was refused: %v", err)
	}
	if err := ValidateStoreDeclarations(StoreValidation{BucketType: BucketTypeHash, Added: []StoreDeclaration{{Name: "C", Engine: "fifoStore"}, {Name: "C", Engine: "hashStore"}}}); err == nil {
		t.Fatalf("a name declared twice in one statement was accepted")
	}
	if err := ValidateStoreDeclarations(StoreValidation{BucketType: BucketTypeHash, Added: []StoreDeclaration{{Name: "C", Engine: "queueStore"}}}); err == nil {
		t.Fatalf("an engine word that is not one of the six was accepted")
	}
}

// specDepthMessage is spec §10.3's depth refusal as the spec prints it, for
// field Reasons in database PhotoJournal. The test fills its values by
// replacing those names, so it compares the code with the spec's own bytes.
const specDepthMessage = `Field "Reasons" is declared 4 levels deep; hoardDB nests declarations at most 3
(a bucket, a store on its documents, a store on those elements).
Give "Reasons" its own top-level bucket instead:
  hoarddb bucket create PhotoJournal Reasons --type=fifo
and reference it from the element by its key. A fourth level is usually a sign that
the deepest level grows on its own, which is what a top-level bucket is for.`

// TestDeclarationDepthLimitRefusedWithTheFix is spec T-09.
func TestDeclarationDepthLimitRefusedWithTheFix(t *testing.T) {
	s := newTestServer(t)
	deep := StoreDeclaration{Name: "Comments", Engine: "fifoStore", Fields: []StoreDeclaration{
		{Name: "Flags", Engine: "fifoStore", Fields: []StoreDeclaration{{Name: "Why", Engine: "fifoStore"}}},
	}}
	resp := createWithStores(t, s, "app", "Post", BucketTypeHash, deep)
	if resp.Success {
		t.Fatalf("a depth-4 declaration was accepted")
	}
	want := strings.NewReplacer(`"Reasons"`, `"Why"`, "PhotoJournal Reasons", "app Why").Replace(specDepthMessage)
	if got := strings.TrimPrefix(resp.Message, ErrStoreDeclarationInvalid.Error()+": "); got != want {
		t.Fatalf("the depth refusal reads\n%s\nwant spec §10.3's\n%s", got, want)
	}
	if _, found, _ := s.readCatalogEntry("app", "Post"); found {
		t.Fatalf("a refused declaration left a catalog entry")
	}
}

func TestParentTypeRestrictedToHashAndBTree(t *testing.T) {
	s := newTestServer(t)
	for _, btype := range []BucketType{BucketTypeFIFO, BucketTypeLIFO, BucketTypeHeap, BucketTypeBlob, BucketTypeHash, BucketTypeBTree} {
		resp := createWithStores(t, s, "app", "P"+string(btype), btype, StoreDeclaration{Name: "Comments", Engine: "fifoStore"})
		allowed := btype == BucketTypeHash || btype == BucketTypeBTree
		if resp.Success != allowed {
			t.Errorf("a %s bucket declaring a store field: success %v, want %v (%s)", btype, resp.Success, allowed, resp.Message)
		}
		if !allowed && !strings.Contains(resp.Message, "references the "+string(btype)+" entry by key") {
			t.Errorf("the %s refusal %q does not give the alternative", btype, resp.Message)
		}
	}
}

func TestDeclarationOptionsRoundTrip(t *testing.T) {
	stores := []catalogStore{{Name: "A", ID: 1, Engine: "fifo"}, {Name: "C", ID: 3, Engine: "hash"}, {Name: "F", ID: 4, Engine: "fifo", Parent: 1}}
	options := storeOptions(stores, 5)
	// The options cross the wire as BSON on a migration or a forward.
	encoded, err := bson.Marshal(options)
	if err != nil {
		t.Fatalf("encoding the options: %v", err)
	}
	var decoded map[string]interface{}
	if err := bson.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("decoding the options: %v", err)
	}
	for name, candidate := range map[string]map[string]interface{}{"in process": options, "off the wire": decoded} {
		declared, recorded, err := parseStoreDeclarations(candidate)
		if err != nil || declared != nil || recorded == nil {
			t.Fatalf("%s: parse = %v, %v, %v; want the recorded form", name, declared, recorded, err)
		}
		if !reflect.DeepEqual(recorded.stores, stores) || recorded.next != 5 {
			t.Fatalf("%s: round trip gave %+v, next %d; want %+v, 5: a reopened or migrated bucket would renumber its stores", name, recorded.stores, recorded.next, stores)
		}
	}
}
