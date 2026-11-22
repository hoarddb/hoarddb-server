package server

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// TestInsertRefusesAValueOfTheWrongDeclaredType is the declared-type test: the
// owner's schema from root AGENTS.md rule 11, with Age int and CreatedDate
// datetime, refuses "notanint" and 5, naming the field, its declared type
// and the value's type, and stores nothing; a conforming document is stored.
func TestInsertRefusesAValueOfTheWrongDeclaredType(t *testing.T) {
	s := newTestServer(t)
	createTypedBucket(t, s, "Age2", bson.A{
		bson.M{"name": "Age", "type": "int"}, bson.M{"name": "CreatedDate", "type": "datetime"}, bson.M{"name": "Email", "type": "string"},
	}, "")
	_, err := s.Insert(InsertRequest{Database: "app", Bucket: "Age2", Key: "k1", Data: map[string]interface{}{"Age": "notanint", "CreatedDate": int32(5)}})
	if err == nil {
		t.Fatal(`{Age: "notanint", CreatedDate: 5} was stored in a bucket declaring Age int and CreatedDate datetime`)
	}
	if codeFor(err) != transport.ErrFieldType {
		t.Errorf("the refusal answers %s; want FIELD_TYPE", codeFor(err))
	}
	for _, part := range []string{`Field "Age"`, "is declared int", "is a string"} {
		if !strings.Contains(err.Error(), part) {
			t.Errorf("the refusal %q does not say %q", err, part)
		}
	}
	_, err = s.Insert(InsertRequest{Database: "app", Bucket: "Age2", Key: "k2", Data: map[string]interface{}{"Age": int32(36), "CreatedDate": int32(5)}})
	if err == nil || !strings.Contains(err.Error(), `Field "CreatedDate"`) || !strings.Contains(err.Error(), "is declared datetime") ||
		!strings.Contains(err.Error(), "is a 32-bit integer") {
		t.Errorf("CreatedDate 5 was answered %v; want the field, datetime and a 32-bit integer named", err)
	}
	for _, key := range []string{"k1", "k2"} {
		if documentOf(t, s, "app", "Age2", key) != nil {
			t.Errorf("the refused document %s was stored", key)
		}
	}
	mustInsert(t, s, "app", "Age2", "k3", map[string]interface{}{"Age": int32(36), "CreatedDate": time.Unix(1759492800, 0).UTC()})
}

func declareReq(database, bucket string, declared ...StoreDeclaration) DeclareStoreFieldsRequest {
	req := DeclareStoreFieldsRequest{Database: database, Name: bucket}
	for _, entry := range storesOption(declared...)["stores"].([]interface{}) {
		req.Stores = append(req.Stores, entry.(map[string]interface{}))
	}
	return req
}

func TestAlterAddsStoreField(t *testing.T) {
	dataDir := t.TempDir()
	cs := startCatalogServerAt(t, dataDir)
	seedPost(t, cs.core) // Comments 1, Ranked 2, Tags 3; next 4

	// A declare whose commit fails claims nothing, in memory or on disk.
	fail := func(storage.KV, uint16, uint64, uint64) error { return errors.New("injected commit failure") }
	store.ApplyFaultHook.Store(&fail)
	resp, err := cs.core.DeclareStoreFieldsCtx(context.Background(), declareReq("app", "Post", StoreDeclaration{Name: "Notes", Engine: "hashStore"}))
	store.ApplyFaultHook.Store(nil)
	if err != nil || resp.Success {
		t.Fatalf("a declare whose commit fails = %+v, %v; want Success false", resp, err)
	}
	if _, found := servedBucket(t, cs.core, "app", "Post").storeFields().find("Notes", 0); found {
		t.Fatalf("a declare whose commit failed left Notes in the bucket's declaration, which the catalog does not hold")
	}

	resp, err = cs.core.DeclareStoreFieldsCtx(context.Background(), declareReq("app", "Post", StoreDeclaration{Name: "Notes", Engine: "hashStore"}))
	if err != nil || !resp.Success {
		t.Fatalf("declaring Notes: %+v, %v", resp, err)
	}
	if got := fieldLen(t, cs.core, "p1", "Notes"); got != 0 {
		t.Fatalf("an existing document's new Notes store has %d elements, want 0", got)
	}
	fieldOK(t)(cs.core.FieldPutCtx(context.Background(), FieldRequest{Database: "app", Bucket: "Post", Key: "p1", Field: "Notes", Element: "n1", Data: map[string]interface{}{"text": "hi"}}))
	entry, _, err := cs.core.readCatalogEntry("app", "Post")
	notes, found := (&storeFieldSet{stores: entry.Stores}).find("Notes", 0)
	if err != nil || !found || notes.ID != 4 || entry.NextStoreID != 5 {
		t.Fatalf("the catalog records Notes as %+v (found %v), next %d (%v); want id 4, next 5", notes, found, entry.NextStoreID, err)
	}

	cs = restartCatalog(t, cs, dataDir)
	got, err := cs.core.FieldGet(FieldRequest{Database: "app", Bucket: "Post", Key: "p1", Field: "Notes", Element: "n1"})
	if err != nil || !got.Found {
		t.Fatalf("after a restart Notes[n1] = %+v, %v; want the element", got, err)
	}
}

func TestDeclareIsIdempotent(t *testing.T) {
	s := newPostServer(t)
	for attempt := range 2 {
		resp, err := s.DeclareStoreFieldsCtx(context.Background(), declareReq("app", "Post", StoreDeclaration{Name: "Notes", Engine: "hashStore"}))
		if err != nil || !resp.Success {
			t.Fatalf("declare attempt %d: %+v, %v", attempt+1, resp, err)
		}
	}
	entry, _, _ := s.readCatalogEntry("app", "Post")
	count := 0
	for _, declared := range entry.Stores {
		if declared.Name == "Notes" {
			count++
		}
	}
	if count != 1 || entry.NextStoreID != 5 {
		t.Fatalf("two identical declares recorded Notes %d times with next %d; want once, next 5 (the second consumes no id)", count, entry.NextStoreID)
	}
	// The whole existing declaration, re-sent, is an identity too.
	resp, err := s.DeclareStoreFieldsCtx(context.Background(), declareReq("app", "Post", StoreDeclaration{Name: "Comments", Engine: "fifoStore"}))
	if err != nil || !resp.Success || !strings.Contains(resp.Message, "already declares") {
		t.Fatalf("re-declaring Comments = %+v, %v; want success, already declared", resp, err)
	}
}

// TestDeclarePreservesEveryCatalogField: the declare writes the entry it
// read. A fixture sets every exported field non-zero, so a field added to
// catalogEntry later and left zero here fails the fixture, not silently the
// test.
func TestDeclarePreservesEveryCatalogField(t *testing.T) {
	s := newTestServer(t)
	if _, err := s.CreateDatabase("app"); err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}
	options := storesOption(StoreDeclaration{Name: "Comments", Engine: "fifoStore"})
	options["indices"] = []interface{}{"title"}
	// Spread, so the entry records parts: the fixture needs every field non-zero.
	options["placement"] = "spread"
	if resp, err := s.CreateBucket(CreateBucketRequest{Database: "app", Name: "Post", Type: BucketTypeHash, Options: options, ReplicationFactor: 3}); err != nil || !resp.Success {
		t.Fatalf("creating Post: %+v, %v", resp, err)
	}
	// next_index_id is written only when it differs from the index count,
	// which no 1.0 create produces, so the record is given one the way a
	// later binary's add-index would leave it.
	setCatalogNextIndexID(t, s, "app", "Post", 7)
	// A typed field and a mode are recorded the same way, so the declare is
	// seen to keep them.
	var record bson.D
	if err := bson.Unmarshal(withRawFields(t, rawCatalogEntry(t, s, "app", "Post"),
		bson.A{bson.D{{Key: "name", Value: "title"}, {Key: "type", Value: "string"}}}), &record); err != nil {
		t.Fatalf("decoding the catalog record: %v", err)
	}
	// part and part_of are on a part's entry only; the fixture gives them to
	// this one so the declare is seen to keep them (spread MVP step 1).
	typed, err := bson.Marshal(append(record,
		bson.E{Key: "field_types", Value: "warn"}, bson.E{Key: "part", Value: int32(3)}, bson.E{Key: "part_of", Value: int64(9)}))
	if err != nil {
		t.Fatalf("encoding the catalog record: %v", err)
	}
	writeRawCatalogEntry(t, s, "app", "Post", typed)
	before, _, err := s.readCatalogEntry("app", "Post")
	if err != nil {
		t.Fatalf("reading the entry: %v", err)
	}
	fixture := reflect.ValueOf(before)
	for i := range fixture.NumField() {
		field := fixture.Type().Field(i)
		if field.IsExported() && fixture.Field(i).IsZero() {
			t.Fatalf("the fixture leaves catalogEntry.%s zero: set it, or the test cannot see the declare drop it", field.Name)
		}
	}
	if resp, err := s.DeclareStoreFieldsCtx(context.Background(), declareReq("app", "Post", StoreDeclaration{Name: "Tags", Engine: "hashStore"})); err != nil || !resp.Success {
		t.Fatalf("declaring Tags: %+v, %v", resp, err)
	}
	after, _, err := s.readCatalogEntry("app", "Post")
	if err != nil {
		t.Fatalf("reading the entry: %v", err)
	}
	afterValue := reflect.ValueOf(after)
	for i := range fixture.NumField() {
		field := fixture.Type().Field(i)
		if !field.IsExported() || field.Name == "Stores" || field.Name == "NextStoreID" {
			continue
		}
		if !reflect.DeepEqual(fixture.Field(i).Interface(), afterValue.Field(i).Interface()) {
			t.Errorf("the declare changed catalogEntry.%s from %v to %v", field.Name, fixture.Field(i).Interface(), afterValue.Field(i).Interface())
		}
	}
}

// setCatalogNextIndexID writes next_index_id into a bucket's catalog record.
func setCatalogNextIndexID(t *testing.T, s *Server, database, bucket string, next int32) {
	t.Helper()
	snap := s.kv.NewSnapshot()
	encoded, err := snap.Get(catalogKey(database, bucket))
	snap.Discard()
	if err != nil {
		t.Fatalf("reading the catalog record of %s/%s: %v", database, bucket, err)
	}
	var record bson.D
	if err := bson.Unmarshal(encoded, &record); err != nil {
		t.Fatalf("decoding the catalog record: %v", err)
	}
	rewritten, err := bson.Marshal(append(record, bson.E{Key: "next_index_id", Value: next}))
	if err != nil {
		t.Fatalf("encoding the catalog record: %v", err)
	}
	batch := s.kv.NewBatch()
	defer batch.Discard()
	if err := batch.Set(catalogKey(database, bucket), rewritten); err != nil {
		t.Fatalf("writing the catalog record: %v", err)
	}
	if err := batch.Commit(true); err != nil {
		t.Fatalf("committing the catalog record: %v", err)
	}
}

func TestDeclareRefusesEngineChange(t *testing.T) {
	s := newPostServer(t)
	before, _, _ := s.readCatalogEntry("app", "Post")
	_, err := s.DeclareStoreFieldsCtx(context.Background(), declareReq("app", "Post", StoreDeclaration{Name: "Comments", Engine: "hashStore"}))
	if !errors.Is(err, ErrStoreDeclarationInvalid) || codeFor(err) != transport.ErrInvalidArgument {
		t.Fatalf("re-declaring the fifo Comments as a hashStore = %v; want ErrStoreDeclarationInvalid", err)
	}
	for _, part := range []string{"already declared as fifoStore", "not supported in 1.0", "no preview and no undo"} {
		if !strings.Contains(err.Error(), part) {
			t.Fatalf("the refusal %q does not say %q", err, part)
		}
	}
	after, _, _ := s.readCatalogEntry("app", "Post")
	if !reflect.DeepEqual(before.Stores, after.Stores) || before.NextStoreID != after.NextStoreID {
		t.Fatalf("a refused declare changed the catalog from %+v to %+v", before.Stores, after.Stores)
	}
}

// walHoldsDeclare reports whether a node's WAL holds a declare of bucket;
// with bucket "*", a declare of any bucket, the empty name included.
func walHoldsDeclare(t *testing.T, s *Server, bucket string) bool {
	t.Helper()
	entries, err := s.wal.ReadFrom(1)
	if err != nil {
		t.Fatalf("reading the WAL: %v", err)
	}
	for _, e := range entries {
		if transport.Opcode(e.Op) == transport.OpDeclareStoreFields && (bucket == "*" || e.Bucket == bucket) {
			return true
		}
	}
	return false
}

func TestDeclarePlacedOnReplicaSetNotBroadcast(t *testing.T) {
	tc := newTestCluster(t, 5, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	tc.ok(0, transport.OpDeclareStoreFields, declareReq(testDatabase, testBucket, StoreDeclaration{Name: "Tags", Engine: "hashStore"}))
	hosts := make(map[string]bool)
	for _, id := range mustReplicaIDs(t, tc.nodes[0].server.core.cluster, testDatabase, testBucket) {
		hosts[id] = true
	}
	deadline := time.Now().Add(20 * time.Second)
	for index, node := range tc.nodes {
		if !hosts[tc.addrs[index]] {
			if walHoldsDeclare(t, node.server.core, "*") {
				t.Errorf("node %d (%s), outside the replica set, logged a declare: it was broadcast", index+1, tc.addrs[index])
			}
			continue
		}
		got := walHoldsDeclare(t, node.server.core, testBucket)
		for !got && time.Now().Before(deadline) {
			time.Sleep(50 * time.Millisecond)
			got = walHoldsDeclare(t, node.server.core, testBucket)
		}
		if !got {
			t.Errorf("replica node %d (%s) never logged the declare of %q", index+1, tc.addrs[index], testBucket)
		}
	}
}

// TestOldGenerationDeclareIsGated: a declare logged against a dropped
// generation of a bucket must not reach the re-created bucket on replay.
func TestOldGenerationDeclareIsGated(t *testing.T) {
	// Two nodes at RF 1: a lone node takes the standalone path and logs
	// nothing, so there would be nothing to replay.
	tc := newTestCluster(t, 2, 1)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	owner, err := tc.nodes[0].server.core.cluster.Owner(placementOf(testDatabase, testBucket))
	if err != nil {
		t.Fatalf("owner: %v", err)
	}
	ownerIndex := nodeIndexByAddr(t, tc.addrs, owner.ID)
	tc.ok(ownerIndex, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	tc.ok(ownerIndex, transport.OpDeclareStoreFields, declareReq(testDatabase, testBucket, StoreDeclaration{Name: "A", Engine: "fifoStore"}))
	tc.ok(ownerIndex, transport.OpDeleteBucket, DeleteBucketRequest{Database: testDatabase, Name: testBucket})
	tc.ok(ownerIndex, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	if !walHoldsDeclare(t, tc.nodes[ownerIndex].server.core, testBucket) {
		t.Fatalf("the owner's WAL holds no declare: the test would replay nothing")
	}
	tc.restartNode(ownerIndex)
	core := tc.nodes[ownerIndex].server.core
	if _, found := servedBucket(t, core, testDatabase, testBucket).storeFields().find("A", 0); found {
		t.Fatalf("after replay the re-created bucket declares A, which only its dropped predecessor declared")
	}
	if entry, _, _ := core.readCatalogEntry(testDatabase, testBucket); len(entry.Stores) != 0 {
		t.Fatalf("after replay the re-created bucket's catalog entry records %+v", entry.Stores)
	}
}
