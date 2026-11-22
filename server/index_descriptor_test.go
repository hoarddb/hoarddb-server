package server

import (
	"bytes"
	"reflect"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"

	"github.com/hoarddb/hoarddb-server/transport"
)

// The index descriptor in the catalog record (GAP-14 build doc Step 1). These
// tests read and write the catalog as raw BSON on purpose: the record is the
// contract a restart, a migration and a rollback all read, and a test that
// went through the Go types would pass for a field the types agree on and the
// bytes do not carry.

// catalogIndicesOf decodes the "indices" array of a catalog record.
func catalogIndicesOf(t *testing.T, encoded []byte) []bson.M {
	t.Helper()
	var record struct {
		Indices []bson.M `bson:"indices"`
	}
	if err := bson.Unmarshal(encoded, &record); err != nil {
		t.Fatalf("decoding a catalog record: %v", err)
	}
	return record.Indices
}

// storedCatalogIndices reads a bucket's catalog record as it is on disk.
func storedCatalogIndices(t *testing.T, s *Server, database, bucket string) []bson.M {
	t.Helper()
	snap := s.kv.NewSnapshot()
	defer snap.Discard()
	encoded, err := snap.Get(catalogKey(database, bucket))
	if err != nil {
		t.Fatalf("reading the catalog record of %s/%s: %v", database, bucket, err)
	}
	return catalogIndicesOf(t, encoded)
}

// recordedIndices is what the server would write for a bucket's declared
// indexes, decoded the same way storedCatalogIndices decodes the disk.
func recordedIndices(t *testing.T, b *inMemoryBucket) []bson.M {
	t.Helper()
	encoded, err := bson.Marshal(struct {
		Indices interface{} `bson:"indices"`
	}{catalogIndices(b)})
	if err != nil {
		t.Fatalf("encoding the recorded indices: %v", err)
	}
	return catalogIndicesOf(t, encoded)
}

// attachDeclaration runs a stored index list through the attach path — the
// catalog decode, indexOptions, prepareBucketLocked — and returns what the
// server would record for it again.
func attachDeclaration(t *testing.T, s *Server, stored []bson.M) ([]bson.M, *CreateBucketResponse) {
	t.Helper()
	encoded, err := bson.Marshal(bson.M{"indices": stored})
	if err != nil {
		t.Fatalf("encoding the fixture: %v", err)
	}
	var entry catalogEntry
	if err := bson.Unmarshal(encoded, &entry); err != nil {
		t.Fatalf("decoding the fixture as a catalog entry: %v", err)
	}
	bucket := &inMemoryBucket{}
	resp, err := s.prepareBucketLocked(bucket, CreateBucketRequest{
		Database: "app", Name: "attached", Type: BucketTypeHash, Options: indexOptions(entry.Indices),
	})
	if err != nil {
		t.Fatalf("prepareBucketLocked: %v", err)
	}
	return recordedIndices(t, bucket), resp
}

// writeCatalogIndices replaces the indices array of a bucket's catalog record,
// the way a record written by another binary would read.
func writeCatalogIndices(t *testing.T, s *Server, database, bucket string, indices []bson.M) {
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
	replaced := false
	for i := range record {
		if record[i].Key == "indices" {
			record[i].Value = indices
			replaced = true
		}
	}
	if !replaced {
		record = append(record, bson.E{Key: "indices", Value: indices})
	}
	rewritten, err := bson.Marshal(record)
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

// TestCatalogRoundTripsAFullIndexDescriptor: every descriptor field survives
// a restart — the record is read into memory and would be written back byte
// for byte — and the attach path carries each field, the non-ready states
// included, rather than rebuilding a declaration from field names.
func TestCatalogRoundTripsAFullIndexDescriptor(t *testing.T) {
	full := []bson.M{
		{"id": int32(0), "name": "Email_1", "fields": bson.A{"Email"}, "unique": true, "version": int32(2)},
		{"id": int32(1), "name": "CreatedAt_1", "fields": bson.A{"CreatedAt"}, "expire_after_seconds": int64(3600), "version": int32(2)},
		{"id": int32(2), "name": "by_customer_day", "fields": bson.A{"CustomerID", "Day"}, "version": int32(2)},
		{"id": int32(3), "name": "Zero_1", "fields": bson.A{"Zero"}, "expire_after_seconds": int64(0), "version": int32(2)},
	}

	dataDir := t.TempDir()
	first := startCatalogServerAt(t, dataDir)
	if resp, err := first.core.CreateDatabase("app"); err != nil || !resp.Success {
		t.Fatalf("CreateDatabase = %+v, %v", resp, err)
	}
	if resp, err := first.core.CreateBucket(CreateBucketRequest{
		Database: "app", Name: "sessions", Type: BucketTypeHash,
		Options: map[string]interface{}{"indices": []interface{}{"Email"}},
	}); err != nil || !resp.Success {
		t.Fatalf("CreateBucket = %+v, %v", resp, err)
	}
	writeCatalogIndices(t, first.core, "app", "sessions", full)

	second := restartCatalog(t, first, dataDir)
	got := recordedIndices(t, servedBucket(t, second.core, "app", "sessions"))
	want := storedCatalogIndices(t, second.core, "app", "sessions")
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("after a restart the bucket would record its indexes as\n  %v\nbut the catalog holds\n  %v", got, want)
	}

	// The states a rebuild or a verifier leaves are carried by the attach path
	// as they are; an attach that rebuilt the declaration from names would
	// report every index ready.
	for _, state := range []string{"building", "conflicted"} {
		stored := []bson.M{{"id": int32(0), "name": "Email_1", "fields": bson.A{"Email"}, "unique": true, "state": state, "version": int32(2)}}
		attached, resp := attachDeclaration(t, second.core, stored)
		if !resp.Success {
			t.Fatalf("state %q: the attach refused the record: %s", state, resp.Message)
		}
		if want := catalogIndicesOf(t, mustMarshal(t, bson.M{"indices": stored})); !reflect.DeepEqual(attached, want) {
			t.Errorf("state %q: the attach path records\n  %v\nfor the stored\n  %v", state, attached, want)
		}
	}
}

// TestPreBuildCatalogIndexLoadsAsAOneFieldDescriptor: a record written before
// the descriptor existed, {field, type}, is read as a one-field descriptor
// whose id is its list position — the id every entry it wrote carries — and
// whose version is absent, which is what marks it for the upgrade rebuild.
func TestPreBuildCatalogIndexLoadsAsAOneFieldDescriptor(t *testing.T) {
	s := newTestServer(t)
	attached, resp := attachDeclaration(t, s, []bson.M{
		{"field": "Email", "type": "equality"},
		{"field": "Age", "type": "range"},
	})
	if !resp.Success {
		t.Fatalf("a pre-build record was refused at attach: %s", resp.Message)
	}
	want := []bson.M{
		{"id": int32(0), "name": "Email_1", "fields": primitive.A{"Email"}},
		{"id": int32(1), "name": "Age_1", "fields": primitive.A{"Age"}},
	}
	if !reflect.DeepEqual(attached, want) {
		t.Fatalf("a pre-build record loads as\n  %v\nwant\n  %v", attached, want)
	}
}

// TestCatalogNeverWritesTheV1IndexFields: field and type are read, never
// written again. A record carrying both forms would be read two ways by two
// binaries.
func TestCatalogNeverWritesTheV1IndexFields(t *testing.T) {
	s := newTestServer(t)
	if _, err := s.CreateDatabase("app"); err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}
	if resp, err := s.CreateBucket(CreateBucketRequest{
		Database: "app", Name: "users", Type: BucketTypeHash,
		Options: map[string]interface{}{"indices": []interface{}{"Email", map[string]interface{}{"field": "Age"}}},
	}); err != nil || !resp.Success {
		t.Fatalf("CreateBucket = %+v, %v", resp, err)
	}
	stored := storedCatalogIndices(t, s, "app", "users")
	if len(stored) != 2 {
		t.Fatalf("the catalog records %d indexes, want 2: %v", len(stored), stored)
	}
	for _, index := range stored {
		for _, v1Key := range []string{"field", "type"} {
			if _, written := index[v1Key]; written {
				t.Errorf("a freshly written index record carries the pre-build key %q: %v", v1Key, index)
			}
		}
		if _, ok := index["version"]; !ok {
			t.Errorf("a freshly written index record has no version, which reads as declared before this build: %v", index)
		}
	}
}

// TestIndexIDIsNeverReused is a pin in 1.0, where no index is added or
// dropped after create: ids are the declaration order, 0…n-1, and a restart
// keeps them. A bucket declared [B, A] must not have its ids swapped by
// anything that sorts names.
func TestIndexIDIsNeverReused(t *testing.T) {
	dataDir := t.TempDir()
	first := startCatalogServerAt(t, dataDir)
	if _, err := first.core.CreateDatabase("app"); err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}
	if resp, err := first.core.CreateBucket(CreateBucketRequest{
		Database: "app", Name: "u", Type: BucketTypeHash,
		Options: map[string]interface{}{"indices": []interface{}{"B", "A"}},
	}); err != nil || !resp.Success {
		t.Fatalf("CreateBucket = %+v, %v", resp, err)
	}
	check := func(stage string, s *Server) {
		t.Helper()
		stored := storedCatalogIndices(t, s, "app", "u")
		if len(stored) != 2 {
			t.Fatalf("%s: %d indexes recorded, want 2: %v", stage, len(stored), stored)
		}
		for id, name := range []string{"B_1", "A_1"} {
			if stored[id]["id"] != int32(id) || stored[id]["name"] != name {
				t.Errorf("%s: index %d is %v, want id %d named %s", stage, id, stored[id], id, name)
			}
		}
		if got := recordedIndices(t, servedBucket(t, s, "app", "u")); !reflect.DeepEqual(got, stored) {
			t.Errorf("%s: the served bucket would record %v over the stored %v", stage, got, stored)
		}
	}
	check("after create", first.core)
	second := restartCatalog(t, first, dataDir)
	check("after restart", second.core)
}

// TestMigratedBucketCarriesTheFullDescriptor extends
// TestMigratedIndexedBucketSearchesLikeItsSource: after a migration the
// target's catalog records the source's descriptors byte for byte, not a
// declaration rebuilt from field names.
func TestMigratedBucketCarriesTheFullDescriptor(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})

	const users = "described_users"
	pair := migrationPairFor(t, tc, users)
	tc.ok(pair.source, transport.OpCreateBucket, CreateBucketRequest{
		Database: testDatabase, Name: users, Type: BucketTypeHash,
		Options: map[string]interface{}{"indices": []interface{}{"email", "age"}},
	})
	tc.ok(pair.source, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: users, Key: "ada",
		Data: map[string]interface{}{"email": "ada@example.com", "age": 36}})

	source, target := tc.nodes[pair.source].server.core, tc.nodes[pair.target].server.core
	pair.pull(t, tc, users)

	rawIndices := func(s *Server) []byte {
		snap := s.kv.NewSnapshot()
		defer snap.Discard()
		encoded, err := snap.Get(catalogKey(testDatabase, users))
		if err != nil {
			t.Fatalf("reading the catalog record: %v", err)
		}
		value, err := bson.Raw(encoded).LookupErr("indices")
		if err != nil {
			t.Fatalf("the catalog record has no indices: %v", err)
		}
		return value.Value
	}
	if sourceBytes, targetBytes := rawIndices(source), rawIndices(target); !bytes.Equal(sourceBytes, targetBytes) {
		t.Fatalf("the migrated catalog record's indices differ from the source's:\n  source %v\n  target %v",
			bson.Raw(sourceBytes), bson.Raw(targetBytes))
	}
	if stored := storedCatalogIndices(t, target, testDatabase, users); len(stored) != 2 || stored[0]["version"] == nil {
		t.Fatalf("the target records %v, want two descriptors carrying their version", stored)
	}
}

// TestBucketStatsRendersTheDescriptor: describe names each index by its
// descriptor — a unique index says so, a compound index names its fields in
// order, a TTL index its expiry — in the []string the response already has.
func TestBucketStatsRendersTheDescriptor(t *testing.T) {
	dataDir := t.TempDir()
	first := startCatalogServerAt(t, dataDir)
	if _, err := first.core.CreateDatabase("app"); err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}
	if resp, err := first.core.CreateBucket(CreateBucketRequest{
		Database: "app", Name: "orders", Type: BucketTypeHash,
		Options: map[string]interface{}{"indices": []interface{}{"Email"}},
	}); err != nil || !resp.Success {
		t.Fatalf("CreateBucket = %+v, %v", resp, err)
	}
	writeCatalogIndices(t, first.core, "app", "orders", []bson.M{
		{"id": int32(0), "name": "Email_1", "fields": bson.A{"Email"}, "version": int32(2)},
		{"id": int32(1), "name": "Login_1", "fields": bson.A{"Login"}, "unique": true, "version": int32(2)},
		{"id": int32(2), "name": "CustomerID_1_CreatedAt_1", "fields": bson.A{"CustomerID", "CreatedAt"}, "version": int32(2)},
		{"id": int32(3), "name": "CreatedAt_1", "fields": bson.A{"CreatedAt"}, "expire_after_seconds": int64(3600), "version": int32(2)},
	})
	second := restartCatalog(t, first, dataDir)
	stats, err := second.core.BucketStats(BucketStatsRequest{Database: "app", Bucket: "orders"})
	if err != nil {
		t.Fatalf("BucketStats: %v", err)
	}
	want := []string{"Email_1", "Login_1 (unique)", "CustomerID_1_CreatedAt_1", "CreatedAt_1 (ttl 3600s)"}
	if strings.Join(stats.Indexes, "|") != strings.Join(want, "|") {
		t.Fatalf("describe renders the indexes as %q, want %q", stats.Indexes, want)
	}
}

// TestUnknownIndexVersionOpensTheBucketAndRefusesTheIndex (spec §10, §3.7): a
// rollback after an upgrade reads a record a newer binary wrote. An index this
// binary cannot read is held, never dropped: the bucket opens and serves
// find; search on that index is refused naming both versions; writes are
// refused while it is unique, because a write without the declared
// constraint is a downgrade nothing can repair afterwards; and a rewrite of
// the record — here the rebuild of the bucket's pre-build Name index — writes
// the element back byte for byte (§4 F-11).
func TestUnknownIndexVersionOpensTheBucketAndRefusesTheIndex(t *testing.T) {
	dataDir := t.TempDir()
	first := startCatalogServerAt(t, dataDir)
	if _, err := first.core.CreateDatabase("app"); err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}
	if resp, err := first.core.CreateBucket(CreateBucketRequest{Database: "app", Name: "users", Type: BucketTypeHash}); err != nil || !resp.Success {
		t.Fatalf("CreateBucket = %+v, %v", resp, err)
	}
	mustInsert(t, first.core, "app", "users", "u1", map[string]interface{}{"Email": "a@example.com", "Name": "a"})
	newer := bson.M{"id": int32(0), "name": "Email_1", "fields": bson.A{"Email"}, "unique": true, "version": int32(3),
		"collation": bson.M{"locale": "fr"}}
	writeCatalogIndices(t, first.core, "app", "users", []bson.M{newer, {"field": "Name"}})
	elementBytes := func(s *Server) []byte {
		snap := s.kv.NewSnapshot()
		defer snap.Discard()
		encoded, err := snap.Get(catalogKey("app", "users"))
		if err != nil {
			t.Fatalf("reading the catalog record: %v", err)
		}
		element, err := bson.Raw(encoded).LookupErr("indices", "0")
		if err != nil {
			t.Fatalf("the record has no first index: %v", err)
		}
		return append([]byte(nil), element.Value...)
	}
	written := elementBytes(first.core)

	second := restartCatalog(t, first, dataDir)
	s := second.core
	if doc := documentOf(t, s, "app", "users", "u1"); doc == nil {
		t.Fatal("the bucket holding an unreadable index did not open: find answered nothing")
	}
	stored := storedCatalogIndices(t, s, "app", "users")
	if len(stored) != 2 || stored[1]["version"] != int32(2) {
		t.Fatalf("precondition: the pre-build Name index was not rebuilt, so nothing rewrote the record: %v", stored)
	}
	if after := elementBytes(s); !bytes.Equal(after, written) {
		t.Fatalf("a rewrite of the record changed the index it cannot read:\n  written %v\n  now     %v", bson.Raw(written), bson.Raw(after))
	}
	hits, err := s.Search(SearchRequest{Database: "app", Bucket: "users", Filter: map[string]interface{}{"Email": "a@example.com"}})
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(hits.Results) != 0 || !strings.Contains(hits.Message, "Email_1") || !strings.Contains(hits.Message, "3") || !strings.Contains(hits.Message, "2") {
		t.Fatalf("search on the unreadable index answered %+v, want a refusal naming it and both versions", hits)
	}
	if refusal := insertOutcome(t, s, "app", "users", "u2", map[string]interface{}{"Email": "b@example.com"}); refusal == "" || !strings.Contains(refusal, "Email_1") {
		t.Fatalf("a write to a bucket holding an unreadable unique index answered %q, want a refusal naming the index", refusal)
	}
}
