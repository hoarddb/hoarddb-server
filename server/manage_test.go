package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/storage/kvfault"
	"github.com/hoarddb/hoarddb-server/transport"
)

// newTestServer builds a core server backed by a temporary data directory.
// Nothing listens on the network: these tests exercise the service layer the
// QUIC handlers call into.
func newTestServer(t *testing.T) *Server {
	t.Helper()
	return newTestServerWith(t, nil)
}

// newTestServerFloorMemtable is newTestServer at the 32 MiB memtable floor,
// whose 5,033,164-byte atomic bound the batch-bound tests need to reach with a
// small fixture; the 64 MiB every other test server chooses (server/main_test.go)
// bounds a batch at about 10 MB.
func newTestServerFloorMemtable(t *testing.T) *Server {
	t.Helper()
	return newTestServerWith(t, func(cfg *config.Config) { cfg.Storage.MemtableMB = config.MinMemtableMB })
}

// newTestServerWith is newTestServer with edit applied to the config first.
func newTestServerWith(t *testing.T, edit func(*config.Config)) *Server {
	t.Helper()

	cfg, err := config.Load("")
	if err != nil {
		t.Fatalf("loading default config: %v", err)
	}
	cfg.Storage.DataDir = t.TempDir()
	cfg.Server.ReplicationFactor = 1
	if edit != nil {
		edit(cfg)
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	qs, err := NewService(cfg, logger)
	if err != nil {
		t.Fatalf("creating server: %v", err)
	}
	t.Cleanup(func() {
		if err := qs.Stop(); err != nil {
			t.Logf("stopping server: %v", err)
		}
	})
	return qs.core
}

// createTestBucket creates a database and a bucket of the given type.
func createTestBucket(t testing.TB, s *Server, db, bucket string, btype BucketType) {
	t.Helper()

	resp, err := s.CreateDatabase(db)
	if err != nil {
		t.Fatalf("CreateDatabase(%q): %v", db, err)
	}
	if !resp.Success && !strings.Contains(resp.Message, "already exists") {
		t.Fatalf("CreateDatabase(%q) = %+v", db, resp)
	}
	created, err := s.CreateBucket(CreateBucketRequest{
		Database: db,
		Name:     bucket,
		Type:     btype,
	})
	if err != nil || !created.Success {
		t.Fatalf("CreateBucket(%q) = %+v, %v", bucket, created, err)
	}
}

func TestListDatabasesAndBuckets(t *testing.T) {
	s := newTestServer(t)

	empty, err := s.ListDatabases(ListDatabasesRequest{})
	if err != nil {
		t.Fatalf("ListDatabases on a fresh server: %v", err)
	}
	if len(empty.Databases) != 0 {
		t.Fatalf("fresh server reported %d databases, want 0", len(empty.Databases))
	}

	createTestBucket(t, s, "app", "users", BucketTypeHash)
	createTestBucket(t, s, "app", "events", BucketTypeBTree)

	databases, err := s.ListDatabases(ListDatabasesRequest{})
	if err != nil {
		t.Fatalf("ListDatabases: %v", err)
	}
	if len(databases.Databases) != 1 || databases.Databases[0].Name != "app" {
		t.Fatalf("ListDatabases = %+v, want one database named app", databases.Databases)
	}
	if databases.Databases[0].BucketCount != 2 {
		t.Errorf("BucketCount = %d, want 2", databases.Databases[0].BucketCount)
	}

	buckets, err := s.ListBuckets(ListBucketsRequest{Database: "app"})
	if err != nil {
		t.Fatalf("ListBuckets: %v", err)
	}
	if !buckets.Found {
		t.Fatal("ListBuckets reported Found=false for an existing database")
	}
	if len(buckets.Buckets) != 2 {
		t.Fatalf("ListBuckets returned %d buckets, want 2", len(buckets.Buckets))
	}
	if buckets.Buckets[0].Name != "events" || buckets.Buckets[0].Type != BucketTypeBTree {
		t.Errorf("first bucket = %+v, want events/btree", buckets.Buckets[0])
	}
}

func TestListBucketsUnknownDatabaseExplainsFix(t *testing.T) {
	s := newTestServer(t)

	_, err := s.ListBuckets(ListBucketsRequest{Database: "nope"})
	if err == nil {
		t.Fatal("ListBuckets on a missing database returned no error")
	}
	if want := "nope"; !contains(err.Error(), want) {
		t.Errorf("error %q does not name the missing database %q", err, want)
	}
	if !contains(err.Error(), "show databases") {
		t.Errorf("error %q does not tell the developer what to run next", err)
	}
}

func TestCountAndUpdate(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "users", BucketTypeHash)

	for _, doc := range []struct {
		key  string
		name string
		age  int64
	}{
		{"u1", "ada", 41},
		{"u2", "grace", 45},
	} {
		resp, err := s.Insert(InsertRequest{
			Database: "app",
			Bucket:   "users",
			Key:      doc.key,
			Data:     map[string]interface{}{"Name": doc.name, "Age": float64(doc.age)},
		})
		if err != nil || !resp.Success {
			t.Fatalf("Insert(%q) = %+v, %v", doc.key, resp, err)
		}
	}

	count, err := s.Count(CountRequest{Database: "app", Bucket: "users"})
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count.Count != 2 {
		t.Fatalf("Count = %d, want 2", count.Count)
	}

	updated, err := s.Update(UpdateRequest{
		Database: "app",
		Bucket:   "users",
		Key:      "u1",
		Set:      map[string]interface{}{"City": "London"},
		Inc:      map[string]int64{"Age": 1},
		Unset:    []string{"Name"},
	})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if !updated.Success {
		t.Fatalf("Update failed: %s", updated.Message)
	}
	if got := updated.Doc["Age"]; got != int64(42) {
		t.Errorf("$inc left Age = %v (%T), want 42", got, got)
	}
	if got := updated.Doc["City"]; got != "London" {
		t.Errorf("$set left City = %v, want London", got)
	}
	if _, still := updated.Doc["Name"]; still {
		t.Errorf("$unset left Name = %v", updated.Doc["Name"])
	}

	found, err := s.Find(FindRequest{Database: "app", Bucket: "users", Key: "u1"})
	if err != nil || !found.Found {
		t.Fatalf("Find after Update = %+v, %v", found, err)
	}
	var doc map[string]interface{}
	if err := bson.Unmarshal(found.Data, &doc); err != nil {
		t.Fatalf("decode found document: %v", err)
	}
	if got := doc["Age"]; got != int64(42) {
		t.Errorf("persisted Age = %v (%T), want 42", got, got)
	}
}

func TestUpdateMissingDocumentReportsIt(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "users", BucketTypeHash)

	resp, err := s.Update(UpdateRequest{
		Database: "app",
		Bucket:   "users",
		Key:      "ghost",
		Set:      map[string]interface{}{"Name": "nobody"},
	})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if resp.Success {
		t.Fatal("Update on a missing document reported success")
	}
	if resp.Message == "" {
		t.Error("Update on a missing document gave no message")
	}
}

// rangeBandWidth mirrors the db-comparison bench's range leg: a band of 100
// consecutive keys.
const rangeBandWidth = 100

// seedRangeBand inserts count documents keyed k0000000.. into a btree bucket,
// shaped like the db-comparison bench's documents: a sequence number and a
// ~100-byte payload, plus a label so each document carries several keys.
func seedRangeBand(tb testing.TB, s *Server, database, bucket string, count int) {
	tb.Helper()
	createTestBucket(tb, s, database, bucket, BucketTypeBTree)
	payload := strings.Repeat("abcdefghij", 10)
	for seq := 0; seq < count; seq++ {
		key := fmt.Sprintf("k%07d", seq)
		resp, err := s.Insert(InsertRequest{
			Database: database,
			Bucket:   bucket,
			Key:      key,
			Data:     map[string]interface{}{"seq": int32(seq), "payload": payload, "label": key},
		})
		if err != nil || !resp.Success {
			tb.Fatalf("Insert(%q) = %+v, %v", key, resp, err)
		}
	}
}

// BenchmarkRangeQueryDecode measures the range response's construction as
// handleBTreeRange performs it — RangeQuery, then the BSON marshal onto the
// wire — over a 100-entry band, and reports the per-entry cost.
func BenchmarkRangeQueryDecode(b *testing.B) {
	s := newBenchServer(b)
	seedRangeBand(b, s, "app", "band", rangeBandWidth)
	request := BTreeRangeRequest{Database: "app", Bucket: "band", Low: "k0000000", High: fmt.Sprintf("k%07d", rangeBandWidth-1)}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rangeResponse, err := s.RangeQuery(request)
		if err != nil {
			b.Fatalf("RangeQuery: %v", err)
		}
		if len(rangeResponse.Entries) != rangeBandWidth {
			b.Fatalf("RangeQuery returned %d entries, want %d", len(rangeResponse.Entries), rangeBandWidth)
		}
		if _, err := bson.Marshal(rangeResponse); err != nil {
			b.Fatalf("marshaling the range response: %v", err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*rangeBandWidth), "ns/entry")
}

func TestBTreeRangeFirstLast(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "events", BucketTypeBTree)

	for _, key := range []string{"a", "b", "c", "d"} {
		resp, err := s.Insert(InsertRequest{
			Database: "app",
			Bucket:   "events",
			Key:      key,
			Data:     map[string]interface{}{"Key": key},
		})
		if err != nil || !resp.Success {
			t.Fatalf("Insert(%q) = %+v, %v", key, resp, err)
		}
	}

	ranged, err := s.RangeQuery(BTreeRangeRequest{Database: "app", Bucket: "events", Low: "b", High: "c"})
	if err != nil {
		t.Fatalf("RangeQuery: %v", err)
	}
	if len(ranged.Entries) != 2 {
		t.Fatalf("RangeQuery returned %d entries, want 2 (b, c)", len(ranged.Entries))
	}
	if ranged.Entries[0].Key != "b" || ranged.Entries[1].Key != "c" {
		t.Errorf("RangeQuery keys = %q, %q; want b, c", ranged.Entries[0].Key, ranged.Entries[1].Key)
	}

	first, err := s.Edge("app", "events", false)
	if err != nil {
		t.Fatalf("Edge(first): %v", err)
	}
	if len(first.Entries) != 1 || first.Entries[0].Key != "a" {
		t.Errorf("first = %+v, want key a", first.Entries)
	}

	last, err := s.Edge("app", "events", true)
	if err != nil {
		t.Fatalf("Edge(last): %v", err)
	}
	if len(last.Entries) != 1 || last.Entries[0].Key != "d" {
		t.Errorf("last = %+v, want key d", last.Entries)
	}
}

func TestRangeOnWrongBucketTypeExplainsFix(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "users", BucketTypeHash)

	_, err := s.RangeQuery(BTreeRangeRequest{Database: "app", Bucket: "users", Low: "a", High: "z"})
	if err == nil {
		t.Fatal("RangeQuery on a hash bucket returned no error")
	}
	if !contains(err.Error(), "btree") {
		t.Errorf("error %q does not name the required bucket type", err)
	}
}

func TestBlobWriteAndRead(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "files", BucketTypeBlob)

	written, err := s.BlobWrite(BlobWriteRequest{
		Database: "app",
		Bucket:   "files",
		Key:      "hello",
		Data:     []byte("hello world"),
		Metadata: map[string]interface{}{"name": "hello.txt"},
	})
	if err != nil {
		t.Fatalf("BlobWrite: %v", err)
	}
	if !written.Success || written.Size != len("hello world") {
		t.Fatalf("BlobWrite = %+v, want success with size 11", written)
	}

	read, err := s.BlobRead(BlobReadRequest{Database: "app", Bucket: "files", Key: "hello"})
	if err != nil {
		t.Fatalf("BlobRead: %v", err)
	}
	if !read.Found {
		t.Fatal("BlobRead reported Found=false for a blob that was just written")
	}
	if string(read.Data) != "hello world" {
		t.Errorf("BlobRead data = %q, want %q", read.Data, "hello world")
	}
	if read.Metadata["name"] != "hello.txt" {
		t.Errorf("BlobRead metadata = %+v, want name=hello.txt", read.Metadata)
	}

	count, err := s.Count(CountRequest{Database: "app", Bucket: "files"})
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count.Count != 1 {
		t.Errorf("Count = %d after one blob write, want 1", count.Count)
	}
}

func TestBucketStatsReportsTypeAndIndexes(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "users", BucketTypeHash)

	stats, err := s.BucketStats(BucketStatsRequest{Database: "app", Bucket: "users"})
	if err != nil {
		t.Fatalf("BucketStats: %v", err)
	}
	if !stats.Found {
		t.Fatal("BucketStats reported Found=false for an existing bucket")
	}
	if stats.Type != BucketTypeHash {
		t.Errorf("Type = %q, want hash", stats.Type)
	}
}

func TestDropBucketRemovesItFromTheBucketList(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "users", BucketTypeHash)
	createTestBucket(t, s, "app", "events", BucketTypeBTree)

	dropped, err := s.DeleteBucket(DeleteBucketRequest{Database: "app", Name: "users"})
	if err != nil {
		t.Fatalf("DeleteBucket: %v", err)
	}
	if !dropped.Success {
		t.Fatalf("DeleteBucket failed: %s", dropped.Message)
	}

	buckets, err := s.ListBuckets(ListBucketsRequest{Database: "app"})
	if err != nil {
		t.Fatalf("ListBuckets: %v", err)
	}
	if len(buckets.Buckets) != 1 || buckets.Buckets[0].Name != "events" {
		t.Fatalf("buckets after drop = %+v, want only events", buckets.Buckets)
	}

	if _, err := os.Stat(filepath.Join(s.config.Storage.DataDir, "app", "users")); !os.IsNotExist(err) {
		t.Errorf("bucket directory still present after drop: %v", err)
	}
}

func TestDropDatabaseRemovesData(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "users", BucketTypeHash)

	if _, err := s.Insert(InsertRequest{
		Database: "app",
		Bucket:   "users",
		Key:      "u1",
		Data:     map[string]interface{}{"Name": "ada"},
	}); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	dropped, err := s.DeleteDatabase(DeleteDatabaseRequest{Name: "app"})
	if err != nil {
		t.Fatalf("DeleteDatabase: %v", err)
	}
	if !dropped.Success {
		t.Fatalf("DeleteDatabase failed: %s", dropped.Message)
	}

	databases, err := s.ListDatabases(ListDatabasesRequest{})
	if err != nil {
		t.Fatalf("ListDatabases: %v", err)
	}
	if len(databases.Databases) != 0 {
		t.Fatalf("databases after drop = %+v, want none", databases.Databases)
	}

	// Recreating the database must not resurrect the dropped data.
	createTestBucket(t, s, "app", "users", BucketTypeHash)
	count, err := s.Count(CountRequest{Database: "app", Bucket: "users"})
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count.Count != 0 {
		t.Errorf("Count = %d after drop and recreate, want 0", count.Count)
	}
}

func TestHealthCheckReportsLiveCounts(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "users", BucketTypeHash)
	if _, err := s.Insert(InsertRequest{
		Database: "app",
		Bucket:   "users",
		Key:      "u1",
		Data:     map[string]interface{}{"Name": "ada"},
	}); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	health, err := s.HealthCheck()
	if err != nil {
		t.Fatalf("HealthCheck: %v", err)
	}
	if health.BucketCount != 1 {
		t.Errorf("BucketCount = %d, want 1", health.BucketCount)
	}
}

// TestCountWithFilter tests that Count applies a filter and returns the filtered count.
func TestCountWithFilter(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "users", BucketTypeHash)

	// Insert documents with different statuses
	for i := 0; i < 3; i++ {
		if _, err := s.Insert(InsertRequest{
			Database: "app",
			Bucket:   "users",
			Key:      "active-" + string(rune('a'+i)),
			Data:     map[string]interface{}{"_id_": "active-" + string(rune('a'+i)), "Status": "active"},
		}); err != nil {
			t.Fatalf("Insert active: %v", err)
		}
	}
	for i := 0; i < 2; i++ {
		if _, err := s.Insert(InsertRequest{
			Database: "app",
			Bucket:   "users",
			Key:      "archived-" + string(rune('a'+i)),
			Data:     map[string]interface{}{"_id_": "archived-" + string(rune('a'+i)), "Status": "archived"},
		}); err != nil {
			t.Fatalf("Insert archived: %v", err)
		}
	}

	// Count with filter
	resp, err := s.Count(CountRequest{
		Database: "app",
		Bucket:   "users",
		Filter:   map[string]interface{}{"Status": "active"},
	})
	if err != nil {
		t.Fatalf("Count with filter: %v", err)
	}
	if resp.Count != 3 {
		t.Fatalf("filtered count = %d, want 3", resp.Count)
	}

	// Count without filter should return total
	resp, err = s.Count(CountRequest{
		Database: "app",
		Bucket:   "users",
	})
	if err != nil {
		t.Fatalf("Count without filter: %v", err)
	}
	if resp.Count != 5 {
		t.Fatalf("total count = %d, want 5", resp.Count)
	}
}

// TestDistinctAllValues tests that Distinct returns unique values of a field.
func TestDistinctAllValues(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "users", BucketTypeHash)

	// Insert documents with duplicate age values
	ages := []float64{18, 25, 25, 32, 18}
	for i, age := range ages {
		if _, err := s.Insert(InsertRequest{
			Database: "app",
			Bucket:   "users",
			Key:      "user-" + string(rune('0'+rune(i))),
			Data:     map[string]interface{}{"_id_": "user-" + string(rune('0'+rune(i))), "Age": age},
		}); err != nil {
			t.Fatalf("Insert: %v", err)
		}
	}

	resp, err := s.Distinct(DistinctRequest{
		Database: "app",
		Bucket:   "users",
		Field:    "Age",
	})
	if err != nil {
		t.Fatalf("Distinct: %v", err)
	}

	// Should have 3 distinct values: 18, 25, 32
	if len(resp.Values) != 3 {
		t.Fatalf("distinct values count = %d, want 3", len(resp.Values))
	}

	// Check that all expected values are present
	valueMap := make(map[float64]bool)
	for _, v := range resp.Values {
		if numVal, ok := toFloat64(v); ok {
			valueMap[numVal] = true
		}
	}
	for _, age := range []float64{18, 25, 32} {
		if !valueMap[age] {
			t.Fatalf("distinct value %f missing", age)
		}
	}
}

// TestDistinctWithFilter tests that Distinct applies a filter before collecting distinct values.
func TestDistinctWithFilter(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "users", BucketTypeHash)

	// Insert documents with status and age
	docs := []map[string]interface{}{
		{"Status": "active", "Age": float64(18)},
		{"Status": "active", "Age": float64(25)},
		{"Status": "active", "Age": float64(32)},
		{"Status": "inactive", "Age": float64(30)},
		{"Status": "inactive", "Age": float64(40)},
		{"Status": "inactive", "Age": float64(50)},
	}
	for i, doc := range docs {
		doc["_id_"] = "user-" + string(rune('0'+rune(i)))
		if _, err := s.Insert(InsertRequest{
			Database: "app",
			Bucket:   "users",
			Key:      doc["_id_"].(string),
			Data:     doc,
		}); err != nil {
			t.Fatalf("Insert: %v", err)
		}
	}

	resp, err := s.Distinct(DistinctRequest{
		Database: "app",
		Bucket:   "users",
		Field:    "Age",
		Filter:   map[string]interface{}{"Status": "active"},
	})
	if err != nil {
		t.Fatalf("Distinct with filter: %v", err)
	}

	// Should have 3 distinct ages from active users: 18, 25, 32
	if len(resp.Values) != 3 {
		t.Fatalf("distinct values count = %d, want 3", len(resp.Values))
	}

	valueMap := make(map[float64]bool)
	for _, v := range resp.Values {
		if numVal, ok := toFloat64(v); ok {
			valueMap[numVal] = true
		}
	}
	for _, age := range []float64{18, 25, 32} {
		if !valueMap[age] {
			t.Fatalf("distinct value %f missing from filtered results", age)
		}
	}
}

// TestDistinctTypeAware tests that Distinct treats 25 (int) and 25.0 (float) as the same value.
func TestDistinctTypeAware(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "data", BucketTypeHash)

	// Insert documents with the same numeric value but different types
	for i, val := range []interface{}{float64(25), float64(25), "25"} {
		if _, err := s.Insert(InsertRequest{
			Database: "app",
			Bucket:   "data",
			Key:      "doc-" + string(rune('0'+rune(i))),
			Data:     map[string]interface{}{"_id_": "doc-" + string(rune('0'+rune(i))), "Value": val},
		}); err != nil {
			t.Fatalf("Insert: %v", err)
		}
	}

	resp, err := s.Distinct(DistinctRequest{
		Database: "app",
		Bucket:   "data",
		Field:    "Value",
	})
	if err != nil {
		t.Fatalf("Distinct: %v", err)
	}

	// Should have 2 distinct values: 25.0 (numeric) and "25" (string)
	if len(resp.Values) != 2 {
		t.Fatalf("distinct values count = %d, want 2 (25 as number and 25 as string)", len(resp.Values))
	}
}

// TestUpdateWithPush tests the $push operator appends to an array.
func TestUpdateWithPush(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "data", BucketTypeHash)

	// Insert document with initial array
	if _, err := s.Insert(InsertRequest{
		Database: "app",
		Bucket:   "data",
		Key:      "doc-1",
		Data:     map[string]interface{}{"_id_": "doc-1", "Tags": []interface{}{"old"}},
	}); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	// Update with $push
	resp, err := s.Update(UpdateRequest{
		Database: "app",
		Bucket:   "data",
		Key:      "doc-1",
		Push:     map[string]interface{}{"Tags": "new"},
	})
	if err != nil {
		t.Fatalf("Update with push: %v", err)
	}

	if !resp.Success {
		t.Fatalf("Update failed: %s", resp.Message)
	}

	tags, _ := resp.Doc["Tags"].([]interface{})
	if len(tags) != 2 {
		t.Fatalf("tags length = %d, want 2", len(tags))
	}
	if tags[0] != "old" || tags[1] != "new" {
		t.Fatalf("tags = %v, want [old new]", tags)
	}
}

// TestUpdateWithAddToSet tests the $addToSet operator adds only if not present.
func TestUpdateWithAddToSet(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "data", BucketTypeHash)

	// Insert document with initial array
	if _, err := s.Insert(InsertRequest{
		Database: "app",
		Bucket:   "data",
		Key:      "doc-1",
		Data:     map[string]interface{}{"_id_": "doc-1", "Tags": []interface{}{"a", "b"}},
	}); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	// AddToSet with new value (should add)
	resp, err := s.Update(UpdateRequest{
		Database: "app",
		Bucket:   "data",
		Key:      "doc-1",
		AddToSet: map[string]interface{}{"Tags": "c"},
	})
	if err != nil {
		t.Fatalf("Update with addToSet: %v", err)
	}

	tagsVal, ok := resp.Doc["Tags"]
	if !ok {
		t.Fatal("Tags field missing")
	}

	// Convert bson.A or []interface{} to []interface{}
	var tags []interface{}
	switch v := tagsVal.(type) {
	case []interface{}:
		tags = v
	case bson.A:
		tags = []interface{}(v)
	default:
		t.Fatalf("Tags has unexpected type %T", tagsVal)
	}

	if len(tags) != 3 {
		t.Fatalf("tags length = %d, want 3 (added 'c')", len(tags))
	}

	// AddToSet with existing value (should be no-op)
	resp, err = s.Update(UpdateRequest{
		Database: "app",
		Bucket:   "data",
		Key:      "doc-1",
		AddToSet: map[string]interface{}{"Tags": "a"},
	})
	if err != nil {
		t.Fatalf("Update with addToSet: %v", err)
	}

	tagsVal, ok = resp.Doc["Tags"]
	if !ok {
		t.Fatal("Tags field missing after second addToSet")
	}

	// Convert bson.A or []interface{} to []interface{}
	switch v := tagsVal.(type) {
	case []interface{}:
		tags = v
	case bson.A:
		tags = []interface{}(v)
	default:
		t.Fatalf("Tags has unexpected type %T", tagsVal)
	}

	if len(tags) != 3 {
		t.Fatalf("tags length = %d, want 3 (unchanged, 'a' already present)", len(tags))
	}
}

// TestUpdateWithMul tests the $mul operator multiplies numeric fields.
func TestUpdateWithMul(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "data", BucketTypeHash)

	// Insert document with numeric field
	if _, err := s.Insert(InsertRequest{
		Database: "app",
		Bucket:   "data",
		Key:      "doc-1",
		Data:     map[string]interface{}{"_id_": "doc-1", "Price": float64(100)},
	}); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	// Multiply price by 1.5
	resp, err := s.Update(UpdateRequest{
		Database: "app",
		Bucket:   "data",
		Key:      "doc-1",
		Mul:      map[string]float64{"Price": 1.5},
	})
	if err != nil {
		t.Fatalf("Update with mul: %v", err)
	}

	price := resp.Doc["Price"].(float64)
	if price != 150 {
		t.Fatalf("price = %f, want 150", price)
	}
}

// TestUpdateWithMin tests the $min operator keeps minimum value.
func TestUpdateWithMin(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "data", BucketTypeHash)

	// Insert document with field
	if _, err := s.Insert(InsertRequest{
		Database: "app",
		Bucket:   "data",
		Key:      "doc-1",
		Data:     map[string]interface{}{"_id_": "doc-1", "MinTemp": float64(20)},
	}); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	// Apply $min with smaller value
	resp, err := s.Update(UpdateRequest{
		Database: "app",
		Bucket:   "data",
		Key:      "doc-1",
		Min:      map[string]interface{}{"MinTemp": float64(15)},
	})
	if err != nil {
		t.Fatalf("Update with min: %v", err)
	}

	temp := resp.Doc["MinTemp"].(float64)
	if temp != 15 {
		t.Fatalf("MinTemp = %f, want 15", temp)
	}

	// Apply $min with larger value (should not change)
	resp, err = s.Update(UpdateRequest{
		Database: "app",
		Bucket:   "data",
		Key:      "doc-1",
		Min:      map[string]interface{}{"MinTemp": float64(25)},
	})
	if err != nil {
		t.Fatalf("Update with min: %v", err)
	}

	temp = resp.Doc["MinTemp"].(float64)
	if temp != 15 {
		t.Fatalf("MinTemp = %f, want 15 (unchanged)", temp)
	}
}

// TestUpdateWithMax tests the $max operator keeps maximum value.
func TestUpdateWithMax(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "data", BucketTypeHash)

	// Insert document with field
	if _, err := s.Insert(InsertRequest{
		Database: "app",
		Bucket:   "data",
		Key:      "doc-1",
		Data:     map[string]interface{}{"_id_": "doc-1", "MaxTemp": float64(30)},
	}); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	// Apply $max with larger value
	resp, err := s.Update(UpdateRequest{
		Database: "app",
		Bucket:   "data",
		Key:      "doc-1",
		Max:      map[string]interface{}{"MaxTemp": float64(35)},
	})
	if err != nil {
		t.Fatalf("Update with max: %v", err)
	}

	temp := resp.Doc["MaxTemp"].(float64)
	if temp != 35 {
		t.Fatalf("MaxTemp = %f, want 35", temp)
	}
}

// TestUpdateWithRename tests the $rename operator renames a field.
func TestUpdateWithRename(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "data", BucketTypeHash)

	// Insert document with field
	if _, err := s.Insert(InsertRequest{
		Database: "app",
		Bucket:   "data",
		Key:      "doc-1",
		Data:     map[string]interface{}{"_id_": "doc-1", "OldName": "value"},
	}); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	// Rename field
	resp, err := s.Update(UpdateRequest{
		Database: "app",
		Bucket:   "data",
		Key:      "doc-1",
		Rename:   map[string]string{"OldName": "NewName"},
	})
	if err != nil {
		t.Fatalf("Update with rename: %v", err)
	}

	if _, ok := resp.Doc["OldName"]; ok {
		t.Fatal("OldName should be deleted")
	}
	if newVal, ok := resp.Doc["NewName"].(string); !ok || newVal != "value" {
		t.Fatalf("NewName = %v, want 'value'", resp.Doc["NewName"])
	}
}

// TestMultiUpdateAllMatching tests that multi: true updates all matching documents.
func TestMultiUpdateAllMatching(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "users", BucketTypeHash)

	// Insert documents with different statuses
	for i := 0; i < 5; i++ {
		if _, err := s.Insert(InsertRequest{
			Database: "app",
			Bucket:   "users",
			Key:      "user-" + string(rune('0'+rune(i))),
			Data: map[string]interface{}{
				"_id_":   "user-" + string(rune('0'+rune(i))),
				"Status": "inactive",
			},
		}); err != nil {
			t.Fatalf("Insert: %v", err)
		}
	}

	// Multi-update to change all to active
	resp, err := s.Update(UpdateRequest{
		Database: "app",
		Bucket:   "users",
		Filter:   map[string]interface{}{"Status": "inactive"},
		Set:      map[string]interface{}{"Status": "active"},
		Multi:    true,
	})
	if err != nil {
		t.Fatalf("Multi-update: %v", err)
	}

	if !resp.Success {
		t.Fatalf("Update failed: %s", resp.Message)
	}

	if !strings.Contains(resp.Message, "updated 5 documents") {
		t.Fatalf("Message does not mention 5 documents: %q", resp.Message)
	}

	// Verify all were updated by scanning
	scanResp, err := s.Scan(ScanRequest{
		Database: "app",
		Bucket:   "users",
		Filter:   map[string]interface{}{"Status": "active"},
	})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}

	if scanResp.Total != 5 {
		t.Fatalf("Found %d active users, want 5", scanResp.Total)
	}
}

// TestMultiUpdateZeroMatches tests that multi: true on zero matches returns 0 updated.
func TestMultiUpdateZeroMatches(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "users", BucketTypeHash)

	// Insert documents
	for i := 0; i < 3; i++ {
		if _, err := s.Insert(InsertRequest{
			Database: "app",
			Bucket:   "users",
			Key:      "user-" + string(rune('0'+rune(i))),
			Data: map[string]interface{}{
				"_id_":   "user-" + string(rune('0'+rune(i))),
				"Status": "active",
			},
		}); err != nil {
			t.Fatalf("Insert: %v", err)
		}
	}

	// Multi-update with filter that matches nothing
	resp, err := s.Update(UpdateRequest{
		Database: "app",
		Bucket:   "users",
		Filter:   map[string]interface{}{"Status": "archived"},
		Set:      map[string]interface{}{"Status": "active"},
		Multi:    true,
	})
	if err != nil {
		t.Fatalf("Multi-update: %v", err)
	}

	if !resp.Success {
		t.Fatalf("Update failed: %s", resp.Message)
	}

	if !strings.Contains(resp.Message, "updated 0 documents") {
		t.Fatalf("Message does not mention 0 documents: %q", resp.Message)
	}
}

// TestMultiUpdateWarningOnLarge tests that multi: true >1000 docs warns.
func TestMultiUpdateWarningOnLarge(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "large", BucketTypeHash)

	// Insert 1500 documents
	for i := 0; i < 1500; i++ {
		if _, err := s.Insert(InsertRequest{
			Database: "app",
			Bucket:   "large",
			Key:      "doc-" + fmt.Sprintf("%04d", i),
			Data: map[string]interface{}{
				"_id_":   "doc-" + fmt.Sprintf("%04d", i),
				"Status": "pending",
			},
		}); err != nil {
			t.Fatalf("Insert: %v", err)
		}
	}

	// Multi-update all
	resp, err := s.Update(UpdateRequest{
		Database: "app",
		Bucket:   "large",
		Filter:   map[string]interface{}{"Status": "pending"},
		Set:      map[string]interface{}{"Status": "done"},
		Multi:    true,
	})
	if err != nil {
		t.Fatalf("Multi-update: %v", err)
	}

	if !strings.Contains(resp.Message, "consider batching") {
		t.Fatalf("Message does not warn about batching: %q", resp.Message)
	}
}

func contains(haystack, needle string) bool {
	return strings.Contains(haystack, needle)
}

// stateReadFailingKV is a node store whose next read of one key through a
// snapshot fails, once, as a store read fault would.
type stateReadFailingKV struct {
	storage.KV
	mu      sync.Mutex
	failKey []byte
}

func (k *stateReadFailingKV) failNextReadOf(key []byte) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.failKey = key
}

func (k *stateReadFailingKV) NewSnapshot() storage.KVSnapshot {
	return &stateReadFailingSnapshot{KVSnapshot: k.KV.NewSnapshot(), kv: k}
}

type stateReadFailingSnapshot struct {
	storage.KVSnapshot
	kv *stateReadFailingKV
}

func (sn *stateReadFailingSnapshot) Get(key []byte) ([]byte, error) {
	sn.kv.mu.Lock()
	fail := sn.kv.failKey != nil && bytes.Equal(key, sn.kv.failKey)
	if fail {
		sn.kv.failKey = nil
	}
	sn.kv.mu.Unlock()
	if fail {
		return nil, errors.New("injected read fault")
	}
	return sn.KVSnapshot.Get(key)
}

// TestBucketStatsReturnsACountError: a bucket whose entry count
// cannot be read is not an empty bucket. BucketStats answered Entries 0 with
// no error; it must fail, naming the bucket.
func TestBucketStatsReturnsACountError(t *testing.T) {
	// serverOnStore starts no background goroutine, so the store can be
	// wrapped without racing the reclaimer that NewService starts.
	failing := &stateReadFailingKV{KV: kvfault.New()}
	s := serverOnStore(t, failing)
	createIndexedHashBucket(t, s, "app", "users", "Name")
	b := lookupBucket(s, "app", "users")
	if resp, err := s.InsertCtx(withWriteVersion(context.Background(), 7), InsertRequest{
		Database: "app", Bucket: "users", Key: "u1", Data: map[string]interface{}{"Name": "a"}}); err != nil || !resp.Success {
		t.Fatalf("insert: %+v, %v", resp, err)
	}

	failing.failNextReadOf(storage.Key(b.slot, b.databaseID, b.id.Load(), storage.NSState))
	stats, err := s.BucketStats(BucketStatsRequest{Database: "app", Bucket: "users"})
	if err == nil || !strings.Contains(err.Error(), `"users"`) {
		t.Fatalf("BucketStats with an unreadable entry count answered %+v, %v; want an error naming bucket \"users\"", stats, err)
	}
}

// TestMultiUpdateOnAClosedBucketIsRefused: a caller that
// resolved a bucket and lost the race to a drop must not write into the key
// range the drop handed to the reclaimer. The single-document update checks
// the bucket's closed flag after taking its lock; the multi-document one did
// not. Since CG-12 both take the bucket through lockBucket, so the bucket is
// closed in the window between the resolve and the lock, which is where a
// drop lands.
func TestMultiUpdateOnAClosedBucketIsRefused(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "users", BucketTypeHash)
	mustInsert(t, s, "app", "users", "u1", map[string]interface{}{"Name": "a"})
	retained := servedBucket(t, s, "app", "users")
	closeAfterResolve := func(_, name string) {
		if name == "users" {
			closeRecordForTest(s, retained.rec)
		}
	}
	afterResolveHook.Store(&closeAfterResolve)
	t.Cleanup(func() { afterResolveHook.Store(nil) })

	resp, err := s.UpdateCtx(context.Background(), UpdateRequest{
		Database: "app", Bucket: "users", Multi: true,
		Filter: map[string]interface{}{"Name": "a"}, Set: map[string]interface{}{"Name": "b"},
	})
	if err == nil {
		t.Fatalf("a multi-document update of a closed bucket answered %+v, want a bucket-not-found error", resp)
	}
	if doc := documentOf(t, s, "app", "users", "u1"); doc["Name"] != "a" {
		t.Errorf("the refused update still wrote: Name is %v, want a", doc["Name"])
	}
}

// renameShapes are the overlapping $rename sets and two that never
// overlapped. Part 1 applied the pairs in source-name order, so every copy
// reached one document; part 2 (typed-fields spec §4.9.2, OC-3) refuses an
// overlapping set before any operator runs, so an overlapping shape is refused
// on every run, its want is the stored document unchanged, and every copy and
// every replay refuses alike.
var renameShapes = []struct {
	name    string
	stored  map[string]interface{}
	req     UpdateRequest
	want    map[string]interface{}
	refused bool
}{
	{
		name:    "chain {a:b, b:c}",
		stored:  map[string]interface{}{"a": int32(1)},
		req:     UpdateRequest{Rename: map[string]string{"a": "b", "b": "c"}},
		want:    map[string]interface{}{"a": int32(1)},
		refused: true,
	},
	{
		name:    "swap {a:b, b:a}",
		stored:  map[string]interface{}{"a": int32(1)},
		req:     UpdateRequest{Rename: map[string]string{"a": "b", "b": "a"}},
		want:    map[string]interface{}{"a": int32(1)},
		refused: true,
	},
	{
		name:    "shared target {a:t, x:t}",
		stored:  map[string]interface{}{"a": int32(1), "x": int32(2)},
		req:     UpdateRequest{Rename: map[string]string{"a": "t", "x": "t"}},
		want:    map[string]interface{}{"a": int32(1), "x": int32(2)},
		refused: true,
	},
	{
		name:   "disjoint {a:b, c:d}",
		stored: map[string]interface{}{"a": int32(1), "c": int32(2)},
		req:    UpdateRequest{Rename: map[string]string{"a": "b", "c": "d"}},
		want:   map[string]interface{}{"b": int32(1), "d": int32(2)},
	},
	{
		// $set runs before $rename, as it always has.
		name:   "$set then $rename on one key",
		stored: map[string]interface{}{"a": int32(1)},
		req:    UpdateRequest{Set: map[string]interface{}{"a": int32(5)}, Rename: map[string]string{"a": "b"}},
		want:   map[string]interface{}{"b": int32(5)},
	},
}

// TestApplyUpdateOperatorsRenameIsDeterministic: The owner, every
// replica and every replay of the log re-run the operators on their own copy,
// so a $rename whose pairs overlap must give one answer however Go happens to
// order the map: since part 2 that answer is the refusal, on every run. Before
// part 1 each overlapping shape gave two documents in 400 runs (the false-pass
// probability is below 2^-399 per shape).
func TestApplyUpdateOperatorsRenameIsDeterministic(t *testing.T) {
	const runs = 400
	for _, shape := range renameShapes {
		t.Run(shape.name, func(t *testing.T) {
			outcomeCounts := map[string]int{}
			firstDivergentRun := 0
			var firstOutcome string
			for run := 1; run <= runs; run++ {
				doc := maps.Clone(shape.stored)
				err := applyUpdateOperators(doc, "k", shape.req)
				if (err != nil) != shape.refused {
					t.Fatalf("run %d: applyUpdateOperators = %v; refused should be %v", run, err, shape.refused)
				}
				// fmt prints a map's keys sorted, so equal documents print alike.
				outcome := fmt.Sprint(doc)
				if err != nil {
					if !reflect.DeepEqual(doc, shape.want) {
						t.Fatalf("run %d: the refused update changed the document to %v", run, doc)
					}
					outcome = err.Error()
				}
				if run == 1 {
					firstOutcome = outcome
				} else if outcome != firstOutcome && firstDivergentRun == 0 {
					firstDivergentRun = run
				}
				outcomeCounts[outcome]++
			}
			t.Logf("%d runs: %v", runs, outcomeCounts)
			if len(outcomeCounts) != 1 {
				t.Fatalf("one update gave %d different documents in %d runs (first divergence on run %d): %v",
					len(outcomeCounts), runs, firstDivergentRun, outcomeCounts)
			}
			if want := fmt.Sprint(shape.want); !shape.refused && firstOutcome != want {
				t.Errorf("document = %s, want %s (pairs apply in source-name order)", firstOutcome, want)
			}
		})
	}
}

// TestUpdateDottedKeyIsNotALiteralTopLevelField: $set, $inc and
// $unset of a dotted key added or removed a top-level field literally named
// "Address.City" and left the nested member as it was. A key is a path.
func TestUpdateDottedKeyIsNotALiteralTopLevelField(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "P", BucketTypeHash)
	mustInsert(t, s, "app", "P", "a", map[string]interface{}{
		"Address": map[string]interface{}{"City": "Tirana", "Zip": "1001", "Visits": int32(1)},
	})
	for _, req := range []UpdateRequest{
		{Set: map[string]interface{}{"Address.City": "Paris"}},
		{Inc: map[string]int64{"Address.Visits": 1}},
		{Unset: []string{"Address.Zip"}},
	} {
		req.Database, req.Bucket, req.Key = "app", "P", "a"
		if _, err := s.Update(req); err != nil {
			t.Fatalf("update %+v: %v", req, err)
		}
	}
	stored := documentOf(t, s, "app", "P", "a")
	for _, literal := range []string{"Address.City", "Address.Visits", "Address.Zip"} {
		if _, added := stored[literal]; added {
			t.Errorf("the update added a top-level field literally named %q: %v", literal, stored)
		}
	}
	want := map[string]interface{}{"City": "Paris", "Visits": int64(2)}
	if got := stored["Address"]; !reflect.DeepEqual(got, want) {
		t.Errorf("Address = %v, want %v: $set, $inc and $unset reach the nested members", got, want)
	}
}

// TestUpdateRefusesOverlappingRenames checks that a $rename whose
// sources and targets overlap — a target that is another pair's source, a
// shared target, a pair inside itself — is refused before any operator runs,
// with a message naming the pairs, and the document is unchanged. The refusal
// depends only on the request, so every copy and every replay refuses alike
// (criterion as amended 2026-10-03; typed-fields build SA-3).
func TestUpdateRefusesOverlappingRenames(t *testing.T) {
	for _, shape := range renameOverlapShapes {
		s := newTestServer(t)
		createTestBucket(t, s, "app", "P", BucketTypeHash)
		mustInsert(t, s, "app", "P", "a", maps.Clone(shape.stored))
		_, err := s.Update(UpdateRequest{Database: "app", Bucket: "P", Key: "a", Rename: shape.rename})
		if err == nil || !strings.Contains(err.Error(), shape.says) || !strings.Contains(err.Error(), "Nothing was changed") {
			t.Errorf("%s: the update answered %v; want a refusal saying %q", shape.name, err, shape.says)
		}
		stored := documentOf(t, s, "app", "P", "a")
		delete(stored, "_id_")
		if !reflect.DeepEqual(stored, shape.stored) {
			t.Errorf("%s: the refused update changed the document to %v", shape.name, stored)
		}
	}
	// A non-overlapping multi-pair $rename still works.
	s := newTestServer(t)
	createTestBucket(t, s, "app", "P", BucketTypeHash)
	mustInsert(t, s, "app", "P", "a", map[string]interface{}{"a": int32(1), "c": int32(2)})
	if _, err := s.Update(UpdateRequest{Database: "app", Bucket: "P", Key: "a", Rename: map[string]string{"a": "b", "c": "d"}}); err != nil {
		t.Fatalf("a disjoint rename was refused: %v", err)
	}
}

// TestCreateExistingDatabaseAnswersAlreadyExists pins the decision that creation is not idempotent:
// creating a database that exists is
// an ALREADY_EXISTS error frame, with nothing logged, on a standalone node and
// through a cluster node alike.
func TestCreateExistingDatabaseAnswersAlreadyExists(t *testing.T) {
	assertRefused := func(t *testing.T, answer *transport.Frame) {
		t.Helper()
		if answer.Opcode != transport.OpError {
			t.Fatalf("second create answered %s, want ERROR with ALREADY_EXISTS", answer.Opcode)
		}
		if got, want := decodeErrorCode(t, answer.Payload), int(transport.ErrAlreadyExists); got != want {
			t.Errorf("error code = %d, want %d (ALREADY_EXISTS)", got, want)
		}
		if message := decodeErrorMessage(t, answer.Payload); !strings.Contains(message, `database "app" already exists`) {
			t.Errorf("message %q does not say the database exists", message)
		}
	}
	t.Run("standalone", func(t *testing.T) {
		s := newTestServer(t)
		handler := NewHandler(s)
		create := func() *transport.Frame {
			answer, err := handler(context.Background(), transport.NewFrame(transport.OpCreateDB, mustMarshal(t, CreateDatabaseRequest{Name: "app"})))
			if err != nil {
				t.Fatalf("create database: %v", err)
			}
			return answer
		}
		if first := create(); first.Opcode == transport.OpError {
			t.Fatalf("first create refused: %s", first.Payload)
		}
		assertRefused(t, create())
	})
	t.Run("cluster", func(t *testing.T) {
		tc := newTestCluster(t, 2, 2)
		tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
		assertRefused(t, tc.frame(0, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"}))
	})
}

// TestUserValueShapedLikeTheNowSentinelIsNotRewritten is BUG-0151: a user's own
// sub-document {"$$now": true} is data, stored as written, on every write path
// that fixes now(). now() itself is a marker no stored value has the shape of
// (TestNowMarkerStoresTheCurrentTime).
func TestUserValueShapedLikeTheNowSentinelIsNotRewritten(t *testing.T) {
	literal := func() map[string]interface{} { return map[string]interface{}{"$$now": true} }
	requireLiteral := func(t *testing.T, where string, field string, got interface{}) {
		t.Helper()
		stored, isDocument := got.(map[string]interface{})
		if !isDocument || len(stored) != 1 || stored["$$now"] != true {
			t.Errorf("%s: %s stored as %T %v; want the sub-document {\"$$now\": true} as written", where, field, got, got)
		}
	}
	s := newTestServer(t)
	createTestBucket(t, s, "app", "events", BucketTypeHash)

	doorWrite(t, s, transport.OpPut, InsertRequest{Database: "app", Bucket: "events", Key: "put",
		Data: map[string]interface{}{"n": literal(), "list": []interface{}{literal()}}})
	stored := documentOf(t, s, "app", "events", "put")
	requireLiteral(t, "put", "n", stored["n"])
	list, _ := stored["list"].(primitive.A)
	if len(list) != 1 {
		t.Fatalf("put: list stored as %v", stored["list"])
	}
	requireLiteral(t, "put", "list[0]", list[0])

	doorWrite(t, s, transport.OpBatchPut, BatchInsertRequest{Database: "app", Bucket: "events",
		Data: []map[string]interface{}{{"_id_": "batch", "n": literal()}}})
	requireLiteral(t, "batch put", "n", documentOf(t, s, "app", "events", "batch")["n"])

	mustInsert(t, s, "app", "events", "upd", map[string]interface{}{"seed": 1})
	doorWrite(t, s, transport.OpUpdate, UpdateRequest{Database: "app", Bucket: "events", Key: "upd",
		Set: map[string]interface{}{"n": literal()}})
	requireLiteral(t, "update $set", "n", documentOf(t, s, "app", "events", "upd")["n"])

	// A cluster logs the payload resolveDocumentIDs returns; for a write that
	// names its key and holds no now() that is the payload as sent.
	payload := marshalRequest(t, InsertRequest{Database: "app", Bucket: "events", Key: "k",
		Data: map[string]interface{}{"n": literal()}})
	if logged := payloadAsLogged(t, transport.OpPut, payload); !bytes.Equal(logged, payload) {
		t.Errorf("a cluster would log a rewritten payload for a user value shaped like the sentinel:\n sent   %x\n logged %x", payload, logged)
	}
}

// TestNowMarkerStoresTheCurrentTime: the marker a client sends for now() is
// still replaced, on a put, a batch and an update, by the server's time of the
// write, whether it sits beside a user's {"$$now": true} or not.
func TestNowMarkerStoresTheCurrentTime(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "events", BucketTypeHash)
	before := time.Now().Add(-time.Second)

	doorWrite(t, s, transport.OpPut, InsertRequest{Database: "app", Bucket: "events", Key: "put",
		Data: map[string]interface{}{"at": NowMarker(), "kept": map[string]interface{}{"$$now": true}}})
	doorWrite(t, s, transport.OpBatchPut, BatchInsertRequest{Database: "app", Bucket: "events",
		Data: []map[string]interface{}{{"_id_": "batch", "at": NowMarker()}}})
	mustInsert(t, s, "app", "events", "upd", map[string]interface{}{"seed": 1})
	doorWrite(t, s, transport.OpUpdate, UpdateRequest{Database: "app", Bucket: "events", Key: "upd",
		Set: map[string]interface{}{"at": NowMarker()}})

	for _, key := range []string{"put", "batch", "upd"} {
		stored, isDate := documentOf(t, s, "app", "events", key)["at"].(primitive.DateTime)
		if !isDate {
			t.Errorf("%s: at is not a datetime after now()", key)
			continue
		}
		if instant := stored.Time(); instant.Before(before) || instant.After(time.Now().Add(time.Second)) {
			t.Errorf("%s: at = %s; want the time of the write", key, instant)
		}
	}
	kept, _ := documentOf(t, s, "app", "events", "put")["kept"].(map[string]interface{})
	if kept["$$now"] != true {
		t.Errorf("put: kept = %v beside a now() marker; want {\"$$now\": true} as written", kept)
	}
}

// TestEmptyStringIdIsRefusedOrDocumented is BUG-0153: an _id_ present as an
// empty string used to be folded into "absent" and silently replaced by a
// generated key, so a loader whose key column was sometimes empty grew a
// fresh random key on every re-run. The owner's ruling of 2026-10-18 chose
// refusal: an _id_ must be a non-empty string, or be omitted for a generated
// one. Every document write path that reads an _id_ refuses it before
// anything is stored, and the cluster's door refuses it before the log.
func TestEmptyStringIdIsRefusedOrDocumented(t *testing.T) {
	const wantFix = "non-empty string"

	s := newTestServer(t)
	createTestBucket(t, s, "app", "users", BucketTypeHash)
	createTestBucket(t, s, "app", "events", BucketTypeBTree)
	createTestBucket(t, s, "app", "queue", BucketTypeFIFO)
	if resp := createWithStores(t, s, "app", "post", BucketTypeHash,
		StoreDeclaration{Name: "Notes", Engine: "hashStore"},
		StoreDeclaration{Name: "History", Engine: "fifoStore"}); !resp.Success {
		t.Fatalf("creating post with store fields: %+v", resp)
	}
	mustInsert(t, s, "app", "post", "p1", map[string]interface{}{"Title": "x"})

	empty := func() map[string]interface{} {
		return map[string]interface{}{"_id_": "", "x": int32(1)}
	}

	writes := []struct {
		name string
		call func() error
	}{
		{"insert", func() error {
			_, err := s.Insert(InsertRequest{Database: "app", Bucket: "users", Data: empty()})
			return err
		}},
		{"btree insert", func() error {
			_, err := s.Insert(InsertRequest{Database: "app", Bucket: "events", Data: empty()})
			return err
		}},
		{"batch insert", func() error {
			_, err := s.BatchInsert(BatchInsertRequest{Database: "app", Bucket: "users",
				Data: []map[string]interface{}{{"_id_": "ok"}, empty()}})
			return err
		}},
		{"push", func() error {
			_, err := s.Push(PushRequest{Database: "app", Bucket: "queue", Data: empty()})
			return err
		}},
		{"field push", func() error {
			_, err := s.FieldPushCtx(context.Background(), FieldRequest{Database: "app", Bucket: "post",
				Key: "p1", Field: "History", Data: empty()})
			return err
		}},
		{"field put", func() error {
			_, err := s.FieldPutCtx(context.Background(), FieldRequest{Database: "app", Bucket: "post",
				Key: "p1", Field: "Notes", Element: "n1", Data: empty()})
			return err
		}},
	}
	for _, write := range writes {
		t.Run(write.name, func(t *testing.T) {
			err := write.call()
			if err == nil {
				t.Fatal("an empty-string _id_ was accepted; want a refusal naming the fix")
			}
			if !contains(err.Error(), wantFix) {
				t.Errorf("refusal %q does not say an _id_ must be a non-empty string", err)
			}
		})
	}

	// The second document of the batch is the empty one, and the refusal says
	// which document it is, as an E-18 refusal does.
	if _, err := s.BatchInsert(BatchInsertRequest{Database: "app", Bucket: "users",
		Data: []map[string]interface{}{{"_id_": "ok"}, empty()}}); err == nil || !contains(err.Error(), "Document 2") {
		t.Errorf("the batch refusal %v does not locate the empty _id_ at document 2", err)
	}

	// The cluster's door refuses before logging: a write refused only by the
	// applier would still be logged and shipped to every replica.
	doors := []struct {
		name string
		op   transport.Opcode
		call func() []byte
	}{
		{"insert at the door", transport.OpPut, func() []byte {
			return marshalRequest(t, InsertRequest{Database: "app", Bucket: "users", Data: empty()})
		}},
		{"batch insert at the door", transport.OpBatchPut, func() []byte {
			return marshalRequest(t, BatchInsertRequest{Database: "app", Bucket: "users",
				Data: []map[string]interface{}{empty()}})
		}},
		{"push at the door", transport.OpFIFOPush, func() []byte {
			return marshalRequest(t, PushRequest{Database: "app", Bucket: "queue", Data: empty()})
		}},
		{"field put at the door", transport.OpFieldPut, func() []byte {
			return marshalRequest(t, FieldRequest{Database: "app", Bucket: "post",
				Key: "p1", Field: "Notes", Element: "n1", Data: empty()})
		}},
	}
	for _, door := range doors {
		t.Run(door.name, func(t *testing.T) {
			if _, err := resolveDocumentIDs(door.op, door.call()); err == nil {
				t.Error("resolveDocumentIDs accepted an empty-string _id_; want a refusal before the log")
			}
		})
	}

	// Nothing the refusals touched was stored.
	if count, err := s.Count(CountRequest{Database: "app", Bucket: "users"}); err != nil || count.Count != 0 {
		t.Errorf("the refused writes stored %d documents (err %v), want 0", count.Count, err)
	}

	// The substitution the docs already promise is unchanged: an absent or
	// non-string _id_ still gets a generated key.
	for _, document := range []map[string]interface{}{{"x": int32(1)}, {"_id_": 7, "x": int32(1)}} {
		resp, err := s.Insert(InsertRequest{Database: "app", Bucket: "users", Data: document})
		if err != nil || !resp.Success || resp.ID == "" {
			t.Errorf("insert of %v = %+v, %v; want a generated key", document, resp, err)
		}
	}
}
