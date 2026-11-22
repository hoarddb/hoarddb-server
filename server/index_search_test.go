package server

import (
	"testing"
)

// createIndexedTestBucket creates a hash bucket with the given secondary
// indexes declared through the same Options shape the wire uses.
func createIndexedTestBucket(t *testing.T, s *Server, db, bucket string, indices []interface{}) {
	t.Helper()

	if resp, err := s.CreateDatabase(db); err != nil || !resp.Success {
		t.Fatalf("CreateDatabase(%q) = %+v, %v", db, resp, err)
	}
	resp, err := s.CreateBucket(CreateBucketRequest{
		Database: db,
		Name:     bucket,
		Type:     BucketTypeHash,
		Options:  map[string]interface{}{"indices": indices},
	})
	if err != nil || !resp.Success {
		t.Fatalf("CreateBucket(%q) = %+v, %v", bucket, resp, err)
	}
}

// TestInsertIsIndexedAndSearchable covers the documented promise: declare an
// index on a bucket, insert a document, and search by that field finds it.
// A bucket that reports an index but answers no search is worse than no index
// at all, so this asserts the whole path — declare, insert, search.
func TestInsertIsIndexedAndSearchable(t *testing.T) {
	s := newTestServer(t)
	createIndexedTestBucket(t, s, "app", "users", []interface{}{"Email"})

	docs := []map[string]interface{}{
		{"Name": "alice_smith", "Email": "dalek@example.com", "Age": 42},
		{"Name": "second_user", "Email": "two@example.com", "Age": 7},
	}
	for _, doc := range docs {
		ins, err := s.Insert(InsertRequest{Database: "app", Bucket: "users", Data: doc})
		if err != nil || !ins.Success {
			t.Fatalf("Insert(%v) = %+v, %v", doc["Email"], ins, err)
		}
	}

	bucket, err := s.resolveBucket("app", "users")
	if err != nil {
		t.Fatalf("resolving users: %v", err)
	}
	if bucket.idxMgr == nil {
		t.Fatal("bucket has no index manager after declaring indices: [\"Email\"]")
	}
	t.Logf("indexes: %+v", bucket.idxMgr.ListIndexes())

	hits, err := s.Search(SearchRequest{
		Database: "app",
		Bucket:   "users",
		Filter:   map[string]interface{}{"Email": "dalek@example.com"},
	})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if hits.Message != "" {
		t.Fatalf("Search message = %q, want empty", hits.Message)
	}
	if len(hits.Results) != 1 {
		t.Fatalf("Search matched %d documents (%+v), want exactly 1", len(hits.Results), hits.Results)
	}
	if got := decodedDocuments(t, hits.Results)[0]["Name"]; got != "alice_smith" {
		t.Fatalf("Search returned %v, want alice_smith", got)
	}
	if hits.Total != 1 {
		t.Fatalf("Search total = %d, want 1", hits.Total)
	}

	// A field with no index is answered by a full scan that says so (GAP-14
	// S-8) — not by zero hits, which is the failure this assertion guarded.
	unindexed, err := s.Search(SearchRequest{
		Database: "app",
		Bucket:   "users",
		Filter:   map[string]interface{}{"Name": "alice_smith"},
	})
	if err != nil {
		t.Fatalf("Search on unindexed field: %v", err)
	}
	if unindexed.Total != 1 || indexUsedOf(t, unindexed) != fullScan {
		t.Fatalf("searching an unindexed field answered %+v; want its one match, served by %q", unindexed, fullScan)
	}
}

// TestSearchFindsNothingForUnknownValue keeps "no results" honest: the index
// must not match a value that was never written.
func TestSearchFindsNothingForUnknownValue(t *testing.T) {
	s := newTestServer(t)
	createIndexedTestBucket(t, s, "app", "users", []interface{}{"Email"})

	if ins, err := s.Insert(InsertRequest{Database: "app", Bucket: "users", Data: map[string]interface{}{
		"Email": "dalek@example.com",
	}}); err != nil || !ins.Success {
		t.Fatalf("Insert = %+v, %v", ins, err)
	}

	hits, err := s.Search(SearchRequest{
		Database: "app",
		Bucket:   "users",
		Filter:   map[string]interface{}{"Email": "nobody@example.com"},
	})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(hits.Results) != 0 {
		t.Fatalf("Search for an unwritten value returned %+v", hits.Results)
	}
}
