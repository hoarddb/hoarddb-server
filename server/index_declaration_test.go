package server

import (
	"log/slog"
	"strings"
	"testing"
)

// TestUnknownIndexTypeIsRefused covers an unknown index type. An index declared with a
// type the server does not build — "unique" above all, which a migrating user
// reads as a data-integrity promise — was built as a plain equality index and
// the create reported success. It must be refused, naming the type and the
// supported set. The accepted spellings stay accepted.
func TestUnknownIndexTypeIsRefused(t *testing.T) {
	s := newTestServer(t)
	if _, err := s.CreateDatabase("app"); err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}
	for i, tt := range []struct {
		kind   string
		accept bool
	}{
		{kind: "", accept: true},
		{kind: "equality", accept: true},
		{kind: "range", accept: true},
		{kind: "Range", accept: true},
		{kind: "unique", accept: false},
		{kind: "compound", accept: false},
		{kind: "text", accept: false},
	} {
		name := "b" + string(rune('a'+i))
		resp, err := s.CreateBucket(CreateBucketRequest{
			Database: "app", Name: name, Type: BucketTypeHash,
			Options: map[string]interface{}{"indices": []interface{}{map[string]interface{}{"field": "Email", "type": tt.kind}}},
		})
		if err != nil {
			t.Fatalf("type %q: CreateBucket: %v", tt.kind, err)
		}
		if resp.Success != tt.accept {
			t.Errorf("index type %q: create success = %v, want %v (%s)", tt.kind, resp.Success, tt.accept, resp.Message)
			continue
		}
		if !tt.accept && (!strings.Contains(resp.Message, tt.kind) || !strings.Contains(resp.Message, "range")) {
			t.Errorf("index type %q refused without naming it and the supported set: %s", tt.kind, resp.Message)
		}
	}
}

// TestIndexOnAnEngineThatDoesNotMaintainOneIsRefused covers the engine check on
// the current tree. Only the hash engine maintains a secondary index; a btree
// (or any other) bucket declared with one was accepted, its index was never
// written, and search then answered "no results" for documents that match —
// a wrong answer with a success behind it. The create must be refused with
// the reason, and a hash bucket's declaration must still work end to end.
func TestIndexOnAnEngineThatDoesNotMaintainOneIsRefused(t *testing.T) {
	s := newTestServer(t)
	if _, err := s.CreateDatabase("app"); err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}
	indexed := map[string]interface{}{"indices": []string{"Email"}}
	for _, bucketType := range []BucketType{BucketTypeBTree, BucketTypeHeap, BucketTypeFIFO, BucketTypeLIFO, BucketTypeBlob} {
		resp, err := s.CreateBucket(CreateBucketRequest{Database: "app", Name: "i" + string(bucketType), Type: bucketType, Options: indexed})
		if err != nil {
			t.Fatalf("%s: CreateBucket: %v", bucketType, err)
		}
		if resp.Success {
			t.Errorf("a %s bucket was created with a secondary index nothing maintains; search would answer no results for matching documents", bucketType)
			continue
		}
		if !strings.Contains(resp.Message, "hash") {
			t.Errorf("%s: refused without saying which engine maintains indexes: %s", bucketType, resp.Message)
		}
	}

	if resp, err := s.CreateBucket(CreateBucketRequest{Database: "app", Name: "users", Type: BucketTypeHash, Options: indexed}); err != nil || !resp.Success {
		t.Fatalf("an indexed hash bucket: %+v, %v", resp, err)
	}
	if resp, err := s.Insert(InsertRequest{Database: "app", Bucket: "users", Key: "u1", Data: map[string]interface{}{"Email": "a@example.com"}}); err != nil || !resp.Success {
		t.Fatalf("insert: %+v, %v", resp, err)
	}
	found, err := s.Search(SearchRequest{Database: "app", Bucket: "users", Filter: map[string]interface{}{"Email": "a@example.com"}})
	if err != nil || found.Total != 1 {
		t.Fatalf("search on the indexed hash bucket: %+v, %v; want one result", found, err)
	}
}

// declareIndexes creates a hash bucket with the given indices value and
// returns the create's response.
func declareIndexes(t *testing.T, s *Server, bucket string, indices interface{}) *CreateBucketResponse {
	t.Helper()
	if resp, err := s.CreateDatabase("app"); err != nil || (!resp.Success && !strings.Contains(resp.Message, "already exists")) {
		t.Fatalf("CreateDatabase = %+v, %v", resp, err)
	}
	resp, err := s.CreateBucket(CreateBucketRequest{
		Database: "app", Name: bucket, Type: BucketTypeHash,
		Options: map[string]interface{}{"indices": indices},
	})
	if err != nil {
		t.Fatalf("CreateBucket(%s): %v", bucket, err)
	}
	return resp
}

// requireRefused fails unless the create was refused with a message naming
// every one of want.
func requireRefused(t *testing.T, what string, resp *CreateBucketResponse, want ...string) {
	t.Helper()
	if resp.Success {
		t.Errorf("%s was accepted: %s", what, resp.Message)
		return
	}
	for _, w := range want {
		if !strings.Contains(resp.Message, w) {
			t.Errorf("%s was refused without naming %q: %s", what, w, resp.Message)
		}
	}
}

// TestUnknownIndexKeyIsRefused is §4 F-2(e): a key the parser does not know
// was ignored, so {field: "Email", unique: true} built a plain index and the
// create reported success. Every unknown key is refused, naming it and the
// keys that are accepted.
func TestUnknownIndexKeyIsRefused(t *testing.T) {
	s := newTestServer(t)
	resp := declareIndexes(t, s, "painted", []interface{}{map[string]interface{}{"field": "Email", "colour": "red"}})
	requireRefused(t, `an index object with the key "colour"`, resp,
		"colour", "fields", "field", "name", "unique", "expire_after_seconds")
}

// TestTTLIndexRefusesUnique (spec §10): a TTL index is not unique — a unique
// index whose documents expire out from under it is a promise that lapses on
// a timer (spec §6.1).
func TestTTLIndexRefusesUnique(t *testing.T) {
	s := newTestServer(t)
	resp := declareIndexes(t, s, "sessions", []interface{}{map[string]interface{}{
		"fields": []interface{}{"CreatedAt"}, "unique": true, "expire_after_seconds": int64(60),
	}})
	requireRefused(t, "a unique TTL index", resp, "unique", "expire_after_seconds")
}

// TestDescendingComponentIsRefused: a descending component matters only to
// index-assisted sort, which is not built, so accepting it would record a
// promise nothing keeps.
func TestDescendingComponentIsRefused(t *testing.T) {
	s := newTestServer(t)
	resp := declareIndexes(t, s, "events", []interface{}{map[string]interface{}{"field": "At", "desc": true}})
	requireRefused(t, "a descending index component", resp, "desc", "sort")
}

// TestRangeTypeIsAcceptedAndWarned: "range" never meant anything — the two
// types were identical on disk — so it is accepted, the type is discarded,
// and one Warn line says so, naming the index.
func TestRangeTypeIsAcceptedAndWarned(t *testing.T) {
	s := newTestServer(t)
	logger, captured := newLogCapture()
	s.logger = logger
	resp := declareIndexes(t, s, "people", []interface{}{
		"Email",
		map[string]interface{}{"field": "Age", "type": "range"},
	})
	if !resp.Success {
		t.Fatalf("a range-typed index was refused: %s", resp.Message)
	}
	if got := captured.count(slog.LevelWarn, "Age_1", "range"); got != 1 {
		t.Errorf("%d Warn lines name the range-typed index Age_1, want 1", got)
	}
	if got := captured.count(slog.LevelWarn, "Email_1"); got != 0 {
		t.Errorf("%d Warn lines name the untyped index Email_1, want 0", got)
	}
	for _, index := range storedCatalogIndices(t, s, "app", "people") {
		if _, written := index["type"]; written {
			t.Errorf("the catalog records the discarded type: %v", index)
		}
	}
}

// TestDuplicateIndexDeclarationIsRefused: two indexes on one field list are an
// ambiguity the planner cannot break, and two indexes with one name make the
// name useless; a field named twice in one index is a mistake. The old parser
// dropped the second silently.
func TestDuplicateIndexDeclarationIsRefused(t *testing.T) {
	s := newTestServer(t)
	requireRefused(t, "two indexes on [Email]", declareIndexes(t, s, "twice", []interface{}{
		"Email", map[string]interface{}{"fields": []interface{}{"Email"}},
	}), "Email", "same fields")
	requireRefused(t, `two indexes named "x"`, declareIndexes(t, s, "named", []interface{}{
		map[string]interface{}{"field": "Email", "name": "x"},
		map[string]interface{}{"field": "Login", "name": "x"},
	}), `"x"`)
	requireRefused(t, "a field named twice in one index", declareIndexes(t, s, "repeat", []interface{}{
		map[string]interface{}{"fields": []interface{}{"Email", "Email"}},
	}), "Email", "twice")
}

// TestIndexDeclarationRefusesADottedPath: an index on a path was
// accepted, read a top-level key literally named "a.b", indexed every document
// as null, and a unique one refused distinct values as duplicates. Every form
// of declaration that names a path is refused with E-17 before anything is
// created.
func TestIndexDeclarationRefusesADottedPath(t *testing.T) {
	for _, declared := range []interface{}{
		"a.b",
		map[string]interface{}{"field": "a.b", "unique": true},
		map[string]interface{}{"fields": []interface{}{"Customer", "Lines[0]"}},
		map[string]interface{}{"field": "Address.Seen", "expire_after_seconds": int64(60)},
	} {
		_, err := parseIndexSpecs(map[string]interface{}{"indices": []interface{}{declared}}, nil)
		if err == nil {
			t.Errorf("the index declaration %v was accepted; an index on a path must be refused", declared)
			continue
		}
		message := err.Error()
		for _, part := range []string{"this release indexes top-level fields only", "Queries on", "by scanning the bucket", "keep a top-level copy"} {
			if !strings.Contains(message, part) {
				t.Errorf("the refusal of %v = %q; want E-17, which says %q", declared, message, part)
			}
		}
	}
}
