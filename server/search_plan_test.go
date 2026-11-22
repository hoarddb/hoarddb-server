package server

import (
	"errors"
	"log/slog"
	"sort"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/storage/kvfault"
	"github.com/hoarddb/hoarddb-server/store"
)

// Compound indexes and the planner (GAP-14 build doc Steps 11–13).

// fullScan is what IndexUsed says when no index served a query.
const fullScan = "none — full scan"

// indexUsedOf reads a response's index_used field as the wire carries it, so
// the test states the contract a client reads.
func indexUsedOf(t *testing.T, response interface{}) string {
	t.Helper()
	encoded, err := bson.Marshal(response)
	if err != nil {
		t.Fatalf("encoding the response: %v", err)
	}
	value, err := bson.Raw(encoded).LookupErr("index_used")
	if err != nil {
		return ""
	}
	used, _ := value.StringValueOK()
	return used
}

// searchKeys runs a search and returns the matched documents' keys, sorted,
// and the index that served it.
func searchKeys(t *testing.T, s *Server, bucket string, filter map[string]interface{}) ([]string, string) {
	t.Helper()
	resp, err := s.Search(SearchRequest{Database: "app", Bucket: bucket, Filter: filter})
	if err != nil {
		t.Fatalf("search(%v): %v", filter, err)
	}
	if resp.Message != "" {
		t.Fatalf("search(%v) answered %q", filter, resp.Message)
	}
	return documentKeysOf(t, resp.Results), indexUsedOf(t, resp)
}

// scanKeys runs find with the same filter and returns the matched keys, sorted.
func scanKeys(t *testing.T, s *Server, bucket string, filter map[string]interface{}) []string {
	t.Helper()
	resp, err := s.Scan(ScanRequest{Database: "app", Bucket: bucket, Filter: filter, Limit: 1000})
	if err != nil {
		t.Fatalf("find(%v): %v", filter, err)
	}
	return documentKeysOf(t, resp.Documents)
}

func documentKeysOf(t *testing.T, documents []bson.Raw) []string {
	t.Helper()
	keys := make([]string, 0, len(documents))
	for _, document := range documents {
		key, _ := document.Lookup("_id_").StringValueOK()
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// seedABC creates app/abc with a compound index on (A, B, C) and five
// documents, and returns the server.
func seedABC(t *testing.T, s *Server, indices ...interface{}) {
	t.Helper()
	createUniqueBucket(t, s, "app", "abc", indices...)
	for key, doc := range map[string]map[string]interface{}{
		"k1": {"A": "a1", "B": "b1", "C": "c1"},
		"k2": {"A": "a1", "B": "b1", "C": "c2"},
		"k3": {"A": "a1", "B": "b2", "C": "c1"},
		"k4": {"A": "a2", "B": "b1", "C": "c1"},
		"k5": {"A": "a2", "B": "b2"},
	} {
		mustInsert(t, s, "app", "abc", key, doc)
	}
}

// TestCompoundPrefixQueryUsesTheIndex (spec §10): a query on a prefix of a
// compound index's fields uses it, and answers what find answers.
func TestCompoundPrefixQueryUsesTheIndex(t *testing.T) {
	s := newTestServer(t)
	seedABC(t, s, map[string]interface{}{"fields": []interface{}{"A", "B", "C"}})
	for _, filter := range []map[string]interface{}{
		{"A": "a1"},
		{"A": "a1", "B": "b1"},
		{"A": "a1", "B": "b1", "C": "c2"},
	} {
		keys, used := searchKeys(t, s, "abc", filter)
		if used != "A_1_B_1_C_1" {
			t.Errorf("search(%v) was served by %q, want A_1_B_1_C_1", filter, used)
		}
		if want := scanKeys(t, s, "abc", filter); strings.Join(keys, ",") != strings.Join(want, ",") {
			t.Errorf("search(%v) = %v, find = %v", filter, keys, want)
		}
	}
}

// TestCompoundNonPrefixQueryIsRefusedOrScans (spec §10; S-8, recommendation
// taken: scan): a query that is not a prefix never uses the index; it is
// answered by a scan that says so, and the answer equals find's.
func TestCompoundNonPrefixQueryIsRefusedOrScans(t *testing.T) {
	s := newTestServer(t)
	seedABC(t, s, map[string]interface{}{"fields": []interface{}{"A", "B", "C"}})
	for _, filter := range []map[string]interface{}{{"B": "b1"}, {"C": "c1"}, {"B": "b2", "C": "c1"}} {
		keys, used := searchKeys(t, s, "abc", filter)
		if used != fullScan {
			t.Errorf("search(%v) was served by %q, want %q", filter, used, fullScan)
		}
		if want := scanKeys(t, s, "abc", filter); strings.Join(keys, ",") != strings.Join(want, ",") {
			t.Errorf("search(%v) = %v, find = %v", filter, keys, want)
		}
	}
}

// TestPlannerTieBreakIsDeterministic: two equally good indexes — the lower id
// wins, every time and after a restart. Picking in map order would pass most
// runs, which is why this runs a hundred.
func TestPlannerTieBreakIsDeterministic(t *testing.T) {
	dataDir := t.TempDir()
	first := startCatalogServerAt(t, dataDir)
	seedABC(t, first.core,
		map[string]interface{}{"fields": []interface{}{"A", "B"}},
		map[string]interface{}{"fields": []interface{}{"A", "C"}},
	)
	check := func(stage string, s *Server) {
		t.Helper()
		for run := 0; run < 100; run++ {
			if _, used := searchKeys(t, s, "abc", map[string]interface{}{"A": "a1"}); used != "A_1_B_1" {
				t.Fatalf("%s, run %d: the tie went to %q, want A_1_B_1 (the lower id)", stage, run, used)
			}
		}
	}
	check("before a restart", first.core)
	check("after a restart", restartCatalog(t, first, dataDir).core)
}

// TestCompoundUniqueRefusesSecondDocument: unique over a compound key refuses
// the second document with the same pair, and takes one that shares only a
// prefix of it.
func TestCompoundUniqueRefusesSecondDocument(t *testing.T) {
	s := newTestServer(t)
	createUniqueBucket(t, s, "app", "orders", map[string]interface{}{"fields": []interface{}{"CustomerID", "Day"}, "unique": true})
	mustInsert(t, s, "app", "orders", "o1", map[string]interface{}{"CustomerID": "c1", "Day": "mon"})
	mustInsert(t, s, "app", "orders", "o2", map[string]interface{}{"CustomerID": "c1", "Day": "tue"})
	refusal := insertOutcome(t, s, "app", "orders", "o3", map[string]interface{}{"CustomerID": "c1", "Day": "mon"})
	if refusal == "" || !strings.Contains(refusal, `(CustomerID, Day) = ("c1", "mon")`) || !strings.Contains(refusal, `"o1"`) {
		t.Fatalf("a duplicate compound key answered %q, want a refusal naming (CustomerID, Day) = (\"c1\", \"mon\") and o1", refusal)
	}
}

// TestPlannerNeverChoosesAConflictedIndex: a conflicted unique index on Email
// would win the tie with a compound index on (Email, Name) — it is unique —
// but it is never chosen; the compound index serves, and the answer includes
// both holders of the duplicate.
func TestPlannerNeverChoosesAConflictedIndex(t *testing.T) {
	dataDir := t.TempDir()
	first := seedConflictedUnique(t, dataDir)
	reopened := startCatalogServerAt(t, dataDir)
	writeCatalogIndices(t, reopened.core, "app", "users", []bson.M{
		{"id": int32(0), "name": "Email_1", "fields": bson.A{"Email"}, "unique": true},
		{"id": int32(1), "name": "Email_1_Name_1", "fields": bson.A{"Email", "Name"}},
	})
	_ = first
	s := restartCatalog(t, reopened, dataDir).core
	if stored := storedCatalogIndices(t, s, "app", "users"); stored[0]["state"] != "conflicted" {
		t.Fatalf("precondition: the unique index is %v, want conflicted", stored[0])
	}
	keys, used := searchKeys(t, s, "users", map[string]interface{}{"Email": "same@example.com"})
	if used != "Email_1_Name_1" || strings.Join(keys, ",") != "dup-a,dup-b" {
		t.Fatalf("search served by %q returned %v, want Email_1_Name_1 and both holders", used, keys)
	}
}

// plantEntry writes an index entry of index 0 for value under document key,
// raw, as a bug or a crash between two writers could leave one.
func plantEntry(t *testing.T, s *Server, bucket *inMemoryBucket, indexID uint16, values []interface{}, key string) {
	t.Helper()
	encoded, err := store.EncodeKey(values)
	if err != nil {
		t.Fatalf("EncodeKey: %v", err)
	}
	batch := s.kv.NewBatch()
	defer batch.Discard()
	if err := batch.Set(append(append(indexEntryPrefix(bucket, indexID), encoded...), key...), []byte{}); err != nil {
		t.Fatalf("planting: %v", err)
	}
	if err := batch.Commit(false); err != nil {
		t.Fatalf("committing the plant: %v", err)
	}
}

// TestResidualFilterReAppliesCoveredPredicates (spec §10; guard 3): an entry
// for a value the document no longer carries is not an answer. Every
// predicate is re-applied to the document, the ones the index covers too.
func TestResidualFilterReAppliesCoveredPredicates(t *testing.T) {
	s := newTestServer(t)
	createUniqueBucket(t, s, "app", "users", "Email")
	mustInsert(t, s, "app", "users", "u1", map[string]interface{}{"Email": "new@example.com"})
	plantEntry(t, s, servedBucket(t, s, "app", "users"), 0, []interface{}{"old@example.com"}, "u1")
	keys, used := searchKeys(t, s, "users", map[string]interface{}{"Email": "old@example.com"})
	if used != "Email_1" || len(keys) != 0 {
		t.Fatalf("search for a value no document holds, served by %q, returned %v: the stale entry was served", used, keys)
	}
}

// TestStaleEntryIsNeverServed is the failing test for stale index entries, as a planted
// entry: whatever left it — a bug, a crash, a future writer that forgot the
// index — search answers what the documents say, on a compound index too.
func TestStaleEntryIsNeverServed(t *testing.T) {
	s := newTestServer(t)
	createUniqueBucket(t, s, "app", "orders", map[string]interface{}{"fields": []interface{}{"CustomerID", "Day"}})
	mustInsert(t, s, "app", "orders", "o1", map[string]interface{}{"CustomerID": "c1", "Day": "tue"})
	plantEntry(t, s, servedBucket(t, s, "app", "orders"), 0, []interface{}{"c1", "mon"}, "o1")
	for _, filter := range []map[string]interface{}{{"CustomerID": "c1", "Day": "mon"}, {"CustomerID": "c1"}} {
		keys, _ := searchKeys(t, s, "orders", filter)
		if want := scanKeys(t, s, "orders", filter); strings.Join(keys, ",") != strings.Join(want, ",") {
			t.Errorf("search(%v) = %v, find = %v: a stale entry was served", filter, keys, want)
		}
	}
}

// TestOrphanIndexEntryIsCountedAndLoggedOnce (spec §10, §6.5): an entry whose
// document does not resolve is skipped — never repaired on the read path —
// counted every time, and logged once per bucket per start, naming the index
// and the document key.
func TestOrphanIndexEntryIsCountedAndLoggedOnce(t *testing.T) {
	s := newTestServer(t)
	logger, captured := newLogCapture()
	s.logger = logger
	createUniqueBucket(t, s, "app", "users", "Email")
	mustInsert(t, s, "app", "users", "u1", map[string]interface{}{"Email": "a@example.com"})
	plantEntry(t, s, servedBucket(t, s, "app", "users"), 0, []interface{}{"a@example.com"}, "ghost")
	const counter = "hoarddb_index_orphan_skips_total"
	before := gatheredCounter(t, counter)
	for i := 0; i < 2; i++ {
		if keys, _ := searchKeys(t, s, "users", map[string]interface{}{"Email": "a@example.com"}); strings.Join(keys, ",") != "u1" {
			t.Fatalf("search returned %v, want u1 alone", keys)
		}
	}
	if moved := gatheredCounter(t, counter) - before; moved != 2 {
		t.Errorf("%s moved by %v over two searches, want 2", counter, moved)
	}
	if logged := captured.count(slog.LevelWarn, "Email_1", "ghost"); logged != 1 {
		t.Errorf("%d Warn lines name the orphan, want 1 per bucket per start", logged)
	}
}

// failingReads is a store whose snapshots fail a read of any key containing
// the armed marker: the read error a disk gives.
type failingReads struct {
	storage.KV
	marker *atomic.Pointer[string]
}

func (f failingReads) NewSnapshot() storage.KVSnapshot {
	return failingSnapshot{KVSnapshot: f.KV.NewSnapshot(), marker: f.marker}
}

type failingSnapshot struct {
	storage.KVSnapshot
	marker *atomic.Pointer[string]
}

func (f failingSnapshot) Get(key []byte) ([]byte, error) {
	if marker := f.marker.Load(); marker != nil && strings.Contains(string(key), *marker) {
		return nil, errors.New("injected read failure")
	}
	return f.KVSnapshot.Get(key)
}

// TestSearchReturnsAStoreReadError (C-20): a document the store fails to read
// was skipped as if it did not exist, and the search answered fewer results
// with no error. The error is returned.
func TestSearchReturnsAStoreReadError(t *testing.T) {
	marker := &atomic.Pointer[string]{}
	s := newKVFaultServer(t.TempDir(), failingReads{KV: kvfault.New(), marker: marker})
	createUniqueBucket(t, s, "app", "users", "Email")
	mustInsert(t, s, "app", "users", "unreadable-doc", map[string]interface{}{"Email": "a@example.com"})
	armed := "unreadable-doc"
	marker.Store(&armed)
	resp, err := s.Search(SearchRequest{Database: "app", Bucket: "users", Filter: map[string]interface{}{"Email": "a@example.com"}})
	if err == nil {
		t.Fatalf("a search whose hit could not be read answered %+v and no error", resp)
	}
}

// TestSearchResponseNamesTheIndexUsed (spec §10, GAP-27): a prefix query names
// the compound index that served it; a non-prefix query, and a query on a
// field no index covers, name the full scan.
func TestSearchResponseNamesTheIndexUsed(t *testing.T) {
	s := newTestServer(t)
	seedABC(t, s, map[string]interface{}{"fields": []interface{}{"A", "B", "C"}})
	for filter, want := range map[string]string{"A": "A_1_B_1_C_1", "B": fullScan, "Unindexed": fullScan} {
		if _, used := searchKeys(t, s, "abc", map[string]interface{}{filter: "x"}); used != want {
			t.Errorf("search on %s names %q, want %q", filter, used, want)
		}
	}
}

// TestFindResponseNamesNoIndex (RD-9): find has no index path in 1.0, so it
// always says it scanned — the true answer, and the one that tells a developer
// why find is slower than search.
func TestFindResponseNamesNoIndex(t *testing.T) {
	s := newTestServer(t)
	seedABC(t, s, map[string]interface{}{"fields": []interface{}{"A", "B", "C"}})
	for _, request := range []ScanRequest{
		{Database: "app", Bucket: "abc"},
		{Database: "app", Bucket: "abc", Filter: map[string]interface{}{"A": "a1"}},
		{Database: "app", Bucket: "abc", Filter: map[string]interface{}{"A": "a1"}, Sort: map[string]int{"C": 1}},
	} {
		resp, err := s.Scan(request)
		if err != nil {
			t.Fatalf("find(%v): %v", request.Filter, err)
		}
		if used := indexUsedOf(t, resp); used != fullScan {
			t.Errorf("find(%v, sort %v) names %q, want %q", request.Filter, request.Sort, used, fullScan)
		}
	}
}
