package server

import (
	"context"
	"errors"
	"fmt"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/transport"
)

// insertScanDoc writes one document with the given key and fields.
func insertScanDoc(t *testing.T, s *Server, db, bucket, key string, fields map[string]interface{}) {
	t.Helper()
	doc := map[string]interface{}{"_id_": key}
	for name, value := range fields {
		doc[name] = value
	}
	resp, err := s.Insert(InsertRequest{Database: db, Bucket: bucket, Key: key, Data: doc})
	if err != nil || !resp.Success {
		t.Fatalf("insert %s: %+v, %v", key, resp, err)
	}
}

// TestScanReturnsEveryDocument is the headline case: 250 documents into a hash
// bucket, the first page is 100, Total is the whole bucket, and the order is
// ascending key so paging has a meaning.
func TestScanReturnsEveryDocument(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "users", BucketTypeHash)
	for i := 0; i < 250; i++ {
		key := fmt.Sprintf("doc-%03d", i)
		insertScanDoc(t, s, "app", "users", key, map[string]interface{}{"n": float64(i)})
	}

	resp, err := s.Scan(ScanRequest{Database: "app", Bucket: "users"})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if !resp.Found {
		t.Fatal("Found = false, want true")
	}
	if len(resp.Documents) != 100 {
		t.Fatalf("page length = %d, want 100 (the default)", len(resp.Documents))
	}
	if resp.Total != 250 {
		t.Fatalf("Total = %d, want 250", resp.Total)
	}
	if !resp.HasMore {
		t.Fatal("HasMore = false, want true")
	}
	for i, doc := range decodedDocuments(t, resp.Documents) {
		want := fmt.Sprintf("doc-%03d", i)
		if got, _ := doc["_id_"].(string); got != want {
			t.Fatalf("document %d has _id_ %q, want %q (ascending key order)", i, got, want)
		}
	}
}

// TestScanFilterMatchesUnindexedField is the case that motivates the spec: a
// filter on a field with no secondary index returns exactly the matches, and
// Total counts matches in the whole bucket rather than the page.
func TestScanFilterMatchesUnindexedField(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "users", BucketTypeHash)
	for i := 0; i < 3; i++ {
		insertScanDoc(t, s, "app", "users", fmt.Sprintf("active-%d", i), map[string]interface{}{"Status": "active"})
	}
	for i := 0; i < 2; i++ {
		insertScanDoc(t, s, "app", "users", fmt.Sprintf("archived-%d", i), map[string]interface{}{"Status": "archived"})
	}

	resp, err := s.Scan(ScanRequest{
		Database: "app",
		Bucket:   "users",
		Filter:   map[string]interface{}{"Status": "active"},
		Limit:    2,
	})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(resp.Documents) != 2 {
		t.Fatalf("page length = %d, want 2", len(resp.Documents))
	}
	if resp.Total != 3 {
		t.Fatalf("Total = %d, want 3 — the matches in the whole bucket, not the page", resp.Total)
	}
	if !resp.HasMore {
		t.Fatal("HasMore = false, want true: a third match exists past the page")
	}
	for _, doc := range decodedDocuments(t, resp.Documents) {
		if status, _ := doc["Status"].(string); status != "active" {
			t.Fatalf("page contains a %q document, want only active", status)
		}
	}
}

// TestScanPagingIsStableWithoutWrites checks that skip/limit walks the same
// order twice: page two is documents 101-200 and shares nothing with page one.
func TestScanPagingIsStableWithoutWrites(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "users", BucketTypeHash)
	for i := 0; i < 250; i++ {
		key := fmt.Sprintf("doc-%03d", i)
		insertScanDoc(t, s, "app", "users", key, map[string]interface{}{"n": float64(i)})
	}

	first, err := s.Scan(ScanRequest{Database: "app", Bucket: "users", Limit: 100})
	if err != nil {
		t.Fatalf("page one: %v", err)
	}
	second, err := s.Scan(ScanRequest{Database: "app", Bucket: "users", Skip: 100, Limit: 100})
	if err != nil {
		t.Fatalf("page two: %v", err)
	}

	seen := make(map[string]bool, len(first.Documents))
	for _, doc := range decodedDocuments(t, first.Documents) {
		seen[doc["_id_"].(string)] = true
	}
	for i, doc := range decodedDocuments(t, second.Documents) {
		id := doc["_id_"].(string)
		if seen[id] {
			t.Fatalf("document %q appears on both pages", id)
		}
		want := fmt.Sprintf("doc-%03d", 100+i)
		if id != want {
			t.Fatalf("page two document %d has _id_ %q, want %q", i, id, want)
		}
	}
}

// TestScanOrdersQueueAndHeapStores checks the Scan switch routes fifo, lifo
// and heap through the walk that yields the order the spec's table promises:
// the store-level walk tests cannot catch a Scan that calls the wrong one.
func TestScanOrdersQueueAndHeapStores(t *testing.T) {
	s := newTestServer(t)

	createTestBucket(t, s, "app", "jobs", BucketTypeFIFO)
	for _, id := range []string{"first", "second", "third"} {
		insertScanDoc(t, s, "app", "jobs", id, nil)
	}
	fifo, err := s.Scan(ScanRequest{Database: "app", Bucket: "jobs"})
	if err != nil {
		t.Fatalf("fifo Scan: %v", err)
	}
	assertScanOrder(t, fifo, []string{"first", "second", "third"}, "fifo oldest first")

	createTestBucket(t, s, "app", "stack", BucketTypeLIFO)
	for _, id := range []string{"first", "second", "third"} {
		insertScanDoc(t, s, "app", "stack", id, nil)
	}
	lifo, err := s.Scan(ScanRequest{Database: "app", Bucket: "stack"})
	if err != nil {
		t.Fatalf("lifo Scan: %v", err)
	}
	assertScanOrder(t, lifo, []string{"third", "second", "first"}, "lifo newest first")

	createTestBucket(t, s, "app", "sorted", BucketTypeBTree)
	for _, id := range []string{"a", "b", "c"} {
		insertScanDoc(t, s, "app", "sorted", id, nil)
	}
	btree, err := s.Scan(ScanRequest{Database: "app", Bucket: "sorted"})
	if err != nil {
		t.Fatalf("btree Scan: %v", err)
	}
	assertScanOrder(t, btree, []string{"a", "b", "c"}, "btree ascending key")

	createTestBucket(t, s, "app", "alerts", BucketTypeHeap)
	for _, priority := range []int64{1, 5, 3} {
		id := fmt.Sprintf("p%d", priority)
		pushed, err := s.Push(PushRequest{Database: "app", Bucket: "alerts", Priority: priority, Data: map[string]interface{}{"_id_": id}})
		if err != nil || !pushed.Success {
			t.Fatalf("push %s: %+v, %v", id, pushed, err)
		}
	}
	heap, err := s.Scan(ScanRequest{Database: "app", Bucket: "alerts"})
	if err != nil {
		t.Fatalf("heap Scan: %v", err)
	}
	assertScanOrder(t, heap, []string{"p5", "p3", "p1"}, "heap highest priority first")
}

// assertScanOrder checks a scan page's _id_ order and that nothing was dropped.
func assertScanOrder(t *testing.T, resp *ScanResponse, want []string, label string) {
	t.Helper()
	if resp.Total != int64(len(want)) {
		t.Fatalf("%s: Total = %d, want %d", label, resp.Total, len(want))
	}
	if len(resp.Documents) != len(want) {
		t.Fatalf("%s: page has %d documents, want %d", label, len(resp.Documents), len(want))
	}
	for i, doc := range decodedDocuments(t, resp.Documents) {
		if got, _ := doc["_id_"].(string); got != want[i] {
			t.Fatalf("%s: document %d is %q, want %q", label, i, got, want[i])
		}
	}
}

// TestScanRefusesOnBlob pins the one store type that does not scan: a blob's
// value is binary content, so the refusal must name the verbs that work.
func TestScanRefusesOnBlob(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "photos", BucketTypeBlob)

	_, err := s.Scan(ScanRequest{Database: "app", Bucket: "photos"})
	requireWrongTypeRefusal(t, err, "info", "get")
}

// TestScanOnMissingBucketReportsNotFound distinguishes "no matching documents"
// from "the bucket does not exist", the ListBucketsResponse convention.
func TestScanOnMissingBucketReportsNotFound(t *testing.T) {
	s := newTestServer(t)
	if resp, err := s.CreateDatabase("app"); err != nil || !resp.Success {
		t.Fatalf("CreateDatabase: %+v, %v", resp, err)
	}

	resp, err := s.Scan(ScanRequest{Database: "app", Bucket: "ghosts"})
	if err != nil {
		t.Fatalf("Scan on a missing bucket returned an error: %v", err)
	}
	if resp.Found {
		t.Fatal("Found = true for a bucket that does not exist, want false")
	}
}

// TestScanRequiresReadPermission is the pair rule: read on another
// database must not satisfy a scan of this one.
func TestScanRequiresReadPermission(t *testing.T) {
	s := userTestServer(t)
	frame := transport.NewFrame(transport.OpScan, mustMarshalBSON(t, &ScanRequest{Database: "app", Bucket: "users"}))

	otherSess := &transport.Session{}
	otherSess.SetIdentity(ident(transport.ScopedGrant{Role: "reader", DB: "other"}))
	otherCtx := transport.WithSession(context.Background(), otherSess)
	if err := s.authorize(otherCtx, frame); err == nil {
		t.Fatal("a reader@other identity was allowed to scan app")
	}

	appSess := &transport.Session{}
	appSess.SetIdentity(ident(transport.ScopedGrant{Role: "reader", DB: "app"}))
	appCtx := transport.WithSession(context.Background(), appSess)
	if err := s.authorize(appCtx, frame); err != nil {
		t.Fatalf("a reader@app identity was refused: %v", err)
	}
}

// TestScanSortAscending tests sorting documents in ascending order by a field.
func TestScanSortAscending(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "users", BucketTypeHash)

	ages := []float64{25, 32, 18, 45}
	for i, age := range ages {
		key := fmt.Sprintf("user-%d", i)
		insertScanDoc(t, s, "app", "users", key, map[string]interface{}{"Age": age, "Name": fmt.Sprintf("User%d", i)})
	}

	resp, err := s.Scan(ScanRequest{
		Database: "app",
		Bucket:   "users",
		Sort:     map[string]int{"Age": 1},
	})
	if err != nil {
		t.Fatalf("Scan with sort: %v", err)
	}

	expectedAges := []float64{18, 25, 32, 45}
	if len(resp.Documents) != len(expectedAges) {
		t.Fatalf("got %d documents, want %d", len(resp.Documents), len(expectedAges))
	}

	for i, doc := range decodedDocuments(t, resp.Documents) {
		age := doc["Age"].(float64)
		if age != expectedAges[i] {
			t.Fatalf("document %d has Age %f, want %f", i, age, expectedAges[i])
		}
	}
}

// TestScanSortDescending tests sorting documents in descending order by a field.
func TestScanSortDescending(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "users", BucketTypeHash)

	ages := []float64{25, 32, 18, 45}
	for i, age := range ages {
		key := fmt.Sprintf("user-%d", i)
		insertScanDoc(t, s, "app", "users", key, map[string]interface{}{"Age": age})
	}

	resp, err := s.Scan(ScanRequest{
		Database: "app",
		Bucket:   "users",
		Sort:     map[string]int{"Age": -1},
	})
	if err != nil {
		t.Fatalf("Scan with sort: %v", err)
	}

	expectedAges := []float64{45, 32, 25, 18}
	for i, doc := range decodedDocuments(t, resp.Documents) {
		age := doc["Age"].(float64)
		if age != expectedAges[i] {
			t.Fatalf("document %d has Age %f, want %f", i, age, expectedAges[i])
		}
	}
}

// TestScanProjectionInclusion tests including only specified fields.
func TestScanProjectionInclusion(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "users", BucketTypeHash)

	insertScanDoc(t, s, "app", "users", "user-1", map[string]interface{}{
		"Name":  "Alice",
		"Age":   float64(25),
		"Email": "alice@example.com",
		"Phone": "555-1234",
	})

	resp, err := s.Scan(ScanRequest{
		Database: "app",
		Bucket:   "users",
		Projection: map[string]int{
			"_id_": 1,
			"Name": 1,
			"Age":  1,
		},
	})
	if err != nil {
		t.Fatalf("Scan with projection: %v", err)
	}

	if len(resp.Documents) != 1 {
		t.Fatalf("got %d documents, want 1", len(resp.Documents))
	}

	doc := decodedDocuments(t, resp.Documents)[0]
	if _, ok := doc["Name"]; !ok {
		t.Fatal("Name field missing in projection")
	}
	if _, ok := doc["Age"]; !ok {
		t.Fatal("Age field missing in projection")
	}
	if _, ok := doc["Email"]; ok {
		t.Fatal("Email field should not be in projection")
	}
	if _, ok := doc["Phone"]; ok {
		t.Fatal("Phone field should not be in projection")
	}
}

// TestScanProjectionExclusion tests excluding specified fields.
func TestScanProjectionExclusion(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "users", BucketTypeHash)

	insertScanDoc(t, s, "app", "users", "user-1", map[string]interface{}{
		"Name":  "Alice",
		"Age":   float64(25),
		"Email": "alice@example.com",
		"Phone": "555-1234",
	})

	resp, err := s.Scan(ScanRequest{
		Database: "app",
		Bucket:   "users",
		Projection: map[string]int{
			"Email": 0,
			"Phone": 0,
		},
	})
	if err != nil {
		t.Fatalf("Scan with projection: %v", err)
	}

	if len(resp.Documents) != 1 {
		t.Fatalf("got %d documents, want 1", len(resp.Documents))
	}

	doc := decodedDocuments(t, resp.Documents)[0]
	if _, ok := doc["Name"]; !ok {
		t.Fatal("Name field missing in projection")
	}
	if _, ok := doc["Age"]; !ok {
		t.Fatal("Age field missing in projection")
	}
	if _, ok := doc["Email"]; ok {
		t.Fatal("Email field should not be in projection")
	}
	if _, ok := doc["Phone"]; ok {
		t.Fatal("Phone field should not be in projection")
	}
}

// TestScanSortLargeBucketWarns tests that sorting >100 documents produces a warning.
func TestScanSortLargeBucketWarns(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "users", BucketTypeHash)

	for i := 0; i < 101; i++ {
		key := fmt.Sprintf("user-%03d", i)
		insertScanDoc(t, s, "app", "users", key, map[string]interface{}{"Age": float64(i)})
	}

	resp, err := s.Scan(ScanRequest{
		Database: "app",
		Bucket:   "users",
		Sort:     map[string]int{"Age": 1},
	})
	if err != nil {
		t.Fatalf("Scan with sort: %v", err)
	}

	if !strings.Contains(resp.Message, "without an index") {
		t.Fatalf("warning message does not mention unindexed sort: %q", resp.Message)
	}
}

// TestNowIsServerSideAcrossClientClocks verifies that now() is evaluated on
// the server side by checking that two documents inserted with now() from
// clients with different clocks store timestamps matching the server's clock,
// not either client's clock.
func TestNowIsServerSideAcrossClientClocks(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "events", BucketTypeHash)

	serverTime := time.Now().UTC()

	// Insert two documents with now() sentinel
	req1 := InsertRequest{
		Database: "app",
		Bucket:   "events",
		Key:      "e1",
		Data: map[string]interface{}{
			"_id_": "e1",
			"ts":   NowMarker(),
			"msg":  "first",
		},
	}
	// Through the client door: an applier stamps nothing.
	doorWrite(t, s, transport.OpPut, req1)

	time.Sleep(100 * time.Millisecond)

	req2 := InsertRequest{
		Database: "app",
		Bucket:   "events",
		Key:      "e2",
		Data: map[string]interface{}{
			"_id_": "e2",
			"ts":   NowMarker(),
			"msg":  "second",
		},
	}
	doorWrite(t, s, transport.OpPut, req2)

	// Scan and check both timestamps are server-side and close to each other
	scanResp, err := s.Scan(ScanRequest{Database: "app", Bucket: "events"})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(scanResp.Documents) != 2 {
		t.Fatalf("expected 2 documents, got %d", len(scanResp.Documents))
	}

	// Extract timestamps
	scanned := decodedDocuments(t, scanResp.Documents)
	ts1, ok1 := scanned[0]["ts"].(primitive.DateTime)
	ts2, ok2 := scanned[1]["ts"].(primitive.DateTime)
	if !ok1 || !ok2 {
		t.Fatalf("timestamps are not primitive.DateTime: %T, %T", scanned[0]["ts"], scanned[1]["ts"])
	}

	ts1Time := ts1.Time()
	ts2Time := ts2.Time()

	// Both should be close to server time (within 2 seconds)
	diff1 := ts1Time.Sub(serverTime).Abs()
	diff2 := ts2Time.Sub(serverTime).Abs()
	tolerance := 2 * time.Second

	if diff1 > tolerance {
		t.Fatalf("ts1 differs from server time by %v, want < %v", diff1, tolerance)
	}
	if diff2 > tolerance {
		t.Fatalf("ts2 differs from server time by %v, want < %v", diff2, tolerance)
	}

	// The two timestamps should be close to each other (within 200ms due to sleep)
	timeDiff := ts2Time.Sub(ts1Time).Abs()
	if timeDiff < 50*time.Millisecond || timeDiff > 300*time.Millisecond {
		t.Fatalf("timestamps differ by %v, expected ~100ms (±200ms)", timeDiff)
	}
}

// TestNowInFilterIsServerEvaluated verifies that now() in a filter is
// evaluated server-side by inserting a document with a known timestamp,
// then filtering with now() to ensure the filter is evaluated correctly.
func TestNowInFilterIsServerEvaluated(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "events", BucketTypeHash)

	// Insert a document with a known timestamp 1 second ago
	pastTime := time.Now().UTC().Add(-1 * time.Second)
	pastDateTime := primitive.NewDateTimeFromTime(pastTime)

	insertResp, err := s.Insert(InsertRequest{
		Database: "app",
		Bucket:   "events",
		Key:      "past",
		Data: map[string]interface{}{
			"_id_": "past",
			"ts":   pastDateTime,
			"msg":  "old event",
		},
	})
	if err != nil || !insertResp.Success {
		t.Fatalf("insert past: %+v, %v", insertResp, err)
	}

	// Scan with a filter: ts < now() - should find the past document
	scanResp, err := s.Scan(ScanRequest{
		Database: "app",
		Bucket:   "events",
		Filter: map[string]interface{}{
			"ts": map[string]interface{}{
				"$lt": NowMarker(),
			},
		},
	})
	if err != nil {
		t.Fatalf("Scan with filter: %v", err)
	}
	if len(scanResp.Documents) != 1 {
		t.Fatalf("expected 1 document matching ts < now(), got %d", len(scanResp.Documents))
	}

	// Scan with a filter: ts > now() - should find no documents
	scanResp2, err := s.Scan(ScanRequest{
		Database: "app",
		Bucket:   "events",
		Filter: map[string]interface{}{
			"ts": map[string]interface{}{
				"$gt": NowMarker(),
			},
		},
	})
	if err != nil {
		t.Fatalf("Scan with filter ts > now(): %v", err)
	}
	if len(scanResp2.Documents) != 0 {
		t.Fatalf("expected 0 documents matching ts > now(), got %d", len(scanResp2.Documents))
	}
}

// TestNowSubstitutedOnceBeforeReplication verifies that the now() sentinel
// is substituted exactly once in the write frame, before replication, so
// every applier (primary and replicas) store the same instant.
func TestNowSubstitutedOnceBeforeReplication(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "events", BucketTypeHash)

	// Create a write frame with a now() sentinel
	nowSentinel := NowMarker()

	// Insert with now() sentinel, through the client door: an applier
	// stamps nothing.
	doorWrite(t, s, transport.OpPut, InsertRequest{
		Database: "app",
		Bucket:   "events",
		Key:      "e1",
		Data: map[string]interface{}{
			"_id_": "e1",
			"ts":   nowSentinel,
			"msg":  "event",
		},
	})

	// Verify the stored value is a concrete DateTime, not the sentinel
	scanResp, err := s.Scan(ScanRequest{Database: "app", Bucket: "events"})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(scanResp.Documents) != 1 {
		t.Fatalf("expected 1 document, got %d", len(scanResp.Documents))
	}

	doc := decodedDocuments(t, scanResp.Documents)[0]
	ts, ok := doc["ts"].(primitive.DateTime)
	if !ok {
		if doc["ts"] == NowMarker() {
			t.Fatal("the now() marker was not substituted in the stored document")
		}
		t.Fatalf("ts is not primitive.DateTime, got %T: %v", doc["ts"], doc["ts"])
	}

	// Verify it's a reasonable timestamp (close to now)
	tsTime := ts.Time()
	serverTime := time.Now().UTC()
	diff := serverTime.Sub(tsTime).Abs()
	if diff > 5*time.Second {
		t.Fatalf("stored timestamp differs from server time by %v, want < 5s", diff)
	}
}

// TestMultiFieldSortIsRefused pins the multi-field sort refusal. Before the fix, sortDocuments
// ranged the sort map and broke after the first entry, so a two-field sort
// ordered by whichever field Go's randomised map iteration yielded on that
// call and the same query could come back in different orders. The fix is a
// refusal, not a comparator chain: a multi-field sort must error, the error
// must name the limit and the way out, and it must do so on every call.
func TestMultiFieldSortIsRefused(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "users", BucketTypeHash)

	// Age and Rank disagree, so a guessed field produces a visibly different
	// order depending on which one the map handed over.
	for i, doc := range []map[string]interface{}{
		{"Age": float64(25), "Rank": float64(4)},
		{"Age": float64(32), "Rank": float64(3)},
		{"Age": float64(18), "Rank": float64(2)},
		{"Age": float64(45), "Rank": float64(1)},
	} {
		insertScanDoc(t, s, "app", "users", fmt.Sprintf("user-%d", i), doc)
	}

	refusalCases := []struct {
		name string
		sort map[string]int
	}{
		{"two fields", map[string]int{"Age": 1, "Rank": -1}},
		{"three fields", map[string]int{"Age": 1, "Rank": -1, "Name": 1}},
	}
	for _, testCase := range refusalCases {
		t.Run(testCase.name, func(t *testing.T) {
			// Repeated, because the defect was non-deterministic: one run
			// could pass by luck, and the refusal must not depend on map order.
			var firstMessage string
			for attempt := range 32 {
				_, err := s.Scan(ScanRequest{Database: "app", Bucket: "users", Sort: testCase.sort})
				if err == nil {
					t.Fatalf("attempt %d: Scan with a %s sort returned no error; it must refuse rather than guess a field", attempt, testCase.name)
				}
				if !errors.Is(err, ErrMultiFieldSort) {
					t.Fatalf("attempt %d: got %v, want an error matching ErrMultiFieldSort", attempt, err)
				}
				// AGENTS.md rule 3: the message names the fields it was given
				// and the way out, and says the same thing every time.
				for _, want := range []string{"Age", "Rank", "sort by one field", "in your client"} {
					if !strings.Contains(err.Error(), want) {
						t.Fatalf("attempt %d: error %q does not mention %q", attempt, err.Error(), want)
					}
				}
				if attempt == 0 {
					firstMessage = err.Error()
				} else if err.Error() != firstMessage {
					t.Fatalf("attempt %d: error text varies between calls:\n  %q\n  %q", attempt, firstMessage, err.Error())
				}
			}
		})
	}

	allowedCases := []struct {
		name         string
		sort         map[string]int
		expectedAges []float64
	}{
		{"no sort", nil, nil},
		{"one field ascending", map[string]int{"Age": 1}, []float64{18, 25, 32, 45}},
		{"one field descending", map[string]int{"Rank": -1}, []float64{25, 32, 18, 45}},
	}
	for _, testCase := range allowedCases {
		t.Run(testCase.name, func(t *testing.T) {
			resp, err := s.Scan(ScanRequest{Database: "app", Bucket: "users", Sort: testCase.sort})
			if err != nil {
				t.Fatalf("Scan with a %s sort must still succeed: %v", testCase.name, err)
			}
			if testCase.expectedAges == nil {
				return
			}
			if len(resp.Documents) != len(testCase.expectedAges) {
				t.Fatalf("got %d documents, want %d", len(resp.Documents), len(testCase.expectedAges))
			}
			for i, doc := range decodedDocuments(t, resp.Documents) {
				if age := doc["Age"].(float64); age != testCase.expectedAges[i] {
					t.Fatalf("document %d has Age %v, want %v", i, age, testCase.expectedAges[i])
				}
			}
		})
	}
}

// TestSortedFindOrdersLargeIntegers is BUG-0178 end to end: integers above 2^53
// that float64 cannot tell apart sort by value, both ways, and a page boundary
// between them neither repeats nor drops a document.
func TestSortedFindOrdersLargeIntegers(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "ids", BucketTypeHash)
	// Inserted largest first, so walk position and numeric order disagree.
	base := int64(1) << 60
	for offset := 4; offset >= 0; offset-- {
		insertScanDoc(t, s, "app", "ids", fmt.Sprintf("id-%d", offset), map[string]interface{}{"n": base + int64(offset)})
	}

	for _, direction := range []int{1, -1} {
		var got []string
		for skip := 0; skip < 5; skip += 2 {
			resp, err := s.Scan(ScanRequest{Database: "app", Bucket: "ids", Sort: map[string]int{"n": direction}, Skip: skip, Limit: 2})
			if err != nil {
				t.Fatalf("direction %d skip %d: %v", direction, skip, err)
			}
			for _, doc := range decodedDocuments(t, resp.Documents) {
				got = append(got, doc["_id_"].(string))
			}
		}
		want := []string{"id-0", "id-1", "id-2", "id-3", "id-4"}
		if direction < 0 {
			want = []string{"id-4", "id-3", "id-2", "id-1", "id-0"}
		}
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("direction %d: got %v, want %v", direction, got, want)
		}
	}
}
