package server

import (
	"bytes"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"
)

// scanBandWidth is the number of documents the Scan and Search benchmarks
// return per call: the same 100-document band the range benchmark uses,
// which is also Scan's default page size.
const scanBandWidth = 100

// scanBandGroup is the one value every seeded document carries in its indexed
// "group" field, so a Search for it hits the whole band.
const scanBandGroup = "band"

// seedIndexedBand creates a bucket of the given type — with an index on
// "group" when it is a hash bucket, the one engine that maintains one — and inserts count
// documents keyed k0000000.., shaped like seedRangeBand's documents (a
// sequence number, a ~100-byte payload and a label) plus the indexed group
// field. One bucket serves both the unqualified Scan and the Search
// measurement.
func seedIndexedBand(tb testing.TB, s *Server, database, bucket string, bucketType BucketType, count int) {
	tb.Helper()
	if resp, err := s.CreateDatabase(database); err != nil || (!resp.Success && !strings.Contains(resp.Message, "already exists")) {
		tb.Fatalf("CreateDatabase(%q) = %+v, %v", database, resp, err)
	}
	// Only a hash bucket maintains a secondary index; any other engine's
	// create refuses one, so the band is indexed where it can be.
	var options map[string]interface{}
	if bucketType == BucketTypeHash {
		options = map[string]interface{}{"indices": []interface{}{"group"}}
	}
	created, err := s.CreateBucket(CreateBucketRequest{
		Database: database,
		Name:     bucket,
		Type:     bucketType,
		Options:  options,
	})
	if err != nil || !created.Success {
		tb.Fatalf("CreateBucket(%q) = %+v, %v", bucket, created, err)
	}
	payload := strings.Repeat("abcdefghij", 10)
	for seq := 0; seq < count; seq++ {
		key := fmt.Sprintf("k%07d", seq)
		inserted, err := s.Insert(InsertRequest{
			Database: database,
			Bucket:   bucket,
			Key:      key,
			Data:     map[string]interface{}{"seq": int32(seq), "payload": payload, "label": key, "group": scanBandGroup},
		})
		if err != nil || !inserted.Success {
			tb.Fatalf("Insert(%q) = %+v, %v", key, inserted, err)
		}
	}
}

// BenchmarkScanNoQualifiers measures an unqualified find() — Scan with no
// filter, sort or projection — as handleScan performs it: Scan, then the BSON
// marshal onto the wire, over a 100-document band.
func BenchmarkScanNoQualifiers(b *testing.B) {
	s := newBenchServer(b)
	seedIndexedBand(b, s, "app", "band", BucketTypeHash, scanBandWidth)
	request := ScanRequest{Database: "app", Bucket: "band"}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		scanResponse, err := s.Scan(request)
		if err != nil {
			b.Fatalf("Scan: %v", err)
		}
		if len(scanResponse.Documents) != scanBandWidth {
			b.Fatalf("Scan returned %d documents, want %d", len(scanResponse.Documents), scanBandWidth)
		}
		if _, err := bson.Marshal(scanResponse); err != nil {
			b.Fatalf("marshaling the scan response: %v", err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*scanBandWidth), "ns/result")
}

// BenchmarkSearch measures a secondary-index search as handleSearch performs
// it — Search, then the BSON marshal onto the wire — for an indexed value all
// 100 seeded documents share.
func BenchmarkSearch(b *testing.B) {
	s := newBenchServer(b)
	seedIndexedBand(b, s, "app", "band", BucketTypeHash, scanBandWidth)
	request := SearchRequest{Database: "app", Bucket: "band", Filter: map[string]interface{}{"group": scanBandGroup}}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		searchResponse, err := s.Search(request)
		if err != nil {
			b.Fatalf("Search: %v", err)
		}
		if len(searchResponse.Results) != scanBandWidth {
			b.Fatalf("Search returned %d results (message %q), want %d", len(searchResponse.Results), searchResponse.Message, scanBandWidth)
		}
		if _, err := bson.Marshal(searchResponse); err != nil {
			b.Fatalf("marshaling the search response: %v", err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*scanBandWidth), "ns/result")
}

// decodedDocuments decodes scan or search documents into maps, for tests that
// assert on their fields. The server now hands them over as BSON.
func decodedDocuments(tb testing.TB, documents []bson.Raw) []map[string]interface{} {
	tb.Helper()
	decoded := make([]map[string]interface{}, 0, len(documents))
	for position, document := range documents {
		var fields map[string]interface{}
		if err := bson.Unmarshal(document, &fields); err != nil {
			tb.Fatalf("decoding document %d: %v", position, err)
		}
		decoded = append(decoded, fields)
	}
	return decoded
}

// wireScanResponse and wireSearchResponse decode a response frame
// independently of the server's own types, so the checks below read the
// bytes a client receives, whatever Go type built them.
type wireScanResponse struct {
	Documents []bson.Raw `bson:"documents"`
	Total     int64      `bson:"total"`
}

type wireSearchResponse struct {
	Results []bson.Raw `bson:"results"`
	Total   int        `bson:"total"`
}

// scanOnWire runs Scan as handleScan does — Scan, then bson.Marshal — and
// returns the frame as a client decodes it.
func scanOnWire(t *testing.T, s *Server, request ScanRequest) wireScanResponse {
	t.Helper()
	scanResponse, err := s.Scan(request)
	if err != nil {
		t.Fatalf("Scan(%+v): %v", request, err)
	}
	frame, err := bson.Marshal(scanResponse)
	if err != nil {
		t.Fatalf("marshaling the scan response: %v", err)
	}
	var onWire wireScanResponse
	if err := bson.Unmarshal(frame, &onWire); err != nil {
		t.Fatalf("decoding the scan response frame: %v", err)
	}
	return onWire
}

// searchOnWire is scanOnWire for Search and handleSearch.
func searchOnWire(t *testing.T, s *Server, request SearchRequest) wireSearchResponse {
	t.Helper()
	searchResponse, err := s.Search(request)
	if err != nil {
		t.Fatalf("Search(%+v): %v", request, err)
	}
	if searchResponse.Message != "" {
		t.Fatalf("Search(%+v) message = %q, want none", request, searchResponse.Message)
	}
	frame, err := bson.Marshal(searchResponse)
	if err != nil {
		t.Fatalf("marshaling the search response: %v", err)
	}
	var onWire wireSearchResponse
	if err := bson.Unmarshal(frame, &onWire); err != nil {
		t.Fatalf("decoding the search response frame: %v", err)
	}
	return onWire
}

// seedScanBand creates a bucket of any of the five scannable store types and
// fills it with count documents shaped like seedIndexedBand's.
func seedScanBand(t *testing.T, s *Server, bucket string, bucketType BucketType, count int) {
	t.Helper()
	switch bucketType {
	case BucketTypeHash, BucketTypeBTree:
		seedIndexedBand(t, s, "app", bucket, bucketType, count)
	case BucketTypeFIFO, BucketTypeLIFO:
		createTestBucket(t, s, "app", bucket, bucketType)
		for seq := 0; seq < count; seq++ {
			insertScanDoc(t, s, "app", bucket, fmt.Sprintf("q%03d", seq), map[string]interface{}{"seq": int32(seq), "payload": strings.Repeat("abcdefghij", 10)})
		}
	case BucketTypeHeap:
		createTestBucket(t, s, "app", bucket, bucketType)
		for seq := 0; seq < count; seq++ {
			pushed, err := s.Push(PushRequest{Database: "app", Bucket: bucket, Priority: int64(seq), Data: map[string]interface{}{"seq": int32(seq), "payload": strings.Repeat("abcdefghij", 10)}})
			if err != nil || !pushed.Success {
				t.Fatalf("Push(%d) = %+v, %v", seq, pushed, err)
			}
		}
	default:
		t.Fatalf("seedScanBand does not know store type %v", bucketType)
	}
}

// scanBandPair is the small and large band sizes the calibrated scan tests
// measure; both fit one default Scan page so the response grows with the band.
const (
	scanSmallEntries = scanBandWidth / 2
	scanLargeEntries = scanBandWidth
)

// scanPerEntryAllocations returns the allocations each extra document cost a
// scan with the given qualifiers, measured over the bucket pair, and the
// allocations of one map decode of a document of the large bucket.
func scanPerEntryAllocations(t *testing.T, s *Server, smallBucket, largeBucket string, qualifiers ScanRequest) (perEntry, oneDecode float64) {
	t.Helper()
	scanOver := func(bucket string) func() {
		request := qualifiers
		request.Database, request.Bucket = "app", bucket
		return func() {
			scanResponse, err := s.Scan(request)
			if err != nil {
				t.Fatalf("Scan(%+v): %v", request, err)
			}
			if _, err := bson.Marshal(scanResponse); err != nil {
				t.Fatalf("marshaling the scan response: %v", err)
			}
		}
	}
	scanned, err := s.Scan(ScanRequest{Database: "app", Bucket: largeBucket, Limit: 1})
	if err != nil || len(scanned.Documents) != 1 {
		t.Fatalf("Scan(%q, limit 1) = %+v, %v", largeBucket, scanned, err)
	}
	perEntry = addedAllocationsPerEntry(t, scanSmallEntries, scanLargeEntries, scanOver(smallBucket), scanOver(largeBucket))
	oneDecode = mapDecodeAllocations(t, scanned.Documents[0])
	if oneDecode < 1 {
		t.Fatalf("one map decode measured %v allocations; the measurement is broken", oneDecode)
	}
	return perEntry, oneDecode
}

// TestScanNoDecodeWithoutQualifiers holds find() and find({}) to the bar Find
// already meets, on every store type Scan serves: with no filter, sort or
// projection, the stored BSON goes to the wire with no decode into a map and
// no re-encode from one. Each extra document must cost fewer allocations than
// one map decode of it, measured in the same test.
func TestScanNoDecodeWithoutQualifiers(t *testing.T) {
	s := newTestServer(t)
	stores := []struct {
		label      string
		bucketType BucketType
	}{{"hash", BucketTypeHash}, {"btree", BucketTypeBTree}, {"fifo", BucketTypeFIFO}, {"lifo", BucketTypeLIFO}, {"heap", BucketTypeHeap}}
	requestForms := []struct {
		label  string
		filter map[string]interface{}
	}{{"find()", nil}, {"find({})", map[string]interface{}{}}}

	for _, store := range stores {
		smallBucket, largeBucket := store.label+"small", store.label+"large"
		seedScanBand(t, s, smallBucket, store.bucketType, scanSmallEntries)
		seedScanBand(t, s, largeBucket, store.bucketType, scanLargeEntries)
		for _, form := range requestForms {
			onWire := scanOnWire(t, s, ScanRequest{Database: "app", Bucket: largeBucket, Filter: form.filter})
			if len(onWire.Documents) != scanLargeEntries || onWire.Total != int64(scanLargeEntries) {
				t.Fatalf("%s on %s returned %d documents of %d, want %d of %d",
					form.label, largeBucket, len(onWire.Documents), onWire.Total, scanLargeEntries, scanLargeEntries)
			}
			perEntry, oneDecode := scanPerEntryAllocations(t, s, smallBucket, largeBucket, ScanRequest{Filter: form.filter})
			if perEntry >= oneDecode {
				t.Errorf("%s on a %s bucket: each extra document cost %.2f allocations; one map decode of it costs %.0f, so the scan is decoding or re-encoding every document",
					form.label, store.label, perEntry, oneDecode)
			}
		}
	}
}

// TestSearchDecodesNothing holds Search to the same bar: nothing reads a hit's
// fields, so no hit is decoded into a map or re-encoded from one.
// Hash only: it is the one store that maintains secondary indexes
// (Server.buildEngines wires the index manager into the hash store alone), so
// a btree bucket's Search has no hits to measure.
func TestSearchDecodesNothing(t *testing.T) {
	s := newTestServer(t)
	seedIndexedBand(t, s, "app", "small", BucketTypeHash, scanSmallEntries)
	seedIndexedBand(t, s, "app", "large", BucketTypeHash, scanLargeEntries)

	onWire := searchOnWire(t, s, SearchRequest{Database: "app", Bucket: "large", Filter: map[string]interface{}{"group": scanBandGroup}})
	if len(onWire.Results) != scanLargeEntries || onWire.Total != scanLargeEntries {
		t.Fatalf("Search returned %d results of %d, want %d of %d", len(onWire.Results), onWire.Total, scanLargeEntries, scanLargeEntries)
	}
	searchOver := func(bucket string) func() {
		request := SearchRequest{Database: "app", Bucket: bucket, Filter: map[string]interface{}{"group": scanBandGroup}}
		return func() {
			searchResponse, err := s.Search(request)
			if err != nil {
				t.Fatalf("Search(%+v): %v", request, err)
			}
			if _, err := bson.Marshal(searchResponse); err != nil {
				t.Fatalf("marshaling the search response: %v", err)
			}
		}
	}
	perEntry := addedAllocationsPerEntry(t, scanSmallEntries, scanLargeEntries, searchOver("small"), searchOver("large"))
	oneDecode := mapDecodeAllocations(t, onWire.Results[0])
	if perEntry >= oneDecode {
		t.Errorf("each extra Search hit cost %.2f allocations; one map decode of it costs %.0f, so the search is decoding or re-encoding every hit", perEntry, oneDecode)
	}
}

// TestQualifiedScanDecodesOnlyWhatItReturns is the other half of the
// decision to hand documents over as BSON. A filtered or sorted scan reads
// the fields its filter and its sort key name from the stored bytes, with the
// resolver, and a projected one decodes only the documents on its page
// (bson-codec-spec 7.2), so an extra walked document costs less than one map
// decode of it. Each still answers correctly through the raw wire shape.
//
// Before step 7a this test, as TestScanStillDecodesWhenQualified, asserted
// the opposite: that each walked document cost at least a map decode.
func TestQualifiedScanDecodesOnlyWhatItReturns(t *testing.T) {
	s := newTestServer(t)
	seedIndexedBand(t, s, "app", "small", BucketTypeHash, scanSmallEntries)
	seedIndexedBand(t, s, "app", "band", BucketTypeHash, scanLargeEntries)

	qualified := []struct {
		label     string
		qualifier ScanRequest
	}{
		{"filtered", ScanRequest{Filter: map[string]interface{}{"label": "k0000042"}}},
		{"sorted", ScanRequest{Sort: map[string]int{"seq": -1}, Limit: 3}},
		{"projected", ScanRequest{Projection: map[string]int{"label": 1}, Limit: 1}},
	}
	for _, scan := range qualified {
		perEntry, oneDecode := scanPerEntryAllocations(t, s, "small", "band", scan.qualifier)
		if perEntry >= oneDecode {
			t.Errorf("a %s scan cost %.2f allocations per extra walked document; one map decode of it costs %.0f, so the scan is decoding every walked document",
				scan.label, perEntry, oneDecode)
		}
	}

	filtered := scanOnWire(t, s, ScanRequest{Database: "app", Bucket: "band", Filter: map[string]interface{}{"label": "k0000042"}})
	if len(filtered.Documents) != 1 || filtered.Total != 1 {
		t.Fatalf("filtered scan returned %d documents of %d, want 1 of 1", len(filtered.Documents), filtered.Total)
	}
	if seq := filtered.Documents[0].Lookup("seq").Int32(); seq != 42 {
		t.Errorf("filtered scan returned seq %d, want 42", seq)
	}

	sorted := scanOnWire(t, s, ScanRequest{Database: "app", Bucket: "band", Sort: map[string]int{"seq": -1}, Limit: 3})
	if len(sorted.Documents) != 3 || sorted.Total != scanBandWidth {
		t.Fatalf("sorted scan returned %d documents of %d, want 3 of %d", len(sorted.Documents), sorted.Total, scanBandWidth)
	}
	for position, want := range []int32{99, 98, 97} {
		if seq := sorted.Documents[position].Lookup("seq").Int32(); seq != want {
			t.Errorf("sorted scan document %d has seq %d, want %d (descending)", position, seq, want)
		}
	}

	projected := scanOnWire(t, s, ScanRequest{Database: "app", Bucket: "band", Projection: map[string]int{"label": 1}, Limit: 1})
	if len(projected.Documents) != 1 {
		t.Fatalf("projected scan returned %d documents, want 1", len(projected.Documents))
	}
	fields, err := projected.Documents[0].Elements()
	if err != nil {
		t.Fatalf("reading the projected document: %v", err)
	}
	if len(fields) != 1 || fields[0].Key() != "label" {
		t.Errorf("projected document is %s, want only the label field", projected.Documents[0])
	}
}

// walkPerEntryAllocations is what each extra document costs the bucket walk
// alone, before anything reads it: the walk's own copy of the stored bytes.
func walkPerEntryAllocations(t *testing.T, s *Server, smallBucket, largeBucket string) float64 {
	t.Helper()
	walkOver := func(bucket string) func() {
		return func() {
			view, snap, err := s.resolveForRead("app", bucket)
			if err != nil {
				t.Fatalf("resolving %s: %v", bucket, err)
			}
			defer snap.Discard()
			if err := walkBucketDocuments(view, "app", bucket, func([]byte) error { return nil }); err != nil {
				t.Fatalf("walking %s: %v", bucket, err)
			}
		}
	}
	return addedAllocationsPerEntry(t, scanSmallEntries, scanLargeEntries, walkOver(smallBucket), walkOver(largeBucket))
}

// lookupAllocations is the allocations of one resolver Lookup on a stored
// document, measured, so the bound below is not a picked number.
func lookupAllocations(t *testing.T, stored []byte, path bson.Path) float64 {
	t.Helper()
	document, err := bson.TrustStored(stored)
	if err != nil {
		t.Fatalf("framing a stored document: %v", err)
	}
	return testing.AllocsPerRun(allocationRuns, func() {
		if _, _, err := document.Lookup(path); err != nil {
			t.Fatalf("Lookup: %v", err)
		}
	})
}

// TestFilteredScanAllocatesPerVisitedDocumentOnlyWhatItReturns holds the
// filter to its claim (bson-codec-spec 7.2, build doc step 7a): beyond the
// walk's own copy, a visited document costs a filtered scan fewer
// allocations than one Lookup plus one, so the filter allocates nothing of
// its own per document; a document on the page adds only the page's
// growth. Allocation counts move under the race detector, so it skips there.
func TestFilteredScanAllocatesPerVisitedDocumentOnlyWhatItReturns(t *testing.T) {
	if raceEnabled {
		t.Skip("the race detector changes allocation counts")
	}
	s := newTestServer(t)
	seedIndexedBand(t, s, "app", "small", BucketTypeHash, scanSmallEntries)
	seedIndexedBand(t, s, "app", "band", BucketTypeHash, scanLargeEntries)
	walkCost := walkPerEntryAllocations(t, s, "small", "band")
	lookupCost := lookupAllocations(t, findStoredBytes(t, s, "app", "band", "k0000042"), bson.Path{bson.Name("label")})

	filters := []struct {
		label  string
		filter map[string]interface{}
	}{
		{"equality that matches nothing", map[string]interface{}{"label": "absent"}},
		{"range, $in and $exists together", map[string]interface{}{
			"seq": map[string]interface{}{"$gte": int32(1000)}, "group": map[string]interface{}{"$in": []interface{}{"x", "y"}},
			"payload": map[string]interface{}{"$exists": true}}},
		{"$or of a missing path and a $regex", map[string]interface{}{"$or": []interface{}{
			map[string]interface{}{"address.zip": "0150"}, map[string]interface{}{"label": map[string]interface{}{"$regex": "^zz"}}}}},
		{"equality that matches every document", map[string]interface{}{"group": scanBandGroup}},
	}
	for _, scan := range filters {
		perEntry, _ := scanPerEntryAllocations(t, s, "small", "band", ScanRequest{Filter: scan.filter})
		bound := walkCost + lookupCost + 1
		if perEntry >= bound {
			t.Errorf("a scan filtered by %s cost %.2f allocations per extra visited document; the walk costs %.2f and a Lookup %.0f, so the bound is %.2f",
				scan.label, perEntry, walkCost, lookupCost, bound)
		}
		t.Logf("%s: %.2f allocations per visited document (walk %.2f, Lookup %.0f)", scan.label, perEntry, walkCost, lookupCost)
	}
}

// TestScanAndSearchReturnStoredBytesUnchanged proves the passthrough drifts
// no document: an unqualified scan and a search both carry each stored
// document byte for byte — the bytes Find returns for the same key — through
// documents a map round trip could disturb: many keys, a nested document, an
// array, an int64, a NaN, a datetime and binary data.
func TestScanAndSearchReturnStoredBytesUnchanged(t *testing.T) {
	s := newTestServer(t)
	createIndexedTestBucket(t, s, "app", "mixed", []interface{}{"kind"})

	stamp := time.Date(2026, 9, 24, 18, 30, 15, 123_000_000, time.UTC)
	documents := map[string]map[string]interface{}{
		"doc-a": {"seq": int32(1), "big": int64(math.MaxInt64), "ratio": 0.1, "name": "alpha", "flag": true, "gone": nil},
		"doc-b": {"seq": int32(2), "nan": math.NaN(), "at": stamp, "tags": []interface{}{"x", int32(7), 2.5}},
		"doc-c": {"seq": int32(3), "blob": primitive.Binary{Subtype: 0x00, Data: []byte{0x00, 0xff, 0x10, 0x80}},
			"nested": map[string]interface{}{"z": int32(1), "a": "first", "m": map[string]interface{}{"deep": 1.5}}},
		"doc-d": {"seq": int32(4), "k1": "v1", "k2": "v2", "k3": "v3", "k4": "v4", "k5": "v5", "k6": "v6", "k7": "v7"},
	}
	storedBytes := make(map[string][]byte, len(documents))
	for key, fields := range documents {
		fields["kind"] = "mixed"
		fields["key"] = key
		inserted, err := s.Insert(InsertRequest{Database: "app", Bucket: "mixed", Key: key, Data: fields})
		if err != nil || !inserted.Success {
			t.Fatalf("Insert(%q) = %+v, %v", key, inserted, err)
		}
		found, err := s.Find(FindRequest{Database: "app", Bucket: "mixed", Key: key})
		if err != nil || !found.Found {
			t.Fatalf("Find(%q) = %+v, %v", key, found, err)
		}
		storedBytes[key] = found.Data
	}

	assertStoredBytes := func(label string, onWire []bson.Raw) {
		t.Helper()
		if len(onWire) != len(documents) {
			t.Fatalf("%s carries %d documents, want %d", label, len(onWire), len(documents))
		}
		for _, document := range onWire {
			key, ok := document.Lookup("key").StringValueOK()
			if !ok {
				t.Fatalf("%s carries a document with no key field: %s", label, document)
			}
			if !bytes.Equal(document, storedBytes[key]) {
				t.Errorf("%s changed %q on the way to the wire:\n  stored %x\n  wire   %x", label, key, storedBytes[key], []byte(document))
			}
		}
	}

	scanned := scanOnWire(t, s, ScanRequest{Database: "app", Bucket: "mixed"})
	assertStoredBytes("an unqualified scan", scanned.Documents)
	searched := searchOnWire(t, s, SearchRequest{Database: "app", Bucket: "mixed", Filter: map[string]interface{}{"kind": "mixed"}})
	assertStoredBytes("a search", searched.Results)
}

// TestSortedScanAllocatesPerVisitedDocumentOnlyWhatItKeeps is the sorted
// scan's bound (build doc step 7a): beyond the walk's own copy, which each of
// its two passes makes, a visited document costs fewer allocations than one
// Lookup plus one, for a number key and a missing one. A string key's text
// is the candidate's own, the one allocation a kept key may add. It skips
// under the race detector, which changes allocation counts.
func TestSortedScanAllocatesPerVisitedDocumentOnlyWhatItKeeps(t *testing.T) {
	if raceEnabled {
		t.Skip("the race detector changes allocation counts")
	}
	s := newTestServer(t)
	seedIndexedBand(t, s, "app", "small", BucketTypeHash, scanSmallEntries)
	seedIndexedBand(t, s, "app", "band", BucketTypeHash, scanLargeEntries)
	walkCost := walkPerEntryAllocations(t, s, "small", "band")
	lookupCost := lookupAllocations(t, findStoredBytes(t, s, "app", "band", "k0000042"), bson.Path{bson.Name("seq")})

	sorts := []struct {
		label    string
		request  ScanRequest
		ownAlloc float64
	}{
		{"a number key, one page", ScanRequest{Sort: map[string]int{"seq": -1}, Limit: 3}, 0},
		{"a missing key, filtered", ScanRequest{Sort: map[string]int{"absent": 1}, Filter: map[string]interface{}{"group": scanBandGroup}, Limit: 3}, 0},
		{"a string key", ScanRequest{Sort: map[string]int{"label": 1}, Limit: 3}, 1},
	}
	for _, scan := range sorts {
		perEntry, _ := scanPerEntryAllocations(t, s, "small", "band", scan.request)
		// Pass 2 walks again only as far as the page's last position, which
		// for a descending number key is the end of the bucket.
		bound := 2*walkCost + lookupCost + scan.ownAlloc + 1
		if perEntry >= bound {
			t.Errorf("a scan sorted by %s cost %.2f allocations per extra visited document; two walks cost %.2f, a Lookup %.0f and the key %.0f, so the bound is %.2f",
				scan.label, perEntry, 2*walkCost, lookupCost, scan.ownAlloc, bound)
		}
		t.Logf("%s: %.2f allocations per visited document (walk %.2f, Lookup %.0f)", scan.label, perEntry, walkCost, lookupCost)
	}
}
