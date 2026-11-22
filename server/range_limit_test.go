package server

import (
	"fmt"
	"strings"
	"testing"
)

// rangeLimit is the page a bounded range read asks for in the range-read
// benchmark and test: a page-sized read, as a paginating client makes.
const rangeLimit = 20

// wideRangeBandWidth is the width of the wide band the range benchmark measures a
// bounded range read over.
const wideRangeBandWidth = 100_000

// seedWideRangeBand fills a btree bucket with count documents keyed
// k0000000.., shaped like seedRangeBand's, in batches so a 100,000-key band
// seeds in seconds.
func seedWideRangeBand(tb testing.TB, s *Server, database, bucket string, count int) {
	tb.Helper()
	const batchSize = 1000
	createTestBucket(tb, s, database, bucket, BucketTypeBTree)
	payload := strings.Repeat("abcdefghij", 10)
	for first := 0; first < count; first += batchSize {
		documents := make([]map[string]interface{}, 0, batchSize)
		for seq := first; seq < min(first+batchSize, count); seq++ {
			key := fmt.Sprintf("k%07d", seq)
			documents = append(documents, map[string]interface{}{"_id_": key, "seq": int32(seq), "payload": payload, "label": key})
		}
		inserted, err := s.BatchInsert(BatchInsertRequest{Database: database, Bucket: bucket, Data: documents})
		if err != nil || !inserted.Success {
			tb.Fatalf("BatchInsert(%d documents from %d) = %+v, %v", len(documents), first, inserted, err)
		}
	}
}

// BenchmarkRangeQueryLimit measures a bounded range read — RangeQuery with a
// limit of 20 — over a band exactly as wide as the limit and over a
// 100,000-key band of the same bucket. Both return the same 20
// entries; a read whose cost follows the limit costs the same on both.
func BenchmarkRangeQueryLimit(b *testing.B) {
	s := newBenchServer(b)
	seedWideRangeBand(b, s, "app", "wide", wideRangeBandWidth)

	bands := []struct {
		name  string
		width int
	}{{"band=20", rangeLimit}, {"band=100000", wideRangeBandWidth}}
	for _, band := range bands {
		b.Run(band.name, func(b *testing.B) {
			request := BTreeRangeRequest{Database: "app", Bucket: "wide", Low: "k0000000", High: fmt.Sprintf("k%07d", band.width-1), Limit: rangeLimit}
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				rangeResponse, err := s.RangeQuery(request)
				if err != nil {
					b.Fatalf("RangeQuery: %v", err)
				}
				if len(rangeResponse.Entries) != rangeLimit {
					b.Fatalf("RangeQuery returned %d entries, want %d", len(rangeResponse.Entries), rangeLimit)
				}
			}
		})
	}
}

// TestRangeQueryStopsAtLimit holds a bounded range read to the work its limit
// names: over a 1,000-key band, a limit of 20 moves the store's iterator 20
// times and returns the band's first 20 entries, where it used to walk the
// whole band and truncate afterwards. A range with no limit, and
// the four callers that must see every entry — Scan, Count, Distinct and a
// multi-document Update — still read the whole band.
func TestRangeQueryStopsAtLimit(t *testing.T) {
	const bandWidth = 1000
	s := newTestServer(t)
	seedWideRangeBand(t, s, "app", "band", bandWidth)
	fullBand := BTreeRangeRequest{Database: "app", Bucket: "band", Low: "k0000000", High: fmt.Sprintf("k%07d", bandWidth-1)}

	rangeOverCountedView := func(t *testing.T, request BTreeRangeRequest) ([]BTreeEntry, int64) {
		t.Helper()
		snap := s.kv.NewSnapshot()
		defer snap.Discard()
		counts := &storeCallCounts{}
		view := countedBucket(t, s, "app", "band", snapshotKV{snap: snap}, counts)
		counts.reset()
		entries, err := rangeQueryEntries(view, request, testBudget(queryMethodBTreeRange), testWalk(queryMethodBTreeRange))
		if err != nil {
			t.Fatalf("rangeQueryEntries(%+v): %v", request, err)
		}
		return entries, counts.iteratorSteps.Load()
	}

	t.Run("limited", func(t *testing.T) {
		limited := fullBand
		limited.Limit = rangeLimit
		entries, steps := rangeOverCountedView(t, limited)
		if len(entries) != rangeLimit {
			t.Fatalf("a range limited to %d returned %d entries", rangeLimit, len(entries))
		}
		for position, entry := range entries {
			if want := fmt.Sprintf("k%07d", position); entry.Key != want {
				t.Errorf("entry %d has key %q, want %q: a limit returns the band's first entries", position, entry.Key, want)
			}
		}
		if steps > rangeLimit {
			t.Errorf("a range limited to %d over a %d-key band moved the iterator %d times; want at most %d",
				rangeLimit, bandWidth, steps, rangeLimit)
		}
	})

	t.Run("unlimited", func(t *testing.T) {
		entries, steps := rangeOverCountedView(t, fullBand)
		if len(entries) != bandWidth {
			t.Fatalf("a range with no limit returned %d entries, want the whole band of %d", len(entries), bandWidth)
		}
		if steps < bandWidth {
			t.Errorf("a range with no limit moved the iterator %d times over a %d-key band; it must visit every entry", steps, bandWidth)
		}
	})

	t.Run("wholeBandCallers", func(t *testing.T) {
		scanned, err := s.Scan(ScanRequest{Database: "app", Bucket: "band"})
		if err != nil || scanned.Total != bandWidth {
			t.Errorf("Scan total = %+v, %v; want %d", scanned, err, bandWidth)
		}
		counted, err := s.Count(CountRequest{Database: "app", Bucket: "band", Filter: map[string]interface{}{"payload": strings.Repeat("abcdefghij", 10)}})
		if err != nil || counted.Count != bandWidth {
			t.Errorf("filtered Count = %+v, %v; want %d", counted, err, bandWidth)
		}
		distinct, err := s.Distinct(DistinctRequest{Database: "app", Bucket: "band", Field: "seq"})
		if err != nil || len(distinct.Values) != bandWidth {
			t.Errorf("Distinct(seq) returned %d values, %v; want %d", len(distinct.Values), err, bandWidth)
		}
		updated, err := s.Update(UpdateRequest{Database: "app", Bucket: "band", Multi: true, Filter: map[string]interface{}{}, Inc: map[string]int64{"touched": 1}})
		if want := multiUpdateMessage(bandWidth); err != nil || updated.Message != want {
			t.Errorf("multi-update = %+v, %v; want message %q", updated, err, want)
		}
	})
}
