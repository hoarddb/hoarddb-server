package server

import (
	"fmt"
	"math/rand/v2"
	"runtime"
	"strings"
	"sync"
	"testing"
)

// scanCostPostBody is a post of about 1.2 KB, the size bench/social writes.
var scanCostPostBody = strings.Repeat("p", 1200)

// seedScanCostBucket creates a bucket of btype and writes posts documents to
// it in batches, each with an ascending _id_ so every store type accepts them.
func seedScanCostBucket(tb testing.TB, s *Server, bucket string, btype BucketType, posts int) {
	tb.Helper()
	createTestBucket(tb, s, "app", bucket, btype)
	const batchSize = 1000
	for start := 0; start < posts; start += batchSize {
		batch := make([]map[string]interface{}, 0, batchSize)
		for i := start; i < min(start+batchSize, posts); i++ {
			batch = append(batch, map[string]interface{}{"_id_": fmt.Sprintf("post-%06d", i), "body": scanCostPostBody})
		}
		inserted, err := s.BatchInsert(BatchInsertRequest{Database: "app", Bucket: bucket, Data: batch})
		if err != nil || !inserted.Success {
			tb.Fatalf("seeding %s with posts %d..%d: %+v, %v", bucket, start, start+len(batch), inserted, err)
		}
	}
}

// scanBytesPerRun is the heap bytes one Scan allocates, averaged over runs.
// Bytes, not wall time, because an allocation count does not depend on how
// busy the host is.
func scanBytesPerRun(tb testing.TB, s *Server, req ScanRequest, runs int) uint64 {
	tb.Helper()
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	for range runs {
		resp, err := s.Scan(req)
		if err != nil || !resp.Found {
			tb.Fatalf("Scan(%+v) = %+v, %v", req, resp, err)
		}
	}
	runtime.ReadMemStats(&after)
	return (after.TotalAlloc - before.TotalAlloc) / uint64(runs)
}

// TestUnfilteredScanCostIsIndependentOfBucketSize is the failing test for scan cost:
// an unfiltered page of 20 must cost the same on a 10,000-post timeline as on
// a 20-post one. Before the fix the walk read and copied every entry to count
// Total, and the larger bucket allocated about 309× the smaller.
func TestUnfilteredScanCostIsIndependentOfBucketSize(t *testing.T) {
	s := newTestServer(t)
	seedScanCostBucket(t, s, "short", BucketTypeLIFO, 20)
	seedScanCostBucket(t, s, "long", BucketTypeLIFO, 10_000)

	const runs = 20
	shortReq := ScanRequest{Database: "app", Bucket: "short", Limit: 20}
	longReq := ScanRequest{Database: "app", Bucket: "long", Limit: 20}

	shortBytes := scanBytesPerRun(t, s, shortReq, runs)
	longBytes := scanBytesPerRun(t, s, longReq, runs)
	if longBytes > 2*shortBytes {
		t.Errorf("a 20-post page of a 10,000-post LIFO allocated %d bytes, %.1f× the %d bytes of the same page of a 20-post one; want at most 2×: the page must not cost O(bucket)",
			longBytes, float64(longBytes)/float64(shortBytes), shortBytes)
	}

	shortAllocs := testing.AllocsPerRun(runs, func() { _, _ = s.Scan(shortReq) })
	longAllocs := testing.AllocsPerRun(runs, func() { _, _ = s.Scan(longReq) })
	if longAllocs > 2*shortAllocs {
		t.Errorf("a 20-post page of a 10,000-post LIFO made %.0f allocations, against %.0f on a 20-post one; want at most 2×",
			longAllocs, shortAllocs)
	}
}

// TestUnfilteredScanVisitsOnlyThePage counts the entries an unfiltered scan
// walks, through the query walk's per-entry hook, for every store type the
// scan serves: a page at skip s of limit n walks at most s+n+1 entries, and
// Total is still the whole bucket.
func TestUnfilteredScanVisitsOnlyThePage(t *testing.T) {
	const posts = 2000
	s := newTestServer(t)
	types := []BucketType{BucketTypeHash, BucketTypeBTree, BucketTypeFIFO, BucketTypeLIFO, BucketTypeHeap}
	for _, btype := range types {
		seedScanCostBucket(t, s, "visits-"+string(btype), btype, posts)
	}

	var visitsMu sync.Mutex
	visits := 0
	countVisit := func(string) {
		visitsMu.Lock()
		visits++
		visitsMu.Unlock()
	}
	queryVisitHook.Store(&countVisit)
	t.Cleanup(func() { queryVisitHook.Store(nil) })

	for _, btype := range types {
		for _, page := range []struct{ skip, limit int }{{0, 20}, {100, 20}} {
			visitsMu.Lock()
			visits = 0
			visitsMu.Unlock()
			resp, err := s.Scan(ScanRequest{Database: "app", Bucket: "visits-" + string(btype), Skip: page.skip, Limit: page.limit})
			if err != nil {
				t.Fatalf("%s skip %d: Scan: %v", btype, page.skip, err)
			}
			visitsMu.Lock()
			walked := visits
			visitsMu.Unlock()
			if walked > page.skip+page.limit+1 {
				t.Errorf("%s: a page at skip %d of limit %d walked %d of %d entries, want at most %d",
					btype, page.skip, page.limit, walked, posts, page.skip+page.limit+1)
			}
			if resp.Total != posts || len(resp.Documents) != page.limit || !resp.HasMore {
				t.Errorf("%s skip %d: Total %d, %d documents, HasMore %v; want %d, %d, true",
					btype, page.skip, resp.Total, len(resp.Documents), resp.HasMore, posts, page.limit)
			}
		}
	}
}

// BenchmarkLIFOScanPage20 measures reading the newest 20 posts of a
// LIFO timeline, through Server.Scan, at four timeline lengths. Posts are 400
// to 2,000 bytes, as bench/social writes them.
func BenchmarkLIFOScanPage20(b *testing.B) {
	for _, posts := range []int{20, 100, 1000, 10_000} {
		b.Run(fmt.Sprintf("posts=%d", posts), func(b *testing.B) {
			s := newBenchServer(b)
			bucket := fmt.Sprintf("timeline-%d", posts)
			createTestBucket(b, s, "app", bucket, BucketTypeLIFO)
			sizes := rand.New(rand.NewPCG(uint64(posts), 2302))
			for start := 0; start < posts; start += 500 {
				batch := make([]map[string]interface{}, 0, 500)
				for i := start; i < min(start+500, posts); i++ {
					batch = append(batch, map[string]interface{}{"_id_": fmt.Sprintf("post-%06d", i), "body": strings.Repeat("p", 400+sizes.IntN(1601))})
				}
				if inserted, err := s.BatchInsert(BatchInsertRequest{Database: "app", Bucket: bucket, Data: batch}); err != nil || !inserted.Success {
					b.Fatalf("seeding %s: %+v, %v", bucket, inserted, err)
				}
			}
			req := ScanRequest{Database: "app", Bucket: bucket, Limit: 20}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				resp, err := s.Scan(req)
				if err != nil || len(resp.Documents) != min(20, posts) || resp.Total != int64(posts) {
					b.Fatalf("Scan: %d documents, Total %d, %v", len(resp.Documents), resp.Total, err)
				}
			}
		})
	}
}
