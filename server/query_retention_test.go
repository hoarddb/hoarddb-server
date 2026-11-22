package server

import (
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/hoarddb/hoarddb-server/store"
)

// seedRetentionBucket fills a bucket with count documents of about 1 KiB,
// each with a low-cardinality "group" (five values) for distinct and filters.
func seedRetentionBucket(t *testing.T, s *Server, database, bucket string, count int) {
	t.Helper()
	padding := strings.Repeat("x", 1000)
	const batchSize = 1000
	for start := 0; start < count; start += batchSize {
		batch := make([]map[string]interface{}, 0, batchSize)
		for i := start; i < start+batchSize && i < count; i++ {
			batch = append(batch, map[string]interface{}{
				"_id_":  fmt.Sprintf("k%06d", i),
				"group": fmt.Sprintf("g%d", i%5),
				"n":     int32(i),
				"pad":   padding,
			})
		}
		resp, err := s.BatchInsert(BatchInsertRequest{Database: database, Bucket: bucket, Data: batch})
		if err != nil || !resp.Success {
			t.Fatalf("seeding %s.%s at %d: %+v, %v", database, bucket, start, resp, err)
		}
	}
}

// TestQueryRetentionIsBounded is T-6 (query-pipelining spec §8). This step
// lands its walk-shape cases: on a 20,000 × 1 KiB btree bucket, a filtered
// count, a distinct and a filtered find walk the bucket by callback and never
// ask the store for the whole band at once, Range(nil, nil, 0), which builds
// every entry into one slice before the first is visited. Step 5 adds the
// result accountant's cases (checkAccountedRetention).
func TestQueryRetentionIsBounded(t *testing.T) {
	const docCount = 20000
	s := newTestServer(t)
	createTestBucket(t, s, "app", "retained", BucketTypeBTree)
	seedRetentionBucket(t, s, "app", "retained", docCount)

	var wholeBand atomic.Int64
	countWholeBand := func(low, high interface{}, limit int) {
		if low == nil && high == nil && limit == 0 {
			wholeBand.Add(1)
		}
	}
	store.BTreeRangeHook.Store(&countWholeBand)
	t.Cleanup(func() { store.BTreeRangeHook.Store(nil) })

	walks := []struct {
		name string
		run  func() error
	}{
		{"filtered count", func() error {
			resp, err := s.Count(CountRequest{Database: "app", Bucket: "retained", Filter: map[string]interface{}{"group": "g1"}})
			if err == nil && resp.Count != docCount/5 {
				err = fmt.Errorf("count = %d, want %d", resp.Count, docCount/5)
			}
			return err
		}},
		{"distinct", func() error {
			resp, err := s.Distinct(DistinctRequest{Database: "app", Bucket: "retained", Field: "group"})
			if err == nil && len(resp.Values) != 5 {
				err = fmt.Errorf("distinct returned %d values, want 5", len(resp.Values))
			}
			return err
		}},
		{"filtered find", func() error {
			resp, err := s.Scan(ScanRequest{Database: "app", Bucket: "retained", Filter: map[string]interface{}{"group": "g1"}, Limit: 100})
			if err == nil && (len(resp.Documents) != 100 || resp.Total != docCount/5) {
				err = fmt.Errorf("find returned %d documents of %d, want 100 of %d (%s)", len(resp.Documents), resp.Total, docCount/5, resp.Message)
			}
			return err
		}},
	}
	for _, walk := range walks {
		t.Run(walk.name, func(t *testing.T) {
			before := wholeBand.Load()
			if err := walk.run(); err != nil {
				t.Fatalf("%s: %v", walk.name, err)
			}
			if calls := wholeBand.Load() - before; calls != 0 {
				t.Errorf("%s made %d Range(nil, nil, 0) call(s): it materialised the whole %d-document bucket "+
					"before visiting the first, so it retains O(bucket) instead of O(page) (B-1)", walk.name, calls, docCount)
			}
		})
	}

	createTestBucketWithOptions(t, s, "app", "retainedHash", BucketTypeHash, map[string]interface{}{"indices": []string{"group"}})
	seedRetentionBucket(t, s, "app", "retainedHash", docCount)
	checkAccountedRetention(t, s, docCount)
}

// checkAccountedRetention is T-6's accountant cases (step 5): whatever the
// bucket holds, a query's high-water retention is its page — plus the sort
// keys it ranks by, or the distinct values and their seen-set keys — plus a
// little slack, never the bucket.
func checkAccountedRetention(t *testing.T, s *Server, docCount int) {
	t.Helper()
	const slack = 4 << 10
	lastReport := recordBudgets(t)
	accounted := []struct {
		name    string
		method  string
		buckets []string
		// run returns what the answer itself encodes to, and what the shape
		// legitimately holds beside it.
		run func(bucket string) (pageBytes, besidePage int64, err error)
	}{
		{"filtered find", "SCAN", []string{"retained", "retainedHash"}, func(bucket string) (int64, int64, error) {
			resp, err := s.Scan(ScanRequest{Database: "app", Bucket: bucket, Filter: map[string]interface{}{"group": "g1"}, Limit: 100})
			if err != nil {
				return 0, 0, err
			}
			return encodedElementBytes(t, resp, "documents"), 0, expectLength(len(resp.Documents), 100)
		}},
		{"sorted find at skip 5000", "SCAN", []string{"retained", "retainedHash"}, func(bucket string) (int64, int64, error) {
			resp, err := s.Scan(ScanRequest{Database: "app", Bucket: bucket, Sort: map[string]int{"n": -1}, Skip: 5000, Limit: 100})
			if err != nil {
				return 0, 0, err
			}
			// skip+limit int32 keys, each charged its 4 value bytes plus 16.
			return encodedElementBytes(t, resp, "documents"), (5000 + 100) * (4 + 16), expectLength(len(resp.Documents), 100)
		}},
		{"filtered count", "COUNT", []string{"retained", "retainedHash"}, func(bucket string) (int64, int64, error) {
			resp, err := s.Count(CountRequest{Database: "app", Bucket: bucket, Filter: map[string]interface{}{"group": "g1"}})
			if err != nil {
				return 0, 0, err
			}
			return 0, 0, expectLength(int(resp.Count), docCount/5)
		}},
		{"distinct", "DISTINCT", []string{"retained"}, func(bucket string) (int64, int64, error) {
			resp, err := s.Distinct(DistinctRequest{Database: "app", Bucket: bucket, Field: "group"})
			if err != nil {
				return 0, 0, err
			}
			var keys int64
			for _, value := range resp.Values {
				keys += int64(len(valueToDistinctKey(value)))
			}
			return encodedElementBytes(t, resp, "values"), keys, expectLength(len(resp.Values), 5)
		}},
		{"search", "SEARCH", []string{"retainedHash"}, func(bucket string) (int64, int64, error) {
			resp, err := s.Search(SearchRequest{Database: "app", Bucket: bucket, Filter: map[string]interface{}{"group": "g1"}, Limit: 100})
			if err != nil {
				return 0, 0, err
			}
			return encodedElementBytes(t, resp, "results"), 0, expectLength(len(resp.Results), 100)
		}},
	}
	for _, query := range accounted {
		for _, bucket := range query.buckets {
			t.Run(query.name+" on "+bucket, func(t *testing.T) {
				pageBytes, besidePage, err := query.run(bucket)
				if err != nil {
					t.Fatalf("%s: %v", query.name, err)
				}
				report := lastReport(query.method)
				if !report.reported {
					t.Fatalf("%s returned without reporting a result budget: no accountant bounds what it retains (B-1)", query.name)
				}
				if bound := pageBytes + besidePage + slack; report.highWater > bound {
					t.Errorf("%s held up to %d bytes; its page is %d bytes and it keeps %d beside it, so it held more than "+
						"the page plus %d bytes of slack of a %d-document bucket (B-1)", query.name, report.highWater, pageBytes, besidePage, slack, docCount)
				}
				if report.highWater < pageBytes {
					t.Errorf("%s reported a high-water of %d bytes, below its own %d-byte page: the accountant does not see the page",
						query.name, report.highWater, pageBytes)
				}
			})
		}
	}
}

// expectLength is a query's result-size check, as an error.
func expectLength(got, want int) error {
	if got != want {
		return fmt.Errorf("returned %d elements, want %d", got, want)
	}
	return nil
}

// TestBTreeWalkErrorReachesCallerUnwrapped: a query's own error from inside a
// btree walk — here a filter the matcher refuses at the first document — reaches
// the caller exactly as it does from a hash bucket's walk, with no
// "iterating key" prefix from BTreeStore.IterateDocuments (spec §9 hazard 8).
func TestBTreeWalkErrorReachesCallerUnwrapped(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "ordered", BucketTypeBTree)
	createTestBucket(t, s, "app", "hashed", BucketTypeHash)
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("k%02d", i)
		insertScanDoc(t, s, "app", "ordered", key, map[string]interface{}{"n": int32(i)})
		insertScanDoc(t, s, "app", "hashed", key, map[string]interface{}{"n": int32(i)})
	}
	badFilter := map[string]interface{}{"n": map[string]interface{}{"$bogus": 1}}

	walks := []struct {
		name string
		run  func(bucket string) error
	}{
		{"count", func(bucket string) error {
			_, err := s.Count(CountRequest{Database: "app", Bucket: bucket, Filter: badFilter})
			return err
		}},
		{"distinct", func(bucket string) error {
			_, err := s.Distinct(DistinctRequest{Database: "app", Bucket: bucket, Field: "n", Filter: badFilter})
			return err
		}},
		{"find", func(bucket string) error {
			_, err := s.Scan(ScanRequest{Database: "app", Bucket: bucket, Filter: badFilter})
			return err
		}},
	}
	for _, walk := range walks {
		t.Run(walk.name, func(t *testing.T) {
			hashErr := walk.run("hashed")
			btreeErr := walk.run("ordered")
			if hashErr == nil || btreeErr == nil {
				t.Fatalf("a refused filter did not fail the %s: hash %v, btree %v", walk.name, hashErr, btreeErr)
			}
			if strings.Contains(btreeErr.Error(), "iterating key") {
				t.Errorf("the btree %s's error carries the walk's wrap: %q", walk.name, btreeErr)
			}
			if btreeErr.Error() != hashErr.Error() {
				t.Errorf("the btree %s's error differs from the hash bucket's:\n btree %q\n  hash %q", walk.name, btreeErr, hashErr)
			}
		})
	}
}
