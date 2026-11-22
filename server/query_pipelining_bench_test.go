package server

import (
	"fmt"
	"math/rand"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The query-pipelining benchmarks (spec §8, BM-3..BM-6). Each is run on the
// commit before its step and on the step's commit, on one host and one
// configuration, and the two figures are compared with each other only: a
// single node acknowledges without fsync, so none of these is set against
// another engine's numbers.
//
// They use only symbols that exist before the step they measure, so the same
// file runs on both commits.

// seedBenchBucket fills a hash bucket with count documents in batches, each
// carrying a group every document shares so a filter matches all of them.
func seedBenchBucket(b *testing.B, s *Server, database, bucket string, count int) {
	b.Helper()
	const batchSize = 1000
	for start := 0; start < count; start += batchSize {
		batch := make([]map[string]interface{}, 0, batchSize)
		for i := start; i < start+batchSize && i < count; i++ {
			doc := benchDoc(fmt.Sprintf("k%06d", i))
			doc["group"] = "all"
			doc["version"] = int32(0)
			batch = append(batch, doc)
		}
		resp, err := s.BatchInsert(BatchInsertRequest{Database: database, Bucket: bucket, Data: batch})
		if err != nil || !resp.Success {
			b.Fatalf("seeding %s.%s at %d: %+v, %v", database, bucket, start, resp, err)
		}
	}
}

// BenchmarkHotBucketReadScaling is BM-3: point reads of one bucket from every
// P at GOMAXPROCS 1, 4, 8 and 16. Before QP-4 every read takes the bucket's
// RWMutex read side, whose counter is one cache line every reader writes; the
// figure is ns per read across all readers.
func BenchmarkHotBucketReadScaling(b *testing.B) {
	const docCount = 1000
	for _, procs := range []int{1, 4, 8, 16} {
		b.Run(fmt.Sprintf("gomaxprocs=%d", procs), func(b *testing.B) {
			s := newBenchServer(b)
			createTestBucket(b, s, "app", "hot", BucketTypeHash)
			seedBenchBucket(b, s, "app", "hot", docCount)

			previous := runtime.GOMAXPROCS(procs)
			b.Cleanup(func() { runtime.GOMAXPROCS(previous) })

			var misses atomic.Int64
			var worker atomic.Int64
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				next := int(worker.Add(1)) * 7919
				for pb.Next() {
					key := fmt.Sprintf("k%06d", next%docCount)
					next++
					resp, err := s.Find(FindRequest{Database: "app", Bucket: "hot", Key: key})
					if err != nil || !resp.Found {
						misses.Add(1)
					}
				}
			})
			b.StopTimer()
			if n := misses.Load(); n != 0 {
				b.Fatalf("%d point reads missed or failed", n)
			}
		})
	}
}

// BenchmarkReadsDuviewMultiUpdate is BM-4: point reads of a bucket while a
// multi-document update rewrites all 10,000 of its documents in a loop. It
// reports the read latency's p50 and p99, and — the residual QP-4 does not
// remove (QF-8) — the latency of opening the node store's snapshot, which is
// what resolveForReadIn does under db.mu, sampled beside each read while the
// updates' commits are in flight.
func BenchmarkReadsDuviewMultiUpdate(b *testing.B) {
	const docCount = 10000
	s := newBenchServer(b)
	createTestBucket(b, s, "app", "busy", BucketTypeHash)
	seedBenchBucket(b, s, "app", "busy", docCount)

	stop := make(chan struct{})
	var updaterDone sync.WaitGroup
	var updates atomic.Int64
	var updateErr atomic.Pointer[error]
	updaterDone.Add(1)
	go func() {
		defer updaterDone.Done()
		for version := int32(1); ; version++ {
			select {
			case <-stop:
				return
			default:
			}
			resp, err := s.Update(UpdateRequest{
				Database: "app", Bucket: "busy", Multi: true,
				Filter: map[string]interface{}{"group": "all"},
				Set:    map[string]interface{}{"version": version},
			})
			if err == nil && !resp.Success {
				err = fmt.Errorf("multi-update refused: %s", resp.Message)
			}
			if err != nil {
				updateErr.Store(&err)
				return
			}
			updates.Add(1)
		}
	}()
	// Reads start once the first update is under way, so every sample is
	// taken against a bucket with a writer on it.
	for updates.Load() == 0 && updateErr.Load() == nil {
		time.Sleep(time.Millisecond)
	}

	readLatencies := make([]time.Duration, 0, b.N)
	snapshotLatencies := make([]time.Duration, 0, b.N)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("k%06d", (i*7919)%docCount)
		began := time.Now()
		resp, err := s.Find(FindRequest{Database: "app", Bucket: "busy", Key: key})
		readLatencies = append(readLatencies, time.Since(began))
		if err != nil || !resp.Found {
			b.Fatalf("reading %s during the update: %+v, %v", key, resp, err)
		}
		began = time.Now()
		snap := s.kv.NewSnapshot()
		snapshotLatencies = append(snapshotLatencies, time.Since(began))
		snap.Discard()
	}
	b.StopTimer()
	close(stop)
	updaterDone.Wait()
	if err := updateErr.Load(); err != nil {
		b.Fatalf("multi-update: %v", *err)
	}

	b.ReportMetric(float64(percentile(readLatencies, 0.50).Nanoseconds()), "read-p50-ns")
	b.ReportMetric(float64(percentile(readLatencies, 0.99).Nanoseconds()), "read-p99-ns")
	b.ReportMetric(float64(percentile(snapshotLatencies, 0.50).Nanoseconds()), "snapshot-p50-ns")
	b.ReportMetric(float64(percentile(snapshotLatencies, 0.99).Nanoseconds()), "snapshot-p99-ns")
	b.ReportMetric(float64(updates.Load()), "multi-updates")
}

// BenchmarkSortedPaging is BM-6 (spec §8, QF-13): the two-pass sort's cost on
// the workload server/scan_sorted_paging_test.go calls the commonest paging
// workload there is. A hash bucket of 20,000 documents of about 1 KiB, each
// with a CreatedAt number in an order unrelated to its key, sorted
// {CreatedAt: 1} with limit 100, at skip 0 and 5,000, unfiltered and with one
// filter that every document matches. The acceptance band is the spec's:
// after ns/op ≤ 1.21 × before and after B/op ≤ before, at every point.
func BenchmarkSortedPaging(b *testing.B) {
	const docCount = 20000
	s := newBenchServer(b)
	createTestBucket(b, s, "app", "events", BucketTypeHash)
	padding := strings.Repeat("x", 900)
	createdAt := rand.New(rand.NewSource(6)).Perm(docCount)
	const batchSize = 1000
	for start := 0; start < docCount; start += batchSize {
		batch := make([]map[string]interface{}, 0, batchSize)
		for i := start; i < start+batchSize; i++ {
			doc := benchDoc(fmt.Sprintf("k%06d", i))
			doc["group"] = "all"
			doc["CreatedAt"] = float64(createdAt[i])
			doc["payload"] = padding
			batch = append(batch, doc)
		}
		resp, err := s.BatchInsert(BatchInsertRequest{Database: "app", Bucket: "events", Data: batch})
		if err != nil || !resp.Success {
			b.Fatalf("seeding app.events at %d: %+v, %v", start, resp, err)
		}
	}

	for _, filtered := range []bool{false, true} {
		for _, skip := range []int{0, 5000} {
			name := fmt.Sprintf("filtered=%v/skip=%d", filtered, skip)
			request := ScanRequest{Database: "app", Bucket: "events", Sort: map[string]int{"CreatedAt": 1}, Skip: skip, Limit: 100}
			if filtered {
				request.Filter = map[string]interface{}{"group": "all"}
			}
			b.Run(name, func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					resp, err := s.Scan(request)
					if err != nil || len(resp.Documents) != 100 || resp.Total != docCount {
						b.Fatalf("sorted page: %d documents of %d, %v", len(resp.Documents), resp.Total, err)
					}
				}
			})
		}
	}
}
