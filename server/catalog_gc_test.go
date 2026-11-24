package server

import (
	"fmt"
	"os"
	"runtime"
	"runtime/metrics"
	"strings"
	"sync"
	"testing"
	"time"
)

// catalogGCShape is one kind of bucket the collector's cost is measured for.
// The shapes differ in what the catalog holds beside the bucket itself: a
// declared index or store field is a pointer-bearing declaration, and a
// declared replication factor is an entry in Cluster.bucketFactors (catalog GC
// spec Open Questions 3 and 6).
type catalogGCShape struct {
	name    string
	request func(database, bucket string) CreateBucketRequest
}

// catalogGCShapes are plain (an empty LIFO), one declared store field, one
// declared index, one declared replication factor, and two declared typed
// fields with no store field or index (typed-only: the shape the interned
// declaration set is for).
var catalogGCShapes = []catalogGCShape{
	{name: "plain", request: func(database, bucket string) CreateBucketRequest {
		return CreateBucketRequest{Database: database, Name: bucket, Type: BucketTypeLIFO}
	}},
	{name: "storefield", request: func(database, bucket string) CreateBucketRequest {
		// Only hash and btree buckets can declare store fields.
		return CreateBucketRequest{Database: database, Name: bucket, Type: BucketTypeHash, Options: map[string]interface{}{
			"stores": []interface{}{map[string]interface{}{"name": "items", "engine": "fifo"}},
		}}
	}},
	{name: "index", request: func(database, bucket string) CreateBucketRequest {
		return CreateBucketRequest{Database: database, Name: bucket, Type: BucketTypeHash, Options: map[string]interface{}{
			"indices": []interface{}{"name"},
		}}
	}},
	{name: "factor", request: func(database, bucket string) CreateBucketRequest {
		return CreateBucketRequest{Database: database, Name: bucket, Type: BucketTypeLIFO, ReplicationFactor: 2}
	}},
	{name: "typed", request: func(database, bucket string) CreateBucketRequest {
		return CreateBucketRequest{Database: database, Name: bucket, Type: BucketTypeHash, Options: map[string]interface{}{
			"fields": []interface{}{
				map[string]interface{}{"name": "Name", "type": "string", "required": true},
				map[string]interface{}{"name": "Age", "type": "int"},
			},
		}}
	}},
}

// catalogGCDatabase is the one database every fixture bucket lives in.
const catalogGCDatabase = "app"

// catalogGCBucketName is the name of the i-th fixture bucket.
func catalogGCBucketName(i int) string { return fmt.Sprintf("post-%08d", i) }

// buildCatalogGCFixture creates count buckets of shape through the production
// create path, so the catalog entry, the state document and the bucket struct
// are the real ones, and returns how many it created.
func buildCatalogGCFixture(tb testing.TB, s *Server, shape catalogGCShape, count int) int {
	tb.Helper()
	if created, err := s.CreateDatabase(catalogGCDatabase); err != nil || !created.Success {
		tb.Fatalf("creating database %q: %+v, %v", catalogGCDatabase, created, err)
	}
	built := 0
	for i := 0; i < count; i++ {
		created, err := s.CreateBucket(shape.request(catalogGCDatabase, catalogGCBucketName(i)))
		if err != nil || !created.Success {
			tb.Fatalf("creating %s bucket %d: %+v, %v", shape.name, i, created, err)
		}
		built++
	}
	return built
}

// TestCatalogGCFixtureBuildsEveryShape proves each shape the benchmarks
// measure builds, is served, and carries its declaration: a benchmark over a
// fixture that quietly built the wrong thing would record a number for nothing.
func TestCatalogGCFixtureBuildsEveryShape(t *testing.T) {
	const count = 200
	for _, shape := range catalogGCShapes {
		t.Run(shape.name, func(t *testing.T) {
			s := newTestServer(t)
			if built := buildCatalogGCFixture(t, s, shape, count); built != count {
				t.Fatalf("built %d buckets, want %d", built, count)
			}
			if attached := s.bucketsAttached.Load(); attached != count {
				t.Fatalf("bucketsAttached = %d, want %d", attached, count)
			}
			for _, i := range []int{0, count - 1} {
				name := catalogGCBucketName(i)
				bucket := s.serverDataAccessor().getDB(catalogGCDatabase).getBucket(name)
				if bucket == nil || bucket.id.Load() == 0 {
					t.Fatalf("bucket %s is not served", name)
				}
				stats, err := s.BucketStats(BucketStatsRequest{Database: catalogGCDatabase, Bucket: name})
				if err != nil || !stats.Found {
					t.Fatalf("describing %s: %+v, %v", name, stats, err)
				}
				switch shape.name {
				case "storefield":
					if len(stats.Stores) != 1 {
						t.Errorf("%s declares %d store fields, want 1", name, len(stats.Stores))
					}
				case "index":
					if len(stats.Indexes) != 1 {
						t.Errorf("%s declares %d indexes, want 1", name, len(stats.Indexes))
					}
				case "factor":
					if factor := bucket.replicationFactor.Load(); factor != 2 {
						t.Errorf("%s declares replication factor %d, want 2", name, factor)
					}
				case "typed":
					if len(stats.Fields) != 2 || len(stats.Stores) != 0 || len(stats.Indexes) != 0 {
						t.Errorf("%s declares %d typed fields, %d store fields and %d indexes, want 2, 0 and 0",
							name, len(stats.Fields), len(stats.Stores), len(stats.Indexes))
					}
				}
			}
		})
	}
}

// catalogGCSizes are the catalog sizes the benchmarks measure. A million
// buckets take about 48 s to build, so it runs only when asked for.
func catalogGCSizes() []int {
	sizes := []int{1_000, 10_000, 100_000}
	if os.Getenv("HOARDDB_CATALOG_1M") == "1" {
		sizes = append(sizes, 1_000_000)
	}
	return sizes
}

// BenchmarkGCCPUPerOp's load: spec 2.1's 400,000 inserts over 64 hot buckets.
const (
	catalogGCOpCount    = 400_000
	catalogGCHotBuckets = 64
)

// readHeapBytes is the bytes of live heap objects (HeapAlloc), so a delta
// across a build is what the build left resident.
func readHeapBytes() uint64 {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return stats.HeapAlloc
}

// readCollectorCounts returns the heap objects the collector traces and the
// scannable heap bytes it reads. HeapObjects alone would count pointer-free
// objects too, so both are read (spec 8, review M-6).
func readCollectorCounts() (objects, scannableBytes uint64) {
	samples := []metrics.Sample{{Name: "/gc/heap/objects:objects"}, {Name: "/gc/scan/heap:bytes"}}
	metrics.Read(samples)
	return samples[0].Value.Uint64(), samples[1].Value.Uint64()
}

// readCollectorCPUSeconds is the cumulative CPU the collector has used: assist,
// background, idle and stop-the-world.
func readCollectorCPUSeconds() float64 {
	samples := []metrics.Sample{{Name: "/cpu/classes/gc/total:cpu-seconds"}}
	metrics.Read(samples)
	return samples[0].Value.Float64()
}

// settleCollector runs two full collections so that what one reads afterwards
// is what stays live, not what the last build left to sweep.
func settleCollector() {
	runtime.GC()
	runtime.GC()
}

// perBucketGrowth is (after-before)/count for a collector counter. The two
// counters are read across a build, and at small N a collection in between can
// free more than the build added; an unsigned subtraction then wraps to about
// 1.8e16 and still prints a number. A negative growth is reported as zero.
func perBucketGrowth(before, after uint64, count int) float64 {
	if after < before {
		return 0
	}
	return float64(after-before) / float64(count)
}

// TestCatalogGCObjectsDeltaDoesNotWrap pins the unsigned-subtraction wrap that
// printed 1.8e16 objects per bucket at N=1000: the collector freed more than
// the build added, so after was below before.
func TestCatalogGCObjectsDeltaDoesNotWrap(t *testing.T) {
	cases := []struct {
		name          string
		before, after uint64
		count         int
		want          float64
	}{
		{"growth", 1000, 3000, 1000, 2},
		{"no change", 5000, 5000, 1000, 0},
		{"collector freed more than the build added", 5000, 4000, 1000, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := perBucketGrowth(tc.before, tc.after, tc.count); got != tc.want {
				t.Fatalf("perBucketGrowth(%d, %d, %d) = %g, want %g", tc.before, tc.after, tc.count, got, tc.want)
			}
		})
	}
}

// bestForcedGC is the shortest of runs forced full collections on one
// processor: the cost of one mark of everything the process holds.
func bestForcedGC(runs int) time.Duration {
	previous := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(previous)
	best := time.Duration(1<<63 - 1)
	for i := 0; i < runs; i++ {
		start := time.Now()
		runtime.GC()
		if elapsed := time.Since(start); elapsed < best {
			best = elapsed
		}
	}
	return best
}

// hostLoad is the one-minute load average, so a number recorded from a run
// carries the load it ran under. It is "unknown" where /proc/loadavg does not
// exist.
func hostLoad() string {
	loadAverages, err := os.ReadFile("/proc/loadavg")
	if err != nil {
		return "unknown"
	}
	fields := strings.Fields(string(loadAverages))
	if len(fields) == 0 {
		return "unknown"
	}
	return fields[0]
}

// catalogGCResults remembers the last measurement per sub-benchmark. The
// testing package calls a benchmark again with a larger b.N whenever the first
// call ran in under the benchtime, and a measurement that builds a catalog
// must not repeat for that; such a call (b.N above 1) reports the last
// measurement. A call with b.N of 1 is a new run (-count, or -benchtime=1x)
// and measures afresh.
var (
	catalogGCResultsMu sync.Mutex
	catalogGCResults   = map[string]map[string]float64{}
)

// measureOnce runs measure unless this is a larger-b.N repeat of a run that
// already measured, and reports the metrics either way.
func measureOnce(b *testing.B, key string, measure func() map[string]float64) {
	b.Helper()
	catalogGCResultsMu.Lock()
	metricValues, done := catalogGCResults[key]
	catalogGCResultsMu.Unlock()
	if !done || b.N == 1 {
		metricValues = measure()
		catalogGCResultsMu.Lock()
		catalogGCResults[key] = metricValues
		catalogGCResultsMu.Unlock()
		// The values are logged as well as reported, because the node store's
		// own log lines can split a benchmark's result line when -count repeats.
		b.Logf("RESULT %s: GOMAXPROCS=%d load average %s %v", key, runtime.GOMAXPROCS(0), hostLoad(), metricValues)
	}
	for unit, value := range metricValues {
		b.ReportMetric(value, unit)
	}
}

// BenchmarkCatalogGCMark builds a catalog of N buckets through the production
// create path and reports what the collector pays for it: heap objects and
// scannable bytes per bucket (the delta across two forced collections, divided
// by N) and the best of three forced full collections on one processor. The
// shapes are plain, one declared store field, one declared index, one
// declared replication factor and typed-only; heap-B/bucket is the HeapAlloc
// delta, the resident bytes per bucket rule 11 asks for.
//
// Nothing here asserts a number: a time asserted by go test fails on a loaded
// host and teaches people to ignore it (catalog GC spec 8). Record the numbers
// with the host and its load beside them.
//
//	go test -run XXX -bench 'BenchmarkCatalogGCMark' -benchtime=1x ./server/
//	HOARDDB_CATALOG_1M=1 go test -run XXX -bench 'BenchmarkCatalogGCMark/plain/N=1000000' -benchtime=1x -timeout 30m ./server/
func BenchmarkCatalogGCMark(b *testing.B) {
	for _, shape := range catalogGCShapes {
		for _, count := range catalogGCSizes() {
			b.Run(fmt.Sprintf("%s/N=%d", shape.name, count), func(b *testing.B) {
				measureOnce(b, b.Name(), func() map[string]float64 {
					s := newBenchServer(b)
					settleCollector()
					objectsBefore, scannableBefore := readCollectorCounts()
					heapBefore := readHeapBytes()
					if built := buildCatalogGCFixture(b, s, shape, count); built != count {
						b.Fatalf("built %d buckets, want %d", built, count)
					}
					settleCollector()
					objectsAfter, scannableAfter := readCollectorCounts()
					heapAfter := readHeapBytes()
					forcedGC := bestForcedGC(3)
					runtime.KeepAlive(s)
					return map[string]float64{
						"objects/bucket":     perBucketGrowth(objectsBefore, objectsAfter, count),
						"scannable-B/bucket": perBucketGrowth(scannableBefore, scannableAfter, count),
						"heap-B/bucket":      (float64(heapAfter) - float64(heapBefore)) / float64(count),
						"forced-gc-ms":       float64(forcedGC.Microseconds()) / 1000,
					}
				})
			})
		}
	}
}

// BenchmarkGCCPUPerOp is the cost the bug is about: with a catalog of N plain
// buckets in memory, 400,000 inserts into 64 hot buckets from one goroutine,
// and the collector's CPU per insert and the number of cycles it took. The
// catalog is live heap, so it raises the pacer's goal; both effects are in the
// figure. Recorded, not asserted.
//
//	go test -run XXX -bench BenchmarkGCCPUPerOp -benchtime=1x ./server/
func BenchmarkGCCPUPerOp(b *testing.B) {
	plain := catalogGCShapes[0]
	for _, count := range catalogGCSizes() {
		b.Run(fmt.Sprintf("N=%d", count), func(b *testing.B) {
			measureOnce(b, b.Name(), func() map[string]float64 {
				s := newBenchServer(b)
				if built := buildCatalogGCFixture(b, s, plain, count); built != count {
					b.Fatalf("built %d buckets, want %d", built, count)
				}
				settleCollector()
				var before, after runtime.MemStats
				runtime.ReadMemStats(&before)
				cpuBefore := readCollectorCPUSeconds()
				start := time.Now()
				for op := 0; op < catalogGCOpCount; op++ {
					key := fmt.Sprintf("k%08d", op)
					_, err := s.Insert(InsertRequest{
						Database: catalogGCDatabase,
						Bucket:   catalogGCBucketName(op % catalogGCHotBuckets),
						Key:      key,
						Data:     benchDoc(key),
					})
					if err != nil {
						b.Fatalf("insert %d: %v", op, err)
					}
				}
				elapsed := time.Since(start)
				cpuSeconds := readCollectorCPUSeconds() - cpuBefore
				runtime.ReadMemStats(&after)
				runtime.KeepAlive(s)
				return map[string]float64{
					"gc-cpu-us/op": cpuSeconds * 1e6 / catalogGCOpCount,
					"gc-cycles":    float64(after.NumGC - before.NumGC),
					"ops/s":        catalogGCOpCount / elapsed.Seconds(),
				}
			})
		})
	}
}
