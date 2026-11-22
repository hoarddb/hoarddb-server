package wire

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
)

// The benchmarks measure a node whose log and Badger fsync, so they mean something only on
// a real disk. HOARDDB_BENCH_DIR names the directory they build their node under (default:
// the temp dir, which is tmpfs on most Linux hosts, where fsync is free and the numbers
// are meaningless, so a tmpfs run is skipped unless HOARDDB_BENCH_ALLOW_TMPFS=1).
// Every benchmark runs once per table-sync mode, stepwise and msync, as sub-benchmarks, so
// the mode is in the name of every result line; HOARDDB_BENCH_TABLE_SYNC=stepwise|msync
// runs one mode only. Each runs at least benchMinOps operations so the p99.9 means
// something: use -benchtime=1x, because the operation count is fixed here, not b.N.
const (
	benchMinOps      = 20000
	benchParallelism = 16
	benchPreloaded   = 1000
)

var benchTableSyncModes = []string{storage.TableSyncStepwise, storage.TableSyncMsync}

// benchRoot is the directory a benchmark builds under, with the filesystem it is on
// logged. It skips the benchmark on tmpfs unless asked not to.
func benchRoot(b *testing.B) string {
	b.Helper()
	root := os.Getenv("HOARDDB_BENCH_DIR")
	if root == "" {
		root = os.TempDir()
	}
	root, err := filepath.Abs(root)
	if err != nil {
		b.Fatalf("resolving HOARDDB_BENCH_DIR %q: %v", root, err)
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		b.Fatalf("creating HOARDDB_BENCH_DIR %q: %v", root, err)
	}
	fsType := filesystemOf(root)
	b.Logf("benchmark directory %s on filesystem %s", root, fsType)
	if fsType == "tmpfs" && os.Getenv("HOARDDB_BENCH_ALLOW_TMPFS") != "1" {
		b.Skipf("%s is tmpfs, where fsync costs nothing and the numbers mean nothing. Set HOARDDB_BENCH_DIR to a directory on a real disk, or HOARDDB_BENCH_ALLOW_TMPFS=1 to run here anyway", root)
	}
	return root
}

// filesystemOf is the filesystem type of the mount that holds path, "unknown" when the
// host has no /proc/self/mountinfo.
func filesystemOf(path string) string {
	if resolved, err := filepath.EvalSymlinks(path); err == nil {
		path = resolved
	}
	mounts, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		return "unknown"
	}
	return mountFilesystem(string(mounts), path)
}

// mountFilesystem finds the type of the deepest mount in a mountinfo listing that
// contains path.
func mountFilesystem(mountinfo, path string) string {
	best, bestLen := "unknown", -1
	for _, line := range strings.Split(mountinfo, "\n") {
		head, tail, found := strings.Cut(line, " - ")
		fields := strings.Fields(head)
		kind := strings.Fields(tail)
		if !found || len(fields) < 5 || len(kind) < 1 {
			continue
		}
		mount := fields[4]
		inside := mount == "/" || path == mount || strings.HasPrefix(path, mount+"/")
		if inside && len(mount) > bestLen {
			best, bestLen = kind[0], len(mount)
		}
	}
	return best
}

// benchModes runs run once per table-sync mode as a sub-benchmark named after the mode.
func benchModes(b *testing.B, run func(b *testing.B, tableSync string)) {
	b.Helper()
	only := os.Getenv("HOARDDB_BENCH_TABLE_SYNC")
	for _, mode := range benchTableSyncModes {
		if only != "" && only != mode {
			continue
		}
		b.Run("tablesync="+mode, func(b *testing.B) { run(b, mode) })
	}
}

// benchNode is a fresh node with database app and bucket User under the benchmark
// directory, opened with the table-sync mode given explicitly.
func benchNode(b *testing.B, tableSync string) *Node {
	b.Helper()
	dir, err := os.MkdirTemp(benchRoot(b), "hoarddb-bench-")
	if err != nil {
		b.Fatalf("creating the benchmark directory: %v", err)
	}
	b.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			b.Errorf("removing %s: %v", dir, err)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	node, err := OpenNode(ctx, NodeConfig{Dir: dir, TableSync: tableSync})
	if err != nil {
		b.Fatalf("OpenNode(%s): %v", dir, err)
	}
	b.Cleanup(func() {
		if err := node.Close(); err != nil {
			b.Errorf("closing the node: %v", err)
		}
	})
	u30iSchema(b, node)
	b.Logf("table sync %s, %d CPUs, directory %s", tableSync, runtime.NumCPU(), dir)
	return node
}

// u30iBenchUser is the three-field document the benchmarks write.
func u30iBenchUser(id string, age int32) bson.D {
	return bson.D{{Key: "_id_", Value: id}, {Key: "Name", Value: "Ada Lovelace"}, {Key: "Age", Value: age}, {Key: "City", Value: "London"}}
}

// benchPercentile is the q quantile (0 to 1) of ascending latencies, nearest rank.
func benchPercentile(sorted []time.Duration, q float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	rank := int(q*float64(len(sorted))+0.999999999) - 1
	return sorted[min(max(rank, 0), len(sorted)-1)]
}

// benchReport publishes the latencies of one run as p50, p99 and p99.9 and replaces the
// per-b.N ns/op, which is meaningless when the operation count is fixed, with the mean.
func benchReport(b *testing.B, tableSync string, latencies []time.Duration, elapsed time.Duration) {
	b.Helper()
	slices.Sort(latencies)
	b.ReportMetric(float64(elapsed.Nanoseconds())/float64(len(latencies)), "ns/op")
	b.ReportMetric(float64(benchPercentile(latencies, 0.50).Nanoseconds()), "p50-ns")
	b.ReportMetric(float64(benchPercentile(latencies, 0.99).Nanoseconds()), "p99-ns")
	b.ReportMetric(float64(benchPercentile(latencies, 0.999).Nanoseconds()), "p99.9-ns")
	b.ReportMetric(float64(len(latencies))/elapsed.Seconds(), "ops/s")
	b.ReportMetric(float64(len(latencies)), "ops")
	b.Logf("table sync %s: %d ops in %s", tableSync, len(latencies), elapsed.Round(time.Millisecond))
}

// benchRun runs op benchMinOps times on workers goroutines, op(worker, i) being the i-th
// call of that worker, and reports the latency distribution.
func benchRun(b *testing.B, tableSync string, workers int, op func(worker, i int) error) {
	b.Helper()
	perWorker := benchMinOps / workers
	latencies := make([]time.Duration, 0, perWorker*workers)
	var mu sync.Mutex
	var wg sync.WaitGroup
	failures := make(chan error, workers)
	b.ResetTimer()
	start := time.Now()
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			own := make([]time.Duration, 0, perWorker)
			for i := 0; i < perWorker; i++ {
				began := time.Now()
				if err := op(worker, i); err != nil {
					failures <- err
					return
				}
				own = append(own, time.Since(began))
			}
			mu.Lock()
			latencies = append(latencies, own...)
			mu.Unlock()
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	b.StopTimer()
	close(failures)
	for err := range failures {
		b.Fatalf("a benchmark operation failed: %v", err)
	}
	benchReport(b, tableSync, latencies, elapsed)
}

func BenchmarkInsert(b *testing.B) {
	benchModes(b, func(b *testing.B, tableSync string) {
		node := benchNode(b, tableSync)
		ctx := context.Background()
		benchRun(b, tableSync, 1, func(_, i int) error {
			_, err := node.Insert(ctx, u30iDB, "User", u30iBenchUser(fmt.Sprintf("u%d", i), int32(i)))
			return err
		})
	})
}

func BenchmarkInsertParallel16(b *testing.B) {
	benchModes(b, func(b *testing.B, tableSync string) {
		node := benchNode(b, tableSync)
		ctx := context.Background()
		benchRun(b, tableSync, benchParallelism, func(worker, i int) error {
			_, err := node.Insert(ctx, u30iDB, "User", u30iBenchUser(fmt.Sprintf("p%d-%d", worker, i), int32(i)))
			return err
		})
	})
}

func BenchmarkGet(b *testing.B) {
	benchModes(b, func(b *testing.B, tableSync string) {
		node := benchNode(b, tableSync)
		ctx := context.Background()
		for i := 0; i < benchPreloaded; i++ {
			if _, err := node.Insert(ctx, u30iDB, "User", u30iBenchUser(fmt.Sprintf("u%d", i), int32(i))); err != nil {
				b.Fatalf("Insert: %v", err)
			}
		}
		benchRun(b, tableSync, 1, func(_, i int) error {
			_, err := node.Get(ctx, u30iDB, "User", fmt.Sprintf("u%d", i%benchPreloaded))
			return err
		})
	})
}

func BenchmarkUpsertSameDoc(b *testing.B) {
	benchModes(b, func(b *testing.B, tableSync string) {
		node := benchNode(b, tableSync)
		ctx := context.Background()
		benchRun(b, tableSync, 1, func(_, i int) error {
			_, err := node.Upsert(ctx, u30iDB, "User", u30iBenchUser("same", int32(i)))
			return err
		})
	})
}

func TestBenchPercentile(t *testing.T) {
	latencies := make([]time.Duration, 1000)
	for i := range latencies {
		latencies[i] = time.Duration(i+1) * time.Millisecond
	}
	for q, want := range map[float64]time.Duration{0.50: 500 * time.Millisecond, 0.99: 990 * time.Millisecond, 0.999: 999 * time.Millisecond, 1: 1000 * time.Millisecond} {
		if got := benchPercentile(latencies, q); got != want {
			t.Errorf("quantile %v = %v, want %v", q, got, want)
		}
	}
	if got := benchPercentile(nil, 0.5); got != 0 {
		t.Errorf("quantile of nothing = %v, want 0", got)
	}
}

func TestMountFilesystemPicksTheDeepestMountThatHoldsThePath(t *testing.T) {
	mounts := strings.Join([]string{
		"26 1 252:0 / / rw,relatime shared:1 - ext4 /dev/mapper/root rw",
		"30 26 0:25 / /tmp rw,nosuid shared:5 - tmpfs tmpfs rw,size=15G",
		"31 26 0:26 / /tmpfs-not rw shared:6 - xfs /dev/sdb rw",
	}, "\n")
	for path, want := range map[string]string{
		"/tmp/x/y":         "tmpfs",
		"/tmp":             "tmpfs",
		"/tmpfs-not/data":  "xfs",
		"/mnt/data/.cache": "ext4",
		"/tmpfoo":          "ext4",
	} {
		if got := mountFilesystem(mounts, path); got != want {
			t.Errorf("filesystem of %s = %q, want %q", path, got, want)
		}
	}
}
