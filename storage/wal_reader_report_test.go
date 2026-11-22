package storage

import (
	"flag"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// The cost report is not a pass/fail test. It measures what the build doc asks
// a builder to report (replication-stream-build.md 6.5 and SA-1.3) and prints
// it; run it on a real disk, because fsync is the cost being measured:
//
//	go test -run TestWALReadBatchCostReport -wal-report -wal-report-dir=/path/on/the/disk ./storage/ -v
var (
	walReport    = flag.Bool("wal-report", false, "run the ReadBatch cost report (slow, writes about 1.1 GiB)")
	walReportDir = flag.String("wal-report-dir", "", "directory for the cost report's logs; default is the test's temporary directory")
)

func reportDir(t *testing.T, name string) string {
	t.Helper()
	if *walReportDir == "" {
		return t.TempDir()
	}
	dir, err := os.MkdirTemp(*walReportDir, name)
	if err != nil {
		t.Fatalf("creating a report directory: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

// fillLog appends entries of entryBytes until the log holds about totalBytes,
// from 16 appenders so the fsync is shared as it is in production.
func fillLog(t *testing.T, w *WAL, totalBytes, entryBytes int) int {
	t.Helper()
	perAppender := totalBytes / entryBytes / 16
	payload := []byte(strings.Repeat("d", entryBytes))
	var wg sync.WaitGroup
	for a := 0; a < 16; a++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perAppender; i++ {
				if _, err := w.Append("bucket", OpInsert, []byte("k"), payload); err != nil {
					t.Errorf("Append: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
	return perAppender * 16
}

// appendLatencies runs 16 appenders for duration and returns every append's latency.
func appendLatencies(w *WAL, duration time.Duration, entryBytes int, stop <-chan struct{}) []time.Duration {
	payload := []byte(strings.Repeat("a", entryBytes))
	var mu sync.Mutex
	var all []time.Duration
	var wg sync.WaitGroup
	deadline := time.Now().Add(duration)
	for a := 0; a < 16; a++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var mine []time.Duration
			for time.Now().Before(deadline) {
				select {
				case <-stop:
					mu.Lock()
					all = append(all, mine...)
					mu.Unlock()
					return
				default:
				}
				started := time.Now()
				if _, err := w.Append("live", OpInsert, []byte("k"), payload); err != nil {
					return
				}
				mine = append(mine, time.Since(started))
			}
			mu.Lock()
			all = append(all, mine...)
			mu.Unlock()
		}()
	}
	wg.Wait()
	return all
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	return sorted[int(float64(len(sorted)-1)*p)]
}

func TestWALReadBatchCostReport(t *testing.T) {
	if !*walReport {
		t.Skip("run with -wal-report to measure")
	}

	t.Run("100 MB plain log, 16 appenders running", func(t *testing.T) {
		w, err := OpenWAL(reportDir(t, "plain"), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = w.Close() }()
		entries := fillLog(t, w, 100<<20, 4<<10)
		upTo := w.SyncedSequence()

		baseline := appendLatencies(w, 3*time.Second, 4<<10, nil)
		sort.Slice(baseline, func(i, j int) bool { return baseline[i] < baseline[j] })

		cursor, err := w.OpenCursor(1)
		if err != nil {
			t.Fatal(err)
		}
		stop := make(chan struct{})
		var read int
		var readTook time.Duration
		var readerDone sync.WaitGroup
		readerDone.Add(1)
		go func() {
			defer readerDone.Done()
			started := time.Now()
			for {
				batch, next, err := w.ReadBatch(cursor, upTo, 4<<20)
				if err != nil {
					t.Errorf("ReadBatch: %v", err)
					break
				}
				if len(batch) == 0 {
					break
				}
				read += len(batch)
				cursor = next
			}
			readTook = time.Since(started)
			close(stop)
		}()
		during := appendLatencies(w, 60*time.Second, 4<<10, stop)
		readerDone.Wait()
		sort.Slice(during, func(i, j int) bool { return during[i] < during[j] })

		t.Logf("log: %d entries of 4 KiB (%d MiB); ReadBatch read %d of them in %v (%.0f MB/s)",
			entries, entries*4/1024, read, readTook, float64(read)*4096/1e6/readTook.Seconds())
		t.Logf("appends with no reader:      n=%d p50=%v p99=%v max=%v", len(baseline), percentile(baseline, .5), percentile(baseline, .99), percentile(baseline, 1))
		t.Logf("appends during the ReadBatch: n=%d p50=%v p99=%v max=%v", len(during), percentile(during, .5), percentile(during, .99), percentile(during, 1))

		// The same read through WAL.ReadFrom, which holds the append lock: the
		// slowest append is the read.
		started := time.Now()
		readFromDone := make(chan struct{})
		go func() {
			defer close(readFromDone)
			if _, err := w.ReadFrom(1); err != nil {
				t.Errorf("ReadFrom: %v", err)
			}
		}()
		time.Sleep(5 * time.Millisecond)
		heldFor := time.Now()
		if _, err := w.Append("live", OpInsert, []byte("k"), []byte("probe")); err != nil {
			t.Fatal(err)
		}
		t.Logf("an append issued 5 ms into WAL.ReadFrom of the same log took %v (ReadFrom took %v in all)", time.Since(heldFor), func() time.Duration { <-readFromDone; return time.Since(started) }())
	})

	t.Run("1 GiB sealed replay", func(t *testing.T) {
		keys := newSealedTestKeySource(t)
		dir := reportDir(t, "sealed")
		w, err := OpenWAL(dir, keys)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = w.Close() }()
		entries := fillLog(t, w, 1<<30, 64<<10)
		upTo := w.SyncedSequence()

		cursor, err := w.OpenCursor(1)
		if err != nil {
			t.Fatal(err)
		}
		started := time.Now()
		read := 0
		for {
			batch, next, err := w.ReadBatch(cursor, upTo, 4<<20)
			if err != nil {
				t.Fatal(err)
			}
			if len(batch) == 0 {
				break
			}
			read += len(batch)
			cursor = next
		}
		took := time.Since(started)
		t.Logf("sealed log: %d entries of 64 KiB (1 GiB); ReadBatch decrypted %d in %v (%.0f MB/s)", entries, read, took, float64(read)*65536/1e6/took.Seconds())

		started = time.Now()
		all, err := w.ReadFrom(1)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("WAL.ReadFrom of the same sealed log: %d entries in %v", len(all), time.Since(started))
	})
}
