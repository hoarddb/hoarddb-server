package storage

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// dirSpaceModel is a filesystem of a fixed size whose only files are the log
// directory's: its free space is what the files there do not take. A test
// reads it through the statfs seam, so a spare that keeps its blocks shows.
type dirSpaceModel struct {
	dir   string
	total uint64
}

func (m dirSpaceModel) free(string) (uint64, error) {
	entries, err := os.ReadDir(m.dir)
	if err != nil {
		return 0, err
	}
	used := uint64(0)
	for _, entry := range entries {
		info, err := entry.Info()
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return 0, err
		}
		used += uint64(info.Size())
	}
	if used >= m.total {
		return 0, nil
	}
	return m.total - used, nil
}

// TestWALPreallocPartialFillIsDeleted: ext4 keeps the blocks a failed
// fallocate managed to allocate (NR-1). A fill whose allocator got a third of
// the segment and then ran out deletes wal_spare.tmp before the failure is
// reported, so the free space is what it was before the fill, and the spare is
// idle. The filler tries again after its retry interval, and only then: a
// second attempt happens once the interval has passed, and that one fills the
// spare. A process that dies after the partial allocation leaves the
// wal_spare.tmp to the next open, whose B1 deletes it. Break by: leaving the
// file on the failure path; deleting it only on the next retry; a filler that
// never retries after a failure (S7 verifier FIX-1).
func TestWALPreallocPartialFillIsDeleted(t *testing.T) {
	const size = 3 << 20
	partialAllocator := func(f *os.File, segment int64) error {
		if _, err := f.WriteAt(make([]byte, segment/3), 0); err != nil {
			return err
		}
		return errNoRoom
	}

	t.Run("the failed fill gives the space back and the filler retries", func(t *testing.T) {
		dir := t.TempDir()
		space := dirSpaceModel{dir: dir, total: 1 << 30}
		var full atomic.Bool
		var attempts atomic.Int64
		w, gate := openWithHeldFiller(t, dir, size, func(sys *walSyscalls) {
			sys.statfsFree = space.free
			sys.fillRetry = 20 * time.Millisecond
			realFallocate := sys.fallocate
			sys.fallocate = func(f *os.File, segment int64) error {
				if full.Load() {
					attempts.Add(1)
					// One partial allocation, then the disk has room again: the
					// retry is what fills the spare.
					full.Store(false)
					return partialAllocator(f, segment)
				}
				return realFallocate(f, segment)
			}
		})
		defer closeWAL(t, w)
		freeBefore, err := space.free(dir)
		if err != nil {
			t.Fatal(err)
		}
		full.Store(true)
		watch := watchFill(t, w)
		gate.open()
		if err := watch.wait(); !errors.Is(err, syscall.ENOSPC) {
			t.Fatalf("the fill whose allocation ran out ended with %v, want ENOSPC", err)
		}
		// Checked at the moment the failure is reported: before the retry.
		requireAbsent(t, filepath.Join(dir, walSpareTmpName), "when the partial fill's failure is reported")
		freeAfter, err := space.free(dir)
		if err != nil {
			t.Fatal(err)
		}
		if freeAfter != freeBefore {
			t.Fatalf("%d bytes are free after the failed fill and %d were before it: the partial allocation was kept", freeAfter, freeBefore)
		}
		waitForReadySpare(t, w)
		if got := attempts.Load(); got != 1 {
			t.Fatalf("%d partial allocations, want 1", got)
		}
	})

	t.Run("a process that dies after the partial allocation", func(t *testing.T) {
		dir := t.TempDir()
		var dying, allocated atomic.Bool
		w, gate := openWithHeldFiller(t, dir, size, func(sys *walSyscalls) {
			realFallocate, realRemove := sys.fallocate, sys.remove
			sys.fallocate = func(f *os.File, segment int64) error {
				if dying.Load() {
					allocated.Store(true)
					return partialAllocator(f, segment)
				}
				return realFallocate(f, segment)
			}
			// The process dies after the partial allocation, before the
			// delete: the leftover stays.
			sys.remove = func(path string) error {
				if dying.Load() && allocated.Load() && filepath.Base(path) == walSpareTmpName {
					return fmt.Errorf("the process died before removing %s", walSpareTmpName)
				}
				return realRemove(path)
			}
		})
		dying.Store(true)
		watch := watchFill(t, w)
		gate.open()
		if err := watch.wait(); !errors.Is(err, syscall.ENOSPC) {
			t.Fatalf("the dying fill ended with %v, want ENOSPC", err)
		}
		if info, err := os.Stat(filepath.Join(dir, walSpareTmpName)); err != nil || info.Size() == 0 {
			t.Fatalf("the scenario needs the partial %s left behind: %v, %v", walSpareTmpName, info, err)
		}
		abandonWAL(w)
		dying.Store(false)

		var removed atomic.Bool
		reopened, gate := openWithHeldFiller(t, dir, size, func(sys *walSyscalls) {
			realRemove := sys.remove
			sys.remove = func(path string) error {
				if filepath.Base(path) == walSpareTmpName {
					removed.Store(true)
				}
				return realRemove(path)
			}
		})
		defer closeWAL(t, reopened)
		defer gate.open()
		if !removed.Load() {
			t.Fatalf("the open never removed the leftover %s", walSpareTmpName)
		}
		requireAbsent(t, filepath.Join(dir, walSpareTmpName), "after the open that follows the dead process")
	})
}

// lockedBuffer is a log destination a background goroutine writes while the
// test reads it.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) lines(substr string) []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	var found []string
	for _, line := range strings.Split(b.buf.String(), "\n") {
		if strings.Contains(line, substr) {
			found = append(found, line)
		}
	}
	return found
}

// captureLog sends the default logger to a buffer for the rest of t.
func captureLog(t *testing.T) *lockedBuffer {
	t.Helper()
	logged := &lockedBuffer{}
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(logged, &slog.HandlerOptions{Level: slog.LevelInfo})))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return logged
}

// TestWALFillFailureWarnsOnceThenHourly: a fill that keeps failing is one WARN,
// then at most one an hour with the count of failures since the last, not one
// every 10 s for as long as the disk is full (spec §4.3, S7 verifier ADV-2);
// the first fill that succeeds after a failure logs one INFO. Break by: a WARN
// per failure; no count; no INFO on success, or one per success.
func TestWALFillFailureWarnsOnceThenHourly(t *testing.T) {
	logged := captureLog(t)
	clock := newWALTestClock()
	var full atomic.Bool
	var attempts atomic.Int64
	w, gate := openWithHeldFiller(t, t.TempDir(), spareTestSegment, func(sys *walSyscalls) {
		sys.now = clock.read
		sys.fillRetry = time.Millisecond
		real := sys.statfsFree
		sys.statfsFree = func(dirPath string) (uint64, error) {
			if full.Load() {
				attempts.Add(1)
				return 0, nil
			}
			return real(dirPath)
		}
	})
	defer closeWAL(t, w)
	waitForAttempts := func(n int64) {
		t.Helper()
		deadline := time.Now().Add(fillWaitLimit)
		for attempts.Load() < n {
			if time.Now().After(deadline) {
				t.Fatalf("%d fill attempts within %v, want %d: the background filler did not retry", attempts.Load(), fillWaitLimit, n)
			}
			time.Sleep(time.Millisecond)
		}
	}
	const warning = "could not prepare the next write-ahead log segment"
	full.Store(true)
	gate.open()
	waitForAttempts(5)
	// The fifth attempt has been counted; wait for its WARN decision too.
	time.Sleep(20 * time.Millisecond)
	if got := logged.lines(warning); len(got) != 1 {
		t.Fatalf("%d WARN lines after 5 failed fills in the same hour, want 1:\n%s", len(got), strings.Join(got, "\n"))
	}

	clock.advance(time.Hour)
	seen := attempts.Load()
	waitForAttempts(seen + 2)
	// The second WARN is logged after its attempt is counted, so wait for the
	// line itself; a short fixed sleep is not enough on a loaded machine.
	warnDeadline := time.Now().Add(fillWaitLimit)
	for len(logged.lines(warning)) < 2 && time.Now().Before(warnDeadline) {
		time.Sleep(time.Millisecond)
	}
	warned := logged.lines(warning)
	if len(warned) != 2 {
		t.Fatalf("%d WARN lines after an hour of failed fills, want 2:\n%s", len(warned), strings.Join(warned, "\n"))
	}
	if !strings.Contains(warned[1], "failures_since_last_warning=") || strings.Contains(warned[1], "failures_since_last_warning=0 ") {
		t.Fatalf("the hourly WARN does not count the failures since the last one: %s", warned[1])
	}

	full.Store(false)
	waitForReadySpare(t, w)
	const recovered = "prepared the next write-ahead log segment again"
	if got := logged.lines(recovered); len(got) != 1 {
		t.Fatalf("%d INFO lines when a fill succeeded after failures, want 1:\n%s", len(got), strings.Join(got, "\n"))
	}
	// A rotation takes the spare and the next fill succeeds: no second INFO.
	fillToRotationPoint(t, w)
	if _, err := appendRotationEntry(w, 999); err != nil {
		t.Fatalf("the rotating append: %v", err)
	}
	waitForReadySpare(t, w)
	if got := logged.lines(recovered); len(got) != 1 {
		t.Fatalf("%d recovery INFO lines after a second successful fill, want 1", len(got))
	}
}

// TestWALStatsSnapshot: the counters behind the node's WAL metrics (spec §8)
// move as the log works: each group commit is a flush, in the bucket of its
// duration (0.5 ms to 1 s, doubling, cumulative as Prometheus reads them);
// each rotation counts; a rotation that waited for the fill in flight counts
// as a wait with its time; fills count their zeros and their time; a failed
// fill counts as a failure; the spare's readiness and the mode are reported.
// A log that does not preallocate fills nothing. Break by: any counter not
// moved where its event happens, or buckets that are not cumulative.
func TestWALStatsSnapshot(t *testing.T) {
	t.Run("off", func(t *testing.T) {
		w := openBirthWAL(t, t.TempDir(), nil)
		defer closeWAL(t, w)
		appended := fillToRotationPoint(t, w)
		if _, err := appendRotationEntry(w, appended); err != nil {
			t.Fatalf("the rotating append: %v", err)
		}
		stats := w.Stats()
		if stats.Preallocated || stats.SpareReady || stats.ZeroFillBytes != 0 || stats.RotationWaits != 0 {
			t.Fatalf("a log that does not preallocate reports preallocation: %+v", stats)
		}
		if stats.Flushes != uint64(appended+1) || stats.Rotations != 1 {
			t.Fatalf("%d appends and one rotation reported %d flushes and %d rotations", appended+1, stats.Flushes, stats.Rotations)
		}
	})

	t.Run("on", func(t *testing.T) {
		// The open fills its first segment (chunk 1); the filler's fill is
		// held at its first chunk (chunk 2).
		// The third fill, the one after the rotation, fails its B6 fsync.
		gate := newFillGate("chunk", 2)
		var begins atomic.Int64
		var failed atomic.Bool
		withOpenSyscalls(t, func(sys *walSyscalls) {
			sys.fillStage = func(stage string) {
				if stage == "begin" {
					begins.Add(1)
				}
				gate.hook(stage)
			}
			real := sys.fsync
			sys.fsync = func(f *os.File) error {
				if filepath.Base(f.Name()) == walSpareTmpName && begins.Load() == 3 && failed.CompareAndSwap(false, true) {
					return fmt.Errorf("simulated EIO on a spare's fsync: %w", syscall.EIO)
				}
				return real(f)
			}
		})
		w := openPreallocatedWAL(t, t.TempDir(), nil, spareTestSegment)
		defer closeWAL(t, w)
		defer gate.open()
		gate.waitHeld(t)

		stats := w.Stats()
		if !stats.Preallocated || stats.SpareReady {
			t.Fatalf("with the filler held mid-fill: Preallocated %v, SpareReady %v; want true, false", stats.Preallocated, stats.SpareReady)
		}
		if want := uint64(spareTestSegment - walFileHeaderLen); stats.ZeroFillBytes < want || stats.ZeroFillTime <= 0 {
			t.Fatalf("after the open's fill: ZeroFillBytes %d (want at least %d), ZeroFillTime %v", stats.ZeroFillBytes, want, stats.ZeroFillTime)
		}

		appended := fillToRotationPoint(t, w)
		stats = w.Stats()
		if stats.Flushes != uint64(appended) {
			t.Fatalf("%d appends reported %d flushes", appended, stats.Flushes)
		}
		if stats.FlushTime <= 0 {
			t.Fatalf("%d flushes took %v in total", stats.Flushes, stats.FlushTime)
		}
		for i := 1; i < WALFlushBuckets; i++ {
			if stats.FlushesAtMost[i] < stats.FlushesAtMost[i-1] {
				t.Fatalf("the flush buckets are not cumulative: %v", stats.FlushesAtMost)
			}
		}
		if last := stats.FlushesAtMost[WALFlushBuckets-1]; last > stats.Flushes || last == 0 {
			t.Fatalf("%d flushes, %d of them at most %v: want them in the buckets (%v)", stats.Flushes, last, WALFlushBucketBounds()[WALFlushBuckets-1], stats.FlushesAtMost)
		}

		// The rotation waits for the held fill.
		rotated := make(chan error, 1)
		go func() { _, err := appendRotationEntry(w, appended); rotated <- err }()
		waitForGoroutineIn(t, "storage.(*WAL).prepareSpareSegment", "[chan receive")
		time.Sleep(5 * time.Millisecond)
		gate.open()
		if err := <-rotated; err != nil {
			t.Fatalf("the rotating append: %v", err)
		}
		stats = w.Stats()
		if stats.Rotations != 1 || stats.RotationWaits != 1 || stats.RotationWaitTime < 5*time.Millisecond {
			t.Fatalf("one rotation that waited for the fill: Rotations %d, RotationWaits %d, RotationWaitTime %v", stats.Rotations, stats.RotationWaits, stats.RotationWaitTime)
		}

		// The rotation took the spare; the next fill fails.
		deadline := time.Now().Add(fillWaitLimit)
		for !failed.Load() || w.Stats().SpareFillFailures == 0 {
			if time.Now().After(deadline) {
				t.Fatalf("the fill after the rotation did not fail within %v (failed %v, SpareFillFailures %d)", fillWaitLimit, failed.Load(), w.Stats().SpareFillFailures)
			}
			time.Sleep(time.Millisecond)
		}
		if got := w.Stats().SpareFillFailures; got != 1 {
			t.Fatalf("one failed fill reported %d failures", got)
		}
	})

	t.Run("spare ready", func(t *testing.T) {
		w := openPreallocatedWAL(t, t.TempDir(), nil, spareTestSegment)
		defer closeWAL(t, w)
		waitForReadySpare(t, w)
		if !w.Stats().SpareReady {
			t.Fatalf("SpareReady is false with %s on disk", walSpareReadyName)
		}
	})
}
