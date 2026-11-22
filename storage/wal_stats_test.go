package storage

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"syscall"
	"testing"
)

// countedRun is one log written by runCountedLog: its directory, the counters
// read before it closed, and the entry bytes its segment files hold, which is
// their total length less one header per segment.
type countedRun struct {
	dir        string
	stats      WALStats
	entryBytes int64
}

// runCountedLog opens a fresh log on keys, appends a few entries of different
// bucket, key and data sizes, and then, when rotate is set, fills the first
// segment and makes one rotating append, so the log ends on a second segment.
func runCountedLog(t *testing.T, keys WALKeySource, rotate bool) countedRun {
	t.Helper()
	dir := t.TempDir()
	w := openBirthWAL(t, dir, keys)
	first := w.activeIndex
	for i, size := range []int{0, 1, 100, 1000} {
		bucket := fmt.Sprintf("bucket-%d", i)
		key := fmt.Sprintf("k%d", i)
		if _, err := w.Append(bucket, OpInsert, []byte(key), bytes.Repeat([]byte{'x'}, size)); err != nil {
			t.Fatalf("Append %d (%d data bytes): %v", i, size, err)
		}
	}
	if rotate {
		appended := fillToRotationPoint(t, w)
		if _, err := appendRotationEntry(w, appended); err != nil {
			t.Fatalf("the rotating append: %v", err)
		}
		if w.activeIndex != first+1 {
			t.Fatalf("the rotating append left segment %d active, want %d", w.activeIndex, first+1)
		}
	}
	stats := w.Stats()
	last := w.activeIndex
	closeBirthWAL(t, w)

	var segmentBytes int64
	for idx := first; idx <= last; idx++ {
		info, err := os.Stat(segmentFilePath(dir, idx))
		if err != nil {
			t.Fatalf("stat segment %d: %v", idx, err)
		}
		segmentBytes += info.Size()
	}
	headers := int64(last-first+1) * walFileHeaderLen
	return countedRun{dir: dir, stats: stats, entryBytes: segmentBytes - headers}
}

// TestWALStatsCountsAppendedBytes: BytesAppended is the framed length of every
// entry written successfully, so the file-size identity is its oracle. A failed
// write is not counted, and a reopen starts from zero. Break by: counting the
// frame word only, or counting before the write succeeds, or counting the body.
func TestWALStatsCountsAppendedBytes(t *testing.T) {
	t.Run("plain", func(t *testing.T) {
		run := runCountedLog(t, nil, true)
		if run.stats.BytesAppended != uint64(run.entryBytes) {
			t.Fatalf("BytesAppended %d, but the segment files hold %d entry bytes across two segments",
				run.stats.BytesAppended, run.entryBytes)
		}
	})

	t.Run("sealed", func(t *testing.T) {
		sealed := runCountedLog(t, newSealedTestKeySource(t), true)
		if sealed.stats.BytesAppended != uint64(sealed.entryBytes) {
			t.Fatalf("sealed: BytesAppended %d, but the segment files hold %d entry bytes",
				sealed.stats.BytesAppended, sealed.entryBytes)
		}
		plain := runCountedLog(t, nil, false)
		sealedSmall := runCountedLog(t, newSealedTestKeySource(t), false)
		if sealedSmall.stats.BytesAppended <= plain.stats.BytesAppended {
			t.Fatalf("the same entries: sealed BytesAppended %d is not greater than plain %d",
				sealedSmall.stats.BytesAppended, plain.stats.BytesAppended)
		}
		if plain.stats.BytesAppended != uint64(plain.entryBytes) || sealedSmall.stats.BytesAppended != uint64(sealedSmall.entryBytes) {
			t.Fatalf("single segment: plain %d of %d bytes, sealed %d of %d bytes",
				plain.stats.BytesAppended, plain.entryBytes, sealedSmall.stats.BytesAppended, sealedSmall.entryBytes)
		}
	})

	t.Run("failed write is not counted", func(t *testing.T) {
		var failNext atomic.Bool
		withOpenSyscalls(t, func(sys *walSyscalls) {
			real := sys.writeAt
			sys.writeAt = func(file *os.File, entry []byte, offset int64) (int, error) {
				if failNext.CompareAndSwap(true, false) {
					return 0, fmt.Errorf("simulated EIO on an entry write: %w", syscall.EIO)
				}
				return real(file, entry, offset)
			}
		})
		w := openBirthWAL(t, t.TempDir(), nil)
		defer closeWAL(t, w)
		if _, err := w.Append("bucket", OpInsert, []byte("ok"), []byte("first")); err != nil {
			t.Fatalf("the first append: %v", err)
		}
		before := w.Stats().BytesAppended
		failNext.Store(true)
		if _, err := w.Append("bucket", OpInsert, []byte("bad"), []byte("second")); !errors.Is(err, syscall.EIO) {
			t.Fatalf("the failed append returned %v, want an error wrapping EIO", err)
		}
		if after := w.Stats().BytesAppended; after != before {
			t.Fatalf("a failed append moved BytesAppended from %d to %d", before, after)
		}
	})

	t.Run("starts at zero on reopen", func(t *testing.T) {
		run := runCountedLog(t, nil, true)
		if run.stats.BytesAppended == 0 {
			t.Fatal("the writing run counted no bytes, so the reopen has nothing to reset")
		}
		w := openBirthWAL(t, run.dir, nil)
		got := w.Stats().BytesAppended
		closeBirthWAL(t, w)
		if got != 0 {
			t.Fatalf("a reopened log reports BytesAppended %d, want 0", got)
		}
	})
}
