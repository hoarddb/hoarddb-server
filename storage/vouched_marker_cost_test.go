package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestTheMarkerWriteCostQuietAndUnderASeedingLoad is M0 of the vouched cut (review R, §4): the time
// of one marker advance, the rename marker of format 1 (WriteFileDurably of its 32 bytes: a
// temporary file, its fsync, the rename, the directory's fsync) against the in-place slot of format 2
// (writeMarkerSlot and syncMarkerFile, as BadgerKV.advanceMarker calls them, on a file of two
// slots), interleaved, on a quiet disk and under a seeding-like load: two workers committing synced
// 64 KiB batches into a store on the same disk, as a joiner's two seed workers do. It measures; it
// asserts nothing, and runs only with HOARDDB_MARKER_COST=1 (on one quiet VM at a time: the shared
// host's disk is never quiet). HOARDDB_MARKER_COST_ADVANCES (default 1000) and
// HOARDDB_MARKER_COST_RUNS (default 3) size it.
func TestTheMarkerWriteCostQuietAndUnderASeedingLoad(t *testing.T) {
	if os.Getenv("HOARDDB_MARKER_COST") != "1" {
		t.Skip("M0, a measurement: set HOARDDB_MARKER_COST=1 and run it on a quiet VM")
	}
	advances := envInt(t, "HOARDDB_MARKER_COST_ADVANCES", 1000)
	runs := envInt(t, "HOARDDB_MARKER_COST_RUNS", 3)
	for run := 1; run <= runs; run++ {
		for _, loaded := range []bool{false, true} {
			rename, slot, loadCommits := measureMarkerWrites(t, advances, loaded)
			condition := "quiet"
			if loaded {
				condition = fmt.Sprintf("loaded (2 workers, %d synced 64 KiB commits meanwhile)", loadCommits)
			}
			t.Logf("M0 run %d, %s, %d advances each: rename marker %s; in-place slot %s", run, condition, advances,
				describeDurations(rename), describeDurations(slot))
		}
	}
}

func envInt(t *testing.T, name string, fallback int) int {
	t.Helper()
	raw := os.Getenv(name)
	if raw == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(raw)
	noErr(t, err, "%s", name)
	return parsed
}

// measureMarkerWrites times advances of each kind, alternating which goes first, and returns the
// durations and how many synced commits the load made meanwhile.
func measureMarkerWrites(t *testing.T, advances int, loaded bool) (rename, slot []time.Duration, loadCommits int64) {
	t.Helper()
	base := t.TempDir()
	renameDir, slotDir := filepath.Join(base, "rename"), filepath.Join(base, "slot")
	noErr(t, os.MkdirAll(renameDir, 0o700))
	noErr(t, os.MkdirAll(slotDir, 0o700))
	renamePath := filepath.Join(renameDir, vouchedFileName)
	format1 := make([]byte, vouchedFormat1Size)
	noErr(t, WriteFileDurably(renamePath, format1))
	noErr(t, WriteFileDurably(filepath.Join(slotDir, vouchedFileName), newVouchedFileBytes(vouchedMarker{floor: 1})))
	file, err := os.OpenFile(filepath.Join(slotDir, vouchedFileName), os.O_RDWR, 0)
	noErr(t, err)
	defer func() { noErr(t, file.Close()) }()

	stop := make(chan struct{})
	var workers sync.WaitGroup
	var commits atomic.Int64
	var loadErr atomic.Pointer[error]
	if loaded {
		kv, err := OpenBadgerKV(filepath.Join(base, "load"), BadgerTuning{MemTableMB: 64}, nil)
		noErr(t, err)
		defer func() { noErr(t, kv.Close()) }()
		value := make([]byte, 2<<10)
		for worker := range 2 {
			workers.Add(1)
			go func() {
				defer workers.Done()
				for batchNumber := 0; ; batchNumber++ {
					select {
					case <-stop:
						return
					default:
					}
					batch := kv.NewBatch()
					for i := range 32 {
						if err := batch.Set([]byte(fmt.Sprintf("w%d/%09d/%02d", worker, batchNumber, i)), value); err != nil {
							loadErr.Store(&err)
							return
						}
					}
					if err := batch.Commit(true); err != nil {
						loadErr.Store(&err)
						return
					}
					commits.Add(1)
				}
			}()
		}
		// Let the load reach its pace before the first advance.
		time.Sleep(2 * time.Second)
	}
	defer func() {
		close(stop)
		workers.Wait()
		if failure := loadErr.Load(); failure != nil {
			t.Fatalf("the load failed: %v", *failure)
		}
	}()

	timeRename := func() {
		start := time.Now()
		noErr(t, WriteFileDurably(renamePath, format1))
		rename = append(rename, time.Since(start))
	}
	timeSlot := func(sequence uint64) {
		encoded := vouchedSlot{vouchedMarker: vouchedMarker{version: sequence, floor: 1}, sequence: sequence + 1}.encode()
		start := time.Now()
		noErr(t, writeMarkerSlot(file, encoded, int64(sequence%2)*vouchedSlotSize))
		noErr(t, syncMarkerFile(file))
		slot = append(slot, time.Since(start))
	}
	commitsBefore := commits.Load()
	for i := range advances {
		if i%2 == 0 {
			timeRename()
			timeSlot(uint64(i))
		} else {
			timeSlot(uint64(i))
			timeRename()
		}
	}
	return rename, slot, commits.Load() - commitsBefore
}

// describeDurations is the median, the p99, the mean and the largest of durations, in ms.
func describeDurations(durations []time.Duration) string {
	sorted := slices.Clone(durations)
	slices.Sort(sorted)
	var total time.Duration
	for _, d := range sorted {
		total += d
	}
	ms := func(d time.Duration) string { return fmt.Sprintf("%.2f", float64(d)/float64(time.Millisecond)) }
	at := func(q float64) time.Duration { return sorted[min(len(sorted)-1, int(q*float64(len(sorted))))] }
	return fmt.Sprintf("median %s ms, p99 %s ms, mean %s ms, max %s ms", ms(at(0.5)), ms(at(0.99)),
		ms(total/time.Duration(len(sorted))), ms(sorted[len(sorted)-1]))
}
