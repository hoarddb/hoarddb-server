package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fillSegments appends until the log has at least want segments and returns the
// first sequence of each.
func fillSegments(t *testing.T, w *WAL, want int) []uint64 {
	t.Helper()
	payload := []byte(strings.Repeat("g", 500))
	for i := 0; i < 400 && len(w.listSegments()) < want+1; i++ {
		if _, err := w.Append("docs", OpInsert, []byte("k"), payload); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	var firsts []uint64
	for _, segment := range w.listSegments() {
		first, found, err := w.firstSequenceOf(segment, -1)
		if err != nil || !found {
			t.Fatalf("first sequence of %s: %d, %v, %v", segment.name, first, found, err)
		}
		firsts = append(firsts, first)
	}
	if len(firsts) < want {
		t.Fatalf("only %d segments; need %d", len(firsts), want)
	}
	return firsts
}

// requireNextIsOrHorizon is the contract of a read: the first entry it returns
// is exactly the sequence asked for, or it says the sequence is gone. Returning
// a later one would hide the entries in between from a replica for good.
func requireNextIsOrHorizon(t *testing.T, entries []WALEntry, err error, from uint64) {
	t.Helper()
	if err != nil {
		if !errors.Is(err, ErrBelowHorizon) {
			t.Fatalf("read at sequence %d failed with %v; want entries from %d or ErrBelowHorizon", from, err, from)
		}
		return
	}
	if len(entries) == 0 || entries[0].Seq != from {
		first := uint64(0)
		if len(entries) > 0 {
			first = entries[0].Seq
		}
		t.Fatalf("a read at sequence %d returned %d entries starting at %d: sequences %d..%d were skipped", from, len(entries), first, from, first-1)
	}
}

// TestOpenCursorNeverSkipsWhenTheWantedSegmentIsPrunedInsideItsCheck prunes the
// segment holding the wanted sequence after OpenCursor's horizon check and
// before its snapshot. The wanted sequence is then below the horizon, and
// falling back to the last segment would position the cursor past it.
func TestOpenCursorNeverSkipsWhenTheWantedSegmentIsPrunedInsideItsCheck(t *testing.T) {
	forEachLayout(t, func(t *testing.T, sealedLog bool) {
		w := openReaderTestWAL(t, t.TempDir(), sealedLog, 4<<10)
		defer func() { _ = w.Close() }()
		firsts := fillSegments(t, w, 3)
		from := firsts[1]

		pruned := false
		w.openCursorHook = func() {
			if pruned {
				return
			}
			pruned = true
			if err := w.Truncate(firsts[2]); err != nil {
				t.Errorf("Truncate: %v", err)
			}
		}
		cursor, err := w.OpenCursor(from)
		if !pruned {
			t.Fatal("the hook never ran")
		}
		if err == nil {
			entries, _, readErr := w.ReadBatch(cursor, w.LastSequence(), 1<<20)
			requireNextIsOrHorizon(t, entries, readErr, from)
			return
		}
		if !errors.Is(err, ErrBelowHorizon) {
			t.Fatalf("OpenCursor(%d) = %v; want ErrBelowHorizon", from, err)
		}
	})
}

// TestReadBatchNamesAHoleInTheLog deletes a middle segment — a hole in the
// retained range, not a pruner outrunning the reader — and reads across it. The
// read must name the missing sequences and must not match ErrBelowHorizon, the
// horizon refusal whose remedy is to re-seed from a snapshot; no snapshot can
// repair this node's own log.
func TestReadBatchNamesAHoleInTheLog(t *testing.T) {
	forEachLayout(t, func(t *testing.T, sealedLog bool) {
		w := openReaderTestWAL(t, t.TempDir(), sealedLog, 4<<10)
		defer func() { _ = w.Close() }()
		firsts := fillSegments(t, w, 3)
		cursor, err := w.OpenCursor(firsts[0])
		if err != nil {
			t.Fatalf("OpenCursor(%d): %v", firsts[0], err)
		}
		// A middle segment disappears, as after a disk error or a hand deletion.
		hole := w.listSegments()[1]
		if err := os.Remove(filepath.Join(w.dir, hole.name)); err != nil {
			t.Fatalf("removing middle segment %s: %v", hole.name, err)
		}
		wantFrom, wantTo := firsts[1], firsts[2]-1

		_, _, err = w.ReadBatch(cursor, w.LastSequence(), 1<<20)
		if err == nil {
			t.Fatal("ReadBatch across a hole returned no error")
		}
		if !strings.Contains(err.Error(), "hole") {
			t.Errorf("the error for a missing middle segment does not say the log has a hole: %v", err)
		}
		if !strings.Contains(err.Error(), fmt.Sprintf("%d..%d", wantFrom, wantTo)) {
			t.Errorf("the error does not name the missing sequences %d..%d: %v", wantFrom, wantTo, err)
		}
		if errors.Is(err, ErrBelowHorizon) {
			t.Errorf("the error for a hole matches ErrBelowHorizon, so a consumer words it as a horizon: %v", err)
		}
	})
}

// TestReadBatchNeverSkipsWhenItsReopenRacesAPrune is F-1's re-open with the
// same gap: the cursor's segment is pruned with its sequence still retained, so
// the read re-opens, and the segment holding that sequence is pruned as well
// before the re-open has looked.
func TestReadBatchNeverSkipsWhenItsReopenRacesAPrune(t *testing.T) {
	forEachLayout(t, func(t *testing.T, sealedLog bool) {
		w := openReaderTestWAL(t, t.TempDir(), sealedLog, 4<<10)
		defer func() { _ = w.Close() }()
		firsts := fillSegments(t, w, 3)
		from := firsts[1]
		// A cursor parked at the end of the first segment, wanting the first
		// entry of the second, as a long-lived reader is between batches.
		cursor := WALCursor{segmentIndex: w.listSegments()[0].index, offset: 1 << 30, nextSeq: from}
		if err := w.Truncate(from); err != nil {
			t.Fatalf("Truncate: %v", err)
		}

		pruned := false
		w.openCursorHook = func() {
			if pruned {
				return
			}
			pruned = true
			if err := w.Truncate(firsts[2]); err != nil {
				t.Errorf("Truncate: %v", err)
			}
		}
		entries, _, err := w.ReadBatch(cursor, w.LastSequence(), 1<<20)
		if !pruned {
			t.Fatal("the re-open never ran, so the test raced nothing")
		}
		requireNextIsOrHorizon(t, entries, err, from)
	})
}

// TestReadersSeeContiguousSequencesWhileTheLogIsPrunedUnderThem is the
// verifier's stress shape, bounded: one appender, a pruner cutting the log just
// behind the synced tail, and readers with small batches on small segments.
// Every batch must start at the sequence after the previous one; a reader that
// was overtaken by the pruner must be told so, never handed a later sequence.
func TestReadersSeeContiguousSequencesWhileTheLogIsPrunedUnderThem(t *testing.T) {
	if testing.Short() {
		t.Skip("a timed stress run")
	}
	forEachLayout(t, func(t *testing.T, sealedLog bool) {
		w := openReaderTestWAL(t, t.TempDir(), sealedLog, 2<<10)
		defer func() { _ = w.Close() }()
		const (
			readers  = 6
			duration = 400 * time.Millisecond
		)
		deadline := time.Now().Add(duration)
		var group sync.WaitGroup

		group.Add(1)
		go func() {
			defer group.Done()
			payload := []byte(strings.Repeat("s", 200))
			for time.Now().Before(deadline) {
				if _, err := w.Append("docs", OpInsert, []byte("k"), payload); err != nil {
					t.Errorf("Append: %v", err)
					return
				}
			}
		}()
		group.Add(1)
		go func() {
			defer group.Done()
			for time.Now().Before(deadline) {
				if synced := w.SyncedSequence(); synced > 4 {
					if err := w.Truncate(synced - 3); err != nil {
						t.Errorf("Truncate: %v", err)
						return
					}
				}
				time.Sleep(200 * time.Microsecond)
			}
		}()

		var batches atomic.Int64
		for range readers {
			group.Add(1)
			go func() {
				defer group.Done()
				var cursor WALCursor
				var have bool
				var previous uint64
				var cursorOpenedAt uint64
				for time.Now().Before(deadline) {
					if !have {
						first, err := w.FirstSequence()
						if err != nil {
							t.Errorf("FirstSequence: %v", err)
							return
						}
						cursor, err = w.OpenCursor(first)
						if errors.Is(err, ErrBelowHorizon) {
							continue
						}
						if err != nil {
							t.Errorf("OpenCursor: %v", err)
							return
						}
						have, previous, cursorOpenedAt = true, 0, first
					}
					entries, next, err := w.ReadBatch(cursor, w.SyncedSequence(), 700)
					if errors.Is(err, ErrBelowHorizon) {
						have = false
						continue
					}
					if err != nil {
						t.Errorf("ReadBatch: %v", err)
						return
					}
					for i, entry := range entries {
						if i == 0 && previous == 0 {
							if entry.Seq != cursorOpenedAt {
								t.Errorf("first read after opening at %d returned %d", cursorOpenedAt, entry.Seq)
								return
							}
						}
						if previous != 0 && entry.Seq != previous+1 {
							t.Errorf("a reader got sequence %d after %d: %d..%d were skipped", entry.Seq, previous, previous+1, entry.Seq-1)
							return
						}
						previous = entry.Seq
					}
					batches.Add(1)
					cursor = next
				}
			}()
		}
		group.Wait()
		if batches.Load() == 0 {
			t.Error("no reader completed a batch, so the run checked nothing")
		}
	})
}
