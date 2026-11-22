package storage

import (
	"errors"
	"strings"
	"testing"
)

// appendUntilRotation appends one entry at a time, reading through a cursor
// after each, until an append lands in a second segment. It returns the cursor
// (parked at the end of the first segment, wanting the first sequence of the
// second) and that sequence.
func appendUntilRotation(t *testing.T, w *WAL, payload []byte) (WALCursor, uint64) {
	t.Helper()
	cursor, err := w.OpenCursor(1)
	if err != nil {
		t.Fatalf("OpenCursor: %v", err)
	}
	for i := 0; i < 200; i++ {
		_, next, err := w.ReadBatch(cursor, w.SyncedSequence(), 1<<20)
		if err != nil {
			t.Fatalf("ReadBatch: %v", err)
		}
		cursor = next
		seq, err := w.Append("docs", OpInsert, []byte("k"), payload)
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		if len(w.listSegments()) == 2 {
			return cursor, seq
		}
	}
	t.Fatal("the log never rotated")
	return WALCursor{}, 0
}

func TestAReadStillFindsASequenceWhoseCursorSegmentWasPruned(t *testing.T) {
	forEachLayout(t, func(t *testing.T, sealedLog bool) {
		w := openReaderTestWAL(t, t.TempDir(), sealedLog, 4<<10)
		defer func() { _ = w.Close() }()
		cursor, firstOfSecond := appendUntilRotation(t, w, []byte(strings.Repeat("s", 500)))

		// Prune the first segment, which the cursor sits at the end of. The
		// sequence the cursor wants is the first entry of the segment that stays.
		if err := w.Truncate(firstOfSecond); err != nil {
			t.Fatalf("Truncate: %v", err)
		}
		if first, err := w.FirstSequence(); err != nil || first != firstOfSecond {
			t.Fatalf("FirstSequence = %d, %v; want %d (the test pruned the wrong amount)", first, err, firstOfSecond)
		}

		entries, _, err := w.ReadBatch(cursor, w.LastSequence(), 1<<20)
		if err != nil {
			t.Fatalf("ReadBatch for sequence %d, which the log still holds: %v", firstOfSecond, err)
		}
		if len(entries) != 1 || entries[0].Seq != firstOfSecond {
			t.Errorf("ReadBatch returned %d entries; want sequence %d alone", len(entries), firstOfSecond)
		}
	})
}

func TestAReadOfAReallyPrunedSequenceStillReportsTheHorizon(t *testing.T) {
	forEachLayout(t, func(t *testing.T, sealedLog bool) {
		w := openReaderTestWAL(t, t.TempDir(), sealedLog, 4<<10)
		defer func() { _ = w.Close() }()
		payload := []byte(strings.Repeat("t", 500))
		cursor, wanted := appendUntilRotation(t, w, payload)
		for i := 0; i < 20; i++ {
			if _, err := w.Append("docs", OpInsert, []byte("k"), payload); err != nil {
				t.Fatalf("append: %v", err)
			}
		}
		if err := w.Truncate(wanted + 10); err != nil {
			t.Fatalf("Truncate: %v", err)
		}
		if first, err := w.FirstSequence(); err != nil || first <= wanted {
			t.Fatalf("FirstSequence = %d, %v; the test did not prune sequence %d", first, err, wanted)
		}
		if _, _, err := w.ReadBatch(cursor, w.LastSequence(), 1<<20); !errors.Is(err, ErrBelowHorizon) {
			t.Fatalf("ReadBatch of pruned sequence %d: %v; want ErrBelowHorizon", wanted, err)
		}
	})
}
