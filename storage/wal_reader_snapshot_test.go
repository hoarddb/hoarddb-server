package storage

import (
	"math"
	"testing"
)

// TestReadBatchStopsAtTheActiveSegmentLengthItSnapshotted appends an entry
// after a reader has taken its snapshot and before it opens the segment, with
// no sequence bound on the read (upTo is the maximum). Only the snapshotted
// length of the active segment stops the reader from also returning the new
// entry, so a reader that ignores that length returns one entry too many. The existing tests bound the read by
// sequence, which hides that mutation.
func TestReadBatchStopsAtTheActiveSegmentLengthItSnapshotted(t *testing.T) {
	forEachLayout(t, func(t *testing.T, sealedLog bool) {
		w := openReaderTestWAL(t, t.TempDir(), sealedLog, 64<<10)
		defer func() { _ = w.Close() }()
		const snapshotted = 3
		appendTestEntries(t, w, snapshotted)

		appended := false
		w.readSnapshotHook = func() {
			if appended {
				return
			}
			appended = true
			if _, err := w.Append("docs", OpInsert, []byte("k"), []byte("after the snapshot")); err != nil {
				t.Errorf("append behind the reader: %v", err)
			}
		}

		cursor, err := w.OpenCursor(1)
		if err != nil {
			t.Fatalf("OpenCursor: %v", err)
		}
		entries, next, err := w.ReadBatch(cursor, math.MaxUint64, 1<<20)
		if err != nil {
			t.Fatalf("ReadBatch: %v", err)
		}
		if !appended {
			t.Fatal("the snapshot hook never ran, so nothing was appended behind the reader")
		}
		if len(entries) != snapshotted {
			t.Fatalf("ReadBatch returned %d entries; want the %d present when it took its snapshot", len(entries), snapshotted)
		}

		w.readSnapshotHook = nil
		later, _, err := w.ReadBatch(next, math.MaxUint64, 1<<20)
		if err != nil {
			t.Fatalf("second ReadBatch: %v", err)
		}
		if len(later) != 1 || later[0].Seq != snapshotted+1 {
			t.Errorf("the next read returned %d entries; want the one appended behind the first (sequence %d)", len(later), snapshotted+1)
		}
	})
}
