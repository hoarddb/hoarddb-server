package storage

import (
	"os"
	"sync/atomic"
	"testing"
	"time"
)

// extendActiveSegment writes bytes after the end of w's active segment through
// the file system, not through the log, as a preallocated segment's zero tail or
// a write the log never committed would leave them.
func extendActiveSegment(t *testing.T, w *WAL, tail []byte) {
	t.Helper()
	path := segmentFilePath(w.dir, w.activeIndex)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatalf("opening the active segment to extend it: %v", err)
	}
	if _, err := f.Write(tail); err != nil {
		t.Fatalf("extending the active segment: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("closing the extended segment: %v", err)
	}
}

// readAllThroughCursor reads every entry from sequence 1 through a cursor.
func readAllThroughCursor(t *testing.T, w *WAL) []WALEntry {
	t.Helper()
	cursor, err := w.OpenCursor(1)
	if err != nil {
		t.Fatalf("OpenCursor(1): %v", err)
	}
	batch, _, err := w.ReadBatch(cursor, ^uint64(0), 1<<30)
	if err != nil {
		t.Fatalf("ReadBatch from 1: %v", err)
	}
	return batch
}

// requireSeqs fails unless entries are exactly the sequences 1..n, in order.
func requireSeqs(t *testing.T, how string, entries []WALEntry, n int) {
	t.Helper()
	if len(entries) != n {
		t.Fatalf("%s returned %d entries, want exactly the %d written", how, len(entries), n)
	}
	for i, entry := range entries {
		if entry.Seq != uint64(i+1) {
			t.Fatalf("%s entry %d has sequence %d, want %d", how, i, entry.Seq, i+1)
		}
	}
}

// TestWALv2ReadersStopAtTheWriteOffset, the write-offset half (build doc S2):
// bytes past the log's write offset in its active segment are never read, by
// ReadFrom, by OpenCursor and ReadBatch, or by FirstSequence and Truncate. The
// file size stops being the bound once a segment can be longer than its entries
// (a preallocated segment's zero tail). The zero-first-word half lands with
// format 2 (S3). Fails on the parent: the readers bound the active segment by
// its file size.
func TestWALv2ReadersStopAtTheWriteOffset(t *testing.T) {
	const written = 5
	for _, mode := range []struct {
		name string
		keys func(t *testing.T) WALKeySource
	}{
		{"plain", func(*testing.T) WALKeySource { return nil }},
		{"sealed", func(t *testing.T) WALKeySource { return newSealedTestKeySource(t) }},
	} {
		t.Run(mode.name+"/zero tail", func(t *testing.T) {
			w, err := OpenWAL(t.TempDir(), mode.keys(t))
			if err != nil {
				t.Fatalf("OpenWAL: %v", err)
			}
			defer closeWAL(t, w)
			appendN(t, w, written)
			extendActiveSegment(t, w, make([]byte, 64<<10))

			readBack, err := w.ReadFrom(1)
			if err != nil {
				t.Fatalf("ReadFrom(1): %v", err)
			}
			requireSeqs(t, "ReadFrom(1)", readBack, written)
			requireSeqs(t, "OpenCursor(1)+ReadBatch", readAllThroughCursor(t, w), written)
			if first, err := w.FirstSequence(); err != nil || first != 1 {
				t.Fatalf("FirstSequence() = %d, %v; want 1", first, err)
			}

			// The next entry goes at the write offset, not after the zeros, so
			// every reader still sees one contiguous run.
			appendN(t, w, 1)
			readBack, err = w.ReadFrom(1)
			if err != nil {
				t.Fatalf("ReadFrom(1) after one more append: %v", err)
			}
			requireSeqs(t, "ReadFrom(1) after one more append", readBack, written+1)
			requireSeqs(t, "ReadBatch after one more append", readAllThroughCursor(t, w), written+1)
		})

		t.Run(mode.name+"/empty active segment with a zero tail", func(t *testing.T) {
			w, err := OpenWAL(t.TempDir(), mode.keys(t))
			if err != nil {
				t.Fatalf("OpenWAL: %v", err)
			}
			defer closeWAL(t, w)
			extendActiveSegment(t, w, make([]byte, 4<<10))

			if first, err := w.FirstSequence(); err != nil || first != w.LastSequence()+1 {
				t.Fatalf("FirstSequence() on an empty log = %d, %v; want LastSequence()+1 = %d", first, err, w.LastSequence()+1)
			}
			if err := w.Truncate(0); err != nil {
				t.Fatalf("Truncate(0) on an empty log: %v", err)
			}
			if _, err := w.OpenCursor(w.LastSequence() + 1); err != nil {
				t.Fatalf("OpenCursor(LastSequence()+1) on an empty log: %v", err)
			}
			appendN(t, w, 2)
			requireSeqs(t, "ReadBatch after two appends", readAllThroughCursor(t, w), 2)
		})
	}

	// The zero-first-word half (S3): a v2 segment whose first word is zero
	// holds no entry, for every reader, including one that runs without mu and
	// so cannot know the write offset (WAL.segmentHolding, from OpenCursor).
	for _, mode := range walKeyModes {
		t.Run(mode.name+"/zero first word without the write offset", func(t *testing.T) {
			w, err := OpenWAL(t.TempDir(), mode.keys(t))
			if err != nil {
				t.Fatalf("OpenWAL: %v", err)
			}
			defer closeWAL(t, w)
			extendActiveSegment(t, w, make([]byte, 4<<10))

			if first, err := w.FirstSequence(); err != nil || first != w.LastSequence()+1 {
				t.Fatalf("FirstSequence() on an empty v2 segment = %d, %v; want LastSequence()+1 = %d", first, err, w.LastSequence()+1)
			}
			if _, err := w.OpenCursor(w.LastSequence() + 1); err != nil {
				t.Fatalf("OpenCursor(LastSequence()+1) on an empty v2 segment: %v", err)
			}
			segment := segmentRef{name: segmentFileName(w.activeIndex), index: w.activeIndex}
			if seq, found, err := w.firstSequenceOf(segment, -1); err != nil || found {
				t.Fatalf("firstSequenceOf an empty v2 segment read whole (no write offset) = %d, %v, %v; want not found and no error", seq, found, err)
			}
		})
	}

	// A whole, valid entry past the write offset is what a write the log never
	// committed looks like. It must not be read either.
	t.Run("plain/a framed entry past the offset", func(t *testing.T) {
		w, err := OpenWAL(t.TempDir(), nil)
		if err != nil {
			t.Fatalf("OpenWAL: %v", err)
		}
		defer closeWAL(t, w)
		appendN(t, w, written)
		body := make([]byte, 0, 32)
		body = append(body, 0, 0, 0, 0, 0, 0, 0, written+1, OpInsert, 0, 0, 0, 1, 'b', 0, 0, 0, 1, 'k')
		// Framed for the active segment's own format and generation, so the
		// entry is valid and only the write offset keeps it out.
		frame := append(frameWord(len(body), w.activeFormat.checksum(nil, body)), body...)
		extendActiveSegment(t, w, frame)

		readBack, err := w.ReadFrom(1)
		if err != nil {
			t.Fatalf("ReadFrom(1): %v", err)
		}
		requireSeqs(t, "ReadFrom(1)", readBack, written)
		requireSeqs(t, "OpenCursor(1)+ReadBatch", readAllThroughCursor(t, w), written)
	})
}

// TestGroupCommitFlushGoesThroughTheTable: an append's group commit is the
// table's flush entry. It is the S1 verifier's finding (b): the BUG-0712 tests
// block inside their injected flush, so a waitForSync that stopped calling the
// table would hang them until the test timeout; this one fails at once and says
// why. Break by: file.Sync() in place of w.sys.flush(file) in waitForSync.
func TestGroupCommitFlushGoesThroughTheTable(t *testing.T) {
	w, err := OpenWAL(t.TempDir(), nil)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	defer closeWAL(t, w)
	var flushes atomic.Int64
	injectFlush(w, func() { flushes.Add(1) })

	done := make(chan error, 1)
	go func() {
		_, err := w.Append("bucket", OpInsert, []byte("key"), []byte("payload"))
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("Append did not return within 60 s")
	}
	if got := flushes.Load(); got != 1 {
		t.Fatalf("the append's group commit called the table's flush %d times, want 1: waitForSync must flush through w.sys.flush", got)
	}
}

// closeWAL closes w and reports a failure to close.
func closeWAL(t *testing.T, w *WAL) {
	t.Helper()
	if err := w.Close(); err != nil {
		t.Errorf("closing the WAL: %v", err)
	}
}
