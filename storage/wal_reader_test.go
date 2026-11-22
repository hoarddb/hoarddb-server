package storage

import (
	"errors"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// setMaxFileSizeForTest lowers the segment size of an open log so a test can
// rotate it in kilobytes instead of writing 64 MB.
func (w *WAL) setMaxFileSizeForTest(size int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.maxFileSize = size
}

// openReaderTestWAL opens a plain or a sealed log with a small segment size.
func openReaderTestWAL(t *testing.T, dir string, sealedLog bool, segmentBytes int64) *WAL {
	t.Helper()
	var keys WALKeySource
	if sealedLog {
		keys = newSealedTestKeySource(t)
	}
	w, err := OpenWAL(dir, keys)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	w.setMaxFileSizeForTest(segmentBytes)
	return w
}

func forEachLayout(t *testing.T, run func(t *testing.T, sealedLog bool)) {
	t.Helper()
	for _, sealedLog := range []bool{false, true} {
		name := "plain"
		if sealedLog {
			name = "sealed"
		}
		t.Run(name, func(t *testing.T) { run(t, sealedLog) })
	}
}

func TestWALReaderSeesEverySequenceOnceUnderAppendsRotationAndTruncation(t *testing.T) {
	forEachLayout(t, func(t *testing.T, sealedLog bool) {
		const (
			appenders    = 16
			perAppender  = 120
			totalEntries = appenders * perAppender
			payloadBytes = 200
		)
		w := openReaderTestWAL(t, t.TempDir(), sealedLog, 64<<10)
		defer func() { _ = w.Close() }()

		cursor, err := w.OpenCursor(1)
		if err != nil {
			t.Fatalf("OpenCursor: %v", err)
		}

		var readerPassed atomic.Uint64
		stopTruncating := make(chan struct{})
		var truncatorDone sync.WaitGroup
		truncatorDone.Add(1)
		go func() {
			defer truncatorDone.Done()
			for {
				select {
				case <-stopTruncating:
					return
				default:
				}
				if passed := readerPassed.Load(); passed > 0 {
					if err := w.Truncate(passed); err != nil {
						t.Errorf("Truncate(%d): %v", passed, err)
						return
					}
				}
				time.Sleep(time.Millisecond)
			}
		}()

		var appendersDone sync.WaitGroup
		payload := strings.Repeat("x", payloadBytes)
		for a := 0; a < appenders; a++ {
			appendersDone.Add(1)
			go func() {
				defer appendersDone.Done()
				for i := 0; i < perAppender; i++ {
					if _, err := w.Append("bucket", OpInsert, []byte("k"), []byte(payload)); err != nil {
						t.Errorf("Append: %v", err)
						return
					}
				}
			}()
		}

		var seen []uint64
		deadline := time.Now().Add(60 * time.Second)
		for len(seen) < totalEntries && time.Now().Before(deadline) {
			entries, next, err := w.ReadBatch(cursor, w.SyncedSequence(), 16<<10)
			if err != nil {
				t.Fatalf("ReadBatch after %d entries: %v", len(seen), err)
			}
			cursor = next
			for _, entry := range entries {
				seen = append(seen, entry.Seq)
			}
			if len(entries) > 0 {
				readerPassed.Store(entries[len(entries)-1].Seq)
			} else {
				time.Sleep(time.Millisecond)
			}
		}
		appendersDone.Wait()
		close(stopTruncating)
		truncatorDone.Wait()

		if len(seen) != totalEntries {
			t.Fatalf("the reader returned %d entries of %d", len(seen), totalEntries)
		}
		for i, seq := range seen {
			if seq != uint64(i+1) {
				t.Fatalf("entry %d has sequence %d, want %d: a sequence was skipped or repeated", i, seq, i+1)
			}
		}
		// The truncator may have pruned the early segments by now; the active
		// segment's index is what says how many times the log rotated.
		if rotations := w.activeIndex - 1; rotations < 3 {
			t.Fatalf("the log rotated %d time(s); the test proved nothing about rotation", rotations)
		}
	})
}

func TestAnAppendCompletesWhileAReaderIsMidDecode(t *testing.T) {
	w := openReaderTestWAL(t, t.TempDir(), false, 64<<10)
	defer func() { _ = w.Close() }()
	appendTestEntries(t, w, 3)

	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	w.readDecodeHook = func() {
		once.Do(func() { close(entered) })
		<-release
	}

	cursor, err := w.OpenCursor(1)
	if err != nil {
		t.Fatalf("OpenCursor: %v", err)
	}
	readerDone := make(chan error, 1)
	go func() {
		_, _, err := w.ReadBatch(cursor, w.LastSequence(), 1<<20)
		readerDone <- err
	}()
	<-entered

	appendDone := make(chan error, 1)
	go func() {
		_, err := w.Append("docs", OpInsert, []byte("k"), []byte("during"))
		appendDone <- err
	}()
	select {
	case err := <-appendDone:
		if err != nil {
			t.Errorf("Append during a read: %v", err)
		}
	case <-time.After(60 * time.Second):
		t.Error("an Append did not return within 60 s while a reader was mid-decode: the reader holds the append lock")
	}
	close(release)
	if err := <-readerDone; err != nil {
		t.Errorf("ReadBatch: %v", err)
	}
}

func TestReadBatchStopsAtTheSequenceItIsGiven(t *testing.T) {
	w := openReaderTestWAL(t, t.TempDir(), false, 64<<10)
	defer func() { _ = w.Close() }()
	if _, err := w.Append("docs", OpInsert, []byte("k"), []byte("synced")); err != nil {
		t.Fatalf("first append: %v", err)
	}

	written := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseHeldAppend := func() { releaseOnce.Do(func() { close(release) }) }
	// Registered after the deferred Close so it runs first: Close waits for an
	// append still inside its fsync, and a failed assertion must not leave one.
	defer releaseHeldAppend()
	w.SetBeforeSyncHook(func(seq uint64) {
		if seq == 2 {
			close(written)
			<-release
		}
	})
	appendDone := make(chan error, 1)
	go func() {
		_, err := w.Append("docs", OpInsert, []byte("k"), []byte("written, not synced"))
		appendDone <- err
	}()
	<-written

	if last, synced := w.LastSequence(), w.SyncedSequence(); last != 2 || synced != 1 {
		t.Fatalf("with an append held before its fsync the log reports last %d, synced %d; want 2 and 1", last, synced)
	}
	cursor, err := w.OpenCursor(1)
	if err != nil {
		t.Fatalf("OpenCursor: %v", err)
	}

	upToSynced, _, err := w.ReadBatch(cursor, w.SyncedSequence(), 1<<20)
	if err != nil {
		t.Fatalf("ReadBatch up to the synced sequence: %v", err)
	}
	if len(upToSynced) != 1 || upToSynced[0].Seq != 1 {
		t.Errorf("reading up to the synced sequence returned %d entries; want only sequence 1", len(upToSynced))
	}
	upToLast, _, err := w.ReadBatch(cursor, w.LastSequence(), 1<<20)
	if err != nil {
		t.Fatalf("ReadBatch up to the last sequence: %v", err)
	}
	if len(upToLast) != 2 {
		t.Errorf("reading up to the last sequence returned %d entries; want both, or the test holds nothing", len(upToLast))
	}

	releaseHeldAppend()
	if err := <-appendDone; err != nil {
		t.Fatalf("held append: %v", err)
	}
	if w.SyncedSequence() != 2 {
		t.Errorf("synced sequence after the held append returned = %d, want 2", w.SyncedSequence())
	}
}

func TestOpenWALTreatsWhatIsOnDiskAsSynced(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWAL(dir, nil)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	appendTestEntries(t, w, 4)
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	reopened, err := OpenWAL(dir, nil)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	if got := reopened.SyncedSequence(); got != 4 {
		t.Errorf("synced sequence after open = %d, want 4", got)
	}
}

func TestReadBatchHonoursTheByteBoundAndReturnsAtLeastOneEntry(t *testing.T) {
	forEachLayout(t, func(t *testing.T, sealedLog bool) {
		w := openReaderTestWAL(t, t.TempDir(), sealedLog, 1<<20)
		defer func() { _ = w.Close() }()
		payload := []byte(strings.Repeat("p", 100<<10))
		for i := 0; i < 6; i++ {
			if _, err := w.Append("docs", OpInsert, []byte("k"), payload); err != nil {
				t.Fatalf("append %d: %v", i, err)
			}
		}

		cursor, err := w.OpenCursor(1)
		if err != nil {
			t.Fatalf("OpenCursor: %v", err)
		}
		// Each entry is a little over 100 KiB: 250 KiB holds two, not three.
		first, cursor, err := w.ReadBatch(cursor, w.LastSequence(), 250<<10)
		if err != nil || len(first) != 2 {
			t.Fatalf("ReadBatch(250 KiB) = %d entries, %v; want 2", len(first), err)
		}
		// A bound smaller than one entry still returns one: a single large entry
		// must ship alone rather than never.
		second, cursor, err := w.ReadBatch(cursor, w.LastSequence(), 1)
		if err != nil || len(second) != 1 || second[0].Seq != 3 {
			t.Fatalf("ReadBatch(1 byte) = %d entries, %v; want sequence 3 alone", len(second), err)
		}
		rest, _, err := w.ReadBatch(cursor, w.LastSequence(), 8<<20)
		if err != nil || len(rest) != 3 || rest[0].Seq != 4 {
			t.Fatalf("ReadBatch(rest) = %d entries, %v; want sequences 4 to 6", len(rest), err)
		}
	})
}

func TestOpenCursorFindsASequenceInALaterSegmentAndRefusesAPrunedOne(t *testing.T) {
	forEachLayout(t, func(t *testing.T, sealedLog bool) {
		w := openReaderTestWAL(t, t.TempDir(), sealedLog, 4<<10)
		defer func() { _ = w.Close() }()
		payload := []byte(strings.Repeat("q", 500))
		const total = 40
		for i := 0; i < total; i++ {
			if _, err := w.Append("docs", OpInsert, []byte("k"), payload); err != nil {
				t.Fatalf("append %d: %v", i, err)
			}
		}
		if segments := len(w.listSegments()); segments < 4 {
			t.Fatalf("only %d segments; the test needs several", segments)
		}

		cursor, err := w.OpenCursor(23)
		if err != nil {
			t.Fatalf("OpenCursor(23): %v", err)
		}
		entries, _, err := w.ReadBatch(cursor, total, 1<<20)
		if err != nil {
			t.Fatalf("ReadBatch: %v", err)
		}
		if len(entries) != total-22 || entries[0].Seq != 23 || entries[len(entries)-1].Seq != total {
			t.Errorf("a cursor at 23 returned %d entries starting at %d; want %d starting at 23",
				len(entries), entries[0].Seq, total-22)
		}

		// One past the tail is a valid place to wait for the next append.
		tail, err := w.OpenCursor(total + 1)
		if err != nil {
			t.Fatalf("OpenCursor(tail+1): %v", err)
		}
		if _, err := w.Append("docs", OpInsert, []byte("k"), payload); err != nil {
			t.Fatalf("append after the cursor: %v", err)
		}
		fresh, _, err := w.ReadBatch(tail, w.LastSequence(), 1<<20)
		if err != nil || len(fresh) != 1 || fresh[0].Seq != total+1 {
			t.Errorf("a cursor at the tail returned %d entries, %v; want sequence %d", len(fresh), err, total+1)
		}

		if err := w.Truncate(30); err != nil {
			t.Fatalf("Truncate: %v", err)
		}
		first, err := w.FirstSequence()
		if err != nil || first <= 1 {
			t.Fatalf("FirstSequence after truncation = %d, %v; the test pruned nothing", first, err)
		}
		_, err = w.OpenCursor(1)
		if !errors.Is(err, ErrBelowHorizon) {
			t.Fatalf("OpenCursor(1) after pruning = %v; want ErrBelowHorizon", err)
		}
		if !strings.Contains(err.Error(), strconv.FormatUint(first, 10)) {
			t.Errorf("ErrBelowHorizon text %q does not name the first retained sequence %d", err, first)
		}
	})
}

func TestAReadPastAPrunedSegmentIsBelowTheHorizon(t *testing.T) {
	w := openReaderTestWAL(t, t.TempDir(), false, 4<<10)
	defer func() { _ = w.Close() }()
	payload := []byte(strings.Repeat("r", 500))
	for i := 0; i < 40; i++ {
		if _, err := w.Append("docs", OpInsert, []byte("k"), payload); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	cursor, err := w.OpenCursor(1)
	if err != nil {
		t.Fatalf("OpenCursor: %v", err)
	}
	if err := w.Truncate(35); err != nil {
		t.Fatalf("Truncate: %v", err)
	}
	if _, _, err := w.ReadBatch(cursor, 40, 1<<20); !errors.Is(err, ErrBelowHorizon) {
		t.Fatalf("a cursor whose segment was pruned before it was read: %v; want ErrBelowHorizon (not a raw file error)", err)
	}
}
