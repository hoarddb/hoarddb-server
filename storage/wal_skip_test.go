package storage

import (
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// appendN appends n entries and returns the sequence of the last.
func appendN(t *testing.T, w *WAL, n int) uint64 {
	t.Helper()
	var seq uint64
	for i := 0; i < n; i++ {
		var err error
		if seq, err = w.Append("docs", OpInsert, []byte(fmt.Sprintf("k%d", i)), []byte("v")); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	return seq
}

// sequencesOf reads the whole log from from with a cursor and returns the
// sequences it found.
func sequencesOf(t *testing.T, w *WAL, from uint64) []uint64 {
	t.Helper()
	cursor, err := w.OpenCursor(from)
	if err != nil {
		t.Fatalf("OpenCursor(%d): %v", from, err)
	}
	var seqs []uint64
	for {
		entries, next, err := w.ReadBatch(cursor, w.LastSequence(), 1<<20)
		if err != nil {
			t.Fatalf("ReadBatch from %d: %v", from, err)
		}
		if len(entries) == 0 {
			return seqs
		}
		for _, entry := range entries {
			seqs = append(seqs, entry.Seq)
		}
		cursor = next
	}
}

// TestAFastForwardIsRecordedAndOutlivesTheProcess: the log passes over 4..10,
// the next append is 11, and a reopen, after an append above the skip or with
// nothing written after it (where only the skip says 10: the scan finds 3),
// never gives a sequence in the skip again.
func TestAFastForwardIsRecordedAndOutlivesTheProcess(t *testing.T) {
	for _, crashed := range []bool{false, true} {
		t.Run(fmt.Sprintf("crashed=%v", crashed), func(t *testing.T) {
			dir := t.TempDir()
			w, err := OpenWAL(dir, nil)
			if err != nil {
				t.Fatalf("OpenWAL: %v", err)
			}
			appendN(t, w, 3)
			if moved, err := w.FastForward(2); err != nil || moved {
				t.Fatalf("FastForward below the log's end moved=%v err=%v; want no move", moved, err)
			}
			if moved, err := w.FastForward(10); err != nil || !moved {
				t.Fatalf("FastForward(10) moved=%v err=%v", moved, err)
			}
			if got := w.LastSequence(); got != 10 {
				t.Fatalf("LastSequence after the fast-forward is %d, want 10", got)
			}
			if !crashed {
				if seq := appendN(t, w, 1); seq != 11 {
					t.Fatalf("the append after the fast-forward is %d, want 11", seq)
				}
			}
			if err := w.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			reopened, err := OpenWAL(dir, nil)
			if err != nil {
				t.Fatalf("reopening: %v", err)
			}
			defer func() { _ = reopened.Close() }()
			if skips := reopened.Skips(); len(skips) != 1 || skips[0] != (WALSkip{After: 3, Through: 10}) {
				t.Fatalf("the reopened log records skips %v, want [{3 10}]", skips)
			}
			want := uint64(12)
			if crashed {
				want = 11
			}
			if seq := appendN(t, reopened, 1); seq != want {
				t.Fatalf("the first append after the reopen is %d, want %d", seq, want)
			}
			chain := reopened.Incarnations()
			if base := chain[len(chain)-1].Base; base != want-1 {
				t.Fatalf("the link minted by the reopen has base %d, want %d", base, want-1)
			}
		})
	}
}

// TestAReadPassesOverASkipAndNotOverAHole: a cursor reads straight across a
// recorded skip, from below it, from inside it, and from a cursor that was
// waiting at the log's tail when the skip was made.
func TestAReadPassesOverASkipAndNotOverAHole(t *testing.T) {
	forEachLayout(t, func(t *testing.T, sealedLog bool) {
		w := openReaderTestWAL(t, t.TempDir(), sealedLog, 64<<20)
		defer func() { _ = w.Close() }()
		appendN(t, w, 3)
		waiting, err := w.OpenCursor(4)
		if err != nil {
			t.Fatalf("OpenCursor at the tail: %v", err)
		}
		if _, err := w.FastForward(10); err != nil {
			t.Fatalf("FastForward: %v", err)
		}
		appendN(t, w, 2)

		for from, want := range map[uint64][]uint64{1: {1, 2, 3, 11, 12}, 4: {11, 12}, 7: {11, 12}, 11: {11, 12}} {
			if got := sequencesOf(t, w, from); fmt.Sprint(got) != fmt.Sprint(want) {
				t.Errorf("a read from %d found %v, want %v", from, got, want)
			}
		}
		entries, _, err := w.ReadBatch(waiting, w.LastSequence(), 1<<20)
		if err != nil || len(entries) != 2 || entries[0].Seq != 11 {
			t.Errorf("the cursor waiting at 4 read %d entries (err %v); want 11 and 12", len(entries), err)
		}
	})
}

// TestAReadAfterTheEntriesBelowASkipArePrunedIsNotBelowTheHorizon: a peer
// that acknowledged everything below the skip asks for the next sequence after
// retention removed what it acknowledged. That sequence is in the skip, so the
// log still holds everything from it on.
func TestAReadAfterTheEntriesBelowASkipArePrunedIsNotBelowTheHorizon(t *testing.T) {
	// One entry a segment, so retention can remove exactly what is below the
	// skip.
	w := openReaderTestWAL(t, t.TempDir(), false, 1)
	defer func() { _ = w.Close() }()
	appendN(t, w, 5)
	if _, err := w.FastForward(55); err != nil {
		t.Fatalf("FastForward: %v", err)
	}
	appendN(t, w, 3)
	if err := w.Truncate(56); err != nil {
		t.Fatalf("Truncate: %v", err)
	}
	if first, err := w.FirstSequence(); err != nil || first != 56 {
		t.Fatalf("setup: the first retained sequence is %d (err %v); want 56", first, err)
	}
	if got := sequencesOf(t, w, 6); fmt.Sprint(got) != fmt.Sprint([]uint64{56, 57, 58}) {
		t.Fatalf("a read from 6, the first sequence of the skip, found %v; want [56 57 58]", got)
	}
	if _, err := w.OpenCursor(5); !errors.Is(err, ErrBelowHorizon) {
		t.Fatalf("a read from 5, below the skip and pruned, answered %v; want ErrBelowHorizon", err)
	}
}

// TestAChainFileWithoutSkipsIsALogWithNone: the file every build before skips
// wrote opens with no skip and its sequence where it was.
func TestAChainFileWithoutSkipsIsALogWithNone(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWAL(dir, nil)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	appendN(t, w, 4)
	chain := w.Incarnations()
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	type earlierChain struct {
		Links []byte `bson:"links"`
	}
	encoded, err := bson.Marshal(earlierChain{Links: encodeIncarnationLinks(chain)})
	if err != nil {
		t.Fatalf("encoding: %v", err)
	}
	if err := WriteFileDurably(filepath.Join(dir, walIncarnationFile), encoded); err != nil {
		t.Fatalf("writing the earlier chain file: %v", err)
	}
	reopened, err := OpenWAL(dir, nil)
	if err != nil {
		t.Fatalf("reopening on an earlier chain file: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	if skips := reopened.Skips(); len(skips) != 0 {
		t.Fatalf("an earlier chain file opened with skips %v", skips)
	}
	if seq := appendN(t, reopened, 1); seq != 5 {
		t.Fatalf("the next append is %d, want 5", seq)
	}
}

// TestADamagedSkipStopsTheOpen: a skip that does not rise, or overlaps the one
// before it, would hide a real hole; the open refuses it with the file's name.
func TestADamagedSkipStopsTheOpen(t *testing.T) {
	for name, skips := range map[string][]WALSkip{
		"not rising":  {{After: 9, Through: 9}},
		"overlapping": {{After: 3, Through: 10}, {After: 8, Through: 20}},
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			w, err := OpenWAL(dir, nil)
			if err != nil {
				t.Fatalf("OpenWAL: %v", err)
			}
			chain := w.Incarnations()
			if err := w.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			if err := writeIncarnationChain(dir, chain, skips); err != nil {
				t.Fatalf("writing the chain: %v", err)
			}
			if _, err := OpenWAL(dir, nil); err == nil {
				t.Fatalf("the log opened with the skips %v", skips)
			}
		})
	}
}

// TestFastForwardsWithNothingBetweenThemAreOneSkip: two fast-forwards with no
// append between them record one run, not two that touch.
func TestFastForwardsWithNothingBetweenThemAreOneSkip(t *testing.T) {
	w, err := OpenWAL(t.TempDir(), nil)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	defer func() { _ = w.Close() }()
	appendN(t, w, 2)
	for _, through := range []uint64{5, 9} {
		if _, err := w.FastForward(through); err != nil {
			t.Fatalf("FastForward(%d): %v", through, err)
		}
	}
	appendN(t, w, 1)
	if _, err := w.FastForward(20); err != nil {
		t.Fatalf("FastForward(20): %v", err)
	}
	want := []WALSkip{{After: 2, Through: 9}, {After: 10, Through: 20}}
	if got := w.Skips(); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("skips %v, want %v", got, want)
	}
}
