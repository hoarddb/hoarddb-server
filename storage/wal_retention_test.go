package storage

import (
	"bytes"
	"fmt"
	"testing"
)

// TestWALFirstSequenceAndTruncation covers the retention horizon: what the log
// still holds after truncation, and the fact that ReadFrom cannot see the hole
// — which is exactly why catch-up checks FirstSequence instead of trusting
// ReadFrom's first result. Entries are ~92 bytes each against a 150-byte
// segment limit, so every entry rotates into its own segment and Truncate has
// whole files to remove.
func TestWALFirstSequenceAndTruncation(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWAL(dir, nil)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	defer func() { _ = w.Close() }()

	// An empty log has no retained entry: the horizon is one past the last
	// sequence, so a peer positioned at the tail has nothing to catch up.
	first, err := w.FirstSequence()
	if err != nil {
		t.Fatalf("first sequence of empty log: %v", err)
	}
	if first != 1 {
		t.Fatalf("empty log horizon = %d, want 1", first)
	}

	w.maxFileSize = 150

	value := bytes.Repeat([]byte("x"), 64)
	seqs := make([]uint64, 0, 8)
	for i := 0; i < 8; i++ {
		seq, err := w.Append("b", OpInsert, []byte(fmt.Sprintf("k%d", i)), value)
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		seqs = append(seqs, seq)
	}
	for i := range seqs {
		if seqs[i] != uint64(i+1) {
			t.Fatalf("entry %d has seq %d, want %d", i, seqs[i], i+1)
		}
	}
	if first, err = w.FirstSequence(); err != nil || first != 1 {
		t.Fatalf("first sequence after appends = %d (%v), want 1", first, err)
	}
	if got := len(w.listSegments()); got != 8 {
		t.Fatalf("rotation: %d segments, want 8 (one per entry)", got)
	}

	// Truncate through seq 4: the four earlier one-entry segments are removed
	// and the retained horizon becomes 4.
	if err := w.Truncate(seqs[3]); err != nil {
		t.Fatalf("truncate through %d: %v", seqs[3], err)
	}
	// Truncate keeps the segment containing the truncation point, so segments
	// 1-3 are removed (their entries are all <= 4) and 4-8 stay.
	if got := len(w.listSegments()); got != 5 {
		t.Fatalf("after truncation: %d segments remain, want 5", got)
	}
	first, err = w.FirstSequence()
	if err != nil {
		t.Fatalf("first sequence after truncate: %v", err)
	}
	if first != seqs[3] {
		t.Fatalf("retained horizon = %d, want %d", first, seqs[3])
	}

	// ReadFrom below the horizon cannot see the hole: it starts at the
	// retained tail. FirstSequence — not the first entry ReadFrom returns —
	// is the truth about what the log covers.
	entries, err := w.ReadFrom(seqs[0])
	if err != nil {
		t.Fatalf("read from seq 1: %v", err)
	}
	if len(entries) != 5 || entries[0].Seq != seqs[3] || entries[4].Seq != seqs[7] {
		t.Fatalf("ReadFrom(%d) returned %d entries from %d to %d, want 5 entries %d..%d",
			seqs[0], len(entries), entries[0].Seq, entries[len(entries)-1].Seq, seqs[3], seqs[7])
	}

	// Truncate through the last entry: only its segment remains.
	if err := w.Truncate(seqs[7]); err != nil {
		t.Fatalf("truncate through %d: %v", seqs[7], err)
	}
	first, err = w.FirstSequence()
	if err != nil {
		t.Fatalf("first sequence after final truncate: %v", err)
	}
	if first != seqs[7] {
		t.Fatalf("retained horizon after final truncate = %d, want %d", first, seqs[7])
	}
	entries, err = w.ReadFrom(seqs[0])
	if err != nil {
		t.Fatalf("read from seq 1 after final truncate: %v", err)
	}
	if len(entries) != 1 || entries[0].Seq != seqs[7] {
		t.Fatalf("ReadFrom(%d) after final truncate returned %d entries, want just seq %d",
			seqs[0], len(entries), seqs[7])
	}
}
