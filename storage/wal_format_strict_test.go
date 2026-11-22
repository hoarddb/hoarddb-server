package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"strings"
	"testing"
)

// The tests in this file close mutations of the format 2 code that the S3
// tests let through (the S3 verifier's report, 2026-10-27). Each names its
// mutation.

// TestWALv2StoredCRCOfZeroIsVerified: in format 2 a stored CRC of 0 is checked
// like any other (spec §3.2); the v1 rule that a zero high half means "no
// checksum recorded" does not apply. An entry framed with a zero CRC at the end
// of a v2 segment is therefore the end of the log, not an entry. Mutation M11:
// the v1 rule applied to v2 segments.
func TestWALv2StoredCRCOfZeroIsVerified(t *testing.T) {
	dir := t.TempDir()
	writeLog(t, dir, nil, 2)
	path := segmentFilePath(dir, 1)
	body := rawPlainBody(3, 1, "q", "k", []byte("unvouched"))
	unvouched := append(frameWord(len(body), 0), body...)
	segment, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatalf("opening the segment: %v", err)
	}
	if _, err := segment.Write(unvouched); err != nil {
		t.Fatalf("writing the entry with a zero CRC: %v", err)
	}
	if err := segment.Close(); err != nil {
		t.Fatalf("closing the segment: %v", err)
	}

	w, entries := reopenAndRead(t, dir, nil)
	requireSeqs(t, "ReadFrom(1) over an entry with a zero CRC", entries, 2)
	if got := w.LastSequence(); got != 2 {
		t.Fatalf("LastSequence() = %d, want 2: an entry whose zero CRC does not match is not the log's", got)
	}
}

// TestWALv2HeaderParserIsStrict: parseWALv2Header refuses every header that is
// not exactly segment format 2: a flags byte without the format bit or with an
// unknown bit, a non-zero reserved byte, and a zero generation. Mutations M20
// (unknown flag bits accepted), M21 (reserved byte not checked) and M25
// (generation 0 accepted).
func TestWALv2HeaderParserIsStrict(t *testing.T) {
	valid := newWALv2Format(true, 7).header
	if _, err := parseWALv2Header(valid); err != nil {
		t.Fatalf("a header the writer built does not parse: %v", err)
	}
	for _, tc := range []struct {
		name   string
		mutate func(header []byte)
		want   string
	}{
		{"an unknown flag bit (M20)", func(h []byte) { h[6] |= 0x04 }, "flags"},
		{"the top flag bit (M20)", func(h []byte) { h[6] |= 0x80 }, "flags"},
		{"sealed without the format bit", func(h []byte) { h[6] = walV2FlagSealed }, "flags"},
		{"a non-zero reserved byte (M21)", func(h []byte) { h[7] = 0x01 }, "reserved"},
		{"generation 0 (M25)", func(h []byte) { binary.BigEndian.PutUint64(h[16:24], 0) }, "generation"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			header := append([]byte(nil), valid...)
			tc.mutate(header)
			if _, err := parseWALv2Header(header); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("parseWALv2Header(% x) = %v, want a refusal naming the %s", header, err, tc.want)
			}
		})
	}
}

// TestWALCursorCarriesRule5AcrossAnEmptySegment: a cursor read that goes
// through an empty segment keeps the last sequence it read, so the first entry
// of the next non-empty segment must still rise above it (spec §3.3 rule 5).
// The image: segment 1 (1..a), an empty segment 2, a segment 3 holding only a
// repeat of sequence a, then segment 4 continuing at a+1. Mutation M07b: the
// cursor's rule 5 reset at a segment boundary, which skips the repeat as a
// sequence below the one wanted and reads on as if the log were whole.
func TestWALCursorCarriesRule5AcrossAnEmptySegment(t *testing.T) {
	dir := t.TempDir()
	segments := writeSegmentedLog(t, dir, nil, 4)
	first := segments[0]
	offsets, end := entryOffsetsOf(t, first)
	if len(offsets) < 2 {
		t.Fatalf("segment 1 holds %d entries, want at least 2", len(offsets))
	}
	repeat := append(append([]byte(nil), first[:walFileHeaderLen]...), first[offsets[len(offsets)-1]:end]...)
	images := map[int][]byte{
		2: segments[1][:walFileHeaderLen],
		3: repeat,
		4: segments[1],
	}
	for index, image := range images {
		if err := os.WriteFile(segmentFilePath(dir, index), image, 0o600); err != nil {
			t.Fatalf("writing segment %d: %v", index, err)
		}
	}
	for _, segment := range (&WAL{dir: dir}).listSegments() {
		if segment.index > 4 {
			if err := os.Remove(segmentFilePath(dir, segment.index)); err != nil {
				t.Fatalf("removing segment %d: %v", segment.index, err)
			}
		}
	}

	w, err := OpenWAL(dir, nil)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	defer closeWAL(t, w)
	cursor, err := w.OpenCursor(1)
	if err != nil {
		t.Fatalf("OpenCursor(1): %v", err)
	}
	batch, _, err := w.ReadBatch(cursor, ^uint64(0), 1<<30)
	if err == nil || !strings.Contains(err.Error(), "does not rise") {
		t.Fatalf("ReadBatch through an empty segment to a repeated sequence = %d entries, %v; want the rule 5 damage error", len(batch), err)
	}
}

// TestWALReadBatchRefusesAStaleEntryInAnEarlierSegment: an entry in an earlier
// segment that fails the CRC of format 2, because it was framed under another
// segment's generation or with no generation at all, is damage to a cursor
// read, reported at that entry as a failed checksum. Mutation M12: the reader
// leaving the generation out of the CRC, or not checking it.
func TestWALReadBatchRefusesAStaleEntryInAnEarlierSegment(t *testing.T) {
	cases := []struct {
		name  string
		keyed bool
		stale func(t *testing.T, keys WALKeySource, segment []byte, seq uint64) []byte
	}{
		{"plain, another generation", false, staleEntryFromAnotherLog},
		{"sealed, another generation", true, staleEntryFromAnotherLog},
		{"plain, no generation in the CRC", false, func(t *testing.T, _ WALKeySource, _ []byte, seq uint64) []byte {
			return rawPlainEntry(seq, 1, "q", "k", []byte("v1 framed"), true)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var keys WALKeySource
			if tc.keyed {
				keys = newSealedTestKeySource(t)
			}
			dir := t.TempDir()
			segments := writeSegmentedLog(t, dir, keys, 2)
			first := segments[0]
			offsets, end := entryOffsetsOf(t, first)
			lastSeq := uint64(len(offsets))
			staleEntry := tc.stale(t, keys, first, lastSeq+1)
			if err := os.WriteFile(segmentFilePath(dir, 1), append(append([]byte(nil), first[:end]...), staleEntry...), 0o600); err != nil {
				t.Fatalf("writing the image: %v", err)
			}

			w, err := OpenWAL(dir, keys)
			if err != nil {
				t.Fatalf("OpenWAL: %v", err)
			}
			defer closeWAL(t, w)
			cursor, err := w.OpenCursor(1)
			if err != nil {
				t.Fatalf("OpenCursor(1): %v", err)
			}
			_, _, err = w.ReadBatch(cursor, ^uint64(0), 1<<30)
			want := fmt.Sprintf("the entry at offset %d failed its checksum", end)
			if err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("ReadBatch over a stale entry in segment 1 = %v, want %q", err, want)
			}
		})
	}
}

// staleEntryFromAnotherLog returns the entry with sequence seq from a log
// written the same way as segment's, so it is framed under that other log's
// generation (and, sealed, against its header).
func staleEntryFromAnotherLog(t *testing.T, keys WALKeySource, segment []byte, seq uint64) []byte {
	t.Helper()
	other := writeSegmentedLog(t, t.TempDir(), keys, 2)
	for _, raw := range other {
		offsets, end := entryOffsetsOf(t, raw)
		for i, offset := range offsets {
			next := end
			if i+1 < len(offsets) {
				next = offsets[i+1]
			}
			entry := raw[offset:next]
			// Bytes 8 to 16 hold the sequence in both framings: the body's
			// first field when plain, the clear sequence when sealed.
			if binary.BigEndian.Uint64(entry[8:16]) == seq {
				if bytes.Equal(raw[16:24], segment[16:24]) {
					t.Fatal("the other log drew the same generation")
				}
				return append([]byte(nil), entry...)
			}
		}
	}
	t.Fatalf("the other log holds no entry with sequence %d", seq)
	return nil
}

// TestWALCursorCarriesRule5AcrossReadBatchCalls: rule 5 holds between two
// ReadBatch calls, not only within one. The first call reads segment 1 to its
// end; a segment 2 that appears afterwards starting below what the cursor
// already read is damage to the second call, not a run of entries below the
// one wanted to skip silently. Fails while the cursor forgets the last
// sequence it read: each call started rule 5 from nothing.
func TestWALCursorCarriesRule5AcrossReadBatchCalls(t *testing.T) {
	dir := t.TempDir()
	first := writeLog(t, dir, nil, 3)
	w, err := OpenWAL(dir, nil)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	defer closeWAL(t, w)
	cursor, err := w.OpenCursor(1)
	if err != nil {
		t.Fatalf("OpenCursor(1): %v", err)
	}
	batch, cursor, err := w.ReadBatch(cursor, ^uint64(0), 1<<30)
	if err != nil {
		t.Fatalf("the first ReadBatch: %v", err)
	}
	requireSeqs(t, "the first ReadBatch", batch, 3)

	// A segment beside the active one that repeats sequences 1 to 3 under
	// the same generation: every entry passes its CRC.
	if err := os.WriteFile(segmentFilePath(dir, w.activeIndex+1), first, 0o600); err != nil {
		t.Fatalf("writing the repeating segment: %v", err)
	}
	batch, _, err = w.ReadBatch(cursor, ^uint64(0), 1<<30)
	if err == nil || !strings.Contains(err.Error(), "does not rise") {
		t.Fatalf("the second ReadBatch over a segment starting below what the cursor read = %d entries, %v; want the rule 5 damage error", len(batch), err)
	}
}

// TestWALCursorBoundedReadsResumeAtTheEntryTheyStoppedAt: a ReadBatch that a
// bound stops (upTo, or maxBytes) has read the next entry without consuming
// it; the next call reads it again, and rule 5 must not compare it to itself.
// The guard for the cursor's last sequence moving only with its offset.
func TestWALCursorBoundedReadsResumeAtTheEntryTheyStoppedAt(t *testing.T) {
	for _, mode := range walKeyModes {
		t.Run(mode.name, func(t *testing.T) {
			dir := t.TempDir()
			keys := mode.keys(t)
			writeLog(t, dir, keys, 6)
			w, err := OpenWAL(dir, keys)
			if err != nil {
				t.Fatalf("OpenWAL: %v", err)
			}
			defer closeWAL(t, w)
			cursor, err := w.OpenCursor(1)
			if err != nil {
				t.Fatalf("OpenCursor(1): %v", err)
			}
			var read []WALEntry
			for upTo := uint64(2); upTo <= 6; upTo += 2 {
				batch, next, err := w.ReadBatch(cursor, upTo, 1)
				if err != nil {
					t.Fatalf("ReadBatch up to %d: %v", upTo, err)
				}
				read = append(read, batch...)
				cursor = next
				batch, next, err = w.ReadBatch(cursor, upTo, 1<<30)
				if err != nil {
					t.Fatalf("ReadBatch up to %d after a maxBytes stop: %v", upTo, err)
				}
				read = append(read, batch...)
				cursor = next
			}
			requireSeqs(t, "the bounded ReadBatch calls", read, 6)
		})
	}
}
