package storage

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/sealed"
)

// walKeyModes runs a test in both key modes: plain, and sealed under one key
// source the subtest keeps for every open it makes.
var walKeyModes = []struct {
	name string
	keys func(t *testing.T) WALKeySource
}{
	{"plain", func(*testing.T) WALKeySource { return nil }},
	{"sealed", func(t *testing.T) WALKeySource { return newSealedTestKeySource(t) }},
}

// v1SealedSignature is bytes 0 to 5 of every sealed v1 segment: magic, version
// 0x02 and the WAL purpose. A v2 segment keeps them, so a build from before
// format 2 takes it for a sealed segment and refuses its flags.
var v1SealedSignature = []byte{0x00, 0x48, 0x44, 0x42, 0x02, 0x04}

// hasSegmentHeader reports whether raw starts with the 24-byte header of a
// sealed v1 or any v2 segment, rather than with a v1 plain entry.
func hasSegmentHeader(raw []byte) bool {
	return len(raw) >= walFileHeaderLen && bytes.Equal(raw[:6], v1SealedSignature)
}

// framesSealed reports whether the entries of the segment raw are sealed: a v1
// header is always sealed, a v2 one says so in flag 0x02.
func framesSealed(raw []byte) bool {
	if !hasSegmentHeader(raw) {
		return false
	}
	return raw[6]&0x01 == 0 || raw[6]&0x02 != 0
}

// entryOffsetsOf walks a segment's framing and returns the offset of each entry
// and the end of the last, reading only lengths, so it works on any format.
func entryOffsetsOf(t *testing.T, raw []byte) ([]int, int) {
	t.Helper()
	pos := 0
	if hasSegmentHeader(raw) {
		pos = walFileHeaderLen
	}
	framing := 8
	if framesSealed(raw) {
		framing = 8 + walSeqFieldLen
	}
	var offsets []int
	for pos+framing <= len(raw) {
		length := int(uint32(binary.BigEndian.Uint64(raw[pos:])))
		if length == 0 || pos+framing+length > len(raw) {
			break
		}
		offsets = append(offsets, pos)
		pos += framing + length
	}
	return offsets, pos
}

// writeLog opens a log in dir, appends n entries whose payloads have fixed
// lengths (so two logs written the same way have their entries at the same
// offsets), closes it, and returns segment 1's bytes.
func writeLog(t *testing.T, dir string, keys WALKeySource, n int) []byte {
	t.Helper()
	w, err := OpenWAL(dir, keys)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	for i := 0; i < n; i++ {
		if _, err := w.Append("bucket", OpInsert, []byte(fmt.Sprintf("key%03d", i)), bytes.Repeat([]byte{'p'}, 40)); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	raw, err := os.ReadFile(segmentFilePath(dir, 1))
	if err != nil {
		t.Fatalf("reading segment 1: %v", err)
	}
	return raw
}

// reopenAndRead opens the log in dir and returns it with every entry it reads.
func reopenAndRead(t *testing.T, dir string, keys WALKeySource) (*WAL, []WALEntry) {
	t.Helper()
	w, err := OpenWAL(dir, keys)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })
	entries, err := w.ReadFrom(1)
	if err != nil {
		t.Fatalf("ReadFrom(1): %v", err)
	}
	return w, entries
}

// TestWALv2HeaderMakesAnOldBuildRefuse pins the bytes that make a build from
// before format 2 refuse a v2 log instead of truncating it (spec §5): bytes 0 to
// 5 are the v1 sealed signature, so the old build parses the header, and byte 6
// is 0x01 (plain) or 0x03 (sealed), which the unchanged sealed.ParseFileHeader
// refuses. Every segment the log creates has this header, the first and every
// one after a rotation, in both key modes. Break by: a new version or purpose
// byte, or flags 0x00. Fails on the parent: a plain segment has no header and a
// sealed one has flags 0x00.
func TestWALv2HeaderMakesAnOldBuildRefuse(t *testing.T) {
	for _, mode := range walKeyModes {
		t.Run(mode.name, func(t *testing.T) {
			dir := t.TempDir()
			w, err := OpenWALWithOptions(dir, mode.keys(t), WALOptions{SegmentBytes: 1 << 10})
			if err != nil {
				t.Fatalf("OpenWALWithOptions: %v", err)
			}
			for i := 0; i < 20; i++ {
				if _, err := w.Append("bucket", OpInsert, []byte(fmt.Sprintf("key%02d", i)), make([]byte, 100)); err != nil {
					t.Fatalf("Append %d: %v", i, err)
				}
			}
			if err := w.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			segments := (&WAL{dir: dir}).listSegments()
			if len(segments) < 2 {
				t.Fatalf("20 appends into 1 KiB segments left %d segments, want a rotation", len(segments))
			}
			wantFlags := byte(0x01)
			if mode.name == "sealed" {
				wantFlags = 0x03
			}
			for _, segment := range segments {
				raw, err := os.ReadFile(segmentFilePath(dir, segment.index))
				if err != nil {
					t.Fatalf("reading %s: %v", segment.name, err)
				}
				if len(raw) < walFileHeaderLen || !bytes.Equal(raw[:6], v1SealedSignature) {
					t.Fatalf("%s starts % x, want the v1 sealed signature % x", segment.name, raw[:min(6, len(raw))], v1SealedSignature)
				}
				if raw[6] != wantFlags || raw[7] != 0 {
					t.Fatalf("%s has flags %#02x and reserved %#02x, want %#02x and 0", segment.name, raw[6], raw[7], wantFlags)
				}
				if binary.BigEndian.Uint64(raw[16:24]) == 0 {
					t.Fatalf("%s has a zero generation", segment.name)
				}
				if mode.name == "plain" && binary.BigEndian.Uint64(raw[8:16]) != 0 {
					t.Fatalf("plain %s names epoch %d, want 0", segment.name, binary.BigEndian.Uint64(raw[8:16]))
				}
				_, err = sealed.ParseFileHeader(raw[:walFileHeaderLen])
				if err == nil || !strings.Contains(err.Error(), "flags must be 0x00") {
					t.Fatalf("the unchanged sealed.ParseFileHeader on %s = %v, want the \"flags must be 0x00\" refusal an old build stops with", segment.name, err)
				}
			}
		})
	}
}

// TestWALv2StaleGenerationEndsTheLog: entries framed under another segment's
// generation, with sequences above the last valid one, end the log where they
// start. The generation is inside every entry's CRC, so the stale entries fail
// it and read as the end of the log: in a sealed segment too, as an end and
// never as a corruption error. Break by: leaving the generation out of the CRC.
// Fails on the parent: the CRC covers the body only, so the copied entries pass.
func TestWALv2StaleGenerationEndsTheLog(t *testing.T) {
	for _, mode := range walKeyModes {
		t.Run(mode.name, func(t *testing.T) {
			keys := mode.keys(t)
			const kept = 3
			current := writeLog(t, t.TempDir(), keys, kept)
			other := writeLog(t, t.TempDir(), keys, kept+2)
			_, end := entryOffsetsOf(t, current)
			image := append(append([]byte(nil), current[:end]...), other[end:]...)
			dir := t.TempDir()
			if err := os.WriteFile(segmentFilePath(dir, 1), image, 0o600); err != nil {
				t.Fatalf("writing the image: %v", err)
			}
			w, entries := reopenAndRead(t, dir, keys)
			requireSeqs(t, "ReadFrom(1) on a tail of another generation's entries", entries, kept)
			if got := w.LastSequence(); got != kept {
				t.Fatalf("LastSequence() = %d, want %d: the stale entries are not the log", got, kept)
			}
		})
	}
}

// TestWALv2SequenceMustRise: an entry whose CRC passes and whose sequence does
// not rise is the end of the log in the tail and damage in an earlier segment;
// and a segment whose first sequence is not above the last of the previous
// non-empty segment, with an empty segment between them, is damage to a reader.
// Break by: dropping rule 5 of spec §3.3, or comparing against the empty
// segment. Fails on the parent: a repeated entry is read as the log.
func TestWALv2SequenceMustRise(t *testing.T) {
	for _, mode := range walKeyModes {
		t.Run(mode.name+"/a repeated entry in the tail is the end", func(t *testing.T) {
			keys := mode.keys(t)
			dir := t.TempDir()
			raw := writeLog(t, dir, keys, 3)
			offsets, end := entryOffsetsOf(t, raw)
			image := append(append([]byte(nil), raw[:end]...), raw[offsets[0]:offsets[1]]...)
			if err := os.WriteFile(segmentFilePath(dir, 1), image, 0o600); err != nil {
				t.Fatalf("writing the image: %v", err)
			}
			w, entries := reopenAndRead(t, dir, keys)
			requireSeqs(t, "ReadFrom(1)", entries, 3)
			if got := w.LastSequence(); got != 3 {
				t.Fatalf("LastSequence() = %d, want 3: a repeated sequence is not the log's last", got)
			}
		})

		t.Run(mode.name+"/a repeated entry in an earlier segment is damage", func(t *testing.T) {
			keys := mode.keys(t)
			dir := t.TempDir()
			segments := writeSegmentedLog(t, dir, keys, 3)
			raw := segments[0]
			offsets, end := entryOffsetsOf(t, raw)
			image := append(append([]byte(nil), raw[:end]...), raw[offsets[0]:offsets[1]]...)
			if err := os.WriteFile(segmentFilePath(dir, 1), image, 0o600); err != nil {
				t.Fatalf("writing the image: %v", err)
			}
			w, err := OpenWAL(dir, keys)
			if err != nil {
				t.Fatalf("OpenWAL: %v", err)
			}
			defer closeWAL(t, w)
			if _, err := w.ReadFrom(1); err == nil || !strings.Contains(err.Error(), "damaged") {
				t.Fatalf("ReadFrom(1) over a repeated sequence in an earlier segment = %v, want the damage error", err)
			}
			cursor, err := w.OpenCursor(1)
			if err != nil {
				t.Fatalf("OpenCursor(1): %v", err)
			}
			if _, _, err := w.ReadBatch(cursor, ^uint64(0), 1<<30); err == nil {
				t.Fatal("ReadBatch from 1 read past a repeated sequence in an earlier segment, want an error")
			}
		})

		t.Run(mode.name+"/an empty segment between does not reset the rule", func(t *testing.T) {
			keys := mode.keys(t)
			dir := t.TempDir()
			segments := writeSegmentedLog(t, dir, keys, 4)
			// Segment 2 becomes empty (its header only), and segment 3 starts
			// again at segment 1's first sequence: a stale segment.
			emptyLen := 0
			if hasSegmentHeader(segments[1]) {
				emptyLen = walFileHeaderLen
			}
			if err := os.WriteFile(segmentFilePath(dir, 2), segments[1][:emptyLen], 0o600); err != nil {
				t.Fatalf("emptying segment 2: %v", err)
			}
			if err := os.WriteFile(segmentFilePath(dir, 3), segments[0], 0o600); err != nil {
				t.Fatalf("replacing segment 3: %v", err)
			}
			w, err := OpenWAL(dir, keys)
			if err != nil {
				t.Fatalf("OpenWAL: %v", err)
			}
			defer closeWAL(t, w)
			if _, err := w.ReadFrom(1); err == nil || !strings.Contains(err.Error(), "damaged") {
				t.Fatalf("ReadFrom(1) over a segment that starts below the last non-empty one = %v, want the damage error", err)
			}
		})
	}
}

// writeSegmentedLog writes a log of at least want segments in dir, through
// small segments, closes it, and returns each segment's bytes.
func writeSegmentedLog(t *testing.T, dir string, keys WALKeySource, want int) [][]byte {
	t.Helper()
	w, err := OpenWALWithOptions(dir, keys, WALOptions{SegmentBytes: 600})
	if err != nil {
		t.Fatalf("OpenWALWithOptions: %v", err)
	}
	for i := 0; i < 400 && len(w.listSegments()) < want+1; i++ {
		if _, err := w.Append("bucket", OpInsert, []byte(fmt.Sprintf("key%03d", i)), bytes.Repeat([]byte{'s'}, 100)); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	var segments [][]byte
	for _, segment := range (&WAL{dir: dir}).listSegments() {
		raw, err := os.ReadFile(segmentFilePath(dir, segment.index))
		if err != nil {
			t.Fatalf("reading %s: %v", segment.name, err)
		}
		segments = append(segments, raw)
	}
	if len(segments) < want {
		t.Fatalf("the log has %d segments, want at least %d", len(segments), want)
	}
	return segments
}

// TestWALv2ZeroRegionNeverAuthenticates: in a sealed segment a zeroed entry
// region is an end, not an entry; and a forged frame whose CRC matches its zero
// bytes (with a sequence that rises, so nothing before the AEAD stops it) is
// opened, fails, and is reported as corruption, never decoded. Break by: a
// "zero frame" shortcut that skips the AEAD. Fails on the parent: its CRC covers
// the on-disk bytes only, so the frame forged over gen and seq fails the CRC and
// reads as a torn tail.
func TestWALv2ZeroRegionNeverAuthenticates(t *testing.T) {
	keys := newSealedTestKeySource(t)

	t.Run("a zeroed region is an end", func(t *testing.T) {
		dir := t.TempDir()
		raw := writeLog(t, dir, keys, 3)
		image := append(append([]byte(nil), raw...), make([]byte, 256)...)
		if err := os.WriteFile(segmentFilePath(dir, 1), image, 0o600); err != nil {
			t.Fatalf("writing the image: %v", err)
		}
		w, entries := reopenAndRead(t, dir, keys)
		requireSeqs(t, "ReadFrom(1)", entries, 3)
		if got := w.LastSequence(); got != 3 {
			t.Fatalf("LastSequence() = %d, want 3", got)
		}
	})

	t.Run("a forged frame over zeros is corruption", func(t *testing.T) {
		dir := t.TempDir()
		raw := writeLog(t, dir, keys, 3)
		const forgedLen = 64
		seqField := binary.BigEndian.AppendUint64(nil, 4)
		zeros := make([]byte, forgedLen)
		checksum := crc32.Update(0, crc32cTable, raw[16:24])
		checksum = crc32.Update(checksum, crc32cTable, seqField)
		checksum = crc32.Update(checksum, crc32cTable, zeros)
		frame := binary.BigEndian.AppendUint64(nil, uint64(forgedLen)|uint64(checksum)<<32)
		frame = append(append(frame, seqField...), zeros...)
		image := append(append([]byte(nil), raw...), frame...)
		if err := os.WriteFile(segmentFilePath(dir, 1), image, 0o600); err != nil {
			t.Fatalf("writing the image: %v", err)
		}
		w, err := OpenWAL(dir, keys)
		if err == nil {
			_ = w.Close()
			t.Fatal("OpenWAL accepted a forged frame over zero bytes, want the corruption error")
		}
		if !strings.Contains(err.Error(), "is corrupt") {
			t.Fatalf("OpenWAL on a forged frame = %v, want it reported as corruption", err)
		}
	})
}

// TestWALv2TornTailAtEveryByte, at the scan (spec §7.1; the open's repair of it
// is S5): for every k, the last entry cut to its first k bytes, and either
// followed by zeros (a preallocated segment's tail) or not (the file ends
// there). The scan ends exactly where that entry starts, with the entries
// before it, and the reopened log gives the next append the next sequence with
// no entry read twice. Break by: rule 4 of spec §3.3 (either variant); rule 3
// (a length longer than what remains) only the unpadded variant can catch,
// because the zeros make every claimed length fit (mutation M27 of the S3
// verifier).
func TestWALv2TornTailAtEveryByte(t *testing.T) {
	for _, padded := range []bool{true, false} {
		for _, mode := range walKeyModes {
			t.Run(fmt.Sprintf("%s/zero padded=%v", mode.name, padded), func(t *testing.T) {
				tornTailAtEveryByte(t, mode.keys(t), padded)
			})
		}
	}
}

// tornTailAtEveryByte is one variant of TestWALv2TornTailAtEveryByte.
func tornTailAtEveryByte(t *testing.T, keys WALKeySource, padded bool) {
	t.Helper()
	raw := writeLog(t, t.TempDir(), keys, 3)
	offsets, end := entryOffsetsOf(t, raw)
	lastStart := offsets[2]
	last := raw[lastStart:end]
	for k := 0; k < len(last); k++ {
		dir := t.TempDir()
		image := append(append([]byte(nil), raw[:lastStart]...), last[:k]...)
		if padded {
			image = append(image, make([]byte, len(last)+64)...)
		}
		if err := os.WriteFile(segmentFilePath(dir, 1), image, 0o600); err != nil {
			t.Fatalf("k=%d: writing the image: %v", k, err)
		}
		scanner := &WAL{dir: dir, keys: keys}
		scanned, err := scanWholeSegment(scanner, segmentRef{name: segmentFileName(1), index: 1})
		if err != nil {
			t.Fatalf("k=%d: scanning: %v", k, err)
		}
		if scanned.validEnd != int64(lastStart) || scanned.entryCount != 2 {
			t.Fatalf("k=%d: the scan ended at %d with %d entries, want %d with 2", k, scanned.validEnd, scanned.entryCount, lastStart)
		}

		w, err := OpenWAL(dir, keys)
		if err != nil {
			t.Fatalf("k=%d: OpenWAL: %v", k, err)
		}
		seq, err := w.Append("bucket", OpInsert, []byte("next"), []byte("payload"))
		if err != nil {
			t.Fatalf("k=%d: Append: %v", k, err)
		}
		if seq != 3 {
			t.Fatalf("k=%d: the next append got sequence %d, want 3", k, seq)
		}
		entries, err := w.ReadFrom(1)
		if err != nil {
			t.Fatalf("k=%d: ReadFrom(1): %v", k, err)
		}
		requireSeqs(t, fmt.Sprintf("k=%d: ReadFrom(1)", k), entries, 3)
		if err := w.Close(); err != nil {
			t.Fatalf("k=%d: Close: %v", k, err)
		}
	}
}

// buildV1SealedSegment writes, by hand, a sealed segment as a build before
// format 2 wrote it: the 24-byte header with flags 0 and generation 0, then
// entries framed as length+CRC of the on-disk bytes, the clear sequence, and
// nonce || ciphertext || tag sealed against header || seq || offset. Sequences
// start at 1.
func buildV1SealedSegment(t *testing.T, keys WALKeySource, segmentIndex int, bodies [][]byte) []byte {
	t.Helper()
	epoch := keys.CurrentEpoch()
	header := sealed.MarshalFileHeader(&sealed.FileHeader{
		Magic: sealed.Magic, Version: sealed.FileVersion, Purpose: sealed.PurposeWALEntry, EpochID: epoch,
	})
	aead, err := walEntryAEAD(keys, epoch, segmentIndex)
	if err != nil {
		t.Fatalf("building the v1 segment's AEAD: %v", err)
	}
	segment := append([]byte(nil), header...)
	for i, body := range bodies {
		seq := uint64(i + 1)
		nonce := make([]byte, walNonceLen)
		if _, err := rand.Read(nonce); err != nil {
			t.Fatalf("nonce: %v", err)
		}
		onDisk := aead.Seal(nonce, nonce, body, walEntryAAD(header, seq, int64(len(segment))))
		segment = binary.BigEndian.AppendUint64(segment, uint64(len(onDisk))|uint64(crc32.Checksum(onDisk, crc32cTable))<<32)
		segment = binary.BigEndian.AppendUint64(segment, seq)
		segment = append(segment, onDisk...)
	}
	return segment
}

// v1EntryBody is an entry body after its sequence, as parseWALEntry reads it:
// op, bucket length, bucket, key length, key, data (no origin block).
func v1EntryBody(op byte, bucket, key, data string) []byte {
	body := []byte{op}
	body = binary.BigEndian.AppendUint32(body, uint32(len(bucket)))
	body = append(body, bucket...)
	body = binary.BigEndian.AppendUint32(body, uint32(len(key)))
	body = append(body, key...)
	return append(body, data...)
}

// TestWALReadsV1Segments: segments written before format 2 still read by v1
// rules: the frozen plain fixtures, a hand-built sealed v1 segment, and a v1
// torn tail, repaired by the v1 scan with its v1 report. Break by: dropping the
// v1 reader (every segment parsed as v2).
func TestWALReadsV1Segments(t *testing.T) {
	t.Run("the frozen plain fixture", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(segmentFilePath(dir, 1), fixtureRefPlaintextWALSegment, 0o600); err != nil {
			t.Fatalf("placing the fixture: %v", err)
		}
		w, entries := reopenAndRead(t, dir, nil)
		if w.Recovery() != nil {
			t.Fatalf("the v1 fixture needed a recovery: %+v", *w.Recovery())
		}
		want := []struct {
			op        byte
			key, data string
		}{{OpInsert, "key1", "value1_data"}, {OpUpdate, "key2", "value2_data_longer"}, {OpDelete, "key3", ""}}
		requireSeqs(t, "ReadFrom(1) of the v1 fixture", entries, len(want))
		for i, entry := range entries {
			if entry.Op != want[i].op || entry.Bucket != "test_bucket" || string(entry.Key) != want[i].key || string(entry.Data) != want[i].data {
				t.Fatalf("entry %d read as %+v, want op %d test_bucket %q %q", i+1, entry, want[i].op, want[i].key, want[i].data)
			}
		}
	})

	t.Run("a sealed v1 segment", func(t *testing.T) {
		keys := newSealedTestKeySource(t)
		dir := t.TempDir()
		segment := buildV1SealedSegment(t, keys, 1, [][]byte{v1EntryBody(OpInsert, "b", "k1", "d1"), v1EntryBody(OpInsert, "b", "k2", "d2")})
		if err := os.WriteFile(segmentFilePath(dir, 1), segment, 0o600); err != nil {
			t.Fatalf("placing the v1 sealed segment: %v", err)
		}
		_, entries := reopenAndRead(t, dir, keys)
		requireSeqs(t, "ReadFrom(1) of the v1 sealed segment", entries, 2)
		if string(entries[1].Key) != "k2" || string(entries[1].Data) != "d2" {
			t.Fatalf("entry 2 read as %+v", entries[1])
		}
	})

	// The v1 variants of TestWALRecoversFromTruncatedBody and
	// TestSealedWALTornTailRepairsWithoutAKey, whose own bodies now run on v2
	// segments: the same damage on a segment an earlier build wrote.
	t.Run("a v1 body cut short is repaired by v1 rules", func(t *testing.T) {
		dir := t.TempDir()
		third := rawPlainEntry(4, 1, "q", "k", make([]byte, 4096), true)
		torn := append(append([]byte(nil), fixtureRefPlaintextWALSegment...), third[:len(third)-2048]...)
		if err := os.WriteFile(segmentFilePath(dir, 1), torn, 0o600); err != nil {
			t.Fatalf("placing the torn fixture: %v", err)
		}
		w, entries := reopenAndRead(t, dir, nil)
		requireSeqs(t, "ReadFrom(1) after the v1 repair", entries, 3)
		if recovery := w.Recovery(); recovery == nil || recovery.RecoveredTo != int64(len(fixtureRefPlaintextWALSegment)) {
			t.Fatalf("the v1 repair reported %+v, want the segment cut back to %d", recovery, len(fixtureRefPlaintextWALSegment))
		}
	})

	t.Run("a sealed v1 torn tail is repaired without a key", func(t *testing.T) {
		keys := newSealedTestKeySource(t)
		dir := t.TempDir()
		segment := buildV1SealedSegment(t, keys, 1, [][]byte{v1EntryBody(OpInsert, "b", "k1", "d1"), v1EntryBody(OpInsert, "b", "k2", "d2")})
		offsets, _ := entryOffsetsOf(t, segment)
		torn := append([]byte(nil), segment[:offsets[1]+8]...)
		if err := os.WriteFile(segmentFilePath(dir, 1), torn, 0o600); err != nil {
			t.Fatalf("placing the torn v1 sealed segment: %v", err)
		}
		w, err := OpenWAL(dir, nil)
		if err != nil {
			t.Fatalf("a torn v1 sealed tail must be repaired without a key, got: %v", err)
		}
		defer closeWAL(t, w)
		recovery := w.Recovery()
		if recovery == nil || recovery.RecoveredTo != int64(offsets[1]) || recovery.DroppedBytes != 8 {
			t.Fatalf("the v1 sealed repair reported %+v, want recovered to %d with 8 bytes dropped", recovery, offsets[1])
		}
		if w.LastSequence() != 1 {
			t.Fatalf("LastSequence() = %d after the repair, want 1 from the clear framing", w.LastSequence())
		}
	})

	t.Run("a v1 torn tail is repaired by v1 rules", func(t *testing.T) {
		dir := t.TempDir()
		torn := append(append([]byte(nil), fixtureRefPlaintextWALSegment...), 0x40, 0, 0, 0, 0, 0, 0, 0)
		if err := os.WriteFile(segmentFilePath(dir, 1), torn, 0o600); err != nil {
			t.Fatalf("placing the torn fixture: %v", err)
		}
		w, entries := reopenAndRead(t, dir, nil)
		requireSeqs(t, "ReadFrom(1) after the v1 repair", entries, 3)
		recovery := w.Recovery()
		if recovery == nil {
			t.Fatal("a torn v1 tail was not reported")
		}
		if recovery.RecoveredTo != int64(len(fixtureRefPlaintextWALSegment)) || recovery.DroppedBytes != 8 ||
			!strings.Contains(recovery.Reason, "implausible entry length 0") {
			t.Fatalf("the v1 repair reported %+v, want recovered to %d, 8 bytes dropped, an implausible length", *recovery, len(fixtureRefPlaintextWALSegment))
		}
	})
}

// requireSegmentUnchanged requires segment index in dir to hold exactly want.
func requireSegmentUnchanged(t *testing.T, dir string, index int, want []byte) {
	t.Helper()
	got, err := os.ReadFile(segmentFilePath(dir, index))
	if err != nil {
		t.Fatalf("reading segment %d: %v", index, err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("segment %d changed after the rotation", index)
	}
}

// walHeader is a v1 plain entry's 8-byte word: the body's length and its
// CRC32C. Only tests build v1 entries now; the writer frames every new segment
// in format 2 (frameWord with walSegmentFormat.checksum).
func walHeader(bodyLen int, body []byte) []byte {
	return frameWord(bodyLen, crc32.Checksum(body, crc32cTable))
}

// scanWholeSegment scans one segment on its own, as the scan sees it with no
// earlier segment to apply rule 5 against (prevSeq 0). The open does not scan
// this way: it compares a v2 candidate with the segment below it
// (WAL.retireSegment).
func scanWholeSegment(w *WAL, seg segmentRef) (walScanResult, error) {
	f, err := os.Open(filepath.Join(w.dir, seg.name))
	if err != nil {
		return walScanResult{}, fmt.Errorf("opening segment %s: %w", seg.name, err)
	}
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil {
		return walScanResult{}, fmt.Errorf("stat segment %s: %w", seg.name, err)
	}
	return w.scanSegmentFile(f, info.Size(), seg.index, 0)
}
