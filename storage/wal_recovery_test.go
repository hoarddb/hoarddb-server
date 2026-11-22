package storage

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// These tests pin the recovery contract of the write-ahead log. Every case
// below is a disk state a crash or a full disk can produce, and the failure
// they guard against is a node that crash-loops on restart: the log used to
// allocate a buffer from an unvalidated length prefix, so an entry whose header
// said "16 exabytes" reached make([]byte, ...) and panicked with
// "makeslice: len out of range". That restart reproduced deterministically
// forever, because nothing ever rewrote the bad bytes.

// appendTestEntries writes n entries and returns their sequence numbers.
func appendTestEntries(t *testing.T, w *WAL, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if _, err := w.Append("docs", OpInsert, []byte("key"), []byte("payload")); err != nil {
			t.Fatalf("append entry %d: %v", i, err)
		}
	}
}

func activeSegmentPath(t *testing.T, dir string, w *WAL) string {
	t.Helper()
	return filepath.Join(dir, fmt.Sprintf("wal_%05d.log", w.activeIndex))
}

// TestWALRecoversFromTornTailWithHugeLengthPrefix is the exact signature of the
// crash that took a node down for good: a length prefix describing far more
// bytes than the file holds. Recovery must keep the entries before it and
// discard the tail, in that order, without panicking. This is the v2 variant
// (the v1 one is in TestWALReadsV1Segments): the open cuts the segment at its
// last entry, reports the 8 non-zero bytes it dropped, and appends to a new
// segment.
func TestWALRecoversFromTornTailWithHugeLengthPrefix(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWAL(dir, nil)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	appendTestEntries(t, w, 3)
	goodEnd := int64(0)
	if info, err := w.activeFile.Stat(); err != nil {
		t.Fatalf("stat active segment: %v", err)
	} else {
		goodEnd = info.Size()
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// A header whose low half claims an impossible length, exactly what a
	// partial write of a length prefix leaves behind.
	path := activeSegmentPath(t, dir, w)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("open segment for corruption: %v", err)
	}
	// The low half is the length; every byte is non-zero, so all 8 count as
	// dropped (a v2 report counts the non-zero extent).
	junk := make([]byte, 8)
	binary.BigEndian.PutUint64(junk, 1<<62|0xfffffff1)
	if _, err := f.Write(junk); err != nil {
		t.Fatalf("write torn header: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close corrupted segment: %v", err)
	}

	reopened, err := OpenWAL(dir, nil)
	if err != nil {
		t.Fatalf("OpenWAL on a torn segment must recover, got: %v", err)
	}
	defer func() { _ = reopened.Close() }()

	rec := reopened.Recovery()
	if rec == nil {
		t.Fatal("no recovery recorded for a torn segment")
	}
	if rec.RecoveredTo != goodEnd {
		t.Fatalf("recovered to offset %d, want %d (the end of the last complete entry)", rec.RecoveredTo, goodEnd)
	}
	if rec.DroppedBytes != 8 {
		t.Fatalf("dropped %d bytes, want the 8-byte torn header", rec.DroppedBytes)
	}
	if info, err := os.Stat(path); err != nil || info.Size() != goodEnd {
		t.Fatalf("the torn segment after the open: %v, %v; want it cut to %d", info, err, goodEnd)
	}
	if reopened.activeIndex != 2 {
		t.Fatalf("the open made segment %d active, want 2: appends go to a new segment", reopened.activeIndex)
	}

	entries, err := reopened.ReadFrom(1)
	if err != nil {
		t.Fatalf("ReadFrom after recovery: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("recovered %d entries, want the 3 written before the damage", len(entries))
	}
}

// A body cut short mid-write: the length prefix is intact but the bytes it
// describes were never all written. The checksum catches this even when the
// length happens to look plausible. This is the v2 variant (the v1 one is in
// TestWALReadsV1Segments): the same damage after the last entry of a crash
// image, cut at that entry, with appends going to a new segment.
func TestWALRecoversFromTruncatedBody(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWAL(dir, nil)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	appendTestEntries(t, w, 2)
	goodEnd, err := w.activeFile.Stat()
	if err != nil {
		t.Fatalf("stat active segment: %v", err)
	}
	goodSize := goodEnd.Size()

	// Append a complete third entry, then cut it in half: header and partial
	// body, which is what a full disk leaves behind.
	if _, err := w.Append("docs", OpInsert, []byte("key"), make([]byte, 4096)); err != nil {
		t.Fatalf("append third entry: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	path := activeSegmentPath(t, dir, w)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat segment: %v", err)
	}
	if err := os.Truncate(path, info.Size()-2048); err != nil {
		t.Fatalf("truncate segment mid-entry: %v", err)
	}

	reopened, err := OpenWAL(dir, nil)
	if err != nil {
		t.Fatalf("OpenWAL on a half-written entry must recover, got: %v", err)
	}
	defer func() { _ = reopened.Close() }()

	if rec := reopened.Recovery(); rec == nil {
		t.Fatal("no recovery recorded for a half-written entry")
	} else if rec.Segment != 1 || rec.RecoveredTo != goodSize {
		t.Fatalf("recovered segment %d to offset %d, want segment 1 to %d (the end of the second entry)", rec.Segment, rec.RecoveredTo, goodSize)
	}
	if info, err := os.Stat(path); err != nil || info.Size() != goodSize {
		t.Fatalf("the torn segment after the open: %v, %v; want it cut to %d", info, err, goodSize)
	}

	entries, err := reopened.ReadFrom(1)
	if err != nil {
		t.Fatalf("ReadFrom after recovery: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("recovered %d entries, want the 2 complete ones", len(entries))
	}

	// The repaired log must still accept writes, and they must be readable.
	if _, err := reopened.Append("docs", OpInsert, []byte("after"), []byte("recovery")); err != nil {
		t.Fatalf("append after repair: %v", err)
	}
	entries, err = reopened.ReadFrom(1)
	if err != nil {
		t.Fatalf("ReadFrom after appending to a repaired log: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("recovered %d entries after repair and one append, want 3", len(entries))
	}
}

// An entry written before the checksum existed has a zero high half. It must
// still read, or an upgrade would silently lose the log.
func TestWALReadsEntriesWrittenWithoutChecksum(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWAL(dir, nil)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Hand-build a legacy entry: length in the low 32 bits, zero high half.
	body := make([]byte, 0, 64)
	seqBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(seqBuf, 7)
	body = append(body, seqBuf...)
	body = append(body, OpInsert)
	bucketBytes := []byte("docs")
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(bucketBytes)))
	body = append(body, lenBuf...)
	body = append(body, bucketBytes...)
	key := []byte("legacykey")
	binary.BigEndian.PutUint32(lenBuf, uint32(len(key)))
	body = append(body, lenBuf...)
	body = append(body, key...)
	body = append(body, []byte("legacypayload")...)

	entry := make([]byte, 8)
	binary.BigEndian.PutUint64(entry, uint64(uint32(len(body))))
	entry = append(entry, body...)

	path := filepath.Join(dir, "wal_00001.log")
	if err := os.WriteFile(path, entry, 0o644); err != nil {
		t.Fatalf("write legacy segment: %v", err)
	}

	reopened, err := OpenWAL(dir, nil)
	if err != nil {
		t.Fatalf("OpenWAL on a legacy segment: %v", err)
	}
	defer func() { _ = reopened.Close() }()

	if rec := reopened.Recovery(); rec != nil {
		t.Fatalf("legacy segment reported as damaged: %+v", rec)
	}
	entries, err := reopened.ReadFrom(1)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("read %d entries from a legacy segment, want 1", len(entries))
	}
	if entries[0].Seq != 7 || entries[0].Bucket != "docs" || string(entries[0].Key) != "legacykey" || string(entries[0].Data) != "legacypayload" {
		t.Fatalf("legacy entry decoded as %+v", entries[0])
	}
	if got := reopened.LastSequence(); got != 7 {
		t.Fatalf("LastSequence = %d, want 7 from the legacy entry", got)
	}
}

// Damage in a segment that is not the active one is a hole in the middle of the
// log. The entries after it cannot be trusted, so this stops with an error
// naming the file instead of quietly returning a shorter history.
func TestWALRefusesDamageInAnEarlierSegment(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWAL(dir, nil)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	// Force a rotation by shrinking the file limit, then write into segment 2.
	w.maxFileSize = 64
	appendTestEntries(t, w, 4)
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	segments := w.listSegments()
	if len(segments) < 2 {
		t.Fatalf("expected a rotated log, got %d segment(s)", len(segments))
	}

	// Corrupt the first segment's tail.
	first := filepath.Join(dir, segments[0].name)
	info, err := os.Stat(first)
	if err != nil {
		t.Fatalf("stat first segment: %v", err)
	}
	if err := os.Truncate(first, info.Size()-4); err != nil {
		t.Fatalf("truncate first segment: %v", err)
	}

	reopened, err := OpenWAL(dir, nil)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	defer func() { _ = reopened.Close() }()

	if _, err := reopened.ReadFrom(1); err == nil {
		t.Fatal("ReadFrom accepted a damaged non-active segment; want an error naming it")
	}
}

// The writer must not leave a torn entry behind when a write fails: the next
// append would otherwise start on top of the partial bytes.
func TestWALAppendRejectsEntryOverTheReadLimit(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWAL(dir, nil)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	defer func() { _ = w.Close() }()

	oversized := make([]byte, walMaxEntrySize+1)
	if _, err := w.Append("docs", OpInsert, []byte("key"), oversized); err == nil {
		t.Fatal("Append accepted an entry the reader could never return; want rejection")
	}
	// The refusal must leave the log usable and the sequence unburned.
	if _, err := w.Append("docs", OpInsert, []byte("key"), []byte("small")); err != nil {
		t.Fatalf("append after rejection: %v", err)
	}
	entries, err := w.ReadFrom(1)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("log holds %d entries after one rejected and one accepted append, want 1", len(entries))
	}
	if entries[0].Seq != 1 {
		t.Fatalf("accepted entry has sequence %d, want 1: a rejected append must not burn a sequence", entries[0].Seq)
	}
}
