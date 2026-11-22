package storage

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// openWALOrFail opens the log in dir and closes it when the test ends.
func openWALOrFail(t *testing.T, dir string, keys WALKeySource) *WAL {
	t.Helper()
	w, err := OpenWAL(dir, keys)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	return w
}

// appendKeyed appends n entries with fixed-length keys and payloads, the shape
// writeLog uses, so an entry appended here has exactly the length of one
// writeLog wrote.
func appendKeyed(t *testing.T, w *WAL, first, n int) {
	t.Helper()
	for i := first; i < first+n; i++ {
		if _, err := w.Append("bucket", OpInsert, []byte(fmt.Sprintf("key%03d", i)), bytes.Repeat([]byte{'p'}, 40)); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
}

// headerOnlySegment is what a crash leaves when an open had named its new
// segment and written nothing to it: a v2 header in the given key mode.
func headerOnlySegment(keys WALKeySource) []byte {
	var epoch uint64
	if keys != nil {
		epoch = keys.CurrentEpoch()
	}
	return newWALv2Format(keys != nil, epoch).header
}

// segmentSize is the size of segment index in dir.
func segmentSize(t *testing.T, dir string, index int) int64 {
	t.Helper()
	info, err := os.Stat(segmentFilePath(dir, index))
	if err != nil {
		t.Fatalf("stat %s: %v", segmentFileName(index), err)
	}
	return info.Size()
}

// nonZeroExtent is the length of b up to and including its last non-zero byte.
func nonZeroExtent(b []byte) int64 {
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] != 0 {
			return int64(i + 1)
		}
	}
	return 0
}

// TestWALRestartTwiceWithoutWritesKeepsSequence (P1): an open that writes
// nothing still leaves a new, empty segment as the highest one, and the next
// open must find the last sequence below it, in the highest segment that holds
// an entry. The crash variant is the same log after a crash between an open
// naming its segment and any write: no Close, no state file. Break by: scanning
// only the last segment (the crash variant has no previous incarnation above 0
// to hide it). Fails on the parent: it scans the last segment only, finds no
// entry there, and gives sequence 1 again.
func TestWALRestartTwiceWithoutWritesKeepsSequence(t *testing.T) {
	for _, mode := range walKeyModes {
		t.Run(mode.name+"/restart without a write", func(t *testing.T) {
			keys := mode.keys(t)
			dir := t.TempDir()
			w := openWALOrFail(t, dir, keys)
			appendKeyed(t, w, 0, 3)
			closeWAL(t, w)
			w = openWALOrFail(t, dir, keys)
			closeWAL(t, w)

			w = openWALOrFail(t, dir, keys)
			defer closeWAL(t, w)
			requireNextAppend(t, w, 4)
		})

		t.Run(mode.name+"/a crash after an open named its segment", func(t *testing.T) {
			keys := mode.keys(t)
			dir := t.TempDir()
			w := openWALOrFail(t, dir, keys)
			appendKeyed(t, w, 0, 3)
			closeWAL(t, w)
			highest := w.listSegments()
			next := highest[len(highest)-1].index + 1
			if err := os.WriteFile(segmentFilePath(dir, next), headerOnlySegment(keys), 0o600); err != nil {
				t.Fatalf("writing the header-only segment: %v", err)
			}
			if err := os.Remove(filepath.Join(dir, "state.bson")); err != nil && !errors.Is(err, fs.ErrNotExist) {
				t.Fatalf("removing state.bson: %v", err)
			}

			w = openWALOrFail(t, dir, keys)
			defer closeWAL(t, w)
			requireNextAppend(t, w, 4)
		})
	}
}

// requireNextAppend appends one entry, requires it to get sequence want, and
// requires the log to read 1..want with no sequence twice.
func requireNextAppend(t *testing.T, w *WAL, want uint64) {
	t.Helper()
	seq, err := w.Append("bucket", OpInsert, []byte("next"), []byte("payload"))
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if seq != want {
		t.Fatalf("the next append got sequence %d, want %d", seq, want)
	}
	entries, err := w.ReadFrom(1)
	if err != nil {
		t.Fatalf("ReadFrom(1): %v", err)
	}
	requireSeqs(t, "ReadFrom(1)", entries, int(want))
}

// TestWALOpenFloorsAtThePreviousIncarnation (BD-2): when the chain's last link
// has a base above the last entry the open finds, the log continues above the
// base, never below it: a sequence peers may already hold is never given again.
// The missing sequences read as a hole. Break by: dropping the floor in
// mintIncarnation. Fails on the parent: the next append is 4.
func TestWALOpenFloorsAtThePreviousIncarnation(t *testing.T) {
	dir := t.TempDir()
	w := openWALOrFail(t, dir, nil)
	appendKeyed(t, w, 0, 3)
	closeWAL(t, w)

	chain, skips, _, err := loadIncarnationChain(dir)
	if err != nil {
		t.Fatalf("loading the chain: %v", err)
	}
	chain[len(chain)-1].Base = 10
	if err := writeIncarnationChain(dir, chain, skips); err != nil {
		t.Fatalf("raising the last link's base: %v", err)
	}

	w = openWALOrFail(t, dir, nil)
	defer closeWAL(t, w)
	seq, err := w.Append("bucket", OpInsert, []byte("next"), []byte("payload"))
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if seq != 11 {
		t.Fatalf("the next append got sequence %d, want 11: above the previous link's base of 10", seq)
	}
	cursor, err := w.OpenCursor(1)
	if err != nil {
		t.Fatalf("OpenCursor(1): %v", err)
	}
	var readErr error
	for i := 0; i < 5 && readErr == nil; i++ {
		_, cursor, readErr = w.ReadBatch(cursor, ^uint64(0), 1<<30)
	}
	if readErr == nil || !strings.Contains(readErr.Error(), "hole") {
		t.Fatalf("ReadBatch from 1 across sequences 4 to 10 = %v, want them reported as a hole", readErr)
	}
}

// TestWALOpenSyncsTheTailSegment: the open fsyncs the tail segment before it
// returns, whether or not it cut anything, on v1 and v2 tails: the bytes the
// scan read back can still be page cache after a process crash, and the open
// counts them as synced (fixes BUG-0718). Break by: fsyncing only a torn tail.
// Fails on the parent: an intact tail is never fsynced at open.
func TestWALOpenSyncsTheTailSegment(t *testing.T) {
	cases := []struct {
		name  string
		keys  func(t *testing.T) WALKeySource
		image func(t *testing.T, keys WALKeySource) []byte
	}{
		{"v2 plain", func(*testing.T) WALKeySource { return nil }, func(t *testing.T, keys WALKeySource) []byte {
			return writeLog(t, t.TempDir(), keys, 3)
		}},
		{"v2 sealed", func(t *testing.T) WALKeySource { return newSealedTestKeySource(t) }, func(t *testing.T, keys WALKeySource) []byte {
			return writeLog(t, t.TempDir(), keys, 3)
		}},
		{"v2 plain torn", func(*testing.T) WALKeySource { return nil }, func(t *testing.T, keys WALKeySource) []byte {
			raw := writeLog(t, t.TempDir(), keys, 3)
			return raw[:len(raw)-5]
		}},
		{"v1 plain", func(*testing.T) WALKeySource { return nil }, func(*testing.T, WALKeySource) []byte {
			return append([]byte(nil), fixtureRefPlaintextWALSegment...)
		}},
		{"v1 plain torn", func(*testing.T) WALKeySource { return nil }, func(*testing.T, WALKeySource) []byte {
			return append(append([]byte(nil), fixtureRefPlaintextWALSegment...), 0x40, 0, 0)
		}},
		{"v1 sealed", func(t *testing.T) WALKeySource { return newSealedTestKeySource(t) }, func(t *testing.T, keys WALKeySource) []byte {
			return buildV1SealedSegment(t, keys, 1, [][]byte{v1EntryBody(OpInsert, "b", "k1", "d1")})
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			keys := tc.keys(t)
			dir := t.TempDir()
			if err := os.WriteFile(segmentFilePath(dir, 1), tc.image(t, keys), 0o600); err != nil {
				t.Fatalf("placing the tail: %v", err)
			}
			var recorder walSyscallRecorder
			recorder.recordFromOpen(t)
			w := openWALOrFail(t, dir, keys)
			opened := recorder.seen()
			closeWAL(t, w)

			tailSynced := -1
			for i, event := range opened {
				if event == "fsync wal_00001.log" {
					tailSynced = i
				}
			}
			if tailSynced < 0 {
				t.Fatalf("the open never fsynced the tail segment wal_00001.log (events %v)", opened)
			}
			for i, event := range opened {
				if strings.HasPrefix(event, "rename ") && strings.HasSuffix(event, ">wal_00002.log") && i < tailSynced {
					t.Fatalf("the new segment was named before the tail was fsynced (events %v)", opened)
				}
			}
		})
	}
}

// TestWALOpenRemovesStateFile: a state.bson naming a lower segment as the
// active one is neither read nor obeyed, and the open removes it and a leftover
// state.bson.tmp after the new segment's name is durable, then fsyncs the
// directory; neither a rotation nor Close writes it again (fixes BUG-0720).
// Break by: still reading it, or writing it on rotation or Close. Fails on the
// parent: it appends to the segment the file names and keeps the file.
func TestWALOpenRemovesStateFile(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWALWithOptions(dir, nil, WALOptions{SegmentBytes: 600})
	if err != nil {
		t.Fatalf("OpenWALWithOptions: %v", err)
	}
	for i := 0; len(w.listSegments()) < 2; i++ {
		appendKeyed(t, w, i, 1)
	}
	last := w.LastSequence()
	closeWAL(t, w)

	stale, err := bson.Marshal(bson.M{"active_index": 1, "last_seq": int64(1)})
	if err != nil {
		t.Fatalf("encoding the stale state file: %v", err)
	}
	statePath := filepath.Join(dir, "state.bson")
	if err := os.WriteFile(statePath, stale, 0o600); err != nil {
		t.Fatalf("writing state.bson: %v", err)
	}
	if err := os.WriteFile(statePath+".tmp", []byte("half a state file"), 0o600); err != nil {
		t.Fatalf("writing state.bson.tmp: %v", err)
	}
	segment1, err := os.ReadFile(segmentFilePath(dir, 1))
	if err != nil {
		t.Fatalf("reading segment 1: %v", err)
	}

	var recorder walSyscallRecorder
	recorder.recordFromOpen(t)
	w, err = OpenWALWithOptions(dir, nil, WALOptions{SegmentBytes: 600})
	if err != nil {
		t.Fatalf("reopening: %v", err)
	}
	defer closeWAL(t, w)
	opened := recorder.seen()
	for _, name := range []string{"state.bson", "state.bson.tmp"} {
		if _, err := os.Stat(filepath.Join(dir, name)); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("%s is still in the log directory after the open (stat: %v)", name, err)
		}
	}
	requireRemovedAfterTheNewName(t, opened, "wal_00003.log")

	seq, err := w.Append("bucket", OpInsert, []byte("next"), []byte("payload"))
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if seq != last+1 {
		t.Fatalf("the next append got sequence %d, want %d", seq, last+1)
	}
	requireSegmentUnchanged(t, dir, 1, segment1)

	for i := 0; len(w.listSegments()) < 5; i++ {
		appendKeyed(t, w, 100+i, 1)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := os.Stat(statePath); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("a rotation or Close wrote state.bson again (stat: %v)", err)
	}
}

// requireRemovedAfterTheNewName requires the recorded open to remove
// state.bson and state.bson.tmp after the directory fsync that made newName
// durable, and to fsync the directory after the removals.
func requireRemovedAfterTheNewName(t *testing.T, events []string, newName string) {
	t.Helper()
	named, durable, removedState, removedTmp, syncedAfter := -1, -1, -1, -1, -1
	for i, event := range events {
		switch {
		case event == "rename wal_spare.tmp>"+newName:
			named = i
		case event == "syncDir wal" || strings.HasPrefix(event, "syncDir "):
			if named >= 0 && durable < 0 {
				durable = i
			}
			if removedState >= 0 && removedTmp >= 0 {
				syncedAfter = i
			}
		case event == "remove state.bson":
			removedState = i
		case event == "remove state.bson.tmp":
			removedTmp = i
		}
	}
	if named < 0 || durable < 0 {
		t.Fatalf("the open did not name %s and fsync the directory (events %v)", newName, events)
	}
	if removedState < durable || removedTmp < durable {
		t.Fatalf("state.bson or state.bson.tmp was not removed after %s's name was durable (events %v)", newName, events)
	}
	if syncedAfter < 0 {
		t.Fatalf("no directory fsync after the state files were removed (events %v)", events)
	}
}

// TestWALBirthNeverReplacesASegment: a birth refuses a target name that already
// exists rather than renaming over it, whatever led to the index, so a segment
// holding entries can never be replaced by an empty one (the S4 verifier's
// carry: a stale state.bson could set the active index below the highest
// segment). The error names the file and says to start with a new data
// directory. Break by: a plain rename (os.Rename replaces). Fails on the
// parent: the rename replaces segment 1 and its entries are gone.
func TestWALBirthNeverReplacesASegment(t *testing.T) {
	for _, mode := range walKeyModes {
		t.Run(mode.name, func(t *testing.T) {
			keys := mode.keys(t)
			dir := t.TempDir()
			w := openWALOrFail(t, dir, keys)
			defer closeWAL(t, w)
			appendKeyed(t, w, 0, 2)
			active := w.activeIndex
			before, err := os.ReadFile(segmentFilePath(dir, active))
			if err != nil {
				t.Fatalf("reading the active segment: %v", err)
			}

			w.mu.Lock()
			_, err = w.birthSegment(active)
			w.mu.Unlock()
			if err == nil {
				t.Fatalf("a birth renamed over %s, which holds entries", segmentFileName(active))
			}
			if !errors.Is(err, fs.ErrExist) || !strings.Contains(err.Error(), segmentFileName(active)) ||
				!strings.Contains(err.Error(), "new data directory") {
				t.Fatalf("the refused birth said %q, want an ErrExist naming %s and saying to start with a new data directory", err, segmentFileName(active))
			}
			requireSegmentUnchanged(t, dir, active, before)
			if _, err := os.Stat(filepath.Join(dir, walSpareTmpName)); !errors.Is(err, fs.ErrNotExist) {
				t.Fatalf("the refused birth left %s behind (stat: %v)", walSpareTmpName, err)
			}
			entries, err := w.ReadFrom(1)
			if err != nil {
				t.Fatalf("ReadFrom(1): %v", err)
			}
			requireSeqs(t, "ReadFrom(1) after the refused birth", entries, 2)
		})
	}
}

// TestWALv2CleanZeroTailIsNotARecovery: a v2 tail that ends in zeros after its
// last entry (the tail of a preallocated segment) is the end of the log, not a
// recovery: the open reports nothing, cuts the zeros, and appends to a new
// segment. Break by: reading a zero word after an entry as torn (a report).
// Fails on the parent: the zeros are reported as a repaired partial entry.
func TestWALv2CleanZeroTailIsNotARecovery(t *testing.T) {
	for _, mode := range walKeyModes {
		t.Run(mode.name, func(t *testing.T) {
			keys := mode.keys(t)
			raw := writeLog(t, t.TempDir(), keys, 3)
			dir := t.TempDir()
			if err := os.WriteFile(segmentFilePath(dir, 1), append(append([]byte(nil), raw...), make([]byte, 4096)...), 0o600); err != nil {
				t.Fatalf("writing the image: %v", err)
			}
			w := openWALOrFail(t, dir, keys)
			defer closeWAL(t, w)
			if recovery := w.Recovery(); recovery != nil {
				t.Fatalf("a clean zero tail was reported as a recovery: %+v", *recovery)
			}
			if got := segmentSize(t, dir, 1); got != int64(len(raw)) {
				t.Fatalf("the old tail is %d bytes after the open, want %d: cut at its last entry", got, len(raw))
			}
			requireNextAppend(t, w, 4)
			if w.activeIndex != 2 {
				t.Fatalf("the append went to segment %d, want 2: every open starts a new segment", w.activeIndex)
			}
		})
	}
}

// TestWALv2TornTailAtEveryByteAtOpen is TestWALv2TornTailAtEveryByte at the
// open (spec §7.1): for every k, the last entry cut to k bytes, zero padded or
// not. The open keeps the two entries before it, cuts the segment where the
// torn entry starts, reports exactly the non-zero bytes it dropped (no report
// when there are none), and the next append goes to a new segment with the
// next sequence. Break by: measuring the drop to the file size. Fails on the
// parent: it reports the zero padding as dropped.
func TestWALv2TornTailAtEveryByteAtOpen(t *testing.T) {
	for _, padded := range []bool{true, false} {
		for _, mode := range walKeyModes {
			t.Run(fmt.Sprintf("%s/zero padded=%v", mode.name, padded), func(t *testing.T) {
				keys := mode.keys(t)
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
					w, err := OpenWAL(dir, keys)
					if err != nil {
						t.Fatalf("k=%d: OpenWAL: %v", k, err)
					}
					dropped := nonZeroExtent(last[:k])
					recovery := w.Recovery()
					switch {
					case dropped == 0 && recovery != nil:
						t.Fatalf("k=%d: nothing non-zero was dropped and the open reported %+v", k, *recovery)
					case dropped > 0 && (recovery == nil || recovery.Segment != 1 || recovery.RecoveredTo != int64(lastStart) || recovery.DroppedBytes != dropped):
						t.Fatalf("k=%d: the open reported %+v, want segment 1 recovered to %d with %d bytes dropped", k, recovery, lastStart, dropped)
					}
					if got := segmentSize(t, dir, 1); got != int64(lastStart) {
						t.Fatalf("k=%d: the tail is %d bytes after the open, want %d", k, got, lastStart)
					}
					seq, err := w.Append("bucket", OpInsert, []byte("next"), []byte("payload"))
					if err != nil || seq != 3 {
						t.Fatalf("k=%d: the next append = %d, %v; want sequence 3", k, seq, err)
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
			})
		}
	}
}

// TestWALv2TornEntryWithZeroWordAndWrittenBody: sector writes are not
// ordered, so a torn entry can keep a zero length word while a later, complete
// and valid entry of the same generation landed behind it. The open must never
// write over that region: an append of exactly the torn entry's length at the
// end of the log would make the later entry follow it, and read back, with a
// sequence the log gives again. Break by: appending at validEnd instead of
// retiring the tail. Fails on the parent: it appends in place and the stale
// entry reads back.
func TestWALv2TornEntryWithZeroWordAndWrittenBody(t *testing.T) {
	for _, mode := range walKeyModes {
		t.Run(mode.name, func(t *testing.T) {
			keys := mode.keys(t)
			raw := writeLog(t, t.TempDir(), keys, 3)
			offsets, end := entryOffsetsOf(t, raw)
			image := append([]byte(nil), raw[:end]...)
			copy(image[offsets[1]:offsets[1]+8], make([]byte, 8))
			dir := t.TempDir()
			if err := os.WriteFile(segmentFilePath(dir, 1), image, 0o600); err != nil {
				t.Fatalf("writing the image: %v", err)
			}

			w := openWALOrFail(t, dir, keys)
			appendKeyed(t, w, 1, 1)
			closeWAL(t, w)

			w = openWALOrFail(t, dir, keys)
			defer closeWAL(t, w)
			entries, err := w.ReadFrom(1)
			if err != nil {
				t.Fatalf("ReadFrom(1): %v", err)
			}
			requireSeqs(t, "ReadFrom(1)", entries, 2)
			for _, entry := range entries {
				if string(entry.Key) == "key002" {
					t.Fatalf("the entry written behind the torn one read back as sequence %d", entry.Seq)
				}
			}
		})
	}
}

// TestWALOpenReportsBytesDroppedFromAWalkedOverSegment (BD-3): a segment the
// walk-back steps over, because it holds no entry, can still hold non-zero
// bytes after its header (a torn first entry whose word stayed zero). Those
// bytes are cut and reported, and the report names that segment. Break by:
// measuring only the tail segment. Fails on the parent: it reads the highest
// segment as empty and reports nothing.
func TestWALOpenReportsBytesDroppedFromAWalkedOverSegment(t *testing.T) {
	for _, mode := range walKeyModes {
		t.Run(mode.name, func(t *testing.T) {
			keys := mode.keys(t)
			dir := t.TempDir()
			writeLog(t, dir, keys, 3)
			next := (&WAL{dir: dir}).listSegments()
			index := next[len(next)-1].index + 1
			stray := append(append(make([]byte, 8), bytes.Repeat([]byte{0xab}, 100)...), make([]byte, 50)...)
			if err := os.WriteFile(segmentFilePath(dir, index), append(headerOnlySegment(keys), stray...), 0o600); err != nil {
				t.Fatalf("writing the walked-over segment: %v", err)
			}

			w := openWALOrFail(t, dir, keys)
			defer closeWAL(t, w)
			recovery := w.Recovery()
			if recovery == nil || recovery.Segment != index || recovery.RecoveredTo != walFileHeaderLen || recovery.DroppedBytes != 108 {
				t.Fatalf("the open reported %+v, want segment %d recovered to %d with 108 bytes dropped", recovery, index, walFileHeaderLen)
			}
			if got := segmentSize(t, dir, index); got != walFileHeaderLen {
				t.Fatalf("the walked-over segment is %d bytes after the open, want its %d-byte header", got, walFileHeaderLen)
			}
			requireNextAppend(t, w, 4)
		})
	}
}

// TestWALOpenWalksOverATailWhoseFirstEntryDoesNotRise: at open, the highest
// segment's first entry must rise above the first sequence of the previous
// non-empty segment (spec §3.3 rule 5 at open). A segment whose entries start
// again from 1 is not the log's tail: it is walked over and cut, and the log
// continues after the segment below it. Break by: scanning the candidate with
// no previous sequence.
func TestWALOpenWalksOverATailWhoseFirstEntryDoesNotRise(t *testing.T) {
	dir := t.TempDir()
	writeLog(t, dir, nil, 3)
	stale := writeLog(t, t.TempDir(), nil, 1)
	listed := (&WAL{dir: dir}).listSegments()
	index := listed[len(listed)-1].index + 1
	if err := os.WriteFile(segmentFilePath(dir, index), stale, 0o600); err != nil {
		t.Fatalf("writing the stale segment: %v", err)
	}
	w := openWALOrFail(t, dir, nil)
	defer closeWAL(t, w)
	if got := segmentSize(t, dir, index); got != walFileHeaderLen {
		t.Fatalf("the stale segment is %d bytes after the open, want its %d-byte header", got, walFileHeaderLen)
	}
	requireNextAppend(t, w, 4)
}

// TestReopenStartsANewSegmentAndKey replaces TestSealCounterRestoredFromTheFile:
// every open of a sealed log starts a new segment, so its file key is the new
// index's and its seal counter starts at 0; the old segment is never appended
// to under its old key again. Break by: adopting the old tail and restoring its
// counter.
func TestReopenStartsANewSegmentAndKey(t *testing.T) {
	dir := t.TempDir()
	keys := newSealedTestKeySource(t)
	restoreSealLimit(t, 8)

	w := openWALOrFail(t, dir, keys)
	appendKeyed(t, w, 0, 5)
	closeWAL(t, w)
	before, err := os.ReadFile(segmentFilePath(dir, 1))
	if err != nil {
		t.Fatalf("reading segment 1: %v", err)
	}

	reopened := openWALOrFail(t, dir, keys)
	if reopened.activeIndex != 2 || reopened.sealCounter != 0 {
		t.Fatalf("after the reopen the active segment is %d with seal counter %d, want 2 and 0", reopened.activeIndex, reopened.sealCounter)
	}
	wantAEAD, err := walEntryAEAD(keys, keys.CurrentEpoch(), 2)
	if err != nil {
		t.Fatalf("deriving segment 2's key: %v", err)
	}
	probe := []byte("probe")
	nonce := make([]byte, walNonceLen)
	if !bytes.Equal(reopened.sealAEAD.Seal(nil, nonce, probe, nil), wantAEAD.Seal(nil, nonce, probe, nil)) {
		t.Fatal("the reopened log seals under a key other than segment 2's")
	}
	appendKeyed(t, reopened, 5, 8)
	requireSegmentUnchanged(t, dir, 1, before)
	if got := len(reopened.listSegments()); got != 2 {
		t.Fatalf("8 appends under the new key left %d segments, want 2: the counter starts at 0", got)
	}
	closeWAL(t, reopened)
	readSealedWAL(t, dir, keys, 13)
}

// TestWALFirstOpenOfAV1LogStartsAV2Segment replaces
// TestWALv1TailKeepsItsFormatUntilARotation: a v1 tail is never appended to.
// The first open on this build repairs and fsyncs it by v1 rules and starts a
// v2 segment beside it, in both key modes; the v1 segment stays byte for byte
// what the earlier build wrote. Break by: appending to the tail.
func TestWALFirstOpenOfAV1LogStartsAV2Segment(t *testing.T) {
	cases := []struct {
		name      string
		keys      func(t *testing.T) WALKeySource
		segment   func(t *testing.T, keys WALKeySource) []byte
		entries   int
		wantFlags byte
	}{
		{"plain", func(*testing.T) WALKeySource { return nil }, func(*testing.T, WALKeySource) []byte {
			return append([]byte(nil), fixtureRefPlaintextWALSegment...)
		}, 3, 0x01},
		{"sealed", func(t *testing.T) WALKeySource { return newSealedTestKeySource(t) }, func(t *testing.T, keys WALKeySource) []byte {
			return buildV1SealedSegment(t, keys, 1, [][]byte{v1EntryBody(OpInsert, "b", "k1", "d1"), v1EntryBody(OpInsert, "b", "k2", "d2")})
		}, 2, 0x03},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			keys := tc.keys(t)
			dir := t.TempDir()
			v1 := tc.segment(t, keys)
			if err := os.WriteFile(segmentFilePath(dir, 1), v1, 0o600); err != nil {
				t.Fatalf("placing the v1 segment: %v", err)
			}
			w := openWALOrFail(t, dir, keys)
			defer closeWAL(t, w)
			requireNextAppend(t, w, uint64(tc.entries+1))
			requireSegmentUnchanged(t, dir, 1, v1)
			raw, err := os.ReadFile(segmentFilePath(dir, 2))
			if err != nil {
				t.Fatalf("reading segment 2: %v", err)
			}
			if !hasSegmentHeader(raw) || raw[6] != tc.wantFlags {
				t.Fatalf("segment 2 begins % x, want a v2 header with flags %#02x", raw[:min(len(raw), walFileHeaderLen)], tc.wantFlags)
			}
		})
	}
}

// TestWALOpenWithoutPreallocateRemovesTheReadySpare: a wal_spare.ready left by
// a start with preallocation on is removed by an open that does not
// preallocate, with one INFO line, since nothing would ever take it and it
// holds a segment's worth of disk; an open that preallocates keeps it, validates
// it and takes it as its new segment (spec §2.1 "At open", §3.4 step 7). Break
// by: leaving it.
func TestWALOpenWithoutPreallocateRemovesTheReadySpare(t *testing.T) {
	for _, preallocate := range []bool{false, true} {
		t.Run(fmt.Sprintf("preallocate=%v", preallocate), func(t *testing.T) {
			var logged bytes.Buffer
			previous := slog.Default()
			slog.SetDefault(slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelInfo})))
			t.Cleanup(func() { slog.SetDefault(previous) })

			dir := t.TempDir()
			readyPath := filepath.Join(dir, walSpareReadyName)
			spare := append(headerOnlySegment(nil), make([]byte, 4096)...)
			if err := os.WriteFile(readyPath, spare, 0o600); err != nil {
				t.Fatalf("writing %s: %v", walSpareReadyName, err)
			}
			spareInfo, err := os.Stat(readyPath)
			if err != nil {
				t.Fatal(err)
			}
			// The spare is a whole segment at this size, so a preallocating
			// open validates it and takes it as its new segment.
			w, err := OpenWALWithOptions(dir, nil, WALOptions{Preallocate: preallocate, SegmentBytes: int64(len(spare))})
			if err != nil {
				t.Fatalf("OpenWALWithOptions: %v", err)
			}
			defer closeWAL(t, w)
			_, statErr := os.Stat(readyPath)
			const line = "removed the preallocated spare segment left by an earlier start with storage.wal_preallocate on"
			if preallocate {
				active, err := os.Stat(segmentFilePath(dir, w.activeIndex))
				if err != nil || !os.SameFile(spareInfo, active) || strings.Contains(logged.String(), line) {
					t.Fatalf("an open that preallocates removed or reported the ready spare instead of taking it (stat: %v; log: %s)", err, logged.String())
				}
				return
			}
			if !errors.Is(statErr, fs.ErrNotExist) {
				t.Fatalf("%s is still there after an open without preallocation (stat: %v)", walSpareReadyName, statErr)
			}
			if strings.Count(logged.String(), line) != 1 {
				t.Fatalf("the removal was not reported once at INFO; the log:\n%s", logged.String())
			}
		})
	}
}

// TestWALOpenAfterTruncateStartsAboveTheHighestSegment: once retention has
// removed the lowest segments, the listing no longer starts at 1, and the new
// segment must still take the highest index + 1, never a count of segments
// (which names a segment that exists, and the birth refuses it) and never an
// index below the highest. Break by: next = len(segments) + 1.
func TestWALOpenAfterTruncateStartsAboveTheHighestSegment(t *testing.T) {
	for _, mode := range walKeyModes {
		t.Run(mode.name, func(t *testing.T) {
			keys := mode.keys(t)
			dir := t.TempDir()
			w, err := OpenWALWithOptions(dir, keys, WALOptions{SegmentBytes: 600})
			if err != nil {
				t.Fatalf("OpenWALWithOptions: %v", err)
			}
			for i := 0; len(w.listSegments()) < 4; i++ {
				appendKeyed(t, w, i, 1)
			}
			segments := w.listSegments()
			bound, _, err := w.firstSequenceOf(segments[2], -1)
			if err != nil {
				t.Fatalf("first sequence of %s: %v", segments[2].name, err)
			}
			if err := w.Truncate(bound); err != nil {
				t.Fatalf("Truncate(%d): %v", bound, err)
			}
			last := w.LastSequence()
			closeWAL(t, w)
			remaining := (&WAL{dir: dir}).listSegments()
			if remaining[0].index == 1 {
				t.Fatalf("Truncate(%d) kept segment 1; the scenario needs the listing to start above 1", bound)
			}
			highest := remaining[len(remaining)-1].index

			w, err = OpenWALWithOptions(dir, keys, WALOptions{SegmentBytes: 600})
			if err != nil {
				t.Fatalf("reopening a log whose lowest segment is %s: %v", remaining[0].name, err)
			}
			defer closeWAL(t, w)
			if w.activeIndex != highest+1 {
				t.Fatalf("the open started segment %d, want %d: one above the highest", w.activeIndex, highest+1)
			}
			seq, err := w.Append("bucket", OpInsert, []byte("next"), []byte("payload"))
			if err != nil || seq != last+1 {
				t.Fatalf("the next append = %d, %v; want sequence %d", seq, err, last+1)
			}
			entries, err := w.ReadFrom(bound)
			if err != nil {
				t.Fatalf("ReadFrom(%d): %v", bound, err)
			}
			if len(entries) == 0 || entries[0].Seq != bound || entries[len(entries)-1].Seq != last+1 {
				t.Fatalf("ReadFrom(%d) read %d entries, want %d through %d", bound, len(entries), bound, last+1)
			}
		})
	}
}
