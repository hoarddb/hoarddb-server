package storage

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// segmentBirthWatch records every open's and rotation's calls and, at each
// rename to a wal_N.log name, checks the file being named: it must already
// have a parseable v2 header, fsynced after its last write (spec §2.1, owner
// D9). A name a pre-change build scans must never point at a headerless file.
type segmentBirthWatch struct {
	recorder walSyscallRecorder
	mu       sync.Mutex
	named    map[string]bool
	problems []string
}

func watchSegmentBirths(t *testing.T) *segmentBirthWatch {
	t.Helper()
	watch := &segmentBirthWatch{named: map[string]bool{}}
	watch.recorder.beforeRename = func(oldPath, newPath string) {
		newName := filepath.Base(newPath)
		if !isWALSegmentFile(newName) && newName != walSpareReadyName {
			return
		}
		oldName := filepath.Base(oldPath)
		events := watch.recorder.seen()
		// A ready spare was made durable under its .tmp name before it was
		// named ready (that rename is checked here too); what it must show
		// since then is a flush after any write, which is a re-head.
		since := 0
		if oldName == walSpareReadyName {
			for i, event := range events {
				if event == "rename "+walSpareTmpName+">"+walSpareReadyName {
					since = i
				}
			}
		}
		// lastFsync counts a data-only flush only for a re-head, an overwrite
		// of written blocks; a birth needs the whole-file fsync (B6), which
		// makes the size and extents durable too.
		lastWrite, lastFsync, lastFullFsync := -1, -1, -1
		for i := since; i < len(events); i++ {
			switch events[i] {
			case "writeAt " + oldName:
				lastWrite = i
			case "fsync " + oldName:
				lastFsync, lastFullFsync = i, i
			case "fdatasync " + oldName:
				if oldName == walSpareReadyName {
					lastFsync = i
				}
			}
		}
		if oldName == walSpareReadyName && lastWrite < 0 {
			lastFsync = max(lastFsync, since)
		}
		if newName == walSpareReadyName {
			if lastFullFsync < 0 || lastFullFsync < lastWrite {
				watch.mu.Lock()
				watch.problems = append(watch.problems, fmt.Sprintf("%s was renamed to %s before it was fsynced (events %v)", oldName, newName, events))
				watch.mu.Unlock()
			}
			return
		}

		watch.mu.Lock()
		defer watch.mu.Unlock()
		watch.named[newName] = true
		header, err := readHeaderPrefix(oldPath)
		if err != nil {
			watch.problems = append(watch.problems, fmt.Sprintf("renaming %s to %s: %v", oldName, newName, err))
			return
		}
		if _, err := parseWALv2Header(header); err != nil {
			watch.problems = append(watch.problems, fmt.Sprintf("%s was renamed to %s without a v2 header: %v", oldName, newName, err))
		}
		if lastFsync < 0 || lastFsync < lastWrite {
			watch.problems = append(watch.problems, fmt.Sprintf("%s was renamed to %s before its header was fsynced (events %v)", oldName, newName, events))
		}
	}
	watch.recorder.recordFromOpen(t)
	return watch
}

// activateAfterAFailedOpen opens a new log in dir whose first segment cannot
// be born (its birth's fsync fails once, or with preallocation its allocation),
// so the open starts with no active segment; 10 s later the next append
// activates one (spec §4.3), and a rotation follows.
func activateAfterAFailedOpen(t *testing.T, dir string, preallocate bool) {
	t.Helper()
	clock := newWALTestClock()
	var failing atomic.Bool
	failing.Store(true)
	injected := errors.New("simulated failure of the open's segment birth")
	withOpenSyscalls(t, func(sys *walSyscalls) {
		sys.now = clock.read
		realFsync, realFallocate := sys.fsync, sys.fallocate
		sys.fsync = func(f *os.File) error {
			if !preallocate && filepath.Base(f.Name()) == walSpareTmpName && failing.CompareAndSwap(true, false) {
				return injected
			}
			return realFsync(f)
		}
		sys.fallocate = func(f *os.File, size int64) error {
			if preallocate && failing.Load() {
				return injected
			}
			return realFallocate(f, size)
		}
	})
	w, err := OpenWALWithOptions(dir, nil, WALOptions{Preallocate: preallocate, SegmentBytes: 4 << 10})
	if err != nil {
		t.Fatalf("an open whose segment birth failed did not start: %v", err)
	}
	if w.activeFile != nil {
		t.Fatalf("the scenario needs an open with no active segment")
	}
	failing.Store(false)
	clock.advance(10 * time.Second)
	appendBirthPayloads(t, w, 40)
	closeBirthWAL(t, w)
}

// readHeaderPrefix reads up to a v2 header's worth of bytes from path.
func readHeaderPrefix(path string) ([]byte, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", filepath.Base(path), err)
	}
	if len(contents) > walFileHeaderLen {
		contents = contents[:walFileHeaderLen]
	}
	return contents, nil
}

// check fails t unless every wal_N.log in dir was named by a recorded rename,
// with every rename's checks passing, and still has a v2 header.
func (watch *segmentBirthWatch) check(t *testing.T, dir string) {
	t.Helper()
	watch.mu.Lock()
	defer watch.mu.Unlock()
	for _, problem := range watch.problems {
		t.Error(problem)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("listing %s: %v", dir, err)
	}
	segments := 0
	for _, entry := range entries {
		if !isWALSegmentFile(entry.Name()) {
			continue
		}
		segments++
		if !watch.named[entry.Name()] {
			t.Errorf("%s was created under its segment name instead of being born under wal_spare.tmp and renamed (events %v)",
				entry.Name(), watch.recorder.seen())
		}
		header, err := readHeaderPrefix(filepath.Join(dir, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := parseWALv2Header(header); err != nil {
			t.Errorf("%s has no v2 header: %v", entry.Name(), err)
		}
	}
	if segments == 0 {
		t.Fatalf("the scenario left no segment in %s", dir)
	}
}

// appendBirthPayloads appends count entries of 512 bytes and fails t on any error.
func appendBirthPayloads(t *testing.T, w *WAL, count int) {
	t.Helper()
	payload := bytes.Repeat([]byte("b"), 512)
	for i := 0; i < count; i++ {
		if _, err := w.Append("bucket", OpInsert, []byte(fmt.Sprintf("key%03d", i)), payload); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
}

// openBirthWAL opens dir with 4 KiB segments, so a few appends rotate.
func openBirthWAL(t *testing.T, dir string, keys WALKeySource) *WAL {
	t.Helper()
	w, err := OpenWALWithOptions(dir, keys, WALOptions{SegmentBytes: 4 << 10})
	if err != nil {
		t.Fatalf("OpenWALWithOptions: %v", err)
	}
	return w
}

func closeBirthWAL(t *testing.T, w *WAL) {
	t.Helper()
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func birthTestKeys() *mockWALKeySource {
	return &mockWALKeySource{currentEpoch: 3, epochKey: bytes.Repeat([]byte{7}, 32)}
}

// TestWALEveryNamedSegmentHasAV2Header (off and on; open and rotation; the
// activation path joins in S8): every wal_N.log name the log creates is a
// rename of a file that already holds a parseable v2 header, fsynced before the
// rename; with preallocation that file is a ready spare, fsynced before it was
// named ready and fdatasynced after a re-head. Break by: today's
// create-then-write-header order (os.OpenFile on the segment name, then
// startSegment), in the open or in WAL.rotate; a spare named ready before its
// fsync; a re-head without its flush.
func TestWALEveryNamedSegmentHasAV2Header(t *testing.T) {
	cases := []struct {
		name     string
		scenario func(t *testing.T, dir string)
	}{
		{"open a new plain log", func(t *testing.T, dir string) {
			w := openBirthWAL(t, dir, nil)
			appendBirthPayloads(t, w, 1)
			closeBirthWAL(t, w)
		}},
		{"open a new sealed log", func(t *testing.T, dir string) {
			w := openBirthWAL(t, dir, birthTestKeys())
			appendBirthPayloads(t, w, 1)
			closeBirthWAL(t, w)
		}},
		{"rotate a plain log", func(t *testing.T, dir string) {
			w := openBirthWAL(t, dir, nil)
			appendBirthPayloads(t, w, 40)
			closeBirthWAL(t, w)
		}},
		{"rotate a sealed log", func(t *testing.T, dir string) {
			w := openBirthWAL(t, dir, birthTestKeys())
			appendBirthPayloads(t, w, 40)
			closeBirthWAL(t, w)
		}},
		{"open a plain log with a key", func(t *testing.T, dir string) {
			w := openBirthWAL(t, dir, nil)
			appendBirthPayloads(t, w, 2)
			closeBirthWAL(t, w)
			w = openBirthWAL(t, dir, birthTestKeys())
			appendBirthPayloads(t, w, 2)
			closeBirthWAL(t, w)
		}},
		{"open and rotate a preallocated plain log", func(t *testing.T, dir string) {
			w := openPreallocatedWAL(t, dir, nil, 4<<10)
			appendBirthPayloads(t, w, 40)
			closeBirthWAL(t, w)
		}},
		{"open and rotate a preallocated sealed log", func(t *testing.T, dir string) {
			w := openPreallocatedWAL(t, dir, birthTestKeys(), 4<<10)
			appendBirthPayloads(t, w, 40)
			closeBirthWAL(t, w)
		}},
		{"a preallocated open re-heads a stale ready spare", func(t *testing.T, dir string) {
			if err := os.MkdirAll(dir, 0o700); err != nil {
				t.Fatal(err)
			}
			writeSpare(t, dir, walSpareReadyName, newWALv2Format(true, 9).header, 4<<10)
			w := openPreallocatedWAL(t, dir, nil, 4<<10)
			appendBirthPayloads(t, w, 2)
			closeBirthWAL(t, w)
		}},
		{"activate a log the open could not give a segment", func(t *testing.T, dir string) {
			activateAfterAFailedOpen(t, dir, false)
		}},
		{"activate a preallocated log the open could not give a segment", func(t *testing.T, dir string) {
			activateAfterAFailedOpen(t, dir, true)
		}},
		{"a leftover wal_spare.tmp from a crashed birth", func(t *testing.T, dir string) {
			if err := os.MkdirAll(dir, 0o700); err != nil {
				t.Fatal(err)
			}
			// Longer than a header and not zero: a birth that reused the
			// leftover instead of starting a fresh file would leave its bytes
			// behind the header, where they would read as a torn entry.
			if err := os.WriteFile(filepath.Join(dir, "wal_spare.tmp"), bytes.Repeat([]byte{0xee}, 4096), 0o600); err != nil {
				t.Fatal(err)
			}
			w := openBirthWAL(t, dir, nil)
			if info, err := os.Stat(segmentFilePath(dir, w.activeIndex)); err != nil || info.Size() != walFileHeaderLen {
				t.Fatalf("the segment born over a leftover wal_spare.tmp: %v, %v; want exactly its %d-byte header", info, err, walFileHeaderLen)
			}
			appendBirthPayloads(t, w, 40)
			closeBirthWAL(t, w)
			if _, err := os.Stat(filepath.Join(dir, "wal_spare.tmp")); !os.IsNotExist(err) {
				t.Errorf("wal_spare.tmp is still in the log directory after the births (stat: %v)", err)
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "wal")
			watch := watchSegmentBirths(t)
			tc.scenario(t, dir)
			watch.check(t, dir)
		})
	}
}

// TestWALNewSegmentNameIsFollowedByADirectoryFsync: every new segment's name is
// made durable before anything is written to the segment under it, so an entry
// acknowledged there cannot lose its file to a power cut (fixes BUG-0719). The
// recorded order, for every segment that is written: its rename, then a
// directory fsync, then its first write. Fails on the parent, which never
// fsyncs the directory. Break by: dropping the directory fsync from the birth,
// or making it before the rename.
func TestWALNewSegmentNameIsFollowedByADirectoryFsync(t *testing.T) {
	if reflect.ValueOf(realWALSyscalls().syncDir).Pointer() != reflect.ValueOf(syncDirectoryPath).Pointer() {
		t.Fatalf("the production table's syncDir is not syncDirectoryPath: every test below replaces it, so only this check sees a no-op")
	}

	dir := filepath.Join(t.TempDir(), "wal")
	var recorder walSyscallRecorder
	recorder.recordFromOpen(t)
	w := openBirthWAL(t, dir, nil)
	appendBirthPayloads(t, w, 40)
	closeBirthWAL(t, w)
	w = openBirthWAL(t, dir, birthTestKeys())
	appendBirthPayloads(t, w, 40)
	closeBirthWAL(t, w)

	events := recorder.seen()
	firstWrite := map[string]int{}
	named := map[string]int{}
	for i, event := range events {
		call, file, _ := strings.Cut(event, " ")
		switch call {
		case "writeAt":
			if _, seen := firstWrite[file]; !seen && isWALSegmentFile(file) {
				firstWrite[file] = i
			}
		case "rename":
			if _, to, ok := strings.Cut(file, ">"); ok && isWALSegmentFile(to) {
				named[to] = i
			}
		}
	}
	if len(firstWrite) < 3 {
		t.Fatalf("the scenario wrote to %d segments, want at least 3 (events %v)", len(firstWrite), events)
	}
	for segment, write := range firstWrite {
		rename, wasRenamed := named[segment]
		if !wasRenamed {
			rename = -1
		}
		synced := false
		for i := rename + 1; i < write; i++ {
			if strings.HasPrefix(events[i], "syncDir ") {
				synced = true
				break
			}
		}
		if !synced {
			t.Errorf("no directory fsync between %s getting its name and the first write to it (events %v)", segment, events)
		}
	}
	for segment, rename := range named {
		if _, written := firstWrite[segment]; written {
			continue
		}
		synced := false
		for _, event := range events[rename+1:] {
			if strings.HasPrefix(event, "syncDir ") {
				synced = true
				break
			}
		}
		if !synced {
			t.Errorf("%s was named and no directory fsync followed (events %v)", segment, events)
		}
	}
}

// TestWALSegmentNamesNeverMatchTheSpare: the two names a segment is born under
// are never segment names, so no reader, WAL.Truncate or listing ever takes a
// spare for a segment, and a pre-change build never sees one. Break by:
// loosening walSegmentPattern (for example to every "wal_" file).
func TestWALSegmentNamesNeverMatchTheSpare(t *testing.T) {
	for _, name := range []string{"wal_spare.tmp", "wal_spare.ready"} {
		if isWALSegmentFile(name) {
			t.Errorf("isWALSegmentFile(%q) = true, want false: a spare is not a segment", name)
		}
	}

	dir := t.TempDir()
	w, err := OpenWAL(dir, nil)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	defer closeBirthWAL(t, w)
	appendBirthPayloads(t, w, 3)

	// Spares that look like the best segment in the directory: a v2 header and
	// a higher-looking position in a listing.
	spareBytes := append(newWALv2Format(false, 0).header, make([]byte, 4096)...)
	for _, name := range []string{"wal_spare.tmp", "wal_spare.ready"} {
		if err := os.WriteFile(filepath.Join(dir, name), spareBytes, 0o600); err != nil {
			t.Fatalf("writing %s: %v", name, err)
		}
	}

	for _, segment := range w.listSegments() {
		if strings.HasPrefix(segment.name, "wal_spare") {
			t.Errorf("listSegments returned the spare %s", segment.name)
		}
	}
	entries, err := w.ReadFrom(1)
	if err != nil {
		t.Fatalf("ReadFrom(1) beside the spares: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("ReadFrom(1) beside the spares returned %d entries, want 3", len(entries))
	}
	first, err := w.FirstSequence()
	if err != nil || first != 1 {
		t.Fatalf("FirstSequence beside the spares = %d, %v; want 1", first, err)
	}
	if err := w.Truncate(w.LastSequence()); err != nil {
		t.Fatalf("Truncate beside the spares: %v", err)
	}
	for _, name := range []string{"wal_spare.tmp", "wal_spare.ready"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Errorf("Truncate touched the spare %s: %v", name, err)
		}
	}
}

// TestWALFailedBirthDeletesItsTemporaryFile: a birth that fails before the
// rename deletes its wal_spare.tmp at once, so the failure holds no disk, and
// it refuses the append without naming a segment or consuming a sequence.
// Break by: returning the error without the remove.
func TestWALFailedBirthDeletesItsTemporaryFile(t *testing.T) {
	dir := t.TempDir()
	w := openBirthWAL(t, dir, nil)
	defer closeBirthWAL(t, w)
	appendBirthPayloads(t, w, 2)
	before := w.LastSequence()

	realFsync := w.sys.fsync
	w.sys.fsync = func(file *os.File) error {
		if filepath.Base(file.Name()) == "wal_spare.tmp" {
			return fmt.Errorf("injected fsync failure")
		}
		return realFsync(file)
	}
	payload := bytes.Repeat([]byte("b"), 512)
	var appendErr error
	for i := 0; i < 20 && appendErr == nil; i++ {
		_, appendErr = w.Append("bucket", OpInsert, []byte(fmt.Sprintf("late%02d", i)), payload)
	}
	if appendErr == nil {
		t.Fatalf("20 appends into 4 KiB segments never needed a new segment")
	}
	if _, err := os.Stat(filepath.Join(dir, "wal_spare.tmp")); !os.IsNotExist(err) {
		t.Fatalf("wal_spare.tmp is still in the log directory after its birth failed (stat: %v)", err)
	}
	segments := w.listSegments()
	if got := segments[len(segments)-1].index; got != 1 {
		t.Fatalf("the failed birth named a segment: the highest is wal_%05d.log, want wal_00001.log", got)
	}
	entries, err := w.ReadFrom(1)
	if err != nil {
		t.Fatalf("ReadFrom(1) after the failed birth: %v", err)
	}
	if last := entries[len(entries)-1].Seq; last != w.LastSequence() || w.LastSequence() < before {
		t.Fatalf("after the failed birth the last entry is %d and LastSequence is %d (was %d)", last, w.LastSequence(), before)
	}
}
