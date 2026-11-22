package storage

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
)

// injectFlush runs before before each group-commit flush of w and then performs
// the real one. It replaces the groupSyncFile assignment the BUG-0712 tests
// used to make: the same seam, now the table's two flush entries, the
// whole-file one of a growing segment and the data-only one of a preallocated
// segment.
func injectFlush(w *WAL, before func()) {
	realFlush, realFlushData := w.sys.flush, w.sys.flushData
	w.sys.flush = func(file *os.File) error {
		before()
		return realFlush(file)
	}
	w.sys.flushData = func(file *os.File) error {
		before()
		return realFlushData(file)
	}
}

// failFlushes replaces both of w's group-commit flush entries with flush.
func failFlushes(w *WAL, flush func(*os.File) error) {
	w.sys.flush = flush
	w.sys.flushData = flush
}

// walFlushModes are the two modes the group-commit tests run in: a growing log
// as OpenWAL opens it, and a preallocated one. A preallocated log is opened
// with segmentBytes itself, so its spares are that size and not 64 MiB.
var walFlushModes = []struct {
	name string
	open func(t *testing.T, dir string, segmentBytes int64) *WAL
}{
	{"off", func(t *testing.T, dir string, segmentBytes int64) *WAL {
		t.Helper()
		w, err := OpenWAL(dir, nil)
		if err != nil {
			t.Fatalf("OpenWAL: %v", err)
		}
		w.SetMaxSegmentBytes(segmentBytes)
		return w
	}},
	{"on", func(t *testing.T, dir string, segmentBytes int64) *WAL {
		t.Helper()
		return openPreallocatedWAL(t, dir, nil, segmentBytes)
	}},
}

// walSyscallEvent is one call the recorder saw: the table entry and the base
// name of the file it was made on (both names for a rename).
type walSyscallEvent struct {
	call string
	file string
}

// walSyscallRecorder wraps the real table and records every call in order. The
// order tests of the later steps (rotation, birth, open) reuse it.
type walSyscallRecorder struct {
	mu     sync.Mutex
	events []walSyscallEvent
	// beforeRename, when set, runs before each real rename (see wrap).
	beforeRename func(oldPath, newPath string)
}

// attach replaces every entry of w's table with a recording one that then makes
// the real call. Call it before the first append.
func (r *walSyscallRecorder) attach(w *WAL) {
	w.sys = r.wrap(w.sys)
}

// recordFromOpen makes every open in this test start with a recording table, so
// the calls the open itself makes (a new segment's birth) are recorded too.
func (r *walSyscallRecorder) recordFromOpen(t *testing.T) {
	t.Helper()
	previous := walSyscallsAtOpen
	walSyscallsAtOpen = func() walSyscalls { return r.wrap(previous()) }
	t.Cleanup(func() { walSyscallsAtOpen = previous })
}

// record appends one event.
func (r *walSyscallRecorder) record(call, file string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, walSyscallEvent{call: call, file: file})
}

// wrap returns real with every entry recording its call first. beforeRename,
// when set, runs before the real rename, with the files still at their old
// names: the birth tests read the file being named there.
func (r *walSyscallRecorder) wrap(real walSyscalls) walSyscalls {
	wrapped := real
	wrapped.flush = func(file *os.File) error {
		r.record("flush", filepath.Base(file.Name()))
		return real.flush(file)
	}
	wrapped.flushData = func(file *os.File) error {
		r.record("flushData", filepath.Base(file.Name()))
		return real.flushData(file)
	}
	wrapped.fsync = func(file *os.File) error {
		r.record("fsync", filepath.Base(file.Name()))
		return real.fsync(file)
	}
	wrapped.fallocate = func(file *os.File, size int64) error {
		r.record("fallocate", filepath.Base(file.Name()))
		return real.fallocate(file, size)
	}
	wrapped.fdatasync = func(file *os.File) error {
		r.record("fdatasync", filepath.Base(file.Name()))
		return real.fdatasync(file)
	}
	wrapped.ftruncate = func(file *os.File, size int64) error {
		r.record("ftruncate", filepath.Base(file.Name()))
		return real.ftruncate(file, size)
	}
	wrapped.rename = func(oldPath, newPath string) error {
		if r.beforeRename != nil {
			r.beforeRename(oldPath, newPath)
		}
		r.record("rename", filepath.Base(oldPath)+">"+filepath.Base(newPath))
		return real.rename(oldPath, newPath)
	}
	wrapped.writeAt = func(file *os.File, entry []byte, offset int64) (int, error) {
		r.record("writeAt", filepath.Base(file.Name()))
		return real.writeAt(file, entry, offset)
	}
	wrapped.syncDir = func(dirPath string) error {
		r.record("syncDir", filepath.Base(dirPath))
		return real.syncDir(dirPath)
	}
	wrapped.remove = func(path string) error {
		r.record("remove", filepath.Base(path))
		return real.remove(path)
	}
	wrapped.close = func(file *os.File) error {
		r.record("close", filepath.Base(file.Name()))
		return real.close(file)
	}
	return wrapped
}

// seen returns the recorded events as "call file" strings.
func (r *walSyscallRecorder) seen() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.events))
	for i, event := range r.events {
		out[i] = event.call + " " + event.file
	}
	return out
}

// TestWALSyscallTableRecordsOrder: a recording table sees the group-commit flush
// of an append and then Close's fsync of the same segment, in that order. It is
// the recorder every later order test reuses, so it also pins that the flush and
// the fsync are separate table entries (the flush becomes fdatasync in S7).
func TestWALSyscallTableRecordsOrder(t *testing.T) {
	w, err := OpenWAL(t.TempDir(), nil)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	var recorder walSyscallRecorder
	recorder.attach(w)

	if _, err := w.Append("bucket", OpInsert, []byte("key"), []byte("payload")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	got := recorder.seen()
	// The open's own calls (the tail fsync, the birth) are recorded too
	// when a test starts recording from the open; only the order of the
	// flush before the close fsync is the point here.
	var calls []string
	for _, event := range got {
		if event == "flush wal_00001.log" || event == "fsync wal_00001.log" {
			calls = append(calls, event)
		}
	}
	want := []string{"flush wal_00001.log", "fsync wal_00001.log"}
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("recorded %v, want the group-commit flush then Close's fsync %v (all events: %v)", calls, want, got)
	}
}

// TestOpenWALIsOpenWALWithOptionsOff: OpenWAL is OpenWALWithOptions with
// WALOptions{} (no preallocation, the default segment size): the same appends
// give byte-identical segments, and SegmentBytes 0 means 64 MiB. The
// generation is fixed, since each segment otherwise draws its own.
func TestOpenWALIsOpenWALWithOptionsOff(t *testing.T) {
	fixWALGeneration(t, frozenWALGeneration)
	appendAll := func(w *WAL) {
		t.Helper()
		for i := 0; i < 20; i++ {
			if _, err := w.Append("bucket", OpInsert, []byte(fmt.Sprintf("key%02d", i)), []byte("payload")); err != nil {
				t.Fatalf("Append %d: %v", i, err)
			}
		}
		if err := w.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}

	plainDir, optionsDir := t.TempDir(), t.TempDir()
	plain, err := OpenWAL(plainDir, nil)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	appendAll(plain)
	withOptions, err := OpenWALWithOptions(optionsDir, nil, WALOptions{})
	if err != nil {
		t.Fatalf("OpenWALWithOptions: %v", err)
	}
	if withOptions.maxFileSize != defaultMaxFileSize {
		t.Fatalf("SegmentBytes 0 gave a %d-byte segment limit, want %d", withOptions.maxFileSize, defaultMaxFileSize)
	}
	appendAll(withOptions)

	plainBytes, err := os.ReadFile(segmentFilePath(plainDir, 1))
	if err != nil {
		t.Fatalf("reading the OpenWAL segment: %v", err)
	}
	optionsBytes, err := os.ReadFile(segmentFilePath(optionsDir, 1))
	if err != nil {
		t.Fatalf("reading the OpenWALWithOptions segment: %v", err)
	}
	if !bytes.Equal(plainBytes, optionsBytes) {
		t.Fatalf("OpenWAL wrote %d bytes and OpenWALWithOptions(WALOptions{}) wrote %d different bytes for the same appends",
			len(plainBytes), len(optionsBytes))
	}
}

// TestOpenWALWithOptionsSegmentBytes: a positive SegmentBytes is the segment
// limit from the first append, so a small segment rotates without a later
// SetMaxSegmentBytes, and a preallocated log's segments are born at that size.
func TestOpenWALWithOptionsSegmentBytes(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWALWithOptions(dir, nil, WALOptions{Preallocate: true, SegmentBytes: 4 << 10})
	if err != nil {
		t.Fatalf("OpenWALWithOptions: %v", err)
	}
	defer func() {
		if err := w.Close(); err != nil {
			t.Errorf("closing the WAL: %v", err)
		}
	}()
	payload := bytes.Repeat([]byte("x"), 512)
	for i := 0; i < 40; i++ {
		if _, err := w.Append("bucket", OpInsert, []byte(fmt.Sprintf("key%02d", i)), payload); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	segments, err := filepath.Glob(filepath.Join(dir, "wal_*.log"))
	if err != nil {
		t.Fatalf("listing segments: %v", err)
	}
	if len(segments) < 2 {
		t.Fatalf("40 appends of 512 bytes into 4 KiB segments left %d segment files, want a rotation", len(segments))
	}
}
