package storage

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// rotationKeyModes are the two key modes every rotation test runs in.
var rotationKeyModes = []struct {
	name string
	keys func() WALKeySource
}{
	{"plain", func() WALKeySource { return nil }},
	{"sealed", func() WALKeySource { return birthTestKeys() }},
}

// rotationPayload is the payload every rotation test appends, so the number of
// appends a 4 KiB segment holds is the same in every log of one key mode.
var rotationPayload = bytes.Repeat([]byte("r"), 512)

// rotationOrigin is the origin the tests' unsynced appends carry.
var rotationOrigin = WALOrigin{Kind: WALOriginReplica, Source: "n1", Seq: 1}

// appendRotationEntry appends entry i of a rotation test.
func appendRotationEntry(w *WAL, i int) (uint64, error) {
	return w.Append("bucket", OpInsert, []byte(fmt.Sprintf("key%03d", i)), rotationPayload)
}

// appendsBeforeRotation is how many entries a new log in this key mode takes
// before the next append rotates: it is measured on a throwaway log, so a test
// can arm a fault for exactly the rotating append.
func appendsBeforeRotation(t *testing.T, keys WALKeySource) int {
	t.Helper()
	w := openBirthWAL(t, t.TempDir(), keys)
	defer closeBirthWAL(t, w)
	first := w.activeIndex
	for i := 0; i < 64; i++ {
		if _, err := appendRotationEntry(w, i); err != nil {
			t.Fatalf("measuring the rotation point: Append %d: %v", i, err)
		}
		if w.activeIndex != first {
			return i
		}
	}
	t.Fatalf("64 appends into 4 KiB segments never rotated")
	return 0
}

// walTestClock is a clock a test moves by hand.
type walTestClock struct {
	mu  sync.Mutex
	now time.Time
}

func newWALTestClock() *walTestClock {
	return &walTestClock{now: time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC)}
}

func (c *walTestClock) read() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *walTestClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// abandonWAL is a process dying with w open: its descriptors go and nothing
// is flushed, closed in order or written. Its filler goroutine is stopped, as
// a dead process's would be (a fill it cancels deletes its wal_spare.tmp, which
// the next open would delete anyway).
func abandonWAL(w *WAL) {
	w.stopFiller()
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.activeFile != nil {
		_ = w.activeFile.Close()
		w.activeFile = nil
	}
}

// rotationFault fails one call of a rotation through the table, once armed.
type rotationFault struct {
	name string
	// install replaces the entry of w's table this fault fails. outgoing and
	// incoming are the base names of the segment the rotation retires and the
	// one it starts; fail reports whether this call is the one to fail.
	install func(w *WAL, outgoing, incoming string, fail func() bool, injected error)
	// poisons is whether the failure comes after R0, where the log is
	// poisoned; R0's failure is a refusal.
	poisons bool
}

// preallocatedRotationFaults are the rotation steps only a preallocated log
// makes, or makes through another entry: R1's data-only flush and R2's cut of
// the outgoing segment and its fsync (spec §2.2, E11).
var preallocatedRotationFaults = []rotationFault{
	{"R1 the data-only flush of the outgoing segment", func(w *WAL, outgoing, _ string, fail func() bool, injected error) {
		real := w.sys.flushData
		w.sys.flushData = func(file *os.File) error {
			if filepath.Base(file.Name()) == outgoing && fail() {
				return injected
			}
			return real(file)
		}
	}, true},
	{"R2 the cut of the outgoing segment", func(w *WAL, outgoing, _ string, fail func() bool, injected error) {
		real := w.sys.ftruncate
		w.sys.ftruncate = func(file *os.File, size int64) error {
			if filepath.Base(file.Name()) == outgoing && fail() {
				return injected
			}
			return real(file, size)
		}
	}, true},
	{"R2 the fsync of the cut segment", func(w *WAL, outgoing, _ string, fail func() bool, injected error) {
		real := w.sys.fsync
		w.sys.fsync = func(file *os.File) error {
			if filepath.Base(file.Name()) == outgoing && fail() {
				return injected
			}
			return real(file)
		}
	}, true},
}

var rotationFaults = []rotationFault{
	{"R0 the birth's fsync of wal_spare.tmp", func(w *WAL, _, _ string, fail func() bool, injected error) {
		real := w.sys.fsync
		w.sys.fsync = func(file *os.File) error {
			if filepath.Base(file.Name()) == walSpareTmpName && fail() {
				return injected
			}
			return real(file)
		}
	}, false},
	{"R1 the flush of the outgoing segment", func(w *WAL, outgoing, _ string, fail func() bool, injected error) {
		// A rotation that fsyncs the outgoing segment through either entry
		// is failed, so the parent's own fsync is failed the same way.
		realFlush, realFsync := w.sys.flush, w.sys.fsync
		w.sys.flush = func(file *os.File) error {
			if filepath.Base(file.Name()) == outgoing && fail() {
				return injected
			}
			return realFlush(file)
		}
		w.sys.fsync = func(file *os.File) error {
			if filepath.Base(file.Name()) == outgoing && fail() {
				return injected
			}
			return realFsync(file)
		}
	}, true},
	{"R3 the close of the outgoing segment", func(w *WAL, outgoing, _ string, fail func() bool, injected error) {
		real := w.sys.close
		w.sys.close = func(file *os.File) error {
			err := real(file)
			if filepath.Base(file.Name()) == outgoing && fail() {
				return errors.Join(injected, err)
			}
			return err
		}
	}, true},
	{"R3 the rename to the next segment name", func(w *WAL, _, incoming string, fail func() bool, injected error) {
		real := w.sys.rename
		w.sys.rename = func(oldPath, newPath string) error {
			if filepath.Base(newPath) == incoming && fail() {
				return injected
			}
			return real(oldPath, newPath)
		}
	}, true},
	{"R5 the directory fsync after the rename", func(w *WAL, _, _ string, fail func() bool, injected error) {
		real := w.sys.syncDir
		w.sys.syncDir = func(dirPath string) error {
			if fail() {
				return injected
			}
			return real(dirPath)
		}
	}, true},
}

// faultedRotation is a log whose rotating append failed at one step.
type faultedRotation struct {
	w        *WAL
	dir      string
	appended int
	injected error
	clock    *walTestClock
	// rotateErr is what the rotating append returned.
	rotateErr error
	// outgoingBefore is the outgoing segment's bytes just before the
	// rotating append.
	outgoingBefore []byte
}

// faultRotation opens a log in keys' mode with 4 KiB segments and a test clock,
// fills its first segment to the rotation point, arms fault and makes the
// rotating append, which fails once through the table.
func faultRotation(t *testing.T, keys WALKeySource, fault rotationFault) *faultedRotation {
	t.Helper()
	return faultRotationWith(t, keys, false, fault)
}

// faultRotationWith is faultRotation on a preallocated log when preallocate is
// set. A rotation happens at the same append in both modes: it is decided by
// the write offset, not the file's size.
func faultRotationWith(t *testing.T, keys WALKeySource, preallocate bool, fault rotationFault) *faultedRotation {
	t.Helper()
	appended := appendsBeforeRotation(t, keys)
	dir := t.TempDir()
	var w *WAL
	if preallocate {
		w = openPreallocatedWAL(t, dir, keys, 4<<10)
	} else {
		w = openBirthWAL(t, dir, keys)
	}
	clock := newWALTestClock()
	w.sys.now = clock.read
	for i := 0; i < appended; i++ {
		if _, err := appendRotationEntry(w, i); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	if preallocate {
		// The rotation takes a ready spare. Were it to find none, it would
		// fill one itself, and a fault armed for R3 or R5 would land in that
		// fill's own rename or directory fsync instead: a refusal, not the
		// rotation step the fault names.
		waitForReadySpare(t, w)
	}
	outgoing := segmentFileName(w.activeIndex)
	before, err := os.ReadFile(filepath.Join(dir, outgoing))
	if err != nil {
		t.Fatal(err)
	}
	injected := fmt.Errorf("simulated EIO at %s", fault.name)
	var armed atomic.Bool
	armed.Store(true)
	fault.install(w, outgoing, segmentFileName(w.activeIndex+1), func() bool { return armed.CompareAndSwap(true, false) }, injected)

	_, rotateErr := appendRotationEntry(w, appended)
	if armed.Load() {
		t.Fatalf("the rotating append never reached the armed call (%s); it returned %v", fault.name, rotateErr)
	}
	if !errors.Is(rotateErr, injected) {
		t.Fatalf("the rotating append returned %v, want the injected failure at %s", rotateErr, fault.name)
	}
	return &faultedRotation{w: w, dir: dir, appended: appended, injected: injected, clock: clock, rotateErr: rotateErr, outgoingBefore: before}
}

// TestWALRotationSyscallOrder: a rotation prepares the next segment before it
// touches the current one, then flushes the outgoing segment, closes it, names
// the next one, makes the name durable, and only then writes to it (spec §2.2
// R0 to R6). With preallocation the next segment is a spare born before the
// rotation (each zero chunk fdatasynced, the file fsynced, renamed to
// wal_spare.ready, the directory fsynced), the flush is the data-only one, and
// R2 cuts the outgoing segment to its entries and fsyncs it before it is
// closed. Break by moving any one call across its neighbour: the birth after
// the flush (the parent's order), the close before the flush, the rename before
// the close, the directory fsync before the rename or not at all; the spare
// named before its fsync, R2's cut before R1 or without its fsync.
func TestWALRotationSyscallOrder(t *testing.T) {
	for _, mode := range rotationKeyModes {
		for _, preallocate := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/preallocate=%v", mode.name, preallocate), func(t *testing.T) {
				rotationSyscallOrder(t, mode.keys(), preallocate)
			})
		}
	}
}

// rotationSyscallOrder is one run of TestWALRotationSyscallOrder.
func rotationSyscallOrder(t *testing.T, keys WALKeySource, preallocate bool) {
	var recorder walSyscallRecorder
	// From the open: a preallocated log's filler keeps the table the
	// open had, and the spare's birth is the filler's.
	recorder.recordFromOpen(t)
	w, err := OpenWALWithOptions(t.TempDir(), keys, WALOptions{Preallocate: preallocate, SegmentBytes: 4 << 10})
	if err != nil {
		t.Fatal(err)
	}
	first := w.activeIndex
	for i := 0; w.activeIndex == first; i++ {
		if i == 64 {
			t.Fatalf("64 appends into 4 KiB segments never rotated")
		}
		if _, err := appendRotationEntry(w, i); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	closeBirthWAL(t, w)

	events := recorder.seen()
	outgoing, incoming := segmentFileName(first), segmentFileName(first+1)
	find := func(from int, event string) int {
		for i := from; i < len(events); i++ {
			if events[i] == event {
				return i
			}
		}
		return -1
	}
	lastBefore := func(end int, event string) int {
		for i := end - 1; i >= 0; i-- {
			if events[i] == event {
				return i
			}
		}
		return -1
	}

	spare := walSpareTmpName
	if preallocate {
		spare = walSpareReadyName
	}
	rename := find(0, "rename "+spare+">"+incoming)
	if rename < 0 {
		t.Fatalf("no rename of %s to %s (events %v)", spare, incoming, events)
	}
	closed := lastBefore(len(events), "close "+outgoing)
	lastWrite := lastBefore(rename, "writeAt "+outgoing)
	syncDir := find(rename+1, "syncDir "+filepath.Base(w.dir))
	firstWrite := find(0, "writeAt "+incoming)

	var steps []orderStep
	if preallocate {
		// The spare's birth, by the filler while the appends go on: its
		// zeros, its fsync, its name, the name made durable, all before the
		// rotation names it N+1.
		named := lastBefore(rename, "rename "+walSpareTmpName+">"+walSpareReadyName)
		birth := []orderStep{
			{"the spare's last zero chunk", lastBefore(named, "fdatasync "+walSpareTmpName)},
			{"B6 the spare's fsync", lastBefore(named, "fsync "+walSpareTmpName)},
			{"B7 the rename to " + walSpareReadyName, named},
			{"B7 the directory fsync", find(named+1, "syncDir "+filepath.Base(w.dir))},
			{"R3 the rename to " + incoming, rename},
		}
		requireStepOrder(t, "the spare's birth", birth, events)
		// The rotation: the data-only flush, then R2's cut and fsync of N.
		truncated := lastBefore(rename, "ftruncate "+outgoing)
		steps = []orderStep{
			{"the last write to " + outgoing, lastWrite},
			{"R1 the data-only flush of " + outgoing, lastBefore(truncated, "flushData "+outgoing)},
			{"R2 the cut of " + outgoing, truncated},
			{"R2 the fsync of " + outgoing, lastBefore(closed, "fsync "+outgoing)},
			{"R3 the close of " + outgoing, closed},
			{"R3 the rename to " + incoming, rename},
			{"R5 the directory fsync", syncDir},
			{"the first write to " + incoming, firstWrite},
		}
		if named >= 0 {
			if write := lastBefore(named, "writeAt "+walSpareTmpName); write > birth[0].at {
				t.Fatalf("the spare's last write (event %d) has no fdatasync after it before its rename (events %v)", write, events)
			}
		}
	} else {
		steps = []orderStep{
			{"the last write to " + outgoing, lastWrite},
			{"R0 the birth's fsync of " + walSpareTmpName, lastBefore(rename, "fsync "+walSpareTmpName)},
			{"R1 the flush of " + outgoing, lastBefore(closed, "flush "+outgoing)},
			{"R3 the close of " + outgoing, closed},
			{"R3 the rename to " + incoming, rename},
			{"R5 the directory fsync", syncDir},
			{"the first write to " + incoming, firstWrite},
		}
	}
	requireStepOrder(t, "the rotation", steps, events)
}

// orderStep is one call an order test expects, and where the recorder saw it
// (-1 for never).
type orderStep struct {
	name string
	at   int
}

// requireStepOrder fails t unless every step was seen, each after the one
// before it.
func requireStepOrder(t *testing.T, what string, steps []orderStep, events []string) {
	t.Helper()
	for i, step := range steps {
		if step.at < 0 {
			t.Fatalf("%s never made %s (events %v)", what, step.name, events)
		}
		if i > 0 && steps[i-1].at >= step.at {
			t.Fatalf("in %s, %s (event %d) does not come after %s (event %d) (events %v)",
				what, step.name, step.at, steps[i-1].name, steps[i-1].at, events)
		}
	}
}

// TestWALRotationFlushFailurePoisons: a rotation that fails after it has
// prepared the next segment (R1, R3, R5; R2 joins with preallocation) poisons
// the log. Every write entry point then fails with that failure, no SyncThrough
// succeeds even for a sequence written before it, nothing clears it with time,
// and readers still read every entry of the outgoing segment. Fails on the
// parent, whose rotation returns the fsync error and leaves the log usable, so
// a later fsync can report success for pages the kernel already dropped (fixes
// BUG-0717). Break by: returning a step's error without setting syncErr, or
// dropping the poison check of writeLocked or FastForward.
func TestWALRotationFlushFailurePoisons(t *testing.T) {
	type poisoningFault struct {
		fault       rotationFault
		preallocate bool
	}
	var faults []poisoningFault
	for _, fault := range rotationFaults {
		if fault.poisons {
			faults = append(faults, poisoningFault{fault, false}, poisoningFault{fault, true})
		}
	}
	for _, fault := range preallocatedRotationFaults {
		faults = append(faults, poisoningFault{fault, true})
	}
	for _, mode := range rotationKeyModes {
		for _, pf := range faults {
			fault := pf.fault
			name := mode.name + "/off/" + fault.name
			if pf.preallocate {
				name = mode.name + "/on/" + fault.name
			}
			t.Run(name, func(t *testing.T) {
				faulted := faultRotationWith(t, mode.keys(), pf.preallocate, fault)
				w, injected, appended := faulted.w, faulted.injected, faulted.appended
				defer abandonWAL(w)

				requirePoisoned := func(when string) {
					t.Helper()
					if _, err := appendRotationEntry(w, 900); !errors.Is(err, injected) {
						t.Fatalf("%s: Append returned %v, want the poison of %q", when, err, injected)
					}
					if _, err := w.AppendUnsyncedFrom("bucket", OpInsert, []byte("unsynced"), rotationPayload, rotationOrigin); !errors.Is(err, injected) {
						t.Fatalf("%s: AppendUnsyncedFrom returned %v, want the poison", when, err)
					}
					for _, seq := range []uint64{1, uint64(appended), uint64(appended) + 1} {
						if err := w.SyncThrough(seq); !errors.Is(err, injected) {
							t.Fatalf("%s: SyncThrough(%d) returned %v, want the poison: no flush may be reported after a failed one", when, seq, err)
						}
					}
					if _, err := w.FastForward(uint64(appended) + 100); !errors.Is(err, injected) {
						t.Fatalf("%s: FastForward returned %v, want the poison", when, err)
					}
					// An append that passed the check before the rotation
					// poisoned the log reaches writeLocked: it is refused there
					// too, under mu, never written.
					if _, _, err := w.writeLocked("bucket", OpInsert, []byte("raced"), rotationPayload, WALOrigin{}); !errors.Is(err, injected) {
						t.Fatalf("%s: writeLocked returned %v, want the poison", when, err)
					}
				}
				requirePoisoned("right after the failure")
				faulted.clock.advance(time.Hour)
				requirePoisoned("an hour later")

				if got := w.LastSequence(); got != uint64(appended) {
					t.Fatalf("LastSequence is %d after the poisoned rotation, want %d", got, appended)
				}
				readBack, err := w.ReadFrom(1)
				if err != nil {
					t.Fatalf("ReadFrom(1) on the poisoned log: %v", err)
				}
				requireSeqs(t, "ReadFrom(1) on the poisoned log", readBack, appended)
			})
		}
	}
}

// TestWALRefusedRotationConsumesNoSequence: a rotation that cannot prepare the
// next segment (R0) refuses the entry and changes nothing: no sequence is
// drawn, the outgoing segment stays active, open and byte for byte the same,
// and the log is not poisoned. For 10 s every append is refused at once with
// the same error, without a call to the table, and WriteRefusal reports it;
// then appends resume, the next one rotates, and ReadBatch from 1 has no hole.
// The S2 half (LastSequence unchanged) is in here too. Fails on the parent,
// whose rotation closes the outgoing segment before the birth fails and leaves
// the log with no active file (fixes BUG-0716). Break by: closing the outgoing
// segment before R0, poisoning on R0, no rate limit, or a rate limit that never
// expires.
func TestWALRefusedRotationConsumesNoSequence(t *testing.T) {
	for _, mode := range rotationKeyModes {
		t.Run(mode.name, func(t *testing.T) {
			faulted := faultRotation(t, mode.keys(), rotationFaults[0])
			w, injected, appended := faulted.w, faulted.injected, faulted.appended
			defer func() { closeWAL(t, w) }()
			outgoing := w.activeIndex

			if w.activeFile == nil {
				t.Fatalf("the refused rotation left the log with no active segment (BUG-0716): the outgoing segment %s must stay active and open", segmentFileName(outgoing))
			}
			if got := w.LastSequence(); got != uint64(appended) {
				t.Fatalf("the refused rotation moved LastSequence to %d, want %d", got, appended)
			}
			if err := w.poisonedErr(); err != nil {
				t.Fatalf("the refused rotation poisoned the log: %v", err)
			}
			if !strings.Contains(faulted.rotateErr.Error(), w.dir) {
				t.Fatalf("the refusal %q does not name the log directory %s", faulted.rotateErr, w.dir)
			}
			if err := w.WriteRefusal(); !errors.Is(err, injected) {
				t.Fatalf("WriteRefusal returned %v right after the refusal, want it", err)
			}
			after, err := os.ReadFile(segmentFilePath(w.dir, outgoing))
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(after, faulted.outgoingBefore) {
				t.Fatalf("the refused rotation changed %s (%d bytes before, %d after)", segmentFileName(outgoing), len(faulted.outgoingBefore), len(after))
			}
			if _, err := os.Stat(filepath.Join(w.dir, walSpareTmpName)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("the refused birth left %s behind (stat: %v)", walSpareTmpName, err)
			}

			// Within 10 s: refused at once, with the cached error, and the
			// table is not called.
			var calls atomic.Int64
			counting := w.sys
			counting.remove = func(path string) error { calls.Add(1); return os.Remove(path) }
			counting.writeAt = func(f *os.File, b []byte, off int64) (int, error) { calls.Add(1); return f.WriteAt(b, off) }
			counting.fsync = func(f *os.File) error { calls.Add(1); return f.Sync() }
			counting.flush = func(f *os.File) error { calls.Add(1); return f.Sync() }
			w.sys = counting
			faulted.clock.advance(9 * time.Second)
			for _, attempt := range []string{"Append", "AppendUnsyncedFrom"} {
				var err error
				if attempt == "Append" {
					_, err = appendRotationEntry(w, appended)
				} else {
					_, err = w.AppendUnsyncedFrom("bucket", OpInsert, []byte("unsynced"), rotationPayload, rotationOrigin)
				}
				if !errors.Is(err, injected) {
					t.Fatalf("%s 9 s after the refusal returned %v, want the cached refusal", attempt, err)
				}
			}
			if got := calls.Load(); got != 0 {
				t.Fatalf("the appends inside the 10 s window made %d table calls, want 0: the cached refusal is returned at once", got)
			}
			if got := w.LastSequence(); got != uint64(appended) {
				t.Fatalf("the refused appends moved LastSequence to %d, want %d", got, appended)
			}

			// After 10 s: the refusal is gone and the next append rotates.
			faulted.clock.advance(time.Second)
			if err := w.WriteRefusal(); err != nil {
				t.Fatalf("WriteRefusal 10 s after the refusal returned %v, want nil", err)
			}
			seq, err := appendRotationEntry(w, appended)
			if err != nil {
				t.Fatalf("the append 10 s after the refusal: %v", err)
			}
			if seq != uint64(appended+1) {
				t.Fatalf("the append after the refusal got sequence %d, want %d (no sequence consumed by the refusal)", seq, appended+1)
			}
			if w.activeIndex != outgoing+1 {
				t.Fatalf("the append after the refusal left segment %d active, want %d", w.activeIndex, outgoing+1)
			}
			requireSeqs(t, "ReadBatch from 1", readAllThroughCursor(t, w), appended+1)

			// And after a restart (the BUG-0716 record's own check).
			if err := w.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			w = openBirthWAL(t, faulted.dir, mode.keys())
			requireSeqs(t, "ReadBatch from 1 after a restart", readAllThroughCursor(t, w), appended+1)
		})
	}
}

// requireCrashImageOpens opens dir and checks what every crash image of a
// rotation must give: the entries 1..n, no recovery report, the next append
// n+1, and a highest segment in format 2.
func requireCrashImageOpens(t *testing.T, dir string, keys WALKeySource, opts WALOptions, n int) {
	t.Helper()
	opts.SegmentBytes = 4 << 10
	w, err := OpenWALWithOptions(dir, keys, opts)
	if err != nil {
		t.Fatalf("opening the crash image: %v", err)
	}
	defer closeWAL(t, w)
	if report := w.Recovery(); report != nil {
		t.Fatalf("the crash image opened with a recovery report %+v; a rotation never leaves a torn entry", *report)
	}
	readBack, err := w.ReadFrom(1)
	if err != nil {
		t.Fatalf("ReadFrom(1) on the crash image: %v", err)
	}
	requireSeqs(t, "ReadFrom(1) on the crash image", readBack, n)
	segments := w.listSegments()
	header, err := readHeaderPrefix(filepath.Join(dir, segments[len(segments)-1].name))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := parseWALv2Header(header); err != nil {
		t.Fatalf("the highest segment %s after opening the crash image is not format 2: %v", segments[len(segments)-1].name, err)
	}
	seq, err := appendRotationEntry(w, n)
	if err != nil {
		t.Fatalf("appending to the opened crash image: %v", err)
	}
	if seq != uint64(n+1) {
		t.Fatalf("the first append to the opened crash image got sequence %d, want %d", seq, n+1)
	}
	requireSeqs(t, "ReadBatch from 1 on the opened crash image", readAllThroughCursor(t, w), n+1)
}

// writeSpare writes a spare file of the given name holding header and then
// zeros up to size.
func writeSpare(t *testing.T, dir, name string, header []byte, size int64) {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, header, 0o600); err != nil {
		t.Fatal(err)
	}
	if size > int64(len(header)) {
		if err := os.Truncate(path, size); err != nil {
			t.Fatal(err)
		}
	}
}

// TestWALRotationCrashImages: every image a crash inside a rotation can leave
// opens with every synced entry, no recovery report, contiguous sequences and a
// format 2 highest segment (spec §2.2.1). The hand-built images are the rows of
// the crash-window table: the rename lost, the truncate lost, the next segment
// named with nothing or 64 MiB of zeros behind its header, a stale spare. The
// faulted images are real rotations failed at each step and then abandoned
// without a Close. Break by: the open's walk-back, or its spare rules.
func TestWALRotationCrashImages(t *testing.T) {
	for _, mode := range rotationKeyModes {
		t.Run(mode.name, func(t *testing.T) {
			keys := mode.keys()
			var epoch uint64
			if keys != nil {
				epoch = keys.CurrentEpoch()
			}
			// staleHeader is a spare header this run would not use: the other
			// key mode, or a sealed one of another epoch.
			staleHeader := newWALv2Format(true, epoch+1).header
			if keys == nil {
				staleHeader = newWALv2Format(true, 1).header
			}

			images := []struct {
				name  string
				opts  WALOptions
				image func(t *testing.T, dir string, highest int)
			}{
				{"rename lost, off: wal_spare.tmp holds the next header", WALOptions{}, func(t *testing.T, dir string, _ int) {
					writeSpare(t, dir, walSpareTmpName, headerOnlySegment(keys), 0)
				}},
				// The on images' spares are the size requireCrashImageOpens opens
				// with, so the open validates and takes them rather than
				// deleting them for their size.
				{"rename lost, on: wal_spare.ready holds a whole spare", WALOptions{Preallocate: true}, func(t *testing.T, dir string, _ int) {
					writeSpare(t, dir, walSpareReadyName, headerOnlySegment(keys), 4<<10)
				}},
				{"truncate lost: the tail has a zero tail", WALOptions{}, func(t *testing.T, dir string, highest int) {
					path := segmentFilePath(dir, highest)
					info, err := os.Stat(path)
					if err != nil {
						t.Fatal(err)
					}
					if err := os.Truncate(path, info.Size()+64<<10); err != nil {
						t.Fatal(err)
					}
				}},
				{"the next segment named, its header only", WALOptions{}, func(t *testing.T, dir string, highest int) {
					writeSpare(t, dir, segmentFileName(highest+1), headerOnlySegment(keys), 0)
				}},
				{"the next segment named, 64 MiB of zeros behind its header", WALOptions{}, func(t *testing.T, dir string, highest int) {
					writeSpare(t, dir, segmentFileName(highest+1), headerOnlySegment(keys), 64<<20)
				}},
				{"a stale wal_spare.ready, off", WALOptions{}, func(t *testing.T, dir string, _ int) {
					writeSpare(t, dir, walSpareReadyName, staleHeader, 64<<20)
				}},
				{"a stale wal_spare.ready, on", WALOptions{Preallocate: true}, func(t *testing.T, dir string, _ int) {
					writeSpare(t, dir, walSpareReadyName, staleHeader, 4<<10)
				}},
			}
			for _, image := range images {
				t.Run(image.name, func(t *testing.T) {
					dir := t.TempDir()
					w := openBirthWAL(t, dir, keys)
					const written = 20
					for i := 0; i < written; i++ {
						if _, err := appendRotationEntry(w, i); err != nil {
							t.Fatalf("Append %d: %v", i, err)
						}
					}
					highest := w.activeIndex
					closeBirthWAL(t, w)
					image.image(t, dir, highest)
					requireCrashImageOpens(t, dir, keys, image.opts, written)
				})
			}

			for _, fault := range rotationFaults {
				t.Run("faulted at "+fault.name, func(t *testing.T) {
					faulted := faultRotation(t, keys, fault)
					abandonWAL(faulted.w)
					requireCrashImageOpens(t, faulted.dir, keys, WALOptions{}, faulted.appended)
				})
			}
			for _, fault := range preallocatedRotationFaults {
				t.Run("on, faulted at "+fault.name, func(t *testing.T) {
					faulted := faultRotationWith(t, keys, true, fault)
					abandonWAL(faulted.w)
					requireCrashImageOpens(t, faulted.dir, keys, WALOptions{Preallocate: true}, faulted.appended)
				})
			}
		})
	}
}

// holdRotationAt makes the rotating append of a log filled to its rotation
// point stop inside one step, under mu, until release is closed, and then fail
// that step with injected. It returns once the rotation is held, with the
// channel that receives the rotating append's error.
func holdRotationAt(t *testing.T, w *WAL, appended int, step string, injected error) (release chan struct{}, rotated chan error) {
	t.Helper()
	outgoing, incoming := segmentFileName(w.activeIndex), segmentFileName(w.activeIndex+1)
	entered, release := make(chan struct{}), make(chan struct{})
	var armed atomic.Bool
	armed.Store(true)
	hold := func() error {
		close(entered)
		<-release
		return injected
	}
	switch step {
	case "R1":
		realFlush := w.sys.flush
		w.sys.flush = func(f *os.File) error {
			if filepath.Base(f.Name()) == outgoing && armed.CompareAndSwap(true, false) {
				return hold()
			}
			return realFlush(f)
		}
	case "R3 rename":
		realRename := w.sys.rename
		w.sys.rename = func(oldPath, newPath string) error {
			if filepath.Base(newPath) == incoming && armed.CompareAndSwap(true, false) {
				return hold()
			}
			return realRename(oldPath, newPath)
		}
	default:
		t.Fatalf("holdRotationAt: no step %q", step)
	}
	rotated = make(chan error, 1)
	go func() { _, err := appendRotationEntry(w, appended); rotated <- err }()
	<-entered
	return release, rotated
}

// fillToRotation opens a log in keys' mode with 4 KiB segments and appends
// until the next append rotates; it returns the log and how many it appended.
func fillToRotation(t *testing.T, keys WALKeySource) (*WAL, int) {
	t.Helper()
	appended := appendsBeforeRotation(t, keys)
	w := openBirthWAL(t, t.TempDir(), keys)
	for i := 0; i < appended; i++ {
		if _, err := appendRotationEntry(w, i); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	return w, appended
}

// TestWALFastForwardRacingAPoisoningRotation: a FastForward that starts while a
// rotation holds mu inside R1, whose flush then fails, gets the poison once it
// takes mu. It must not fsync the segment after the failed flush and durably
// record a skip on a poisoned log (the BUG-0717 class). Break by: checking the
// poison before mu is taken (S6 verifier finding F1).
func TestWALFastForwardRacingAPoisoningRotation(t *testing.T) {
	for _, mode := range rotationKeyModes {
		t.Run(mode.name, func(t *testing.T) {
			w, appended := fillToRotation(t, mode.keys())
			defer abandonWAL(w)
			injected := errors.New("simulated EIO at R1")
			release, rotated := holdRotationAt(t, w, appended, "R1", injected)

			fastForward := make(chan error, 1)
			go func() { _, err := w.FastForward(uint64(appended) + 100); fastForward <- err }()
			// Long enough for FastForward to pass any check made before mu and
			// wait on it; a slow scheduler can only make this test pass on a
			// broken build, never fail on a correct one.
			time.Sleep(100 * time.Millisecond)
			close(release)
			if err := <-rotated; !errors.Is(err, injected) {
				t.Fatalf("the rotating append returned %v, want the injected R1 failure", err)
			}
			if err := <-fastForward; !errors.Is(err, injected) {
				t.Fatalf("a FastForward that raced the poisoning rotation returned %v (LastSequence now %d), want the poison", err, w.LastSequence())
			}
			if got := w.LastSequence(); got != uint64(appended) {
				t.Fatalf("LastSequence is %d after the race, want %d: a poisoned log recorded a skip", got, appended)
			}
		})
	}
}

// TestWALSyncThroughRacingAPoisoningRotation: a SyncThrough that starts while a
// rotation holds mu inside R3 (the outgoing segment already closed), whose
// rename then fails, gets the poison once it takes mu, not "the log is closed":
// the error must say the log is unusable and the node must restart. Break by:
// checking the poison only before mu is taken (S6 verifier finding F2).
func TestWALSyncThroughRacingAPoisoningRotation(t *testing.T) {
	for _, mode := range rotationKeyModes {
		t.Run(mode.name, func(t *testing.T) {
			w, appended := fillToRotation(t, mode.keys())
			defer abandonWAL(w)
			injected := errors.New("simulated EIO at R3's rename")
			release, rotated := holdRotationAt(t, w, appended, "R3 rename", injected)

			syncThrough := make(chan error, 1)
			go func() { syncThrough <- w.SyncThrough(uint64(appended)) }()
			time.Sleep(100 * time.Millisecond)
			close(release)
			if err := <-rotated; !errors.Is(err, injected) {
				t.Fatalf("the rotating append returned %v, want the injected R3 failure", err)
			}
			if err := <-syncThrough; !errors.Is(err, injected) {
				t.Fatalf("a SyncThrough that raced the poisoning rotation returned %v, want the poison", err)
			}
		})
	}
}

// countOpenFDs is the number of descriptors this process has open.
func countOpenFDs(t *testing.T) int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Skipf("no /proc/self/fd to count descriptors: %v", err)
	}
	return len(entries)
}

// TestWALFailedRotationClosesThePreparedSegment: a rotation that fails after
// R0 opened the next segment's file closes it, whether the failure is R0's own
// or poisons the log, so a node that keeps refusing or a poisoned one does not
// leak a descriptor per attempt. The garbage collector is off so no finalizer
// closes a leaked file behind the test's back. Break by: not closing the
// prepared file in poisonRotation (S6 verifier mutation C1).
func TestWALFailedRotationClosesThePreparedSegment(t *testing.T) {
	defer debug.SetGCPercent(debug.SetGCPercent(-1))
	for _, mode := range rotationKeyModes {
		// What a rotation that succeeds leaves open once the process "dies".
		before := countOpenFDs(t)
		w := openBirthWAL(t, t.TempDir(), mode.keys())
		for i, first := 0, w.activeIndex; w.activeIndex == first; i++ {
			if _, err := appendRotationEntry(w, i); err != nil {
				t.Fatal(err)
			}
		}
		abandonWAL(w)
		good := countOpenFDs(t) - before

		for _, fault := range rotationFaults {
			t.Run(mode.name+"/"+fault.name, func(t *testing.T) {
				before := countOpenFDs(t)
				faulted := faultRotation(t, mode.keys(), fault)
				abandonWAL(faulted.w)
				if leaked := countOpenFDs(t) - before; leaked != good {
					t.Fatalf("after a rotation failed at %s, %d descriptors stay open; a rotation that succeeds leaves %d", fault.name, leaked, good)
				}
			})
		}
	}
}

// TestWALRotationResetsTheSealCounter: R6 starts the new segment's seal counter
// at 0, since its file key is new. With the limit lowered to 3 entries per key,
// nine small entries rotate exactly twice. Break by: leaving sealCounter as it
// was in install (S6 verifier mutation S1), which rotates on every append.
func TestWALRotationResetsTheSealCounter(t *testing.T) {
	restoreSealLimit(t, 3)
	w := openBirthWAL(t, t.TempDir(), birthTestKeys())
	defer closeWAL(t, w)
	first := w.activeIndex
	for i := 0; i < 9; i++ {
		if _, err := w.Append("bucket", OpInsert, []byte(fmt.Sprintf("k%d", i)), []byte("v")); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	if got := w.activeIndex - first; got != 2 {
		t.Fatalf("nine entries with 3 per file key rotated %d times, want 2", got)
	}
	if w.sealCounter != 3 {
		t.Fatalf("the seal counter of the third segment is %d after its 3 entries, want 3", w.sealCounter)
	}
}
