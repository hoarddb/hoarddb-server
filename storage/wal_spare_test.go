package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// spareTestSegment is the segment size the preallocation tests open with: one
// zero chunk, so a fill is a few writes and the tests stay small on disk.
const spareTestSegment = 16 << 10

// openPreallocatedWAL opens dir with preallocated segments of size bytes.
func openPreallocatedWAL(t *testing.T, dir string, keys WALKeySource, size int64) *WAL {
	t.Helper()
	w, err := OpenWALWithOptions(dir, keys, WALOptions{Preallocate: true, SegmentBytes: size})
	if err != nil {
		t.Fatalf("OpenWALWithOptions with preallocation: %v", err)
	}
	return w
}

// withOpenSyscalls makes every open in this test start from previous's table
// changed by change, so the calls the open and its filler make (the filler
// keeps the table the open had) go through it.
func withOpenSyscalls(t *testing.T, change func(sys *walSyscalls)) {
	t.Helper()
	previous := walSyscallsAtOpen
	walSyscallsAtOpen = func() walSyscalls {
		sys := previous()
		change(&sys)
		return sys
	}
	t.Cleanup(func() { walSyscallsAtOpen = previous })
}

// fillGate holds the n-th time any fill reaches a stage until release is
// closed. entered is closed when it holds. Calls before and after pass.
type fillGate struct {
	stage   string
	n       int64
	seen    atomic.Int64
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func newFillGate(stage string, n int64) *fillGate {
	return &fillGate{stage: stage, n: n, entered: make(chan struct{}), release: make(chan struct{})}
}

func (g *fillGate) hook(stage string) {
	if stage != g.stage || g.seen.Add(1) != g.n {
		return
	}
	close(g.entered)
	// Bounded, so a broken build that holds the wrong fill (the open's own,
	// say) fails the test's assertions instead of hanging it.
	select {
	case <-g.release:
	case <-time.After(fillWaitLimit):
	}
}

// open lets the held fill go on. It is safe to call more than once.
func (g *fillGate) open() { g.once.Do(func() { close(g.release) }) }

// waitHeld fails t unless the gate holds a fill within fillWaitLimit.
func (g *fillGate) waitHeld(t *testing.T) {
	t.Helper()
	select {
	case <-g.entered:
	case <-time.After(fillWaitLimit):
		t.Fatalf("no fill reached stage %q a %d-th time within %v: the background filler did not get to it", g.stage, g.n, fillWaitLimit)
	}
}

// spareStateOf reads w's spare state.
func spareStateOf(w *WAL) spareState {
	w.spareMu.Lock()
	defer w.spareMu.Unlock()
	return w.spareState
}

// waitForReadySpare waits up to fillWaitLimit for w's filler to make a spare
// ready, and checks the state is not a lie: wal_spare.ready is on disk.
func waitForReadySpare(t *testing.T, w *WAL) {
	t.Helper()
	deadline := time.Now().Add(fillWaitLimit)
	for spareStateOf(w) != spareReady {
		if time.Now().After(deadline) {
			t.Fatalf("no spare became ready within %v (state %d): the background filler did not finish a fill", fillWaitLimit, spareStateOf(w))
		}
		time.Sleep(time.Millisecond)
	}
	if _, err := os.Stat(filepath.Join(w.dir, walSpareReadyName)); err != nil {
		t.Fatalf("the spare state is ready and %s is not on disk: %v", walSpareReadyName, err)
	}
}

// fillWaitLimit is how long a test waits for a background goroutine to start
// or finish before it fails. It is generous on purpose: a loaded CI shard can
// keep the filler off the CPU for longer than a few seconds, and a test that
// fails then reports a scheduler, not a product bug. A failure after this long
// still means the goroutine never got there.
var fillWaitLimit = 60 * time.Second

// fillWatch follows one background fill on w: the fill running when the watch
// began, or else the next one to start. Take it before releasing a held fill
// with gate.open(). A released fill can end before the test first looks, and a
// watch taken after that waits for the filler's retry, 10 s away, which is how
// a fill that ended in microseconds failed a test at its 10 s deadline.
type fillWatch struct {
	t        *testing.T
	w        *WAL
	fill     *spareFill
	ends     int64
	failures uint64
}

// watchFill starts following the fill running on w, or the next one to start.
func watchFill(t *testing.T, w *WAL) *fillWatch {
	t.Helper()
	w.spareMu.Lock()
	fill := w.spareCurrent
	w.spareMu.Unlock()
	return &fillWatch{
		t:        t,
		w:        w,
		fill:     fill,
		ends:     w.counters.zeroFillNanos.Load(),
		failures: w.counters.fillFailures.Load(),
	}
}

// wait returns what the watched fill ended with, waiting up to fillWaitLimit
// for it to start and then to end. It polls the real condition, the claimed
// spare, rather than assuming the filler has run by a fixed time. A fill that
// starts and ends between two polls leaves no spareCurrent to see, but it does
// bump zeroFillNanos once per fill, so the watch reads its outcome from
// fillFailures. An unseen failure cannot be returned, so it fails the test.
func (f *fillWatch) wait() error {
	f.t.Helper()
	deadline := time.Now().Add(fillWaitLimit)
	for {
		f.w.spareMu.Lock()
		current := f.w.spareCurrent
		f.w.spareMu.Unlock()
		if f.fill == nil {
			f.fill = current
		}
		if f.fill != nil {
			select {
			case <-f.fill.done:
				return f.fill.err
			case <-time.After(fillWaitLimit):
				f.t.Fatalf("the fill started but did not end within %v", fillWaitLimit)
			}
		}
		// Nothing is claimed now, so every fill that ended after the watch began
		// has finished its bookkeeping: fillFailures is final.
		if f.w.counters.zeroFillNanos.Load() != f.ends {
			if f.w.counters.fillFailures.Load() != f.failures {
				f.t.Fatalf("a fill ended with an error between two polls, and the test did not keep it")
			}
			return nil
		}
		if time.Now().After(deadline) {
			f.t.Fatalf("no fill started within %v: the background filler never claimed the spare", fillWaitLimit)
		}
		time.Sleep(time.Millisecond)
	}
}

// headerGeneration is the generation in the v2 header of the file at path.
func headerGeneration(t *testing.T, path string) uint64 {
	t.Helper()
	header, err := readHeaderPrefix(path)
	if err != nil {
		t.Fatal(err)
	}
	format, err := parseWALv2Header(header)
	if err != nil {
		t.Fatalf("%s has no v2 header: %v", filepath.Base(path), err)
	}
	return format.gen
}

// statOrFail returns the FileInfo of path.
func statOrFail(t *testing.T, path string) os.FileInfo {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return info
}

// contentOrFail reads the whole file at path. The negative "this spare was not
// used" checks compare content, not os.SameFile: the unusable spare is deleted
// and a new segment may be handed its freed inode (tmpfs does this), so
// SameFile cannot tell a reused spare from a reused inode number.
func contentOrFail(t *testing.T, path string) []byte {
	t.Helper()
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return content
}

// requireAbsent fails t when path exists.
func requireAbsent(t *testing.T, path, why string) {
	t.Helper()
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("%s: %s is still there (stat: %v)", why, filepath.Base(path), err)
	}
}

// spareKeyModes are the key modes a spare's validation runs in.
var spareKeyModes = []struct {
	name string
	keys func() WALKeySource
}{
	{"plain", func() WALKeySource { return nil }},
	{"sealed", func() WALKeySource { return birthTestKeys() }},
}

// TestWALSpareValidation: a spare is used only once it is exactly what a
// segment born now would be (spec §2.1 "Validation at use"). At open, a
// wal_spare.tmp is deleted before anything can rename it, even beside a ready
// spare the open takes (E15: the background fill's own delete must not be what
// removes it). A wal_spare.ready of the wrong size, with a bad header, or with
// a non-zero byte in the 4 KiB after its header is deleted and not used, at the
// open and at a rotation. A ready spare whose sealed flag or epoch is not this
// run's is re-headed, same file, with a new generation, and used. (With
// Preallocate false the open deletes a ready spare with the INFO line:
// TestWALOpenWithoutPreallocateRemovesTheReadySpare.) Break by: dropping the
// open's removal of wal_spare.tmp, any one validation check, or the re-head.
func TestWALSpareValidation(t *testing.T) {
	for _, mode := range spareKeyModes {
		t.Run(mode.name, func(t *testing.T) {
			keys := mode.keys()
			good := headerOnlySegment(keys)

			t.Run("a leftover wal_spare.tmp beside a ready spare is deleted at open", func(t *testing.T) {
				dir := t.TempDir()
				writeSpare(t, dir, walSpareReadyName, good, spareTestSegment)
				readyBefore := statOrFail(t, filepath.Join(dir, walSpareReadyName))
				if err := os.WriteFile(filepath.Join(dir, walSpareTmpName), bytes.Repeat([]byte{0xee}, 4096), 0o600); err != nil {
					t.Fatal(err)
				}
				// The filler's first fill is held before its own delete, so
				// only the open can have removed the leftover.
				gate := newFillGate("begin", 1)
				withOpenSyscalls(t, func(sys *walSyscalls) { sys.fillStage = gate.hook })
				w := openPreallocatedWAL(t, dir, keys, spareTestSegment)
				defer closeWAL(t, w)
				defer gate.open()
				gate.waitHeld(t)
				requireAbsent(t, filepath.Join(dir, walSpareTmpName), "the open with a ready spare")
				if active := statOrFail(t, segmentFilePath(dir, w.activeIndex)); !os.SameFile(readyBefore, active) {
					t.Fatalf("the open did not take the valid ready spare as its new segment")
				}
			})

			t.Run("a half-written wal_spare.tmp from a crash mid-fill is never used", func(t *testing.T) {
				dir := t.TempDir()
				writeSpare(t, dir, walSpareTmpName, good, spareTestSegment/2)
				tmpBefore := contentOrFail(t, filepath.Join(dir, walSpareTmpName))
				w := openPreallocatedWAL(t, dir, keys, spareTestSegment)
				defer closeWAL(t, w)
				active := statOrFail(t, segmentFilePath(dir, w.activeIndex))
				if bytes.Equal(tmpBefore, contentOrFail(t, segmentFilePath(dir, w.activeIndex))) || active.Size() != spareTestSegment {
					t.Fatalf("the new segment is the crashed fill's file or not a whole segment (%d bytes)", active.Size())
				}
				appendBirthPayloads(t, w, 3)
			})

			// size is what the spare file is extended to with zeros; 0 keeps
			// it as written.
			unusable := []struct {
				name  string
				spare func() []byte
				size  int64
			}{
				{"one chunk too long", func() []byte { return append(append([]byte(nil), good...), make([]byte, spareTestSegment)...) }, 0},
				{"cut short by a crash", func() []byte { return append(append([]byte(nil), good...), make([]byte, spareTestSegment/2)...) }, 0},
				{"flags 0x00, a v1 header", func() []byte { return withHeaderByte(good, 6, 0x00) }, spareTestSegment},
				{"an unknown flag", func() []byte { return withHeaderByte(good, 6, good[6]|0x80) }, spareTestSegment},
				{"a reserved byte set", func() []byte { return withHeaderByte(good, 7, 0x01) }, spareTestSegment},
				{"a zero generation", func() []byte {
					spare := append([]byte(nil), good...)
					binary.BigEndian.PutUint64(spare[16:24], 0)
					return spare
				}, spareTestSegment},
				{"no magic", func() []byte { return withHeaderByte(good, 1, 0x00) }, spareTestSegment},
				{"a non-zero byte right after the header", func() []byte { return withTailByte(good, walFileHeaderLen) }, spareTestSegment},
				{"a non-zero byte at the end of the 4 KiB checked", func() []byte { return withTailByte(good, walFileHeaderLen+4095) }, spareTestSegment},
			}
			for _, bad := range unusable {
				t.Run("at open, deleted and not used: "+bad.name, func(t *testing.T) {
					dir := t.TempDir()
					writeSpare(t, dir, walSpareReadyName, bad.spare(), bad.size)
					badBefore := contentOrFail(t, filepath.Join(dir, walSpareReadyName))
					w := openPreallocatedWAL(t, dir, keys, spareTestSegment)
					defer closeWAL(t, w)
					if bytes.Equal(badBefore, contentOrFail(t, segmentFilePath(dir, w.activeIndex))) {
						t.Fatalf("the open used a spare that is %s", bad.name)
					}
					appendBirthPayloads(t, w, 3)
					requireSeqs(t, "ReadFrom(1)", mustReadFrom(t, w, 1), 3)
				})
			}

			t.Run("at a rotation, deleted and not used: a non-zero byte after the header", func(t *testing.T) {
				dir := t.TempDir()
				w := openPreallocatedWAL(t, dir, keys, spareTestSegment)
				defer closeWAL(t, w)
				waitForReadySpare(t, w)
				readyPath := filepath.Join(dir, walSpareReadyName)
				f, err := os.OpenFile(readyPath, os.O_RDWR, 0)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := f.WriteAt([]byte{0xff}, walFileHeaderLen+10); err != nil {
					t.Fatal(err)
				}
				if err := f.Close(); err != nil {
					t.Fatal(err)
				}
				badBefore := contentOrFail(t, readyPath)
				first := w.activeIndex
				n := 0
				for ; w.activeIndex == first; n++ {
					if n == 200 {
						t.Fatalf("200 appends never rotated")
					}
					if _, err := appendRotationEntry(w, n); err != nil {
						t.Fatalf("Append %d: %v", n, err)
					}
				}
				if bytes.Equal(badBefore, contentOrFail(t, segmentFilePath(dir, w.activeIndex))) {
					t.Fatalf("the rotation used a spare with a non-zero byte after its header")
				}
				requireSeqs(t, "ReadFrom(1)", mustReadFrom(t, w, 1), n)
			})

			stale := []struct {
				name   string
				header func() []byte
			}{
				{"the other key mode", func() []byte {
					if keys == nil {
						return newWALv2Format(true, 3).header
					}
					return newWALv2Format(false, 0).header
				}},
			}
			if keys != nil {
				stale = append(stale, struct {
					name   string
					header func() []byte
				}{"another epoch", func() []byte { return newWALv2Format(true, keys.CurrentEpoch()+1).header }})
			}
			for _, old := range stale {
				t.Run("at open, re-headed and used: "+old.name, func(t *testing.T) {
					dir := t.TempDir()
					oldHeader := old.header()
					writeSpare(t, dir, walSpareReadyName, oldHeader, spareTestSegment)
					oldGen := binary.BigEndian.Uint64(oldHeader[16:24])
					spareBefore := statOrFail(t, filepath.Join(dir, walSpareReadyName))
					w := openPreallocatedWAL(t, dir, keys, spareTestSegment)
					activePath := segmentFilePath(dir, w.activeIndex)
					if !os.SameFile(spareBefore, statOrFail(t, activePath)) {
						closeWAL(t, w)
						t.Fatalf("the open did not re-head and use the stale spare (a stale spare is not an unusable one)")
					}
					header, err := readHeaderPrefix(activePath)
					if err != nil {
						t.Fatal(err)
					}
					format, err := parseWALv2Header(header)
					if err != nil {
						t.Fatal(err)
					}
					if format.sealed != (keys != nil) || (keys != nil && format.epochID != keys.CurrentEpoch()) {
						t.Fatalf("the re-headed segment says sealed=%v epoch %d; this run is sealed=%v", format.sealed, format.epochID, keys != nil)
					}
					if format.gen == oldGen {
						t.Fatalf("the re-head kept the stale header's generation %#x", oldGen)
					}
					appendBirthPayloads(t, w, 3)
					closeWAL(t, w)
					w = openPreallocatedWAL(t, dir, keys, spareTestSegment)
					defer closeWAL(t, w)
					requireSeqs(t, "ReadFrom(1) after a reopen", mustReadFrom(t, w, 1), 3)
				})
			}
		})
	}
}

// withHeaderByte is header with byte i replaced.
func withHeaderByte(header []byte, i int, b byte) []byte {
	spare := append([]byte(nil), header...)
	spare[i] = b
	return spare
}

// withTailByte is header followed by zeros with one 0x01 at offset.
func withTailByte(header []byte, offset int) []byte {
	spare := append(append([]byte(nil), header...), make([]byte, offset+1-len(header))...)
	spare[offset] = 0x01
	return spare
}

// mustReadFrom is w.ReadFrom(seq), failing t on an error.
func mustReadFrom(t *testing.T, w *WAL, seq uint64) []WALEntry {
	t.Helper()
	entries, err := w.ReadFrom(seq)
	if err != nil {
		t.Fatalf("ReadFrom(%d): %v", seq, err)
	}
	return entries
}

// TestWALRotationWaitsForTheInFlightFill (-race): a rotation that finds the
// background fill running waits for it with mu held and takes that spare; it
// never starts a fill of its own beside it (owner D11). The filler is held in
// the middle of its zeros; a forced rotation must not return, and no other
// wal_spare.tmp may be created, until the filler is let go; then the new
// segment is the held fill's file and every acknowledged entry reads back.
// Break by: a synchronous fill that does not wait for the one in flight.
func TestWALRotationWaitsForTheInFlightFill(t *testing.T) {
	for _, mode := range spareKeyModes {
		t.Run(mode.name, func(t *testing.T) {
			keys := mode.keys()
			// The open fills its first segment itself (no spare yet): begin 1.
			// The filler's fill after the open is begin 2, held at its first
			// chunk of zeros (chunk 2; the open's fill made chunk 1).
			var created atomic.Int64
			gate := newFillGate("chunk", 2)
			withOpenSyscalls(t, func(sys *walSyscalls) {
				sys.fillStage = func(stage string) {
					if stage == "created" {
						created.Add(1)
					}
					gate.hook(stage)
				}
			})
			dir := t.TempDir()
			w := openPreallocatedWAL(t, dir, keys, spareTestSegment)
			defer closeWAL(t, w)
			defer gate.open()
			gate.waitHeld(t)
			heldGen := headerGeneration(t, filepath.Join(dir, walSpareTmpName))
			createdWhileHeld := created.Load()

			first := w.activeIndex
			n := 0
			for ; ; n++ {
				if n == 200 {
					t.Fatalf("200 appends never reached the rotation point")
				}
				// Stop one short: the next append rotates.
				if w.writeOff+int64(walFrameSize(t, w, n)) > w.maxFileSize {
					break
				}
				if _, err := appendRotationEntry(w, n); err != nil {
					t.Fatalf("Append %d: %v", n, err)
				}
			}
			rotated := make(chan error, 1)
			go func() { _, err := appendRotationEntry(w, n); rotated <- err }()
			select {
			case err := <-rotated:
				t.Fatalf("the rotating append returned (%v) while the fill it needs was still held: it did not wait", err)
			case <-time.After(200 * time.Millisecond):
			}
			if got := created.Load(); got != createdWhileHeld {
				t.Fatalf("%d wal_spare.tmp files were created while the background fill was held, want 0: the rotation filled beside it", got-createdWhileHeld)
			}
			gate.open()
			if err := <-rotated; err != nil {
				t.Fatalf("the rotating append after the fill was let go: %v", err)
			}
			if w.activeIndex != first+1 {
				t.Fatalf("the rotating append left segment %d active, want %d", w.activeIndex, first+1)
			}
			if got := headerGeneration(t, segmentFilePath(dir, w.activeIndex)); got != heldGen {
				t.Fatalf("the new segment has generation %#x, not the held fill's %#x: the rotation did not take the fill it waited for", got, heldGen)
			}
			requireSeqs(t, "ReadFrom(1)", mustReadFrom(t, w, 1), n+1)
			requireSeqs(t, "ReadBatch from 1", readAllThroughCursor(t, w), n+1)
		})
	}
}

// TestWALRotationRefusesWhenTheFillItWaitedForFails: a rotation that waited for
// the background fill and saw it fail refuses the entry with that failure; it
// does not fill again at once (spec §9: within 10 s a failed fill is not
// retried, and a refusal changes nothing). The log is not poisoned, no
// sequence is drawn, and the refusal is the rotation's cached one. Break by:
// ignoring the failure of the fill waited for and filling synchronously.
func TestWALRotationRefusesWhenTheFillItWaitedForFails(t *testing.T) {
	injected := fmt.Errorf("simulated EIO mid-fill: %w", syscall.EIO)
	var armed atomic.Bool
	// The open's fill is chunk 1; the filler's is held at chunk 2.
	gate := newFillGate("chunk", 2)
	withOpenSyscalls(t, func(sys *walSyscalls) {
		sys.fillStage = gate.hook
		real := sys.fsync
		sys.fsync = func(f *os.File) error {
			if filepath.Base(f.Name()) == walSpareTmpName && armed.CompareAndSwap(true, false) {
				return injected
			}
			return real(f)
		}
	})
	w := openPreallocatedWAL(t, t.TempDir(), nil, spareTestSegment)
	defer closeWAL(t, w)
	defer gate.open()
	gate.waitHeld(t)
	n := 0
	for ; w.writeOff+int64(walFrameSize(t, w, n)) <= w.maxFileSize; n++ {
		if _, err := appendRotationEntry(w, n); err != nil {
			t.Fatalf("Append %d: %v", n, err)
		}
	}
	// The fill's B6 fails once; a fill started after it would succeed.
	armed.Store(true)
	rotated := make(chan error, 1)
	go func() { _, err := appendRotationEntry(w, n); rotated <- err }()
	waitForGoroutineIn(t, "storage.(*WAL).prepareSpareSegment", "[chan receive")
	gate.open()
	if err := <-rotated; !errors.Is(err, injected) {
		t.Fatalf("the rotation that waited for a failed fill returned %v, want that fill's failure", err)
	}
	if err := w.poisonedErr(); err != nil {
		t.Fatalf("a failed fill poisoned the log: %v", err)
	}
	if got := w.LastSequence(); got != uint64(n) {
		t.Fatalf("the refused rotation moved LastSequence to %d, want %d", got, n)
	}
	if err := w.WriteRefusal(); !errors.Is(err, injected) {
		t.Fatalf("WriteRefusal after the refused rotation returned %v, want the fill's failure", err)
	}
}

// waitForGoroutineIn waits up to fillWaitLimit for a goroutine whose stack has
// fn and whose state starts with state, such as a rotation blocked on a fill's
// done channel: the one point a test cannot see from outside.
func waitForGoroutineIn(t *testing.T, fn, state string) {
	t.Helper()
	deadline := time.Now().Add(fillWaitLimit)
	buf := make([]byte, 1<<20)
	for {
		stacks := buf[:runtime.Stack(buf, true)]
		for _, stack := range bytes.Split(stacks, []byte("\n\n")) {
			if bytes.Contains(stack, []byte(fn)) && bytes.Contains(stack, []byte(state)) {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("no goroutine in %s reached %s within %v", fn, state, fillWaitLimit)
		}
		time.Sleep(time.Millisecond)
	}
}

// walFrameSize is the on-disk size of rotation entry i in w's active format.
func walFrameSize(t *testing.T, w *WAL, i int) int {
	t.Helper()
	bodyLen := 8 + 1 + 4 + len("bucket") + 4 + len(fmt.Sprintf("key%03d", i)) + len(rotationPayload)
	if w.activeFormat.sealed {
		return bodyLen + 8 + walNonceLen + walTagLen
	}
	return 8 + bodyLen
}

// TestWALRotationUnderLoad (-race): 64 writers on 64 KiB preallocated segments
// rotate at least 50 times; every acknowledged sequence reads back exactly
// once and in order, through ReadFrom and through ReadBatch, and the filler
// makes a spare ready again after the last rotation. Break by: letting the
// spare hand-off race mu (a rotation that takes the spare without spareMu, or
// one that does not give it back to the filler).
func TestWALRotationUnderLoad(t *testing.T) {
	dir := t.TempDir()
	w := openPreallocatedWAL(t, dir, nil, 64<<10)
	defer closeWAL(t, w)
	first := w.activeIndex

	const writers = 64
	var wg sync.WaitGroup
	var mu sync.Mutex
	acked := map[uint64]bool{}
	failures := make(chan error, writers)
	var stop atomic.Bool
	for writer := 0; writer < writers; writer++ {
		wg.Add(1)
		go func(writer int) {
			defer wg.Done()
			for i := 0; !stop.Load(); i++ {
				seq, err := w.Append("bucket", OpInsert, []byte(fmt.Sprintf("w%02d-%05d", writer, i)), rotationPayload)
				if err != nil {
					failures <- fmt.Errorf("writer %d append %d: %w", writer, i, err)
					return
				}
				mu.Lock()
				if acked[seq] {
					mu.Unlock()
					failures <- fmt.Errorf("sequence %d acknowledged twice", seq)
					return
				}
				acked[seq] = true
				mu.Unlock()
			}
		}(writer)
	}
	deadline := time.Now().Add(60 * time.Second)
	for {
		w.mu.Lock()
		rotations := w.activeIndex - first
		w.mu.Unlock()
		if rotations >= 50 || time.Now().After(deadline) {
			break
		}
		time.Sleep(time.Millisecond)
	}
	stop.Store(true)
	wg.Wait()
	close(failures)
	for err := range failures {
		t.Fatal(err)
	}
	if rotations := w.activeIndex - first; rotations < 50 {
		t.Fatalf("the load rotated %d times in 60 s, want at least 50", rotations)
	}
	n := len(acked)
	if uint64(n) != w.LastSequence() {
		t.Fatalf("%d appends were acknowledged and the log's last sequence is %d", n, w.LastSequence())
	}
	requireSeqs(t, "ReadFrom(1) after the load", mustReadFrom(t, w, 1), n)
	requireSeqs(t, "ReadBatch from 1 after the load", readAllThroughCursor(t, w), n)
	waitForReadySpare(t, w)
}

// TestWALFlushIsFdatasyncOnlyOnPreallocatedSegments: the group commit and R1
// flush a preallocated segment with the data-only flush, and a growing one
// with a whole-file fsync; R2 cuts a retired preallocated segment and fsyncs
// it, since its size changed (spec §4.1). Break by: the data-only flush on a
// growing segment (it would not make the size durable), or none on a
// preallocated one.
func TestWALFlushIsFdatasyncOnlyOnPreallocatedSegments(t *testing.T) {
	for _, preallocate := range []bool{false, true} {
		t.Run(fmt.Sprintf("preallocate=%v", preallocate), func(t *testing.T) {
			var recorder walSyscallRecorder
			recorder.recordFromOpen(t)
			dir := t.TempDir()
			w, err := OpenWALWithOptions(dir, nil, WALOptions{Preallocate: preallocate, SegmentBytes: spareTestSegment})
			if err != nil {
				t.Fatal(err)
			}
			first := w.activeIndex
			for i := 0; w.activeIndex == first; i++ {
				if i == 200 {
					t.Fatalf("200 appends never rotated")
				}
				if _, err := appendRotationEntry(w, i); err != nil {
					t.Fatalf("Append %d: %v", i, err)
				}
			}
			closeWAL(t, w)
			outgoing := segmentFileName(first)
			want, other := "flush "+outgoing, "flushData "+outgoing
			if preallocate {
				want, other = other, want
			}
			events := recorder.seen()
			count := map[string]int{}
			for _, event := range events {
				count[event]++
			}
			if count[want] == 0 || count[other] != 0 {
				t.Fatalf("%s was flushed %d times with %q and %d times with %q; want only the first (events %v)",
					outgoing, count[want], want, count[other], other, events)
			}
			truncated := indexOf(events, "ftruncate "+outgoing)
			if !preallocate {
				if truncated >= 0 {
					t.Fatalf("a growing segment was cut at its rotation (events %v)", events)
				}
				return
			}
			lastFlush := lastIndexOf(events, want)
			fsynced := indexAfter(events, truncated, "fsync "+outgoing)
			if truncated < 0 || truncated < lastFlush || fsynced < 0 {
				t.Fatalf("R2 must cut %s after its last flush and then fsync it (events %v)", outgoing, events)
			}
		})
	}
}

func indexOf(events []string, event string) int { return indexAfter(events, -1, event) }

func indexAfter(events []string, after int, event string) int {
	for i := after + 1; i < len(events); i++ {
		if events[i] == event {
			return i
		}
	}
	return -1
}

func lastIndexOf(events []string, event string) int {
	for i := len(events) - 1; i >= 0; i-- {
		if events[i] == event {
			return i
		}
	}
	return -1
}

// TestWALEntryLargerThanASegment: on a preallocated log, an entry that does not
// fit even a fresh segment is written there and extends the file past its
// preallocation; the segment is then grown, and its group commits use the
// whole-file fsync until it is retired, since its size changed. The entry
// reads back after a reopen. Break by: fdatasync on a grown segment.
func TestWALEntryLargerThanASegment(t *testing.T) {
	var recorder walSyscallRecorder
	recorder.recordFromOpen(t)
	dir := t.TempDir()
	w := openPreallocatedWAL(t, dir, nil, spareTestSegment)
	if _, err := w.Append("bucket", OpInsert, []byte("small"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	big := bytes.Repeat([]byte("B"), 2*spareTestSegment)
	if _, err := w.Append("bucket", OpInsert, []byte("big"), big); err != nil {
		t.Fatalf("appending an entry larger than a segment: %v", err)
	}
	grown := segmentFileName(w.activeIndex)
	if size := segmentSize(t, dir, w.activeIndex); size <= spareTestSegment {
		t.Fatalf("the segment holding the large entry is %d bytes, want it grown past %d", size, spareTestSegment)
	}
	if _, err := w.Append("bucket", OpInsert, []byte("after"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	closeWAL(t, w)
	events := recorder.seen()
	if i := indexOf(events, "flushData "+grown); i >= 0 {
		t.Fatalf("the grown segment %s was flushed with fdatasync (event %d of %v)", grown, i, events)
	}
	if indexOf(events, "flush "+grown) < 0 {
		t.Fatalf("the grown segment %s was never flushed with fsync (events %v)", grown, events)
	}

	w = openPreallocatedWAL(t, dir, nil, spareTestSegment)
	defer closeWAL(t, w)
	entries := mustReadFrom(t, w, 1)
	requireSeqs(t, "ReadFrom(1) after a reopen", entries, 3)
	if !bytes.Equal(entries[1].Data, big) {
		t.Fatalf("the large entry read back as %d bytes, want %d", len(entries[1].Data), len(big))
	}
}

// TestWALWriteErrorOnPreallocatedSegmentPoisons: a failed entry write on a
// preallocated segment poisons the log instead of cutting the segment back:
// the cut would shrink a file whose size fdatasync relies on never changing.
// The reopen repairs the log. Break by: rollbackPartialWrite's truncate on a
// preallocated file.
func TestWALWriteErrorOnPreallocatedSegmentPoisons(t *testing.T) {
	var recorder walSyscallRecorder
	recorder.recordFromOpen(t)
	dir := t.TempDir()
	w := openPreallocatedWAL(t, dir, nil, spareTestSegment)
	appendBirthPayloads(t, w, 2)
	injected := errors.New("simulated EIO on an overwrite")
	active := segmentFileName(w.activeIndex)
	real := w.sys.writeAt
	var armed atomic.Bool
	armed.Store(true)
	w.sys.writeAt = func(f *os.File, b []byte, off int64) (int, error) {
		if filepath.Base(f.Name()) == active && armed.CompareAndSwap(true, false) {
			return 0, injected
		}
		return real(f, b, off)
	}
	if _, err := w.Append("bucket", OpInsert, []byte("fails"), []byte("v")); !errors.Is(err, injected) {
		t.Fatalf("the failing append returned %v, want the injected error", err)
	}
	if i := indexOf(recorder.seen(), "ftruncate "+active); i >= 0 {
		t.Fatalf("the failed write cut the preallocated segment %s", active)
	}
	if size := segmentSize(t, dir, w.activeIndex); size != spareTestSegment {
		t.Fatalf("the preallocated segment is %d bytes after the failed write, want %d", size, spareTestSegment)
	}
	if _, err := w.Append("bucket", OpInsert, []byte("next"), []byte("v")); !errors.Is(err, injected) {
		t.Fatalf("the append after the failed write returned %v, want the poison", err)
	}
	if err := w.SyncThrough(2); !errors.Is(err, injected) {
		t.Fatalf("SyncThrough after the failed write returned %v, want the poison", err)
	}
	closeWAL(t, w)

	w = openPreallocatedWAL(t, dir, nil, spareTestSegment)
	defer closeWAL(t, w)
	requireSeqs(t, "ReadFrom(1) after the reopen", mustReadFrom(t, w, 1), 2)
	requireNextAppend(t, w, 3)
}

// TestWALProductionFlushIsTheRightCall (S1 verifier finding (a)): the table an
// open fills has the real flushes, and a log picks the whole-file one for a
// growing segment and the data-only one for a preallocated segment. Every test
// that reaches a flush replaces it first, so without this a no-op flush in the
// production table passes them all. Break by: a no-op flush in
// realWALSyscalls.
func TestWALProductionFlushIsTheRightCall(t *testing.T) {
	pointer := func(fn func(*os.File) error) uintptr { return reflect.ValueOf(fn).Pointer() }
	fileSync, datasync := pointer((*os.File).Sync), pointer(fdatasyncFile)
	for _, preallocate := range []bool{false, true} {
		t.Run(fmt.Sprintf("preallocate=%v", preallocate), func(t *testing.T) {
			w, err := OpenWALWithOptions(t.TempDir(), nil, WALOptions{Preallocate: preallocate, SegmentBytes: spareTestSegment})
			if err != nil {
				t.Fatal(err)
			}
			defer closeWAL(t, w)
			want, name := fileSync, "(*os.File).Sync"
			if preallocate {
				want, name = datasync, "fdatasyncFile"
			}
			if got := pointer(w.segmentFlush()); got != want {
				t.Fatalf("the group commit of a segment with preallocate=%v is not %s", preallocate, name)
			}
		})
	}
	real := realWALSyscalls()
	if pointer(real.flush) != fileSync || pointer(real.flushData) != datasync {
		t.Fatalf("realWALSyscalls' flush entries are not (*os.File).Sync and fdatasyncFile")
	}
}

// TestWALSegmentBytesCountsTheSpare: the disk the log holds includes its ready
// spare and a spare being filled, which are not segments (spec §2.1, P16).
// Break by: counting segment names only.
func TestWALSegmentBytesCountsTheSpare(t *testing.T) {
	dir := t.TempDir()
	w := openPreallocatedWAL(t, dir, nil, spareTestSegment)
	defer closeWAL(t, w)
	waitForReadySpare(t, w)
	got, err := w.SegmentBytes()
	if err != nil {
		t.Fatal(err)
	}
	segments := segmentSize(t, dir, w.activeIndex)
	if want := segments + spareTestSegment; got != want {
		t.Fatalf("SegmentBytes = %d with one %d-byte segment and a %d-byte ready spare, want %d", got, segments, spareTestSegment, want)
	}
	// A spare being filled is disk the log holds as well.
	if err := os.WriteFile(filepath.Join(dir, walSpareTmpName), make([]byte, 1000), 0o600); err != nil {
		t.Fatal(err)
	}
	if got, err = w.SegmentBytes(); err != nil || got != segments+spareTestSegment+1000 {
		t.Fatalf("SegmentBytes = %d, %v with a 1000-byte %s beside them, want %d", got, err, walSpareTmpName, segments+spareTestSegment+1000)
	}
}

// openWithHeldFiller opens a preallocated log in dir whose first background
// fill (the second fill: the open makes the first) is held at "begin" until
// the returned gate opens, with change applied to the open's table.
func openWithHeldFiller(t *testing.T, dir string, size int64, change func(sys *walSyscalls)) (*WAL, *fillGate) {
	t.Helper()
	gate := newFillGate("begin", 2)
	withOpenSyscalls(t, func(sys *walSyscalls) {
		sys.fillStage = gate.hook
		change(sys)
	})
	w := openPreallocatedWAL(t, dir, nil, size)
	gate.waitHeld(t)
	return w, gate
}

// TestWALFailedFillDeletesItsSpareAtOnce: a background fill that fails after
// its blocks were allocated and some zeros written deletes wal_spare.tmp before
// the failure is reported, closes it, and leaves the spare idle. On ext4 a
// failed fallocate keeps the blocks it got (NR-1), so the delete is what gives
// the space back; waiting for the next retry would hold it for 10 s, and
// forever on a filesystem that stays full. The log is not poisoned. Break by:
// leaving the file on the failure path, deleting it only on the next attempt,
// or not closing it.
func TestWALFailedFillDeletesItsSpareAtOnce(t *testing.T) {
	defer debug.SetGCPercent(debug.SetGCPercent(-1))
	injected := fmt.Errorf("simulated ENOSPC mid-fill: %w", syscall.ENOSPC)
	var armed atomic.Bool
	dir := t.TempDir()
	// Three chunks; the second chunk's write fails once the fault is armed.
	const size = 3 << 20
	w, gate := openWithHeldFiller(t, dir, size, func(sys *walSyscalls) {
		real := sys.writeAt
		sys.writeAt = func(f *os.File, b []byte, off int64) (int, error) {
			if armed.Load() && filepath.Base(f.Name()) == walSpareTmpName && off >= walSpareChunk {
				return 0, injected
			}
			return real(f, b, off)
		}
	})
	defer closeWAL(t, w)
	before := countOpenFDs(t)
	armed.Store(true)
	watch := watchFill(t, w)
	gate.open()
	if err := watch.wait(); !errors.Is(err, injected) {
		t.Fatalf("the background fill ended with %v, want the injected failure", err)
	}
	requireAbsent(t, filepath.Join(dir, walSpareTmpName), "when the failed fill is reported")
	if got := spareStateOf(w); got != spareIdle {
		t.Fatalf("the spare state after a failed fill is %d, want idle", got)
	}
	if after := countOpenFDs(t); after != before {
		t.Fatalf("%d descriptors are open after the failed fill, %d before it: the fill leaked its file", after, before)
	}
	if err := w.poisonedErr(); err != nil {
		t.Fatalf("a failed background fill poisoned the log: %v", err)
	}
}

// TestWALFillRefusesWithoutRoomForTwoSegments: a fill starts only when the
// filesystem has room for two segments (B2), and refuses with ENOSPC before it
// allocates or writes anything: a fill must never take the filesystem to its
// last bytes, which the store needs. Break by: skipping the statfs check.
func TestWALFillRefusesWithoutRoomForTwoSegments(t *testing.T) {
	var full atomic.Bool
	var allocations atomic.Int64
	dir := t.TempDir()
	w, gate := openWithHeldFiller(t, dir, spareTestSegment, func(sys *walSyscalls) {
		realStatfs, realFallocate := sys.statfsFree, sys.fallocate
		sys.statfsFree = func(dirPath string) (uint64, error) {
			if full.Load() {
				return 2*spareTestSegment - 1, nil
			}
			return realStatfs(dirPath)
		}
		sys.fallocate = func(f *os.File, size int64) error {
			if full.Load() {
				allocations.Add(1)
			}
			return realFallocate(f, size)
		}
	})
	defer closeWAL(t, w)
	full.Store(true)
	watch := watchFill(t, w)
	gate.open()
	if err := watch.wait(); !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("a fill with one byte less than two segments free ended with %v, want ENOSPC", err)
	}
	if got := allocations.Load(); got != 0 {
		t.Fatalf("the fill called fallocate %d times on a filesystem without room for two segments", got)
	}
	requireAbsent(t, filepath.Join(dir, walSpareTmpName), "a fill refused for room")
	requireAbsent(t, filepath.Join(dir, walSpareReadyName), "a fill refused for room")
}

// TestWALFillerRetriesEveryTenSeconds: a failing background fill is not
// retried in a loop: the filler waits walSpareRetryInterval between attempts,
// so a full disk costs one statfs per 10 s, not a spinning goroutine. Break by:
// retrying at once after a failure.
func TestWALFillerRetriesEveryTenSeconds(t *testing.T) {
	var full atomic.Bool
	var attempts atomic.Int64
	w, gate := openWithHeldFiller(t, t.TempDir(), spareTestSegment, func(sys *walSyscalls) {
		real := sys.statfsFree
		sys.statfsFree = func(dirPath string) (uint64, error) {
			if full.Load() {
				attempts.Add(1)
				return 0, nil
			}
			return real(dirPath)
		}
	})
	defer closeWAL(t, w)
	full.Store(true)
	gate.open()
	deadline := time.Now().Add(fillWaitLimit)
	for attempts.Load() < 1 {
		if time.Now().After(deadline) {
			t.Fatalf("the filler made no fill attempt on a full disk within %v of the gate opening", fillWaitLimit)
		}
		time.Sleep(time.Millisecond)
	}
	// The retry is 10 s away, so a second attempt inside this window is the bug.
	time.Sleep(300 * time.Millisecond)
	if got := attempts.Load(); got != 1 {
		t.Fatalf("the filler made %d fill attempts on a full disk, want 1 and the next after 10 s", got)
	}
}

// TestWALCloseStopsTheFiller: Close cancels a fill in flight and waits for the
// filler to exit; the cancelled fill deletes its wal_spare.tmp and leaves no
// ready spare, and no goroutine is left behind. Break by: Close not stopping
// or not waiting for the filler, or a cancelled fill keeping its file.
func TestWALCloseStopsTheFiller(t *testing.T) {
	baseline := runtime.NumGoroutine()
	dir := t.TempDir()
	// Three chunks, so a cancel lands between them. The filler's fill is the
	// second; hold it after its first chunk (chunk 4: the open's made 3).
	gate := newFillGate("chunk", 4)
	withOpenSyscalls(t, func(sys *walSyscalls) { sys.fillStage = gate.hook })
	w := openPreallocatedWAL(t, dir, nil, 3<<20)
	gate.waitHeld(t)
	closed := make(chan error, 1)
	go func() { closed <- w.Close() }()
	// Close must be waiting for the filler, not done without it.
	select {
	case err := <-closed:
		gate.open()
		t.Fatalf("Close returned (%v) while the filler was still filling", err)
	case <-time.After(100 * time.Millisecond):
	}
	gate.open()
	if err := <-closed; err != nil {
		t.Fatalf("Close: %v", err)
	}
	requireAbsent(t, filepath.Join(dir, walSpareTmpName), "a fill cancelled by Close")
	requireAbsent(t, filepath.Join(dir, walSpareReadyName), "a fill cancelled by Close")
	deadline := time.Now().Add(60 * time.Second)
	for runtime.NumGoroutine() > baseline {
		if time.Now().After(deadline) {
			t.Fatalf("%d goroutines after Close, %d before the open: the filler did not exit", runtime.NumGoroutine(), baseline)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestFillWaitSurvivesAStarvedScheduler: the filler claims the spare a while
// after the wait for a fill starts, as on a loaded CI shard where the filler
// goroutine gets no CPU for a stretch. The wait must hold until the fill starts,
// not give up at a short fixed deadline. The busy goroutines keep every P
// occupied during the delay. The delay is 1.5 s, not the 12 s a CI shard saw,
// to keep the suite fast; the guard below holds the limit above 12 s, so
// reverting it to the old 10 s fails here too. Break by: a deadline shorter
// than a starved filler's delay, or a wait that gives up without polling the
// spare.
func TestFillWaitSurvivesAStarvedScheduler(t *testing.T) {
	if fillWaitLimit <= 12*time.Second {
		t.Fatalf("fillWaitLimit is %v: a fill the filler starts 12 s late on a loaded shard would fail the wait", fillWaitLimit)
	}

	w := openPreallocatedWAL(t, t.TempDir(), nil, spareTestSegment)
	defer closeWAL(t, w)
	waitForReadySpare(t, w)
	// What a rotation does when it takes the ready spare, minus the wake: the
	// filler stays idle until the test wakes it after the delay.
	w.setSpareIdle()
	if err := os.Remove(filepath.Join(w.dir, walSpareReadyName)); err != nil {
		t.Fatalf("removing the ready spare a rotation would have taken: %v", err)
	}

	stop := make(chan struct{})
	var busy sync.WaitGroup
	for i := 0; i < runtime.NumCPU()*2; i++ {
		busy.Add(1)
		go func() {
			defer busy.Done()
			for n := 1; ; n++ {
				select {
				case <-stop:
					return
				default:
				}
				if n%1000 == 0 {
					runtime.Gosched()
				}
			}
		}()
	}
	defer func() {
		close(stop)
		busy.Wait()
	}()

	// Watch before the wake: a fill that ran before the watch began would leave
	// nothing to wait for but the retry.
	watch := watchFill(t, w)
	const delay = 1500 * time.Millisecond
	woke := make(chan struct{})
	go func() {
		defer close(woke)
		time.Sleep(delay)
		w.wakeFiller()
	}()
	defer func() { <-woke }()

	if err := watch.wait(); err != nil {
		t.Fatalf("the fill the filler started %v after the wait began ended with %v, want success", delay, err)
	}
}
