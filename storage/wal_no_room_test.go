package storage

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// errNoRoom is a fallocate that found the filesystem full, as the kernel
// reports it.
var errNoRoom = fmt.Errorf("simulated fallocate on a full disk: %w", syscall.ENOSPC)

// requireOnRefusalText fails t unless err is the spec §4.3 refusal of a
// preallocating log whose segments are size bytes: it names the directory, the
// segment size, the room to free with the command that shows it, and the key
// that stops the reservation.
func requireOnRefusalText(t *testing.T, err error, dir, size, room string) {
	t.Helper()
	if err == nil {
		t.Fatalf("got no error, want the no-room refusal")
	}
	for _, want := range []string{
		"the write-ahead log in " + dir + " has no room for its next " + size + " segment: ",
		". Writes are refused until there is.\n",
		"Free at least " + room + " on the filesystem that holds " + dir + " (run: df -h " + dir + "), and writes resume on their own within 10 s;\n",
		"or set storage.wal_preallocate: off (HOARDDB_WAL_PREALLOCATE=off) and restart to stop reserving space ahead of the log",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("the refusal does not say %q:\n%v", want, err)
		}
	}
}

// fillToRotationPoint appends to w until the next append rotates, and returns
// how many it appended.
func fillToRotationPoint(t *testing.T, w *WAL) int {
	t.Helper()
	n := 0
	for ; w.writeOff+int64(walFrameSize(t, w, n)) <= w.maxFileSize; n++ {
		if _, err := appendRotationEntry(w, n); err != nil {
			t.Fatalf("Append %d: %v", n, err)
		}
	}
	return n
}

// TestWALPreallocENOSPCRefusesWithTheFix: a preallocating log whose next
// segment cannot be filled for want of room refuses the entry with the text
// that says what to free and how (spec §4.3), and changes nothing: the current
// segment stays active and byte for byte the same, the log is not poisoned, no
// sequence is drawn. WriteRefusal reports it. For 10 s every append is refused
// at once without another fill; then, once there is room, appends resume and
// ReadBatch from 1 has no hole. The B2 check refuses the same way without
// calling fallocate. Break by: poisoning; retiring N before R0; no rate limit;
// the off variant of the text in on mode.
func TestWALPreallocENOSPCRefusesWithTheFix(t *testing.T) {
	for _, cause := range []string{"fallocate", "B2 statfs"} {
		t.Run(cause, func(t *testing.T) {
			clock := newWALTestClock()
			var full atomic.Bool
			var allocations, statfsCalls atomic.Int64
			dir := t.TempDir()
			w, gate := openWithHeldFiller(t, dir, 4<<10, func(sys *walSyscalls) {
				sys.now = clock.read
				realFallocate, realStatfs := sys.fallocate, sys.statfsFree
				sys.fallocate = func(f *os.File, size int64) error {
					if full.Load() {
						allocations.Add(1)
						if cause == "fallocate" {
							return errNoRoom
						}
					}
					return realFallocate(f, size)
				}
				sys.statfsFree = func(dirPath string) (uint64, error) {
					if full.Load() {
						statfsCalls.Add(1)
						if cause == "B2 statfs" {
							return 8<<10 - 1, nil
						}
					}
					return realStatfs(dirPath)
				}
			})
			defer closeWAL(t, w)
			appended := fillToRotationPoint(t, w)
			// The background fill fails too, so the rotation finds no spare and
			// fills one itself.
			full.Store(true)
			watch := watchFill(t, w)
			gate.open()
			if err := watch.wait(); !errors.Is(err, syscall.ENOSPC) {
				t.Fatalf("the background fill on a full disk ended with %v, want ENOSPC", err)
			}
			outgoing := w.activeIndex
			before, err := os.ReadFile(segmentFilePath(dir, outgoing))
			if err != nil {
				t.Fatal(err)
			}

			_, rotateErr := appendRotationEntry(w, appended)
			if !errors.Is(rotateErr, syscall.ENOSPC) {
				t.Fatalf("the rotating append on a full disk returned %v, want ENOSPC", rotateErr)
			}
			requireOnRefusalText(t, rotateErr, dir, "4 KiB", "8 KiB")
			refusal := w.WriteRefusal()
			requireOnRefusalText(t, refusal, dir, "4 KiB", "8 KiB")
			if err := w.poisonedErr(); err != nil {
				t.Fatalf("a full disk at R0 poisoned the log: %v", err)
			}
			if got := w.LastSequence(); got != uint64(appended) {
				t.Fatalf("the refusal moved LastSequence to %d, want %d", got, appended)
			}
			if w.activeIndex != outgoing || w.activeFile == nil {
				t.Fatalf("the refusal left segment %d active (file %v), want %d still active and open", w.activeIndex, w.activeFile, outgoing)
			}
			after, err := os.ReadFile(segmentFilePath(dir, outgoing))
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(before, after) {
				t.Fatalf("the refusal changed %s (%d bytes before, %d after): it was retired before R0", segmentFileName(outgoing), len(before), len(after))
			}
			if cause == "B2 statfs" && allocations.Load() != 0 {
				t.Fatalf("the B2 check called fallocate %d times on a filesystem without room for two segments", allocations.Load())
			}

			// Within 10 s: refused at once, with no new fill.
			allocationsBefore, statfsBefore := allocations.Load(), statfsCalls.Load()
			clock.advance(9 * time.Second)
			if _, err := appendRotationEntry(w, appended); !errors.Is(err, syscall.ENOSPC) {
				t.Fatalf("an append 9 s after the refusal returned %v, want the cached refusal", err)
			}
			if _, err := w.AppendUnsyncedFrom("bucket", OpInsert, []byte("unsynced"), rotationPayload, rotationOrigin); !errors.Is(err, syscall.ENOSPC) {
				t.Fatalf("AppendUnsyncedFrom 9 s after the refusal returned %v, want the cached refusal", err)
			}
			if allocations.Load() != allocationsBefore || statfsCalls.Load() != statfsBefore {
				t.Fatalf("the appends inside the 10 s window tried another fill (%d fallocate, %d statfs calls); want the cached refusal at once",
					allocations.Load()-allocationsBefore, statfsCalls.Load()-statfsBefore)
			}

			// Room again, and 10 s on: the next append fills, rotates and lands.
			full.Store(false)
			clock.advance(time.Second)
			seq, err := appendRotationEntry(w, appended)
			if err != nil {
				t.Fatalf("the append once there is room again: %v", err)
			}
			if seq != uint64(appended+1) {
				t.Fatalf("the append after the refusal got sequence %d, want %d", seq, appended+1)
			}
			if w.activeIndex != outgoing+1 {
				t.Fatalf("the append after the refusal left segment %d active, want %d", w.activeIndex, outgoing+1)
			}
			if err := w.WriteRefusal(); err != nil {
				t.Fatalf("WriteRefusal after the rotation succeeded: %v", err)
			}
			requireSeqs(t, "ReadBatch from 1", readAllThroughCursor(t, w), appended+1)
		})
	}
}

// TestWALOpenWithoutRoomStartsAndRefusesWrites: an open that cannot bear its
// new segment still opens, with no active segment (spec §3.4 step 7, §4.3).
// Reads work. Append, AppendUnsyncedFrom, SyncThrough above what is synced and
// FastForward are refused with the §4.3 text, and WriteRefusal says so for as
// long as there is no segment, past the 10 s between attempts (HEALTH_CHECK
// and /ready read it); SyncThrough at or below what is synced returns nil. Once
// there is room, the next write activates a segment and lands at the next
// sequence. Break by: failing the open; AppendUnsyncedFrom or FastForward
// writing with no active segment; a WriteRefusal that clears after 10 s while
// there is still no segment; poisoning.
func TestWALOpenWithoutRoomStartsAndRefusesWrites(t *testing.T) {
	for _, preallocate := range []bool{true, false} {
		t.Run(fmt.Sprintf("preallocate=%v", preallocate), func(t *testing.T) {
			dir := t.TempDir()
			const segment = 4 << 10
			// A previous run wrote three entries.
			w, err := OpenWALWithOptions(dir, nil, WALOptions{Preallocate: preallocate, SegmentBytes: segment})
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 3; i++ {
				if _, err := appendRotationEntry(w, i); err != nil {
					t.Fatalf("Append %d: %v", i, err)
				}
			}
			closeWAL(t, w)
			// No ready spare: the open has to make its segment.
			if err := os.Remove(filepath.Join(dir, walSpareReadyName)); err != nil && !errors.Is(err, os.ErrNotExist) {
				t.Fatal(err)
			}

			clock := newWALTestClock()
			var full atomic.Bool
			full.Store(true)
			withOpenSyscalls(t, func(sys *walSyscalls) {
				sys.now = clock.read
				realFallocate, realWriteAt := sys.fallocate, sys.writeAt
				sys.fallocate = func(f *os.File, size int64) error {
					if full.Load() {
						return errNoRoom
					}
					return realFallocate(f, size)
				}
				// The off mode's birth is its 24-byte header.
				sys.writeAt = func(f *os.File, b []byte, off int64) (int, error) {
					if full.Load() && !preallocate && filepath.Base(f.Name()) == walSpareTmpName {
						return 0, fmt.Errorf("simulated header write on a full disk: %w", syscall.ENOSPC)
					}
					return realWriteAt(f, b, off)
				}
			})
			w, err = OpenWALWithOptions(dir, nil, WALOptions{Preallocate: preallocate, SegmentBytes: segment})
			if err != nil {
				t.Fatalf("an open with no room for its new segment failed instead of starting with writes refused: %v", err)
			}
			defer closeWAL(t, w)
			if err := w.poisonedErr(); err != nil {
				t.Fatalf("an open with no room poisoned the log: %v", err)
			}
			requireSeqs(t, "ReadFrom with no active segment", mustReadFrom(t, w, 1), 3)
			requireSeqs(t, "ReadBatch with no active segment", readAllThroughCursor(t, w), 3)

			requireRefused := func(what string, err error) {
				t.Helper()
				if !errors.Is(err, syscall.ENOSPC) {
					t.Fatalf("%s with no active segment returned %v, want the refusal", what, err)
				}
				if preallocate {
					requireOnRefusalText(t, err, dir, "4 KiB", "8 KiB")
				} else if !strings.Contains(err.Error(), "the write-ahead log in "+dir+" could not start its next segment") {
					t.Fatalf("%s with no active segment returned %v, want the off variant of the refusal", what, err)
				}
			}
			requireRefused("WriteRefusal", w.WriteRefusal())
			for _, at := range []time.Duration{0, 10 * time.Second} {
				// Inside the 10 s, and again after the next attempt failed.
				clock.advance(at)
				_, err := appendRotationEntry(w, 3)
				requireRefused("Append", err)
				_, err = w.AppendUnsyncedFrom("bucket", OpInsert, []byte("unsynced"), rotationPayload, rotationOrigin)
				requireRefused("AppendUnsyncedFrom", err)
				requireRefused("SyncThrough above the synced sequence", w.SyncThrough(4))
				_, err = w.FastForward(10)
				requireRefused("FastForward", err)
				if err := w.SyncThrough(3); err != nil {
					t.Fatalf("SyncThrough of an entry already synced, with no active segment: %v", err)
				}
				if got := w.LastSequence(); got != 3 {
					t.Fatalf("the refused writes moved LastSequence to %d, want 3", got)
				}
			}
			// The 10 s since the last attempt have passed, and the log still
			// has no segment: HEALTH_CHECK must still say so.
			clock.advance(10 * time.Second)
			requireRefused("WriteRefusal 10 s after the last attempt, still with no segment", w.WriteRefusal())
			if err := w.poisonedErr(); err != nil {
				t.Fatalf("the refused writes poisoned the log: %v", err)
			}

			full.Store(false)
			seq, err := appendRotationEntry(w, 3)
			if err != nil {
				t.Fatalf("the first append once there is room: %v", err)
			}
			if seq != 4 {
				t.Fatalf("the first append after the activation got sequence %d, want 4", seq)
			}
			if err := w.WriteRefusal(); err != nil {
				t.Fatalf("WriteRefusal once a segment is active: %v", err)
			}
			if _, err := w.AppendUnsyncedFrom("bucket", OpInsert, []byte("unsynced"), rotationPayload, rotationOrigin); err != nil {
				t.Fatalf("AppendUnsyncedFrom after the activation: %v", err)
			}
			if err := w.SyncThrough(5); err != nil {
				t.Fatalf("SyncThrough after the activation: %v", err)
			}
			requireSeqs(t, "ReadBatch after the activation", readAllThroughCursor(t, w), 5)
		})
	}
}

// TestWALOffModeSegmentFailureText: a log that does not preallocate and cannot
// create its next 24-byte segment says so, and does not send the operator to
// a preallocation key that is already off (spec §4.3, the off variant), at the
// open and at a rotation, even when the cause is a full disk. Break by: the on
// mode's text in off mode.
func TestWALOffModeSegmentFailureText(t *testing.T) {
	check := func(t *testing.T, err error, dir string) {
		t.Helper()
		want := "the write-ahead log in " + dir + " could not start its next segment: "
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("got %v, want the off variant %q", err, want)
		}
		for _, want := range []string{
			". Writes are refused until it can.\n",
			"Check that " + dir + " is writable and has free space (run: df -h " + dir + "); writes resume on their own within 10 s",
		} {
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("the off refusal does not say %q:\n%v", want, err)
			}
		}
		if strings.Contains(err.Error(), "wal_preallocate") {
			t.Fatalf("the off refusal sends the operator to the preallocation key:\n%v", err)
		}
	}
	failHeader := func(full *atomic.Bool) func(sys *walSyscalls) {
		return func(sys *walSyscalls) {
			real := sys.writeAt
			sys.writeAt = func(f *os.File, b []byte, off int64) (int, error) {
				if full.Load() && filepath.Base(f.Name()) == walSpareTmpName {
					return 0, fmt.Errorf("simulated header write on a full disk: %w", syscall.ENOSPC)
				}
				return real(f, b, off)
			}
		}
	}

	t.Run("open", func(t *testing.T) {
		var full atomic.Bool
		full.Store(true)
		withOpenSyscalls(t, failHeader(&full))
		dir := t.TempDir()
		w, err := OpenWAL(dir, nil)
		if err != nil {
			t.Fatalf("an open that cannot create its segment failed: %v", err)
		}
		defer closeWAL(t, w)
		check(t, w.WriteRefusal(), dir)
	})
	t.Run("rotation", func(t *testing.T) {
		var full atomic.Bool
		withOpenSyscalls(t, failHeader(&full))
		dir := t.TempDir()
		w := openBirthWAL(t, dir, nil)
		defer closeWAL(t, w)
		appended := fillToRotationPoint(t, w)
		full.Store(true)
		_, err := appendRotationEntry(w, appended)
		check(t, err, dir)
	})
}

// TestWALWriteRefusalReportsAPoisonedLog: a log poisoned by a failed flush
// refuses every write with "restart the node", and WriteRefusal says so, so
// HEALTH_CHECK and /ready take the node out of service instead of reporting it
// ready while every write it is sent fails (S6 verifier open item). Break by:
// WriteRefusal reading only the directory guard and the rotation refusal.
func TestWALWriteRefusalReportsAPoisonedLog(t *testing.T) {
	for _, mode := range walFlushModes {
		t.Run(mode.name, func(t *testing.T) {
			w := mode.open(t, t.TempDir(), 4<<10)
			defer func() {
				if err := w.Close(); err != nil {
					t.Logf("closing the poisoned log: %v", err)
				}
			}()
			if err := w.WriteRefusal(); err != nil {
				t.Fatalf("WriteRefusal on a healthy log: %v", err)
			}
			injected := fmt.Errorf("simulated EIO on flush: %w", syscall.EIO)
			failFlushes(w, func(*os.File) error { return injected })
			if _, err := w.Append("bucket", OpInsert, []byte("k"), []byte("v")); !errors.Is(err, injected) {
				t.Fatalf("the append whose flush failed returned %v, want the flush failure", err)
			}
			refusal := w.WriteRefusal()
			if !errors.Is(refusal, injected) {
				t.Fatalf("WriteRefusal on a poisoned log returned %v, want the flush failure", refusal)
			}
			if !strings.Contains(refusal.Error(), "restart the node") {
				t.Fatalf("WriteRefusal on a poisoned log does not say to restart the node: %v", refusal)
			}
		})
	}
}
