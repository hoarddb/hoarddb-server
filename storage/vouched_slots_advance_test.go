package storage

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/internal/storeimage"

	badger "github.com/dgraph-io/badger/v4"
)

// markerCalls replaces the calls an advance of the marker makes (the store directory's fsync,
// the slot's write, the marker's fdatasync) for one test: each call is recorded under its name,
// and before runs first and may fail it, in which case the call does nothing.
type markerCalls struct {
	mu     sync.Mutex
	calls  []string
	before func(call string) error
}

const (
	callDirectoryFsync = "directory fsync"
	callSlotWrite      = "slot write"
	callFdatasync      = "fdatasync"
)

func installMarkerCalls(t *testing.T, before func(call string) error) *markerCalls {
	t.Helper()
	m := &markerCalls{before: before}
	restoreWrite, restoreSync, restoreDir := writeMarkerSlot, syncMarkerFile, syncDirectory
	t.Cleanup(func() { writeMarkerSlot, syncMarkerFile, syncDirectory = restoreWrite, restoreSync, restoreDir })
	syncDirectory = func(dirPath string) error {
		if err := m.call(callDirectoryFsync); err != nil {
			return err
		}
		return restoreDir(dirPath)
	}
	writeMarkerSlot = func(file *os.File, slot []byte, offset int64) error {
		if err := m.call(callSlotWrite); err != nil {
			return err
		}
		return restoreWrite(file, slot, offset)
	}
	syncMarkerFile = func(file *os.File) error {
		if err := m.call(callFdatasync); err != nil {
			return err
		}
		return restoreSync(file)
	}
	return m
}

func (m *markerCalls) call(name string) error {
	m.mu.Lock()
	m.calls = append(m.calls, name)
	before := m.before
	m.mu.Unlock()
	if before == nil {
		return nil
	}
	return before(name)
}

func (m *markerCalls) recorded() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.calls...)
}

// rotateTheMemtable commits unsynced fill until a memtable log the directory did not hold appears.
func rotateTheMemtable(t *testing.T, kv *BadgerKV, dir, prefix string) {
	t.Helper()
	before, err := memtableLogs(dir)
	noErr(t, err)
	for batch := range 400 {
		noErr(t, commitFill(kv, fmt.Sprintf("%s-%03d", prefix, batch)))
		now, err := memtableLogs(dir)
		noErr(t, err)
		for name := range now {
			if !before[name] {
				return
			}
		}
	}
	t.Fatal("SCENARIO NOT REACHED: no memtable log was created")
}

// waitForTheFlushes waits until the store directory holds one memtable log, the active one: every
// rotated memtable is in a table and its log removed, so an image copied file by file afterwards
// is not copied across a flush.
func waitForTheFlushes(t *testing.T, dir string) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for {
		logs, err := memtableLogs(dir)
		noErr(t, err)
		if len(logs) == 1 {
			return
		}
		isTrue(t, time.Now().Before(deadline), "SCENARIO NOT REACHED: the rotated memtables were not flushed in 90 s")
		time.Sleep(5 * time.Millisecond)
	}
}

// TestALogCreatedAfterTheVersionIsReadIsCoveredByADirectoryFsync is review R-2 item 1, the dirty
// directory: [GUARD the count of Badger's log files is read after the sync's version, so a log
// created between the two reads makes the advance fsync the store directory; red when the count
// is read before the version]. Read before, the count misses a log that can hold a commit at or
// below the version, no directory fsync runs, and a power cut can drop that log's entry while the
// marker vouches its commits (I-1).
func TestALogCreatedAfterTheVersionIsReadIsCoveredByADirectoryFsync(t *testing.T) {
	kv, dir := openVouchedForTest(t)
	setDurably(t, kv, "first") // covers the files Open created
	calls := installMarkerCalls(t, nil)
	setDurably(t, kv, "second")
	equal(t, []string{callSlotWrite, callFdatasync}, calls.recorded(), "SCENARIO NOT REACHED: an advance with no new log "+
		"fsynced the directory")

	var once sync.Once
	var hookErr error
	restore := badger.SetSyncTestHooks(badger.SyncTestHooks{AfterCapture: func() {
		// After the version was read and before the sync returns: a new log appears.
		once.Do(func() {
			defer func() {
				if r := recover(); r != nil {
					hookErr = fmt.Errorf("the hook failed: %v", r)
				}
			}()
			rotateTheMemtable(t, kv, dir, "dirty")
		})
	}})
	t.Cleanup(restore)
	calls = installMarkerCalls(t, nil)
	setUnsynced(t, kv, "third")
	noErr(t, kv.Sync())
	noErr(t, hookErr)
	equal(t, []string{callDirectoryFsync, callSlotWrite, callFdatasync}, calls.recorded(),
		"a log created after the version was read was not covered by a directory fsync")
}

// TestTheFirstAdvanceAfterAnOpenSyncsTheDirectory is review R2 item 12: [GUARD the files Open
// created (its active log, a value-log file) count as not yet fsynced, so the first advance after
// an open fsyncs the directory, and the next one with no new file does not; red when the open
// takes them as covered].
func TestTheFirstAdvanceAfterAnOpenSyncsTheDirectory(t *testing.T) {
	for _, start := range []string{"a first start", "a reopen"} {
		t.Run(start, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "badger")
			if start == "a reopen" {
				first, err := openVouchedTestStore(dir, vouchedTestTuning)
				noErr(t, err)
				setDurably(t, first, "before the reopen")
				noErr(t, first.Close())
			}
			kv, err := openVouchedTestStore(dir, vouchedTestTuning)
			noErr(t, err)
			defer func() { noErr(t, kv.Close()) }()
			calls := installMarkerCalls(t, nil)
			setDurably(t, kv, "first")
			setDurably(t, kv, "second")
			equal(t, []string{callDirectoryFsync, callSlotWrite, callFdatasync, callSlotWrite, callFdatasync},
				calls.recorded(), "the calls of the first two advances after %s", start)
		})
	}
}

// TestALogCreatedDuringTheDirectoryFsyncIsFsyncedAtTheNextAdvance is review R2 item 11: [GUARD the
// count an advance remembers is the one it read before its directory fsync, so a log created while
// the fsync runs is fsynced at the next advance; red when the count is read again after the fsync].
func TestALogCreatedDuringTheDirectoryFsyncIsFsyncedAtTheNextAdvance(t *testing.T) {
	kv, dir := openVouchedForTest(t)
	var once sync.Once
	var hookErr error
	calls := installMarkerCalls(t, func(call string) error {
		if call == callDirectoryFsync {
			once.Do(func() {
				defer func() {
					if r := recover(); r != nil {
						hookErr = fmt.Errorf("the hook failed: %v", r)
					}
				}()
				rotateTheMemtable(t, kv, dir, "during")
			})
		}
		return nil
	})
	setDurably(t, kv, "first")
	noErr(t, hookErr)
	setDurably(t, kv, "second")
	equal(t, []string{callDirectoryFsync, callSlotWrite, callFdatasync, callDirectoryFsync, callSlotWrite, callFdatasync},
		calls.recorded(), "a log created during the first advance's directory fsync was taken as covered by it")
}

// TestTheNextAdvanceWritesTheSlotTheOpenDidNotChoose is review R-2 item 4: [GUARD after each open
// the next advance writes the slot that does not hold the marker the open chose, at its sequence
// plus 1, whichever slot that is; red when the slot follows a count that restarts with the
// process]. Three lives, so the chosen slot is 0, then 1, then 0 again.
func TestTheNextAdvanceWritesTheSlotTheOpenDidNotChoose(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "badger")
	for life := range 3 {
		kv, err := openVouchedTestStore(dir, vouchedTestTuning)
		noErr(t, err)
		chosen, found, err := readVouchedFile(dir)
		noErr(t, err)
		isTrue(t, found, "no marker")
		before, err := os.ReadFile(filepath.Join(dir, vouchedFileName))
		noErr(t, err)
		setDurably(t, kv, fmt.Sprintf("life-%d", life))
		after, err := os.ReadFile(filepath.Join(dir, vouchedFileName))
		noErr(t, err)
		kept := before[chosen.slot*vouchedSlotSize : (chosen.slot+1)*vouchedSlotSize]
		isTrue(t, bytes.Equal(kept, after[chosen.slot*vouchedSlotSize:(chosen.slot+1)*vouchedSlotSize]),
			"life %d: the first advance overwrote slot %d, the one the open chose", life, chosen.slot)
		now, _, err := readVouchedFile(dir)
		noErr(t, err)
		equal(t, 1-chosen.slot, now.slot, "life %d: the advance's slot", life)
		equal(t, chosen.sequence+1, now.sequence, "life %d: the advance's sequence", life)
		noErr(t, kv.Close())
	}
}

// TestAnAdvanceInAStoreDirectoryMovedAsideIsRefused is review R2 item 18: [GUARD an advance checks
// the directory guard first, so a sync of a store whose directory was moved aside under it fails
// and poisons instead of vouching through the open descriptor; red when the advance skips the
// guard]. The rename the in-place write replaced failed here by itself; a descriptor does not.
func TestAnAdvanceInAStoreDirectoryMovedAsideIsRefused(t *testing.T) {
	parent := t.TempDir()
	dir := filepath.Join(parent, "badger")
	kv, err := openVouchedTestStore(dir, vouchedTestTuning)
	noErr(t, err)
	t.Cleanup(func() { _ = kv.Close() }) // the store was moved aside; its close is not under test
	setDurably(t, kv, "before")
	// The guard's own interval would let a write through for up to 100 ms after the move: the
	// commit itself may pass it, the advance must not.
	setUnsynced(t, kv, "after")
	noErr(t, os.Rename(dir, filepath.Join(parent, "moved-aside")))
	noErr(t, os.Mkdir(dir, 0o700))
	calls := installMarkerCalls(t, nil)
	err = kv.Sync()
	var gone *DirectoryGoneError
	isTrue(t, errors.As(err, &gone), "want %T in the chain of %v", gone, err)
	isTrue(t, kv.WriteRefusal() != nil, "the refused advance did not poison the store")
	equal(t, []string(nil), calls.recorded(), "the advance wrote the marker of a directory moved aside")
}

// TestEveryCallOfAnAdvanceCrashedOrFailedLeavesTheOldMarkerOrTheNew is the crash sweep of an advance
// (review R-2 items 5, 6 and 7, R2 item 13): [GUARD at every call of an advance (the directory's
// fsync when a log was created, the slot's write, the fdatasync), a kill before the call, a power
// cut with the slot torn, and the call's failure each leave a store that opens at the marker before
// the advance or, only once the slot's write completed, the one after; a failure poisons the store
// and no later sync writes the marker again, and the sync reports success only after the
// fdatasync; red when the advance writes the slot holding the valid marker, retries, or reports
// success early].
func TestEveryCallOfAnAdvanceCrashedOrFailedLeavesTheOldMarkerOrTheNew(t *testing.T) {
	for _, newLog := range []bool{false, true} {
		calls := []string{callSlotWrite, callFdatasync}
		if newLog {
			calls = append([]string{callDirectoryFsync}, calls...)
		}
		for _, at := range calls {
			name := fmt.Sprintf("new log %v, at the %s", newLog, at)
			t.Run(name+", killed before it", func(t *testing.T) { sweepKill(t, newLog, at) })
			t.Run(name+", failed", func(t *testing.T) { sweepFailure(t, newLog, at, false) })
			if at == callSlotWrite {
				t.Run(name+", torn and failed", func(t *testing.T) { sweepFailure(t, newLog, at, true) })
			}
		}
	}
}

// sweepStore opens a store with one advance done ("old"), then a new log when asked, then commits
// "new" unsynced: the next sync advances the marker past "new".
func sweepStore(t *testing.T, newLog bool) (*BadgerKV, string, vouchedMarker) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "badger")
	kv, err := openVouchedTestStore(dir, vouchedTestTuning)
	noErr(t, err)
	setDurably(t, kv, "old")
	old := markerOnDisk(t, dir)
	if newLog {
		rotateTheMemtable(t, kv, dir, "fill")
		waitForTheFlushes(t, dir)
	}
	setUnsynced(t, kv, "new")
	return kv, dir, old
}

// requireOpensAt opens the image dir and requires the cut at want: "old" always there, "new" only
// when the marker vouches it.
func requireOpensAt(t *testing.T, dir string, want vouchedMarker, newVouched bool) {
	t.Helper()
	kv, err := openVouchedTestStore(dir, vouchedTestTuning)
	noErr(t, err, "the image did not open")
	defer func() { noErr(t, kv.Close()) }()
	equal(t, want.version, kv.VouchedOpenReport().Vouched, "the image's cut")
	isTrue(t, storeHas(t, kv, "old"), "the commit the marker vouched before the advance is gone")
	isTrue(t, storeHas(t, kv, "new") == newVouched, "the image holds \"new\": %v, want %v", !newVouched, newVouched)
}

func sweepKill(t *testing.T, newLog bool, at string) {
	kv, dir, old := sweepStore(t, newLog)
	defer func() { noErr(t, kv.Close()) }()
	var kill storeimage.Capture
	var beforeWrite []byte
	var captureErr error
	calls := installMarkerCalls(t, func(call string) error {
		var err error
		if call == callSlotWrite {
			beforeWrite, err = os.ReadFile(filepath.Join(dir, vouchedFileName))
		}
		if call == at && err == nil {
			// The kill: every file as the page cache holds it right before the call.
			kill, err = storeimage.Take(dir)
		}
		if err != nil && captureErr == nil {
			captureErr = err
		}
		return nil
	})
	noErr(t, kv.Sync())
	noErr(t, captureErr)
	isTrue(t, kill != nil, "SCENARIO NOT REACHED: the call %q was not made (calls: %v)", at, calls.recorded())
	// The slot's write is in the page cache before its fdatasync: a kill there keeps it.
	newVouched := at == callFdatasync
	image := filepath.Join(t.TempDir(), "kill")
	noErr(t, storeimage.Write(image, kill))
	want := old
	if newVouched {
		want = markerOnDisk(t, dir)
	}
	requireOpensAt(t, image, want, newVouched)

	if at != callFdatasync {
		return
	}
	// A power cut before the fdatasync completed: the slot's write torn, its first half on the disk.
	written := kill[vouchedFileName]
	lo, hi := tornRegion(t, beforeWrite, written)
	torn := bytes.Clone(written)
	copy(torn[lo+(hi-lo)/2:hi], beforeWrite[lo+(hi-lo)/2:hi])
	powerCut := storeimage.Capture{}
	for name, content := range kill {
		powerCut[name] = content
	}
	powerCut[vouchedFileName] = torn
	cut := filepath.Join(t.TempDir(), "power-cut")
	noErr(t, storeimage.Write(cut, powerCut))
	requireOpensAt(t, cut, old, false)
}

func sweepFailure(t *testing.T, newLog bool, at string, torn bool) {
	kv, dir, old := sweepStore(t, newLog)
	defer func() { _ = kv.Close() }() // a poisoned store's close is not under test
	before, err := os.ReadFile(filepath.Join(dir, vouchedFileName))
	noErr(t, err)
	injected := errors.New("an injected failure of the " + at)
	failed := false
	calls := installMarkerCalls(t, func(call string) error {
		if call != at || failed {
			return nil
		}
		failed = true
		return injected
	})
	if torn {
		restore := writeMarkerSlot
		writeMarkerSlot = func(file *os.File, slot []byte, offset int64) error {
			if err := calls.call(callSlotWrite); err != nil {
				// Half the slot's 40 bytes reach the file, then the write fails.
				if _, writeErr := file.WriteAt(slot[:vouchedSlotPayload/2], offset); writeErr != nil {
					return errors.Join(err, writeErr)
				}
				return err
			}
			_, err := file.WriteAt(slot, offset)
			return err
		}
		t.Cleanup(func() { writeMarkerSlot = restore })
	}
	err = kv.Sync()
	isTrue(t, errors.Is(err, injected), "want the injected failure in the chain of %v", err)
	isTrue(t, kv.WriteRefusal() != nil, "a failed %s did not poison the store", at)
	callsAfterFailure := len(calls.recorded())
	isTrue(t, kv.Sync() != nil, "a poisoned store reported a sync")
	setDurablyFails := kv.NewBatch()
	noErr(t, setDurablyFails.Set([]byte("later"), []byte("after the failure")))
	isTrue(t, setDurablyFails.Commit(true) != nil, "a poisoned store took a synced commit")
	equal(t, callsAfterFailure, len(calls.recorded()), "the advance was retried after its %s failed", at)

	m, found, err := readVouchedMarker(dir)
	noErr(t, err, "the marker after a failed %s", at)
	isTrue(t, found, "the marker is gone")
	// A failed fdatasync leaves the slot's write in the page cache, so a kill keeps the advance:
	// Badger's sync had succeeded, so the marker vouches only durable commits, and the caller was
	// still told the sync failed (I-6 is about the report, the disk may hold either slot). Before
	// the slot's write completed, only the old marker can be read.
	newVouched := at == callFdatasync && m != old
	if !newVouched {
		equal(t, old, m, "the marker after a failed %s", at)
	}
	after, err := os.ReadFile(filepath.Join(dir, vouchedFileName))
	noErr(t, err)
	if at == callDirectoryFsync {
		equal(t, before, after, "a failed directory fsync was followed by the slot's write")
	} else {
		chosen, err := decodeVouchedFile("VOUCHED", before)
		noErr(t, err)
		equal(t, before[chosen.slot*vouchedSlotSize:(chosen.slot+1)*vouchedSlotSize],
			after[chosen.slot*vouchedSlotSize:(chosen.slot+1)*vouchedSlotSize], "the failed advance touched the valid slot")
	}
	// The image as the failure left it opens at the marker it holds; "new" is otherwise the node
	// log's to replay.
	image, err := storeimage.Take(dir)
	noErr(t, err)
	imageDir := filepath.Join(t.TempDir(), "image")
	noErr(t, storeimage.Write(imageDir, image))
	requireOpensAt(t, imageDir, m, newVouched)
}

// tornFirstAdvance makes the first advance of kv fail with its slot torn at the middle of its 40
// bytes, as a power cut in the write leaves it, and returns the marker file's bytes after it.
func tornFirstAdvance(t *testing.T, kv *BadgerKV, dir string) []byte {
	t.Helper()
	restore := writeMarkerSlot
	t.Cleanup(func() { writeMarkerSlot = restore })
	writeMarkerSlot = func(file *os.File, slot []byte, offset int64) error {
		if _, err := file.WriteAt(slot[:vouchedSlotPayload/2], offset); err != nil {
			return err
		}
		return errors.New("the power was cut in the slot's write")
	}
	isTrue(t, kv.Sync() != nil, "SCENARIO NOT REACHED: the torn advance reported success")
	writeMarkerSlot = restore
	raw, err := os.ReadFile(filepath.Join(dir, vouchedFileName))
	noErr(t, err)
	file, err := decodeVouchedFile("VOUCHED", raw)
	noErr(t, err)
	equal(t, vouchedFile{marker: vouchedMarker{version: 0, floor: 1}, sequence: 1, slot: 0, otherNotWhole: "it fails its checksum"},
		file,
		"SCENARIO NOT REACHED: the torn first advance did not leave the first start's slot")
	return raw
}

// takeImageBetweenCompactions takes an image of a running store in which every table the copied
// MANIFEST names is present. storeimage.Take copies the MANIFEST and then the files one by one,
// and passes over a file removed meanwhile; a compaction that finishes during the copy removes
// tables the copied MANIFEST still names, and the image is then one no power cut leaves (the store
// rightly refuses it: "its MANIFEST names a table file the directory does not hold"). Table files
// are never renamed or reused, so an image taken while the directory's set of tables did not
// change holds them all; the copy is repeated until one is.
func takeImageBetweenCompactions(t *testing.T, dir string) storeimage.Capture {
	t.Helper()
	tableNames := func() []string {
		paths, err := filepath.Glob(filepath.Join(dir, "*.sst"))
		noErr(t, err)
		for i, path := range paths {
			paths[i] = filepath.Base(path)
		}
		sort.Strings(paths)
		return paths
	}
	deadline := time.Now().Add(90 * time.Second)
	for {
		before := tableNames()
		image, err := storeimage.Take(dir)
		noErr(t, err)
		copied := make([]string, 0, len(before))
		for name := range image {
			if strings.HasSuffix(name, ".sst") {
				copied = append(copied, name)
			}
		}
		sort.Strings(copied)
		if slices.Equal(before, copied) && slices.Equal(before, tableNames()) {
			return image
		}
		isTrue(t, time.Now().Before(deadline), "SCENARIO NOT REACHED: no image between two compactions in 90 s")
	}
}

// TestAFallbackToVersionZeroOpens is review R2 item 19: [GUARD a store whose first advance was torn
// falls back to the first start's slot, version 0, and opens: with a table its MANIFEST names, the
// table is kept (the cut never cuts a table); with a table the MANIFEST does not name, the table is
// set aside (R-3: a marker that vouches nothing loses nothing by it); red when either refuses].
func TestAFallbackToVersionZeroOpens(t *testing.T) {
	t.Run("a table the MANIFEST names", func(t *testing.T) {
		running := filepath.Join(t.TempDir(), "badger")
		kv, err := openVouchedTestStore(running, vouchedTestTuning)
		noErr(t, err)
		defer func() { _ = kv.Close() }() // poisoned by the torn advance; its close is not under test
		deadline := time.Now().Add(90 * time.Second)
		for batch := 0; kv.TableCount() == 0; batch++ {
			isTrue(t, time.Now().Before(deadline), "SCENARIO NOT REACHED: no flush in 90 s")
			noErr(t, commitFill(kv, fmt.Sprintf("fill-%04d", batch)))
		}
		waitForTheFlushes(t, running)
		tornFirstAdvance(t, kv, running)
		// The store stays open: the torn advance poisoned it, and a close would flush the memtable
		// a power cut leaves in its log.
		image := takeImageBetweenCompactions(t, running)
		imageDir := filepath.Join(t.TempDir(), "image")
		noErr(t, storeimage.Write(imageDir, image))
		reopened, err := openVouchedTestStore(imageDir, vouchedTestTuning)
		noErr(t, err, "a store whose first advance was torn, with a named table, did not open")
		defer func() { noErr(t, reopened.Close()) }()
		equal(t, uint64(0), reopened.VouchedOpenReport().Vouched, "the cut")
		isTrue(t, reopened.TableCount() > 0, "the named table is gone")
		isTrue(t, storeHas(t, reopened, "fill-0000-00"), "the named table's first commit is gone")
	})

	t.Run("a table the MANIFEST does not name", func(t *testing.T) {
		var image crashPointImage
		for _, candidate := range driveFirstFlushUnsynced(t) {
			if candidate.step == badger.FlushTableSynced && candidate.kind == "power cut" {
				image = candidate
			}
		}
		isTrue(t, image.files != nil, "SCENARIO NOT REACHED: no power-cut image at %q", badger.FlushTableSynced)
		// The torn first advance of another store: the bytes a power cut in that write leaves.
		scratch := filepath.Join(t.TempDir(), "scratch")
		other, err := openVouchedTestStore(scratch, vouchedTestTuning)
		noErr(t, err)
		setUnsynced(t, other, "a commit")
		torn := tornFirstAdvance(t, other, scratch)
		_ = other.Close() // poisoned; its close is not under test
		dir := filepath.Join(t.TempDir(), "image")
		writePrefixImage(t, dir, image.files)
		noErr(t, os.WriteFile(filepath.Join(dir, vouchedFileName), torn, 0o600))
		isTrue(t, len(tableFiles(t, dir)) == 1, "SCENARIO NOT REACHED: the image holds no unnamed table")
		kv, err := openVouchedTestStore(dir, prefixTuning)
		noErr(t, err, "a store whose first advance was torn, with an unnamed table, did not open")
		equal(t, uint64(0), kv.VouchedOpenReport().Vouched, "the cut")
		noErr(t, kv.Close())
	})
}
