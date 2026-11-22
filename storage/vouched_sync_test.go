package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/internal/storeimage"

	badger "github.com/dgraph-io/badger/v4"
)

func openVouchedForTest(t *testing.T) (*BadgerKV, string) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "badger")
	kv, err := openVouchedTestStore(dir, vouchedTestTuning)
	noErr(t, err)
	t.Cleanup(func() { noErr(t, kv.Close()) })
	return kv, dir
}

// versionOfKey returns the Badger version key is visible at.
func versionOfKey(t *testing.T, kv *BadgerKV, key string) uint64 {
	t.Helper()
	var version uint64
	noErr(t, kv.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return fmt.Errorf("reading %s: %w", key, err)
		}
		version = item.Version()
		return nil
	}))
	return version
}

// memtableLogs lists the memtable logs of dir. It returns an error rather than failing the
// test, because the sync hook that calls it must not exit the goroutine inside a sync.
func memtableLogs(dir string) (map[string]bool, error) {
	logs := map[string]bool{}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("listing %s: %w", dir, err)
	}
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".mem") {
			logs[entry.Name()] = true
		}
	}
	return logs, nil
}

// TestTheMarkerMovesOnlyOnAReportedSync is T9: the marker moves only when a sync reported
// success, after Badger's sync returned and before the success is reported, and never covers
// a commit that returned after the version was read (I-4, I-6).
func TestTheMarkerMovesOnlyOnAReportedSync(t *testing.T) {
	t.Run("(a) a failed sync and a poisoned store leave the file", func(t *testing.T) {
		kv, dir := openVouchedForTest(t)
		setDurably(t, kv, "before")
		before, err := os.ReadFile(filepath.Join(dir, vouchedFileName))
		noErr(t, err)
		setUnsynced(t, kv, "after")
		FailBadgerSync(kv, errors.New("an injected sync failure"))
		isTrue(t, kv.Sync() != nil, "want an error")
		isTrue(t, kv.Sync() != nil, "a poisoned store reported a sync")
		after, err := os.ReadFile(filepath.Join(dir, vouchedFileName))
		noErr(t, err)
		equal(t, before, after, "a failed sync moved the marker")
	})

	t.Run("(a) a sync during which another failed leaves the file", func(t *testing.T) {
		kv, dir := openVouchedForTest(t)
		setDurably(t, kv, "before")
		before, err := os.ReadFile(filepath.Join(dir, vouchedFileName))
		noErr(t, err)
		setUnsynced(t, kv, "after")
		PoisonDuringNextSync(kv, errors.New("another commit's sync failed meanwhile"))
		isTrue(t, kv.Sync() != nil, "a sync during which the store was poisoned reported success")
		after, err := os.ReadFile(filepath.Join(dir, vouchedFileName))
		noErr(t, err)
		equal(t, before, after, "the marker moved although the poison was set before the sync reported")
	})

	t.Run("(b) a failed marker write poisons and returns ErrCommittedNotSynced", func(t *testing.T) {
		kv, _ := openVouchedForTest(t)
		setDurably(t, kv, "before")
		restore := syncMarkerFile
		t.Cleanup(func() { syncMarkerFile = restore })
		syncMarkerFile = func(*os.File) error { return errors.New("an injected fdatasync failure of the marker") }
		batch := kv.NewBatch()
		noErr(t, batch.Set([]byte("k"), []byte("v")))
		err := batch.Commit(true)
		isTrue(t, errors.Is(err, ErrCommittedNotSynced), "want %v in the chain of %v", ErrCommittedNotSynced, err)
		isTrue(t, kv.WriteRefusal() != nil, "a failed marker write did not poison the store")
	})

	t.Run("(c) two syncs that both succeed leave the higher version", func(t *testing.T) {
		kv, dir := openVouchedForTest(t)
		setUnsynced(t, kv, "first")
		real := kv.db.SyncVouched
		var calls atomic.Int32
		gate := make(chan struct{})
		var gateOnce sync.Once
		openGate := func() { gateOnce.Do(func() { close(gate) }) }
		// Registered after the store's cleanup, so it runs first: a failed test must not leave the
		// first sync parked with the store's lifecycle lock held, or Close would wait for good.
		t.Cleanup(openGate)
		kv.syncDB = func() (uint64, error) {
			version, err := real()
			if calls.Add(1) == 1 {
				<-gate
			}
			return version, err
		}
		firstDone := make(chan error, 1)
		go func() { firstDone <- kv.Sync() }()
		deadline := time.Now().Add(60 * time.Second)
		for calls.Load() == 0 {
			isTrue(t, time.Now().Before(deadline), "SCENARIO NOT REACHED: the first sync never reached Badger")
			time.Sleep(time.Millisecond)
		}
		setUnsynced(t, kv, "second")
		noErr(t, kv.Sync())
		higher := markerOnDisk(t, dir).version
		equal(t, versionOfKey(t, kv, "second"), higher, "SCENARIO NOT REACHED: the second sync did not vouch its commit")
		openGate()
		noErr(t, <-firstDone)
		equal(t, higher, markerOnDisk(t, dir).version, "the slower sync wrote its lower version over the higher one")
	})

	t.Run("(d) after Commit(true) an image taken at once keeps the commit", func(t *testing.T) {
		kv, dir := openVouchedForTest(t)
		setDurably(t, kv, "acknowledged")
		image, err := storeimage.Take(dir)
		noErr(t, err)
		imageDir := filepath.Join(t.TempDir(), "badger")
		noErr(t, storeimage.Write(imageDir, image))
		reopened, err := openVouchedTestStore(imageDir, vouchedTestTuning)
		noErr(t, err)
		defer func() { noErr(t, reopened.Close()) }()
		isTrue(t, storeHas(t, reopened, "acknowledged"), "a commit whose Commit(true) returned is gone after the cut")
	})

	t.Run("(e) the slot's write and its fdatasync happen after DB.Sync returned and before success", func(t *testing.T) {
		kv, _ := openVouchedForTest(t)
		setDurably(t, kv, "before")
		var mu sync.Mutex
		var events []string
		record := func(event string) {
			mu.Lock()
			defer mu.Unlock()
			events = append(events, event)
		}
		real := kv.db.SyncVouched
		kv.syncDB = func() (uint64, error) {
			version, err := real()
			record("DB.Sync returned")
			return version, err
		}
		restoreWrite, restoreSync, restoreDir := writeMarkerSlot, syncMarkerFile, syncDirectory
		t.Cleanup(func() { writeMarkerSlot, syncMarkerFile, syncDirectory = restoreWrite, restoreSync, restoreDir })
		writeMarkerSlot = func(file *os.File, slot []byte, offset int64) error {
			record("write of a marker slot")
			return restoreWrite(file, slot, offset)
		}
		syncMarkerFile = func(file *os.File) error {
			record("fdatasync of the marker file")
			return restoreSync(file)
		}
		syncDirectory = func(dirPath string) error {
			record("fsync of the store directory")
			return syncDirectoryPath(dirPath)
		}
		setUnsynced(t, kv, "after")
		noErr(t, kv.Sync())
		record("the sync reported success")
		mu.Lock()
		defer mu.Unlock()
		// No directory fsync: the first advance ("before") covered the files Open created, and
		// none was created since (the dirty-directory test has the other case).
		equal(t, []string{"DB.Sync returned", "write of a marker slot", "fdatasync of the marker file",
			"the sync reported success"}, events)
	})

	t.Run("(f) the version is read before the capture", func(t *testing.T) {
		kv, dir := openVouchedForTest(t)
		setDurably(t, kv, "early")
		early := versionOfKey(t, kv, "early")
		var once sync.Once
		var hookRan, rotated bool
		var late uint64
		var hookErr error
		restore := badger.SetSyncTestHooks(badger.SyncTestHooks{AfterCapture: func() {
			once.Do(func() {
				hookRan = true
				logsAtCapture, err := memtableLogs(dir)
				if err != nil {
					hookErr = err
					return
				}
				// Commits that fill the captured memtable until a new log appears, then the late
				// commit, which lands in a memtable this sync never captured.
				for batch := 0; batch < 200 && !rotated; batch++ {
					if hookErr = commitFill(kv, fmt.Sprintf("fill-%03d", batch)); hookErr != nil {
						return
					}
					logsNow, err := memtableLogs(dir)
					if err != nil {
						hookErr = err
						return
					}
					for name := range logsNow {
						if !logsAtCapture[name] {
							rotated = true
						}
					}
				}
				b := kv.NewBatch()
				if hookErr = b.Set([]byte("late"), []byte("after the capture")); hookErr != nil {
					return
				}
				if hookErr = b.Commit(false); hookErr != nil {
					return
				}
			})
		}})
		t.Cleanup(restore)
		noErr(t, kv.Sync())
		isTrue(t, hookRan, "SCENARIO NOT REACHED: SyncTestHooks.AfterCapture never ran")
		noErr(t, hookErr)
		isTrue(t, rotated, "SCENARIO NOT REACHED: the memtable did not rotate inside the hook")
		late = versionOfKey(t, kv, "late")
		marker := markerOnDisk(t, dir)
		isTrue(t, marker.version < late, "the marker vouches version %d, which covers the commit at version %d that "+
			"returned after the capture, in a memtable log this sync never synced", marker.version, late)
		isTrue(t, marker.version >= early, "the marker is below a commit that returned before the sync")
	})
}

// commitFill commits about 100 KiB under prefix in one unsynced batch (an 8 MiB memtable fills in about 80).
func commitFill(kv *BadgerKV, prefix string) error {
	batch := kv.NewBatch()
	for i := range 50 {
		if err := batch.Set([]byte(fmt.Sprintf("%s-%02d", prefix, i)), make([]byte, 2<<10)); err != nil {
			return err
		}
	}
	return batch.Commit(false)
}

// TestTheFloorSampleIsReadInOrder is T13 (b)'s storage half: the floor written with a version
// is the one sampled before that version was read. A hold released, and the synced sequence
// moved up, after the version was read (inside AfterCapture) must not raise the floor the
// marker carries with it, or a first write whose commit returned there would sit below the
// floor of a marker that does not vouch it.
func TestTheFloorSampleIsReadInOrder(t *testing.T) {
	kv, dir := openVouchedForTest(t)
	var floor atomic.Uint64
	floor.Store(5)
	kv.SetLogFloorSource(func() uint64 { return floor.Load() })
	setUnsynced(t, kv, "a write before the sync")
	var once sync.Once
	restore := badger.SetSyncTestHooks(badger.SyncTestHooks{AfterCapture: func() {
		once.Do(func() { floor.Store(50) })
	}})
	t.Cleanup(restore)
	noErr(t, kv.Sync())
	equal(t, uint64(50), floor.Load(), "SCENARIO NOT REACHED: the hook did not run")
	equal(t, uint64(5), markerOnDisk(t, dir).floor, "the marker carries a floor sampled after its version was read")
}

// TestTheMarkerAdvancesItsFloorAlone: [GUARD a sync whose version equals the marker's and whose
// sampled floor is higher writes the marker with that floor; red when the advance requires a
// higher version] (review R-8, mutation M-b). Today a lower floor only costs more replay; once
// retention truncates the log below the marker's floor (BUG-0829) it is what keeps the boot scan's
// start honest. A lower or equal floor at the same version writes nothing.
func TestTheMarkerAdvancesItsFloorAlone(t *testing.T) {
	kv, dir := openVouchedForTest(t)
	var floor atomic.Uint64
	floor.Store(3)
	kv.SetLogFloorSource(func() uint64 { return floor.Load() })
	setDurably(t, kv, "a")
	at := markerOnDisk(t, dir)
	equal(t, uint64(3), at.floor, "SCENARIO NOT REACHED: the synced commit's marker does not carry the sampled floor")
	_, advancesBefore, _ := kv.VouchedCounts()

	floor.Store(9)
	noErr(t, kv.Sync())
	after := markerOnDisk(t, dir)
	equal(t, at.version, after.version, "SCENARIO NOT REACHED: the version moved with no commit")
	equal(t, uint64(9), after.floor, "a sync at the same version with a higher floor did not advance the marker's floor")
	_, advances, _ := kv.VouchedCounts()
	equal(t, advancesBefore+1, advances, "the floor's advance was not counted")

	floor.Store(4)
	noErr(t, kv.Sync())
	equal(t, after, markerOnDisk(t, dir), "a lower floor at the same version moved the marker")
	_, unchanged, _ := kv.VouchedCounts()
	equal(t, advances, unchanged, "a lower floor at the same version wrote the marker")
}
