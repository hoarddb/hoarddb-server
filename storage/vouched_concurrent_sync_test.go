package storage

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Two store syncs can run at once (Badger's DB.SyncVouched takes a read lock), and each samples
// the log floor before it reads its version and outside vouchedState.mu. These tests put two of
// them in the orders that matter and check what the marker and the next open make of it.

// TestTwoConcurrentSyncsThatSampledTheirFloorsOutOfOrderLeaveAMarkerTheNextOpenReads: [GUARD a
// sync that sampled a lower floor before a concurrent sync sampled a higher one, and that writes
// the marker last with the higher version, writes the higher floor, so the newer slot never
// carries a floor below the older slot's and the next open after a clean Close succeeds; red
// while the advance writes its own sampled floor: the open refuses the store as damaged (review
// of S-3, F-1)]. The order: A samples floor 3 and stalls; B samples 7, syncs, reads its version
// and advances (V_B, 7); A then syncs, reads V_A > V_B and advances.
func TestTwoConcurrentSyncsThatSampledTheirFloorsOutOfOrderLeaveAMarkerTheNextOpenReads(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "badger")
	kv, err := openVouchedTestStore(dir, vouchedTestTuning)
	noErr(t, err)
	var mu sync.Mutex
	samples := 0
	aSampled := make(chan struct{})
	releaseA := make(chan struct{})
	kv.SetLogFloorSource(func() uint64 {
		mu.Lock()
		samples++
		sample := samples
		mu.Unlock()
		if sample == 1 {
			// A's sample is taken here, before it stalls; the value is the one read then.
			close(aSampled)
			<-releaseA
			return 3
		}
		return 7
	})
	setUnsynced(t, kv, "a")
	var aErr error
	var syncs sync.WaitGroup
	syncs.Add(1)
	go func() {
		defer syncs.Done()
		aErr = kv.Sync()
	}()
	<-aSampled
	setUnsynced(t, kv, "b")
	noErr(t, kv.Sync(), "sync B")
	afterB := markerOnDisk(t, dir)
	equal(t, uint64(7), afterB.floor, "SCENARIO NOT REACHED: B's advance does not carry its floor")
	setUnsynced(t, kv, "c")
	close(releaseA)
	syncs.Wait()
	noErr(t, aErr, "sync A")
	afterA := markerOnDisk(t, dir)
	isTrue(t, afterA.version > afterB.version, "SCENARIO NOT REACHED: A did not advance the version past B's (%+v after %+v)",
		afterA, afterB)
	equal(t, uint64(7), afterA.floor, "A's advance lowered the floor B's had written")
	noErr(t, kv.Close())

	reopened, err := openVouchedTestStore(dir, vouchedTestTuning)
	noErr(t, err, "the store two concurrent syncs left, closed cleanly, did not open")
	equal(t, uint64(7), reopened.VouchedFloor(), "the reopened store's floor")
	noErr(t, reopened.Close())
}

// blockFirstSlotWrite makes the next marker slot write wait, holding vouchedState.mu, until the
// returned release is called; inWrite is closed once it waits. Every later write goes through.
func blockFirstSlotWrite(t *testing.T, slotWrites *atomic.Int64) (inWrite <-chan struct{}, release func()) {
	t.Helper()
	entered, released := make(chan struct{}), make(chan struct{})
	var once sync.Once
	restore := writeMarkerSlot
	t.Cleanup(func() { writeMarkerSlot = restore })
	writeMarkerSlot = func(file *os.File, slot []byte, offset int64) error {
		slotWrites.Add(1)
		once.Do(func() { close(entered); <-released })
		return restore(file, slot, offset)
	}
	return entered, func() { close(released) }
}

// requirePoisonedWith checks that err is the store's sync poison and carries cause.
func requirePoisonedWith(t *testing.T, err, cause error, who string) {
	t.Helper()
	var poison *StoreSyncFailedError
	isTrue(t, errors.As(err, &poison), "%s returned %v, not the store's sync poison", who, err)
	isTrue(t, errors.Is(err, cause), "%s's poison does not carry the failure that set it: %v", who, err)
}

// TestASyncQueuedBehindAFailedMarkerAdvanceReturnsThePoison: [GUARD a sync that passed its poison
// check while another sync's marker advance was in flight, and then waited for the marker's mutex,
// returns the poison that advance's failure set and writes nothing; red while the poison is read
// only before the mutex: the queued sync advances the marker after the failure, so the failed
// advance is retried (review of S-3, F-2: "A returned the error, B returned nil, syncMarkerFile
// called twice")].
func TestASyncQueuedBehindAFailedMarkerAdvanceReturnsThePoison(t *testing.T) {
	kv, _ := openVouchedForTest(t)
	setDurably(t, kv, "warm")
	var slotWrites, markerSyncs atomic.Int64
	inWrite, releaseA := blockFirstSlotWrite(t, &slotWrites)
	injected := errors.New("injected fdatasync failure")
	restoreSync := syncMarkerFile
	t.Cleanup(func() { syncMarkerFile = restoreSync })
	syncMarkerFile = func(file *os.File) error {
		if markerSyncs.Add(1) == 1 {
			return injected
		}
		return restoreSync(file)
	}
	bSynced := make(chan struct{})
	realSync := kv.syncDB
	var badgerSyncs atomic.Int64
	kv.syncDB = func() (uint64, error) {
		version, err := realSync()
		if badgerSyncs.Add(1) == 2 {
			close(bSynced)
		}
		return version, err
	}

	setUnsynced(t, kv, "a")
	var aErr, bErr error
	var syncs sync.WaitGroup
	syncs.Add(1)
	go func() { defer syncs.Done(); aErr = kv.Sync() }()
	<-inWrite
	setUnsynced(t, kv, "b")
	syncs.Add(1)
	go func() { defer syncs.Done(); bErr = kv.Sync() }()
	<-bSynced
	// B's Badger sync has returned and nothing is poisoned yet, so B passes the check before
	// the mutex; the pause lets it reach the mutex A holds.
	time.Sleep(100 * time.Millisecond)
	releaseA()
	syncs.Wait()

	isTrue(t, errors.Is(aErr, injected), "SCENARIO NOT REACHED: A's advance did not fail: %v", aErr)
	requirePoisonedWith(t, bErr, injected, "the sync queued behind the failed advance")
	equal(t, int64(1), slotWrites.Load(), "slot writes: the failed advance was retried")
	equal(t, int64(1), markerSyncs.Load(), "marker fdatasyncs: the failed advance was retried")
}

// TestASyncQueuedOnTheMarkerWhileAnotherBadgerSyncFailedReturnsThePoison: [GUARD a sync whose own
// Badger sync succeeded, that passed its poison check and waits for the marker's mutex while a
// third sync's Badger sync fails and poisons the store, returns that poison and writes nothing:
// the failed Badger sync may have covered a write this one's success does not prove on disk
// (write_refusal.go); red while the poison is read only before the mutex (review of S-3, F-2,
// the Badger-sync window)]. C holds the mutex in its slot write; B queues; A's Badger sync fails.
func TestASyncQueuedOnTheMarkerWhileAnotherBadgerSyncFailedReturnsThePoison(t *testing.T) {
	kv, _ := openVouchedForTest(t)
	setDurably(t, kv, "warm")
	var slotWrites atomic.Int64
	inWrite, releaseC := blockFirstSlotWrite(t, &slotWrites)
	injected := errors.New("injected Badger sync failure")
	bSynced := make(chan struct{})
	realSync := kv.syncDB
	var badgerSyncs atomic.Int64
	kv.syncDB = func() (uint64, error) {
		switch badgerSyncs.Add(1) {
		case 2:
			version, err := realSync()
			close(bSynced)
			return version, err
		case 3:
			return 0, injected
		}
		return realSync()
	}

	setUnsynced(t, kv, "c")
	var bErr, cErr error
	var syncs sync.WaitGroup
	syncs.Add(1)
	go func() { defer syncs.Done(); cErr = kv.Sync() }()
	<-inWrite
	setUnsynced(t, kv, "b")
	syncs.Add(1)
	go func() { defer syncs.Done(); bErr = kv.Sync() }()
	<-bSynced
	time.Sleep(100 * time.Millisecond)
	aErr := kv.Sync()
	isTrue(t, errors.Is(aErr, injected), "SCENARIO NOT REACHED: A's Badger sync did not fail: %v", aErr)
	releaseC()
	syncs.Wait()

	noErr(t, cErr, "C held the mutex before the failure and its own advance succeeded")
	requirePoisonedWith(t, bErr, injected, "the sync queued while a Badger sync failed")
	equal(t, int64(1), slotWrites.Load(), "slot writes: the queued sync advanced the marker after the failure")
}
