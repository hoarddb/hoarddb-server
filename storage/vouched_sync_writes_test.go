package storage

import (
	"path/filepath"
	"testing"
)

// TestTheRingStoreRunsWithSyncWritesOff pins a precondition of Badger change 13 (BUG-0827, slice 2):
// with SyncWrites on, Badger's valueLog.sync returns at once without an msync and valueLog.write
// only logs a failed msync of its own, so the flush's value-log sync and the kept failure would
// both be skipped silently. The ring store (OpenVouchedBadgerKV) and the legacy one (OpenBadgerKV)
// must open with it off; turning it on is a decision that has to revisit change 13 first.
func TestTheRingStoreRunsWithSyncWritesOff(t *testing.T) {
	ring, err := openVouchedTestStore(filepath.Join(t.TempDir(), "badger"), BadgerTuning{MemTableMB: 8})
	noErr(t, err)
	defer func() { noErr(t, ring.Close()) }()
	isTrue(t, !ring.db.Opts().SyncWrites, "the ring store opened with SyncWrites on: Badger change 13's value-log "+
		"sync at a flush is skipped under it (valueLog.sync returns without an msync)")

	legacy, err := OpenBadgerKV(filepath.Join(t.TempDir(), "legacy"), BadgerTuning{MemTableMB: 8}, nil)
	noErr(t, err)
	defer func() { noErr(t, legacy.Close()) }()
	isTrue(t, !legacy.db.Opts().SyncWrites, "the legacy store opened with SyncWrites on: Badger change 13's value-log "+
		"sync at a flush is skipped under it")
}
