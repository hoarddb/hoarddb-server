package storage

import (
	"errors"
	"strings"
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v4"
)

// TestAWriteBehindADamagedLevelZeroTableIsRefusedWithTheRepair is BUG-0381's
// write half at the node store: a write that used to wait forever for room
// behind a level 0 stalled by a damaged table is refused with an error that
// names the file and says to restore the node, and the store reports it as its
// write refusal (HEALTH_CHECK and /ready read that).
func TestAWriteBehindADamagedLevelZeroTableIsRefusedWithTheRepair(t *testing.T) {
	dir := t.TempDir()
	payloads := bug0381Payloads()

	seedOpts, err := bug0381BadgerOptions(dir, 0)
	if err != nil {
		t.Fatalf("building the seed options: %v", err)
	}
	seedDB, err := badger.Open(seedOpts)
	if err != nil {
		t.Fatalf("opening the store to seed tables: %v", err)
	}
	if err := bug0381SeedTables(seedDB, payloads); err != nil {
		t.Fatalf("seeding tables: %v", err)
	}
	if err := seedDB.Close(); err != nil {
		t.Fatalf("closing the seeded store: %v", err)
	}
	damagedTable := bug0381TableFiles(t, dir)[0]
	bug0381FlipDataBlock(t, damagedTable)

	runOpts, err := bug0381BadgerOptions(dir, 2)
	if err != nil {
		t.Fatalf("building the run options: %v", err)
	}
	runDB, err := badger.Open(runOpts.WithLogger(&bug0381RecordingLogger{}))
	if err != nil {
		t.Fatalf("reopening the damaged store: %v", err)
	}
	kv, err := newBadgerKV(runDB, dir)
	if err != nil {
		t.Fatalf("wrapping the store: %v", err)
	}
	defer func() { _ = kv.Close() }() // its error is the shutdown test's subject

	refused := make(chan error, 1)
	go func() {
		for index := 0; ; index++ {
			batch := kv.NewBatch()
			if err := batch.Set(bug0381Key(index%bug0381KeyUniverse), payloads[index%bug0381KeyUniverse]); err != nil {
				refused <- err
				return
			}
			if err := batch.Commit(false); err != nil {
				refused <- err
				return
			}
		}
	}()

	var writeErr error
	select {
	case writeErr = <-refused:
	case <-time.After(stalledCloseBound):
		t.Fatalf("a write behind a damaged level-0 table was not refused within %s", stalledCloseBound)
	}
	var refusal *StoreRefusesWritesError
	if !errors.As(writeErr, &refusal) {
		t.Fatalf("the write was refused with %T, not a StoreRefusesWritesError: %v", writeErr, writeErr)
	}
	for _, want := range []string{damagedTable, "damaged", "restore"} {
		if !strings.Contains(writeErr.Error(), want) {
			t.Errorf("the refusal does not contain %q: %v", want, writeErr)
		}
	}
	if strings.Contains(writeErr.Error(), "free space") {
		t.Errorf("the refusal blames the disk, which is not the cause: %v", writeErr)
	}
	if reported := kv.WriteRefusal(); reported == nil || !strings.Contains(reported.Error(), damagedTable) {
		t.Errorf("WriteRefusal = %v, want the refusal naming %s", reported, damagedTable)
	}
}
