package storage

import (
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v4"
)

// stalledCloseBound is how long Close of a stalled store may take. Closing a
// healthy store of this size takes milliseconds; the bound is generous so a
// loaded host does not fail it, and short enough that a hang fails the test
// instead of the package timeout.
const stalledCloseBound = 30 * time.Second

// TestClosingAStoreStalledBehindADamagedTableReturnsAndNamesIt is BUG-0381's
// shutdown half. A damaged data block in a level-0 table makes every
// compaction that includes it fail, level 0 reaches the stall limit, and the
// flush goroutine parks waiting for a compaction that cannot succeed. Close
// used to wait for that goroutine forever, so a node in that state could not be
// stopped. Close must return in bounded time and say which table is damaged and
// what to do.
func TestClosingAStoreStalledBehindADamagedTableReturnsAndNamesIt(t *testing.T) {
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

	var committed atomic.Int64
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		cursor := 0
		for {
			if bug0381WriteBatch(runDB, payloads, &cursor) != nil {
				return
			}
			committed.Add(1)
		}
	}()

	waitForTheStall(t, runDB, &committed)

	closed := make(chan error, 1)
	go func() { closed <- kv.Close() }()
	var closeErr error
	select {
	case closeErr = <-closed:
	case <-time.After(stalledCloseBound):
		t.Fatalf("Close of a store stalled behind a damaged level-0 table did not return within %s", stalledCloseBound)
	}

	if closeErr == nil {
		t.Fatalf("Close returned nil; it must report the damaged table")
	}
	for _, want := range []string{damagedTable, "restore"} {
		if !strings.Contains(closeErr.Error(), want) {
			t.Errorf("Close error %q does not contain %q", closeErr, want)
		}
	}
	if !errors.Is(closeErr, badger.ErrL0StalledByDamagedTable) {
		t.Errorf("Close error %q does not wrap ErrL0StalledByDamagedTable", closeErr)
	}
	select {
	case <-writerDone:
	case <-time.After(stalledCloseBound):
		t.Errorf("the blocked writer was not released by Close")
	}
}

// waitForTheStall returns once level 0 holds the stall limit and no batch has
// committed for two seconds, which is when the flush goroutine is parked.
func waitForTheStall(t *testing.T, db *badger.DB, committed *atomic.Int64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	lastCount, lastChange := int64(-1), time.Now()
	for time.Now().Before(deadline) {
		if count := committed.Load(); count != lastCount {
			lastCount, lastChange = count, time.Now()
		} else if bug0381L0Count(db) >= 6 && time.Since(lastChange) > 2*time.Second {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("the damaged store never stalled (level 0 holds %d tables)", bug0381L0Count(db))
}
