package storage

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// callWithin runs call on its own goroutine and reports whether it returned
// within limit and whether it panicked. A user of a closed store that blocks
// forever (BUG-0675) must fail the test, not hang the shard.
func callWithin(limit time.Duration, call func()) (returned, panicked bool) {
	finished := make(chan bool, 1)
	go func() {
		defer func() { finished <- recover() != nil }()
		call()
	}()
	select {
	case panicked = <-finished:
		return true, panicked
	case <-time.After(limit):
		return false, false
	}
}

// TestUsingAClosedStoreFailsInsteadOfPanicking pins BUG-0675: a store user
// that arrives after Close gets an error. Badger's Sync dereferences the
// memtable that Close released, so the call took the process down.
func TestUsingAClosedStoreFailsInsteadOfPanicking(t *testing.T) {
	kv, err := OpenBadgerKV(t.TempDir(), BadgerTuning{MemTableMB: 32}, nil)
	if err != nil {
		t.Fatalf("opening the store: %v", err)
	}
	if err := kv.Close(); err != nil {
		t.Fatalf("closing the store: %v", err)
	}

	uses := []struct {
		name string
		use  func() error
	}{
		{"Sync", kv.Sync},
		{"a synced batch commit", func() error {
			batch := kv.NewBatch()
			defer batch.Discard()
			if err := batch.Set([]byte("k"), []byte("v")); err != nil {
				return err
			}
			return batch.Commit(true)
		}},
		{"a snapshot read", func() error {
			snapshot := kv.NewSnapshot()
			defer snapshot.Discard()
			_, err := snapshot.Get([]byte("k"))
			if errors.Is(err, ErrKeyNotFound) {
				return nil // a closed store must not answer as if it were open and empty
			}
			return err
		}},
	}
	for _, use := range uses {
		t.Run(use.name, func(t *testing.T) {
			var useErr error
			returned, panicked := callWithin(5*time.Second, func() { useErr = use.use() })
			switch {
			case !returned:
				t.Fatalf("%s on a closed store did not return within 5s", use.name)
			case panicked:
				t.Fatalf("%s on a closed store panicked", use.name)
			case useErr == nil:
				t.Fatalf("%s on a closed store reported success", use.name)
			}
		})
	}
}

// TestCloseWhileWritersCommitLeavesNoHungUsers pins BUG-0675: Close while
// transactions are committing leaves Badger's commit watermark short of the
// last timestamp it issued, so a transaction opened afterwards waited forever.
// Each round closes a store under sixteen committing writers and then opens a
// snapshot, which must return.
func TestCloseWhileWritersCommitLeavesNoHungUsers(t *testing.T) {
	const rounds = 40
	hung := 0
	for round := 0; round < rounds; round++ {
		kv, err := OpenBadgerKV(t.TempDir(), BadgerTuning{MemTableMB: 32}, nil)
		if err != nil {
			t.Fatalf("round %d: opening the store: %v", round, err)
		}
		var stop atomic.Bool
		var writers sync.WaitGroup
		for writer := 0; writer < 16; writer++ {
			writers.Add(1)
			go func() {
				defer writers.Done()
				for i := 0; !stop.Load(); i++ {
					batch := kv.NewBatch()
					if err := batch.Set([]byte(fmt.Sprintf("w%d-%d", writer, i)), []byte("v")); err != nil {
						batch.Discard()
						return
					}
					if err := batch.Commit(false); err != nil {
						return
					}
				}
			}()
		}
		time.Sleep(30 * time.Millisecond)
		if err := kv.Close(); err != nil {
			t.Fatalf("round %d: closing the store: %v", round, err)
		}
		if returned, _ := callWithin(3*time.Second, func() { kv.NewSnapshot().Discard() }); !returned {
			hung++
		}
		stop.Store(true)
	}
	if hung > 0 {
		t.Fatalf("%d of %d snapshots opened after Close never returned", hung, rounds)
	}
}

// TestAClosedStoreRefusesWithErrStoreClosed pins the error a late user sees:
// one that names the shutdown and matches ErrStoreClosed, on every entry
// point, so a caller can tell it from a damaged store.
func TestAClosedStoreRefusesWithErrStoreClosed(t *testing.T) {
	kv, err := OpenBadgerKV(t.TempDir(), BadgerTuning{MemTableMB: 32}, nil)
	if err != nil {
		t.Fatalf("opening the store: %v", err)
	}
	openBatch := kv.NewBatch()
	if err := openBatch.Set([]byte("before"), []byte("v")); err != nil {
		t.Fatalf("setting a key before Close: %v", err)
	}
	if err := kv.Close(); err != nil {
		t.Fatalf("closing the store: %v", err)
	}

	if err := openBatch.Commit(true); !errors.Is(err, ErrStoreClosed) {
		t.Errorf("a batch opened before Close, committed after: %v; want ErrStoreClosed", err)
	}
	if err := kv.Sync(); !errors.Is(err, ErrStoreClosed) {
		t.Errorf("Sync after Close: %v; want ErrStoreClosed", err)
	}
	lateBatch := kv.NewBatch()
	if err := lateBatch.Set([]byte("k"), []byte("v")); !errors.Is(err, ErrStoreClosed) {
		t.Errorf("Set on a batch opened after Close: %v; want ErrStoreClosed", err)
	}
	lateSnapshot := kv.NewSnapshot()
	if _, err := lateSnapshot.Get([]byte("k")); !errors.Is(err, ErrStoreClosed) {
		t.Errorf("Get on a snapshot opened after Close: %v; want ErrStoreClosed", err)
	}
	walk := lateSnapshot.NewIterator(IterOptions{})
	walk.Seek(nil)
	if walk.Valid() || !errors.Is(walk.Err(), ErrStoreClosed) {
		t.Errorf("a walk of a closed store: valid=%t err=%v; want not valid and ErrStoreClosed", walk.Valid(), walk.Err())
	}
	walk.Close()
	lateSnapshot.Discard()
}

// TestEveryAcknowledgedCommitSurvivesACloseUnderLoad pins the data-safety half
// of BUG-0675: a synced commit that returned nil before or while Close ran is
// in the store after a reopen, and one that Close cut short returned an error
// and so was never acknowledged.
func TestEveryAcknowledgedCommitSurvivesACloseUnderLoad(t *testing.T) {
	dir := t.TempDir()
	tuning := BadgerTuning{MemTableMB: 32}
	kv, err := OpenBadgerKV(dir, tuning, nil)
	if err != nil {
		t.Fatalf("opening the store: %v", err)
	}
	var acknowledged sync.Map
	var writers sync.WaitGroup
	for writer := 0; writer < 8; writer++ {
		writers.Add(1)
		go func() {
			defer writers.Done()
			for i := 0; ; i++ {
				key := fmt.Sprintf("w%d-%d", writer, i)
				batch := kv.NewBatch()
				if err := batch.Set([]byte(key), []byte("v")); err != nil {
					batch.Discard()
					return
				}
				if err := batch.Commit(true); err != nil {
					return
				}
				acknowledged.Store(key, true)
			}
		}()
	}
	time.Sleep(100 * time.Millisecond)
	if err := kv.Close(); err != nil {
		t.Fatalf("closing the store under load: %v", err)
	}
	writers.Wait()

	reopened, err := OpenBadgerKV(dir, tuning, nil)
	if err != nil {
		t.Fatalf("reopening the store: %v", err)
	}
	defer func() { logClose(t, "the reopened store", reopened.Close()) }()
	snapshot := reopened.NewSnapshot()
	defer snapshot.Discard()
	count, lost := 0, 0
	acknowledged.Range(func(key, _ any) bool {
		count++
		if _, err := snapshot.Get([]byte(key.(string))); err != nil {
			lost++
		}
		return true
	})
	if count == 0 {
		t.Fatal("no commit was acknowledged before Close; the test proved nothing")
	}
	if lost > 0 {
		t.Fatalf("%d of %d acknowledged commits are missing after a reopen", lost, count)
	}
}

// TestDiscardingTransactionsOpenedBeforeCloseDoesNotHang pins BUG-0681: a
// batch or snapshot opened before Close and discarded after it hung once more
// than about a hundred were pending, because Badger's watermark channel has no
// reader after Close.
func TestDiscardingTransactionsOpenedBeforeCloseDoesNotHang(t *testing.T) {
	const pending = 300
	opens := []struct {
		name    string
		open    func(kv *BadgerKV) func()
		discard string
	}{
		{"batches", func(kv *BadgerKV) func() { return kv.NewBatch().Discard }, "Discard of a batch"},
		{"snapshots", func(kv *BadgerKV) func() { return kv.NewSnapshot().Discard }, "Discard of a snapshot"},
	}
	for _, opened := range opens {
		t.Run(opened.name, func(t *testing.T) {
			kv, err := OpenBadgerKV(t.TempDir(), BadgerTuning{MemTableMB: 32}, nil)
			if err != nil {
				t.Fatalf("opening the store: %v", err)
			}
			discards := make([]func(), pending)
			for i := range discards {
				discards[i] = opened.open(kv)
			}
			if err := kv.Close(); err != nil {
				t.Fatalf("closing the store: %v", err)
			}
			returned, panicked := callWithin(5*time.Second, func() {
				for _, discard := range discards {
					discard()
				}
			})
			switch {
			case !returned:
				t.Fatalf("%s of %d opened before Close did not return within 5s after it", opened.discard, pending)
			case panicked:
				t.Fatalf("%s opened before Close panicked after it", opened.discard)
			}
		})
	}
}
