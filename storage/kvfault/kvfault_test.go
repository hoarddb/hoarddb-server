package kvfault_test

import (
	"testing"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/storage/kvfault"
	"github.com/hoarddb/hoarddb-server/storage/kvtest"
)

func TestFaultStoreContract(t *testing.T) {
	kvtest.Run(t, func(t *testing.T) storage.KV {
		// A small bound keeps the oversize clause cheap in memory.
		store := kvfault.New()
		store.SetMaxBatchBytes(1 << 20)
		return store
	})
}

// TestFailNextReadOfArmsOneGetOfExactlyThatKey: the test-only read fault fails
// the next read of the armed key once, leaves another key alone, and does
// nothing when it was never armed. It is the knob the server's BUG-0583 tests
// use to fail a handler's pre-write store read.
func TestFailNextReadOfArmsOneGetOfExactlyThatKey(t *testing.T) {
	store := kvfault.New()
	batch := store.NewBatch()
	for _, key := range []string{"armed", "other"} {
		if err := batch.Set([]byte(key), []byte("v")); err != nil {
			t.Fatalf("writing %q: %v", key, err)
		}
	}
	if err := batch.Commit(true); err != nil {
		t.Fatalf("committing: %v", err)
	}

	// Unarmed: the read succeeds.
	snapshot := store.NewSnapshot()
	if _, err := snapshot.Get([]byte("armed")); err != nil {
		t.Fatalf("an unarmed read failed: %v", err)
	}
	snapshot.Discard()

	store.FailNextReadOf([]byte("armed"))
	snapshot = store.NewSnapshot()
	if _, err := snapshot.Get([]byte("armed")); err == nil {
		t.Fatal("the armed read succeeded, want the injected failure")
	}
	if value, err := snapshot.Get([]byte("armed")); err != nil || string(value) != "v" {
		t.Fatalf("the fault was not one-shot: the next read returned %q, %v", value, err)
	}
	if value, err := snapshot.Get([]byte("other")); err != nil || string(value) != "v" {
		t.Fatalf("the fault hit another key: the read returned %q, %v", value, err)
	}
	snapshot.Discard()
}
