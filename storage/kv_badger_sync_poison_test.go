package storage_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/storage"
)

// errDiskSync is the failure FailBadgerSync injects.
var errDiskSync = errors.New("injected EIO on sync")

// TestASyncOnlyFailureIsACommittedBatchAndPoisonsTheStore (BUG-0809 follow-up): Commit(true)
// commits the transaction before it syncs, so a failed sync must say the batch is committed
// (ErrCommittedNotSynced), not that the commit failed, or a caller applies the batch again. The
// sync failure then refuses every later commit, as the write-ahead log refuses after its own
// failed fsync (BUG-0807): a later sync that succeeds cannot show the earlier batch is on disk.
// [BREAK: return the sync error unwrapped; BREAK: keep accepting commits after it]
func TestASyncOnlyFailureIsACommittedBatchAndPoisonsTheStore(t *testing.T) {
	kv, err := storage.OpenBadgerKV(t.TempDir(), storage.BadgerTuning{MemTableMB: 32}, nil)
	if err != nil {
		t.Fatalf("opening the Badger store: %v", err)
	}
	t.Cleanup(func() { _ = kv.Close() })
	storage.FailBadgerSync(kv, errDiskSync)

	batch := kv.NewBatch()
	if err := batch.Set([]byte("first"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	err = batch.Commit(true)
	if !errors.Is(err, storage.ErrCommittedNotSynced) || !errors.Is(err, errDiskSync) {
		t.Fatalf("Commit(true) with a failing sync answered %v; want it to wrap ErrCommittedNotSynced and the sync's error", err)
	}
	snapshot := kv.NewSnapshot()
	got, err := snapshot.Get([]byte("first"))
	snapshot.Discard()
	if err != nil || string(got) != "1" {
		t.Fatalf("the batch whose sync failed reads %q, %v; want it committed and readable", got, err)
	}

	later := kv.NewBatch()
	if err := later.Set([]byte("second"), []byte("2")); err != nil {
		t.Fatal(err)
	}
	err = later.Commit(false)
	var refused *storage.StoreRefusesWritesError
	var unsynced *storage.StoreSyncFailedError
	if !errors.As(err, &refused) || !errors.As(err, &unsynced) {
		t.Fatalf("a commit after the failed sync answered %v; want a StoreRefusesWritesError caused by the failed sync", err)
	}
	if !strings.Contains(err.Error(), "restart the node") {
		t.Fatalf("the refusal %q does not say how to recover", err)
	}
	snapshot = kv.NewSnapshot()
	_, err = snapshot.Get([]byte("second"))
	snapshot.Discard()
	if !errors.Is(err, storage.ErrKeyNotFound) {
		t.Fatalf("the refused batch was written: Get answered %v", err)
	}
	if refusal := kv.WriteRefusal(); !errors.As(refusal, &unsynced) {
		t.Fatalf("WriteRefusal answered %v after a failed sync; want the sync failure, so HEALTH_CHECK and /ready report it", refusal)
	}
}

// TestAFailedStoreSyncPoisonsTheStore: KV.Sync failing refuses later commits the same way.
func TestAFailedStoreSyncPoisonsTheStore(t *testing.T) {
	kv, err := storage.OpenBadgerKV(t.TempDir(), storage.BadgerTuning{MemTableMB: 32}, nil)
	if err != nil {
		t.Fatalf("opening the Badger store: %v", err)
	}
	t.Cleanup(func() { _ = kv.Close() })
	storage.FailBadgerSync(kv, errDiskSync)
	if err := kv.Sync(); !errors.Is(err, errDiskSync) {
		t.Fatalf("Sync answered %v, want the injected failure", err)
	}
	batch := kv.NewBatch()
	if err := batch.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	var unsynced *storage.StoreSyncFailedError
	if err := batch.Commit(false); !errors.As(err, &unsynced) {
		t.Fatalf("a commit after a failed Sync answered %v; want it refused with the sync failure", err)
	}
}

func openPoisonTestKV(t *testing.T) *storage.BadgerKV {
	t.Helper()
	kv, err := storage.OpenBadgerKV(t.TempDir(), storage.BadgerTuning{MemTableMB: 32}, nil)
	if err != nil {
		t.Fatalf("opening the Badger store: %v", err)
	}
	t.Cleanup(func() { _ = kv.Close() })
	return kv
}

// TestASyncAfterAFailedSyncNeverSucceeds (BUG-0809 round 2, S2): a write committed unsynced
// before a sync failed is waiting on a group Sync; a later Sync that Badger completes proves
// nothing about it, so Sync must keep refusing once the store is poisoned.
// [BREAK: Sync ignores the poison]
func TestASyncAfterAFailedSyncNeverSucceeds(t *testing.T) {
	kv := openPoisonTestKV(t)
	waiting := kv.NewBatch()
	if err := waiting.Set([]byte("waiting"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := waiting.Commit(false); err != nil {
		t.Fatal(err)
	}
	storage.FailBadgerSyncOnce(kv, errDiskSync)
	if err := kv.Sync(); !errors.Is(err, errDiskSync) {
		t.Fatalf("the failing Sync answered %v", err)
	}
	var unsynced *storage.StoreSyncFailedError
	if err := kv.Sync(); !errors.As(err, &unsynced) {
		t.Fatalf("a Sync after a failed one answered %v; want the sync failure, never nil", err)
	}
}

// TestASyncThatCompletesAfterAPoisonDoesNotReportSuccess (BUG-0809 round 2, S3): the poison is
// checked again after the sync, in Sync and in Commit(true), because a commit that passed the
// check just before another sync failed would otherwise be acknowledged.
// [BREAK: no re-check after the sync]
func TestASyncThatCompletesAfterAPoisonDoesNotReportSuccess(t *testing.T) {
	kv := openPoisonTestKV(t)
	storage.PoisonDuringNextSync(kv, errDiskSync)
	var unsynced *storage.StoreSyncFailedError
	if err := kv.Sync(); !errors.As(err, &unsynced) {
		t.Fatalf("a Sync that finished after a poison answered %v; want the sync failure", err)
	}

	kv = openPoisonTestKV(t)
	batch := kv.NewBatch()
	if err := batch.Set([]byte("racing"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	storage.PoisonDuringNextSync(kv, errDiskSync)
	err := batch.Commit(true)
	if !errors.Is(err, storage.ErrCommittedNotSynced) || !errors.As(err, &unsynced) {
		t.Fatalf("Commit(true) whose sync finished after a poison answered %v; want ErrCommittedNotSynced with the sync failure", err)
	}
}

// TestDropPrefixesRefusesOnAPoisonedStore: a prefix drop is a mutating call, refused like a commit.
func TestDropPrefixesRefusesOnAPoisonedStore(t *testing.T) {
	kv := openPoisonTestKV(t)
	storage.FailBadgerSync(kv, errDiskSync)
	_ = kv.Sync()
	var unsynced *storage.StoreSyncFailedError
	if err := kv.DropPrefixes([][]byte{[]byte("p")}); !errors.As(err, &unsynced) {
		t.Fatalf("DropPrefixes on a poisoned store answered %v; want the sync failure", err)
	}
}
