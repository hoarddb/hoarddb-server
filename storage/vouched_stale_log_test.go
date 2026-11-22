package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v4"
)

// TestADeleteInAFlushedTableStaysDeletedWhenItsStaleLogIsReplayed is the deterministic form of a
// failure T10 (TestStoreReopensAsAPrefix) hit in about 3 runs in 100 of one seed on a pool VM:
// "the second image reopened is not the prefix of its commits ... unexpected k/089 (held at
// version 15; set by 14@15, deleted by 34@35)". A deleted key was back after a power cut.
//
// How it happened. A memtable's log stayed on disk after its table was in the MANIFEST: for an
// instant always, and for as long as a reader held the memtable. Nothing synced that log at the
// flush, so after a power cut it could hold less than its table: here the set of a key, without
// the delete that followed. Meanwhile a compaction with nothing below dropped the delete marker
// together with the set it shadowed, as it may. At the next open the stale log was replayed, its
// entries at or below the vouched version were kept, and the set was visible again.
//
// BUG-0858, fixed in Badger change 12: a flush removes its memtable's log, durably, after the
// MANIFEST names the table and before the table is in a level, whatever references the memtable
// has. The image here is the store's files with the first log as of the last sync, if the log is
// still on disk (deleteStaysDeletedOverAFlushedLog).
func TestADeleteInAFlushedTableStaysDeletedWhenItsStaleLogIsReplayed(t *testing.T) {
	deleteStaysDeletedOverAFlushedLog(t,
		func(dir string) (*BadgerKV, error) { return openVouchedTestStore(dir, vouchedTestTuning) },
		func(kv *BadgerKV) string { return fmt.Sprintf("the open reported %+v", kv.VouchedOpenReport()) },
		logAsOfTheSync)
}

// staleLogImage says which bytes of the first memtable log an image keeps, when the log is still
// on disk: the bytes as of the last sync (a power cut: its later pages were never written back),
// or every byte it holds now (a process kill: the page cache keeps them).
type staleLogImage int

const (
	logAsOfTheSync staleLogImage = iota
	logAsItIsNow
)

// deleteStaysDeletedOverAFlushedLog is the scenario of
// TestADeleteInAFlushedTableStaysDeletedWhenItsStaleLogIsReplayed, for any open of the store
// and either image of the first log. A key is set and synced, deleted, its memtable pinned by a
// reader and flushed, and a compaction to one level drops the delete marker with the set under
// it; the image is reopened and the key must stay deleted. open opens the store at a directory,
// and describe says what the reopen did, for the failure message.
func deleteStaysDeletedOverAFlushedLog(t *testing.T, open func(dir string) (*BadgerKV, error),
	describe func(*BadgerKV) string, imageOf staleLogImage) {
	t.Helper()
	running := filepath.Join(t.TempDir(), "running")
	kv, err := open(running)
	noErr(t, err, "opening the store")
	defer func() { _ = kv.Close() }() // a test's close; the assertion below is the result
	commit := func(synced bool, sets map[string][]byte, deletes ...string) {
		t.Helper()
		batch := kv.NewBatch()
		for _, key := range deletes {
			noErr(t, batch.Delete([]byte(key)), "deleting %s", key)
		}
		for key, value := range sets {
			noErr(t, batch.Set([]byte(key), value), "setting %s", key)
		}
		noErr(t, batch.Commit(synced), "committing")
	}

	commit(true, map[string][]byte{"doomed": []byte("set")})
	firstLog := filepath.Join(running, "00001.mem")
	logAtSync, err := os.ReadFile(firstLog)
	noErr(t, err, "reading the first memtable log as of the sync")
	commit(false, nil, "doomed")
	commit(false, map[string][]byte{"later": []byte("x")})

	pinTxn := kv.db.NewTransaction(false)
	pin := pinTxn.NewIterator(badger.DefaultIteratorOptions)
	defer func() {
		pin.Close()
		pinTxn.Discard()
	}()

	fill := make([]byte, 768<<10)
	for i := range fill {
		fill[i] = byte(i*31 + 7)
	}
	deadline := time.Now().Add(60 * time.Second)
	compacted := func() bool {
		for _, table := range kv.db.Tables() {
			if table.Level > 0 {
				return true
			}
		}
		return false
	}
	for i := 0; !compacted(); i++ {
		if time.Now().After(deadline) {
			t.Fatal("SCENARIO NOT REACHED: no table was flushed and compacted below level 0 in 60 s")
		}
		commit(false, map[string][]byte{fmt.Sprintf("fill/%03d", i%64): fill})
	}
	noErr(t, kv.db.Flatten(1), "compacting the tables to one level")

	var tablesHighest uint64
	for _, table := range kv.db.Tables() {
		tablesHighest = max(tablesHighest, table.MaxVersion)
	}
	if tablesHighest < 2 {
		t.Fatalf("SCENARIO NOT REACHED: the tables' highest version is %d, below the delete's", tablesHighest)
	}
	noErr(t, kv.db.View(func(txn *badger.Txn) error {
		all := txn.NewIterator(badger.IteratorOptions{AllVersions: true})
		defer all.Close()
		for all.Seek([]byte("doomed")); all.Valid() && string(all.Item().Key()) == "doomed"; all.Next() {
			return fmt.Errorf("SCENARIO NOT REACHED: the running store still holds \"doomed\" at version %d: "+
				"the compaction did not drop its delete marker", all.Item().Version())
		}
		return nil
	}), "looking for a version of the deleted key in the running store")

	image := filepath.Join(t.TempDir(), "image")
	copyStoreFiles(t, running, image)
	stat, statErr := os.Stat(firstLog)
	logOnDisk := statErr == nil
	if logOnDisk && imageOf == logAsOfTheSync {
		noErr(t, os.WriteFile(filepath.Join(image, "00001.mem"), logAtSync, 0o600), "writing the first log as of the sync")
		noErr(t, os.Truncate(filepath.Join(image, "00001.mem"), stat.Size()), "keeping the first log's size")
	}

	reopened, err := open(image)
	noErr(t, err, "the image did not open")
	defer func() { _ = reopened.Close() }()
	snapshot := reopened.NewSnapshot()
	defer snapshot.Discard()
	if value, err := snapshot.Get([]byte("doomed")); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("a key deleted at version 2, at or below the tables' highest version %d, is back after the reopen "+
			"with the value %q (read error: %v; the first log was on disk at the image: %v): a flushed memtable's log "+
			"was replayed over tables that no longer hold its delete (%s)", tablesHighest, value, err, logOnDisk, describe(reopened))
	}
	// What keeps it deleted (BUG-0858's fix): the flush removed the first log, durably, before its
	// table was in a level, although a reader still holds the memtable.
	if logOnDisk {
		t.Fatalf("the first memtable log %s is still on disk after its table was flushed and compacted: the flush must "+
			"remove it, durably, before the table is in a level, whatever reader holds the memtable", firstLog)
	}
}

// TestADeleteInAFlushedTableStaysDeletedAfterAKill is the route the cut adds (the stale-log
// design note of 2026-11-18, step 4 of its mechanism): no power cut, only a process kill, so the
// first log keeps every byte it got. The vouched version falls inside that flushed log; the cut
// keeps the set at or below it and discards the delete above it, whose only other copy a
// compaction dropped.
func TestADeleteInAFlushedTableStaysDeletedAfterAKill(t *testing.T) {
	deleteStaysDeletedOverAFlushedLog(t,
		func(dir string) (*BadgerKV, error) { return openVouchedTestStore(dir, vouchedTestTuning) },
		func(kv *BadgerKV) string { return fmt.Sprintf("the open reported %+v", kv.VouchedOpenReport()) },
		logAsItIsNow)
}

// TestADeleteInAFlushedTableStaysDeletedOnTheLegacyOpen is BUG-0858's power-cut route on the
// open next ships (OpenBadgerKV, no vouched cut): the first log as of the last sync holds the set
// without the delete, and the whole log is replayed over tables that no longer hold the delete.
func TestADeleteInAFlushedTableStaysDeletedOnTheLegacyOpen(t *testing.T) {
	deleteStaysDeletedOverAFlushedLog(t,
		func(dir string) (*BadgerKV, error) { return OpenBadgerKV(dir, vouchedTestTuning, nil) },
		func(*BadgerKV) string { return "the legacy open, no cut" },
		logAsOfTheSync)
}
