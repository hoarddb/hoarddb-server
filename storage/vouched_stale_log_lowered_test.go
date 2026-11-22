package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v4"
)

// memtableLogNames lists the memtable logs in a store directory, oldest first.
func memtableLogNames(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	noErr(t, err, "listing %s", dir)
	var logs []string
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".mem") {
			logs = append(logs, entry.Name())
		}
	}
	sort.Strings(logs)
	return logs
}

// TestADeleteStaysDeletedWhenACompactionLowersTheTablesHighestVersion is the stale-log design
// note's hazard (2026-11-18), kept as a regression test of BUG-0858's fix. Leaving out every log
// entry at or below T, the tables' highest version, is not enough: a compaction can lower T.
// Here the newest flushed memtables hold only a delete and deletes of keys that never existed;
// a reader that reads above them all holds the memtable of the set's log; a compaction to one
// level drops every one of those deletes, so T falls back to an older table, below the set. A log
// of that memtable left on disk then holds entries above T, Open's removal of the logs of named
// tables keeps it, and the set is back. What prevents it is the flush's own order: the log is
// removed, durably, before its table is in a level, whatever reader holds the memtable.
func TestADeleteStaysDeletedWhenACompactionLowersTheTablesHighestVersion(t *testing.T) {
	for attempt := 1; attempt <= 5; attempt++ {
		reached, failure := deleteStaysDeletedUnderALoweredT(t)
		if reached {
			if failure != "" {
				t.Fatal(failure)
			}
			return
		}
		t.Logf("attempt %d: %s", attempt, failure)
	}
	t.Fatal("SCENARIO NOT REACHED in 5 attempts")
}

func deleteStaysDeletedUnderALoweredT(t *testing.T) (reached bool, failure string) {
	running := filepath.Join(t.TempDir(), "running")
	kv, err := openVouchedTestStore(running, vouchedTestTuning)
	noErr(t, err, "opening the store")
	defer func() { _ = kv.Close() }() // a test's close; the result is the return value
	commit := func(synced bool, sets map[string][]byte, deletes ...string) {
		t.Helper()
		batch := kv.NewBatch()
		for _, key := range deletes {
			noErr(t, batch.Delete([]byte(key)), "deleting")
		}
		for key, value := range sets {
			noErr(t, batch.Set([]byte(key), value), "setting %s", key)
		}
		noErr(t, batch.Commit(synced), "committing")
	}
	// History that survives the compaction: sets in a table below level 0.
	fill := make([]byte, 768<<10)
	for i := range fill {
		fill[i] = byte(i*31 + 7)
	}
	deadline := time.Now().Add(60 * time.Second)
	for i := 0; ; i++ {
		below := false
		for _, table := range kv.db.Tables() {
			below = below || table.Level > 0
		}
		if below {
			break
		}
		if time.Now().After(deadline) {
			return false, "no table below level 0 in 60 s"
		}
		commit(false, map[string][]byte{fmt.Sprintf("old/%03d", i%64): fill})
	}
	// Every memtable written so far reaches a table, so the active log holds only what follows.
	for i := 0; i < 12; i++ {
		commit(false, map[string][]byte{fmt.Sprintf("old/%03d", i%64): fill})
	}
	time.Sleep(500 * time.Millisecond)
	logs := memtableLogNames(t, running)
	if len(logs) == 0 {
		return false, "no memtable log on disk"
	}
	staleLog := filepath.Join(running, logs[len(logs)-1])

	// The set, synced: the marker vouches it and the log holds it durably.
	commit(true, map[string][]byte{"doomed": []byte("set")})
	logAtSync, err := os.ReadFile(staleLog)
	noErr(t, err, "reading the log as of the sync")
	// The delete, then deletes of keys that never existed, until the log rotates.
	commit(false, nil, "doomed")
	bigKey := strings.Repeat("k", 30<<10)
	// The flusher is held from here until the reader below holds the memtable, so the reader
	// cannot miss it: the flush, and with it the log's removal, comes after the pin.
	release := make(chan struct{})
	released := false
	letGo := func() {
		if !released {
			released = true
			close(release)
		}
	}
	defer badger.SetFlushTestHook(func(db *badger.DB) {
		if filepath.Clean(db.Opts().Dir) == filepath.Clean(running) {
			<-release
		}
	})()
	defer letGo() // runs before the store's close deferred above
	// A newer log is the rotation.
	for i := 0; ; i++ {
		var deletes []string
		for j := 0; j < 16; j++ {
			deletes = append(deletes, fmt.Sprintf("%s/%06d", bigKey, i*16+j))
		}
		commit(false, nil, deletes...)
		if now := memtableLogNames(t, running); len(now) > 0 && now[len(now)-1] > filepath.Base(staleLog) {
			break
		}
		if i > 2000 {
			return false, "the log never rotated"
		}
	}
	// A reader that reads above every entry of that log holds its memtable.
	pinTxn := kv.db.NewTransaction(false)
	pin := pinTxn.NewIterator(badger.DefaultIteratorOptions)
	defer func() {
		pin.Close()
		pinTxn.Discard()
	}()
	letGo()
	time.Sleep(time.Second)
	noErr(t, kv.db.Flatten(1), "flattening")
	var tablesHighest uint64
	for _, table := range kv.db.Tables() {
		tablesHighest = max(tablesHighest, table.MaxVersion)
	}
	var doomedSeen bool
	noErr(t, kv.db.View(func(txn *badger.Txn) error {
		all := txn.NewIterator(badger.IteratorOptions{AllVersions: true})
		defer all.Close()
		all.Seek([]byte("doomed"))
		doomedSeen = all.Valid() && string(all.Item().Key()) == "doomed"
		return nil
	}), "scanning")
	if doomedSeen {
		return false, "the compaction kept a version of doomed"
	}
	stat, statErr := os.Stat(staleLog)
	logOnDisk := statErr == nil
	image := filepath.Join(t.TempDir(), "image")
	copyStoreFiles(t, running, image)
	if logOnDisk {
		noErr(t, os.WriteFile(filepath.Join(image, filepath.Base(staleLog)), logAtSync, 0o600), "the log as of the sync")
		noErr(t, os.Truncate(filepath.Join(image, filepath.Base(staleLog)), stat.Size()), "the log's size")
	}
	reopened, err := openVouchedTestStore(image, vouchedTestTuning)
	if err != nil {
		return true, fmt.Sprintf("the image did not open: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	snapshot := reopened.NewSnapshot()
	defer snapshot.Discard()
	if value, err := snapshot.Get([]byte("doomed")); !errors.Is(err, ErrKeyNotFound) {
		return true, fmt.Sprintf("the deleted key is back with %q (read error %v) after a compaction lowered the tables' "+
			"highest version to %d; the log %s was on disk at the image: %v (open report %+v)", value, err, tablesHighest,
			filepath.Base(staleLog), logOnDisk, reopened.VouchedOpenReport())
	}
	if logOnDisk {
		return true, fmt.Sprintf("the log %s is still on disk after its memtable was flushed and compacted: the flush "+
			"must remove it, durably, before its table is in a level, whatever reader holds the memtable", filepath.Base(staleLog))
	}
	return true, ""
}
