/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

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

	"github.com/stretchr/testify/require"

	"github.com/dgraph-io/badger/v4/table"
)

// blockTableFiles makes the next count table files impossible to create: a
// directory stands where each would go, so table.CreateTable's exclusive
// create fails, while a memtable's log, a .mem file, can still be created.
// Every flush attempt reserves a new file ID, so count bounds the attempts
// that fail.
func blockTableFiles(t *testing.T, db *DB, count int) {
	t.Helper()
	next := db.lc.nextFileID.Load()
	for id := next; id < next+uint64(count); id++ {
		require.NoError(t, os.Mkdir(table.NewFilename(id, db.opt.Dir), 0o700))
	}
}

// writeUntilRefused writes 64 KiB values, one transaction each, until a
// commit fails, and sends that error. It gives up after maxWrites.
func writeUntilRefused(db *DB, maxWrites int, refused chan<- error) {
	value := make([]byte, 64<<10)
	for index := 0; index < maxWrites; index++ {
		err := db.Update(func(txn *Txn) error {
			return txn.Set([]byte(fmt.Sprintf("key-%06d", index)), value)
		})
		if err != nil {
			refused <- err
			return
		}
	}
	refused <- nil
}

// TestAFlushThatKeepsFailingRefusesWritesAndCloseReturns: when a memtable
// cannot be flushed, upstream retries every second forever. Writes go on
// being acknowledged into memtables that cannot reach a table, then block
// once every memtable is full, and Close waits for the flush and never
// returns. A flush that keeps failing must refuse writes with its cause, and
// Close must return, reporting what it could not flush.
func TestAFlushThatKeepsFailingRefusesWritesAndCloseReturns(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)
	db, err := Open(getTestOptions(dir).WithMemTableSize(8 << 20).WithNumMemtables(2))
	require.NoError(t, err)
	blockTableFiles(t, db, 1000)

	refused := make(chan error, 1)
	go writeUntilRefused(db, 10000, refused)
	select {
	case err := <-refused:
		require.Error(t, err, "10,000 writes of 64 KiB were acknowledged with no flush possible")
		require.Contains(t, err.Error(), "file exists", "the refusal does not give the flush's cause")
	case <-time.After(20 * time.Second):
		t.Fatal("after 20 s the writes were neither refused nor done: they were blocked, or still acknowledged, with no flush possible")
	}

	closed := make(chan error, 1)
	go func() { closed <- db.Close() }()
	select {
	case err := <-closed:
		require.Error(t, err, "Close reported success with memtables it could not flush")
		require.True(t, strings.Contains(err.Error(), "not flushed"), "Close's error does not say what it could not flush: %v", err)
	case <-time.After(15 * time.Second):
		t.Fatal("Close did not return within 15 s: it waits for a flush that cannot succeed")
	}
}

// TestARemovedDirectoryRefusesWritesAndCloseReturns: with the DB's directory
// removed, the memtable logs and the tables cannot be created. Upstream
// handed the full memtable to the flusher before creating its successor, so
// the failed creation left no memtable and the next write failed an
// assertion that exits the process; Close then waited for a flush that
// cannot succeed. Every write once the memtable is full must be refused, and
// Close must return.
func TestARemovedDirectoryRefusesWritesAndCloseReturns(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)
	db, err := Open(getTestOptions(dir).WithMemTableSize(8 << 20).WithNumMemtables(2))
	require.NoError(t, err)
	require.NoError(t, os.RemoveAll(dir))

	refused := make(chan error, 1)
	go writeUntilRefused(db, 10000, refused)
	select {
	case err := <-refused:
		require.Error(t, err, "10,000 writes of 64 KiB were acknowledged into a removed directory")
		require.Contains(t, err.Error(), "no such file or directory", "the refusal does not give its cause")
	case <-time.After(20 * time.Second):
		t.Fatal("after 20 s the writes were neither refused nor done")
	}
	// The write after the refusal is refused too, rather than meet no memtable.
	err = db.Update(func(txn *Txn) error { return txn.Set([]byte("after"), []byte("refusal")) })
	require.Error(t, err, "a write after the refusal was acknowledged")

	closed := make(chan error, 1)
	go func() { closed <- db.Close() }()
	select {
	case err := <-closed:
		require.Error(t, err, "Close reported success with a memtable it could not flush")
	case <-time.After(15 * time.Second):
		t.Fatal("Close did not return within 15 s")
	}
}

// TestAFlushThatRecoversLiftsTheRefusal: the refusal names ErrWritesRefused
// and WriteRefusal reports it; once a flush succeeds, writes are accepted
// again and nothing acknowledged before the refusal is lost.
func TestAFlushThatRecoversLiftsTheRefusal(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)
	db, err := Open(getTestOptions(dir).WithMemTableSize(8 << 20).WithNumMemtables(2))
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	require.NoError(t, db.WriteRefusal())
	// Attempts 1 to 4 fail and the refusal starts at the third; the fifth
	// succeeds.
	blockTableFiles(t, db, flushFailuresBeforeRefusal+1)

	refused := make(chan error, 1)
	go writeUntilRefused(db, 10000, refused)
	var refusal error
	select {
	case refusal = <-refused:
	case <-time.After(20 * time.Second):
		t.Fatal("no write was refused within 20 s")
	}
	require.ErrorIs(t, refusal, ErrWritesRefused)
	require.Contains(t, refusal.Error(), dir)
	require.ErrorIs(t, db.WriteRefusal(), ErrWritesRefused)

	require.Eventually(t, func() bool { return db.WriteRefusal() == nil }, 20*time.Second, 50*time.Millisecond,
		"the refusal was not lifted after the flush succeeded")
	require.NoError(t, db.Update(func(txn *Txn) error { return txn.Set([]byte("after"), []byte("recovery")) }))
	require.NoError(t, db.View(func(txn *Txn) error {
		_, err := txn.Get([]byte("key-000000"))
		return err
	}))
}

// TestADropAfterRefusedWritesFailsPromptly: a DropPrefix while flushes keep
// failing stops the flush goroutine, which leaves the memtables it could not
// flush in imm, one more of them than flushChan holds (the one being flushed
// is in imm and no longer in the channel). The drop must fail promptly with
// ErrWritesRefused, and reads and Close must not stall after it. Restarting
// the flusher pushed every one of them into a channel of NumMemtables while
// holding db.lock, before any goroutine could drain it: a deadlock.
func TestADropAfterRefusedWritesFailsPromptly(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)
	db, err := Open(getTestOptions(dir).WithMemTableSize(8 << 20).WithNumMemtables(2))
	require.NoError(t, err)
	blockTableFiles(t, db, 1000)

	refused := make(chan error, 1)
	go writeUntilRefused(db, 10000, refused)
	select {
	case err := <-refused:
		require.Error(t, err)
	case <-time.After(20 * time.Second):
		t.Fatal("no write was refused within 20 s")
	}

	dropped := make(chan error, 1)
	go func() { dropped <- db.DropPrefix([]byte("key-")) }()
	select {
	case err := <-dropped:
		require.Error(t, err, "a drop with memtables it could not flush reported success")
	case <-time.After(15 * time.Second):
		t.Fatal("DropPrefix did not return within 15 s")
	}

	read := make(chan error, 1)
	go func() {
		read <- db.View(func(txn *Txn) error {
			_, err := txn.Get([]byte("key-000000"))
			return err
		})
	}()
	select {
	case err := <-read:
		require.NoError(t, err, "a key written before the refusal was not readable after the failed drop")
	case <-time.After(10 * time.Second):
		t.Fatal("a read did not return within 10 s of the failed drop")
	}

	closed := make(chan error, 1)
	go func() { closed <- db.Close() }()
	select {
	case err := <-closed:
		require.Error(t, err, "Close reported success with memtables it could not flush")
	case <-time.After(15 * time.Second):
		t.Fatal("Close did not return within 15 s after the failed drop")
	}
}

// TestARefusedWriteIsAbsentAndAnAcknowledgedOnePresent: writes from many
// goroutines are grouped into one writeRequests call. When the refusal comes
// at request k of a group, requests before k are already in the memtable, and
// its log. Each must be reported as written and each request from k on as
// refused: after a reopen, every write reported refused is absent and every
// write reported done is present. Failing the whole group reported writes as
// refused that a reopen then returned.
func TestARefusedWriteIsAbsentAndAnAcknowledgedOnePresent(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)
	opts := getTestOptions(dir).WithMemTableSize(8 << 20).WithNumMemtables(2)
	db, err := Open(opts)
	require.NoError(t, err)
	blockTableFiles(t, db, 1000)
	blocked, err := filepath.Glob(filepath.Join(dir, "*.sst"))
	require.NoError(t, err)

	const writers = 16
	type outcome struct {
		key     string
		refused bool
	}
	outcomes := make(chan outcome, writers*10000)
	var wg sync.WaitGroup
	for writer := 0; writer < writers; writer++ {
		wg.Add(1)
		go func(writer int) {
			defer wg.Done()
			value := make([]byte, 16<<10)
			for index := 0; index < 10000; index++ {
				key := fmt.Sprintf("w%02d-%06d", writer, index)
				err := db.Update(func(txn *Txn) error { return txn.Set([]byte(key), value) })
				outcomes <- outcome{key: key, refused: err != nil}
				if err != nil {
					return
				}
			}
		}(writer)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("the writers were neither refused nor done within 30 s")
	}
	close(outcomes)
	closed := make(chan error, 1)
	go func() { closed <- db.Close() }()
	select {
	case <-closed:
	case <-time.After(15 * time.Second):
		t.Fatal("Close did not return within 15 s")
	}

	for _, path := range blocked {
		require.NoError(t, os.Remove(path))
	}
	reopened, err := Open(opts)
	require.NoError(t, err)
	defer func() { require.NoError(t, reopened.Close()) }()
	var acknowledged, refused, refusedPresent, acknowledgedMissing int
	require.NoError(t, reopened.View(func(txn *Txn) error {
		for result := range outcomes {
			_, err := txn.Get([]byte(result.key))
			present := err == nil
			if err != nil && !errors.Is(err, ErrKeyNotFound) {
				return err
			}
			switch {
			case result.refused:
				refused++
				if present {
					refusedPresent++
				}
			default:
				acknowledged++
				if !present {
					acknowledgedMissing++
				}
			}
		}
		return nil
	}))
	t.Logf("acknowledged %d, refused %d", acknowledged, refused)
	require.Positive(t, refused)
	require.Zero(t, refusedPresent, "%d of %d writes reported refused were present after a reopen", refusedPresent, refused)
	require.Zero(t, acknowledgedMissing, "%d of %d acknowledged writes were missing after a reopen", acknowledgedMissing, acknowledged)
}

// TestAFlushRefusalWithRoomInTheMemtableRefusesWrites: the refusal is set by
// the flusher after the full memtable was handed over, so the replacement
// memtable is empty and ensureRoomForWrite finds room and does not refuse.
// Only the check in sendToWriteCh stops a write then. Without it the write is
// acknowledged into a memtable whose successors cannot be flushed.
func TestAFlushRefusalWithRoomInTheMemtableRefusesWrites(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)
	db, err := Open(getTestOptions(dir).WithMemTableSize(8 << 20).WithNumMemtables(2))
	require.NoError(t, err)
	blockTableFiles(t, db, 1000)

	// Fill one memtable and no more: stop once it has been handed to the
	// flusher, which then fails and sets the refusal.
	value := make([]byte, 64<<10)
	handedOver := func() bool {
		db.lock.RLock()
		defer db.lock.RUnlock()
		return len(db.imm) > 0
	}
	for index := 0; !handedOver(); index++ {
		require.Less(t, index, 1000, "no memtable was handed to the flusher")
		require.NoError(t, db.Update(func(txn *Txn) error {
			return txn.Set([]byte(fmt.Sprintf("key-%06d", index)), value)
		}))
	}
	require.Eventually(t, func() bool { return db.WriteRefusal() != nil }, 20*time.Second, 50*time.Millisecond,
		"the flusher did not set the refusal")

	db.lock.RLock()
	hasRoom := !db.mt.isFull()
	db.lock.RUnlock()
	require.True(t, hasRoom, "the memtable is full, so ensureRoomForWrite would refuse and this test would not reach sendToWriteCh's check")

	err = db.Update(func(txn *Txn) error { return txn.Set([]byte("after"), []byte("refusal")) })
	require.ErrorIs(t, err, ErrWritesRefused, "a write was acknowledged while the flusher keeps failing")

	closed := make(chan error, 1)
	go func() { closed <- db.Close() }()
	select {
	case <-closed:
	case <-time.After(15 * time.Second):
		t.Fatal("Close did not return within 15 s")
	}
}

// TestAManifestSyncFailingOnceInAFlushLeavesAStoreThatReopens is BUG-0868's
// failing-first test. A flush names its table in the MANIFEST; the record is
// applied in memory and written before the sync, so when the sync fails the
// record may be on disk (it is, with this injection) while upstream's failure
// path deleted the table file, and the retry named a second table. The store
// ran on and did not reopen: "file does not exist for table 1". The fix keeps
// the table file and does not retry the naming (the failed-fsync argument C1
// applied to the directory), so whatever the MANIFEST holds, the next open
// finds a consistent store. The test needs the failure to have happened, waits
// until the flush either finished or refused writes, closes, and reopens with
// every written key.
func TestAManifestSyncFailingOnceInAFlushLeavesAStoreThatReopens(t *testing.T) {
	dir := t.TempDir()
	opts := getTestOptions(dir).WithMemTableSize(1 << 20).WithValueThreshold(1 << 10)
	db, err := Open(opts)
	require.NoError(t, err)
	var failures atomic.Int32
	original := syncFunc
	syncFunc = func(f *os.File) error {
		if failures.CompareAndSwap(0, 1) {
			return errors.New("injected: the MANIFEST sync failed once")
		}
		return original(f)
	}
	defer func() { syncFunc = original }()

	rotated := activeMemTable(db)
	var written []string
	for index := 0; activeMemTable(db) == rotated; index++ {
		require.Less(t, index, 100000, "the memtable never rotated")
		key := fmt.Sprintf("key-%06d", index)
		require.NoError(t, db.Update(func(txn *Txn) error { return txn.Set([]byte(key), make([]byte, 512)) }))
		written = append(written, key)
	}
	deadline := time.Now().Add(30 * time.Second)
	for failures.Load() == 0 || (numImmutables(db) > 0 && db.flushRefusal.Load() == nil) {
		require.True(t, time.Now().Before(deadline), "SCENARIO NOT REACHED: the flush neither failed, finished nor refused writes in 30 s")
		time.Sleep(10 * time.Millisecond)
	}
	require.Equal(t, int32(1), failures.Load(), "SCENARIO NOT REACHED: the MANIFEST sync did not fail")
	// Close reports the memtable it could not flush, after the fix; its error is not the result.
	t.Logf("Close after the failed sync: %v", db.Close())

	reopened, err := Open(opts)
	require.NoError(t, err, "the store does not reopen after one failed MANIFEST sync in a flush")
	defer func() { require.NoError(t, reopened.Close()) }()
	// The naming is not retried: the one memtable gives one table, the one the
	// failed change named, and no second table holds the same entries.
	require.Len(t, reopened.Tables(), 1, "the flush was retried after its MANIFEST change failed")
	require.NoError(t, reopened.View(func(txn *Txn) error {
		for _, key := range written {
			if _, err := txn.Get([]byte(key)); err != nil {
				return fmt.Errorf("%s after the reopen: %w", key, err)
			}
		}
		return nil
	}))
}

// failManifestSyncOnce makes the next MANIFEST sync fail once and returns how many failed, for a
// test to require the failure happened; it restores syncFunc at the test's end.
func failManifestSyncOnce(t *testing.T) *atomic.Int32 {
	t.Helper()
	var failures atomic.Int32
	original := syncFunc
	syncFunc = func(f *os.File) error {
		if failures.CompareAndSwap(0, 1) {
			return errors.New("injected: the MANIFEST sync failed once")
		}
		return original(f)
	}
	t.Cleanup(func() { syncFunc = original })
	return &failures
}

// writeUntilRotated writes 512-byte values under prefix until the active memtable rotates, and
// returns the keys.
func fillUntilRotated(t *testing.T, db *DB, prefix string) []string {
	t.Helper()
	rotated := activeMemTable(db)
	var written []string
	for index := 0; activeMemTable(db) == rotated; index++ {
		require.Less(t, index, 100000, "the memtable never rotated")
		key := fmt.Sprintf("%s-%06d", prefix, index)
		require.NoError(t, db.Update(func(txn *Txn) error { return txn.Set([]byte(key), make([]byte, 512)) }))
		written = append(written, key)
	}
	return written
}

// requireKeys requires every key in db.
func requireKeys(t *testing.T, db *DB, keys []string) {
	t.Helper()
	require.NoError(t, db.View(func(txn *Txn) error {
		for _, key := range keys {
			if _, err := txn.Get([]byte(key)); err != nil {
				return fmt.Errorf("%s: %w", key, err)
			}
		}
		return nil
	}))
}

// TestAManifestRecordLostAfterAFailedSyncLeavesAStoreThatReopens is BUG-0868's other branch: the
// failed sync's record did not reach the disk (the MANIFEST is cut back to its size before the
// failed change), in the first flush and in a second one. The kept table is then unnamed: Badger
// deletes it at the next open and replays the memtable's log, which the fix never removed; every
// key is there (review R2 F-9).
func TestAManifestRecordLostAfterAFailedSyncLeavesAStoreThatReopens(t *testing.T) {
	for _, flush := range []int{1, 2} {
		t.Run(fmt.Sprintf("flush %d", flush), func(t *testing.T) {
			dir := t.TempDir()
			opts := getTestOptions(dir).WithMemTableSize(1 << 20).WithValueThreshold(1 << 10)
			db, err := Open(opts)
			require.NoError(t, err)
			var written []string
			if flush == 2 {
				written = append(written, fillUntilRotated(t, db, "first")...)
				deadline := time.Now().Add(30 * time.Second)
				for numImmutables(db) > 0 {
					require.True(t, time.Now().Before(deadline), "SCENARIO NOT REACHED: the first flush did not finish")
					time.Sleep(10 * time.Millisecond)
				}
			}
			manifest := filepath.Join(dir, ManifestFilename)
			before, err := os.Stat(manifest)
			require.NoError(t, err)
			failures := failManifestSyncOnce(t)
			written = append(written, fillUntilRotated(t, db, "failed")...)
			deadline := time.Now().Add(30 * time.Second)
			for failures.Load() == 0 || db.flushRefusal.Load() == nil {
				require.True(t, time.Now().Before(deadline), "SCENARIO NOT REACHED: the flush did not fail and refuse writes")
				time.Sleep(10 * time.Millisecond)
			}
			t.Logf("Close after the failed sync: %v", db.Close())
			cutManifestTo(t, manifest, before.Size())

			reopened, err := Open(opts)
			require.NoError(t, err, "the store does not reopen when the failed record did not reach the disk")
			defer func() { require.NoError(t, reopened.Close()) }()
			requireKeys(t, reopened, written)
		})
	}
}

// cutManifestTo shortens the test's MANIFEST to size, as a failed sync that lost the last record
// leaves it.
func cutManifestTo(t *testing.T, manifest string, size int64) {
	t.Helper()
	file, err := os.OpenFile(manifest, os.O_RDWR, 0)
	require.NoError(t, err)
	require.NoError(t, file.Truncate(size), "cutting the failed record off the MANIFEST")
	require.NoError(t, file.Close())
}

// TestADropPrefixWhoseManifestSyncFailsOnceLeavesAStoreThatReopens is BUG-0868's drop-shaped twin:
// DropPrefix flushes the memtables before it drops; a MANIFEST sync failing once in that flush fails
// the drop with the kept error, and the store reopens holding every key the drop did not drop
// (review R2 F-9, the reviewer's TestR2ManifestSyncFailsOnceInADropPrefix).
func TestADropPrefixWhoseManifestSyncFailsOnceLeavesAStoreThatReopens(t *testing.T) {
	dir := t.TempDir()
	opts := getTestOptions(dir).WithMemTableSize(1 << 20).WithValueThreshold(1 << 10)
	db, err := Open(opts)
	require.NoError(t, err)
	var kept []string
	for index := range 200 {
		key := fmt.Sprintf("kept-%04d", index)
		require.NoError(t, db.Update(func(txn *Txn) error { return txn.Set([]byte(key), make([]byte, 512)) }))
		kept = append(kept, key)
	}
	// A drop with nothing under its prefix returns before it flushes.
	require.NoError(t, db.Update(func(txn *Txn) error { return txn.Set([]byte("dropped-1"), []byte("goes")) }))
	failures := failManifestSyncOnce(t)
	dropErr := db.DropPrefix([]byte("dropped-"))
	require.Equal(t, int32(1), failures.Load(), "SCENARIO NOT REACHED: the drop's flush did not sync the MANIFEST")
	require.Error(t, dropErr, "the drop succeeded over a failed MANIFEST sync")
	t.Logf("Close after the failed drop: %v", db.Close())

	reopened, err := Open(opts)
	require.NoError(t, err, "the store does not reopen after a drop whose MANIFEST sync failed")
	defer func() { require.NoError(t, reopened.Close()) }()
	requireKeys(t, reopened, kept)
}
