/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// stalledWriter writes new keys in a loop and reports the error that ends it.
type stalledWriter struct {
	committed atomic.Int64
	failed    chan error
}

func startStalledWriter(db *DB) *stalledWriter {
	writer := &stalledWriter{failed: make(chan error, 1)}
	go func() {
		for index := 2 * corruptBlockEntries; ; index++ {
			err := db.Update(func(txn *Txn) error {
				return txn.Set(corruptBlockKey(index), corruptBlockValue(index))
			})
			if err != nil {
				writer.failed <- err
				return
			}
			writer.committed.Add(1)
		}
	}()
	return writer
}

// stalledStoreOptions is two level-0 tables at a stall limit of two, one slot
// for a flushing memtable and a 1 MiB memtable, with the compactors off so the
// test chooses when a compaction runs.
func stalledStoreOptions(dir string) Options {
	return corruptBlockOptions(dir).WithNumLevelZeroTables(1).WithNumLevelZeroTablesStall(2).
		WithNumMemtables(2).WithMemTableSize(1 << 20).WithValueThreshold(1 << 10)
}

func openStalledStore(t *testing.T, damage bool) (*DB, string) {
	t.Helper()
	dir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)
	t.Cleanup(func() { removeDir(dir) })
	writeTable(t, dir, 0, corruptBlockEntries)
	writeTable(t, dir, corruptBlockEntries, 2*corruptBlockEntries)
	tablePaths := tablesIn(t, dir)
	require.Len(t, tablePaths, 2)
	if damage {
		flipByte(t, tablePaths[0])
	}
	db, err := Open(stalledStoreOptions(dir))
	require.NoError(t, err)
	return db, tablePaths[0]
}

// waitUntilWritesStop returns once more than 1000 writes have committed and
// none has for a second, which is a writer blocked in ensureRoomForWrite.
func waitUntilWritesStop(t *testing.T, writer *stalledWriter) {
	t.Helper()
	lastCount, lastChange := int64(-1), time.Now()
	for deadline := time.Now().Add(time.Minute); ; {
		require.True(t, time.Now().Before(deadline), "the writes never stopped")
		if count := writer.committed.Load(); count != lastCount {
			lastCount, lastChange = count, time.Now()
		} else if count > 1000 && time.Since(lastChange) > time.Second {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestAWriteBlockedBehindACorruptBlockIsRefusedNamingTheTable is BUG-0381's
// write half: with level 0 at its stall limit and a recorded compaction failure
// on an unreadable block, a write that would wait for room is refused at once
// with an error that names the table, instead of waiting for a compaction that
// cannot succeed.
func TestAWriteBlockedBehindACorruptBlockIsRefusedNamingTheTable(t *testing.T) {
	db, damagedTable := openStalledStore(t, true)
	defer func() { _ = db.Close() }() // the close error is TestCloseOfAStoreStalled...'s subject
	require.Error(t, db.lc.doCompact(0, compactionPriority{level: 0, score: 1.71, adjusted: 1.71}))

	writer := startStalledWriter(db)
	select {
	case writeErr := <-writer.failed:
		require.ErrorIs(t, writeErr, ErrWritesRefused)
		require.ErrorIs(t, writeErr, ErrL0StalledByDamagedTable)
		require.Contains(t, writeErr.Error(), damagedTable)
		require.ErrorIs(t, db.WriteRefusal(), ErrL0StalledByDamagedTable)
	case <-time.After(time.Minute):
		t.Fatal("a write behind a corrupt level-0 table was not refused within 1m")
	}
}

// TestAWriteBlockedByASlowFlushOfAHealthyStoreIsNeverRefused: the same store
// without the damage reaches the same stall (no compaction has run), and its
// writes wait. They are not refused, and they go on when a compaction drains
// level 0.
func TestAWriteBlockedByASlowFlushIsNeverRefusedWithoutACorruptBlock(t *testing.T) {
	db, _ := openStalledStore(t, false)
	defer func() { require.NoError(t, db.Close()) }()

	writer := startStalledWriter(db)
	waitUntilWritesStop(t, writer)
	select {
	case writeErr := <-writer.failed:
		t.Fatalf("a write blocked behind a healthy level 0 was refused: %v", writeErr)
	case <-time.After(2 * time.Second):
	}
	require.NoError(t, db.WriteRefusal())

	stalledAt := writer.committed.Load()
	require.NoError(t, db.lc.doCompact(0, compactionPriority{level: 0, score: 1.71, adjusted: 1.71}))
	for deadline := time.Now().Add(time.Minute); writer.committed.Load() == stalledAt; {
		require.True(t, time.Now().Before(deadline), "the writer did not go on after level 0 drained")
		time.Sleep(20 * time.Millisecond)
	}
}
