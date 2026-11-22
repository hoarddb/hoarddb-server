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

// TestCloseOfAStoreStalledByACorruptBlockReturnsAndNamesTheTable is BUG-0381:
// with a table that has a corrupt block in level 0, the compaction that would
// drain level 0 fails and keeps its inputs, level 0 sits at the stall limit,
// the flush goroutine parks in addLevel0Table, the channel behind it fills, and
// Close waited for room in that channel forever.
func TestCloseOfAStoreStalledByACorruptBlockReturnsAndNamesTheTable(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)
	writeTable(t, dir, 0, corruptBlockEntries)
	writeTable(t, dir, corruptBlockEntries, 2*corruptBlockEntries)
	tablePaths := tablesIn(t, dir)
	require.Len(t, tablePaths, 2)
	flipByte(t, tablePaths[0])

	db, err := Open(corruptBlockOptions(dir).
		WithNumLevelZeroTables(1).WithNumLevelZeroTablesStall(2).WithNumMemtables(2).WithMemTableSize(1 << 20).WithValueThreshold(1 << 10))
	require.NoError(t, err)
	// The compactors are off so the failure is recorded once, deterministically.
	require.Error(t, db.lc.doCompact(0, compactionPriority{level: 0, score: 1.71, adjusted: 1.71}))

	var committed atomic.Int64
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		for index := 2 * corruptBlockEntries; ; index++ {
			err := db.Update(func(txn *Txn) error {
				return txn.Set(corruptBlockKey(index), corruptBlockValue(index))
			})
			if err != nil {
				return
			}
			committed.Add(1)
		}
	}()
	// Stalled: no write has committed for a second, long after the memtable filled.
	lastCount, lastChange := int64(-1), time.Now()
	for deadline := time.Now().Add(time.Minute); ; {
		require.True(t, time.Now().Before(deadline), "the store never stalled")
		if count := committed.Load(); count != lastCount {
			lastCount, lastChange = count, time.Now()
		} else if count > 1000 && time.Since(lastChange) > time.Second {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	closed := make(chan error, 1)
	go func() { closed <- db.Close() }()
	select {
	case closeErr := <-closed:
		require.ErrorIs(t, closeErr, ErrL0StalledByDamagedTable)
		require.Contains(t, closeErr.Error(), tablePaths[0])
	case <-time.After(30 * time.Second):
		t.Fatal("Close of a store stalled behind a corrupt level-0 table did not return within 30s")
	}
	<-writerDone
}
