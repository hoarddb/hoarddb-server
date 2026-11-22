/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Added by the hoardDB project, 2026-11-18: the gate tests of the open step
 * (DB.removeLogsOfNamedTables) and the flush step hook. The open step removes
 * every replayed log whose highest version is at or below the tables' highest,
 * which is sound only while commit versions are issued in write order. These
 * tests pin what keeps that true. See hoardDB/third_party/README.md.
 */

package badger

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestManagedModeKeepsTheLogOfACommitBelowTheTables writes at versions the
// caller chooses, out of order: a table holds version 10, and the next
// memtable's log holds a commit at version 5. The open step would take that log
// for the leftover of a flush whose table the MANIFEST names and remove it, and
// the commit would be lost; managed mode must skip the removal (the review's
// version-order gate: DB.removeLogsOfNamedTables). Red when the managed-mode
// condition is removed from the step.
func TestManagedModeKeepsTheLogOfACommitBelowTheTables(t *testing.T) {
	dir := t.TempDir()
	opt := cutTestOptions(dir)
	db, err := OpenManaged(opt)
	require.NoError(t, err)
	closeAtCleanup(t, db)

	writeAt := func(commitVersion uint64, prefix string, count int) {
		batch := db.NewWriteBatchAt(commitVersion)
		for i := range count {
			require.NoError(t, batch.Set([]byte(prefix+string(rune('a'+i%26))+string(rune('a'+i/26))), make([]byte, 512)))
		}
		require.NoError(t, batch.Flush())
	}
	first := activeMemTable(db)
	for round := 0; activeMemTable(db) == first; round++ {
		require.Less(t, round, 1000, "the first memtable never rotated")
		writeAt(10, "high-", 64)
	}
	waitUntilNoImmutables(t, db)
	var tablesHighest uint64
	for _, info := range db.Tables() {
		tablesHighest = max(tablesHighest, info.MaxVersion)
	}
	require.Equal(t, uint64(10), tablesHighest, "SCENARIO NOT REACHED: the flushed table does not hold version 10")

	writeAt(5, "late-", 1)
	require.NoError(t, db.Sync())
	imageDir := writeImage(t, imageAt(t, dir))

	reopened, err := OpenManaged(opt.WithDir(imageDir).WithValueDir(imageDir))
	require.NoError(t, err)
	defer func() { require.NoError(t, reopened.Close()) }()
	txn := reopened.NewTransactionAt(math.MaxUint64, false)
	defer txn.Discard()
	_, err = txn.Get([]byte("late-aa"))
	require.NoError(t, err, "a commit at version 5, in a log beside a table holding version 10, was removed by the open step "+
		"in managed mode, where commit versions are the caller's and the step's premise does not hold")
}

// TestALegacyDropPrefixKeepsTheVersionOrderTheOpenStepRelies is the one call
// of hoardDB's legacy store that rewrites tables behind the flusher's back
// (DB.DropPrefix); it keeps the versions it finds and the next commit takes a
// version above every table's, so a log written after a drop is never at or
// below the tables' highest version and the open step keeps it. If a drop ever
// reset or lowered versions, this commit would be lost at the reopen.
func TestALegacyDropPrefixKeepsTheVersionOrderTheOpenStepRelies(t *testing.T) {
	dir := t.TempDir()
	opt := cutTestOptions(dir)
	db, err := Open(opt)
	require.NoError(t, err)
	closeAtCleanup(t, db)

	setKey(t, db, "drop-1")
	setKey(t, db, "keep-1")
	require.NoError(t, commitUntilRotatedFrom(db, activeMemTable(db), "fill"))
	waitUntilNoImmutables(t, db)
	require.NoError(t, db.DropPrefix([]byte("drop-")))
	setKey(t, db, "after-the-drop")
	require.NoError(t, db.Sync())
	imageDir := writeImage(t, imageAt(t, dir))

	reopened, err := Open(opt.WithDir(imageDir).WithValueDir(imageDir))
	require.NoError(t, err)
	defer func() { require.NoError(t, reopened.Close()) }()
	require.True(t, hasKey(t, reopened, "after-the-drop"), "a commit after a DropPrefix is gone after a reopen: the open step took its "+
		"log for the leftover of a flushed memtable")
	require.True(t, hasKey(t, reopened, "keep-1"))
	require.False(t, hasKey(t, reopened, "drop-1"), "the dropped key is back")
}
