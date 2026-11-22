/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dgraph-io/badger/v4/options"
	"github.com/dgraph-io/badger/v4/table"
)

const corruptBlockEntries = 3000

func corruptBlockKey(index int) []byte {
	return []byte(fmt.Sprintf("key-%05d", index))
}

// corruptBlockValue is 256 bytes that do not compress, so a block holds a few
// entries and a flipped byte lands in one of them.
func corruptBlockValue(index int) []byte {
	var value []byte
	seed := corruptBlockKey(index)
	for len(value) < 256 {
		digest := sha256.Sum256(seed)
		value = append(value, digest[:]...)
		seed = digest[:]
	}
	return value[:256]
}

func corruptBlockOptions(dir string) Options {
	return getTestOptions(dir).
		WithChecksumVerificationMode(options.OnBlockRead).
		WithNumCompactors(0)
}

// writeTable writes entries [from, to) and closes the DB, which flushes them
// to a level-0 table of their own.
func writeTable(t *testing.T, dir string, from, to int) {
	t.Helper()
	db, err := Open(corruptBlockOptions(dir))
	require.NoError(t, err)
	wb := db.NewWriteBatch()
	for index := from; index < to; index++ {
		require.NoError(t, wb.Set(corruptBlockKey(index), corruptBlockValue(index)))
	}
	require.NoError(t, wb.Flush())
	require.NoError(t, db.Close())
}

func tablesIn(t *testing.T, dir string) []string {
	t.Helper()
	tablePaths, err := filepath.Glob(filepath.Join(dir, "*.sst"))
	require.NoError(t, err)
	sort.Strings(tablePaths)
	return tablePaths
}

// flipByte flips one bit a third of the way into the table, inside its data
// blocks: the index and its checksum are at the end of the file.
func flipByte(t *testing.T, tablePath string) {
	t.Helper()
	tableBytes, err := os.ReadFile(tablePath)
	require.NoError(t, err)
	tableBytes[len(tableBytes)/3] ^= 0x10
	require.NoError(t, os.WriteFile(tablePath, tableBytes, 0o600))
}

// openCorruptBlockDB opens a DB of two level-0 tables, keys [0, n) and
// [n, 2n), with one bit flipped in a data block of the first.
func openCorruptBlockDB(t *testing.T) *DB {
	t.Helper()
	dir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)
	t.Cleanup(func() { removeDir(dir) })
	writeTable(t, dir, 0, corruptBlockEntries)
	writeTable(t, dir, corruptBlockEntries, 2*corruptBlockEntries)
	tablePaths := tablesIn(t, dir)
	require.Len(t, tablePaths, 2)
	flipByte(t, tablePaths[0])
	db, err := Open(corruptBlockOptions(dir))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return db
}

// TestGetReturnsTheErrorOfACorruptBlock: a Get of a key in a block that fails
// its checksum returns the error, never ErrKeyNotFound.
func TestGetReturnsTheErrorOfACorruptBlock(t *testing.T) {
	db := openCorruptBlockDB(t)
	var failed int
	require.NoError(t, db.View(func(txn *Txn) error {
		for index := 0; index < 2*corruptBlockEntries; index++ {
			item, err := txn.Get(corruptBlockKey(index))
			if errors.Is(err, ErrKeyNotFound) {
				t.Errorf("%s reads as not found: the corrupt block was taken for a missing key", corruptBlockKey(index))
				continue
			}
			if err != nil {
				require.Contains(t, err.Error(), table.ErrBlockRead.Error())
				failed++
				continue
			}
			value, err := item.ValueCopy(nil)
			require.NoError(t, err)
			require.Equal(t, corruptBlockValue(index), value)
		}
		return nil
	}))
	require.Positive(t, failed, "no Get reported the corrupt block")
}

// TestIterationStopsAtACorruptBlock: an iteration in either direction stops
// at the corrupt block and Error reports it. It must not step over the block
// to the other table's keys, nor end as though the data ended there.
func TestIterationStopsAtACorruptBlock(t *testing.T) {
	db := openCorruptBlockDB(t)
	for _, reverse := range []bool{false, true} {
		require.NoError(t, db.View(func(txn *Txn) error {
			opts := DefaultIteratorOptions
			opts.Reverse = reverse
			it := txn.NewIterator(opts)
			defer it.Close()
			next, step := 0, 1
			if reverse {
				next, step = 2*corruptBlockEntries-1, -1
			}
			seen := 0
			for it.Rewind(); it.Valid(); it.Next() {
				require.Equal(t, string(corruptBlockKey(next)), string(it.Item().Key()),
					"reverse=%v: the iteration stepped over keys after %d of them", reverse, seen)
				next += step
				seen++
			}
			require.Less(t, seen, 2*corruptBlockEntries)
			require.ErrorIs(t, it.Error(), table.ErrBlockRead, "reverse=%v: iteration stopped after %d keys with no error", reverse, seen)
			return nil
		}))
	}
}

// TestCompactionKeepsATableWithACorruptBlock: a compaction that cannot read a
// block of its input fails and keeps the input. Committing it would delete the
// corrupt table, and with it every key the iteration did not reach.
func TestCompactionKeepsATableWithACorruptBlock(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)
	writeTable(t, dir, 0, corruptBlockEntries)
	writeTable(t, dir, corruptBlockEntries, 2*corruptBlockEntries)
	tablePaths := tablesIn(t, dir)
	require.Len(t, tablePaths, 2)
	flipByte(t, tablePaths[0])

	db, err := Open(corruptBlockOptions(dir))
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	// Both tables are in level 0, which Flatten counts as flat already, so
	// the level-0 compaction is asked for directly.
	err = db.lc.doCompact(0, compactionPriority{level: 0, score: 1.71, adjusted: 1.71})
	require.Error(t, err, "a compaction over a corrupt block succeeded")
	require.Contains(t, err.Error(), table.ErrBlockRead.Error())
	_, err = os.Stat(tablePaths[0])
	require.NoError(t, err, "the table with the corrupt block was deleted")

	// The last key of the corrupt table is after the corrupt block: the
	// compaction would have dropped it.
	require.NoError(t, db.View(func(txn *Txn) error {
		item, err := txn.Get(corruptBlockKey(corruptBlockEntries - 1))
		require.NoError(t, err)
		value, err := item.ValueCopy(nil)
		require.NoError(t, err)
		require.Equal(t, corruptBlockValue(corruptBlockEntries-1), value)
		return nil
	}))
}

// levelTablesAfterCompaction writes two level-0 tables, keys [0, n) and
// [n, 2n), compacts them into the base level as tables of about 256 KiB, closes
// the DB and returns the level tables' files in key order. A walk over a level
// is a ConcatIterator, which the level-0 tests above never reach.
func levelTablesAfterCompaction(t *testing.T, dir string) []string {
	t.Helper()
	writeTable(t, dir, 0, corruptBlockEntries)
	writeTable(t, dir, corruptBlockEntries, 2*corruptBlockEntries)
	db, err := Open(corruptBlockOptions(dir).WithBaseTableSize(256 << 10))
	require.NoError(t, err)
	// A level-0 compaction takes the tables that overlap its first, and these
	// two do not overlap, so it runs once for each.
	for db.lc.levels[0].numTables() > 0 {
		require.NoError(t, db.lc.doCompact(0, compactionPriority{level: 0, score: 1.71, adjusted: 1.71}))
	}
	var levelTables []TableInfo
	for _, info := range db.Tables() {
		require.NotZero(t, info.Level, "a level-0 table survived the compaction")
		levelTables = append(levelTables, info)
	}
	require.NoError(t, db.Close())
	require.GreaterOrEqual(t, len(levelTables), 3, "the compaction wrote too few tables to walk across")
	sort.Slice(levelTables, func(i, j int) bool { return string(levelTables[i].Left) < string(levelTables[j].Left) })
	tablePaths := make([]string, len(levelTables))
	for i, info := range levelTables {
		tablePaths[i] = table.NewFilename(info.ID, dir)
	}
	return tablePaths
}

// requireWalkStopsWithError walks the whole DB in one direction and requires
// every key returned to follow the last with none skipped, and the walk to end
// on table.ErrBlockRead.
func requireWalkStopsWithError(t *testing.T, db *DB, reverse bool) {
	t.Helper()
	require.NoError(t, db.View(func(txn *Txn) error {
		opts := DefaultIteratorOptions
		opts.Reverse = reverse
		it := txn.NewIterator(opts)
		defer it.Close()
		next, step := 0, 1
		if reverse {
			next, step = 2*corruptBlockEntries-1, -1
		}
		seen := 0
		for it.Rewind(); it.Valid(); it.Next() {
			require.Equal(t, string(corruptBlockKey(next)), string(it.Item().Key()),
				"reverse=%v: the walk stepped over keys after %d of them", reverse, seen)
			next += step
			seen++
		}
		require.Less(t, seen, 2*corruptBlockEntries)
		require.ErrorIs(t, it.Error(), table.ErrBlockRead, "reverse=%v: the walk ended after %d keys with no error", reverse, seen)
		return nil
	}))
}

// TestALevelWalkStopsAtACorruptBlock: a walk over a level stays on the table
// whose block failed, in both directions, rather than move on to the next
// table (ConcatIterator.Next).
func TestALevelWalkStopsAtACorruptBlock(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)
	tablePaths := levelTablesAfterCompaction(t, dir)
	flipByte(t, tablePaths[0])

	db, err := Open(corruptBlockOptions(dir))
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	t.Run("forward", func(t *testing.T) { requireWalkStopsWithError(t, db, false) })
	t.Run("reverse", func(t *testing.T) { requireWalkStopsWithError(t, db, true) })
}

// TestAReverseSeekIntoACorruptBlockIsAnError: a reverse iterator that stood in
// a sound block and then seeks into a corrupt one reports the block, rather
// than step back through the block it stood in before (Iterator.seekForPrev).
func TestAReverseSeekIntoACorruptBlockIsAnError(t *testing.T) {
	db := openCorruptBlockDB(t)
	damagedKey := -1
	require.NoError(t, db.View(func(txn *Txn) error {
		for index := 0; index < corruptBlockEntries && damagedKey < 0; index++ {
			if _, err := txn.Get(corruptBlockKey(index)); err != nil {
				damagedKey = index
			}
		}
		return nil
	}))
	require.GreaterOrEqual(t, damagedKey, 0, "no key reads from the corrupt block")

	require.NoError(t, db.View(func(txn *Txn) error {
		opts := DefaultIteratorOptions
		opts.Reverse = true
		it := txn.NewIterator(opts)
		defer it.Close()
		it.Seek(corruptBlockKey(corruptBlockEntries - 1))
		require.True(t, it.Valid(), "the seek into a sound block failed: %v", it.Error())
		it.Seek(corruptBlockKey(damagedKey))
		if it.Valid() {
			require.LessOrEqual(t, string(it.Item().Key()), string(corruptBlockKey(damagedKey)),
				"a reverse seek to %s answered a key after it", corruptBlockKey(damagedKey))
		}
		require.ErrorIs(t, it.Error(), table.ErrBlockRead)
		return nil
	}))
}

// TestALevelWalkStopsAtACorruptFirstBlock: a walk that reaches a table whose
// first block fails stops there, rather than take it for an empty table and
// move on to the one after (the loop in ConcatIterator.Next).
func TestALevelWalkStopsAtACorruptFirstBlock(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)
	tablePaths := levelTablesAfterCompaction(t, dir)
	tableBytes, err := os.ReadFile(tablePaths[1])
	require.NoError(t, err)
	tableBytes[100] ^= 0x10
	require.NoError(t, os.WriteFile(tablePaths[1], tableBytes, 0o600))

	db, err := Open(corruptBlockOptions(dir))
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	requireWalkStopsWithError(t, db, false)
}

// TestADamagedTableIsAnErrorNotAPanicAtEveryOffset inverts one byte at every
// 997th offset of a table, each in a fresh copy of the DB, and reads every key.
// Each copy must answer an error, at the open or in the walk, and none may
// panic. (The table package's TestEveryDamagedBlockByteIsAnErrorNotAPanic
// damages every byte of every block; this one runs the whole read path.)
// A block's entry count was decoded before its checksum was compared, so a
// damaged count drove a slice out of range: on a Get that panicked
// on the caller's goroutine, and in a compaction it would end the process.
func TestADamagedTableIsAnErrorNotAPanicAtEveryOffset(t *testing.T) {
	soundDir := t.TempDir()
	db, err := Open(sweepOptions(soundDir))
	require.NoError(t, err)
	wb := db.NewWriteBatch()
	for index := 0; index < corruptBlockEntries; index++ {
		require.NoError(t, wb.Set(corruptBlockKey(index), corruptBlockValue(index)))
	}
	require.NoError(t, wb.Flush())
	require.NoError(t, db.Close())
	tablePaths := tablesIn(t, soundDir)
	require.Len(t, tablePaths, 1)
	soundTable, err := os.ReadFile(tablePaths[0])
	require.NoError(t, err)
	soundCopy := t.TempDir()
	require.NoError(t, os.CopyFS(soundCopy, os.DirFS(soundDir)))
	answeredError, panicked := readEveryKeyRecovering(t, soundCopy)
	require.Nil(t, panicked)
	require.False(t, answeredError, "the undamaged copy answered an error")

	var copies, panics, silent int
	for offset := 0; offset < len(soundTable); offset += 997 {
		copies++
		damagedDir := t.TempDir()
		require.NoError(t, os.CopyFS(damagedDir, os.DirFS(soundDir)))
		damagedTable := append([]byte(nil), soundTable...)
		damagedTable[offset] ^= 0xff
		require.NoError(t, os.WriteFile(filepath.Join(damagedDir, filepath.Base(tablePaths[0])), damagedTable, 0o600))

		answeredError, panicked := readEveryKeyRecovering(t, damagedDir)
		switch {
		case panicked != nil:
			panics++
			t.Errorf("byte %d of %d inverted: panicked: %v", offset, len(soundTable), panicked)
		case !answeredError:
			silent++
			t.Errorf("byte %d of %d inverted: every key read back with no error", offset, len(soundTable))
		}
		require.NoError(t, os.RemoveAll(damagedDir))
	}
	require.Zero(t, panics, "%d of %d damaged copies panicked", panics, copies)
	require.Zero(t, silent, "%d of %d damaged copies answered no error", silent, copies)
}

// sweepOptions keeps the memtable and value log files small: allocating them
// at each open, not the reads, is most of the time of each of the 800-odd
// copies.
func sweepOptions(dir string) Options {
	return corruptBlockOptions(dir).WithMemTableSize(8 << 20).WithValueLogFileSize(4 << 20).WithBlockCacheSize(1 << 20)
}

// readEveryKeyRecovering opens dir and walks every key, reporting whether
// the open or the walk answered an error, or what it panicked with. A walk
// loads each block once, where a Get of every key would load each block once
// per key in it; both load blocks through Table.block, where the count was
// decoded. Every key the walk returns must be the next one written, with its
// value.
func readEveryKeyRecovering(t *testing.T, dir string) (answeredError bool, panicked any) {
	defer func() {
		if recovered := recover(); recovered != nil {
			panicked = recovered
		}
	}()
	db, err := Open(sweepOptions(dir))
	if err != nil {
		return true, nil
	}
	defer func() { require.NoError(t, db.Close()) }()
	require.NoError(t, db.View(func(txn *Txn) error {
		it := txn.NewIterator(DefaultIteratorOptions)
		defer it.Close()
		next := 0
		for it.Rewind(); it.Valid(); it.Next() {
			require.Equal(t, string(corruptBlockKey(next)), string(it.Item().Key()), "the walk returned a damaged or out-of-order key with no error")
			value, err := it.Item().ValueCopy(nil)
			require.NoError(t, err)
			require.Equal(t, corruptBlockValue(next), value, "%s read back a damaged value with no error", corruptBlockKey(next))
			next++
		}
		answeredError = it.Error() != nil
		if !answeredError {
			require.Equal(t, corruptBlockEntries, next, "the walk ended early with no error")
		}
		return nil
	}))
	return answeredError, nil
}
