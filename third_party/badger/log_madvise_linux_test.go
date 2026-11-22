//go:build linux

/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
)

// TestLogMappingsAreAdvisedRandomAccess checks that every memory mapping of a
// memtable WAL or value-log file carries the kernel's random-access hint
// (MADV_RANDOM, shown as "rr" in /proc/self/smaps VmFlags): at creation, at
// reopen, and after a remap. Without the hint, a kernel that sizes file folios
// by the readahead window gives the tail of an appended log large folios, and
// every DB.Sync then writes a whole folio back for a few dirtied KB.
func TestLogMappingsAreAdvisedRandomAccess(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)

	opt := getTestOptions(dir).WithMemTableSize(1 << 20).WithValueThreshold(1 << 10)
	db, err := Open(opt)
	require.NoError(t, err)

	requireAdvisedRandom(t, "the new memtable's WAL", db.mt.wal)
	currentVlog := db.vlog.filesMap[db.vlog.maxFid]
	requireAdvisedRandom(t, "the new value-log file", currentVlog)

	// Grow the value-log file the way valueLog.write does when one request
	// outgrows its preallocation: the mapping is remapped.
	currentVlog.lock.Lock()
	require.NoError(t, currentVlog.Truncate(int64(len(currentVlog.Data))*2))
	currentVlog.lock.Unlock()
	requireAdvisedRandom(t, "the value-log file after it was grown", currentVlog)

	require.NoError(t, db.Update(func(txn *Txn) error {
		if err := txn.Set([]byte("inline"), []byte("a value kept in the WAL")); err != nil {
			return err
		}
		return txn.Set([]byte("separate"), make([]byte, 4<<10))
	}))
	require.NoError(t, db.Close())

	// Reopen after a clean Close: Close flushed the memtable, so there is no
	// WAL to replay here (the replay is TestLogMappingsAreAdvisedRandomAccessAfterReplay),
	// only a new memtable WAL and the value-log files opened again.
	db, err = Open(opt)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	requireAdvisedRandom(t, "the memtable WAL after reopen", db.mt.wal)
	for fid, reopened := range db.vlog.filesMap {
		requireAdvisedRandom(t, fmt.Sprintf("value-log file %d after reopen", fid), reopened)
	}
}

// TestLogMappingsAreAdvisedRandomAccessAfterReplay checks the advice on a WAL
// that is replayed at open (memTable.UpdateSkipList), which keeps readahead
// for the replay and must take it away afterwards. The WAL comes from a crash
// image: a copy of the directory of an open store after DB.Sync, so the
// memtable was never flushed and the copy has a WAL to replay.
//
// Removing the advice from logFile.Truncate alone does not fail this test on
// Linux, and is not meant to: mremap keeps a mapping's advice, so on Linux that
// call matters only on a system whose z.Truncate maps afresh.
func TestLogMappingsAreAdvisedRandomAccessAfterReplay(t *testing.T) {
	source, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)
	defer removeDir(source)

	opt := getTestOptions(source).WithMemTableSize(1 << 20).WithValueThreshold(1 << 10)
	db, err := Open(opt)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	require.NoError(t, db.Update(func(txn *Txn) error {
		return txn.Set([]byte("unflushed"), []byte("a value kept in the WAL"))
	}))
	require.NoError(t, db.Sync())
	db.lock.RLock()
	walName := filepath.Base(db.mt.wal.path)
	walEnd := int64(db.mt.wal.writeAt)
	db.lock.RUnlock()

	t.Run("a WAL the replay truncates", func(t *testing.T) {
		// The preallocated WAL is longer than its entries, so the replay ends
		// in logFile.Truncate, a remap. A read-write Open would hand the
		// replayed memtable to the flush goroutine, which can flush it and
		// delete its WAL before the check runs (it did, in a run of this
		// test). openMemTable is the call openMemTables makes for each WAL,
		// and the memtable it returns is the test's alone.
		image := crashImage(t, source)
		defer removeDir(image)
		const imageFid = 1 << 20
		require.NoError(t, os.Rename(filepath.Join(image, walName), db.mtFilePath(imageFid)))
		replayed, err := db.openMemTable(imageFid, os.O_RDWR)
		require.NoError(t, err)
		defer replayed.DecrRef()
		require.False(t, replayed.sl.Empty(), "the crash image's WAL replayed no entries")
		require.Less(t, replayed.wal.size.Load(), uint32(2*opt.MemTableSize),
			"the replay did not truncate the WAL, so this case does not reach logFile.Truncate")
		requireAdvisedRandom(t, "a replayed memtable's WAL", replayed.wal)
	})

	t.Run("a WAL that ends where its entries end", func(t *testing.T) {
		// A WAL already cut to its entries (an earlier replay truncated it) is
		// not truncated again, so only the advice given after the replay
		// leaves it random. Read-only, because a read-only store does not
		// flush the replayed memtable and the check cannot race the flush.
		image := crashImage(t, source)
		defer removeDir(image)
		require.NoError(t, os.Truncate(filepath.Join(image, walName), walEnd))
		replayed, err := Open(opt.WithDir(image).WithValueDir(image).WithReadOnly(true))
		require.NoError(t, err)
		defer func() { require.NoError(t, replayed.Close()) }()
		requireReplayedWALsAdvisedRandom(t, replayed)
	})
}

// crashImage copies the directory of an open store, as a crash would leave
// it, and returns the copy.
func crashImage(t *testing.T, storeDir string) string {
	t.Helper()
	image, err := os.MkdirTemp("", "badger-crash-image")
	require.NoError(t, err)
	require.NoError(t, copyDir(storeDir, image))
	require.NoError(t, os.Remove(filepath.Join(image, lockFile)))
	return image
}

func requireReplayedWALsAdvisedRandom(t *testing.T, db *DB) {
	t.Helper()
	db.lock.RLock()
	defer db.lock.RUnlock()
	require.NotEmpty(t, db.imm, "the crash image replayed no WAL, so there is nothing to check")
	for _, replayed := range db.imm {
		requireAdvisedRandom(t, "a replayed memtable's WAL", replayed.wal)
	}
}

func requireAdvisedRandom(t *testing.T, which string, lf *logFile) {
	t.Helper()
	vmFlags := mappingVmFlags(t, lf.Data)
	require.Contains(t, vmFlags, "rr",
		"%s (%s) is mapped without MADV_RANDOM; VmFlags: %v", which, lf.path, vmFlags)
}

// mappingVmFlags returns the VmFlags of the mapping that starts at mapped[0],
// read from /proc/self/smaps.
func mappingVmFlags(t *testing.T, mapped []byte) []string {
	t.Helper()
	start := uint64(uintptr(unsafe.Pointer(&mapped[0])))
	smaps, err := os.Open("/proc/self/smaps")
	require.NoError(t, err)
	defer func() { require.NoError(t, smaps.Close()) }()

	lines := bufio.NewScanner(smaps)
	inMapping := false
	for lines.Scan() {
		line := lines.Text()
		if flags, found := strings.CutPrefix(line, "VmFlags:"); found {
			if inMapping {
				return strings.Fields(flags)
			}
			continue
		}
		addressRange, _, _ := strings.Cut(line, " ")
		low, _, isHeader := strings.Cut(addressRange, "-")
		if !isHeader {
			continue
		}
		mappingStart, err := strconv.ParseUint(low, 16, 64)
		if err != nil {
			continue
		}
		inMapping = mappingStart == start
	}
	require.NoError(t, lines.Err())
	t.Fatalf("no mapping in /proc/self/smaps starts at %#x", start)
	return nil
}
