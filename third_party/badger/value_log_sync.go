/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Added by the hoardDB project, 2026-11-20: every msync of a value-log file
 * goes through syncValueLogFile, which keeps a failed one, with the test
 * seams of those msyncs (hoardDB's BUG-0827, slice 2). See
 * hoardDB/third_party/README.md.
 */

package badger

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
)

// valueLogFileSync msyncs a value-log file; tests replace it to inject a
// failure.
var valueLogFileSync = func(lf *logFile) error { return lf.Sync() }

// valueLogFileSyncedHook is set only by tests (SyncTestHooks.AfterValueLogFileSync).
var valueLogFileSyncedHook atomic.Pointer[func(path string, covered int64)]

// syncValueLogFile msyncs the value-log file lf, whose first covered bytes
// were written before the call, and on success tells the test hook that
// those bytes are durable. valueLog.sync (DB.Sync's value-log step and a
// memtable flush's) and logFile.doneWriting (a file's rotation) sync through
// it.
//
// A failed msync is kept and returned by every later one of the file. The
// kernel can drop the dirty pages of a failed writeback and report the error
// once, so the next msync of the file can succeed with the bytes gone. With
// the failure kept, a flush retry cannot name a table whose pointers name
// those bytes, and DB.Sync cannot report them durable (hoardDB would then
// write a marker that vouches them). The store stops flushing (change 4
// refuses writes) and its next Open replays the logs.
func syncValueLogFile(lf *logFile, covered uint32) error {
	if failed := lf.syncFailed.Load(); failed != nil {
		return *failed
	}
	if err := valueLogFileSync(lf); err != nil {
		failure := fmt.Errorf("syncing the value-log file %s failed, so its unsynced bytes may be lost; no later sync "+
			"of it is reported durable until the store is reopened: %w", lf.path, err)
		lf.syncFailed.CompareAndSwap(nil, &failure)
		return *lf.syncFailed.Load()
	}
	if hook := valueLogFileSyncedHook.Load(); hook != nil {
		(*hook)(lf.path, int64(covered))
	}
	return nil
}

// valueLogFileSyncAtOpen fsyncs a value-log file by its path; tests replace it
// to inject a failure.
var valueLogFileSyncAtOpen = func(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("%w (and closing it: %v)", err, f.Close())
	}
	return f.Close()
}

// syncValueLogFilesAtOpen runs in Open before the flusher starts, when the
// replay left memtables to flush: it fsyncs every value-log file in the value
// directory. Those memtables' pointers name bytes an earlier process wrote,
// which a process kill leaves in the page cache only, and their flush runs
// before Open has opened the value log, so it cannot sync it
// (DB.flushToNamedTable). A failure fails Open. The test hook hears each file
// as durable whole (covered math.MaxInt64).
func (db *DB) syncValueLogFilesAtOpen() error {
	if db.opt.InMemory || db.opt.ReadOnly || len(db.imm) == 0 {
		return nil
	}
	entries, err := os.ReadDir(db.opt.ValueDir)
	if err != nil {
		return fmt.Errorf("while listing the value-log files to sync before the replayed memtables are flushed: %w", err)
	}
	for _, entry := range entries {
		if !entry.Type().IsRegular() || !strings.HasSuffix(entry.Name(), ".vlog") {
			continue
		}
		path := filepath.Join(db.opt.ValueDir, entry.Name())
		if err := valueLogFileSyncAtOpen(path); err != nil {
			return fmt.Errorf("while syncing the value-log file %s before the replayed memtables are flushed: %w", path, err)
		}
		if hook := valueLogFileSyncedHook.Load(); hook != nil {
			(*hook)(path, math.MaxInt64) // the whole file
		}
	}
	// The files' directory entries too: an earlier process may have created
	// one and stopped before any directory fsync (review F3).
	if err := flushSyncValueDir(db.opt.ValueDir); err != nil {
		return fmt.Errorf("while syncing the value directory %s before the replayed memtables are flushed: %w", db.opt.ValueDir, err)
	}
	return nil
}

// valueDirSyncFailure is a flush's failed fsync of the value directory. After
// one, a later fsync can succeed without the entry having reached the disk,
// so every later flush fails with it (change 4 refuses writes) until the
// store is reopened. hoardDB.
type valueDirSyncFailure struct{ err error }

// syncValueLogForFlush makes the value-log bytes a flushed table can point at
// durable before the MANIFEST names the table (DB.flushToNamedTable): the
// newest value-log file's msync (valueLog.sync; a rotated file was synced at
// its rotation, change 3), and, when the value log created a file since the
// last fsync of the value directory, that fsync, since a file's msync does
// not commit its directory entry on every filesystem (review F3). The count
// is read before the msync: a file the memtable points into was counted
// before any value was written to it.
func (db *DB) syncValueLogForFlush(mt *memTable) error {
	if failed := db.valueDirSyncFailed.Load(); failed != nil {
		return failed.err
	}
	created := db.vlog.filesCreated.Load()
	if err := db.vlog.sync(); err != nil {
		return fmt.Errorf("while syncing the value log before flushing the memtable of log %s: %w", mt.wal.path, err)
	}
	if created == db.vlog.dirSyncedFiles.Load() {
		return nil
	}
	if err := flushSyncValueDir(db.opt.ValueDir); err != nil {
		failure := &valueDirSyncFailure{err: fmt.Errorf("syncing the value directory %s before flushing the memtable "+
			"of log %s failed, so a value-log file's entry may not be durable; no flush names a table until the "+
			"store is reopened: %w", db.opt.ValueDir, mt.wal.path, err)}
		db.valueDirSyncFailed.CompareAndSwap(nil, failure)
		return db.valueDirSyncFailed.Load().err
	}
	db.vlog.dirSyncedFiles.Store(created)
	return nil
}

// flushSyncValueDir fsyncs the value directory in a flush (DB.flushToNamedTable);
// tests replace it to count the calls or inject a failure.
var flushSyncValueDir = syncDir
