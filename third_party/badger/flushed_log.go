/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Added by the hoardDB project, 2026-11-18: a flushed memtable's log is
 * removed, durably, after the MANIFEST names its table and before the table
 * is in a level, and Open removes a log whose table the MANIFEST already
 * names instead of replaying it (hoardDB's BUG-0858). See
 * hoardDB/third_party/README.md. 2026-11-19: Close also keeps the table of a
 * flush whose MANIFEST change failed (hoardDB's BUG-0868).
 */

package badger

import (
	"errors"
	"fmt"
	"os"
	"sync/atomic"

	"github.com/dgraph-io/ristretto/v2/z"
)

// Why. Badger removed a flushed memtable's log when the memtable's last
// reference went, with no directory fsync, and nothing synced that log at the
// flush. A reader or a sync capture kept it on disk after its table was in
// level 0, where a compaction can drop a delete marker together with the
// versions under it. After a crash the log was replayed: under hoardDB's
// vouched cut its entries at or below the vouched version were kept and the
// delete above it discarded, and after a power cut the log could hold a set
// without the delete that followed. Either way a deleted key came back.
//
// The order that closes it, per flush: the table is synced (CreateTable), the
// MANIFEST change naming it is synced (levelsController.nameLevel0Table), the
// log is unlinked and the directory fsynced (DB.retireMemTableLog), and only
// then is the table inserted into level 0 (levelsController.insertLevel0Table),
// where a compaction can pick it. So no compaction ever reads a table whose
// log a crash can leave on disk. A crash inside that window can leave the log
// beside its named table; Open removes such a log, durably, before any
// compaction starts (DB.removeLogsOfNamedTables).

// retireRemoveLog and retireSyncDir remove a flushed log and fsync the store
// directory; tests replace them to inject a failure or to stop inside the
// retirement.
var (
	retireRemoveLog = os.Remove
	retireSyncDir   = syncDir
)

// retireMemTableLog removes the log of a memtable whose table the MANIFEST
// names, and makes the removal durable. The name goes now whatever references
// the memtable has: a reader reads the skiplist, which never needed the log,
// and a sync capture msyncs the unlinked file; the last reference releases it
// (logFile.closeRetired). A failed unlink can be retried. A failed directory
// fsync cannot: after one, a later fsync can succeed without the unlink having
// reached the disk, so the failure is kept and returned by every retry, the
// table stays out of level 0, the flush loop refuses writes (change 4), and
// the next Open makes the removal durable: it removes the log if it is still
// there, and fsyncs the directory in any case before compactions start
// (removeLogsOfNamedTables). The memtable is never the active one: the
// flusher takes only rotated memtables, and DropPrefix rotates the active one
// out before it flushes it.
func (db *DB) retireMemTableLog(mt *memTable) error {
	if mt.retireFailed != nil {
		return mt.retireFailed
	}
	if !mt.logRetired.Load() {
		if err := retireRemoveLog(mt.wal.path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("while removing the flushed memtable log %s: %w", mt.wal.path, err)
		}
		mt.logRetired.Store(true)
		db.flushStep(FlushLogUnlinked)
	}
	if err := retireSyncDir(db.opt.Dir); err != nil {
		mt.retireFailed = fmt.Errorf("syncing the directory %s after removing the flushed memtable log %s failed, "+
			"so the removal is not known to be durable; its table stays out of level 0 until the store is reopened: %w",
			db.opt.Dir, mt.wal.path, err)
		return mt.retireFailed
	}
	return nil
}

// releaseNamedTables runs in DB.close after the flusher has stopped: a
// memtable whose table the MANIFEST names and whose log's removal never became
// durable keeps that table out of every level. The table is closed and kept on
// disk: the next Open loads it from the MANIFEST and removes the log
// (removeLogsOfNamedTables). DecrRef would delete the file of a named table.
// It returns how many such tables it closed: their memtables' writes are all in
// them, and Close says so instead of "their logs are replayed".
func (db *DB) releaseNamedTables() (int, error) {
	var errs []error
	released := 0
	for _, mt := range db.imm {
		// A table whose naming failed is kept too, and not counted: its log
		// was never removed (memTable.failedNaming, BUG-0868).
		if mt.failedNaming != nil {
			if err := mt.failedNaming.Close(-1); err != nil {
				errs = append(errs, fmt.Errorf("closing table %d, whose naming in the MANIFEST failed: %w", mt.failedNaming.ID(), err))
			}
			mt.failedNaming = nil
		}
		if mt.named == nil {
			continue
		}
		if err := mt.named.Close(-1); err != nil {
			errs = append(errs, fmt.Errorf("closing table %d, which the MANIFEST names and no level holds: %w", mt.named.ID(), err))
		}
		mt.named = nil
		released++
	}
	return released, errors.Join(errs...)
}

// removeLogsOfNamedTables runs in Open after the levels are loaded and before
// any compaction or flush starts. A replayed log whose every entry is at or
// below the tables' highest version belongs to a memtable whose table the
// MANIFEST already names: commit versions are written in order and memtables
// are flushed in order, so a memtable never flushed holds only versions above
// every table's. Such a log is what a crash inside the retirement leaves (its
// table is in no compaction's output yet) or, in a store last run before this
// change, a log a reader kept past its flush. Its entries are in the tables or
// were dropped there by a compaction, as they may be; replayed, they would be
// flushed into a second table that can outlive a compaction of the first and
// bring back what that compaction dropped. The logs go.
//
// Then the directory is fsynced, whether or not this open removed anything
// (review F2): an unlink an earlier process applied, by a flush whose directory
// fsync failed or by an open that failed after its own unlink, can be in the
// page cache only, and this open no longer sees that log. Without the fsync,
// compactions would start on its table, and a power cut afterwards could bring
// the log back beside tables they have cut. One directory fsync per open.
//
// Relies on the version order: DB.RunValueLogGC, DB.Load, the stream writer,
// DB.BanNamespace and managed mode write at versions they choose. Under a
// vouched cut they are refused; managed mode skips the removal (not the
// fsync); hoardDB's other stores call none of them (DropPrefix keeps versions).
func (db *DB) removeLogsOfNamedTables() error {
	if db.opt.InMemory {
		return nil
	}
	var removed []string
	var tablesHighest uint64
	if !db.opt.managedTxns && len(db.imm) > 0 {
		for _, info := range db.Tables() {
			tablesHighest = max(tablesHighest, info.MaxVersion)
		}
		kept := db.imm[:0]
		for _, mt := range db.imm {
			if mt.maxVersion > tablesHighest {
				kept = append(kept, mt)
				continue
			}
			if err := retireRemoveLog(mt.wal.path); err != nil && !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("while removing the memtable log %s, whose table the MANIFEST already names: %w", mt.wal.path, err)
			}
			mt.logRetired.Store(true)
			removed = append(removed, mt.wal.path)
			mt.DecrRef()
		}
		db.imm = kept
	}
	if err := retireSyncDir(db.opt.Dir); err != nil {
		return fmt.Errorf("while syncing the directory %s before compactions start (memtable logs this open removed, "+
			"whose tables the MANIFEST already names: %v): %w", db.opt.Dir, removed, err)
	}
	if len(removed) > 0 {
		db.vouchedReport.LogsOfNamedTables = len(removed)
		db.opt.Infof("Removed %d memtable logs whose tables the MANIFEST already names (at or below version %d): %v",
			len(removed), tablesHighest, removed)
	}
	return nil
}

// flushTestHook is set only by tests (SetFlushTestHook).
var flushTestHook atomic.Pointer[func(*DB)]

// SetFlushTestHook installs a function DB.flushMemtable calls with its DB
// before it flushes each memtable, and returns the function that puts the
// previous hook back. A hook that blocks holds that DB's flusher in front of
// the flush: the memtable stays in db.imm and its log stays on disk, the only
// copy of its commits. The hook is package-wide; a test that sets it compares
// the DB (DB.Opts().Dir) and returns at once for any other. Production code
// must not call it. hoardDB.
func SetFlushTestHook(hook func(db *DB)) (restore func()) {
	var next *func(*DB)
	if hook != nil {
		next = &hook
	}
	previous := flushTestHook.Swap(next)
	return func() { flushTestHook.Store(previous) }
}

// openCompactionsHook is called by Open, with the store directory, right
// before it starts the compactions; tests replace it to see what has happened
// by then (the open step's directory fsync must be before it).
var openCompactionsHook = func(dir string) {}

// FlushStep names a point inside a flush at which FlushStepHook is called. The
// points are the ones a crash can land between, in the order the flush takes
// them (DB.flushToNamedTable).
type FlushStep string

// The steps of a flush to disk, in order.
const (
	// FlushTableSynced: the table file is written and synced; the MANIFEST does
	// not name it and the memtable's log is on disk.
	FlushTableSynced FlushStep = "table synced"
	// FlushTableNamed: the MANIFEST change naming the table is synced; the log
	// is still on disk.
	FlushTableNamed FlushStep = "table named"
	// FlushLogUnlinked: the log is unlinked; the directory is not yet fsynced.
	FlushLogUnlinked FlushStep = "log unlinked"
	// FlushDirectorySynced: the directory is fsynced after the unlink; the
	// table is in no level yet.
	FlushDirectorySynced FlushStep = "directory synced"
	// FlushTableInLevelZero: the table is in level 0, where a compaction can
	// pick it; the flush is complete except for advancing db.imm.
	FlushTableInLevelZero FlushStep = "table in level 0"
)

// flushStepHook is set only by tests (SetFlushStepHook).
var flushStepHook atomic.Pointer[func(step FlushStep, db *DB)]

// SetFlushStepHook installs a function the flusher calls, on its own
// goroutine, at each FlushStep of a flush to disk, and returns the function
// that puts the previous hook back. The flusher does nothing else while the
// hook runs, so a test can copy the store directory at exactly that point: the
// images of every instant a crash can land at. The hook is package-wide; a
// test compares DB.Opts().Dir and returns at once for any other store.
// Production code must not call it. hoardDB.
func SetFlushStepHook(hook func(step FlushStep, db *DB)) (restore func()) {
	var next *func(step FlushStep, db *DB)
	if hook != nil {
		next = &hook
	}
	previous := flushStepHook.Swap(next)
	return func() { flushStepHook.Store(previous) }
}

func (db *DB) flushStep(step FlushStep) {
	if hook := flushStepHook.Load(); hook != nil {
		(*hook)(step, db)
	}
}

// closeRetired releases a log whose name the flush already removed
// (DB.retireMemTableLog): it unmaps the file, drops its pages (an unlinked
// file needs no writeback) and closes it.
func (lf *logFile) closeRetired() error {
	if lf.Fd == nil {
		return nil
	}
	if err := z.Munmap(lf.Data); err != nil {
		return fmt.Errorf("while munmap file: %s, error: %w", lf.Fd.Name(), err)
	}
	lf.Data = nil
	if err := lf.Fd.Truncate(0); err != nil {
		return fmt.Errorf("while truncate file: %s, error: %w", lf.Fd.Name(), err)
	}
	if err := lf.Fd.Close(); err != nil {
		return fmt.Errorf("while close file: %s, error: %w", lf.Fd.Name(), err)
	}
	return nil
}
