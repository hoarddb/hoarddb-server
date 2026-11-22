/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Added by the hoardDB project, 2026-11-17: the vouched cut (WithVouchedCut,
 * DB.SyncVouched, DB.VouchedCutReport, ErrVouchedCutRefused) and its test
 * seams. See hoardDB/third_party/README.md.
 */

package badger

import (
	"errors"
	"fmt"
	"os"

	"github.com/dgraph-io/badger/v4/y"
)

// VouchedCutReport says what the vouched cut did at Open. hoardDB.
type VouchedCutReport struct {
	Vouched       uint64 // the version the store was opened with (WithVouchedCut)
	Kept          uint64 // the highest version kept: the larger of Vouched and the tables' highest version
	DiscardedTxns uint64 // commits above Vouched discarded from the memtable logs
	LogsTruncated int    // memtable logs cut short and fsynced
	LogsRemoved   int    // memtable logs the cut emptied, deleted
	// LogsOfNamedTables counts the memtable logs Open removed, durably,
	// because the MANIFEST already names their tables (a crash inside a
	// flushed log's retirement; DB.removeLogsOfNamedTables). Set with or
	// without WithVouchedCut.
	LogsOfNamedTables int
}

// VouchedCutReport returns what the vouched cut did at Open; for a store
// opened without WithVouchedCut, only LogsOfNamedTables is set. hoardDB.
func (db *DB) VouchedCutReport() VouchedCutReport {
	return db.vouchedReport
}

// ErrVouchedCutRefused is wrapped by the error a call gets when it would
// write out of version order in a store opened with WithVouchedCut, and by
// Open's refusal of the option with ReadOnly, InMemory or managed mode.
// hoardDB.
var ErrVouchedCutRefused = errors.New("refused under a vouched cut")

// vouchedCutRefusal is the error of a call refused under a vouched cut. It
// names the call and says why.
type vouchedCutRefusal struct{ call string }

func (r vouchedCutRefusal) Error() string {
	return "this store was opened with a vouched cut (WithVouchedCut), which keeps commit versions in write order; " +
		r.call + " would write out of that order, so it was refused"
}

func (r vouchedCutRefusal) Unwrap() error { return ErrVouchedCutRefused }

// refuseUnderVouchedCut returns the refusal of call when the store was
// opened with WithVouchedCut, and nil otherwise. The refused calls write
// entries at versions they choose (DB.Load, the value-log GC's rewrite,
// DB.BanNamespace at version 1) or drop data outside any commit
// (DB.DropAll, DB.DropPrefix, the stream writer), so after them a version
// no longer says which commits came before it, and the cut could drop
// vouched data or keep a hole.
func (db *DB) refuseUnderVouchedCut(call string) error {
	if !db.opt.vouchedCut {
		return nil
	}
	return vouchedCutRefusal{call: call}
}

// checkVouchedCutOptions refuses WithVouchedCut where the cut cannot hold:
// a read-only open cannot truncate a log, an in-memory store has none, and
// managed mode lets the caller choose commit versions, so version order is
// no longer write order.
func checkVouchedCutOptions(opt *Options) error {
	if !opt.vouchedCut {
		return nil
	}
	switch {
	case opt.ReadOnly:
		return fmt.Errorf("WithVouchedCut cannot be used with ReadOnly: the cut truncates the memtable logs at open, "+
			"which a read-only open cannot do: %w", ErrVouchedCutRefused)
	case opt.InMemory:
		return fmt.Errorf("WithVouchedCut cannot be used with InMemory: an in-memory store has no memtable logs to cut: %w",
			ErrVouchedCutRefused)
	case opt.managedTxns:
		return vouchedCutRefusal{call: "managed mode (OpenManaged)"}
	}
	return nil
}

// vouchedCutScan is the cut's state while Open replays the memtable logs,
// oldest first (DB.openMemTables). hoardDB.
type vouchedCutScan struct {
	version uint64
	// cutStarted is set once a log, this one or an earlier one, has handed
	// over an entry above version. Versions are written in order, so from
	// there on every entry must be above it.
	cutStarted bool
	// logCut and logCutOffset are the log being replayed: whether it holds an
	// entry above version, and the offset of the first such entry, which is
	// where its transaction starts (logFile.iterate hands a transaction over
	// at its end marker, its first entry carrying the offset it starts at).
	logCut       bool
	logCutOffset uint32
	// logsTruncated and logsRemoved count the logs the cut shortened and
	// those it emptied; discardedTxns counts the commits it left out, one per
	// version, lastDiscarded being the version of the last entry left out.
	logsTruncated, logsRemoved   int
	discardedTxns, lastDiscarded uint64
}

// startLog resets the state of the log being replayed.
func (scan *vouchedCutScan) startLog() {
	scan.logCut, scan.logCutOffset = false, 0
}

// discards reports whether the replay of the log at logPath must leave out
// the entry at offset with the given key: every entry above the vouched
// version is discarded. An entry at or below it after the cut point means
// versions are out of write order (only the calls refused under the cut write
// so), and cutting there would drop vouched data, so it fails the open.
func (scan *vouchedCutScan) discards(key []byte, offset uint32, logPath string) (bool, error) {
	version := y.ParseTs(key)
	if version > scan.version {
		if !scan.logCut {
			scan.logCut, scan.logCutOffset, scan.cutStarted = true, offset, true
		}
		// A transaction's entries share its version and are handed over
		// together, so a new version is a new commit.
		if version != scan.lastDiscarded {
			scan.discardedTxns, scan.lastDiscarded = scan.discardedTxns+1, version
		}
		return true, nil
	}
	if scan.cutStarted {
		return false, fmt.Errorf("the memtable log %s holds version %d after the vouched cut point, where every "+
			"version must be above the vouched version %d: commit versions are out of write order (a call that is "+
			"refused under the cut wrote this store), and cutting here would drop vouched data", logPath, version, scan.version)
	}
	return false, nil
}

// cutSyncFile fsyncs a memtable log the cut truncated (T5 replaces it).
var cutSyncFile = func(f *os.File) error { return f.Sync() }

// cutSyncDir fsyncs the store directory after the cut (T5 replaces it).
var cutSyncDir = syncDir

// syncValueLogHook is set only by tests. DB.SyncVouched calls it after the
// memtable logs' msyncs and before the value log's, so a test can hold a sync
// where a power cut leaves a value-log pointer durable without its bytes.
var syncValueLogHook func()

// syncAfterUnlockHook is set only by tests. DB.SyncVouched calls it right
// after it releases db.lock, once both captures are taken and before any
// msync. Unlike syncCaptureHook, which runs under that lock, a commit can
// return while it runs: every commit takes db.lock.Lock in
// DB.ensureRoomForWrite.
var syncAfterUnlockHook func()

// SyncTestHooks are the test seams of DB.Sync and DB.SyncVouched; a nil
// field is no hook. hoardDB.
type SyncTestHooks struct {
	// AfterCapture runs right after the sync has captured the memtables and
	// released db.lock, before any msync. A commit started here can return,
	// and the memtable can rotate, while the sync waits.
	AfterCapture func()
	// BeforeValueLogSync runs after the memtable logs' msyncs and before the
	// value log's.
	BeforeValueLogSync func()
	// AfterValueLogFileSync runs after every msync of a value-log file that
	// succeeded, by any caller (DB.Sync, a memtable flush, a file's rotation),
	// on the caller's goroutine, with the file's path and covered, the length
	// of its prefix that msync made durable (written before the msync began;
	// math.MaxInt64 for the whole file, from the fsync Open makes before it
	// flushes replayed memtables).
	// A test that builds the image of a power cut takes each value-log file
	// at its covered prefix. It must not sync or write the store.
	AfterValueLogFileSync func(path string, covered int64)
}

// SetSyncTestHooks installs the hooks and returns the function that puts the
// previous ones back. It is a test seam, exported only because tests in other
// packages (hoardDB's storage and cluster tests) need it; production code
// must not call it. The hooks are package-wide, so a test that sets them must
// not run in parallel with another that syncs. hoardDB.
func SetSyncTestHooks(h SyncTestHooks) (restore func()) {
	previousAfterCapture, previousBeforeValueLogSync := syncAfterUnlockHook, syncValueLogHook
	syncAfterUnlockHook, syncValueLogHook = h.AfterCapture, h.BeforeValueLogSync
	var fileSynced *func(path string, covered int64)
	if h.AfterValueLogFileSync != nil {
		fileSynced = &h.AfterValueLogFileSync
	}
	previousFileSynced := valueLogFileSyncedHook.Swap(fileSynced)
	return func() {
		syncAfterUnlockHook, syncValueLogHook = previousAfterCapture, previousBeforeValueLogSync
		valueLogFileSyncedHook.Store(previousFileSynced)
	}
}
