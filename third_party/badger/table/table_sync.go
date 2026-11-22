/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Added by the hoardDB project, 2026-10-26: how CreateTable makes a new table
 * file durable. See hoardDB/third_party/README.md.
 */

package table

import (
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"syscall"

	"github.com/dgraph-io/badger/v4/options"
	"github.com/dgraph-io/ristretto/v2/z"
)

// DefaultTableSyncStepSize is the writeback step of options.TableSyncStepwise
// when Options.SyncStepSize is not set: 4 MiB measured the lowest write p99 of
// the step sizes tried (see the table-writeback patch decision record in the
// hoardDB repository).
const DefaultTableSyncStepSize = 4 << 20

// tableSyncCalls are the system calls CreateTable uses to make a new table
// file durable. They are a variable so a test can watch them.
type tableSyncCalls struct {
	// msync is the one blocking whole-file sync (upstream's z.Msync).
	msync func(mapped []byte) error
	// startWriteback queues the dirty pages of [offset, offset+length) for
	// writeback and does not wait for them. Nil where the platform has no
	// ranged writeback.
	startWriteback func(fd uintptr, offset, length int64) error
	// waitWriteback writes back [offset, offset+length) and waits until every
	// page of it has reached the device. Nil with startWriteback.
	waitWriteback func(fd uintptr, offset, length int64) error
}

// tableSync is platformTableSync except in tests.
var tableSync = platformTableSync

// rangedWritebackRefused is set once the kernel refused sync_file_range
// (isRangedWritebackRefusal): the refusal comes from the kernel or the
// sandbox, not from one table, so later tables in this process go straight to
// the msync instead of making a call that will be refused again.
var rangedWritebackRefused atomic.Bool

// isRangedWritebackRefusal reports whether err is the kernel refusing ranged
// writeback outright rather than failing it: ENOSYS (no such call), EPERM (a
// seccomp profile that filters it with an errno), EINVAL or EOPNOTSUPP (a
// kernel or filesystem that does not take it). The kernel returns each of
// these before it writes anything back or reads the file's writeback error,
// so the msync that follows sees any writeback error still unreported.
func isRangedWritebackRefusal(err error) bool {
	return errors.Is(err, syscall.ENOSYS) || errors.Is(err, syscall.EPERM) ||
		errors.Is(err, syscall.EINVAL) || errors.Is(err, syscall.EOPNOTSUPP)
}

// tableSyncFixHint is appended to a stepwise writeback error. The setting is
// hoardDB's (storage/badger.go maps it onto Options.TableSyncMode).
const tableSyncFixHint = "if sync_file_range(2) is what fails on this system, set storage.badger_table_sync to msync " +
	"(or HOARDDB_BADGER_TABLE_SYNC=msync) and restart to sync tables with one msync each; " +
	"an I/O error (EIO) more likely means the disk is failing, which msync will not fix"

// syncNewTable makes the table file mf durable as opts.SyncMode asks, before
// CreateTable returns and so before the MANIFEST can name the table.
//
// TableSyncStepwise queues the file for writeback a step at a time, so the
// device is fed in pieces instead of all at once, then waits once for the
// whole range. It leaves the file's metadata (size, block allocation, the
// directory entry) to the journal: every caller fsyncs the MANIFEST before a
// table is used or its inputs deleted, and on ext4 or XFS that fsync commits
// the journal up to and including this file's metadata, with a cache flush
// ahead of it that covers the data written back here. The one msync it
// replaces committed the journal itself, once per table, and on ext4 each
// commit stalled every write behind it.
func syncNewTable(mf *z.MmapFile, opts *Options, calls tableSyncCalls) error {
	switch opts.SyncMode {
	case options.TableSyncMsync:
		return calls.msync(mf.Data)
	case options.TableSyncStepwise:
		if calls.startWriteback == nil || rangedWritebackRefused.Load() {
			return calls.msync(mf.Data)
		}
		err := writeBackInSteps(mf.Fd.Fd(), int64(len(mf.Data)), int64(opts.SyncStepSize), calls)
		// A refusal after some steps succeeded still falls back: the msync
		// covers the whole mapping and waits for pages already under
		// writeback, so the steps before it change nothing it guarantees.
		if isRangedWritebackRefusal(err) {
			rangedWritebackRefused.Store(true)
			return calls.msync(mf.Data)
		}
		// Anything else, EIO above all, is not retried with the msync: a
		// failed wait has handed the file's writeback error to this call, and
		// an msync after it could report success over pages that never reached
		// the device. A failed step is treated the same way, to be safe.
		if err != nil {
			return fmt.Errorf("%w (%s)", err, tableSyncFixHint)
		}
		return nil
	default:
		return fmt.Errorf("unknown table sync mode %d (want options.TableSyncStepwise or options.TableSyncMsync)", opts.SyncMode)
	}
}

// writeBackInSteps starts writeback of [0, tableBytes) in ranges of at most
// stepBytes, then waits for the whole range. The waits are not per step:
// waiting on each 256 KiB step measured a p99 of 81 ms against 46 ms for 4 MiB
// steps and one wait.
func writeBackInSteps(fd uintptr, tableBytes, stepBytes int64, calls tableSyncCalls) error {
	if stepBytes <= 0 {
		stepBytes = DefaultTableSyncStepSize
	}
	for offset := int64(0); offset < tableBytes; offset += stepBytes {
		length := min(stepBytes, tableBytes-offset)
		if err := calls.startWriteback(fd, offset, length); err != nil {
			return fmt.Errorf("starting writeback of bytes %d-%d of %d: %w", offset, offset+length, tableBytes, err)
		}
	}
	// A length of zero would mean "to the end of the file"; an empty table
	// has nothing to wait for.
	if tableBytes == 0 {
		return nil
	}
	if err := calls.waitWriteback(fd, 0, tableBytes); err != nil {
		return fmt.Errorf("waiting for writeback of %d bytes: %w", tableBytes, err)
	}
	return nil
}

// discardUnsyncedTable unmaps, closes and removes a table file whose sync
// failed, so a flush that retries the table does not keep a descriptor and a
// mapping per attempt. It does not use mf.Close, which msyncs first and, when
// that fails (as it can after a failed writeback), returns with the mapping
// and the descriptor still open. No MANIFEST names the file yet, so removing
// it loses nothing; if the removal fails, the next open removes the file as
// one the MANIFEST does not name (revertToManifest).
func discardUnsyncedTable(mf *z.MmapFile) error {
	path := mf.Fd.Name()
	var unmapErr, closeErr, removeErr error
	if err := z.Munmap(mf.Data); err != nil {
		unmapErr = fmt.Errorf("unmapping unsynced table %s: %w", path, err)
	}
	mf.Data = nil
	if err := mf.Fd.Close(); err != nil {
		closeErr = fmt.Errorf("closing unsynced table %s: %w", path, err)
	}
	if err := os.Remove(path); err != nil {
		removeErr = fmt.Errorf("removing unsynced table %s (the next open removes it): %w", path, err)
	}
	return errors.Join(unmapErr, closeErr, removeErr)
}
