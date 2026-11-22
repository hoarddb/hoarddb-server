//go:build linux

/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Added by the hoardDB project, 2026-10-26. See hoardDB/third_party/README.md.
 */

package table

import (
	"fmt"

	"github.com/dgraph-io/ristretto/v2/z"
	"golang.org/x/sys/unix"
)

// platformTableSync writes a table back with sync_file_range(2). The pages
// were dirtied through the file's shared mapping; the kernel tracks those as
// dirty page-cache pages (write-notify on a filesystem with writeback), so a
// ranged writeback covers them as it covers write(2).
var platformTableSync = tableSyncCalls{
	msync:          z.Msync,
	startWriteback: startRangeWriteback,
	waitWriteback:  waitRangeWriteback,
}

func startRangeWriteback(fd uintptr, offset, length int64) error {
	return syncFileRange(fd, offset, length, unix.SYNC_FILE_RANGE_WRITE)
}

// waitRangeWriteback waits for pages already under writeback (started by
// startRangeWriteback), writes the ones still dirty, and waits for those too.
func waitRangeWriteback(fd uintptr, offset, length int64) error {
	return syncFileRange(fd, offset, length,
		unix.SYNC_FILE_RANGE_WAIT_BEFORE|unix.SYNC_FILE_RANGE_WRITE|unix.SYNC_FILE_RANGE_WAIT_AFTER)
}

// syncFileRange keeps the errno matchable: syncNewTable falls back to the
// msync on the ones that mean the call is refused (isRangedWritebackRefusal).
func syncFileRange(fd uintptr, offset, length int64, flags int) error {
	if err := unix.SyncFileRange(int(fd), offset, length, flags); err != nil {
		return fmt.Errorf("sync_file_range: %w", err)
	}
	return nil
}
