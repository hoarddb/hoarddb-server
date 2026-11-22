//go:build linux

/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Added by the hoardDB project, 2026-10-26. See hoardDB/third_party/README.md.
 */

package table

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/dgraph-io/badger/v4/options"
)

// TestTableWritebackLeavesNoDirtyPagesOnARealDisk runs the real
// sync_file_range calls, which the other tests replace: once CreateTable
// returns, no page of the new table may be dirty or under writeback, for a
// table that does not end on a step boundary. It cannot cut power, so it asks
// the kernel (cachestat(2)), as the DB.Sync patch tests do. It does not show
// that the file's metadata is durable: that is the MANIFEST fsync's job.
func TestTableWritebackLeavesNoDirtyPagesOnARealDisk(t *testing.T) {
	dir := t.TempDir()
	var fsStat unix.Statfs_t
	require.NoError(t, unix.Statfs(dir, &fsStat))
	if fsStat.Type == unix.TMPFS_MAGIC || fsStat.Type == unix.RAMFS_MAGIC {
		t.Skip("tmpfs never writes back, so its pages stay dirty; set TMPDIR to a disk")
	}

	for _, stepSize := range []int{0, 1 << 20, 256 << 10} {
		tbl, err := createTableOfMiB(t, dir, 9, Options{SyncMode: options.TableSyncStepwise, SyncStepSize: stepSize})
		require.NoError(t, err, "step %d", stepSize)
		require.NotZero(t, len(tbl.Data)%(256<<10), "the table ends on a step boundary")

		var pageCache unix.Cachestat_t
		err = unix.Cachestat(uint(tbl.Fd.Fd()), &unix.CachestatRange{}, &pageCache, 0)
		if errors.Is(err, unix.ENOSYS) {
			t.Skip("cachestat(2) needs Linux 6.5 or later")
		}
		require.NoError(t, err)
		require.Zero(t, pageCache.Dirty, "step %d: dirty pages left in %s after CreateTable", stepSize, tbl.Fd.Name())
		require.Zero(t, pageCache.Writeback, "step %d: pages under writeback in %s after CreateTable", stepSize, tbl.Fd.Name())

		require.NoError(t, tbl.DecrRef())
		_, err = os.Stat(tbl.Filename())
		require.True(t, os.IsNotExist(err), "the table file was not removed")
	}
}

// TestTableWritebackFailureLeavesNoFileOrMapping checks that a CreateTable
// whose sync failed releases what it opened: the flush retries the table, and
// each attempt that keeps a descriptor and a mapping of the half-written file
// leaks both until the process ends. The file is removed too, so the next
// open does not depend on revertToManifest to clear it. When the removal
// itself fails, the sync error is still the one returned, with the removal's
// error alongside.
func TestTableWritebackFailureLeavesNoFileOrMapping(t *testing.T) {
	tests := []struct {
		name         string
		removalFails bool
		wantFileGone bool
	}{
		{"removed", false, true},
		{"removal refused", true, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.removalFails && os.Geteuid() == 0 {
				t.Skip("root removes a file from a read-only directory")
			}
			dir := t.TempDir()
			recorder := &syncRecorder{startErr: fmt.Errorf("sync_file_range: %w", syscall.EIO), failFromStart: 2}
			if tt.removalFails {
				// After the file was created and before the sync fails.
				recorder.beforeStartErr = func() { require.NoError(t, os.Chmod(dir, 0o555)) }
				t.Cleanup(func() { require.NoError(t, os.Chmod(dir, 0o755)) })
			}
			recordTableSync(t, recorder, true)

			_, err := createTableOfMiB(t, dir, 9, Options{SyncMode: options.TableSyncStepwise, SyncStepSize: 1 << 20})
			require.Error(t, err)
			require.True(t, errors.Is(err, syscall.EIO), "the sync error is masked: %v", err)
			if tt.removalFails {
				require.True(t, errors.Is(err, fs.ErrPermission), "the failed removal is not reported: %v", err)
			}

			tablePath := NewFilename(1, dir)
			require.Empty(t, openDescriptorsOf(t, tablePath), "a descriptor of %s is still open", tablePath)
			require.Empty(t, mappingsOf(t, tablePath), "%s is still mapped", tablePath)
			_, statErr := os.Stat(tablePath)
			require.Equal(t, tt.wantFileGone, os.IsNotExist(statErr), "file present: %v", statErr)
		})
	}
}

// openDescriptorsOf lists this process's descriptors open on path.
func openDescriptorsOf(t *testing.T, path string) []string {
	entries, err := os.ReadDir("/proc/self/fd")
	require.NoError(t, err)
	var open []string
	for _, entry := range entries {
		target, err := os.Readlink(filepath.Join("/proc/self/fd", entry.Name()))
		if err != nil {
			continue // closed between ReadDir and Readlink (the ReadDir's own descriptor)
		}
		if strings.TrimSuffix(target, " (deleted)") == path {
			open = append(open, entry.Name())
		}
	}
	return open
}

// mappingsOf lists this process's memory mappings of path.
func mappingsOf(t *testing.T, path string) []string {
	maps, err := os.ReadFile("/proc/self/maps")
	require.NoError(t, err)
	var mapped []string
	for _, line := range strings.Split(string(maps), "\n") {
		if strings.HasSuffix(strings.TrimSuffix(line, " (deleted)"), " "+path) {
			mapped = append(mapped, line)
		}
	}
	return mapped
}
