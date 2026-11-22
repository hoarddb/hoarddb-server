//go:build linux

/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Added by the hoardDB project, 2026-11-18: BUG-0858's fix under a reader and
 * a parked sync (change 12, the flushed log's retirement). See
 * hoardDB/third_party/README.md.
 */

package badger

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// openDescriptorsOf returns the targets of this process's descriptors that
// name path (an unlinked file reads "<path> (deleted)").
func openDescriptorsOf(t *testing.T, path string) []string {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	require.NoError(t, err)
	var open []string
	for _, entry := range entries {
		target, err := os.Readlink(filepath.Join("/proc/self/fd", entry.Name()))
		if err == nil && strings.HasPrefix(target, path) {
			open = append(open, target)
		}
	}
	return open
}

// TestARetiredLogServesItsReaderAndAParkedSync retires a memtable's log while
// a reader holds the memtable and a DB.SyncVouched that captured it is parked
// before its msyncs. The sync then msyncs the unlinked mapping and succeeds,
// the reader reads every key, and once both have let go the file is released:
// no descriptor of this process names it.
func TestARetiredLogServesItsReaderAndAParkedSync(t *testing.T) {
	for _, open := range retireOpens {
		t.Run(open.name, func(t *testing.T) {
			dir := t.TempDir()
			db, err := Open(open.tune(cutTestOptions(dir)))
			require.NoError(t, err)
			closeAtCleanup(t, db)
			first := activeMemTable(db)
			logPath := first.wal.path
			for i := range 8 {
				setKey(t, db, fmt.Sprintf("held-%d", i))
			}

			letGo := holdFlushesOf(t, dir)
			require.NoError(t, commitUntilRotatedFrom(db, first, "rotate"))
			reader := db.NewTransaction(false)
			pin := reader.NewIterator(DefaultIteratorOptions)

			parked, resume := make(chan struct{}), make(chan struct{})
			restore := SetSyncTestHooks(SyncTestHooks{AfterCapture: func() {
				close(parked)
				<-resume
			}})
			defer restore()
			synced := make(chan error, 1)
			go func() {
				_, err := db.SyncVouched()
				synced <- err
			}()
			<-parked
			letGo()
			waitUntilNoImmutables(t, db)
			_, statErr := os.Stat(logPath)
			require.ErrorIs(t, statErr, os.ErrNotExist, "the flushed log is still on disk while a reader and a sync hold its memtable")
			close(resume)
			require.NoError(t, <-synced, "a sync that captured the memtable failed after its log was removed")

			found := 0
			for pin.Seek([]byte("held-")); pin.ValidForPrefix([]byte("held-")); pin.Next() {
				found++
			}
			require.Equal(t, 8, found, "the reader of the retired memtable lost keys")
			pin.Close()
			reader.Discard()
			require.Empty(t, openDescriptorsOf(t, logPath), "the retired log is still open after its last reference went")
		})
	}
}
