/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Added by the hoardDB project, 2026-11-19: Close closes the table of a
 * flush whose MANIFEST change failed (hoardDB's BUG-0868). See
 * hoardDB/third_party/README.md.
 */

package badger

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// openTableFilesIn lists the files of dir ending in .sst that this process holds open.
func openTableFilesIn(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	require.NoError(t, err)
	var open []string
	for _, entry := range entries {
		target, err := os.Readlink(filepath.Join("/proc/self/fd", entry.Name()))
		if err != nil {
			continue // a descriptor closed while the directory was read
		}
		if strings.HasPrefix(target, dir+string(os.PathSeparator)) && strings.HasSuffix(target, ".sst") {
			open = append(open, target)
		}
	}
	return open
}

// TestCloseClosesTheTableOfAFailedNaming: [GUARD Close closes and keeps the table a flush kept when
// its MANIFEST change failed; red when DB.releaseNamedTables skips it, leaving its file open past
// Close] (review R2 F-9, mutation X7). The file itself stays for the next open.
func TestCloseClosesTheTableOfAFailedNaming(t *testing.T) {
	dir := t.TempDir()
	opts := getTestOptions(dir).WithMemTableSize(1 << 20).WithValueThreshold(1 << 10)
	db, err := Open(opts)
	require.NoError(t, err)
	failures := failManifestSyncOnce(t)
	fillUntilRotated(t, db, "kept")
	deadline := time.Now().Add(30 * time.Second)
	for failures.Load() == 0 || db.flushRefusal.Load() == nil {
		require.True(t, time.Now().Before(deadline), "SCENARIO NOT REACHED: the flush did not fail and refuse writes")
		time.Sleep(10 * time.Millisecond)
	}
	require.NotEmpty(t, openTableFilesIn(t, dir), "SCENARIO NOT REACHED: no kept table is open before Close")
	t.Logf("Close after the failed sync: %v", db.Close())
	require.Empty(t, openTableFilesIn(t, dir), "Close left the kept table open")
	tables, err := filepath.Glob(filepath.Join(dir, "*.sst"))
	require.NoError(t, err)
	require.Len(t, tables, 1, "Close did not keep the table on disk")
}

// recordingLogger keeps the lines Badger logs at error level.
type recordingLogger struct {
	mu     sync.Mutex
	errors []string
}

func (l *recordingLogger) Errorf(format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.errors = append(l.errors, fmt.Sprintf(format, args...))
}
func (l *recordingLogger) Warningf(string, ...interface{}) {}
func (l *recordingLogger) Infof(string, ...interface{})    {}
func (l *recordingLogger) Debugf(string, ...interface{})   {}

func (l *recordingLogger) lines() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.errors...)
}

// TestAFailedNamingIsLoggedOnceAndNotAsARetry: [GUARD a flush whose MANIFEST change failed is
// logged once, saying it is not retried until the store is reopened, and writes are refused with
// ErrFlushNotNamed in the chain; red when the flusher logs 'retrying' once a second for a flush it
// does not retry] (review R2 F-9).
func TestAFailedNamingIsLoggedOnceAndNotAsARetry(t *testing.T) {
	dir := t.TempDir()
	logger := &recordingLogger{}
	opts := getTestOptions(dir).WithMemTableSize(1 << 20).WithValueThreshold(1 << 10).WithLogger(logger)
	db, err := Open(opts)
	require.NoError(t, err)
	defer func() { t.Logf("Close: %v", db.Close()) }()
	failures := failManifestSyncOnce(t)
	fillUntilRotated(t, db, "kept")
	deadline := time.Now().Add(30 * time.Second)
	for failures.Load() == 0 || db.flushRefusal.Load() == nil {
		require.True(t, time.Now().Before(deadline), "SCENARIO NOT REACHED: the flush did not fail and refuse writes")
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(2 * flushRetryInterval)
	var named, retrying int
	for _, line := range logger.lines() {
		if strings.Contains(line, "not retried until the store is reopened") {
			named++
		}
		if strings.Contains(line, "retrying") {
			retrying++
		}
	}
	require.Equal(t, 1, named, "the failed naming was not logged once: %q", logger.lines())
	require.Zero(t, retrying, "the flusher says it is retrying a flush it does not retry: %q", logger.lines())
	refusal := db.WriteRefusal()
	require.ErrorIs(t, refusal, ErrWritesRefused)
	require.ErrorIs(t, refusal, ErrFlushNotNamed)
}
