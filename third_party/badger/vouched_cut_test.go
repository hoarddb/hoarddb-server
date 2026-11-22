/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Added by the hoardDB project, 2026-11-17: the tests of the vouched cut
 * (WithVouchedCut, change 12) that do not depend on the page cache, and the
 * image helper the cut's tests share. See hoardDB/third_party/README.md.
 */

package badger

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// storeImage is the bytes of every regular file of a store directory as of
// one instant, copied at that instant. An image of what a power cut can leave
// is assembled from such copies, file by file, and never by rewriting a file:
// a test that synthesised a log or a value log from what it remembers would
// test its own model, not the store.
type storeImage map[string][]byte

// imageAt copies every regular file of dir now. The MANIFEST is copied first,
// for copyDirImage's reason: a table is created and synced before the MANIFEST
// names it, so every table the copied MANIFEST names is already on disk when
// the directory is listed.
//
// A running store removes a memtable log when its flush finishes, so a log listed a moment
// ago can be gone when its turn comes. An image without it, taken with a MANIFEST copied
// before the flush, would lack the log and the table both: not a state a crash leaves. The
// copy is then repeated from the MANIFEST, which by then names the flushed table.
func imageAt(t *testing.T, dir string) storeImage {
	t.Helper()
	const attempts = 100
	for range attempts {
		if image, ok := imageOnce(t, dir); ok {
			return image
		}
	}
	require.FailNow(t, "SCENARIO NOT REACHED", "a file of %s vanished during every one of %d copies", dir, attempts)
	return nil
}

// imageOnce is one pass of imageAt; ok is false when a listed file was gone when it was read.
func imageOnce(t *testing.T, dir string) (image storeImage, ok bool) {
	t.Helper()
	image = storeImage{}
	read := func(name string) bool {
		content, err := os.ReadFile(filepath.Join(dir, name))
		if errors.Is(err, os.ErrNotExist) && name != ManifestFilename {
			return false
		}
		require.NoError(t, err, "copying %s into an image", name)
		image[name] = content
		return true
	}
	read(ManifestFilename)
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, entry := range entries {
		if !entry.Type().IsRegular() || entry.Name() == ManifestFilename || entry.Name() == lockFile {
			continue
		}
		if !read(entry.Name()) {
			return nil, false
		}
	}
	return image, true
}

// only returns the files of the image whose names keep accepts.
func (image storeImage) only(keep func(name string) bool) storeImage {
	kept := storeImage{}
	for name, content := range image {
		if keep(name) {
			kept[name] = content
		}
	}
	return kept
}

func isValueLogFile(name string) bool { return strings.HasSuffix(name, ".vlog") }

// writeImage writes base's files into a new directory, each file of a later
// override replacing the one of the same name, and returns the directory.
func writeImage(t *testing.T, base storeImage, overrides ...storeImage) string {
	t.Helper()
	dir := t.TempDir()
	merged := storeImage{}
	for _, image := range append([]storeImage{base}, overrides...) {
		for name, content := range image {
			merged[name] = content
		}
	}
	for name, content := range merged {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), content, 0o600))
	}
	return dir
}

// cutTestOptions are small memtables and a small value threshold, as
// openSyncLockTestDB's, in dir. Level 0 stalls at two tables and nothing
// compacts, so once holdTheFlusher has run a rotated memtable stays in db.imm
// and its log stays on disk.
func cutTestOptions(dir string) Options {
	return getTestOptions(dir).
		WithMemTableSize(1 << 20).
		WithValueThreshold(1 << 10).
		WithValueLogFileSize(1 << 20).
		WithNumCompactors(0).
		WithNumLevelZeroTables(1).
		WithNumLevelZeroTablesStall(2)
}

// commitBatch commits 64 inline 512-byte values under prefix in one
// transaction.
func commitBatch(db *DB, prefix string) error {
	return db.Update(func(txn *Txn) error {
		for i := range 64 {
			if err := txn.Set([]byte(fmt.Sprintf("%s-%02d", prefix, i)), make([]byte, 512)); err != nil {
				return err
			}
		}
		return nil
	})
}

func activeMemTable(db *DB) *memTable {
	db.lock.RLock()
	defer db.lock.RUnlock()
	return db.mt
}

// commitUntilRotatedFrom commits batches until mt is no longer the active
// memtable. The batch that rotated it is in the new active memtable.
func commitUntilRotatedFrom(db *DB, mt *memTable, prefix string) error {
	for batch := 0; activeMemTable(db) == mt; batch++ {
		if batch > 1000 {
			return errors.New("the memtable never rotated")
		}
		if err := commitBatch(db, fmt.Sprintf("%s-%04d", prefix, batch)); err != nil {
			return err
		}
	}
	return nil
}

// holdTheFlusher flushes memtables until level 0 holds two tables, then
// rotates one more memtable and waits until the flusher has taken it. With
// the options of cutTestOptions that flush waits at the level-0 stall, after
// it has written its table and named it in the MANIFEST
// (levelsController.addLevel0Table), so every memtable rotated after this
// returns waits in the flush channel and keeps its log as the only copy of
// its commits.
func holdTheFlusher(t *testing.T, db *DB) {
	t.Helper()
	waitUntil := func(what string, done func() bool) {
		deadline := time.Now().Add(30 * time.Second)
		for !done() {
			require.True(t, time.Now().Before(deadline), "%s did not happen within 30 s", what)
			time.Sleep(10 * time.Millisecond)
		}
	}
	immutables := func() int {
		db.lock.RLock()
		defer db.lock.RUnlock()
		return len(db.imm)
	}
	for batch := 0; db.lc.levels[0].numTables() < 2; batch++ {
		require.NoError(t, commitUntilRotatedFrom(db, activeMemTable(db), fmt.Sprintf("hold-%04d", batch)))
		waitUntil("a memtable flush", func() bool { return immutables() == 0 })
	}
	require.NoError(t, commitUntilRotatedFrom(db, activeMemTable(db), "held"))
	waitUntil("the flusher taking the held memtable", func() bool { return len(db.flushChan) == 0 })
}

func setKey(t *testing.T, db *DB, key string) {
	t.Helper()
	require.NoError(t, db.Update(func(txn *Txn) error { return txn.Set([]byte(key), []byte("value of "+key)) }))
}

// hasKey reports whether key is visible in db; any error but a miss fails the
// test.
func hasKey(t *testing.T, db *DB, key string) bool {
	t.Helper()
	err := db.View(func(txn *Txn) error {
		_, err := txn.Get([]byte(key))
		return err
	})
	if errors.Is(err, ErrKeyNotFound) {
		return false
	}
	require.NoError(t, err, "reading %s", key)
	return true
}

// TestTheCutIsDurableBeforeTheFirstCommit checks that the cut's truncation and
// removals are on disk before Open returns: the truncated log is fsynced
// after its truncation, the directory after the removal of the log the cut
// emptied. The versions the cut discards are handed out again, so if the old
// bytes survived a crash right after the open, a later, higher vouched version
// would vouch them (I-2). A seam test: cutSyncFile and cutSyncDir are
// replaced by recorders, as manifest.go's syncFunc is in its tests; it does
// not cut the power.
func TestTheCutIsDurableBeforeTheFirstCommit(t *testing.T) {
	dir := t.TempDir()
	opt := cutTestOptions(dir).WithVouchedCut(0)
	db, err := Open(opt)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	holdTheFlusher(t, db)

	setKey(t, db, "vouched")
	vouched, err := db.SyncVouched()
	require.NoError(t, err)
	// The first log keeps "vouched" and loses everything after it; the second
	// holds only commits above the vouched version, so the cut empties it.
	cutLog := activeMemTable(db)
	setKey(t, db, "cut-from-the-first-log")
	require.NoError(t, commitUntilRotatedFrom(db, cutLog, "rotate"))
	removedLog := activeMemTable(db)
	setKey(t, db, "in-the-removed-log")
	cutLogName, removedLogName := filepath.Base(cutLog.wal.Fd.Name()), filepath.Base(removedLog.wal.Fd.Name())

	image := imageAt(t, dir)
	require.Contains(t, image, cutLogName, "SCENARIO NOT REACHED: the first log is not in the image (it was flushed)")
	require.Contains(t, image, removedLogName)
	imageDir := writeImage(t, image)

	var events []string
	var openReturned atomic.Bool
	var cutLogSizeAtSync int64 = -1
	var removedLogExistedAtDirSync bool
	restoreFile, restoreDir := cutSyncFile, cutSyncDir
	t.Cleanup(func() { cutSyncFile, cutSyncDir = restoreFile, restoreDir })
	cutSyncFile = func(f *os.File) error {
		name := filepath.Base(f.Name())
		events = append(events, fmt.Sprintf("fsync %s (open returned: %v)", name, openReturned.Load()))
		if name == cutLogName {
			info, err := f.Stat()
			if err != nil {
				return err
			}
			cutLogSizeAtSync = info.Size()
		}
		return f.Sync()
	}
	cutSyncDir = func(syncedDir string) error {
		events = append(events, fmt.Sprintf("fsync the directory (open returned: %v)", openReturned.Load()))
		_, err := os.Stat(filepath.Join(syncedDir, removedLogName))
		removedLogExistedAtDirSync = err == nil
		return syncDir(syncedDir)
	}

	reopened, err := Open(opt.WithDir(imageDir).WithValueDir(imageDir).WithVouchedCut(vouched).
		WithNumCompactors(2).WithNumLevelZeroTables(5).WithNumLevelZeroTablesStall(15))
	require.NoError(t, err)
	openReturned.Store(true)
	defer func() { require.NoError(t, reopened.Close()) }()

	require.Equal(t, []string{
		fmt.Sprintf("fsync %s (open returned: false)", cutLogName),
		"fsync the directory (open returned: false)",
	}, events, "the cut must fsync the truncated log and then the directory, both inside Open: "+
		"without them a crash right after the open can bring the discarded commits back under versions handed out again")
	require.Less(t, cutLogSizeAtSync, int64(len(image[cutLogName])),
		"the log %s was fsynced before it was truncated (its size then was %d, %d in the image)",
		cutLogName, cutLogSizeAtSync, len(image[cutLogName]))
	require.False(t, removedLogExistedAtDirSync, "the directory was fsynced before the log %s the cut emptied was removed", removedLogName)
	require.True(t, hasKey(t, reopened, "vouched"))
	require.False(t, hasKey(t, reopened, "cut-from-the-first-log"))
	require.False(t, hasKey(t, reopened, "in-the-removed-log"))
}

// TestTheCutIsOptIn pins I-7 at the store: without WithVouchedCut nothing of
// the cut runs, so every log entry is kept; with it, the calls that write out
// of version order are refused, and ReadOnly, InMemory and managed mode
// refuse the option.
func TestTheCutIsOptIn(t *testing.T) {
	t.Run("without the option every log entry is kept", func(t *testing.T) {
		dir := t.TempDir()
		opt := cutTestOptions(dir)
		db, err := Open(opt)
		require.NoError(t, err)
		defer func() { require.NoError(t, db.Close()) }()
		setKey(t, db, "synced")
		require.NoError(t, db.Sync())
		setKey(t, db, "unsynced")
		imageDir := writeImage(t, imageAt(t, dir))

		reopened, err := Open(opt.WithDir(imageDir).WithValueDir(imageDir))
		require.NoError(t, err)
		defer func() { require.NoError(t, reopened.Close()) }()
		require.True(t, hasKey(t, reopened, "synced"))
		require.True(t, hasKey(t, reopened, "unsynced"), "a store opened without WithVouchedCut lost a log entry")
		require.Equal(t, VouchedCutReport{}, reopened.VouchedCutReport())
	})

	refused := []struct {
		call string
		run  func(db *DB) error
	}{
		{"DB.RunValueLogGC", func(db *DB) error { return db.RunValueLogGC(0.5) }},
		{"DB.Load", func(db *DB) error { return db.Load(bytes.NewReader(nil), 1) }},
		{"DB.DropAll", func(db *DB) error { return db.DropAll() }},
		{"DB.DropPrefix", func(db *DB) error { return db.DropPrefix([]byte("synced")) }},
		// Cancel releases what a Prepare that was not refused took (writes
		// blocked, compactions stopped); it is a no-op after a refusal.
		{"StreamWriter.Prepare", func(db *DB) error {
			writer := db.NewStreamWriter()
			defer writer.Cancel()
			return writer.Prepare()
		}},
		{"StreamWriter.PrepareIncremental", func(db *DB) error {
			writer := db.NewStreamWriter()
			defer writer.Cancel()
			return writer.PrepareIncremental()
		}},
		{"DB.BanNamespace", func(db *DB) error { return db.BanNamespace(1) }},
	}
	for _, tc := range refused {
		t.Run("refuses "+tc.call, func(t *testing.T) {
			db, err := Open(cutTestOptions(t.TempDir()).WithVouchedCut(0))
			require.NoError(t, err)
			defer func() { require.NoError(t, db.Close()) }()
			setKey(t, db, "synced")

			err = tc.run(db)
			require.ErrorIs(t, err, ErrVouchedCutRefused, "%s under a vouched cut", tc.call)
			require.ErrorContains(t, err, tc.call)
			require.True(t, hasKey(t, db, "synced"), "%s changed the store although it was refused", tc.call)
			setKey(t, db, "after-the-refusal")
		})
	}

	t.Run("managed mode refuses the option", func(t *testing.T) {
		db, err := OpenManaged(cutTestOptions(t.TempDir()).WithVouchedCut(0))
		if err == nil {
			require.NoError(t, db.Close())
		}
		require.ErrorIs(t, err, ErrVouchedCutRefused)
		require.ErrorContains(t, err, "managed mode")
	})
	t.Run("ReadOnly refuses the option", func(t *testing.T) {
		dir := t.TempDir()
		created, err := Open(cutTestOptions(dir))
		require.NoError(t, err)
		require.NoError(t, created.Close())
		db, err := Open(cutTestOptions(dir).WithReadOnly(true).WithVouchedCut(0))
		if err == nil {
			require.NoError(t, db.Close())
		}
		require.ErrorIs(t, err, ErrVouchedCutRefused)
		require.ErrorContains(t, err, "ReadOnly")
	})
	t.Run("InMemory refuses the option", func(t *testing.T) {
		db, err := Open(DefaultOptions("").WithInMemory(true).WithLoggingLevel(WARNING).WithVouchedCut(0))
		if err == nil {
			require.NoError(t, db.Close())
		}
		require.ErrorIs(t, err, ErrVouchedCutRefused)
		require.ErrorContains(t, err, "InMemory")
	})
}

// TestLogFilesCreatedMovesWithEveryNewLogFile: [GUARD every memtable log and
// value-log file the DB creates moves DB.LogFilesCreated, and Open starts it
// above 0; red when a rotation or a new value-log file is not counted]. The
// caller of SyncVouched fsyncs the directory only when the count moved, so a
// file left out of it could lose its directory entry at a power cut while the
// caller's marker vouches its commits (store-vouched-cut slice S-3).
func TestLogFilesCreatedMovesWithEveryNewLogFile(t *testing.T) {
	opt := DefaultOptions(t.TempDir()).WithLogger(nil).WithMemTableSize(1 << 20).
		WithValueLogFileSize(1 << 20).WithValueThreshold(1 << 10)
	db, err := Open(opt)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	atOpen := db.LogFilesCreated()
	require.Positive(t, atOpen, "Open must count the files it found or created as not yet covered")

	// The file ids only grow: the next memtable log's id and the newest value-log file's.
	memFid, vlogFid := db.nextMemFid, db.vlog.maxFid
	// Small values rotate the memtable; values above the threshold fill value-log files.
	for i := 0; i < 2000; i++ {
		require.NoError(t, db.Update(func(txn *Txn) error {
			if err := txn.Set([]byte(fmt.Sprintf("small-%04d", i)), bytes.Repeat([]byte{'s'}, 900)); err != nil {
				return err
			}
			return txn.Set([]byte(fmt.Sprintf("large-%04d", i)), bytes.Repeat([]byte{'l'}, 8<<10))
		}))
	}
	db.lock.Lock()
	createdLogs := db.nextMemFid - memFid
	db.lock.Unlock()
	db.vlog.filesLock.RLock()
	createdVlogs := int(db.vlog.maxFid - vlogFid)
	db.vlog.filesLock.RUnlock()
	require.Positive(t, createdLogs, "SCENARIO NOT REACHED: the memtable never rotated")
	require.Positive(t, createdVlogs, "SCENARIO NOT REACHED: no value-log file was created")
	require.Equal(t, uint64(createdLogs)+uint64(createdVlogs), db.LogFilesCreated()-atOpen,
		"the count did not move once per file created")
}
