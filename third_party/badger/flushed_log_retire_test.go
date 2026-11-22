/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Added by the hoardDB project, 2026-11-18: the tests of BUG-0858's fix
 * (change 12, the flushed log's retirement). A flush removes its memtable's
 * log, durably, after the MANIFEST names the table and before a compaction
 * can see the table; Open removes a log whose table the MANIFEST already
 * names instead of replaying it. See hoardDB/third_party/README.md.
 */

package badger

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// retireOpens are the two opens the retirement applies to: with a vouched
// cut (the ring store) and without (the legacy store, OpenBadgerKV).
var retireOpens = []struct {
	name string
	tune func(Options) Options
}{
	{"vouched", func(o Options) Options { return o.WithVouchedCut(0) }},
	{"legacy", func(o Options) Options { return o }},
}

// namedButNotVisible returns the ids of the tables the MANIFEST names that
// are in no level: a flush's table between its MANIFEST change and its
// insertion into level 0.
func namedButNotVisible(db *DB) []uint64 {
	visible := map[uint64]bool{}
	for _, table := range db.Tables() {
		visible[table.ID] = true
	}
	db.manifest.appendLock.Lock()
	defer db.manifest.appendLock.Unlock()
	var ids []uint64
	for id := range db.manifest.manifest.Tables {
		if !visible[id] {
			ids = append(ids, id)
		}
	}
	return ids
}

// namedTableCount returns how many tables the MANIFEST names.
func namedTableCount(db *DB) int {
	db.manifest.appendLock.Lock()
	defer db.manifest.appendLock.Unlock()
	return len(db.manifest.manifest.Tables)
}

// waitForANamedTableOutOfTheLevels waits until the MANIFEST names a table
// that is in no level and returns its id.
func waitForANamedTableOutOfTheLevels(t *testing.T, db *DB) uint64 {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		if ids := namedButNotVisible(db); len(ids) == 1 {
			return ids[0]
		}
		require.True(t, time.Now().Before(deadline), "no flush named a table and stopped before level 0 within 30 s")
		time.Sleep(5 * time.Millisecond)
	}
}

// heldMemTable returns the memtable the flusher holds at the level-0 stall
// after holdTheFlusher: the oldest in db.imm.
func heldMemTable(t *testing.T, db *DB) *memTable {
	t.Helper()
	db.lock.RLock()
	defer db.lock.RUnlock()
	require.NotEmpty(t, db.imm, "no memtable waits for its flush")
	return db.imm[0]
}

// TestAFlushedLogIsGoneBeforeItsTableIsVisible holds a flush where its table
// is named in the MANIFEST and not yet in level 0 (the level-0 stall, nothing
// compacts), with a reader holding the memtable. The memtable's log must be
// gone from the directory by then: once the table is in a level a compaction
// can drop a delete marker of it, and a log left on disk, replayed after a
// crash, brings back the set that marker shadowed (BUG-0858). The log goes
// whatever references the memtable has; the reader keeps reading the
// skiplist.
func TestAFlushedLogIsGoneBeforeItsTableIsVisible(t *testing.T) {
	for _, open := range retireOpens {
		t.Run(open.name, func(t *testing.T) {
			opt := open.tune(cutTestOptions(t.TempDir()))
			db, err := Open(opt)
			require.NoError(t, err)
			closeAtCleanup(t, db)
			holdTheFlusher(t, db)
			held := heldMemTable(t, db)
			reader := db.NewTransaction(false)
			pin := reader.NewIterator(DefaultIteratorOptions)
			t.Cleanup(func() {
				pin.Close()
				reader.Discard()
			})

			// The flush unlinks the log right after its MANIFEST change, so a
			// look in between can still see it; it must be gone within moments,
			// while the table still waits for level 0.
			table := waitForANamedTableOutOfTheLevels(t, db)
			var statErr error
			for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
				if _, statErr = os.Stat(held.wal.path); statErr != nil {
					break
				}
			}
			require.ErrorIs(t, statErr, os.ErrNotExist, "the MANIFEST names table %d, which waits for level 0, and the log %s "+
				"of the memtable it was flushed from is still on disk 5 s later", table, filepath.Base(held.wal.path))
			require.Equal(t, []uint64{table}, namedButNotVisible(db), "the table went into a level while the test watched")

			pin.Rewind()
			require.True(t, pin.Valid(), "the reader that holds the flushed memtable reads nothing")
		})
	}
}

// TestALogLeftBesideItsNamedTableIsNotReplayed builds the image of a crash
// between the MANIFEST change that names a flush's table and the durable
// removal of its memtable's log: the table named and in no level, the log on
// disk, at its bytes as of the last sync (a power cut) or whole (a kill). The
// reopened store must not replay that log: its entries are all in the named
// table, and replayed (under the cut, without the delete above the vouched
// version) and flushed again it makes a second table that can outlive a
// compaction of the first, bringing back the set that compaction dropped with
// its delete marker.
func TestALogLeftBesideItsNamedTableIsNotReplayed(t *testing.T) {
	for _, open := range retireOpens {
		for _, whole := range []bool{false, true} {
			name := open.name + "/log as of the sync"
			if whole {
				name = open.name + "/log whole"
			}
			t.Run(name, func(t *testing.T) {
				dir := t.TempDir()
				opt := open.tune(cutTestOptions(dir))
				db, err := Open(opt)
				require.NoError(t, err)
				closeAtCleanup(t, db)
				fillLevelZeroToStall(t, db)

				leftover := activeMemTable(db)
				setKey(t, db, "doomed")
				vouched, err := db.SyncVouched()
				require.NoError(t, err)
				logAtSync, err := os.ReadFile(leftover.wal.path)
				require.NoError(t, err)
				require.NoError(t, db.Update(func(txn *Txn) error { return txn.Delete([]byte("doomed")) }))
				// The log's bytes after the last commit it takes are all it will
				// ever hold: a rotated memtable is never written again.
				var logWhole []byte
				for batch := 0; activeMemTable(db) == leftover; batch++ {
					require.Less(t, batch, 1000, "the memtable never rotated")
					logWhole, err = os.ReadFile(leftover.wal.path)
					require.NoError(t, err)
					require.NoError(t, commitBatch(db, fmt.Sprintf("rotate-%04d", batch)))
				}
				waitForANamedTableOutOfTheLevels(t, db)
				// The flush removes the log right after naming the table; copying the
				// directory meanwhile could list the log and then not find it. The
				// image gets the log back from the bytes read above either way.
				for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
					if _, statErr := os.Stat(leftover.wal.path); statErr != nil {
						break
					}
				}

				leftLog := logAtSync
				if whole {
					leftLog = logWhole
				}
				image := writeImage(t, imageAt(t, dir), storeImage{filepath.Base(leftover.wal.path): leftLog})
				reopenOpt := opt.WithDir(image).WithValueDir(image)
				if opt.vouchedCut {
					reopenOpt = reopenOpt.WithVouchedCut(vouched)
				}
				reopened, err := Open(reopenOpt)
				require.NoError(t, err)
				defer func() { require.NoError(t, reopened.Close()) }()
				// The image's active log is replayed too, legitimately, on the legacy
				// open (the cut empties it on the vouched one); only the leftover
				// must not be.
				leftoverName := filepath.Base(leftover.wal.path)
				reopened.lock.RLock()
				var replayed []string
				for _, mt := range reopened.imm {
					replayed = append(replayed, filepath.Base(mt.wal.path))
				}
				reopened.lock.RUnlock()
				require.NotContains(t, replayed, leftoverName, "the reopened store replayed the log %s, whose table the MANIFEST "+
					"already names, into a memtable it will flush into a second table", leftoverName)
				_, statErr := os.Stat(filepath.Join(image, leftoverName))
				require.ErrorIs(t, statErr, os.ErrNotExist, "the log %s, whose table the MANIFEST names, is still on disk after the open", leftoverName)
				require.False(t, hasKey(t, reopened, "doomed"), "the deleted key is back after the reopen")
			})
		}
	}
}

// setRetireSeams replaces retireRemoveLog and retireSyncDir for the test.
func setRetireSeams(t *testing.T, remove func(string) error, syncDirectory func(string) error) {
	t.Helper()
	previousRemove, previousSync := retireRemoveLog, retireSyncDir
	if remove != nil {
		retireRemoveLog = remove
	}
	if syncDirectory != nil {
		retireSyncDir = syncDirectory
	}
	t.Cleanup(func() { retireRemoveLog, retireSyncDir = previousRemove, previousSync })
}

// setOpenCompactionsHook replaces openCompactionsHook for the test.
func setOpenCompactionsHook(t *testing.T, hook func(dir string)) {
	t.Helper()
	previous := openCompactionsHook
	openCompactionsHook = hook
	t.Cleanup(func() { openCompactionsHook = previous })
}

// holdFlushesOf holds the flusher of the store at dir in front of each flush
// until the returned function is called (SetFlushTestHook).
func holdFlushesOf(t *testing.T, dir string) (letGo func()) {
	t.Helper()
	release := make(chan struct{})
	restore := SetFlushTestHook(func(db *DB) {
		if filepath.Clean(db.Opts().Dir) == filepath.Clean(dir) {
			<-release
		}
	})
	var once bool
	letGo = func() {
		if !once {
			once = true
			close(release)
		}
	}
	t.Cleanup(func() {
		letGo()
		restore()
	})
	return letGo
}

func waitUntilNoImmutables(t *testing.T, db *DB) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for numImmutables(db) > 0 {
		require.True(t, time.Now().Before(deadline), "the memtables were not flushed within 30 s")
		time.Sleep(5 * time.Millisecond)
	}
}

// TestTheRemovalIsDurableBeforeTheTableIsInALevel records the retirement's
// steps through its seams: the log is unlinked after the MANIFEST names the
// table, the directory is fsynced after the unlink, and the table is in no
// level until both are done.
func TestTheRemovalIsDurableBeforeTheTableIsInALevel(t *testing.T) {
	for _, open := range retireOpens {
		t.Run(open.name, func(t *testing.T) {
			dir := t.TempDir()
			db, err := Open(open.tune(cutTestOptions(dir)))
			require.NoError(t, err)
			closeAtCleanup(t, db)
			first := activeMemTable(db)
			var steps []string
			setRetireSeams(t,
				func(path string) error {
					steps = append(steps, fmt.Sprintf("unlink %s: tables named and in no level %d", filepath.Base(path), len(namedButNotVisible(db))))
					return os.Remove(path)
				},
				func(directory string) error {
					_, statErr := os.Stat(first.wal.path)
					steps = append(steps, fmt.Sprintf("directory fsync: log gone %v, tables named and in no level %d",
						errors.Is(statErr, os.ErrNotExist), len(namedButNotVisible(db))))
					return syncDir(directory)
				})
			require.NoError(t, commitUntilRotatedFrom(db, first, "rotate"))
			waitUntilNoImmutables(t, db)
			require.Empty(t, namedButNotVisible(db), "the flushed table is not in level 0 after its flush")
			require.GreaterOrEqual(t, len(steps), 2, "the flush did not retire its log: steps %v", steps)
			require.Equal(t, []string{
				"unlink 00001.mem: tables named and in no level 1",
				"directory fsync: log gone true, tables named and in no level 1",
			}, steps[:2], "the retirement's steps out of order: the log must be unlinked after the MANIFEST names the table, "+
				"the directory fsynced after the unlink, and the table kept out of level 0 until both are done")
		})
	}
}

// TestACrashInsideTheRemovalLeavesTheKeyDeleted takes the image inside the
// retirement of the first memtable's log: before the unlink (the log whole
// beside its named table), and before the directory fsync (the unlink not
// durable, so the log can be back). The reopened store removes that log
// instead of replaying it; then, with the reopened flusher held, a compaction
// of the named table drops the delete marker and the set, and the key must
// stay deleted once the flusher runs again: a replayed log would have been
// flushed into a table holding the set alone.
func TestACrashInsideTheRemovalLeavesTheKeyDeleted(t *testing.T) {
	for _, open := range retireOpens {
		for _, stop := range []string{"before the unlink", "before the directory fsync"} {
			t.Run(open.name+"/"+stop, func(t *testing.T) {
				dir := t.TempDir()
				opt := open.tune(cutTestOptions(dir))
				db, err := Open(opt)
				require.NoError(t, err)
				closeAtCleanup(t, db)
				first := activeMemTable(db)
				setKey(t, db, "doomed")
				vouched, err := db.SyncVouched()
				require.NoError(t, err)
				require.NoError(t, db.Update(func(txn *Txn) error { return txn.Delete([]byte("doomed")) }))

				var image storeImage
				var logBytes []byte
				setRetireSeams(t,
					func(path string) error {
						if path == first.wal.path && image == nil {
							var readErr error
							logBytes, readErr = os.ReadFile(path)
							if readErr != nil {
								return readErr
							}
							if stop == "before the unlink" {
								image = imageAt(t, dir)
							}
						}
						return os.Remove(path)
					},
					func(directory string) error {
						if stop == "before the directory fsync" && image == nil && logBytes != nil {
							image = imageAt(t, dir)
							image[filepath.Base(first.wal.path)] = logBytes
						}
						return syncDir(directory)
					})
				require.NoError(t, commitUntilRotatedFrom(db, first, "rotate"))
				waitUntilNoImmutables(t, db)
				require.NotNil(t, image, "SCENARIO NOT REACHED: the first log's retirement never ran")
				require.Contains(t, image, filepath.Base(first.wal.path), "the image holds no first log")

				imageDir := writeImage(t, image)
				reopenOpt := opt.WithDir(imageDir).WithValueDir(imageDir).WithNumLevelZeroTables(5).WithNumLevelZeroTablesStall(15)
				if opt.vouchedCut {
					reopenOpt = reopenOpt.WithVouchedCut(vouched)
				}
				letGo := holdFlushesOf(t, imageDir)
				reopened, err := Open(reopenOpt)
				require.NoError(t, err)
				defer func() { require.NoError(t, reopened.Close()) }()
				defer letGo() // runs before the Close above: a held flusher would keep Close waiting
				require.False(t, hasKey(t, reopened, "doomed"), "the deleted key is back right after the reopen")
				require.NoError(t, reopened.Flatten(1))
				letGo()
				waitUntilNoImmutables(t, reopened)
				require.False(t, hasKey(t, reopened, "doomed"), "the deleted key is back after a compaction of its table "+
					"and the flush of what the reopen replayed")
				require.Equal(t, 1, reopened.VouchedCutReport().LogsOfNamedTables,
					"the open did not remove the log whose table the MANIFEST names")
			})
		}
	}
}

// TestAFailedDirectoryFsyncNamesNoSecondTable fails every directory fsync
// after a flushed log's removal. The MANIFEST names the table once; the table
// stays out of level 0 (the removal is not known to be durable), the memtable
// keeps serving reads, writes are refused after the flush loop's failures,
// Close returns and keeps the table, and the next open removes nothing it
// needs and names no second table.
func TestAFailedDirectoryFsyncNamesNoSecondTable(t *testing.T) {
	for _, open := range retireOpens {
		t.Run(open.name, func(t *testing.T) {
			dir := t.TempDir()
			opt := open.tune(cutTestOptions(dir))
			db, err := Open(opt)
			require.NoError(t, err)
			first := activeMemTable(db)
			setKey(t, db, "kept")
			namedBefore := namedTableCount(db)
			injected := errors.New("injected directory fsync failure")
			setRetireSeams(t, nil, func(string) error { return injected })
			require.NoError(t, commitUntilRotatedFrom(db, first, "rotate"))

			deadline := time.Now().Add(30 * time.Second)
			for db.WriteRefusal() == nil {
				require.True(t, time.Now().Before(deadline), "writes were not refused within 30 s of a failing removal")
				time.Sleep(20 * time.Millisecond)
			}
			require.ErrorIs(t, db.WriteRefusal(), ErrWritesRefused)
			require.ErrorIs(t, db.WriteRefusal(), injected)
			require.Equal(t, namedBefore+1, namedTableCount(db), "the retries named another table")
			require.Len(t, namedButNotVisible(db), 1, "the table is in a level although its log's removal is not durable")
			require.True(t, hasKey(t, db, "kept"), "the memtable of the named table no longer serves reads")
			closeErr := db.Close()
			require.Error(t, closeErr, "Close reported nothing about the memtable it could not flush")

			setRetireSeams(t, os.Remove, syncDir)
			reopened, err := Open(opt)
			require.NoError(t, err)
			defer func() { require.NoError(t, reopened.Close()) }()
			require.True(t, hasKey(t, reopened, "kept"), "the key of the named table is gone after the reopen")
			require.Equal(t, namedBefore+1, namedTableCount(reopened), "the reopen named a second table")
			_, statErr := os.Stat(first.wal.path)
			require.ErrorIs(t, statErr, os.ErrNotExist, "the log of the named table is still on disk after the reopen")
		})
	}
}

// TestAFailedUnlinkIsRetriedWithoutASecondTable fails the first two unlinks
// of a flushed log. The retry removes the log and inserts the table it
// already named; it never builds a second one.
func TestAFailedUnlinkIsRetriedWithoutASecondTable(t *testing.T) {
	for _, open := range retireOpens {
		t.Run(open.name, func(t *testing.T) {
			db, err := Open(open.tune(cutTestOptions(t.TempDir())))
			require.NoError(t, err)
			closeAtCleanup(t, db)
			first := activeMemTable(db)
			namedBefore := namedTableCount(db)
			failures := 0
			setRetireSeams(t, func(path string) error {
				if failures < 2 {
					failures++
					return errors.New("injected unlink failure")
				}
				return os.Remove(path)
			}, nil)
			require.NoError(t, commitUntilRotatedFrom(db, first, "rotate"))
			waitUntilNoImmutables(t, db)
			require.Equal(t, 2, failures)
			require.Equal(t, namedBefore+1, namedTableCount(db), "the retries named another table")
			require.Empty(t, namedButNotVisible(db), "the named table is not in level 0 after the retry")
			_, statErr := os.Stat(first.wal.path)
			require.ErrorIs(t, statErr, os.ErrNotExist)
			require.NoError(t, db.WriteRefusal())
		})
	}
}

// TestAFailedDirectoryFsyncIsNotRetriedIntoSuccess fails only the first
// directory fsync after a flushed log's removal. A later fsync can succeed
// without the unlink having reached the disk, so the failure stands for the
// rest of the process: the table stays out of level 0 and writes are refused,
// though every later fsync would succeed.
func TestAFailedDirectoryFsyncIsNotRetriedIntoSuccess(t *testing.T) {
	for _, open := range retireOpens {
		t.Run(open.name, func(t *testing.T) {
			db, err := Open(open.tune(cutTestOptions(t.TempDir())))
			require.NoError(t, err)
			first := activeMemTable(db)
			failed := false
			setRetireSeams(t, nil, func(directory string) error {
				if !failed {
					failed = true
					return errors.New("injected directory fsync failure")
				}
				return syncDir(directory)
			})
			require.NoError(t, commitUntilRotatedFrom(db, first, "rotate"))
			deadline := time.Now().Add(30 * time.Second)
			for db.WriteRefusal() == nil {
				require.True(t, time.Now().Before(deadline), "writes were not refused within 30 s: a retry took a later "+
					"fsync's success for the durable removal of the log (table in no level: %v)", namedButNotVisible(db))
				time.Sleep(20 * time.Millisecond)
			}
			require.Len(t, namedButNotVisible(db), 1, "the table went into level 0 after a failed directory fsync")
			require.Error(t, db.Close())
		})
	}
}

// dropPrefixTestOptions are a store opened without the cut (the cut refuses
// DB.DropPrefix), small memtables, as the legacy store's DropPrefixes uses.
//
// The value log is small too: an image copies every file whole, and the
// default value-log file is preallocated far larger (an 8 GB VM ran out of
// memory copying it under -race).
func dropPrefixTestOptions(dir string) Options {
	return getTestOptions(dir).WithMemTableSize(1 << 20).WithValueThreshold(1 << 10).WithValueLogFileSize(1 << 20)
}

// writeAfterFailedDrop commits keys one at a time until a write is refused or
// count are written, and returns the acknowledged keys.
func writeAfterFailedDrop(t *testing.T, db *DB, count int) []string {
	t.Helper()
	var acked []string
	for i := range count {
		key := fmt.Sprintf("after-%03d", i)
		if err := db.Update(func(txn *Txn) error { return txn.Set([]byte(key), []byte("v")) }); err != nil {
			t.Logf("write %d refused: %v", i, err)
			break
		}
		acked = append(acked, key)
	}
	return acked
}

// TestAFailedRemovalInsideDropPrefixLosesNoAcknowledgedWrite fails the
// retirement inside DB.DropPrefix's own flush of the active memtable (review
// F1 of BUG-0858's fix). DropPrefix returns the error and writes resume. Every
// write acknowledged after it must survive: before the fix the failed memtable
// stayed the active one, its table already named (built before the failure)
// and, after a failed directory fsync, its log already unlinked, so the
// writes went where no table and no log kept them.
func TestAFailedRemovalInsideDropPrefixLosesNoAcknowledgedWrite(t *testing.T) {
	t.Run("the directory fsync fails once", func(t *testing.T) {
		dir := t.TempDir()
		opt := dropPrefixTestOptions(dir)
		db, err := Open(opt)
		require.NoError(t, err)
		setKey(t, db, "keep-1")
		setKey(t, db, "drop-1")
		failed := false
		setRetireSeams(t, nil, func(directory string) error {
			if !failed {
				failed = true
				return errors.New("injected directory fsync failure")
			}
			return syncDir(directory)
		})
		require.Error(t, db.DropPrefix([]byte("drop-")), "SCENARIO NOT REACHED: the drop's flush did not fail")
		acked := writeAfterFailedDrop(t, db, 20)
		require.NotEmpty(t, acked, "SCENARIO NOT REACHED: no write was acknowledged after the failed drop")
		require.NoError(t, db.Sync(), "a sync after the acknowledged writes")
		closeErr := db.Close()
		t.Logf("Close: %v", closeErr)

		reopened, err := Open(opt)
		require.NoError(t, err)
		defer func() { require.NoError(t, reopened.Close()) }()
		var lost []string
		for _, key := range acked {
			if !hasKey(t, reopened, key) {
				lost = append(lost, key)
			}
		}
		require.Empty(t, lost, "%d of %d writes acknowledged after the failed DropPrefix (and synced) are gone after "+
			"Close and a reopen", len(lost), len(acked))
		require.True(t, hasKey(t, reopened, "keep-1"), "a key committed before the failed drop is gone")
	})

	t.Run("the unlink fails twice", func(t *testing.T) {
		dir := t.TempDir()
		opt := dropPrefixTestOptions(dir)
		db, err := Open(opt)
		require.NoError(t, err)
		setKey(t, db, "keep-1")
		setKey(t, db, "drop-1")
		first := activeMemTable(db)
		failures := 0
		setRetireSeams(t, func(path string) error {
			if failures < 2 {
				failures++
				return errors.New("injected unlink failure")
			}
			return os.Remove(path)
		}, nil)
		require.Error(t, db.DropPrefix([]byte("drop-")), "SCENARIO NOT REACHED: the drop's flush did not fail")
		acked := writeAfterFailedDrop(t, db, 5)
		require.Len(t, acked, 5, "a write was refused after the failed drop")
		// The restarted flusher retries the removal and succeeds; a crash right
		// after leaves this image.
		deadline := time.Now().Add(20 * time.Second)
		for {
			if _, statErr := os.Stat(first.wal.path); errors.Is(statErr, os.ErrNotExist) {
				break
			}
			require.True(t, time.Now().Before(deadline), "SCENARIO NOT REACHED: the retry never removed the log")
			time.Sleep(20 * time.Millisecond)
		}
		waitUntilNoImmutables(t, db)
		imageDir := writeImage(t, imageAt(t, dir))
		reopened, err := Open(opt.WithDir(imageDir).WithValueDir(imageDir))
		require.NoError(t, err)
		defer func() { require.NoError(t, reopened.Close()) }()
		var lost []string
		for _, key := range acked {
			if !hasKey(t, reopened, key) {
				lost = append(lost, key)
			}
		}
		require.Empty(t, lost, "%d of %d writes acknowledged after the failed DropPrefix and before the flusher's "+
			"retry are not in the image taken after it", len(lost), len(acked))
		require.True(t, hasKey(t, reopened, "keep-1"), "a key committed before the failed drop is gone")
		require.NoError(t, db.Close())
	})
}

// TestTheOpenSyncsTheDirectoryBeforeCompactionsStart checks, through the
// retirement's seams, that every open fsyncs the store directory before it
// returns (and so before compactions start): after removing a leftover log of
// a named table (review F2: no test failed with that fsync removed), and when
// it removed nothing, because an unlink an earlier process applied (a flush
// whose directory fsync failed; an open that failed after its own unlink) can
// be in the page cache only, and a power cut after compactions have read the
// table would bring the log back.
func TestTheOpenSyncsTheDirectoryBeforeCompactionsStart(t *testing.T) {
	for _, open := range retireOpens {
		t.Run(open.name+"/nothing to remove", func(t *testing.T) {
			dir := t.TempDir()
			opt := open.tune(cutTestOptions(dir))
			db, err := Open(opt)
			require.NoError(t, err)
			setKey(t, db, "kept")
			require.NoError(t, db.Close())
			var steps []string
			setRetireSeams(t,
				func(path string) error { steps = append(steps, "unlink "+filepath.Base(path)); return os.Remove(path) },
				func(directory string) error { steps = append(steps, "directory fsync"); return syncDir(directory) })
			setOpenCompactionsHook(t, func(string) { steps = append(steps, "compactions start") })
			reopened, err := Open(opt)
			require.NoError(t, err)
			opened := append([]string(nil), steps...)
			require.NoError(t, reopened.Close())
			// The order is the point (review R3): a directory fsync after the
			// compactions start would let one read a table whose log a power
			// cut can bring back.
			require.Equal(t, []string{"directory fsync", "compactions start"}, opened, "an open that removed no log did "+
				"not fsync the directory before it started the compactions")
		})
		t.Run(open.name+"/a leftover log", func(t *testing.T) {
			dir := t.TempDir()
			opt := open.tune(cutTestOptions(dir))
			db, err := Open(opt)
			require.NoError(t, err)
			closeAtCleanup(t, db)
			first := activeMemTable(db)
			setKey(t, db, "kept")
			vouched, err := db.SyncVouched()
			require.NoError(t, err)
			var image storeImage
			setRetireSeams(t, func(path string) error {
				if path == first.wal.path && image == nil {
					image = imageAt(t, dir)
				}
				return os.Remove(path)
			}, nil)
			require.NoError(t, commitUntilRotatedFrom(db, first, "rotate"))
			waitUntilNoImmutables(t, db)
			// Closed before the seams change: its flusher could retire another
			// memtable through them meanwhile (a data race under -race).
			require.NoError(t, db.Close())
			require.NotNil(t, image, "SCENARIO NOT REACHED: the first log's retirement never ran")
			imageDir := writeImage(t, image)
			reopenOpt := opt.WithDir(imageDir).WithValueDir(imageDir)
			if opt.vouchedCut {
				reopenOpt = reopenOpt.WithVouchedCut(vouched)
			}
			var steps []string
			setRetireSeams(t,
				func(path string) error {
					if filepath.Dir(path) == imageDir {
						steps = append(steps, "unlink "+filepath.Base(path))
					}
					return os.Remove(path)
				},
				func(directory string) error {
					if directory == imageDir {
						steps = append(steps, "directory fsync")
					}
					return syncDir(directory)
				})
			setOpenCompactionsHook(t, func(directory string) {
				if directory == imageDir {
					steps = append(steps, "compactions start")
				}
			})
			// Held so that the flush of a replayed log (the legacy open replays the
			// image's active log) cannot add steps meanwhile.
			letGo := holdFlushesOf(t, imageDir)
			reopened, err := Open(reopenOpt)
			require.NoError(t, err)
			opened := append([]string(nil), steps...)
			defer func() { require.NoError(t, reopened.Close()) }()
			defer letGo() // runs before the Close above
			require.Equal(t, []string{"unlink " + filepath.Base(first.wal.path), "directory fsync", "compactions start"}, opened,
				"the open did not fsync the directory after removing the log whose table the MANIFEST names, before it started the compactions")
			require.True(t, hasKey(t, reopened, "kept"))
		})
	}
}

// TestAFailedOpenStepReturnsNoStoreAndItsCause fails the open step's unlink,
// then its directory fsync, on an image holding a log beside its named table
// (review F7 and F2). Open must return no DB and an error errors.Is matches to
// the cause; the next open, which may no longer see the log, must still fsync
// the directory before it returns.
func TestAFailedOpenStepReturnsNoStoreAndItsCause(t *testing.T) {
	for _, open := range retireOpens {
		for _, failing := range []string{"unlink", "directory fsync"} {
			t.Run(open.name+"/"+failing, func(t *testing.T) {
				dir := t.TempDir()
				opt := open.tune(cutTestOptions(dir))
				db, err := Open(opt)
				require.NoError(t, err)
				closeAtCleanup(t, db)
				first := activeMemTable(db)
				setKey(t, db, "doomed")
				vouched, err := db.SyncVouched()
				require.NoError(t, err)
				require.NoError(t, db.Update(func(txn *Txn) error { return txn.Delete([]byte("doomed")) }))
				var image storeImage
				setRetireSeams(t, func(path string) error {
					if path == first.wal.path && image == nil {
						image = imageAt(t, dir)
					}
					return os.Remove(path)
				}, nil)
				require.NoError(t, commitUntilRotatedFrom(db, first, "rotate"))
				waitUntilNoImmutables(t, db)
				// Closed before the seams change (see TestTheOpenSyncsTheDirectoryBeforeCompactionsStart).
				require.NoError(t, db.Close())
				require.NotNil(t, image, "SCENARIO NOT REACHED: the first log's retirement never ran")
				imageDir := writeImage(t, image)
				reopenOpt := opt.WithDir(imageDir).WithValueDir(imageDir)
				if opt.vouchedCut {
					reopenOpt = reopenOpt.WithVouchedCut(vouched)
				}

				injected := errors.New("injected open-step failure")
				if failing == "unlink" {
					setRetireSeams(t, func(string) error { return injected }, syncDir)
				} else {
					setRetireSeams(t, os.Remove, func(string) error { return injected })
				}
				failed, err := Open(reopenOpt)
				require.ErrorIs(t, err, injected, "the open's error does not carry its cause")
				require.Nil(t, failed, "a failed open returned a DB")

				syncs := 0
				setRetireSeams(t, os.Remove, func(directory string) error {
					if directory == imageDir {
						syncs++
					}
					return syncDir(directory)
				})
				// Held so that the flush of a replayed log (the legacy open replays
				// the image's active log) cannot fsync the directory meanwhile.
				letGo := holdFlushesOf(t, imageDir)
				reopened, err := Open(reopenOpt)
				require.NoError(t, err, "the open after the failed one")
				opened := syncs
				defer func() { require.NoError(t, reopened.Close()) }()
				defer letGo() // runs before the Close above
				require.Equal(t, 1, opened, "the open after a failed open step did not fsync the directory before it returned")
				require.False(t, hasKey(t, reopened, "doomed"), "the deleted key is back")
			})
		}
	}
}
