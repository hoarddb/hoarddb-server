//go:build linux

/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Added by the hoardDB project, 2026-11-17: the tests of the vouched cut
 * (WithVouchedCut, DB.SyncVouched, change 12) that build images of a live
 * store, as the sync-order tests do. See hoardDB/third_party/README.md.
 */

package badger

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// versionOf returns the version key is visible at in db.
func versionOf(t *testing.T, db *DB, key string) uint64 {
	t.Helper()
	var version uint64
	require.NoError(t, db.View(func(txn *Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return fmt.Errorf("reading %s: %w", key, err)
		}
		version = item.Version()
		return nil
	}))
	return version
}

// commitAt commits key on its own and returns the version it took.
func commitAt(t *testing.T, db *DB, key string) uint64 {
	t.Helper()
	setKey(t, db, key)
	return versionOf(t, db, key)
}

// visibleVersions returns every user key of db with the version it is
// visible at.
func visibleVersions(t *testing.T, db *DB) map[string]uint64 {
	t.Helper()
	versions := map[string]uint64{}
	require.NoError(t, db.View(func(txn *Txn) error {
		iterator := txn.NewIterator(IteratorOptions{})
		defer iterator.Close()
		for iterator.Rewind(); iterator.Valid(); iterator.Next() {
			versions[string(iterator.Item().KeyCopy(nil))] = iterator.Item().Version()
		}
		return nil
	}))
	return versions
}

// TestEntriesAboveTheMarkerAreGoneAndStayGone checks the cut at open (I-1,
// I-2): commits above the vouched version are not visible after the open,
// the report counts them, and once the store has taken their versions again
// for new commits and vouched those, a second image does not bring them back.
// The flusher is held (no compactors, level 0 at its stall) so the log the
// cut truncated is still a log when the second image is taken: left
// untruncated, it would replay the old commits under the new, higher vouched
// version (the spec's probe: commits 7 and 8 returned).
func TestEntriesAboveTheMarkerAreGoneAndStayGone(t *testing.T) {
	held := func(o Options) Options {
		return o.WithNumCompactors(0).WithNumLevelZeroTables(1).WithNumLevelZeroTablesStall(2)
	}
	db, opt := openSyncLockTestDB(t, func(o Options) Options { return held(o).WithVouchedCut(0) })
	closeAtCleanup(t, db)
	fillLevelZeroToStall(t, db)

	vouchedKeys := []string{"vouched-1", "vouched-2", "vouched-3", "vouched-4", "vouched-5", "vouched-6"}
	for _, key := range vouchedKeys {
		setKey(t, db, key)
	}
	vouched, err := db.SyncVouched()
	require.NoError(t, err)
	require.Equal(t, versionOf(t, db, vouchedKeys[len(vouchedKeys)-1]), vouched,
		"SyncVouched did not return the version of the last commit before it")
	discardedKeys := []string{"discarded-1", "discarded-2"}
	var discardedVersions []uint64
	for _, key := range discardedKeys {
		discardedVersions = append(discardedVersions, commitAt(t, db, key))
	}

	firstImage := writeImage(t, imageAt(t, opt.Dir))
	first, err := Open(held(opt).WithDir(firstImage).WithValueDir(firstImage).WithVouchedCut(vouched))
	require.NoError(t, err)
	closeAtCleanup(t, first)
	for _, key := range vouchedKeys {
		require.True(t, hasKey(t, first, key), "%s, at or below the vouched version %d, is gone after the cut", key, vouched)
	}
	for _, key := range discardedKeys {
		require.False(t, hasKey(t, first, key), "%s, above the vouched version %d, is visible after the cut", key, vouched)
	}
	firstReport := first.VouchedCutReport()

	// The versions the cut discarded are handed out again.
	againKeys := []string{"again-1", "again-2"}
	for i, key := range againKeys {
		require.Equal(t, discardedVersions[i], commitAt(t, first, key),
			"SCENARIO NOT REACHED: %s did not take the discarded version %d again", key, discardedVersions[i])
	}
	vouchedAgain, err := first.SyncVouched()
	require.NoError(t, err)
	require.Equal(t, discardedVersions[len(discardedVersions)-1], vouchedAgain)

	secondImage := writeImage(t, imageAt(t, firstImage))
	second, err := Open(opt.WithDir(secondImage).WithValueDir(secondImage).WithVouchedCut(vouchedAgain).
		WithNumCompactors(2).WithNumLevelZeroTables(5).WithNumLevelZeroTablesStall(15))
	require.NoError(t, err)
	defer func() { require.NoError(t, second.Close()) }()
	for _, key := range discardedKeys {
		require.False(t, hasKey(t, second, key), "%s, discarded by the first cut, came back after a second image "+
			"under the vouched version %d: the truncation was not durable", key, vouchedAgain)
	}
	for _, key := range append(vouchedKeys, againKeys...) {
		require.True(t, hasKey(t, second, key), "%s is gone after the second image", key)
	}
	require.Equal(t, VouchedCutReport{Vouched: vouched, Kept: vouched, DiscardedTxns: 2, LogsTruncated: 1}, firstReport,
		"the first open's report does not count the two commits it discarded")
}

// TestLargeValueUnderAnUnfinishedSyncIsReplayedWhole is BUG-0827's route (c)
// at the store: a value in the value log whose pointer reached the disk in a
// memtable log that a sync made durable, while the value log's own msync had
// not run. A power cut between the two msyncs leaves the pointer without its
// bytes. The commit is above the vouched version, because that sync never
// reported, so the cut discards it and the node replays it from its log; here
// the key must be absent, never present with an empty or unreadable value.
func TestLargeValueUnderAnUnfinishedSyncIsReplayedWhole(t *testing.T) {
	db, opt := openSyncLockTestDB(t, func(o Options) Options {
		return o.WithValueLogFileSize(8 << 20).WithVouchedCut(0)
	})
	closeAtCleanup(t, db)

	setKey(t, db, "before")
	vouched, err := db.SyncVouched()
	require.NoError(t, err)
	atLastCompletedSync := imageAt(t, opt.Dir)

	large := make([]byte, 1<<20)
	for i := range large {
		large[i] = byte(i%251 + 1)
	}
	require.NoError(t, db.Update(func(txn *Txn) error { return txn.Set([]byte("large"), large) }))

	parked := make(chan struct{})
	release := make(chan struct{})
	var once, releaseOnce sync.Once
	restore := syncValueLogHook
	syncValueLogHook = func() {
		once.Do(func() {
			close(parked)
			<-release
		})
	}
	syncDone := make(chan error, 1)
	go func() {
		_, err := db.SyncVouched()
		syncDone <- err
	}()
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		<-syncDone
		syncValueLogHook = restore
	})
	select {
	case <-parked:
	case <-time.After(60 * time.Second):
		t.Fatal("SCENARIO NOT REACHED: DB.SyncVouched never reached the point between the active log's msync and the value log's")
	}

	// The memtable logs as they are now (synced), the value log as of the last
	// completed sync.
	imageDir := writeImage(t, imageAt(t, opt.Dir), atLastCompletedSync.only(isValueLogFile))
	releaseOnce.Do(func() { close(release) })
	require.NoError(t, <-syncDone)
	syncDone <- nil

	reopened, err := Open(opt.WithDir(imageDir).WithValueDir(imageDir).WithVouchedCut(vouched))
	require.NoError(t, err)
	defer func() { require.NoError(t, reopened.Close()) }()
	require.True(t, hasKey(t, reopened, "before"))
	err = reopened.View(func(txn *Txn) error {
		item, err := txn.Get([]byte("large"))
		if err != nil {
			return err
		}
		read, err := item.ValueCopy(nil)
		return fmt.Errorf("present with %d bytes of %d (read error: %v)", len(read), len(large), err)
	})
	if !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("a 1 MiB value committed above the vouched version %d, whose value log was not synced at the cut, "+
			"must be absent after the open, to be replayed whole by the node; it is %v", vouched, err)
	}
}

// TestAFlushedTableVouchesWithoutASync checks that a flushed table vouches
// what it holds with no sync and no marker: commits, a flush, more commits,
// an image, an open with the vouched version 0. Exactly the commits up to the
// table's highest version are visible (I-1, C = max(V, T)).
func TestAFlushedTableVouchesWithoutASync(t *testing.T) {
	db, opt := openSyncLockTestDB(t, func(o Options) Options { return o.WithNumCompactors(0).WithVouchedCut(0) })
	closeAtCleanup(t, db)

	require.NoError(t, writeUntilRotated(db, activeMemTable(db), "flushed"))
	deadline := time.Now().Add(30 * time.Second)
	for numImmutables(db) > 0 {
		require.True(t, time.Now().Before(deadline), "the memtable flush did not finish")
		time.Sleep(10 * time.Millisecond)
	}
	require.NoError(t, writeFill(db, "unflushed", 2))
	lastVersion := versionOf(t, db, "unflushed-00001-63")
	var tableMax uint64
	for _, table := range db.Tables() {
		tableMax = max(tableMax, table.MaxVersion)
	}
	require.NotZero(t, tableMax, "SCENARIO NOT REACHED: no table was flushed")
	require.Less(t, tableMax, lastVersion, "SCENARIO NOT REACHED: every commit is in a table")
	committed := visibleVersions(t, db)

	imageDir := writeImage(t, imageAt(t, opt.Dir))
	reopened, err := Open(opt.WithDir(imageDir).WithValueDir(imageDir).WithVouchedCut(0))
	require.NoError(t, err)
	defer func() { require.NoError(t, reopened.Close()) }()
	visible := visibleVersions(t, reopened)
	var gone, kept []string
	for key, version := range committed {
		_, isVisible := visible[key]
		if version <= tableMax && !isVisible {
			gone = append(gone, fmt.Sprintf("%s@%d", key, version))
		}
		if version > tableMax && isVisible {
			kept = append(kept, fmt.Sprintf("%s@%d", key, version))
		}
	}
	require.Zero(t, len(gone), "%d keys at or below the flushed table's highest version %d are gone after the open, among them %v",
		len(gone), tableMax, gone[:min(3, len(gone))])
	require.Zero(t, len(kept), "%d keys above the flushed table's highest version %d and the vouched version 0 are visible "+
		"after the open, among them %v", len(kept), tableMax, kept[:min(3, len(kept))])
	require.Equal(t, tableMax, reopened.VouchedCutReport().Kept)
}

// TestNoVersionAtOrBelowTheMarkerIsTakenAgain checks the version floor (I-3):
// a store whose highest version is below the vouched version gives its next
// commit a version above it, so that commit, unsynced, is discarded by the
// next cut. Given a version at or below the marker, it would be kept as if a
// sync had vouched it.
func TestNoVersionAtOrBelowTheMarkerIsTakenAgain(t *testing.T) {
	db, opt := openSyncLockTestDB(t, func(o Options) Options { return o.WithVouchedCut(0) })
	highest := commitAt(t, db, "before")
	require.NoError(t, db.Close())
	vouched := highest + 100

	reopened, err := Open(opt.WithVouchedCut(vouched))
	require.NoError(t, err)
	closeAtCleanup(t, reopened)
	after := commitAt(t, reopened, "after")
	require.Greater(t, after, vouched, "the first commit after an open under the vouched version %d took version %d", vouched, after)

	imageDir := writeImage(t, imageAt(t, opt.Dir))
	again, err := Open(opt.WithDir(imageDir).WithValueDir(imageDir).WithVouchedCut(vouched))
	require.NoError(t, err)
	defer func() { require.NoError(t, again.Close()) }()
	require.True(t, hasKey(t, again, "before"))
	require.False(t, hasKey(t, again, "after"), "an unsynced commit after the open is kept by the next cut under the same vouched version")
}

// TestSyncVouchedReadsTheVersionBeforeTheCapture is T9 (f) at the store: the
// version SyncVouched returns must be read before it captures the memtables.
// A commit that returns after the capture and lands in a memtable rotated in
// after it sits in a log this call never syncs, so the returned version must
// not cover it; and every commit that returned before the call must be at or
// below it (I-4). The commit and the rotation run inside
// SyncTestHooks.AfterCapture, after the sync's read lock is released: under
// that lock no commit can return (store-vouched-cut-build.md, review edit F1).
func TestSyncVouchedReadsTheVersionBeforeTheCapture(t *testing.T) {
	db, _ := openSyncLockTestDB(t, func(o Options) Options { return o.WithNumCompactors(0) })
	closeAtCleanup(t, db)
	early := commitAt(t, db, "early")

	var once sync.Once
	var hookRan, rotated bool
	var late, capturedMax uint64
	var hookErr error
	restore := SetSyncTestHooks(SyncTestHooks{AfterCapture: func() {
		once.Do(func() {
			hookRan = true
			captured := activeMemTable(db)
			if hookErr = writeUntilRotated(db, captured, "rotate"); hookErr != nil {
				return
			}
			if hookErr = db.Update(func(txn *Txn) error { return txn.Set([]byte("late"), []byte("after the capture")) }); hookErr != nil {
				return
			}
			hookErr = db.View(func(txn *Txn) error {
				item, err := txn.Get([]byte("late"))
				if err != nil {
					return err
				}
				late = item.Version()
				return nil
			})
			rotated = activeMemTable(db) != captured
			capturedMax = captured.maxVersion
		})
	}})
	t.Cleanup(restore)

	vouched, err := db.SyncVouched()
	require.NoError(t, err)
	require.True(t, hookRan, "SCENARIO NOT REACHED: SyncTestHooks.AfterCapture never ran inside DB.SyncVouched")
	require.NoError(t, hookErr)
	require.True(t, rotated, "SCENARIO NOT REACHED: the memtable did not rotate inside the hook")
	require.Less(t, capturedMax, late, "SCENARIO NOT REACHED: the late commit is in the memtable the sync captured")

	require.Less(t, vouched, late, "DB.SyncVouched returned version %d, which covers the commit at version %d that "+
		"returned after its capture, in a memtable log it never synced: the version was read after the capture", vouched, late)
	require.GreaterOrEqual(t, vouched, early, "DB.SyncVouched returned version %d, below the commit at version %d "+
		"that returned before it was called", vouched, early)
}
