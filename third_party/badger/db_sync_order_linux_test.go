//go:build linux

/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Added by the hoardDB project, 2026-11-15: the tests of BUG-0824. DB.Sync
 * must sync the logs of the rotated-out memtables, oldest first, before the
 * active one, and stop at a log that fails: synced active-first, a power cut
 * between the msyncs left a later commit durable without an earlier one. See
 * hoardDB/third_party/README.md.
 */

package badger

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSyncCoversRotatedMemtableLogBeforeActive checks that what DB.Sync has
// made durable is, at every instant, a prefix of the commits: a commit is never
// durable while an earlier one is not (BUG-0824). hoardDB's replay skips a
// record at or below the position its pair record names, so a store that keeps
// a later write and its pair record without an earlier write never gets the
// earlier one back.
//
// The scenario: "early" is committed unsynced into memtable A; another writer
// rotates A out, and its flush is held (level 0 is at its stall), so A's log is
// the only copy of "early"; "late" is committed into the new active memtable B;
// DB.Sync runs. The power is cut inside that Sync, right after the msync of the
// active log.
//
// A test cannot cut the power and cannot wrap an mmap write, so the cut is
// modelled, strictly: a byte that was written and not synced at the cut is
// dropped. The kernel says which log is synced (cachestat(2): no dirty and no
// under-writeback page, the proxy TestSyncCoversImmutableMemtables uses). A log
// it reports clean keeps its bytes; a log with unsynced pages falls back to the
// bytes it had when it was last seen clean, which the test saved then. The
// image is the directory with those logs, reopened.
//
// The instant is reached with syncActiveWALHook, which DB.Sync calls
// immediately before active.SyncWAL(). The test runs that one msync itself from
// inside the hook (DB.Sync's own next statement repeats it, with nothing to
// write) and reads the kernel's account there: the state DB.Sync is in right
// after the msync of the active log. Before the fix nothing else was synced by
// then (the active log went first), so that was the state between its two
// msyncs; with the fix the rotated-out log is synced before the hook is
// reached, and the image keeps both commits.
func TestSyncCoversRotatedMemtableLogBeforeActive(t *testing.T) {
	db, opt := openSyncLockTestDB(t, func(o Options) Options {
		return o.WithNumCompactors(0).WithNumLevelZeroTables(1).WithNumLevelZeroTablesStall(2)
	})
	closeAtCleanup(t, db)
	fillLevelZeroToStall(t, db)
	// hoardDB: the flusher waits in front of each flush from here, so the
	// rotated memtable's log stays on disk, the only copy of its commit. At the
	// level-0 stall the flush has already named its table and removed the log
	// (BUG-0858's fix). Released in a cleanup before the close.
	holdFlushesOf(t, opt.Dir)

	commit := func(key, value string) {
		t.Helper()
		require.NoError(t, db.Update(func(txn *Txn) error { return txn.Set([]byte(key), []byte(value)) }))
	}
	activeMemtable := func() *memTable {
		db.lock.RLock()
		defer db.lock.RUnlock()
		return db.mt
	}
	synced := func(walPath string) bool {
		pages := pageCacheOf(t, walPath)
		return pages.Dirty == 0 && pages.Writeback == 0
	}

	// A is the active memtable. One full Sync makes its log clean, and its bytes
	// at that moment are all a power cut is certain to keep of it until the
	// next sync that covers it.
	rotatedOut := activeMemtable()
	rotatedWAL := rotatedOut.wal.Fd.Name()
	require.NoError(t, db.Sync())
	require.True(t, synced(rotatedWAL), "the log %s has unsynced pages right after DB.Sync returned: "+
		"the page-cache proxy cannot see a sync on this filesystem", rotatedWAL)
	rotatedWALAtLastSync, err := os.ReadFile(rotatedWAL)
	require.NoError(t, err)

	commit("early", "committed before the rotation, into memtable A")
	require.Same(t, rotatedOut, activeMemtable(), "the memtable rotated before the early commit; the scenario did not set up")

	require.NoError(t, writeUntilRotated(db, rotatedOut, "rotate"))
	active := activeMemtable()
	commit("late", "committed after the rotation, into memtable B")
	require.Same(t, active, activeMemtable(), "the memtable rotated again before the late commit; the scenario did not set up")
	db.lock.RLock()
	require.Len(t, db.imm, 1, "want exactly the rotated-out memtable waiting for its flush")
	require.Same(t, rotatedOut, db.imm[0])
	db.lock.RUnlock()
	if synced(rotatedWAL) {
		t.Skip("the kernel wrote the rotated log back before Sync (background writeback); this run cannot tell")
	}

	// The cut: DB.Sync has captured B and A, and the msync of B's log has just
	// returned.
	parked := parkSyncInActiveWAL(t, db)
	require.Same(t, active, parked.mt, "DB.Sync captured another active memtable than the one that holds the late commit")
	require.NoError(t, parked.mt.SyncWAL())
	require.True(t, synced(parked.walPath), "the active log %s has unsynced pages after its msync: "+
		"the page-cache proxy cannot see a sync on this filesystem", parked.walPath)
	rotatedIsSynced := synced(rotatedWAL)
	image := copyDirImage(t, opt.Dir)
	if !rotatedIsSynced {
		require.NoError(t, os.WriteFile(filepath.Join(image, filepath.Base(rotatedWAL)), rotatedWALAtLastSync, 0o600))
	}
	require.Equal(t, 1, numImmutables(db), "the rotated memtable was flushed during the test; the flusher was meant to be held")
	require.NoError(t, parked.resume(t))

	// The image opens with level 0 below its stall, so the reopened store can
	// flush the logs it replays and close.
	reopened, err := Open(opt.WithDir(image).WithValueDir(image).
		WithNumCompactors(2).WithNumLevelZeroTables(5).WithNumLevelZeroTablesStall(15))
	require.NoError(t, err)
	defer func() { require.NoError(t, reopened.Close()) }()
	inImage := func(key string) bool {
		t.Helper()
		err := reopened.View(func(txn *Txn) error {
			_, err := txn.Get([]byte(key))
			return err
		})
		if errors.Is(err, ErrKeyNotFound) {
			return false
		}
		require.NoError(t, err)
		return true
	}
	early, late := inImage("early"), inImage("late")
	t.Logf("at the cut: active log %s synced, rotated log %s synced=%v; after the reopen: early present=%v, late present=%v",
		filepath.Base(parked.walPath), filepath.Base(rotatedWAL), rotatedIsSynced, early, late)
	if late && !early {
		t.Fatalf("a power cut inside DB.Sync, after the msync of the active memtable's log %s and before the msync of the rotated-out %s, "+
			"keeps the commit \"late\" and loses the commit \"early\", which was committed first: "+
			"the durable store is not a prefix of its commits across a memtable rotation (BUG-0824)",
			filepath.Base(parked.walPath), filepath.Base(rotatedWAL))
	}
	// The image alone can pass under the old order if the flusher wins a race:
	// it writes the rotated memtable's table and names it in the MANIFEST
	// before it waits at the level-0 stall (levelsController.addLevel0Table),
	// and the memtable stays in db.imm meanwhile, so numImmutables does not
	// see it. From then on "early" is durable through the table whatever its
	// log holds. The log's page-cache state at the cut does not depend on the
	// flusher, so it is asserted, not only logged.
	require.True(t, rotatedIsSynced, "DB.Sync reached the msync of the active memtable's log %s while the rotated-out log %s "+
		"still had unsynced pages: a power cut there can keep a later commit without an earlier one (BUG-0824)",
		filepath.Base(parked.walPath), filepath.Base(rotatedWAL))
}

// rotatedLogsScenario is a store with two memtables rotated out and held in
// front of their flush, and a third active: three logs, each the only copy of
// one commit. logs, commits and atLastSync are in rotation order, oldest
// first; the last log is the active one.
//
// The flusher is held in front of the flush of the oldest memtable
// (holdFlushesOf), so no table is named for it and every log stays on disk.
// (Held at the level-0 stall instead, as before BUG-0858's fix, the oldest
// memtable's table was named and its commit durable through it whatever its
// log held; the fix now removes that log there.) The tests read the order from
// the logs' page-cache state and from the second and third commits, never from
// the first commit alone.
type rotatedLogsScenario struct {
	db      *DB
	opt     Options
	logs    [3]*memTable
	commits [3]string
	// atLastSync holds each log's bytes when the kernel last reported it
	// synced: all a power cut is certain to keep of it until the next sync that
	// covers it.
	atLastSync [3][]byte
}

func walIsSynced(t *testing.T, walPath string) bool {
	t.Helper()
	pages := pageCacheOf(t, walPath)
	return pages.Dirty == 0 && pages.Writeback == 0
}

// openWithTwoRotatedLogs builds the scenario. Each log is synced on its own
// while it is the active one and its bytes are saved; then one commit goes into
// it, unsynced; then fill writes rotate it out. The flusher is held as in
// TestSyncCoversRotatedMemtableLogBeforeActive: it waits in front of the
// flush of the oldest memtable, and the next one waits in the flush channel.
func openWithTwoRotatedLogs(t *testing.T) *rotatedLogsScenario {
	t.Helper()
	db, opt := openSyncLockTestDB(t, func(o Options) Options {
		return o.WithNumCompactors(0).WithNumLevelZeroTables(1).WithNumLevelZeroTablesStall(2)
	})
	closeAtCleanup(t, db)
	fillLevelZeroToStall(t, db)
	holdFlushesOf(t, opt.Dir)

	scenario := &rotatedLogsScenario{db: db, opt: opt, commits: [3]string{"first", "second", "third"}}
	activeMemtable := func() *memTable {
		db.lock.RLock()
		defer db.lock.RUnlock()
		return db.mt
	}
	for position, commit := range scenario.commits {
		mt := activeMemtable()
		scenario.logs[position] = mt
		walPath := mt.wal.Fd.Name()
		require.NoError(t, mt.SyncWAL())
		require.True(t, walIsSynced(t, walPath), "the log %s has unsynced pages right after its msync: "+
			"the page-cache proxy cannot see a sync on this filesystem", walPath)
		atLastSync, err := os.ReadFile(walPath)
		require.NoError(t, err)
		scenario.atLastSync[position] = atLastSync

		require.NoError(t, db.Update(func(txn *Txn) error {
			return txn.Set([]byte(commit), []byte("the only copy is in "+filepath.Base(walPath)))
		}))
		require.Same(t, mt, activeMemtable(), "the memtable rotated before the %s commit; the scenario did not set up", commit)
		if position < len(scenario.commits)-1 {
			require.NoError(t, writeUntilRotated(db, mt, fmt.Sprintf("rotate-%d", position)))
		}
	}

	// Copied under the lock and compared after it: a failed require inside the
	// lock would leave it held and hang the Close in the cleanup.
	db.lock.RLock()
	rotatedOut := append([]*memTable(nil), db.imm...)
	db.lock.RUnlock()
	require.Equal(t, scenario.logs[:2], rotatedOut, "want exactly the two rotated-out memtables waiting for their flush, oldest first")
	require.Same(t, scenario.logs[2], activeMemtable())
	for _, mt := range scenario.logs {
		if walIsSynced(t, mt.wal.Fd.Name()) {
			t.Skipf("the kernel wrote the log %s back before Sync (background writeback); this run cannot tell", mt.wal.Fd.Name())
		}
	}
	return scenario
}

// commitsAfterACutNow reports which of the three commits a power cut at this
// instant keeps, under the model of TestSyncCoversRotatedMemtableLogBeforeActive:
// a log the kernel reports synced keeps its bytes, any other falls back to its
// bytes at its last sync. It also returns which logs the kernel reports synced.
func (scenario *rotatedLogsScenario) commitsAfterACutNow(t *testing.T) (kept, synced [3]bool) {
	t.Helper()
	for position, mt := range scenario.logs {
		synced[position] = walIsSynced(t, mt.wal.Fd.Name())
	}
	image := copyDirImage(t, scenario.opt.Dir)
	for position, mt := range scenario.logs {
		if !synced[position] {
			imageLog := filepath.Join(image, filepath.Base(mt.wal.Fd.Name()))
			require.NoError(t, os.WriteFile(imageLog, scenario.atLastSync[position], 0o600))
		}
	}
	require.Equal(t, 2, numImmutables(scenario.db), "a rotated memtable was flushed during the test; the flusher was meant to be held")

	// The image opens with level 0 below its stall, so the reopened store can
	// flush the logs it replays and close.
	reopened, err := Open(scenario.opt.WithDir(image).WithValueDir(image).
		WithNumCompactors(2).WithNumLevelZeroTables(5).WithNumLevelZeroTablesStall(15))
	require.NoError(t, err)
	defer func() { require.NoError(t, reopened.Close()) }()
	for position, commit := range scenario.commits {
		err := reopened.View(func(txn *Txn) error {
			_, err := txn.Get([]byte(commit))
			return err
		})
		if !errors.Is(err, ErrKeyNotFound) {
			require.NoError(t, err)
			kept[position] = true
		}
	}
	return kept, synced
}

// requireAPrefix fails the test when a cut kept a commit and lost an earlier one.
func (scenario *rotatedLogsScenario) requireAPrefix(t *testing.T, cut string, kept [3]bool) {
	t.Helper()
	for later := 1; later < len(kept); later++ {
		for earlier := range later {
			if kept[later] && !kept[earlier] {
				t.Errorf("a power cut %s keeps the commit %q and loses the commit %q, which was committed first: "+
					"the durable store is not a prefix of its commits across a memtable rotation (BUG-0824); kept, oldest first: %v",
					cut, scenario.commits[later], scenario.commits[earlier], kept)
			}
		}
	}
}

// TestSyncCoversTwoRotatedMemtableLogsBeforeActive holds DB.Sync where it is
// about to sync the active memtable's log, with two rotated-out memtables
// waiting for their flush. Both of their logs must be synced by then, so a
// power cut there keeps the two earlier commits whatever it does to the third.
// Once Sync returns, all three logs are synced and all three commits kept.
func TestSyncCoversTwoRotatedMemtableLogsBeforeActive(t *testing.T) {
	scenario := openWithTwoRotatedLogs(t)

	parked := parkSyncInActiveWAL(t, scenario.db)
	require.Same(t, scenario.logs[2], parked.mt, "DB.Sync captured another active memtable than the one that holds the third commit")
	kept, synced := scenario.commitsAfterACutNow(t)
	t.Logf("in front of the active log's msync: synced, oldest first: %v; kept by a cut there: %v", synced, kept)
	if !synced[0] || !synced[1] {
		t.Errorf("DB.Sync reached the msync of the active memtable's log %s with a rotated-out log unsynced (synced, oldest first: %v): "+
			"the active log's commits become durable before the earlier ones in the rotated logs (BUG-0824)",
			filepath.Base(parked.walPath), synced)
	}
	if !kept[0] || !kept[1] {
		t.Errorf("a power cut in front of the active log's msync loses a commit of a rotated-out log (kept, oldest first: %v)", kept)
	}
	scenario.requireAPrefix(t, "inside DB.Sync, in front of the msync of the active memtable's log", kept)

	require.NoError(t, parked.resume(t))
	kept, synced = scenario.commitsAfterACutNow(t)
	require.Equal(t, [3]bool{true, true, true}, synced, "a log has unsynced pages after DB.Sync returned")
	require.Equal(t, [3]bool{true, true, true}, kept, "a commit made before DB.Sync is not in the image taken after it returned")
}

// failWALSyncs makes every msync of the memtable's log fail until the test
// ends, with no hook in DB.Sync: the log's mapping is replaced by the same
// mapping one byte in, and msync(2) refuses an address that is not page-aligned
// (EINVAL) before it writes anything. Nothing else reads the mapping of a
// rotated-out memtable while the flusher is held; the cleanup restores it
// before the store closes.
//
// This is not a failed write. The msync fails before any I/O: the log's pages
// stay dirty and the kernel records no error for the file. A real I/O error is
// the opposite (the failed pages are left clean and the error is reported
// once), and it is not modelled here. So the tests that use this check DB.Sync's
// control flow, that the call issues no later msync, and cannot see what a
// second Sync or kernel writeback does to the later logs after a real failure
// (BUG-0827).
func failWALSyncs(t *testing.T, mt *memTable) {
	t.Helper()
	mapping := mt.wal.Data
	mt.wal.Data = mapping[1:]
	t.Cleanup(func() { mt.wal.Data = mapping })
}

// TestSyncStopsAtAFailedRotatedMemtableLog fails the msync of a rotated-out
// memtable's log and checks what DB.Sync does next: it returns the error and
// syncs no later log, the active one included. Syncing a later log after an
// earlier one failed would make later commits durable without the earlier
// ones, which is the state the sync order exists to prevent.
//
// The two cases also show the order with three logs. When both rotated logs
// fail, the error names the oldest and not the newer one, which Sync never
// reached. When only the newer one fails, the oldest is synced by then.
func TestSyncStopsAtAFailedRotatedMemtableLog(t *testing.T) {
	cases := []struct {
		name string
		// failing lists the rotated logs whose msync fails, by position.
		failing []int
		// reached is the position of the log DB.Sync must stop at.
		reached int
		cut     string
	}{
		{
			name:    "both rotated logs fail",
			failing: []int{0, 1},
			reached: 0,
			cut:     "inside DB.Sync, before any log is synced",
		},
		{
			name:    "the newer rotated log fails",
			failing: []int{1},
			reached: 1,
			cut:     "inside DB.Sync, after the msync of the oldest rotated log and before the next",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			scenario := openWithTwoRotatedLogs(t)
			for _, position := range tc.failing {
				failWALSyncs(t, scenario.logs[position])
			}
			var reachedActive atomic.Bool
			syncActiveWALHook = func(*memTable) { reachedActive.Store(true) }
			t.Cleanup(func() { syncActiveWALHook = nil })

			err := scenario.db.Sync()
			require.Error(t, err, "DB.Sync returned nil although the msync of a rotated-out memtable's log failed")
			require.ErrorContains(t, err, scenario.logs[tc.reached].wal.Fd.Name())
			for position := tc.reached + 1; position < 2; position++ {
				if later := scenario.logs[position].wal.Fd.Name(); strings.Contains(err.Error(), later) {
					t.Errorf("DB.Sync went on to the rotated log %s after the msync of the older %s failed: %v",
						later, scenario.logs[tc.reached].wal.Fd.Name(), err)
				}
			}
			if reachedActive.Load() {
				t.Errorf("DB.Sync went on to sync the active memtable's log after the msync of the rotated-out %s failed: "+
					"the active log's commits become durable without the earlier ones (BUG-0824)", scenario.logs[tc.reached].wal.Fd.Name())
			}

			kept, synced := scenario.commitsAfterACutNow(t)
			t.Logf("after the failed DB.Sync (%v): synced, oldest first: %v; kept by a cut there: %v", err, synced, kept)
			for position := range tc.reached {
				if !synced[position] || !kept[position] {
					t.Errorf("the rotated log at position %d, older than the one that failed, is not synced (synced %v, kept %v): "+
						"DB.Sync does not sync the rotated logs oldest first", position, synced, kept)
				}
			}
			for position := tc.reached; position < len(synced); position++ {
				if synced[position] {
					t.Errorf("the log %s, at or after the one whose msync failed, has no unsynced page after DB.Sync returned "+
						"(it had some before the call): DB.Sync synced it, or the kernel wrote it back within the call",
						scenario.logs[position].wal.Fd.Name())
				}
			}
			scenario.requireAPrefix(t, tc.cut, kept)
		})
	}
}
