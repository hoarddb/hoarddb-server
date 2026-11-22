//go:build linux

/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Added by the hoardDB project, 2026-11-20: the tests of the value-log sync at
 * a memtable flush (hoardDB's BUG-0827, route (c) through a flush, the vouched
 * cut's slice 2). See hoardDB/third_party/README.md.
 */

package badger

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// durableValueLog records, for each value-log file of one store, the longest
// prefix an msync made durable (SyncTestHooks.AfterValueLogFileSync): the
// value-log files a power cut leaves are those prefixes, zeros after them.
type durableValueLog struct {
	dir     string
	mu      sync.Mutex
	covered map[string]int64
}

func (d *durableValueLog) synced(path string, covered int64) {
	if filepath.Clean(filepath.Dir(path)) != d.dir {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if name := filepath.Base(path); covered > d.covered[name] {
		d.covered[name] = covered
	}
}

// image returns now's files with each value-log file cut to its durable
// prefix and zero-filled to its length; a value-log file no msync covered
// since the record began is left out.
func (d *durableValueLog) image(now storeImage) storeImage {
	d.mu.Lock()
	defer d.mu.Unlock()
	image := now.only(func(name string) bool { return !isValueLogFile(name) })
	for name, content := range now.only(isValueLogFile) {
		covered, ok := d.covered[name]
		if !ok {
			continue
		}
		cut := make([]byte, len(content))
		copy(cut, content[:min(int(covered), len(content))])
		image[name] = cut
	}
	return image
}

// recordDurableValueLog starts recording the value-log msyncs of the store in
// dir until the test ends.
func recordDurableValueLog(t *testing.T, dir string) *durableValueLog {
	d := &durableValueLog{dir: filepath.Clean(dir), covered: map[string]int64{}}
	t.Cleanup(SetSyncTestHooks(SyncTestHooks{AfterValueLogFileSync: d.synced}))
	return d
}

// largeTestValue is a 1 MiB value of nonzero bytes, above the test stores'
// 1 KiB value threshold, so it goes to the value log.
func largeTestValue() []byte {
	large := make([]byte, 1<<20)
	for i := range large {
		large[i] = byte(i%251 + 1)
	}
	return large
}

// TestLargeValueVouchedByAFlushIsWhole is the store half of the vouched cut's
// T3b (BUG-0827, route (c) through a flush): a 1 MiB value committed after the
// last sync, its memtable flushed before any further sync. Once the MANIFEST
// names the table, the table vouches the value's pointer whatever the marker
// says (C = max(V, T)), so the value-log bytes it points at must be durable by
// then. The image is the store at the instant the MANIFEST change naming the
// table is synced, every value-log file at its last msync's durable prefix.
// Before the fix the flush synced no value log, the image held the pointer
// without its bytes, and the value read back as zeros or failed. The second
// case puts the value in a value-log file that rotated before the flush: the
// flush syncs only the newest file, and the rotated one rests on its sync at
// rotation (change 3); without that sync this case fails.
func TestLargeValueVouchedByAFlushIsWhole(t *testing.T) {
	for _, tc := range []struct {
		name         string
		valueLogSize int64
		rotated      bool
	}{
		{name: "in the newest value-log file", valueLogSize: 8 << 20},
		{name: "in a value-log file rotated before the flush", valueLogSize: 1 << 20, rotated: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, opt := openSyncLockTestDB(t, func(o Options) Options {
				return o.WithValueLogFileSize(tc.valueLogSize).WithNumCompactors(0).WithVouchedCut(0)
			})
			closeAtCleanup(t, db)
			durable := recordDurableValueLog(t, opt.Dir)

			setKey(t, db, "before")
			vouched, err := db.SyncVouched()
			require.NoError(t, err)

			large := largeTestValue()
			require.NoError(t, db.Update(func(txn *Txn) error { return txn.Set([]byte("large"), large) }))
			largeVersion := versionOf(t, db, "large")
			db.vlog.filesLock.RLock()
			rotated := db.vlog.maxFid > 1
			db.vlog.filesLock.RUnlock()
			require.Equal(t, tc.rotated, rotated, "SCENARIO NOT REACHED: the value log rotated (%v) after the large value", rotated)

			// The image at the first table naming of this store from here on: the
			// flush of the memtable that holds the pointer (nothing rotated before).
			images := make(chan storeImage, 1)
			var once sync.Once
			t.Cleanup(SetFlushStepHook(func(step FlushStep, flushing *DB) {
				if step != FlushTableNamed || filepath.Clean(flushing.Opts().Dir) != filepath.Clean(opt.Dir) {
					return
				}
				once.Do(func() { images <- durable.image(imageAt(t, opt.Dir)) })
			}))
			require.NoError(t, writeUntilRotated(db, activeMemTable(db), "fill"))
			var image storeImage
			select {
			case image = <-images:
			case <-time.After(60 * time.Second):
				t.Fatal("SCENARIO NOT REACHED: the memtable holding the large value was not flushed")
			}

			imageDir := writeImage(t, image)
			reopened, err := Open(opt.WithDir(imageDir).WithValueDir(imageDir).WithVouchedCut(vouched))
			require.NoError(t, err)
			defer func() { require.NoError(t, reopened.Close()) }()
			require.GreaterOrEqual(t, reopened.VouchedCutReport().Kept, largeVersion,
				"SCENARIO NOT REACHED: no table in the image vouches the large value (version %d, marker %d)", largeVersion, vouched)
			var read []byte
			err = reopened.View(func(txn *Txn) error {
				item, err := txn.Get([]byte("large"))
				if err != nil {
					return err
				}
				read, err = item.ValueCopy(nil)
				return err
			})
			if err != nil || !bytes.Equal(read, large) {
				t.Fatalf("a 1 MiB value vouched by a flushed table reads back with %d of its %d bytes intact (error: %v) "+
					"after a power cut at the instant the MANIFEST named the table: the table vouches a pointer whose "+
					"value-log bytes were not durable", commonPrefix(read, large), len(large), err)
			}
		})
	}
}

func commonPrefix(a, b []byte) int {
	n := 0
	for n < len(a) && n < len(b) && a[n] == b[n] {
		n++
	}
	return n
}

// flushEvents records, in order, what one store's flusher did: "flush" when
// it takes a memtable (SetFlushTestHook), each FlushStep, and "value log
// synced" for each value-log msync on any goroutine.
type flushEvents struct {
	dir    string
	mu     sync.Mutex
	events []string
	syncs  []time.Duration
}

func (e *flushEvents) add(event string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.events = append(e.events, event)
}

func (e *flushEvents) snapshot() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.events...)
}

// TestAFlushSyncsTheValueLogBeforeNamingItsTable counts the value-log msyncs of
// a flush and places them: exactly one per flush, after the flusher takes the
// memtable and before the MANIFEST names its table, whether or not the
// memtable points into the value log. It also logs how long each took.
// Mutations it must catch: no sync at the flush (none between the two), and
// the sync after the table is named or in level 0 (it comes after "table
// named").
func TestAFlushSyncsTheValueLogBeforeNamingItsTable(t *testing.T) {
	for _, withLarge := range []bool{true, false} {
		t.Run(fmt.Sprintf("a large value in the memtable: %v", withLarge), func(t *testing.T) {
			db, opt := openSyncLockTestDB(t, func(o Options) Options {
				return o.WithValueLogFileSize(8 << 20).WithNumCompactors(0)
			})
			closeAtCleanup(t, db)
			events := &flushEvents{dir: filepath.Clean(opt.Dir)}
			ours := func(flushing *DB) bool { return filepath.Clean(flushing.Opts().Dir) == events.dir }
			t.Cleanup(SetSyncTestHooks(SyncTestHooks{AfterValueLogFileSync: func(path string, covered int64) {
				if filepath.Clean(filepath.Dir(path)) == events.dir {
					events.add("value log synced")
				}
			}}))
			realSync := valueLogFileSync
			valueLogFileSync = func(lf *logFile) error {
				start := time.Now()
				err := realSync(lf)
				if filepath.Clean(filepath.Dir(lf.path)) == events.dir {
					events.mu.Lock()
					events.syncs = append(events.syncs, time.Since(start))
					events.mu.Unlock()
				}
				return err
			}
			t.Cleanup(func() { valueLogFileSync = realSync })
			t.Cleanup(SetFlushTestHook(func(flushing *DB) {
				if ours(flushing) {
					events.add("flush")
				}
			}))
			t.Cleanup(SetFlushStepHook(func(step FlushStep, flushing *DB) {
				if ours(flushing) {
					events.add(string(step))
				}
			}))

			if withLarge {
				require.NoError(t, db.Update(func(txn *Txn) error { return txn.Set([]byte("large"), largeTestValue()) }))
			}
			require.NoError(t, writeUntilRotated(db, activeMemTable(db), "fill"))
			deadline := time.Now().Add(60 * time.Second)
			for !strings.Contains(strings.Join(events.snapshot(), ","), string(FlushTableInLevelZero)) {
				require.True(t, time.Now().Before(deadline), "SCENARIO NOT REACHED: no flush finished; events %v", events.snapshot())
				time.Sleep(5 * time.Millisecond)
			}

			got := events.snapshot()
			first := -1
			for i, event := range got {
				if event == "flush" {
					first = i
					break
				}
			}
			require.GreaterOrEqual(t, first, 0, "SCENARIO NOT REACHED: the flusher never took a memtable; events %v", got)
			var named int
			for named = first; named < len(got) && got[named] != string(FlushTableNamed); named++ {
			}
			require.Less(t, named, len(got), "SCENARIO NOT REACHED: the flush named no table; events %v", got)
			syncsBefore := 0
			for _, event := range got[first:named] {
				if event == "value log synced" {
					syncsBefore++
				}
			}
			require.Equal(t, 1, syncsBefore, "the flush made %d value-log msyncs between taking the memtable and naming "+
				"its table, want exactly 1 (events in order: %v)", syncsBefore, got)
			events.mu.Lock()
			t.Logf("value-log msyncs of this store, in order: %v; events %v", events.syncs, got)
			events.mu.Unlock()
		})
	}
}

// TestAFailedValueLogSyncInAFlushIsNotRetriedIntoSuccess: a value-log msync
// that fails may have dropped the dirty pages and cleared the error, so the
// next msync of the file can succeed with the bytes gone. A flush whose
// value-log sync failed must never name its table on a retry, and a later
// DB.SyncVouched must fail too: a sync that consumed the error and reported
// the value log durable would let the table, or hoardDB's marker, vouch lost
// bytes. Mutation it must catch: the failure not kept (the retry's msync
// succeeds and the table is named).
func TestAFailedValueLogSyncInAFlushIsNotRetriedIntoSuccess(t *testing.T) {
	db, opt := openSyncLockTestDB(t, func(o Options) Options {
		return o.WithValueLogFileSize(8 << 20).WithNumCompactors(0)
	})
	dir := filepath.Clean(opt.Dir)
	require.NoError(t, db.Update(func(txn *Txn) error { return txn.Set([]byte("large"), largeTestValue()) }))

	injected := errors.New("injected msync failure of a value-log file")
	realSync := valueLogFileSync
	var mu sync.Mutex
	failed := 0
	valueLogFileSync = func(lf *logFile) error {
		if filepath.Clean(filepath.Dir(lf.path)) == dir {
			mu.Lock()
			defer mu.Unlock()
			if failed == 0 {
				failed++
				return injected
			}
		}
		return realSync(lf)
	}
	t.Cleanup(func() { valueLogFileSync = realSync })
	var namedMu sync.Mutex
	named := false
	t.Cleanup(SetFlushStepHook(func(step FlushStep, flushing *DB) {
		if step == FlushTableNamed && filepath.Clean(flushing.Opts().Dir) == dir {
			namedMu.Lock()
			named = true
			namedMu.Unlock()
		}
	}))

	require.NoError(t, writeUntilRotated(db, activeMemTable(db), "fill"))
	// The first attempt fails at once; the flusher retries every
	// flushRetryInterval. Two retries' worth.
	time.Sleep(2*flushRetryInterval + flushRetryInterval/2)
	mu.Lock()
	require.Equal(t, 1, failed, "SCENARIO NOT REACHED: the flush made no value-log msync")
	mu.Unlock()
	namedMu.Lock()
	require.False(t, named, "a flush whose value-log msync failed named its table on a retry: the retry's msync "+
		"succeeded on a file whose dirty pages the failure may have dropped")
	namedMu.Unlock()
	// DB.SyncVouched flattens its errors (y.CombineErrors), so the cause is matched by its text.
	_, err := db.SyncVouched()
	require.Error(t, err, "DB.SyncVouched after a failed value-log msync reported the value log durable")
	require.Contains(t, err.Error(), injected.Error(), "DB.SyncVouched failed for another reason than the kept msync failure")

	closed := make(chan error, 1)
	go func() { closed <- db.Close() }()
	select {
	case err := <-closed:
		require.Error(t, err, "Close reported success with a memtable it could not flush")
	case <-time.After(30 * time.Second):
		t.Fatal("Close did not return within 30 s")
	}
}

// TestAMemtableReplayedAtOpenSyncsTheValueLogBeforeItsTable: Open hands the
// memtables it replayed to the flusher before it opens the value log. Their
// pointers can name value-log bytes a killed process left in the page cache
// only, so their flush must sync the value log too, and it must wait for the
// value log to be open: before that the flush finds no value-log file, syncs
// nothing, and names its table anyway (and reads the value log's state while
// Open writes it, which -race reports). Mutation it must catch: the flush not
// waiting for the value log's open.
func TestAMemtableReplayedAtOpenSyncsTheValueLogBeforeItsTable(t *testing.T) {
	db, opt := openSyncLockTestDB(t, func(o Options) Options {
		return o.WithValueLogFileSize(8 << 20).WithNumCompactors(0)
	})
	require.NoError(t, db.Update(func(txn *Txn) error { return txn.Set([]byte("large"), largeTestValue()) }))
	setKey(t, db, "small")
	// The image of a killed process: the active memtable's log as it is now.
	imageDir := writeImage(t, imageAt(t, opt.Dir))
	require.NoError(t, db.Close())

	events := &flushEvents{dir: filepath.Clean(imageDir)}
	ours := func(flushing *DB) bool { return filepath.Clean(flushing.Opts().Dir) == events.dir }
	t.Cleanup(SetSyncTestHooks(SyncTestHooks{AfterValueLogFileSync: func(path string, covered int64) {
		if filepath.Clean(filepath.Dir(path)) == events.dir {
			events.add("value log synced")
		}
	}}))
	t.Cleanup(SetFlushStepHook(func(step FlushStep, flushing *DB) {
		if ours(flushing) {
			events.add(string(step))
		}
	}))
	realSyncDir := flushSyncValueDir
	flushSyncValueDir = func(dir string) error {
		if filepath.Clean(dir) == events.dir {
			events.add("value directory synced")
		}
		return realSyncDir(dir)
	}
	t.Cleanup(func() { flushSyncValueDir = realSyncDir })
	reopened, err := Open(opt.WithDir(imageDir).WithValueDir(imageDir))
	require.NoError(t, err)
	defer func() { require.NoError(t, reopened.Close()) }()
	deadline := time.Now().Add(60 * time.Second)
	for !strings.Contains(strings.Join(events.snapshot(), ","), string(FlushTableNamed)) {
		require.True(t, time.Now().Before(deadline), "SCENARIO NOT REACHED: the replayed memtable was not flushed; events %v",
			events.snapshot())
		time.Sleep(5 * time.Millisecond)
	}
	got := events.snapshot()
	require.Equal(t, "value log synced", got[0], "the flush of a memtable replayed at open named its table with no "+
		"value-log msync before it (events in order: %v)", got)
	named := strings.Index(strings.Join(got, ","), string(FlushTableNamed))
	require.Contains(t, strings.Join(got, ",")[:named], "value directory synced", "the flush of a memtable replayed "+
		"at open named its table with no fsync of the value directory before it (events in order: %v)", got)
	require.True(t, hasKey(t, reopened, "large"), "the replayed large value is gone")
}

// TestAFailedValueLogSyncAtOpenFailsTheOpen: the fsync Open makes of the
// value-log files before it flushes replayed memtables is what lets those
// flushes skip the value log; when it fails, Open fails with its cause
// rather than flush tables whose pointers name bytes it could not make
// durable.
func TestAFailedValueLogSyncAtOpenFailsTheOpen(t *testing.T) {
	db, opt := openSyncLockTestDB(t, func(o Options) Options { return o.WithValueLogFileSize(8 << 20) })
	require.NoError(t, db.Update(func(txn *Txn) error { return txn.Set([]byte("large"), largeTestValue()) }))
	imageDir := writeImage(t, imageAt(t, opt.Dir))
	require.NoError(t, db.Close())

	injected := errors.New("injected fsync failure of a value-log file at open")
	realSync := valueLogFileSyncAtOpen
	valueLogFileSyncAtOpen = func(path string) error { return injected }
	t.Cleanup(func() { valueLogFileSyncAtOpen = realSync })
	reopened, err := Open(opt.WithDir(imageDir).WithValueDir(imageDir))
	if err == nil {
		require.NoError(t, reopened.Close())
	}
	require.ErrorIs(t, err, injected, "Open flushed replayed memtables after the fsync of their value log failed")
}

// TestAFailedValueLogMsyncOutsideAFlushIsKept: a flush syncs only the newest
// value-log file and leans on the msync a rotated file got at its rotation,
// and on the msync logFile.Truncate makes before it grows the newest file
// past its preallocation. A failed msync there can drop pages that earlier,
// acknowledged pointers name; if a retry's msync then succeeds, the file
// rotates or grows, the next flush syncs a newer file, and a table is named
// over the lost bytes. Each case fails one msync (rotation; growth), then
// asserts that the next write that reaches it fails too and that no table is
// ever named, through Close's flush. Mutations they must catch: doneWriting,
// or the growth's msync, calling the file sync without the keep.
func TestAFailedValueLogMsyncOutsideAFlushIsKept(t *testing.T) {
	// A value is at most the value-log file size (1 MiB here) and the file is
	// preallocated at twice that, so the growth needs one request whose values
	// pass the preallocation together: three of 900 KiB after the first.
	for _, tc := range []struct {
		name   string
		second []int  // the values of the one transaction that reaches the failing msync
		where  string // what the failed write's error says the msync was for
	}{
		{name: "at a rotation", second: []int{600 << 10}, where: "Unable to sync value log"},
		{name: "at a growth past the preallocation", second: []int{900 << 10, 900 << 10, 900 << 10},
			where: "before truncating it"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, opt := openSyncLockTestDB(t, func(o Options) Options {
				return o.WithValueLogFileSize(1 << 20).WithNumCompactors(0)
			})
			dir := filepath.Clean(opt.Dir)
			var namedMu sync.Mutex
			named := false
			t.Cleanup(SetFlushStepHook(func(step FlushStep, flushing *DB) {
				if step == FlushTableNamed && filepath.Clean(flushing.Opts().Dir) == dir {
					namedMu.Lock()
					named = true
					namedMu.Unlock()
				}
			}))
			value := func(size int, seed byte) []byte {
				v := make([]byte, size)
				for i := range v {
					v[i] = byte(i%241) + seed
				}
				return v
			}
			// An acknowledged pointer into the first value-log file.
			require.NoError(t, db.Update(func(txn *Txn) error { return txn.Set([]byte("a"), value(512<<10, 1)) }))

			injected := errors.New("injected msync failure of a value-log file")
			realSync := valueLogFileSync
			var mu sync.Mutex
			failed := 0
			valueLogFileSync = func(lf *logFile) error {
				if filepath.Clean(filepath.Dir(lf.path)) == dir {
					mu.Lock()
					defer mu.Unlock()
					if failed == 0 {
						failed++
						return injected
					}
				}
				return realSync(lf)
			}
			t.Cleanup(func() { valueLogFileSync = realSync })

			err := db.Update(func(txn *Txn) error {
				for i, size := range tc.second {
					if err := txn.Set([]byte(fmt.Sprintf("b%d", i)), value(size, byte(2+i))); err != nil {
						return err
					}
				}
				return nil
			})
			require.ErrorContains(t, err, injected.Error(), "SCENARIO NOT REACHED: the write did not reach the failing msync")
			require.ErrorContains(t, err, tc.where, "SCENARIO NOT REACHED: the injected failure hit another msync than the "+
				"one this case is about")
			err = db.Update(func(txn *Txn) error { return txn.Set([]byte("c"), value(2<<10, 3)) })
			require.Error(t, err, "the write after a failed value-log msync was acknowledged: the retry's msync succeeded "+
				"on a file whose dirty pages the failure may have dropped")
			require.Contains(t, err.Error(), injected.Error(), "the write after the failed msync failed for another reason")

			closed := make(chan error, 1)
			go func() { closed <- db.Close() }()
			select {
			case <-closed:
			case <-time.After(30 * time.Second):
				t.Fatal("Close did not return within 30 s")
			}
			namedMu.Lock()
			defer namedMu.Unlock()
			require.False(t, named, "a table was named over a value-log file whose msync failed")
		})
	}
}

// TestAFlushSyncsTheValueDirectoryAfterAValueLogFileWasCreated: the flush's
// msync makes a value-log file's bytes durable, not its directory entry, and
// a file created since the store's last directory fsync could vanish at a
// power cut on a filesystem that does not commit a new file's entry with its
// first fsync. A flush therefore fsyncs the value directory before the
// MANIFEST names its table when a value-log file was created since the last
// such fsync, and only then. Mutation it must catch: no directory fsync.
func TestAFlushSyncsTheValueDirectoryAfterAValueLogFileWasCreated(t *testing.T) {
	db, opt := openSyncLockTestDB(t, func(o Options) Options {
		return o.WithValueLogFileSize(1 << 20).WithNumCompactors(0)
	})
	closeAtCleanup(t, db)
	events := &flushEvents{dir: filepath.Clean(opt.Dir)}
	ours := func(flushing *DB) bool { return filepath.Clean(flushing.Opts().Dir) == events.dir }
	realSyncDir := flushSyncValueDir
	flushSyncValueDir = func(dir string) error {
		if filepath.Clean(dir) == events.dir {
			events.add("value directory synced")
		}
		return realSyncDir(dir)
	}
	t.Cleanup(func() { flushSyncValueDir = realSyncDir })
	t.Cleanup(SetFlushTestHook(func(flushing *DB) {
		if ours(flushing) {
			events.add("flush")
		}
	}))
	t.Cleanup(SetFlushStepHook(func(step FlushStep, flushing *DB) {
		if ours(flushing) && step == FlushTableNamed {
			events.add(string(step))
		}
	}))
	flushOnce := func(prefix string) []string {
		t.Helper()
		before := len(events.snapshot())
		require.NoError(t, writeUntilRotated(db, activeMemTable(db), prefix))
		deadline := time.Now().Add(60 * time.Second)
		for {
			got := events.snapshot()[before:]
			if strings.Contains(strings.Join(got, ","), string(FlushTableNamed)) {
				return got
			}
			require.True(t, time.Now().Before(deadline), "SCENARIO NOT REACHED: no table named; events %v", got)
			time.Sleep(5 * time.Millisecond)
		}
	}

	// A value larger than the 1 MiB file rotates the value log: a new file.
	require.NoError(t, db.Update(func(txn *Txn) error { return txn.Set([]byte("large"), largeTestValue()) }))
	first := flushOnce("first")
	require.Equal(t, []string{"flush", "value directory synced", string(FlushTableNamed)}, first,
		"a flush after a value-log file was created did not fsync the value directory before naming its table")
	second := flushOnce("second")
	require.Equal(t, []string{"flush", string(FlushTableNamed)}, second,
		"a flush with no value-log file created since the last directory fsync fsynced the directory")
}
