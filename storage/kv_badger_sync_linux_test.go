//go:build linux

package storage

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"golang.org/x/sys/unix"
)

// TestBadgerSyncCoversARotatedMemtable defends against a regression: a write committed
// without a sync, whose memtable another writer then rotates out, must be made
// durable by the next barrier (Commit(true) or KV.Sync). Badger v4.9.6's
// DB.Sync synced only the active memtable's log, so the rotated one stayed in
// the page cache until its flush to a table, and a power cut in that window
// lost an acknowledged write.
//
// A test cannot pull the power, so the proxy is the kernel's own account of
// the file: after the barrier returns, the page cache must hold no dirty and no
// under-writeback page of the .mem file that holds the write (cachestat(2)).
// That is the condition fsync/msync establishes and the one a power cut tests.
// The flusher is held, so the rotated memtable stays immutable and unflushed
// for the whole test: no compactors and a level-0 stall of 2 let the first two
// flushes fill level 0, and from then on badger.SetFlushTestHook keeps the
// flusher in front of the next flush until the cleanup.
func TestBadgerSyncCoversARotatedMemtable(t *testing.T) {
	barriers := []struct {
		name    string
		barrier func(kv *BadgerKV) error
	}{
		{"a batch committed with sync", func(kv *BadgerKV) error {
			barrierBatch := kv.NewBatch()
			if err := barrierBatch.Set([]byte("bug-0215-barrier"), []byte("B")); err != nil {
				return fmt.Errorf("staging the barrier write: %w", err)
			}
			return barrierBatch.Commit(true)
		}},
		{"KV.Sync", func(kv *BadgerKV) error { return kv.Sync() }},
	}
	for _, tt := range barriers {
		t.Run(tt.name, func(t *testing.T) {
			storeDir := diskBackedTempDir(t)
			kv := openStoreWithHeldFlusher(t, storeDir)

			// Two memtables flushed: level 0 is at the stall.
			filler := &memtableFiller{t: t, kv: kv, dir: storeDir}
			for range 2 {
				filler.fillUntilMemFiles(2)
				waitForMemFiles(t, storeDir, 1)
			}
			// From here the flusher waits in front of each flush, so the rotated memtable's log
			// stays on disk, the only copy of the unsynced write. (At the level-0 stall the
			// flush has already named its table and removed the log: BUG-0858's fix.)
			holdFlushesAt(t, storeDir)

			unsyncedMarker := bytes.Repeat([]byte("bug-0215-unsynced-write-"), 8)
			unsyncedBatch := kv.NewBatch()
			if err := unsyncedBatch.Set([]byte("bug-0215-unsynced"), unsyncedMarker); err != nil {
				t.Fatalf("staging the unsynced write: %v", err)
			}
			if err := unsyncedBatch.Commit(false); err != nil {
				t.Fatalf("committing the unsynced write: %v", err)
			}

			// Another writer rotates the memtable holding it; its flush stalls.
			filler.fillUntilMemFiles(2)
			rotatedWAL := memFiles(t, storeDir)[0]
			walBytes, err := os.ReadFile(rotatedWAL)
			if err != nil {
				t.Fatalf("reading the rotated memtable's log: %v", err)
			}
			if !bytes.Contains(walBytes, unsyncedMarker) {
				t.Fatalf("the rotated memtable's log %s does not hold the unsynced write; the scenario did not set up", rotatedWAL)
			}
			if dirtyPages, _ := pageCacheState(t, rotatedWAL); dirtyPages == 0 {
				t.Skipf("the kernel wrote %s back before the barrier (background writeback); this run cannot tell a barrier from no barrier", rotatedWAL)
			}

			if err := tt.barrier(kv); err != nil {
				t.Fatalf("barrier: %v", err)
			}

			walsAfterBarrier := memFiles(t, storeDir)
			if len(walsAfterBarrier) != 2 || walsAfterBarrier[0] != rotatedWAL {
				t.Fatalf("the rotated memtable was flushed during the barrier (logs now %v); the flusher was meant to be held", walsAfterBarrier)
			}
			// Control: the active memtable's log, which every Badger version
			// syncs, reads clean. A failure below is then the barrier, not the
			// proxy.
			if activeDirty, activeWriteback := pageCacheState(t, walsAfterBarrier[1]); activeDirty != 0 || activeWriteback != 0 {
				t.Fatalf("the active memtable's log has %d dirty and %d under-writeback pages after the barrier; the proxy cannot see a sync on this filesystem",
					activeDirty, activeWriteback)
			}
			dirtyPages, writebackPages := pageCacheState(t, rotatedWAL)
			if dirtyPages != 0 || writebackPages != 0 {
				t.Errorf("after the barrier returned, the rotated memtable's log %s still has %d dirty and %d under-writeback pages: "+
					"an earlier committed write is not durable, and a power cut now loses it",
					filepath.Base(rotatedWAL), dirtyPages, writebackPages)
			}
		})
	}
}

// holdFlushesAt holds the flusher of the store at storeDir in front of each flush
// (badger.SetFlushTestHook) until the test's cleanup, which runs before the store's close
// registered earlier.
func holdFlushesAt(t *testing.T, storeDir string) {
	t.Helper()
	release := make(chan struct{})
	restore := badger.SetFlushTestHook(func(db *badger.DB) {
		if filepath.Clean(db.Opts().Dir) == filepath.Clean(storeDir) {
			<-release
		}
	})
	t.Cleanup(func() {
		close(release)
		restore()
	})
}

// openStoreWithHeldFlusher opens the node store's options with an 8 MiB
// memtable, no compactors and a level-0 stall of 2.
func openStoreWithHeldFlusher(t *testing.T, storeDir string) *BadgerKV {
	t.Helper()
	opts, err := badgerOptions(storeDir, BadgerTuning{MemTableMB: 32}, nil)
	if err != nil {
		t.Fatalf("building the store options: %v", err)
	}
	opts = opts.
		WithMemTableSize(8 << 20).
		WithNumCompactors(0).
		WithNumLevelZeroTables(1).
		WithNumLevelZeroTablesStall(2).
		WithLogger(nil)
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatalf("opening Badger at %s: %v", storeDir, err)
	}
	kv, err := newBadgerKV(db, storeDir)
	if err != nil {
		t.Fatalf("wrapping the store: %v", err)
	}
	t.Cleanup(func() {
		if err := kv.Close(); err != nil {
			t.Errorf("closing the store: %v", err)
		}
	})
	return kv
}

type memtableFiller struct {
	t       *testing.T
	kv      *BadgerKV
	dir     string
	nextKey int
}

// fillUntilMemFiles commits unsynced 1 KiB writes until the store has
// wantMemFiles .mem files, which is when a rotation has happened.
func (f *memtableFiller) fillUntilMemFiles(wantMemFiles int) {
	f.t.Helper()
	fillValue := bytes.Repeat([]byte{0x5A}, 1<<10)
	for range 1000 {
		if len(memFiles(f.t, f.dir)) >= wantMemFiles {
			return
		}
		fillBatch := f.kv.NewBatch()
		for range 256 {
			if err := fillBatch.Set([]byte(fmt.Sprintf("fill-%09d", f.nextKey)), fillValue); err != nil {
				f.t.Fatalf("staging a fill write: %v", err)
			}
			f.nextKey++
		}
		if err := fillBatch.Commit(false); err != nil {
			f.t.Fatalf("committing a fill batch: %v", err)
		}
	}
	f.t.Fatalf("no memtable rotation after %d fill writes", f.nextKey)
}

func waitForMemFiles(t *testing.T, storeDir string, wantMemFiles int) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for len(memFiles(t, storeDir)) != wantMemFiles {
		if time.Now().After(deadline) {
			t.Fatalf("the store still has %d .mem files after 30s, want %d: a flush did not finish", len(memFiles(t, storeDir)), wantMemFiles)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// memFiles lists the memtable logs, oldest first: Badger names them by a
// zero-padded, increasing file id.
func memFiles(t *testing.T, storeDir string) []string {
	t.Helper()
	walPaths, err := filepath.Glob(filepath.Join(storeDir, "*.mem"))
	if err != nil {
		t.Fatalf("listing memtable logs: %v", err)
	}
	sort.Strings(walPaths)
	return walPaths
}

func pageCacheState(t *testing.T, path string) (dirtyPages, writebackPages uint64) {
	t.Helper()
	walFile, err := os.Open(path)
	if err != nil {
		t.Fatalf("opening %s: %v", path, err)
	}
	defer func() {
		if err := walFile.Close(); err != nil {
			t.Errorf("closing %s: %v", path, err)
		}
	}()
	var cacheState unix.Cachestat_t
	// A zero range is the whole file.
	err = unix.Cachestat(uint(walFile.Fd()), &unix.CachestatRange{}, &cacheState, 0)
	if errors.Is(err, unix.ENOSYS) {
		t.Skip("cachestat(2) needs Linux 6.5 or later")
	}
	if err != nil {
		t.Fatalf("cachestat on %s: %v", path, err)
	}
	return cacheState.Dirty, cacheState.Writeback
}

// diskBackedTempDir returns a temporary directory on a filesystem with a
// writeback page cache. tmpfs and ramfs never write back, so fsync leaves their
// pages dirty and the proxy would fail the patched build too; a host whose
// /tmp is tmpfs gets a directory under the user cache directory instead.
func diskBackedTempDir(t *testing.T) string {
	t.Helper()
	if candidate := t.TempDir(); isDiskBacked(t, candidate) {
		return candidate
	}
	cacheRoot, err := os.UserCacheDir()
	if err != nil {
		t.Skipf("the temporary directory is in memory and there is no user cache directory: %v", err)
	}
	if err := os.MkdirAll(cacheRoot, 0o700); err != nil {
		t.Fatalf("creating %s: %v", cacheRoot, err)
	}
	candidate, err := os.MkdirTemp(cacheRoot, "hoarddb-bug-0215-")
	if err != nil {
		t.Fatalf("creating a directory under %s: %v", cacheRoot, err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(candidate); err != nil {
			t.Errorf("removing %s: %v", candidate, err)
		}
	})
	if !isDiskBacked(t, candidate) {
		t.Skip("no disk-backed directory: both the temporary and the user cache directory are in memory")
	}
	return candidate
}

func isDiskBacked(t *testing.T, dir string) bool {
	t.Helper()
	var fsStat unix.Statfs_t
	if err := unix.Statfs(dir, &fsStat); err != nil {
		t.Fatalf("statfs %s: %v", dir, err)
	}
	return fsStat.Type != unix.TMPFS_MAGIC && fsStat.Type != unix.RAMFS_MAGIC
}
