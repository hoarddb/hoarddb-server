/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Modified by the hoardDB project, 2026-10-01: DB.Sync also syncs the WAL of
 * every immutable memtable. See hoardDB/third_party/README.md.
 *
 * Modified by the hoardDB project, 2026-10-05: a memtable flush that keeps
 * failing refuses writes (ErrWritesRefused, DB.WriteRefusal) instead of
 * acknowledging them while it retries forever; Close stops retrying it and
 * reports the memtables it left unflushed; a full memtable that cannot be
 * replaced stays current and refuses the write instead of leaving no
 * memtable. See hoardDB/third_party/README.md.
 *
 * Modified by the hoardDB project, 2026-10-18: Close does not wait for a level-0
 * compaction that cannot succeed (ErrL0StalledByDamagedTable). See
 * hoardDB/third_party/README.md.
 *
 * Modified by the hoardDB project, 2026-10-19: a write that would wait for room
 * behind a level 0 held at its stall limit by a damaged table is refused,
 * naming the table. See hoardDB/third_party/README.md.
 *
 * Modified by the hoardDB project, 2026-10-26: Open refuses an unknown
 * TableSyncMode. See hoardDB/third_party/README.md.
 *
 * Modified by the hoardDB project, 2026-10-27: DB.Sync syncs the active
 * memtable's WAL after releasing db.lock, holding a reference on it, so writes
 * that rotate the memtable and reads do not wait for the msync. See
 * hoardDB/third_party/README.md.
 *
 * Modified by the hoardDB project, 2026-11-15: DB.Sync syncs the logs of the
 * immutable memtables, oldest first, before the active memtable's log, so one
 * call never forces a commit to disk ahead of an earlier one; after a log
 * fails, this call syncs no later log. See hoardDB/third_party/README.md.
 *
 * Modified by the hoardDB project, 2026-11-15: the DB carries the prefix bloom
 * counters, and Open refuses a BloomPrefixRules list whose ids a table's index
 * could not keep apart. See hoardDB/third_party/README.md.
 *
 * Modified by the hoardDB project, 2026-11-17: the vouched cut. Open refuses
 * WithVouchedCut with ReadOnly, InMemory or managed mode, cuts the memtable
 * logs above the vouched version and starts commit versions above it;
 * DB.writeRequests records the highest version written to a memtable, which
 * DB.SyncVouched reads before its capture (DB.Sync calls it); the calls that
 * write out of version order are refused under the cut. See
 * hoardDB/third_party/README.md.
 *
 * Modified by the hoardDB project, 2026-11-18: a flush on disk names its
 * table, removes the memtable's log durably and only then inserts the table
 * into level 0 (DB.flushToNamedTable); Open removes a log whose table the
 * MANIFEST already names before compactions start; Close closes, and keeps,
 * a named table no level holds; DB.flushMemtable calls the flush test hook.
 * See hoardDB/third_party/README.md.
 * Modified by the hoardDB project, 2026-11-19: a flush whose MANIFEST change
 * fails keeps its table file and is not retried (memTable.failedNaming,
 * ErrFlushNotNamed, logged once, hoardDB's BUG-0868). See
 * hoardDB/third_party/README.md.
 * Modified by the hoardDB project, 2026-11-20: a flush on disk syncs the
 * value log before it builds its table (DB.flushToNamedTable), and Open
 * fsyncs the value-log files before it flushes replayed memtables
 * (DB.syncValueLogFilesAtOpen; hoardDB's BUG-0827, slice 2). See
 * hoardDB/third_party/README.md.
 */

package badger

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"expvar"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	humanize "github.com/dustin/go-humanize"

	"github.com/dgraph-io/badger/v4/fb"
	"github.com/dgraph-io/badger/v4/options"
	"github.com/dgraph-io/badger/v4/pb"
	"github.com/dgraph-io/badger/v4/skl"
	"github.com/dgraph-io/badger/v4/table"
	"github.com/dgraph-io/badger/v4/y"
	"github.com/dgraph-io/ristretto/v2"
	"github.com/dgraph-io/ristretto/v2/z"
)

var (
	badgerPrefix = []byte("!badger!")       // Prefix for internal keys used by badger.
	txnKey       = []byte("!badger!txn")    // For indicating end of entries in txn.
	bannedNsKey  = []byte("!badger!banned") // For storing the banned namespaces.
)

type closers struct {
	updateSize  *z.Closer
	compactors  *z.Closer
	memtable    *z.Closer
	writes      *z.Closer
	valueGC     *z.Closer
	pub         *z.Closer
	cacheHealth *z.Closer
}

type lockedKeys struct {
	sync.RWMutex
	keys map[uint64]struct{}
}

func (lk *lockedKeys) add(key uint64) {
	lk.Lock()
	defer lk.Unlock()
	lk.keys[key] = struct{}{}
}

func (lk *lockedKeys) has(key uint64) bool {
	lk.RLock()
	defer lk.RUnlock()
	_, ok := lk.keys[key]
	return ok
}

func (lk *lockedKeys) all() []uint64 {
	lk.RLock()
	defer lk.RUnlock()
	keys := make([]uint64, 0, len(lk.keys))
	for key := range lk.keys {
		keys = append(keys, key)
	}
	return keys
}

// DB provides the various functions required to interact with Badger.
// DB is thread-safe.
type DB struct {
	testOnlyDBExtensions

	lock sync.RWMutex // Guards list of inmemory tables, not individual reads and writes.

	// syncs is read-locked by every Sync for its whole run and write-locked by
	// close before it closes the value log. Sync syncs the memtable WALs outside
	// db.lock, so close no longer waits for them at db.lock; without this, a
	// Sync in progress reached valueLog.sync after valueLog.Close had taken its
	// file locks for good, and waited forever.
	syncs sync.RWMutex

	dirLockGuard *directoryLockGuard
	// nil if Dir and ValueDir are the same
	valueDirGuard *directoryLockGuard

	closers closers

	mt  *memTable   // Our latest (actively written) in-memory table
	imm []*memTable // Add here only AFTER pushing to flushChan.

	// flushCond signals the (serial) write path that the flush goroutine has made
	// progress, i.e. it has removed a memtable from imm and there may now be room
	// in flushChan. Its Locker is db.lock (write lock), the same lock that guards
	// imm and that ensureRoomForWrite holds, so the room check and the wait are
	// entered atomically. This replaces the 10ms busy-sleep that previously
	// polled flushChan from writeRequests.
	flushCond *sync.Cond

	// Initialized via openMemTables.
	nextMemFid int

	opt       Options
	manifest  *manifestFile
	lc        *levelsController
	vlog      valueLog
	writeCh   chan *request
	flushChan chan *memTable // For flushing memtables.
	closeOnce sync.Once      // For closing DB only once.

	blockWrites atomic.Int32
	isClosed    atomic.Uint32

	// flushRefusal is set once the memtable flush has failed
	// flushFailuresBeforeRefusal times in a row, and cleared when a flush
	// succeeds; while set, writes are refused. rotateRefusal is set while a
	// full memtable cannot be replaced, and cleared when one is. Both hold an
	// error wrapping ErrWritesRefused.
	flushRefusal  atomic.Pointer[error]
	rotateRefusal atomic.Pointer[error]
	// unflushed counts the memtables Close left unflushed because their flush
	// kept failing, and unflushedCause is the last flush error. Written by the
	// flush goroutine and read by close after it has exited.
	unflushed      int
	unflushedCause error

	// gcActive is set while a vlog GC rewrite (scan + write-back) is in flight,
	// and gcDiscardTs records the DB's max version captured at its start. While
	// active, last-level compaction clamps its discard threshold to gcDiscardTs
	// (see levelsController.subcompact) so it will not purge any version newer than
	// the rewrite's start — in particular a tombstone for a key the rewrite is
	// about to write back. That tombstone survives to shadow the write-back, so a
	// key deleted mid-GC is not resurrected (#2286). Released when GC finishes.
	gcActive    atomic.Bool
	gcDiscardTs atomic.Uint64

	orc              *oracle
	bannedNamespaces *lockedKeys
	threshold        *vlogThreshold

	pub        *publisher
	registry   *KeyRegistry
	blockCache *ristretto.Cache[[]byte, *table.Block]
	indexCache *ristretto.Cache[uint64, *fb.TableIndex]
	allocPool  *z.AllocatorPool

	// bloomPrefix counts what the prefix bloom filter did on this DB, per rule
	// (BloomPrefixStats). It belongs to the DB so two DBs in one process never
	// share a count. hoardDB.
	bloomPrefix table.BloomPrefixCounters

	// writtenVersion is the highest commit version fully written to a
	// memtable: DB.writeRequests raises it right after DB.writeToLSM returned
	// for a request and before any waiter of the group is released, and Open
	// starts it at the version nextTxnTs starts from. One goroutine writes in
	// version order, so every version at or below it is in a memtable, and a
	// caller whose commit has returned finds it at or below it.
	// DB.SyncVouched reads it before its capture. hoardDB.
	writtenVersion atomic.Uint64
	// replayCut is the vouched cut's state while openMemTables replays the
	// logs, nil at every other time and without WithVouchedCut. hoardDB.
	replayCut *vouchedCutScan
	// vouchedReport is what the cut did, set during Open (DB.VouchedCutReport).
	vouchedReport VouchedCutReport
	// logFilesCreated moves each time a memtable log or a value-log file is
	// created, before any commit is written to it; Open starts it at 1, so the
	// files Open found or created count as not yet covered by a directory
	// fsync of the caller's (DB.LogFilesCreated). hoardDB.
	logFilesCreated atomic.Uint64

	// valueLogOpen is closed once Open has opened the value log. Open starts
	// the flusher, and hands it the replayed memtables, before it opens the
	// value log; a flush before that does not touch the value log, whose
	// files Open synced already (DB.syncValueLogFilesAtOpen). hoardDB.
	valueLogOpen chan struct{}

	// valueDirSyncFailed is a flush's failed fsync of the value directory,
	// kept (DB.syncValueLogForFlush). hoardDB.
	valueDirSyncFailed atomic.Pointer[valueDirSyncFailure]
}

const (
	kvWriteChCapacity = 1000
)

func checkAndSetOptions(opt *Options) error {
	// It's okay to have zero compactors which will disable all compactions but
	// we cannot have just one compactor otherwise we will end up with all data
	// on level 2.
	if opt.NumCompactors == 1 {
		return errors.New("Cannot have 1 compactor. Need at least 2")
	}

	if opt.InMemory && (opt.Dir != "" || opt.ValueDir != "") {
		return errors.New("Cannot use badger in Disk-less mode with Dir or ValueDir set")
	}
	// Refused here, not at the first flush, where it would fail every flush
	// and end in refused writes.
	if opt.TableSyncMode != options.TableSyncStepwise && opt.TableSyncMode != options.TableSyncMsync {
		return fmt.Errorf("unknown TableSyncMode %d: use options.TableSyncStepwise or options.TableSyncMsync", opt.TableSyncMode)
	}
	if err := checkBloomPrefixRules(opt.BloomPrefixRules); err != nil {
		return err
	}
	if err := checkVouchedCutOptions(opt); err != nil {
		return err
	}
	opt.maxBatchSize = (15 * opt.MemTableSize) / 100
	opt.maxBatchCount = opt.maxBatchSize / int64(skl.MaxNodeSize)

	// This is the maximum value, vlogThreshold can have if dynamic thresholding is enabled.
	opt.maxValueThreshold = math.Min(maxValueThreshold, float64(opt.maxBatchSize))
	if opt.VLogPercentile < 0.0 || opt.VLogPercentile > 1.0 {
		return errors.New("vlogPercentile must be within range of 0.0-1.0")
	}

	// We are limiting opt.ValueThreshold to maxValueThreshold for now.
	if opt.ValueThreshold > maxValueThreshold {
		return fmt.Errorf("Invalid ValueThreshold, must be less or equal to %d",
			maxValueThreshold)
	}

	// If ValueThreshold is greater than opt.maxBatchSize, we won't be able to push any data using
	// the transaction APIs. Transaction batches entries into batches of size opt.maxBatchSize.
	if opt.ValueThreshold > opt.maxBatchSize {
		return fmt.Errorf("Valuethreshold %d greater than max batch size of %d. Either "+
			"reduce opt.ValueThreshold or increase opt.BaseTableSize.",
			opt.ValueThreshold, opt.maxBatchSize)
	}
	// ValueLogFileSize should be strictly LESS than 2<<30 otherwise we will
	// overflow the uint32 when we mmap it in OpenMemtable.
	if !(opt.ValueLogFileSize < 2<<30 && opt.ValueLogFileSize >= 1<<20) {
		return ErrValueLogSize
	}

	if opt.ReadOnly {
		// Do not perform compaction in read only mode.
		opt.CompactL0OnClose = false
	}

	needCache := (opt.Compression != options.None) || (len(opt.EncryptionKey) > 0)
	if needCache && opt.BlockCacheSize == 0 {
		panic("BlockCacheSize should be set since compression/encryption are enabled")
	}
	return nil
}

// Open returns a new DB object.
func Open(opt Options) (*DB, error) {
	if err := checkAndSetOptions(&opt); err != nil {
		return nil, err
	}
	var dirLockGuard, valueDirLockGuard *directoryLockGuard

	// Create directories and acquire lock on it only if badger is not running in InMemory mode.
	// We don't have any directories/files in InMemory mode so we don't need to acquire
	// any locks on them.
	if !opt.InMemory {
		if err := createDirs(opt); err != nil {
			return nil, err
		}
		var err error
		if !opt.BypassLockGuard {
			dirLockGuard, err = acquireDirectoryLock(opt.Dir, lockFile, opt.ReadOnly)
			if err != nil {
				return nil, err
			}
			defer func() {
				if dirLockGuard != nil {
					_ = dirLockGuard.release()
				}
			}()
			absDir, err := filepath.Abs(opt.Dir)
			if err != nil {
				return nil, err
			}
			absValueDir, err := filepath.Abs(opt.ValueDir)
			if err != nil {
				return nil, err
			}
			if absValueDir != absDir {
				valueDirLockGuard, err = acquireDirectoryLock(opt.ValueDir, lockFile, opt.ReadOnly)
				if err != nil {
					return nil, err
				}
				defer func() {
					if valueDirLockGuard != nil {
						_ = valueDirLockGuard.release()
					}
				}()
			}
		}
	}

	manifestFile, manifest, err := openOrCreateManifestFile(opt)
	if err != nil {
		return nil, err
	}
	defer func() {
		if manifestFile != nil {
			_ = manifestFile.close()
		}
	}()

	db := &DB{
		imm:              make([]*memTable, 0, opt.NumMemtables),
		flushChan:        make(chan *memTable, opt.NumMemtables),
		writeCh:          make(chan *request, kvWriteChCapacity),
		opt:              opt,
		manifest:         manifestFile,
		dirLockGuard:     dirLockGuard,
		valueDirGuard:    valueDirLockGuard,
		orc:              newOracle(opt),
		pub:              newPublisher(),
		allocPool:        z.NewAllocatorPool(8),
		bannedNamespaces: &lockedKeys{keys: make(map[uint64]struct{})},
		threshold:        initVlogThreshold(&opt),
		valueLogOpen:     make(chan struct{}),
	}

	db.flushCond = sync.NewCond(&db.lock)
	db.syncChan = opt.syncChan

	// Cleanup all the goroutines started by badger in case of an error.
	defer func() {
		if err != nil {
			opt.Errorf("Received err: %v. Cleaning up...", err)
			db.cleanup()
			db = nil
		}
	}()

	if opt.BlockCacheSize > 0 {
		numInCache := opt.BlockCacheSize / int64(opt.BlockSize)
		if numInCache == 0 {
			// Make the value of this variable at least one since the cache requires
			// the number of counters to be greater than zero.
			numInCache = 1
		}

		config := ristretto.Config[[]byte, *table.Block]{
			NumCounters: numInCache * 8,
			MaxCost:     opt.BlockCacheSize,
			BufferItems: 64,
			Metrics:     true,
			OnExit:      table.BlockEvictHandler,
		}
		db.blockCache, err = ristretto.NewCache[[]byte, *table.Block](&config)
		if err != nil {
			return nil, y.Wrap(err, "failed to create data cache")
		}
	}

	if opt.IndexCacheSize > 0 {
		// Index size is around 5% of the table size.
		indexSz := int64(float64(opt.MemTableSize) * 0.05)
		numInCache := opt.IndexCacheSize / indexSz
		if numInCache == 0 {
			// Make the value of this variable at least one since the cache requires
			// the number of counters to be greater than zero.
			numInCache = 1
		}

		config := ristretto.Config[uint64, *fb.TableIndex]{
			NumCounters: numInCache * 8,
			MaxCost:     opt.IndexCacheSize,
			BufferItems: 64,
			Metrics:     true,
		}
		db.indexCache, err = ristretto.NewCache(&config)
		if err != nil {
			return nil, y.Wrap(err, "failed to create bf cache")
		}
	}

	db.closers.cacheHealth = z.NewCloser(1)
	go db.monitorCache(db.closers.cacheHealth)

	if db.opt.InMemory {
		db.opt.SyncWrites = false
		// If badger is running in memory mode, push everything into the LSM Tree.
		db.opt.ValueThreshold = math.MaxInt32
	}
	krOpt := KeyRegistryOptions{
		ReadOnly:                      opt.ReadOnly,
		Dir:                           opt.Dir,
		EncryptionKey:                 opt.EncryptionKey,
		EncryptionKeyRotationDuration: opt.EncryptionKeyRotationDuration,
		InMemory:                      opt.InMemory,
	}

	if db.registry, err = OpenKeyRegistry(krOpt); err != nil {
		return db, err
	}
	db.calculateSize()
	db.closers.updateSize = z.NewCloser(1)
	go db.updateSize(db.closers.updateSize)

	db.logFilesCreated.Store(1) // hoardDB: see DB.LogFilesCreated.
	if err := db.openMemTables(db.opt); err != nil {
		return nil, y.Wrapf(err, "while opening memtables")
	}

	if !db.opt.ReadOnly {
		if db.mt, err = db.newMemTable(); err != nil {
			return nil, y.Wrapf(err, "cannot create memtable")
		}
	}

	// newLevelsController potentially loads files in directory.
	if db.lc, err = newLevelsController(db, &manifest); err != nil {
		return db, err
	}

	// Initialize vlog struct.
	db.vlog.init(db)

	if !opt.ReadOnly {
		// hoardDB: before any compaction or flush, a replayed log whose table
		// the MANIFEST already names goes, durably (flushed_log.go). A step
		// added here; nothing in Open moves.
		// Assigned to Open's err, not shadowed, so the deferred cleanup runs
		// (review F7).
		if err = db.removeLogsOfNamedTables(); err != nil {
			return nil, err
		}
		// hoardDB (BUG-0827, slice 2): a step added; nothing in Open moves.
		if err = db.syncValueLogFilesAtOpen(); err != nil {
			return nil, err
		}
		openCompactionsHook(db.opt.Dir)
		db.closers.compactors = z.NewCloser(1)
		db.lc.startCompact(db.closers.compactors)

		db.closers.memtable = z.NewCloser(1)
		go func() {
			db.flushMemtable(db.closers.memtable) // Need levels controller to be up.
		}()
		// Flush them to disk asap.
		for _, mt := range db.imm {
			db.flushChan <- mt
		}
	}
	// We do increment nextTxnTs below. So, no need to do it here.
	db.orc.nextTxnTs = db.MaxVersion()
	// hoardDB: under a vouched cut the first commit takes a version above the
	// vouched one, which can be above every version the store holds (a replay
	// of deletes compacted below it), so no commit ever takes a version that
	// a sync already vouched (I-3).
	if db.opt.vouchedCut {
		db.orc.nextTxnTs = max(db.orc.nextTxnTs, db.opt.vouchedVersion)
		db.vouchedReport.Vouched = db.opt.vouchedVersion
		db.vouchedReport.Kept = db.orc.nextTxnTs
	}
	db.writtenVersion.Store(db.orc.nextTxnTs)
	db.opt.Infof("Set nextTxnTs to %d", db.orc.nextTxnTs)

	if err = db.vlog.open(db); err != nil {
		return db, y.Wrapf(err, "During db.vlog.open")
	}
	close(db.valueLogOpen)

	// Let's advance nextTxnTs to one more than whatever we observed via
	// replaying the logs.
	db.orc.txnMark.Done(db.orc.nextTxnTs)
	// In normal mode, we must update readMark so older versions of keys can be removed during
	// compaction when run in offline mode via the flatten tool.
	db.orc.readMark.Done(db.orc.nextTxnTs)
	db.orc.incrementNextTs()

	go db.threshold.listenForValueThresholdUpdate()

	if err := db.initBannedNamespaces(); err != nil {
		return db, fmt.Errorf("While setting banned keys: %w", err)
	}

	db.closers.writes = z.NewCloser(1)
	go db.doWrites(db.closers.writes)

	if !db.opt.InMemory && !db.opt.ReadOnly {
		db.closers.valueGC = z.NewCloser(1)
		go db.vlog.waitOnGC(db.closers.valueGC)
	}

	db.closers.pub = z.NewCloser(1)
	go db.pub.listenForUpdates(db.closers.pub)

	valueDirLockGuard = nil
	dirLockGuard = nil
	manifestFile = nil
	return db, nil
}

// initBannedNamespaces retrieves the banned namespaces from the DB and updates in-memory structure.
func (db *DB) initBannedNamespaces() error {
	if db.opt.NamespaceOffset < 0 {
		return nil
	}
	return db.View(func(txn *Txn) error {
		iopts := DefaultIteratorOptions
		iopts.Prefix = bannedNsKey
		iopts.PrefetchValues = false
		iopts.InternalAccess = true
		itr := txn.NewIterator(iopts)
		defer itr.Close()
		for itr.Rewind(); itr.Valid(); itr.Next() {
			key := y.BytesToU64(itr.Item().Key()[len(bannedNsKey):])
			db.bannedNamespaces.add(key)
		}
		return nil
	})
}

func (db *DB) MaxVersion() uint64 {
	var maxVersion uint64
	update := func(a uint64) {
		if a > maxVersion {
			maxVersion = a
		}
	}
	db.lock.Lock()
	// In read only mode, we do not create new mem table.
	if !db.opt.ReadOnly {
		update(db.mt.maxVersion)
	}
	for _, mt := range db.imm {
		update(mt.maxVersion)
	}
	db.lock.Unlock()
	for _, ti := range db.Tables() {
		update(ti.MaxVersion)
	}
	return maxVersion
}

func (db *DB) monitorCache(c *z.Closer) {
	defer c.Done()
	count := 0
	analyze := func(name string, metrics *ristretto.Metrics) {
		// If the mean life expectancy is less than 10 seconds, the cache
		// might be too small.
		le := metrics.LifeExpectancySeconds()
		if le == nil {
			return
		}
		lifeTooShort := le.Count > 0 && float64(le.Sum)/float64(le.Count) < 10
		hitRatioTooLow := metrics.Ratio() > 0 && metrics.Ratio() < 0.4
		if lifeTooShort && hitRatioTooLow {
			db.opt.Warningf("%s might be too small. Metrics: %s\n", name, metrics)
			db.opt.Warningf("Cache life expectancy (in seconds): %+v\n", le)

		} else if le.Count > 1000 && count%5 == 0 {
			db.opt.Infof("%s metrics: %s\n", name, metrics)
		}
	}

	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-c.HasBeenClosed():
			return
		case <-ticker.C:
		}

		analyze("Block cache", db.BlockCacheMetrics())
		analyze("Index cache", db.IndexCacheMetrics())
		count++
	}
}

// cleanup stops all the goroutines started by badger. This is used in open to
// cleanup goroutines in case of an error.
func (db *DB) cleanup() {
	db.stopMemoryFlush()
	db.stopCompactions()

	db.blockCache.Close()
	db.indexCache.Close()
	if db.closers.updateSize != nil {
		db.closers.updateSize.Signal()
	}
	if db.closers.valueGC != nil {
		db.closers.valueGC.Signal()
	}
	if db.closers.writes != nil {
		db.closers.writes.Signal()
	}
	if db.closers.pub != nil {
		db.closers.pub.Signal()
	}

	db.orc.Stop()

	// Do not use vlog.Close() here. vlog.Close truncates the files. We don't
	// want to truncate files unless the user has specified the truncate flag.
}

// BlockCacheMetrics returns the metrics for the underlying block cache.
func (db *DB) BlockCacheMetrics() *ristretto.Metrics {
	if db.blockCache != nil {
		return db.blockCache.Metrics
	}
	return nil
}

// IndexCacheMetrics returns the metrics for the underlying index cache.
func (db *DB) IndexCacheMetrics() *ristretto.Metrics {
	if db.indexCache != nil {
		return db.indexCache.Metrics
	}
	return nil
}

// Close closes a DB. It's crucial to call it to ensure all the pending updates make their way to
// disk. Calling DB.Close() multiple times would still only close the DB once.
func (db *DB) Close() error {
	var err error
	db.closeOnce.Do(func() {
		err = db.close()
	})
	return err
}

// IsClosed denotes if the badger DB is closed or not. A DB instance should not
// be used after closing it.
func (db *DB) IsClosed() bool {
	return db.isClosed.Load() == 1
}

func (db *DB) close() (err error) {
	defer db.allocPool.Release()
	// leftCurrent is the active memtable, left unpushed below because the
	// flush goroutine has stopped draining; db.unflushed is read only after
	// stopMemoryFlush has waited for that goroutine.
	var leftCurrent int
	var leftCause error
	// namedLeft (hoardDB, review F6) counts the unflushed memtables whose
	// tables the MANIFEST already names: their logs are removed and their
	// writes are in those tables, so they are not "replayed at the next open".
	var namedLeft int
	defer func() {
		if stalledErr := db.lc.stalledByUnreadableTable(); stalledErr != nil {
			err = errors.Join(stalledErr, err)
		}
		if unflushed := db.unflushed + leftCurrent; unflushed > 0 {
			if db.unflushedCause != nil {
				leftCause = db.unflushedCause
			}
			text := fmt.Sprintf("DB.Close: %d memtables not flushed, their logs stay in %s and are replayed at the "+
				"next open", max(unflushed-namedLeft, 0), db.opt.Dir)
			if namedLeft > 0 {
				text += fmt.Sprintf("; %d more had their tables named in the MANIFEST, which hold their writes and are "+
					"loaded at the next open, and their logs were removed", namedLeft)
			}
			err = errors.Join(fmt.Errorf("%s: %w", text, leftCause), err)
		}
	}()

	db.opt.Debugf("Closing database")
	db.opt.Infof("Lifetime L0 stalled for: %s\n", time.Duration(db.lc.l0stallsMs.Load()))

	// Set blockWrites/isClosed and wake any writer parked in ensureRoomForWrite's
	// flushCond.Wait ATOMICALLY under db.lock. ensureRoomForWrite checks IsClosed()
	// and calls flushCond.Wait while holding db.lock, so we must hold the same lock
	// here; otherwise the wakeup can be lost in this interleaving: the writer reads
	// isClosed==0, then we Store(1)+Broadcast (writer not yet enqueued in Wait, so
	// the Broadcast is lost), then the writer Waits forever and
	// closers.writes.SignalAndWait below hangs (stopMemoryFlush, which would unstall
	// the flush goroutine, is only reached later). Holding db.lock serializes the
	// two: either the writer sees isClosed==1 and returns errNoRoom without waiting,
	// or it is already in Wait (which released db.lock) and the Broadcast reaches it.
	// Either way the write goroutine can finish so SignalAndWait completes.
	db.lock.Lock()
	db.blockWrites.Store(1)
	db.isClosed.Store(1)
	if db.flushCond != nil {
		db.flushCond.Broadcast()
	}
	db.lock.Unlock()

	if db.closers.valueGC != nil {
		// Stop value GC first.
		db.closers.valueGC.SignalAndWait()
	}

	// Stop writes next.
	db.closers.writes.SignalAndWait()

	// Don't accept any more write.
	close(db.writeCh)

	db.closers.pub.SignalAndWait()
	db.closers.cacheHealth.Signal()

	// Make sure that block writer is done pushing stuff into memtable!
	// Otherwise, you will have a race condition: we are trying to flush memtables
	// and remove them completely, while the block / memtable writer is still
	// trying to push stuff into the memtable. This will also resolve the value
	// offset problem: as we push into memtable, we update value offsets there.
	if db.mt != nil {
		if db.mt.sl.Empty() {
			// Remove the memtable if empty.
			db.mt.DecrRef()
		} else {
			db.opt.Debugf("Flushing memtable")
			for {
				// The flush goroutine has stopped draining flushChan: it keeps
				// failing. Leave the memtable, whose log is on disk.
				if refusal := db.flushRefusal.Load(); refusal != nil {
					leftCurrent = 1
					leftCause = *refusal
					break
				}
				pushedMemTable := func() bool {
					db.lock.Lock()
					defer db.lock.Unlock()
					y.AssertTrue(db.mt != nil)
					select {
					case db.flushChan <- db.mt:
						db.imm = append(db.imm, db.mt) // Flusher will attempt to remove this from s.imm.
						db.mt = nil                    // Will segfault if we try writing!
						db.opt.Debugf("pushed to flush chan\n")
						return true
					default:
						// If we fail to push, we need to unlock and wait for a short while.
						// The flushing operation needs to update s.imm. Otherwise, we have a
						// deadlock.
						// TODO: Think about how to do this more cleanly, maybe without any locks.
					}
					return false
				}()
				if pushedMemTable {
					break
				}
				// The flush goroutine may be parked in addLevel0Table waiting for
				// a level-0 compaction that cannot succeed (BUG-0381). isClosed
				// is set, so waking it makes it add its table and take the next
				// memtable, which makes room here. Without this the push waits
				// for a compaction that never comes.
				db.lc.levels[0].signalL0Drained()
				time.Sleep(10 * time.Millisecond)
			}
		}
	}
	db.stopMemoryFlush()
	db.stopCompactions()
	// hoardDB: a table the MANIFEST names whose log's removal never became
	// durable is in no level; it is closed here and kept on disk.
	released, namedErr := db.releaseNamedTables()
	namedLeft = released
	if namedErr != nil {
		err = errors.Join(err, fmt.Errorf("DB.Close: %w", namedErr))
	}

	// Force Compact L0
	// We don't need to care about cstatus since no parallel compaction is running.
	if db.opt.CompactL0OnClose {
		err := db.lc.doCompact(173, compactionPriority{level: 0, score: 1.73})
		switch err {
		case errFillTables:
			// This error only means that there might be enough tables to do a compaction. So, we
			// should not report it to the end user to avoid confusing them.
		case nil:
			db.opt.Debugf("Force compaction on level 0 done")
		default:
			db.opt.Warningf("While forcing compaction on level 0: %v", err)
		}
	}

	// Now close the value log, once every Sync in progress has finished with
	// it and released its memtables (a Sync that starts now refuses).
	db.syncs.Lock()
	if vlogErr := db.vlog.Close(); vlogErr != nil {
		err = y.Wrap(vlogErr, "DB.Close")
	}
	db.syncs.Unlock()

	db.opt.Infof(db.LevelsToString())
	if lcErr := db.lc.close(); err == nil {
		err = y.Wrap(lcErr, "DB.Close")
	}
	db.opt.Debugf("Waiting for closer")
	db.closers.updateSize.SignalAndWait()
	db.orc.Stop()
	db.blockCache.Close()
	db.indexCache.Close()

	db.threshold.close()

	if db.opt.InMemory {
		return
	}

	if db.dirLockGuard != nil {
		if guardErr := db.dirLockGuard.release(); err == nil {
			err = y.Wrap(guardErr, "DB.Close")
		}
	}
	if db.valueDirGuard != nil {
		if guardErr := db.valueDirGuard.release(); err == nil {
			err = y.Wrap(guardErr, "DB.Close")
		}
	}
	if manifestErr := db.manifest.close(); err == nil {
		err = y.Wrap(manifestErr, "DB.Close")
	}
	if registryErr := db.registry.Close(); err == nil {
		err = y.Wrap(registryErr, "DB.Close")
	}

	// Fsync directories to ensure that lock file, and any other removed files whose directory
	// we haven't specifically fsynced, are guaranteed to have their directory entry removal
	// persisted to disk.
	if syncErr := db.syncDir(db.opt.Dir); err == nil {
		err = y.Wrap(syncErr, "DB.Close")
	}
	if syncErr := db.syncDir(db.opt.ValueDir); err == nil {
		err = y.Wrap(syncErr, "DB.Close")
	}

	return err
}

// VerifyChecksum verifies checksum for all tables on all levels.
// This method can be used to verify checksum, if opt.ChecksumVerificationMode is NoVerification.
func (db *DB) VerifyChecksum() error {
	return db.lc.verifyChecksum()
}

const (
	lockFile = "LOCK"
)

// syncActiveWALHook is set only by tests. DB.Sync calls it with the active
// memtable just before it syncs that memtable's WAL, so a test can hold a Sync
// inside that msync and see what else can run meanwhile. By then the WAL of
// every immutable memtable Sync captured is synced; a Sync that failed on one
// of those never calls it.
var syncActiveWALHook func(*memTable)

// syncCaptureHook is set only by tests. DB.Sync calls it between capturing the
// active memtable and capturing the immutable ones, with db.lock still held, so
// a test can try to rotate the memtable between the two captures.
var syncCaptureHook func()

// LogFilesCreated returns a count that moves each time a memtable log or a
// value-log file is created, and that Open starts at 1. A file is counted
// before any commit is written to it, and DB.SyncVouched's version is
// published after the commit is written, so a count read after that version
// counts every file holding a commit at or below it. hoardDB: the caller
// fsyncs the directory, making those files' entries durable, only when the
// count moved since its last directory fsync (store-vouched-cut slice S-3).
func (db *DB) LogFilesCreated() uint64 {
	return db.logFilesCreated.Load()
}

// Sync syncs database content to disk. This function provides
// more control to user to sync data whenever required.
func (db *DB) Sync() error {
	_, err := db.SyncVouched()
	return err
}

// SyncVouched syncs database content to disk as Sync does and returns the
// highest commit version it made durable: every commit at or below it, and
// every commit that had returned before the call, is on disk when it returns
// nil. On an error it returns 0. hoardDB: the caller records the version
// (hoardDB's sync marker) and passes it to WithVouchedCut at the next open.
//
// The version is read before the capture of the memtables, never after it: a
// commit that returns after the capture can land in a memtable rotated in
// after it, whose log this call never syncs, so a version read later could
// vouch a commit that is not durable. Reading it first is the whole contract
// (store-vouched-cut-spec.md §3.3, trap 1 of its build document).
func (db *DB) SyncVouched() (uint64, error) {
	vouched := db.writtenVersion.Load()
	if db.opt.InMemory || db.opt.ReadOnly {
		// InMemory and read-only modes do not use WAL/vlog files, so Sync is a no-op.
		return vouched, nil
	}

	/**
	Make an attempt to sync both the logs, the active memtable's WAL and the vLog (1847).
	Cases:
	- All_ok			:: If both the logs sync successfully.

	- Entry_Lost		:: If an entry with a value pointer was present in the active memtable's WAL,
						:: and the WAL was synced but there was an error in syncing the vLog.
						:: The entry will be considered lost and this case will need to be handled during recovery.

	- Entries_Lost		:: If there were errors in syncing both the logs, multiple entries would be lost.

	- Entries_Lost      :: If the active memtable's WAL is not synced but the vLog is synced, it will
						:: result in entries being lost because recovery of the active memtable is done from its WAL.
						:: Check `UpdateSkipList` in memtable.go.

	- Nothing_lost		:: If an entry with its value was present in the active memtable's WAL, and the WAL was synced,
						:: but there was an error in syncing the vLog.
						:: Nothing is lost for this very specific entry because the entry is completely present in the memtable's WAL.

	- Partially_lost    :: If entries were written partially in either of the logs,
						:: the logs will be truncated during recovery.
						:: As a result of truncation, some entries might be lost.
					    :: Assume that 4KB of data is to be synced and invoking `Sync` results only in syncing 3KB
	                    :: of data and then the machine shuts down or the disk failure happens,
						:: this will result in partial writes. [[This case needs verification]]
	*/
	// An entry committed before this call is in the active memtable, in an
	// immutable one, or in a level-0 table already synced (a memtable leaves
	// db.imm only after its flush). One read lock captures all the memtables
	// that can hold it.
	//
	// A memtable rotated out by ensureRoomForWrite is not synced there, and its
	// WAL is the only durable copy of its entries until handleMemTableFlush
	// writes it to a level-0 table. Without syncing it here, an entry committed
	// before this call is lost on power failure if another write rotated its
	// memtable in between.
	//
	// Every WAL, the active one included, is synced after the lock is released:
	// held, a write that rotates the memtable (ensureRoomForWrite takes
	// db.lock.Lock) waits for the msync, and every read queues behind that
	// pending Lock. Appends to the active WAL already run beside its msync (the
	// write path takes no db.lock). A reference on each memtable keeps a
	// concurrent flush, DropAll or Close from unmapping and deleting a WAL while
	// it is synced; a rotation of the active memtable meanwhile changes nothing,
	// since the msync covers its whole mapping.
	//
	// Invariant (BUG-0824): the WALs are synced in the order their memtables
	// were rotated, oldest first, the active one last, and the first failure
	// ends the syncing. Every commit in one memtable precedes every commit in
	// the next (one goroutine writes, and it rotates between two writes), so
	// what Sync has forced to disk is at every instant a prefix of the commits.
	// Synced active-first, a power cut between the two msyncs kept a later
	// commit and lost an earlier one, and hoardDB's replay, which skips by
	// position, never restored it. A WAL synced after an older one failed
	// would leave the same state, so this call syncs no later WAL then. That
	// holds for this call, not for the store: a concurrent Sync the kernel did
	// not report the failure to, and the kernel's own writeback, can still
	// write the later WALs.
	db.syncs.RLock()
	defer db.syncs.RUnlock()
	db.lock.RLock()
	// close sets isClosed under db.lock before it hands the active memtable
	// to the flusher and sets db.mt to nil, so checked here db.mt is live.
	if db.IsClosed() {
		db.lock.RUnlock()
		return 0, ErrDBClosed
	}
	// Invariant: both captures happen under this one read lock, so their
	// order does not matter. Captured under two locks with the immutables
	// first, a rotation in between can move the memtable that was active at
	// the call into db.imm after the immutables were copied and before db.mt
	// was read, and its log, holding entries committed before the call, is
	// synced by neither capture.
	active := db.mt
	active.IncrRef()
	if syncCaptureHook != nil {
		syncCaptureHook()
	}
	immutables := make([]*memTable, len(db.imm))
	copy(immutables, db.imm)
	for _, imm := range immutables {
		imm.IncrRef()
	}
	db.lock.RUnlock()
	if syncAfterUnlockHook != nil {
		syncAfterUnlockHook()
	}

	// db.imm is in rotation order: ensureRoomForWrite appends, the flusher
	// takes from the front, and openMemTables fills it in file-id order.
	var memtableSyncError error
	for _, imm := range immutables {
		if memtableSyncError == nil {
			if err := imm.SyncWAL(); err != nil {
				memtableSyncError = fmt.Errorf("while syncing immutable memtable WAL %s: %w", imm.wal.Fd.Name(), err)
			}
		}
		imm.DecrRef()
	}
	if memtableSyncError == nil {
		if syncActiveWALHook != nil {
			syncActiveWALHook(active)
		}
		memtableSyncError = active.SyncWAL()
	}
	active.DecrRef()

	if syncValueLogHook != nil {
		syncValueLogHook()
	}
	// The value log is synced whatever the WALs returned, as before: a value
	// without the WAL entry that points at it makes no commit visible.
	vLogSyncError := db.vlog.sync()
	if err := y.CombineErrors(memtableSyncError, vLogSyncError); err != nil {
		return 0, err
	}
	return vouched, nil
}

// getMemtables returns the current memtables and get references.
func (db *DB) getMemTables() ([]*memTable, func()) {
	db.lock.RLock()
	defer db.lock.RUnlock()

	var tables []*memTable

	// Mutable memtable does not exist in read-only mode.
	if !db.opt.ReadOnly {
		// Get mutable memtable.
		tables = append(tables, db.mt)
		db.mt.IncrRef()
	}

	// Get immutable memtables.
	last := len(db.imm) - 1
	for i := range db.imm {
		tables = append(tables, db.imm[last-i])
		db.imm[last-i].IncrRef()
	}
	return tables, func() {
		for _, tbl := range tables {
			tbl.DecrRef()
		}
	}
}

// get returns the value in memtable or disk for given key.
// Note that value will include meta byte.
//
// IMPORTANT: We should never write an entry with an older timestamp for the same key, We need to
// maintain this invariant to search for the latest value of a key, or else we need to search in all
// tables and find the max version among them.  To maintain this invariant, we also need to ensure
// that all versions of a key are always present in the same table from level 1, because compaction
// can push any table down.
//
// Update(23/09/2020) - We have dropped the move key implementation. Earlier we
// were inserting move keys to fix the invalid value pointers but we no longer
// do that. For every get("fooX") call where X is the version, we will search
// for "fooX" in all the levels of the LSM tree. This is expensive but it
// removes the overhead of handling move keys completely.
func (db *DB) get(key []byte) (y.ValueStruct, error) {
	if db.IsClosed() {
		return y.ValueStruct{}, ErrDBClosed
	}
	tables, decr := db.getMemTables() // Lock should be released.
	defer decr()

	var maxVs y.ValueStruct
	version := y.ParseTs(key)

	y.NumGetsAdd(db.opt.MetricsEnabled, 1)
	for i := 0; i < len(tables); i++ {
		vs := tables[i].sl.Get(key)
		y.NumMemtableGetsAdd(db.opt.MetricsEnabled, 1)
		if vs.Meta == 0 && vs.Value == nil {
			continue
		}
		// Found the required version of the key, return immediately.
		if vs.Version == version {
			y.NumGetsWithResultsAdd(db.opt.MetricsEnabled, 1)
			return vs, nil
		}
		if maxVs.Version < vs.Version {
			maxVs = vs
		}
	}
	return db.lc.get(key, maxVs, 0)
}

var requestPool = sync.Pool{
	New: func() interface{} {
		return new(request)
	},
}

func (db *DB) writeToLSM(b *request) error {
	// We should check the length of b.Prts and b.Entries only when badger is not
	// running in InMemory mode. In InMemory mode, we don't write anything to the
	// value log and that's why the length of b.Ptrs will always be zero.
	if !db.opt.InMemory && len(b.Ptrs) != len(b.Entries) {
		return fmt.Errorf("Ptrs and Entries don't match: %+v", b)
	}

	for i, entry := range b.Entries {
		var err error
		if entry.skipVlogAndSetThreshold(db.valueThreshold()) {
			// Will include deletion / tombstone case.
			err = db.mt.Put(entry.Key,
				y.ValueStruct{
					Value: entry.Value,
					// Ensure value pointer flag is removed. Otherwise, the value will fail
					// to be retrieved during iterator prefetch. `bitValuePointer` is only
					// known to be set in write to LSM when the entry is loaded from a backup
					// with lower ValueThreshold and its value was stored in the value log.
					Meta:      entry.meta &^ bitValuePointer,
					UserMeta:  entry.UserMeta,
					ExpiresAt: entry.ExpiresAt,
				})
		} else {
			// Write pointer to Memtable.
			err = db.mt.Put(entry.Key,
				y.ValueStruct{
					Value:     b.Ptrs[i].Encode(),
					Meta:      entry.meta | bitValuePointer,
					UserMeta:  entry.UserMeta,
					ExpiresAt: entry.ExpiresAt,
				})
		}
		if err != nil {
			return y.Wrapf(err, "while writing to memTable")
		}
	}
	if db.opt.SyncWrites {
		return db.mt.SyncWAL()
	}
	return nil
}

// writeRequests is called serially by only one goroutine.
func (db *DB) writeRequests(reqs []*request) error {
	if len(reqs) == 0 {
		return nil
	}

	done := func(err error) {
		for _, r := range reqs {
			r.Err = err
			r.Wg.Done()
		}
	}
	// failFrom reports the requests before reqs[failed] as written, since they
	// are in the memtable and its log, and fails the rest with err. Failing the
	// whole group told callers that writes a reopen returns were refused.
	failFrom := func(failed int, err error) {
		db.pub.sendUpdates(reqs[:failed])
		for i, r := range reqs {
			if i >= failed {
				r.Err = err
			}
			r.Wg.Done()
		}
	}
	db.opt.Debugf("writeRequests called. Writing to value log")
	err := db.vlog.write(reqs)
	if err != nil {
		done(err)
		return err
	}

	db.opt.Debugf("Writing to memtable")
	var count int
	for i, b := range reqs {
		if len(b.Entries) == 0 {
			continue
		}
		count += len(b.Entries)
		// ensureRoomForWrite now blocks internally on db.flushCond until flushChan
		// has room (or the DB is closing), so we no longer busy-poll with a sleep
		// here. It returns errNoRoom only when the DB is shutting down, in which
		// case we surface the error instead of writing to a memtable being torn
		// down, and an ErrWritesRefused when the memtables cannot be persisted.
		if err := db.ensureRoomForWrite(); err != nil {
			failFrom(i, err)
			return y.Wrap(err, "writeRequests")
		}
		if err := db.writeToLSM(b); err != nil {
			failFrom(i, err)
			return y.Wrap(err, "writeRequests")
		}
		// hoardDB: the request is in the memtable; its waiters are released
		// after this loop (done, failFrom), so a commit that returns is at or
		// below writtenVersion. A transaction's last entry is its end marker,
		// which carries its commit version; the requests without one (managed
		// mode, the value-log GC's rewrite, DB.Load, DB.BanNamespace) come only
		// from calls refused under a vouched cut.
		if version := y.ParseTs(b.Entries[len(b.Entries)-1].Key); version > db.writtenVersion.Load() {
			db.writtenVersion.Store(version)
		}
	}

	db.opt.Debugf("Sending updates to subscribers")
	db.pub.sendUpdates(reqs)

	done(nil)
	db.opt.Debugf("%d entries written", count)
	return nil
}

func (db *DB) sendToWriteCh(entries []*Entry) (*request, error) {
	if db.blockWrites.Load() == 1 {
		return nil, ErrBlockedWrites
	}
	if refusal := db.flushRefusal.Load(); refusal != nil {
		return nil, *refusal
	}
	var count, size int64
	for _, e := range entries {
		size += e.estimateSizeAndSetThreshold(db.valueThreshold())
		count++
	}
	y.NumBytesWrittenUserAdd(db.opt.MetricsEnabled, size)
	if count >= db.opt.maxBatchCount || size >= db.opt.maxBatchSize {
		return nil, ErrTxnTooBig
	}

	// We can only service one request because we need each txn to be stored in a contiguous section.
	// Txns should not interleave among other txns or rewrites.
	req := requestPool.Get().(*request)
	req.reset()
	req.Entries = entries
	req.Wg.Add(1)
	req.IncrRef()     // for db write
	db.writeCh <- req // Handled in doWrites.
	y.NumPutsAdd(db.opt.MetricsEnabled, int64(len(entries)))

	return req, nil
}

func (db *DB) doWrites(lc *z.Closer) {
	defer lc.Done()
	pendingCh := make(chan struct{}, 1)

	writeRequests := func(reqs []*request) {
		if err := db.writeRequests(reqs); err != nil {
			db.opt.Errorf("writeRequests: %v", err)
		}
		<-pendingCh
	}

	// This variable tracks the number of pending writes.
	reqLen := new(expvar.Int)
	y.PendingWritesSet(db.opt.MetricsEnabled, db.opt.Dir, reqLen)

	reqs := make([]*request, 0, 10)
	for {
		var r *request
		select {
		case r = <-db.writeCh:
		case <-lc.HasBeenClosed():
			goto closedCase
		}

		for {
			reqs = append(reqs, r)
			reqLen.Set(int64(len(reqs)))

			if len(reqs) >= 3*kvWriteChCapacity {
				pendingCh <- struct{}{} // blocking.
				goto writeCase
			}

			select {
			// Either push to pending, or continue to pick from writeCh.
			case r = <-db.writeCh:
			case pendingCh <- struct{}{}:
				goto writeCase
			case <-lc.HasBeenClosed():
				goto closedCase
			}
		}

	closedCase:
		// All the pending request are drained.
		// Don't close the writeCh, because it has be used in several places.
		for {
			select {
			case r = <-db.writeCh:
				reqs = append(reqs, r)
			default:
				pendingCh <- struct{}{} // Push to pending before doing a write.
				writeRequests(reqs)
				return
			}
		}

	writeCase:
		go writeRequests(reqs)
		reqs = make([]*request, 0, 10)
		reqLen.Set(0)
	}
}

// batchSet applies a list of badger.Entry. If a request level error occurs it
// will be returned.
//
//	Check(kv.BatchSet(entries))
func (db *DB) batchSet(entries []*Entry) error {
	req, err := db.sendToWriteCh(entries)
	if err != nil {
		return err
	}

	return req.Wait()
}

// batchSetAsync is the asynchronous version of batchSet. It accepts a callback
// function which is called when all the sets are complete. If a request level
// error occurs, it will be passed back via the callback.
//
//	err := kv.BatchSetAsync(entries, func(err error)) {
//	   Check(err)
//	}
func (db *DB) batchSetAsync(entries []*Entry, f func(error)) error {
	req, err := db.sendToWriteCh(entries)
	if err != nil {
		return err
	}
	go func() {
		err := req.Wait()
		// Write is complete. Let's call the callback function now.
		f(err)
	}()
	return nil
}

var errNoRoom = errors.New("No room for write")

// ErrWritesRefused is wrapped by the error a write gets while the DB cannot
// persist its memtables: a memtable flush has failed several times in a row,
// or a full memtable could not be replaced because its successor's log could
// not be created. The error names the cause. Reads are still served.
var ErrWritesRefused = errors.New("writes are refused: the memtables cannot be persisted")

// ErrL0StalledByDamagedTable is wrapped by the error Close returns when level 0
// was at its stall limit because every compaction that includes a table with an
// unreadable block fails and keeps its inputs (BUG-0381). The error names the
// table; Close still completes.
var ErrL0StalledByDamagedTable = errors.New("level 0 is stalled behind a table that cannot be read")

// ErrFlushNotNamed is in the chain of a flush whose MANIFEST change failed: the flush keeps its
// table and is not retried until the store is reopened (memTable.failedNaming), so writes stay
// refused until then. hoardDB's BUG-0868.
var ErrFlushNotNamed = errors.New("a flush could not record its table in the MANIFEST")

// flushFailuresBeforeRefusal is how many flush attempts in a row must fail
// before writes are refused. One failure may be transient; the flush is
// retried every flushRetryInterval regardless, and a success lifts the
// refusal.
const flushFailuresBeforeRefusal = 3

const flushRetryInterval = time.Second

// WriteRefusal returns the error writes are refused with, wrapping
// ErrWritesRefused, or nil when the DB accepts writes.
func (db *DB) WriteRefusal() error {
	if refusal := db.flushRefusal.Load(); refusal != nil {
		return *refusal
	}
	if refusal := db.rotateRefusal.Load(); refusal != nil {
		return *refusal
	}
	return db.lc.damagedStallRefusal()
}

// wakeBlockedWriters wakes a writer waiting in ensureRoomForWrite so it re-checks
// whether it should be refused. It takes db.lock because the waiter releases it
// only inside flushCond.Wait.
func (db *DB) wakeBlockedWriters() {
	db.lock.Lock()
	defer db.lock.Unlock()
	if db.flushCond != nil {
		db.flushCond.Broadcast()
	}
}

// ensureRoomForWrite is always called serially. It blocks (without busy-polling)
// until the current memtable can be flushed, i.e. until flushChan has room.
func (db *DB) ensureRoomForWrite() error {
	var err error
	db.lock.Lock()
	defer db.lock.Unlock()

	y.AssertTrue(db.mt != nil) // A nil mt indicates that DB is being closed.
	if !db.mt.isFull() {
		return nil
	}

	// The successor is created before the full memtable is handed to the
	// flusher. Created after, a failure (its log cannot be created: the
	// directory is gone, the disk is full) left db.mt nil, and the next write
	// failed the assertion above, which exits the process.
	next, err := db.newMemTable()
	if err != nil {
		refusal := fmt.Errorf("%w: creating a memtable log in %s: %w", ErrWritesRefused, db.opt.Dir, err)
		db.rotateRefusal.Store(&refusal)
		return refusal
	}
	db.rotateRefusal.Store(nil)

	for {
		select {
		case db.flushChan <- db.mt:
			db.opt.Debugf("Flushing memtable, mt.size=%d size of flushChan: %d\n",
				db.mt.sl.MemSize(), len(db.flushChan))
			// We manage to push this task. Let's modify imm.
			db.imm = append(db.imm, db.mt)
			// New memtable is empty. We certainly have room.
			db.mt = next
			return nil
		default:
			// flushChan is full. Rather than busy-sleep, wait to be signalled when
			// the flush goroutine drains a memtable from imm (which frees a slot in
			// flushChan). flushCond.Wait atomically releases db.lock (allowing the
			// flusher to update imm and push) and re-acquires it on wake; we then
			// re-attempt the non-blocking push in this loop.
			//
			// Bail out with errNoRoom ONLY when the DB is closing, so that Close's
			// closers.writes.SignalAndWait() (which waits for the write goroutine)
			// cannot hang here. The gate must be IsClosed() (set solely by close()),
			// NOT blockWrites: blockWrites is also raised transiently by blockWrite()
			// during DropPrefix/DropAll, and their prepareToDrop() drain flushes any
			// already-accepted writes through this path expressly "so that we don't
			// miss any entries". Bailing on blockWrites there would drop acknowledged
			// writes under flushChan pressure (compaction is still running during the
			// drain, so waiting for room is safe and cannot deadlock). This matches
			// the pre-cond behavior, which looped until the push succeeded.
			if db.IsClosed() {
				next.DecrRef()
				return errNoRoom
			}
			// The flusher is not draining: refuse rather than wait for it.
			if refusal := db.flushRefusal.Load(); refusal != nil {
				next.DecrRef()
				return *refusal
			}
			// The flusher is waiting for a level-0 compaction that cannot
			// succeed: refuse rather than wait for it (BUG-0381).
			if refusal := db.lc.damagedStallRefusal(); refusal != nil {
				next.DecrRef()
				return refusal
			}
			db.flushCond.Wait()
		}
	}
}

func arenaSize(opt Options) int64 {
	return opt.MemTableSize + opt.maxBatchSize + opt.maxBatchCount*int64(skl.MaxNodeSize)
}

// buildL0Table builds a new table from the memtable.
func buildL0Table(iter y.Iterator, dropPrefixes [][]byte, bopts table.Options) *table.Builder {
	defer iter.Close()

	b := table.NewTableBuilder(bopts)
	for iter.Rewind(); iter.Valid(); iter.Next() {
		if len(dropPrefixes) > 0 && hasAnyPrefixes(iter.Key(), dropPrefixes) {
			continue
		}
		vs := iter.Value()
		var vp valuePointer
		if vs.Meta&bitValuePointer > 0 {
			vp.Decode(vs.Value)
		}
		b.Add(iter.Key(), iter.Value(), vp.Len)
	}

	return b
}

// handleMemTableFlush must be run serially.
//
// hoardDB: on disk the flush names its table in the MANIFEST, removes the
// memtable's log durably, and only then inserts the table into level 0
// (flushed_log.go has why). A memtable whose table is already named (a
// retry after the removal failed) is not built again.
func (db *DB) handleMemTableFlush(mt *memTable, dropPrefixes [][]byte) error {
	if !db.opt.InMemory {
		return db.flushToNamedTable(mt)
	}
	bopts := buildTableOptions(db)
	itr := mt.sl.NewUniIterator(false)
	builder := buildL0Table(itr, nil, bopts)
	defer builder.Close()

	// buildL0Table can return nil if the none of the items in the skiplist are
	// added to the builder. This can happen when drop prefix is set and all
	// the items are skipped.
	if builder.Empty() {
		builder.Finish()
		return nil
	}

	fileID := db.lc.reserveFileID()
	tbl, err := table.OpenInMemoryTable(builder.Finish(), fileID, &bopts)
	if err != nil {
		return y.Wrap(err, "error while creating table")
	}
	// We own a ref on tbl.
	err = db.lc.addLevel0Table(tbl) // This will incrRef
	_ = tbl.DecrRef()               // Releases our ref.
	return err
}

// flushToNamedTable is handleMemTableFlush on disk. Like upstream's, it
// builds the table from every entry (buildL0Table is given no prefixes to
// drop; DropPrefix drops them from the levels afterwards). hoardDB.
func (db *DB) flushToNamedTable(mt *memTable) error {
	if mt.nameFailed != nil {
		return mt.nameFailed
	}
	tbl := mt.named
	if tbl == nil {
		// hoardDB (BUG-0827 route (c), slice 2 of the vouched cut): the value
		// log is synced before the table is built, so before the MANIFEST
		// names it. A named table vouches every pointer it holds, whatever
		// hoardDB's marker says (the store reopens through max(marker,
		// tables' highest version)), and without this a power cut could keep
		// the table and lose the value-log bytes its pointers name: the value
		// then reads back empty, with no error. The memtable is immutable, so
		// its pointers name bytes written before this call: in the newest
		// value-log file, which valueLog.sync msyncs whole, or in a rotated
		// one, which logFile.doneWriting synced before the newest file id
		// moved (change 3). Synced here, before the build, a failure leaves no
		// table file to clean up; it is kept (syncValueLogFile), so a retry
		// fails too and change 4 refuses writes.
		//
		// A memtable Open replayed can be flushed while Open is still opening
		// the value log (Open hands the replayed memtables to the flusher
		// first, and blocks when there are more than the flush queue holds,
		// so the flush must not wait for the value log). Its pointers name
		// bytes in files that already existed, which Open fsynced before the
		// flusher started (syncValueLogFilesAtOpen), and nothing writes the
		// value log before Open returns: such a flush has nothing to sync.
		select {
		case <-db.valueLogOpen:
			if err := db.syncValueLogForFlush(mt); err != nil {
				return err
			}
		default:
		}
		bopts := buildTableOptions(db)
		itr := mt.sl.NewUniIterator(false)
		builder := buildL0Table(itr, nil, bopts)
		defer builder.Close()
		if builder.Empty() {
			builder.Finish()
			return nil
		}
		var err error
		tbl, err = table.CreateTable(table.NewFilename(db.lc.reserveFileID(), db.opt.Dir), builder)
		if err != nil {
			return y.Wrap(err, "error while creating table")
		}
		db.flushStep(FlushTableSynced)
		if err := db.lc.nameLevel0Table(tbl); err != nil {
			// The MANIFEST may name the table now (BUG-0868): keep the file
			// and never retry the naming (memTable.failedNaming).
			mt.failedNaming = tbl
			mt.nameFailed = fmt.Errorf("%w: naming table %d in the MANIFEST failed, so whether the MANIFEST names it "+
				"is not known; the table file and the memtable's log are kept and the flush is not retried until the "+
				"store is reopened: %w", ErrFlushNotNamed, tbl.ID(), err)
			return mt.nameFailed
		}
		mt.named = tbl
		db.flushStep(FlushTableNamed)
	}
	if err := db.retireMemTableLog(mt); err != nil {
		return err
	}
	mt.named = nil
	db.flushStep(FlushDirectorySynced)
	db.lc.insertLevel0Table(tbl) // This will incrRef
	db.flushStep(FlushTableInLevelZero)
	_ = tbl.DecrRef() // Releases our ref.
	return nil
}

// flushMemtable must keep running until flushChan is closed. A flush that
// fails is retried every flushRetryInterval; after flushFailuresBeforeRefusal
// failures in a row writes are refused until one succeeds. Once Close has
// signalled lc, a memtable whose flush has failed that many times is not
// retried: it and every memtable queued after it (flushing those first would
// put newer versions below older ones) are left unflushed, and their logs are
// replayed at the next open.
func (db *DB) flushMemtable(lc *z.Closer) {
	defer lc.Done()

	for mt := range db.flushChan {
		if mt == nil {
			continue
		}
		if db.unflushed > 0 {
			db.unflushed++
			continue
		}
		if hook := flushTestHook.Load(); hook != nil {
			(*hook)(db)
		}

		for failures := 1; ; failures++ {
			if err := db.handleMemTableFlush(mt, nil); err != nil {
				// A flush that kept its table after a failed naming is not retried: say so once.
				switch {
				case !errors.Is(err, ErrFlushNotNamed):
					db.opt.Errorf("error flushing memtable to disk: %v, retrying", err)
				case failures == 1:
					db.opt.Errorf("error flushing memtable to disk: %v", err)
				}
				if failures >= flushFailuresBeforeRefusal && db.flushRefusal.Load() == nil {
					refusal := fmt.Errorf("%w: flushing a memtable to %s failed %d times in a row: %w",
						ErrWritesRefused, db.opt.Dir, failures, err)
					db.flushRefusal.Store(&refusal)
					// Wake a writer waiting in ensureRoomForWrite for room that
					// this goroutine will not make.
					db.lock.Lock()
					db.flushCond.Broadcast()
					db.lock.Unlock()
				}
				if failures >= flushFailuresBeforeRefusal && db.flushStopped() {
					db.unflushed++
					db.unflushedCause = err
					break
				}
				time.Sleep(flushRetryInterval)
				continue
			}
			db.flushRefusal.Store(nil)

			// Update s.imm. Need a lock.
			db.lock.Lock()
			// This is a single-threaded operation. mt corresponds to the head of
			// db.imm list. Once we flush it, we advance db.imm. The next mt
			// which would arrive here would match db.imm[0], because we acquire a
			// lock over DB when pushing to flushChan.
			// TODO: This logic is dirty AF. Any change and this could easily break.
			y.AssertTrue(mt == db.imm[0])
			db.imm = db.imm[1:]
			mt.DecrRef() // Return memory.
			// unlock
			db.lock.Unlock()
			// A memtable has been flushed and removed from imm; a slot in flushChan
			// is now free. Wake any writer blocked in ensureRoomForWrite so it can
			// retry its non-blocking push. Broadcasting after unlock is safe because
			// the waiter re-checks (re-attempts the push) under db.lock in a loop.
			db.flushCond.Broadcast()
			break
		}
	}
}

func exists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return true, err
}

// This function does a filewalk, calculates the size of vlog and sst files and stores it in
// y.LSMSize and y.VlogSize.
func (db *DB) calculateSize() {
	if db.opt.InMemory {
		return
	}
	newInt := func(val int64) *expvar.Int {
		v := new(expvar.Int)
		v.Add(val)
		return v
	}

	totalSize := func(dir string) (int64, int64) {
		var lsmSize, vlogSize int64
		err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			ext := filepath.Ext(path)
			switch ext {
			case ".sst":
				lsmSize += info.Size()
			case ".vlog":
				vlogSize += info.Size()
			}
			return nil
		})
		if err != nil {
			db.opt.Debugf("Got error while calculating total size of directory: %s", dir)
		}
		return lsmSize, vlogSize
	}

	lsmSize, vlogSize := totalSize(db.opt.Dir)
	y.LSMSizeSet(db.opt.MetricsEnabled, db.opt.Dir, newInt(lsmSize))
	// If valueDir is different from dir, we'd have to do another walk.
	if db.opt.ValueDir != db.opt.Dir {
		_, vlogSize = totalSize(db.opt.ValueDir)
	}
	y.VlogSizeSet(db.opt.MetricsEnabled, db.opt.ValueDir, newInt(vlogSize))
}

func (db *DB) updateSize(lc *z.Closer) {
	defer lc.Done()
	if db.opt.InMemory {
		return
	}

	metricsTicker := time.NewTicker(time.Minute)
	defer metricsTicker.Stop()

	for {
		select {
		case <-metricsTicker.C:
			db.calculateSize()
		case <-lc.HasBeenClosed():
			return
		}
	}
}

// RunValueLogGC triggers a value log garbage collection.
//
// It picks value log files to perform GC based on statistics that are collected
// during compactions.  If no such statistics are available, then log files are
// picked in random order. The process stops as soon as the first log file is
// encountered which does not result in garbage collection.
//
// When a log file is picked, it is first sampled. If the sample shows that we
// can discard at least discardRatio space of that file, it would be rewritten.
//
// If a call to RunValueLogGC results in no rewrites, then an ErrNoRewrite is
// thrown indicating that the call resulted in no file rewrites.
//
// We recommend setting discardRatio to 0.5, thus indicating that a file be
// rewritten if half the space can be discarded.  This results in a lifetime
// value log write amplification of 2 (1 from original write + 0.5 rewrite +
// 0.25 + 0.125 + ... = 2). Setting it to higher value would result in fewer
// space reclaims, while setting it to a lower value would result in more space
// reclaims at the cost of increased activity on the LSM tree. discardRatio
// must be in the range (0.0, 1.0), both endpoints excluded, otherwise an
// ErrInvalidRequest is returned.
//
// Only one GC is allowed at a time. If another value log GC is running, or DB
// has been closed, this would return an ErrRejected.
//
// Note: Every time GC is run, it would produce a spike of activity on the LSM
// tree.
func (db *DB) RunValueLogGC(discardRatio float64) error {
	if err := db.refuseUnderVouchedCut("DB.RunValueLogGC"); err != nil {
		return err
	}
	if db.opt.InMemory {
		return ErrGCInMemoryMode
	}
	if db.opt.ReadOnly {
		return ErrGCInReadOnlyMode
	}
	if discardRatio >= 1.0 || discardRatio <= 0.0 {
		return ErrInvalidRequest
	}

	// Pick a log file and run GC
	return db.vlog.runGC(discardRatio)
}

// Size returns the size of lsm and value log files in bytes. It can be used to decide how often to
// call RunValueLogGC.
func (db *DB) Size() (lsm, vlog int64) {
	if y.LSMSizeGet(db.opt.MetricsEnabled, db.opt.Dir) == nil {
		lsm, vlog = 0, 0
		return
	}
	lsm = y.LSMSizeGet(db.opt.MetricsEnabled, db.opt.Dir).(*expvar.Int).Value()
	vlog = y.VlogSizeGet(db.opt.MetricsEnabled, db.opt.ValueDir).(*expvar.Int).Value()
	return
}

// Sequence represents a Badger sequence.
type Sequence struct {
	lock      sync.Mutex
	db        *DB
	key       []byte
	next      uint64
	leased    uint64
	bandwidth uint64
}

// Next would return the next integer in the sequence, updating the lease by running a transaction
// if needed.
func (seq *Sequence) Next() (uint64, error) {
	seq.lock.Lock()
	defer seq.lock.Unlock()
	if seq.next >= seq.leased {
		if err := seq.updateLease(); err != nil {
			return 0, err
		}
	}
	val := seq.next
	seq.next++
	return val, nil
}

// Release the leased sequence to avoid wasted integers. This should be done right
// before closing the associated DB. However it is valid to use the sequence after
// it was released, causing a new lease with full bandwidth.
func (seq *Sequence) Release() error {
	seq.lock.Lock()
	defer seq.lock.Unlock()
	err := seq.db.Update(func(txn *Txn) error {
		item, err := txn.Get(seq.key)
		if err != nil {
			return err
		}

		var num uint64
		if err := item.Value(func(v []byte) error {
			num = binary.BigEndian.Uint64(v)
			return nil
		}); err != nil {
			return err
		}

		if num == seq.leased {
			var buf [8]byte
			binary.BigEndian.PutUint64(buf[:], seq.next)
			return txn.SetEntry(NewEntry(seq.key, buf[:]))
		}

		return nil
	})
	if err != nil {
		return err
	}
	seq.leased = seq.next
	return nil
}

func (seq *Sequence) updateLease() error {
	return seq.db.Update(func(txn *Txn) error {
		item, err := txn.Get(seq.key)
		switch {
		case err == ErrKeyNotFound:
			seq.next = 0
		case err != nil:
			return err
		default:
			var num uint64
			if err := item.Value(func(v []byte) error {
				num = binary.BigEndian.Uint64(v)
				return nil
			}); err != nil {
				return err
			}
			seq.next = num
		}

		lease := seq.next + seq.bandwidth
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], lease)
		if err = txn.SetEntry(NewEntry(seq.key, buf[:])); err != nil {
			return err
		}
		seq.leased = lease
		return nil
	})
}

// GetSequence would initiate a new sequence object, generating it from the stored lease, if
// available, in the database. Sequence can be used to get a list of monotonically increasing
// integers. Multiple sequences can be created by providing different keys. Bandwidth sets the
// size of the lease, determining how many Next() requests can be served from memory.
//
// GetSequence is not supported on ManagedDB. Calling this would result in a panic.
func (db *DB) GetSequence(key []byte, bandwidth uint64) (*Sequence, error) {
	if db.opt.managedTxns {
		panic("Cannot use GetSequence with managedDB=true.")
	}
	if db.opt.ReadOnly {
		panic("Cannot use GetSequence in read-only mode.")
	}

	switch {
	case len(key) == 0:
		return nil, ErrEmptyKey
	case bandwidth == 0:
		return nil, ErrZeroBandwidth
	}
	seq := &Sequence{
		db:        db,
		key:       key,
		next:      0,
		leased:    0,
		bandwidth: bandwidth,
	}
	err := seq.updateLease()
	return seq, err
}

// Tables gets the TableInfo objects from the level controller. If withKeysCount
// is true, TableInfo objects also contain counts of keys for the tables.
func (db *DB) Tables() []TableInfo {
	return db.lc.getTableInfo()
}

// Levels gets the LevelInfo.
func (db *DB) Levels() []LevelInfo {
	return db.lc.getLevelInfo()
}

// EstimateSize can be used to get rough estimate of data size for a given prefix.
func (db *DB) EstimateSize(prefix []byte) (uint64, uint64) {
	var onDiskSize, uncompressedSize uint64
	tables := db.Tables()
	for _, ti := range tables {
		if bytes.HasPrefix(ti.Left, prefix) && bytes.HasPrefix(ti.Right, prefix) {
			onDiskSize += uint64(ti.OnDiskSize)
			uncompressedSize += uint64(ti.UncompressedSize)
		}
	}
	return onDiskSize, uncompressedSize
}

// Ranges can be used to get rough key ranges to divide up iteration over the DB. The ranges here
// would consider the prefix, but would not necessarily start or end with the prefix. In fact, the
// first range would have nil as left key, and the last range would have nil as the right key.
func (db *DB) Ranges(prefix []byte, numRanges int) []*keyRange {
	var splits []string
	tables := db.Tables()

	// We just want table ranges here and not keys count.
	for _, ti := range tables {
		// We don't use ti.Left, because that has a tendency to store !badger keys. Skip over tables
		// at upper levels. Only choose tables from the last level.
		if ti.Level != db.opt.MaxLevels-1 {
			continue
		}
		if bytes.HasPrefix(ti.Right, prefix) {
			splits = append(splits, string(ti.Right))
		}
	}

	// If the number of splits is low, look at the offsets inside the
	// tables to generate more splits.
	if len(splits) < 32 {
		numTables := len(tables)
		if numTables == 0 {
			numTables = 1
		}
		numPerTable := 32 / numTables
		if numPerTable == 0 {
			numPerTable = 1
		}
		splits = db.lc.keySplits(numPerTable, prefix)
	}

	// If the number of splits is still < 32, then look at the memtables.
	if len(splits) < 32 {
		maxPerSplit := 10000
		mtSplits := func(mt *memTable) {
			if mt == nil {
				return
			}
			count := 0
			iter := mt.sl.NewIterator()
			for iter.SeekToFirst(); iter.Valid(); iter.Next() {
				if count%maxPerSplit == 0 {
					// Add a split every maxPerSplit keys.
					if bytes.HasPrefix(iter.Key(), prefix) {
						splits = append(splits, string(iter.Key()))
					}
				}
				count += 1
			}
			_ = iter.Close()
		}

		db.lock.Lock()
		defer db.lock.Unlock()
		var memTables []*memTable
		memTables = append(memTables, db.imm...)
		for _, mt := range memTables {
			mtSplits(mt)
		}
		mtSplits(db.mt)
	}

	// We have our splits now. Let's convert them to ranges.
	sort.Strings(splits)
	var ranges []*keyRange
	var start []byte
	for _, key := range splits {
		ranges = append(ranges, &keyRange{left: start, right: y.SafeCopy(nil, []byte(key))})
		start = y.SafeCopy(nil, []byte(key))
	}
	ranges = append(ranges, &keyRange{left: start})

	// Figure out the approximate table size this range has to deal with.
	for _, t := range tables {
		tr := keyRange{left: t.Left, right: t.Right}
		for _, r := range ranges {
			if len(r.left) == 0 || len(r.right) == 0 {
				continue
			}
			if r.overlapsWith(tr) {
				r.size += int64(t.UncompressedSize)
			}
		}
	}

	var total int64
	for _, r := range ranges {
		total += r.size
	}
	if total == 0 {
		return ranges
	}
	// Figure out the average size, so we know how to bin the ranges together.
	avg := total / int64(numRanges)

	var out []*keyRange
	var i int
	for i < len(ranges) {
		r := ranges[i]
		cur := &keyRange{left: r.left, size: r.size, right: r.right}
		i++
		for ; i < len(ranges); i++ {
			next := ranges[i]
			if cur.size+next.size > avg {
				break
			}
			cur.right = next.right
			cur.size += next.size
		}
		out = append(out, cur)
	}
	return out
}

// MaxBatchCount returns max possible entries in batch
func (db *DB) MaxBatchCount() int64 {
	return db.opt.maxBatchCount
}

// MaxBatchSize returns max possible batch size
func (db *DB) MaxBatchSize() int64 {
	return db.opt.maxBatchSize
}

// flushStopped reports whether the memtable flush goroutine has been signalled to
// stop. It is used by addLevel0Table to break out of the L0 stall wait during
// shutdown, including the Open-error cleanup path where db.isClosed may be unset.
func (db *DB) flushStopped() bool {
	if db.closers.memtable == nil {
		return false
	}
	select {
	case <-db.closers.memtable.HasBeenClosed():
		return true
	default:
		return false
	}
}

func (db *DB) stopMemoryFlush() {
	// Stop memtable flushes.
	if db.closers.memtable != nil {
		// Signal the flush goroutine's closer first so flushStopped() observes the
		// stop, then wake any flush goroutine stalled in addLevel0Table's cond.Wait
		// (which is not watching the closer channel) so it force-adds its table and
		// returns. Closing flushChan makes the goroutine's `range` loop terminate
		// once it is no longer blocked in addLevel0Table.
		db.closers.memtable.Signal()
		db.lc.levels[0].signalL0Drained()
		close(db.flushChan)
		db.closers.memtable.SignalAndWait()
	}
}

func (db *DB) stopCompactions() {
	// Stop compactions.
	if db.closers.compactors != nil {
		db.closers.compactors.SignalAndWait()
	}
}

func (db *DB) startCompactions() {
	// Resume compactions.
	if db.closers.compactors != nil {
		db.closers.compactors = z.NewCloser(1)
		db.lc.startCompact(db.closers.compactors)
	}
}

func (db *DB) startMemoryFlush() {
	// Start memory fluhser.
	if db.closers.memtable != nil {
		// A stop leaves in imm only the memtables whose flush kept failing.
		// They go first, in order, so imm[0] stays the next one flushed. There
		// can be one more of them than NumMemtables (the one being flushed had
		// left the channel), and no goroutine drains the channel yet, so it is
		// made to hold them all: a push that waited here would wait forever,
		// holding db.lock.
		db.lock.Lock()
		db.flushChan = make(chan *memTable, max(db.opt.NumMemtables, len(db.imm)))
		for _, mt := range db.imm {
			db.flushChan <- mt
		}
		db.lock.Unlock()
		db.unflushed, db.unflushedCause = 0, nil
		db.closers.memtable = z.NewCloser(1)
		go func() {
			db.flushMemtable(db.closers.memtable)
		}()
	}
}

// Flatten can be used to force compactions on the LSM tree so all the tables fall on the same
// level. This ensures that all the versions of keys are colocated and not split across multiple
// levels, which is necessary after a restore from backup. During Flatten, live compactions are
// stopped. Ideally, no writes are going on during Flatten. Otherwise, it would create competition
// between flattening the tree and new tables being created at level zero.
func (db *DB) Flatten(workers int) error {
	if db.opt.ReadOnly {
		panic("Cannot flatten in read-only mode.")
	}

	db.stopCompactions()
	defer db.startCompactions()

	compactAway := func(cp compactionPriority) error {
		db.opt.Infof("Attempting to compact with %+v\n", cp)
		errCh := make(chan error, 1)
		for i := 0; i < workers; i++ {
			go func() {
				errCh <- db.lc.doCompact(175, cp)
			}()
		}
		var success int
		var rerr error
		for i := 0; i < workers; i++ {
			err := <-errCh
			if err != nil {
				rerr = err
				db.opt.Warningf("While running doCompact with %+v. Error: %v\n", cp, err)
			} else {
				success++
			}
		}
		if success == 0 {
			return rerr
		}
		// We could do at least one successful compaction. So, we'll consider this a success.
		db.opt.Infof("%d compactor(s) succeeded. One or more tables from level %d compacted.\n",
			success, cp.level)
		return nil
	}

	hbytes := func(sz int64) string {
		return humanize.IBytes(uint64(sz))
	}

	t := db.lc.levelTargets()
	for {
		db.opt.Infof("\n")
		var levels []int
		for i, l := range db.lc.levels {
			sz := l.getTotalSize()
			db.opt.Infof("Level: %d. %8s Size. %8s Max.\n",
				i, hbytes(l.getTotalSize()), hbytes(t.targetSz[i]))
			if sz > 0 {
				levels = append(levels, i)
			}
		}
		if len(levels) <= 1 {
			prios := db.lc.pickCompactLevels(nil)
			if len(prios) == 0 || prios[0].score <= 1.0 {
				db.opt.Infof("All tables consolidated into one level. Flattening done.\n")
				return nil
			}
			if err := compactAway(prios[0]); err != nil {
				return err
			}
			continue
		}
		// Create an artificial compaction priority, to ensure that we compact the level.
		cp := compactionPriority{level: levels[0], score: 1.71}
		if err := compactAway(cp); err != nil {
			return err
		}
	}
}

func (db *DB) blockWrite() error {
	// Stop accepting new writes.
	if !db.blockWrites.CompareAndSwap(0, 1) {
		return ErrBlockedWrites
	}

	// Make all pending writes finish. The following will also close writeCh.
	db.closers.writes.SignalAndWait()
	db.opt.Infof("Writes flushed. Stopping compactions now...")
	return nil
}

func (db *DB) unblockWrite() {
	db.closers.writes = z.NewCloser(1)
	go db.doWrites(db.closers.writes)

	// Resume writes.
	db.blockWrites.Store(0)
}

func (db *DB) prepareToDrop() (func(), error) {
	if db.opt.ReadOnly {
		panic("Attempting to drop data in read-only mode.")
	}
	// In order prepare for drop, we need to block the incoming writes and
	// write it to db. Then, flush all the pending memtable. So that, we
	// don't miss any entries.
	if err := db.blockWrite(); err != nil {
		return func() {}, err
	}
	reqs := make([]*request, 0, 10)
	for {
		select {
		case r := <-db.writeCh:
			reqs = append(reqs, r)
		default:
			if err := db.writeRequests(reqs); err != nil {
				db.opt.Errorf("writeRequests: %v", err)
			}
			db.stopMemoryFlush()
			resume := func() {
				db.opt.Infof("Resuming writes")
				db.startMemoryFlush()
				db.unblockWrite()
			}
			// Dropping with memtables unflushed would leave their keys behind.
			if db.unflushed > 0 {
				notFlushed := fmt.Errorf("%w: %d memtables could not be flushed before the drop: %w",
					ErrWritesRefused, db.unflushed, db.unflushedCause)
				resume()
				return func() {}, notFlushed
			}
			return resume, nil
		}
	}
}

// DropAll would drop all the data stored in Badger. It does this in the following way.
// - Stop accepting new writes.
// - Pause memtable flushes and compactions.
// - Pick all tables from all levels, create a changeset to delete all these
// tables and apply it to manifest.
// - Pick all log files from value log, and delete all of them. Restart value log files from zero.
// - Resume memtable flushes and compactions.
//
// NOTE: DropAll is resilient to concurrent writes, but not to reads. It is up to the user to not do
// any reads while DropAll is going on, otherwise they may result in panics. Ideally, both reads and
// writes are paused before running DropAll, and resumed after it is finished.
func (db *DB) DropAll() error {
	if err := db.refuseUnderVouchedCut("DB.DropAll"); err != nil {
		return err
	}
	f, err := db.dropAll()
	if f != nil {
		f()
	}
	return err
}

func (db *DB) dropAll() (func(), error) {
	db.opt.Infof("DropAll called. Blocking writes...")
	f, err := db.prepareToDrop()
	if err != nil {
		return f, err
	}
	// prepareToDrop will stop all the incoming write and flushes any pending memtables.
	// Before we drop, we'll stop the compaction because anyways all the data are going to
	// be deleted.
	db.stopCompactions()
	resume := func() {
		db.startCompactions()
		f()
	}
	// Block all foreign interactions with memory tables.
	db.lock.Lock()
	defer db.lock.Unlock()

	// Remove inmemory tables. Calling DecrRef for safety. Not sure if they're absolutely needed.
	db.mt.DecrRef()
	for _, mt := range db.imm {
		mt.DecrRef()
	}
	db.imm = db.imm[:0]
	db.mt, err = db.newMemTable() // Set it up for future writes.
	if err != nil {
		return resume, y.Wrapf(err, "cannot open new memtable")
	}

	num, err := db.lc.dropTree()
	if err != nil {
		return resume, err
	}
	db.opt.Infof("Deleted %d SSTables. Now deleting value logs...\n", num)

	num, err = db.vlog.dropAll()
	if err != nil {
		return resume, err
	}
	db.lc.nextFileID.Store(1)
	db.opt.Infof("Deleted %d value log files. DropAll done.\n", num)
	db.blockCache.Clear()
	db.indexCache.Clear()
	db.threshold.Clear(db.opt)
	return resume, nil
}

// DropPrefix would drop all the keys with the provided prefix. It does this in the following way:
//   - Stop accepting new writes.
//   - Stop memtable flushes before acquiring lock. Because we're acquiring lock here
//     and memtable flush stalls for lock, which leads to deadlock
//   - Flush out all memtables, skipping over keys with the given prefix, Kp.
//   - Write out the value log header to memtables when flushing, so we don't accidentally bring Kp
//     back after a restart.
//   - Stop compaction.
//   - Compact L0->L1, skipping over Kp.
//   - Compact rest of the levels, Li->Li, picking tables which have Kp.
//   - Resume memtable flushes, compactions and writes.
func (db *DB) DropPrefix(prefixes ...[]byte) error {
	if err := db.refuseUnderVouchedCut("DB.DropPrefix"); err != nil {
		return err
	}
	if len(prefixes) == 0 {
		return nil
	}
	db.opt.Infof("DropPrefix called for %s", prefixes)
	f, err := db.prepareToDrop()
	if err != nil {
		return err
	}
	defer f()

	var filtered [][]byte
	if filtered, err = db.filterPrefixesToDrop(prefixes); err != nil {
		return err
	}
	// If there is no prefix for which the data already exist, do not do anything.
	if len(filtered) == 0 {
		db.opt.Infof("No prefixes to drop")
		return nil
	}
	// Block all foreign interactions with memory tables.
	db.lock.Lock()
	defer db.lock.Unlock()

	// hoardDB (BUG-0858's fix, review F1): the active memtable is rotated out
	// before it is flushed, so a memtable whose flush has named its table or
	// removed its log is never the active one. Flushed in place, a failure in
	// the log's removal left it active: writes acknowledged after the failed
	// drop went into a memtable whose table was already built (and, after a
	// failed directory fsync, whose log was already gone) and were lost. Now
	// the failed memtable stays in db.imm for the restarted flusher and new
	// writes go to a memtable with a log of its own.
	next, err := db.newMemTable()
	if err != nil {
		return fmt.Errorf("cannot create new mem table: %w", err)
	}
	db.imm = append(db.imm, db.mt)
	db.mt = next
	for _, memtable := range db.imm {
		if memtable.sl.Empty() {
			memtable.DecrRef()
			continue
		}
		db.opt.Debugf("Flushing memtable")
		if err := db.handleMemTableFlush(memtable, filtered); err != nil {
			db.opt.Errorf("While trying to flush memtable: %v", err)
			return err
		}
		memtable.DecrRef()
	}
	db.stopCompactions()
	defer db.startCompactions()
	db.imm = db.imm[:0]

	// Drop prefixes from the levels.
	if err := db.lc.dropPrefixes(filtered); err != nil {
		return err
	}
	db.opt.Infof("DropPrefix done")
	return nil
}

func (db *DB) filterPrefixesToDrop(prefixes [][]byte) ([][]byte, error) {
	var filtered [][]byte
	for _, prefix := range prefixes {
		err := db.View(func(txn *Txn) error {
			iopts := DefaultIteratorOptions
			iopts.Prefix = prefix
			iopts.PrefetchValues = false
			itr := txn.NewIterator(iopts)
			defer itr.Close()
			itr.Rewind()
			if itr.ValidForPrefix(prefix) {
				filtered = append(filtered, prefix)
			}
			return nil
		})
		if err != nil {
			return filtered, err
		}
	}
	return filtered, nil
}

// Checks if the key is banned. Returns the respective error if the key belongs to any of the banned
// namepspaces. Else it returns nil.
func (db *DB) isBanned(key []byte) error {
	if db.opt.NamespaceOffset < 0 {
		return nil
	}
	if len(key) <= db.opt.NamespaceOffset+8 {
		return nil
	}
	if db.bannedNamespaces.has(y.BytesToU64(key[db.opt.NamespaceOffset:])) {
		return ErrBannedKey
	}
	return nil
}

// BanNamespace bans a namespace. Read/write to keys belonging to any of such namespace is denied.
func (db *DB) BanNamespace(ns uint64) error {
	if err := db.refuseUnderVouchedCut("DB.BanNamespace"); err != nil {
		return err
	}
	if db.opt.NamespaceOffset < 0 {
		return ErrNamespaceMode
	}
	if db.opt.ReadOnly {
		panic("Cannot ban namespace in read-only mode.")
	}
	db.opt.Infof("Banning namespace: %d", ns)
	// First set the banned namespaces in DB and then update the in-memory structure.
	key := y.KeyWithTs(append(bannedNsKey, y.U64ToBytes(ns)...), 1)
	entry := []*Entry{{
		Key:   key,
		Value: nil,
	}}
	req, err := db.sendToWriteCh(entry)
	if err != nil {
		return err
	}
	if err := req.Wait(); err != nil {
		return err
	}
	db.bannedNamespaces.add(ns)
	return nil
}

// BannedNamespaces returns the list of prefixes banned for DB.
func (db *DB) BannedNamespaces() []uint64 {
	return db.bannedNamespaces.all()
}

// KVList contains a list of key-value pairs.
type KVList = pb.KVList

// Subscribe can be used to watch key changes for the given key prefixes and the ignore string.
// At least one prefix should be passed, or an error will be returned.
// You can use an empty prefix to monitor all changes to the DB.
// Ignore string is the byte ranges for which prefix matching will be ignored.
// For example: ignore = "2-3", and prefix = "abc" will match for keys "abxxc", "abdfc" etc.
// This function blocks until the given context is done or an error occurs.
// The given function will be called with a new KVList containing the modified keys and the
// corresponding values.
func (db *DB) Subscribe(ctx context.Context, cb func(kv *KVList) error, matches []pb.Match) error {
	if cb == nil {
		return ErrNilCallback
	}

	c := z.NewCloser(1)
	s, err := db.pub.newSubscriber(c, matches)
	if err != nil {
		return y.Wrapf(err, "while creating a new subscriber")
	}
	slurp := func(batch *pb.KVList) error {
		for {
			select {
			case kvs := <-s.sendCh:
				batch.Kv = append(batch.Kv, kvs.Kv...)
			default:
				if len(batch.GetKv()) > 0 {
					return cb(batch)
				}
				return nil
			}
		}
	}

	drain := func() {
		for {
			select {
			case _, ok := <-s.sendCh:
				if !ok {
					// Channel is closed.
					return
				}
			default:
				return
			}
		}
	}
	for {
		select {
		case <-c.HasBeenClosed():
			// No need to delete here. Closer will be called only while
			// closing DB. Subscriber will be deleted by cleanSubscribers.
			err := slurp(new(pb.KVList))
			// Drain if any pending updates.
			c.Done()
			return err
		case <-ctx.Done():
			c.Done()
			s.active.Store(0)
			drain()
			db.pub.deleteSubscriber(s.id)
			// Delete the subscriber to avoid further updates.
			return ctx.Err()
		case batch := <-s.sendCh:
			err := slurp(batch)
			if err != nil {
				c.Done()
				s.active.Store(0)
				drain()
				// Delete the subscriber if there is an error by the callback.
				db.pub.deleteSubscriber(s.id)
				return err
			}
		}
	}
}

func (db *DB) syncDir(dir string) error {
	if db.opt.InMemory {
		return nil
	}
	return syncDir(dir)
}

func createDirs(opt Options) error {
	for _, path := range []string{opt.Dir, opt.ValueDir} {
		dirExists, err := exists(path)
		if err != nil {
			return y.Wrapf(err, "Invalid Dir: %q", path)
		}
		if !dirExists {
			if opt.ReadOnly {
				return fmt.Errorf("Cannot find directory %q for read-only open", path)
			}
			// Try to create the directory
			err = os.MkdirAll(path, 0700)
			if err != nil {
				return y.Wrapf(err, "Error Creating Dir: %q", path)
			}
		}
	}
	return nil
}

// Stream the contents of this DB to a new DB with options outOptions that will be
// created in outDir.
func (db *DB) StreamDB(outOptions Options) error {
	outDir := outOptions.Dir

	// Open output DB.
	outDB, err := OpenManaged(outOptions)
	if err != nil {
		return y.Wrapf(err, "cannot open out DB at %s", outDir)
	}
	defer outDB.Close()
	writer := outDB.NewStreamWriter()
	if err := writer.Prepare(); err != nil {
		return y.Wrapf(err, "cannot create stream writer in out DB at %s", outDir)
	}

	// Stream contents of DB to the output DB.
	stream := db.NewStreamAt(math.MaxUint64)
	stream.LogPrefix = fmt.Sprintf("Streaming DB to new DB at %s", outDir)

	stream.Send = func(buf *z.Buffer) error {
		return writer.Write(buf)
	}
	if err := stream.Orchestrate(context.Background()); err != nil {
		return y.Wrapf(err, "cannot stream DB to out DB at %s", outDir)
	}
	if err := writer.Flush(); err != nil {
		return y.Wrapf(err, "cannot flush writer")
	}
	return nil
}

// Opts returns a copy of the DB options.
func (db *DB) Opts() Options {
	return db.opt
}

type CacheType int

const (
	BlockCache CacheType = iota
	IndexCache
)

// CacheMaxCost updates the max cost of the given cache (either block or index cache).
// The call will have an effect only if the DB was created with the cache. Otherwise it is
// a no-op. If you pass a negative value, the function will return the current value
// without updating it.
func (db *DB) CacheMaxCost(cache CacheType, maxCost int64) (int64, error) {
	if db == nil {
		return 0, nil
	}

	if maxCost < 0 {
		switch cache {
		case BlockCache:
			return db.blockCache.MaxCost(), nil
		case IndexCache:
			return db.indexCache.MaxCost(), nil
		default:
			return 0, errors.New("invalid cache type")
		}
	}

	switch cache {
	case BlockCache:
		db.blockCache.UpdateMaxCost(maxCost)
		return maxCost, nil
	case IndexCache:
		db.indexCache.UpdateMaxCost(maxCost)
		return maxCost, nil
	default:
		return 0, errors.New("invalid cache type")
	}
}

func (db *DB) LevelsToString() string {
	levels := db.Levels()
	h := func(sz int64) string {
		return humanize.IBytes(uint64(sz))
	}
	base := func(b bool) string {
		if b {
			return "B"
		}
		return " "
	}

	var b strings.Builder
	b.WriteRune('\n')
	for _, li := range levels {
		b.WriteString(fmt.Sprintf(
			"Level %d [%s]: NumTables: %02d. Size: %s of %s. Score: %.2f->%.2f"+
				" StaleData: %s Target FileSize: %s\n",
			li.Level, base(li.IsBaseLevel), li.NumTables,
			h(li.Size), h(li.TargetSize), li.Score, li.Adjusted, h(li.StaleDatSize),
			h(li.TargetFileSize)))
	}
	b.WriteString("Level Done\n")
	return b.String()
}
