package storage

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/skl"
)

// BadgerKV is the node's one shared Badger instance behind the KV seam.
type BadgerKV struct {
	db  *badger.DB
	dir string
	// dirGuard refuses writes once dir is no longer the directory the store
	// opened: Badger would go on writing to the open, unlinked memtable log
	// and acknowledge writes that are lost at the next restart (BUG-0390).
	dirGuard *dirGuard

	// lifecycle orders every use of the open store against Close. Badger
	// leaves its commit watermark short when Close runs under a committing
	// transaction, so a transaction opened on the closed store waits forever,
	// a Sync dereferences a released memtable and a commit sends on a closed
	// channel (BUG-0675). Users hold the read side for the one call that
	// touches Badger, and Close takes the write side to set closed, which
	// drains the calls in flight and refuses the ones that follow.
	lifecycle sync.RWMutex
	closed    bool

	// syncPoison is set by the first failed sync and refuses every later
	// commit (StoreSyncFailedError).
	syncPoison atomic.Pointer[StoreSyncFailedError]
	// syncDB is db.SyncVouched, which returns the highest version the sync made durable (0
	// on an error); a test replaces it to fail a sync.
	syncDB func() (uint64, error)
	// vouched is the ring store's sync marker (OpenVouchedBadgerKV); off for OpenBadgerKV.
	vouched vouchedState
}

// ErrStoreClosed is what a store user gets once the node store is closed. A
// user that was in flight when the server began to stop is the usual one.
var ErrStoreClosed = errors.New("the node store is closed because this node is shutting down; " +
	"retry the request against another node, or on this one once it has restarted")

var _ KV = (*BadgerKV)(nil)

// OpenBadgerKV opens the node's shared store at dir with today's per-bucket
// Badger options unchanged (the shared-storage build doc's §2.3): the engine
// spike tunes the shared instance, and changing the literals here would
// confound its baseline. key is nil for plaintext or a 32-byte data key.
func OpenBadgerKV(dir string, tuning BadgerTuning, key []byte) (*BadgerKV, error) {
	opts, err := badgerOptions(dir, tuning, key)
	if err != nil {
		return nil, err
	}
	if err := guardStoreBeforeOpen(dir, opts, tuning, false); err != nil {
		return nil, err
	}
	db, err := openBadgerDB(dir, opts)
	if err != nil {
		return nil, err
	}
	return finishBadgerOpen(db, dir, opts)
}

// guardStoreBeforeOpen runs the three guards every open of the node store runs, in their order.
// nothingVouched is true only for a vouched store whose marker vouches no commit (guardStoreFiles).
func guardStoreBeforeOpen(dir string, opts badger.Options, tuning BadgerTuning, nothingVouched bool) error {
	// Badger would panic on a cut table, or start empty over a cut manifest
	// and delete the tables, so refuse first.
	leftovers, err := guardStoreFiles(dir, opts, nothingVouched)
	if err != nil {
		return err
	}
	// Badger would replay a crashed larger memtable into a smaller arena and
	// exit the process, so refuse first, in words an operator can act on.
	if err := guardMemtableShrink(dir, opts.MemTableSize, tuning); err != nil {
		return err
	}
	// The only guard that changes the store runs last, so a refusal never
	// follows a change.
	if len(leftovers) > 0 {
		if err := repairFirstFlushCrash(dir, opts, leftovers, nothingVouched); err != nil {
			return err
		}
	}
	return nil
}

// missingTableText is the start of Badger's refusal of a MANIFEST that names a table file the
// directory lacks (levels.go: revertToManifest); errors.Is cannot see it through y.Wrap.
const missingTableText = "file does not exist for table"

// openBadgerDB opens Badger with opts, naming a checksum failure or a missing table file as damage.
func openBadgerDB(dir string, opts badger.Options) (*badger.DB, error) {
	db, err := badger.Open(opts)
	if err != nil && badgerChecksumMismatch(err) {
		return nil, &StoreDamagedError{Dir: dir, Damage: fmt.Sprintf("a file failed its checksum (%v)", err), Cause: err}
	}
	if err != nil && strings.Contains(err.Error(), missingTableText) {
		// Damage too, so the ring node's open can add its advice (review R2 F-6).
		return nil, &StoreDamagedError{Dir: dir, Cause: err,
			Damage: fmt.Sprintf("its MANIFEST names a table file the directory does not hold (%v)", err)}
	}
	if err != nil {
		return nil, fmt.Errorf("opening the node store at %s: %w", dir, err)
	}
	return db, nil
}

// finishBadgerOpen records the memtable size and wraps the open instance, closing it on failure.
func finishBadgerOpen(db *badger.DB, dir string, opts badger.Options) (*BadgerKV, error) {
	if err := recordMemtableSize(dir, opts.MemTableSize); err != nil {
		return nil, closeAfterOpenFailure(db, err)
	}
	kv, err := newBadgerKV(db, dir)
	if err != nil {
		return nil, closeAfterOpenFailure(db, err)
	}
	return kv, nil
}

// newBadgerKV wraps an open Badger instance whose files are in dir.
func newBadgerKV(db *badger.DB, dir string) (*BadgerKV, error) {
	guard, err := newDirGuard(dir)
	if err != nil {
		return nil, fmt.Errorf("opening the node store at %s: %w", dir, err)
	}
	return &BadgerKV{db: db, dir: dir, dirGuard: guard, syncDB: db.SyncVouched}, nil
}

// closeAfterOpenFailure closes a store that opened but cannot be used, and
// returns err with the close's own failure, if any.
func closeAfterOpenFailure(db *badger.DB, err error) error {
	if closeErr := db.Close(); closeErr != nil {
		return errors.Join(err, fmt.Errorf("closing the store after that: %w", closeErr))
	}
	return err
}

// NewBatch starts a read-write transaction. It is a Txn and never a
// WriteBatch: WriteBatch commits on its own when it reaches the batch bound,
// which would split one logical batch into two commits.
func (kv *BadgerKV) NewBatch() KVBatch {
	kv.lifecycle.RLock()
	defer kv.lifecycle.RUnlock()
	if kv.closed {
		return closedBatch{}
	}
	return &badgerBatch{kv: kv, db: kv.db, dir: kv.dir, dirGuard: kv.dirGuard, txn: kv.db.NewTransaction(true)}
}

// NewSnapshot opens a read-only transaction, which reads a timestamp and does
// no I/O.
func (kv *BadgerKV) NewSnapshot() KVSnapshot {
	kv.lifecycle.RLock()
	defer kv.lifecycle.RUnlock()
	if kv.closed {
		return closedSnapshot{}
	}
	return &badgerSnapshot{kv: kv, txn: kv.db.NewTransaction(false), dir: kv.dir}
}

// discardTransaction ends txn unless the store is closed. Badger's Txn.Discard
// posts the transaction's timestamp to a watermark channel that holds 100 and
// has no reader after Close, so the hundred-and-first discard of a transaction
// opened before Close would block for good (BUG-0681). A closed database has
// nothing left to release the timestamp to, and the transaction is garbage.
// The read side is held so a Close that has begun is waited for, as in Commit.
func (kv *BadgerKV) discardTransaction(txn *badger.Txn) {
	kv.lifecycle.RLock()
	defer kv.lifecycle.RUnlock()
	if kv.closed {
		return
	}
	txn.Discard()
}

// Sync makes every write committed before it durable: it syncs the value log,
// the active memtable's log and the log of every memtable rotated out and not
// yet flushed to a table. The last is the patched Badger in third_party/badger
// ; upstream v4.9.6 syncs only the active memtable. The store is
// opened with SyncWrites off, which is store-wide, so a durable batch is a
// commit followed by this.
func (kv *BadgerKV) Sync() error {
	kv.lifecycle.RLock()
	defer kv.lifecycle.RUnlock()
	if kv.closed {
		return fmt.Errorf("syncing the node store: %w", ErrStoreClosed)
	}
	// A sync that Badger completes after a failed one proves nothing about the writes the
	// failed one covered (write_refusal.go), so a poisoned store never reports success.
	if poison := kv.syncPoison.Load(); poison != nil {
		return fmt.Errorf("syncing the node store: %w", poison)
	}
	// syncAndVouch poisons on a failed sync, reads the poison again (a failure recorded while
	// this sync ran may cover a write this sync was to make durable) and advances the marker.
	if err := kv.syncAndVouch(); err != nil {
		return fmt.Errorf("syncing the node store: %w", err)
	}
	return nil
}

// poisonSync records the first failed sync, so every later commit is refused, and
// returns cause.
func (kv *BadgerKV) poisonSync(cause error) error {
	kv.syncPoison.CompareAndSwap(nil, &StoreSyncFailedError{Cause: cause})
	return cause
}

// PrefixBloomStats returns the store's prefix bloom counters, which start at
// zero at every open and belong to this store alone.
func (kv *BadgerKV) PrefixBloomStats() PrefixBloomStats {
	counted := kv.db.BloomPrefixStats()
	ruleStats := func(ruleID uint32) PrefixBloomRuleStats {
		rule := counted.Rules[ruleID]
		return PrefixBloomRuleStats{
			Iterators:    rule.Iterators,
			Candidates:   rule.Candidates,
			Skipped:      rule.Skipped,
			Unmarked:     rule.Unmarked,
			PrefixHashes: rule.PrefixHashes,
		}
	}
	return PrefixBloomStats{Document: ruleStats(DocumentPrefixRule), Store: ruleStats(StorePrefixRule), Refused: counted.Refused}
}

// MaxBatchBytes is Badger's per-transaction bound, 15% of the memtable.
func (kv *BadgerKV) MaxBatchBytes() int64 {
	return kv.db.MaxBatchSize()
}

// ValueThreshold is the size from which Badger keeps a value in its value log and counts
// only a pointer for it against a transaction, so a 6 MiB value is cheap for a batch and
// eight 700 KiB values are not. The store opens with Badger's default, the largest the
// threshold ever adapts to.
func (kv *BadgerKV) ValueThreshold() int64 {
	return kv.db.Opts().ValueThreshold
}

// BatchEntryBound is the most keys Badger commits in one transaction when its
// byte bound is batchBytes: it sizes the arena for a skiplist node of maximum
// height per entry, so the entry bound is the byte bound over that node size.
// It is computable without an open store, so a member can reason about the
// bound of the smallest memtable another member may run.
func BatchEntryBound(batchBytes int64) int64 {
	return batchBytes / int64(skl.MaxNodeSize)
}

// DropPrefixes removes every key under each prefix with one DB.DropPrefix call:
// one memtable flush and one compaction stop for all of them. Every writer on
// the node gets ErrBlockedWrites while it runs, so it is the shared-storage
// spec's §5.2.6 escape hatch for a maintenance window and nothing else. A nil
// return does not prove the prefixes empty: Badger logs and swallows a level-0
// compaction failure, which is why the caller never deletes a reclaim record
// on the strength of it.
func (kv *BadgerKV) DropPrefixes(prefixes [][]byte) error {
	if poison := kv.syncPoison.Load(); poison != nil {
		return fmt.Errorf("dropping %d key prefixes from the node store: %w", len(prefixes), &StoreRefusesWritesError{Dir: kv.dir, Cause: poison})
	}
	if err := kv.db.DropPrefix(prefixes...); err != nil {
		return fmt.Errorf("dropping %d key prefixes from the node store: %w", len(prefixes), refusedWrite(kv.dir, err))
	}
	return nil
}

// Close closes the Badger instance. It waits for the commits and syncs in
// flight and refuses every use that begins after it.
func (kv *BadgerKV) Close() error {
	kv.lifecycle.Lock()
	kv.closed = true
	kv.lifecycle.Unlock()
	if kv.vouched.on {
		defer repairAdvice.Delete(kv.dir)
	}
	// No sync runs once closed is set, so the marker's descriptor is no longer written.
	if kv.vouched.file != nil {
		if err := kv.vouched.file.Close(); err != nil {
			return errors.Join(fmt.Errorf("closing the store's sync marker: %w", err), kv.db.Close())
		}
	}
	if err := kv.db.Close(); err != nil {
		if errors.Is(err, badger.ErrL0StalledByDamagedTable) {
			// The store is closed and all its data is on disk; the damage is
			// what the operator has to act on, and writes were blocked by it.
			stalled := closeStalledError(kv.dir, err)
			slog.Error("the node store was stalled by a damaged table and has been closed", "error", stalled)
			return stalled
		}
		return fmt.Errorf("closing the node store: %w", err)
	}
	return nil
}

type badgerBatch struct {
	kv       *BadgerKV
	db       *badger.DB
	dir      string
	dirGuard *dirGuard
	txn      *badger.Txn
	// refusal is the first error a Set or Delete returned. It sticks, so a
	// caller that ignores it still cannot commit half a batch.
	refusal error
	// entries counts the keys Badger accepted, to tell a batch that ran out of
	// entries from one that ran out of bytes: Badger reports both as
	// ErrTxnTooBig.
	entries int64
}

func (b *badgerBatch) Set(key, value []byte) error {
	if b.refusal != nil {
		return b.refusal
	}
	// Badger keeps references to key and value until the commit.
	err := b.txn.Set(bytes.Clone(key), bytes.Clone(value))
	if corpusTapEnabled && err == nil {
		recordCorpusValue(key, value)
	}
	return b.refuse(err, key)
}

func (b *badgerBatch) Delete(key []byte) error {
	if b.refusal != nil {
		return b.refusal
	}
	return b.refuse(b.txn.Delete(bytes.Clone(key)), key)
}

func (b *badgerBatch) refuse(err error, key []byte) error {
	switch {
	case err == nil:
		b.entries++
		return nil
	case errors.Is(err, badger.ErrTxnTooBig):
		b.refusal = &BatchTooLargeError{
			Bytes:   b.db.MaxBatchSize(),
			Entries: b.db.MaxBatchCount(),
			// Badger refuses when the count reaches the bound, and its count
			// starts at 1 for the transaction's own commit marker.
			EntryCountReached: b.entries+2 >= b.db.MaxBatchCount(),
			Key:               bytes.Clone(key),
		}
	case len(key) > MaxKeyBytes:
		b.refusal = &KeyTooLargeError{Size: len(key), Limit: MaxKeyBytes, Key: bytes.Clone(key)}
	default:
		b.refusal = &KeyError{Operation: "adding a key to a batch", Key: bytes.Clone(key), Cause: err}
	}
	return b.refusal
}

func (b *badgerBatch) Commit(sync bool) error {
	if b.refusal != nil {
		b.Discard()
		return b.refusal
	}
	// Held through the commit and the sync: Close waits for them, and nothing
	// that starts after Close reaches Badger.
	b.kv.lifecycle.RLock()
	defer b.kv.lifecycle.RUnlock()
	if b.kv.closed {
		// No Discard: see discardTransaction.
		return fmt.Errorf("committing a batch: %w", ErrStoreClosed)
	}
	if poison := b.kv.syncPoison.Load(); poison != nil {
		b.txn.Discard()
		return fmt.Errorf("committing a batch: %w", &StoreRefusesWritesError{Dir: b.dir, Cause: poison})
	}
	if gone := b.dirGuard.check(); gone != nil {
		b.txn.Discard()
		return fmt.Errorf("committing a batch: %w", &StoreRefusesWritesError{Dir: b.dir, Cause: gone})
	}
	if err := b.txn.Commit(); err != nil {
		return fmt.Errorf("committing a batch: %w", refusedWrite(b.dir, err))
	}
	if !sync {
		return nil
	}
	// The transaction is committed and readable from here on: a failed sync is not a failed
	// commit, and a caller that applied the batch again would apply it twice (BUG-0809).
	// syncAndVouch poisons a failed sync or marker write, and reads the poison again: another
	// commit's sync may have failed after this one passed the check above and before this sync
	// finished, and this batch is then one the failed sync may have left unwritten.
	if err := b.kv.syncAndVouch(); err != nil {
		return fmt.Errorf("syncing a committed batch: %w: %w", ErrCommittedNotSynced, err)
	}
	return nil
}

func (b *badgerBatch) Discard() {
	b.kv.discardTransaction(b.txn)
}

type badgerSnapshot struct {
	kv  *BadgerKV
	txn *badger.Txn
	dir string
}

func (s *badgerSnapshot) Get(key []byte) ([]byte, error) {
	item, err := s.txn.Get(key)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, &KeyError{Operation: "reading a key", Key: bytes.Clone(key), Cause: ErrKeyNotFound}
	}
	if err != nil {
		return nil, &KeyError{Operation: "reading a key", Key: bytes.Clone(key), Cause: damagedRead(s.dir, err)}
	}
	stored, err := item.ValueCopy(nil)
	if err != nil {
		return nil, &KeyError{Operation: "reading the value of a key", Key: bytes.Clone(key), Cause: err}
	}
	return stored, nil
}

// NewIterator walks with AllVersions set, always. A default Badger iterator
// drops delete markers and older versions inside Iterator.parseItem without
// surfacing them, so a visit counter built on one reads zero over a run of a
// million markers — the exact walk the reclaimer's budget exists to charge.
// Liveness and the bounds are therefore decided here, in Go; Badger is given
// only the prefix the bounds share, so it can leave out the tables that cannot
// hold a key in range. Bounds made by DocumentIterOptions or StoreIterOptions
// also name the document or store prefix every key in range starts with, and
// Badger then leaves out the tables whose bloom filter does not hold it.
func (s *badgerSnapshot) NewIterator(opts IterOptions) KVIterator {
	inner := s.txn.NewIterator(badger.IteratorOptions{
		AllVersions:    true,
		PrefetchValues: false,
		Reverse:        opts.Reverse,
		Prefix:         sharedPrefix(opts.Lower, opts.Upper),
		BloomPrefix:    opts.filterPrefix(),
	})
	return &badgerIterator{inner: inner, opts: opts, dir: s.dir}
}

// sharedPrefix returns the longest prefix of lower and upper both start with,
// or nil when either bound is open or they share nothing. Every key in
// [lower, upper) sorts between the two bounds, so it starts with that prefix
// too, which is what lets Badger skip every table whose key range misses it.
// Upper itself starts with the prefix as well, and that is deliberate: Badger's
// Valid is false for a key outside Prefix, and a reverse walk seeks to the
// exclusive Upper first, so a longer prefix that excluded Upper would end the
// walk before it reached the keys below it.
func sharedPrefix(lower, upper []byte) []byte {
	if len(lower) == 0 || len(upper) == 0 {
		return nil
	}
	shared := 0
	for shared < len(lower) && shared < len(upper) && lower[shared] == upper[shared] {
		shared++
	}
	if shared == 0 {
		return nil
	}
	return lower[:shared]
}

func (s *badgerSnapshot) Discard() {
	s.kv.discardTransaction(s.txn)
}

type badgerIterator struct {
	inner  *badger.Iterator
	opts   IterOptions
	dir    string
	visits int64
	// err is the error the inner iterator stopped on, which ended the walk.
	err error

	positioned bool
	// key is the current key as a caller has seen it: made or handed over the
	// first time Key is called at a position, so a walk that never asks for a
	// key copies none.
	key []byte
	// keyScratch holds the current key's bytes until Key hands them to a
	// caller. The walk needs the key across Next to pass the rest of a key's
	// version run, and Badger recycles an Item's key buffer once the iterator
	// moves (badger iterator.go:Iterator.Next pushes the item to the waste list
	// that newItem pops), so the bytes are copied while the item is valid —
	// into this buffer, reused from key to key.
	keyScratch []byte
	// standing is set while a forward walk is positioned on a key and the
	// inner iterator still stands on that key's newest version: Next passes
	// the rest of the key's run before it settles.
	standing bool
	// liveItem is that newest version, valid until the inner iterator moves,
	// so a forward Value copies from it on demand and a walk that never asks
	// copies nothing.
	liveItem *badger.Item
	// scratch holds a reverse walk's live version, reused from key to key. A
	// reverse walk learns which version is the newest only after the inner
	// iterator has moved past it, and Badger recycles an Item once it does
	// (badger iterator.go:Iterator.Next pushes it to the waste list that
	// newItem pops), so the bytes must be copied while the item is valid —
	// into this buffer, reused while no Value claims it, and handed over to
	// the caller by Value.
	scratch   []byte
	valueRead bool
	value     []byte
	valueErr  error
}

func (it *badgerIterator) Seek(key []byte) {
	target := key
	// An empty target is the same request as nil. Badger turns an empty key
	// into its Prefix, which for a reverse walk is the wrong end.
	if it.opts.Reverse {
		if len(target) == 0 || (it.opts.Upper != nil && bytes.Compare(target, it.opts.Upper) > 0) {
			target = it.opts.Upper
		}
	} else if len(target) == 0 || (it.opts.Lower != nil && bytes.Compare(target, it.opts.Lower) < 0) {
		target = it.opts.Lower
	}
	// Badger seeks to the start (or, in reverse, the end) on an empty key.
	it.inner.Seek(target)
	it.standing = false
	it.err = nil
	it.settle()
}

func (it *badgerIterator) Next() {
	if it.standing {
		key := it.key
		if key == nil {
			key = it.keyScratch
		}
		it.inner.Next()
		it.passRun(key)
		it.standing = false
	}
	it.settle()
}

// settle moves to the next key inside the bounds whose newest version is live
// — or to the next key at all, with IncludeDeleted — counting every version it
// passes. With AllVersions, Badger yields a key's versions newest first going
// forward and oldest first in reverse, so a key's newest version is the first
// of its run forward and the last in reverse.
func (it *badgerIterator) settle() {
	it.positioned = false
	it.key = nil
	it.liveItem, it.valueRead, it.value, it.valueErr = nil, false, nil, nil
	for it.inner.Valid() {
		item := it.inner.Item()
		if it.pastEnd(item.Key()) {
			return
		}
		if it.beforeStart(item.Key()) {
			it.visits++
			it.inner.Next()
			continue
		}
		if it.opts.Reverse {
			if it.settleReverse(item) {
				return
			}
			continue
		}
		if it.settleForward(item) {
			return
		}
	}
	it.stoppedOnError()
}

// stoppedOnError records the error the inner iterator stopped on, if it did,
// and reports whether it had. The patched Badger stops a walk at a table block
// it cannot read rather than step over it.
func (it *badgerIterator) stoppedOnError() bool {
	if readErr := it.inner.Error(); readErr != nil {
		it.err = damagedRead(it.dir, readErr)
		return true
	}
	return false
}

// settleForward stops on key if its newest version, item, is live — or with
// IncludeDeleted at all — leaving the inner iterator on item so Value can copy
// it later. Otherwise it passes the key's run.
func (it *badgerIterator) settleForward(item *badger.Item) bool {
	it.visits++
	// The key is copied into the reusable scratch, not into a fresh buffer:
	// the walk needs its bytes across the Next that ends the version run, and
	// Key hands them over only when a caller actually asks for the key.
	it.keyScratch = item.KeyCopy(it.keyScratch[:0])
	live := !item.IsDeletedOrExpired()
	if live || it.opts.IncludeDeleted {
		it.positioned, it.standing = true, true
		if live {
			it.liveItem = item
		} else {
			it.valueRead, it.valueErr = true, &KeyError{Operation: "reading a key", Key: bytes.Clone(item.Key()), Cause: ErrKeyNotFound}
		}
		return true
	}
	it.inner.Next()
	it.passRun(it.keyScratch)
	return false
}

// settleReverse reads key's run oldest first, copying each live version into
// scratch, and stops on key if the last — the newest — was live, or with
// IncludeDeleted at all.
func (it *badgerIterator) settleReverse(item *badger.Item) bool {
	// A reverse walk learns which version is the newest only after the inner
	// iterator has moved past it, and that move recycles the item's key bytes,
	// so the run's key is copied into the reusable scratch while it is valid.
	key := item.KeyCopy(it.keyScratch[:0])
	it.keyScratch = key
	var live bool
	var readErr error
	for {
		live, readErr = it.copyVersion(item)
		it.visits++
		it.inner.Next()
		if !it.inner.Valid() || !bytes.Equal(it.inner.Item().Key(), key) {
			break
		}
		item = it.inner.Item()
	}
	// A run cut short by an error has not shown its newest version, so what
	// was read of it cannot be answered as the key's value.
	if !it.inner.Valid() && it.stoppedOnError() {
		return true
	}
	switch {
	case live:
		it.positioned = true
		if readErr != nil {
			it.valueRead, it.valueErr = true, readErr
		}
		return true
	case it.opts.IncludeDeleted:
		it.positioned = true
		it.valueRead, it.valueErr = true, &KeyError{Operation: "reading a key", Key: bytes.Clone(key), Cause: ErrKeyNotFound}
		return true
	}
	return false
}

// passRun counts and passes the versions of key the inner iterator stands on.
func (it *badgerIterator) passRun(key []byte) {
	for it.inner.Valid() && bytes.Equal(it.inner.Item().Key(), key) {
		it.visits++
		it.inner.Next()
	}
}

// pastEnd reports whether key lies beyond the bound the walk ends at.
func (it *badgerIterator) pastEnd(key []byte) bool {
	if it.opts.Reverse {
		return it.opts.Lower != nil && bytes.Compare(key, it.opts.Lower) < 0
	}
	return it.opts.Upper != nil && bytes.Compare(key, it.opts.Upper) >= 0
}

// beforeStart reports whether a reverse walk is still at or above its
// exclusive Upper, where Badger's reverse seek lands (at or before the key).
// A forward walk cannot be: Seek clamps its target to Lower.
func (it *badgerIterator) beforeStart(key []byte) bool {
	return it.opts.Reverse && it.opts.Upper != nil && bytes.Compare(key, it.opts.Upper) >= 0
}

// copyVersion copies one version's value into scratch while its item is still
// valid, and reports whether the version is live.
func (it *badgerIterator) copyVersion(item *badger.Item) (bool, error) {
	if item.IsDeletedOrExpired() {
		return false, nil
	}
	copied, err := item.ValueCopy(it.scratch[:0])
	if err != nil {
		return true, &KeyError{Operation: "reading the value of a key", Key: bytes.Clone(item.Key()), Cause: err}
	}
	it.scratch = copied
	return true, nil
}

func (it *badgerIterator) Valid() bool {
	return it.positioned
}

func (it *badgerIterator) Key() []byte {
	if it.key != nil {
		return it.key
	}
	// Hand the scratch buffer over rather than copying it a second time; the
	// next position starts a new buffer. A walk that never calls Key never
	// pays for one.
	it.key = it.keyScratch
	it.keyScratch = nil
	return it.key
}

// Value copies the current value the first time it is asked for: from the
// standing item going forward, from scratch in reverse.
func (it *badgerIterator) Value() ([]byte, error) {
	if it.valueRead {
		return it.value, it.valueErr
	}
	it.valueRead = true
	if it.liveItem == nil {
		// The caller keeps the bytes, so scratch is handed over rather than
		// copied again, and the next key's copy starts a new buffer. An empty
		// value is nil, as ValueCopy(nil) answers.
		if len(it.scratch) > 0 {
			it.value = it.scratch
		}
		it.scratch = nil
		return it.value, nil
	}
	stored, err := it.liveItem.ValueCopy(nil)
	if err != nil {
		it.valueErr = &KeyError{Operation: "reading the value of a key", Key: bytes.Clone(it.key), Cause: err}
		return nil, it.valueErr
	}
	it.value = stored
	return it.value, nil
}

func (it *badgerIterator) Visits() int64 {
	return it.visits
}

func (it *badgerIterator) Err() error {
	return it.err
}

func (it *badgerIterator) Close() {
	it.inner.Close()
}

// Size returns the store's LSM and value-log bytes, as Badger last measured
// them: at open, then once a minute, so the figure can be a minute stale.
//
// Badger's own DB.Size returns 0, 0 and no error whenever it has no figure,
// which is exactly the healthy-looking zero a capacity reading must not
// report. After Open it has none in two cases, and only two: an in-memory
// store, and one opened with metrics disabled (badger v4.9.6
// db.go:DB.calculateSize stores nothing for either, and runs synchronously
// inside Open otherwise). Both are refused here. Neither is how the node
// store opens (badgerOptions leaves MetricsEnabled at its default, true).
func (kv *BadgerKV) Size() (lsm, vlog int64, err error) {
	opts := kv.db.Opts()
	if opts.InMemory {
		return 0, 0, errors.New("the node store is in memory, so it has no size on disk to report")
	}
	if !opts.MetricsEnabled {
		return 0, 0, errors.New("the node store was opened with Badger metrics disabled, so it cannot report its size")
	}
	lsm, vlog = kv.db.Size()
	return lsm, vlog, nil
}

// closeStalledError is Close's error for a store stalled behind a damaged table, with the store's
// repair advice (withRepairAdvice).
func closeStalledError(dir string, err error) error {
	return fmt.Errorf("closing the node store at %s: it was stalled by a damaged file, so no write could "+
		"complete: %w%s", dir, err, withRepairAdvice(dir))
}
