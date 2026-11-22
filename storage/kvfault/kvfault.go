// Package kvfault is an in-memory storage.KV for tests that need to lose
// power. Badger cannot show what a power loss does to a committed-but-unsynced
// batch — a committed batch cannot be un-committed and there is no filesystem
// interface to wrap — so the shared-storage spec's T-26(c) runs its power-loss
// arm here instead.
//
// It is a non-test package so the server's tests can import it. Production
// code must never import it.
package kvfault

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/hoarddb/hoarddb-server/storage"
)

// DefaultMaxBatchBytes is the batch bound a Store enforces unless a test sets
// another; it is of the same order as the shared Badger instance's bound.
const DefaultMaxBatchBytes = 4 << 20

// batchEntryOverhead is charged per write on top of its key and value, so a
// batch of many tiny writes is bounded too, as it is on Badger.
const batchEntryOverhead = 16

// version is one committed write of a key. A delete is a version too, and
// stays one until Compact, so Visits counts it as it would on Badger.
type version struct {
	commit  uint64
	deleted bool
	stored  []byte
}

// Store is an in-memory storage.KV that remembers which committed batches
// have been synced, so PowerLoss can discard the ones that have not.
type Store struct {
	mu            sync.Mutex
	versions      map[string][]version // oldest first
	lastCommit    uint64
	syncedCommit  uint64
	maxBatchBytes int64
	// failReadKey is the one-shot read fault FailNextReadOf armed: the next
	// read of that key, through a snapshot Get or an iterator's Value, fails
	// once. Nil when no fault is armed.
	failReadKey []byte
	// syncs counts synced batches and Sync calls, so a test can pin that a
	// path adds no synced write, or exactly one.
	syncs int64
}

var _ storage.KV = (*Store)(nil)

// New returns an empty store with DefaultMaxBatchBytes.
func New() *Store {
	return &Store{versions: make(map[string][]version), maxBatchBytes: DefaultMaxBatchBytes}
}

// SetMaxBatchBytes changes the batch bound, so a test can reach it cheaply.
func (s *Store) SetMaxBatchBytes(bound int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maxBatchBytes = bound
}

// FailNextReadOf arms a one-shot read fault: the next read of key, whether
// through a snapshot Get or an iterator's Value, returns an error instead of
// the stored bytes, as a transient disk error does. Tests use it to drive a
// handler's store read onto its failure path; production code never calls it.
// The key is the raw store key, not the document's user key.
func (s *Store) FailNextReadOf(key []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failReadKey = bytes.Clone(key)
}

// injectedReadFailure is the error the armed read returns, shaped as the Badger
// store's own read errors are (a storage.KeyError naming the key), so the code
// that logs the key of a failed read sees it as it would in production.
func injectedReadFailure(key []byte) error {
	return &storage.KeyError{Operation: "reading a key", Key: bytes.Clone(key), Cause: errors.New("injected read failure")}
}

// consumeFailRead reports whether the one-shot fault for key is armed and
// clears it. The caller holds s.mu.
func (s *Store) consumeFailRead(key []byte) bool {
	if s.failReadKey == nil || !bytes.Equal(s.failReadKey, key) {
		return false
	}
	s.failReadKey = nil
	return true
}

// PowerLoss discards every batch committed without a sync since the last
// synced batch or Sync — what the page cache held and the disk did not.
func (s *Store) PowerLoss() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, history := range s.versions {
		kept := history[:0]
		for _, v := range history {
			if v.commit <= s.syncedCommit {
				kept = append(kept, v)
			}
		}
		if len(kept) == 0 {
			delete(s.versions, key)
			continue
		}
		s.versions[key] = kept
	}
	s.lastCommit = s.syncedCommit
}

// Compact drops every superseded version and every delete marker, as an LSM
// compaction eventually does. No snapshot may be open while it runs.
func (s *Store) Compact() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, history := range s.versions {
		newest := history[len(history)-1]
		if newest.deleted {
			delete(s.versions, key)
			continue
		}
		s.versions[key] = []version{newest}
	}
}

// NewBatch starts a batch.
func (s *Store) NewBatch() storage.KVBatch {
	return &batch{store: s}
}

// NewSnapshot fixes a read view at the last commit.
func (s *Store) NewSnapshot() storage.KVSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return &snapshot{store: s, readCommit: s.lastCommit}
}

// Sync marks every batch committed so far durable.
func (s *Store) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.syncedCommit = s.lastCommit
	s.syncs++
	return nil
}

// Syncs is the number of synced batch commits and Sync calls so far.
func (s *Store) Syncs() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.syncs
}

// MaxBatchBytes is the store's batch bound.
func (s *Store) MaxBatchBytes() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.maxBatchBytes
}

// Close is a no-op: the store lives in memory.
func (s *Store) Close() error {
	return nil
}

type write struct {
	key     []byte
	deleted bool
	stored  []byte
}

type batch struct {
	store   *Store
	writes  []write
	bytes   int64
	refusal error
}

func (b *batch) add(w write) error {
	if b.refusal != nil {
		return b.refusal
	}
	b.bytes += int64(len(w.key)+len(w.stored)) + batchEntryOverhead
	if bound := b.store.MaxBatchBytes(); b.bytes >= bound {
		b.refusal = &storage.BatchTooLargeError{Bytes: bound, Key: w.key}
		return b.refusal
	}
	b.writes = append(b.writes, w)
	return nil
}

func (b *batch) Set(key, value []byte) error {
	return b.add(write{key: bytes.Clone(key), stored: bytes.Clone(value)})
}

func (b *batch) Delete(key []byte) error {
	return b.add(write{key: bytes.Clone(key), deleted: true})
}

func (b *batch) Commit(sync bool) error {
	if b.refusal != nil {
		return b.refusal
	}
	s := b.store
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastCommit++
	for _, w := range b.writes {
		k := string(w.key)
		s.versions[k] = append(s.versions[k], version{commit: s.lastCommit, deleted: w.deleted, stored: w.stored})
	}
	if sync {
		s.syncedCommit = s.lastCommit
		s.syncs++
	}
	b.writes = nil
	return nil
}

func (b *batch) Discard() {
	b.writes = nil
}

type snapshot struct {
	store      *Store
	readCommit uint64
}

// visible returns the versions of history a snapshot at readCommit can see,
// oldest first.
func visible(history []version, readCommit uint64) []version {
	end := 0
	for end < len(history) && history[end].commit <= readCommit {
		end++
	}
	return history[:end]
}

func (sn *snapshot) Get(key []byte) ([]byte, error) {
	sn.store.mu.Lock()
	defer sn.store.mu.Unlock()
	if sn.store.consumeFailRead(key) {
		return nil, injectedReadFailure(key)
	}
	seen := visible(sn.store.versions[string(key)], sn.readCommit)
	if len(seen) == 0 || seen[len(seen)-1].deleted {
		return nil, fmt.Errorf("reading key %x: %w", key, storage.ErrKeyNotFound)
	}
	return bytes.Clone(seen[len(seen)-1].stored), nil
}

// NewIterator materialises the snapshot's keys within the bounds. That is
// O(keys) per iterator, which is the fault store's price for simplicity; it
// holds test-sized data only.
func (sn *snapshot) NewIterator(opts storage.IterOptions) storage.KVIterator {
	sn.store.mu.Lock()
	defer sn.store.mu.Unlock()
	var entries []entry
	for k, history := range sn.store.versions {
		key := []byte(k)
		if opts.Lower != nil && bytes.Compare(key, opts.Lower) < 0 {
			continue
		}
		if opts.Upper != nil && bytes.Compare(key, opts.Upper) >= 0 {
			continue
		}
		seen := visible(history, sn.readCommit)
		if len(seen) == 0 {
			continue
		}
		newest := seen[len(seen)-1]
		entries = append(entries, entry{key: key, versions: int64(len(seen)), live: !newest.deleted, stored: newest.stored})
	}
	sort.Slice(entries, func(i, j int) bool { return bytes.Compare(entries[i].key, entries[j].key) < 0 })
	return &iterator{store: sn.store, entries: entries, reverse: opts.Reverse, includeDeleted: opts.IncludeDeleted, position: -1}
}

func (sn *snapshot) Discard() {}

type entry struct {
	key      []byte
	versions int64
	live     bool
	stored   []byte
}

type iterator struct {
	store          *Store
	entries        []entry
	reverse        bool
	includeDeleted bool
	position       int
	visits         int64
}

func (it *iterator) Seek(key []byte) {
	switch {
	case key == nil && it.reverse:
		it.position = len(it.entries) - 1
	case key == nil:
		it.position = 0
	case it.reverse:
		// The last entry at or before key.
		it.position = sort.Search(len(it.entries), func(i int) bool { return bytes.Compare(it.entries[i].key, key) > 0 }) - 1
	default:
		it.position = sort.Search(len(it.entries), func(i int) bool { return bytes.Compare(it.entries[i].key, key) >= 0 })
	}
	it.settle()
}

func (it *iterator) Next() {
	it.step()
	it.settle()
}

func (it *iterator) step() {
	if it.reverse {
		it.position--
		return
	}
	it.position++
}

func (it *iterator) inRange() bool {
	return it.position >= 0 && it.position < len(it.entries)
}

// settle counts the current entry's versions and moves past it until a live
// one is found, or stops at once with includeDeleted.
func (it *iterator) settle() {
	for it.inRange() {
		it.visits += it.entries[it.position].versions
		if it.entries[it.position].live || it.includeDeleted {
			return
		}
		it.step()
	}
}

func (it *iterator) Valid() bool {
	return it.inRange()
}

func (it *iterator) Key() []byte {
	return it.entries[it.position].key
}

func (it *iterator) Value() ([]byte, error) {
	current := it.entries[it.position]
	if !current.live {
		return nil, fmt.Errorf("reading key %x: %w", current.key, storage.ErrKeyNotFound)
	}
	it.store.mu.Lock()
	armed := it.store.consumeFailRead(current.key)
	it.store.mu.Unlock()
	if armed {
		return nil, injectedReadFailure(current.key)
	}
	return bytes.Clone(current.stored), nil
}

func (it *iterator) Visits() int64 {
	return it.visits
}

// Err is always nil: the fault store holds its keys in memory, so a walk ends
// only at the end of its range.
func (it *iterator) Err() error {
	return nil
}

func (it *iterator) Close() {}

// SyncFault wraps a storage.KV so a test can fail the sync of a synced commit after the
// batch itself committed: the batch is in the store and readable, and only the sync
// reported an error, as when Badger's Sync fails after its transaction committed
// (storage.badgerBatch.Commit). As on Badger, the error wraps storage.ErrCommittedNotSynced
// and every later commit through the wrapper is refused with a storage.StoreSyncFailedError.
// Every other call goes to the wrapped store.
type SyncFault struct {
	storage.KV
	armed  atomic.Bool
	poison atomic.Pointer[storage.StoreSyncFailedError]
}

// WrapSyncFault returns inner behind a sync fault that is not armed.
func WrapSyncFault(inner storage.KV) *SyncFault {
	return &SyncFault{KV: inner}
}

// FailNextSync arms a one-shot fault: the next Commit(true) commits its batch unsynced and
// then returns an error, as a failed sync of a committed batch does.
func (f *SyncFault) FailNextSync() {
	f.armed.Store(true)
}

// NewBatch starts a batch of the wrapped store whose synced commit the fault can fail.
func (f *SyncFault) NewBatch() storage.KVBatch {
	return &syncFaultBatch{KVBatch: f.KV.NewBatch(), fault: f}
}

type syncFaultBatch struct {
	storage.KVBatch
	fault *SyncFault
}

// Sync refuses once an injected failure has poisoned the wrapper, as BadgerKV.Sync does: a
// later sync proves nothing about the writes the failed one covered.
func (f *SyncFault) Sync() error {
	if poison := f.poison.Load(); poison != nil {
		return fmt.Errorf("syncing the node store: %w", poison)
	}
	return f.KV.Sync()
}

func (b *syncFaultBatch) Commit(sync bool) error {
	if poison := b.fault.poison.Load(); poison != nil {
		b.Discard()
		return fmt.Errorf("committing a batch: %w", &storage.StoreRefusesWritesError{Dir: "kvfault", Cause: poison})
	}
	if !sync || !b.fault.armed.CompareAndSwap(true, false) {
		return b.KVBatch.Commit(sync)
	}
	if err := b.KVBatch.Commit(false); err != nil {
		return err
	}
	b.fault.poison.CompareAndSwap(nil, &storage.StoreSyncFailedError{Cause: errInjectedSync})
	return fmt.Errorf("syncing a committed batch: %w: %w", storage.ErrCommittedNotSynced, errInjectedSync)
}

// errInjectedSync is the failure an armed SyncFault reports.
var errInjectedSync = errors.New("injected sync failure")
