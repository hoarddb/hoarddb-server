package wire

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

// ExpiryRemover issues the removal of one entry's item through the write path, as the
// owner's own system write: the Writer admits it only on the owner of the pair, under the lease
// and the pair lock, and the applier's precondition refuses it when the item is no longer
// expired or no longer there. A write the node may not order (it holds only a copy, the pair is
// held) is not an error: it returns skipped true and the entry stays.
type ExpiryRemover interface {
	// RemoveDocument deletes the document the entry names.
	RemoveDocument(ctx context.Context, entry storage.ExpiryEntry) (skipped bool, err error)
	// RemoveElement takes the queue element the entry names, by position.
	RemoveElement(ctx context.Context, entry storage.ExpiryEntry) (skipped bool, err error)
}

// ExpirySweeper is the node's expiry pass (expiry spec 3.3): one Seek of the node-local expiry
// queue per pass, a walk while at is at or before the owner's clock, bounded by the caller's
// budget. It keeps one cursor and nothing per bucket, per slot or per pair. The clock decides
// only WHICH entries are proposed; every removal is one replicated write whose outcome depends
// on no clock, and whose precondition is judged again under the pair lock.
type ExpirySweeper struct {
	kv      storage.KV
	remover ExpiryRemover
	now     func() int64

	// lock runs fn under the pair lock of (slot, db), the lock every write and every copy's
	// apply orders under; nil runs fn at once (a store with no pairs, as in a test).
	lock func(slot uint16, db uint64, fn func()) (held bool)
	// declared gives the expiry declaration in force for an entry's bucket, so verification can tell
	// an entry the declaration still expires from one it no longer does (drop expire, set expire at
	// another field); nil or false makes a live item wait.
	declared func(entry storage.ExpiryEntry) (cluster.AdmittedExpire, bool)
	// afterVerify is a test hook run between the first verification of a stale entry and the
	// locked one that precedes its drop.
	afterVerify func()

	// maxWall bounds a pass by elapsed time as well as by keys, so one slow write to a copy cannot
	// hold the goroutine that also runs the dedupe round and the reclaim records.
	maxWall time.Duration
	wall    func() time.Time

	mu     sync.Mutex
	cursor []byte

	removedDocuments, removedElements, orphans atomic.Uint64
}

// WithExpiryTable gives the sweeper the expiry declaration in force for an entry's bucket. An item
// is removed only when the declaration still expires it and its own time is the entry's.
func (s *ExpirySweeper) WithExpiryTable(declared func(entry storage.ExpiryEntry) (cluster.AdmittedExpire, bool)) *ExpirySweeper {
	s.declared = declared
	return s
}

// defaultExpiryWallBudget is how long one pass may run before it leaves the rest for the next tick.
const defaultExpiryWallBudget = 200 * time.Millisecond

// WithWallBudget sets the wall-time bound of a pass and the clock that measures it.
func (s *ExpirySweeper) WithWallBudget(budget time.Duration, wall func() time.Time) *ExpirySweeper {
	s.maxWall, s.wall = budget, wall
	return s
}

// WithPairLock makes the sweeper drop stale entries under the pair lock, so a write that lands
// between its read and its delete is seen, not undone.
func (s *ExpirySweeper) WithPairLock(lock func(slot uint16, db uint64, fn func()) (held bool)) *ExpirySweeper {
	s.lock = lock
	return s
}

// NewExpirySweeper returns a sweeper over kv. now is the owner's wall clock in milliseconds.
func NewExpirySweeper(kv storage.KV, remover ExpiryRemover, now func() int64) *ExpirySweeper {
	return &ExpirySweeper{kv: kv, remover: remover, now: clockOrSystem(now), maxWall: defaultExpiryWallBudget, wall: time.Now}
}

// ExpiryPassResult is what one pass did.
type ExpiryPassResult struct {
	// Visited is the entries examined, the number charged to the reclaim budget.
	Visited int
	// Removed is the removals the write path accepted.
	Removed int
	// Dropped is the stale entries deleted without a removal: orphans of a deleted document, a
	// dropped bucket, or an element that is gone and not leased.
	Dropped int
	// Skipped is the entries left for later: a copy's, a leased element's, a refused removal.
	Skipped int
}

// Pass examines at most budget entries due at the owner's clock and acts on each as the spec's
// step 2 to 4 say. It never holds a lock across a removal. A failed removal is returned after the
// pass ends, the entry left for the next tick; the pass goes on to the next entry.
func (s *ExpirySweeper) Pass(ctx context.Context, budget int) (ExpiryPassResult, error) {
	var pass ExpiryPassResult
	if budget <= 0 {
		return pass, nil
	}
	now := s.now()
	s.mu.Lock()
	start := bytes.Clone(s.cursor)
	s.mu.Unlock()
	due, reachedEnd, err := s.dueEntries(start, now, budget)
	if err != nil {
		return pass, err
	}
	var firstErr error
	var last []byte
	started := s.wall()
	for _, key := range due {
		if last != nil && s.wall().Sub(started) >= s.maxWall {
			reachedEnd = false
			break
		}
		if err := ctx.Err(); err != nil {
			return pass, fmt.Errorf("sweeping expiry entries: %w", err)
		}
		last = key
		pass.Visited++
		entry, err := storage.ParseExpiryKey(key)
		if err != nil {
			return pass, fmt.Errorf("a key in the expiry namespace does not parse: %w", err)
		}
		if err := s.settle(ctx, key, entry, &pass); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	s.mu.Lock()
	if reachedEnd || last == nil {
		s.cursor = nil
	} else {
		s.cursor = append(bytes.Clone(last), 0)
	}
	s.mu.Unlock()
	return pass, firstErr
}

// dueEntries lists up to limit keys from start (or the namespace start) whose time is at or
// before now, from one snapshot, and says whether the walk reached the first later entry or the
// end of the namespace.
func (s *ExpirySweeper) dueEntries(start []byte, now int64, limit int) ([][]byte, bool, error) {
	snapshot := s.kv.NewSnapshot()
	defer snapshot.Discard()
	prefix := storage.ExpiryPrefix()
	it := snapshot.NewIterator(storage.IterOptions{Lower: prefix, Upper: storage.PrefixEnd(prefix)})
	defer it.Close()
	seek := prefix
	if len(start) > 0 {
		seek = start
	}
	var keys [][]byte
	for it.Seek(seek); it.Valid(); it.Next() {
		entry, err := storage.ParseExpiryKey(it.Key())
		if err != nil {
			return nil, false, fmt.Errorf("a key in the expiry namespace does not parse: %w", err)
		}
		if entry.AtMillis > now {
			return keys, true, it.Err()
		}
		if len(keys) == limit {
			return keys, false, it.Err()
		}
		keys = append(keys, bytes.Clone(it.Key()))
	}
	return keys, true, it.Err()
}

// action is what verification decided for one entry.
type action int

const (
	actionWait   action = iota // leave it: leased, or nothing to decide yet
	actionDrop                 // stale: delete the entry only
	actionRemove               // present and due: ask the remover
)

// verify reads the stored data an entry names and decides, from one snapshot, what to do. It
// writes nothing.
func (s *ExpirySweeper) verify(entry storage.ExpiryEntry) (action, error) {
	snapshot := s.kv.NewSnapshot()
	defer snapshot.Discard()
	ref := storage.BucketRef{Slot: entry.Slot, DatabaseID: entry.DBID, BucketID: entry.BucketID}
	state, err := ReadHeaderState(snapshot, DocumentKey{Ref: ref, EncID: entry.EncID})
	if err != nil {
		return actionWait, fmt.Errorf("verifying an expiry entry: %w", err)
	}
	if state != HeaderLive {
		// The document is gone, deleted, or its bucket dropped or retired: an orphan.
		return actionDrop, nil
	}
	var table cluster.AdmittedExpire
	known := false
	if s.declared != nil {
		table, known = s.declared(entry)
	}
	if !known {
		return actionWait, nil
	}
	if entry.Kind == storage.ExpiryKindDocument {
		// The declaration must still expire documents on a field, and the document's field must still
		// hold the entry's time: after `drop expire at`, or `set expire at` another field, the entry is stale.
		if table.BucketField == 0 {
			return actionDrop, nil
		}
		stored, err := readScalar(snapshot, store.DocumentPrefix(entry.Slot, entry.DBID, entry.BucketID, entry.EncID), table.BucketField)
		if err != nil {
			return actionWait, fmt.Errorf("verifying an expiry entry: %w", err)
		}
		if at, ok := expiryDatetime(stored); !ok || at != entry.AtMillis {
			return actionDrop, nil
		}
		return actionRemove, nil
	}
	var path []byte
	declaredStore := false
	for _, expiring := range table.Stores {
		if expiring.FieldID == entry.FieldID {
			path, declaredStore = expiring.Path, true
		}
	}
	if !declaredStore {
		return actionDrop, nil
	}
	field := store.FieldRef{Owner: storage.Key(entry.Slot, entry.DBID, entry.BucketID, storage.NSFieldStore, entry.EncID), FieldID: entry.FieldID, Engine: store.FieldFIFO}
	element, err := snapshot.Get(store.FieldElementKey(field.StorePrefix(), storage.NSQueue, store.PositionRemainder(entry.Position)))
	switch {
	case err == nil:
		// Something sits at the position, but it is the entry's element only if it carries the entry's
		// datetime at the declared path.
		if elementCarries(element, path, entry.AtMillis) {
			return actionRemove, nil
		}
		return actionDrop, nil
	case !errors.Is(err, storage.ErrKeyNotFound):
		return actionWait, fmt.Errorf("verifying an expiry entry: %w", err)
	}
	// Not in the queue: popped, acked or dead-lettered (gone), or leased (held under a lease
	// record and returned to this position later). With a lease out the two cannot be told
	// apart, so the entry waits; positions are never reused, so waiting is safe.
	leased, err := store.FieldHasLeases(snapshot, field)
	if err != nil {
		return actionWait, fmt.Errorf("verifying an expiry entry: %w", err)
	}
	if leased {
		return actionWait, nil
	}
	return actionDrop, nil
}

// settle acts on one due entry: it verifies the entry against the stored data, drops it when it
// is stale, and otherwise asks the remover. Nothing is deleted on the strength of the entry alone.
func (s *ExpirySweeper) settle(ctx context.Context, key []byte, entry storage.ExpiryEntry, pass *ExpiryPassResult) error {
	decided, err := s.verify(entry)
	if err != nil {
		return err
	}
	switch decided {
	case actionDrop:
		if s.afterVerify != nil {
			s.afterVerify()
		}
		return s.drop(key, entry, pass)
	case actionRemove:
		if entry.Kind == storage.ExpiryKindDocument {
			return s.remove(ctx, entry, s.remover.RemoveDocument, pass)
		}
		return s.remove(ctx, entry, s.remover.RemoveElement, pass)
	}
	pass.Skipped++
	return nil
}

func (s *ExpirySweeper) remove(ctx context.Context, entry storage.ExpiryEntry, send func(context.Context, storage.ExpiryEntry) (bool, error), pass *ExpiryPassResult) error {
	skipped, err := send(ctx, entry)
	switch {
	case err != nil:
		pass.Skipped++
		return fmt.Errorf("removing an expired item: %w", err)
	case skipped:
		pass.Skipped++
	default:
		pass.Removed++
		if entry.Kind == storage.ExpiryKindDocument {
			s.removedDocuments.Add(1)
		} else {
			s.removedElements.Add(1)
		}
	}
	return nil
}

// drop deletes one stale entry, after verifying it again under the pair lock: a delete followed
// by a re-insert of the same id at the same time sets the very same key for a live document, and a
// drop that trusted the first look would delete that document's only entry.
func (s *ExpirySweeper) drop(key []byte, entry storage.ExpiryEntry, pass *ExpiryPassResult) error {
	var dropErr error
	run := func() {
		decided, err := s.verify(entry)
		if err != nil {
			dropErr = err
			return
		}
		if decided != actionDrop {
			pass.Skipped++
			return
		}
		batch := s.kv.NewBatch()
		defer batch.Discard()
		if err := batch.Delete(key); err != nil {
			dropErr = fmt.Errorf("dropping a stale expiry entry: %w", err)
			return
		}
		if err := batch.Commit(false); err != nil {
			dropErr = fmt.Errorf("committing the drop of a stale expiry entry: %w", err)
			return
		}
		pass.Dropped++
		s.orphans.Add(1)
	}
	// A pair this node does not hold has no writer or apply to order against, so its orphan entry is
	// dropped without the lock: taking the lock would create the pair on a node that gave it up.
	if s.lock == nil || !s.lock(entry.Slot, entry.DBID, run) {
		run()
	}
	return dropErr
}

// ExpiryCounts is what a node reports of its expiry sweep to /metrics: three monotonic counters
// and one gauge, no label. Nothing is per bucket.
type ExpiryCounts struct {
	RemovedDocuments, RemovedElements, Orphans uint64
	// Backlog is the entries due now, counted up to the limit it was asked with.
	Backlog int
	// BacklogUnknown says the queue could not be read, so Backlog is not a count of anything.
	BacklogUnknown bool
}

// Counts returns the counters and the backlog gauge, walking at most backlogLimit due entries.
func (s *ExpirySweeper) Counts(backlogLimit int) (ExpiryCounts, error) {
	counts := ExpiryCounts{RemovedDocuments: s.removedDocuments.Load(), RemovedElements: s.removedElements.Load(), Orphans: s.orphans.Load()}
	due, _, err := s.dueEntries(nil, s.now(), backlogLimit)
	counts.Backlog = len(due)
	return counts, err
}

// bucketBacklogWalk bounds how many due entries one describe walks to count a bucket's.
const bucketBacklogWalk = 100_000

// BucketBacklog counts the entries due now (the owner's clock) that belong to one bucket, for
// describe. The queue is ordered by time and not by bucket, so this walks the due entries and
// matches the bucket; it stops at limit matches or after bucketBacklogWalk entries, and more says
// the true count is at least what it returned. It is on demand and per call, never a gauge.
func (s *ExpirySweeper) BucketBacklog(dbID, bucketID uint64, limit int) (count int, more bool, err error) {
	snapshot := s.kv.NewSnapshot()
	defer snapshot.Discard()
	prefix := storage.ExpiryPrefix()
	it := snapshot.NewIterator(storage.IterOptions{Lower: prefix, Upper: storage.PrefixEnd(prefix)})
	defer it.Close()
	now, walked := s.now(), 0
	for it.Seek(prefix); it.Valid(); it.Next() {
		entry, err := storage.ParseExpiryKey(it.Key())
		if err != nil {
			return count, false, fmt.Errorf("a key in the expiry namespace does not parse: %w", err)
		}
		if entry.AtMillis > now {
			break
		}
		if walked++; walked > bucketBacklogWalk {
			return count, true, nil
		}
		if entry.DBID == dbID && entry.BucketID == bucketID {
			if count++; count >= limit {
				return count, true, nil
			}
		}
	}
	return count, false, it.Err()
}

// heldPairLock runs fn under the pair lock of (slot, db) only when the node holds the pair, and
// says whether it did. It never creates a pair, so dropping an orphan entry cannot resurrect a
// slot the node gave up (a retire, a rebalance, a dropped database).
func heldPairLock(pairs *cluster.PairSet) func(slot uint16, db uint64, fn func()) bool {
	return func(slot uint16, db uint64, fn func()) bool {
		return pairs.WithHeld(slot, db, func(*cluster.Pair) { fn() })
	}
}
