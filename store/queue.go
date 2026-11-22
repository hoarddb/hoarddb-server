package store

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
)

// QueueMode is the order a queue bucket pops in.
type QueueMode int

const (
	// QueueFIFO pops the oldest entry first.
	QueueFIFO QueueMode = iota
	// QueueLIFO pops the newest entry first.
	QueueLIFO
)

// QueueStore is a fifo or lifo bucket on the node's shared store (shared-storage
// spec §3.4): an entry is the key [s][id][0x04][position : 8], and the cursors
// head, tail and next live in the bucket's state document beside its count, so
// every push and pop writes the entry and its cursors in one batch.
//
// A pop seeks to its cursor — [s][id][0x04][head] for fifo, a reverse seek from
// [s][id][0x04][tail] for lifo — and never to the namespace start, which would
// walk every delete marker a pop-heavy queue has left since the last
// compaction (the tombstone wall, T-06). A deletion at or after head that
// leaves no live key before it advances head past the deleted run in the same
// batch, or the next pop seeks onto that run (the head rule, T-29).
//
// Every mutation goes through Apply with Keyed false: a position is assigned at
// apply time in sequence order (spec §4.3), so a failed queue apply shifts every
// later one and is never applied beneath the mark (spec §4.4, SA-27). owed is
// the bucket's failed-apply carry, which the caller owns: it outlives the
// engine (Carry).
//
// A verb added to this engine later must call ReclaimVerbHook in the same
// change (build doc §1 trap 5).
type QueueStore struct {
	owed       *OwedApplies
	kv         storage.KV
	slot       uint16
	databaseID uint64
	id         uint64
	mode       QueueMode
	// entry, when set, replaces the bucket's 0x04 prefix: the queue is one
	// document's store field, whose elements sit under its own store prefix.
	entry []byte
}

// NewQueueStore returns the queue store of bucket id in slot, which must be the
// bucket's recorded slot, carrying its failed applies in owed. It reads
// nothing.
func NewQueueStore(kv storage.KV, ref storage.BucketRef, owed *OwedApplies, mode QueueMode) *QueueStore {
	return &QueueStore{owed: owed, kv: kv, slot: ref.Slot, databaseID: ref.DatabaseID, id: ref.BucketID, mode: mode}
}

// Carry is the bucket's failed-apply carry the store was built with.
func (q *QueueStore) Carry() *OwedApplies { return q.owed }

func (q *QueueStore) entryPrefix() []byte {
	if q.entry != nil {
		return q.entry
	}
	return storage.Key(q.slot, q.databaseID, q.id, storage.NSQueue)
}

func (q *QueueStore) positionKey(position uint64) []byte {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], position)
	return append(append([]byte(nil), q.entryPrefix()...), encoded[:]...)
}

func positionOf(key []byte) uint64 {
	return binary.BigEndian.Uint64(key[len(key)-8:])
}

func (q *QueueStore) apply(info ApplyInfo, op func(storage.KVSnapshot, *BucketState, storage.KVBatch) error) error {
	_, err := Apply(ApplyRequest{KV: q.kv, Slot: q.slot, DatabaseID: q.databaseID, ID: q.id, Keyed: false, Info: info, Owed: q.owed, Op: op})
	return err
}

// fromHead is an iterator over the queue's entries positioned at the first
// live entry at or after head, walking towards the tail.
func (q *QueueStore) fromHead(snap storage.KVSnapshot, head uint64) storage.KVIterator {
	prefix := q.entryPrefix()
	it := snap.NewIterator(storage.IterOptions{Lower: prefix, Upper: prefixEnd(prefix)})
	it.Seek(q.positionKey(head))
	return it
}

// fromTail is an iterator positioned at the last live entry at or before tail,
// walking towards the head.
func (q *QueueStore) fromTail(snap storage.KVSnapshot, tail uint64) storage.KVIterator {
	prefix := q.entryPrefix()
	it := snap.NewIterator(storage.IterOptions{Lower: prefix, Upper: prefixEnd(prefix), Reverse: true})
	it.Seek(q.positionKey(tail))
	return it
}

// fromCursor is the iterator a pop or a peek reads: from head for fifo, from
// tail for lifo.
func (q *QueueStore) fromCursor(snap storage.KVSnapshot, st BucketState) storage.KVIterator {
	if q.mode == QueueLIFO {
		return q.fromTail(snap, st.Tail)
	}
	return q.fromHead(snap, st.Head)
}

// Push appends entry and returns the position it took.
func (q *QueueStore) Push(info ApplyInfo, entry []byte) (uint64, error) {
	callReclaimVerbHook("queue push")
	var position uint64
	if err := q.apply(info, func(_ storage.KVSnapshot, st *BucketState, batch storage.KVBatch) error {
		position = st.Next
		return q.pushEntries(st, batch, [][]byte{entry})
	}); err != nil {
		return 0, fmt.Errorf("pushing a queue entry: %w", err)
	}
	return position, nil
}

// PushBatch appends every entry in one batch.
func (q *QueueStore) PushBatch(info ApplyInfo, entries [][]byte) error {
	callReclaimVerbHook("queue push")
	if err := q.apply(info, func(_ storage.KVSnapshot, st *BucketState, batch storage.KVBatch) error {
		return q.pushEntries(st, batch, entries)
	}); err != nil {
		return fmt.Errorf("pushing %d queue entries: %w", len(entries), err)
	}
	return nil
}

// pushEntries writes each entry at next. The cursors merge forward (spec §4.4
// rule 3) rather than being assigned, so no apply can move them backwards.
func (q *QueueStore) pushEntries(st *BucketState, batch storage.KVBatch, entries [][]byte) error {
	for _, entry := range entries {
		position := st.Next
		key := q.positionKey(position)
		if err := batch.Set(key, entry); err != nil {
			return fmt.Errorf("writing a queue entry of %d bytes at position %d: %w", len(entry), position, err)
		}
		if q.mode == QueueLIFO {
			// Before Tail moves: the gap it leaves is what the call looks for.
			st.pushedLifo(position)
		}
		st.Next = max(st.Next, position+1)
		st.Tail = max(st.Tail, position)
		st.Entries++
		st.LogicalBytes += int64(len(key) + len(entry))
	}
	return nil
}

// Pop removes and returns the entry at the cursor — the oldest for fifo, the
// newest for lifo — with its cursor move in one batch. It returns nil, nil for
// an empty queue.
func (q *QueueStore) Pop(info ApplyInfo) ([]byte, error) {
	var popped []byte
	if _, err := q.PopN(info, 1, 0, func(entry []byte) error {
		// Value returns a copy (storage.KVIterator.Value), so the entry is the
		// caller's to keep.
		popped = entry
		return nil
	}); err != nil {
		return nil, err
	}
	return popped, nil
}

// PopN removes up to n entries in pop order, from the cursor, in one batch
// with the cursor move and the state document. It calls visit with each
// entry before that entry's delete is added to the batch, and stops at the
// first error visit returns: the operation then returns that error, nothing
// is committed, and the store is as it was. It reads no value past the n-th.
// It returns how many it removed. now is the stamped lease time: with it the
// pop first returns the bucket's expired leases (claim-ack-spec §4.10); 0, an
// entry logged before claim existed, skips that step.
func (q *QueueStore) PopN(info ApplyInfo, n int, now int64, visit func(entry []byte) error) (PopResult, error) {
	callReclaimVerbHook("queue pop")
	result, err := popN(q, info, n, now, visit)
	if err != nil {
		if n == 1 {
			// A plain pop() reaches the user in a failure message, and the user
			// gave no limit.
			return PopResult{}, fmt.Errorf("popping a queue entry: %w", err)
		}
		return PopResult{}, fmt.Errorf("popping queue entries (limit %d): %w", n, err)
	}
	return result, nil
}

// walkTake is the one walk of pop order that takes entries off the queue, inside
// an operation: up to w.n entries from the cursor, each handed to w.visit and
// then to w.take. take is the per-entry action (pop's adds the delete), so a
// verb that moves entries rather than deleting them shares this walk. The
// cursor moves per entry in walk order, as k single pops move it. With
// allowStop set, a visit that returns ErrStopShort ends the walk before that
// entry's take and keeps what was taken; without it the same error aborts like
// any other. The cursor-lost rule compares with Entries as the walk starts,
// after the return step that ran before it in the same operation.
func (q *QueueStore) walkTake(scope opScope, w takeWalk) (int, error) {
	st := scope.st
	counted := st.Entries
	removed := 0
	stopped := false
	it := q.walkSource(scope, w)
	defer it.Close()
	for removed < w.n && it.Valid() {
		key := append([]byte(nil), it.Key()...)
		entry, err := it.Value()
		if err != nil {
			return 0, fmt.Errorf("reading the queue entry at position %d: %w", positionOf(key), err)
		}
		if err := w.visit(key, entry); err != nil {
			if w.allowStop && errors.Is(err, ErrStopShort) {
				// Before take: the entry stays, and so does everything the
				// walk took before it.
				stopped = true
				break
			}
			return 0, err
		}
		if err := w.take(st, scope.batch, key, entry); err != nil {
			return 0, err
		}
		q.moveCursorPast(st, positionOf(key))
		jumped := st.tookLive(q.mode, positionOf(key))
		removed++
		// Never step past the n-th entry: the step itself walks any delete
		// markers beyond it, which a lifo keeps below its cursor (BUG-0580).
		switch {
		case removed >= w.n:
		case jumped:
			// The run is taken and the cursor is back at ResumeAt. Stepping on
			// would walk every delete marker between them, so seek again; the
			// snapshot still holds the keys this apply took, but the jump put
			// the cursor on the far side of all of them.
			it.Seek(q.cursorKey(*st))
		default:
			it.Next()
		}
	}
	if err := it.Err(); err != nil {
		return 0, fmt.Errorf("walking the queue from its cursor: %w", err)
	}
	if !stopped && int64(removed) < min(int64(w.n), counted) {
		return 0, fmt.Errorf("%w: queue bucket %d counts %d entries and holds %d at its cursor", ErrQueueCursorLost, q.id, counted, removed)
	}
	return removed, nil
}

// walkSource is what walkTake reads: the snapshot from the cursor, or, when the
// apply wrote entries back, the snapshot merged with the overlay
// (claim-ack-spec §4.3). The snapshot side starts at the cursor as loaded, not
// as the return step lowered it: below the loaded cursor the snapshot holds no
// live key, only the markers of entries taken since. While the loaded cursor
// was armed, the snapshot side seeks to ResumeAt once it has yielded the
// loaded ResumeBelow run keys (A-3).
func (q *QueueStore) walkSource(scope opScope, w takeWalk) takeSource {
	if w.over == nil || w.over.empty() {
		return q.fromCursor(scope.snap, *scope.st)
	}
	loaded := w.loaded
	run := snapshotRun{left: loaded.ResumeBelow, resume: q.positionKey(loaded.ResumeAt)}
	if q.mode == QueueLIFO {
		run.onRun = func(key []byte) bool { return positionOf(key) > loaded.ResumeAt }
		return newMergedWalk(q.fromTail(scope.snap, loaded.Tail), run, w.over, true, q.positionKey(scope.st.Tail))
	}
	run.onRun = func(key []byte) bool { return positionOf(key) < loaded.ResumeAt }
	return newMergedWalk(q.fromHead(scope.snap, loaded.Head), run, w.over, false, q.positionKey(scope.st.Head))
}

// cursorKey is the key a walk from the cursor seeks to: Head for fifo, Tail
// for lifo.
func (q *QueueStore) cursorKey(st BucketState) []byte {
	if q.mode == QueueLIFO {
		return q.positionKey(st.Tail)
	}
	return q.positionKey(st.Head)
}

func (q *QueueStore) bucketKey() (uint16, uint64, uint64) { return q.slot, q.databaseID, q.id }

func (q *QueueStore) deliveryLayout() claimLayout { return claimLayoutQueue }

func (q *QueueStore) removeEntry(st *BucketState, batch storage.KVBatch, key, entry []byte) error {
	return removeQueueEntry(st, batch, key, entry)
}

func (q *QueueStore) wroteBack(st *BucketState, origin []byte) {
	st.writtenBack(q.mode, binary.BigEndian.Uint64(origin))
}

// ClaimN moves up to args.N entries, in pop order, under a new lease in one
// apply (claim-ack-spec §4.9). visit is called with each entry before its move
// is added to the batch, as PopN's visitor is. A visitor that returns
// ErrStopShort ends the walk and keeps what was taken; any other visitor error
// aborts the apply with nothing moved. A first entry over the hard cap is
// named in the Claim, with a nil error, and nothing is taken.
func (q *QueueStore) ClaimN(info ApplyInfo, args ClaimArgs, visit func(entry []byte) error) (Claim, error) {
	callReclaimVerbHook("queue claim")
	claim, err := claimN(q, info, args, visit)
	if err != nil {
		return Claim{}, fmt.Errorf("claiming queue entries (limit %d): %w", args.N, err)
	}
	return claim, nil
}

// Ack acknowledges the named entries of a lease, or with no item ids all of
// them (claim-ack-spec §4.11). An unknown or expired lease is a result, not an
// error.
func (q *QueueStore) Ack(info ApplyInfo, args LeaseArgs, itemIDs [][]byte) (LeaseResult, error) {
	callReclaimVerbHook("queue ack")
	result, err := ack(q, info, args, itemIDs)
	if err != nil {
		return LeaseResult{}, fmt.Errorf("acknowledging lease %x: %w", args.LeaseID, err)
	}
	return result, nil
}

// Release writes every entry of a live lease back to the queue by the return
// rule, which counts toward MaxDeliveries, and drops the lease
// (claim-ack-spec §4.11).
func (q *QueueStore) Release(info ApplyInfo, args LeaseArgs) (LeaseResult, error) {
	callReclaimVerbHook("queue release")
	result, err := release(q, info, args)
	if err != nil {
		return LeaseResult{}, fmt.Errorf("releasing lease %x: %w", args.LeaseID, err)
	}
	return result, nil
}

// Extend moves a live lease's deadline to the effective now plus leaseMs.
func (q *QueueStore) Extend(info ApplyInfo, args LeaseArgs, leaseMs int64) (LeaseResult, error) {
	callReclaimVerbHook("queue extend")
	result, err := extend(q, info, args, leaseMs)
	if err != nil {
		return LeaseResult{}, fmt.Errorf("extending lease %x: %w", args.LeaseID, err)
	}
	return result, nil
}

// DeadList lists dead entries from after (exclusive) in key order, oldest first, from
// one snapshot with no apply and no lock (claim-ack-spec §4.12). It stops at
// limit, or when visit returns ErrStopShort, and answers the last id listed when
// more remain. stoppedShort is true when visit ended the list; a nil next
// without it is a complete list (see deadList).
func (q *QueueStore) DeadList(limit int, after []byte, visit func(DeadRecord) error) (next []byte, stoppedShort bool, err error) {
	callReclaimVerbHook("queue dead list")
	next, stoppedShort, err = deadList(q.kv.NewSnapshot(), storage.BucketRef{Slot: q.slot, DatabaseID: q.databaseID, BucketID: q.id}, limit, after, visit)
	if err != nil {
		return nil, false, fmt.Errorf("listing the dead entries of a queue: %w", err)
	}
	return next, stoppedShort, nil
}

// Requeue writes the selected dead entries back to their origin with their
// return count reset (claim-ack-spec §4.12).
func (q *QueueStore) Requeue(info ApplyInfo, sel DeadSelector) (DeadMove, error) {
	callReclaimVerbHook("queue requeue")
	move, err := deadMove(q, info, sel, true)
	if err != nil {
		return DeadMove{}, fmt.Errorf("requeueing dead queue entries: %w", err)
	}
	return move, nil
}

// Discard deletes the selected dead entries (claim-ack-spec §4.12).
func (q *QueueStore) Discard(info ApplyInfo, sel DeadSelector) (DeadMove, error) {
	callReclaimVerbHook("queue discard")
	move, err := deadMove(q, info, sel, false)
	if err != nil {
		return DeadMove{}, fmt.Errorf("discarding dead queue entries: %w", err)
	}
	return move, nil
}

// RestoreDead writes one dead entry from a dump (claim-ack-spec §4.12, §7.3),
// after validating it.
func (q *QueueStore) RestoreDead(info ApplyInfo, record DeadRecord) error {
	callReclaimVerbHook("queue restore dead")
	if err := restoreDead(q, info, record); err != nil {
		return fmt.Errorf("restoring a dead queue entry: %w", err)
	}
	return nil
}

// moveCursorPast is a pop's cursor move for the entry it took at position.
// A lifo leaves Tail where it is at position 0, so a walk that ends there
// leaves Tail where its last positive position put it.
func (q *QueueStore) moveCursorPast(st *BucketState, position uint64) {
	if q.mode == QueueLIFO {
		if position > 0 {
			st.Tail = position - 1
		}
		return
	}
	st.Head = max(st.Head, position+1)
}

// ErrQueueCursorLost is a queue whose state document counts entries its range
// does not hold at the cursor: the state and the range disagree, which no
// write of this engine leaves behind.
var ErrQueueCursorLost = errors.New("the queue's cursor points past its entries")

func removeQueueEntry(st *BucketState, batch storage.KVBatch, key, entry []byte) error {
	if err := batch.Delete(key); err != nil {
		return fmt.Errorf("deleting the queue entry at position %d: %w", positionOf(key), err)
	}
	st.Entries--
	st.LogicalBytes -= int64(len(key) + len(entry))
	return nil
}

// Peek returns the entry Pop would return, without removing it, or nil, nil
// for an empty queue.
func (q *QueueStore) Peek() ([]byte, error) {
	var first []byte
	if err := q.PeekN(1, func(entry []byte) error {
		// Value returns a copy (storage.KVIterator.Value), so the entry is the
		// caller's to keep.
		first = entry
		return nil
	}); err != nil {
		return nil, err
	}
	return first, nil
}

// PeekN calls visit with up to n entries in pop order, from the cursor,
// without writing, and stops at the first error visit returns. It reads no
// value past the n-th entry. A walk that ends without being stopped, having
// visited fewer than min(n, Entries) entries, is ErrQueueCursorLost, as a Pop
// of the same state is.
func (q *QueueStore) PeekN(n int, visit func(entry []byte) error) error {
	_, err := q.PeekCounts(n, visit)
	return err
}

// PeekCounts is PeekN, and answers the counts of the state document the walk
// read, so a peek reports its claimed and dead entries without a second read.
func (q *QueueStore) PeekCounts(n int, visit func(entry []byte) error) (DeliveryCounts, error) {
	callReclaimVerbHook("queue peek")
	visited := 0
	stopped := false
	counts, err := q.walkCounted(func(entry []byte) error {
		visited++
		if err := visit(entry); err != nil {
			stopped = true
			return err
		}
		if visited == n {
			stopped = true
			return errWalkDone
		}
		return nil
	})
	if err != nil {
		return DeliveryCounts{}, fmt.Errorf("peeking %d queue entries: %w", n, err)
	}
	if !stopped && int64(visited) < min(int64(n), counts.Entries) {
		return DeliveryCounts{}, fmt.Errorf("peeking %d queue entries: %w: queue bucket %d counts %d entries and holds %d", n, ErrQueueCursorLost, q.id, counts.Entries, visited)
	}
	return counts, nil
}

// errWalkDone ends a walk early; walk does not report it.
var errWalkDone = errors.New("walk done")

// walk visits every live entry in pop order from one snapshot: oldest first
// for fifo, newest first for lifo.
func (q *QueueStore) walk(visit func(entry []byte) error) error {
	_, err := q.walkCounted(visit)
	return err
}

// walkCounted is walk, and returns the counts of the state document it read the
// cursors from, so a caller can compare them with what the range held without a
// second snapshot.
func (q *QueueStore) walkCounted(visit func(entry []byte) error) (DeliveryCounts, error) {
	snap := q.kv.NewSnapshot()
	defer snap.Discard()
	st, err := LoadBucketState(snap, q.slot, q.databaseID, q.id)
	if err != nil {
		return DeliveryCounts{}, fmt.Errorf("reading the queue's cursors: %w", err)
	}
	counts, err := deliveryCountsOf(snap, q.slot, q.databaseID, q.id, st)
	if err != nil {
		return DeliveryCounts{}, err
	}
	it := q.fromCursor(snap, st)
	defer it.Close()
	for ; it.Valid(); it.Next() {
		entry, err := it.Value()
		if err != nil {
			return counts, fmt.Errorf("reading the queue entry at position %d: %w", positionOf(it.Key()), err)
		}
		if err := visit(entry); err != nil {
			if errors.Is(err, errWalkDone) {
				return counts, nil
			}
			return counts, err
		}
	}
	if err := it.Err(); err != nil {
		return counts, fmt.Errorf("walking the queue: %w", err)
	}
	return counts, nil
}

// Walk calls visit for each entry in pop order. It never consumes the queue.
func (q *QueueStore) Walk(visit func(entry []byte) error) error {
	return q.walk(visit)
}

// Len returns the number of entries, from the bucket's state document.
func (q *QueueStore) Len() (int64, error) {
	snap := q.kv.NewSnapshot()
	defer snap.Discard()
	st, err := LoadBucketState(snap, q.slot, q.databaseID, q.id)
	if err != nil {
		return 0, fmt.Errorf("reading the entry count: %w", err)
	}
	return st.Entries, nil
}

// DocCount reports how many entries the range holds, walked rather than read
// from the state document, because a dump's manifest must not trust a counter
// kept for something else. It counts what IterateDump emits as documents:
// available and claimed entries.
func (q *QueueStore) DocCount() (int64, error) {
	return dumpedDocuments(q)
}

// DeleteAtPosition removes the pos-th live entry counted from head in insertion
// order (0 = the oldest), for fifo and lifo alike (spec §3.3), by walking pos
// entries — O(pos). It carries no WAL sequence: no server path calls it.
func (q *QueueStore) DeleteAtPosition(pos int) error {
	callReclaimVerbHook("queue DeleteAtPosition")
	return q.deleteWhere(func(walked int, _ []byte) bool { return walked == pos },
		func(walked int) error { return fmt.Errorf("position %d out of range [0, %d)", pos, walked) })
}

// DeleteAtID removes the entry whose _id_ is entryID by walking from head until
// it matches — O(entries), because the key carries a position, never the id
// (spec §3.3). It carries no WAL sequence: no server path calls it.
func (q *QueueStore) DeleteAtID(entryID string) error {
	callReclaimVerbHook("queue DeleteAtID")
	return q.deleteWhere(func(_ int, entry []byte) bool {
		id, ok := bson.Raw(entry).Lookup("_id_").StringValueOK()
		return ok && id == entryID
	}, func(int) error { return fmt.Errorf("entry with ID %q not found", entryID) })
}

// deleteWhere deletes the first entry from head, in insertion order, that match
// accepts. A victim with no live entry before it at or after head moves head
// past the deleted run — to the next live entry, or to next when none is left —
// in the same batch (spec §3.4, the head rule).
func (q *QueueStore) deleteWhere(match func(walked int, entry []byte) bool, notFound func(walked int) error) error {
	var missing error
	err := q.apply(ApplyInfo{}, func(snap storage.KVSnapshot, st *BucketState, batch storage.KVBatch) error {
		it := q.fromHead(snap, st.Head)
		defer it.Close()
		walked := 0
		for ; it.Valid(); it.Next() {
			entry, err := it.Value()
			if err != nil {
				return fmt.Errorf("reading the queue entry at position %d: %w", positionOf(it.Key()), err)
			}
			if !match(walked, entry) {
				walked++
				continue
			}
			removedKey := append([]byte(nil), it.Key()...)
			if err := removeQueueEntry(st, batch, removedKey, entry); err != nil {
				return err
			}
			st.tookLive(q.mode, positionOf(removedKey))
			// A returned entry's 'c' record goes with it (claim/ack build
			// stop-and-ask 7): every remover of a 0x04 key keeps Returned the
			// count of 'c' records.
			if st.Returned > 0 {
				if _, err := dropStoredReturnCount(opScope{snap: snap, st: st, batch: batch}, returnCountKeyOf(removedKey)); err != nil {
					return err
				}
			}
			if walked == 0 {
				st.Head = st.Next
				if it.Next(); it.Valid() {
					st.Head = positionOf(it.Key())
				} else if err := it.Err(); err != nil {
					// Moving head to next would drop every entry past the damage.
					return fmt.Errorf("finding the queue's new head: %w", err)
				}
			}
			return nil
		}
		if err := it.Err(); err != nil {
			return fmt.Errorf("walking the queue: %w", err)
		}
		missing = notFound(walked)
		return missing
	})
	if missing != nil {
		return missing
	}
	if err != nil {
		return fmt.Errorf("deleting a queue entry: %w", err)
	}
	return nil
}
