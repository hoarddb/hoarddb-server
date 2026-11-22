package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/ids"
	"github.com/hoarddb/hoarddb-server/storage"
)

// HeapStore is a priority queue on the node's shared store: an element is the
// key [s][id][0x04][inverted priority : 8][entry id], so the store's own key
// order is highest priority first, and its count is the bucket's state
// document (shared-storage spec §3.3). There is no in-memory heap: Peek is a
// seek to the first key, Pop is that seek plus a delete in one batch, and
// opening a heap costs nothing instead of an O(entries) rebuild.
//
// Every mutation goes through Apply under the bucket mutex the caller holds,
// never beneath the mark on replay (Keyed false: a failed heap operation
// changes what every later one sees, spec §4.4, SA-27). owed is the bucket's
// failed-apply carry, which the caller owns: it outlives the engine (Carry).
//
// A verb added to this engine later must call ReclaimVerbHook in the same
// change (build doc §1 trap 5).
type HeapStore struct {
	owed       *OwedApplies
	kv         storage.KV
	slot       uint16
	databaseID uint64
	id         uint64
}

// HeapItem is one element of a batch push.
type HeapItem struct {
	Priority int64
	Value    interface{}
}

// NewHeapStore returns the heap store of bucket id in slot, which must be the
// bucket's recorded slot, carrying its failed applies in owed. It reads
// nothing.
func NewHeapStore(kv storage.KV, slot uint16, databaseID, id uint64, owed *OwedApplies) *HeapStore {
	return &HeapStore{owed: owed, kv: kv, slot: slot, databaseID: databaseID, id: id}
}

// Carry is the bucket's failed-apply carry the store was built with.
func (h *HeapStore) Carry() *OwedApplies { return h.owed }

// invertedPriority is the key form of a priority, highest first — spec §3.3
// rev 5.2.2, verbatim. The XOR with 1<<63 flips the sign bit, making
// two's-complement big-endian order match numeric order for all int64 values;
// the complement then reverses that order, so a forward seek from the start
// of [s][id][0x04] reaches the highest priority. Two earlier forms are struck
// in the spec with their arithmetic: uint64(math.MaxInt64-priority) ^ (1<<63)
// overflows for every negative priority, and uint64(priority) ^ (1<<63) alone
// pops the lowest first.
func invertedPriority(priority int64) uint64 {
	return ^(uint64(priority) ^ (1 << 63))
}

// priorityOf inverts invertedPriority over the whole int64 domain (spec §3.3).
func priorityOf(inverted uint64) int64 {
	return int64(^inverted ^ (1 << 63))
}

func (h *HeapStore) elementPrefix() []byte {
	return storage.Key(h.slot, h.databaseID, h.id, storage.NSQueue)
}

func (h *HeapStore) makeKey(entryID string, priority int64) []byte {
	var inverted [8]byte
	binary.BigEndian.PutUint64(inverted[:], invertedPriority(priority))
	return storage.Key(h.slot, h.databaseID, h.id, storage.NSQueue, inverted[:], []byte(entryID))
}

func (h *HeapStore) apply(info ApplyInfo, op func(storage.KVSnapshot, *BucketState, storage.KVBatch) error) error {
	_, err := Apply(ApplyRequest{KV: h.kv, Slot: h.slot, DatabaseID: h.databaseID, ID: h.id, Keyed: false, Info: info, Owed: h.owed, Op: op})
	return err
}

// entryIDFor returns the id a heap entry is stored under: the value's own _id_
// when it has one, otherwise a fresh UUID.
//
// Replay must reproduce the same entry, and the entry id is part of the key —
// a replayed push that minted a new id would add a second entry under a new
// key instead of overwriting the one it already wrote. Nothing recorded in the
// doc means the caller never named an entry, which is the ordinary push.
func entryIDFor(value interface{}) (string, error) {
	if doc, ok := value.(map[string]interface{}); ok {
		if id, ok := doc["_id_"].(string); ok && id != "" {
			return id, nil
		}
	}
	id, err := ids.NewDocumentID()
	if err != nil {
		return "", fmt.Errorf("generating entry id: %w", err)
	}
	return id, nil
}

// pendingPush is one element ready for the batch.
type pendingPush struct {
	key     []byte
	entryID string
	encoded []byte
}

func preparePush(h *HeapStore, priority int64, value interface{}) (pendingPush, error) {
	// The complement key has no overflow at math.MinInt64, but the value stays
	// refused as a reserved sentinel (build doc Step 11, rev 5.2.2): heaps
	// written before this layout never held it, and admitting it now would be
	// an API change no step asks for.
	if priority == math.MinInt64 {
		return pendingPush{}, fmt.Errorf("priority must be in range [%d, %d], got %d", math.MinInt64+1, math.MaxInt64, priority)
	}
	entryID, err := entryIDFor(value)
	if err != nil {
		return pendingPush{}, err
	}
	encoded, err := bson.Marshal(value)
	if err != nil {
		return pendingPush{}, fmt.Errorf("marshaling value: %w", err)
	}
	return pendingPush{key: h.makeKey(entryID, priority), entryID: entryID, encoded: encoded}, nil
}

// putElements adds pushes to batch. An element already stored under the same
// priority and id is overwritten, not added twice, so a replayed or
// re-delivered push does not grow the heap. An overwrite of an element that
// had been returned to the heap drops its 'c' record: the new element has
// never been delivered (claim-ack-spec §4.10). The record is looked up only
// while the bucket counts a returned element.
func putElements(snap storage.KVSnapshot, st *BucketState, batch storage.KVBatch, pushes []pendingPush) error {
	written := make(map[string][]byte, len(pushes))
	for _, push := range pushes {
		previous, seen := written[string(push.key)]
		if !seen {
			stored, err := snap.Get(push.key)
			switch {
			case errors.Is(err, storage.ErrKeyNotFound):
			case err != nil:
				return fmt.Errorf("reading heap entry %q: %w", push.entryID, err)
			default:
				previous, seen = stored, true
				if st.Returned > 0 {
					if _, err := dropStoredReturnCount(opScope{snap: snap, st: st, batch: batch}, returnCountKeyOf(push.key)); err != nil {
						return fmt.Errorf("pushing over heap entry %q: %w", push.entryID, err)
					}
				}
			}
		}
		if seen {
			st.LogicalBytes += int64(len(push.encoded) - len(previous))
		} else {
			st.Entries++
			st.LogicalBytes += int64(len(push.key) + len(push.encoded))
		}
		if err := batch.Set(push.key, push.encoded); err != nil {
			return fmt.Errorf("putting heap entry %q: %w", push.entryID, err)
		}
		written[string(push.key)] = push.encoded
	}
	return nil
}

// PushItem adds a value with the given priority and returns the entry id it
// was stored under. A value carrying an _id_ is stored under that id, and an
// element already present at the same priority is overwritten.
func (h *HeapStore) PushItem(info ApplyInfo, priority int64, value interface{}) (string, error) {
	callReclaimVerbHook("heap push")
	push, err := preparePush(h, priority, value)
	if err != nil {
		return "", err
	}
	if err := h.apply(info, func(snap storage.KVSnapshot, st *BucketState, batch storage.KVBatch) error {
		return putElements(snap, st, batch, []pendingPush{push})
	}); err != nil {
		return "", fmt.Errorf("pushing heap entry: %w", err)
	}
	return push.entryID, nil
}

// PushItemBatch adds every item in one batch and returns their entry ids.
func (h *HeapStore) PushItemBatch(info ApplyInfo, items []HeapItem) ([]string, error) {
	callReclaimVerbHook("heap push")
	pushes := make([]pendingPush, 0, len(items))
	entryIDs := make([]string, 0, len(items))
	for _, item := range items {
		push, err := preparePush(h, item.Priority, item.Value)
		if err != nil {
			return nil, err
		}
		pushes = append(pushes, push)
		entryIDs = append(entryIDs, push.entryID)
	}
	if err := h.apply(info, func(snap storage.KVSnapshot, st *BucketState, batch storage.KVBatch) error {
		return putElements(snap, st, batch, pushes)
	}); err != nil {
		return nil, fmt.Errorf("pushing heap entries: %w", err)
	}
	return entryIDs, nil
}

// firstElement returns the highest-priority element's key and value, or nil
// keys for an empty heap: a seek to the first key of [s][id][0x04].
func (h *HeapStore) firstElement(snap storage.KVSnapshot) ([]byte, []byte, error) {
	prefix := h.elementPrefix()
	it := snap.NewIterator(storage.IterOptions{Lower: prefix, Upper: prefixEnd(prefix)})
	defer it.Close()
	it.Seek(nil)
	if !it.Valid() {
		if err := it.Err(); err != nil {
			return nil, nil, fmt.Errorf("seeking the first heap entry: %w", err)
		}
		return nil, nil, nil
	}
	value, err := it.Value()
	if err != nil {
		return nil, nil, fmt.Errorf("reading the first heap entry: %w", err)
	}
	return append([]byte(nil), it.Key()...), value, nil
}

// PopItem removes and returns the BSON-encoded value of the highest-priority
// entry — the seek and the delete in one batch with the state. Returns nil,
// nil for an empty heap. A heap whose state counts entries its range does not
// hold is ErrQueueCursorLost, as its peek is, never empty (pop-n spec A-4).
func (h *HeapStore) PopItem(info ApplyInfo) ([]byte, error) {
	var popped []byte
	if _, err := h.PopN(info, 1, 0, func(entry []byte) error {
		// Value returns a copy (storage.KVIterator.Value), so the entry is the
		// caller's to keep.
		popped = entry
		return nil
	}); err != nil {
		return nil, err
	}
	return popped, nil
}

// PopN is QueueStore.PopN for a heap: highest priority first, ties in
// entry-id order (the order PeekN and PopItem take them in).
func (h *HeapStore) PopN(info ApplyInfo, n int, now int64, visit func(entry []byte) error) (PopResult, error) {
	callReclaimVerbHook("heap pop")
	result, err := popN(h, info, n, now, visit)
	if err != nil {
		if n == 1 {
			// A plain pop() reaches the user in a failure message, and the user
			// gave no limit.
			return PopResult{}, fmt.Errorf("popping a heap entry: %w", err)
		}
		return PopResult{}, fmt.Errorf("popping heap entries (limit %d): %w", n, err)
	}
	return result, nil
}

// walkTake is QueueStore.walkTake for a heap: the forward walk of the element
// prefix PeekN reads, merged with the elements this apply wrote back, with no
// cursor to move.
func (h *HeapStore) walkTake(scope opScope, w takeWalk) (int, error) {
	counted := scope.st.Entries
	removed := 0
	stopped := false
	prefix := h.elementPrefix()
	snapshotSide := scope.snap.NewIterator(storage.IterOptions{Lower: prefix, Upper: prefixEnd(prefix)})
	var it takeSource = snapshotSide
	if w.over != nil && !w.over.empty() {
		it = newMergedWalk(snapshotSide, snapshotRun{}, w.over, false, nil)
	}
	defer it.Close()
	for it.Seek(nil); removed < w.n && it.Valid(); {
		key := append([]byte(nil), it.Key()...)
		entry, err := it.Value()
		if err != nil {
			return 0, fmt.Errorf("reading a heap entry: %w", err)
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
		if err := w.take(scope.st, scope.batch, key, entry); err != nil {
			return 0, err
		}
		removed++
		// Never step past the n-th entry: the step walks the delete
		// markers of entries popped since the last compaction.
		if removed < w.n {
			it.Next()
		}
	}
	if err := it.Err(); err != nil {
		return 0, fmt.Errorf("walking the heap: %w", err)
	}
	if !stopped && int64(removed) < min(int64(w.n), counted) {
		return 0, fmt.Errorf("%w: heap bucket %d counts %d entries and holds %d", ErrQueueCursorLost, h.id, counted, removed)
	}
	return removed, nil
}

func (h *HeapStore) bucketKey() (uint16, uint64, uint64) { return h.slot, h.databaseID, h.id }

func (h *HeapStore) deliveryLayout() claimLayout { return claimLayoutHeap }

func (h *HeapStore) removeEntry(st *BucketState, batch storage.KVBatch, key, entry []byte) error {
	return removeElement(st, batch, key, entry)
}

// wroteBack is nothing for a heap: it has no cursor, and an element written
// back is found by its key's place in the order.
func (h *HeapStore) wroteBack(*BucketState, []byte) {}

// ClaimN is QueueStore.ClaimN for a heap: highest priority first, ties in
// entry-id order.
func (h *HeapStore) ClaimN(info ApplyInfo, args ClaimArgs, visit func(entry []byte) error) (Claim, error) {
	callReclaimVerbHook("heap claim")
	claim, err := claimN(h, info, args, visit)
	if err != nil {
		return Claim{}, fmt.Errorf("claiming heap entries (limit %d): %w", args.N, err)
	}
	return claim, nil
}

// Ack is QueueStore.Ack for a heap.
func (h *HeapStore) Ack(info ApplyInfo, args LeaseArgs, itemIDs [][]byte) (LeaseResult, error) {
	callReclaimVerbHook("heap ack")
	result, err := ack(h, info, args, itemIDs)
	if err != nil {
		return LeaseResult{}, fmt.Errorf("acknowledging lease %x: %w", args.LeaseID, err)
	}
	return result, nil
}

// Release is QueueStore.Release for a heap: an entry whose key a pushed twin
// now holds is dead-lettered collided rather than written over it.
func (h *HeapStore) Release(info ApplyInfo, args LeaseArgs) (LeaseResult, error) {
	callReclaimVerbHook("heap release")
	result, err := release(h, info, args)
	if err != nil {
		return LeaseResult{}, fmt.Errorf("releasing lease %x: %w", args.LeaseID, err)
	}
	return result, nil
}

// Extend is QueueStore.Extend for a heap.
func (h *HeapStore) Extend(info ApplyInfo, args LeaseArgs, leaseMs int64) (LeaseResult, error) {
	callReclaimVerbHook("heap extend")
	result, err := extend(h, info, args, leaseMs)
	if err != nil {
		return LeaseResult{}, fmt.Errorf("extending lease %x: %w", args.LeaseID, err)
	}
	return result, nil
}

// DeadList lists dead entries from after (exclusive) in key order, highest priority first, in dead order within one key, from
// one snapshot with no apply and no lock (claim-ack-spec §4.12). It stops at
// limit, or when visit returns ErrStopShort, and answers the last id listed when
// more remain. stoppedShort is true when visit ended the list; a nil next
// without it is a complete list (see deadList).
func (h *HeapStore) DeadList(limit int, after []byte, visit func(DeadRecord) error) (next []byte, stoppedShort bool, err error) {
	callReclaimVerbHook("heap dead list")
	next, stoppedShort, err = deadList(h.kv.NewSnapshot(), storage.BucketRef{Slot: h.slot, DatabaseID: h.databaseID, BucketID: h.id}, limit, after, visit)
	if err != nil {
		return nil, false, fmt.Errorf("listing the dead entries of a heap: %w", err)
	}
	return next, stoppedShort, nil
}

// Requeue writes the selected dead entries back to their origin with their
// return count reset (claim-ack-spec §4.12). A heap key that a live element holds is
// refused for that entry, by name.
func (h *HeapStore) Requeue(info ApplyInfo, sel DeadSelector) (DeadMove, error) {
	callReclaimVerbHook("heap requeue")
	move, err := deadMove(h, info, sel, true)
	if err != nil {
		return DeadMove{}, fmt.Errorf("requeueing dead heap entries: %w", err)
	}
	return move, nil
}

// Discard deletes the selected dead entries (claim-ack-spec §4.12).
func (h *HeapStore) Discard(info ApplyInfo, sel DeadSelector) (DeadMove, error) {
	callReclaimVerbHook("heap discard")
	move, err := deadMove(h, info, sel, false)
	if err != nil {
		return DeadMove{}, fmt.Errorf("discarding dead heap entries: %w", err)
	}
	return move, nil
}

// RestoreDead writes one dead entry from a dump (claim-ack-spec §4.12, §7.3),
// after validating it.
func (h *HeapStore) RestoreDead(info ApplyInfo, record DeadRecord) error {
	callReclaimVerbHook("heap restore dead")
	if err := restoreDead(h, info, record); err != nil {
		return fmt.Errorf("restoring a dead heap entry: %w", err)
	}
	return nil
}

// removeElement adds an element's delete to batch and updates st.
func removeElement(st *BucketState, batch storage.KVBatch, key, value []byte) error {
	if err := batch.Delete(key); err != nil {
		return fmt.Errorf("deleting a heap entry: %w", err)
	}
	st.Entries--
	st.LogicalBytes -= int64(len(key) + len(value))
	return nil
}

// PeekItem returns the BSON-encoded value of the highest-priority entry
// without removing it, or nil, nil for an empty heap: one seek, O(log n).
func (h *HeapStore) PeekItem() ([]byte, error) {
	callReclaimVerbHook("heap peek")
	snap := h.kv.NewSnapshot()
	defer snap.Discard()
	_, value, err := h.firstElement(snap)
	return value, err
}

// PeekN calls visit with up to n entries' BSON values, highest priority first
// (ties in entry-id order, the order PopItem takes them in), from one snapshot
// and without writing. It stops at the first error visit returns and reads no
// value past the n-th. A walk that ends without being stopped, having visited
// fewer than min(n, Entries) entries, is ErrQueueCursorLost, the rule the
// queue engines follow.
func (h *HeapStore) PeekN(n int, visit func(entry []byte) error) error {
	_, err := h.PeekCounts(n, visit)
	return err
}

// PeekCounts is PeekN, and answers the counts of the state document the walk
// read, so a peek reports its claimed and dead entries without a second read.
func (h *HeapStore) PeekCounts(n int, visit func(entry []byte) error) (DeliveryCounts, error) {
	callReclaimVerbHook("heap peek")
	snap := h.kv.NewSnapshot()
	defer snap.Discard()
	st, err := LoadBucketState(snap, h.slot, h.databaseID, h.id)
	if err != nil {
		return DeliveryCounts{}, fmt.Errorf("peeking %d heap entries: reading the entry count: %w", n, err)
	}
	counts, err := deliveryCountsOf(snap, h.slot, h.databaseID, h.id, st)
	if err != nil {
		return DeliveryCounts{}, fmt.Errorf("peeking %d heap entries: %w", n, err)
	}
	prefix := h.elementPrefix()
	it := snap.NewIterator(storage.IterOptions{Lower: prefix, Upper: prefixEnd(prefix)})
	defer it.Close()
	visited := 0
	for it.Seek(nil); it.Valid(); it.Next() {
		entry, err := it.Value()
		if err != nil {
			return DeliveryCounts{}, fmt.Errorf("peeking %d heap entries: reading a heap entry: %w", n, err)
		}
		visited++
		if err := visit(entry); err != nil {
			return DeliveryCounts{}, fmt.Errorf("peeking %d heap entries: %w", n, err)
		}
		if visited == n {
			return counts, nil
		}
	}
	if err := it.Err(); err != nil {
		return DeliveryCounts{}, fmt.Errorf("peeking %d heap entries: walking the heap: %w", n, err)
	}
	if int64(visited) < min(int64(n), st.Entries) {
		return DeliveryCounts{}, fmt.Errorf("peeking %d heap entries: %w: heap bucket %d counts %d entries and holds %d", n, ErrQueueCursorLost, h.id, st.Entries, visited)
	}
	return counts, nil
}

// Walk calls visit for each entry's BSON value in highest-priority-first
// order: one snapshot, one walk of the key order. It never consumes the heap.
func (h *HeapStore) Walk(visit func(value []byte) error) error {
	snap := h.kv.NewSnapshot()
	defer snap.Discard()
	prefix := h.elementPrefix()
	it := snap.NewIterator(storage.IterOptions{Lower: prefix, Upper: prefixEnd(prefix)})
	defer it.Close()
	for it.Seek(nil); it.Valid(); it.Next() {
		value, err := it.Value()
		if err != nil {
			return fmt.Errorf("reading a heap entry: %w", err)
		}
		if err := visit(value); err != nil {
			return err
		}
	}
	if err := it.Err(); err != nil {
		return fmt.Errorf("walking the heap: %w", err)
	}
	return nil
}

// Len returns the number of entries, from the bucket's state document.
func (h *HeapStore) Len() (int64, error) {
	snap := h.kv.NewSnapshot()
	defer snap.Discard()
	st, err := LoadBucketState(snap, h.slot, h.databaseID, h.id)
	if err != nil {
		return 0, fmt.Errorf("reading the entry count: %w", err)
	}
	return st.Entries, nil
}

// DocCount reports how many entries this store holds, walked from storage
// rather than read from the state document, because a dump's manifest must
// not trust a counter kept for something else. It counts what IterateDump emits
// as documents: available and claimed elements.
func (h *HeapStore) DocCount() (int64, error) {
	return dumpedDocuments(h)
}

// heapEntryDocument is the document {_id_, priority, value} of the element
// whose key suffix is [inverted priority : 8][entry id]: the priority lives
// only in the key, so a dump, and a dead entry, carry it re-attached. value
// embeds the stored bytes unchanged (as bson.Raw).
func heapEntryDocument(suffix, value []byte) ([]byte, error) {
	entryID := string(suffix[8:])
	doc, err := bson.Marshal(struct {
		ID       string   `bson:"_id_"`
		Priority int64    `bson:"priority"`
		Value    bson.Raw `bson:"value"`
	}{ID: entryID, Priority: priorityOf(binary.BigEndian.Uint64(suffix[:8])), Value: bson.Raw(value)})
	if err != nil {
		return nil, fmt.Errorf("marshaling the document of heap entry %q: %w", entryID, err)
	}
	return doc, nil
}

// DeleteAtPosition removes the pos-th entry in priority order (0 = highest)
// by walking pos live entries from the first key — O(pos) (spec §3.3). It
// carries no WAL sequence: no server path calls it.
func (h *HeapStore) DeleteAtPosition(pos int) error {
	callReclaimVerbHook("heap DeleteAtPosition")
	return h.deleteWhere(func(walked int, _ []byte) bool { return walked == pos },
		func(walked int) error { return fmt.Errorf("position %d out of range [0, %d)", pos, walked) })
}

// DeleteAtID removes the entry with the given entry id by walking the heap's
// range until the key's entry id matches — O(entries), because the key leads
// with the priority, never the id (spec §3.3). It carries no WAL sequence: no
// server path calls it.
func (h *HeapStore) DeleteAtID(entryID string) error {
	callReclaimVerbHook("heap DeleteAtID")
	suffix := []byte(entryID)
	prefixLen := len(h.elementPrefix()) + 8
	return h.deleteWhere(func(_ int, key []byte) bool { return bytes.Equal(key[prefixLen:], suffix) },
		func(int) error { return fmt.Errorf("entry with ID %q not found", entryID) })
}

// deleteWhere deletes the first entry, in key order, that match accepts.
func (h *HeapStore) deleteWhere(match func(walked int, key []byte) bool, notFound func(walked int) error) error {
	var missing error
	err := h.apply(ApplyInfo{}, func(snap storage.KVSnapshot, st *BucketState, batch storage.KVBatch) error {
		prefix := h.elementPrefix()
		it := snap.NewIterator(storage.IterOptions{Lower: prefix, Upper: prefixEnd(prefix)})
		defer it.Close()
		walked := 0
		for it.Seek(nil); it.Valid(); it.Next() {
			if match(walked, it.Key()) {
				value, err := it.Value()
				if err != nil {
					return fmt.Errorf("reading a heap entry: %w", err)
				}
				removedKey := append([]byte(nil), it.Key()...)
				if err := removeElement(st, batch, removedKey, value); err != nil {
					return err
				}
				// A returned element's 'c' record goes with it (claim/ack
				// build stop-and-ask 7).
				if st.Returned > 0 {
					_, err = dropStoredReturnCount(opScope{snap: snap, st: st, batch: batch}, returnCountKeyOf(removedKey))
				}
				return err
			}
			walked++
		}
		if err := it.Err(); err != nil {
			return fmt.Errorf("walking the heap: %w", err)
		}
		missing = notFound(walked)
		return missing
	})
	if missing != nil {
		return missing
	}
	if err != nil {
		return fmt.Errorf("deleting heap entry: %w", err)
	}
	return nil
}
