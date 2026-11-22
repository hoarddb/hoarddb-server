package store

import (
	"bytes"
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/hoarddb/hoarddb-server/storage"
)

// DumpEnumerator is what a dump needs of any bucket: its documents, and for a
// queue or a heap the dead entries beside them, from the same snapshot
// (claim-ack-spec §7.1, §7.2). IterateDump calls documents for every available
// and every claimed entry in pop order. It calls dead for each heap twin of a
// claimed entry, which restore-by-push would otherwise overwrite, and then for
// every dead entry in key order. Engines with no dead entries are wrapped by
// DocumentsOnly. A queue and a heap have no IterateDocuments of their own: a
// walk that could not hand a dead entry to anyone would drop it silently.
type DumpEnumerator interface {
	IterateDump(documents func(key, value []byte) error, dead func(DeadRecord) error) error
	DocCount() (int64, error)
}

var (
	_ DumpEnumerator = (*QueueStore)(nil)
	_ DumpEnumerator = (*HeapStore)(nil)
)

// documentsOnly gives an Enumerator the dump walk's shape.
type documentsOnly struct{ Enumerator }

// DocumentsOnly is engine as a DumpEnumerator whose dead callback is never
// called: a hash, btree or blob bucket has no dead entry.
func DocumentsOnly(engine Enumerator) DumpEnumerator { return documentsOnly{engine} }

// IterateDump walks the documents; dead is never called.
func (d documentsOnly) IterateDump(documents func(key, value []byte) error, _ func(DeadRecord) error) error {
	return d.IterateDocuments(documents)
}

// pendingEntry is one claimed entry the dump collects: where it came from and
// the lease holding it.
type pendingEntry struct {
	origin, lease []byte
}

// collectPending lists the bucket's claimed entries by one pass over 'e' that
// reads keys only, sorted by origin suffix in the walk's direction. It is
// skipped, with no seek, when nothing is pending (claim-ack-spec §7.1).
func collectPending(snap storage.KVSnapshot, ref storage.BucketRef, st BucketState, reverse bool) []pendingEntry {
	slot, databaseID, id := ref.Slot, ref.DatabaseID, ref.BucketID
	if st.Pending == 0 {
		return nil
	}
	prefix := kindKey(slot, databaseID, id, kindClaimed)
	it := snap.NewIterator(storage.IterOptions{Lower: prefix, Upper: prefixEnd(prefix)})
	defer it.Close()
	pending := make([]pendingEntry, 0, st.Pending)
	for it.Seek(nil); it.Valid(); it.Next() {
		rest := it.Key()[len(prefix):]
		pending = append(pending, pendingEntry{lease: bytes.Clone(rest[:leaseIDBytes]), origin: bytes.Clone(rest[leaseIDBytes:])})
	}
	slices.SortFunc(pending, func(a, b pendingEntry) int { return bytes.Compare(a.origin, b.origin) })
	if reverse {
		slices.Reverse(pending)
	}
	return pending
}

// dumpMerge is one engine's side of the merge.
type dumpMerge struct {
	slot       uint16
	databaseID uint64
	id         uint64
	layout     claimLayout
	reverse    bool
	// live is positioned at the first available entry in pop order.
	live storage.KVIterator
	// document emits one entry of the bucket: origin is its 0x04 suffix.
	document func(origin, entry []byte) error
	dead     func(DeadRecord) error
}

// run merges the available entries with the claimed ones in pop order. A
// claimed entry's origin found among the available ones is a heap twin: the
// available entry is the document and the claimed one goes to the dead list as
// collided, with the deliveries a return would have recorded.
func (m dumpMerge) run(snap storage.KVSnapshot, pending []pendingEntry) error {
	prefixLen := originPrefixBytes
	for next := 0; m.live.Valid() || next < len(pending); {
		order := 0
		switch {
		case !m.live.Valid():
			order = -1
		case next < len(pending):
			order = bytes.Compare(pending[next].origin, m.live.Key()[prefixLen:])
			if m.reverse {
				order = -order
			}
		default:
			order = 1
		}
		if order > 0 {
			value, err := m.live.Value()
			if err != nil {
				return fmt.Errorf("reading an available entry: %w", err)
			}
			if err := m.document(m.live.Key()[prefixLen:], value); err != nil {
				return err
			}
			m.live.Next()
			continue
		}
		claimed := pending[next]
		next++
		returns, entry, err := m.readClaimed(snap, claimed)
		if err != nil {
			return err
		}
		if order < 0 {
			if err := m.document(claimed.origin, entry); err != nil {
				return err
			}
			continue
		}
		if err := m.emitTwin(claimed, returns, entry); err != nil {
			return err
		}
	}
	return m.live.Err()
}

func (m dumpMerge) readClaimed(snap storage.KVSnapshot, claimed pendingEntry) (returns uint64, entry []byte, err error) {
	value, err := snap.Get(kindKey(m.slot, m.databaseID, m.id, kindClaimed, claimed.lease, claimed.origin))
	if err != nil {
		return 0, nil, fmt.Errorf("reading claimed entry %x of lease %x: %w", claimed.origin, claimed.lease, err)
	}
	returns, entry, err = parseClaimedValue(value)
	if err != nil {
		return 0, nil, fmt.Errorf("%w: lease %x: %w", ErrDeliveryInvariant, claimed.lease, err)
	}
	return returns, entry, nil
}

// emitTwin sends a claimed heap entry whose key a live element also holds to the
// dead list. A queue's positions are never reused, so a twin there is a broken
// bucket, not a case to merge.
func (m dumpMerge) emitTwin(claimed pendingEntry, returns uint64, entry []byte) error {
	if m.layout != claimLayoutHeap {
		return fmt.Errorf("%w: claimed position %x is also an available entry", ErrDeliveryInvariant, claimed.origin)
	}
	data, err := heapEntryDocument(claimed.origin, entry)
	if err != nil {
		return err
	}
	return m.dead(DeadRecord{
		DeadID: claimed.origin, Data: data, Deliveries: int32(returns) + 1,
		DeadAt: time.Now().UnixMilli(), Reason: DeadReasonCollided,
	})
}

// IterateDump is DumpEnumerator for a queue: the available and the claimed
// entries in pop order from one snapshot, as documents with a nil key.
func (q *QueueStore) IterateDump(documents func(key, value []byte) error, dead func(DeadRecord) error) error {
	snap := q.kv.NewSnapshot()
	defer snap.Discard()
	st, err := LoadBucketState(snap, q.slot, q.databaseID, q.id)
	if err != nil {
		return fmt.Errorf("reading the queue's cursors: %w", err)
	}
	reverse := q.mode == QueueLIFO
	pending := collectPending(snap, storage.BucketRef{Slot: q.slot, DatabaseID: q.databaseID, BucketID: q.id}, st, reverse)
	live := q.fromCursor(snap, st)
	defer live.Close()
	merge := dumpMerge{
		slot: q.slot, databaseID: q.databaseID, id: q.id, layout: claimLayoutQueue, reverse: reverse, live: live, dead: dead,
		document: func(_, entry []byte) error { return documents(nil, entry) },
	}
	if err := merge.run(snap, pending); err != nil {
		return fmt.Errorf("walking the queue: %w", err)
	}
	return dumpDeadEntries(snap, storage.BucketRef{Slot: q.slot, DatabaseID: q.databaseID, BucketID: q.id}, st, dead)
}

// IterateDump is DumpEnumerator for a heap: the available and the claimed
// elements in key order, as {_id_, priority, value} documents keyed by entry id.
func (h *HeapStore) IterateDump(documents func(key, value []byte) error, dead func(DeadRecord) error) error {
	snap := h.kv.NewSnapshot()
	defer snap.Discard()
	st, err := LoadBucketState(snap, h.slot, h.databaseID, h.id)
	if err != nil {
		return fmt.Errorf("reading the heap's entry count: %w", err)
	}
	pending := collectPending(snap, storage.BucketRef{Slot: h.slot, DatabaseID: h.databaseID, BucketID: h.id}, st, false)
	prefix := h.elementPrefix()
	live := snap.NewIterator(storage.IterOptions{Lower: prefix, Upper: prefixEnd(prefix)})
	defer live.Close()
	live.Seek(nil)
	merge := dumpMerge{
		slot: h.slot, databaseID: h.databaseID, id: h.id, layout: claimLayoutHeap, live: live, dead: dead,
		document: func(origin, entry []byte) error {
			if len(origin) < 8 {
				// A KeyError keeps the key for the node log and out of the message.
				return &storage.KeyError{Operation: "parsing a heap key", Key: bytes.Clone(origin),
					Cause: fmt.Errorf("malformed suffix (too short), want \"8-byte priority + entry id\"")}
			}
			document, err := heapEntryDocument(origin, entry)
			if err != nil {
				return err
			}
			if err := documents(origin[8:], document); err != nil {
				return fmt.Errorf("iterating heap entry %q: %w", origin[8:], err)
			}
			return nil
		},
	}
	if err := merge.run(snap, pending); err != nil {
		return fmt.Errorf("walking the heap: %w", err)
	}
	return dumpDeadEntries(snap, storage.BucketRef{Slot: h.slot, DatabaseID: h.databaseID, BucketID: h.id}, st, dead)
}

// dumpDeadEntries sends every dead entry to dead in key order, from the
// snapshot the documents came from. A bucket with none opens no iterator on
// the 'x' range (claim-ack-spec §7.4).
func dumpDeadEntries(snap storage.KVSnapshot, ref storage.BucketRef, st BucketState, dead func(DeadRecord) error) error {
	slot, databaseID, id := ref.Slot, ref.DatabaseID, ref.BucketID
	if st.Dead == 0 {
		return nil
	}
	// deadWalk discards nothing: the caller's deferred Discard owns the snapshot.
	if _, _, err := deadWalk(snap, storage.BucketRef{Slot: slot, DatabaseID: databaseID, BucketID: id}, math.MaxInt, nil, dead); err != nil {
		return fmt.Errorf("walking the dead entries: %w", err)
	}
	return nil
}

// dumpedDocuments counts what IterateDump emits as documents, so DocCount
// agrees with a dump by construction.
func dumpedDocuments(engine DumpEnumerator) (int64, error) {
	var count int64
	err := engine.IterateDump(func(_, _ []byte) error {
		count++
		return nil
	}, func(DeadRecord) error { return nil })
	return count, err
}
