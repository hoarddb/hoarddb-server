package store

import (
	"encoding/binary"
	"fmt"

	"github.com/hoarddb/hoarddb-server/storage"
)

// DeliveryCounts is what a read of a queue or a heap reports beside its entries
// (claim-ack-spec 8.1): how many entries are claimed, how many are dead, and
// when the soonest lease ends. All of it comes from the one state document the
// read already loaded, plus one seek when a lease exists.
type DeliveryCounts struct {
	// Entries is the entries available to take: the number len() answers.
	Entries int64
	// Pending is the entries claimed and not yet acknowledged.
	Pending int64
	// Dead is the entries on the dead list.
	Dead int64
	// NextDeadline is the soonest lease deadline in ms, 0 when no lease exists.
	// The server compares it with its own clock to say expired (claim-ack-spec
	// 8.2); a read never runs the return step.
	NextDeadline int64
}

// deliveryCountsOf reads a state's counts and, only when it holds a lease, the
// first 'd' key of the same snapshot. A bucket that never claimed pays no seek.
func deliveryCountsOf(snap storage.KVSnapshot, slot uint16, databaseID, id uint64, st BucketState) (DeliveryCounts, error) {
	counts := DeliveryCounts{Entries: st.Entries, Pending: st.Pending, Dead: st.Dead}
	if st.Leases == 0 {
		return counts, nil
	}
	prefix := kindKey(slot, databaseID, id, kindDeadline)
	it := snap.NewIterator(storage.IterOptions{Lower: prefix, Upper: prefixEnd(prefix)})
	defer it.Close()
	it.Seek(nil)
	if it.Valid() {
		counts.NextDeadline = int64(binary.BigEndian.Uint64(it.Key()[len(prefix):]))
	}
	if err := it.Err(); err != nil {
		return DeliveryCounts{}, fmt.Errorf("reading the soonest lease deadline: %w", err)
	}
	return counts, nil
}

// Counts reads the queue's counts from one snapshot.
func (q *QueueStore) Counts() (DeliveryCounts, error) {
	snap := q.kv.NewSnapshot()
	defer snap.Discard()
	st, err := LoadBucketState(snap, q.slot, q.databaseID, q.id)
	if err != nil {
		return DeliveryCounts{}, fmt.Errorf("reading the entry count: %w", err)
	}
	return deliveryCountsOf(snap, q.slot, q.databaseID, q.id, st)
}

// Counts reads the heap's counts from one snapshot.
func (h *HeapStore) Counts() (DeliveryCounts, error) {
	snap := h.kv.NewSnapshot()
	defer snap.Discard()
	st, err := LoadBucketState(snap, h.slot, h.databaseID, h.id)
	if err != nil {
		return DeliveryCounts{}, fmt.Errorf("reading the entry count: %w", err)
	}
	return deliveryCountsOf(snap, h.slot, h.databaseID, h.id, st)
}

// idleClaimOf reports whether a claim stamped at now would take nothing and
// return nothing (claim-ack-spec 5.3), with the counts its answer carries: no
// entry is available, and either no lease exists, or none has ended by
// max(now, LeaseClock) and none lies beyond the forward-jump guard of
// min(now, LeaseClock). The apply's effective now lies between the two
// (effectiveNow), so whenever the logged claim would return a lease, by its
// deadline or by the guard, this answers false. One state Get, and two 'd'
// seeks when a lease exists.
func idleClaimOf(snap storage.KVSnapshot, slot uint16, databaseID, id uint64, now int64) (DeliveryCounts, bool, error) {
	st, err := LoadBucketState(snap, slot, databaseID, id)
	if err != nil {
		return DeliveryCounts{}, false, fmt.Errorf("reading the entry count: %w", err)
	}
	counts := DeliveryCounts{Entries: st.Entries, Pending: st.Pending, Dead: st.Dead}
	if st.Entries != 0 {
		return counts, false, nil
	}
	if st.Leases == 0 {
		return counts, true, nil
	}
	prefix := kindKey(slot, databaseID, id, kindDeadline)
	it := snap.NewIterator(storage.IterOptions{Lower: prefix, Upper: prefixEnd(prefix)})
	defer it.Close()
	it.Seek(nil)
	if !it.Valid() {
		if err := it.Err(); err != nil {
			return counts, false, fmt.Errorf("reading the soonest lease deadline: %w", err)
		}
		return counts, false, fmt.Errorf("%w: %d leases and no lease deadline", ErrDeliveryInvariant, st.Leases)
	}
	if soonest := int64(binary.BigEndian.Uint64(it.Key()[len(prefix):])); soonest <= max(now, st.LeaseClock) {
		return counts, false, nil
	}
	it.Seek(kindKey(slot, databaseID, id, kindDeadline, bigEndian(uint64(min(now, st.LeaseClock)+MaxLeaseMs+1))))
	if it.Valid() {
		return counts, false, nil
	}
	if err := it.Err(); err != nil {
		return counts, false, fmt.Errorf("seeking a lease beyond the forward-jump guard: %w", err)
	}
	return counts, true, nil
}

// IdleClaim reports, from one snapshot, whether a claim stamped at now would
// take and return nothing, so its owner may answer it without logging it.
func (q *QueueStore) IdleClaim(now int64) (DeliveryCounts, bool, error) {
	snap := q.kv.NewSnapshot()
	defer snap.Discard()
	return idleClaimOf(snap, q.slot, q.databaseID, q.id, now)
}

// IdleClaim is QueueStore.IdleClaim for a heap.
func (h *HeapStore) IdleClaim(now int64) (DeliveryCounts, bool, error) {
	snap := h.kv.NewSnapshot()
	defer snap.Discard()
	return idleClaimOf(snap, h.slot, h.databaseID, h.id, now)
}
