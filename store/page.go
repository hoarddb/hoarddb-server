package store

import (
	"fmt"

	"github.com/hoarddb/hoarddb-server/storage"
)

// PageRequest asks an engine for one unfiltered page of its entries in natural
// order: the Limit entries after the first Skip. Limit must be positive.
type PageRequest struct {
	Skip  int
	Limit int
	// Step runs for every entry the walk passes, on the page or before it, so
	// a caller can stop a long skip. Its error ends the walk and is returned
	// unchanged.
	Step func() error
	// Visit receives each page entry's stored bytes, which are its to keep.
	// Its error ends the walk and is returned unchanged.
	Visit func(entry []byte) error
}

// readPage serves an engine's Page: the page from the iterator open returns
// and the bucket's entry count, both read from one snapshot, so the count
// agrees with the page. The walk stops at the page's last entry and reads no
// value before Skip, which makes a page cost O(Skip + Limit) iterator steps
// and Limit value copies, whatever the bucket holds.
func readPage(kv storage.KV, ref storage.BucketRef, open func(storage.KVSnapshot, BucketState) storage.KVIterator, req PageRequest) (int64, error) {
	slot, databaseID, id := ref.Slot, ref.DatabaseID, ref.BucketID
	snap := kv.NewSnapshot()
	defer snap.Discard()
	st, err := LoadBucketState(snap, slot, databaseID, id)
	if err != nil {
		return 0, fmt.Errorf("reading the entry count: %w", err)
	}
	it := open(snap, st)
	defer it.Close()

	end := req.Skip + req.Limit
	for passed := 0; passed < end && it.Valid(); {
		if err := req.Step(); err != nil {
			return 0, err
		}
		if passed >= req.Skip {
			entry, err := it.Value()
			if err != nil {
				return 0, fmt.Errorf("reading an entry: %w", err)
			}
			if err := req.Visit(entry); err != nil {
				return 0, err
			}
		}
		passed++
		// Moving past the page's last entry would settle on the next one,
		// which the page does not need.
		if passed < end {
			it.Next()
		}
	}
	if err := it.Err(); err != nil {
		return 0, fmt.Errorf("walking the page: %w", err)
	}
	return st.Entries, nil
}

// fromStart is an iterator over bounds positioned at their first live key.
func fromStart(bounds storage.IterOptions) func(storage.KVSnapshot, BucketState) storage.KVIterator {
	return func(snap storage.KVSnapshot, _ BucketState) storage.KVIterator {
		it := snap.NewIterator(bounds)
		it.Seek(nil)
		return it
	}
}

// Page returns one page of the queue in pop order — oldest first for fifo,
// newest first for lifo — and its entry count, from one snapshot. The walk
// starts at the cursor, as a pop does.
func (q *QueueStore) Page(req PageRequest) (int64, error) {
	return readPage(q.kv, storage.BucketRef{Slot: q.slot, DatabaseID: q.databaseID, BucketID: q.id}, q.fromCursor, req)
}

// Page returns one page of the hash bucket in ascending key order and its
// document count, from one snapshot.
func (h *HashStore) Page(req PageRequest) (int64, error) {
	return readPage(h.kv, storage.BucketRef{Slot: h.slot, DatabaseID: h.databaseID, BucketID: h.id}, fromStart(h.documentRange()), req)
}

// Page returns one page of the btree bucket in key order and its document
// count, from one snapshot.
func (b *BTreeStore) Page(req PageRequest) (int64, error) {
	prefix := b.documentPrefix()
	return readPage(b.kv, storage.BucketRef{Slot: b.slot, DatabaseID: b.databaseID, BucketID: b.id}, fromStart(storage.IterOptions{Lower: prefix, Upper: prefixEnd(prefix)}), req)
}

// Page returns one page of the heap, highest priority first, and its entry
// count, from one snapshot.
func (h *HeapStore) Page(req PageRequest) (int64, error) {
	prefix := h.elementPrefix()
	return readPage(h.kv, storage.BucketRef{Slot: h.slot, DatabaseID: h.databaseID, BucketID: h.id}, fromStart(storage.IterOptions{Lower: prefix, Upper: prefixEnd(prefix)}), req)
}
