package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/rand"
	"sort"
	"strconv"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/store"
)

// The data set and the two answering strategies live here so main.go reads as
// the experiment's shape. Every data set is deterministic: a fixed seed means
// the same bytes every run, so a delta between two runs is a real delta and not
// a different data set.

// elementPrefix is the payload prefix every queued element carries. The number
// in it is the element's position in the queue's own order — which is what the
// scan-and-sort side must RECOVER, and what the native side gets for free from
// the key order. Using the payload rather than the key is deliberate: the scan
// side is handed nothing but values, exactly as an unindexed column would be.
const elementPrefix = "element-"

// elementNumber is the number elementBytes encodes, in the queue's own order.
func elementNumber(entry []byte) (int, bool) {
	s := string(entry)
	if len(s) != len(elementPrefix)+9 {
		return 0, false
	}
	n, err := strconv.Atoi(s[len(elementPrefix):])
	if err != nil {
		return 0, false
	}
	return n, true
}

// elementBytes is the queued payload for element i.
func elementBytes(i int) []byte {
	return []byte(fmt.Sprintf("%s%09d", elementPrefix, i))
}

// doc is the document the heap and btree fills write: flat and small, so the
// comparison is about ORDER rather than payload size or schema.
type doc struct {
	id       string
	priority int64
}

func (d doc) bson() map[string]interface{} {
	return map[string]interface{}{
		"_id_":     d.id,
		"priority": d.priority,
		"payload":  fmt.Sprintf("doc-%09s", d.id),
	}
}

// dump is what HeapStore.IterateDocuments hands back: the entry id, the priority
// the element is ordered by, and the element itself as raw BSON.
type dump struct {
	id       string
	priority int64
	value    []byte
}

// digest is how a leg compares its two answers. The full value is never
// compared — a leg answers "which element", and the digest is only a stable
// stand-in for "the same element the other side found".
func digest(v []byte) string {
	if v == nil {
		return ""
	}
	sum := sha256.Sum256(v)
	return hex.EncodeToString(sum[:8])
}

// fillQueue writes n entries in a fixed order. The queue's own position key
// ([s][id][0x04][position:8], store/queue.go:60) means the order the caller
// pushed in IS the key order, which is the property under test.
func fillQueue(q *store.QueueStore, n int) error {
	info := store.ApplyInfo{}
	batch := make([][]byte, 0, batchSize)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := q.PushBatch(info, batch); err != nil {
			return fmt.Errorf("pushing a batch of %d: %w", len(batch), err)
		}
		batch = batch[:0]
		return nil
	}
	for i := 0; i < n; i++ {
		batch = append(batch, elementBytes(i))
		if len(batch) == batchSize {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	return flush()
}

// fillHeap writes n elements with pseudo-random priorities, so "the most urgent
// element" is a real answer that has to be found rather than a coincidence of
// push order. The priorities span a wide range so the answer is neither the
// first nor the last key written.
func fillHeap(h *store.HeapStore, n int) error {
	info := store.ApplyInfo{}
	rnd := newRand(0xC0FFEE)
	batch := make([]store.HeapItem, 0, batchSize)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if _, err := h.PushItemBatch(info, batch); err != nil {
			return fmt.Errorf("pushing a batch of %d: %w", len(batch), err)
		}
		batch = batch[:0]
		return nil
	}
	for i := 0; i < n; i++ {
		d := doc{id: fmt.Sprintf("e%09d", i), priority: rnd.Int63n(1 << 40)}
		batch = append(batch, store.HeapItem{Priority: d.priority, Value: d.bson()})
		if len(batch) == batchSize {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	return flush()
}

// fillBTree writes n documents whose user keys are the integers 0..n-1. The
// btree key is EncodeComponent(user key) (store/btree.go:53), so the store's
// key order is the numeric order and a range is a bounded seek.
func fillBTree(b *store.BTreeStore, n int) error {
	info := store.ApplyInfo{}
	items := make([]map[string]interface{}, 0, batchSize)
	flush := func() error {
		if len(items) == 0 {
			return nil
		}
		if _, err := b.PutBatch(info, store.PutBatchRequest{Items: items}); err != nil {
			return fmt.Errorf("putting a batch of %d: %w", len(items), err)
		}
		items = items[:0]
		return nil
	}
	for i := 0; i < n; i++ {
		items = append(items, doc{id: strconv.Itoa(i), priority: int64(i)}.bson())
		if len(items) == batchSize {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	return flush()
}

// ---------------------------------------------------------------------------
// The scan-and-sort side of every leg
//
// This is the deliberately naive half, and the reason is worth stating: the
// sort runs on values the engine hands over, with no index, no optimizer and no
// prepared plan. A mature engine would sort on a column and pay to maintain an
// index for it on every write. The write leg is measured separately for
// exactly that reason — see the write leg in main.go.
// ---------------------------------------------------------------------------

// sortIntoOrder sorts queue entries into the order a planner would have had to
// compute, recovering that order from each entry's own payload. It is the
// fifo leg's naive side: the fifo store hands entries over oldest-first already,
// so what this adds is the work of RECOVERING that order from the values rather
// than being handed it by the key order — the sort a document engine performs.
func sortIntoOrder(entries [][]byte) [][]byte {
	out := make([][]byte, len(entries))
	copy(out, entries)
	sort.SliceStable(out, func(i, j int) bool {
		a, aok := elementNumber(out[i])
		b, bok := elementNumber(out[j])
		if !aok || !bok {
			// An entry whose number cannot be read sorts last rather than
			// silently deciding the answer.
			return !aok && bok
		}
		return a < b
	})
	return out
}

// heapDump reads every heap element with the priority it is ordered by. It uses
// the store's own IterateDump, so it is handed the same view a client
// would get — no reaching into the key layout, no privileged knowledge of where
// the inverted priority sits in the key.
func heapDump(h *store.HeapStore, onRead func()) ([]dump, error) {
	var out []dump
	if err := h.IterateDump(func(key, value []byte) error {
		onRead()
		var d struct {
			ID       string   `bson:"_id_"`
			Priority int64    `bson:"priority"`
			Value    bson.Raw `bson:"value"`
		}
		if err := bson.Unmarshal(value, &d); err != nil {
			return fmt.Errorf("decoding a heap dump entry %q: %w", d.ID, err)
		}
		out = append(out, dump{id: d.ID, priority: d.Priority, value: []byte(d.Value)})
		return nil
	}, func(store.DeadRecord) error {
		// The benchmark's heap never dead-letters, so there is nothing to carry.
		return nil
	}); err != nil {
		return nil, err
	}
	return out, nil
}

// btreeDump reads every document in a btree bucket with its user key, again
// through the store's own iteration rather than the key bytes.
func btreeDump(b *store.BTreeStore, onRead func()) ([]dump, error) {
	var out []dump
	if err := b.IterateDocuments(func(key, value []byte) error {
		onRead()
		out = append(out, dump{id: string(key), priority: 0, value: value})
		return nil
	}); err != nil {
		return nil, err
	}
	return out, nil
}

// newRand is a small deterministic generator, so the harness does not depend on
// math/rand's stream for reproducibility across Go versions.
func newRand(seed int64) *rand.Rand {
	return rand.New(rand.NewSource(seed))
}
