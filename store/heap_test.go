package store

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/storage/kvfault"
)

// openTestHeap returns a heap store on a fresh fault store and the store, so
// a test can build the heap again over the same keys the way a restart does.
func openTestHeap(t *testing.T) (*HeapStore, storage.KV) {
	t.Helper()
	kv := kvfault.New()
	return NewHeapStore(kv, 1, 1, 1, new(OwedApplies)), kv
}

// heapLen is the heap's count, failing the test on a store error.
func heapLen(t *testing.T, h *HeapStore) int64 {
	t.Helper()
	n, err := h.Len()
	if err != nil {
		t.Fatalf("Len: %v", err)
	}
	return n
}

// TestHeapPushWithSameIDOverwrites pins the invariant that makes heap replay
// safe, and it is the reason the heap cannot be exempted from replay the way
// FIFO and LIFO are.
//
// Badger does not fsync per entry, so a crash can lose the most recent heap
// pushes and the log is what restores them. Restoring them can only be an
// overwrite if every applier agrees on the element id: the id is the Badger key
// *and* half the in-memory entry, so an applier that minted its own id added a
// second element instead of rewriting the one already there.
func TestHeapPushWithSameIDOverwrites(t *testing.T) {
	heap, kv := openTestHeap(t)

	element := map[string]interface{}{"_id_": "element-1", "n": 1}

	if _, err := heap.PushItem(ApplyInfo{}, 3, element); err != nil {
		t.Fatalf("first PushItem: %v", err)
	}
	if heapLen(t, heap) != 1 {
		t.Fatalf("Len = %d after one push, want 1", heapLen(t, heap))
	}

	// Re-applying the same element — a replay, or a re-delivered entry.
	if _, err := heap.PushItem(ApplyInfo{}, 3, element); err != nil {
		t.Fatalf("second PushItem: %v", err)
	}
	if heapLen(t, heap) != 1 {
		t.Fatalf("Len = %d after re-applying the same element, want 1: it was added twice", heapLen(t, heap))
	}

	// A distinct element does add.
	if _, err := heap.PushItem(ApplyInfo{}, 1, map[string]interface{}{"_id_": "element-2", "n": 2}); err != nil {
		t.Fatalf("PushItem of a second element: %v", err)
	}
	if heapLen(t, heap) != 2 {
		t.Fatalf("Len = %d after pushing a distinct element, want 2", heapLen(t, heap))
	}

	// And a heap built again over the same store must agree with what is
	// stored, or the count would change on every restart.
	reopened := NewHeapStore(kv, 1, 1, 1, new(OwedApplies))
	if heapLen(t, reopened) != 2 {
		t.Fatalf("Len = %d after reopening, want 2: the count disagrees with what is stored", heapLen(t, reopened))
	}
}

// TestHeapPushItemBatchDeduplicates covers the batch path, which follows the same
// rule: an id repeated at the same priority must not grow the heap.
//
// The stored key is (priority, entryID), so the same id at a *different* priority
// is a different element — that is the existing key design, not an accident, and
// it is why the repeat below keeps its priority.
func TestHeapPushItemBatchDeduplicates(t *testing.T) {
	heap, _ := openTestHeap(t)

	items := []HeapItem{
		{Priority: 1, Value: map[string]interface{}{"_id_": "a", "n": 1}},
		{Priority: 2, Value: map[string]interface{}{"_id_": "b", "n": 2}},
		{Priority: 1, Value: map[string]interface{}{"_id_": "a", "n": 1}},
	}
	ids, err := heap.PushItemBatch(ApplyInfo{}, items)
	if err != nil {
		t.Fatalf("PushItemBatch: %v", err)
	}
	if len(ids) != len(items) {
		t.Fatalf("PushItemBatch returned %d ids for %d items", len(ids), len(items))
	}
	if heapLen(t, heap) != 2 {
		t.Fatalf("Len = %d after a batch repeating an id at the same priority, want 2", heapLen(t, heap))
	}
}

// TestHeapNegativePrioritiesOrder: the inverted-priority key orders every
// int64, negatives included, highest first — the XOR with 1<<63 makes
// two's-complement order numeric and the complement reverses it (spec §3.3
// rev 5.2.2), so the pop's forward seek takes the highest.
func TestHeapNegativePrioritiesOrder(t *testing.T) {
	heap, _ := openTestHeap(t)
	for _, priority := range []int64{-1, 3, -5, 0} {
		if _, err := heap.PushItem(ApplyInfo{}, priority, map[string]interface{}{"priority": priority}); err != nil {
			t.Fatalf("PushItem(%d): %v", priority, err)
		}
	}
	var popped []int64
	for i := 0; i < 4; i++ {
		value, err := heap.PopItem(ApplyInfo{})
		if err != nil || value == nil {
			t.Fatalf("PopItem %d: %v (empty %v)", i, err, value == nil)
		}
		var element struct {
			Priority int64 `bson:"priority"`
		}
		if err := bson.Unmarshal(value, &element); err != nil {
			t.Fatalf("decoding popped element: %v", err)
		}
		popped = append(popped, element.Priority)
	}
	if want := []int64{3, 0, -1, -5}; !reflect.DeepEqual(popped, want) {
		t.Fatalf("popped priorities %v, want highest first %v", popped, want)
	}
}

// TestHeapPriorityKeyRoundTripsAndDescends: the stated inverse recovers every
// priority, the int64 extremes included, and the key's unsigned order is
// descending priority, so a dump re-attaches the priority an entry had and a
// forward seek meets the highest first (spec §3.3 rev 5.2.2).
func TestHeapPriorityKeyRoundTripsAndDescends(t *testing.T) {
	ascending := []int64{math.MinInt64, math.MinInt64 + 1, -1 << 40, -5, -1, 0, 1, 3, 1 << 40, math.MaxInt64 - 1, math.MaxInt64}
	for i, priority := range ascending {
		inverted := invertedPriority(priority)
		if got := priorityOf(inverted); got != priority {
			t.Errorf("priorityOf(invertedPriority(%d)) = %d: the inverse does not round-trip", priority, got)
		}
		if i == 0 {
			continue
		}
		lower := invertedPriority(ascending[i-1])
		if inverted >= lower {
			t.Errorf("key of %d is %#x, key of the lower %d is %#x: a higher priority must sort first", priority, inverted, ascending[i-1], lower)
		}
	}
}

// newCountedHeap is a heap over a countingKV, so a test sees the values a walk
// reads and the batches it commits.
func newCountedHeap(t *testing.T) (*HeapStore, *countingKV) {
	t.Helper()
	kv := &countingKV{KV: kvfault.New()}
	return NewHeapStore(kv, 1, 1, 1, new(OwedApplies)), kv
}

// pushHeapEntries pushes count entries "e<priority>" in shuffled priority
// order (the multiplier is coprime with every count used here).
func pushHeapEntries(t *testing.T, heap *HeapStore, count int) {
	t.Helper()
	for i := 0; i < count; i++ {
		priority := int64((i * 7) % count)
		if _, err := heap.PushItem(ApplyInfo{}, priority, map[string]interface{}{"_id_": fmt.Sprintf("e%d", priority), "p": priority}); err != nil {
			t.Fatalf("PushItem(%d): %v", priority, err)
		}
	}
}

// pushTiedHeapEntries pushes a round of entries in which three share one
// priority; round makes the ids of each round distinct.
func pushTiedHeapEntries(t *testing.T, heap *HeapStore, round int) {
	t.Helper()
	for _, entry := range []struct {
		id       string
		priority int64
	}{
		{"low", 1}, {"tie-c", 5}, {"top", 9}, {"tie-a", 5}, {"mid", 3}, {"tie-b", 5}, {"under", 2}, {"high", 7},
	} {
		id := fmt.Sprintf("%s-r%d", entry.id, round)
		if _, err := heap.PushItem(ApplyInfo{}, entry.priority, map[string]interface{}{"_id_": id}); err != nil {
			t.Fatalf("PushItem(%s): %v", id, err)
		}
	}
}

func heapEntryID(t testing.TB, entry []byte) string {
	t.Helper()
	id, ok := bson.Raw(entry).Lookup("_id_").StringValueOK()
	if !ok {
		t.Fatalf("heap entry %v has no string _id_", bson.Raw(entry))
	}
	return id
}

func peekHeap(t testing.TB, heap *HeapStore, n int) []string {
	t.Helper()
	var ids []string
	if err := heap.PeekN(n, func(entry []byte) error {
		ids = append(ids, heapEntryID(t, entry))
		return nil
	}); err != nil {
		t.Fatalf("PeekN(%d): %v", n, err)
	}
	return ids
}

func TestHeapPeekNIsHighestPriorityFirst(t *testing.T) {
	heap, _ := newCountedHeap(t)
	pushHeapEntries(t, heap, 10)
	if got, want := peekHeap(t, heap, 3), []string{"e9", "e8", "e7"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("PeekN(3) = %v, want %v", got, want)
	}
}

// TestHeapPeekNReadsNoValuePastN also decided that PeekItem stays a separate
// one-seek read: see the build report.
func TestHeapPeekNReadsNoValuePastN(t *testing.T) {
	for _, n := range []int{1, 3} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			heap, kv := newCountedHeap(t)
			pushHeapEntries(t, heap, 10)
			kv.valueReads.Store(0)
			if got := len(peekHeap(t, heap, n)); got != n {
				t.Fatalf("PeekN(%d) returned %d entries", n, got)
			}
			if got := kv.valueReads.Load(); got != int64(n) {
				t.Fatalf("PeekN(%d) read %d values, want exactly %d", n, got, n)
			}
		})
	}
}

func TestHeapPeekNDoesNotMutate(t *testing.T) {
	heap, kv := newCountedHeap(t)
	pushHeapEntries(t, heap, 10)
	commitsBefore := kv.commits.Load()
	first := peekHeap(t, heap, 3)
	if again := peekHeap(t, heap, 3); !reflect.DeepEqual(first, again) {
		t.Fatalf("a second PeekN(3) = %v, the first said %v", again, first)
	}
	if got := kv.commits.Load(); got != commitsBefore {
		t.Fatalf("PeekN committed %d batches, want none", got-commitsBefore)
	}
	if heapLen(t, heap) != 10 {
		t.Fatalf("Len after peeking = %d, want 10", heapLen(t, heap))
	}
	value, err := heap.PopItem(ApplyInfo{})
	if err != nil || value == nil {
		t.Fatalf("PopItem: %v (empty %v)", err, value == nil)
	}
	if got := heapEntryID(t, value); got != first[0] {
		t.Fatalf("next pop = %s, want the first peeked %s", got, first[0])
	}
}

// TestHeapPeekNCursorLost: a state that counts entries the range does not hold
// is an error for a peek, never "empty" and never a short list.
func TestHeapPeekNCursorLost(t *testing.T) {
	cases := []struct {
		name string
		drop []string
	}{
		{"range holds none", []string{"a", "b", "c"}},
		{"range holds one", []string{"b", "c"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			heap, kv := newCountedHeap(t)
			for i, id := range []string{"a", "b", "c"} {
				if _, err := heap.PushItem(ApplyInfo{}, int64(3-i), map[string]interface{}{"_id_": id}); err != nil {
					t.Fatalf("PushItem(%s): %v", id, err)
				}
			}
			priorities := map[string]int64{"a": 3, "b": 2, "c": 1}
			batch := kv.NewBatch()
			defer batch.Discard()
			for _, id := range tc.drop {
				if err := batch.Delete(heap.makeKey(id, priorities[id])); err != nil {
					t.Fatalf("deleting %s: %v", id, err)
				}
			}
			if err := batch.Commit(false); err != nil {
				t.Fatalf("committing the deletes: %v", err)
			}
			err := heap.PeekN(5, func([]byte) error { return nil })
			if !errors.Is(err, ErrQueueCursorLost) {
				t.Fatalf("PeekN(5) = %v, want ErrQueueCursorLost", err)
			}
		})
	}
}
