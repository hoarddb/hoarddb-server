package store

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/storage/kvfault"
)

// countingKV counts the stored versions every iterator it hands out visits,
// and the batches committed through it, so a test can see what one verb cost.
type countingKV struct {
	storage.KV
	visits  atomic.Int64
	commits atomic.Int64
	// valueReads counts Value calls: the reads a walk pays in bytes, which
	// visits (stored versions touched) does not show.
	valueReads atomic.Int64
	// failValues makes every iterator Value read fail, a read error that
	// ApplyFaultHook cannot inject because it runs only before the commit.
	failValues atomic.Bool
	// returnCountGets counts point reads of 'c' records, the lookup a bucket
	// that never had an entry returned must not pay.
	returnCountGets atomic.Int64
	// batchBytes is what every batch added: len(key)+len(value) and
	// deliveryKeyOverhead per Set and Delete, Badger's charge rounded up.
	batchBytes atomic.Int64
}

// errValueReadFailed is the read error failValues injects.
var errValueReadFailed = errors.New("injected value read failure")

func (c *countingKV) NewSnapshot() storage.KVSnapshot {
	return countingSnapshot{KVSnapshot: c.KV.NewSnapshot(), counter: c}
}

func (c *countingKV) NewBatch() storage.KVBatch {
	return countingBatch{KVBatch: c.KV.NewBatch(), counter: c}
}

type countingSnapshot struct {
	storage.KVSnapshot
	counter *countingKV
}

func (s countingSnapshot) NewIterator(opts storage.IterOptions) storage.KVIterator {
	return &countingIterator{KVIterator: s.KVSnapshot.NewIterator(opts), counter: s.counter}
}

func (s countingSnapshot) Get(key []byte) ([]byte, error) {
	if len(key) > 11 && key[10] == storage.NSDelivery && key[11] == kindReturnCount {
		s.counter.returnCountGets.Add(1)
	}
	return s.KVSnapshot.Get(key)
}

type countingIterator struct {
	storage.KVIterator
	counter *countingKV
}

func (it *countingIterator) Value() ([]byte, error) {
	it.counter.valueReads.Add(1)
	if it.counter.failValues.Load() {
		return nil, errValueReadFailed
	}
	return it.KVIterator.Value()
}

func (it *countingIterator) Close() {
	it.counter.visits.Add(it.Visits())
	it.KVIterator.Close()
}

type countingBatch struct {
	storage.KVBatch
	counter *countingKV
}

func (b countingBatch) Set(key, value []byte) error {
	b.counter.batchBytes.Add(int64(len(key) + len(value) + deliveryKeyOverhead))
	return b.KVBatch.Set(key, value)
}

func (b countingBatch) Delete(key []byte) error {
	b.counter.batchBytes.Add(int64(len(key) + deliveryKeyOverhead))
	return b.KVBatch.Delete(key)
}

func (b countingBatch) Commit(sync bool) error {
	b.counter.commits.Add(1)
	return b.KVBatch.Commit(sync)
}

func newTestQueue(t *testing.T, mode QueueMode) (*QueueStore, *countingKV) {
	t.Helper()
	kv := &countingKV{KV: kvfault.New()}
	return NewQueueStore(kv, storage.BucketRef{Slot: 1, DatabaseID: 1, BucketID: 1}, new(OwedApplies), mode), kv
}

// queueEntry is a pushed element: a BSON document carrying its own _id_ and n.
func queueEntry(t testing.TB, n int) []byte {
	t.Helper()
	encoded, err := bson.Marshal(map[string]interface{}{"_id_": fmt.Sprintf("e%d", n), "n": n})
	if err != nil {
		t.Fatalf("encoding entry %d: %v", n, err)
	}
	return encoded
}

func entryNumber(t testing.TB, entry []byte) int {
	t.Helper()
	n, ok := bson.Raw(entry).Lookup("n").Int32OK()
	if !ok {
		t.Fatalf("entry %v has no n", bson.Raw(entry))
	}
	return int(n)
}

func pushNumbered(t testing.TB, q *QueueStore, from, to int) {
	t.Helper()
	for n := from; n < to; n++ {
		if _, err := q.Push(ApplyInfo{}, queueEntry(t, n)); err != nil {
			t.Fatalf("push %d: %v", n, err)
		}
	}
}

func popNumber(t testing.TB, q *QueueStore) int {
	t.Helper()
	entry, err := q.Pop(ApplyInfo{})
	if err != nil || entry == nil {
		t.Fatalf("pop: %v (empty %v)", err, entry == nil)
	}
	return entryNumber(t, entry)
}

func queueState(t *testing.T, q *QueueStore) BucketState {
	t.Helper()
	snap := q.kv.NewSnapshot()
	defer snap.Discard()
	st, err := LoadBucketState(snap, q.slot, 1, q.id)
	if err != nil {
		t.Fatalf("loading the queue's state: %v", err)
	}
	return st
}

// assertCursorsAgreeWithRange checks the state document against the range:
// the count is the live entries, and every live position lies in [head, next)
// and at or below tail.
func assertCursorsAgreeWithRange(t *testing.T, q *QueueStore) {
	t.Helper()
	st := queueState(t, q)
	snap := q.kv.NewSnapshot()
	defer snap.Discard()
	prefix := q.entryPrefix()
	it := snap.NewIterator(storage.IterOptions{Lower: prefix, Upper: prefixEnd(prefix)})
	defer it.Close()
	var live int64
	for it.Seek(nil); it.Valid(); it.Next() {
		position := positionOf(it.Key())
		if position < st.Head || position >= st.Next || position > st.Tail {
			t.Fatalf("live entry at position %d outside head %d, tail %d, next %d", position, st.Head, st.Tail, st.Next)
		}
		live++
	}
	if live != st.Entries {
		t.Fatalf("state counts %d entries, the range holds %d", st.Entries, live)
	}
}

// TestQueuePopDoesNotWalkTheWall is T-06: pushes and pops on the real store
// with no compaction asked for. A pop that seeks to head lands on the first
// live key; a pop that seeks to the namespace start walks every delete marker
// left before it, so the last pops would visit far more stored versions than
// the first. The assertion counts visited versions, not time: a
// wall-clock ratio failed under a loaded `go test ./... -race` with no change
// to the pop path, while a count is the same on any machine.
func TestQueuePopDoesNotWalkTheWall(t *testing.T) {
	store, err := storage.OpenBadgerKV(t.TempDir(), storage.BadgerTuning{MemTableMB: 32}, nil)
	if err != nil {
		t.Fatalf("opening the store: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("closing the store: %v", err)
		}
	})
	kv := &countingKV{KV: store}
	q := NewQueueStore(kv, storage.BucketRef{Slot: 1, DatabaseID: 1, BucketID: 1}, new(OwedApplies), QueueFIFO)

	const entries, sample, perBatch = 100_000, 1_000, 1_000
	entry := []byte("0123456789abcdef")
	batch := make([][]byte, perBatch)
	for i := range batch {
		batch[i] = entry
	}
	for pushed := 0; pushed < entries; pushed += perBatch {
		if err := q.PushBatch(ApplyInfo{}, batch); err != nil {
			t.Fatalf("pushing at %d: %v", pushed, err)
		}
	}
	visitsOfPops := func() int64 {
		before := kv.visits.Load()
		for i := 0; i < sample; i++ {
			if popped, err := q.Pop(ApplyInfo{}); err != nil || popped == nil {
				t.Fatalf("pop: %v (empty %v)", err, popped == nil)
			}
		}
		return kv.visits.Load() - before
	}
	first := visitsOfPops()
	// The walk is quadratic over the run, so a window ten times the first ends
	// the test at once rather than letting a broken pop run for minutes.
	for popped := sample; popped < entries-sample; popped += sample {
		if window := visitsOfPops(); window > 10*first {
			t.Fatalf("after %d pops a window of %d visited %d stored versions, the first visited %d: a pop walks the delete markers behind head",
				popped, sample, window, first)
		}
	}
	last := visitsOfPops()
	// A pop that seeks to head touches a handful of versions at most; one that
	// walks the markers behind head touches ~entries of them per pop by the end.
	if perPop := last / sample; perPop > 4 {
		t.Fatalf("the last %d pops visited %d stored versions (%d per pop), the first %d visited %d: a pop walks the delete markers behind head",
			sample, last, perPop, sample, first)
	}
	if last > 2*first {
		t.Fatalf("the last %d pops visited %d stored versions, the first %d visited %d: a pop walks the delete markers behind head",
			sample, last, sample, first)
	}
}

// TestDeleteAtPositionZeroAdvancesHead is T-29(a).
func TestDeleteAtPositionZeroAdvancesHead(t *testing.T) {
	const entries, deleted = 10_000, 1_000
	for _, mode := range []QueueMode{QueueFIFO, QueueLIFO} {
		t.Run(modeName(mode), func(t *testing.T) {
			q, kv := newTestQueue(t, mode)
			if err := q.PushBatch(ApplyInfo{}, numberedEntries(t, 0, entries)); err != nil {
				t.Fatalf("push: %v", err)
			}
			for i := 0; i < deleted; i++ {
				if err := q.DeleteAtPosition(0); err != nil {
					t.Fatalf("DeleteAtPosition(0) #%d: %v", i, err)
				}
			}
			if st := queueState(t, q); st.Head != deleted {
				t.Errorf("head = %d after %d DeleteAtPosition(0), want %d: head is not past the deleted run", st.Head, deleted, deleted)
			} else {
				assertCursorsAgreeWithRange(t, q)
			}
			if mode == QueueFIFO {
				kv.visits.Store(0)
				if n := popNumber(t, q); n != deleted {
					t.Fatalf("the next pop took entry %d, want %d", n, deleted)
				}
				if visits := kv.visits.Load(); visits != 1 {
					t.Fatalf("the next pop's seek visited %d versions, want 1: it walked delete markers behind head", visits)
				}
			}
		})
	}
}

// TestDeleteAtIDOfHeadAdvancesHead is T-29(b): the head entry deleted by id
// moves head in the delete's own batch.
func TestDeleteAtIDOfHeadAdvancesHead(t *testing.T) {
	for _, mode := range []QueueMode{QueueFIFO, QueueLIFO} {
		t.Run(modeName(mode), func(t *testing.T) {
			q, kv := newTestQueue(t, mode)
			pushNumbered(t, q, 0, 5)
			kv.commits.Store(0)
			if err := q.DeleteAtID("e0"); err != nil {
				t.Fatalf("DeleteAtID(e0): %v", err)
			}
			if commits := kv.commits.Load(); commits != 1 {
				t.Fatalf("DeleteAtID committed %d batches, want 1: the delete and the head move must be one batch", commits)
			}
			if st := queueState(t, q); st.Head != 1 {
				t.Fatalf("head = %d after deleting the head entry by id, want 1", st.Head)
			}
			assertCursorsAgreeWithRange(t, q)
		})
	}
}

// TestDeleteAtPositionKDoesNotMoveHead is T-29(c): a delete with a live entry
// before it leaves head alone, and the count is right after a restart.
func TestDeleteAtPositionKDoesNotMoveHead(t *testing.T) {
	for _, mode := range []QueueMode{QueueFIFO, QueueLIFO} {
		t.Run(modeName(mode), func(t *testing.T) {
			q, kv := newTestQueue(t, mode)
			pushNumbered(t, q, 0, 5)
			if err := q.DeleteAtPosition(2); err != nil {
				t.Fatalf("DeleteAtPosition(2): %v", err)
			}
			if st := queueState(t, q); st.Head != 0 {
				t.Fatalf("head = %d after deleting position 2, want 0", st.Head)
			}
			reopened := NewQueueStore(kv, storage.BucketRef{Slot: 1, DatabaseID: 1, BucketID: 1}, new(OwedApplies), mode)
			if n, err := reopened.Len(); err != nil || n != 4 {
				t.Fatalf("Len after reopening = %d (%v), want 4", n, err)
			}
			if n, err := reopened.DocCount(); err != nil || n != 4 {
				t.Fatalf("DocCount after reopening = %d (%v), want 4", n, err)
			}
			assertCursorsAgreeWithRange(t, reopened)
		})
	}
}

func modeName(mode QueueMode) string {
	if mode == QueueLIFO {
		return "lifo"
	}
	return "fifo"
}

func numberedEntries(t testing.TB, from, to int) [][]byte {
	t.Helper()
	entries := make([][]byte, 0, to-from)
	for n := from; n < to; n++ {
		entries = append(entries, queueEntry(t, n))
	}
	return entries
}

// The tests below port the behavioural assertions of the retired segment
// store's tests that still have a subject (build doc Step 12 item 3):
// ordering, peek and peek(N), the walk, deletes by id, enumeration, the
// oversize refusal, and surviving a reopen.

// TestQueuePopOrder: fifo pops in push order, lifo in reverse, including
// across interleaved pushes and pops (segments_test.go's order tests).
func TestQueuePopOrder(t *testing.T) {
	cases := []struct {
		mode QueueMode
		want []int
	}{
		{QueueFIFO, []int{0, 1, 2, 3, 4, 5}},
		{QueueLIFO, []int{2, 5, 4, 3, 1, 0}},
	}
	for _, tc := range cases {
		t.Run(modeName(tc.mode), func(t *testing.T) {
			q, _ := newTestQueue(t, tc.mode)
			pushNumbered(t, q, 0, 3)
			got := []int{popNumber(t, q)}
			pushNumbered(t, q, 3, 6)
			for i := 0; i < 5; i++ {
				got = append(got, popNumber(t, q))
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("popped %v, want %v", got, tc.want)
			}
			if entry, err := q.Pop(ApplyInfo{}); err != nil || entry != nil {
				t.Fatalf("pop of an empty queue = %v, %v; want nil, nil", entry, err)
			}
			assertCursorsAgreeWithRange(t, q)
		})
	}
}

// peekAll collects what q.PeekN(n) hands its visitor, in order.
func peekAll(t testing.TB, q *QueueStore, n int) [][]byte {
	t.Helper()
	var peeked [][]byte
	if err := q.PeekN(n, func(entry []byte) error {
		peeked = append(peeked, entry)
		return nil
	}); err != nil {
		t.Fatalf("PeekN(%d): %v", n, err)
	}
	return peeked
}

func entryNumbers(t testing.TB, entries [][]byte) []int {
	t.Helper()
	var numbers []int
	for _, entry := range entries {
		numbers = append(numbers, entryNumber(t, entry))
	}
	return numbers
}

// TestQueuePeekDoesNotConsume: peek returns what pop would, repeatedly, and
// peek(N) the next N in pop order (peek_test.go, segments_close_dirty_test.go).
// It is also the regression pin that a fifo peeks oldest first and a lifo
// newest first.
func TestQueuePeekDoesNotConsume(t *testing.T) {
	cases := []struct {
		mode  QueueMode
		peekN []int
	}{
		{QueueFIFO, []int{0, 1, 2}},
		{QueueLIFO, []int{199, 198, 197}},
	}
	for _, tc := range cases {
		t.Run(modeName(tc.mode), func(t *testing.T) {
			q, _ := newTestQueue(t, tc.mode)
			pushNumbered(t, q, 0, 200)
			for i := 0; i < 3; i++ {
				entry, err := q.Peek()
				if err != nil || entry == nil {
					t.Fatalf("peek %d: %v (empty %v)", i, err, entry == nil)
				}
				if n := entryNumber(t, entry); n != tc.peekN[0] {
					t.Fatalf("peek %d returned %d, want %d", i, n, tc.peekN[0])
				}
			}
			got := entryNumbers(t, peekAll(t, q, 3))
			if !reflect.DeepEqual(got, tc.peekN) {
				t.Fatalf("PeekN(3) = %v, want %v", got, tc.peekN)
			}
			if n, err := q.Len(); err != nil || n != 200 {
				t.Fatalf("Len after peeking = %d (%v), want 200", n, err)
			}
			if n := popNumber(t, q); n != tc.peekN[0] {
				t.Fatalf("pop after peek returned %d, want the peeked %d", n, tc.peekN[0])
			}
		})
	}
}

// TestPeekNPastTheEnd, TestPeekNEmpty and TestPeekNSingle pin that asking for
// more than a queue holds is not an error.
func TestPeekNPastTheEnd(t *testing.T) {
	for _, mode := range []QueueMode{QueueFIFO, QueueLIFO} {
		t.Run(modeName(mode), func(t *testing.T) {
			q, _ := newTestQueue(t, mode)
			pushNumbered(t, q, 0, 3)
			if got := len(peekAll(t, q, 10)); got != 3 {
				t.Fatalf("PeekN(10) on 3 entries returned %d, want 3", got)
			}
		})
	}
}

func TestPeekNEmpty(t *testing.T) {
	for _, mode := range []QueueMode{QueueFIFO, QueueLIFO} {
		t.Run(modeName(mode), func(t *testing.T) {
			q, _ := newTestQueue(t, mode)
			if got := len(peekAll(t, q, 5)); got != 0 {
				t.Fatalf("PeekN(5) on an empty queue returned %d entries, want 0", got)
			}
		})
	}
}

func TestPeekNSingle(t *testing.T) {
	for _, mode := range []QueueMode{QueueFIFO, QueueLIFO} {
		t.Run(modeName(mode), func(t *testing.T) {
			q, _ := newTestQueue(t, mode)
			pushNumbered(t, q, 7, 8)
			if got := entryNumbers(t, peekAll(t, q, 5)); !reflect.DeepEqual(got, []int{7}) {
				t.Fatalf("PeekN(5) on one entry = %v, want [7]", got)
			}
		})
	}
}

// TestQueuePeekNReadsNoValuePastN: the walk stops after the n-th entry is
// handed over. It used to load the value of the (n+1)th before testing
// whether it was done, which on a large next entry is a wasted read of up to a
// document limit.
func TestQueuePeekNReadsNoValuePastN(t *testing.T) {
	for _, mode := range []QueueMode{QueueFIFO, QueueLIFO} {
		for _, n := range []int{1, 3} {
			t.Run(fmt.Sprintf("%s/n=%d", modeName(mode), n), func(t *testing.T) {
				q, kv := newTestQueue(t, mode)
				pushNumbered(t, q, 0, 10)
				kv.valueReads.Store(0)
				if got := len(peekAll(t, q, n)); got != n {
					t.Fatalf("PeekN(%d) returned %d entries", n, got)
				}
				if got := kv.valueReads.Load(); got != int64(n) {
					t.Fatalf("PeekN(%d) read %d values, want exactly %d", n, got, n)
				}
			})
		}
	}
	t.Run("Peek reads one", func(t *testing.T) {
		q, kv := newTestQueue(t, QueueFIFO)
		pushNumbered(t, q, 0, 10)
		kv.valueReads.Store(0)
		if _, err := q.Peek(); err != nil {
			t.Fatalf("Peek: %v", err)
		}
		if got := kv.valueReads.Load(); got != 1 {
			t.Fatalf("Peek read %d values, want 1", got)
		}
	})
}

// errVisitorRefused is what the refusing visitors of the PeekN tests return.
var errVisitorRefused = errors.New("visitor refused the entry")

// TestPeekNStopsWhenTheVisitorRefuses: a visitor that errors on the 2nd entry
// ends the walk there, with no value read past it and the error still
// matchable. One row per engine level: queue, heap and field.
func TestPeekNStopsWhenTheVisitorRefuses(t *testing.T) {
	t.Run("field", func(t *testing.T) {
		p, kv := newCountedFieldParent(t)
		ref := p.ref("p1", 1, FieldFIFO)
		p.pushEngineElements(ref, 0, 10)
		snap := p.kv.NewSnapshot()
		defer snap.Discard()
		kv.valueReads.Store(0)
		visits := 0
		err := FieldPeekN(snap, ref, 100, func(FieldEntry) error {
			visits++
			if visits == 2 {
				return errVisitorRefused
			}
			return nil
		})
		if !errors.Is(err, errVisitorRefused) {
			t.Fatalf("FieldPeekN returned %v, want the visitor's error", err)
		}
		if visits != 2 {
			t.Fatalf("visitor called %d times, want 2", visits)
		}
		if got := kv.valueReads.Load(); got != 2 {
			t.Fatalf("walk read %d values, want 2", got)
		}
	})
	t.Run("heap", func(t *testing.T) {
		heap, kv := newCountedHeap(t)
		pushHeapEntries(t, heap, 10)
		kv.valueReads.Store(0)
		visits := 0
		err := heap.PeekN(100, func([]byte) error {
			visits++
			if visits == 2 {
				return errVisitorRefused
			}
			return nil
		})
		if !errors.Is(err, errVisitorRefused) {
			t.Fatalf("PeekN returned %v, want the visitor's error", err)
		}
		if visits != 2 {
			t.Fatalf("visitor called %d times, want 2", visits)
		}
		if got := kv.valueReads.Load(); got != 2 {
			t.Fatalf("walk read %d values, want 2", got)
		}
	})
	for _, mode := range []QueueMode{QueueFIFO, QueueLIFO} {
		t.Run("queue/"+modeName(mode), func(t *testing.T) {
			q, kv := newTestQueue(t, mode)
			pushNumbered(t, q, 0, 10)
			kv.valueReads.Store(0)
			visits := 0
			err := q.PeekN(100, func([]byte) error {
				visits++
				if visits == 2 {
					return errVisitorRefused
				}
				return nil
			})
			if !errors.Is(err, errVisitorRefused) {
				t.Fatalf("PeekN returned %v, want the visitor's error", err)
			}
			if visits != 2 {
				t.Fatalf("visitor called %d times, want 2", visits)
			}
			if got := kv.valueReads.Load(); got != 2 {
				t.Fatalf("walk read %d values, want 2", got)
			}
		})
	}
}

// dropQueueEntries deletes the entries at positions from the range and leaves
// the state document alone, the disagreement ErrQueueCursorLost reports.
func dropQueueEntries(t *testing.T, q *QueueStore, positions ...uint64) {
	t.Helper()
	batch := q.kv.NewBatch()
	defer batch.Discard()
	for _, position := range positions {
		if err := batch.Delete(q.positionKey(position)); err != nil {
			t.Fatalf("deleting position %d: %v", position, err)
		}
	}
	if err := batch.Commit(false); err != nil {
		t.Fatalf("committing the deletes: %v", err)
	}
}

// TestQueuePeekNCursorLost: a state that counts entries the range does not
// hold is an error for a peek, as it is for a pop, never "empty" and never a
// short list that reads as the whole queue.
func TestQueuePeekNCursorLost(t *testing.T) {
	cases := []struct {
		name string
		drop []uint64
	}{
		{"range holds none", []uint64{0, 1, 2}},
		{"range holds one", []uint64{1, 2}},
	}
	for _, mode := range []QueueMode{QueueFIFO, QueueLIFO} {
		for _, tc := range cases {
			t.Run(modeName(mode)+"/"+tc.name, func(t *testing.T) {
				q, _ := newTestQueue(t, mode)
				pushNumbered(t, q, 0, 3)
				dropQueueEntries(t, q, tc.drop...)
				err := q.PeekN(5, func([]byte) error { return nil })
				if !errors.Is(err, ErrQueueCursorLost) {
					t.Fatalf("PeekN(5) = %v, want ErrQueueCursorLost", err)
				}
			})
		}
	}
}

// TestPeekNEqualsNPops is the definition of peek({limit: N}): after the cursor
// has moved, PeekN(k) is element for element what k Pops return on an
// identical copy. One row per engine; the heap row carries entries of equal
// priority, whose order is the entry-id bytes (spec §9) and which only this
// test shows peek and pop agree on.
func TestPeekNEqualsNPops(t *testing.T) {
	type engine struct {
		name  string
		build func(t *testing.T) (peekN func(k int) []string, pop func() string)
	}
	queueEngine := func(mode QueueMode) engine {
		return engine{name: modeName(mode), build: func(t *testing.T) (func(int) []string, func() string) {
			q, _ := newTestQueue(t, mode)
			pushNumbered(t, q, 0, 12)
			for i := 0; i < 4; i++ {
				popNumber(t, q)
			}
			pushNumbered(t, q, 12, 16)
			peekN := func(k int) []string {
				var seen []string
				for _, entry := range peekAll(t, q, k) {
					seen = append(seen, fmt.Sprint(entryNumber(t, entry)))
				}
				return seen
			}
			return peekN, func() string { return fmt.Sprint(popNumber(t, q)) }
		}}
	}
	heapEngine := engine{name: "heap", build: func(t *testing.T) (func(int) []string, func() string) {
		heap, _ := newCountedHeap(t)
		pushTiedHeapEntries(t, heap, 0)
		for i := 0; i < 4; i++ {
			if value, err := heap.PopItem(ApplyInfo{}); err != nil || value == nil {
				t.Fatalf("pop %d: %v (empty %v)", i, err, value == nil)
			}
		}
		pushTiedHeapEntries(t, heap, 1)
		peekN := func(k int) []string {
			var seen []string
			if err := heap.PeekN(k, func(entry []byte) error {
				seen = append(seen, heapEntryID(t, entry))
				return nil
			}); err != nil {
				t.Fatalf("PeekN(%d): %v", k, err)
			}
			return seen
		}
		return peekN, func() string {
			value, err := heap.PopItem(ApplyInfo{})
			if err != nil || value == nil {
				t.Fatalf("pop of the twin: %v (empty %v)", err, value == nil)
			}
			return heapEntryID(t, value)
		}
	}}
	engines := []engine{queueEngine(QueueFIFO), queueEngine(QueueLIFO), heapEngine}
	for _, eng := range engines {
		t.Run(eng.name, func(t *testing.T) {
			peekN, _ := eng.build(t)
			_, popTwin := eng.build(t)
			const k = 6
			peeked := peekN(k)
			if len(peeked) != k {
				t.Fatalf("PeekN(%d) returned %d entries", k, len(peeked))
			}
			for i, want := range peeked {
				if got := popTwin(); got != want {
					t.Fatalf("pop %d of the twin = %s, peek said %s (peeked %v)", i, got, want, peeked)
				}
			}
		})
	}
}

// TestQueueWalkIsOrderedReadOnlyAndSkipsDeletes: the walk is pop order, never
// consumes, and skips deleted entries (segments_walk_test.go), and
// IterateDump agrees with DocCount (enumerate_test.go).
func TestQueueWalkIsOrderedReadOnlyAndSkipsDeletes(t *testing.T) {
	cases := []struct {
		mode QueueMode
		want []int
	}{
		{QueueFIFO, []int{0, 1, 3, 4}},
		{QueueLIFO, []int{4, 3, 1, 0}},
	}
	for _, tc := range cases {
		t.Run(modeName(tc.mode), func(t *testing.T) {
			q, _ := newTestQueue(t, tc.mode)
			pushNumbered(t, q, 0, 5)
			if err := q.DeleteAtID("e2"); err != nil {
				t.Fatalf("DeleteAtID(e2): %v", err)
			}
			var walked []int
			if err := q.Walk(func(entry []byte) error {
				walked = append(walked, entryNumber(t, entry))
				return nil
			}); err != nil {
				t.Fatalf("Walk: %v", err)
			}
			if !reflect.DeepEqual(walked, tc.want) {
				t.Fatalf("Walk = %v, want %v", walked, tc.want)
			}
			var iterated []int
			if err := q.IterateDump(func(key, value []byte) error {
				if key != nil {
					t.Errorf("IterateDump gave key %q; a queue entry has none", key)
				}
				iterated = append(iterated, entryNumber(t, value))
				return nil
			}, ignoreDead); err != nil {
				t.Fatalf("IterateDump: %v", err)
			}
			if !reflect.DeepEqual(iterated, tc.want) {
				t.Fatalf("IterateDump = %v, want %v", iterated, tc.want)
			}
			if n, err := q.DocCount(); err != nil || n != int64(len(tc.want)) {
				t.Fatalf("DocCount = %d (%v), want %d", n, err, len(tc.want))
			}
			if n, err := q.Len(); err != nil || n != int64(len(tc.want)) {
				t.Fatalf("Len after the walks = %d (%v), want %d: a walk consumed entries", n, err, len(tc.want))
			}
			if err := q.DeleteAtID("e2"); err == nil {
				t.Fatal("deleting e2 twice succeeded; an id no entry has must be refused")
			}
			if err := q.DeleteAtPosition(4); err == nil {
				t.Fatal("DeleteAtPosition(4) of a four-entry queue succeeded; it must be refused as out of range")
			}
		})
	}
}

// TestQueueRefusesAnEntryLargerThanOneBatch: an entry the store cannot write
// in one atomic batch is refused as the batch's answer, not carried as a store
// failure, and leaves the queue as it was (segments_oversize_test.go).
func TestQueueRefusesAnEntryLargerThanOneBatch(t *testing.T) {
	fault := kvfault.New()
	fault.SetMaxBatchBytes(64 << 10)
	q := NewQueueStore(fault, storage.BucketRef{Slot: 1, DatabaseID: 1, BucketID: 1}, new(OwedApplies), QueueFIFO)
	pushNumbered(t, q, 0, 1)
	_, err := q.Push(ApplyInfo{Seq: 0}, make([]byte, 128<<10))
	if !errors.Is(err, storage.ErrBatchTooLarge) {
		t.Fatalf("pushing an entry over the batch bound: %v, want storage.ErrBatchTooLarge", err)
	}
	if errors.Is(err, ErrApplyFailed) {
		t.Fatalf("an oversize entry was carried as a store failure: %v", err)
	}
	if n, err := q.Len(); err != nil || n != 1 {
		t.Fatalf("Len after the refusal = %d (%v), want 1", n, err)
	}
	if n := popNumber(t, q); n != 0 {
		t.Fatalf("pop after the refusal took %d, want 0", n)
	}
}

// TestQueueSurvivesReopen: a queue built again over the same store pops what
// was left, in order, and pushes after it (segments_test.go's reopen case).
func TestQueueSurvivesReopen(t *testing.T) {
	for _, mode := range []QueueMode{QueueFIFO, QueueLIFO} {
		t.Run(modeName(mode), func(t *testing.T) {
			q, kv := newTestQueue(t, mode)
			pushNumbered(t, q, 0, 4)
			popNumber(t, q)
			reopened := NewQueueStore(kv, storage.BucketRef{Slot: 1, DatabaseID: 1, BucketID: 1}, new(OwedApplies), mode)
			pushNumbered(t, reopened, 4, 5)
			var got []int
			for i := 0; i < 4; i++ {
				got = append(got, popNumber(t, reopened))
			}
			want := []int{1, 2, 3, 4}
			if mode == QueueLIFO {
				want = []int{4, 2, 1, 0}
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("after reopening popped %v, want %v", got, want)
			}
		})
	}
}

// The PopN tests below are pop-n's store contract (pop-n spec §11, build doc
// steps 1-3). Each property is one table over the engines: a popNSubject puts
// a queue, a heap and a store field behind the same verbs.

// popNState is the part of a store's state a pop moves: the cursors, the
// count and the logical size (a store field's count, and its parent's size).
type popNState struct {
	Head, Tail            uint64
	Entries, LogicalBytes int64
}

// popNSubject is one store a PopN test drives.
type popNSubject struct {
	kv *countingKV
	// ids names entries by their identity, not their bytes: an entry is
	// encoded from a map, so twins can hold one entry in different bytes.
	ids func(entries [][]byte) []string
	// push adds entries numbered from to to-1, in that order.
	push  func(from, to int)
	popN  func(info ApplyInfo, n int, visit func(entry []byte) error) (int, error)
	pop   func() []byte
	peekN func(n int) [][]byte
	state func() popNState
	owes  func() bool
	// dropAllBut deletes every entry from the range but the first keep in
	// pop order, leaving the state document alone: the disagreement the
	// cursor-lost rule reports.
	dropAllBut func(keep int)
	// plant writes keys into the store's own key range outside its entry
	// namespace and returns them with their values.
	plant func() map[string][]byte
	// lost is the engine's cursor-lost sentinel.
	lost error
	// nested names the entries whose nested stores still exist; nil where
	// the engine has none.
	nested func() []string
}

// withoutLeaseTime is a bucket engine's PopN as pop-n's tests drive it: no
// lease time, so no return step, and the count it removed.
func withoutLeaseTime(popN func(ApplyInfo, int, int64, func([]byte) error) (PopResult, error)) func(ApplyInfo, int, func([]byte) error) (int, error) {
	return func(info ApplyInfo, n int, visit func([]byte) error) (int, error) {
		popped, err := popN(info, n, 0, visit)
		return popped.Removed, err
	}
}

type popNEngine struct {
	name  string
	build func(t *testing.T) popNSubject
}

func queuePopNEngine(mode QueueMode) popNEngine {
	return popNEngine{name: modeName(mode), build: func(t *testing.T) popNSubject {
		q, kv := newTestQueue(t, mode)
		return popNSubject{
			kv: kv,
			ids: func(entries [][]byte) []string {
				names := make([]string, 0, len(entries))
				for _, n := range entryNumbers(t, entries) {
					names = append(names, fmt.Sprint(n))
				}
				return names
			},
			push: func(from, to int) { pushNumbered(t, q, from, to) },
			popN: withoutLeaseTime(q.PopN),
			pop: func() []byte {
				entry, err := q.Pop(ApplyInfo{})
				if err != nil {
					t.Fatalf("Pop: %v", err)
				}
				return entry
			},
			peekN: func(n int) [][]byte { return peekAll(t, q, n) },
			state: func() popNState {
				st := queueState(t, q)
				return popNState{Head: st.Head, Tail: st.Tail, Entries: st.Entries, LogicalBytes: st.LogicalBytes}
			},
			owes:       q.Carry().Owes,
			dropAllBut: func(keep int) { dropAllButFirst(t, kv, q.fromCursor, queueState(t, q), keep) },
			plant: func() map[string][]byte {
				return plantOutsideTheEntries(t, kv, q.slot, q.id)
			},
			lost: ErrQueueCursorLost,
		}
	}}
}

// dropAllButFirst deletes every key cursor walks but the first keep, in one
// batch that does not touch the state document.
func dropAllButFirst(t *testing.T, kv storage.KV, cursor func(storage.KVSnapshot, BucketState) storage.KVIterator, st BucketState, keep int) {
	t.Helper()
	snap := kv.NewSnapshot()
	defer snap.Discard()
	it := cursor(snap, st)
	defer it.Close()
	batch := kv.NewBatch()
	defer batch.Discard()
	for walked := 0; it.Valid(); it.Next() {
		walked++
		if walked <= keep {
			continue
		}
		if err := batch.Delete(append([]byte(nil), it.Key()...)); err != nil {
			t.Fatalf("deleting %x: %v", it.Key(), err)
		}
	}
	if err := it.Err(); err != nil {
		t.Fatalf("walking the entries to drop: %v", err)
	}
	if err := batch.Commit(false); err != nil {
		t.Fatalf("committing the drops: %v", err)
	}
}

// plantOutsideTheEntries writes keys into bucket id's range under namespace
// bytes on both sides of storage.NSQueue: 0x03 below it, the state namespace
// 0x05 (beside the state document, never over it) and 0x0D, the byte the
// claim/ack design proposes for claimed entries, above it. A reverse walk
// widened to the bucket reaches the first; a forward one the others.
func plantOutsideTheEntries(t *testing.T, kv storage.KV, slot uint16, id uint64) map[string][]byte {
	t.Helper()
	planted := map[string][]byte{
		string(storage.Key(slot, 1, id, storage.NSIndex, []byte("planted-below"))):   []byte("index"),
		string(storage.Key(slot, 1, id, storage.NSState, []byte("planted-state"))):   []byte("state"),
		string(storage.Key(slot, 1, id, 0x0D, []byte{0, 0, 0, 0, 0, 0, 0, 1})):       []byte("claimed"),
		string(storage.Key(slot, 1, id, 0x0D, []byte{0xff, 0xff, 0xff, 0xff, 0xff})): []byte("claimed-high"),
	}
	batch := kv.NewBatch()
	defer batch.Discard()
	for key, value := range planted {
		if err := batch.Set([]byte(key), value); err != nil {
			t.Fatalf("planting %x: %v", key, err)
		}
	}
	if err := batch.Commit(false); err != nil {
		t.Fatalf("committing the planted keys: %v", err)
	}
	return planted
}

// heapPopNEngine is a heap whose entry n has the priority priorityOf(n). Its
// ids sort in an order other than push order, so where priorities tie the
// entry-id order is what is exercised.
func heapPopNEngine(name string, priorityOf func(n int) int64) popNEngine {
	return popNEngine{name: name, build: func(t *testing.T) popNSubject {
		heap, kv := newCountedHeap(t)
		walk := func(snap storage.KVSnapshot, _ BucketState) storage.KVIterator {
			prefix := heap.elementPrefix()
			it := snap.NewIterator(storage.IterOptions{Lower: prefix, Upper: prefixEnd(prefix)})
			it.Seek(nil)
			return it
		}
		state := func() BucketState {
			snap := kv.NewSnapshot()
			defer snap.Discard()
			st, err := LoadBucketState(snap, heap.slot, 1, heap.id)
			if err != nil {
				t.Fatalf("loading the heap's state: %v", err)
			}
			return st
		}
		return popNSubject{
			kv: kv,
			ids: func(entries [][]byte) []string {
				names := make([]string, 0, len(entries))
				for _, entry := range entries {
					names = append(names, heapEntryID(t, entry))
				}
				return names
			},
			push: func(from, to int) {
				for n := from; n < to; n++ {
					id := fmt.Sprintf("x%d-n%d", (n*7)%10, n)
					if _, err := heap.PushItem(ApplyInfo{}, priorityOf(n), map[string]interface{}{"_id_": id}); err != nil {
						t.Fatalf("PushItem(%s): %v", id, err)
					}
				}
			},
			popN: withoutLeaseTime(heap.PopN),
			pop: func() []byte {
				value, err := heap.PopItem(ApplyInfo{})
				if err != nil {
					t.Fatalf("PopItem: %v", err)
				}
				return value
			},
			peekN: func(n int) [][]byte {
				var peeked [][]byte
				if err := heap.PeekN(n, func(entry []byte) error {
					peeked = append(peeked, entry)
					return nil
				}); err != nil {
					t.Fatalf("PeekN(%d): %v", n, err)
				}
				return peeked
			},
			state: func() popNState {
				st := state()
				return popNState{Head: st.Head, Tail: st.Tail, Entries: st.Entries, LogicalBytes: st.LogicalBytes}
			},
			owes:       heap.Carry().Owes,
			dropAllBut: func(keep int) { dropAllButFirst(t, kv, walk, state(), keep) },
			plant: func() map[string][]byte {
				return plantOutsideTheEntries(t, kv, heap.slot, heap.id)
			},
			lost: ErrQueueCursorLost,
		}
	}}
}

// bucketPopNEngines are the bucket engines: fifo, lifo and heap.
func bucketPopNEngines() []popNEngine {
	return []popNEngine{
		queuePopNEngine(QueueFIFO),
		queuePopNEngine(QueueLIFO),
		heapPopNEngine("heap/tied", func(int) int64 { return 5 }),
		heapPopNEngine("heap/mixed", func(n int) int64 { return int64(n % 3) }),
	}
}

// popNEngines are every engine a pop runs on: the bucket engines and the
// ordered store fields.
func popNEngines() []popNEngine {
	return append(bucketPopNEngines(), fieldPopNEngines(false)...)
}

// moveTheCursor gives s entries behind a cursor that has moved: push 5, pop
// 2, push 3, leaving six.
func moveTheCursor(t *testing.T, s popNSubject) {
	t.Helper()
	s.push(0, 5)
	for i := 0; i < 2; i++ {
		if s.pop() == nil {
			t.Fatalf("pop %d of the setup found the store empty", i)
		}
	}
	s.push(5, 8)
}

// popNCollect runs s.popN with a visitor that keeps every entry.
func popNCollect(s popNSubject, info ApplyInfo, n int) ([][]byte, int, error) {
	var visited [][]byte
	removed, err := s.popN(info, n, func(entry []byte) error {
		visited = append(visited, entry)
		return nil
	})
	return visited, removed, err
}

// TestPopNEqualsPeekNThenKPops is the definition of pop({limit: N}) (spec
// §1): on twin stores, PopN(k) returns exactly what PeekN(k) of the other
// returns, and afterwards the first holds exactly what k Pops leave on a
// third.
func TestPopNEqualsPeekNThenKPops(t *testing.T) {
	assertPopNEqualsPeekNThenKPops(t, bucketPopNEngines(), nil)
}

// assertPopNEqualsPeekNThenKPops is the definition's check over engines.
// wantFirst, by engine name, is the entry the pop takes first, said outright:
// a direction a pop and a peek share (cursorSeek) would flip both twins
// unseen.
func assertPopNEqualsPeekNThenKPops(t *testing.T, engines []popNEngine, wantFirst map[string]string) {
	t.Helper()
	for _, engine := range engines {
		for _, k := range []int{1, 4, 6, 9} {
			t.Run(fmt.Sprintf("%s/k=%d", engine.name, k), func(t *testing.T) {
				popped, peeked, popTwin := engine.build(t), engine.build(t), engine.build(t)
				for _, s := range []popNSubject{popped, peeked, popTwin} {
					moveTheCursor(t, s)
				}
				want := popped.ids(peeked.peekN(k))
				visited, removed, err := popNCollect(popped, ApplyInfo{}, k)
				if err != nil {
					t.Fatalf("PopN(%d): %v", k, err)
				}
				got := popped.ids(visited)
				if removed != len(want) || !reflect.DeepEqual(got, want) {
					t.Fatalf("PopN(%d) removed %d and returned %v; PeekN(%d) of the twin returned %v", k, removed, got, k, want)
				}
				if first, ok := wantFirst[engine.name]; ok && got[0] != first {
					t.Fatalf("PopN(%d) took %s first, want %s", k, got[0], first)
				}
				for i := 0; i < k; i++ {
					popTwin.pop()
				}
				left, wantLeft := popped.ids(popped.peekN(1000)), popTwin.ids(popTwin.peekN(1000))
				if !reflect.DeepEqual(left, wantLeft) {
					t.Fatalf("after PopN(%d) the store holds %v; after %d Pops the twin holds %v", k, left, k, wantLeft)
				}
			})
		}
	}
}

// TestPopNStateEqualsKPops: the state PopN(k) leaves is, field for field,
// the state k Pops leave. The drained rows pop a lifo through position 0 as
// the last entry: k pops leave Tail 0, a single cursor move from the last
// position would leave it where it was loaded.
func TestPopNStateEqualsKPops(t *testing.T) {
	type shape struct {
		name  string
		setup func(t *testing.T, s popNSubject)
		k     int
	}
	shapes := []shape{
		{"moved", moveTheCursor, 4},
		{"drained", func(_ *testing.T, s popNSubject) { s.push(0, 3) }, 3},
	}
	for _, engine := range append(popNEngines(), fieldPopNEngines(true)...) {
		for _, sh := range shapes {
			t.Run(engine.name+"/"+sh.name, func(t *testing.T) {
				popped, popTwin := engine.build(t), engine.build(t)
				sh.setup(t, popped)
				sh.setup(t, popTwin)
				if _, removed, err := popNCollect(popped, ApplyInfo{}, sh.k); err != nil || removed != sh.k {
					t.Fatalf("PopN(%d) removed %d: %v", sh.k, removed, err)
				}
				for i := 0; i < sh.k; i++ {
					popTwin.pop()
				}
				if got, want := popped.state(), popTwin.state(); got != want {
					t.Fatalf("state after PopN(%d) = %+v; after %d Pops = %+v", sh.k, got, sh.k, want)
				}
			})
		}
	}
}

// TestPopNIsOneCommit: PopN(5) is one batch; five Pops are five.
func TestPopNIsOneCommit(t *testing.T) {
	for _, engine := range popNEngines() {
		t.Run(engine.name, func(t *testing.T) {
			popped, popTwin := engine.build(t), engine.build(t)
			popped.push(0, 5)
			popTwin.push(0, 5)
			before := popped.kv.commits.Load()
			if _, removed, err := popNCollect(popped, ApplyInfo{}, 5); err != nil || removed != 5 {
				t.Fatalf("PopN(5) removed %d: %v", removed, err)
			}
			if got := popped.kv.commits.Load() - before; got != 1 {
				t.Fatalf("PopN(5) committed %d batches, want 1", got)
			}
			before = popTwin.kv.commits.Load()
			for i := 0; i < 5; i++ {
				popTwin.pop()
			}
			if got := popTwin.kv.commits.Load() - before; got != 5 {
				t.Fatalf("five Pops committed %d batches, want 5", got)
			}
		})
	}
}

// errPopRefused is the refusing visitor's sentinel in the PopN tests.
var errPopRefused = errors.New("visitor refused the pop")

// TestPopNRefusedByTheVisitorRemovesNothing is the data-loss requirement at
// the store (spec O-6): a visitor that refuses the 3rd of 5 entries gets its
// error back, and nothing is committed, so the next PopN returns the same
// entries.
func TestPopNRefusedByTheVisitorRemovesNothing(t *testing.T) {
	for _, engine := range append(popNEngines(), fieldPopNEngines(true)...) {
		t.Run(engine.name, func(t *testing.T) {
			s := engine.build(t)
			s.push(0, 5)
			var nestedBefore []string
			if s.nested != nil {
				nestedBefore = s.nested()
			}
			before := s.ids(s.peekN(5))
			stateBefore := s.state()
			commits := s.kv.commits.Load()
			visits := 0
			removed, err := s.popN(ApplyInfo{}, 5, func([]byte) error {
				visits++
				if visits == 3 {
					return errPopRefused
				}
				return nil
			})
			if !errors.Is(err, errPopRefused) {
				t.Fatalf("PopN = %d, %v; want the visitor's error", removed, err)
			}
			if removed != 0 {
				t.Fatalf("a refused PopN reported %d removed, want 0", removed)
			}
			if got := s.kv.commits.Load() - commits; got != 0 {
				t.Fatalf("a refused PopN committed %d batches, want none", got)
			}
			if got := s.state(); got != stateBefore || got.Entries != 5 {
				t.Fatalf("state after the refusal = %+v, want %+v (5 entries)", got, stateBefore)
			}
			if s.nested != nil {
				if got := s.nested(); !reflect.DeepEqual(got, nestedBefore) {
					t.Fatalf("after the refusal the entries with nested stores are %v, want %v", got, nestedBefore)
				}
			}
			visited, removed, err := popNCollect(s, ApplyInfo{}, 5)
			if got := s.ids(visited); err != nil || removed != 5 || !reflect.DeepEqual(got, before) {
				t.Fatalf("the next PopN(5) = %v (%d, %v), want the five entries %v", got, removed, err, before)
			}
		})
	}
}

// TestPopNRefusalIsNotAStoreFailure: the visitor's refusal is the operation's
// answer, never carried as the store failing; a value the store fails to read
// still is a store failure.
func TestPopNRefusalIsNotAStoreFailure(t *testing.T) {
	s := queuePopNEngine(QueueFIFO).build(t)
	s.push(0, 5)
	_, err := s.popN(ApplyInfo{Seq: 1, Record: true}, 5, func([]byte) error { return errPopRefused })
	if !errors.Is(err, errPopRefused) || errors.Is(err, ErrApplyFailed) {
		t.Fatalf("a refused PopN = %v; want the visitor's error and not ErrApplyFailed", err)
	}
	if s.owes() {
		t.Fatal("a refused PopN left the bucket owing an apply")
	}
	s.kv.failValues.Store(true)
	_, removed, err := popNCollect(s, ApplyInfo{Seq: 2, Record: true}, 5)
	s.kv.failValues.Store(false)
	if !errors.Is(err, ErrApplyFailed) || !errors.Is(err, errValueReadFailed) {
		t.Fatalf("PopN over a failing read = %d, %v; want ErrApplyFailed wrapping the read error", removed, err)
	}
	if got := s.state().Entries; got != 5 {
		t.Fatalf("after the failed read the store counts %d entries, want 5", got)
	}
}

// TestPopNErrorReadsNaturallyForAPlainPop: a pop() the user gave no limit to
// does not fail with "(limit 1)" in its message; a pop of several names how
// many it was asked for.
func TestPopNErrorReadsNaturallyForAPlainPop(t *testing.T) {
	for _, engine := range bucketPopNEngines() {
		t.Run(engine.name, func(t *testing.T) {
			s := engine.build(t)
			s.push(0, 5)
			s.kv.failValues.Store(true)
			defer s.kv.failValues.Store(false)
			_, _, plain := popNCollect(s, ApplyInfo{}, 1)
			_, _, several := popNCollect(s, ApplyInfo{}, 3)
			if plain == nil || several == nil {
				t.Fatalf("PopN over a failing read returned %v and %v", plain, several)
			}
			if strings.Contains(plain.Error(), "limit") {
				t.Errorf("a plain pop's error names a limit nobody gave: %v", plain)
			}
			if !strings.Contains(several.Error(), "(limit 3)") {
				t.Errorf("a pop of three does not name its limit: %v", several)
			}
		})
	}
}

// TestPopNReadsNoValuePastN: the walk reads exactly n values.
func TestPopNReadsNoValuePastN(t *testing.T) {
	for _, engine := range popNEngines() {
		for _, n := range []int{1, 3} {
			t.Run(fmt.Sprintf("%s/n=%d", engine.name, n), func(t *testing.T) {
				s := engine.build(t)
				s.push(0, 5)
				s.kv.valueReads.Store(0)
				s.kv.visits.Store(0)
				if _, removed, err := popNCollect(s, ApplyInfo{}, n); err != nil || removed != n {
					t.Fatalf("PopN(%d) removed %d: %v", n, removed, err)
				}
				if got := s.kv.valueReads.Load(); got != int64(n) {
					t.Fatalf("PopN(%d) read %d values, want exactly %d", n, got, n)
				}
				// A step past the n-th entry walks whatever delete markers lie
				// beyond it (a lifo keeps a run below its cursor, BUG-0580).
				if got := s.kv.visits.Load(); got != int64(n) {
					t.Fatalf("PopN(%d) visited %d stored versions, want exactly %d: it stepped past the n-th entry", n, got, n)
				}
			})
		}
	}
}

// TestPopNCursorLost: a state that counts entries its range does not hold is
// an error, never "empty" and never a short pop that commits what it found.
func TestPopNCursorLost(t *testing.T) {
	cases := []struct {
		name string
		keep int
		n    int
	}{
		{"range holds none", 0, 5},
		{"range holds one", 1, 5},
		{"range holds none, pop one", 0, 1},
	}
	for _, engine := range popNEngines() {
		for _, tc := range cases {
			t.Run(engine.name+"/"+tc.name, func(t *testing.T) {
				s := engine.build(t)
				s.push(0, 3)
				s.dropAllBut(tc.keep)
				before := s.state()
				_, removed, err := popNCollect(s, ApplyInfo{}, tc.n)
				if !errors.Is(err, s.lost) {
					t.Fatalf("PopN(%d) = %d, %v; want %v", tc.n, removed, err, s.lost)
				}
				if got := s.state(); got != before {
					t.Fatalf("state after the refusal = %+v, want it unchanged %+v", got, before)
				}
			})
		}
	}
}

// TestPopNReplayIsIdempotent: a replayed PopN at or below the mark removes
// nothing, and a PopN whose commit failed re-applies to the same entries.
func TestPopNReplayIsIdempotent(t *testing.T) {
	for _, engine := range popNEngines() {
		t.Run(engine.name, func(t *testing.T) {
			s := engine.build(t)
			s.push(0, 5)
			if _, removed, err := popNCollect(s, ApplyInfo{Seq: 7}, 2); err != nil || removed != 2 {
				t.Fatalf("PopN(2) at sequence 7 removed %d: %v", removed, err)
			}
			replayed, removed, err := popNCollect(s, ApplyInfo{Seq: 7, Replay: true}, 2)
			if err != nil || removed != 0 || len(replayed) != 0 {
				t.Fatalf("the replay of sequence 7 removed %d and visited %d (%v), want 0, 0, nil", removed, len(replayed), err)
			}
			if got := s.state().Entries; got != 3 {
				t.Fatalf("after the replay the store counts %d entries, want 3", got)
			}

			twin := engine.build(t)
			twin.push(0, 5)
			want := twin.ids(twin.peekN(2))
			fail := func(kv storage.KV, _ uint16, _ uint64, seq uint64) error {
				if kv == storage.KV(twin.kv) && seq == 9 {
					return errors.New("injected commit failure")
				}
				return nil
			}
			ApplyFaultHook.Store(&fail)
			_, _, err = popNCollect(twin, ApplyInfo{Seq: 9}, 2)
			ApplyFaultHook.Store(nil)
			if !errors.Is(err, ErrApplyFailed) {
				t.Fatalf("PopN(2) whose commit is dropped = %v, want ErrApplyFailed", err)
			}
			visited, removed, err := popNCollect(twin, ApplyInfo{Seq: 9}, 2)
			if got := twin.ids(visited); err != nil || removed != 2 || !reflect.DeepEqual(got, want) {
				t.Fatalf("the re-apply of sequence 9 = %v (%d, %v), want the same two entries %v", got, removed, err, want)
			}
		})
	}
}

// TestPopNReadsOnlyTheEntryNamespace pins that PopN walks the entry namespace
// (storage.NSQueue) and nothing else of the bucket's range: keys planted
// under other namespace bytes, 0x0D among them, are never visited, removed or
// counted. It is claim/ack's precondition that pop never sees claimed
// entries (spec §11, §19 C-10).
func TestPopNReadsOnlyTheEntryNamespace(t *testing.T) {
	for _, engine := range popNEngines() {
		t.Run(engine.name, func(t *testing.T) {
			s := engine.build(t)
			s.push(0, 5)
			entries := s.ids(s.peekN(5))
			planted := s.plant()
			visited, removed, err := popNCollect(s, ApplyInfo{}, 1000)
			if err != nil || removed != 5 {
				t.Fatalf("PopN(1000) removed %d: %v, want the 5 entries", removed, err)
			}
			if got := s.ids(visited); !reflect.DeepEqual(got, entries) {
				t.Fatalf("PopN(1000) visited %v, want only the entries %v", got, entries)
			}
			snap := s.kv.NewSnapshot()
			defer snap.Discard()
			for key, value := range planted {
				stored, err := snap.Get([]byte(key))
				if err != nil || !bytes.Equal(stored, value) {
					t.Fatalf("planted key %x = %q (%v), want %q", key, stored, err, value)
				}
			}
			if left := s.state().Entries; left != 0 {
				t.Fatalf("after PopN(1000) the store counts %d entries, want 0", left)
			}
		})
	}
}

// lifoWithAKeptEntryAndChurn is BUG-0580's shape on a real store: one old
// entry kept at the bottom of a lifo, then churn pairs of push and pop above
// it, with no compaction asked for. The markers the pops leave are Badger
// tombstones, so only the real store shows what a read must walk.
func lifoWithAKeptEntryAndChurn(t *testing.T, churn int) (*QueueStore, *countingKV) {
	t.Helper()
	store, err := storage.OpenBadgerKV(t.TempDir(), storage.BadgerTuning{MemTableMB: 32}, nil)
	if err != nil {
		t.Fatalf("opening the store: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("closing the store: %v", err)
		}
	})
	kv := &countingKV{KV: store}
	q := NewQueueStore(kv, storage.BucketRef{Slot: 1, DatabaseID: 1, BucketID: 1}, new(OwedApplies), QueueLIFO)
	pushNumbered(t, q, 0, 1)
	for pair := 1; pair <= churn; pair++ {
		pushNumbered(t, q, pair, pair+1)
		if popped := popNumber(t, q); popped != pair {
			t.Fatalf("churn pair %d popped entry %d, want the one it pushed", pair, popped)
		}
	}
	return q, kv
}

// maxVisitsBesideTheKeptEntry is "a small constant" in BUG-0580's acceptance
// criteria: the seek onto the kept entry and a step or two, independent of how
// many delete markers the churn left.
const maxVisitsBesideTheKeptEntry = 4

// TestLifoPeekAfterPushPopChurnDoesNotWalkTheWall is BUG-0580: a lifo peek
// writes nothing, so it paid every marker between the cursor and the kept entry
// every time (1,999 and 19,999 stored versions at these sizes before the fix).
// The assertion counts visited versions, not time (T-06).
func TestLifoPeekAfterPushPopChurnDoesNotWalkTheWall(t *testing.T) {
	for _, churn := range []int{1_000, 10_000} {
		t.Run(fmt.Sprintf("churn=%d", churn), func(t *testing.T) {
			q, kv := lifoWithAKeptEntryAndChurn(t, churn)
			for peek := 1; peek <= 2; peek++ {
				kv.visits.Store(0)
				if err := q.PeekN(1, func(entry []byte) error {
					if n := entryNumber(t, entry); n != 0 {
						t.Fatalf("peek %d saw entry %d, want the kept entry 0", peek, n)
					}
					return nil
				}); err != nil {
					t.Fatalf("peek %d: %v", peek, err)
				}
				if visits := kv.visits.Load(); visits > maxVisitsBesideTheKeptEntry {
					t.Fatalf("peek %d after %d push+pop pairs visited %d stored versions, want at most %d: a lifo walks the delete markers between its cursor and the next live entry (BUG-0580)",
						peek, churn, visits, maxVisitsBesideTheKeptEntry)
				}
			}
		})
	}
}

// TestLifoPopAfterPushPopChurnDoesNotWalkTheWall is BUG-0580 for the first pop,
// which paid the run once.
func TestLifoPopAfterPushPopChurnDoesNotWalkTheWall(t *testing.T) {
	for _, churn := range []int{1_000, 10_000} {
		t.Run(fmt.Sprintf("churn=%d", churn), func(t *testing.T) {
			q, kv := lifoWithAKeptEntryAndChurn(t, churn)
			kv.visits.Store(0)
			if popped := popNumber(t, q); popped != 0 {
				t.Fatalf("the pop took entry %d, want the kept entry 0", popped)
			}
			if visits := kv.visits.Load(); visits > maxVisitsBesideTheKeptEntry {
				t.Fatalf("the first pop after %d push+pop pairs visited %d stored versions, want at most %d: a lifo walks the delete markers between its cursor and the next live entry (BUG-0580)",
					churn, visits, maxVisitsBesideTheKeptEntry)
			}
		})
	}
}

// TestLifoPopNReseeksAfterTheCursorJumps guards the re-seek in takeN: a PopN
// that drains a run and has more to take must seek again at the jumped cursor.
// Stepping the old iterator instead walks every delete marker between the run
// and the kept entry, and every answer is still right, so only a visit count
// shows it.
func TestLifoPopNReseeksAfterTheCursorJumps(t *testing.T) {
	q, kv := lifoWithAKeptEntryAndChurn(t, 10_000)
	pushNumbered(t, q, 10_001, 10_002)
	kv.visits.Store(0)
	var taken []int
	popped, err := q.PopN(ApplyInfo{}, 2, 0, func(entry []byte) error {
		taken = append(taken, entryNumber(t, entry))
		return nil
	})
	if err != nil || popped.Removed != 2 || len(taken) != 2 || taken[0] != 10_001 || taken[1] != 0 {
		t.Fatalf("PopN(2) = %d, %v, took %v, want the pushed entry 10001 and then the kept entry 0", popped.Removed, err, taken)
	}
	if visits := kv.visits.Load(); visits > 2*maxVisitsBesideTheKeptEntry {
		t.Fatalf("PopN(2) across a cursor jump visited %d stored versions, want at most %d: the walk stepped over the delete markers instead of seeking at the jumped cursor",
			visits, 2*maxVisitsBesideTheKeptEntry)
	}
}
