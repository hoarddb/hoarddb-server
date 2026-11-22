package store

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"reflect"
	"testing"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/storage/kvfault"
)

const (
	fieldTestSlot uint16 = 7
	fieldTestID   uint64 = 11
)

// fieldParent is a hash bucket on a real Badger store with the operations of
// its field stores applied through its own Apply, as the server does.
type fieldParent struct {
	t    *testing.T
	dir  string
	kv   storage.KV
	hash *HashStore
}

func openFieldParent(t *testing.T, dir string) *fieldParent {
	t.Helper()
	kv, err := storage.OpenBadgerKV(dir, storage.BadgerTuning{MemTableMB: 32}, nil)
	if err != nil {
		t.Fatalf("opening the store: %v", err)
	}
	return &fieldParent{t: t, dir: dir, kv: kv, hash: NewHashStore(kv, fieldTestSlot, 1, fieldTestID, new(OwedApplies))}
}

func newFieldParent(t *testing.T) *fieldParent {
	t.Helper()
	p := openFieldParent(t, t.TempDir())
	t.Cleanup(func() { _ = p.kv.Close() })
	return p
}

// newFaultFieldParent is a parent on the fault store, whose batch bound a
// test can set.
func newFaultFieldParent(t *testing.T) (*fieldParent, *kvfault.Store) {
	t.Helper()
	kv := kvfault.New()
	return &fieldParent{t: t, kv: kv, hash: NewHashStore(kv, fieldTestSlot, 1, fieldTestID, new(OwedApplies))}, kv
}

// newCountedFieldParent is a parent over a countingKV, so a test sees the
// values a walk reads and the batches it commits.
func newCountedFieldParent(t *testing.T) (*fieldParent, *countingKV) {
	t.Helper()
	kv := &countingKV{KV: kvfault.New()}
	return &fieldParent{t: t, kv: kv, hash: NewHashStore(kv, fieldTestSlot, 1, fieldTestID, new(OwedApplies))}, kv
}

func (p *fieldParent) reopen() {
	p.t.Helper()
	if err := p.kv.Close(); err != nil {
		p.t.Fatalf("closing the store: %v", err)
	}
	reopened := openFieldParent(p.t, p.dir)
	p.kv, p.hash = reopened.kv, reopened.hash
}

// apply runs op inside the parent's Apply. Keyed follows the field's engine,
// as the server sets it.
func (p *fieldParent) apply(info ApplyInfo, keyed bool, op func(w FieldWrite) error) (bool, error) {
	return Apply(ApplyRequest{KV: p.kv, Slot: fieldTestSlot, DatabaseID: 1, ID: fieldTestID, Keyed: keyed, Info: info, Owed: p.hash.Carry(),
		Op: func(snap storage.KVSnapshot, st *BucketState, b storage.KVBatch) error {
			return op(FieldWrite{Snap: snap, State: st, Batch: b})
		}})
}

func (p *fieldParent) ref(documentKey string, fieldID uint16, engine FieldEngine) FieldRef {
	p.t.Helper()
	owner, err := FieldOwnerPrefix(fieldTestSlot, 1, fieldTestID, documentKey)
	if err != nil {
		p.t.Fatalf("owner %q: %v", documentKey, err)
	}
	return FieldRef{Owner: owner, FieldID: fieldID, Engine: engine}
}

func (p *fieldParent) push(ref FieldRef, n int) uint64 {
	p.t.Helper()
	var position uint64
	if _, err := p.apply(ApplyInfo{}, false, func(w FieldWrite) error {
		var err error
		position, err = FieldPush(w, ref, queueEntry(p.t, n))
		return err
	}); err != nil {
		p.t.Fatalf("pushing %d: %v", n, err)
	}
	return position
}

func (p *fieldParent) heapPush(ref FieldRef, priority int64, n int) {
	p.t.Helper()
	if _, err := p.apply(ApplyInfo{}, false, func(w FieldWrite) error {
		return FieldHeapPush(w, ref, priority, fmt.Sprintf("e%d", n), queueEntry(p.t, n))
	}); err != nil {
		p.t.Fatalf("heap-pushing %d at %d: %v", n, priority, err)
	}
}

func (p *fieldParent) put(ref FieldRef, elementKey string, n int) {
	p.t.Helper()
	if _, err := p.apply(ApplyInfo{}, true, func(w FieldWrite) error {
		return FieldPut(w, ref, elementKey, queueEntry(p.t, n))
	}); err != nil {
		p.t.Fatalf("putting %q: %v", elementKey, err)
	}
}

// pop returns the popped element's n, or -1 for an empty instance.
func (p *fieldParent) pop(ref FieldRef) int {
	p.t.Helper()
	var popped FieldEntry
	var found bool
	if _, err := p.apply(ApplyInfo{}, false, func(w FieldWrite) error {
		var err error
		popped, found, err = FieldPop(w, ref)
		return err
	}); err != nil {
		p.t.Fatalf("popping: %v", err)
	}
	if !found {
		return -1
	}
	return entryNumber(p.t, popped.Value)
}

func (p *fieldParent) length(ref FieldRef) uint64 {
	p.t.Helper()
	snap := p.kv.NewSnapshot()
	defer snap.Discard()
	n, err := FieldLen(snap, ref)
	if err != nil {
		p.t.Fatalf("FieldLen: %v", err)
	}
	return n
}

func (p *fieldParent) list(ref FieldRef, req FieldListRequest) ([]FieldEntry, bool) {
	p.t.Helper()
	snap := p.kv.NewSnapshot()
	defer snap.Discard()
	entries, hasMore, err := FieldList(snap, ref, req)
	if err != nil {
		p.t.Fatalf("FieldList: %v", err)
	}
	return entries, hasMore
}

func (p *fieldParent) state() BucketState {
	p.t.Helper()
	snap := p.kv.NewSnapshot()
	defer snap.Discard()
	st, err := LoadBucketState(snap, fieldTestSlot, 1, fieldTestID)
	if err != nil {
		p.t.Fatalf("loading the parent's state: %v", err)
	}
	return st
}

// TestFieldStoreKeyspaceIsPerDocument is spec T-02.
func TestFieldStoreKeyspaceIsPerDocument(t *testing.T) {
	p := newFieldParent(t)
	one, two := p.ref("p1", 1, FieldFIFO), p.ref("p2", 1, FieldFIFO)
	for n := range 5 {
		p.push(one, n)
	}
	p.push(two, 100)
	if got := p.length(one); got != 5 {
		t.Fatalf("p1's FieldLen = %d, want 5", got)
	}
	if got := p.length(two); got != 1 {
		t.Fatalf("p2's FieldLen = %d, want 1: pushes to p1 reached p2's instance", got)
	}
	if got := p.pop(two); got != 100 {
		t.Fatalf("p2 popped %d, want its own element 100", got)
	}
	if got := p.pop(two); got != -1 {
		t.Fatalf("p2 popped %d after its only element, want empty", got)
	}
	for want := range 5 {
		if got := p.pop(one); got != want {
			t.Fatalf("p1 popped %d, want %d: popping p2 moved p1's order", got, want)
		}
	}
}

// TestFifoFieldPopsOldestOfThatDocument is spec T-03, with its lifo and heap
// cases.
func TestFifoFieldPopsOldestOfThatDocument(t *testing.T) {
	t.Run("fifo", func(t *testing.T) {
		p := newFieldParent(t)
		one, two := p.ref("p1", 1, FieldFIFO), p.ref("p2", 1, FieldFIFO)
		for n := range 6 {
			p.push(one, n)
			p.push(two, 100+n)
		}
		for want := range 6 {
			if got := p.pop(one); got != want {
				t.Fatalf("fifo p1 popped %d, want %d (the oldest of p1)", got, want)
			}
		}
	})
	t.Run("lifo", func(t *testing.T) {
		p := newFieldParent(t)
		one, two := p.ref("p1", 1, FieldLIFO), p.ref("p2", 1, FieldLIFO)
		for n := range 6 {
			p.push(one, n)
			p.push(two, 100+n)
		}
		for want := 5; want >= 0; want-- {
			if got := p.pop(one); got != want {
				t.Fatalf("lifo p1 popped %d, want %d (the newest of p1)", got, want)
			}
		}
	})
	t.Run("heap", func(t *testing.T) {
		p := newFieldParent(t)
		one, two := p.ref("p1", 2, FieldHeap), p.ref("p2", 2, FieldHeap)
		priorities := []int64{-5, 7, 0, math.MaxInt64, -1, 3, math.MinInt64 + 1}
		for n, priority := range priorities {
			p.heapPush(one, priority, n)
			p.heapPush(two, priority+1-2*int64(n%2), 100+n)
		}
		for _, want := range []int{3, 1, 5, 2, 4, 0, 6} {
			if got := p.pop(one); got != want {
				t.Fatalf("heap p1 popped %d (priority %d), want %d (priority %d)", got, priorities[max(got, 0)], want, priorities[want])
			}
		}
	})
}

// TestFieldCountersSurviveRestart is spec T-04.
func TestFieldCountersSurviveRestart(t *testing.T) {
	p := openFieldParent(t, t.TempDir())
	t.Cleanup(func() { _ = p.kv.Close() })
	ref := p.ref("p1", 1, FieldFIFO)
	for n := range 4 {
		p.push(ref, n)
	}
	p.pop(ref)
	p.reopen()
	if got := p.length(ref); got != 3 {
		t.Fatalf("FieldLen after reopen = %d, want 3", got)
	}
	for range 3 {
		p.pop(ref)
	}
	p.reopen()
	if got := p.length(ref); got != 0 {
		t.Fatalf("FieldLen after popping empty and reopening = %d, want 0", got)
	}
	if position := p.push(ref, 9); position != 4 {
		t.Fatalf("the push after the instance was popped empty took position %d, want 4: a position was reused", position)
	}
}

// TestFieldEntriesAreNotCountedAsDocuments is spec T-05.
func TestFieldEntriesAreNotCountedAsDocuments(t *testing.T) {
	p := newFieldParent(t)
	for _, key := range []string{"p1", "p2", "p3"} {
		if err := p.hash.Put(ApplyInfo{}, key, map[string]interface{}{"title": key}, 0); err != nil {
			t.Fatalf("putting %s: %v", key, err)
		}
	}
	documentBytes := p.state().LogicalBytes
	fifo, hash := p.ref("p1", 1, FieldFIFO), p.ref("p2", 2, FieldHash)
	for n := range 30 {
		p.push(fifo, n)
	}
	for n := range 20 {
		p.put(hash, fmt.Sprintf("k%02d", n), n)
	}

	checks := map[string]func() (int64, error){
		"Len":     p.hash.Len,
		"Recount": p.hash.Recount,
		"DocCount": func() (int64, error) {
			return p.hash.DocCount()
		},
		"IterateDocuments": func() (int64, error) {
			var n int64
			err := p.hash.IterateDocuments(func(_, _ []byte) error { n++; return nil })
			return n, err
		},
	}
	for name, count := range checks {
		if got, err := count(); err != nil || got != 3 {
			t.Errorf("%s = %d, %v, want 3: field elements were counted as documents", name, got, err)
		}
	}
	st := p.state()
	if st.Entries != 3 {
		t.Errorf("BucketState.Entries = %d, want 3", st.Entries)
	}
	// Every key under the field namespace is charged: the elements and the
	// two instance states.
	var fieldBytes int64
	snap := p.kv.NewSnapshot()
	defer snap.Discard()
	lower := storage.Key(fieldTestSlot, 1, fieldTestID, storage.NSFieldStore)
	it := snap.NewIterator(storage.IterOptions{Lower: lower, Upper: prefixEnd(lower)})
	defer it.Close()
	for it.Seek(nil); it.Valid(); it.Next() {
		value, err := it.Value()
		if err != nil {
			t.Fatalf("reading %x: %v", it.Key(), err)
		}
		fieldBytes += int64(len(it.Key()) + len(value))
	}
	if st.LogicalBytes != documentBytes+fieldBytes {
		t.Errorf("LogicalBytes = %d, want the documents' %d plus the field stores' %d (S-8)", st.LogicalBytes, documentBytes, fieldBytes)
	}
}

// TestFieldQueueMatchesBucketQueue replays one script of pushes and pops on
// a field instance and on a queue bucket: the pop order and the length after
// every step must agree, so the cursor arithmetic was reproduced, not
// re-derived.
func TestFieldQueueMatchesBucketQueue(t *testing.T) {
	for _, mode := range []struct {
		name   string
		engine FieldEngine
		queue  QueueMode
	}{{"fifo", FieldFIFO, QueueFIFO}, {"lifo", FieldLIFO, QueueLIFO}} {
		t.Run(mode.name, func(t *testing.T) {
			p := newFieldParent(t)
			ref := p.ref("p1", 1, mode.engine)
			bucket := NewQueueStore(p.kv, storage.BucketRef{Slot: fieldTestSlot, DatabaseID: 1, BucketID: fieldTestID + 1}, new(OwedApplies), mode.queue)
			next := 0
			for step := range 200 {
				if step%3 == 2 || (step%7 == 0 && step > 0) {
					popped, err := bucket.Pop(ApplyInfo{})
					if err != nil {
						t.Fatalf("step %d: bucket pop: %v", step, err)
					}
					want := -1
					if popped != nil {
						want = entryNumber(t, popped)
					}
					if got := p.pop(ref); got != want {
						t.Fatalf("step %d: the field popped %d, the bucket %d", step, got, want)
					}
				} else {
					if _, err := bucket.Push(ApplyInfo{}, queueEntry(t, next)); err != nil {
						t.Fatalf("step %d: bucket push: %v", step, err)
					}
					p.push(ref, next)
					next++
				}
				bucketLen, err := bucket.Len()
				if err != nil {
					t.Fatalf("step %d: bucket Len: %v", step, err)
				}
				if got := p.length(ref); int64(got) != bucketLen {
					t.Fatalf("step %d: the field's length is %d, the bucket's %d", step, got, bucketLen)
				}
			}
		})
	}
}

func TestFieldPopSeeksToCursor(t *testing.T) {
	for _, mode := range []struct {
		name   string
		engine FieldEngine
		want   uint64
	}{{"fifo", FieldFIFO, 60}, {"lifo", FieldLIFO, 39}} {
		t.Run(mode.name, func(t *testing.T) {
			p := newFieldParent(t)
			ref := p.ref("p1", 1, mode.engine)
			for n := range 100 {
				p.push(ref, n)
			}
			for range 60 {
				p.pop(ref)
			}
			var sought []byte
			hook := func(seekKey []byte) { sought = append([]byte(nil), seekKey...) }
			fieldPopSeekHook.Store(&hook)
			t.Cleanup(func() { fieldPopSeekHook.Store(nil) })
			p.pop(ref)
			want := FieldElementKey(ref.StorePrefix(), storage.NSQueue, PositionRemainder(mode.want))
			if !bytes.Equal(sought, want) {
				t.Fatalf("the 61st pop sought %x, want the cursor %x ([..][0x04][%d]): a seek to anything else walks the tombstone wall", sought, want, mode.want)
			}
		})
	}
}

func TestFieldListNeverReadsState(t *testing.T) {
	p := newFieldParent(t)
	ref := p.ref("p1", 1, FieldFIFO)
	for n := range 10 {
		p.push(ref, n)
	}
	all, hasMore := p.list(ref, FieldListRequest{Limit: 100, MaxBytes: 1 << 20})
	if len(all) != 10 || hasMore {
		t.Fatalf("listing everything returned %d entries, hasMore %v; want 10, false (the state must not appear)", len(all), hasMore)
	}
	for i, entry := range all {
		if got := entryNumber(t, entry.Value); got != i || entry.Position != uint64(i) {
			t.Fatalf("entry %d is element %d at position %d, want element %d at %d", i, got, entry.Position, i, i)
		}
	}
	page, hasMore := p.list(ref, FieldListRequest{Limit: 4, MaxBytes: 1 << 20})
	if len(page) != 4 || !hasMore {
		t.Fatalf("a limit of 4 returned %d entries, hasMore %v; want 4, true", len(page), hasMore)
	}
	rest, hasMore := p.list(ref, FieldListRequest{After: page[3].Remainder, Limit: 6, MaxBytes: 1 << 20})
	if len(rest) != 6 || hasMore || rest[0].Position != 4 {
		t.Fatalf("after position 3 with a limit of 6: %d entries from position %d, hasMore %v; want 6 from 4, false", len(rest), rest[0].Position, hasMore)
	}
	entrySize := len(all[0].Remainder) + len(all[0].Value)
	bounded, hasMore := p.list(ref, FieldListRequest{Limit: 100, MaxBytes: 3*entrySize + 1})
	if len(bounded) != 3 || !hasMore {
		t.Fatalf("a byte bound of three entries returned %d, hasMore %v; want 3, true", len(bounded), hasMore)
	}
}

// TestFieldWriteAdvancesTheParentMark: a field write is a write to its parent
// bucket, gated and marked by the parent's applied_seq.
func TestFieldWriteAdvancesTheParentMark(t *testing.T) {
	p := newFieldParent(t)
	ref := p.ref("p1", 1, FieldFIFO)
	push := func(info ApplyInfo) (bool, error) {
		return p.apply(info, false, func(w FieldWrite) error {
			_, err := FieldPush(w, ref, queueEntry(t, 1))
			return err
		})
	}
	if applied, err := push(ApplyInfo{Seq: 7}); err != nil || !applied {
		t.Fatalf("the push at sequence 7 = %v, %v; want applied", applied, err)
	}
	if got := p.state().AppliedSeq; got != 7 {
		t.Fatalf("the parent's applied_seq = %d after a field push at 7, want 7", got)
	}
	if applied, err := push(ApplyInfo{Seq: 7, Replay: true}); err != nil || applied {
		t.Fatalf("replaying sequence 7 = %v, %v; want skipped by the gate", applied, err)
	}
	if got := p.length(ref); got != 1 {
		t.Fatalf("FieldLen after a replayed push = %d, want 1: the replay pushed twice", got)
	}
}

func TestFieldVerbsRefuseTheWrongEngine(t *testing.T) {
	p := newFieldParent(t)
	hash, fifo := p.ref("p1", 1, FieldHash), p.ref("p1", 2, FieldFIFO)
	_, err := p.apply(ApplyInfo{}, false, func(w FieldWrite) error {
		_, err := FieldPush(w, hash, queueEntry(t, 1))
		return err
	})
	if err == nil || errors.Is(err, ErrApplyFailed) {
		t.Fatalf("a push to a hash field = %v, want a refusal", err)
	}
	_, err = p.apply(ApplyInfo{}, true, func(w FieldWrite) error { return FieldPut(w, fifo, "k", queueEntry(t, 1)) })
	if err == nil || errors.Is(err, ErrApplyFailed) {
		t.Fatalf("a put to a fifo field = %v, want a refusal", err)
	}
}

func TestFieldKeyedElementsRoundTrip(t *testing.T) {
	p := newFieldParent(t)
	ref := p.ref("p1", 3, FieldBTree)
	for n, key := range []string{"b", "a\x00z", "c", "a"} {
		p.put(ref, key, n)
	}
	p.put(ref, "b", 42)
	if got := p.length(ref); got != 4 {
		t.Fatalf("FieldLen after four keys and one overwrite = %d, want 4", got)
	}
	entries, _ := p.list(ref, FieldListRequest{Limit: 10, MaxBytes: 1 << 20})
	var keys []string
	for _, entry := range entries {
		keys = append(keys, entry.ElementKey)
	}
	if want := []string{"a", "a\x00z", "b", "c"}; fmt.Sprint(keys) != fmt.Sprint(want) {
		t.Fatalf("listed keys %q, want %q in key order", keys, want)
	}
	snap := p.kv.NewSnapshot()
	value, found, err := FieldGet(snap, ref, "b")
	snap.Discard()
	if err != nil || !found || entryNumber(t, value) != 42 {
		t.Fatalf("FieldGet(b) = %v, %v; want the overwrite, 42", found, err)
	}
	var existed bool
	if _, err := p.apply(ApplyInfo{}, true, func(w FieldWrite) error {
		var err error
		existed, err = FieldDelete(w, ref, "a\x00z")
		return err
	}); err != nil || !existed {
		t.Fatalf("deleting a\\x00z = %v, %v; want existed", existed, err)
	}
	if got := p.length(ref); got != 3 {
		t.Fatalf("FieldLen after a delete = %d, want 3", got)
	}
	if _, err := p.apply(ApplyInfo{}, true, func(w FieldWrite) error {
		var err error
		existed, err = FieldDelete(w, ref, "absent")
		return err
	}); err != nil || existed {
		t.Fatalf("deleting an absent key = %v, %v; want not existed", existed, err)
	}
	if got := p.length(ref); got != 3 {
		t.Fatalf("FieldLen after deleting an absent key = %d, want 3", got)
	}
}

// fieldPeekNumbers is the n of every element FieldPeekN hands over, in order.
func (p *fieldParent) fieldPeekNumbers(ref FieldRef, n int) []int {
	p.t.Helper()
	snap := p.kv.NewSnapshot()
	defer snap.Discard()
	var numbers []int
	if err := FieldPeekN(snap, ref, n, func(entry FieldEntry) error {
		numbers = append(numbers, entryNumber(p.t, entry.Value))
		return nil
	}); err != nil {
		p.t.Fatalf("FieldPeekN(%d): %v", n, err)
	}
	return numbers
}

// pushEngineElements fills ref the way the equality test needs: for heap,
// priorities that repeat, so ties are in the data.
func (p *fieldParent) pushEngineElements(ref FieldRef, from, to int) {
	p.t.Helper()
	for n := from; n < to; n++ {
		if ref.Engine == FieldHeap {
			p.heapPush(ref, int64(n%3), n)
		} else {
			p.push(ref, n)
		}
	}
}

// TestFieldPeekNEqualsNPops is the definition of peek({limit: N}) on a field:
// after pops have moved the cursor, FieldPeekN(k) is element for element what
// k FieldPops return on a twin. The heap rows hold equal priorities.
func TestFieldPeekNEqualsNPops(t *testing.T) {
	// wantFirst is the element a pop takes first, said outright so a direction
	// both verbs share (cursorSeek) cannot flip unseen; 0 is not asserted.
	wantFirst := map[FieldEngine]int{FieldFIFO: 4, FieldLIFO: 15}
	for _, engine := range []FieldEngine{FieldFIFO, FieldLIFO, FieldHeap} {
		t.Run(engine.String(), func(t *testing.T) {
			build := func() (*fieldParent, FieldRef) {
				p, _ := newCountedFieldParent(t)
				ref := p.ref("p1", 1, engine)
				p.pushEngineElements(ref, 0, 12)
				for range 4 {
					p.pop(ref)
				}
				p.pushEngineElements(ref, 12, 16)
				return p, ref
			}
			peeker, peekedRef := build()
			twin, twinRef := build()
			const k = 7
			peeked := peeker.fieldPeekNumbers(peekedRef, k)
			if len(peeked) != k {
				t.Fatalf("FieldPeekN(%d) returned %d elements", k, len(peeked))
			}
			if first, ok := wantFirst[engine]; ok && peeked[0] != first {
				t.Fatalf("FieldPeekN(%d) starts at element %d, want %d", k, peeked[0], first)
			}
			for i, want := range peeked {
				if got := twin.pop(twinRef); got != want {
					t.Fatalf("pop %d of the twin = %d, peek said %d (peeked %v)", i, got, want, peeked)
				}
			}
		})
	}
}

func TestFieldPeekNDoesNotMutate(t *testing.T) {
	for _, engine := range []FieldEngine{FieldFIFO, FieldLIFO, FieldHeap} {
		t.Run(engine.String(), func(t *testing.T) {
			p, kv := newCountedFieldParent(t)
			ref := p.ref("p1", 1, engine)
			p.pushEngineElements(ref, 0, 10)
			commitsBefore, stateBefore := kv.commits.Load(), p.state()
			first := p.fieldPeekNumbers(ref, 3)
			if again := p.fieldPeekNumbers(ref, 3); fmt.Sprint(again) != fmt.Sprint(first) {
				t.Fatalf("a second FieldPeekN(3) = %v, the first said %v", again, first)
			}
			if got := kv.commits.Load(); got != commitsBefore {
				t.Fatalf("FieldPeekN committed %d batches, want none", got-commitsBefore)
			}
			if !reflect.DeepEqual(p.state(), stateBefore) {
				t.Fatalf("FieldPeekN changed the parent's state")
			}
			if got := p.length(ref); got != 10 {
				t.Fatalf("FieldLen after peeking = %d, want 10", got)
			}
			if got := p.pop(ref); got != first[0] {
				t.Fatalf("next pop = %d, want the first peeked %d", got, first[0])
			}
		})
	}
}

// TestFieldPeekNCursorLost: one rule, no short result. A count the range does
// not back is ErrFieldCursorLost whether the walk found none of the elements
// or some of them.
func TestFieldPeekNCursorLost(t *testing.T) {
	for _, engine := range []FieldEngine{FieldFIFO, FieldLIFO, FieldHeap} {
		for _, keep := range []int{0, 1} {
			t.Run(fmt.Sprintf("%s/range holds %d", engine, keep), func(t *testing.T) {
				p, _ := newCountedFieldParent(t)
				ref := p.ref("p1", 1, engine)
				p.pushEngineElements(ref, 0, 3)
				stored, _ := p.list(ref, FieldListRequest{Limit: 10, MaxBytes: 1 << 20})
				batch := p.kv.NewBatch()
				defer batch.Discard()
				for _, entry := range stored[keep:] {
					if err := batch.Delete(FieldElementKey(ref.StorePrefix(), ref.elementInner(), entry.Remainder)); err != nil {
						t.Fatalf("deleting an element: %v", err)
					}
				}
				if err := batch.Commit(false); err != nil {
					t.Fatalf("committing the deletes: %v", err)
				}
				snap := p.kv.NewSnapshot()
				defer snap.Discard()
				err := FieldPeekN(snap, ref, 5, func(FieldEntry) error { return nil })
				if !errors.Is(err, ErrFieldCursorLost) {
					t.Fatalf("FieldPeekN(5) = %v, want ErrFieldCursorLost", err)
				}
			})
		}
	}
}

// TestFieldPeekNIsOneSeek pins the structure of the walk: one seek for any n,
// not one per element. It says nothing about cost.
func TestFieldPeekNIsOneSeek(t *testing.T) {
	for _, engine := range []FieldEngine{FieldFIFO, FieldLIFO, FieldHeap} {
		t.Run(engine.String(), func(t *testing.T) {
			p, _ := newCountedFieldParent(t)
			ref := p.ref("p1", 1, engine)
			p.pushEngineElements(ref, 0, 150)
			seeks := 0
			hook := func([]byte) { seeks++ }
			fieldPopSeekHook.Store(&hook)
			t.Cleanup(func() { fieldPopSeekHook.Store(nil) })
			if got := len(p.fieldPeekNumbers(ref, 100)); got != 100 {
				t.Fatalf("FieldPeekN(100) returned %d elements", got)
			}
			if seeks != 1 {
				t.Fatalf("FieldPeekN(100) seeked %d times, want 1", seeks)
			}
		})
	}
}

// TestFieldPeekNIgnoresNestedStoreKeys pins the key layout the walk relies on:
// a store nested under an element lives under inner byte 0x07, outside the
// element range, so no walk of the elements can reach it and none needs a
// skip. If FieldElementAnchor ever moves L3 keys under NSQueue this goes red.
func TestFieldPeekNIgnoresNestedStoreKeys(t *testing.T) {
	p, kv := newCountedFieldParent(t)
	ref := p.ref("p1", 1, FieldFIFO)
	for n := range 5 {
		position := p.push(ref, n)
		nested := FieldRef{Owner: FieldElementAnchor(ref.StorePrefix(), PositionRemainder(position)), FieldID: 3, Engine: FieldFIFO}
		p.push(nested, 100+n)
		p.push(nested, 200+n)
	}
	visitsBefore := kv.visits.Load()
	if got := p.fieldPeekNumbers(ref, 3); fmt.Sprint(got) != "[0 1 2]" {
		t.Fatalf("FieldPeekN(3) = %v, want [0 1 2]: nested elements were returned", got)
	}
	if got := kv.visits.Load() - visitsBefore; got != 3 {
		t.Fatalf("the walk visited %d keys, want 3", got)
	}
}

// fieldPopNEngines are the ordered store fields as popNSubjects, one row per
// engine. With nested, every element carries an L3 fifo store of one
// element, which a pop removes with the element.
func fieldPopNEngines(nested bool) []popNEngine {
	var engines []popNEngine
	for _, engine := range []FieldEngine{FieldFIFO, FieldLIFO, FieldHeap} {
		engines = append(engines, fieldPopNEngine(engine, nested))
	}
	return engines
}

func fieldPopNEngine(engine FieldEngine, nested bool) popNEngine {
	name := "field/" + engine.String()
	if nested {
		name += "/nested"
	}
	return popNEngine{name: name, build: func(t *testing.T) popNSubject {
		p, kv := newCountedFieldParent(t)
		ref := p.ref("p1", 1, engine)
		if nested {
			ref.Nested = FieldLayout{3: FieldFIFO}
		}
		type pushed struct {
			n      int
			anchor []byte
		}
		var elements []pushed
		fieldState := func(snap storage.KVSnapshot) FieldInstanceState {
			state, _, err := loadFieldState(snap, FieldStateKey(ref.StorePrefix()))
			if err != nil {
				t.Fatalf("loading the field's state: %v", err)
			}
			return state
		}
		return popNSubject{
			kv: kv,
			ids: func(entries [][]byte) []string {
				names := make([]string, 0, len(entries))
				for _, n := range entryNumbers(t, entries) {
					names = append(names, fmt.Sprint(n))
				}
				return names
			},
			push: func(from, to int) {
				for n := from; n < to; n++ {
					var remainder []byte
					if engine == FieldHeap {
						p.heapPush(ref, int64(n%3), n)
						var err error
						if remainder, err = HeapRemainder(int64(n%3), fmt.Sprintf("e%d", n)); err != nil {
							t.Fatalf("heap remainder of %d: %v", n, err)
						}
					} else {
						remainder = PositionRemainder(p.push(ref, n))
					}
					anchor := FieldElementAnchor(ref.StorePrefix(), remainder)
					if nested {
						p.push(FieldRef{Owner: anchor, FieldID: 3, Engine: FieldFIFO}, 1000+n)
					}
					elements = append(elements, pushed{n: n, anchor: anchor})
				}
			},
			popN: func(info ApplyInfo, n int, visit func(entry []byte) error) (int, error) {
				removed := 0
				if _, err := p.apply(info, false, func(w FieldWrite) error {
					var err error
					removed, err = FieldPopN(w, ref, n, func(entry FieldEntry) error { return visit(entry.Value) })
					return err
				}); err != nil {
					return 0, err
				}
				return removed, nil
			},
			pop: func() []byte {
				var popped FieldEntry
				if _, err := p.apply(ApplyInfo{}, false, func(w FieldWrite) error {
					var err error
					popped, _, err = FieldPop(w, ref)
					return err
				}); err != nil {
					t.Fatalf("FieldPop: %v", err)
				}
				return popped.Value
			},
			peekN: func(n int) [][]byte {
				snap := kv.NewSnapshot()
				defer snap.Discard()
				var peeked [][]byte
				if err := FieldPeekN(snap, ref, n, func(entry FieldEntry) error {
					peeked = append(peeked, entry.Value)
					return nil
				}); err != nil {
					t.Fatalf("FieldPeekN(%d): %v", n, err)
				}
				return peeked
			},
			state: func() popNState {
				snap := kv.NewSnapshot()
				defer snap.Discard()
				state := fieldState(snap)
				return popNState{Head: state.Head, Tail: state.Tail, Entries: int64(state.Count), LogicalBytes: p.state().LogicalBytes}
			},
			owes: p.hash.Carry().Owes,
			dropAllBut: func(keep int) {
				dropAllButFirst(t, kv, func(snap storage.KVSnapshot, _ BucketState) storage.KVIterator {
					return cursorSeek(snap, ref, fieldState(snap))
				}, BucketState{}, keep)
			},
			plant: func() map[string][]byte {
				// Beside the element namespace in the store's own prefix: the
				// state's 0x05 (not the state key itself), and 0x03 and 0x0D on
				// either side of 0x04.
				prefix := ref.StorePrefix()
				planted := map[string][]byte{
					string(FieldElementKey(prefix, storage.NSIndex, []byte("planted-below"))):   []byte("index"),
					string(FieldElementKey(prefix, storage.NSState, []byte("planted-state"))):   []byte("state"),
					string(FieldElementKey(prefix, 0x0D, []byte{0, 0, 0, 0, 0, 0, 0, 1})):       []byte("claimed"),
					string(FieldElementKey(prefix, 0x0D, []byte{0xff, 0xff, 0xff, 0xff, 0xff})): []byte("claimed-high"),
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
			},
			lost: ErrFieldCursorLost,
			nested: func() []string {
				if !nested {
					return nil
				}
				var holding []string
				for _, element := range elements {
					if fieldKeysUnder(t, kv, element.anchor) > 0 {
						holding = append(holding, fmt.Sprint(element.n))
					}
				}
				return holding
			},
		}
	}}
}

// TestFieldPopNEqualsPeekNThenKPops is pop({limit: N})'s definition on a
// store field: FieldPopN(k) returns what FieldPeekN(k) of a twin returns, and
// leaves what k FieldPops leave. The first element is asserted outright.
func TestFieldPopNEqualsPeekNThenKPops(t *testing.T) {
	// After moveTheCursor: fifo holds 2..7, lifo 7..5 then 2..0, and the heap
	// (priority n%3, ties in id order) has popped 2 and 1, so 5 leads.
	assertPopNEqualsPeekNThenKPops(t, fieldPopNEngines(false), map[string]string{
		"field/fifo": "2", "field/lifo": "7", "field/heap": "5",
	})
}

// TestFieldPopNRemovesNestedStores: each popped element's L3 stores go in the
// pop's batch, and a blob L3 store's payload path is collected for the caller
// to unlink after the commit; an unpopped element's stores are untouched.
func TestFieldPopNRemovesNestedStores(t *testing.T) {
	for _, engine := range []FieldEngine{FieldFIFO, FieldLIFO, FieldHeap} {
		t.Run(engine.String(), func(t *testing.T) {
			p, _ := newCountedFieldParent(t)
			root := t.TempDir()
			ref := p.ref("p1", 1, engine)
			ref.Nested = FieldLayout{3: FieldFIFO, 4: FieldBlob}
			type element struct {
				anchor  []byte
				blobID  string
				payload string
				keys    int
			}
			elements := map[int]*element{}
			for n := range 5 {
				var remainder []byte
				if engine == FieldHeap {
					p.heapPush(ref, int64(n%3), n)
					var err error
					if remainder, err = HeapRemainder(int64(n%3), fmt.Sprintf("e%d", n)); err != nil {
						t.Fatalf("heap remainder of %d: %v", n, err)
					}
				} else {
					remainder = PositionRemainder(p.push(ref, n))
				}
				anchor := FieldElementAnchor(ref.StorePrefix(), remainder)
				p.push(FieldRef{Owner: anchor, FieldID: 3, Engine: FieldFIFO}, 100+n)
				blobRef := FieldRef{Owner: anchor, FieldID: 4, Engine: FieldBlob}
				blob, err := PrepareFieldBlob(root, blobRef, nil, bytes.Repeat([]byte{byte(n)}, InlineThreshold), nil)
				if err != nil {
					t.Fatalf("preparing the blob of %d: %v", n, err)
				}
				if blob.PayloadPath() == "" {
					t.Fatalf("the blob of %d is inline; the test needs a payload file", n)
				}
				if _, err := p.apply(ApplyInfo{}, true, func(w FieldWrite) error {
					_, err := FieldBlobPut(w, blobRef, "photo", blob)
					return err
				}); err != nil {
					t.Fatalf("putting the blob of %d: %v", n, err)
				}
				elements[n] = &element{anchor: anchor, blobID: blob.BlobID(), payload: blob.PayloadPath(), keys: fieldKeysUnder(t, p.kv, anchor)}
			}

			var payloads []string
			var popped []int
			if _, err := Apply(ApplyRequest{KV: p.kv, Slot: fieldTestSlot, DatabaseID: 1, ID: fieldTestID, Info: ApplyInfo{}, Owed: p.hash.Carry(),
				Op: func(snap storage.KVSnapshot, st *BucketState, b storage.KVBatch) error {
					w := FieldWrite{Snap: snap, State: st, Batch: b, Payloads: &payloads}
					_, err := FieldPopN(w, ref, 3, func(entry FieldEntry) error {
						popped = append(popped, entryNumber(t, entry.Value))
						return nil
					})
					return err
				}}); err != nil {
				t.Fatalf("FieldPopN(3): %v", err)
			}
			if len(popped) != 3 {
				t.Fatalf("FieldPopN(3) popped %v", popped)
			}
			var wantPayloads []string
			isPopped := map[int]bool{}
			for _, n := range popped {
				isPopped[n] = true
				wantPayloads = append(wantPayloads, elements[n].payload)
			}
			if !reflect.DeepEqual(payloads, wantPayloads) {
				t.Fatalf("FieldPopN(3) collected payloads %q, want the popped elements' %q", payloads, wantPayloads)
			}
			snap := p.kv.NewSnapshot()
			defer snap.Discard()
			for n, e := range elements {
				left := fieldKeysUnder(t, p.kv, e.anchor)
				_, metaErr := snap.Get(blobMetaKeyOf(e.anchor, []byte(e.blobID)))
				switch {
				case isPopped[n] && left != 0:
					t.Errorf("popped element %d kept %d keys of its nested stores", n, left)
				case isPopped[n] && !errors.Is(metaErr, storage.ErrKeyNotFound):
					t.Errorf("popped element %d kept its blob's metadata (%v)", n, metaErr)
				case !isPopped[n] && left != e.keys:
					t.Errorf("unpopped element %d holds %d keys of its nested stores, want its %d", n, left, e.keys)
				case !isPopped[n] && metaErr != nil:
					t.Errorf("unpopped element %d lost its blob's metadata: %v", n, metaErr)
				}
			}
		})
	}
}
