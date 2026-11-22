package store

import (
	"testing"
)

// heapTake runs FieldHeapTake in the parent's Apply.
func (p *fieldParent) heapTake(ref FieldRef, entryID string) (FieldEntry, bool) {
	p.t.Helper()
	var taken FieldEntry
	var found bool
	if _, err := p.apply(ApplyInfo{}, false, func(w FieldWrite) error {
		var err error
		taken, found, err = FieldHeapTake(w, ref, entryID)
		return err
	}); err != nil {
		p.t.Fatalf("taking %q: %v", entryID, err)
	}
	return taken, found
}

func TestU63HeapTakeRemovesOneEntryByIdAndLeavesTheOrder(t *testing.T) {
	p := newFieldParent(t)
	ref := p.ref("doc", 3, FieldHeap)
	p.heapPush(ref, 1, 1)
	p.heapPush(ref, 9, 9)
	p.heapPush(ref, 5, 5)
	snap := p.kv.NewSnapshot()
	peeked, found, err := FieldHeapEntry(snap, ref, "e5")
	snap.Discard()
	if err != nil || !found || entryNumber(t, peeked.Value) != 5 || p.length(ref) != 3 {
		t.Fatalf("FieldHeapEntry = %+v, %v, %v; len %d: want e5 found and nothing removed", peeked, found, err, p.length(ref))
	}
	taken, found := p.heapTake(ref, "e5")
	if !found || entryNumber(t, taken.Value) != 5 || taken.Priority != 5 || taken.EntryID != "e5" {
		t.Fatalf("take e5 = %+v, %v", taken, found)
	}
	if got := p.length(ref); got != 2 {
		t.Fatalf("len = %d after the take, want 2", got)
	}
	if first, second := p.pop(ref), p.pop(ref); first != 9 || second != 1 {
		t.Fatalf("pops after the take = %d, %d, want 9 then 1", first, second)
	}
}

func TestU63HeapTakeOfAnAbsentEntryWritesNothing(t *testing.T) {
	p := newFieldParent(t)
	ref := p.ref("doc", 3, FieldHeap)
	if _, found := p.heapTake(ref, "e1"); found {
		t.Fatal("take from an empty heap found something")
	}
	p.heapPush(ref, 1, 1)
	before := p.state()
	if _, found := p.heapTake(ref, "nope"); found {
		t.Fatal("take of an absent entry found something")
	}
	if after := p.state(); after.LogicalBytes != before.LogicalBytes || p.length(ref) != 1 {
		t.Fatalf("a take of nothing changed the store: %+v -> %+v", before, after)
	}
}

func TestU63HeapTakeRefusesOtherEngines(t *testing.T) {
	p := newFieldParent(t)
	ref := p.ref("doc", 3, FieldFIFO)
	if _, err := p.apply(ApplyInfo{}, false, func(w FieldWrite) error {
		_, _, err := FieldHeapTake(w, ref, "e1")
		return err
	}); err == nil {
		t.Fatal("take by entry id from a fifo was accepted")
	}
}
