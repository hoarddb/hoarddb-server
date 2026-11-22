package store

import (
	"errors"
	"fmt"
	"testing"

	"github.com/hoarddb/hoarddb-server/storage"
)

// fieldKeysUnder counts the keys stored under prefix.
func fieldKeysUnder(t *testing.T, kv storage.KV, prefix []byte) int {
	t.Helper()
	snap := kv.NewSnapshot()
	defer snap.Discard()
	it := snap.NewIterator(storage.IterOptions{Lower: prefix, Upper: prefixEnd(prefix)})
	defer it.Close()
	n := 0
	for it.Seek(nil); it.Valid(); it.Next() {
		n++
	}
	return n
}

func (p *fieldParent) putDocument(key string) {
	p.t.Helper()
	if err := p.hash.Put(ApplyInfo{}, key, map[string]interface{}{"title": key}, 0); err != nil {
		p.t.Fatalf("putting document %s: %v", key, err)
	}
}

// populate gives documentKey a fifo instance of n elements and a hash
// instance of n elements.
func (p *fieldParent) populate(documentKey string, n int) {
	p.t.Helper()
	fifo, hash := p.ref(documentKey, 1, FieldFIFO), p.ref(documentKey, 2, FieldHash)
	for i := range n {
		p.push(fifo, i)
		p.put(hash, fmt.Sprintf("k%d", i), i)
	}
}

// TestDeletingDocumentRemovesItsFieldEntries is spec T-07, the store half.
func TestDeletingDocumentRemovesItsFieldEntries(t *testing.T) {
	p := newFieldParent(t)
	for _, key := range []string{"p1", "p10"} {
		p.putDocument(key)
		p.populate(key, 5)
	}
	owner := p.ref("p1", 0, 0).Owner
	otherOwner := p.ref("p10", 0, 0).Owner
	otherKeys := fieldKeysUnder(t, p.kv, otherOwner)

	// A failed commit leaves the document and every element: they are one
	// batch.
	fail := func(storage.KV, uint16, uint64, uint64) error { return errors.New("injected commit failure") }
	ApplyFaultHook.Store(&fail)
	err := p.hash.Delete(ApplyInfo{}, "p1")
	ApplyFaultHook.Store(nil)
	if !errors.Is(err, ErrApplyFailed) {
		t.Fatalf("the delete under a failed commit = %v, want ErrApplyFailed", err)
	}
	if doc, _ := p.hash.Get("p1"); doc == nil {
		t.Fatalf("a failed delete removed the document")
	}
	if got := fieldKeysUnder(t, p.kv, owner); got != 12 {
		t.Fatalf("a failed delete left %d of p1's 12 field-store keys (10 elements, 2 states): the cascade is not in the document's batch", got)
	}

	// The delete that succeeds is one commit: a cascade in a batch of its own
	// would leave the document deleted and its elements alive if the second
	// commit failed.
	counting := &countingKV{KV: p.kv}
	if err := NewHashStore(counting, fieldTestSlot, 1, fieldTestID, new(OwedApplies)).Delete(ApplyInfo{}, "p1"); err != nil {
		t.Fatalf("deleting p1: %v", err)
	}
	if got := counting.commits.Load(); got != 1 {
		t.Fatalf("deleting p1 committed %d batches, want 1: the document and its field stores must be one batch", got)
	}
	if got := fieldKeysUnder(t, p.kv, owner); got != 0 {
		t.Fatalf("%d field-store keys of p1 survived its delete", got)
	}
	p.putDocument("p1")
	fifo := p.ref("p1", 1, FieldFIFO)
	if got := p.length(fifo); got != 0 {
		t.Fatalf("the re-created p1 has FieldLen %d, want 0: it adopted its predecessor's elements", got)
	}
	if entries, _ := p.list(fifo, FieldListRequest{Limit: 10, MaxBytes: 1 << 20}); len(entries) != 0 {
		t.Fatalf("the re-created p1 lists %d elements, want none", len(entries))
	}
	if got := fieldKeysUnder(t, p.kv, otherOwner); got != otherKeys {
		t.Fatalf("p10 holds %d field-store keys after p1's delete, want %d: the cascade reached a document whose key p1 prefixes", got, otherKeys)
	}
}

// TestDeleteOfAnAbsentDocumentStillCascades: an orphaned instance under a
// missing parent is what must not survive.
func TestDeleteOfAnAbsentDocumentStillCascades(t *testing.T) {
	p := newFieldParent(t)
	p.populate("ghost", 3)
	if err := p.hash.Delete(ApplyInfo{}, "ghost"); err != nil {
		t.Fatalf("deleting an absent document: %v", err)
	}
	if got := fieldKeysUnder(t, p.kv, p.ref("ghost", 0, 0).Owner); got != 0 {
		t.Fatalf("%d orphaned field-store keys survived the delete of an absent document", got)
	}
}

func TestBTreeDeleteCascades(t *testing.T) {
	p := newFieldParent(t)
	tree := NewBTreeStore(p.kv, fieldTestSlot, 1, fieldTestID, new(OwedApplies))
	for _, key := range []string{"p1", "p10"} {
		if err := tree.Put(ApplyInfo{}, key, map[string]interface{}{"title": key}, 0); err != nil {
			t.Fatalf("putting %s: %v", key, err)
		}
		p.populate(key, 4)
	}
	if err := tree.Delete(ApplyInfo{}, "p1"); err != nil {
		t.Fatalf("deleting p1: %v", err)
	}
	if got := fieldKeysUnder(t, p.kv, p.ref("p1", 0, 0).Owner); got != 0 {
		t.Fatalf("%d field-store keys of btree document p1 survived its delete: the btree owner is not FieldOwnerPrefix's", got)
	}
	if got := fieldKeysUnder(t, p.kv, p.ref("p10", 0, 0).Owner); got != 10 {
		t.Fatalf("p10 holds %d field-store keys, want its 10", got)
	}
}

func TestDeleteCascadeTooLargeIsRefusedWhole(t *testing.T) {
	p, kv := newFaultFieldParent(t)
	p.putDocument("p1")
	p.populate("p1", 40)
	before := p.state()
	// Every write so far was one element per batch. A bound between one
	// element's batch and the whole cascade's refuses only the cascade.
	kv.SetMaxBatchBytes(1024)
	err := p.hash.Delete(ApplyInfo{Seq: before.AppliedSeq + 1}, "p1")
	if !errors.Is(err, storage.ErrBatchTooLarge) {
		t.Fatalf("deleting a document whose stores exceed the batch bound = %v, want storage.ErrBatchTooLarge", err)
	}
	if errors.Is(err, ErrApplyFailed) {
		t.Fatalf("the refusal was reported as a store failure (%v): it is the operation's answer, alike on every replica", err)
	}
	if doc, _ := p.hash.Get("p1"); doc == nil {
		t.Fatalf("the refused delete removed the document")
	}
	if got := p.length(p.ref("p1", 1, FieldFIFO)); got != 40 {
		t.Fatalf("the refused delete left FieldLen %d, want 40", got)
	}
	// The refusal is the delete's answer, so its sequence is consumed: the
	// mark moves past it and nothing else does.
	after := p.state()
	if after.AppliedSeq != before.AppliedSeq+1 || after.Entries != before.Entries || after.LogicalBytes != before.LogicalBytes {
		t.Fatalf("the refused delete moved the parent's state from %+v to %+v; want only the mark moved, to %d", before, after, before.AppliedSeq+1)
	}
}

func TestDeleteCascadeAdjustsLogicalBytes(t *testing.T) {
	p := newFieldParent(t)
	p.putDocument("keep")
	before := p.state().LogicalBytes
	p.putDocument("p1")
	p.populate("p1", 7)
	p.heapPush(p.ref("p1", 3, FieldHeap), 5, 1)
	if err := p.hash.Delete(ApplyInfo{}, "p1"); err != nil {
		t.Fatalf("deleting p1: %v", err)
	}
	if after := p.state().LogicalBytes; after != before {
		t.Fatalf("LogicalBytes after the delete = %d, want %d, its value before the document and its elements were written", after, before)
	}
}
