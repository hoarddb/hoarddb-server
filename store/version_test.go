package store

import (
	"testing"

	"github.com/hoarddb/hoarddb-server/storage/kvfault"
)

// TestVersionedWriteNeverInflatesCount pins the invariant that matters most for
// read-repair versioning: the version is a count-neutral side-key. Counting it
// would break every bucket that writes a versioned document — the "count
// doubles" bug class — and Recount must not see it either.
func TestVersionedWriteNeverInflatesCount(t *testing.T) {
	h := NewHashStore(kvfault.New(), 1, 1, 1, new(OwedApplies))

	if err := h.Put(ApplyInfo{}, "k1", map[string]interface{}{"n": 1}, 10); err != nil {
		t.Fatalf("versioned put: %v", err)
	}
	// An upsert of the same key must not grow the count.
	if err := h.Put(ApplyInfo{}, "k1", map[string]interface{}{"n": 2}, 11); err != nil {
		t.Fatalf("versioned overwrite: %v", err)
	}
	n, _ := h.Len()
	if n != 1 {
		t.Fatalf("Len after versioned writes = %d, want 1", n)
	}
	if got, _ := h.GetVersion("k1"); got != 11 {
		t.Fatalf("GetVersion = %d, want 11", got)
	}

	// Recount walks the document namespace; the version keys live in their
	// own, so the count stays one.
	n, _ = h.Recount()
	if n != 1 {
		t.Fatalf("Recount = %d, want 1 (version keys were counted)", n)
	}

	// Delete clears the document and its version.
	if err := h.Delete(ApplyInfo{}, "k1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	n, _ = h.Len()
	if n != 0 {
		t.Fatalf("Len after delete = %d, want 0", n)
	}
	if got, _ := h.GetVersion("k1"); got != 0 {
		t.Fatalf("GetVersion after delete = %d, want 0", got)
	}
}

// TestVersionedBatchStampsEveryDocAndStaysNeutral: a batch stamps one version
// per document and the count reflects only documents.
func TestVersionedBatchStampsEveryDocAndStaysNeutral(t *testing.T) {
	h := NewHashStore(kvfault.New(), 1, 1, 1, new(OwedApplies))

	ids, err := h.PutBatch(ApplyInfo{}, PutBatchRequest{Items: []map[string]interface{}{
		{"_id_": "a", "n": 1},
		{"_id_": "b", "n": 2},
	}, Version: 7})
	if err != nil {
		t.Fatalf("batch versioned put: %v", err)
	}
	if len(ids) != 2 {
		t.Fatalf("batch returned %d ids, want 2", len(ids))
	}
	for _, id := range ids {
		if got, _ := h.GetVersion(id); got != 7 {
			t.Fatalf("version of %q = %d, want 7", id, got)
		}
	}
	n, _ := h.Len()
	if n != 2 {
		t.Fatalf("Len after batch = %d, want 2", n)
	}
}
