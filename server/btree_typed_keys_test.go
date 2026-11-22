package server

import (
	"math"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/storage/kvfault"
	"github.com/hoarddb/hoarddb-server/store"
)

// typedKeyListingKeys are the keys BUG-0617's listing tests write: numbers
// large enough that strconv allocates, a date, a bool, null, and strings
// including one with a NUL byte and one that is not valid UTF-8, so the
// listing's output is compared across every key type the store formats.
func typedKeyListingKeys() []interface{} {
	return []interface{}{
		int64(-1234567890123), uint64(18446744073709551615), 12345.6789e10,
		time.UnixMilli(1700000000123).UTC(), true, nil,
		"plain", "n\x00x", "\xff\xfe\x00",
		-1.5, math.MaxFloat64, 5e-324, math.Copysign(0, -1),
		time.UnixMilli(-86400000).UTC(), int64(math.MinInt64),
	}
}

func typedKeyListingStore(t *testing.T) *store.BTreeStore {
	t.Helper()
	bt := store.NewBTreeStore(kvfault.New(), 1, 1, 1, new(store.OwedApplies))
	putNumbered(t, bt, typedKeyListingKeys()...)
	return bt
}

// TestRangeQueryEntriesListsEveryKeyTypeAsBefore holds the listing's wire
// keys to displayKey over the store's own entries, the output before BUG-0617:
// numeric, date, bool, null, string and non-UTF-8 keys, byte for byte.
func TestRangeQueryEntriesListsEveryKeyTypeAsBefore(t *testing.T) {
	bt := typedKeyListingStore(t)
	// RangeEntries, not Range: its entries carry Text, the path the fix added.
	var stored []store.BTreeEntry
	err := bt.RangeEntries(nil, nil, 0, func(entry store.BTreeEntry) error {
		stored = append(stored, entry)
		return nil
	})
	if err != nil {
		t.Fatalf("RangeEntries: %v", err)
	}
	if len(stored) != len(typedKeyListingKeys()) {
		t.Fatalf("store holds %d entries, want %d", len(stored), len(typedKeyListingKeys()))
	}
	listed, err := rangeQueryEntries(&inMemoryBucket{btreeStore: bt}, BTreeRangeRequest{}, testBudget(queryMethodBTreeRange), testWalk(queryMethodBTreeRange))
	if err != nil {
		t.Fatalf("rangeQueryEntries: %v", err)
	}
	wire := btreeEntries(stored)
	if len(listed) != len(stored) || len(wire) != len(stored) {
		t.Fatalf("listing returned %d entries and btreeEntries %d, want %d", len(listed), len(wire), len(stored))
	}
	for position, entry := range stored {
		want := displayKey(entry.Key)
		if listed[position].Key != want {
			t.Errorf("entry %d: rangeQueryEntries key %q, want %q", position, listed[position].Key, want)
		}
		if wire[position].Key != want {
			t.Errorf("entry %d: btreeEntries key %q, want %q", position, wire[position].Key, want)
		}
	}
}

// TestBTreeEntriesDoNotReconvertTypedKeys is BUG-0617: the store formats a
// number's text once, so btreeEntries must not copy it to a string again. Its
// only allocation is the reply slice.
func TestBTreeEntriesDoNotReconvertTypedKeys(t *testing.T) {
	bt := store.NewBTreeStore(kvfault.New(), 1, 1, 1, new(store.OwedApplies))
	putNumbered(t, bt, int64(-1234567890123))
	entry, err := bt.First()
	if err != nil {
		t.Fatalf("First: %v", err)
	}
	entries := []store.BTreeEntry{entry}
	allocs := testing.AllocsPerRun(100, func() { _ = btreeEntries(entries) })
	if allocs > 1 {
		t.Errorf("btreeEntries on a numeric key made %.0f allocations, want 1 (the reply slice): the formatted key is copied again", allocs)
	}
}

// TestRangeQueryEntriesDoNotReconvertTypedKeys is BUG-0617 on the listing
// path: over numeric keys, the listing allocates nothing per key beyond what
// the store formatted and the reply slice's growth.
func TestRangeQueryEntriesDoNotReconvertTypedKeys(t *testing.T) {
	const keyCount = 64
	bt := store.NewBTreeStore(kvfault.New(), 1, 1, 1, new(store.OwedApplies))
	for n := 0; n < keyCount; n++ {
		putNumbered(t, bt, int64(-1234567890123+n))
	}
	view := &inMemoryBucket{btreeStore: bt}
	request := BTreeRangeRequest{}
	list := func() {
		if _, err := rangeQueryEntries(view, request, testBudget(queryMethodBTreeRange), testWalk(queryMethodBTreeRange)); err != nil {
			t.Fatalf("rangeQueryEntries: %v", err)
		}
	}
	// The stored-key work that cannot go away is measured by the same walk
	// handing each key to a callback that keeps nothing.
	walkOnly := testing.AllocsPerRun(20, func() {
		if err := bt.RangeEach(nil, nil, 0, func(key, value []byte) error { return nil }); err != nil {
			t.Fatalf("RangeEach: %v", err)
		}
	})
	listing := testing.AllocsPerRun(20, list)
	// The listing's own cost: the BTreeEntry slice growing by doubling and the
	// budget bookkeeping, 13 measured at 64 keys, and nothing per key. A
	// string copy per key would add 64.
	const fixedListingAllocs = 20
	if extra := listing - walkOnly; extra > fixedListingAllocs {
		t.Errorf("listing %d numeric keys made %.0f allocations beyond the walk's %.0f; want at most %d: each key is copied to a string again", keyCount, extra, walkOnly, fixedListingAllocs)
	}
}
