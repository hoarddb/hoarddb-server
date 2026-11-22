package store

import (
	"fmt"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/storage/kvfault"

	"github.com/hoarddb/hoarddb-bson/bson/primitive"
)

// noEscapeDisplayKey and escapeDisplayKey are the two shapes covered here:
// an ordinary string key with no embedded 0x00 (the common case, which must
// cost zero allocations to decode for display) and one that does contain
// 0x00, which must still decode through the escape unchanged.
const (
	noEscapeDisplayKey = "ordinary-key-without-any-nul-byte"
	escapeDisplayKey   = "left\x00right"
)

// encodedStringKeyForTest encodes value as a btree string key component, the
// same encoding BTreeStore.Range hands to displayKeyOf.
func encodedStringKeyForTest(tb testing.TB, value string) []byte {
	tb.Helper()
	encoded, err := EncodeComponent(value)
	if err != nil {
		tb.Fatalf("encoding %q as a btree key component: %v", value, err)
	}
	return encoded
}

// TestDisplayKeyDoesNotAllocateWhenNoEscape proves that a
// string-keyed entry with no embedded 0x00 costs zero allocations in
// displayKeyOf (bytes.ReplaceAll always allocates, even with nothing to
// replace), while a key that does contain 0x00 still decodes the escape back
// to 0x00 — the guard removes the allocation without changing the result.
func TestDisplayKeyDoesNotAllocateWhenNoEscape(t *testing.T) {
	t.Run("no escape: zero allocations", func(t *testing.T) {
		encoded := encodedStringKeyForTest(t, noEscapeDisplayKey)
		var display []byte
		allocs := minAllocs(7, 1000, func() {
			decoded, err := displayKeyOf(encoded)
			if err != nil {
				t.Fatalf("displayKeyOf: %v", err)
			}
			display = decoded
		})
		if string(display) != noEscapeDisplayKey {
			t.Fatalf("displayKeyOf = %q, want %q", display, noEscapeDisplayKey)
		}
		if allocs != 0 {
			t.Errorf("displayKeyOf on a no-escape key allocated %.1f times per call, want 0", allocs)
		}
	})

	t.Run("escape present: still decodes 0x00", func(t *testing.T) {
		encoded := encodedStringKeyForTest(t, escapeDisplayKey)
		display, err := displayKeyOf(encoded)
		if err != nil {
			t.Fatalf("displayKeyOf: %v", err)
		}
		if string(display) != escapeDisplayKey {
			t.Fatalf("displayKeyOf = %q, want %q (the 0x00 escape must round-trip)", display, escapeDisplayKey)
		}
	})
}

// BenchmarkDisplayKeyOfNoEscape times displayKeyOf on the common case: a
// string key with no embedded 0x00 to unescape. The pre-fix and
// post-fix numbers are recorded in the bug file's Fix report.
func BenchmarkDisplayKeyOfNoEscape(b *testing.B) {
	encoded := encodedStringKeyForTest(b, noEscapeDisplayKey)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := displayKeyOf(encoded); err != nil {
			b.Fatalf("displayKeyOf: %v", err)
		}
	}
}

// BenchmarkDisplayKeyOfWithEscape times displayKeyOf on a key that does
// contain 0x00, which must still allocate and decode the escape:
// this benchmark's cost is expected to stay the same before and after the
// fix, unlike the no-escape one above.
func BenchmarkDisplayKeyOfWithEscape(b *testing.B) {
	encoded := encodedStringKeyForTest(b, escapeDisplayKey)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := displayKeyOf(encoded); err != nil {
			b.Fatalf("displayKeyOf: %v", err)
		}
	}
}

// badgerBTreeStore returns a btree store over a real Badger instance holding
// documentCount string-keyed documents, with the copy cost of the Badger
// iterator in the path (the fault store's iterator copies nothing per entry).
func badgerBTreeStore(tb testing.TB, documentCount int) *BTreeStore {
	tb.Helper()
	kv, err := storage.OpenBadgerKV(tb.TempDir(), storage.BadgerTuning{MemTableMB: 32}, nil)
	if err != nil {
		tb.Fatalf("opening the store: %v", err)
	}
	tb.Cleanup(func() { _ = kv.Close() })
	bt := NewBTreeStore(kv, 1, 1, 1, new(OwedApplies))
	for i := 0; i < documentCount; i++ {
		if err := bt.Put(ApplyInfo{}, fmt.Sprintf("k%05d", i), map[string]interface{}{"n": i}, uint64(i+1)); err != nil {
			tb.Fatalf("Put(%d): %v", i, err)
		}
	}
	return bt
}

// TestDocCountCopiesNoKeyOrValue proves the count walk reads neither a key nor
// a value: it asks the iterator only whether it holds a document. Before the
// fix DocCount counted through IterateDocuments, whose entryAt copies each
// document's key and value for a callback that discards both — two
// allocations per document spent to count it.
func TestDocCountCopiesNoKeyOrValue(t *testing.T) {
	const documents = 200
	bt := badgerBTreeStore(t, documents)

	allocs := testing.AllocsPerRun(10, func() {
		n, err := bt.DocCount()
		if err != nil {
			t.Fatalf("DocCount: %v", err)
		}
		if n != documents {
			t.Fatalf("DocCount = %d, want %d", n, documents)
		}
	})
	if allocs > documents/4 {
		t.Errorf("counting %d documents allocated %.0f times, want at most %d: keys and values a count never reads were copied",
			documents, allocs, documents/4)
	}
}

// BenchmarkBTreeDocCount measures the count walk over 1,000 documents.
// BUG-0097's before/after numbers are in the bug file.
func BenchmarkBTreeDocCount(b *testing.B) {
	const documents = 1000
	bt := badgerBTreeStore(b, documents)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if n, err := bt.DocCount(); err != nil || n != documents {
			b.Fatalf("DocCount = %d, %v; want %d", n, err, documents)
		}
	}
}

// rangeBandStore returns a btree store holding entryCount string-keyed
// documents, so a Range with a limit has a full page to collect.
func rangeBandStore(tb testing.TB, entryCount int) *BTreeStore {
	tb.Helper()
	bt := NewBTreeStore(kvfault.New(), 1, 1, 1, new(OwedApplies))
	for i := 0; i < entryCount; i++ {
		key := fmt.Sprintf("k%07d", i)
		if err := bt.Put(ApplyInfo{}, key, map[string]interface{}{"n": i}, uint64(i+1)); err != nil {
			tb.Fatalf("Put(%s): %v", key, err)
		}
	}
	return bt
}

// TestRangeResultsSlicePresizedToLimit proves Range does not regrow its
// result slice when the limit already bounds it: the allocations Range makes
// beyond a RangeEach walk that keeps nothing (the walk's own cost) are the
// one presized slice, not the growslice series from 1 up toward the limit.
func TestRangeResultsSlicePresizedToLimit(t *testing.T) {
	const limit = 100
	bt := rangeBandStore(t, limit+20)

	walkAllocs := testing.AllocsPerRun(20, func() {
		if err := bt.RangeEach(nil, nil, limit, func(key, value []byte) error { return nil }); err != nil {
			t.Fatalf("RangeEach: %v", err)
		}
	})
	rangeAllocs := testing.AllocsPerRun(20, func() {
		entries, err := bt.Range(nil, nil, limit)
		if err != nil || len(entries) != limit {
			t.Fatalf("Range returned %d entries, error %v; want %d", len(entries), err, limit)
		}
	})

	// One allocation for the slice, one for the collecting closure's capture.
	const collectionAllocs = 2
	if extra := rangeAllocs - walkAllocs; extra > collectionAllocs {
		t.Errorf("Range(limit=%d) made %.0f allocations beyond the walk's %.0f, want at most %d: the result slice is regrowing", limit, extra, walkAllocs, collectionAllocs)
	}
}

// BenchmarkRangeBounded measures a limit-bounded Range over a 100-entry page,
// the shape whose result slice used to regrow from nil.
func BenchmarkRangeBounded(b *testing.B) {
	const limit = 100
	bt := rangeBandStore(b, limit+20)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := bt.Range(nil, nil, limit); err != nil {
			b.Fatalf("Range: %v", err)
		}
	}
}

// displayKeyCases are the key types displayKeyOf formats rather than slices,
// with the text a user sees for each.
var displayKeyCases = []struct {
	name string
	key  interface{}
	want string
}{
	{"null", nil, "null"},
	{"bool", true, "true"},
	{"datetime", time.UnixMilli(1700000000123).UTC(), "2023-11-14T22:13:20.123Z"},
	{"signed integer", int64(-1234567890123), "-1234567890123"},
	{"unsigned integer", uint64(18446744073709551615), "18446744073709551615"},
	{"float", 12345.6789e10, "123456789000000"},
	{"string", "plain", "plain"},
	{"ObjectID", primitive.ObjectID{0x65, 0x4a, 0x1f, 0x00, 0x00, 0xde, 0xad, 0xbe, 0xef, 0x01, 0x02, 0x00}, "654a1f0000deadbeef010200"},
}

// TestBTreeRangesOverObjectIDKeys: a btree keyed by ObjectID ranges over its
// documents in key order, with the display form of each key as its 24-character
// hex. The keys go in out of order, so the order comes from the btree.
func TestBTreeRangesOverObjectIDKeys(t *testing.T) {
	bt := NewBTreeStore(kvfault.New(), 1, 1, 1, new(OwedApplies))
	ids := []primitive.ObjectID{
		{0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01},
		{0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02},
		{0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03},
	}
	for i, id := range ids {
		if err := bt.Put(ApplyInfo{}, id, map[string]interface{}{"n": i}, 0); err != nil {
			t.Fatalf("Put(%x): %v", id[:], err)
		}
	}
	entries, err := bt.Range(nil, nil, 10)
	if err != nil {
		t.Fatalf("Range over ObjectID keys: %v", err)
	}
	want := []string{
		fmt.Sprintf("%x", ids[1][:]),
		fmt.Sprintf("%x", ids[2][:]),
		fmt.Sprintf("%x", ids[0][:]),
	}
	if len(entries) != len(want) {
		t.Fatalf("Range returned %d entries, want %d", len(entries), len(want))
	}
	for i, entry := range entries {
		if string(entry.Key) != want[i] {
			t.Fatalf("entry %d key = %q, want %q", i, entry.Key, want[i])
		}
	}
}

// TestDisplayKeyOfFormatsEveryKeyType pins the display text of every key type,
// so removing an allocation from the formatting cannot change what is shown.
func TestDisplayKeyOfFormatsEveryKeyType(t *testing.T) {
	for _, tc := range displayKeyCases {
		t.Run(tc.name, func(t *testing.T) {
			encoded, err := EncodeComponent(tc.key)
			if err != nil {
				t.Fatalf("EncodeComponent(%v): %v", tc.key, err)
			}
			got, err := displayKeyOf(encoded)
			if err != nil {
				t.Fatalf("displayKeyOf: %v", err)
			}
			if string(got) != tc.want {
				t.Errorf("displayKeyOf = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestDisplayKeyDoesNotRoundTripNumericKeys (BUG-0099): a formatted key is
// built once, directly into the []byte displayKeyOf returns, not formatted to
// a string and copied to bytes. Large values, because strconv returns small
// integers without allocating.
func TestDisplayKeyDoesNotRoundTripNumericKeys(t *testing.T) {
	for _, tc := range displayKeyCases {
		switch tc.name {
		case "datetime", "signed integer", "unsigned integer", "float":
		default:
			continue
		}
		t.Run(tc.name, func(t *testing.T) {
			encoded, err := EncodeComponent(tc.key)
			if err != nil {
				t.Fatalf("EncodeComponent(%v): %v", tc.key, err)
			}
			allocs := testing.AllocsPerRun(100, func() {
				if _, err := displayKeyOf(encoded); err != nil {
					t.Fatalf("displayKeyOf: %v", err)
				}
			})
			if allocs > 1 {
				t.Errorf("displayKeyOf(%s key) made %.0f allocations, want 1: the key is formatted to a string and copied to bytes", tc.name, allocs)
			}
		})
	}
}
