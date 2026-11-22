package store

import (
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/storage/kvfault"
)

// TestBTreeEntryTextIsFreshPerEntry pins the invariant BTreeEntry.Text rests
// on (BUG-0617): Text shares Key's memory through unsafe.String, so every
// entry's Key must be its own never-reused slice. Over several keys of each
// Text-bearing type, read by two walks and by First and Last, each entry's
// Text still equals the text it had when read after the iterator moved on, and
// no two Keys share a backing array.
func TestBTreeEntryTextIsFreshPerEntry(t *testing.T) {
	bt := NewBTreeStore(kvfault.New(), 1, 1, 1, new(OwedApplies))
	keys := []interface{}{
		int64(-1234567890123), int64(1234567890123), int64(-7),
		uint64(18446744073709551615), uint64(18446744073709551614), uint64(1 << 40),
		12345.6789e10, -2.5e-300, 3.5e300,
		time.UnixMilli(1700000000123).UTC(), time.UnixMilli(-86400000).UTC(), time.UnixMilli(0).UTC(),
		true, false, nil, // bool has two keys and null one: all there are
		"plain", "n\x00x",
	}
	for n, key := range keys {
		if err := bt.Put(ApplyInfo{}, key, map[string]interface{}{"n": n}, uint64(n+1)); err != nil {
			t.Fatalf("Put(%v): %v", key, err)
		}
	}

	type seen struct {
		entry BTreeEntry
		text  string // a copy made when the entry arrived
	}
	var all []seen
	record := func(entry BTreeEntry) {
		all = append(all, seen{entry: entry, text: string(append([]byte(nil), entry.Key...))})
	}
	for walk := 0; walk < 2; walk++ {
		if err := bt.RangeEntries(nil, nil, 0, func(entry BTreeEntry) error {
			record(entry)
			return nil
		}); err != nil {
			t.Fatalf("RangeEntries: %v", err)
		}
	}
	for _, edge := range []func() (BTreeEntry, error){bt.First, bt.Last} {
		entry, err := edge()
		if err != nil {
			t.Fatalf("edge: %v", err)
		}
		record(entry)
	}

	backing := make(map[*byte]int, len(all))
	textBearing := 0
	for position, got := range all {
		if len(got.entry.Key) == 0 {
			t.Fatalf("entry %d has an empty key", position)
		}
		if first, dup := backing[&got.entry.Key[0]]; dup {
			t.Errorf("entries %d and %d share one Key backing array (%q): a reused buffer would change Text under the first", first, position, got.text)
		}
		backing[&got.entry.Key[0]] = position
		if string(got.entry.Key) != got.text {
			t.Errorf("entry %d: Key is now %q, was %q when read", position, got.entry.Key, got.text)
		}
		if got.entry.Text == "" {
			continue
		}
		textBearing++
		if got.entry.Text != got.text {
			t.Errorf("entry %d: Text = %q, want %q", position, got.entry.Text, got.text)
		}
	}
	// 15 typed keys per full walk, two walks, and First and Last land on a
	// string key and a typed one depending on the type order.
	if want := 2 * 15; textBearing < want {
		t.Errorf("%d entries carried Text, want at least %d", textBearing, want)
	}
}
