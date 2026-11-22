package storage

import (
	"testing"
)

// PrefixIterator used to return an unscoped iterator parked at the instance's
// first key, ignoring the prefix argument. Callers then called Rewind() and
// looped with ValidForPrefix, which yields nothing whenever the requested prefix
// is not the instance's very first key — silent empty results rather than an
// error. This fails on the old behaviour and passes on the fixed one.
func TestPrefixIteratorIsScopedToPrefix(t *testing.T) {
	b, err := OpenBadger(t.TempDir(), BadgerTuning{MemTableMB: 32}, nil)
	if err != nil {
		t.Fatalf("OpenBadger: %v", err)
	}
	defer func() { _ = b.Close() }()

	// A key that sorts before the prefix under test, so a Rewind-based iterator
	// would start outside the prefix.
	for _, k := range []string{"alpha:1", "alpha:2", "beta:1", "beta:2", "gamma:1"} {
		if err := b.Put([]byte(k), []byte("v")); err != nil {
			t.Fatalf("Put %s: %v", k, err)
		}
	}

	prefix := []byte("beta:")
	it, txn := b.PrefixIterator(prefix)
	defer txn.Discard()
	defer it.Close()

	var keys []string
	for ; it.ValidForPrefix(prefix); it.Next() {
		keys = append(keys, string(it.Item().Key()))
	}

	if len(keys) != 2 || keys[0] != "beta:1" || keys[1] != "beta:2" {
		t.Fatalf("prefix iteration returned %v, want [beta:1 beta:2]", keys)
	}
}

// The iterator must stop at the end of the prefix rather than walking into the
// next one.
func TestPrefixIteratorStopsAtPrefixEnd(t *testing.T) {
	b, err := OpenBadger(t.TempDir(), BadgerTuning{MemTableMB: 32}, nil)
	if err != nil {
		t.Fatalf("OpenBadger: %v", err)
	}
	defer func() { _ = b.Close() }()

	for _, k := range []string{"a:1", "b:1", "b:2", "c:1"} {
		if err := b.Put([]byte(k), []byte("v")); err != nil {
			t.Fatalf("Put %s: %v", k, err)
		}
	}

	prefix := []byte("b:")
	it, txn := b.PrefixIterator(prefix)
	defer txn.Discard()
	defer it.Close()

	n := 0
	for ; it.Valid(); it.Next() {
		n++
	}
	if n != 2 {
		t.Fatalf("iterated %d keys, want 2 (keys outside the prefix were reached)", n)
	}
}
