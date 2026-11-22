package storage_test

import (
	"bytes"
	"encoding/binary"
	"slices"
	"testing"

	"github.com/hoarddb/hoarddb-server/storage"
)

// walkFrom is bloomStore.walk with a seek target: every live key and value the iterator
// yields from Seek(target) on.
func (s *bloomStore) walkFrom(opts storage.IterOptions, target []byte) []bloomPair {
	s.t.Helper()
	snapshot := s.kv.NewSnapshot()
	defer snapshot.Discard()
	it := snapshot.NewIterator(opts)
	defer it.Close()
	var pairs []bloomPair
	for it.Seek(target); it.Valid(); it.Next() {
		value, err := it.Value()
		if err != nil {
			s.t.Fatalf("reading the value of %x: %v", it.Key(), err)
		}
		pairs = append(pairs, bloomPair{key: bytes.Clone(it.Key()), value: value})
	}
	if err := it.Err(); err != nil {
		s.t.Fatalf("walking %x..%x from %x: %v", opts.Lower, opts.Upper, target, err)
	}
	return pairs
}

// TestFilteredIterNeverLeavesItsPrefix is the prefix bloom spec's T10, the pin of I-5. A
// filtered iterator leaves out the tables that hold no key under its prefix, and those tables
// may hold the newest version of a neighbouring key: outside the prefix the iterator's merged
// view is stale. So it must never yield a key outside its bounds, forward or in reverse,
// wherever it is told to seek; and bounds widened after a constructor made them must lose the
// filter, not read the neighbours stale.
// Against: passing the filter with wider bounds.
func TestFilteredIterNeverLeavesItsPrefix(t *testing.T) {
	store := openBloomStore(t, storage.OpenBadgerKVNoCompaction)
	id := func(letter byte) []byte { return []byte{storage.TagString, letter, 0x00, 0x00} }
	document := func(letter byte) []byte { return storage.Key(3, 7, 9, storage.NSDocuments, id(letter)) }
	// Three store fields of one document: ids 3, 4 and 5.
	storePrefix := func(fieldID byte) []byte {
		return storage.Key(3, 7, 9, storage.NSFieldStore, id('m'), []byte{0x00, fieldID})
	}
	elements := func(fieldID byte) []byte { return append(storePrefix(fieldID), storage.NSQueue) }
	element := func(fieldID byte, position uint64) []byte {
		return binary.BigEndian.AppendUint64(elements(fieldID), position)
	}
	state := func(fieldID byte) []byte { return append(storePrefix(fieldID), storage.NSState) }

	below, doc, above := document('a'), document('m'), document('z')
	// Table 1: the document, the middle store, and an old version of every neighbour.
	store.apply([]bloomPair{
		{below, []byte("below-old")}, {doc, []byte("header")}, {fieldKeyOf(doc, 2), []byte("field")}, {above, []byte("above-old")},
		{element(3, 0), []byte("lower-store-old")}, {element(4, 0), []byte("e0")}, {state(4), []byte("state-old")}, {element(5, 0), []byte("upper-store-old")},
	})
	store.flush()
	// Table 2: the newest version of every neighbour and no key of the document or of the
	// middle store. Its range covers both, so both reads meet it, and skip it.
	store.apply([]bloomPair{
		{below, []byte("below-new")}, {fieldKeyOf(below, 2), []byte("below-field")}, {above, []byte("above-new")},
		{element(3, 0), []byte("lower-store-new")}, {element(5, 0), []byte("upper-store-new")},
	})
	store.flush()
	// Table 3: keys of the middle store only, one of them (its state) outside the element range.
	store.apply([]bloomPair{{element(4, 1), []byte("e1")}, {state(4), []byte("state-new")}})
	store.flush()
	if tables := store.kv.TableCount(); tables != 3 {
		t.Fatalf("the store holds %d tables, want 3", tables)
	}

	reversed := func(pairs []bloomPair) []bloomPair {
		out := slices.Clone(pairs)
		slices.Reverse(out)
		return out
	}
	documentRule := func(stats storage.PrefixBloomStats) storage.PrefixBloomRuleStats { return stats.Document }
	storeRule := func(stats storage.PrefixBloomStats) storage.PrefixBloomRuleStats { return stats.Store }

	for _, tc := range []struct {
		name string
		opts storage.IterOptions
		rule func(storage.PrefixBloomStats) storage.PrefixBloomRuleStats
		// inside is what the bounds hold, in key order.
		inside []bloomPair
		// belowBounds and aboveBounds are stored keys on either side of the bounds.
		belowBounds, aboveBounds []byte
		// widerLower and widerUpper are bounds that take in a neighbour whose newest version
		// is in the skipped table; wantLower and wantUpper are what they hold.
		widerLower, widerUpper []byte
		wantLower, wantUpper   []bloomPair
	}{
		{
			name: "a document", opts: storage.DocumentIterOptions(doc), rule: documentRule,
			inside:      []bloomPair{{doc, []byte("header")}, {fieldKeyOf(doc, 2), []byte("field")}},
			belowBounds: below, aboveBounds: above,
			widerLower: below, widerUpper: storage.PrefixEnd(above),
			wantLower: []bloomPair{{below, []byte("below-new")}, {fieldKeyOf(below, 2), []byte("below-field")}, {doc, []byte("header")}, {fieldKeyOf(doc, 2), []byte("field")}},
			wantUpper: []bloomPair{{doc, []byte("header")}, {fieldKeyOf(doc, 2), []byte("field")}, {above, []byte("above-new")}},
		},
		{
			name: "a store field's elements", opts: storage.StoreIterOptions(elements(4), storage.PrefixEnd(elements(4))), rule: storeRule,
			inside:      []bloomPair{{element(4, 0), []byte("e0")}, {element(4, 1), []byte("e1")}},
			belowBounds: element(3, 0), aboveBounds: element(5, 0),
			widerLower: elements(3), widerUpper: storage.PrefixEnd(elements(5)),
			wantLower: []bloomPair{{element(3, 0), []byte("lower-store-new")}, {element(4, 0), []byte("e0")}, {element(4, 1), []byte("e1")}},
			wantUpper: []bloomPair{{element(4, 0), []byte("e0")}, {element(4, 1), []byte("e1")}, {state(4), []byte("state-new")}, {element(5, 0), []byte("upper-store-new")}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if storage.BloomPrefixOf(tc.opts) == nil {
				t.Fatal("the constructor returned plain bounds; this test needs the filter")
			}
			reverse := tc.opts
			reverse.Reverse = true
			for _, walk := range []struct {
				what   string
				opts   storage.IterOptions
				target []byte
				want   []bloomPair
			}{
				{"forward from the start", tc.opts, nil, tc.inside},
				{"forward from a key below the bounds", tc.opts, tc.belowBounds, tc.inside},
				{"forward from a key above the bounds", tc.opts, tc.aboveBounds, nil},
				{"forward from the upper bound", tc.opts, tc.opts.Upper, nil},
				{"in reverse from the end", reverse, nil, reversed(tc.inside)},
				{"in reverse from a key above the bounds", reverse, tc.aboveBounds, reversed(tc.inside)},
				{"in reverse from a key below the bounds", reverse, tc.belowBounds, nil},
			} {
				before := store.kv.PrefixBloomStats()
				got := store.walkFrom(walk.opts, walk.target)
				after := store.kv.PrefixBloomStats()
				if !samePairs(got, walk.want) {
					t.Fatalf("%s the filtered iterator yielded%s, want only the keys inside its bounds:%s", walk.what, describePairs(got), describePairs(walk.want))
				}
				if d := tc.rule(after).Skipped - tc.rule(before).Skipped; d != 1 {
					t.Fatalf("%s the iterator skipped %d tables, want the one that holds the neighbours' newest versions and no key of the prefix", walk.what, d)
				}
			}

			// Bounds widened after the constructor made them reach a neighbour whose newest
			// version is in the table the filter would skip.
			widerLower, widerUpper := tc.opts, tc.opts
			widerLower.Lower, widerUpper.Upper = tc.widerLower, tc.widerUpper
			for _, wider := range []struct {
				what string
				opts storage.IterOptions
				want []bloomPair
			}{
				{"a lower bound moved below the prefix", widerLower, tc.wantLower},
				{"an upper bound moved above the prefix", widerUpper, tc.wantUpper},
			} {
				plain := store.walk(storage.IterOptions{Lower: wider.opts.Lower, Upper: wider.opts.Upper})
				if !samePairs(plain, wider.want) {
					t.Fatalf("with %s the plain bounds hold%s, want%s", wider.what, describePairs(plain), describePairs(wider.want))
				}
				before := store.kv.PrefixBloomStats()
				got := store.walk(wider.opts)
				inReverse := wider.opts
				inReverse.Reverse = true
				gotReversed := store.walk(inReverse)
				after := store.kv.PrefixBloomStats()
				if !samePairs(got, wider.want) {
					t.Fatalf("with %s the walk returned%s, want the newest version of every key:%s", wider.what, describePairs(got), describePairs(wider.want))
				}
				if !samePairs(gotReversed, reversed(wider.want)) {
					t.Fatalf("with %s the reverse walk returned%s, want the newest version of every key:%s", wider.what, describePairs(gotReversed), describePairs(reversed(wider.want)))
				}
				if after != before {
					t.Fatalf("with %s the iterator still asked the filter: %+v, were %+v", wider.what, after, before)
				}
			}
		})
	}
}
