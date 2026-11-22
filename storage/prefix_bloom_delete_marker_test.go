package storage_test

import (
	"encoding/binary"
	"testing"

	"github.com/hoarddb/hoarddb-server/storage"
)

// TestDeleteMarkerOnlyTableIsNotSkipped pins that a delete marker counts as a key of its
// store: a table whose only key under a store prefix is the delete marker of an element,
// above a table that still holds the element, must be opened by a filtered list, or the
// popped or deleted element comes back. The store's own writes cannot build that table (a pop
// or delete also writes the store's state key under the same prefix in the same batch, which
// is why the store package's T12 does not see this); a compaction that cuts its output
// between a store's elements and its state key can, so the tables are built here key by key.
// Against: adding no prefix hash for a delete marker.
func TestDeleteMarkerOnlyTableIsNotSkipped(t *testing.T) {
	id := func(letter byte) []byte { return []byte{storage.TagString, letter, 0x00, 0x00} }
	for _, removal := range []struct {
		name  string
		inner byte // the element namespace inside the store field
	}{
		{"a popped element of an ordered store", storage.NSQueue},
		{"a deleted element of a keyed store", storage.NSDocuments},
	} {
		t.Run(removal.name, func(t *testing.T) {
			store := openBloomStore(t, storage.OpenBadgerKVNoCompaction)
			elements := func(letter byte) []byte {
				return storage.Key(3, 7, 9, storage.NSFieldStore, id(letter), []byte{0x00, 0x04, removal.inner})
			}
			element := func(letter byte, position uint64) []byte {
				return binary.BigEndian.AppendUint64(elements(letter), position)
			}
			state := storage.Key(3, 7, 9, storage.NSFieldStore, id('m'), []byte{0x00, 0x04, storage.NSState})
			removed, kept := element('m', 0), element('m', 1)

			// Table A: two elements of the store and its state key.
			store.apply([]bloomPair{{removed, []byte("removed")}, {kept, []byte("kept")}, {state, []byte("state")}})
			store.flush()
			// Table B: the delete marker of the first element and no other key of the store.
			store.apply([]bloomPair{{removed, nil}})
			store.flush()
			// Table C: other stores on both sides, so its range covers the store and it holds
			// none of it; skipping it shows the filter is at work in this read.
			store.apply([]bloomPair{{element('a', 0), []byte("low")}, {element('z', 0), []byte("high")}})
			store.flush()
			if tables := store.kv.TableCount(); tables != 3 {
				t.Fatalf("the store holds %d tables, want the three flushes", tables)
			}

			lower, upper := elements('m'), storage.PrefixEnd(elements('m'))
			want := []bloomPair{{kept, []byte("kept")}}
			if unfiltered := store.walk(storage.IterOptions{Lower: lower, Upper: upper}); !samePairs(unfiltered, want) {
				t.Fatalf("the list without the filter returned%s, want%s", describePairs(unfiltered), describePairs(want))
			}
			before := store.kv.PrefixBloomStats()
			got := store.walk(storage.StoreIterOptions(lower, upper))
			after := store.kv.PrefixBloomStats()
			candidates := after.Store.Candidates - before.Store.Candidates
			skipped := after.Store.Skipped - before.Store.Skipped
			if !samePairs(got, want) {
				t.Fatalf("the filtered list returned%s, want%s (candidates %d, skipped %d): "+
					"the removed element came back through the filter", describePairs(got), describePairs(want), candidates, skipped)
			}
			if d := after.Store.Iterators - before.Store.Iterators; d != 1 {
				t.Fatalf("the list counted %d filtered iterators, want 1", d)
			}
			if candidates != 3 || skipped != 1 {
				t.Fatalf("the list met %d candidate tables and skipped %d, want 3 and 1: "+
					"only the table of the other stores may be skipped, not the one that holds the delete marker", candidates, skipped)
			}
		})
	}
}
