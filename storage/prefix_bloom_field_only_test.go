package storage_test

import (
	"testing"

	"github.com/hoarddb/hoarddb-server/storage"
)

// TestFieldOnlyTableIsNotSkipped is the store half of the prefix bloom spec's T2: a
// document's header is flushed into one table and a later update of one field into another,
// which holds a key of the document and not its header. The read must return the new field,
// and the second table must not be counted as skipped.
// Against: skipping on the header key's hash alone.
func TestFieldOnlyTableIsNotSkipped(t *testing.T) {
	store := openBloomStore(t, storage.OpenBadgerKVNoCompaction)
	low := storage.Key(3, 7, 9, storage.NSDocuments, []byte{storage.TagString, 'a', 0x00, 0x00})
	doc := storage.Key(3, 7, 9, storage.NSDocuments, []byte{storage.TagString, 'm', 0x00, 0x00})
	high := storage.Key(3, 7, 9, storage.NSDocuments, []byte{storage.TagString, 'z', 0x00, 0x00})
	field := fieldKeyOf(doc, 2)

	// Table A: the header and the field's first value.
	store.apply([]bloomPair{{doc, []byte("header")}, {field, []byte("old")}})
	store.flush()
	// Table B: the field alone.
	store.apply([]bloomPair{{field, []byte("new")}})
	store.flush()
	// Table C: neighbours on both sides, so its range covers the document and it holds none of it.
	store.apply([]bloomPair{{low, []byte("low")}, {high, []byte("high")}})
	store.flush()
	if tables := store.kv.TableCount(); tables != 3 {
		t.Fatalf("the store holds %d tables, want the three flushes", tables)
	}

	before := store.kv.PrefixBloomStats()
	got := store.walk(storage.DocumentIterOptions(doc))
	after := store.kv.PrefixBloomStats()
	want := []bloomPair{{doc, []byte("header")}, {field, []byte("new")}}
	if !samePairs(got, want) {
		t.Fatalf("the filtered read returned%s, want%s", describePairs(got), describePairs(want))
	}
	if d := after.Document.Iterators - before.Document.Iterators; d != 1 {
		t.Fatalf("the read counted %d filtered iterators, want 1", d)
	}
	if d := after.Document.Candidates - before.Document.Candidates; d != 3 {
		t.Fatalf("the read counted %d candidate tables, want 3", d)
	}
	if d := after.Document.Skipped - before.Document.Skipped; d != 1 {
		t.Fatalf("the read skipped %d tables, want only the one that holds no key of the document; "+
			"the table with the field and no header must not be skipped", d)
	}
}
