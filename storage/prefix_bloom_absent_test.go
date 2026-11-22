package storage_test

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/hoarddb/hoarddb-server/storage"
)

// TestAbsentPrefixSkipsTablesAndCounts is the prefix bloom spec's T3: for each rule, a read
// of a prefix no table holds, while every table's key range covers it, opens no table: that
// rule's Candidates and Skipped rise by the table count and the read finds nothing. The other
// rule's counters and a second store in the same process do not move (the counters belong to
// the instance).
// Against: removing the probe.
func TestAbsentPrefixSkipsTablesAndCounts(t *testing.T) {
	const tables = 3
	store := openBloomStore(t, storage.OpenBadgerKVNoCompaction)
	bystander := openBloomStore(t, storage.OpenBadgerKVNoCompaction)

	id := func(letter byte) []byte { return []byte{storage.TagString, letter, 0x00, 0x00} }
	storeKey := func(letter byte, position uint64) []byte {
		key := storage.Key(3, 7, 9, storage.NSFieldStore, id(letter), []byte{0x00, 0x04, storage.NSQueue})
		return binary.BigEndian.AppendUint64(key, position)
	}
	for table := range tables {
		value := fmt.Appendf(nil, "t%d", table)
		store.apply([]bloomPair{
			{storage.Key(3, 7, 9, storage.NSDocuments, id('a')), value},
			{storage.Key(3, 7, 9, storage.NSDocuments, id('z')), value},
			{storeKey('a', uint64(table)), value},
			{storeKey('z', uint64(table)), value},
		})
		store.flush()
	}
	if got := store.kv.TableCount(); got != tables {
		t.Fatalf("the store holds %d tables, want %d", got, tables)
	}
	bystander.apply([]bloomPair{{storage.Key(3, 7, 9, storage.NSDocuments, id('a')), []byte("b")}})
	bystander.flush()

	absentDocument := storage.Key(3, 7, 9, storage.NSDocuments, id('m'))
	absentLower := storage.Key(3, 7, 9, storage.NSFieldStore, id('m'), []byte{0x00, 0x04, storage.NSQueue})
	absentUpper := storage.Key(3, 7, 9, storage.NSFieldStore, id('m'), []byte{0x00, 0x04, storage.NSQueue + 1})

	start := store.kv.PrefixBloomStats()
	bystanderStart := bystander.kv.PrefixBloomStats()

	if got := store.walk(storage.DocumentIterOptions(absentDocument)); len(got) != 0 {
		t.Fatalf("the read of an absent document returned%s", describePairs(got))
	}
	afterDocument := store.kv.PrefixBloomStats()
	if d := afterDocument.Document.Iterators - start.Document.Iterators; d != 1 {
		t.Fatalf("rule 1 counted %d iterators, want 1", d)
	}
	if d := afterDocument.Document.Candidates - start.Document.Candidates; d != tables {
		t.Fatalf("rule 1 counted %d candidate tables, want %d", d, tables)
	}
	if d := afterDocument.Document.Skipped - start.Document.Skipped; d != tables {
		t.Fatalf("rule 1 skipped %d tables, want all %d: none holds a key of the document", d, tables)
	}
	if afterDocument.Store != start.Store {
		t.Fatalf("a document read moved the store rule's counters: %+v, were %+v", afterDocument.Store, start.Store)
	}

	if got := store.walk(storage.StoreIterOptions(absentLower, absentUpper)); len(got) != 0 {
		t.Fatalf("the list of an absent store returned%s", describePairs(got))
	}
	// A present store is still read, and in reverse (a lifo peek).
	present := storage.StoreIterOptions(storeKey('a', 0)[:len(absentLower)], storage.PrefixEnd(storeKey('a', 0)[:len(absentLower)]))
	present.Reverse = true
	if got := store.walk(present); len(got) != tables {
		t.Fatalf("the reverse list of a present store returned %d elements, want %d:%s", len(got), tables, describePairs(got))
	}
	afterStore := store.kv.PrefixBloomStats()
	if d := afterStore.Store.Iterators - afterDocument.Store.Iterators; d != 2 {
		t.Fatalf("rule 2 counted %d iterators, want 2", d)
	}
	if d := afterStore.Store.Candidates - afterDocument.Store.Candidates; d != 2*tables {
		t.Fatalf("rule 2 counted %d candidate tables, want %d", d, 2*tables)
	}
	if d := afterStore.Store.Skipped - afterDocument.Store.Skipped; d != tables {
		t.Fatalf("rule 2 skipped %d tables, want the %d of the absent store and none of the present one", d, tables)
	}
	if afterStore.Document != afterDocument.Document {
		t.Fatalf("a store read moved the document rule's counters: %+v, were %+v", afterStore.Document, afterDocument.Document)
	}
	if afterStore.Refused != start.Refused || afterStore.Document.Unmarked != 0 || afterStore.Store.Unmarked != 0 {
		t.Fatalf("refusals or unmarked tables were counted: %+v", afterStore)
	}
	if afterStore.Document.PrefixHashes != start.Document.PrefixHashes || afterStore.Store.PrefixHashes != start.Store.PrefixHashes {
		t.Fatalf("a read moved the build-side counters: %+v, were %+v", afterStore, start)
	}
	if got := bystander.kv.PrefixBloomStats(); got != bystanderStart {
		t.Fatalf("reads of one store moved another store's counters: %+v, were %+v", got, bystanderStart)
	}
}
