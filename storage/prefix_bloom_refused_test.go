package storage_test

import (
	"encoding/binary"
	"testing"

	"github.com/hoarddb/hoarddb-server/storage"
)

// TestNonRulePrefixIsRefused is the prefix bloom spec's T7, the proof of I-4's second half: a
// prefix that no rule names as a whole prefix never filters. The constructors return plain
// bounds for it, and a walk of those moves no counter; and when the store is handed such a
// prefix all the same (the field forced, as a caller could if it were exported), Badger
// refuses it, counts the refusal, and the walk returns what the plain bounds return. A store
// opened without rules refuses every prefix.
// Against: dropping the whole-length check, in Badger (a document prefix followed by one byte
// of a field id is then hashed whole, no filter holds that hash, and the fields are lost) or
// in DocumentIterOptions.
func TestNonRulePrefixIsRefused(t *testing.T) {
	store := openBloomStore(t, storage.OpenBadgerKVNoCompaction)
	id := []byte{storage.TagString, 'm', 0x00, 0x00}
	doc := storage.Key(3, 7, 9, storage.NSDocuments, id)
	owner := storage.Key(3, 7, 9, storage.NSFieldStore, id)
	storePrefix := append(append([]byte(nil), owner...), 0x00, 0x04)
	elements := append(append([]byte(nil), storePrefix...), storage.NSQueue)
	element := func(position uint64) []byte {
		return binary.BigEndian.AppendUint64(append([]byte(nil), elements...), position)
	}
	otherStoreElement := binary.BigEndian.AppendUint64(append(append([]byte(nil), owner...), 0x00, 0x05, storage.NSQueue), 0)
	nodeRecord := storage.NodeKey(storage.NSCatalog, []byte("prefix-bloom-t7"))

	// Two tables, each with keys of the document and of the store and neither with all of them.
	store.apply([]bloomPair{
		{doc, []byte("header")}, {fieldKeyOf(doc, 2), []byte("f2")},
		{element(0), []byte("e0")}, {otherStoreElement, []byte("other")}, {nodeRecord, []byte("node")},
	})
	store.flush()
	store.apply([]bloomPair{{fieldKeyOf(doc, 3), []byte("f3")}, {element(1), []byte("e1")}})
	store.flush()
	if tables := store.kv.TableCount(); tables != 2 {
		t.Fatalf("the store holds %d tables, want 2", tables)
	}

	prefixBounds := func(prefix []byte) storage.IterOptions {
		return storage.IterOptions{Lower: prefix, Upper: storage.PrefixEnd(prefix)}
	}
	documentCut := append(append([]byte(nil), doc...), 0x00)
	for _, tc := range []struct {
		name string
		// constructed is what a constructor returns for the prefix; plain is the same bounds
		// built by hand.
		constructed, plain storage.IterOptions
		// forced is the prefix handed to the store regardless, or nil when the bounds leave it
		// (the store then never asks Badger).
		forced []byte
		// wantKeys is how many keys the bounds hold, so a case cannot pass by reading nothing.
		wantKeys int
	}{
		{"a bucket scope", storage.DocumentIterOptions(storage.Scope(3, 7, 9)), prefixBounds(storage.Scope(3, 7, 9)), storage.Scope(3, 7, 9), 6},
		{"a bucket's document namespace", storage.DocumentIterOptions(storage.Key(3, 7, 9, storage.NSDocuments)), prefixBounds(storage.Key(3, 7, 9, storage.NSDocuments)), storage.Key(3, 7, 9, storage.NSDocuments), 3},
		{"a document owner prefix under NSFieldStore, without the field id", storage.StoreIterOptions(owner, storage.PrefixEnd(owner)), prefixBounds(owner), owner, 3},
		{"the same owner prefix, as a document", storage.DocumentIterOptions(owner), prefixBounds(owner), owner, 3},
		{"an id cut before its terminator", storage.DocumentIterOptions(doc[:len(doc)-2]), prefixBounds(doc[:len(doc)-2]), doc[:len(doc)-2], 3},
		{"a document prefix and one byte of a field id", storage.DocumentIterOptions(documentCut), prefixBounds(documentCut), documentCut, 2},
		{"a field key", storage.DocumentIterOptions(fieldKeyOf(doc, 3)), prefixBounds(fieldKeyOf(doc, 3)), fieldKeyOf(doc, 3), 1},
		{"a store prefix and its inner byte", storage.IterOptions{Lower: elements, Upper: storage.PrefixEnd(elements)}, prefixBounds(elements), elements, 2},
		{"a node record", storage.DocumentIterOptions(nodeRecord), prefixBounds(nodeRecord), nodeRecord, 1},
		{"a store's elements with an upper bound outside the store", storage.StoreIterOptions(elements, storage.PrefixEnd(owner)), storage.IterOptions{Lower: elements, Upper: storage.PrefixEnd(owner)}, nil, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := storage.BloomPrefixOf(tc.constructed); got != nil {
				t.Fatalf("the constructor set the filter prefix %x; the bounds must come back plain", got)
			}
			want := store.walk(tc.plain)
			if len(want) != tc.wantKeys {
				t.Fatalf("the plain bounds hold %d keys, want %d:%s", len(want), tc.wantKeys, describePairs(want))
			}
			before := store.kv.PrefixBloomStats()
			if got := store.walk(tc.constructed); !samePairs(got, want) {
				t.Fatalf("the constructed bounds returned%s, the plain bounds%s", describePairs(got), describePairs(want))
			}
			if after := store.kv.PrefixBloomStats(); after != before {
				t.Fatalf("a walk of plain bounds moved the prefix bloom counters: %+v, were %+v", after, before)
			}
			if tc.forced == nil {
				return
			}
			got := store.walk(storage.WithBloomPrefix(tc.plain, tc.forced))
			after := store.kv.PrefixBloomStats()
			if !samePairs(got, want) {
				t.Fatalf("with the prefix forced the walk returned%s, the plain bounds%s", describePairs(got), describePairs(want))
			}
			if after.Refused != before.Refused+1 {
				t.Fatalf("the forced prefix was refused %d times, want 1", after.Refused-before.Refused)
			}
			if after.Document != before.Document || after.Store != before.Store {
				t.Fatalf("a refused prefix moved a rule's counters: %+v, were %+v", after, before)
			}
		})
	}

	// The whole prefixes are accepted on this store, so the refusals above are not the
	// store refusing everything.
	before := store.kv.PrefixBloomStats()
	if got := store.walk(storage.DocumentIterOptions(doc)); len(got) != 3 {
		t.Fatalf("the document read returned%s, want its three keys", describePairs(got))
	}
	if got := store.walk(storage.StoreIterOptions(elements, storage.PrefixEnd(elements))); len(got) != 2 {
		t.Fatalf("the store list returned%s, want its two elements", describePairs(got))
	}
	after := store.kv.PrefixBloomStats()
	if after.Refused != before.Refused || after.Document.Iterators != before.Document.Iterators+1 || after.Store.Iterators != before.Store.Iterators+1 {
		t.Fatalf("the whole prefixes were not accepted once each: %+v, were %+v", after, before)
	}

	t.Run("a store without rules", func(t *testing.T) {
		ruleless := openBloomStore(t, storage.OpenBadgerKVWithoutPrefixRules)
		ruleless.apply([]bloomPair{{doc, []byte("header")}, {fieldKeyOf(doc, 2), []byte("f2")}, {element(0), []byte("e0")}})
		ruleless.flush()
		for _, opts := range []storage.IterOptions{storage.DocumentIterOptions(doc), storage.StoreIterOptions(elements, storage.PrefixEnd(elements))} {
			if storage.BloomPrefixOf(opts) == nil {
				t.Fatalf("the constructor returned plain bounds for the whole prefix %x", opts.Lower)
			}
			want := ruleless.walk(storage.IterOptions{Lower: opts.Lower, Upper: opts.Upper})
			if got := ruleless.walk(opts); len(got) == 0 || !samePairs(got, want) {
				t.Fatalf("on a store without rules the filtered bounds returned%s, the plain bounds%s", describePairs(got), describePairs(want))
			}
		}
		if stats := ruleless.kv.PrefixBloomStats(); stats.Refused != 2 || stats.Document != (storage.PrefixBloomRuleStats{}) || stats.Store != (storage.PrefixBloomRuleStats{}) {
			t.Fatalf("a store without rules must refuse both prefixes and count nothing else: %+v", stats)
		}
	})
}
