package storage

import (
	"bytes"
	"encoding/binary"
	mathrand "math/rand/v2"
	"slices"
	"testing"
)

// prefixLenComponents is one encoded _id_ of each of the seven tags, built by hand so the
// vectors below do not move when an encoder does.
func prefixLenComponents() map[string][]byte {
	number := append([]byte{TagNumber}, bytes.Repeat([]byte{0x80}, 17)...)
	datetime := append([]byte{TagDateTime}, 0x80, 0, 0, 0, 0, 0, 0, 1)
	objectID := append([]byte{TagObjectID}, bytes.Repeat([]byte{0xAB}, 12)...)
	return map[string][]byte{
		"null":             {TagNull},
		"bool":             {TagBool, 0x01},
		"number":           number,
		"datetime":         datetime,
		"objectid":         objectID,
		"string a":         {TagString, 'a', 0x00, 0x00},
		"string a\\x00":    {TagString, 'a', 0x00, 0xFF, 0x00, 0x00},
		"string ab":        {TagString, 'a', 'b', 0x00, 0x00},
		"string empty":     {TagString, 0x00, 0x00},
		"binary 00 ff 00":  {TagBinary, 0x00, 0xFF, 0xFF, 0x00, 0xFF, 0x00, 0x00},
		"string trail nul": {TagString, 0x00, 0xFF, 0x00, 0xFF, 0x00, 0x00},
	}
}

// TestPrefixLenFunctions is the prefix bloom spec's T8: the outputs of the two rule functions
// are frozen (I-3: a table's rule bit names the function that hashed its prefixes, so an output
// that moves needs a new rule id and a format bump), and both hold the lemma the read side
// rests on.
// Against: reading one byte past the component; a rule that answers for the other namespace.
func TestPrefixLenFunctions(t *testing.T) {
	if DocumentPrefixRule != 1 || StorePrefixRule != 2 {
		t.Fatalf("the rule ids are %d and %d, want 1 and 2: an id names its function on disk", DocumentPrefixRule, StorePrefixRule)
	}
	components := prefixLenComponents()
	wantLen := map[string]int{
		"null": 1, "bool": 2, "number": 18, "datetime": 9, "objectid": 13,
		"string a": 4, "string a\\x00": 6, "string ab": 5, "string empty": 3,
		"binary 00 ff 00": 8, "string trail nul": 7,
	}
	fieldID := []byte{0x00, 0x02}

	type vector struct {
		name     string
		key      []byte
		document int
		store    int
	}
	var vectors []vector
	for name, encID := range components {
		n := wantLen[name]
		header := Key(5, 7, 9, NSDocuments, encID)
		vectors = append(vectors,
			vector{"header " + name, header, 19 + n, 0},
			vector{"field " + name, append(bytes.Clone(header), fieldID...), 19 + n, 0},
			// A document key cut after its id is still its prefix; the tail is not read.
			vector{"field and junk " + name, append(bytes.Clone(header), 0xFF, 0xFF, 0xFF), 19 + n, 0},
		)
		owner := Key(5, 7, 9, NSFieldStore, encID)
		storePrefix := append(bytes.Clone(owner), fieldID...)
		vectors = append(vectors,
			// The owner alone, and the owner cut inside the field id, name no store.
			vector{"owner " + name, owner, 0, 0},
			vector{"owner and half a field id " + name, append(bytes.Clone(owner), 0x00), 0, 0},
			vector{"store prefix " + name, storePrefix, 0, 19 + n + 2},
		)
		for _, inner := range []byte{NSDocuments, NSQueue, NSState, NSFieldStore, NSDelivery} {
			vectors = append(vectors, vector{"store inner " + name, append(bytes.Clone(storePrefix), inner, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x09), 0, 19 + n + 2})
		}
		// A store declared on an element of a store: its keys carry the outer store's prefix.
		nested := append(bytes.Clone(storePrefix), NSFieldStore)
		nested = append(nested, TagString, 'e', 0x00, 0x00, 0x00, 0x03, NSQueue, 0, 0, 0, 0, 0, 0, 0, 1)
		vectors = append(vectors, vector{"store on an element " + name, nested, 0, 19 + n + 2})
	}

	stringHeader := Key(5, 7, 9, NSDocuments, components["string ab"])
	atSlotCount := bytes.Clone(stringHeader)
	binary.BigEndian.PutUint16(atSlotCount[0:2], SlotCount)
	lastSlot := bytes.Clone(stringHeader)
	binary.BigEndian.PutUint16(lastSlot[0:2], SlotCount-1)
	vectors = append(vectors,
		vector{"the last slot", lastSlot, 24, 0},
		vector{"a slot at SlotCount", atSlotCount, 0, 0},
		vector{"a node record", NodeKey(NSCatalog, components["string ab"]), 0, 0},
		vector{"a node record shaped like a document", append(NodeKey(NSDocuments), make([]byte, 20)...), 0, 0},
		vector{"badger's own key", []byte("!badger!txn"), 0, 0},
		vector{"empty", nil, 0, 0},
		vector{"the scope and namespace alone", Key(5, 7, 9, NSDocuments), 0, 0},
		vector{"an unknown tag", Key(5, 7, 9, NSDocuments, []byte{0x11, 0x00}), 0, 0},
		vector{"a number cut short", Key(5, 7, 9, NSDocuments, []byte{TagNumber, 1, 2, 3}), 0, 0},
		vector{"a string with no terminator", Key(5, 7, 9, NSDocuments, []byte{TagString, 'a', 'b'}), 0, 0},
		vector{"a string ending in half a terminator", Key(5, 7, 9, NSDocuments, []byte{TagString, 'a', 0x00}), 0, 0},
		vector{"a string with a 0x00 no encoder writes", Key(5, 7, 9, NSDocuments, []byte{TagString, 'a', 0x00, 0x01, 0x00, 0x00}), 0, 0},
		vector{"a store string with no terminator", Key(5, 7, 9, NSFieldStore, []byte{TagString, 'a', 'b'}), 0, 0},
		// Raw legacy keys under NSDocuments: a first byte that is no tag is 0; one that is a
		// tag ('0' number, 'P' string, 'p' ObjectID) decodes as that tag or fails as it.
		vector{"a raw legacy key, no tag", Key(5, 7, 9, NSDocuments, []byte("user:1")), 0, 0},
		vector{"a raw legacy key starting 0, short", Key(5, 7, 9, NSDocuments, []byte("0042")), 0, 0},
		vector{"a raw legacy key starting 0, long", Key(5, 7, 9, NSDocuments, []byte("0123456789abcdefghij")), 19 + 18, 0},
		vector{"a raw legacy key starting P", Key(5, 7, 9, NSDocuments, []byte("Paris")), 0, 0},
		vector{"a raw legacy key starting p, long", Key(5, 7, 9, NSDocuments, []byte("product:000123")), 19 + 13, 0},
	)
	for _, ns := range []byte{NSVersions, NSIndex, NSQueue, NSState, NSBlobMeta, NSDelivery, NSDedupe, 0x11} {
		vectors = append(vectors, vector{"another namespace", Key(5, 7, 9, ns, components["string ab"], fieldID, []byte{NSQueue}), 0, 0})
	}

	for _, v := range vectors {
		if got := DocumentPrefixLen(v.key); got != v.document {
			t.Errorf("%s: DocumentPrefixLen(%x) = %d, want %d", v.name, v.key, got, v.document)
		}
		if got := StorePrefixLen(v.key); got != v.store {
			t.Errorf("%s: StorePrefixLen(%x) = %d, want %d", v.name, v.key, got, v.store)
		}
	}

	// The lemma, on the vectors and on random keys: an answer n names key[:n] as the prefix,
	// the prefix answers its own length, and so does the prefix with any tail. No key is
	// answered by both rules.
	rng := mathrand.New(mathrand.NewPCG(8, 15))
	keys := make([][]byte, 0, len(vectors)+4000)
	for _, v := range vectors {
		keys = append(keys, v.key)
	}
	for range 4000 {
		keys = append(keys, randomPrefixLenKey(rng, components))
	}
	answered := 0
	for _, key := range keys {
		document, store := DocumentPrefixLen(key), StorePrefixLen(key)
		if document != 0 && store != 0 {
			t.Fatalf("key %x is answered by both rules (%d and %d)", key, document, store)
		}
		for rule, lenOf := range map[string]func([]byte) int{"document": DocumentPrefixLen, "store": StorePrefixLen} {
			n := lenOf(key)
			if n == 0 {
				continue
			}
			answered++
			if n < 0 || n > len(key) {
				t.Fatalf("%s rule: length %d for a key of %d bytes (%x)", rule, n, len(key), key)
			}
			if got := lenOf(key[:n]); got != n {
				t.Fatalf("%s rule: the prefix %x of %x answers %d, want its own length %d", rule, key[:n], key, got, n)
			}
			tail := make([]byte, rng.IntN(12))
			for i := range tail {
				tail[i] = byte(rng.UintN(256))
			}
			extended := append(bytes.Clone(key[:n]), tail...)
			if got := lenOf(extended); got != n {
				t.Fatalf("%s rule: the prefix %x with tail %x answers %d, want %d", rule, key[:n], tail, got, n)
			}
		}
	}
	if answered < 1000 {
		t.Fatalf("only %d keys were answered by a rule; the property ran on too few", answered)
	}
}

// randomPrefixLenKey is a key shaped like a stored one about two times in three, and damaged
// (cut, a byte flipped) otherwise.
func randomPrefixLenKey(rng *mathrand.Rand, components map[string][]byte) []byte {
	names := make([]string, 0, len(components))
	for name := range components {
		names = append(names, name)
	}
	// Map order is random; the draw must not be, so the seed reproduces a failure.
	slices.Sort(names)
	encID := components[names[rng.IntN(len(names))]]
	namespaces := []byte{NSDocuments, NSFieldStore, NSDocuments, NSFieldStore, NSIndex, NSQueue}
	key := Key(uint16(rng.UintN(SlotCount+2)), rng.Uint64(), rng.Uint64(), namespaces[rng.IntN(len(namespaces))], encID)
	tail := make([]byte, rng.IntN(14))
	for i := range tail {
		tail[i] = byte(rng.UintN(256))
	}
	key = append(key, tail...)
	switch rng.IntN(6) {
	case 0:
		key = key[:rng.IntN(len(key)+1)]
	case 1:
		key[rng.IntN(len(key))] ^= byte(1 + rng.UintN(255))
	}
	return key
}
