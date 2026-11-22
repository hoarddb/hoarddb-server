package store

import (
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/storage/kvfault"
)

// The raw-byte unique differential (bson-codec-spec 7.2, build doc step 7a,
// path "unique (store)"). A conflicted unique index's refusal names the value
// two documents hold, rendered from the first holder's stored document, which
// the map path decoded whole. The oracle below is that map path, copied from
// store/unique.go:(*IndexManager).conflictedIndexError as it stood before the
// move (next at bbf315c7). The same documents also go through CheckUnique
// with the whole map, as the server's precheck (server/unique.go) still
// builds it, and with the indexed fields the hash store now reads, which must
// refuse alike.

// oracleConflictedIndexError is conflictedIndexError as it was.
func oracleConflictedIndexError(im *IndexManager, spec IndexSpec) error {
	first, second, found, err := im.indexes[spec.ID].FirstDuplicate(len(spec.Fields))
	if err != nil {
		return err
	}
	refusal := &ConflictedIndexError{Index: spec.Name}
	if !found {
		return refusal
	}
	refusal.Holders = []string{string(first), string(second)}
	snap := im.kv.NewSnapshot()
	defer snap.Discard()
	stored, err := snap.Get(storage.Key(im.slot, 1, im.id, storage.NSDocuments, first))
	if err != nil {
		return refusal
	}
	var document map[string]interface{}
	if bson.Unmarshal(stored, &document) == nil {
		refusal.Value = RenderIndexValue(spec, document)
	}
	return refusal
}

// conflictedHolders stores raw under two keys and gives both one entry of
// spec, as a range that arrived unchecked would; it reports false when the
// document has no entry in spec.
func conflictedHolders(t *testing.T, kv storage.KV, im *IndexManager, spec IndexSpec, raw []byte, decoded map[string]interface{}) bool {
	t.Helper()
	encoded, indexed, err := EntryKey(spec, decoded)
	if err != nil || !indexed {
		return false
	}
	if err := inBatch(kv, func(b storage.KVBatch) error {
		for _, key := range []string{"d1", "d2"} {
			if err := b.Set(storage.Key(1, 1, 1, storage.NSDocuments, []byte(key)), raw); err != nil {
				return err
			}
			if err := im.indexes[spec.ID].Add(b, encoded, []byte(key)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seeding the holders: %v", err)
	}
	return true
}

// TestStoreUniqueMatchesTheMapPath: for every document and every unique
// index over its fields, the conflicted index's refusal reads as the map
// path's, and CheckUnique refuses a second holder alike whether it is given
// the whole document or only its indexed fields.
func TestStoreUniqueMatchesTheMapPath(t *testing.T) {
	documents := differentialStoredDocuments(t)
	conflicts, refusals := 0, 0
	for _, document := range documents {
		var decoded map[string]interface{}
		if err := bson.Unmarshal(document.raw, &decoded); err != nil {
			t.Fatalf("%s: %v", document.name, err)
		}
		stored, err := bson.TrustStored(document.raw)
		if err != nil {
			t.Fatalf("%s: %v", document.name, err)
		}
		for _, spec := range differentialSpecs(document.raw) {
			spec.Unique, spec.ID, spec.Name = true, 0, "unique"
			kv := kvfault.New()
			im, err := NewIndexManager(kv, 1, 1, 1, []IndexSpec{spec})
			if err != nil {
				t.Fatalf("NewIndexManager: %v", err)
			}
			fields, err := im.IndexedFields(stored)
			if err != nil {
				t.Fatalf("%s: reading the indexed fields: %v", document.name, err)
			}
			if err := inBatch(kv, func(b storage.KVBatch) error { return im.OnInsert(b, []byte("held"), decoded) }); err == nil {
				snap := kv.NewSnapshot()
				whole := im.CheckUnique(snap, nil, []byte("second"), nil, decoded)
				read := im.CheckUnique(snap, nil, []byte("second"), nil, fields)
				snap.Discard()
				if errorText(whole) != errorText(read) {
					t.Fatalf("%s, index on %v: CheckUnique with the whole document says %v, with its indexed fields %v",
						document.name, spec.FieldNames(), whole, read)
				}
				if whole != nil {
					refusals++
				}
			}

			kv = kvfault.New()
			im, err = NewIndexManager(kv, 1, 1, 1, []IndexSpec{spec})
			if err != nil {
				t.Fatalf("NewIndexManager: %v", err)
			}
			if !conflictedHolders(t, kv, im, spec, document.raw, decoded) {
				continue
			}
			want, got := oracleConflictedIndexError(im, spec), im.conflictedIndexError(spec)
			if errorText(want) != errorText(got) {
				t.Fatalf("%s, index on %v: the map path says %v, the store says %v", document.name, spec.FieldNames(), want, got)
			}
			conflicts++
		}
	}
	t.Logf("%d documents: %d conflicted refusals and %d duplicate refusals compared", len(documents), conflicts, refusals)
}

// TestConflictedIndexNamesTheValueOfACorruptHolder: a holder whose indexed
// field reads but whose other bytes are not BSON still has its value named.
// The map path decoded the whole document, failed, and left the value out.
func TestConflictedIndexNamesTheValueOfACorruptHolder(t *testing.T) {
	spec := IndexSpec{ID: 0, Name: "a_1", Fields: []IndexKeyPart{{Field: "a"}}, Unique: true, Version: IndexSpecVersion}
	kv := kvfault.New()
	im, err := NewIndexManager(kv, 1, 1, 1, []IndexSpec{spec})
	if err != nil {
		t.Fatalf("NewIndexManager: %v", err)
	}
	if !conflictedHolders(t, kv, im, spec, corruptNested("dalek"), map[string]interface{}{"a": "dalek"}) {
		t.Fatal("the holders have no entry")
	}
	if got := im.conflictedIndexError(spec).Error(); !strings.Contains(got, `a = "dalek"`) {
		t.Fatalf("the refusal is %q; want it to name a = \"dalek\"", got)
	}

	// A holder whose top level does not walk names no value, as before.
	kv = kvfault.New()
	im, err = NewIndexManager(kv, 1, 1, 1, []IndexSpec{spec})
	if err != nil {
		t.Fatalf("NewIndexManager: %v", err)
	}
	unwalkable := rawDoc(rawElem(0x02, "a", append(rawLE32(500), 'x', 0)...))
	if !conflictedHolders(t, kv, im, spec, unwalkable, map[string]interface{}{"a": "dalek"}) {
		t.Fatal("the holders have no entry")
	}
	if got := im.conflictedIndexError(spec).Error(); strings.Contains(got, "dalek") || !strings.Contains(got, `"d1" and "d2"`) {
		t.Fatalf("the refusal is %q; want both holders named and no value", got)
	}
}
