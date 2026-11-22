package store

import (
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/storage/kvfault"

	"github.com/hoarddb/hoarddb-bson/bson/primitive"
)

// inBatch runs add against a new batch of kv and commits it: index writes go
// into the batch of the document write they belong to.
func inBatch(kv storage.KV, add func(storage.KVBatch) error) error {
	batch := kv.NewBatch()
	defer batch.Discard()
	if err := add(batch); err != nil {
		return err
	}
	return batch.Commit(false)
}

// oneFieldSpecs declares one single-field index per name, in order, the way
// this build's declaration parser does.
func oneFieldSpecs(names ...string) []IndexSpec {
	specs := make([]IndexSpec, 0, len(names))
	for position, name := range names {
		fields := []IndexKeyPart{{Field: name}}
		specs = append(specs, IndexSpec{ID: uint16(position), Name: DefaultIndexName(fields), Fields: fields, Version: IndexSpecVersion})
	}
	return specs
}

// planned is the document keys the planner's index returns for filter.
func planned(t *testing.T, im *IndexManager, filter map[string]interface{}) ([][]byte, error) {
	t.Helper()
	plan, err := im.PlanSearch(filter)
	if err != nil || plan == nil {
		return nil, err
	}
	return plan.DocumentKeys()
}

// TestIndexManagerEqualityRoundTrip checks the basic promise of a secondary
// index: what OnInsert writes, Search must find.
func TestIndexManagerEqualityRoundTrip(t *testing.T) {
	kv := kvfault.New()
	im, err := NewIndexManager(kv, 1, 1, 1, oneFieldSpecs("Email"))
	if err != nil {
		t.Fatalf("NewIndexManager: %v", err)
	}

	doc := map[string]interface{}{"_id_": "u1", "Email": "dalek@example.com", "Age": int64(42)}
	if err := inBatch(kv, func(b storage.KVBatch) error { return im.OnInsert(b, []byte("u1"), doc) }); err != nil {
		t.Fatalf("OnInsert: %v", err)
	}

	ids, err := planned(t, im, map[string]interface{}{"Email": "dalek@example.com"})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(ids) != 1 || string(ids[0]) != "u1" {
		t.Fatalf("Search returned %q, want [u1]", ids)
	}

	// A different value must not match.
	miss, err := planned(t, im, map[string]interface{}{"Email": "other@example.com"})
	if err != nil {
		t.Fatalf("Search(miss): %v", err)
	}
	if len(miss) != 0 {
		t.Fatalf("Search for an unwritten value returned %q", miss)
	}

	// Deleting the document removes its index entry.
	if err := inBatch(kv, func(b storage.KVBatch) error { return im.OnDelete(b, []byte("u1"), doc) }); err != nil {
		t.Fatalf("OnDelete: %v", err)
	}
	after, err := planned(t, im, map[string]interface{}{"Email": "dalek@example.com"})
	if err != nil {
		t.Fatalf("Search(after delete): %v", err)
	}
	if len(after) != 0 {
		t.Fatalf("Search after delete returned %q, want none", after)
	}
}

// TestHashStorePutFeedsIndex guards the wiring between the primary store and
// the index manager: a hash bucket with a declared index must index every
// document it stores, or searches silently return nothing.
func TestHashStorePutFeedsIndex(t *testing.T) {
	kv := kvfault.New()
	im, err := NewIndexManager(kv, 1, 1, 1, oneFieldSpecs("Email"))
	if err != nil {
		t.Fatalf("NewIndexManager: %v", err)
	}

	hs := NewHashStore(kv, 1, 1, 1, new(OwedApplies))
	hs.SetIndexManager(im)

	if err := hs.Put(ApplyInfo{}, "u1", map[string]interface{}{
		"_id_": "u1", "Email": "dalek@example.com", "Age": int64(42),
	}, 0); err != nil {
		t.Fatalf("HashStore.Put: %v", err)
	}

	ids, err := planned(t, im, map[string]interface{}{"Email": "dalek@example.com"})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(ids) != 1 || string(ids[0]) != "u1" {
		t.Fatalf("after HashStore.Put, Search returned %q, want [u1]", ids)
	}

	// Deleting through the store must clear the index entry too.
	if err := hs.Delete(ApplyInfo{}, "u1"); err != nil {
		t.Fatalf("HashStore.Delete: %v", err)
	}
	after, err := planned(t, im, map[string]interface{}{"Email": "dalek@example.com"})
	if err != nil {
		t.Fatalf("Search(after delete): %v", err)
	}
	if len(after) != 0 {
		t.Fatalf("Search after HashStore.Delete returned %q, want none", after)
	}
}

// supportedTypesIn returns the list a refusal names after "supported: ".
func supportedTypesIn(t *testing.T, message string) string {
	t.Helper()
	const marker = "supported: "
	start := strings.Index(message, marker)
	if start < 0 {
		t.Fatalf("the refusal %q names no supported list", message)
	}
	rest := message[start+len(marker):]
	end := strings.Index(rest, ")")
	if end < 0 {
		t.Fatalf("the supported list in %q is not closed", message)
	}
	return rest[:end]
}

// TestIndexRefusalListsTheEncoderTypes (U3): the refusal a developer reads when
// an indexed field holds a value no key can hold names exactly the types the
// encoder accepts. Both lists come from the errors the write path returns, so a
// type added to the encoder cannot be missing from the message.
func TestIndexRefusalListsTheEncoderTypes(t *testing.T) {
	decimal := primitive.NewDecimal128(1, 0)
	_, _, indexErr := EntryKey(oneFieldSpecs("Email")[0], map[string]interface{}{"Email": decimal})
	if indexErr == nil {
		t.Fatal("an indexed decimal128 was accepted")
	}
	_, encodeErr := EncodeComponent(decimal)
	if encodeErr == nil {
		t.Fatal("the encoder accepted a decimal128")
	}
	if got, want := supportedTypesIn(t, indexErr.Error()), supportedTypesIn(t, encodeErr.Error()); got != want {
		t.Fatalf("the index refusal lists %q, the encoder lists %q: the developer is told a different set", got, want)
	}
}

// TestEncoderAcceptedTypesAreIndexable (U3b): every type the encoder accepts is
// an indexable value, so the write path never reaches the refusal that names a
// value's BSON type. An ObjectID is the case that used to be the exception: U3
// made it a key type, so an indexed field holding one gets an entry, and a
// refusal naming "an ObjectID" could never be the message a developer reads.
func TestEncoderAcceptedTypesAreIndexable(t *testing.T) {
	accepted := []interface{}{
		nil, primitive.Null{}, true, 7, int64(7), 2.5,
		primitive.DateTime(1), time.UnixMilli(1), "text", []byte{1},
		primitive.Binary{Data: []byte{1}}, primitive.NewObjectID(),
	}
	spec := oneFieldSpecs("Field")[0]
	for _, value := range accepted {
		if _, err := EncodeComponent(value); err != nil {
			t.Fatalf("EncodeComponent(%T) = %v: an accepted type must encode", value, err)
		}
		encoded, indexed, err := EntryKey(spec, map[string]interface{}{"Field": value})
		if err != nil || !indexed || len(encoded) == 0 {
			t.Fatalf("EntryKey with a %T = %x, %v, %v: want an entry, not a refusal", value, encoded, indexed, err)
		}
	}
}
