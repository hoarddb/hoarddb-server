package wire

import (
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/bsontype"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/storage/kvfault"
	"github.com/hoarddb/hoarddb-server/store"
)

// u72BoundWorld is a fault store holding one live document whose scalar fields are the given
// stored values, and an applier over it.
type u72BoundWorld struct {
	kv      *kvfault.Store
	applier *DocumentApplier
	doc     DocumentKey
}

func u72BoundSeed(t *testing.T, bound int64, stored map[uint16][]byte) u72BoundWorld {
	t.Helper()
	kv := kvfault.New()
	kv.SetMaxBatchBytes(bound)
	doc := u35Doc(t, "a")
	insert := u35Insert(doc).Insert
	insert.Fields = nil
	for id, value := range stored {
		insert.Fields = append(insert.Fields, store.DocumentField{FieldID: id, Stored: value})
	}
	u35Commit(t, kv, func(_ storage.KVSnapshot, batch storage.KVBatch) error { return store.WriteInsert(batch, insert) })
	return u72BoundWorld{kv: kv, applier: NewDocumentApplier(kv, nil).WithFieldEngines(u67NoStores{}), doc: doc}
}

func (w u72BoundWorld) update(fields ...cluster.FieldOp) cluster.Op {
	return cluster.Op{Class: cluster.OpUpdate, BucketID: u35Bucket, EncID: w.doc.EncID, Fields: fields}
}

func (w u72BoundWorld) precondition(t *testing.T, op cluster.Op) cluster.Code {
	t.Helper()
	code, err := w.applier.Precondition(u35Slot, u35DB, op)
	if err != nil {
		t.Fatal(err)
	}
	return code
}

func u72Megabyte(fill string) []byte { return stringValue(strings.Repeat(fill, 1<<20)) }

// TestU72MixedStatementIsBoundedByWhatItWrites: [GUARD one accounting walks every field and
// charges what fillUpdate writes: the set values, and the RESULT of a push].
func TestU72MixedStatementIsBoundedByWhatItWrites(t *testing.T) {
	array := appendedArray(nil, u72Megabyte("a"))
	world := u72BoundSeed(t, 4<<20, map[uint16][]byte{4: array})
	mixed := world.update(
		cluster.FieldOp{FieldID: 2, OpCode: cluster.UpdateSet, Value: u72Megabyte("b")},
		cluster.FieldOp{FieldID: 3, OpCode: cluster.UpdateSet, Value: u72Megabyte("c")},
		cluster.FieldOp{FieldID: 4, OpCode: cluster.UpdatePush, Value: u72Megabyte("d")})
	if code := world.precondition(t, mixed); code != cluster.CodeInvalidArgument {
		t.Fatalf("two 1 MiB sets and a push of 1 MiB onto 1 MiB write about 4 MiB: code %v, want INVALID_ARGUMENT", code)
	}
	words, said := world.applier.Explain(cluster.CodeInvalidArgument, u35Slot, u35DB, mixed)
	if !said || !strings.Contains(words, "batch") || !strings.Contains(words, "Nothing was changed") {
		t.Fatalf("Explain = %q, %v", words, said)
	}
	fits := world.update(
		cluster.FieldOp{FieldID: 2, OpCode: cluster.UpdateSet, Value: u72Megabyte("b")},
		cluster.FieldOp{FieldID: 4, OpCode: cluster.UpdatePush, Value: u72Megabyte("d")})
	if code := world.precondition(t, fits); code != 0 {
		t.Fatalf("one set and a push that write 3 MiB under a 4 MiB bound: code %v, want accepted", code)
	}
}

// TestU72RenameChargesBothKeys: [GUARD a rename is charged the moved value, the target key
// and the source's delete key].
func TestU72RenameChargesBothKeys(t *testing.T) {
	value := stringValue(strings.Repeat("x", 100))
	probe := u72BoundSeed(t, 1<<30, map[uint16][]byte{2: value, 3: value})
	prefix := store.DocumentPrefix(u35Slot, u35DB, u35Bucket, probe.doc.EncID)
	keyLen := int64(len(prefix) + 2 + batchEntryOverhead)
	// Two renames write two values and four keys.
	exact := 2*int64(len(value)) + 4*keyLen + insertBatchReserve
	rename := func(w u72BoundWorld) cluster.Op {
		return w.update(
			cluster.FieldOp{FieldID: 2, OpCode: cluster.UpdateRenameTo, Value: []byte{0, 5}},
			cluster.FieldOp{FieldID: 3, OpCode: cluster.UpdateRenameTo, Value: []byte{0, 6}})
	}
	tight := u72BoundSeed(t, exact-1, map[uint16][]byte{2: value, 3: value})
	if code := tight.precondition(t, rename(tight)); code != cluster.CodeInvalidArgument {
		t.Fatalf("a bound one byte under the rename's writes: code %v, want INVALID_ARGUMENT", code)
	}
	exactWorld := u72BoundSeed(t, exact, map[uint16][]byte{2: value, 3: value})
	if code := exactWorld.precondition(t, rename(exactWorld)); code != 0 {
		t.Fatalf("a bound that holds the rename's writes: code %v, want accepted", code)
	}
}

// TestU72RenameOfALargeValueIsNotCappedAs16MiB: [GUARD the 16 MiB cap applies to operators
// that grow a value, not to moving one that is already stored].
func TestU72RenameOfALargeValueIsNotCappedAs16MiB(t *testing.T) {
	big := stringValue(strings.Repeat("x", 17<<20))
	world := u72BoundSeed(t, 64<<20, map[uint16][]byte{2: big})
	op := world.update(cluster.FieldOp{FieldID: 2, OpCode: cluster.UpdateRenameTo, Value: []byte{0, 5}})
	if code := world.precondition(t, op); code != 0 {
		t.Fatalf("a rename of a stored 17 MiB value: code %v, want accepted", code)
	}
	grow := world.update(cluster.FieldOp{FieldID: 2, OpCode: cluster.UpdateSet, Value: big})
	if code := world.precondition(t, grow); code != 0 {
		t.Fatalf("a set of 17 MiB under a 64 MiB bound: code %v, want accepted", code)
	}
}

// TestU72HandBuiltRenameWithABadTargetIsRefusedNotIndexed: [GUARD updateOperandRefusal checks
// the rename value is 2 bytes before it reads it].
func TestU72HandBuiltRenameWithABadTargetIsRefusedNotIndexed(t *testing.T) {
	world := u72BoundSeed(t, 1<<20, map[uint16][]byte{2: stringValue("x")})
	for _, target := range [][]byte{nil, {5}, {0, 0, 5}} {
		op := world.update(cluster.FieldOp{FieldID: 2, OpCode: cluster.UpdateRenameTo, Value: target})
		code, err := world.applier.Precondition(u35Slot, u35DB, op)
		if err != nil || code != cluster.CodeInvalidArgument {
			t.Errorf("a rename to a %d-byte target: code %v, %v, want INVALID_ARGUMENT", len(target), code, err)
		}
	}
}

func u72ModifierDoc(t *testing.T, key string) []byte {
	t.Helper()
	return append([]byte{byte(bsontype.EmbeddedDocument)}, mustRaw(t, bson.D{{Key: key, Value: bson.A{int32(1)}}})...)
}

// TestU72OwnerRefusesAModifierDocument: [GUARD the owner refuses a push or addToSet operand
// whose first field starts with $, whatever client sent it].
func TestU72OwnerRefusesAModifierDocument(t *testing.T) {
	world := u72BoundSeed(t, 1<<20, map[uint16][]byte{2: appendedArray(nil, stringValue("x"))})
	for _, code := range []byte{cluster.UpdatePush, cluster.UpdateAddToSet} {
		for _, modifier := range []string{"$each", "$position", "$slice", "$sort"} {
			op := world.update(cluster.FieldOp{FieldID: 2, OpCode: code, Value: u72ModifierDoc(t, modifier)})
			got, err := world.applier.Precondition(u35Slot, u35DB, op)
			if err != nil || got != cluster.CodeInvalidArgument {
				t.Errorf("%s with %s: code %v, %v, want INVALID_ARGUMENT", updateCodeName(code), modifier, got, err)
				continue
			}
			words, said := world.applier.Explain(got, u35Slot, u35DB, op)
			if !said || !strings.Contains(words, modifier) || !strings.Contains(words, "Nothing was changed") {
				t.Errorf("%s with %s: Explain = %q", updateCodeName(code), modifier, words)
			}
		}
	}
	plain := world.update(cluster.FieldOp{FieldID: 2, OpCode: cluster.UpdatePush, Value: u72ModifierDoc(t, "kind")})
	if got := world.precondition(t, plain); got != 0 {
		t.Errorf("a pushed document whose first field is plain: code %v, want accepted", got)
	}
}

// TestU72ModifierIsRefusedForEveryGoForm: [GUARD refuseModifier reads the marshalled BSON, so
// a bson.M operand is refused like a bson.D one].
func TestU72ModifierIsRefusedForEveryGoForm(t *testing.T) {
	node := u72Node(t)
	for name, operand := range map[string]any{
		"bson.D": bson.D{{Key: "$each", Value: bson.A{"a"}}},
		"bson.M": bson.M{"$slice": int32(2)},
		"struct": struct {
			Sort int32 `bson:"$sort"`
		}{1},
	} {
		u72Refused(t, node, "push "+name, UpdateRequest{Push: bson.D{{Key: "Tags", Value: operand}}}, "push", "Tags", "$")
		u72Refused(t, node, "addToSet "+name, UpdateRequest{AddToSet: bson.D{{Key: "Tags", Value: operand}}}, "addToSet", "Tags", "$")
	}
}
