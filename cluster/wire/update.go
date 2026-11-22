package wire

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/bsontype"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

// UPDATE (conventions section 9.1) takes all nine operation codes. The ones beyond set,
// unset and inc are in update_ops.go. Whatever the apply could fail on is refused before a
// position is taken: the static checks here, and the state checks under the pair lock.

// updateCodeNames words the operation codes.
var updateCodeNames = map[byte]string{
	cluster.UpdateSet: "set", cluster.UpdateUnset: "unset", cluster.UpdateInc: "inc",
	cluster.UpdateMul: "mul", cluster.UpdatePush: "push", cluster.UpdateAddToSet: "addToSet",
	cluster.UpdateMin: "min", cluster.UpdateMax: "max", cluster.UpdateRenameTo: "rename",
}

// A scalarNumber is the decoded form of an int, a long or a double: the type byte then the BSON
// value bytes.
type scalarNumber struct {
	kind  bsontype.Type
	whole int64
	real  float64
}

// readNumber reads an int32, int64 or double of any value, finite or not.
func readNumber(kind bsontype.Type, body []byte) (scalarNumber, bool) {
	switch {
	case kind == bsontype.Int32 && len(body) == 4:
		return scalarNumber{kind: kind, whole: int64(int32(binary.LittleEndian.Uint32(body)))}, true
	case kind == bsontype.Int64 && len(body) == 8:
		return scalarNumber{kind: kind, whole: int64(binary.LittleEndian.Uint64(body))}, true
	case kind == bsontype.Double && len(body) == 8:
		return scalarNumber{kind: kind, real: math.Float64frombits(binary.LittleEndian.Uint64(body))}, true
	}
	return scalarNumber{}, false
}

// parseNumber reads a typed value that must be an int32, int64 or a finite double; operator
// names the operation for the message.
func parseNumber(typed []byte, operator string) (scalarNumber, error) {
	if len(typed) == 0 {
		return scalarNumber{}, errors.New("it has no type byte")
	}
	number, ok := readNumber(bsontype.Type(typed[0]), typed[1:])
	if !ok {
		return scalarNumber{}, fmt.Errorf("it is a %s, and %s takes an int, a long or a double", bsontype.Type(typed[0]), operator)
	}
	if number.kind == bsontype.Double && !isFinite(number.real) {
		return scalarNumber{}, errors.New("it is not a finite number")
	}
	return number, nil
}

// typed is the stored form of n again.
func (n scalarNumber) typed() []byte {
	switch n.kind {
	case bsontype.Int32:
		return binary.LittleEndian.AppendUint32([]byte{byte(n.kind)}, uint32(int32(n.whole)))
	case bsontype.Int64:
		return binary.LittleEndian.AppendUint64([]byte{byte(n.kind)}, uint64(n.whole))
	}
	return binary.LittleEndian.AppendUint64([]byte{byte(n.kind)}, math.Float64bits(n.real))
}

func (n scalarNumber) float() float64 {
	if n.kind == bsontype.Double {
		return n.real
	}
	return float64(n.whole)
}

// addNumbers is MongoDB's $inc arithmetic: two ints stay an int unless the sum leaves its
// range, when it becomes a long; a long and an int are a long; a double makes the result a
// double. A long that would leave its range, and a double that would leave the finite
// numbers, are refused: MongoDB turns the first into a double, but a field that silently
// changes type is not what a developer wrote (build SPEC-GAP U67-4).
func addNumbers(have, amount scalarNumber) (scalarNumber, error) {
	if have.kind == bsontype.Double || amount.kind == bsontype.Double {
		sum := have.float() + amount.float()
		if math.IsInf(sum, 0) {
			return scalarNumber{}, errors.New("the sum is past the largest double, so the add would overflow")
		}
		return scalarNumber{kind: bsontype.Double, real: sum}, nil
	}
	sum := have.whole + amount.whole
	if (amount.whole > 0 && sum < have.whole) || (amount.whole < 0 && sum > have.whole) {
		return scalarNumber{}, errors.New("the sum is outside a long, so the add would overflow")
	}
	if have.kind == bsontype.Int32 && amount.kind == bsontype.Int32 && sum >= math.MinInt32 && sum <= math.MaxInt32 {
		return scalarNumber{kind: bsontype.Int32, whole: sum}, nil
	}
	return scalarNumber{kind: bsontype.Int64, whole: sum}, nil
}

// incResult is the value an inc leaves: the amount when the field holds none, otherwise
// the sum. Both the owner's check and the apply call it, so they cannot disagree.
func incResult(stored, amount []byte) ([]byte, error) {
	add, err := parseNumber(amount, "inc")
	if err != nil {
		return nil, fmt.Errorf("the amount is not a number: %w", err)
	}
	if stored == nil {
		return add.typed(), nil
	}
	have, err := parseNumber(stored, "inc")
	if err != nil {
		return nil, fmt.Errorf("the stored value is not a number: %w", err)
	}
	sum, err := addNumbers(have, add)
	if err != nil {
		return nil, err
	}
	return sum.typed(), nil
}

// updateStoreField returns the first field of the update that is a declared store, or 0.
// A scalar key written under a store field's id would be read as a marker later.
func (a *DocumentApplier) updateStoreField(db uint64, op cluster.Op) (uint16, error) {
	for _, field := range op.Fields {
		ids := []uint16{field.FieldID}
		if field.OpCode == cluster.UpdateRenameTo && len(field.Value) == 2 {
			ids = append(ids, binary.BigEndian.Uint16(field.Value))
		}
		for _, id := range ids {
			_, err := a.engines.FieldEngine(db, op.BucketID, id)
			switch {
			case err == nil:
				return id, nil
			case !errors.Is(err, errUndeclaredField):
				return 0, fmt.Errorf("finding the engine of field %d: %w", id, err)
			}
		}
	}
	return 0, nil
}

// maxUpdateValueBytes bounds what an operator leaves in one field, MongoDB's document limit.
// The store has its own, much larger, value bound that fails after the append, so the owner
// refuses well before it.
const maxUpdateValueBytes = 16 << 20

// updateCodesTaken are the operation codes this build applies. A code outside it is refused
// by name before a position.
var updateCodesTaken = map[byte]bool{cluster.UpdateSet: true, cluster.UpdateUnset: true, cluster.UpdateInc: true,
	cluster.UpdateMul: true,
	cluster.UpdateMin: true, cluster.UpdateMax: true, cluster.UpdatePush: true, cluster.UpdateAddToSet: true,
	cluster.UpdateRenameTo: true}

// updateOpsRefusal words why a list of update operations can never be applied whatever the
// document holds, or returns "". name words a field id: the owner holds ids, a caller that
// resolved the bucket's declaration has names.
func updateOpsRefusal(fields []cluster.FieldOp, name func(uint16) string) string {
	if len(fields) == 0 {
		return "the update names no field. Nothing was changed. Send at least one operator, for example set or inc"
	}
	seen := make([]uint16, 0, 2*len(fields))
	for _, field := range fields {
		if slices.Contains(seen, field.FieldID) {
			return fmt.Sprintf("%s is named by two operations. Nothing was changed. Send one operation for each field", name(field.FieldID))
		}
		seen = append(seen, field.FieldID)
		if field.FieldID < 2 {
			return fmt.Sprintf("field id %d is not a field of a document: 0 and 1 mark the header, which holds the _id_. Nothing was changed. Change the other fields by their own ids", field.FieldID)
		}
		if !updateCodesTaken[field.OpCode] {
			return fmt.Sprintf("update code %s on %s is not available in this build yet. Nothing was changed. Write the field's new value with set",
				updateCodeName(field.OpCode), name(field.FieldID))
		}
		if field.OpCode == cluster.UpdateRenameTo {
			if len(field.Value) != 2 {
				return fmt.Sprintf("rename of %s carries a target of %d bytes. Nothing was changed. A rename names its target by a 2-byte field id", name(field.FieldID), len(field.Value))
			}
			target := binary.BigEndian.Uint16(field.Value)
			if target < 2 || target == field.FieldID || slices.Contains(seen, target) {
				return fmt.Sprintf("rename of %s names a target that is a header, the field itself, or a field this update already changes. Nothing was changed. Rename to a different declared field that no other operation in the statement names", name(field.FieldID))
			}
			seen = append(seen, target)
		}
		if reason := updateOperandRefusal(field, name); reason != "" {
			return reason
		}
	}
	return ""
}

// updateOperandRefusal words why one operation's value can never be applied, or "".
func updateOperandRefusal(field cluster.FieldOp, name func(uint16) string) string {
	switch {
	case field.OpCode == cluster.UpdateUnset && len(field.Value) != 0:
		return fmt.Sprintf("unset of %s carries a value of %d bytes. Nothing was changed. An unset names the field and carries nothing", name(field.FieldID), len(field.Value))
	case field.OpCode == cluster.UpdateUnset || field.OpCode == cluster.UpdateRenameTo:
		return ""
	case len(field.Value) == 0:
		return fmt.Sprintf("the value of %s is empty. Nothing was changed. A value is its type byte then the BSON bytes", name(field.FieldID))
	}
	if err := ringCheckTypedValue(field.Value); err != nil {
		return fmt.Sprintf("the value of %s is not a BSON value: %v. Nothing was changed. Send the type byte then the BSON bytes", name(field.FieldID), err)
	}
	if field.OpCode == cluster.UpdatePush || field.OpCode == cluster.UpdateAddToSet {
		if modifier := updateModifierKey(field.Value); modifier != "" {
			return fmt.Sprintf("%s of %s: it is a document whose first field is %s, and only one plain value is appended. Nothing was changed. Send the value itself, or one update for each value",
				updateCodeName(field.OpCode), name(field.FieldID), modifier)
		}
	}
	var err error
	switch field.OpCode {
	case cluster.UpdateInc, cluster.UpdateMul:
		_, err = parseNumber(field.Value, updateCodeName(field.OpCode))
	case cluster.UpdateMin, cluster.UpdateMax:
		_, err = orderOf(field.Value, updateCodeName(field.OpCode))
	case cluster.UpdateAddToSet:
		if kind := bsontype.Type(field.Value[0]); kind == bsontype.EmbeddedDocument || kind == bsontype.Array {
			err = fmt.Errorf("it is a %s, and addToSet takes scalar values", kind)
		}
	}
	if err != nil && (field.OpCode == cluster.UpdateInc || field.OpCode == cluster.UpdateMul) {
		return fmt.Sprintf("%s of %s needs a number: %v. Nothing was changed. %s", updateCodeName(field.OpCode), name(field.FieldID), err, updateOperandFix(field.OpCode))
	}
	if err != nil {
		return fmt.Sprintf("%s of %s: %v. Nothing was changed. %s", updateCodeName(field.OpCode), name(field.FieldID), err, updateOperandFix(field.OpCode))
	}
	return ""
}

// updateModifierKey is the first field name of a typed embedded document when it starts with
// $, as MongoDB's $each, $position, $slice and $sort do, and "" otherwise. The value has
// been validated as BSON.
func updateModifierKey(typed []byte) string {
	if len(typed) == 0 || bsontype.Type(typed[0]) != bsontype.EmbeddedDocument {
		return ""
	}
	first, err := bson.Raw(typed[1:]).IndexErr(0)
	if err != nil {
		return ""
	}
	if key := first.Key(); strings.HasPrefix(key, "$") {
		return key
	}
	return ""
}

// updateOperandFix words what to send instead of an operand the operator cannot take.
func updateOperandFix(code byte) string {
	switch code {
	case cluster.UpdateInc, cluster.UpdateMul:
		return "Send an int, a long or a finite double"
	case cluster.UpdateAddToSet:
		return "Send a scalar value, or use push to append a document or an array"
	}
	return "Send null, a number, a string, binary data, an ObjectId, a bool, a datetime or a timestamp"
}

func fieldIDWord(id uint16) string { return fmt.Sprintf("field %d", id) }

// updateShapeRefusal is the part of an update's refusals that needs no state.
func (a *DocumentApplier) updateShapeRefusal(db uint64, op cluster.Op) string {
	if reason := updateOpsRefusal(op.Fields, fieldIDWord); reason != "" {
		return reason
	}
	return a.expireFieldRefusal(db, op)
}

func updateCodeName(code byte) string {
	if name, ok := updateCodeNames[code]; ok {
		return name
	}
	return fmt.Sprintf("0x%02x", code)
}

// updateStateRefusal words why an update cannot be applied to what the document holds now:
// an operator refuses the stored value, a number would overflow, an array would outgrow a
// field, or the batch fillUpdate builds would not fit one commit. It is the one accounting
// of the update's bytes: it walks every field and charges what fillUpdate writes (a set
// value, a delete key, a computed result, a rename's moved value and both its keys). It
// reads the owner's state under the pair lock, which the apply runs under too.
func (a *DocumentApplier) updateStateRefusal(slot uint16, db uint64, op cluster.Op) (string, error) {
	snapshot := a.kv.NewSnapshot()
	defer snapshot.Discard()
	prefix := store.DocumentPrefix(slot, db, op.BucketID, op.EncID)
	keyBytes := int64(len(prefix) + 2 + batchEntryOverhead)
	var written int64
	for _, field := range op.Fields {
		switch {
		case field.OpCode == cluster.UpdateSet:
			written += keyBytes + a.batchValueBytes(int64(len(field.Value)))
		case field.OpCode == cluster.UpdateUnset:
			written += keyBytes
		case updateComputes(field.OpCode):
			stored, err := readScalar(snapshot, prefix, field.FieldID)
			if err != nil {
				return "", err
			}
			next, err := updateResult(field.OpCode, stored, field.Value)
			if err != nil {
				return fmt.Sprintf("%s of field %d: %v. Nothing was changed. %s", updateCodeName(field.OpCode), field.FieldID, err, updateFix(field.OpCode)), nil
			}
			if len(next) > maxUpdateValueBytes && (field.OpCode == cluster.UpdatePush || field.OpCode == cluster.UpdateAddToSet) {
				return fmt.Sprintf("%s of field %d would leave a value of %d bytes and a field holds at most %d (16 MiB, MongoDB's document limit). Nothing was changed. Keep the array shorter, or store the elements in a store field", updateCodeName(field.OpCode), field.FieldID, len(next), maxUpdateValueBytes), nil
			}
			if next != nil {
				written += keyBytes + a.batchValueBytes(int64(len(next)))
			}
		case field.OpCode == cluster.UpdateRenameTo:
			stored, err := readScalar(snapshot, prefix, field.FieldID)
			if err != nil {
				return "", err
			}
			if stored != nil {
				written += 2*keyBytes + a.batchValueBytes(int64(len(stored)))
			}
		}
	}
	if written == 0 {
		return "", nil
	}
	path := a.insertPath(slot, db, cluster.Op{BucketID: op.BucketID, EncID: op.EncID})
	if need := written + insertBatchReserve; path.Limit > 0 && need > path.Limit {
		return fmt.Sprintf("this update would write about %d bytes and one batch holds at most %d. Nothing was changed. Split the update, shorten the arrays, or store the large value as a blobStore field", need, path.Limit), nil
	}
	return "", nil
}

// batchValueBytes is what a value of size bytes adds to a batch: in full, except a value the
// store keeps out of line.
func (a *DocumentApplier) batchValueBytes(size int64) int64 {
	if sized, ok := a.kv.(valueThresholder); ok {
		if threshold := sized.ValueThreshold(); threshold > 0 && size >= threshold {
			return outOfLineValueBytes
		}
	}
	return size
}

// readScalar is the stored value of one scalar field, nil when the document has none.
func readScalar(snapshot storage.KVSnapshot, docPrefix []byte, fieldID uint16) ([]byte, error) {
	stored, err := snapshot.Get(store.FieldKey(docPrefix, fieldID))
	if errors.Is(err, storage.ErrKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("reading field %d: %w", fieldID, err)
	}
	return stored, nil
}

// updatePrecondition is check 6a for an update: the fields must be scalars, the operations
// ones this build applies with a value the apply can write, the document live, and an inc
// must have a number to add to.
func (a *DocumentApplier) updatePrecondition(slot uint16, db uint64, op cluster.Op) (cluster.Code, error) {
	storeField, err := a.updateStoreField(db, op)
	if err != nil || storeField != 0 {
		if err == nil {
			return cluster.CodeFieldType, nil
		}
		return 0, err
	}
	if a.updateShapeRefusal(db, op) != "" {
		return cluster.CodeInvalidArgument, nil
	}
	code, err := a.liveTarget(slot, db, op)
	if err != nil || code != 0 {
		return code, err
	}
	reason, err := a.updateStateRefusal(slot, db, op)
	if err != nil {
		return 0, fmt.Errorf("reading the fields an update would change: %w", err)
	}
	if reason != "" {
		return cluster.CodeInvalidArgument, nil
	}
	return 0, nil
}

// explainUpdate words the refusals updatePrecondition returns.
func (a *DocumentApplier) explainUpdate(code cluster.Code, slot uint16, db uint64, op cluster.Op, where string) (string, bool) {
	switch code {
	case cluster.CodeNotFound:
		return fmt.Sprintf("%s does not exist, so it cannot be updated. Nothing was changed. Insert the document first with db.<bucket>.insert({_id_: \"<id>\", …}), or write it with db.<bucket>.upsert({…})", where), true
	case cluster.CodeFieldType:
		field, _ := a.updateStoreField(db, op)
		return fmt.Sprintf("%s: field %d is a store field, and an update changes scalar fields. Nothing was changed. Change a store with its own verbs: push, pop, put or take", where, field), true
	case cluster.CodeInvalidArgument:
		if tombstone, err := a.liveTarget(slot, db, op); err == nil && tombstone == cluster.CodeInvalidArgument {
			return fmt.Sprintf("%s is deleted and its remaining keys are still being removed, so it cannot be updated. Nothing was changed. Retry in a moment, or insert the document again", where), true
		}
		if reason := a.updateShapeRefusal(db, op); reason != "" {
			return fmt.Sprintf("%s: %s", where, reason), true
		}
		if reason, err := a.updateStateRefusal(slot, db, op); err == nil && reason != "" {
			return fmt.Sprintf("%s: %s", where, reason), true
		}
	}
	return "", false
}

// fillUpdate writes an update's keys into batch: set writes the field's key, unset deletes
// it and the other operators write what updateResult computes. The precondition has already refused whatever cannot be
// written, on the same state under the same lock.
func (a *DocumentApplier) fillUpdate(batch storage.KVBatch, rec AppliedRecord) error {
	snapshot := a.kv.NewSnapshot()
	defer snapshot.Discard()
	prefix := store.DocumentPrefix(rec.Slot, rec.DB, rec.Op.BucketID, rec.Op.EncID)
	for _, field := range rec.Op.Fields {
		key := store.FieldKey(prefix, field.FieldID)
		switch field.OpCode {
		case cluster.UpdateSet:
			if err := batch.Set(key, field.Value); err != nil {
				return fmt.Errorf("writing the set of field %d at position %s: %w", field.FieldID, rec.Pos, err)
			}
		case cluster.UpdateUnset:
			if err := batch.Delete(key); err != nil {
				return fmt.Errorf("writing the unset of field %d at position %s: %w", field.FieldID, rec.Pos, err)
			}
		case cluster.UpdateInc, cluster.UpdateMul, cluster.UpdateMin, cluster.UpdateMax, cluster.UpdatePush, cluster.UpdateAddToSet:
			stored, err := readScalar(snapshot, prefix, field.FieldID)
			if err != nil {
				return fmt.Errorf("reading field %d for the %s at position %s: %w", field.FieldID, updateCodeName(field.OpCode), rec.Pos, err)
			}
			next, err := updateResult(field.OpCode, stored, field.Value)
			if err != nil {
				return fmt.Errorf("%s of field %d at position %s: %w", updateCodeName(field.OpCode), field.FieldID, rec.Pos, err)
			}
			if next == nil {
				continue
			}
			if err := batch.Set(key, next); err != nil {
				return fmt.Errorf("writing the %s of field %d at position %s: %w", updateCodeName(field.OpCode), field.FieldID, rec.Pos, err)
			}
		case cluster.UpdateRenameTo:
			stored, err := readScalar(snapshot, prefix, field.FieldID)
			if err != nil {
				return fmt.Errorf("reading field %d for the rename at position %s: %w", field.FieldID, rec.Pos, err)
			}
			if stored == nil {
				continue
			}
			if err := batch.Set(store.FieldKey(prefix, binary.BigEndian.Uint16(field.Value)), stored); err != nil {
				return fmt.Errorf("writing the rename target of field %d at position %s: %w", field.FieldID, rec.Pos, err)
			}
			if err := batch.Delete(key); err != nil {
				return fmt.Errorf("deleting the rename source field %d at position %s: %w", field.FieldID, rec.Pos, err)
			}
		default:
			return fmt.Errorf("position %s: update code 0x%02x on field %d is not one this build applies", rec.Pos, field.OpCode, field.FieldID)
		}
	}
	return applyExpiryDocument(snapshot, batch, rec)
}
