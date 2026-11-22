package wire

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

// GuardVerdict is the owner's reading of a guard (guarded-update spec section 3).
type GuardVerdict struct {
	// Refused says a term differed: the write is answered not_applied, before a position.
	Refused bool
	// NotFound says there is no live document to guard: the refusal is NOT_FOUND, not not_applied.
	NotFound bool
	// FieldID is the first term that differed.
	FieldID uint16
	// Scalars is EncodeScalarSet of the current document, set on a refusal.
	Scalars []byte
}

// Guarder is implemented by an Applier that can evaluate Op.Guard. The writer calls it once
// per ordering attempt, under the pair lock, after Precondition returned 0 and before a
// position is taken.
type Guarder interface {
	// CheckGuard reads the document and compares the op's guard with it.
	CheckGuard(slot uint16, db uint64, op cluster.Op) (GuardVerdict, error)
}

// headerScalarID is the id under which a scalar set carries the document's header, which
// holds its _id_ value. Ids 0 and 1 mark the header and are never fields.
const headerScalarID uint16 = 0

// guardMatches returns the first failing field id, or 0 when every term holds. A term is
// satisfied by storedEqual, the one definition of a match the scan and the index use; a
// null term (type byte 0x0A) is also satisfied by a field that was never set.
func guardMatches(stored map[uint16][]byte, terms []cluster.GuardTerm) uint16 {
	for _, term := range terms {
		have := stored[term.FieldID]
		if len(have) == 0 {
			if len(term.Value) == 1 && term.Value[0] == nullTypeByte {
				continue
			}
			return term.FieldID
		}
		if !storedEqual(have, term.Value) {
			return term.FieldID
		}
	}
	return 0
}

// nullTypeByte is the BSON type byte of null, whose typed value has no further bytes.
const nullTypeByte = 0x0A

// readScalars is readScalar for each id on one snapshot. An id the document has no key for is
// absent from the map.
func readScalars(snapshot storage.KVSnapshot, docPrefix []byte, ids []uint16) (map[uint16][]byte, error) {
	out := make(map[uint16][]byte, len(ids))
	for _, id := range ids {
		stored, err := readScalar(snapshot, docPrefix, id)
		if err != nil {
			return nil, err
		}
		if stored != nil {
			out[id] = stored
		}
	}
	return out, nil
}

// allScalars reads every key of one document in one seek: the header under headerScalarID and
// each field under its id. A store field's marker is among them; the caller names only the
// ids its declaration says are scalars. The bounds are the document's, so each table's bloom
// filter is asked whether it holds a key of the document, as for a read (readStoredDocument).
func allScalars(snapshot storage.KVSnapshot, docPrefix []byte) (map[uint16][]byte, error) {
	iter := snapshot.NewIterator(storage.DocumentIterOptions(docPrefix))
	defer iter.Close()
	out := map[uint16][]byte{}
	for iter.Seek(docPrefix); iter.Valid(); iter.Next() {
		key := iter.Key()
		if !bytes.HasPrefix(key, docPrefix) {
			break
		}
		tail := key[len(docPrefix):]
		if len(tail) != 0 && len(tail) != 2 {
			return nil, fmt.Errorf("reading a document: %d bytes follow its id in key %x", len(tail), key)
		}
		stored, err := iter.Value()
		if err != nil {
			return nil, fmt.Errorf("reading a value of a document: %w", err)
		}
		id := headerScalarID
		if len(tail) == 2 {
			id = binary.BigEndian.Uint16(tail)
		}
		out[id] = bytes.Clone(stored)
	}
	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("reading a document: %w", err)
	}
	return out, nil
}

// EncodeScalarSet is [count:2 BE][(field id:2 BE, typed value length:4 BE, typed value) x count]
// in ascending id order.
func EncodeScalarSet(scalars map[uint16][]byte) []byte {
	ids := make([]uint16, 0, len(scalars))
	for id := range scalars {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	out := binary.BigEndian.AppendUint16(nil, uint16(len(ids)))
	for _, id := range ids {
		out = binary.BigEndian.AppendUint16(out, id)
		out = binary.BigEndian.AppendUint32(out, uint32(len(scalars[id])))
		out = append(out, scalars[id]...)
	}
	return out
}

// DecodeScalarSet is the inverse of EncodeScalarSet. It refuses a count or length that does
// not fit, ids out of order, and trailing bytes.
func DecodeScalarSet(encoded []byte) (map[uint16][]byte, error) {
	if len(encoded) < 2 {
		return nil, errors.New("a scalar set is at least its 2-byte count")
	}
	count := int(binary.BigEndian.Uint16(encoded))
	rest := encoded[2:]
	out := make(map[uint16][]byte, count)
	last := -1
	for range count {
		if len(rest) < 6 {
			return nil, errors.New("a scalar set ends inside an entry")
		}
		id := binary.BigEndian.Uint16(rest)
		length := int(binary.BigEndian.Uint32(rest[2:]))
		rest = rest[6:]
		if int(id) <= last {
			return nil, fmt.Errorf("scalar set id %d follows %d; ids ascend", id, last)
		}
		if length > len(rest) {
			return nil, fmt.Errorf("scalar %d claims %d bytes and %d remain", id, length, len(rest))
		}
		out[id], rest, last = bytes.Clone(rest[:length]), rest[length:], int(id)
	}
	if len(rest) != 0 {
		return nil, fmt.Errorf("a scalar set has %d bytes after its last entry", len(rest))
	}
	return out, nil
}

// CheckGuard implements Guarder. It reads from one snapshot under the pair lock the caller
// holds, so the verdict is of the state the append would follow.
func (a *DocumentApplier) CheckGuard(slot uint16, db uint64, op cluster.Op) (GuardVerdict, error) {
	if len(op.Guard) == 0 {
		return GuardVerdict{}, nil
	}
	snapshot := a.kv.NewSnapshot()
	defer snapshot.Discard()
	doc := DocumentKey{Ref: storage.BucketRef{Slot: slot, DatabaseID: db, BucketID: op.BucketID}, EncID: op.EncID}
	state, err := ReadHeaderState(snapshot, doc)
	if err != nil {
		return GuardVerdict{}, fmt.Errorf("reading the header of document %x for its guard: %w", op.EncID, err)
	}
	if state != HeaderLive {
		// An update's precondition already refused an absent or deleted document; a delete
		// of one is not an error without a guard, but with one there is nothing to compare.
		return GuardVerdict{NotFound: true}, nil
	}
	// An expired document is absent (expiry spec 3.2): the guard never compares against it.
	expired, err := a.documentExpired(snapshot, slot, db, op)
	if err != nil {
		return GuardVerdict{}, fmt.Errorf("judging the expiry of document %x for its guard: %w", op.EncID, err)
	}
	if expired {
		return GuardVerdict{NotFound: true}, nil
	}
	prefix := store.DocumentPrefix(slot, db, op.BucketID, op.EncID)
	ids := make([]uint16, len(op.Guard))
	for i, term := range op.Guard {
		ids[i] = term.FieldID
	}
	stored, err := readScalars(snapshot, prefix, ids)
	if err != nil {
		return GuardVerdict{}, fmt.Errorf("reading the guarded fields of document %x: %w", op.EncID, err)
	}
	failed := guardMatches(stored, op.Guard)
	if failed == 0 {
		return GuardVerdict{}, nil
	}
	current, err := allScalars(snapshot, prefix)
	if err != nil {
		return GuardVerdict{}, fmt.Errorf("reading document %x to answer its refused guard: %w", op.EncID, err)
	}
	return GuardVerdict{Refused: true, FieldID: failed, Scalars: EncodeScalarSet(current)}, nil
}

// The first byte of the result a dedupe entry keeps for an update or delete asked to return
// its document. The tag lets a retry tell a document from the mark of one that was too large.
const (
	// resultDocument is followed by an EncodeScalarSet.
	resultDocument byte = 0x01
	// resultTruncated says the document was over the dedupe cap and nothing follows; the
	// retry's answer says so and the caller runs a get.
	resultTruncated byte = 0x02
)

// updatedScalars is before with fields applied the way fillUpdate writes them: set, unset, the
// six computed operators through updateResult, and rename. It is a pure function of the
// pre-image and the record, so the returned document is the stored one.
func updatedScalars(before map[uint16][]byte, fields []cluster.FieldOp) (map[uint16][]byte, error) {
	after := make(map[uint16][]byte, len(before))
	for id, stored := range before {
		after[id] = stored
	}
	for _, field := range fields {
		switch {
		case field.OpCode == cluster.UpdateSet:
			after[field.FieldID] = field.Value
		case field.OpCode == cluster.UpdateUnset:
			delete(after, field.FieldID)
		case updateComputes(field.OpCode):
			next, err := updateResult(field.OpCode, before[field.FieldID], field.Value)
			if err != nil {
				return nil, fmt.Errorf("%s of field %d: %w", updateCodeName(field.OpCode), field.FieldID, err)
			}
			if next != nil {
				after[field.FieldID] = next
			}
		case field.OpCode == cluster.UpdateRenameTo:
			stored, ok := before[field.FieldID]
			if !ok || len(field.Value) != 2 {
				continue
			}
			after[binary.BigEndian.Uint16(field.Value)] = stored
			delete(after, field.FieldID)
		default:
			return nil, fmt.Errorf("update code 0x%02x on field %d is not one this build applies", field.OpCode, field.FieldID)
		}
	}
	return after, nil
}

// guardResult is the result a write records for the document its return mode names: the
// scalars after an update with ReturnAfter, the ones before with ReturnBefore, and nil for
// ReturnNone or a delete of a document that was not live (there is nothing to show).
func guardResult(op cluster.Op, before, after map[uint16][]byte) []byte {
	var shown map[uint16][]byte
	switch op.Return {
	case cluster.ReturnAfter:
		shown = after
	case cluster.ReturnBefore:
		shown = before
	default:
		return nil
	}
	if len(shown[headerScalarID]) == 0 {
		return nil
	}
	return append([]byte{resultDocument}, EncodeScalarSet(shown)...)
}

// splitGuardResult reads what guardResult wrote: the scalar set, or truncated when the
// document was over the dedupe cap. An empty result has neither.
func splitGuardResult(recorded []byte) (scalars []byte, truncated bool) {
	switch {
	case len(recorded) == 0:
		return nil, false
	case recorded[0] == resultTruncated:
		return nil, true
	case recorded[0] == resultDocument:
		return recorded[1:], false
	}
	return nil, false
}

// fillUpdateWithResult is fillUpdate for an update that asked for its document back: the
// pre-image is read from the snapshot fillUpdate also reads, before the batch commits, so the
// owner, a copy and a replay all build the same result from the same log.
func (a *DocumentApplier) fillUpdateWithResult(batch storage.KVBatch, rec AppliedRecord) ([]byte, error) {
	before, err := a.preImage(rec)
	if err != nil {
		return nil, err
	}
	if err := a.fillUpdate(batch, rec); err != nil {
		return nil, err
	}
	after, err := updatedScalars(before, rec.Op.Fields)
	if err != nil {
		return nil, fmt.Errorf("position %s: building the document after the update: %w", rec.Pos, err)
	}
	return guardResult(rec.Op, before, after), nil
}

// fillDeleteWithResult is fillDelete for a delete that asked for the document as it was.
func (a *DocumentApplier) fillDeleteWithResult(batch storage.KVBatch, rec AppliedRecord) ([]byte, error) {
	before, err := a.preImage(rec)
	if err != nil {
		return nil, err
	}
	if err := a.fillDelete(batch, rec); err != nil {
		return nil, err
	}
	return guardResult(rec.Op, before, nil), nil
}

// preImage is the document's keys before the record applies; a document that is not live has
// none, so the result is empty.
func (a *DocumentApplier) preImage(rec AppliedRecord) (map[uint16][]byte, error) {
	snapshot := a.kv.NewSnapshot()
	defer snapshot.Discard()
	prefix := store.DocumentPrefix(rec.Slot, rec.DB, rec.Op.BucketID, rec.Op.EncID)
	before, err := allScalars(snapshot, prefix)
	if err != nil {
		return nil, fmt.Errorf("position %s: reading the document before it changes: %w", rec.Pos, err)
	}
	if header := before[headerScalarID]; len(header) == 1 && header[0] == tombstoneHeader[0] {
		return nil, nil
	}
	return before, nil
}
