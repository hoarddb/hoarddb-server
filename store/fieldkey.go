package store

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/hoarddb/hoarddb-server/storage"
)

// The key grammar of document-scoped field stores (nested buckets, GAP-22;
// the S-1 ruling of 2026-09-29, document first). Every field-store key is
// built here and nowhere else:
//
//	[s:2][id:8][0x07] ‖ owner(document) ‖ store(F)
//	owner(document) = EncodeComponent(documentKey)
//	store(F)        = fieldID(F):2 big-endian ‖ inner:1 ‖ remainder
//	  inner 0x01  elements by key:    EncodeComponent(elementKey)          → element
//	  inner 0x04  elements by order:  position:8 BE               (fifo, lifo) → element
//	                                  invertedPriority:8 ‖ EncodeComponent(entryID) (heap) → element
//	  inner 0x05  the instance state: no remainder                      → 32 bytes
//	  inner 0x07  the stores of this store's elements (L3):
//	              elementRemainder ‖ store(F')
//
// Document first, so one document's whole footprint across every declared
// field is one contiguous range and its delete cascade is one bounded walk.
// The inner bytes are the bucket-level namespace constants on purpose: a
// reader who knows a bucket's layout knows an instance's. The frozen vectors
// are store/testdata/fieldstore-vectors.json; a change here that moves one of
// them re-points every element ever written.

// FieldOwnerPrefix is the prefix of every field-store key of one document:
// [s][id][0x07] ‖ EncodeComponent(documentKey). The owner is the escaped and
// terminated form, so no document's prefix is a prefix of another's ("a" and
// "ab"), and a btree parent's owner equals its own document key suffix.
func FieldOwnerPrefix(slot uint16, databaseID, id uint64, documentKey string) ([]byte, error) {
	owner, err := EncodeComponent(documentKey)
	if err != nil {
		return nil, fmt.Errorf("encoding the field-store owner for document %q: %w", documentKey, err)
	}
	return storage.Key(slot, databaseID, id, storage.NSFieldStore, owner), nil
}

// DocumentPrefix is slot ‖ database ‖ bucket ‖ 0x01 ‖ encID: a document's
// header key, and the prefix of its scalar field keys (placement §3, `doc`).
// encID is the EncodeComponent form of _id_, which is prefix-free, so no
// other document's prefix is a prefix of this one's.
func DocumentPrefix(slot uint16, db, bucket uint64, encID []byte) []byte {
	return storage.Key(slot, db, bucket, storage.NSDocuments, encID)
}

// FieldKey is the key of one scalar field: docPrefix ‖ fieldID, two bytes
// big-endian (placement §3, `fld`). Field id 1 is _id_ and lives in the header
// key, never here.
func FieldKey(docPrefix []byte, fieldID uint16) []byte {
	key := make([]byte, len(docPrefix), len(docPrefix)+2)
	copy(key, docPrefix)
	return binary.BigEndian.AppendUint16(key, fieldID)
}

// StorePrefix is slot ‖ database ‖ bucket ‖ 0x07 ‖ encID ‖ fieldID: the prefix
// of one store field's whole sub-tree (placement §3, `st`). It starts with the
// field-store namespace, so it is never a prefix of a DocumentPrefix key.
func StorePrefix(slot uint16, db, bucket uint64, encID []byte, fieldID uint16) []byte {
	return FieldStorePrefix(storage.Key(slot, db, bucket, storage.NSFieldStore, encID), fieldID)
}

// FieldStorePrefix is owner ‖ fieldID, the prefix of one store instance. At
// L3, owner is an L2 element's anchor (FieldElementAnchor).
func FieldStorePrefix(owner []byte, fieldID uint16) []byte {
	prefix := make([]byte, len(owner), len(owner)+3)
	copy(prefix, owner)
	return binary.BigEndian.AppendUint16(prefix, fieldID)
}

// FieldElementKey is storePrefix ‖ inner ‖ remainder.
func FieldElementKey(storePrefix []byte, inner byte, remainder []byte) []byte {
	key := make([]byte, 0, len(storePrefix)+1+len(remainder))
	key = append(key, storePrefix...)
	key = append(key, inner)
	return append(key, remainder...)
}

// FieldStateKey is the instance state's key, storePrefix ‖ 0x05.
func FieldStateKey(storePrefix []byte) []byte {
	return FieldElementKey(storePrefix, storage.NSState, nil)
}

// FieldElementAnchor is the owner of the L3 stores declared on one L2
// element: the L2 store's prefix ‖ 0x07 ‖ the element's remainder. L3 keys
// live under their own inner byte so that no walk of the L2 elements (inner
// 0x01 or 0x04) or of its state (0x05) can reach them.
func FieldElementAnchor(storePrefix []byte, elementRemainder []byte) []byte {
	return FieldElementKey(storePrefix, storage.NSFieldStore, elementRemainder)
}

// PositionRemainder is a fifo or lifo element's remainder: 8 bytes, big-endian.
func PositionRemainder(position uint64) []byte {
	return binary.BigEndian.AppendUint64(make([]byte, 0, 8), position)
}

// HeapRemainder is a heap element's remainder: invertedPriority ‖
// EncodeComponent(entryID). Unlike a heap bucket's raw id tail the entry id is
// escaped and terminated, because an L3 key appends to it and a raw tail is
// not self-delimiting.
func HeapRemainder(priority int64, entryID string) ([]byte, error) {
	encodedID, err := EncodeComponent(entryID)
	if err != nil {
		return nil, fmt.Errorf("encoding heap entry id %q: %w", entryID, err)
	}
	remainder := binary.BigEndian.AppendUint64(make([]byte, 0, 8+len(encodedID)), invertedPriority(priority))
	return append(remainder, encodedID...), nil
}

// KeyedRemainder is a hash, btree or blob element's remainder:
// EncodeComponent(elementKey).
func KeyedRemainder(elementKey string) ([]byte, error) {
	encoded, err := EncodeComponent(elementKey)
	if err != nil {
		return nil, fmt.Errorf("encoding element key %q: %w", elementKey, err)
	}
	return encoded, nil
}

// fieldInstanceStateSize is the fixed length of an encoded FieldInstanceState.
const fieldInstanceStateSize = 32

// FieldInstanceState is one store instance's cursors and count, at inner
// 0x05. It is 32 fixed bytes rather than BSON like BucketState because there
// is one per written instance, not one per bucket. An absent state is the
// zero value, so an instance never written costs nothing.
type FieldInstanceState struct {
	Head  uint64
	Tail  uint64
	Next  uint64
	Count uint64
}

// EncodeFieldInstanceState writes Head, Tail, Next, Count, big-endian, in that
// order.
func EncodeFieldInstanceState(state FieldInstanceState) []byte {
	encoded := make([]byte, 0, fieldInstanceStateSize)
	for _, word := range [...]uint64{state.Head, state.Tail, state.Next, state.Count} {
		encoded = binary.BigEndian.AppendUint64(encoded, word)
	}
	return encoded
}

// DecodeFieldInstanceState reads a value EncodeFieldInstanceState wrote. A
// value of any other length is an error naming key.
func DecodeFieldInstanceState(key, encoded []byte) (FieldInstanceState, error) {
	if len(encoded) != fieldInstanceStateSize {
		return FieldInstanceState{}, &storage.KeyError{Operation: "parsing a field-store instance state", Key: bytes.Clone(key),
			Cause: fmt.Errorf("the state is %d bytes, want %d", len(encoded), fieldInstanceStateSize)}
	}
	return FieldInstanceState{
		Head:  binary.BigEndian.Uint64(encoded[0:8]),
		Tail:  binary.BigEndian.Uint64(encoded[8:16]),
		Next:  binary.BigEndian.Uint64(encoded[16:24]),
		Count: binary.BigEndian.Uint64(encoded[24:32]),
	}, nil
}
