// Package store provides store type implementations for hoardDB buckets.
package store

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/hoarddb/hoarddb-server/storage"
)

// SecondaryIndex is one index on the node's shared store: an entry is the key
// [s][id][0x03][index id : 2][encoded key][doc id] with an empty value
// (shared-storage spec §3.6), the key encoded by EncodeKey over the index's
// fields (§3.8; GAP-14 build doc Step 3). The bucket's id is in every key, so two buckets indexing the same
// field name never share an entry — the collision the per-bucket layout's
// "idx:<field>:" key had no way to avoid once instances are shared (CR-11).
// It opens nothing: its writes go into the caller's batch, with the document.
type SecondaryIndex struct {
	kv     storage.KV
	prefix []byte
}

// NewSecondaryIndex returns the index of the bucket id in slot whose id is
// indexID (IndexSpec.ID): fixed at create and never reused within the bucket.
func NewSecondaryIndex(kv storage.KV, slot uint16, databaseID, id uint64, indexID uint16) *SecondaryIndex {
	var indexBytes [2]byte
	binary.BigEndian.PutUint16(indexBytes[:], indexID)
	return &SecondaryIndex{kv: kv, prefix: storage.Key(slot, databaseID, id, storage.NSIndex, indexBytes[:])}
}

// EntryRange bounds every entry of the index, half-open.
func (s *SecondaryIndex) EntryRange() storage.IterOptions {
	return storage.IterOptions{Lower: s.prefix, Upper: prefixEnd(s.prefix)}
}

// makeKey is the entry of docID under an encoded value.
func (s *SecondaryIndex) makeKey(encodedValue, docID []byte) []byte {
	key := make([]byte, 0, len(s.prefix)+len(encodedValue)+len(docID))
	key = append(key, s.prefix...)
	key = append(key, encodedValue...)
	return append(key, docID...)
}

// makePrefix bounds every entry of one encoded value. EncodeComponent's
// output is self-delimiting, so no other value's entries share it.
func (s *SecondaryIndex) makePrefix(encodedValue []byte) []byte {
	return append(append([]byte(nil), s.prefix...), encodedValue...)
}

// Add adds the entry mapping an encoded value to a document key to b.
func (s *SecondaryIndex) Add(b storage.KVBatch, encodedValue, docID []byte) error {
	if err := b.Set(s.makeKey(encodedValue, docID), []byte{}); err != nil {
		return fmt.Errorf("adding an index entry: %w", err)
	}
	return nil
}

// Remove adds the delete of that entry to b.
func (s *SecondaryIndex) Remove(b storage.KVBatch, encodedValue, docID []byte) error {
	if err := b.Delete(s.makeKey(encodedValue, docID)); err != nil {
		return fmt.Errorf("removing an index entry: %w", err)
	}
	return nil
}

// ScanBracket visits, in key order, the document key of every entry of a
// one-field index whose value has type tag and is at most upperInclusive, an
// EncodeComponent output. Both ends of the walk carry the tag: the lower
// bound is prefix‖tag, and the upper is the smaller of the end of
// prefix‖upperInclusive and prefix‖tag+1, so the walk never leaves its
// type's bracket — a scan up to a date bound collects no null, boolean or
// number (GAP-14 build doc Step 14; spec §6.2.1's S1).
func (s *SecondaryIndex) ScanBracket(tag byte, upperInclusive []byte, visit func(docKey []byte) error) error {
	lower := s.makePrefix([]byte{tag})
	upper := prefixEnd(s.makePrefix(upperInclusive))
	if bracketEnd := s.makePrefix([]byte{tag + 1}); bytes.Compare(bracketEnd, upper) < 0 {
		upper = bracketEnd
	}
	snap := s.kv.NewSnapshot()
	defer snap.Discard()
	it := snap.NewIterator(storage.IterOptions{Lower: lower, Upper: upper})
	defer it.Close()
	for it.Seek(nil); it.Valid(); it.Next() {
		entry := it.Key()[len(s.prefix):]
		valueLength, err := componentLen(entry)
		if err != nil {
			return &storage.KeyError{Operation: "reading an index entry", Key: bytes.Clone(it.Key()), Cause: err}
		}
		if err := visit(append([]byte(nil), entry[valueLength:]...)); err != nil {
			return err
		}
	}
	if err := it.Err(); err != nil {
		return fmt.Errorf("walking the index entries: %w", err)
	}
	return nil
}

// prefixEnd is the smallest key above every key that begins with prefix.
func prefixEnd(prefix []byte) []byte {
	end := append([]byte(nil), prefix...)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xFF {
			end[i]++
			return end[:i+1]
		}
	}
	return nil
}

// componentLen is the length of the one EncodeComponent output encoded begins
// with. The grammar lives in EncodedComponentLen; the scan path calls this
// name on every entry, so it stays a plain call with no table or allocation.
func componentLen(encoded []byte) (int, error) {
	return EncodedComponentLen(encoded)
}
