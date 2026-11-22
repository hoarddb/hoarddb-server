package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// The kind byte of an expiry key is the expiry spec's own numbering (§3.1), not a
// document class byte: a document class byte is an Op, and an expiry key is no op.
const (
	// ExpiryKindDocument names a bucket-form entry: the whole document leaves.
	ExpiryKindDocument byte = 0x01
	// ExpiryKindElement names a queue element entry.
	ExpiryKindElement byte = 0x04
)

// expiryBodyHead is the fixed part after the namespace: at:8 ‖ slot:2 ‖ db:8 ‖ bucket:8 ‖ kind:1.
const expiryBodyHead = 8 + 2 + 8 + 8 + 1

// expiryElementTail is the fixed suffix of an element key: fieldID:2 ‖ position:8.
const expiryElementTail = 2 + 8

// ExpiryEntry is one decoded queue key.
type ExpiryEntry struct {
	AtMillis int64 // the expire field's datetime, milliseconds since the epoch
	Slot     uint16
	DBID     uint64
	BucketID uint64
	Kind     byte
	EncID    []byte
	FieldID  uint16 // ExpiryKindElement only
	Position uint64 // ExpiryKindElement only
}

// ExpiryPrefix is NodeKey(NSExpiry), the seek start of a sweep and the bound of the
// namespace.
func ExpiryPrefix() []byte {
	return NodeKey(NSExpiry)
}

// ExpiryKey is NodeKey(NSExpiry) ‖ (at ^ 1<<63):8 BE ‖ slot:2 ‖ db:8 ‖ bucket:8 ‖ kind:1 ‖
// encID [‖ fieldID:2 ‖ position:8 for a queue element]. The sign flip makes negative
// millis sort before zero and positive millis, so the tick's seek finds the due keys
// first.
func ExpiryKey(entry ExpiryEntry) []byte {
	key := ExpiryPrefix()
	key = binary.BigEndian.AppendUint64(key, uint64(entry.AtMillis)^(1<<63))
	key = binary.BigEndian.AppendUint16(key, entry.Slot)
	key = binary.BigEndian.AppendUint64(key, entry.DBID)
	key = binary.BigEndian.AppendUint64(key, entry.BucketID)
	key = append(key, entry.Kind)
	key = append(key, entry.EncID...)
	if entry.Kind == ExpiryKindElement {
		key = binary.BigEndian.AppendUint16(key, entry.FieldID)
		key = binary.BigEndian.AppendUint64(key, entry.Position)
	}
	return key
}

// ParseExpiryKey is the inverse of ExpiryKey. It refuses a key outside the expiry
// namespace, a kind byte other than the two above, and an element key too short to hold
// its tail. A document key has no tail, so its encID is everything after the kind byte.
// The returned encID is a copy, never a view of key.
func ParseExpiryKey(key []byte) (ExpiryEntry, error) {
	prefix := ExpiryPrefix()
	if !bytes.HasPrefix(key, prefix) {
		return ExpiryEntry{}, fmt.Errorf("expiry key of %d bytes does not start with the expiry namespace", len(key))
	}
	body := key[len(prefix):]
	if len(body) < expiryBodyHead {
		return ExpiryEntry{}, fmt.Errorf("expiry key of %d bytes is shorter than its %d-byte head", len(key), len(prefix)+expiryBodyHead)
	}
	var entry ExpiryEntry
	entry.AtMillis = int64(binary.BigEndian.Uint64(body[0:8]) ^ (1 << 63))
	entry.Slot = binary.BigEndian.Uint16(body[8:10])
	entry.DBID = binary.BigEndian.Uint64(body[10:18])
	entry.BucketID = binary.BigEndian.Uint64(body[18:26])
	entry.Kind = body[26]
	rest := body[expiryBodyHead:]
	switch entry.Kind {
	case ExpiryKindDocument:
		entry.EncID = bytes.Clone(rest)
	case ExpiryKindElement:
		if len(rest) < expiryElementTail {
			return ExpiryEntry{}, fmt.Errorf("element expiry key has %d bytes after its head, want at least the %d-byte tail", len(rest), expiryElementTail)
		}
		split := len(rest) - expiryElementTail
		entry.EncID = bytes.Clone(rest[:split])
		entry.FieldID = binary.BigEndian.Uint16(rest[split : split+2])
		entry.Position = binary.BigEndian.Uint64(rest[split+2:])
	default:
		return ExpiryEntry{}, fmt.Errorf("expiry key kind %#02x is neither document %#02x nor element %#02x", entry.Kind, ExpiryKindDocument, ExpiryKindElement)
	}
	return entry, nil
}
