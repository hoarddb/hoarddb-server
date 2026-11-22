package store

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/hoarddb/hoarddb-server/storage"
)

// versionKey is a document's read-repair version on the shared store:
// [s][id][0x02][key], a namespace of its own beside the documents'
// [s][id][0x01], so no document walk or count reaches it. The key is the
// document's key as its engine stores it: a hash key's bytes, a btree key's
// encoding.
func versionKey(slot uint16, databaseID, id uint64, userKey []byte) []byte {
	return storage.Key(slot, databaseID, id, storage.NSVersions, userKey)
}

// versionBytes encodes a write's WAL sequence as the version side-key value.
// Big-endian so a lexicographic compare matches numeric order (not that the
// store compares them — the cluster does — but it keeps the encoding honest).
func versionBytes(seq uint64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], seq)
	return b[:]
}

// versionFromBytes decodes the value of a version side-key; a missing or empty
// key is version 0, which compares as older than any sequenced write.
func versionFromBytes(data []byte) uint64 {
	if len(data) != 8 {
		return 0
	}
	return binary.BigEndian.Uint64(data)
}

// storedAtOrAbove reports whether the version side-key at versionKey, as snap
// holds it, is at or above bound: what a delete bounded by a version spares.
// An absent side-key is version 0.
func storedAtOrAbove(snap storage.KVSnapshot, versionKey []byte, bound uint64) (bool, error) {
	stamped, err := snap.Get(versionKey)
	if errors.Is(err, storage.ErrKeyNotFound) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("reading the version a bounded delete compares: %w", err)
	}
	return versionFromBytes(stamped) >= bound, nil
}
