package cluster

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

const (
	cursorVersion byte = 1
	// cursorFixed is version, database id, bucket id and slot; the uvarint id
	// length, the id and the 4-byte checksum follow.
	cursorFixed = 1 + 8 + 8 + 2
	// maxCursorEncID is the 1024-byte cap of an encoded _id_ (conventions 5).
	maxCursorEncID = 1024
	// cursorMin is the smallest cursor: a one-byte id, the null tag.
	cursorMin = cursorFixed + 1 + 1 + 4
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// ListCursor says where the last page of a list ended: strictly after the document
// (Slot, EncID) of the bucket (DatabaseID, BucketID). The server keeps no cursor
// state; the client carries these bytes (queries section 3, conventions Formats 4).
// It is not named Cursor because the WAL replay already owns that name.
type ListCursor struct {
	DatabaseID, BucketID uint64
	Slot                 uint16
	EncID                []byte
}

// CursorError is a cursor that cannot be used. Reason is one of the sentences of
// the bad-cursor text (queries Errors), which the coordinator wraps with the names.
type CursorError struct{ Reason string }

// Error returns the reason.
func (e *CursorError) Error() string { return e.Reason }

// EncodeCursor writes version, database id, bucket id, slot, uvarint id length,
// the encoded id and a CRC-32C of everything before it. It is 25 to 1049 bytes.
func EncodeCursor(cur ListCursor) []byte {
	raw := make([]byte, 0, cursorFixed+binary.MaxVarintLen64+len(cur.EncID)+4)
	raw = append(raw, cursorVersion)
	raw = binary.BigEndian.AppendUint64(raw, cur.DatabaseID)
	raw = binary.BigEndian.AppendUint64(raw, cur.BucketID)
	raw = binary.BigEndian.AppendUint16(raw, cur.Slot)
	raw = binary.AppendUvarint(raw, uint64(len(cur.EncID)))
	raw = append(raw, cur.EncID...)
	return binary.BigEndian.AppendUint32(raw, crc32.Checksum(raw, castagnoli))
}

// DecodeCursor reads cursor bytes issued for the bucket (dbID, bucketID). It
// refuses, with a *CursorError, a cursor of another version, with a wrong
// checksum, issued for another database or bucket, or malformed; it never
// panics and a cursor it accepts re-encodes to the same bytes.
func DecodeCursor(raw []byte, dbID, bucketID uint64) (ListCursor, error) {
	if len(raw) < cursorMin {
		return ListCursor{}, &CursorError{Reason: "it is truncated"}
	}
	if raw[0] != cursorVersion {
		return ListCursor{}, &CursorError{Reason: "its format version is not 1"}
	}
	body, sum := raw[:len(raw)-4], binary.BigEndian.Uint32(raw[len(raw)-4:])
	if crc32.Checksum(body, castagnoli) != sum {
		return ListCursor{}, &CursorError{Reason: "the checksum does not match"}
	}
	cur := ListCursor{
		DatabaseID: binary.BigEndian.Uint64(raw[1:9]),
		BucketID:   binary.BigEndian.Uint64(raw[9:17]),
		Slot:       binary.BigEndian.Uint16(raw[17:19]),
	}
	if cur.DatabaseID != dbID {
		return ListCursor{}, &CursorError{Reason: fmt.Sprintf("its database id is %d", cur.DatabaseID)}
	}
	if cur.BucketID != bucketID {
		return ListCursor{}, &CursorError{Reason: fmt.Sprintf("it was issued for bucket id %d", cur.BucketID)}
	}
	if cur.Slot >= tableSlots {
		return ListCursor{}, &CursorError{Reason: fmt.Sprintf("its slot %d is not one of the %d slots", cur.Slot, tableSlots)}
	}
	idLen, n, ok := readCanonicalUvarint(body[cursorFixed:])
	if !ok || idLen == 0 || idLen > maxCursorEncID || uint64(len(body)-cursorFixed-n) != idLen {
		return ListCursor{}, &CursorError{Reason: "its id length is not valid"}
	}
	cur.EncID = append([]byte(nil), body[cursorFixed+n:]...)
	return cur, nil
}
