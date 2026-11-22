package storage

import (
	"fmt"
)

const (
	// walRecordVersion is byte 0 of every retained-log record: format 2 (storage-format
	// spec §1.2). A record of another version is refused, never read, never migrated.
	walRecordVersion byte = 0x02
	// WALRecordHeaderLen is the fixed part of a record: the version, the 16-byte request
	// id and the op index. The class is the log entry's own op byte and the body's first
	// byte; the body's length is the entry's.
	WALRecordHeaderLen = 1 + 16 + 1
	// walRecordRequestIDOffset and walRecordOpIndexOffset: bytes 1 to 17 of the record are
	// exactly the dedupe key's suffix requestID ‖ opIndex (conv §18), with no gap byte.
	walRecordRequestIDOffset = 1
	walRecordOpIndexOffset   = 17
	// walRecordMinLen is the shortest record: the header, then the body's class and flags.
	walRecordMinLen = WALRecordHeaderLen + 2
)

// WALRecord is one retained-log record (conv §9): what a catch-up streams and
// what every copy applies without evaluating the schema fence. The body is the
// record body byte for byte ([class][flags][header:40][stamp?][form]); the epoch
// opener's prevEnd is in the body's payload header, not a field here. Class is
// the entry's op byte, cross-checked against the body's first byte on decode.
type WALRecord struct {
	Class     byte
	OpIndex   byte
	RequestID [16]byte
	Body      []byte
}

// WALRecordVersionError is DecodeWALRecord's refusal of a record of another version: a log
// written by a build before v1.1's format, which nothing migrates (R1).
type WALRecordVersionError struct {
	// Version is the byte as read.
	Version byte
}

// Error implements error.
func (e *WALRecordVersionError) Error() string {
	return fmt.Sprintf("version %#02x is not the %#02x this build reads", e.Version, walRecordVersion)
}

// WALRecordClassError is DecodeWALRecord's refusal of a record whose body's first byte is
// not the entry's op byte: the two name the class once each and must agree (R2).
type WALRecordClassError struct {
	// Body is the body's first byte; Entry is the log entry's op byte.
	Body, Entry byte
}

// Error implements error.
func (e *WALRecordClassError) Error() string {
	return fmt.Sprintf("the body says class %#02x and the entry %#02x", e.Body, e.Entry)
}

// EncodeWALRecord lays a record out as [version:1 = 0x02][requestID:16][opIndex:1][body]:
// the 18-byte header then the body, whose first byte is the class the entry's op byte
// repeats. The body's length is not stored: the entry bounds it and the frame word is the
// integrity check.
func EncodeWALRecord(opIndex byte, requestID [16]byte, body []byte) []byte {
	record := make([]byte, 0, WALRecordHeaderLen+len(body))
	record = append(record, walRecordVersion)
	record = append(record, requestID[:]...)
	record = append(record, opIndex)
	return append(record, body...)
}

// DecodeWALRecord reads a record written by EncodeWALRecord, given the entry's op byte. It
// refuses a record shorter than 20 bytes naming its length, a version other than 0x02 (a
// *WALRecordVersionError) and a body whose first byte differs from op (a
// *WALRecordClassError), so a record of another build is never taken for one of this one.
// The returned body is a copy.
func DecodeWALRecord(op byte, record []byte) (WALRecord, error) {
	if len(record) < walRecordMinLen {
		return WALRecord{}, fmt.Errorf("retained-log record: want at least %d bytes (the %d-byte header, the class and the flags), got %d", walRecordMinLen, WALRecordHeaderLen, len(record))
	}
	if record[0] != walRecordVersion {
		return WALRecord{}, fmt.Errorf("retained-log record: %w", &WALRecordVersionError{Version: record[0]})
	}
	if record[WALRecordHeaderLen] != op {
		return WALRecord{}, fmt.Errorf("retained-log record: %w", &WALRecordClassError{Body: record[WALRecordHeaderLen], Entry: op})
	}
	decoded := WALRecord{Class: op, OpIndex: record[walRecordOpIndexOffset], Body: append([]byte(nil), record[WALRecordHeaderLen:]...)}
	copy(decoded.RequestID[:], record[walRecordRequestIDOffset:walRecordRequestIDOffset+16])
	return decoded, nil
}
