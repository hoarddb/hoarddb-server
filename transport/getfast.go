// This file implements the compact GET_FAST payload encoding. The frame header
// (version, length, opcode, request ID) is the standard 22-byte header; only the
// payload is compact, because a point read has a small fixed field set.
// See docs/read-path-optimization-spec.md, Optimization 3.
package transport

import (
	"encoding/binary"
	"fmt"
)

// GetFast request payload limits. The database and bucket names are length-
// prefixed with a single byte and the key with two, so these are the encodable
// maxima. Callers exceeding them must fall back to the standard OpGet path.
const (
	MaxGetFastNameLen = 255
	MaxGetFastKeyLen  = 65535
)

// GET_FAST response status values.
const (
	GetFastFound    uint8 = 0x00
	GetFastNotFound uint8 = 0x01
)

// GetFastRequest is a point read identified by database, bucket and key.
type GetFastRequest struct {
	Database string
	Bucket   string
	Key      string
}

// MarshalGetFast encodes a GetFastRequest into its compact wire payload.
// It returns an error rather than truncating when a field exceeds its encodable
// maximum, because a truncated key is a read of the wrong document.
func MarshalGetFast(req GetFastRequest) ([]byte, error) {
	if len(req.Database) > MaxGetFastNameLen {
		return nil, fmt.Errorf("get-fast database name is %d bytes, maximum %d", len(req.Database), MaxGetFastNameLen)
	}
	if len(req.Bucket) > MaxGetFastNameLen {
		return nil, fmt.Errorf("get-fast bucket name is %d bytes, maximum %d", len(req.Bucket), MaxGetFastNameLen)
	}
	if len(req.Key) > MaxGetFastKeyLen {
		return nil, fmt.Errorf("get-fast key is %d bytes, maximum %d", len(req.Key), MaxGetFastKeyLen)
	}

	payload := make([]byte, 0, 4+len(req.Database)+len(req.Bucket)+len(req.Key))
	payload = append(payload, byte(len(req.Database)))
	payload = append(payload, req.Database...)
	payload = append(payload, byte(len(req.Bucket)))
	payload = append(payload, req.Bucket...)

	var keyLen [2]byte
	binary.LittleEndian.PutUint16(keyLen[:], uint16(len(req.Key)))
	payload = append(payload, keyLen[:]...)
	payload = append(payload, req.Key...)

	return payload, nil
}

// UnmarshalGetFast decodes a compact wire payload into a GetFastRequest.
// Every length is validated against the bytes remaining before it is used, so a
// truncated or hostile payload returns an error instead of panicking.
func UnmarshalGetFast(payload []byte) (GetFastRequest, error) {
	if len(payload) < 1 {
		return GetFastRequest{}, fmt.Errorf("get-fast payload is empty")
	}

	dbLen := int(payload[0])
	if len(payload) < 1+dbLen+1 {
		return GetFastRequest{}, fmt.Errorf("get-fast payload is %d bytes, too short for a %d-byte database name and a bucket length", len(payload), dbLen)
	}
	req := GetFastRequest{Database: string(payload[1 : 1+dbLen])}

	offset := 1 + dbLen
	bktLen := int(payload[offset])
	offset++
	if len(payload) < offset+bktLen+2 {
		return GetFastRequest{}, fmt.Errorf("get-fast payload is %d bytes, too short for a %d-byte bucket name and a key length", len(payload), bktLen)
	}
	req.Bucket = string(payload[offset : offset+bktLen])

	offset += bktLen
	keyLen := int(binary.LittleEndian.Uint16(payload[offset : offset+2]))
	offset += 2
	if len(payload) < offset+keyLen {
		return GetFastRequest{}, fmt.Errorf("get-fast payload is %d bytes, too short for a %d-byte key", len(payload), keyLen)
	}
	req.Key = string(payload[offset : offset+keyLen])

	return req, nil
}

// MarshalGetFastResponse encodes a status byte followed by an optional BSON
// document. The document is empty when the status is not GetFastFound.
func MarshalGetFastResponse(status uint8, document []byte) []byte {
	payload := make([]byte, 0, 1+len(document))
	payload = append(payload, status)
	payload = append(payload, document...)
	return payload
}

// UnmarshalGetFastResponse splits a response payload into its status byte and
// the BSON document that follows it. The returned document may be empty.
func UnmarshalGetFastResponse(payload []byte) (uint8, []byte, error) {
	if len(payload) < 1 {
		return 0, nil, fmt.Errorf("get-fast response payload is empty")
	}
	return payload[0], payload[1:], nil
}
