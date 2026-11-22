package cluster

import (
	"fmt"

	"github.com/google/uuid"

	"github.com/hoarddb/hoarddb-server/ids"
)

// MintID mints the _id_ of an insert that named none: a UUIDv7 in its canonical
// 36-character form (decisions §1), unique across processes and increasing within
// one, so a list by encoded id stays close to insertion order.
//
// The client mints before its first send so a retry carries the same id and the
// dedupe key (request id, op index) finds the first attempt (conventions §18). The
// owner never calls it: applying a logged write generates no value.
func MintID() (string, error) {
	id, err := ids.NewDocumentID()
	if err != nil {
		return "", fmt.Errorf("minting an _id_: %w", err)
	}
	return id, nil
}

// DeriveID is the _id_ the coordinator gives an insert that named none and carries a
// request id. It is the request id with the op index mixed into the last byte, and
// the UUIDv7 version and RFC 4122 variant bits set, in canonical 36-character form.
//
// Deriving instead of drawing at random is what makes the retry safe: the same
// (request id, op index) gives the same id, so the same slot and the same pair, whose
// dedupe entry then recognises the retry. Two operations of one batch differ in op
// index and so get different ids. The request id's own leading bytes are kept, so ids
// from one client keep the order of the request ids it drew.
func DeriveID(requestID [16]byte, opIndex uint8) string {
	raw := requestID
	raw[15] ^= opIndex
	raw[6] = raw[6]&0x0f | 0x70
	raw[8] = raw[8]&0x3f | 0x80
	return uuid.UUID(raw).String()
}
