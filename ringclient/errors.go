package ringclient

import (
	"errors"
	"fmt"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/transport"
)

// ErrNotFound is returned (wrapped, so test it with errors.Is) when a single-value read
// finds nothing: a Get of a missing document, a Pop of an empty store, a Take of an absent
// key. List-shaped reads (Peek, StoreList, List) answer an empty result instead.
var ErrNotFound = errors.New("ringclient: not found")

// Error is a refusal from the server: the ring code and the message, which names the fix.
// Detail is set for the codes that define one (NOT_OWNER, STALE_EPOCH, OWNER_DOWN,
// NO_QUORUM, LEASE_LAPSED, RF_UNSUPPORTED, and NOT_FOUND of an expired document).
type Error struct {
	// Code is the ring code of conventions Errors.
	Code cluster.Code
	// Message is the server's text.
	Message string
	// Detail is the structured detail of the code, or nil.
	Detail *cluster.Detail
}

// Error returns the code and message.
func (e *Error) Error() string { return fmt.Sprintf("%s: %s", e.Code, e.Message) }

// Is makes errors.Is(err, ErrNotFound) true for the server's NOT_FOUND.
func (e *Error) Is(target error) bool { return target == ErrNotFound && e.Code == cluster.CodeNotFound }

// refusalError decodes the payload of an OpError frame, ring or legacy shape.
func refusalError(payload []byte) error {
	if refusal, err := wire.DecodeRingError(payload); err == nil {
		return &Error{Code: refusal.Code, Message: refusal.Message, Detail: refusal.Detail}
	}
	return fmt.Errorf("the server refused the request with an error frame of %d bytes that is not a ring error", len(payload))
}

// frameError names an unexpected opcode.
func frameError(op transport.Opcode, what string) error {
	return fmt.Errorf("%s was answered with opcode %s, want RING_REPLY", what, op)
}

// IsExpired reports whether err is the NOT_FOUND of a document that exists but is past its expire
// time. It reads the structured detail, never the message. An expired miss is also ErrNotFound.
func IsExpired(err error) bool {
	var refusal *Error
	return errors.As(err, &refusal) && refusal.Code == cluster.CodeNotFound && refusal.Detail != nil && refusal.Detail.Expired
}

// ExpiresIn is the client's clock plus d, for filling the datetime field a bucket expires its documents at
// (`expire at <Field>`). The server never computes it: the expiry instant is the developer's.
func ExpiresIn(d time.Duration) time.Time { return time.Now().Add(d) }
