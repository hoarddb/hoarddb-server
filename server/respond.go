package server

import (
	"fmt"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// respondFrame encodes resp as the payload of an opcode frame answering frame.
//
// An encode failure becomes an OpError frame that names the request opcode and
// the cause. It is never sent as an empty payload: a client decodes an empty
// payload into a zero-valued response, which reads as an ordinary miss, and the
// failure is lost.
func respondFrame(frame *transport.Frame, opcode transport.Opcode, resp any) *transport.Frame {
	payload, err := bson.Marshal(resp)
	if err != nil {
		return errorFrame(frame, fmt.Errorf("encoding the response to %s: %w", frame.Opcode, err))
	}
	return transport.NewFrameWithID(opcode, frame.RequestID, payload)
}

// respond is respondFrame for a handler: the nil error keeps the client's
// stream alive, as errorResponse does.
func respond(frame *transport.Frame, opcode transport.Opcode, resp any) (*transport.Frame, error) {
	return respondFrame(frame, opcode, resp), nil
}
