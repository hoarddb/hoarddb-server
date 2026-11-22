package server

import (
	"context"
	"errors"
	"fmt"

	"github.com/hoarddb/hoarddb-server/transport"
)

// handleGetFast processes a compact GET_FAST read. It decodes the compact
// payload into the same FindRequest the standard GET path uses, so there is one
// read implementation and the two encodings cannot drift apart.
func handleGetFast(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	req, err := transport.UnmarshalGetFast(frame.Payload)
	if err != nil {
		// An error frame, not a returned error: the transport treats a returned
		// error as fatal and closes the connection, as handleScan does not.
		// It is the request's fault, so it carries the sentinel that answers
		// INVALID_ARGUMENT, as a payload the scan refuses does (BUG-0549).
		return errorResponse(frame, &sentencedError{cause: errors.Join(ErrMalformedPayload, err),
			message: fmt.Sprintf("decoding get-fast request: %v", err)})
	}

	resp, err := s.Find(FindRequest{Database: req.Database, Bucket: req.Bucket, Key: req.Key})
	if err != nil {
		// An error frame, not a returned error: the transport treats a returned
		// error as fatal and closes the connection.
		return errorResponse(frame, err)
	}

	// A miss is a normal outcome, not an error: status carries it and the
	// document region stays empty. See the tcp-protocol-spec record error
	// codes.
	if !resp.Found {
		return transport.NewFrameWithID(transport.OpGetFastResponse, frame.RequestID,
			transport.MarshalGetFastResponse(transport.GetFastNotFound, nil)), nil
	}

	return transport.NewFrameWithID(transport.OpGetFastResponse, frame.RequestID,
		transport.MarshalGetFastResponse(transport.GetFastFound, resp.Data)), nil
}
