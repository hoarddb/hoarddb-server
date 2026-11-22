package server

import (
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// TestGetFastWithAMalformedPayloadAnswersAnErrorFrame:
// handleGetFast returned its decode failure as a Go error, which the transport
// treats as fatal: INTERNAL, then the connection closed. Every other handler
// answers an undecodable payload with an error frame and keeps the
// connection, so GET_FAST must too, under the same code.
//
// The test calls the handler through NewHandler with no session, not over a
// driver connection: on the driver port the authorization gate decodes the
// payload first and refuses it itself (authz.go:ruleResource), so the handler
// is reached with a payload it cannot decode only from a cluster member's
// internode session, which the gate exempts.
func TestGetFastWithAMalformedPayloadAnswersAnErrorFrame(t *testing.T) {
	s := newTestServer(t)
	handle := NewHandler(s)

	// The code an undecodable payload gets from a sibling handler, read from
	// the server rather than hard-coded.
	scanAnswer, err := handle(t.Context(), transport.NewFrame(transport.OpScan, []byte{0xff, 0xff}))
	if err != nil {
		t.Fatalf("malformed SCAN returned a Go error: %v", err)
	}
	var scanRefusal ErrorResponse
	if scanAnswer.Opcode != transport.OpError || bson.Unmarshal(scanAnswer.Payload, &scanRefusal) != nil {
		t.Fatalf("a malformed SCAN answered %s; want an ERROR frame", answerText(scanAnswer))
	}

	full, err := transport.MarshalGetFast(transport.GetFastRequest{Database: "app", Bucket: "events", Key: "k01"})
	if err != nil {
		t.Fatalf("encoding GET_FAST: %v", err)
	}
	for _, truncated := range [][]byte{full[:len(full)-3], full[:1], {}} {
		answer, err := handle(t.Context(), transport.NewFrame(transport.OpGetFast, truncated))
		if err != nil {
			t.Fatalf("GET_FAST of %d truncated bytes returned a Go error, which the transport answers with INTERNAL and a closed connection: %v", len(truncated), err)
		}
		if answer.Opcode != transport.OpError {
			t.Fatalf("GET_FAST of %d truncated bytes answered %s; want an ERROR frame", len(truncated), answerText(answer))
		}
		var refusal ErrorResponse
		if err := bson.Unmarshal(answer.Payload, &refusal); err != nil {
			t.Fatalf("decoding the error frame: %v", err)
		}
		if refusal.Code != scanRefusal.Code {
			t.Errorf("GET_FAST answered code %d; a malformed SCAN answers code %d", refusal.Code, scanRefusal.Code)
		}
		if !strings.Contains(refusal.Message, "get-fast request") {
			t.Errorf("message %q does not name the get-fast decode failure", refusal.Message)
		}
	}
}
