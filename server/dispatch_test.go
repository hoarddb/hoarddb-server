package server

import (
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// TestUnsupportedOpcodeMessageUsesReadableRequestID: the request id is
// 16 bytes, and the refusal must print it the way the server log does (hex),
// not as a decimal byte array (BUG-0262).
func TestUnsupportedOpcodeMessageUsesReadableRequestID(t *testing.T) {
	s := newTestServer(t)
	const unassignedOpcode = transport.Opcode(0x7F)
	frame := transport.NewFrame(unassignedOpcode, nil)

	resp, err := s.dispatch(rootContext(), frame)
	if err != nil {
		t.Fatalf("dispatching an unknown opcode returned a transport error: %v", err)
	}
	if resp.Opcode != transport.OpError {
		t.Fatalf("unknown opcode answered %s, want OpError", resp.Opcode)
	}
	var refusal ErrorResponse
	if err := bson.Unmarshal(resp.Payload, &refusal); err != nil {
		t.Fatalf("decode refusal: %v", err)
	}

	if !strings.Contains(refusal.Message, "unsupported opcode 0x7F") {
		t.Errorf("refusal %q no longer names the opcode the CLI matches on", refusal.Message)
	}
	if !strings.Contains(refusal.Message, frame.RequestIDString()) {
		t.Errorf("refusal %q does not carry the request id %s as the log prints it", refusal.Message, frame.RequestIDString())
	}
	if strings.Contains(refusal.Message, "[") {
		t.Errorf("refusal %q prints the request id as a byte array", refusal.Message)
	}
}
