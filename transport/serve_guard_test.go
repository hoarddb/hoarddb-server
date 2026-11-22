package transport

import (
	"bufio"
	"bytes"
	"context"
	"strings"
	"testing"
	"time"
)

// oversizedMarker is the payload of the one request the handler below answers
// with a response over the frame limit.
var oversizedMarker = []byte("answer-oversized")

// oversizedResponseHandler authenticates like authEchoHandler and echoes every
// request, except that it answers oversizedMarker with a payload of exactly
// MaxFrameLength bytes: its frame body, opcode and request ID included, is
// over the limit by 1 + RequestIDLen bytes, which is how DecodeFrame measures
// it on the other side.
func oversizedResponseHandler(ctx context.Context, frame *Frame) (*Frame, error) {
	if frame.Opcode != OpAuth && bytes.Equal(frame.Payload, oversizedMarker) {
		return NewFrameWithID(frame.Opcode, frame.RequestID, make([]byte, MaxFrameLength)), nil
	}
	return authEchoHandler(ctx, frame)
}

// TestServeFrameRefusesAnOversizedResponse is the frame-length guard
// (query-pipelining spec §3.3): a handler response whose frame body exceeds
// MaxFrameLength is replaced by OpError RESULT_TOO_LARGE with the request's
// ID, the connection stays usable for the next request, and the server-bug
// counter rises by one. Without the guard the frame is written, the client's
// DecodeFrame refuses its length, and the connection is lost with every
// request on it.
func TestServeFrameRefusesAnOversizedResponse(t *testing.T) {
	ln := startTestTCPListener(t, testTCPConfig(TCPConfig{}), oversizedResponseHandler, nil)
	conn := dialTestTCP(t, ln.Addr().String())
	_ = conn.SetDeadline(time.Now().Add(20 * time.Second))
	br := bufio.NewReader(conn)

	if _, err := conn.Write(NewFrame(OpAuth, []byte("admin")).Encode()); err != nil {
		t.Fatalf("write auth: %v", err)
	}
	if resp := mustDecode(t, br); resp.Opcode != OpAuthOK {
		t.Fatalf("auth response opcode = %s, want AUTH_OK", resp.Opcode)
	}

	guardedBefore := OversizedResponses()
	oversized := NewFrame(OpScan, oversizedMarker)
	if _, err := conn.Write(oversized.Encode()); err != nil {
		t.Fatalf("write the request with the oversized answer: %v", err)
	}
	refusal, err := DecodeFrame(br)
	if err != nil {
		t.Fatalf("decoding the answer to an oversized response: %v — the server wrote a frame over "+
			"MaxFrameLength, which the client cannot read, and the connection is lost", err)
	}
	if refusal.Opcode != OpError {
		t.Fatalf("answer opcode = %s, want OpError", refusal.Opcode)
	}
	if refusal.RequestID != oversized.RequestID {
		t.Fatalf("refusal request ID %x does not echo the request's %x", refusal.RequestID, oversized.RequestID)
	}
	code, message := wireErrorOf(t, refusal)
	if code != ErrResultTooLarge {
		t.Fatalf("refusal code = %s, want RESULT_TOO_LARGE (0x0E)", code)
	}
	for _, want := range []string{"SCAN", "32 MiB", "server bug"} {
		if !strings.Contains(message, want) {
			t.Errorf("refusal message %q does not name %q", message, want)
		}
	}
	if raised := OversizedResponses() - guardedBefore; raised != 1 {
		t.Errorf("OversizedResponses rose by %d, want exactly 1", raised)
	}

	next := NewFrame(OpGet, []byte("after"))
	if _, err := conn.Write(next.Encode()); err != nil {
		t.Fatalf("write the next request: %v", err)
	}
	answer, err := DecodeFrame(br)
	if err != nil {
		t.Fatalf("the connection did not survive the refused response: %v", err)
	}
	if answer.RequestID != next.RequestID || !bytes.Equal(answer.Payload, []byte("after")) {
		t.Fatalf("next answer = %s %x %q, want the echo of request %x", answer.Opcode, answer.RequestID, answer.Payload, next.RequestID)
	}
}

// TestErrorCodeResultTooLargeName pins W-2's code and its name: 0x0E, after
// OVERLOADED, is RESULT_TOO_LARGE.
func TestErrorCodeResultTooLargeName(t *testing.T) {
	if ErrResultTooLarge != 0x0E {
		t.Fatalf("ErrResultTooLarge = %#x, want 0x0E", uint8(ErrResultTooLarge))
	}
	if got := ErrResultTooLarge.String(); got != "RESULT_TOO_LARGE" {
		t.Fatalf("ErrResultTooLarge.String() = %q, want RESULT_TOO_LARGE", got)
	}
}

// TestErrorCodeBatchTooLargeName pins BUG-0467 G11's code and its name: 0x11,
// the next free number after FIELD_TYPE, is BATCH_TOO_LARGE. Drivers switch on
// the number, so it must never move.
func TestErrorCodeBatchTooLargeName(t *testing.T) {
	if ErrBatchTooLarge != 0x11 {
		t.Fatalf("ErrBatchTooLarge = %#x, want 0x11", uint8(ErrBatchTooLarge))
	}
	if got := ErrBatchTooLarge.String(); got != "BATCH_TOO_LARGE" {
		t.Fatalf("ErrBatchTooLarge.String() = %q, want BATCH_TOO_LARGE", got)
	}
}
