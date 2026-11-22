package transport

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestDriverClientRejectsAMismatchedRequestID covers BUG-0263: a response whose
// request id is not the one the client sent is a stream that has fallen out of
// step (a timed-out call's late answer, say), so it must be an error that
// names both ids and the client must close rather than hand it back as the
// answer to the wrong request.
func TestDriverClientRejectsAMismatchedRequestID(t *testing.T) {
	staleID := [RequestIDLen]byte{0xde, 0xad, 0xbe, 0xef}
	handler := func(ctx context.Context, frame *Frame) (*Frame, error) {
		if string(frame.Payload) == "late" {
			return NewFrameWithID(frame.Opcode, staleID, []byte("answer to another request")), nil
		}
		return authEchoHandler(ctx, frame)
	}
	ln := startTestTCPListener(t, testTCPConfig(TCPConfig{}), handler, nil)
	client := dialAuthenticatedDriverClient(t, ln)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	answer, err := client.SendRequest(ctx, OpGet, []byte("late"))
	if err == nil {
		t.Fatalf("a response carrying request id %x was returned as the answer: %+v", staleID, answer)
	}
	staleFrame := &Frame{RequestID: staleID}
	if want := staleFrame.RequestIDString(); !strings.Contains(err.Error(), want) {
		t.Errorf("error %q does not name the id received, %s", err, want)
	}

	client.mu.Lock()
	closed := client.closed
	client.mu.Unlock()
	if !closed {
		t.Error("the client stayed open after a response out of step with its requests")
	}
}

// TestDriverClientAcceptsAConnectionRefusalWithAZeroRequestID keeps the rule
// from swallowing the one legitimate mismatch: a connection-level refusal
// carries an all-zero id (section 1.2 of the driver spec) and must reach
// the caller as a frame, not as an id error.
func TestDriverClientAcceptsAConnectionRefusalWithAZeroRequestID(t *testing.T) {
	var zeroID [RequestIDLen]byte
	handler := func(ctx context.Context, frame *Frame) (*Frame, error) {
		if string(frame.Payload) == "refuse" {
			return NewFrameWithID(OpError, zeroID, []byte("refused")), nil
		}
		return authEchoHandler(ctx, frame)
	}
	ln := startTestTCPListener(t, testTCPConfig(TCPConfig{}), handler, nil)
	client := dialAuthenticatedDriverClient(t, ln)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	answer, err := client.SendRequest(ctx, OpGet, []byte("refuse"))
	if err != nil {
		t.Fatalf("a zero-id refusal was rejected: %v", err)
	}
	if answer.Opcode != OpError {
		t.Errorf("opcode = %v, want OpError", answer.Opcode)
	}
}
