package transport

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// errSiblingFailed is the Go error the failing sibling's handler returns.
var errSiblingFailed = errors.New("the sibling request's handler failed")

// TestAFatalErrorOnOneFrameDoesNotDropTheAnswerToAnotherInFlight is BUG-0563's
// reproduction. A pop is in flight on a pipelined connection, its entry
// already removed, when a sibling request's handler returns a Go error. The
// sibling gets its internal error, and the pop's answer, the only copy of the
// entry, must still reach the client before the server closes the
// connection. The serve loop used to close the connection at once, so the
// pop's response write failed and the entry was lost.
func TestAFatalErrorOnOneFrameDoesNotDropTheAnswerToAnotherInFlight(t *testing.T) {
	popStarted := make(chan struct{})
	releasePop := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releasePop) }) }
	handler := func(ctx context.Context, frame *Frame) (*Frame, error) {
		switch frame.Opcode {
		case OpFIFOPop:
			close(popStarted)
			<-releasePop
			return NewFrameWithID(OpFIFOPopResponse, frame.RequestID, []byte("the popped entry")), nil
		case OpPut:
			return nil, errSiblingFailed
		default:
			return authEchoHandler(ctx, frame)
		}
	}
	ln := startTestTCPListener(t, testTCPConfig(TCPConfig{FrameTimeout: 5 * time.Second}), handler, nil)
	// Registered after the listener, so it runs first: the listener's Close
	// drains the held pop and would otherwise wait for it.
	t.Cleanup(release)
	conn, br := authenticatedTestConn(t, ln)

	pop := NewFrame(OpFIFOPop, nil)
	if _, err := conn.Write(pop.Encode()); err != nil {
		t.Fatalf("writing the pop: %v", err)
	}
	select {
	case <-popStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("the pop's handler never started")
	}
	put := NewFrame(OpPut, nil)
	if _, err := conn.Write(put.Encode()); err != nil {
		t.Fatalf("writing the put: %v", err)
	}

	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	failure := mustDecode(t, br)
	if failure.Opcode != OpError || failure.RequestID != put.RequestID {
		t.Fatalf("the failing put was answered %s for request %x, want OpError for %x", failure.Opcode, failure.RequestID, put.RequestID)
	}
	if code, _ := wireErrorOf(t, failure); code != ErrInternal {
		t.Fatalf("the failing put was answered with code %v, want ErrInternal", code)
	}

	release()
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	answer, err := DecodeFrame(br)
	if err != nil {
		t.Fatalf("the pop in flight when its sibling failed was never answered: %v; "+
			"the server closed the connection without waiting for it, and the entry is lost", err)
	}
	if answer.Opcode != OpFIFOPopResponse || answer.RequestID != pop.RequestID || string(answer.Payload) != "the popped entry" {
		t.Fatalf("the pop was answered %s for request %x with %q, want its entry", answer.Opcode, answer.RequestID, answer.Payload)
	}

	// With nothing left in flight, the connection whose handler failed is
	// closed: the failure still ends it, only after the answers.
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, err := br.ReadByte(); err == nil || isTimeout(err) {
		t.Fatalf("the connection was still open after its in-flight work finished (read: %v)", err)
	}
}
