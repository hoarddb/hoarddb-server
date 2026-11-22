package transport

import (
	"bytes"
	"context"
	"testing"
)

// TestOnlyAuthAndPingAreServedBeforeLogin is the regression test: before login the
// gate passes AUTH and PING and nothing else. An opcode a server sends to a
// client is refused with NOT_AUTHENTICATED and never reaches the handler.
func TestOnlyAuthAndPingAreServedBeforeLogin(t *testing.T) {
	for _, op := range []Opcode{OpAuthOK, OpAuthFail, OpPong, OpError, OpGetResponse} {
		reached := false
		handler := func(ctx context.Context, frame *Frame) (*Frame, error) {
			reached = true
			return NewFrameWithID(frame.Opcode, frame.RequestID, nil), nil
		}
		sess := &Session{RemoteAddr: "198.51.100.7:4000"}
		var written bytes.Buffer
		frame := NewFrame(op, []byte("payload"))
		if err := ServeFrame(context.Background(), sess, frame, &written, handler, discardLogger()); err != nil {
			t.Fatalf("%s: ServeFrame: %v", op, err)
		}
		if reached {
			t.Errorf("%s before login reached the handler", op)
		}
		answer := mustDecode(t, &written)
		if answer.Opcode != OpError {
			t.Errorf("%s before login answered %s, want ERROR", op, answer.Opcode)
			continue
		}
		if code, _ := wireErrorOf(t, answer); code != ErrNotAuthenticated {
			t.Errorf("%s before login answered code %s, want NOT_AUTHENTICATED", op, code)
		}
	}
}
