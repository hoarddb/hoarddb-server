package transport

import (
	"bufio"
	"context"
	"sync/atomic"
	"testing"
)

// TestAnUncheckedNonAuthFailRefusalIsNotCountedAsAFailedLogin (review M1): a handler may refuse an
// AUTH with a plain ERROR frame (the server's replay gate does, UNAVAILABLE) and mark it
// credential-unchecked; more such refusals than auth_max_fails must not lock the address out, and
// a correct login after the handler opens succeeds. Break: ignore the mark for a non-AUTH_FAIL
// opcode in serveAuthFrame.
func TestAnUncheckedNonAuthFailRefusalIsNotCountedAsAFailedLogin(t *testing.T) {
	var shut atomic.Bool
	shut.Store(true)
	handler := func(ctx context.Context, frame *Frame) (*Frame, error) {
		if shut.Load() {
			refusal := NewFrameWithID(OpError, frame.RequestID, nil)
			refusal.MarkCredentialUnchecked()
			return refusal, nil
		}
		return authEchoHandler(ctx, frame)
	}
	ln := startTestTCPListener(t, testTCPConfig(TCPConfig{AuthMaxFails: ledgerTestMaxFails}), handler, nil)
	for i := 0; i < ledgerTestMaxFails+2; i++ {
		conn := dialTestTCP(t, ln.Addr().String())
		if got := authOn(t, conn, bufio.NewReader(conn), "good").Opcode; got != OpError {
			t.Fatalf("attempt %d while shut: got %s, want ERROR", i+1, got)
		}
		awaitLedgerCount(t, ln, conn, 0)
		_ = conn.Close()
	}
	shut.Store(false)
	conn := dialTestTCP(t, ln.Addr().String())
	if got := authOn(t, conn, bufio.NewReader(conn), "good").Opcode; got != OpAuthOK {
		t.Fatalf("a correct login after the gate opened got %s, want AUTH_OK", got)
	}
}
