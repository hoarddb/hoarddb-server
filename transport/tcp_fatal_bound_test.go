package transport

import (
	"context"
	"runtime"
	"testing"
	"time"
)

// TestAFailedConnectionClosesAtTheBoundAndLeaksNoGoroutine: the wait for the
// work in flight is bounded by the frame timeout. A sibling whose handler
// never finishes on its own does not keep the failed connection open: at the
// bound the connection is closed and the sibling's context cancelled, and once
// the listener is closed every goroutine the connection started has exited.
func TestAFailedConnectionClosesAtTheBoundAndLeaksNoGoroutine(t *testing.T) {
	const frameTimeout = 300 * time.Millisecond
	baseline := runtime.NumGoroutine()

	holdStarted := make(chan struct{})
	holdCancelled := make(chan bool, 1)
	handler := func(ctx context.Context, frame *Frame) (*Frame, error) {
		switch frame.Opcode {
		case OpFIFOPop:
			close(holdStarted)
			// It ends when its connection's work is cancelled, or, on a server
			// that never cancels it, after long enough for the test to see that.
			select {
			case <-ctx.Done():
				holdCancelled <- true
				return nil, ctx.Err()
			case <-time.After(5 * time.Second):
				holdCancelled <- false
				return NewFrameWithID(OpFIFOPopResponse, frame.RequestID, nil), nil
			}
		case OpPut:
			return nil, errSiblingFailed
		default:
			return authEchoHandler(ctx, frame)
		}
	}
	ln := startTestTCPListener(t, testTCPConfig(TCPConfig{FrameTimeout: frameTimeout}), handler, nil)
	conn, br := authenticatedTestConn(t, ln)

	if _, err := conn.Write(NewFrame(OpFIFOPop, nil).Encode()); err != nil {
		t.Fatalf("writing the held request: %v", err)
	}
	select {
	case <-holdStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("the held request's handler never started")
	}
	failed := time.Now()
	if _, err := conn.Write(NewFrame(OpPut, nil).Encode()); err != nil {
		t.Fatalf("writing the failing request: %v", err)
	}

	_ = conn.SetReadDeadline(time.Now().Add(frameTimeout + 3*time.Second))
	if failure := mustDecode(t, br); failure.Opcode != OpError {
		t.Fatalf("the failing request was answered %s, want OpError", failure.Opcode)
	}
	if _, err := br.ReadByte(); err == nil || isTimeout(err) {
		t.Fatalf("the failed connection was still open %v after its handler failed, past the %v bound (read: %v)",
			time.Since(failed), frameTimeout, err)
	}

	select {
	case cancelled := <-holdCancelled:
		if !cancelled {
			t.Fatal("the request still in flight at the bound was never cancelled; its goroutine outlives the connection")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the request still in flight at the bound never ended")
	}

	_ = conn.Close()
	if err := ln.Close(); err != nil {
		t.Fatalf("closing the listener: %v", err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for runtime.NumGoroutine() > baseline {
		if time.Now().After(deadline) {
			t.Fatalf("%d goroutines still running after the listener closed, %d before it started", runtime.NumGoroutine(), baseline)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
