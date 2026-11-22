package transport

import (
	"bufio"
	"bytes"
	"context"
	"log/slog"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

// injectedFault is the value the test handler panics with. It must show in the
// server's log and must not reach the client.
const injectedFault = "injected-fault-7731"

type lockedBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.String()
}

// TestAHandlerFaultFailsOnlyItsRequest injects a panic into the request
// handler, through the handler the listener is given, on the driver port and on
// the internode port. The request that hit it is answered INTERNAL
// and its connection is closed; the fault is logged at error level with a
// stack; a request on another connection is unaffected; nothing is left
// running afterwards.
func TestAHandlerFaultFailsOnlyItsRequest(t *testing.T) {
	cases := []struct {
		name     string
		peerMode bool
		faulting Opcode
	}{
		{"driver port", false, OpGet},
		{"internode port", true, OpReplPush},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			baseline := runtime.NumGoroutine()
			logged := &lockedBuffer{}
			deps := &SessionDeps{Logger: slog.New(slog.NewJSONHandler(logged, nil))}
			handler := func(ctx context.Context, frame *Frame) (*Frame, error) {
				if frame.Opcode == tc.faulting && string(frame.Payload) == "fault" {
					panic(injectedFault)
				}
				return authEchoHandler(ctx, frame)
			}
			ln := startTestTCPListener(t, testTCPConfig(TCPConfig{PeerMode: tc.peerMode}), handler, deps)

			faulted, faultedReader := authenticatedTestConn(t, ln)
			request := NewFrame(tc.faulting, []byte("fault"))
			if _, err := faulted.Write(request.Encode()); err != nil {
				t.Fatalf("writing the faulting request: %v", err)
			}
			answer := mustDecode(t, faultedReader)
			if answer.Opcode != OpError {
				t.Fatalf("the faulting request was answered %s, want OpError", answer.Opcode)
			}
			if answer.RequestID != request.RequestID {
				t.Errorf("the answer carries request id %x, want the request's %x", answer.RequestID, request.RequestID)
			}
			code, message := wireErrorOf(t, answer)
			if code != ErrInternal {
				t.Errorf("error code = %s, want INTERNAL", code)
			}
			if !strings.Contains(strings.ToLower(message), "report") {
				t.Errorf("message %q does not tell the operator to report it", message)
			}
			if strings.Contains(message, injectedFault) {
				t.Errorf("message %q repeats the fault's value to the client", message)
			}
			if _, err := faultedReader.ReadByte(); err == nil || isTimeout(err) {
				t.Errorf("the faulted connection was left open (read: %v)", err)
			}

			other, otherReader := authenticatedTestConn(t, ln)
			echo := NewFrame(tc.faulting, []byte("fine"))
			if _, err := other.Write(echo.Encode()); err != nil {
				t.Fatalf("writing on the other connection: %v", err)
			}
			if reply := mustDecode(t, otherReader); reply.Opcode != tc.faulting || string(reply.Payload) != "fine" {
				t.Errorf("the other connection's request was answered %s %q, want its echo", reply.Opcode, reply.Payload)
			}

			assertFaultLogged(t, logged.String())
			stopAndExpectNoLeak(t, ln, baseline)
		})
	}
}

func assertFaultLogged(t *testing.T, log string) {
	t.Helper()
	var faultLine string
	for _, line := range strings.Split(log, "\n") {
		if strings.Contains(line, injectedFault) {
			faultLine = line
		}
	}
	switch {
	case faultLine == "":
		t.Fatalf("the fault was not logged:\n%s", log)
	case !strings.Contains(faultLine, `"level":"ERROR"`):
		t.Errorf("the fault was not logged at error level: %s", faultLine)
	case !strings.Contains(faultLine, "tcp_recover_test.go"):
		t.Errorf("the fault's log line carries no stack naming where it happened: %s", faultLine)
	}
}

func stopAndExpectNoLeak(t *testing.T, ln *TCPListener, baseline int) {
	t.Helper()
	if err := ln.Close(); err != nil {
		t.Fatalf("closing the listener: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for runtime.NumGoroutine() > baseline {
		if time.Now().After(deadline) {
			t.Fatalf("%d goroutines still running after the listener closed, %d before it started", runtime.NumGoroutine(), baseline)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestAFaultingAuthGivesBackItsLedgerReservation: an AUTH admitted by the
// per-address limiter holds a reservation until it is settled. A handler fault
// settles nothing, so the recovery returns the reservation; kept, a few faults
// would lock the address out of logging in.
func TestAFaultingAuthGivesBackItsLedgerReservation(t *testing.T) {
	handler := func(ctx context.Context, frame *Frame) (*Frame, error) {
		if frame.Opcode == OpAuth && string(frame.Payload) == "fault" {
			panic(injectedFault)
		}
		return authEchoHandler(ctx, frame)
	}
	ln := startTestTCPListener(t, testTCPConfig(TCPConfig{AuthMaxFails: ledgerTestMaxFails}), handler, nil)
	conn := dialTestTCP(t, ln.Addr().String())
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	reader := bufio.NewReader(conn)
	if _, err := conn.Write(NewFrame(OpAuth, []byte("fault")).Encode()); err != nil {
		t.Fatalf("writing the faulting AUTH: %v", err)
	}
	if answer := mustDecode(t, reader); answer.Opcode != OpError {
		t.Fatalf("the faulting AUTH was answered %s, want OpError", answer.Opcode)
	}
	deadline := time.Now().Add(3 * time.Second)
	for ln.authLim.reservedAttempts("127.0.0.1") != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("%d reservations still held after the faulting AUTH ended", ln.authLim.reservedAttempts("127.0.0.1"))
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestAFaultDrainsInFlightRequestsBeforeTheConnectionCloses: a request already
// running beside the faulting one still gets its answer, which may be the only
// record that it applied (a pop's removal), and only then does the connection
// close.
func TestAFaultDrainsInFlightRequestsBeforeTheConnectionCloses(t *testing.T) {
	held := make(chan struct{})
	release := make(chan struct{})
	handler := func(ctx context.Context, frame *Frame) (*Frame, error) {
		switch frame.Opcode {
		case OpFIFOPop:
			close(held)
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			return NewFrameWithID(OpFIFOPop, frame.RequestID, []byte("popped")), nil
		case OpGet:
			panic(injectedFault)
		}
		return authEchoHandler(ctx, frame)
	}
	ln := startTestTCPListener(t, testTCPConfig(TCPConfig{FrameTimeout: 10 * time.Second}), handler, discardDeps())
	conn, reader := authenticatedTestConn(t, ln)

	inFlight := NewFrame(OpFIFOPop, nil)
	if _, err := conn.Write(inFlight.Encode()); err != nil {
		t.Fatalf("writing the in-flight request: %v", err)
	}
	select {
	case <-held:
	case <-time.After(3 * time.Second):
		t.Fatal("the in-flight request never started")
	}
	if _, err := conn.Write(NewFrame(OpGet, nil).Encode()); err != nil {
		t.Fatalf("writing the faulting request: %v", err)
	}
	if answer := mustDecode(t, reader); answer.Opcode != OpError {
		t.Fatalf("the faulting request was answered %s, want OpError", answer.Opcode)
	}
	close(release)
	answer := mustDecode(t, reader)
	if answer.Opcode != OpFIFOPop || string(answer.Payload) != "popped" || answer.RequestID != inFlight.RequestID {
		t.Fatalf("the in-flight request was answered %s %q, want its own answer", answer.Opcode, answer.Payload)
	}
	if _, err := reader.ReadByte(); err == nil || isTimeout(err) {
		t.Fatalf("the connection stayed open after the in-flight request answered (read: %v)", err)
	}
}

func discardDeps() *SessionDeps { return &SessionDeps{Logger: discardLogger()} }
