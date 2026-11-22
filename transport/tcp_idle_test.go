package transport

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"testing"
	"time"
)

// TestIdleConnectionTimesOutAfterItsLastResponse is the build doc's RF-2
// test (query-pipelining spec §3.4 step 3): idle is measured from the moment
// the connection last had nothing in flight. A request that runs longer than
// the idle timeout is answered, and the connection then has a whole idle
// timeout, jitter included, before the server closes it: neither at once,
// because the deadline ran out while the request was in flight, nor never,
// because the reader cleared a deadline the last response had armed.
func TestIdleConnectionTimesOutAfterItsLastResponse(t *testing.T) {
	const (
		idleTimeout = 200 * time.Millisecond
		held        = 500 * time.Millisecond
		// jittered draws within ±10% of the timeout.
		earliest = idleTimeout * 9 / 10
		latest   = idleTimeout*11/10 + 200*time.Millisecond
		// The answer is read a moment after the server wrote it, so the
		// lower bound allows for that much.
		readSlack = 40 * time.Millisecond
	)
	slowEcho := func(ctx context.Context, frame *Frame) (*Frame, error) {
		if frame.Opcode == OpGet {
			time.Sleep(held)
		}
		return authEchoHandler(ctx, frame)
	}
	ln := startTestTCPListener(t, testTCPConfig(TCPConfig{IdleTimeout: idleTimeout}), slowEcho, nil)
	conn := dialTestTCP(t, ln.Addr().String())
	br := bufio.NewReader(conn)
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	if _, err := conn.Write(NewFrame(OpAuth, nil).Encode()); err != nil {
		t.Fatalf("writing AUTH: %v", err)
	}
	if auth := mustDecode(t, br); auth.Opcode != OpAuthOK {
		t.Fatalf("AUTH answered %s", auth.Opcode)
	}

	if _, err := conn.Write(NewFrame(OpGet, []byte("slow")).Encode()); err != nil {
		t.Fatalf("writing the held request: %v", err)
	}
	answer, err := DecodeFrame(br)
	if err != nil {
		t.Fatalf("a request held %v on a connection with a %v idle timeout was not answered: %v", held, idleTimeout, err)
	}
	answeredAt := time.Now()
	if answer.Opcode != OpGet {
		t.Fatalf("the held request answered %s", answer.Opcode)
	}

	_, err = br.ReadByte()
	closedAfter := time.Since(answeredAt)
	if !errors.Is(err, io.EOF) && !isTimeout(err) {
		t.Logf("the connection ended with %v", err)
	}
	if isTimeout(err) {
		t.Fatalf("the server never closed the idle connection: it was still open %v after its last answer", closedAfter)
	}
	if closedAfter < earliest-readSlack {
		t.Fatalf("the server closed the connection %v after its last answer, before the %v idle timeout had passed: "+
			"the idle deadline ran while the request was in flight", closedAfter, idleTimeout)
	}
	if closedAfter > latest {
		t.Fatalf("the server closed the connection %v after its last answer, later than %v", closedAfter, latest)
	}
}

// sendDrainedOversizeRequest sends a request over the frame limit, reads its
// refusal, and sends the rest of its body for the server to discard.
func sendDrainedOversizeRequest(t *testing.T, conn interface{ Write([]byte) (int, error) }, br *bufio.Reader) {
	t.Helper()
	bodyLength := uint32(MaxFrameLength + 4096)
	writeOversizeRequestHead(t, conn, bodyLength)
	if refusal := mustDecode(t, br); refusal.Opcode != OpError {
		t.Fatalf("the oversize request answered %s, want ERROR", refusal.Opcode)
	}
	if _, err := conn.Write(make([]byte, int(bodyLength)-1-RequestIDLen)); err != nil {
		t.Fatalf("writing the oversize body: %v", err)
	}
}

// authenticatedTestConn dials the listener and logs in.
func authenticatedTestConn(t *testing.T, ln *TCPListener) (*tls.Conn, *bufio.Reader) {
	t.Helper()
	conn := dialTestTCP(t, ln.Addr().String())
	_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
	br := bufio.NewReader(conn)
	if _, err := conn.Write(NewFrame(OpAuth, nil).Encode()); err != nil {
		t.Fatalf("writing AUTH: %v", err)
	}
	if auth := mustDecode(t, br); auth.Opcode != OpAuthOK {
		t.Fatalf("AUTH answered %s", auth.Opcode)
	}
	return conn, br
}

// TestOversizeRequestGivesBackItsWindowSlot: a request over the frame limit
// is answered on the reader and its body discarded, and the read slot the
// reader reserved for it is given back. Kept, it was never released, so at a
// window of one frame the connection read nothing after it; and with QP-3's
// idle rule, which arms the idle deadline only when nothing is in flight, the
// connection would never idle out after one.
func TestOversizeRequestGivesBackItsWindowSlot(t *testing.T) {
	ln := startTestTCPListener(t, testTCPConfig(TCPConfig{FrameTimeout: 20 * time.Second, PipelineFrames: 1}), authEchoHandler, nil)
	conn, br := authenticatedTestConn(t, ln)
	sendDrainedOversizeRequest(t, conn, br)

	next := NewFrame(OpGet, []byte("after"))
	if _, err := conn.Write(next.Encode()); err != nil {
		t.Fatalf("writing the next request: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	answer, err := DecodeFrame(br)
	if err != nil {
		t.Fatalf("the request after an oversize one was not answered at a window of one frame: %v; "+
			"the oversize request kept its read slot", err)
	}
	if answer.Opcode != OpGet || answer.RequestID != next.RequestID {
		t.Fatalf("the request after an oversize one answered %s", answer.Opcode)
	}
}

// TestIdleDeadlineArmsWithNothingInFlight is RF-2's other half: a reader that
// starts waiting for the next frame with nothing in flight arms the idle
// deadline itself, and must not clear it afterwards. An oversize request is
// answered on the reader, so the reader then waits with nothing in flight
// whatever the scheduling.
func TestIdleDeadlineArmsWithNothingInFlight(t *testing.T) {
	const idleTimeout = 300 * time.Millisecond
	ln := startTestTCPListener(t, testTCPConfig(TCPConfig{IdleTimeout: idleTimeout, FrameTimeout: 20 * time.Second}), authEchoHandler, nil)
	conn, br := authenticatedTestConn(t, ln)
	sendDrainedOversizeRequest(t, conn, br)
	drained := time.Now()

	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, err := br.ReadByte(); isTimeout(err) {
		t.Fatalf("the server never closed a connection idle for %v with nothing in flight (idle timeout %v)", time.Since(drained), idleTimeout)
	}
}
