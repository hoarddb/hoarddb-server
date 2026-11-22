package transport

import (
	"bufio"
	"context"
	"crypto/tls"
	"net"
	"strings"
	"testing"
	"time"
)

// These tests cover this: a connection on the internode port takes a peer
// slot at accept, and nothing made it authenticate, so one address could hold
// the whole peer budget and a member's dial was then refused.

// dialFromLoopbackAlias opens a TLS connection to addr whose source address is
// sourceIP, so a test can stand in for a second host on one machine (all of
// 127.0.0.0/8 routes to the loopback interface on Linux).
func dialFromLoopbackAlias(t *testing.T, addr, sourceIP string) (*tls.Conn, error) {
	t.Helper()
	tlsCfg, err := MakeClientTLSConfig("", "", TrustPolicy{Insecure: &InsecureTrust{Reason: "transport pre-auth test"}}, addr)
	if err != nil {
		t.Fatalf("client TLS config: %v", err)
	}
	tlsCfg.NextProtos = []string{ALPNFor(ProtocolMajor)}
	dialer := &tls.Dialer{
		NetDialer: &net.Dialer{Timeout: 5 * time.Second, LocalAddr: &net.TCPAddr{IP: net.ParseIP(sourceIP)}},
		Config:    tlsCfg,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	netConn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	conn := netConn.(*tls.Conn)
	t.Cleanup(func() { _ = conn.Close() })
	return conn, nil
}

// TestUnauthenticatedInternodeConnectionsCannotHoldThePeerBudget fills one
// address's share of the peer port with connections that complete TLS and
// never authenticate, and requires that a member dialling from another
// address is still admitted. The per-address cap is set equal to the peer
// budget, as the shipped defaults set it (64 and 64).
func TestUnauthenticatedInternodeConnectionsCannotHoldThePeerBudget(t *testing.T) {
	const peerBudget = 8
	limiter := NewConnLimiter(64, peerBudget)
	cfg := testTCPConfig(TCPConfig{PeerMode: true, MaxConnsPerIP: peerBudget})
	ln := startTestTCPListenerWithLimiter(t, cfg, authEchoHandler, nil, limiter)
	addr := ln.Addr().String()

	var refusedMessage string
	for attempt := 0; attempt < peerBudget; attempt++ {
		conn, err := dialFromLoopbackAlias(t, addr, "127.0.0.2")
		if err != nil {
			t.Fatalf("unauthenticated dial %d from 127.0.0.2: %v", attempt+1, err)
		}
		// An admitted connection sends nothing and is answered with nothing;
		// a refused one is answered at once with an error frame.
		_ = conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
		if answer, err := DecodeFrame(bufio.NewReader(conn)); err == nil && answer.Opcode == OpError {
			_, refusedMessage = wireErrorOf(t, answer)
		}
	}

	tlsCfg, err := MakeClientTLSConfig("", "", TrustPolicy{Insecure: &InsecureTrust{Reason: "transport pre-auth test"}}, addr)
	if err != nil {
		t.Fatalf("client TLS config: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	member, err := NewTCPPeerClient(ctx, addr, tlsCfg, "test-token", 1)
	if err != nil {
		_, peers := limiter.Counts()
		t.Fatalf("a member's dial was refused while one other address held unauthenticated connections (%d of %d peer slots in use): %v",
			peers, peerBudget, err)
	}
	t.Cleanup(func() { _ = member.Close() })

	if !strings.Contains(refusedMessage, "HOARDDB_MAX_PEER_CONNS") {
		t.Errorf("the refusal of an address over its unauthenticated share was %q; want it to name HOARDDB_MAX_PEER_CONNS", refusedMessage)
	}
}

// TestUnauthenticatedInternodeConnectionIsClosedAtThePreAuthDeadline keeps an
// unauthenticated internode connection busy with PING, which is answered
// without AUTH, and requires the listener to close it at the pre-AUTH
// deadline anyway and give its peer slot back.
func TestUnauthenticatedInternodeConnectionIsClosedAtThePreAuthDeadline(t *testing.T) {
	limiter := NewConnLimiter(64, 8)
	// The pre-AUTH deadline is never longer than the idle timeout, so a short
	// idle timeout makes it short enough to watch.
	cfg := testTCPConfig(TCPConfig{PeerMode: true, IdleTimeout: time.Second})
	ln := startTestTCPListenerWithLimiter(t, cfg, authEchoHandler, nil, limiter)
	conn := dialTestTCP(t, ln.Addr().String())
	reader := bufio.NewReader(conn)

	connected := time.Now()
	const watchFor = 4 * time.Second
	var closedAfter time.Duration
	for time.Since(connected) < watchFor {
		_ = conn.SetDeadline(time.Now().Add(time.Second))
		if _, err := conn.Write(NewFrame(OpPing, nil).Encode()); err != nil {
			closedAfter = time.Since(connected)
			break
		}
		if _, err := DecodeFrame(reader); err != nil {
			closedAfter = time.Since(connected)
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if closedAfter == 0 {
		t.Fatalf("an unauthenticated internode connection sending PING every 200ms was still served after %s; the pre-AUTH deadline is %s",
			watchFor, cfg.IdleTimeout)
	}
	if closedAfter > 3*time.Second {
		t.Errorf("the unauthenticated connection was closed after %s; want close to the %s pre-AUTH deadline", closedAfter, cfg.IdleTimeout)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, peers := limiter.Counts(); peers == 0 {
			break
		}
		if time.Now().After(deadline) {
			_, peers := limiter.Counts()
			t.Fatalf("%d peer slots still held after the unauthenticated connection was closed", peers)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
