package cluster

import (
	"errors"
	"net"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/transport"
)

func stepTwoBRaftHello() transport.Handshake {
	return transport.Handshake{Channel: transport.HandshakeChannelRaft, ClusterID: [16]byte{9}, NodeID: "n2", Incarnation: 2}
}

func stepTwoBClosedWithin(t *testing.T, conn net.Conn, d time.Duration) bool {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(d)); err != nil {
		t.Fatal(err)
	}
	_, err := conn.Read(make([]byte, 1))
	var timeout net.Error
	return !errors.As(err, &timeout) || !timeout.Timeout()
}

// TestTheRaftChannelHoldsOnlyMaxConnsInboundConnections: peers that connect and never finish
// their handshake, or that are admitted and never used, count against MaxConns; the
// connection past it is closed at accept, and a slot frees when one leaves.
// [GUARD: do not count inbound connections]
func TestTheRaftChannelHoldsOnlyMaxConnsInboundConnections(t *testing.T) {
	_, addr := streamUnderTestLimits(t, RaftLimits{MaxConns: 2})
	dial := func() net.Conn {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		return conn
	}
	first, second, third := dial(), dial(), dial()
	if !stepTwoBClosedWithin(t, third, 2*time.Second) {
		t.Fatal("a third inbound connection past MaxConns 2 stayed open")
	}
	if stepTwoBClosedWithin(t, second, 150*time.Millisecond) {
		t.Fatal("a connection inside the cap was closed")
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if !stepTwoBClosedWithin(t, dial(), 200*time.Millisecond) {
			return
		}
	}
	t.Fatal("no slot freed after an inbound connection left")
}

// TestTheRaftChannelDoesNotCountItsOwnDialsAgainstTheCap: the node's outbound connections to
// peers are not inbound load. [GUARD: count every tracked connection]
func TestTheRaftChannelDoesNotCountItsOwnDialsAgainstTheCap(t *testing.T) {
	layer, addr := streamUnderTestLimits(t, RaftLimits{MaxConns: 1})
	peer, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = peer.Close() })
	go func() {
		for {
			conn, err := peer.Accept()
			if err != nil {
				return
			}
			go func() { _, _ = conn.Read(make([]byte, 64)) }()
		}
	}()
	layer.dial = func(string, time.Duration) (net.Conn, error) { return net.Dial("tcp", peer.Addr().String()) }
	for range 3 {
		conn, err := layer.Dial("peer", time.Second)
		if err != nil {
			t.Fatalf("an outbound dial was refused by the inbound cap: %v", err)
		}
		t.Cleanup(func() { _ = conn.Close() })
	}
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if stepTwoBClosedWithin(t, conn, 200*time.Millisecond) {
		t.Fatal("the one inbound connection was closed because of outbound ones")
	}
}

// TestTheRaftChannelClosesAnAdmittedConnectionThatGoesQuiet: after the handshake the peer
// is silent; the Raft library's read fails at the idle bound instead of waiting forever,
// and a peer that keeps sending keeps its connection. [GUARD: clear the deadline instead
// of arming it]
func TestTheRaftChannelClosesAnAdmittedConnectionThatGoesQuiet(t *testing.T) {
	layer, addr := streamUnderTestLimits(t, RaftLimits{Idle: 300 * time.Millisecond})
	peer := dialWithHandshake(t, addr, stepTwoBRaftHello())
	got := make(chan net.Conn, 1)
	go func() {
		conn, err := layer.Accept()
		if err == nil {
			got <- conn
		}
	}()
	var served net.Conn
	select {
	case served = <-got:
	case <-time.After(3 * time.Second):
		t.Fatal("an admitted peer was never handed to Raft")
	}
	defer func() { _ = served.Close() }() // as the library does, so the layer's Close does not wait for it
	// A peer that keeps talking keeps its connection past several idle periods.
	for range 4 {
		time.Sleep(150 * time.Millisecond)
		if _, err := peer.Write([]byte("x")); err != nil {
			t.Fatal(err)
		}
		if _, err := served.Read(make([]byte, 1)); err != nil {
			t.Fatalf("a peer that kept sending lost its connection: %v", err)
		}
	}
	readErr := make(chan error, 1)
	go func() {
		_, err := served.Read(make([]byte, 1))
		readErr <- err
	}()
	select {
	case err := <-readErr:
		var timeout net.Error
		if !errors.As(err, &timeout) || !timeout.Timeout() {
			t.Fatalf("a silent admitted peer's read gave %v, want an idle timeout", err)
		}
	case <-time.After(2 * time.Second):
		_ = served.Close() // frees the blocked read so the goroutine ends
		t.Fatal("a silent admitted peer's read was still waiting after 2 s; want the 300 ms idle bound")
	}
}

func TestRaftLimitDefaults(t *testing.T) {
	got := RaftLimits{}.withDefaults()
	if got.MaxConns != 64 || got.Idle != 2*time.Minute {
		t.Fatalf("defaults %+v, want 64 connections and a two-minute idle bound", got)
	}
}
