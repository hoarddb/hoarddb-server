package transport

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

func muxTestHello(id string) func() Handshake {
	return func() Handshake { return Handshake{NodeID: id, Incarnation: 1} }
}

func TestPeerMuxRoutesEachChannelToItsOwnServer(t *testing.T) {
	base, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	mux := NewPeerMux(base)
	t.Cleanup(func() { _ = mux.Close() })
	echo := func(tag byte) ChannelHandler {
		return func(_ context.Context, from Handshake, req Message) (*Message, error) {
			return &Message{Env: Envelope{Version: EnvelopeVersion, Kind: req.Env.Kind}, Body: append([]byte{tag}, from.NodeID...)}, nil
		}
	}
	data := ServeChannel(mux.Listener(ChannelData), ChannelData, nil, echo('d'))
	t.Cleanup(func() { _ = data.Close() })
	control := ServeChannel(mux.Listener(ChannelControl), ChannelControl, nil, echo('c'))
	t.Cleanup(func() { _ = control.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, tc := range []struct {
		ch   Channel
		kind byte
		tag  byte
	}{{ChannelData, 0x03, 'd'}, {ChannelControl, 0x09, 'c'}} {
		client := NewChannelClient(mux.Addr().String(), tc.ch, muxTestHello("n9"))
		reply, err := client.Call(ctx, Message{Env: Envelope{Version: EnvelopeVersion, Kind: tc.kind}})
		if err != nil {
			t.Fatalf("%s call: %v", tc.ch, err)
		}
		if len(reply.Body) == 0 || reply.Body[0] != tc.tag || string(reply.Body[1:]) != "n9" {
			t.Fatalf("%s channel was answered by %q, want the %c server for n9", tc.ch, reply.Body, tc.tag)
		}
		if err := client.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestPeerMuxDropsAConnectionThatIsNotAHoardDBNode(t *testing.T) {
	base, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	mux := NewPeerMux(base)
	t.Cleanup(func() { _ = mux.Close() })
	conn, err := net.Dial("tcp", mux.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if _, err := conn.Write([]byte("GET / HTTP/1.1\r\n\r\n")); err != nil {
		t.Fatal(err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if n, err := conn.Read(make([]byte, 1)); err == nil {
		t.Fatalf("the mux answered %d bytes to a stranger instead of closing", n)
	}
}

// TestAChannelServerClosesASilentConnectionAndCapsConnections: an authenticated peer that
// goes quiet is closed after the idle bound, and a connection past the cap is closed at
// accept, so a pinned peer cannot hold descriptors and goroutines forever.
func TestAChannelServerClosesASilentConnectionAndCapsConnections(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := ServeChannelWithLimits(ln, ChannelData, nil, func(context.Context, Handshake, Message) (*Message, error) { return nil, nil },
		ChannelLimits{MaxConns: 1, Idle: 300 * time.Millisecond})
	t.Cleanup(func() { _ = server.Close() })
	open := func() net.Conn {
		conn, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		raw, err := Handshake{Channel: HandshakeChannelData, NodeID: "n9", Incarnation: 1}.Encode()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Write(raw); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		return conn
	}
	closedWithin := func(conn net.Conn, d time.Duration) bool {
		if err := conn.SetReadDeadline(time.Now().Add(d)); err != nil {
			t.Fatal(err)
		}
		_, err := conn.Read(make([]byte, 1))
		var ne net.Error
		return !errors.As(err, &ne) || !ne.Timeout()
	}
	first := open()
	second := open()
	if !closedWithin(second, 150*time.Millisecond) {
		t.Fatal("a second connection past MaxConns 1 stayed open")
	}
	if !closedWithin(first, 2*time.Second) {
		t.Fatal("a silent authenticated connection was never closed")
	}
}

// stepTwoBClosedWithin reports whether the far side closed it within d.
func stepTwoBClosedWithin(t *testing.T, conn net.Conn, d time.Duration) bool {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(d)); err != nil {
		t.Fatal(err)
	}
	_, err := conn.Read(make([]byte, 1))
	var timeout net.Error
	return !errors.As(err, &timeout) || !timeout.Timeout()
}

func stepTwoBDial(t *testing.T, addr string) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// TestPeerMuxHoldsOnlyMaxPendingUnroutedConnections: callers that open connections and say
// nothing hold the mux's routing step. Past MaxPending the mux closes at accept, and a slot
// frees when one of them leaves. [GUARD: take no token in acceptLoop]
func TestPeerMuxHoldsOnlyMaxPendingUnroutedConnections(t *testing.T) {
	base, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	mux := NewPeerMuxWithLimits(base, PeerMuxLimits{MaxPending: 2, PeekTimeout: 10 * time.Second})
	t.Cleanup(func() { _ = mux.Close() })
	first := stepTwoBDial(t, mux.Addr().String())
	second := stepTwoBDial(t, mux.Addr().String())
	third := stepTwoBDial(t, mux.Addr().String())
	if !stepTwoBClosedWithin(t, third, 2*time.Second) {
		t.Fatal("a third silent connection past MaxPending 2 stayed open")
	}
	if stepTwoBClosedWithin(t, second, 150*time.Millisecond) {
		t.Fatal("a connection inside the cap was closed")
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	var fourth net.Conn
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		fourth = stepTwoBDial(t, mux.Addr().String())
		if !stepTwoBClosedWithin(t, fourth, 200*time.Millisecond) {
			return
		}
	}
	t.Fatal("no slot freed after a pending connection left")
}

// TestPeerMuxPeekTimeoutIsTheConfiguredOne: a silent connection is dropped when
// PeekTimeout says, not after a fixed five seconds. [GUARD: use the constant]
func TestPeerMuxPeekTimeoutIsTheConfiguredOne(t *testing.T) {
	base, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	mux := NewPeerMuxWithLimits(base, PeerMuxLimits{PeekTimeout: 200 * time.Millisecond})
	t.Cleanup(func() { _ = mux.Close() })
	if !stepTwoBClosedWithin(t, stepTwoBDial(t, mux.Addr().String()), 2*time.Second) {
		t.Fatal("a silent connection outlived a 200 ms peek timeout")
	}
}

// TestChannelFiveIsRoutedAndChannelSixIsRefused: the identity channel has its own server on the
// shared port, and a handshake that names a channel past it is closed by the mux, not routed.
func TestChannelFiveIsRoutedAndChannelSixIsRefused(t *testing.T) {
	base, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	mux := NewPeerMux(base)
	t.Cleanup(func() { _ = mux.Close() })
	identity := ServeChannel(mux.Listener(ChannelIdentity), ChannelIdentity, nil,
		func(_ context.Context, from Handshake, req Message) (*Message, error) {
			return &Message{Env: Envelope{Version: EnvelopeVersion, Kind: KindIdentityReply}, Body: []byte(from.NodeID)}, nil
		})
	t.Cleanup(func() { _ = identity.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client := NewChannelClient(mux.Addr().String(), ChannelIdentity, muxTestHello("n9"))
	reply, err := client.Call(ctx, Message{Env: Envelope{Version: EnvelopeVersion, Kind: KindIdentityRequest}, Body: make([]byte, 8)})
	if err != nil {
		t.Fatalf("identity call: %v", err)
	}
	if string(reply.Body) != "n9" || reply.Env.Kind != KindIdentityReply {
		t.Fatalf("identity channel answered kind 0x%02x %q, want 0x1c and n9", reply.Env.Kind, reply.Body)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}

	raw, err := Handshake{Channel: HandshakeChannelIdentity, NodeID: "n9", Incarnation: 1}.Encode()
	if err != nil {
		t.Fatal(err)
	}
	raw[5] = HandshakeChannelIdentity + 1
	conn, err := net.Dial("tcp", mux.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if _, err := conn.Write(raw); err != nil {
		t.Fatal(err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if n, err := conn.Read(make([]byte, 1)); err == nil {
		t.Fatalf("the mux answered %d bytes on channel 6 instead of closing", n)
	}
	if _, err := (Handshake{Channel: HandshakeChannelIdentity + 1, NodeID: "n9"}).Encode(); err == nil {
		t.Fatal("Encode accepted channel 6")
	}
}
