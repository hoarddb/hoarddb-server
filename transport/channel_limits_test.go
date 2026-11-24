package transport

import (
	"bytes"
	"context"
	"errors"
	"net"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func stepTwoBServe(t *testing.T, limits ChannelLimits, handle ChannelHandler) *ChannelServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := ServeChannelWithLimits(ln, ChannelData, nil, handle, limits)
	t.Cleanup(func() { _ = server.Close() })
	return server
}

func stepTwoBHello(t *testing.T, addr string) net.Conn {
	t.Helper()
	conn := stepTwoBDial(t, addr)
	raw, err := Handshake{Channel: HandshakeChannelData, NodeID: "n9", Incarnation: 1}.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write(raw); err != nil {
		t.Fatal(err)
	}
	return conn
}

func (s *ChannelServer) heldConns() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.conns)
}

func stepTwoBAwait(t *testing.T, what string, within time.Duration, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("%s did not happen within %s", what, within)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestAChannelServerBoundsAFrameThatTrickles: a peer that sends the first byte of a frame
// and then stops is closed after FrameTimeout, not after the much longer Idle.
// [GUARD: arm only the Idle deadline]
func TestAChannelServerBoundsAFrameThatTrickles(t *testing.T) {
	server := stepTwoBServe(t, ChannelLimits{Idle: time.Minute, FrameTimeout: 300 * time.Millisecond},
		func(context.Context, Handshake, Message) (*Message, error) { return nil, nil })
	conn := stepTwoBHello(t, server.Addr().String())
	if _, err := conn.Write([]byte{ProtocolVersion}); err != nil {
		t.Fatal(err)
	}
	if !stepTwoBClosedWithin(t, conn, 3*time.Second) {
		t.Fatal("a half-sent frame held the connection for longer than FrameTimeout")
	}
}

// TestAChannelServerLetsASilentPeerKeepItsConnectionUntilIdle: the frame deadline is not the
// idle one. A peer that has sent nothing since its handshake is not cut by FrameTimeout.
func TestAChannelServerLetsASilentPeerKeepItsConnectionUntilIdle(t *testing.T) {
	server := stepTwoBServe(t, ChannelLimits{Idle: 5 * time.Second, FrameTimeout: 100 * time.Millisecond},
		func(context.Context, Handshake, Message) (*Message, error) { return nil, nil })
	conn := stepTwoBHello(t, server.Addr().String())
	if stepTwoBClosedWithin(t, conn, 500*time.Millisecond) {
		t.Fatal("a connection with no frame begun was closed by the frame timeout")
	}
}

// TestAChannelServerReleasesAConnectionWhoseReplyIsNeverRead: the peer asks for a large
// reply and never reads it; the write deadline ends the connection and frees its slot.
// [GUARD: no write deadline]
func TestAChannelServerReleasesAConnectionWhoseReplyIsNeverRead(t *testing.T) {
	server := stepTwoBServe(t, ChannelLimits{Idle: time.Minute, FrameTimeout: 300 * time.Millisecond},
		func(context.Context, Handshake, Message) (*Message, error) {
			return &Message{Env: Envelope{Version: EnvelopeVersion, Kind: 0x03}, Body: bytes.Repeat([]byte{1}, 30<<20)}, nil
		})
	conn := stepTwoBHello(t, server.Addr().String())
	if err := WriteFrame(conn, NewFrame(OpReplPush, EncodeEnvelope(Envelope{Version: EnvelopeVersion, Kind: 0x03}))); err != nil {
		t.Fatal(err)
	}
	stepTwoBAwait(t, "the server to hold the connection", 2*time.Second, func() bool { return server.heldConns() == 1 })
	stepTwoBAwait(t, "the server to give up on a reply nobody reads", 5*time.Second, func() bool { return server.heldConns() == 0 })
}

// TestAChannelServerRefusesRequestsPastMaxInFlight: the second request while the first is
// still being handled gets an overloaded error, the connection stays usable, and the
// first completes. [GUARD: no semaphore]
func TestAChannelServerRefusesRequestsPastMaxInFlight(t *testing.T) {
	entered, release := make(chan struct{}, 4), make(chan struct{})
	var releaseOnce sync.Once
	freeHandlers := func() { releaseOnce.Do(func() { close(release) }) }
	server := stepTwoBServe(t, ChannelLimits{MaxInFlight: 1},
		func(context.Context, Handshake, Message) (*Message, error) {
			entered <- struct{}{}
			<-release
			return nil, nil
		})
	// Registered after the server, so it runs before the server's Close: a failing run must
	// not leave the handler blocked, which would hang that Close.
	t.Cleanup(freeHandlers)
	hello := func() Handshake { return Handshake{NodeID: "n9", Incarnation: 1} }
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	first := NewChannelClient(server.Addr().String(), ChannelData, hello)
	second := NewChannelClient(server.Addr().String(), ChannelData, hello)
	t.Cleanup(func() { _ = first.Close(); _ = second.Close() })
	firstDone := make(chan error, 1)
	go func() {
		_, err := first.Call(ctx, Message{Env: Envelope{Version: EnvelopeVersion, Kind: 0x03}})
		firstDone <- err
	}()
	<-entered
	_, err := second.Call(ctx, Message{Env: Envelope{Version: EnvelopeVersion, Kind: 0x03}})
	var refused *WireError
	if !errors.As(err, &refused) || refused.Code != ErrOverloaded {
		t.Fatalf("a request past MaxInFlight 1 got %v, want an overloaded wire error", err)
	}
	freeHandlers()
	if err := <-firstDone; err != nil {
		t.Fatalf("the request inside the limit failed: %v", err)
	}
	if _, err := second.Call(ctx, Message{Env: Envelope{Version: EnvelopeVersion, Kind: 0x03}}); err != nil {
		t.Fatalf("the refused peer's retry on the same connection failed: %v", err)
	}
}

// TestAChannelServerFreesItsSlotBeforeItWritesTheReply (BUG-0921): MaxInFlight limits requests
// being handled, so a peer that never reads its large reply does not keep the only slot while
// the server is blocked writing it. The second request is refused while the handler runs, so it
// is retried until the first handler has returned; before the fix the blocked write keeps the
// slot, and every retry is refused until the test gives up.
func TestAChannelServerFreesItsSlotBeforeItWritesTheReply(t *testing.T) {
	var calls atomic.Int32
	server := stepTwoBServe(t, ChannelLimits{MaxInFlight: 1, FrameTimeout: time.Minute},
		func(context.Context, Handshake, Message) (*Message, error) {
			if calls.Add(1) == 1 {
				return &Message{Env: Envelope{Version: EnvelopeVersion, Kind: 0x03}, Body: bytes.Repeat([]byte{1}, 30<<20)}, nil
			}
			return nil, nil
		})
	stalled := stepTwoBHello(t, server.Addr().String())
	if err := WriteFrame(stalled, NewFrame(OpReplPush, EncodeEnvelope(Envelope{Version: EnvelopeVersion, Kind: 0x03}))); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	second := NewChannelClient(server.Addr().String(), ChannelData, func() Handshake { return Handshake{NodeID: "n8", Incarnation: 1} })
	t.Cleanup(func() { _ = second.Close() })
	for {
		_, err := second.Call(ctx, Message{Env: Envelope{Version: EnvelopeVersion, Kind: 0x03}})
		var refused *WireError
		if err == nil {
			return
		}
		if !errors.As(err, &refused) || refused.Code != ErrOverloaded {
			t.Fatalf("the second request failed with %v, want it admitted", err)
		}
		select {
		case <-ctx.Done():
			t.Fatal("the second request was refused as overloaded for as long as the first reply was being written")
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// TestChannelLimitDefaults pins the defaults the config package's cluster keys repeat.
func TestChannelLimitDefaults(t *testing.T) {
	server := stepTwoBServe(t, ChannelLimits{}, func(context.Context, Handshake, Message) (*Message, error) { return nil, nil })
	want := ChannelLimits{MaxConns: 256, Idle: 2 * time.Minute, MaxInFlight: 256, FrameTimeout: 10 * time.Second, PerPeerInFlight: 64, PriorityInFlight: 64}
	if !reflect.DeepEqual(server.limits, want) {
		t.Fatalf("defaults %+v, want %+v", server.limits, want)
	}
}

// TestAPriorityKindIsNeverRefusedForWantOfASharedSlot (pi liveness): slow requests from
// members fill the shared pool and one member's share of it, and a lease request still gets
// through on its own pool. [GUARD: send the priority kind through the shared pool; drop the
// per-peer share]
func TestAPriorityKindIsNeverRefusedForWantOfASharedSlot(t *testing.T) {
	const slow, lease = byte(0x03), byte(0x05)
	entered, release := make(chan struct{}, 8), make(chan struct{})
	var releaseOnce sync.Once
	server := stepTwoBServe(t, ChannelLimits{MaxInFlight: 3, PerPeerInFlight: 1, Priority: func(kind byte) bool { return kind == lease }},
		func(_ context.Context, _ Handshake, req Message) (*Message, error) {
			if req.Env.Kind == slow {
				entered <- struct{}{}
				<-release
			}
			return nil, nil
		})
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client := func(node string) *ChannelClient {
		c := NewChannelClient(server.Addr().String(), ChannelData, func() Handshake { return Handshake{NodeID: node, Incarnation: 1} })
		t.Cleanup(func() { _ = c.Close() })
		return c
	}
	call := func(c *ChannelClient, kind byte) error {
		_, err := c.Call(ctx, Message{Env: Envelope{Version: EnvelopeVersion, Kind: kind}})
		return err
	}
	overloaded := func(err error) bool {
		var refused *WireError
		return errors.As(err, &refused) && refused.Code == ErrOverloaded
	}
	for _, node := range []string{"n1", "n2"} {
		c := client(node)
		go func() { _ = call(c, slow) }()
		<-entered
	}
	// One member's second slow request is over its share; a third member finds the pool full.
	if err := call(client("n1"), slow); !overloaded(err) {
		t.Fatalf("a second slow request from one member: %v, want overloaded", err)
	}
	third := client("n3")
	go func() { _ = call(third, slow) }()
	<-entered
	if err := call(client("n4"), slow); !overloaded(err) {
		t.Fatalf("a slow request with the shared pool full: %v, want overloaded", err)
	}
	if err := call(client("n1"), lease); err != nil {
		t.Fatalf("a lease request with every shared slot taken by slow ones: %v", err)
	}
}
