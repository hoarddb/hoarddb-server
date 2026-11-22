package transport

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

// TestOnlyAConnectionThatNeverAnsweredIsClosedAfterTheHandshake: a peer that closes before it has
// said anything may run another build, and says so with ErrClosedAfterHandshake; a connection that
// answered and later died (a crashed peer) does not. Scratch proof: drop the served check in
// exchange and the second half is red.
func TestOnlyAConnectionThatNeverAnsweredIsClosedAfterTheHandshake(t *testing.T) {
	closer, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = closer.Close() })
	go func() {
		for {
			conn, err := closer.Accept()
			if err != nil {
				return
			}
			_ = conn.Close()
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	msg := Message{Env: Envelope{Version: EnvelopeVersion, Kind: KindProbe}, Body: make([]byte, 8)}
	fresh := NewChannelClient(closer.Addr().String(), ChannelControl, testHello("caller"))
	t.Cleanup(func() { _ = fresh.Close() })
	if _, err := fresh.Call(ctx, msg); !errors.Is(err, ErrClosedAfterHandshake) {
		t.Fatalf("a peer that closed before answering: %v", err)
	}

	server := startChannelServer(t, ChannelControl, func(context.Context, Handshake, Message) (*Message, error) {
		reply := Message{Env: Envelope{Version: EnvelopeVersion, Kind: KindProbeReply}, Body: make([]byte, 17)}
		return &reply, nil
	})
	client := NewChannelClient(server.Addr().String(), ChannelControl, testHello("caller"))
	t.Cleanup(func() { _ = client.Close() })
	if _, err := client.Call(ctx, msg); err != nil {
		t.Fatal(err)
	}
	_ = server.Close()
	_, err = client.Call(ctx, msg)
	if err == nil {
		t.Fatal("a call to a closed server succeeded")
	}
	if errors.Is(err, ErrClosedAfterHandshake) {
		t.Fatalf("a connection that had answered is reported as closed after the handshake: %v", err)
	}
}
