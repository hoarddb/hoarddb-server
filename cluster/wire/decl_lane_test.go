package wire

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster/peer"
	"github.com/hoarddb/hoarddb-server/transport"
)

// declLaneOwner is an owner whose forwarded writes block until released and whose stored
// reads answer at once, as serveStoredRead does (one seek, no gate).
func declLaneOwner(t *testing.T, release <-chan struct{}) (addr string, blocked func() int) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	inFlight := 0
	owner, err := peer.New(peer.Config{
		Self: "n2", Listener: ln, Addr: func(string) (string, bool) { return "", false },
		Hello: func() transport.Handshake { return transport.Handshake{NodeID: "n2", Incarnation: 1} },
		Data: func(ctx context.Context, _ transport.Handshake, req transport.Message) (*transport.Message, error) {
			env := transport.Envelope{Version: transport.EnvelopeVersion, Kind: req.Env.Kind}
			if req.Env.Kind == transport.KindForwardRead {
				raw, err := EncodeForwardReadStored(nil, false)
				env.Kind = transport.KindForwardReadReply
				return &transport.Message{Env: env, Body: raw}, err
			}
			mu.Lock()
			inFlight++
			mu.Unlock()
			select {
			case <-release:
			case <-ctx.Done():
			}
			mu.Lock()
			inFlight--
			mu.Unlock()
			env.Kind = transport.KindForwardWriteReply
			return &transport.Message{Env: env, Body: EncodeForwardWriteAnswer(WriteAnswer{})}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.Close() })
	return owner.Addr().String(), func() int { mu.Lock(); defer mu.Unlock(); return inFlight }
}

// TestAColdDeclarationFetchDoesNotQueueBehindForwardedWrites is BUG-0770: with every
// connection of the forward lane to the owner held by a forwarded write, a declaration read
// still reaches the owner and is answered within its deadline.
func TestAColdDeclarationFetchDoesNotQueueBehindForwardedWrites(t *testing.T) {
	release := make(chan struct{})
	addr, blocked := declLaneOwner(t, release)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	hello := func() transport.Handshake { return transport.Handshake{NodeID: "n1", Incarnation: 1} }
	caller, err := peer.New(peer.Config{
		Self: "n1", Listener: ln, Addr: func(string) (string, bool) { return addr, true }, Hello: hello,
		BackoffBase: 20 * time.Millisecond, BackoffMax: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = caller.Close() })
	links := &peerLinks{self: "n1", net: caller, hello: hello}

	var writes sync.WaitGroup
	for i := 0; i < peer.ForwardConns+4; i++ {
		writes.Add(1)
		go func() {
			defer writes.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			_, _ = links.call(ctx, "n2", peer.LaneForward, links.envelope(transport.KindForwardWrite, 1, 7, 1), []byte{0}, 0)
		}()
	}
	t.Cleanup(func() { close(release); writes.Wait() })
	deadline := time.Now().Add(5 * time.Second)
	for blocked() < peer.ForwardConns {
		if time.Now().After(deadline) {
			t.Fatalf("only %d forwarded writes reached the owner, want the lane's %d connections all busy", blocked(), peer.ForwardConns)
		}
		time.Sleep(time.Millisecond)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, found, err := links.readStored(ctx, "n2", DeclarationRead{DBID: 1, Slot: 7, EncodedID: []byte("outbox")}); err != nil || found {
		t.Fatalf("the declaration read behind %d busy forwarded writes: found %v, err %v after %s", peer.ForwardConns, found, err, time.Since(start))
	}
}
