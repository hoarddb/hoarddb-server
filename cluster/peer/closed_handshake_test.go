package peer

import (
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/transport"
)

func TestTheDialerLogsAHandshakeThatIsClosed(t *testing.T) {
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
	own, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var warnings []string
	clock := time.Unix(1_700_000_000, 0)
	network, err := New(Config{
		Self: "n1", Listener: own,
		Hello: func() transport.Handshake {
			return transport.Handshake{NodeID: "n1", Incarnation: 1, Min: 1, Max: 1, Writes: 1}
		},
		Addr: func(string) (string, bool) { return closer.Addr().String(), true },
		Now:  func() time.Time { mu.Lock(); defer mu.Unlock(); return clock },
		Warn: func(m string) { mu.Lock(); warnings = append(warnings, m); mu.Unlock() },
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = network.Close() })
	call := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = network.Ask(ctx, "n2", closer.Addr().String(), transport.ChannelIdentity, transport.Message{Env: transport.Envelope{Version: transport.EnvelopeVersion, Kind: transport.KindIdentityRequest}})
	}
	for i := 0; i < 3; i++ {
		call()
	}
	mu.Lock()
	if len(warnings) != 1 || !strings.Contains(warnings[0], "peer n2 closed the connection right after the handshake: it may run another build") {
		t.Fatalf("warnings %q: want exactly one naming n2 (rate limited)", warnings)
	}
	clock = clock.Add(2 * time.Minute)
	mu.Unlock()
	call()
	mu.Lock()
	defer mu.Unlock()
	if len(warnings) != 2 {
		t.Fatalf("%d warnings after the minute, want 2", len(warnings))
	}
}

func TestTheIdentityDialSendsZeroLevelBytes(t *testing.T) {
	seen := make(chan transport.Handshake, 2)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			if hello, err := transport.ReadHandshake(conn); err == nil {
				seen <- hello
			}
			_ = conn.Close()
		}
	}()
	own, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	network, err := New(Config{
		Self: "n1", Listener: own,
		Hello: func() transport.Handshake {
			return transport.Handshake{NodeID: "n1", Incarnation: 1, Min: 1, Max: 1, Writes: 1}
		},
		Addr: func(string) (string, bool) { return ln.Addr().String(), true },
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = network.Close() })
	for _, ch := range []transport.Channel{transport.ChannelIdentity, transport.ChannelData} {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		msg := transport.Message{Env: transport.Envelope{Version: transport.EnvelopeVersion}}
		if ch == transport.ChannelIdentity {
			_, _ = network.Ask(ctx, "n2", ln.Addr().String(), ch, msg)
		} else {
			_, _ = network.Call(ctx, "n2", ch, 0, msg)
		}
		cancel()
		hello := <-seen
		zero := hello.Min == 0 && hello.Max == 0 && hello.Writes == 0
		if ch == transport.ChannelIdentity && !zero {
			t.Fatalf("the identity dial sent range %d-%d (writes %d); it must send zeros so a build from before the range does not close it silently", hello.Min, hello.Max, hello.Writes)
		}
		if ch == transport.ChannelData && (hello.Min != 1 || hello.Max != 1 || hello.Writes != 1) {
			t.Fatalf("the data dial sent range %d-%d (writes %d), want the build's 1-1 (writes 1)", hello.Min, hello.Max, hello.Writes)
		}
	}
}
