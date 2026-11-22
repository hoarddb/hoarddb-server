package peer

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"net"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/transport"
)

type peerIdentity struct {
	cfg *tls.Config
	fp  string
}

func peerTestIdentity(t *testing.T) peerIdentity {
	t.Helper()
	cfg, fp, err := transport.MakeServerTLSConfig(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return peerIdentity{cfg: cfg, fp: fp}
}

type peerNode struct {
	net  *Network
	addr string
}

// peerTestNode serves handler as id with pinned mutual TLS; pins are the fingerprints
// it trusts. addrs resolves member ids for the node's own calls.
func peerTestNode(t *testing.T, id string, who peerIdentity, pins []string, addrs func(string) (string, bool), tweak func(*Config)) peerNode {
	t.Helper()
	trust, err := NewTrust(who.cfg, pins)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{
		Self: id, Listener: trust.Listener(ln), Addr: addrs,
		Data: func(_ context.Context, from transport.Handshake, req transport.Message) (*transport.Message, error) {
			return &transport.Message{Env: transport.Envelope{Version: transport.EnvelopeVersion, Kind: req.Env.Kind}, Body: append([]byte(from.NodeID+":"), req.Body...)}, nil
		},
		Hello: func() transport.Handshake { return transport.Handshake{NodeID: id, Incarnation: 1} },
		TLS:   func(_, addr string) (*tls.Config, error) { return trust.ClientConfig(addr) }, BackoffBase: 100 * time.Millisecond, BackoffMax: time.Second,
	}
	if tweak != nil {
		tweak(&cfg)
	}
	network, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = network.Close() })
	return peerNode{net: network, addr: network.Addr().String()}
}

func peerCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func peerWrite(kind byte, body string) transport.Message {
	return transport.Message{Env: transport.Envelope{Version: transport.EnvelopeVersion, Kind: kind}, Body: []byte(body)}
}

func TestPeerCallRoundTripsOverPinnedTLS(t *testing.T) {
	idA, idB := peerTestIdentity(t), peerTestIdentity(t)
	addrs := map[string]string{}
	lookup := func(id string) (string, bool) { addr, ok := addrs[id]; return addr, ok }
	a := peerTestNode(t, "a", idA, []string{idB.fp}, lookup, nil)
	b := peerTestNode(t, "b", idB, []string{idA.fp}, lookup, nil)
	addrs["a"], addrs["b"] = a.addr, b.addr

	reply, err := a.net.Call(peerCtx(t), "b", transport.ChannelData, LaneMain, peerWrite(0x03, "hello"))
	if err != nil {
		t.Fatalf("a to b: %v", err)
	}
	if string(reply.Body) != "a:hello" {
		t.Fatalf("b answered %q, want it to name the caller: a:hello", reply.Body)
	}
	reply, err = b.net.Call(peerCtx(t), "a", transport.ChannelData, LaneForward, peerWrite(0x03, "back"))
	if err != nil || string(reply.Body) != "b:back" {
		t.Fatalf("b to a on the forward lane: %q, %v", reply.Body, err)
	}
}

// TestAnUnpinnedPeerIsRefusedAndNothingRuns: c is not in b's pinned list, so its call
// fails at TLS and b's handler never runs.
func TestAnUnpinnedPeerIsRefusedAndNothingRuns(t *testing.T) {
	idB, idC := peerTestIdentity(t), peerTestIdentity(t)
	addrs := map[string]string{}
	lookup := func(id string) (string, bool) { addr, ok := addrs[id]; return addr, ok }
	var ran atomic.Int32
	b := peerTestNode(t, "b", idB, []string{}, lookup, func(cfg *Config) {
		cfg.Data = func(context.Context, transport.Handshake, transport.Message) (*transport.Message, error) {
			ran.Add(1)
			return nil, nil
		}
	})
	c := peerTestNode(t, "c", idC, []string{idB.fp}, lookup, nil)
	addrs["b"] = b.addr
	if _, err := c.net.Call(peerCtx(t), "b", transport.ChannelData, LaneMain, peerWrite(0x03, "x")); err == nil {
		t.Fatal("an unpinned node's call was answered")
	}
	if ran.Load() != 0 {
		t.Fatal("the handler ran for an unpinned peer")
	}
}

// TestAServerWhoseKeyIsNotPinnedIsRefused: the pin works in the dialing direction too.
func TestAServerWhoseKeyIsNotPinnedIsRefused(t *testing.T) {
	idA, idB, idOther := peerTestIdentity(t), peerTestIdentity(t), peerTestIdentity(t)
	addrs := map[string]string{}
	lookup := func(id string) (string, bool) { addr, ok := addrs[id]; return addr, ok }
	a := peerTestNode(t, "a", idA, []string{idOther.fp}, lookup, nil)
	b := peerTestNode(t, "b", idB, []string{idA.fp}, lookup, nil)
	addrs["b"] = b.addr
	_, err := a.net.Call(peerCtx(t), "b", transport.ChannelData, LaneMain, peerWrite(0x03, "x"))
	if err == nil || !strings.Contains(err.Error(), "pinned") {
		t.Fatalf("dialing a server whose fingerprint is not pinned: %v, want a refusal naming the pin", err)
	}
}

// TestADeadPeerIsRetriedWithBackoffNotPerFrame: after one failed dial the next calls
// fail at once, without dialing, until the backoff ends.
func TestADeadPeerIsRetriedWithBackoffNotPerFrame(t *testing.T) {
	idA := peerTestIdentity(t)
	var dials atomic.Int32
	now := time.Unix(1000, 0)
	clock := func() time.Time { return now }
	a := peerTestNode(t, "a", idA, nil, func(string) (string, bool) { return "127.0.0.1:1", true }, func(cfg *Config) {
		cfg.Now = clock
		cfg.Dial = func(context.Context, string, transport.Channel, string) (net.Conn, error) {
			dials.Add(1)
			return nil, errors.New("connection refused")
		}
	})
	call := func() error {
		_, err := a.net.Call(peerCtx(t), "b", transport.ChannelData, LaneMain, peerWrite(0x03, "x"))
		return err
	}
	if err := call(); err == nil || errors.Is(err, ErrBackoff) {
		t.Fatalf("first call: %v, want the dial failure", err)
	}
	for range 5 {
		if err := call(); !errors.Is(err, ErrBackoff) {
			t.Fatalf("a call inside the backoff: %v, want ErrBackoff", err)
		}
	}
	if got := dials.Load(); got != 1 {
		t.Fatalf("%d dials during the backoff, want 1", got)
	}
	now = now.Add(150 * time.Millisecond)
	if err := call(); errors.Is(err, ErrBackoff) || err == nil {
		t.Fatalf("a call after the backoff: %v, want a fresh dial failure", err)
	}
	if got := dials.Load(); got != 2 {
		t.Fatalf("%d dials after the backoff ended, want 2", got)
	}
}

func TestAMessageOverTheBoundIsRefusedBeforeItIsSent(t *testing.T) {
	idA := peerTestIdentity(t)
	a := peerTestNode(t, "a", idA, nil, func(string) (string, bool) { return "127.0.0.1:1", true }, func(cfg *Config) { cfg.MaxMessageBytes = 8 })
	_, err := a.net.Call(peerCtx(t), "b", transport.ChannelData, LaneMain, peerWrite(0x03, "123456789"))
	if !errors.Is(err, ErrTooLarge) {
		t.Fatalf("a 9-byte body over an 8-byte bound: %v, want ErrTooLarge", err)
	}
}

// TestTheNetworkAppliesItsMuxAndChannelLimits: the limits in Config reach the mux and the
// data server, not the transport's defaults. A mux that holds one connection refuses the
// second silent one; a data server whose Idle is 200 ms drops a connected, silent peer.
// [GUARD: build the mux or the servers without Config's limits]
func TestTheNetworkAppliesItsMuxAndChannelLimits(t *testing.T) {
	who := peerTestIdentity(t)
	node := peerTestNode(t, "n1", who, []string{who.fp}, func(string) (string, bool) { return "", false }, func(cfg *Config) {
		cfg.MuxLimits = transport.PeerMuxLimits{MaxPending: 1, PeekTimeout: 5 * time.Second}
		cfg.PeerLimits = transport.ChannelLimits{Idle: 200 * time.Millisecond}
	})
	closedWithin := func(conn net.Conn, d time.Duration) bool {
		if err := conn.SetReadDeadline(time.Now().Add(d)); err != nil {
			t.Fatal(err)
		}
		_, err := conn.Read(make([]byte, 1))
		var timeout net.Error
		return !errors.As(err, &timeout) || !timeout.Timeout()
	}
	first, err := net.Dial("tcp", node.addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = first.Close() })
	second, err := net.Dial("tcp", node.addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = second.Close() })
	if !closedWithin(second, 2*time.Second) {
		t.Fatal("a second silent connection stayed open past MuxLimits.MaxPending 1")
	}

	want := transport.ChannelLimits{MaxConns: 7, Idle: 200 * time.Millisecond, MaxInFlight: 3, FrameTimeout: time.Second, PerPeerInFlight: 2, PriorityInFlight: 5}
	configured := peerTestNode(t, "n3", who, []string{who.fp}, func(string) (string, bool) { return "", false }, func(cfg *Config) {
		cfg.PeerLimits = want
	})
	if len(configured.net.servers) != 1 || !reflect.DeepEqual(configured.net.servers[0].Limits(), want) {
		t.Fatalf("the data server runs under %+v, want %+v", configured.net.servers[0].Limits(), want)
	}
}

// TestIdentityServesOneRequestThenCloses: the identity channel carries one question per
// connection, so a caller that asks again dials again; the second request on the same connection
// meets a closed one, and the handler ran once.
func TestIdentityServesOneRequestThenCloses(t *testing.T) {
	var asked atomic.Int32
	// A plain listener stands in for the pinned TLS one: the one-shot rule is the channel server's.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	mux := transport.NewPeerMux(ln)
	t.Cleanup(func() { _ = mux.Close() })
	served := transport.ServeChannelWithLimits(mux.Listener(transport.ChannelIdentity), transport.ChannelIdentity, nil,
		func(context.Context, transport.Handshake, transport.Message) (*transport.Message, error) {
			asked.Add(1)
			return &transport.Message{Env: transport.Envelope{Version: transport.EnvelopeVersion, Kind: transport.KindIdentityReply}, Body: []byte("b")}, nil
		}, transport.ChannelLimits{OneShot: true})
	t.Cleanup(func() { _ = served.Close() })

	conn, err := net.Dial("tcp", mux.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	hello, err := transport.Handshake{Channel: transport.HandshakeChannelIdentity, NodeID: "a", Incarnation: 1}.Encode()
	if err != nil {
		t.Fatal(err)
	}
	payload := append(transport.EncodeEnvelope(transport.Envelope{Version: transport.EnvelopeVersion, Kind: transport.KindIdentityRequest}), make([]byte, 8)...)
	if _, err := conn.Write(hello); err != nil {
		t.Fatal(err)
	}
	if err := transport.WriteFrame(conn, transport.NewFrameWithID(transport.OpClusterProbe, [transport.RequestIDLen]byte{1}, payload)); err != nil {
		t.Fatal(err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(conn)
	if _, err := transport.DecodeFrame(reader); err != nil {
		t.Fatalf("the first request was not answered: %v", err)
	}
	_ = transport.WriteFrame(conn, transport.NewFrameWithID(transport.OpClusterProbe, [transport.RequestIDLen]byte{2}, payload)) // best-effort: the peer may have closed already
	if _, err := transport.DecodeFrame(reader); err == nil {
		t.Fatal("a second request on the same connection was answered")
	}
	if got := asked.Load(); got != 1 {
		t.Fatalf("the handler ran %d times, want once", got)
	}
}

// TestARaftDialToAnUnnamedAddressIsRefused (G1): the Raft stream layer dials by address; an address
// that no member and no configured server holds is refused with no connection made, and one that
// resolves is dialled under that id. [BREAK: fall through to TLS("", addr) on a miss].
func TestARaftDialToAnUnnamedAddressIsRefused(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	var accepted atomic.Int32
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			accepted.Add(1)
			_ = conn.Close()
		}
	}()
	known := map[string]string{}
	var asked []string
	identity := peerTestIdentity(t)
	node := peerTestNode(t, "a", identity, nil, func(string) (string, bool) { return "", false }, func(cfg *Config) {
		cfg.IDAt = func(addr string) (string, bool, error) { id, ok := known[addr]; return id, ok, nil }
		cfg.TLS = func(id, addr string) (*tls.Config, error) {
			asked = append(asked, id)
			return nil, errors.New("stop here: the TLS config was asked for")
		}
	})
	_, err = node.net.DialRaft(ln.Addr().String(), time.Second)
	if err == nil || !strings.Contains(err.Error(), "refused to dial "+ln.Addr().String()) || !strings.Contains(err.Error(), "A join registers the address first") {
		t.Fatalf("a dial to an unnamed address: %v", err)
	}
	if len(asked) != 0 || accepted.Load() != 0 {
		t.Fatalf("a refused dial still asked for TLS %v times and connected %d times", len(asked), accepted.Load())
	}
	known[ln.Addr().String()] = "n7"
	if _, err := node.net.DialRaft(ln.Addr().String(), time.Second); err == nil || len(asked) != 1 || asked[0] != "n7" {
		t.Fatalf("a named address was dialled under %v (err %v), want n7", asked, err)
	}
}

// TestADialThatCouldNotReadTheConfigurationIsNotCalledUnnamed (pi S7): a failed read of who holds an
// address is a transient condition, and the refusal says so instead of saying that no member holds
// the address. [BREAK: dial ignores the error IDAt returns]
func TestADialThatCouldNotReadTheConfigurationIsNotCalledUnnamed(t *testing.T) {
	identity := peerTestIdentity(t)
	node := peerTestNode(t, "a", identity, nil, func(string) (string, bool) { return "", false }, func(cfg *Config) {
		cfg.IDAt = func(string) (string, bool, error) { return "", false, errors.New("leadership could not be verified") }
	})
	_, err := node.net.DialRaft("127.0.0.1:1", time.Second)
	if err == nil || !strings.Contains(err.Error(), "could not read which node holds that address") || !strings.Contains(err.Error(), "leadership could not be verified") ||
		strings.Contains(err.Error(), "A join registers the address first") {
		t.Fatalf("a dial whose address lookup failed: %v", err)
	}
}
