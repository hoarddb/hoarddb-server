package harness

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"path/filepath"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/peer"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Rogue is a peer whose certificate every node pins (so it passes TLS) but that is not
// the member it claims to be: a pinned non-member, or a member's name spoken by another
// key. It is the exposure window the join protocol will have, made a test fixture.
type Rogue struct {
	c      *Cluster
	claim  string
	trust  *peer.Trust
	fp     string
	zeroID bool
}

// NewRogue creates a rogue that claims to be node claim and pins its fingerprint on every
// node. With zeroClusterID its handshake carries the zero cluster id.
func (c *Cluster) NewRogue(claim string, zeroClusterID bool) *Rogue {
	c.tb.Helper()
	return c.newRogue(claim, zeroClusterID, true)
}

// NewUnpinnedRogue is a rogue whose key no node pins: it trusts every member, but no member
// trusts it, so it is refused at TLS before any handshake.
func (c *Cluster) NewUnpinnedRogue(claim string) *Rogue {
	c.tb.Helper()
	return c.newRogue(claim, false, false)
}

func (c *Cluster) newRogue(claim string, zeroClusterID, pinned bool) *Rogue {
	c.tb.Helper()
	cfg, fp, err := transport.MakeServerTLSConfig(filepath.Join(c.root, "rogue-"+claim, "keys"))
	if err != nil {
		c.tb.Fatalf("harness: making the rogue key: %v", err)
	}
	var pins []string
	c.mu.Lock()
	if _, member := c.members[claim]; !member && pinned {
		c.spare[claim] = fp
	}
	for _, m := range c.members {
		if pinned {
			if err := m.trust.AddPin(fp); err != nil {
				c.mu.Unlock()
				c.tb.Fatalf("harness: pinning the rogue on %s: %v", m.id, err)
			}
		}
		pins = append(pins, m.fp)
	}
	c.mu.Unlock()
	trust, err := peer.NewTrust(cfg, pins)
	if err != nil {
		c.tb.Fatalf("harness: the rogue's trust: %v", err)
	}
	return &Rogue{c: c, claim: claim, trust: trust, fp: fp, zeroID: zeroClusterID}
}

func (r *Rogue) hello(target string) transport.Handshake {
	h := cluster.BuildStateRange().Stamp(transport.Handshake{NodeID: r.claim, Incarnation: 1 << 62})
	if !r.zeroID {
		id := r.c.Node(target).Identity()
		_ = id
		h.ClusterID = r.c.Node(target).State().ClusterID
	}
	return h
}

func (r *Rogue) dial(ctx context.Context, addr string) (net.Conn, error) {
	raw, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("rogue dialing %s: %w", addr, err)
	}
	conf, err := r.trust.ClientConfig(addr)
	if err != nil {
		return nil, err
	}
	conn := tls.Client(raw, conf)
	if err := conn.HandshakeContext(ctx); err != nil {
		_ = raw.Close() // best-effort: the dial failed
		return nil, fmt.Errorf("rogue TLS to %s: %w", addr, err)
	}
	return conn, nil
}

// Call sends one message to target on a data or control channel as the rogue and returns
// the reply or the error. An error that is not a *transport.WireError means the target
// dropped the connection before any handler ran.
func (r *Rogue) Call(target string, ch transport.Channel, msg transport.Message) (*transport.Message, error) {
	addr := r.c.members[target].addr
	client := transport.NewChannelClient(addr, ch, func() transport.Handshake { return r.hello(target) })
	client.SetDialer(r.dial)
	defer func() { _ = client.Close() }() // best-effort
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return client.Call(ctx, msg)
}

// RaftClosed opens the raft channel as the rogue and reports whether the target closed the
// connection within a few seconds of the handshake (it must, for a peer that is not a
// current member speaking as itself).
func (r *Rogue) RaftClosed(target string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := r.dial(ctx, r.c.members[target].addr)
	if err != nil {
		return true
	}
	defer func() { _ = conn.Close() }() // best-effort
	h := r.hello(target)
	h.Channel = transport.HandshakeChannelRaft
	raw, err := h.Encode()
	if err != nil {
		r.c.tb.Fatalf("harness: rogue handshake: %v", err)
	}
	if _, err := conn.Write(raw); err != nil {
		return true
	}
	if err := conn.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		return true
	}
	_, err = conn.Read(make([]byte, 1))
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		return false
	}
	return true
}

// CallAs sends one message from member from to member to over the real pinned connection,
// speaking as itself. It is how a test makes a genuine member send what its role does
// not allow (a non-owner pushing a WRITE, a non-copy acking).
func (c *Cluster) CallAs(from, to string, ch transport.Channel, msg transport.Message) (*transport.Message, error) {
	return c.callAs(from, to, ch, msg, false)
}

// CallAsWithoutClusterID is CallAs with the zero cluster id in the handshake: a genuine
// member's key and name, without the cluster id the receiver now requires.
func (c *Cluster) CallAsWithoutClusterID(from, to string, ch transport.Channel, msg transport.Message) (*transport.Message, error) {
	return c.callAs(from, to, ch, msg, true)
}

// ClientAs opens a channel client to node to as node from, on its pinned key, and keeps its
// connection between calls: a test that needs one connection to outlive a membership change (the
// pepper hand-out's per-request check) calls it twice. The caller closes the client.
func (c *Cluster) ClientAs(from, to string, ch transport.Channel) *transport.ChannelClient {
	return c.clientAs(from, to, ch, false)
}

func (c *Cluster) callAs(from, to string, ch transport.Channel, msg transport.Message, zeroID bool) (*transport.Message, error) {
	client := c.clientAs(from, to, ch, zeroID)
	defer func() { _ = client.Close() }() // best-effort
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return client.Call(ctx, msg)
}

func (c *Cluster) clientAs(from, to string, ch transport.Channel, zeroID bool) *transport.ChannelClient {
	c.mu.Lock()
	sender, receiver := c.members[from], c.members[to]
	c.mu.Unlock()
	node := c.Node(from)
	hello := func() transport.Handshake {
		h := node.Hello()
		if zeroID {
			h.ClusterID = [16]byte{}
		}
		return h
	}
	client := transport.NewChannelClient(receiver.addr, ch, hello)
	client.SetDialer(func(ctx context.Context, addr string) (net.Conn, error) {
		raw, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
		if err != nil {
			return nil, fmt.Errorf("dialing %s: %w", to, err)
		}
		conf, err := sender.trust.ClientConfig(addr)
		if err != nil {
			return nil, err
		}
		conn := tls.Client(raw, conf)
		if err := conn.HandshakeContext(ctx); err != nil {
			_ = raw.Close() // best-effort: the dial failed
			return nil, err
		}
		return conn, nil
	})
	return client
}
