package server

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync/atomic"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// Node-to-node connection handling. A peer connection is a pooled internode
// TCP client that authenticates with the cluster token, because the
// transport rejects every non-auth opcode on an unauthenticated session.
// See the quic-removal-decision record: this used to fall back between
// QUIC and an opt-in internode TCP client; TCP is now the only internode
// transport, so there is nothing to fall back to or race against.

// peerConn is the transport surface the cluster uses on a peer connection
// (SendRequest + Close, and Failed to tell a pool that has left service).
// *transport.TCPPeerClient is the only implementer; the interface exists so
// cluster logic never needs to know that. Every send names its lane, from
// the caller table in query-pipelining spec §3.6: replication and cluster
// control on the control lane, client-driven traffic on the request lane.
type peerConn interface {
	SendRequest(ctx context.Context, lane transport.Lane, op transport.Opcode, payload []byte) (*transport.Frame, error)
	// Generation and SendRequestIfUnchanged tie a send to the sockets a peer's
	// claim was confirmed on (confirmSealed).
	Generation() uint64
	SendRequestIfUnchanged(ctx context.Context, lane transport.Lane, op transport.Opcode, payload []byte, generation uint64) (*transport.Frame, error)
	Close() error
	Failed() bool
}

// dial returns a live connection to the peer, opening one if needed. A
// connection that has failed (its pool could not replace a broken socket) is
// not live: dial opens a fresh one instead, so every caller heals the peer
// connection just by using it, whether or not it drops the client on error
// . The failed pool is not closed here: requests still in flight on
// its other sockets finish, and the pool closes each socket as it comes home.
func (p *peer) dial(ctx context.Context) (peerConn, error) {
	p.mu.Lock()
	if p.client != nil && !p.client.Failed() {
		cli := p.client
		p.mu.Unlock()
		return cli, nil
	}
	p.mu.Unlock()

	dctx, cancel := context.WithTimeout(ctx, peerDialTimeout)
	defer cancel()

	// The peer's internode port is its ring address with port+1
	// (the internode-tcp-build record). Derive it before building
	// the pinned client config: the learned store and the configured
	// host:port= fingerprints are keyed by the internode address the dial
	// actually uses, and a ring address would never match.
	peerAddr, err := transport.InternodePeerAddr(p.node.Addr)
	if err != nil {
		return nil, fmt.Errorf("derive internode address for peer %s: %w", p.node.ID, err)
	}
	tlsCfg, err := p.c.internodeClientConfig(peerAddr)
	if err != nil {
		return nil, fmt.Errorf("build pinned TLS config for peer %s: %w", p.node.ID, err)
	}
	pool, err := transport.NewTCPPeerClient(dctx, peerAddr, tlsCfg, p.c.localToken(), transport.InternodePoolSize)
	if err != nil {
		return nil, fmt.Errorf("dial peer %s at %s: %w", p.node.ID, peerAddr, err)
	}
	pool.SetInFlightObserver(internodeInFlightObserver(p.node.ID))
	// Record the certificate the connection presented — not a claim from a
	// request body — so the node's recorded identity means something.
	p.c.s.recordObservedFingerprint(p.node.ID, pool.PeerFingerprint())
	p.c.logger.Debug("peer connection", "peer", p.node.ID, "addr", peerAddr)
	return p.installClient(pool), nil
}

// installClient records the freshly dialed connection. If another goroutine
// connected while this dial was in flight, the existing connection is kept
// — callers may already be using it — and this one is closed rather than
// leaked, exactly as the pre-interface dial did. An existing connection that
// has failed is replaced rather than kept.
func (p *peer) installClient(cli peerConn) peerConn {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.client != nil && !p.client.Failed() {
		existing := p.client
		_ = cli.Close() // best-effort: both dials raced, keep the older
		return existing
	}
	if p.wrapConn != nil {
		cli = p.wrapConn(cli)
	}
	p.client = cli
	return cli
}

// beforePeerCallHook, when set, runs at the start of every peer.call with the
// request about to be sent. Tests use it to count the exchanges an operation
// costs (T-41). Nil in production.
var beforePeerCallHook atomic.Pointer[func(req *ReplPushRequest)]

// call sends one replication request on lane and decodes the response.
// Replication shipping and catch-up use the control lane; a forwarded client
// request uses the request lane (spec §3.6).
//
// It is the one place a replication request leaves this node, so it is where a
// sealed node's rule is enforced whatever the caller checked: a request that
// carries a bucket's data goes only over a connection on which the peer has
// said its store is sealed, and not at all if a socket of that connection has
// been re-established since (confirmSealed).
func (p *peer) call(ctx context.Context, lane transport.Lane, req *ReplPushRequest) (*ReplPushResponse, error) {
	if hook := beforePeerCallHook.Load(); hook != nil {
		(*hook)(req)
	}
	req.Store = p.c.s.storeClaim()
	payload, err := bson.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal replication request: %w", err)
	}
	var frame *transport.Frame
	if p.c.s.storeSealed() && slices.ContainsFunc(req.Entries, carriesBucketData) {
		cli, generation, confirmErr := p.confirmSealed(ctx, placementAction(req.Entries[0], "send"))
		if confirmErr != nil {
			return nil, confirmErr
		}
		frame, err = cli.SendRequestIfUnchanged(ctx, lane, transport.OpReplPush, payload, generation)
	} else {
		cli, dialErr := p.dial(ctx)
		if dialErr != nil {
			return nil, dialErr
		}
		frame, err = cli.SendRequest(ctx, lane, transport.OpReplPush, payload)
	}
	if err != nil {
		return nil, err
	}
	if frame.Opcode == transport.OpError {
		return nil, fmt.Errorf("peer %s returned an error for %s", p.node.ID, transport.OpReplPush)
	}
	var resp ReplPushResponse
	if len(frame.Payload) > 0 {
		if err := decodePeerAnswer(frame.Payload, &resp); err != nil {
			return nil, fmt.Errorf("decode replication response from %s: %w", p.node.ID, err)
		}
	}
	if err := refuseDeepPeerAnswer(&resp); err != nil {
		return nil, fmt.Errorf("decode replication response from %s: %w", p.node.ID, err)
	}
	// Both sides of an epoch refusal wake their gossip (spec §19.3.7).
	if resp.Refusal == refusalEpoch {
		p.c.wakeGossip()
	}
	return &resp, nil
}

// exchangeGossip sends this node's membership view to the peer over cli and
// returns the peer's answer. A non-zero generation makes the send conditional
// on cli's sockets being the ones of that generation (confirmSealed).
func (p *peer) exchangeGossip(ctx context.Context, cli peerConn, generation uint64) (GossipResponse, error) {
	settled, settledOf, handOff := p.c.settledGossip()
	req := &GossipRequest{
		Token:   p.c.localToken(),
		NodeID:  p.c.self.ID,
		Addr:    p.c.self.Addr,
		Epoch:   p.c.Epoch(),
		Nodes:   p.c.memberNodes(),
		Members: p.c.memberIDs(),

		Features:          clusterFeatures(),
		Instance:          p.c.instance,
		AppliedMemtableMB: p.c.s.memtable.appliedMB,
		MemoryLimitBytes:  p.c.s.memtable.advertisedLimitBytes(),
		Bootstrapped:      p.c.bootstrapped.Load(),
		Settled:           settled,
		SettledOf:         settledOf,
		HandOff:           handOff,
	}
	payload, err := bson.Marshal(req)
	if err != nil {
		return GossipResponse{}, fmt.Errorf("marshal gossip request: %w", err)
	}
	var frame *transport.Frame
	if generation != 0 {
		frame, err = cli.SendRequestIfUnchanged(ctx, transport.LaneControl, transport.OpGossip, payload, generation)
	} else {
		frame, err = cli.SendRequest(ctx, transport.LaneControl, transport.OpGossip, payload)
	}
	if err != nil {
		return GossipResponse{}, fmt.Errorf("gossip with peer %s: %w", p.node.ID, err)
	}
	var resp GossipResponse
	if len(frame.Payload) > 0 {
		if err := decodePeerAnswer(frame.Payload, &resp); err != nil {
			return GossipResponse{}, fmt.Errorf("decode gossip response from %s: %w", p.node.ID, err)
		}
	}
	// The answer came from whatever process listens at the address dialed,
	// under the id it gives for itself.
	p.c.observeInstance(resp.Self.ID, resp.Instance, "answering at "+p.node.Addr)
	if resp.DuplicateID && resp.Self.ID != "" {
		p.c.noteDuplicateReported(resp.Self.ID)
	}
	return resp, nil
}

// confirmSealed returns the peer's connection and the socket generation on
// which the peer has said its store is sealed, asking it when that has not
// been said on these sockets. The caller sends with SendRequestIfUnchanged at
// that generation.
//
// What this node remembers of the peer — from an earlier connection, or from
// membership.json — is not enough to send on: a peer that went down sealed can
// come back plaintext under the same identity, and the pool re-establishes its
// sockets to whatever answers at the address. So the claim is tied
// to the sockets. The exchange is itself sent at the generation read before
// it, so the answer came over a socket that existed then; every socket the
// later send may use existed then too, or the send is refused; and only one
// process answers at an address at a time, so they all reach the process that
// said sealed.
//
// A peer already known not to be sealed is refused without asking: a held
// batch is retried every replRetryInterval, and the peer's own gossip is what
// says it has changed.
func (p *peer) confirmSealed(ctx context.Context, action string) (peerConn, uint64, error) {
	if known := p.c.sealedPeers.knowledge(p.node.ID); known.answered && !known.sealed() {
		return nil, 0, p.c.refusal(p.node, known, action)
	}
	for attempt := 0; attempt < confirmAttempts; attempt++ {
		cli, err := p.dial(ctx)
		if err != nil {
			return nil, 0, err
		}
		generation := cli.Generation()
		p.mu.Lock()
		confirmed := p.confirmedOn == cli && p.confirmedGeneration == generation
		p.mu.Unlock()
		if confirmed {
			return cli, generation, nil
		}
		resp, err := p.exchangeGossip(ctx, cli, generation)
		if errors.Is(err, transport.ErrPeerSocketsChanged) {
			continue
		}
		if err != nil {
			return nil, 0, fmt.Errorf("confirming that peer %s still encrypts its store before sending to it: %w", p.node.ID, err)
		}
		p.c.learnStoreFromGossipResponse(p.node, resp)
		if refusal := p.c.refusal(p.node, p.c.sealedPeers.knowledge(p.node.ID), action); refusal != nil {
			return nil, 0, refusal
		}
		p.mu.Lock()
		p.confirmedOn, p.confirmedGeneration = cli, generation
		p.mu.Unlock()
		return cli, generation, nil
	}
	return nil, 0, fmt.Errorf("confirming that peer %s still encrypts its store before sending to it: %w", p.node.ID, transport.ErrPeerSocketsChanged)
}

// gossip exchanges membership views with the peer and verifies agreement.
func (p *peer) gossip(ctx context.Context) error {
	cli, err := p.dial(ctx)
	if err != nil {
		return err
	}
	resp, err := p.exchangeGossip(ctx, cli, 0)
	if err != nil {
		p.dropClient()
		return err
	}
	p.c.learnStoreFromGossipResponse(p.node, resp)
	if !resp.Success {
		if err := p.c.reconcileRejectedView(p.node.ID, resp); err != nil {
			return err
		}
		return fmt.Errorf("peer %s rejected gossip: %s", p.node.ID, resp.Error)
	}
	// A node in encryption.mode=require takes no membership from a peer that is
	// not known to be sealed: reconciling is how it would adopt that peer's
	// newer epoch.
	if known := p.c.sealedPeers.knowledge(p.node.ID); !known.sealed() && p.c.s.requiresSealedPeers() {
		refusal := joinRefusal(p.node, known, "exchange cluster membership with "+describePeer(p.node))
		p.c.noteMembershipRefusal(p.node.ID, refusal)
		return refusal
	}
	p.c.forgetMembershipRefusal(p.node.ID)

	if err := p.c.reconcileAnswer(p.node.ID, resp); err != nil {
		return err
	}
	p.c.mergeSettled(p.node.ID, resp.Settled, resp.SettledOf, resp.HandOff)

	p.c.s.recordNode(p.node.ID, p.node.Addr)
	p.c.recordMemberFeatures(p.node.ID, resp.Features)
	p.c.observePeerMemtable(p.node.ID, resp.Features, resp.AppliedMemtableMB)
	p.c.logger.Debug("peer verified", "node_id", p.node.ID, "addr", p.node.Addr)
	return nil
}
