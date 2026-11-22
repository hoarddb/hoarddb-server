package wire

import (
	"context"
	entropy "crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/transport"
	"github.com/hoarddb/hoarddb-server/version"
)

// identityAskTimeout bounds one IDENTITY_REQUEST when the caller's context sets no deadline.
const identityAskTimeout = 5 * time.Second

// NodeIdentity is what a node told the leader about itself over the identity channel, after the
// leader checked it against the id and address the operator typed.
type NodeIdentity struct {
	// ID and Addr are what the operator typed and the node confirmed.
	ID, Addr string
	// Incarnation is the node's incarnation, the one its later handshakes carry.
	Incarnation int64
	// Witness is the node's own cluster.witness setting.
	Witness bool
	// HoldsRaftState is true when the node bootstrapped or was joined before.
	HoldsRaftState bool
	// ClusterID is the node's cluster, zero for a node that belongs to none.
	ClusterID [16]byte
	// ClientAddr is the address drivers connect to on the node, empty when it publishes none. It has
	// passed cluster.ValidateClientAddr against the data address the pinned key vouches for.
	ClientAddr string
	// Range is the state-level range the node can act at; the zero value is level 0, a node
	// whose build predates the check.
	Range cluster.StateRange
	// Build is the node's build string, empty when it sent none.
	Build string
}

// serveIdentity answers IDENTITY_REQUEST (plan section 1.4): the facts only this node holds, to
// any pinned key, member or not. The channel server closes the connection after the answer. A request
// with the admit bit also ends the BUG-0870 guard's hold on a node that opened to join with no Raft
// state (cluster.Raft.Admit): the asker is a pinned peer whose join, or retried join, is about to add
// this node, and only that ask sets the bit. A plain ask changes nothing.
func (n *Node) serveIdentity(_ context.Context, _ transport.Handshake, req transport.Message) (*transport.Message, error) {
	if n.cl == nil || !n.cl.serving.Load() {
		return nil, errStarting
	}
	if req.Env.Kind != transport.KindIdentityRequest {
		return nil, fmt.Errorf("%w: kind 0x%02x is not an IDENTITY_REQUEST", transport.ErrInvalidKind, req.Env.Kind)
	}
	request, err := DecodeIdentityRequest(req.Body)
	if err != nil {
		return nil, err
	}
	nonce := request.Nonce
	state := n.raft.State()
	body, err := EncodeIdentityReply(IdentityReply{
		Nonce: nonce, Incarnation: n.incarnation, Witness: n.cfg.Witness,
		HoldsRaftState: n.raft.Restarted() || state.StateSeq > 0,
		ClusterID:      state.ClusterID, DataAddr: n.dataAddr(), NodeID: n.cfg.NodeID, ClientAddr: n.cfg.ClientAddr,
		Range: n.stateRange(), Build: version.Version,
	})
	if err != nil {
		return nil, fmt.Errorf("answering an identity request: %w", err)
	}
	if request.Admit {
		admitWithFloor(!n.raft.Restarted() && state.StateSeq == 0, req.Env.StateSeq, n.cl.noteAdmission, n.raft.Admit)
	}
	return &transport.Message{
		Env:  transport.Envelope{Version: transport.EnvelopeVersion, Kind: transport.KindIdentityReply, Epoch: uint64(state.Epoch), StateSeq: uint64(state.StateSeq)},
		Body: body,
	}, nil
}

// AskIdentity asks the node pinned as id at addr who it is, over the identity channel, and
// applies the cross-checks of plan section 1.3. The dial is held to id's pinned key, so the
// identity comes from the authenticated key and the reply's node id is a cross-check of it. It
// changes nothing on either node. A node whose state range does not overlap this node's is
// refused first, with the join text of mixed-build spec 9.3.
func (n *Node) AskIdentity(ctx context.Context, id, addr string) (NodeIdentity, error) {
	return n.askIdentity(ctx, id, addr, false, false)
}

// askIdentity is AskIdentity; registered is true for a node that already holds a committed
// joining row. That row proves the node is ours, so a node that holds Raft state and names no
// cluster is not a cluster of its own but a registered joiner that restarted before applying the
// bootstrap entry, and only the nonce, the id, the cluster it names, the address and the range
// are judged.
func (n *Node) askIdentity(ctx context.Context, id, addr string, registered, admit bool) (NodeIdentity, error) {
	if n.cl == nil {
		return NodeIdentity{}, &cluster.Error{Code: cluster.CodeInvalidArgument, Message: "this node is not in a cluster, so it has no peers to ask. Set cluster.enabled: true and restart it"}
	}
	if fingerprint := n.cl.opts.Fingerprints; fingerprint != nil {
		if _, pinned := fingerprint(id); !pinned {
			return NodeIdentity{}, identityMissingPin(id, addr)
		}
	}
	var nonce [8]byte
	if _, err := entropy.Read(nonce[:]); err != nil {
		return NodeIdentity{}, fmt.Errorf("drawing the nonce of an identity request: %w", err)
	}
	if _, bounded := ctx.Deadline(); !bounded {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, identityAskTimeout)
		defer cancel()
	}
	state := n.raft.State()
	reply, err := n.cl.net.Ask(ctx, id, addr, transport.ChannelIdentity, transport.Message{
		Env:  transport.Envelope{Version: transport.EnvelopeVersion, Kind: transport.KindIdentityRequest, Epoch: uint64(state.Epoch), StateSeq: uint64(state.StateSeq)},
		Body: EncodeIdentityRequest(IdentityRequest{Nonce: binary.BigEndian.Uint64(nonce[:]), Admit: admit}),
	})
	if err != nil {
		return NodeIdentity{}, identityUnreachable(id, addr, err)
	}
	if reply.Env.Kind != transport.KindIdentityReply {
		return NodeIdentity{}, identityUnreachable(id, addr, fmt.Errorf("it answered kind 0x%02x, not IDENTITY_REPLY", reply.Env.Kind))
	}
	answer, err := DecodeIdentityReply(reply.Body)
	if err != nil {
		return NodeIdentity{}, identityUnreachable(id, addr, err)
	}
	// The range is judged before the cross-checks: a node on another build that could not sync
	// its state also looks like a node with a cluster of its own, and the range is the cause.
	if !n.stateRange().Overlaps(answer.Range) {
		return NodeIdentity{}, joinStateRangeRefused(id, addr, answer.Build, answer.Range, n.stateRange())
	}
	return verifyIdentityAs(answer, binary.BigEndian.Uint64(nonce[:]), id, addr, state.ClusterID, registered)
}

// verifyIdentity applies plan section 1.3 to a decoded reply, in the order the plan gives:
// the nonce, the id (I1), the cluster (mismatch), a node that holds state and names no
// cluster (I3), the address (I4), the client address (I6).
func verifyIdentity(reply IdentityReply, nonce uint64, id, addr string, ours [16]byte) (NodeIdentity, error) {
	return verifyIdentityAs(reply, nonce, id, addr, ours, false)
}

// verifyIdentityAs is verifyIdentity; registered skips the own-cluster case (see askIdentity).
func verifyIdentityAs(reply IdentityReply, nonce uint64, id, addr string, ours [16]byte, registered bool) (NodeIdentity, error) {
	switch {
	case reply.Nonce != nonce:
		return NodeIdentity{}, identityUnreachable(id, addr, errors.New("its reply answers another request"))
	case reply.NodeID != id:
		return NodeIdentity{}, identityKeyBelongsToAnother(id, addr, reply.NodeID)
	case reply.ClusterID != [16]byte{} && reply.ClusterID != ours:
		return NodeIdentity{}, identityOtherCluster(id, addr, reply.ClusterID, ours)
	case !registered && reply.HoldsRaftState && reply.ClusterID == [16]byte{}:
		return NodeIdentity{}, identityHoldsOwnCluster(id, addr)
	case reply.DataAddr != addr:
		return NodeIdentity{}, identityAddressDiffers(id, addr, reply.DataAddr)
	}
	if reply.ClientAddr != "" {
		// The data address is the one the node's own pinned key vouches for (the dial was held to
		// it), so the client address is accepted only on that host: the same rule the cluster
		// applies when it commits the row, asked early so the operator hears it from the join.
		if err := cluster.ValidateClientAddr(reply.ClientAddr, addr); err != nil {
			return NodeIdentity{}, identityClientAddrRefused(id, addr, reply.ClientAddr, err)
		}
	}
	return NodeIdentity{
		ID: id, Addr: addr, Incarnation: reply.Incarnation, Witness: reply.Witness,
		HoldsRaftState: reply.HoldsRaftState, ClusterID: reply.ClusterID, ClientAddr: reply.ClientAddr,
		Range: reply.Range, Build: reply.Build,
	}, nil
}
