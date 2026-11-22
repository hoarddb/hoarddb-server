package server

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Join-flow errors. These are the design's §6.2 "operator sees a defined
// reason" paths, never a bare failure.
var (
	// ErrEpochInProgress is returned when a membership change is proposed
	// while one is already being committed. Transitions are serialized
	// (rejected, not queued): only one epoch transition is active at a time.
	ErrEpochInProgress = errors.New("a membership change is already in progress; wait for it to complete and retry")
	// ErrJoinFailedMajority is returned when a join proposal could not get the
	// acknowledgement of a majority of the current members.
	ErrJoinFailedMajority = errors.New("join proposal did not reach a majority of the cluster members")
	// ErrAlreadyMember is returned when adding a node that is already in the
	// member set.
	ErrAlreadyMember = errors.New("the node is already a cluster member; run 'nodes' to list the members")
	// ErrNodeIDTaken is returned when adding a node under an id a member
	// already holds at another address: two nodes under one id would both
	// own its buckets (BUG-0595).
	ErrNodeIDTaken = errors.New("another cluster member already holds this node id")
	// ErrMemberDiskFull is returned when a node's disk is at or above the
	// configured high-water mark and it therefore will not propose or vote for
	// a new member, or will not copy in a bucket it does not already hold.
	ErrMemberDiskFull = errors.New("node disk above the high-water mark")
	// ErrNotMember is returned when removing a node that is not a member.
	ErrNotMember = errors.New("the node is not a cluster member; run 'nodes' to list the members")
	// ErrRemoveSelf is returned when a node tries to remove itself; removal
	// must be issued from another member so the leaving node can drain first.
	ErrRemoveSelf = errors.New("cannot remove this node from itself; run 'remove-node' against another member")
	// ErrJoinSyncIncomplete is the fence a node raises when it is the ring
	// owner of a bucket it does not hold locally. In steady state an owner
	// always has its bucket (DDL is placed on the replica set), so this names
	// both real causes: the bucket was never created, or this node joined after
	// it existed and has not received it yet. Data must never be accepted into
	// an empty store while the real copy lives elsewhere.
	ErrJoinSyncIncomplete = errors.New("this node is the ring owner of this bucket but has no local copy of it: either the bucket " +
		"was never created (run 'show buckets in <database>' to check), or this node joined after it existed and rebalancing has " +
		"not moved the data here yet (run 'migrate <database> <bucket>' on this node to copy it now, or 'nodes' to see the members). " +
		"The data is safe on the previous owner; no write was accepted")
)

// membershipPhaseHook, when set, runs at the top of a membership change's
// receiving handler, before any rule is applied, and on the proposer itself
// where a schedule needs a stop. A non-nil error is answered as a refusal; it
// may block. A test uses it to choose which member answers which phase of a
// change, which a real cluster decides by timing alone. Nil in production.
var membershipPhaseHook atomic.Pointer[func(nodeID, phase, fromNode string, epoch uint64) error]

// membershipSendHook, when set, runs on the proposer before each message of a
// membership change leaves it. A non-nil error counts as an unreachable
// member; it may block until ctx ends, which holds that message. Nil in
// production.
var membershipSendHook atomic.Pointer[func(ctx context.Context, fromNode, toNode, phase string) error]

// ClusterProposeRequest is the vote of the membership protocol
// OpMembershipChange replaced. A member answers it only with a refusal
// (handleClusterPropose); the type stays so that refusal reads the token.
type ClusterProposeRequest struct {
	Token    string `bson:"token"`
	FromNode string `bson:"from_node"`
	// Epoch is the epoch being proposed (current + 1).
	Epoch uint64 `bson:"epoch"`
	// Member is the node being added.
	Member Peer `bson:"member"`
	// Members is the full member set of the proposed epoch, so a voter adopts
	// exactly what the proposer proposed rather than a locally-derived guess.
	Members []Peer `bson:"members"`
}

// ClusterProposeResponse is the answer to the replaced vote, which an older
// proposer can read.
type ClusterProposeResponse struct {
	Success bool   `bson:"success"`
	Error   string `bson:"error,omitempty"`
}

// ClusterAddRequest is the operator-facing request to add a node.
type ClusterAddRequest struct {
	// Address is "host:port" or "id@host:port" for the node being added.
	Address string `bson:"address"`
}

// ClusterAddResponse reports the outcome of the join.
type ClusterAddResponse struct {
	Success bool   `bson:"success"`
	Epoch   uint64 `bson:"epoch"`
	Message string `bson:"message,omitempty"`
	Error   string `bson:"error,omitempty"`
}

// handleClusterAdd is triggered by the operator (`cluster add`) on any
// member. The member runs the join proposal against the rest of the cluster.
func handleClusterAdd(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	var req ClusterAddRequest
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return errorFrame(frame, err), nil
	}
	newcomer, err := parseMember(req.Address)
	if err != nil {
		return errorFrame(frame, err), nil
	}
	if s.cluster != nil {
		if err := refuseLoopbackJoin(s.cluster, newcomer.Addr); err != nil {
			return errorFrame(frame, err), nil
		}
	}
	// A bare address makes parseMember record identity=address, which is
	// correct only for a node that runs with address identity. Resolve the
	// target's announced identity instead (probeMemberIdentity), so
	// `join host:port` works for any node; `identity@address` remains the
	// explicit, probe-free form. A probe failure aborts before any
	// membership change.
	if newcomer.ID == newcomer.Addr && s.cluster != nil {
		resolved, perr := s.cluster.probeMemberIdentity(ctx, newcomer.Addr)
		if perr != nil {
			return errorFrame(frame, fmt.Errorf(
				"cannot determine the identity of %s (%w); pass the identity explicitly: 'join <node-id>@%s'",
				newcomer.Addr, perr, newcomer.Addr)), nil
		}
		newcomer = resolved
	}
	if s.cluster == nil {
		return errorFrame(frame, errors.New("replication is not enabled on this node; the node must be a cluster member to add another")), nil
	}
	// Before the newcomer is dialed: a node that is not running yet would
	// otherwise be refused as unanswered, and the operator would start it
	// under the id that is refused next.
	if err := refuseHeldNodeID(s.cluster.Members(), newcomer); err != nil {
		return errorResponse(frame, err)
	}
	if err := s.cluster.admitNewcomerAsked(ctx, newcomer); err != nil {
		return errorFrame(frame, err), nil
	}
	outcome, err := s.cluster.proposeJoinOutcome(ctx, newcomer)
	if err != nil {
		// A refusal is answered, never returned as the handler's error: a
		// response returned with an error was dropped and the connection
		// failed with it (BUG-0563), and a handler error alone is answered
		// INTERNAL (BUG-0509). As an error frame it carries codeFor's code,
		// UNAVAILABLE for a change already in progress, which a caller retries.
		return errorResponse(frame, err)
	}
	message := fmt.Sprintf("added %s; cluster now at epoch %d (data rebalancing)", newcomer.ID, s.cluster.Epoch())
	if outcome.message != "" {
		message = outcome.message
	}
	return nodeResponse(transport.OpClusterAdd, frame, ClusterAddResponse{
		Success: true,
		Epoch:   s.cluster.Epoch(),
		Message: message,
	}, nil)
}

// probeMemberIdentity asks a reachable node for its self-announced identity,
// used when the operator adds a node by bare address (`join host:port`): the
// ring must record the node's real NODE_ID, not identity=address, or the
// node can never adopt the entry. The exchange is one read-only gossip on
// this side — dial the target's internode port, authenticate as a node with
// the cluster token, send our view, read their member list, return the
// member whose address matches. No local cluster state is touched.
func (c *Cluster) probeMemberIdentity(ctx context.Context, addr string) (ring.Node, error) {
	resp, observed, err := c.probeGossip(ctx, addr)
	if err != nil {
		return ring.Node{}, err
	}
	// Prefer the responder's explicit Self announcement: its member list can
	// hold an address-identity duplicate of itself (booted from bare seed
	// addresses) next to its NODE_ID entry, both on the same Addr, so
	// matching by address alone is ambiguous. Older peers omit Self; fall
	// back to the address match for them.
	if resp.Self.ID != "" && resp.Self.Addr == addr {
		c.s.recordObservedFingerprint(resp.Self.ID, observed)
		return ring.Node{ID: resp.Self.ID, Addr: resp.Self.Addr}, nil
	}
	for _, m := range resp.Nodes {
		if m.Addr == addr {
			c.s.recordObservedFingerprint(m.ID, observed)
			return ring.Node{ID: m.ID, Addr: m.Addr}, nil
		}
	}
	return ring.Node{}, fmt.Errorf(
		"could not determine the identity of %s from its gossip response; the node must be reachable and answer as a member of this cluster — or pass the identity explicitly: 'join <node-id>@%s'",
		addr, addr)
}

// probeGossip is one read-only gossip exchange with the node at addr over an
// ad-hoc pinned connection, returning its answer and the certificate
// fingerprint the dial verified. It registers no peer, so it can be asked of a
// node that is not a member yet. Both questions a join asks of a newcomer go
// through it: who it is (probeMemberIdentity) and whether its store is sealed
// (newcomerStore).
func (c *Cluster) probeGossip(ctx context.Context, addr string) (GossipResponse, string, error) {
	return c.probeGossipWithin(ctx, addr, requiredProbeWait)
}

// requiredProbeWait bounds a probe whose answer a command needs (who a node
// is, whether it remembers its removal): the dial, TLS and AUTH may be slow.
const requiredProbeWait = 5 * time.Second

// optionalProbeWait bounds a probe whose silence the join accepts: the
// newcomer's memtable size and sealed state are asked again on its first
// gossip, so an unanswering newcomer must cost a moment, not the full
// requiredProbeWait. A node that is running answers in milliseconds.
const optionalProbeWait = 1 * time.Second

// probeGossipWithin is probeGossip with the caller's bound on the whole exchange.
func (c *Cluster) probeGossipWithin(ctx context.Context, addr string, wait time.Duration) (GossipResponse, string, error) {
	dctx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()

	internodeAddr, err := transport.InternodePeerAddr(addr)
	if err != nil {
		return GossipResponse{}, "", fmt.Errorf("derive internode address for %s: %w", addr, err)
	}
	tlsCfg, err := c.internodeClientConfig(internodeAddr)
	if err != nil {
		return GossipResponse{}, "", fmt.Errorf("build pinned TLS config for %s: %w", internodeAddr, err)
	}
	// Pool size 1: this is a single ad-hoc probe, not a standing peer
	// connection. NewTCPPeerClient authenticates as NodeUser at dial, so a
	// rejected credential fails here rather than needing a separate AUTH
	// round trip.
	cli, err := transport.NewTCPPeerClient(dctx, internodeAddr, tlsCfg, c.localToken(), 1)
	if err != nil {
		return GossipResponse{}, "", fmt.Errorf("dial %s: %w", internodeAddr, err)
	}
	defer func() { _ = cli.Close() }()
	// The certificate this dial verified is the target's real identity; the
	// ring entry takes it from here, not from a self-asserted request field.
	observed := cli.PeerFingerprint()

	settled, settledOf, handOff := c.settledGossip()
	req := &GossipRequest{
		Token:   c.localToken(),
		NodeID:  c.self.ID,
		Addr:    c.self.Addr,
		Epoch:   c.Epoch(),
		Nodes:   c.memberNodes(),
		Members: c.memberIDs(),

		Features:          clusterFeatures(),
		Instance:          c.instance,
		AppliedMemtableMB: c.s.memtable.appliedMB,
		MemoryLimitBytes:  c.s.memtable.advertisedLimitBytes(),
		Bootstrapped:      c.bootstrapped.Load(),
		Settled:           settled,
		SettledOf:         settledOf,
		HandOff:           handOff,
	}
	payload, err := bson.Marshal(req)
	if err != nil {
		return GossipResponse{}, "", fmt.Errorf("marshal gossip probe: %w", err)
	}
	frame, err := cli.SendRequest(dctx, transport.LaneControl, transport.OpGossip, payload)
	if err != nil {
		return GossipResponse{}, "", fmt.Errorf("gossip probe %s: %w", addr, err)
	}
	var resp GossipResponse
	if err := decodePeerAnswer(frame.Payload, &resp); err != nil {
		return GossipResponse{}, "", fmt.Errorf("decode gossip probe from %s: %w", addr, err)
	}
	return resp, observed, nil
}

// newcomerStore asks a node that is not a member yet whether its store is
// sealed, and records the answer. A node that cannot be reached has not
// answered.
func (c *Cluster) newcomerStore(ctx context.Context, newcomer ring.Node) peerKnowledge {
	resp, _, err := c.probeGossipWithin(ctx, newcomer.Addr, optionalProbeWait)
	if err != nil {
		c.logger.Debug("asking a joining node whether its store is encrypted", "node_id", newcomer.ID, "error", err)
		return peerKnowledge{}
	}
	c.learnStoreFromGossipResponse(newcomer, resp)
	return c.sealedPeers.knowledge(newcomer.ID)
}

// admitNewcomer is the join check on a sealed node (spec §8.1, §8.9). In
// encryption.mode=require a newcomer whose store is plaintext, or that cannot
// be asked, is refused. In mode on a newcomer that says its store is plaintext
// is refused too (the coordinator's ruling on the join freeze): this cluster
// never places data on it, so its join would never settle and schema changes
// would stay paused for as long as it stayed. A newcomer that cannot be asked
// is refused as well; one that answers from a version that does not report is
// admitted, and the placement refusal protects the data. A plaintext node asks
// nothing, so a plaintext cluster's join is what it was.
func (c *Cluster) admitNewcomer(ctx context.Context, newcomer ring.Node) error {
	if !c.s.storeSealed() {
		return nil
	}
	known := c.newcomerStore(ctx, newcomer)
	if known.sealed() {
		return nil
	}
	var refusal error
	switch {
	case c.s.requiresSealedPeers():
		refusal = joinRefusal(newcomer, known, "add "+describePeer(newcomer)+" to the cluster")
	case !known.answered:
		// Fail closed (the coordinator's ruling): a newcomer that is down or
		// slow now may start plaintext a moment later, and a join admitted
		// then could never settle. The operator retries once it answers.
		refusal = fmt.Errorf("refusing to add %s to the cluster: could not ask %s whether its store is encrypted "+
			"(this cluster's stores are encrypted at rest); start %s and run 'join' again",
			describePeer(newcomer), newcomer.ID, newcomer.ID)
	case known.claim == claimPlaintext:
		refusal = plaintextJoinerRefusal(newcomer, known)
	default:
		return nil
	}
	// The refused node stays outside the member set, so its answer is dropped
	// again: the table holds members only.
	c.sealedPeers.retain(c.Members())
	c.peerDrivers.retain(c.Members())
	return refusal
}

// plaintextJoinerRefusal is a node in encryption.mode=on refusing to add a
// node whose store is not encrypted, with the two ways to a cluster that
// agrees.
func plaintextJoinerRefusal(newcomer ring.Node, known peerKnowledge) error {
	return fmt.Errorf("refusing to add %s to the cluster: %s, and this cluster's stores are encrypted at rest (encryption.mode=on), "+
		"which never places data on an unencrypted node: its join would never finish, and schema changes would stay paused. "+
		"%s, then run 'join' again; or, for a cluster without encryption at rest, start every member from a fresh data directory with encryption.mode=off",
		describePeer(newcomer), peerStoreState(newcomer, known), peerStoreFix(newcomer, known, "encryption.mode=on"))
}

// memberAddr returns the address a proposed member set lists for member,
// falling back to the one the proposal's own record of it carries.
func memberAddr(members []ring.Node, member Peer) string {
	for _, listed := range members {
		if listed.ID == member.ID {
			return listed.Addr
		}
	}
	return member.Addr
}

// ClusterRemoveRequest is the operator-facing request to remove a node.
type ClusterRemoveRequest struct {
	// NodeID is the member being removed.
	NodeID string `bson:"node_id"`
}

// ClusterRemoveResponse reports the outcome of the removal.
type ClusterRemoveResponse struct {
	Success bool   `bson:"success"`
	Epoch   uint64 `bson:"epoch"`
	Message string `bson:"message,omitempty"`
	Error   string `bson:"error,omitempty"`
}

// handleClusterRemove removes a member from the cluster by a membership change.
// Issue it against any *other* member; the removed node's buckets re-home to
// the new owners and must be migrated there.
func handleClusterRemove(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	var req ClusterRemoveRequest
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return errorFrame(frame, err), nil
	}
	if s.cluster == nil {
		return errorFrame(frame, errors.New("replication is not enabled on this node")), nil
	}
	outcome, err := s.cluster.proposeRemovalOutcome(ctx, req.NodeID)
	if err != nil {
		// A refusal is an answer, not a handler error (BUG-0509, BUG-0563):
		// see handleClusterAdd.
		return errorResponse(frame, err)
	}
	message := fmt.Sprintf("removed %s; cluster now at epoch %d. Its buckets are moving to their new owners; %s", req.NodeID, s.cluster.Epoch(), handoffsSettledHint)
	if outcome.message != "" {
		message = outcome.message
	}
	return nodeResponse(transport.OpClusterRemove, frame, ClusterRemoveResponse{
		Success: true,
		Epoch:   s.cluster.Epoch(),
		Message: message,
	}, nil)
}

// errOldMembershipProtocol is a new node's answer to the vote of the
// membership protocol it replaced: a member that still sends it is a build
// from before, and changes need every member on one protocol.
var errOldMembershipProtocol = errors.New("this member runs a newer membership protocol; upgrade every member before adding or removing nodes")

// handleClusterPropose answers the vote of the replaced membership protocol
// with a refusal an older proposer can read: a vote here was an adoption,
// which let a set no majority chose become live.
func handleClusterPropose(_ context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	refusal := errOldMembershipProtocol.Error()
	var req ClusterProposeRequest
	noteRequestDecode("handleClusterPropose", frame.Opcode, frame.Payload)
	switch c := s.cluster; {
	case c == nil:
		refusal = "replication is not enabled on this node"
	case bson.Unmarshal(frame.Payload, &req) != nil || !c.ValidateToken(req.Token):
		refusal = "invalid cluster token"
	}
	return nodeResponse(transport.OpClusterPropose, frame, ClusterProposeResponse{Success: false, Error: refusal}, nil)
}

// proposeJoin is the operator-triggered join on the proposing node: a
// membership change that adds newcomer (changeMembership).
func (c *Cluster) proposeJoin(ctx context.Context, newcomer ring.Node) error {
	_, err := c.proposeJoinOutcome(ctx, newcomer)
	return err
}

// proposeJoinOutcome is proposeJoin with the change's outcome.
func (c *Cluster) proposeJoinOutcome(ctx context.Context, newcomer ring.Node) (membershipOutcome, error) {
	refused := membershipOutcome{kind: changeRefused}
	if newcomer.ID == "" || newcomer.Addr == "" {
		return refused, fmt.Errorf("cluster: cannot add a node with an empty identity or address")
	}
	curEpoch, curMembers := c.epochMembers()
	if err := refuseHeldNodeID(curMembers, newcomer); err != nil {
		return refused, err
	}
	// The proposer is also a rebalancing target of a join; fail fast here with
	// the actionable message instead of asking members that will then refuse
	// for the same reason.
	if !c.diskHeadroomOK() {
		return refused, c.diskFullJoinError()
	}
	if err := c.admitNewcomerMemtable(ctx, newcomer); err != nil {
		return refused, err
	}
	if err := c.admitNewcomer(ctx, newcomer); err != nil {
		return refused, err
	}
	newMembers := append(append([]ring.Node{}, curMembers...), newcomer)
	return c.changeMembership(ctx, membershipProposal{epoch: curEpoch + 1, base: curMembers,
		own: proposedSet{members: newMembers, change: newcomer}})
}

// refuseHeldNodeID refuses to add newcomer under an id a member holds: at the
// same address it is that member, at another it is a second node under one id.
func refuseHeldNodeID(members []ring.Node, newcomer ring.Node) error {
	for _, m := range members {
		if m.ID != newcomer.ID {
			continue
		}
		if m.Addr == newcomer.Addr {
			return fmt.Errorf("%w: %s", ErrAlreadyMember, newcomer.ID)
		}
		return fmt.Errorf("%w: refusing to add the node at %s as %q: member %q at %s already holds that id, "+
			"and two nodes under one id would both own its buckets. "+
			"Start the node at %s with its own HOARDDB_NODE_ID and run 'join <node-id>@%s' again",
			ErrNodeIDTaken, newcomer.Addr, newcomer.ID, m.ID, m.Addr, newcomer.Addr, newcomer.Addr)
	}
	return nil
}

// handoffsSettledHint is how an operator tells that the buckets a removal
// moved have been taken over: the gauge reads 0 on every node, but every node
// reads 0 before its placement pass after the change has run, so a 0 in the
// first seconds says nothing.
const handoffsSettledHint = "hoarddb_handoffs_in_progress reads 0 on every node once they have, " +
	"which is not in the first seconds: until a node's placement pass has run, it reads 0 there too"

// proposeRemoval is the operator-triggered removal of a member (§6.3). The
// cluster moves to epoch N+1 without the node; its buckets move to their new
// owners, whose take-over workers copy them in (takeover.go). The
// redundancy cost of an unreachable node is warned about, never silent — that
// is the design's "explicit warning, not a bare failure" contract.
func (c *Cluster) proposeRemoval(ctx context.Context, nodeID string) error {
	_, err := c.proposeRemovalOutcome(ctx, nodeID)
	return err
}

// proposeRemovalOutcome is proposeRemoval with the change's outcome.
func (c *Cluster) proposeRemovalOutcome(ctx context.Context, nodeID string) (membershipOutcome, error) {
	refused := membershipOutcome{kind: changeRefused}
	curEpoch, curMembers := c.epochMembers()
	var leaving *ring.Node
	for i := range curMembers {
		if curMembers[i].ID == nodeID {
			leaving = &curMembers[i]
			break
		}
	}
	if leaving == nil {
		return refused, fmt.Errorf("%w: %s", ErrNotMember, nodeID)
	}
	if nodeID == c.self.ID {
		return refused, fmt.Errorf("%w (%s); issue the removal from another member so this node can drain first", ErrRemoveSelf, nodeID)
	}
	newEpoch := curEpoch + 1
	newMembers := make([]ring.Node, 0, len(curMembers)-1)
	for _, m := range curMembers {
		if m.ID != nodeID {
			newMembers = append(newMembers, m)
		}
	}
	outcome, err := c.changeMembership(ctx, membershipProposal{epoch: newEpoch, base: curMembers,
		own: proposedSet{members: newMembers, change: *leaving}})
	if err != nil || outcome.kind != changeCommitted {
		return outcome, err
	}
	c.logger.Warn("node removed; its buckets are moving to their new owners, each of which takes its buckets over before it accepts writes to them. Reduced redundancy until then; "+
		handoffsSettledHint, "node_id", nodeID, "epoch", newEpoch, "members_remaining", len(newMembers))
	return outcome, nil
}

// maybeSweep launches a background rebalance sweep for the given epoch,
// unless auto-rebalance is disabled. Called after every adoption of a new
// membership epoch on this node — by the proposer (commitChosen), by a member
// on the change's commit and by gossip catch-up (both through adoptInOrder)
// — not just by whichever node happened to propose the change.
//
// This matters because forEachKnownBucket only sees buckets already in this
// node's own local catalog: a bucket held only by nodes B/C/D is invisible
// to a sweep run only on node A, even though A is the one that proposed
// adding a newcomer. Running sweep on every node that adopts means at least
// one node that already knows about any given bucket will always run it,
// and migrateIfMissing's second phase (ask every current replica-set member
// to pull) is what actually backfills a brand-new node — it does not need
// to discover buckets itself. Idempotent, so sweeps racing across several
// nodes for the same epoch are harmless (migrate.go's sweep doc).
func (c *Cluster) maybeSweep(epoch uint64) {
	auto := c.s.config.Server.AutoRebalance
	if !auto {
		c.logger.Info("membership epoch adopted with auto-rebalance disabled; no data was moved (run 'migrate' on each affected bucket to rebalance). "+
			"Schema changes are paused until every moved bucket is copied; 'nodes' lists the 'migrate' commands", "epoch", epoch)
	}
	sweepCtx := c.ctx
	if sweepCtx == nil {
		sweepCtx = context.Background()
	}
	if !c.startStoreGoroutine() {
		return
	}
	go func() {
		defer c.storeWG.Done()
		// The sweep moves copies; the marks of what moved are taken first.
		if err := c.waitForPlacementPass(sweepCtx); err != nil {
			return
		}
		// The worker takes over and settles what the change moved, whatever
		// auto_rebalance says (A-10.6): only a whole copy waits for it.
		c.signalTakeOver()
		if !auto {
			c.confirmUntilSettled(sweepCtx, epoch)
			return
		}
		c.sweepUntilSettled(sweepCtx, epoch)
	}()
}

// placementPassWait is how long a proposal, or a commit, waits for this
// node's placement pass before it answers that a change is in progress.
const placementPassWait = 2 * time.Second

// awaitPlacementPass waits a short while for the placement pass of this node's
// current epoch, and answers ErrEpochInProgress if it has not finished: a node
// takes no further membership change until it has marked what the last one
// moved (step 9.4).
func (c *Cluster) awaitPlacementPass() error {
	ctx, cancel := context.WithTimeout(c.ctx, placementPassWait)
	defer cancel()
	if err := c.waitForPlacementPass(ctx); err != nil || !c.placementPassFinished() {
		return fmt.Errorf("%w: this node is still marking the buckets the last membership change moved; ask again in a moment", ErrEpochInProgress)
	}
	return nil
}

// fenceUnheldBucket is the join fence: it refuses a mutation of a bucket this
// node has decided it owns and does not hold the local store of. In steady
// state an owner always holds the bucket (DDL is placed on the replica set),
// so this fires only for a node that joined mid-history and has not yet
// received the buckets it now owns, or for a write to a bucket that was never
// created — both are refused with a reason rather than silently writing into
// an empty store (which would strand the data on the previous holders).
// A create and a drop at an owner that holds no copy are refused with retry
// while this node is pulling the bucket, another member holds it, or an
// unheard member may (Cluster.refuseCreateBesideCopy, refuseDropBesideCopy;
// R8-D), before anything is logged; a create of a name
// that exists nowhere goes ahead, and a drop of one is not found. The context
// returned carries the epoch the question was asked at.
//
// It does not read the ring. The caller has decided this node is the owner,
// and fences on that decision: a fence that read the ring again could find
// another owner under a membership adopted in between and let the write
// through to a node that holds nothing. It reads the store's state
// under the bucket's lock, which a pull holds from its prepare to its attach,
// so a write that arrives mid-materialise waits for the copy.
func (c *Cluster) fenceUnheldBucket(ctx context.Context, op transport.Opcode, database, bucket string, payload []byte) (context.Context, error) {
	if bucket == "" {
		// Not a bucket-scoped op: database-level DDL has no ring owner.
		return ctx, nil
	}
	switch op {
	case transport.OpCreateBucket:
		if err := c.refuseSchemaChange(op, c.Epoch()); err != nil {
			return ctx, err
		}
		// Neither the wait nor the survey below knows a name only a removed
		// node holds (BUG-0627).
		if err := c.awaitCopyARemovedNodeMayHold(ctx, database, bucket); err != nil {
			return ctx, err
		}
		// A create of a bucket moving here waits for the take-over before it
		// asks anything, holding no fence (step 10b.3): it is answered from the
		// copy the take-over lands, or creates when none was found.
		if err := c.awaitTakeOver(ctx, placementOf(database, bucket), database, bucket); err != nil {
			return ctx, err
		}
		// The question is asked here, before the fence, and never under it
		// (R8-D, review M-4); applyAsOwner re-checks its epoch inside.
		epoch, err := c.refuseCreateBesideCopy(ctx, database, bucket, c.createFactor(payload))
		if err != nil {
			return ctx, err
		}
		return withSurveyEpoch(ctx, epoch), nil
	case transport.OpDeleteBucket:
		if err := c.refuseSchemaChange(op, c.Epoch()); err != nil {
			return ctx, err
		}
		if c.s.bucketStoreOpen(database, bucket) {
			return ctx, nil
		}
		// A drop at an owner that holds no copy is asked the create's question
		// (R8-D (c)): logged, it would reach no copy of the bucket
		// the owner has yet to copy.
		_, err := c.refuseDropBesideCopy(ctx, database, bucket)
		return ctx, err
	case transport.OpDeclareStoreFields, transport.OpAlterBucket:
		if err := c.refuseSchemaChange(op, c.Epoch()); err != nil {
			return ctx, err
		}
	}
	if c.s.bucketStoreOpen(database, bucket) {
		return ctx, nil
	}
	// The refusal names the bucket to the take-over worker: it may be one this
	// node owns since a membership change and has not been asked about yet.
	// One the worker is taking over is refused with what it waits for.
	// A name a removed node owned may be held only by it, and the worker
	// records its status a moment after the wake, so the first op waits for
	// it as the one-member door does (BUG-0636). The wait is at most
	// unheldDataOpWait per call: the forwarded-write loop ends its frame at the
	// first entry this answers with an error, and every answer here is one,
	// so a frame of N entries waits once, not N times. A name the worker has
	// recorded nothing about yet is answered R8's way (refuseRemovedOwnersName,
	// BUG-0693).
	key := placementOf(database, bucket)
	if node := c.removedFormerOwnerOf(key); node != "" {
		if c.removedOwnersWindowClosed(key) {
			return ctx, c.refuseRemovedOwnersName(key, database, bucket, node)
		}
		if err := c.wakeAndAwaitTakeOver(ctx, key, database, bucket, unheldDataOpWait); err != nil {
			return ctx, err
		}
		return ctx, c.refuseRemovedOwnersName(key, database, bucket, node)
	}
	// Any other name keeps the status-only answer: a claim of the bucket's
	// marker is also a plain join pull, which is "not yet" (awaiting_copy),
	// and a drop's retry, which the handler answers, and neither is a hand-off
	// from a removed node (BUG-0638).
	//
	// A mutation asks the members first and wakes the worker only when one may
	// hold the bucket: a name nobody holds is NOT_FOUND, and waking the worker
	// for it starts a pull that would answer the next request "copying it now"
	// (BUG-0685 for the alter, BUG-0686 for every other mutation). Any other
	// answer is a retry, which a bucket a member holds earns. This is the miss
	// path only: a held bucket returned above, before any of this.
	asked := c.refuseUnheldMutation(ctx, op, database, bucket)
	if errors.Is(asked, ErrBucketNotFound) {
		// A drop this node remembers is told as a drop, not as a typo.
		if dropped := c.missingBucketErr(database, bucket); errors.Is(dropped, ErrBucketNotFound) {
			return ctx, dropped
		}
		return ctx, asked
	}
	c.wakeTakeOver(database, bucket)
	if c.takeOverStatusOf(key).recorded() {
		return ctx, c.handoffInProgress(database, bucket)
	}
	// The question always ends in an error: a retry, or not found, returned above.
	return ctx, asked
}

// createFactor is the factor a create's payload declares, or the node
// default: the factor its replica set is placed at.
func (c *Cluster) createFactor(payload []byte) int {
	var req CreateBucketRequest
	if err := bson.Unmarshal(payload, &req); err == nil && req.ReplicationFactor > 0 {
		return req.ReplicationFactor
	}
	return c.rf
}

// fenceUnlandedRead is the join fence for a read: it refuses a read of a bucket
// this node is pulling and has not put in place, which its empty store would
// answer "database not found" or "bucket not found" — and tell the user to
// create what exists. A bucket with no pull in flight is left to the
// handler, so a database or bucket that does not exist is still not found.
//
// Like fenceUnheldBucket it does not read the ring: a caller that serves the
// read as the owner has decided so, and a forwarded read was sent here because
// its coordinator decided this node owns the bucket.
//
// A served bucket passes on bucketServed, which takes no lock: a read never
// waits behind a writer's apply (query-pipelining spec §3.5, I-1). Only a
// bucket with no id yet takes the slow path: bucketStoreOpen, which read-locks
// the bucket's record (peekSettled) — a pull holds that lock from its prepare
// to its attach (materializeRange), so a read that arrives mid-materialise
// waits for the copy and is then served from it — and then the pull registry.
//
// A restore meets the fence too. Its start creates the bucket it writes into,
// which is its ordinary starting state when no pull is in flight; with one in
// flight the pull would then find the bucket served and fail, and the bucket's
// earlier data would be lost once its old holders release it.
func (c *Cluster) fenceUnlandedRead(database, bucket string) error {
	if bucket == "" || c.s.bucketServed(database, bucket) {
		return nil
	}
	if c.s.bucketStoreOpen(database, bucket) || !c.pullPending(database, bucket) {
		return nil
	}
	return readAwaitingCopy("this node", database, bucket)
}

// readAwaitingCopy is the join fence's refusal of a read: holder owns the bucket
// and is still copying it in. It says retry, not create: the data exists, on the
// bucket's other holders.
func readAwaitingCopy(holder, database, bucket string) error {
	return &sentencedError{cause: ErrJoinSyncIncomplete, message: fmt.Sprintf(
		"%s owns bucket %q in database %q since a membership change and is still copying it from the bucket's other holders, "+
			"so it cannot answer for it yet. Retry in a moment: the copy lands on its own ('nodes' shows the members). Nothing was read",
		holder, bucket, database)}
}

// awaitingCopyAt is the join fence's refusal as a coordinator reports it: the
// owner it forwarded to holds no copy of the bucket yet. It names the owner
// where the owner's own text says "this node".
func awaitingCopyAt(owner, database, bucket string) error {
	return &sentencedError{cause: ErrJoinSyncIncomplete, message: fmt.Sprintf(
		"%s owns bucket %q of database %q and has no local copy of it: either the bucket was never created "+
			"(run 'show buckets in %s' to check), or %s joined after it existed and rebalancing has not moved the data there yet "+
			"(run 'migrate %s %s' on %s to copy it now, or 'nodes' to see the members). The data is safe on the previous owner; no write was accepted",
		owner, bucket, database, database, owner, database, bucket, owner)}
}

// missingBucketErr is the refusal for a mutation whose local bucket store is
// absent, named accurately rather than guessed: a drop still draining
// when recentDrops or a pending reclaim record says so, or the
// existing never-created/not-yet-synced text otherwise. Both fenceUnheldBucket
// (the steady-state check) and applyLocal's post-gate recheck (the
// race window, where a drop lands between the two) return through this one
// place so the wording never diverges between them.
func (c *Cluster) missingBucketErr(database, bucket string) error {
	if c.s.recentDrops.contains(database, bucket) {
		return bucketDroppedErr(database, bucket)
	}
	// recentDrops is in-memory and does not survive a restart, and it is
	// bounded, so an old enough drop can have aged out of it. The reclaim
	// record is durable and covers both: it is read second because it costs
	// a store scan and the ring almost always answers first.
	dropped, err := c.s.recentlyDroppedBucket(database, bucket)
	if err != nil {
		c.logger.Warn("checking for a recent drop of a missing bucket failed; falling back to the generic join-sync message",
			"database", database, "bucket", bucket, "error", err)
		return ErrJoinSyncIncomplete
	}
	if dropped {
		return bucketDroppedErr(database, bucket)
	}
	return ErrJoinSyncIncomplete
}

// recentDropLog is the join fence's short memory of buckets this node has
// dropped: a fixed-size ring of the most recent drops, so a write
// racing or following one still gets a message naming the drop even once its
// reclaim record has already been swept for an empty or already-drained
// bucket. It is a UX aid, not the record of truth — the reclaim record and
// the WAL are — and it costs a fixed, small amount of memory regardless of
// how many buckets this node has ever dropped (AGENTS.md root rule 11): it
// never grows with the bucket count, and it holds no per-bucket state beyond
// the ring's fixed capacity.
type recentDropLog struct {
	mu      sync.Mutex
	entries [recentDropLogCapacity]recentDropEntry
	next    int
	filled  bool
}

// recentDropLogCapacity bounds recentDropLog's memory: it is a ring, not a
// growing set, so a burst of drops (a maintenance sweep of many small
// buckets, the small-bucket target this database is built for) ages out the
// oldest entries rather than growing without limit.
const recentDropLogCapacity = 256

type recentDropEntry struct {
	database, bucket string
}

// record notes that database/bucket was just dropped on this node.
func (l *recentDropLog) record(database, bucket string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries[l.next] = recentDropEntry{database: database, bucket: bucket}
	l.next = (l.next + 1) % recentDropLogCapacity
	if l.next == 0 {
		l.filled = true
	}
}

// contains reports whether database/bucket is still within the ring's
// window of recent drops.
func (l *recentDropLog) contains(database, bucket string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	limit := l.next
	if l.filled {
		limit = recentDropLogCapacity
	}
	for i := 0; i < limit; i++ {
		if l.entries[i].database == database && l.entries[i].bucket == bucket {
			return true
		}
	}
	return false
}

// recentlyDroppedBucket reports whether a pending reclaim record names
// (database, bucket) — the signal that a missing local bucket store is a drop
// still draining rather than a bucket this node has yet to receive. It runs
// only on the refusal path a write already took because its local store was
// missing, so its cost is O(pending reclaim records) — normally none, and
// bounded by outstanding drops, never by the bucket count — not O(buckets)
// (AGENTS.md root rule 11).
func (s *Server) recentlyDroppedBucket(database, bucket string) (bool, error) {
	lower, upper := reclaimRecordsRange()
	snap := s.kv.NewSnapshot()
	defer snap.Discard()
	it := snap.NewIterator(storage.IterOptions{Lower: lower, Upper: upper})
	defer it.Close()
	for it.Seek(nil); it.Valid(); it.Next() {
		if len(it.Key()) != reclaimRecordKeyLen {
			continue
		}
		encoded, err := it.Value()
		if err != nil {
			return false, fmt.Errorf("reading the reclaim record at %x: %w", it.Key(), err)
		}
		var record reclaimRecord
		if err := bson.Unmarshal(encoded, &record); err != nil {
			return false, fmt.Errorf("decoding the reclaim record at %x: %w", it.Key(), err)
		}
		if record.Database == database && record.Name == bucket {
			return true, nil
		}
	}
	if err := it.Err(); err != nil {
		return false, fmt.Errorf("walking the reclaim records: %w", err)
	}
	return false, nil
}

func memberIDSetOf(members []ring.Node) map[string]struct{} {
	set := make(map[string]struct{}, len(members))
	for _, m := range members {
		set[m.ID] = struct{}{}
	}
	return set
}

func peersToNodes(ps []Peer) []ring.Node {
	if len(ps) == 0 {
		return nil
	}
	nodes := make([]ring.Node, 0, len(ps))
	for _, p := range ps {
		nodes = append(nodes, ring.Node{ID: p.ID, Addr: p.Addr})
	}
	return nodes
}

// bucketStoreOpen reports whether this node serves the given bucket, which is
// what the join fence keys on: the in-memory entry exists and has an id. An id
// is set only once the bucket's create has committed or its catalog entry has
// attached, with its files open. "The bucket exists in the map" does not
// count: claimBucket links a bare record before the create runs. Nor
// can it be "has an engine handle": once an engine moves into the node store
// it has none of its own, and every bucket of it would read as not received.
func (s *Server) bucketStoreOpen(database, bucket string) bool {
	dd := s.serverDataAccessor()
	if dd == nil {
		return false
	}
	facts, held := s.peekSettled(database, bucket)
	return held && facts.id != 0
}

// bucketServed is bucketStoreOpen without the bucket's lock, for the read path
// (query-pipelining spec §3.5, I-1): the id is stored once the bucket's files
// are in place, and readView serves a bucket on the same load. A false answer
// can be a bucket mid-materialise; a caller that must wait for one asks
// bucketStoreOpen.
func (s *Server) bucketServed(database, bucket string) bool {
	facts, held := s.peekBucket(database, bucket)
	return held && facts.id != 0
}
