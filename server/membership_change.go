package server

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/transport"
)

// A membership change runs in three phases between the members of the
// committed epoch E (membership_paxos.go): prepare (promise a ballot), accept
// (record the set, acting on nothing), commit (the set is chosen: adopt it).
// One opcode carries all three, OpMembershipChange, named by a phase field.

// The outcomes of a membership change that can fail after it was offered.
var (
	// ErrMembershipChangeIndeterminate is a change that was offered to the
	// members but not confirmed by a majority before the command stopped: it
	// may still complete, by a member's recovery.
	ErrMembershipChangeIndeterminate = errors.New("the membership change was offered but not confirmed; it may still complete")
	// ErrMembershipCommittedNotAdoptedHere is a change a majority chose that
	// this node could not adopt: the change is made, and this node is behind.
	ErrMembershipCommittedNotAdoptedHere = errors.New("the membership change is committed, but this node could not adopt it")
	// ErrAnotherMembershipChangeFinished is a command that found another
	// change accepted and unfinished, finished it, and did not make its own.
	ErrAnotherMembershipChangeFinished = errors.New("another membership change was finished instead of this one")
)

// errSelfAcceptRefused is a change whose proposer's own acceptor refused its
// offer: another member's change started meanwhile. It is a refusal like
// ErrEpochInProgress's, and a recovery that meets it backs off.
var errSelfAcceptRefused = fmt.Errorf("%w: another member started a membership change for the epoch", ErrEpochInProgress)

// MembershipChangeRequest is one phase of a membership change. A field a
// phase does not use is left empty. No top-level field may be named bucket,
// database, name, key, id, writeconcern or write_concern: an older build
// reads those from any frame and would route this one as a bucket request.
type MembershipChangeRequest struct {
	Token    string `bson:"token"`
	FromNode string `bson:"from_node"`
	// Phase is "prepare", "accept" or "commit".
	Phase string `bson:"phase"`
	// Epoch is the epoch being decided.
	Epoch uint64 `bson:"epoch"`
	// Ballot orders proposals; on a commit it is the chosen one, for the
	// record only.
	Ballot ballot `bson:"ballot"`
	// BaseMembers is the proposer's committed set, on a prepare.
	BaseMembers []string `bson:"base_members,omitempty"`
	// Members is the proposed set, on an accept and a commit.
	Members []Peer `bson:"members,omitempty"`
	// Change is the node the proposed set adds or removes, on an accept.
	Change *Peer `bson:"change,omitempty"`
	// History is, on a commit to a join's newcomer only, the proposer's
	// membership history from the founding epoch (historyAbove(InitialEpoch -
	// 1)), so the newcomer that adopts its join from the commit holds the
	// cluster's record of its provisional epoch and knows its buckets'
	// previous owners (on-demand pull spec §6.7, stop-and-ask 15). Internode
	// only; no commit to an existing member carries it.
	History []MembershipRecord `bson:"history,omitempty"`
}

// MembershipChangeResponse is a member's answer to one phase; only the
// proposer reads it.
type MembershipChangeResponse struct {
	// Answer is promised, accepted, adopted, behind, not_adopted, ahead,
	// removed, outpromised or refused.
	Answer string `bson:"answer"`
	Reason string `bson:"reason,omitempty"`
	// Epoch is the answering node's committed epoch.
	Epoch    uint64  `bson:"epoch"`
	Promised ballot  `bson:"promised"`
	Accepted *ballot `bson:"accepted,omitempty"`
	// Members and Change are the answering node's acceptance, if any.
	Members []Peer `bson:"members,omitempty"`
	Change  *Peer  `bson:"change,omitempty"`
	// CommittedMembers is, on ahead, the set the node adopted at the
	// decided epoch.
	CommittedMembers []Peer `bson:"committed_members,omitempty"`
}

// membershipAnswerWait bounds one prepare or accept exchange with one member:
// a dial, and an accept's admission checks, which can ask a joining node
// whether its store is sealed. A member that has not answered by then counts
// as unreachable.
const membershipAnswerWait = peerDialTimeout + 7*time.Second

// commitFanOutBound bounds the commit round as a whole: a dial, the commit
// handler's own wait for its placement pass, and a margin of one dial for
// the adoption. A member that has not answered by then counts as unreachable
// and learns the change by gossip. A node in only one of the two epochs, a
// join's newcomer or the node a removal takes out, is told best effort within
// one dial: a newcomer that is not listening yet learns the change by gossip,
// and a partitioned removed node at its next exchange with a member.
const commitFanOutBound = peerDialTimeout + placementPassWait + peerDialTimeout

// handleMembershipChange answers one phase of a membership change.
func handleMembershipChange(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	c := s.cluster
	if c == nil {
		return nodeResponse(transport.OpMembershipChange, frame,
			MembershipChangeResponse{Answer: string(answerRefused), Reason: "replication is not enabled on this node"}, nil)
	}
	var req MembershipChangeRequest
	noteRequestDecode("handleMembershipChange", frame.Opcode, frame.Payload)
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return nodeResponse(transport.OpMembershipChange, frame,
			MembershipChangeResponse{Answer: string(answerRefused), Reason: fmt.Sprintf("decode membership change: %v", err)}, nil)
	}
	if !c.ValidateToken(req.Token) {
		c.logger.Warn("membership change refused: invalid cluster token", "from", req.FromNode)
		return nodeResponse(transport.OpMembershipChange, frame,
			MembershipChangeResponse{Answer: string(answerRefused), Reason: "invalid cluster token"}, nil)
	}
	if err := c.refuseSenderNotOfConnection(ctx, transport.OpMembershipChange, req.FromNode); err != nil {
		return nodeResponse(transport.OpMembershipChange, frame,
			MembershipChangeResponse{Answer: string(answerRefused), Reason: err.Error(), Epoch: c.Epoch()}, nil)
	}
	if hook := membershipPhaseHook.Load(); hook != nil {
		if err := (*hook)(c.self.ID, req.Phase, req.FromNode, req.Epoch); err != nil {
			return nodeResponse(transport.OpMembershipChange, frame,
				MembershipChangeResponse{Answer: string(answerRefused), Reason: err.Error(), Epoch: c.Epoch()}, nil)
		}
	}
	var resp MembershipChangeResponse
	switch req.Phase {
	case phasePrepare:
		resp, _ = c.prepareMembershipChange(req)
	case phaseAccept:
		resp, _ = c.acceptMembershipChange(req)
	case phaseCommit:
		resp, _ = c.commitMembershipDelivered(req)
	default:
		resp = MembershipChangeResponse{Answer: string(answerRefused), Epoch: c.Epoch(),
			Reason: fmt.Sprintf("membership phase %q is not one this node knows", req.Phase)}
	}
	return nodeResponse(transport.OpMembershipChange, frame, resp, nil)
}

// prepareMembershipChange is rule P on this node: promise the ballot, or say
// why not. The error is nil only on a promise.
func (c *Cluster) prepareMembershipChange(req MembershipChangeRequest) (MembershipChangeResponse, error) {
	answer := c.runAcceptorRules(acceptorRequest{phase: phasePrepare, epoch: req.Epoch, ballot: req.Ballot,
		baseMembers: req.BaseMembers})
	return responseOf(answer), answerError(answer, answerPromised)
}

// acceptMembershipChange is rule A on this node: record the proposed set,
// acting on nothing, or say why not. The admission checks come first and
// outside the file lock, since one of them asks a joining node over the
// network; they are asked only while this node is on the epoch the change
// starts from, because a node elsewhere is answered by the epoch rule. The
// error is nil only on an acceptance, and wraps the admission check's
// sentinel when that refused.
func (c *Cluster) acceptMembershipChange(req MembershipChangeRequest) (MembershipChangeResponse, error) {
	members := peersToNodes(req.Members)
	var change ring.Node
	if req.Change != nil {
		change = ring.Node{ID: req.Change.ID, Addr: memberAddr(members, *req.Change)}
	}
	if epoch, _ := c.epochMembers(); !c.removed() && req.Epoch == epoch+1 {
		if err := c.admitProposedSet(req.FromNode, members, change); err != nil {
			return MembershipChangeResponse{Answer: string(answerRefused), Reason: err.Error(), Epoch: epoch}, err
		}
	}
	answer := c.runAcceptorRules(acceptorRequest{phase: phaseAccept, epoch: req.Epoch, ballot: req.Ballot,
		members: members, change: change})
	return responseOf(answer), answerError(answer, answerAccepted)
}

// admitProposedSet is what a member checks of a proposed set before it
// accepts it: the proposer is a member, the set is this node's with one node
// added or removed, a sealed node takes nothing from a plaintext one, and a
// join finds disk headroom here and a newcomer whose store this cluster can
// use.
func (c *Cluster) admitProposedSet(fromNode string, members []ring.Node, change ring.Node) error {
	_, current := c.epochMembers()
	if err := oneChangeOf(current, members, change, fromNode); err != nil {
		return err
	}
	// encryption.mode=require takes no membership change from a plaintext
	// member, as it takes no gossip from one (handleGossip).
	if proposer, ok := c.memberByID(fromNode); ok {
		if err := c.refuseMembershipWithPlaintextPeer(c.ctx, proposer, "accept a membership change proposed by "+describePeer(proposer)); err != nil {
			return err
		}
	}
	if len(members) < len(current) {
		return nil
	}
	// A disk near capacity votes against a join. The vote is this node's
	// only, and a majority can still admit the member; what keeps bytes off
	// a full filesystem is the pull's own guard (refuseLandingOnFullDisk),
	// which covers a removal's re-homed buckets too. A removal is never
	// refused on headroom.
	if !c.diskHeadroomOK() {
		return c.diskFullJoinError()
	}
	// A sealed member asks the newcomer too, and refuses a plaintext one as
	// the proposer would: a plaintext proposer does not ask.
	return c.admitNewcomer(c.ctx, change)
}

// runAcceptorRules applies the acceptor rules under the membership file lock
// and saves the record they leave before the answer goes.
func (c *Cluster) runAcceptorRules(req acceptorRequest) acceptorAnswer {
	// Copied before the file lock: settleMu is never taken under it.
	settledOf := c.settledOfSnapshot()
	c.membershipFileMu.Lock()
	defer c.membershipFileMu.Unlock()
	return c.acceptorStepLocked(req, settledOf)
}

// acceptorStepLocked is the acceptor's critical section: read the epoch, the
// committed set and the record, apply the rules, save, then hold the new
// record in memory. No adoption can complete in between, because an adoption
// and a removal both move the view inside their own hold of this lock. The
// caller holds membershipFileMu and took settledOf before it.
func (c *Cluster) acceptorStepLocked(req acceptorRequest, settledOf map[string]uint64) acceptorAnswer {
	epoch, members := c.epochMembers()
	view := acceptorView{epoch: epoch, members: members, removed: c.removed(), pending: c.pending}
	if epoch >= req.epoch {
		if adopted, recorded := c.membersAt(req.epoch); recorded {
			view.adoptedNext = adopted
		}
	}
	answer := acceptorStep(view, req)
	if !answer.save {
		return answer
	}
	if err := c.writeMembershipRecordLocked(membershipWrite{
		epoch: epoch, members: members, settledOf: settledOf, bootstrapped: c.bootstrapped.Load(), pending: answer.pending,
	}); err != nil {
		refusal := acceptorAnswer{kind: answerRefused, epoch: epoch,
			reason: fmt.Sprintf("this node could not save its answer to membership.json: %v", err), saveErr: err}
		if c.pending != nil && c.pending.Epoch == req.epoch {
			refusal.promised, refusal.accepted = c.pending.Promised, c.pending.Accepted
		}
		return refusal
	}
	c.pending = answer.pending
	if answer.kind == answerAccepted {
		c.armRecovery()
	}
	return answer
}

// commitMembershipDelivered is rule C: the set is chosen, and this node
// adopts it. It waits at most placementPassWait for its own placement pass,
// so a member with a long pass never holds the proposer; it then answers
// behind and adopts by gossip later. The answer is read from the epoch the
// node is at afterwards, because adoptInOrder returns nil both when it
// adopted and when it only woke gossip. The error is nil when the node is at
// the epoch.
func (c *Cluster) commitMembershipDelivered(req MembershipChangeRequest) (MembershipChangeResponse, error) {
	members := peersToNodes(req.Members)
	if !nodeListed(members, req.FromNode) {
		if _, member := c.memberIDSet()[req.FromNode]; !member {
			err := fmt.Errorf("%w: %s, which is not a member, sent a commit of membership epoch %d", ErrMemberMismatch, req.FromNode, req.Epoch)
			return MembershipChangeResponse{Answer: string(answerRefused), Reason: err.Error(), Epoch: c.Epoch()}, err
		}
	}
	if current := c.Epoch(); current >= req.Epoch {
		return c.commitAtAdoptedEpoch(req, members)
	}
	waitCtx, cancel := context.WithTimeout(c.ctx, placementPassWait)
	err := c.waitForPlacementPass(waitCtx)
	cancel()
	if err != nil || !c.placementPassFinished() {
		return MembershipChangeResponse{Answer: string(answerBehind), Epoch: c.Epoch(),
			Reason: "this node is still marking the buckets the last membership change moved; it adopts this one by gossip"}, nil
	}
	// A newcomer's commit carries the history (deliverCommits): it adopts the
	// epochs it carries in order, and the cluster's record of its provisional
	// epoch with its join. Any other commit carries none, as before.
	history := peerGossip{}
	if len(req.History) > 0 {
		history = peerGossip{records: req.History, answered: true}
	}
	if err := c.adoptInOrder(req.FromNode, c.Epoch(), req.Epoch, members, history); err != nil {
		return MembershipChangeResponse{Answer: string(answerNotAdopted), Reason: err.Error(), Epoch: c.Epoch()},
			fmt.Errorf("adopting committed membership epoch %d: %w", req.Epoch, err)
	}
	if epoch := c.Epoch(); epoch < req.Epoch {
		return MembershipChangeResponse{Answer: string(answerBehind), Epoch: epoch,
			Reason: fmt.Sprintf("this node is at membership epoch %d and catches up to %d by gossip", epoch, req.Epoch)}, nil
	}
	return MembershipChangeResponse{Answer: string(answerAdopted), Epoch: c.Epoch()}, nil
}

// commitAtAdoptedEpoch answers a commit of an epoch this node has adopted:
// the same set is delivered; another set cannot happen while at most one set
// is chosen per epoch, so it is a divergence, logged and flagged.
func (c *Cluster) commitAtAdoptedEpoch(req MembershipChangeRequest, members []ring.Node) (MembershipChangeResponse, error) {
	current := c.Epoch()
	recorded, known := c.membersAt(req.Epoch)
	switch {
	case !known:
		return MembershipChangeResponse{Answer: string(answerAhead), Epoch: current,
			Reason: fmt.Sprintf("this node is at membership epoch %d and no longer remembers epoch %d", current, req.Epoch)}, nil
	case memberSetsEqual(memberIDSetOf(recorded), memberIDSetOf(members)):
		return MembershipChangeResponse{Answer: string(answerAdopted), Epoch: current}, nil
	}
	err := fmt.Errorf("%w: membership epoch %d is %v on this node, and %s committed %v at it",
		ErrMemberMismatch, req.Epoch, memberIDsOf(recorded), req.FromNode, memberIDsOf(members))
	c.mismatch.Store(true)
	c.logger.Error("two member sets were committed at one membership epoch", "epoch", req.Epoch, "from", req.FromNode, "error", err)
	return MembershipChangeResponse{Answer: string(answerRefused), Reason: err.Error(), Epoch: current,
		CommittedMembers: nodesToPeers(recorded)}, err
}

// responseOf is the wire form of an acceptor's answer.
func responseOf(answer acceptorAnswer) MembershipChangeResponse {
	resp := MembershipChangeResponse{Answer: string(answer.kind), Reason: answer.reason, Epoch: answer.epoch,
		Promised: answer.promised, Accepted: answer.accepted, Members: nodesToPeers(answer.members),
		CommittedMembers: nodesToPeers(answer.adoptedNext)}
	if answer.change.ID != "" {
		resp.Change = &Peer{ID: answer.change.ID, Addr: answer.change.Addr}
	}
	return resp
}

// answerOf reads a member's answer from the wire.
func answerOf(from string, resp MembershipChangeResponse) acceptorAnswer {
	answer := acceptorAnswer{from: from, kind: answerKind(resp.Answer), reason: resp.Reason, epoch: resp.Epoch,
		promised: resp.Promised, accepted: resp.Accepted, members: peersToNodes(resp.Members),
		adoptedNext: peersToNodes(resp.CommittedMembers)}
	if resp.Change != nil {
		answer.change = ring.Node{ID: resp.Change.ID, Addr: resp.Change.Addr}
	}
	return answer
}

// answerError is nil for the answer wanted, and says why otherwise.
func answerError(answer acceptorAnswer, want answerKind) error {
	if answer.kind == want {
		return nil
	}
	return fmt.Errorf("%w: membership change answered %s: %s", ErrMemberMismatch, answer.kind, answer.reason)
}

// membershipProposal is one change a proposer runs: the set offered for the
// epoch after the committed one, against that committed set.
type membershipProposal struct {
	// epoch is the epoch being decided, E+1.
	epoch uint64
	// base is the committed set at E.
	base []ring.Node
	// own is this command's own change.
	own proposedSet
	// recovery marks a proposer finishing an acceptance it holds.
	recovery bool
}

// changeOutcome is how a membership change ended, as the counter labels it.
type changeOutcome string

// The outcomes of a membership change.
const (
	changeCommitted        = changeOutcome("committed")
	changeFinishedAnother  = changeOutcome("finished_another")
	changeAlreadyCommitted = changeOutcome("already_committed")
	changeRefused          = changeOutcome("refused")
	changeNoMajority       = changeOutcome("no_majority")
	changeIndeterminate    = changeOutcome("indeterminate")
)

// membershipOutcome is a membership change's outcome and, for a success that
// changed nothing, the message the operator is told.
type membershipOutcome struct {
	kind    changeOutcome
	message string
}

// outcomeError is a membership change's error: the text an operator reads,
// matching the sentinel it wraps.
type outcomeError struct {
	text     string
	sentinel error
}

func (e *outcomeError) Error() string { return e.text }
func (e *outcomeError) Unwrap() error { return e.sentinel }

// describeChange names a change as an operator reads it: "removing n5" or
// "adding n5".
func describeChange(base []ring.Node, offered proposedSet) string {
	if len(offered.members) < len(base) {
		return "removing " + offered.change.ID
	}
	return "adding " + offered.change.ID
}

// changeOf is the change that takes base to members, for a set reported by
// another member.
func changeOf(base, members []ring.Node) proposedSet {
	baseIDs, memberIDs := memberIDSetOf(base), memberIDSetOf(members)
	for _, member := range members {
		if _, listed := baseIDs[member.ID]; !listed {
			return proposedSet{members: members, change: member}
		}
	}
	for _, member := range base {
		if _, listed := memberIDs[member.ID]; !listed {
			return proposedSet{members: members, change: member}
		}
	}
	return proposedSet{members: members}
}

// changeMembership runs one membership change from this node, and counts its
// outcome.
func (c *Cluster) changeMembership(ctx context.Context, proposal membershipProposal) (membershipOutcome, error) {
	outcome, err := c.proposeChange(ctx, proposal)
	c.membershipChanges.WithLabelValues(string(outcome.kind)).Inc()
	return outcome, err
}

// proposeChange is a membership change from this node: its preconditions,
// then prepare, accept and commit (membership_paxos.go). One change runs from
// a node at a time.
func (c *Cluster) proposeChange(ctx context.Context, proposal membershipProposal) (membershipOutcome, error) {
	if err := c.awaitPlacementPass(); err != nil {
		return membershipOutcome{kind: changeRefused}, err
	}
	if !c.transition.CompareAndSwap(false, true) {
		return membershipOutcome{kind: changeRefused}, ErrEpochInProgress
	}
	defer c.transition.Store(false)
	return c.runMembershipChange(ctx, proposal)
}

// runMembershipChange is the proposer, holding transition.
func (c *Cluster) runMembershipChange(ctx context.Context, proposal membershipProposal) (membershipOutcome, error) {
	promises, at, ended, err := c.preparePhase(ctx, proposal)
	if ended != nil {
		return *ended, err
	}
	offered, another := chooseValue(proposal.own, promises.yes)

	if hook := membershipPhaseHook.Load(); hook != nil {
		if err := (*hook)(c.self.ID, "self-accept", c.self.ID, proposal.epoch); err != nil {
			return membershipOutcome{kind: changeRefused}, fmt.Errorf("membership change not made: %w", err)
		}
	}
	acceptRequest := acceptorRequest{phase: phaseAccept, epoch: proposal.epoch, ballot: at,
		members: offered.members, change: offered.change}
	accepts := newPhaseTally(len(proposal.base), answerAccepted)
	own := c.runAcceptorRules(acceptRequest)
	switch accepts.countSelf(own) {
	case selfAhead:
		return c.finishAhead(proposal, own)
	case selfRefused:
		if own.saveErr != nil {
			return membershipOutcome{kind: changeRefused}, c.saveFailure("acceptance", own.saveErr)
		}
		// A competitor's higher prepare reached this node since its own
		// promise: nothing has left it, so nothing changed.
		return membershipOutcome{kind: changeRefused}, &outcomeError{sentinel: errSelfAcceptRefused, text: fmt.Sprintf(
			"membership change not made: another member (%s) started a membership change for epoch %d while this one was being prepared, "+
				"so nothing was offered and nothing changed. Run 'nodes' to see the result, then run your command again if it is still wanted",
			own.promised.Node, proposal.epoch)}
	}
	c.runPhase(ctx, othersThan(proposal.base, c.self.ID), c.wireRequest(phaseAccept, proposal, at, offered), accepts)
	if accepts.ahead != nil {
		return c.finishAhead(proposal, *accepts.ahead)
	}
	if !accepts.chosen() {
		return membershipOutcome{kind: changeIndeterminate}, indeterminateError(proposal, offered, another)
	}
	return c.commitChosen(proposal, at, offered, another)
}

// preparePhase allocates a ballot, promises it to itself, and asks the other
// members of the committed epoch to promise it. It returns the promises it
// used once a majority has promised, or ends the command: on an ahead
// answer, on a refusal of its own promise, or when a majority can no longer
// promise. A phase lost only to bare higher promises, which a lost proposer
// leaves behind, is tried once more at a round above them.
func (c *Cluster) preparePhase(ctx context.Context, proposal membershipProposal) (*phaseTally, ballot, *membershipOutcome, error) {
	var seenRound uint64
	for attempt := 0; ; attempt++ {
		at, own := c.selfPromise(proposal, seenRound)
		promises := newPhaseTally(len(proposal.base), answerPromised)
		switch promises.countSelf(own) {
		case selfAhead:
			outcome, err := c.finishAhead(proposal, own)
			return nil, at, &outcome, err
		case selfRefused:
			if own.saveErr != nil {
				return nil, at, &membershipOutcome{kind: changeRefused}, c.saveFailure("promise", own.saveErr)
			}
			return nil, at, &membershipOutcome{kind: changeRefused},
				fmt.Errorf("membership change not made: %w", answerError(own, answerPromised))
		}
		c.runPhase(ctx, othersThan(proposal.base, c.self.ID), c.wireRequest(phasePrepare, proposal, at, proposal.own), promises)
		switch {
		case promises.ahead != nil:
			outcome, err := c.finishAhead(proposal, *promises.ahead)
			return nil, at, &outcome, err
		case promises.chosen():
			return promises, at, nil, nil
		case attempt == 0 && promises.bareOnly && promises.refusals > 0:
			seenRound = promises.highestRound
			c.logger.Info("members had promised a higher ballot for this membership epoch, left by a change that did not finish; asking again at a higher one",
				"epoch", proposal.epoch, "round", seenRound+1)
			continue
		}
		return nil, at, &membershipOutcome{kind: changeNoMajority}, &outcomeError{sentinel: ErrJoinFailedMajority, text: fmt.Sprintf(
			"membership change not made: only %d of %d members of epoch %d promised (need %d), so nothing was offered and nothing changed. "+
				"Run 'nodes' to see which members are down, then run the command again",
			len(promises.yes), len(proposal.base), proposal.epoch-1, promises.needed)}
	}
}

// saveFailure is the error for a proposer that could not write its own answer
// to membership.json. It is a local fault, so it names the file and the cause
// and does not claim a peer disagrees.
func (c *Cluster) saveFailure(answer string, cause error) error {
	return fmt.Errorf("membership change not made: this node could not save its own %s to %s, so nothing was sent and nothing changed: %w. "+
		"Check that the disk holding the data directory has free space and that the user running hoarddb-server can write to it, then run the command again",
		answer, c.membershipPath(), cause)
}

// selfPromise allocates the proposer's ballot and promises it to itself in one
// hold of the file lock, so the round is above every ballot the record holds
// and rule P cannot refuse it; only the epoch rule can.
func (c *Cluster) selfPromise(proposal membershipProposal, seenRound uint64) (ballot, acceptorAnswer) {
	settledOf := c.settledOfSnapshot()
	c.membershipFileMu.Lock()
	defer c.membershipFileMu.Unlock()
	at := allocateBallot(c.pending, proposal.epoch, seenRound, c.self.ID)
	return at, c.acceptorStepLocked(acceptorRequest{phase: phasePrepare, epoch: proposal.epoch, ballot: at,
		baseMembers: memberIDsOf(proposal.base)}, settledOf)
}

// wireRequest is one phase's request as it is sent.
func (c *Cluster) wireRequest(phase string, proposal membershipProposal, at ballot, offered proposedSet) MembershipChangeRequest {
	req := MembershipChangeRequest{Token: c.localToken(), FromNode: c.self.ID, Phase: phase, Epoch: proposal.epoch, Ballot: at}
	switch phase {
	case phasePrepare:
		req.BaseMembers = memberIDsOf(proposal.base)
	case phaseAccept:
		req.Members = nodesToPeers(offered.members)
		req.Change = &Peer{ID: offered.change.ID, Addr: offered.change.Addr}
	case phaseCommit:
		req.Members = nodesToPeers(offered.members)
	}
	return req
}

// othersThan is members without self.
func othersThan(members []ring.Node, self string) []ring.Node {
	others := make([]ring.Node, 0, len(members))
	for _, member := range members {
		if member.ID != self {
			others = append(others, member)
		}
	}
	return others
}

// runPhase asks targets for one phase at once and counts their answers into
// tally. It returns as soon as the phase is decided: a majority said yes, a
// majority can no longer say yes, or a member reported the epoch adopted. The
// stragglers are cancelled through the phase's context; an answer that comes
// later is never used.
func (c *Cluster) runPhase(ctx context.Context, targets []ring.Node, req MembershipChangeRequest, tally *phaseTally) {
	if tally.chosen() || tally.lost() {
		return
	}
	phaseCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	type memberReply struct {
		answer    acceptorAnswer
		reachable bool
	}
	// Buffered for every target, so a straggler that answers after the
	// phase returned never blocks.
	replies := make(chan memberReply, len(targets))
	for _, target := range targets {
		go func() {
			answer, reachable := c.askMember(phaseCtx, target, req, membershipAnswerWait)
			replies <- memberReply{answer: answer, reachable: reachable}
		}()
	}
	for range targets {
		reply := <-replies
		tally.add(reply.answer, reply.reachable)
		if tally.ahead != nil || tally.chosen() || tally.lost() {
			return
		}
	}
}

// askMember sends one phase to one member and reads its answer; reachable is
// false when it could not be asked or did not answer within wait.
func (c *Cluster) askMember(ctx context.Context, target ring.Node, req MembershipChangeRequest, wait time.Duration) (acceptorAnswer, bool) {
	if hook := membershipSendHook.Load(); hook != nil {
		if err := (*hook)(ctx, c.self.ID, target.ID, req.Phase); err != nil {
			c.logger.Debug("membership change message not sent", "node_id", target.ID, "phase", req.Phase, "error", err)
			return acceptorAnswer{from: target.ID}, false
		}
	}
	askCtx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	payload, err := bson.Marshal(req)
	if err != nil {
		c.logger.Warn("could not encode a membership change message", "node_id", target.ID, "error", err)
		return acceptorAnswer{from: target.ID}, false
	}
	cli, err := c.peerFor(target).dial(askCtx)
	if err != nil {
		c.logger.Debug("membership change: member unreachable", "node_id", target.ID, "phase", req.Phase, "error", err)
		return acceptorAnswer{from: target.ID}, false
	}
	frame, err := cli.SendRequest(askCtx, transport.LaneControl, transport.OpMembershipChange, payload)
	if err != nil {
		c.logger.Debug("membership change: member did not answer", "node_id", target.ID, "phase", req.Phase, "error", err)
		return acceptorAnswer{from: target.ID}, false
	}
	if frame.Opcode == transport.OpError {
		c.noteOldMembershipPeer(target.ID, frame)
		return acceptorAnswer{from: target.ID, kind: answerOldBinary, reason: "the member does not implement membership changes"}, true
	}
	var resp MembershipChangeResponse
	if err := decodePeerAnswer(frame.Payload, &resp); err != nil {
		c.logger.Warn("membership change: undecodable answer", "node_id", target.ID, "error", err)
		return acceptorAnswer{from: target.ID}, false
	}
	return answerOf(target.ID, resp), true
}

// noteOldMembershipPeer logs, once per member, that it answered a membership
// change with an error frame: a build without this protocol answers
// "unsupported opcode", and membership changes need every member upgraded.
func (c *Cluster) noteOldMembershipPeer(nodeID string, frame *transport.Frame) {
	if _, logged := c.oldMembershipPeers.LoadOrStore(nodeID, struct{}{}); logged {
		return
	}
	c.logger.Warn(fmt.Sprintf("member %s runs a build without this release's membership protocol; upgrade every member before adding or removing nodes", nodeID),
		"node_id", nodeID, "answer", decodeErrorText(frame))
}

// decodeErrorText is the message an error frame carries, or its raw bytes; an
// error frame the depth scan refuses is not read.
func decodeErrorText(frame *transport.Frame) string {
	var errorBody struct {
		Message string `bson:"message"`
	}
	err := decodePeerAnswer(frame.Payload, &errorBody)
	switch {
	case err == nil && errorBody.Message != "":
		return errorBody.Message
	case errors.Is(err, ErrDocumentTooDeep):
		return err.Error()
	}
	return string(frame.Payload)
}

// commitChosen is the set chosen: adopt it here, tell every member of both
// epochs, and report. A local adoption that fails does not stop the commits:
// the set is chosen, and the other members must learn it.
func (c *Cluster) commitChosen(proposal membershipProposal, at ballot, offered proposedSet, another bool) (membershipOutcome, error) {
	if hook := membershipPhaseHook.Load(); hook != nil {
		// A test holds the proposer here; the set is chosen whatever it
		// answers.
		_ = (*hook)(c.self.ID, "chosen", c.self.ID, proposal.epoch)
	}
	caughtUp, adoptErr := c.adoptChosen(proposal.epoch, offered.members)
	answers := c.deliverCommits(proposal, at, offered)
	c.logger.Debug("the members answered the commit", "epoch", proposal.epoch, "answers", fmt.Sprint(answers))
	if adoptErr == nil && !caughtUp {
		c.maybeSweep(proposal.epoch)
	}
	change := describeChange(proposal.base, offered)
	c.logger.Info("cluster membership change committed", "epoch", proposal.epoch, "members", len(offered.members), "change", change)
	if proposal.recovery {
		c.logger.Warn("finished a membership change a member had accepted and no proposer finished",
			"change", change, "epoch", proposal.epoch, "proposed_by", at.Node)
	}
	switch {
	case adoptErr != nil:
		return membershipOutcome{kind: changeCommitted}, &outcomeError{sentinel: ErrMembershipCommittedNotAdoptedHere, text: fmt.Sprintf(
			"the change (%s) is committed at membership epoch %d, but this node could not adopt it: %v. The other members have it, "+
				"and this node catches up by gossip once the cause is fixed. Run 'nodes' against another member to see the new membership",
			change, proposal.epoch, adoptErr)}
	case another:
		return membershipOutcome{kind: changeFinishedAnother}, &outcomeError{sentinel: ErrAnotherMembershipChangeFinished, text: fmt.Sprintf(
			"another membership change (%s) was in flight and is now committed at epoch %d; your change (%s) was not made. "+
				"Run 'nodes', and run your command again if it is still wanted",
			change, proposal.epoch, describeChange(proposal.base, proposal.own))}
	}
	return membershipOutcome{kind: changeCommitted}, nil
}

// adoptChosen adopts a chosen set on the proposer. A set that does not list
// this node is its removal, which a recovery on the removed node can finish:
// that goes through the catch-up's removal branch, as gossip would. caughtUp
// says the node was already at the epoch, or the catch-up swept for it.
func (c *Cluster) adoptChosen(epoch uint64, members []ring.Node) (caughtUp bool, err error) {
	if !nodeListed(members, c.self.ID) {
		return true, c.adoptInOrder(c.self.ID, epoch-1, epoch, members, peerGossip{})
	}
	err = c.adoptMembership(epoch, members)
	if errors.Is(err, errAdoptionNotAhead) {
		// Gossip took this node to the epoch, or past it, meanwhile: the
		// catch-up adopted and swept, and the view stays where it is.
		return true, c.committedAsProposed(epoch, members)
	}
	if err != nil {
		return false, fmt.Errorf("adopting membership epoch %d: %w", epoch, err)
	}
	return false, nil
}

// deliverCommits sends the chosen set to every member of both epochs, the
// node a removal takes out and a join's newcomer included, and waits for each
// reachable member's answer, within commitFanOutBound, and counts the answers
// by kind ("unreachable" for a member that did not answer). No answer changes
// the outcome: the set was chosen before the first commit left.
func (c *Cluster) deliverCommits(proposal membershipProposal, at ballot, offered proposedSet) map[answerKind]int {
	ctx, cancel := context.WithTimeout(c.ctx, commitFanOutBound)
	defer cancel()
	targets := othersThan(proposal.base, c.self.ID)
	for _, member := range offered.members {
		if member.ID != c.self.ID && !nodeListed(targets, member.ID) {
			targets = append(targets, member)
		}
	}
	req := c.wireRequest(phaseCommit, proposal, at, offered)
	// A join's newcomer adopts its join from this commit before it gossips,
	// so the commit hands it the history a gossip answer would (spec §6.7).
	newcomerReq := req
	newcomerReq.History = c.historyAbove(InitialEpoch - 1)
	var answersMu sync.Mutex
	answers := make(map[answerKind]int)
	var commits sync.WaitGroup
	for _, target := range targets {
		wait := commitFanOutBound
		if !nodeListed(proposal.base, target.ID) || !nodeListed(offered.members, target.ID) {
			wait = peerDialTimeout
		}
		targetReq := req
		if !nodeListed(proposal.base, target.ID) && nodeListed(offered.members, target.ID) {
			targetReq = newcomerReq
		}
		commits.Add(1)
		go func() {
			defer commits.Done()
			answer, reachable := c.askMember(ctx, target, targetReq, wait)
			c.noteCommitAnswer(target, proposal.epoch, answer, reachable)
			kind := answer.kind
			if !reachable {
				kind = "unreachable"
			}
			answersMu.Lock()
			answers[kind]++
			answersMu.Unlock()
		}()
	}
	commits.Wait()
	return answers
}

// noteCommitAnswer logs one member's answer to a commit.
func (c *Cluster) noteCommitAnswer(target ring.Node, epoch uint64, answer acceptorAnswer, reachable bool) {
	switch {
	case !reachable:
		c.logger.Warn(fmt.Sprintf("member %s did not answer the commit of membership epoch %d; it learns it by gossip", target.ID, epoch),
			"node_id", target.ID, "epoch", epoch)
	case answer.kind == answerAdopted:
	case answer.kind == answerBehind:
		c.logger.Debug("a member will adopt the committed membership by gossip", "node_id", target.ID, "epoch", epoch, "reason", answer.reason)
	case answer.kind == answerNotAdopted:
		c.logger.Warn(fmt.Sprintf("member %s could not adopt membership epoch %d: %s; it catches up by gossip once the cause is fixed",
			target.ID, epoch, answer.reason), "node_id", target.ID, "epoch", epoch)
	default:
		c.logger.Error("a member answered the commit of a chosen membership with a refusal", "node_id", target.ID,
			"epoch", epoch, "answer", answer.kind, "reason", answer.reason)
	}
}

// finishAhead is a proposer that heard that the epoch is already adopted:
// it adopts the set reported, as a commit would (through the catch-up, which
// also takes a set without this node as its removal), and compares it with
// its own change. The same set is a re-run of a change already made.
func (c *Cluster) finishAhead(proposal membershipProposal, answer acceptorAnswer) (membershipOutcome, error) {
	reported := answer.adoptedNext
	if reported != nil {
		from := answer.from
		if from == "" {
			from = c.self.ID
		}
		if err := c.adoptInOrder(from, proposal.epoch-1, proposal.epoch, reported, peerGossip{}); err != nil {
			c.logger.Warn("could not adopt the membership a member reported committed; this node catches up by gossip",
				"epoch", proposal.epoch, "from", from, "error", err)
		}
	}
	if reported != nil && sameMemberIDs(memberIDsOf(reported), memberIDsOf(proposal.own.members)) {
		return membershipOutcome{kind: changeAlreadyCommitted, message: fmt.Sprintf(
			"this change is already committed: membership epoch %d is the change (%s). Nothing more to do; run 'nodes' to see the membership",
			proposal.epoch, describeChange(proposal.base, proposal.own))}, nil
	}
	committed := "another change"
	if reported != nil {
		committed = describeChange(proposal.base, changeOf(proposal.base, reported))
	}
	return membershipOutcome{kind: changeRefused}, &outcomeError{sentinel: ErrMemberMismatch, text: fmt.Sprintf(
		"this node was behind: membership epoch %d is already committed (%s). Run 'nodes', and run your command again if it is still wanted",
		proposal.epoch, committed)}
}

// indeterminateError is a change that was offered and not confirmed: it may
// still complete, by a member's recovery.
func indeterminateError(proposal membershipProposal, offered proposedSet, another bool) error {
	offeredChange := describeChange(proposal.base, offered)
	listed := "still listed"
	if len(offered.members) > len(proposal.base) {
		listed = "listed"
	}
	text := fmt.Sprintf("the change (%s) was offered to the members of epoch %d, but this command stopped before a majority confirmed it. "+
		"It usually completes within a minute, and within about five minutes while members are unreachable. "+
		"Run 'nodes' to see whether %s is %s before you try again", offeredChange, proposal.epoch-1, offered.change.ID, listed)
	if another {
		text = fmt.Sprintf("another membership change (%s) was in flight; this command offered it to finish it, but stopped before a majority "+
			"confirmed it. It usually completes within a minute, and within about five minutes while members are unreachable. "+
			"Your change (%s) was not made. Run 'nodes' to see whether %s is %s, then run your command again if it is still wanted",
			offeredChange, describeChange(proposal.base, proposal.own), offered.change.ID, listed)
	}
	return &outcomeError{sentinel: ErrMembershipChangeIndeterminate, text: text}
}

// oneChangeOf checks that proposed is base with exactly the change added or
// removed, and that the proposer is a member of base.
func oneChangeOf(base, proposed []ring.Node, change ring.Node, fromNode string) error {
	if !nodeListed(base, fromNode) {
		return fmt.Errorf("%w: proposer %s is not a member of the current membership epoch", ErrMemberMismatch, fromNode)
	}
	baseIDs, proposedIDs := memberIDSetOf(base), memberIDSetOf(proposed)
	_, inBase := baseIDs[change.ID]
	_, inProposed := proposedIDs[change.ID]
	switch {
	case len(proposed) == len(base)+1 && !inBase && inProposed:
		delete(proposedIDs, change.ID)
	case len(proposed) == len(base)-1 && inBase && !inProposed:
		delete(baseIDs, change.ID)
	default:
		return fmt.Errorf("%w: the proposed set %v is not the current set %v with %s added or removed; "+
			"a membership change adds or removes exactly one node", ErrMemberMismatch, memberIDsOf(proposed), memberIDsOf(base), change.ID)
	}
	if !memberSetsEqual(baseIDs, proposedIDs) {
		return fmt.Errorf("%w: the proposed set %v changes more than %s against the current set %v",
			ErrMemberMismatch, memberIDsOf(proposed), change.ID, memberIDsOf(base))
	}
	return nil
}
