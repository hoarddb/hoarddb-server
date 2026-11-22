package server

import (
	"fmt"
	"slices"

	"github.com/hoarddb/hoarddb-server/ring"
)

// A membership change is single-decree Paxos per epoch (L. Lamport, "Paxos
// Made Simple", 2001, §2.2 and §2.3): the decree is the member set of the
// next epoch, and the acceptors are the members of the committed set of this
// one. A member first accepts a proposed set, durably and acting on nothing,
// and adopts it only once a majority has accepted it at one ballot. So at
// most one set is ever adopted at an epoch, and a proposer lost mid-change
// leaves only acceptances, which a later ballot finishes.

// ballot orders proposals: Round first, then Node compared as byte strings.
// Node ids are addresses, so which node wins a tie is chosen by no one. It is
// written to membership.json and carried on the wire, so it has both tags.
type ballot struct {
	Round uint64 `json:"round" bson:"round"`
	Node  string `json:"node" bson:"node"`
}

// less reports whether b orders before other.
func (b ballot) less(other ballot) bool {
	if b.Round != other.Round {
		return b.Round < other.Round
	}
	return b.Node < other.Node
}

// atMost reports whether b orders at or before other.
func (b ballot) atMost(other ballot) bool {
	return !other.less(b)
}

// pendingChange is this node's acceptor state for the epoch after its own.
// Placement, gossip and take-over never read it: an accepted set that is not
// known to be chosen must act on nothing.
type pendingChange struct {
	// Epoch is the epoch being decided: this node's epoch + 1.
	Epoch uint64 `json:"epoch"`
	// Promised is the highest ballot this node promised or accepted at.
	Promised ballot `json:"promised"`
	// Accepted is the ballot of the set this node accepted, if any.
	Accepted *ballot `json:"accepted,omitempty"`
	// Members is the set it accepted.
	Members []ring.Node `json:"members,omitempty"`
	// Change is the member that set adds or removes.
	Change ring.Node `json:"change"`
}

// clone copies p so a caller can change the copy without touching the
// record another holds.
func (p *pendingChange) clone() *pendingChange {
	if p == nil {
		return nil
	}
	copied := *p
	if p.Accepted != nil {
		accepted := *p.Accepted
		copied.Accepted = &accepted
	}
	copied.Members = append([]ring.Node(nil), p.Members...)
	return &copied
}

// restorePending takes the acceptance membership.json recorded, at start. Only
// an acceptance for the epoch after the loaded one is this node's: every
// writer clears it when it adopts that epoch, so one for any other epoch is a
// record no writer of this build left, and it is dropped (the next write
// drops it from disk). A node that recorded its removal restores none: it is
// no acceptor, and clearing its acceptance was what its removal did.
func (c *Cluster) restorePending(persisted *pendingChange, removed bool) {
	if persisted == nil {
		return
	}
	epoch := c.Epoch()
	if removed || persisted.Epoch != epoch+1 {
		c.logger.Warn("membership.json holds a promise for a membership epoch this node is not deciding; it is dropped",
			"pending_epoch", persisted.Epoch, "epoch", epoch, "removed", removed)
		return
	}
	c.membershipFileMu.Lock()
	c.pending = persisted
	c.membershipFileMu.Unlock()
	c.restoredAcceptance = persisted.Accepted != nil
}

// The phases of a membership change, as the wire names them.
const (
	phasePrepare = "prepare"
	phaseAccept  = "accept"
	phaseCommit  = "commit"
)

// answerKind is a member's answer to one phase of a membership change.
type answerKind string

// The answers, as the wire names them.
const (
	answerPromised = answerKind("promised")
	answerAccepted = answerKind("accepted")
	answerAdopted  = answerKind("adopted")
	// answerBehind: the member is on an older epoch than the change needs.
	answerBehind = answerKind("behind")
	// answerNotAdopted: a commit the member could not adopt.
	answerNotAdopted = answerKind("not_adopted")
	// answerAhead: the member has already adopted the epoch being decided,
	// and reports the set it adopted.
	answerAhead   = answerKind("ahead")
	answerRemoved = answerKind("removed")
	// answerOutpromised: refused because the member promised a ballot at or
	// above the request's. Bare, with no acceptance attached, it is what a
	// lost proposer leaves behind, which one retry at a higher round gets
	// past; nothing else is that.
	answerOutpromised = answerKind("outpromised")
	// answerRefused: any other refusal, such as another base set, a set
	// that is not one change, or a test seam's.
	answerRefused = answerKind("refused")
	// answerOldBinary: the member answered with an error frame, as a build
	// without this protocol does.
	answerOldBinary = answerKind("old_binary")
)

// acceptorView is what the acceptor rules read of a node, under its
// membership file lock.
type acceptorView struct {
	// epoch and members are the node's committed epoch and set.
	epoch   uint64
	members []ring.Node
	removed bool
	pending *pendingChange
	// adoptedNext is the set the node adopted at the epoch a request
	// decides, when the node is at or past it and its history holds it.
	adoptedNext []ring.Node
}

// acceptorRequest is one phase of a membership change as the rules read it.
type acceptorRequest struct {
	phase string
	// epoch is the epoch being decided.
	epoch  uint64
	ballot ballot
	// baseMembers is the proposer's committed set, on a prepare.
	baseMembers []string
	// members and change are the proposed set and the node it adds or
	// removes, on an accept.
	members []ring.Node
	change  ring.Node
}

// acceptorAnswer is what the rules answer, and the record to save before the
// answer leaves when save is set.
type acceptorAnswer struct {
	// from is the member that answered, when the answer came over the wire.
	from   string
	kind   answerKind
	reason string
	// saveErr is why this node could not save its own answer to disk; set
	// only on the proposer's answer to itself, where it is a local failure
	// and not a disagreement with a peer.
	saveErr error
	// epoch is the answering node's committed epoch.
	epoch    uint64
	promised ballot
	accepted *ballot
	// members and change are the answering node's acceptance, if any.
	members []ring.Node
	change  ring.Node
	// adoptedNext is the set the node adopted at the decided epoch, on
	// answerAhead.
	adoptedNext []ring.Node
	save        bool
	pending     *pendingChange
}

// acceptorStep applies the acceptor's rules to one prepare or accept: rule E
// (the epoch), then rule P (prepare: promise only above every promise) or
// rule A (accept: only at or above the promise, and raise the promise to the
// ballot). It is Lamport's acceptor with the epoch rule added, and keeps:
//
//   - I-1: the accepted ballot is at most the promised one;
//   - I-2: within one epoch neither ever decreases;
//   - I-3: a node never promises or accepts for an epoch it has adopted, or
//     once it is removed, so dropping the record then is safe.
func acceptorStep(view acceptorView, req acceptorRequest) acceptorAnswer {
	if answer, decided := epochRule(view, req); decided {
		return answer
	}
	// A record for an earlier epoch is one an adoption should have
	// dropped: it counts as none, and the epoch starts from the zero ballot.
	current := &pendingChange{Epoch: req.epoch}
	if view.pending != nil && view.pending.Epoch == req.epoch {
		current = view.pending.clone()
	}
	answer := acceptorAnswer{epoch: view.epoch, promised: current.Promised, accepted: current.Accepted,
		members: current.Members, change: current.Change}
	switch req.phase {
	case phasePrepare:
		if !sameMemberIDs(memberIDsOf(view.members), req.baseMembers) {
			answer.kind = answerRefused
			answer.reason = fmt.Sprintf("this node's membership epoch %d is %v, and the change was prepared against %v",
				view.epoch, memberIDsOf(view.members), req.baseMembers)
			return answer
		}
		if req.ballot.atMost(current.Promised) {
			answer.kind = answerOutpromised
			answer.reason = fmt.Sprintf("this node has promised ballot %d/%s for membership epoch %d", current.Promised.Round, current.Promised.Node, req.epoch)
			return answer
		}
		current.Promised = req.ballot
		answer.kind = answerPromised
	case phaseAccept:
		// That the set is one change of this node's, proposed by a member,
		// is checked before the rules, with the admission checks.
		if req.ballot.less(current.Promised) {
			answer.kind = answerOutpromised
			answer.reason = fmt.Sprintf("this node has promised ballot %d/%s for membership epoch %d", current.Promised.Round, current.Promised.Node, req.epoch)
			return answer
		}
		accepted := req.ballot
		current.Promised = accepted
		current.Accepted = &accepted
		current.Members = slices.Clone(req.members)
		current.Change = req.change
		answer.kind = answerAccepted
		answer.accepted = current.Accepted
		answer.members = current.Members
		answer.change = current.Change
	default:
		answer.kind = answerRefused
		answer.reason = fmt.Sprintf("membership phase %q is not one the acceptor takes", req.phase)
		return answer
	}
	answer.promised = current.Promised
	answer.save = true
	answer.pending = current
	return answer
}

// epochRule is rule E: a removed node refuses every phase; a node at or past
// the epoch being decided refuses as ahead, reporting the set it adopted,
// except that it answers an accept of exactly that set accepted, recording
// nothing; a node behind it refuses as behind. decided is false when the node
// is on the epoch before the decided one, where rules P and A apply.
func epochRule(view acceptorView, req acceptorRequest) (acceptorAnswer, bool) {
	answer := acceptorAnswer{epoch: view.epoch}
	switch {
	case view.removed:
		answer.kind = answerRemoved
		answer.reason = fmt.Sprintf("this node was removed from the cluster at membership epoch %d", view.epoch)
	case view.epoch >= req.epoch:
		if req.phase == phaseAccept && view.adoptedNext != nil &&
			sameMemberIDs(memberIDsOf(view.adoptedNext), memberIDsOf(req.members)) {
			answer.kind = answerAccepted
			return answer, true
		}
		answer.kind = answerAhead
		answer.adoptedNext = view.adoptedNext
		answer.reason = fmt.Sprintf("this node is at membership epoch %d, past epoch %d", view.epoch, req.epoch)
	case view.epoch+1 < req.epoch:
		answer.kind = answerBehind
		answer.reason = fmt.Sprintf("this node is at membership epoch %d, behind the change to epoch %d", view.epoch, req.epoch)
	default:
		return acceptorAnswer{}, false
	}
	return answer, true
}

// sameMemberIDs reports whether two lists name the same members.
func sameMemberIDs(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	set := make(map[string]struct{}, len(a))
	for _, id := range a {
		set[id] = struct{}{}
	}
	for _, id := range b {
		if _, listed := set[id]; !listed {
			return false
		}
	}
	return true
}

// allocateBallot is the proposer's next ballot for epoch: a round above every
// round its record holds for that epoch and above seenRound, the highest a
// refusal reported in this command, with this node's id. The caller then
// promises it to itself in the same lock hold, so rule P cannot refuse it.
func allocateBallot(pending *pendingChange, epoch, seenRound uint64, self string) ballot {
	round := seenRound
	if pending != nil && pending.Epoch == epoch {
		round = max(round, pending.Promised.Round)
		if pending.Accepted != nil {
			round = max(round, pending.Accepted.Round)
		}
	}
	return ballot{Round: round + 1, Node: self}
}

// proposedSet is a value a proposer offers: a member set and the node it adds
// or removes.
type proposedSet struct {
	members []ring.Node
	change  ring.Node
}

// chooseValue is the value rule: among the promises the proposer used, its own
// included, the acceptance with the highest ballot, if any, is what it must
// propose; otherwise its own. another reports that the set offered is not the
// proposer's own change.
func chooseValue(own proposedSet, promises []acceptorAnswer) (offered proposedSet, another bool) {
	var highest *ballot
	offered = own
	for _, promise := range promises {
		if promise.accepted == nil || (highest != nil && !highest.less(*promise.accepted)) {
			continue
		}
		highest = promise.accepted
		offered = proposedSet{members: promise.members, change: promise.change}
	}
	return offered, highest != nil && !sameMemberIDs(memberIDsOf(offered.members), memberIDsOf(own.members))
}

// phaseTally counts one phase's answers. The phase succeeds once a majority of
// the epoch's members said yes, counting the proposer only on its own yes,
// and fails once the members that refused or could not be reached exceed the
// size less a majority: a refusal from a minority does not stop it.
type phaseTally struct {
	size, needed int
	want         answerKind
	yes          []acceptorAnswer
	failed       int
	// bareOnly stays true while every refusal is a bare outpromise, which
	// is what the one retry of a prepare is for.
	bareOnly     bool
	refusals     int
	highestRound uint64
	// ahead is the first answer from a member past the epoch.
	ahead *acceptorAnswer
	// oldBinary counts members that answered with an error frame.
	oldBinary int
}

// newPhaseTally counts a phase among size members that wants answer want.
func newPhaseTally(size int, want answerKind) *phaseTally {
	return &phaseTally{size: size, needed: size/2 + 1, want: want, bareOnly: true}
}

// add counts one member's answer; reachable is false when it could not be
// asked or did not answer.
func (tally *phaseTally) add(answer acceptorAnswer, reachable bool) {
	if !reachable {
		tally.failed++
		return
	}
	tally.highestRound = max(tally.highestRound, answer.promised.Round)
	if answer.accepted != nil {
		tally.highestRound = max(tally.highestRound, answer.accepted.Round)
	}
	switch answer.kind {
	case tally.want:
		tally.yes = append(tally.yes, answer)
		return
	case answerAhead:
		if tally.ahead == nil {
			copied := answer
			tally.ahead = &copied
		}
	case answerOldBinary:
		tally.oldBinary++
	}
	tally.failed++
	tally.refusals++
	if answer.kind != answerOutpromised || answer.accepted != nil {
		tally.bareOnly = false
	}
}

// chosen reports whether a majority said yes.
func (tally *phaseTally) chosen() bool { return len(tally.yes) >= tally.needed }

// lost reports whether a majority can no longer say yes.
func (tally *phaseTally) lost() bool { return tally.failed > tally.size-tally.needed }

// selfVerdict is how a proposer reads its own acceptor's answer to its own
// self-promise or self-accept: only its own yes lets it go on, and a yes it
// did not get is never counted.
type selfVerdict int

const (
	selfProceed selfVerdict = iota
	// selfAhead: this node has adopted the epoch; the command ends by the
	// adopted set (5b).
	selfAhead
	// selfRefused: this node refused its own ballot, or is removed; the
	// command ends before anything leaves it.
	selfRefused
)

// countSelf adds the proposer's own answer to its phase, as a yes only when it
// is one, and reports how the proposer goes on. A refused self-promise or
// self-accept ends the attempt before anything leaves the node: counting it
// anyway lets two sets be chosen.
func (tally *phaseTally) countSelf(answer acceptorAnswer) selfVerdict {
	verdict := readSelfAnswer(answer, tally.want)
	if verdict == selfProceed {
		tally.yes = append(tally.yes, answer)
	}
	return verdict
}

// readSelfAnswer reads the proposer's own answer, wanting want.
func readSelfAnswer(answer acceptorAnswer, want answerKind) selfVerdict {
	switch answer.kind {
	case want:
		return selfProceed
	case answerAhead:
		return selfAhead
	default:
		return selfRefused
	}
}
