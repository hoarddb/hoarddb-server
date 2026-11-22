package server

import (
	"flag"
	"fmt"
	"math/rand"
	"slices"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/ring"
)

// The acceptor rules and the proposer's use of them, tested as a pure state
// machine: no network, no clock, no lock, no disk. A small world of five
// acceptors runs the production rules (acceptorStep, allocateBallot,
// chooseValue, phaseTally, readSelfAnswer); the named schedules each fail
// when one rule is broken by hand, and the explorer runs random schedules.

var (
	membershipExplorerSeed = flag.Int64("seed", 0,
		"replay one schedule of TestTheMembershipAcceptorExplorer by its seed (0: the default seeds)")
	membershipExplorerSchedules = flag.Int("explorer-schedules", 4000,
		"how many seeded schedules TestTheMembershipAcceptorExplorer runs")
)

// simEpoch is the epoch every simulated change decides; the acceptors start
// one before it.
const simEpoch = InitialEpoch + 1

// simNode is one acceptor's state, as membership.json and its view hold it.
type simNode struct {
	id      string
	epoch   uint64
	members []ring.Node
	pending *pendingChange
	history map[uint64][]ring.Node
}

// simWorld is five acceptors, the proposers running against them, and the
// messages between them when delivery is scheduled rather than direct.
type simWorld struct {
	nodes []*simNode
	base  []ring.Node
	// direct delivers every message as it is sent; otherwise messages wait
	// in pool until the schedule delivers or drops them.
	direct bool
	pool   []*simMessage
	// acceptedAt is, per ballot, the set each acceptor recorded at it.
	acceptedAt map[ballot]map[string]string
	chosen     []string
	adopted    map[string]bool
	violations []string
	outcomes   map[string]int
}

// simMessage is one request from a proposer to an acceptor, or one answer
// back.
type simMessage struct {
	proposer *simProposer
	attempt  int
	to       *simNode
	request  acceptorRequest
	// answer is set on an answer travelling back.
	answer *acceptorAnswer
}

// newSimWorld makes five acceptors a0 to a4 on the first epoch.
func newSimWorld(direct bool) *simWorld {
	world := &simWorld{direct: direct, acceptedAt: make(map[ballot]map[string]string),
		adopted: make(map[string]bool), outcomes: make(map[string]int)}
	for index := 0; index < 5; index++ {
		world.base = append(world.base, ring.Node{ID: fmt.Sprintf("a%d", index), Addr: fmt.Sprintf("a%d", index)})
	}
	for _, member := range world.base {
		world.nodes = append(world.nodes, &simNode{id: member.ID, epoch: simEpoch - 1, members: slices.Clone(world.base),
			history: map[uint64][]ring.Node{simEpoch - 1: slices.Clone(world.base)}})
	}
	return world
}

// joining is the set that adds newcomer to the world's first set.
func (world *simWorld) joining(newcomer string) proposedSet {
	change := ring.Node{ID: newcomer, Addr: newcomer}
	return proposedSet{members: append(slices.Clone(world.base), change), change: change}
}

// setKey names a member set by its sorted ids.
func setKey(members []ring.Node) string {
	ids := memberIDsOf(members)
	slices.Sort(ids)
	return strings.Join(ids, ",")
}

// violate records that M1 or an invariant failed.
func (world *simWorld) violate(format string, args ...any) {
	world.violations = append(world.violations, fmt.Sprintf(format, args...))
}

// handle runs the acceptor rules on node for request and checks the
// invariants on the record they leave.
func (world *simWorld) handle(node *simNode, request acceptorRequest) acceptorAnswer {
	view := acceptorView{epoch: node.epoch, members: node.members, pending: node.pending}
	if node.epoch >= request.epoch {
		view.adoptedNext = node.history[request.epoch]
	}
	before := node.pending.clone()
	answer := acceptorStep(view, request)
	if !answer.save {
		return answer
	}
	node.pending = answer.pending
	world.checkRecord(node, before)
	if answer.kind == answerAccepted {
		world.recordAccept(request.ballot, node.id, request.members)
	}
	return answer
}

// checkRecord holds I-1 (accepted at most promised) and I-2 (within an epoch
// neither ever decreases) after one acceptor transition.
func (world *simWorld) checkRecord(node *simNode, before *pendingChange) {
	after := node.pending
	if after.Accepted != nil && !after.Accepted.atMost(after.Promised) {
		world.violate("I-1 on %s: accepted %v above promised %v", node.id, *after.Accepted, after.Promised)
	}
	if before == nil || before.Epoch != after.Epoch {
		return
	}
	if after.Promised.less(before.Promised) {
		world.violate("I-2 on %s: promised went from %v down to %v", node.id, before.Promised, after.Promised)
	}
	if before.Accepted != nil && (after.Accepted == nil || after.Accepted.less(*before.Accepted)) {
		world.violate("I-2 on %s: accepted went from %v down to %v", node.id, *before.Accepted, after.Accepted)
	}
}

// recordAccept logs an acceptance and checks that a ballot carries one set
// and that at most one set is chosen.
func (world *simWorld) recordAccept(at ballot, nodeID string, members []ring.Node) {
	key := setKey(members)
	byNode := world.acceptedAt[at]
	if byNode == nil {
		byNode = make(map[string]string)
		world.acceptedAt[at] = byNode
	}
	for other, otherKey := range byNode {
		if otherKey != key {
			world.violate("two sets at one ballot %v: %s on %s, %s on %s", at, otherKey, other, key, nodeID)
		}
	}
	byNode[nodeID] = key
	count := 0
	for _, recorded := range byNode {
		if recorded == key {
			count++
		}
	}
	if count >= len(world.nodes)/2+1 && !slices.Contains(world.chosen, key) {
		world.chosen = append(world.chosen, key)
		if len(world.chosen) > 1 {
			world.violate("two sets chosen at epoch %d: %v", simEpoch, world.chosen)
		}
	}
}

// adopt moves node to members at epoch, as a commit or a proposer's own
// adoption does, and checks that no node, and no two nodes, adopt two sets
// at one epoch.
func (world *simWorld) adopt(node *simNode, epoch uint64, members []ring.Node) {
	if node.epoch >= epoch {
		if recorded := node.history[epoch]; setKey(recorded) != setKey(members) {
			world.violate("%s adopted %s at epoch %d and is then told %s", node.id, setKey(recorded), epoch, setKey(members))
		}
		return
	}
	if node.epoch+1 < epoch {
		return
	}
	node.epoch = epoch
	node.members = slices.Clone(members)
	node.history[epoch] = node.members
	if node.pending != nil && node.pending.Epoch <= epoch {
		node.pending = nil
	}
	world.adopted[setKey(members)] = true
	if len(world.adopted) > 1 {
		world.violate("two sets adopted at epoch %d: %v", epoch, adoptedKeys(world.adopted))
	}
}

// adoptedKeys lists the adopted sets, sorted.
func adoptedKeys(set map[string]bool) []string {
	var keys []string
	for key := range set {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}

// node finds an acceptor by id.
func (world *simWorld) node(id string) *simNode {
	for _, node := range world.nodes {
		if node.id == id {
			return node
		}
	}
	return nil
}

// simProposer is one proposer: an operator's command or a recovery on a
// home acceptor, or, for the schedules that need none, a proposer with no
// acceptor of its own.
type simProposer struct {
	world    *simWorld
	name     string
	home     *simNode
	own      proposedSet
	recovery bool
	at       ballot
	attempt  int
	// stage is where the proposer is: "prepare", "self-accept" (waiting to
	// accept its own offer), "accept", or "done".
	stage     string
	tally     *phaseTally
	heard     map[string]bool
	offered   proposedSet
	another   bool
	retried   bool
	seenRound uint64
	outcome   string
	lost      bool
}

// propose makes a proposer of own; home is nil for one with no acceptor.
func (world *simWorld) propose(name string, home *simNode, own proposedSet) *simProposer {
	return &simProposer{world: world, name: name, home: home, own: own}
}

// others is every acceptor but the proposer's home.
func (proposer *simProposer) others() []*simNode {
	var others []*simNode
	for _, node := range proposer.world.nodes {
		if node != proposer.home {
			others = append(others, node)
		}
	}
	return others
}

// begin allocates a ballot and, on a proposer with a home, promises it to
// itself in the same step; it reports whether the prepare phase is open.
func (proposer *simProposer) begin() bool {
	var record *pendingChange
	if proposer.home != nil {
		record = proposer.home.pending
	}
	return proposer.beginAt(allocateBallot(record, simEpoch, proposer.seenRound, proposer.name))
}

// beginAt opens the prepare phase at a given ballot.
func (proposer *simProposer) beginAt(at ballot) bool {
	proposer.at = at
	proposer.attempt++
	proposer.stage = phasePrepare
	proposer.tally = newPhaseTally(len(proposer.world.nodes), answerPromised)
	proposer.heard = make(map[string]bool)
	if proposer.home == nil {
		return true
	}
	answer := proposer.world.handle(proposer.home, proposer.prepareRequest())
	proposer.heard[proposer.home.id] = true
	switch proposer.tally.countSelf(answer) {
	case selfAhead:
		proposer.finishAhead(answer)
		return false
	case selfRefused:
		proposer.finish("refused")
		return false
	}
	return true
}

func (proposer *simProposer) prepareRequest() acceptorRequest {
	return acceptorRequest{phase: phasePrepare, epoch: simEpoch, ballot: proposer.at, baseMembers: memberIDsOf(proposer.world.base)}
}

func (proposer *simProposer) acceptRequest() acceptorRequest {
	return acceptorRequest{phase: phaseAccept, epoch: simEpoch, ballot: proposer.at,
		members: proposer.offered.members, change: proposer.offered.change}
}

// send sends the proposer's current phase to the named acceptors.
func (proposer *simProposer) send(ids ...string) {
	for _, id := range ids {
		node := proposer.world.node(id)
		request := proposer.prepareRequest()
		if proposer.stage == phaseAccept {
			request = proposer.acceptRequest()
		}
		message := &simMessage{proposer: proposer, attempt: proposer.attempt, to: node, request: request}
		if proposer.world.direct {
			proposer.world.deliver(message)
			continue
		}
		proposer.world.pool = append(proposer.world.pool, message)
	}
}

// sendAll sends the current phase to every other acceptor.
func (proposer *simProposer) sendAll() {
	for _, node := range proposer.others() {
		proposer.send(node.id)
	}
}

// deliver hands a message to its acceptor, or an answer to its proposer.
func (world *simWorld) deliver(message *simMessage) {
	if message.answer == nil {
		if message.request.phase == phaseCommit {
			world.adopt(message.to, message.request.epoch, message.request.members)
			return
		}
		answer := world.handle(message.to, message.request)
		reply := &simMessage{proposer: message.proposer, attempt: message.attempt, to: message.to,
			request: message.request, answer: &answer}
		if world.direct {
			world.deliver(reply)
			return
		}
		world.pool = append(world.pool, reply)
		return
	}
	message.proposer.receive(message)
}

// receive counts one answer, discarding any that the phase no longer wants:
// a phase that has returned uses only the answers that ended it.
func (proposer *simProposer) receive(message *simMessage) {
	if proposer.lost || message.attempt != proposer.attempt || message.request.phase != proposer.stage ||
		proposer.heard[message.to.id] {
		return
	}
	proposer.heard[message.to.id] = true
	proposer.count(*message.answer, true)
}

// timeOut counts every acceptor the current phase has not heard from as
// unreachable.
func (proposer *simProposer) timeOut() {
	for _, node := range proposer.others() {
		if proposer.stage != phasePrepare && proposer.stage != phaseAccept {
			return
		}
		if !proposer.heard[node.id] {
			proposer.heard[node.id] = true
			proposer.count(acceptorAnswer{}, false)
		}
	}
}

// count adds one answer to the phase and moves the proposer on when the
// phase is decided.
func (proposer *simProposer) count(answer acceptorAnswer, reachable bool) {
	proposer.tally.add(answer, reachable)
	switch {
	case proposer.tally.ahead != nil:
		proposer.finishAhead(*proposer.tally.ahead)
	case proposer.tally.chosen() && proposer.stage == phasePrepare:
		proposer.offered, proposer.another = chooseValue(proposer.own, proposer.tally.yes)
		proposer.stage = "self-accept"
	case proposer.tally.chosen():
		proposer.commit()
	case proposer.tally.lost() && proposer.stage == phasePrepare:
		if proposer.tally.bareOnly && !proposer.retried {
			proposer.retried = true
			proposer.seenRound = max(proposer.seenRound, proposer.tally.highestRound)
			if proposer.begin() {
				proposer.sendAll()
			}
			return
		}
		proposer.finish("no_majority")
	case proposer.tally.lost():
		proposer.finish("indeterminate")
	}
}

// selfAccept accepts the offered set at the proposer's own acceptor, then
// opens the accept phase; it reports whether the phase is open. A proposer
// with no home only opens it.
func (proposer *simProposer) selfAccept() bool {
	proposer.stage = phaseAccept
	proposer.tally = newPhaseTally(len(proposer.world.nodes), answerAccepted)
	proposer.heard = map[string]bool{}
	proposer.attempt++
	if proposer.home == nil {
		return true
	}
	answer := proposer.world.handle(proposer.home, proposer.acceptRequest())
	proposer.heard[proposer.home.id] = true
	switch proposer.tally.countSelf(answer) {
	case selfAhead:
		proposer.finishAhead(answer)
		return false
	case selfRefused:
		proposer.finish("refused")
		return false
	}
	return true
}

// commit is the set chosen: the proposer adopts it and sends the commits.
func (proposer *simProposer) commit() {
	if proposer.home != nil {
		proposer.world.adopt(proposer.home, simEpoch, proposer.offered.members)
	}
	if proposer.another {
		proposer.finish("finished_another")
	} else {
		proposer.finish("committed")
	}
	for _, node := range proposer.others() {
		message := &simMessage{proposer: proposer, to: node,
			request: acceptorRequest{phase: phaseCommit, epoch: simEpoch, members: proposer.offered.members}}
		if proposer.world.direct {
			continue
		}
		proposer.world.pool = append(proposer.world.pool, message)
	}
}

// commitTo delivers the proposer's commit to the named acceptors at once.
func (proposer *simProposer) commitTo(ids ...string) {
	for _, id := range ids {
		proposer.world.adopt(proposer.world.node(id), simEpoch, proposer.offered.members)
	}
}

// finishAhead is 5b: the proposer adopts the set a member reports it adopted
// and compares it with its own change.
func (proposer *simProposer) finishAhead(answer acceptorAnswer) {
	if proposer.home != nil && answer.adoptedNext != nil {
		proposer.world.adopt(proposer.home, simEpoch, answer.adoptedNext)
	}
	if answer.adoptedNext != nil && setKey(answer.adoptedNext) == setKey(proposer.own.members) {
		proposer.finish("already_committed")
		return
	}
	proposer.finish("refused_ahead")
}

// finish ends the proposer with an outcome.
func (proposer *simProposer) finish(outcome string) {
	proposer.stage = "done"
	proposer.outcome = outcome
	proposer.world.outcomes[outcome]++
}

// assertNoViolation fails the test with the world's violations.
func (world *simWorld) assertNoViolation(t *testing.T) {
	t.Helper()
	for _, violation := range world.violations {
		t.Error(violation)
	}
}

// mustSelfAccept moves a proposer whose prepare phase has returned on to its
// accept phase; it fails the test if the prepare phase has not returned.
func mustSelfAccept(t *testing.T, proposer *simProposer) bool {
	t.Helper()
	if proposer.stage != "self-accept" {
		t.Fatalf("test setup: %s is at %q (outcome %q), not ready to accept", proposer.name, proposer.stage, proposer.outcome)
	}
	return proposer.selfAccept()
}

// TestTheMembershipAcceptorSchedules runs the named schedules, each against
// the production rules; each row names the rule whose break by hand makes it
// fail.
func TestTheMembershipAcceptorSchedules(t *testing.T) {
	t.Run("M-1, five acceptors (break: rule A without Promised = b)", func(t *testing.T) {
		world := newSimWorld(true)
		p1 := world.propose("p1", nil, world.joining("x1"))
		p2 := world.propose("p2", nil, world.joining("x2"))
		p3 := world.propose("p3", nil, world.joining("x3"))
		p1.beginAt(ballot{Round: 1, Node: "p1"})
		p1.send("a0", "a1", "a2")
		p2.beginAt(ballot{Round: 1, Node: "p2"})
		p2.send("a2", "a3", "a4")
		mustSelfAccept(t, p2)
		p2.send("a0", "a1", "a3", "a4")
		if p2.outcome != "committed" {
			t.Fatalf("test setup: p2 ended %q, want committed", p2.outcome)
		}
		mustSelfAccept(t, p1)
		p1.send("a0", "a1")
		p3.beginAt(ballot{Round: 2, Node: "p3"})
		p3.send("a0", "a1", "a2")
		mustSelfAccept(t, p3)
		p3.send("a0", "a1", "a2")
		if setKey(p3.offered.members) != setKey(p2.offered.members) {
			t.Errorf("p3 offered %s; %s was chosen at a lower ballot", setKey(p3.offered.members), setKey(p2.offered.members))
		}
		world.assertNoViolation(t)
	})

	t.Run("duplicate ballot (break: rule P refusing only b < Promised)", func(t *testing.T) {
		world := newSimWorld(true)
		a0 := world.node("a0")
		first := world.propose("a0", a0, world.joining("x1"))
		second := world.propose("a0", a0, world.joining("x2"))
		duplicate := allocateBallot(a0.pending, simEpoch, 0, "a0")
		firstOpen := first.beginAt(duplicate)
		secondOpen := second.beginAt(duplicate)
		if !firstOpen || secondOpen {
			t.Errorf("of two proposers on a0 at one ballot, the first proceeds %v and the second %v; only the first may",
				firstOpen, secondOpen)
		}
		if firstOpen {
			first.send("a1", "a2")
		}
		if secondOpen {
			second.send("a3", "a4")
		}
		if firstOpen && mustSelfAccept(t, first) {
			first.send("a1", "a2")
			first.commitTo("a1", "a2")
		}
		if secondOpen && mustSelfAccept(t, second) {
			second.send("a3", "a4")
			second.commitTo("a3", "a4")
		}
		world.assertNoViolation(t)
	})

	t.Run("stale accept after adoption (break: rule A without rule E's epoch check)", func(t *testing.T) {
		world := newSimWorld(true)
		p2 := world.propose("p2", nil, world.joining("x2"))
		p1 := world.propose("p1", nil, world.joining("x1"))
		p2.beginAt(ballot{Round: 1, Node: "p2"})
		p2.send("a0", "a1", "a2")
		mustSelfAccept(t, p2)
		p1.beginAt(ballot{Round: 2, Node: "p1"})
		p1.send("a2", "a3", "a4")
		mustSelfAccept(t, p1)
		p1.send("a2", "a3", "a4")
		p1.commitTo("a0", "a1", "a2")
		before := p2.tally.chosen()
		p2.send("a0", "a1", "a2")
		if len(p2.tally.yes) > 0 || p2.tally.chosen() != before {
			t.Errorf("acceptors that adopted %s answered %d stale accepts of %s accepted",
				setKey(p1.offered.members), len(p2.tally.yes), setKey(p2.offered.members))
		}
		world.assertNoViolation(t)
	})

	t.Run("lost proposer (break: the value rule dropped)", func(t *testing.T) {
		world := newSimWorld(true)
		lost := world.propose("p1", nil, world.joining("x1"))
		lost.beginAt(ballot{Round: 1, Node: "p1"})
		lost.send("a0", "a1", "a2")
		mustSelfAccept(t, lost)
		lost.send("a0", "a1")
		p2 := world.propose("p2", nil, world.joining("x2"))
		p2.beginAt(ballot{Round: 2, Node: "p2"})
		p2.send("a1", "a2", "a3")
		mustSelfAccept(t, p2)
		p2.send("a1", "a2", "a3")
		a0 := world.node("a0")
		recovery := world.propose("a0", a0, proposedSet{members: a0.pending.Members, change: a0.pending.Change})
		recovery.recovery = true
		recovery.seenRound = 2
		recovery.begin()
		recovery.send("a3", "a4")
		mustSelfAccept(t, recovery)
		recovery.send("a3", "a4")
		if setKey(p2.offered.members) != setKey(lost.offered.members) {
			t.Errorf("p2 offered %s; it must finish the lost proposer's %s, which a promise it used reports",
				setKey(p2.offered.members), setKey(lost.offered.members))
		}
		world.assertNoViolation(t)
	})

	t.Run("duelling recoveries (no single break; one set)", func(t *testing.T) {
		world := newSimWorld(true)
		lost := world.propose("p1", nil, world.joining("x1"))
		lost.beginAt(ballot{Round: 1, Node: "p1"})
		lost.send("a0", "a1", "a2")
		mustSelfAccept(t, lost)
		lost.send("a0", "a1")
		a0, a1 := world.node("a0"), world.node("a1")
		r0 := world.propose("a0", a0, proposedSet{members: a0.pending.Members, change: a0.pending.Change})
		r1 := world.propose("a1", a1, proposedSet{members: a1.pending.Members, change: a1.pending.Change})
		r0.seenRound, r1.seenRound = 1, 1
		r0.begin()
		r1.begin()
		r0.send("a2")
		r1.send("a2", "a3")
		r0.send("a3", "a4")
		mustSelfAccept(t, r1)
		r1.send("a2", "a3")
		r1.commitTo("a2", "a3", "a4")
		if r0.stage == "self-accept" {
			r0.selfAccept()
			r0.send("a2", "a3", "a4")
		}
		if r1.outcome != "committed" {
			t.Errorf("recovery on a1 ended %q, want committed", r1.outcome)
		}
		if r0.outcome == "committed" && setKey(r0.offered.members) != setKey(r1.offered.members) {
			t.Errorf("both recoveries committed, %s and %s", setKey(r0.offered.members), setKey(r1.offered.members))
		}
		t.Logf("recovery on a0 ended %q, on a1 %q", r0.outcome, r1.outcome)
		world.assertNoViolation(t)
	})

	t.Run("competitor between phases (break: a refused self-accept counted anyway)", func(t *testing.T) {
		world := newSimWorld(true)
		a0, a4 := world.node("a0"), world.node("a4")
		p1 := world.propose("a0", a0, world.joining("x1"))
		p2 := world.propose("a4", a4, world.joining("x5"))
		p1.begin()
		p1.send("a1", "a2")
		p2.begin()
		p2.send("a0", "a3")
		mustSelfAccept(t, p2)
		p2.send("a0", "a3")
		if p2.outcome != "committed" {
			t.Fatalf("test setup: p2 ended %q, want committed", p2.outcome)
		}
		if p1.selfAccept() {
			p1.send("a1", "a2")
		}
		if p1.outcome != "refused" {
			t.Errorf("p1, whose own acceptor promised a higher ballot between its phases, ended %q, want refused", p1.outcome)
		}
		world.assertNoViolation(t)
	})

	t.Run("the exception needs equality (break: accepted on an adopted node without comparing the set)", func(t *testing.T) {
		world := newSimWorld(true)
		a0, a4 := world.node("a0"), world.node("a4")
		p2 := world.propose("a0", a0, world.joining("x0"))
		p1 := world.propose("a4", a4, world.joining("x4"))
		p2.begin()
		p2.send("a3", "a2")
		p1.begin()
		p1.send("a1", "a2")
		mustSelfAccept(t, p1)
		p1.send("a1", "a2")
		p1.commitTo("a1", "a2")
		if p2.selfAccept() {
			p2.send("a1", "a2", "a3")
		}
		if p2.outcome != "refused_ahead" {
			t.Errorf("p2 ended %q; a member that adopted %s answers ahead, which ends it refused",
				p2.outcome, setKey(p1.offered.members))
		}
		world.assertNoViolation(t)
	})
}

// explorerRun is one seeded schedule: five acceptors, three operator
// proposers on random acceptors, random delivery, drop and duplicate of every
// message, proposers lost at any time, recoveries on acceptors that hold an
// acceptance, and twin proposers that take a running proposer's ballot.
func explorerRun(seed int64) *simWorld {
	random := rand.New(rand.NewSource(seed))
	world := newSimWorld(false)
	var proposers []*simProposer
	for index := 0; index < 3; index++ {
		home := world.nodes[random.Intn(len(world.nodes))]
		proposers = append(proposers, world.propose(home.id, home, world.joining(fmt.Sprintf("x%d", index))))
	}
	waiting := slices.Clone(proposers)
	active := func(home *simNode) *simProposer {
		for _, proposer := range proposers {
			if proposer.home == home && proposer.stage != "" && proposer.stage != "done" && !proposer.lost {
				return proposer
			}
		}
		return nil
	}
	for step := 0; step < 600; step++ {
		switch roll := random.Intn(100); {
		case roll < 8 && len(waiting) > 0:
			proposer := waiting[0]
			waiting = waiting[1:]
			if active(proposer.home) == nil && proposer.begin() {
				proposer.sendAll()
			}
		case roll < 55 && len(world.pool) > 0:
			index := random.Intn(len(world.pool))
			message := world.pool[index]
			if random.Intn(10) != 0 {
				world.pool = slices.Delete(world.pool, index, index+1)
			}
			world.deliver(message)
		case roll < 62 && len(world.pool) > 0:
			index := random.Intn(len(world.pool))
			world.pool = slices.Delete(world.pool, index, index+1)
		case roll < 75:
			for _, proposer := range proposers {
				if proposer.stage == "self-accept" && !proposer.lost && random.Intn(2) == 0 {
					if proposer.selfAccept() {
						proposer.sendAll()
					}
				}
			}
		case roll < 78:
			for _, proposer := range proposers {
				if proposer.stage != "done" && proposer.stage != "" && !proposer.lost && random.Intn(4) == 0 {
					proposer.lost = true
					world.outcomes["lost"]++
				}
			}
		case roll < 86:
			for _, proposer := range proposers {
				if proposer.stage != "done" && proposer.stage != "" && !proposer.lost && random.Intn(3) == 0 {
					proposer.timeOut()
				}
			}
		case roll < 94:
			home := world.nodes[random.Intn(len(world.nodes))]
			if home.pending == nil || home.pending.Accepted == nil || home.epoch >= simEpoch || active(home) != nil {
				continue
			}
			recovery := world.propose(home.id, home, proposedSet{members: home.pending.Members, change: home.pending.Change})
			recovery.recovery = true
			proposers = append(proposers, recovery)
			if recovery.begin() {
				recovery.sendAll()
			}
		default:
			running := active(world.nodes[random.Intn(len(world.nodes))])
			if running == nil || running.stage != phasePrepare {
				continue
			}
			// Two proposers on one node at one ballot: the second's
			// self-promise must be refused.
			twin := world.propose(running.name, running.home, world.joining(fmt.Sprintf("t%d", step)))
			proposers = append(proposers, twin)
			if twin.beginAt(running.at) {
				twin.sendAll()
			}
		}
	}
	return world
}

// explorerSeeds is the seeds to run: the one given by -seed, or the default
// list.
func explorerSeeds() []int64 {
	if *membershipExplorerSeed != 0 {
		return []int64{*membershipExplorerSeed}
	}
	seeds := make([]int64, *membershipExplorerSchedules)
	for index := range seeds {
		seeds[index] = int64(index + 1)
	}
	return seeds
}

// TestTheMembershipAcceptorExplorer runs the seeded schedules and asserts M1:
// at most one set chosen, and at most one set adopted, at the epoch.
func TestTheMembershipAcceptorExplorer(t *testing.T) {
	outcomes := make(map[string]int)
	failed := 0
	for _, seed := range explorerSeeds() {
		world := explorerRun(seed)
		for outcome, count := range world.outcomes {
			outcomes[outcome] += count
		}
		var twoSets []string
		for _, violation := range world.violations {
			if strings.HasPrefix(violation, "two sets") || strings.Contains(violation, "is then told") {
				twoSets = append(twoSets, violation)
			}
		}
		if len(twoSets) > 0 {
			failed++
			if failed <= 5 {
				t.Errorf("seed %d (replay with -seed %d): %s", seed, seed, strings.Join(twoSets, "; "))
			}
		}
	}
	t.Logf("%d schedules, %d with two sets; outcomes %v", len(explorerSeeds()), failed, outcomes)
}

// TestTheMembershipInvariantHoldsAfterEveryRule runs the explorer's schedules
// and asserts I-1 and I-2 after every acceptor transition, and that no ballot
// carries two sets.
func TestTheMembershipInvariantHoldsAfterEveryRule(t *testing.T) {
	broken := 0
	for _, seed := range explorerSeeds() {
		world := explorerRun(seed)
		var invariant []string
		for _, violation := range world.violations {
			if strings.HasPrefix(violation, "I-") || strings.HasPrefix(violation, "two sets at one ballot") {
				invariant = append(invariant, violation)
			}
		}
		if len(invariant) > 0 {
			broken++
			if broken <= 5 {
				t.Errorf("seed %d (replay with -seed %d): %s", seed, seed, strings.Join(invariant, "; "))
			}
		}
	}
	t.Logf("%d schedules, %d with an invariant broken", len(explorerSeeds()), broken)
}
