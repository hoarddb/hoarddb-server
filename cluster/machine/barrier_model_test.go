package machine

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// This file checks BarrierPair against a second, deliberately plain
// implementation of write §8 and the brief's table, over random event
// sequences from fixed seeds. The reference keeps its state in a different
// shape (a phase string, a parked list, plain booleans) and is written from
// the spec text, not from barrier.go, so a slip in one of them shows up as a
// disagreement. The invariants are then checked against facts the harness
// records from the event stream itself, not from either implementation.

// refOp is one parked operation in the reference.
type refOp struct {
	id   uint16
	read bool
}

// barrierRefOut is what the reference says the caller must do.
type barrierRefOut struct {
	req, stopped, ready, clear, refuse bool
	forward                            []uint16
}

// refModel is the reference: one pair on one node.
type refModel struct {
	phase              string // "idle", "send" or "wait"
	peer               string
	lastEpoch, toEpoch uint64
	oldEpoch, drainTo  uint64
	proved, sent, got  bool
	clockStart, bound  time.Duration
	clockKnown         bool
	current            bool
	applied, target    cluster.Position
	parked             []refOp
	rank1              bool
}

func (m *refModel) forget() {
	m.phase, m.peer = "idle", ""
	m.oldEpoch, m.drainTo = 0, 0
	m.proved, m.sent, m.got = false, false, false
	m.clockKnown = false
	m.current = false
	m.applied, m.target = cluster.Position{}, cluster.Position{}
}

func (m *refModel) releaseParked() []uint16 {
	var ids []uint16
	for _, o := range m.parked {
		ids = append(ids, o.id)
	}
	m.parked = nil
	return ids
}

// bothFacts: the pair holds every acknowledged write, and the old owner is known stopped.
func (m *refModel) bothFacts() bool {
	return m.phase == "wait" && m.clockKnown && !m.sent && m.proved && m.current && !m.applied.Less(m.target)
}

func (m *refModel) maybeReady(out *barrierRefOut) {
	if !m.bothFacts() {
		return
	}
	m.sent = true
	m.phase = "idle"
	out.ready, out.clear = true, true
	out.forward = m.releaseParked()
}

func opIDOf(id [16]byte) uint16 { return uint16(id[0])<<8 | uint16(id[1]) }

func opID(n uint16) [16]byte {
	var id [16]byte
	id[0], id[1] = byte(n>>8), byte(n)
	return id
}

func (m *refModel) step(ev BarrierEvent) barrierRefOut {
	var out barrierRefOut
	switch e := ev.(type) {
	case EpochCommitted:
		m.commit(e, &out)
	case HandoffReqArrived:
		m.request(e, &out)
	case HandoffStoppedArrived:
		if e.Valid && m.phase == "wait" && e.ToEpoch == m.toEpoch && m.peer != "" && e.From == m.peer {
			m.proved = true
			m.maybeReady(&out)
		}
	case HandoffReadyArrived:
		m.readyArrived(e, &out)
	case CatchUpDone:
		if m.phase == "wait" {
			m.applied, m.target, m.current = e.AppliedPos, e.Target, e.Current
			m.maybeReady(&out)
		}
	case RetryTick:
		switch {
		case m.phase == "send":
			out.req, out.stopped = true, true
		case m.phase == "wait" && m.clockKnown && !m.proved && e.At-m.clockStart >= m.bound:
			m.proved = true
			m.maybeReady(&out)
		}
	case WriteArrived:
		if m.phase != "idle" {
			m.parked = append(m.parked, refOp{id: opIDOf(e.RequestID)})
		}
	case ReadArrived:
		switch {
		case m.phase == "send":
			m.parked = append(m.parked, refOp{id: opIDOf(e.RequestID), read: true})
		case m.phase == "wait" && !e.PeerReachable && !e.CallerWillWait:
			out.refuse = true
		case m.phase == "wait":
			m.parked = append(m.parked, refOp{id: opIDOf(e.RequestID), read: true})
		}
	case DrainDeadline:
	}
	return out
}

func (m *refModel) commit(e EpochCommitted, out *barrierRefOut) {
	if e.Epoch <= m.lastEpoch {
		return
	}
	becomesOwner := e.SelfID == e.Rank1ID
	if m.phase == "wait" && !m.clockKnown && e.Epoch == m.toEpoch && becomesOwner {
		// The commit a HANDOFF_REQ announced before it reached this node.
		m.lastEpoch, m.rank1 = e.Epoch, true
		m.clockKnown, m.clockStart, m.bound = true, e.At, e.Bound
		pred := e.OldRank1ID
		if pred == e.SelfID {
			pred = ""
		}
		if m.peer != pred {
			m.peer, m.proved = pred, false
		}
		m.maybeReady(out)
		return
	}
	hadHold := m.phase != "idle"
	if hadHold {
		out.forward = m.releaseParked()
		m.forget()
	} else {
		m.sent = false
	}
	previous := m.lastEpoch
	m.lastEpoch, m.rank1 = e.Epoch, becomesOwner
	switch {
	case e.Moved && e.SelfID == e.OldRank1ID && !becomesOwner:
		m.forget()
		m.phase, m.peer, m.drainTo, m.oldEpoch, m.toEpoch = "send", e.Rank1ID, e.Appended, previous, e.Epoch
		out.req, out.stopped = true, true
	case (e.Moved || hadHold) && becomesOwner:
		m.forget()
		m.phase, m.toEpoch = "wait", e.Epoch
		m.clockKnown, m.clockStart, m.bound = true, e.At, e.Bound
		if e.OldRank1ID != e.SelfID {
			m.peer = e.OldRank1ID
		}
	default:
		out.clear = hadHold
	}
}

func (m *refModel) request(e HandoffReqArrived, out *barrierRefOut) {
	if !e.Valid || e.FromEpoch >= e.ToEpoch {
		return
	}
	switch {
	case m.phase == "send":
		if e.ToEpoch == m.toEpoch {
			out.req, out.stopped = true, true
		}
	case m.phase == "wait":
	case m.sent && e.ToEpoch == m.toEpoch:
		out.ready = true
	case e.ToEpoch > m.lastEpoch:
		m.forget()
		m.phase, m.peer, m.toEpoch, m.rank1 = "wait", e.From, e.ToEpoch, true
	}
}

func (m *refModel) readyArrived(e HandoffReadyArrived, out *barrierRefOut) {
	if m.phase != "send" || e.ToEpoch != m.toEpoch || e.From != m.peer {
		return
	}
	if m.drainTo != 0 && e.AppliedPos.Less(cluster.Position{Epoch: m.oldEpoch, Seq: m.drainTo}) {
		return
	}
	m.phase, m.got, m.applied = "idle", true, e.AppliedPos
	out.clear = true
	out.forward = m.releaseParked()
}

// ---- the harness

type modelCounts struct {
	sendingEntered, awaitingEntered, readyByStopped, readyByCatchUp, readyByBound, readyByCommit int
	holdsDropped, held, refused, readyReceived, duplicatesAfterEnd, preempted, resentReady       int
}

type harness struct {
	t     *testing.T
	seed  int64
	step  int
	pair  BarrierPair
	ref   refModel
	count *modelCounts

	now      time.Duration
	epoch    uint64
	owner    string
	nextOp   uint16
	history  []BarrierEvent
	commitAt map[uint64]time.Duration
	pred     map[uint64]string // who was rank 1 before epoch x
	stopped  map[uint64]map[string]bool

	answered  map[uint16]bool
	pending   map[uint16]bool
	returned  map[uint16]int
	mustReady bool // this node entered an awaiting hold and has not sent READY since
}

func (h *harness) fail(format string, args ...any) {
	h.t.Helper()
	h.t.Fatalf("seed %d step %d: %s", h.seed, h.step, fmt.Sprintf(format, args...))
}

func idsOf(ops []HeldOp) []uint16 {
	var ids []uint16
	for _, o := range ops {
		ids = append(ids, opIDOf(o.RequestID))
	}
	return ids
}

func refIDs(ops []refOp) []uint16 {
	var ids []uint16
	for _, o := range ops {
		ids = append(ids, o.id)
	}
	return ids
}

func phaseOf(h Hold) string {
	switch h {
	case HoldSending:
		return "send"
	case HoldAwaiting:
		return "wait"
	}
	return "idle"
}

// compare fails on the first disagreement between the machine and the reference.
func (h *harness) compare(act BarrierAction, out barrierRefOut) {
	if act.SendHandoffReq != out.req || act.SendStopped != out.stopped || act.SendReady != out.ready ||
		act.ClearHold != out.clear || (act.Refuse != 0) != out.refuse || !reflect.DeepEqual(idsOf(act.ForwardHeld), out.forward) {
		h.fail("action disagrees: machine %+v, reference %+v", act, out)
	}
	p, r := &h.pair, &h.ref
	same := phaseOf(p.Hold) == r.phase && p.ToEpoch == r.toEpoch && p.Peer == r.peer && p.Epoch == r.lastEpoch &&
		p.StopProved == r.proved && p.ReadySent == r.sent && p.ReadyRecv == r.got &&
		p.Current == r.current && p.AppliedPos == r.applied && p.Target == r.target &&
		p.DrainTo == r.drainTo && p.Rank1 == r.rank1 && reflect.DeepEqual(idsOf(p.Held), refIDs(r.parked))
	if same {
		return
	}
	h.fail("state disagrees:\nmachine   %+v\nreference %+v", *p, *r)
}

func (h *harness) pickEpoch(rng *rand.Rand) uint64 {
	switch rng.Intn(10) {
	case 0, 1, 2, 3:
		return h.epoch
	case 4, 5, 6:
		return h.epoch + 1
	case 7:
		return h.epoch - 1
	}
	if h.epoch > 4 {
		return h.epoch - uint64(2+rng.Intn(3))
	}
	return h.epoch
}

func (h *harness) pickNode(rng *rand.Rand, prefer string) string {
	if prefer != "" && rng.Intn(10) < 6 {
		return prefer
	}
	return []string{nodeA, nodeB, nodeC}[rng.Intn(3)]
}

func (h *harness) newEvent(rng *rand.Rand) BarrierEvent {
	h.now += time.Duration(rng.Intn(400)) * time.Millisecond
	if rng.Intn(12) == 0 {
		h.now += 2200 * time.Millisecond // let a bound elapse now and then
	}
	// Self is node a throughout: the old owner in some epochs, the new one in others.
	switch n := rng.Intn(100); {
	case n < 14: // a committed epoch
		newOwner := h.owner
		if rng.Intn(100) < 65 {
			newOwner = []string{nodeA, nodeB, nodeC}[rng.Intn(3)]
		}
		ev := EpochCommitted{
			At: h.now, Epoch: h.epoch + 1, SelfID: nodeA, OldRank1ID: h.owner, Rank1ID: newOwner,
			Moved: newOwner != h.owner, Appended: uint64(rng.Intn(12)), Bound: stopBound,
		}
		h.epoch++
		h.commitAt[ev.Epoch], h.pred[ev.Epoch] = h.now, h.owner
		h.owner = newOwner
		return ev
	case n < 17: // a duplicate of an earlier commit
		return EpochCommitted{At: h.now, Epoch: h.epoch - uint64(rng.Intn(2)), SelfID: nodeA, OldRank1ID: nodeB, Rank1ID: nodeA, Moved: true, Bound: stopBound}
	case n < 25:
		to := h.pickEpoch(rng)
		from := to - 1
		if rng.Intn(7) == 0 {
			from = to
		}
		return HandoffReqArrived{From: h.pickNode(rng, h.pair.Peer), To: h.pickNode(rng, ""), FromEpoch: from, ToEpoch: to, Valid: rng.Intn(10) < 8}
	case n < 33:
		return HandoffStoppedArrived{From: h.pickNode(rng, h.pair.Peer), To: h.pickNode(rng, ""), ToEpoch: h.pickEpoch(rng), Valid: rng.Intn(10) < 9}
	case n < 41:
		return HandoffReadyArrived{From: h.pickNode(rng, h.pair.Peer), To: h.pickNode(rng, ""), ToEpoch: h.pickEpoch(rng),
			AppliedPos: barrierPos(h.epoch-uint64(rng.Intn(2)), uint64(rng.Intn(13)))}
	case n < 53:
		ep := h.epoch - uint64(rng.Intn(2))
		return CatchUpDone{At: h.now, AppliedPos: barrierPos(ep, uint64(rng.Intn(13))), Target: barrierPos(ep, uint64(rng.Intn(13))), Current: rng.Intn(10) < 7}
	case n < 66:
		h.nextOp++
		return WriteArrived{RequestID: opID(h.nextOp), Body: []byte{byte(h.nextOp)}}
	case n < 78:
		h.nextOp++
		return ReadArrived{RequestID: opID(h.nextOp), PeerReachable: rng.Intn(2) == 0, CallerWillWait: rng.Intn(2) == 0}
	case n < 82:
		return DrainDeadline{At: h.now}
	default:
		return RetryTick{At: h.now}
	}
}

// isBarrierMessage reports the epoch a barrier message names.
func isBarrierMessage(ev BarrierEvent) (toEpoch uint64, ok bool) {
	switch e := ev.(type) {
	case HandoffReqArrived:
		return e.ToEpoch, true
	case HandoffStoppedArrived:
		return e.ToEpoch, true
	case HandoffReadyArrived:
		return e.ToEpoch, true
	}
	return 0, false
}

func (h *harness) apply(ev BarrierEvent) {
	pre := clonePair(h.pair)
	act := h.pair.Apply(ev)
	out := h.ref.step(ev)
	h.compare(act, out)
	h.invariants(pre, ev, act)
}

func (h *harness) invariants(pre BarrierPair, ev BarrierEvent, act BarrierAction) {
	p := &h.pair
	c := h.count

	// A new owner never orders before the predecessor is proved stopped: READY
	// is the only way out of an awaiting hold, and it needs a STOPPED from the
	// predecessor of this epoch or the bound measured from this epoch's commit.
	if st, ok := ev.(HandoffStoppedArrived); ok && st.Valid {
		if h.stopped[st.ToEpoch] == nil {
			h.stopped[st.ToEpoch] = map[string]bool{}
		}
		h.stopped[st.ToEpoch][st.From] = true
	}
	if act.SendReady && pre.Hold == HoldAwaiting {
		x := p.ToEpoch
		byMessage := h.pred[x] != nodeA && h.stopped[x][h.pred[x]]
		at, known := h.commitAt[x]
		byBound := known && h.now-at >= stopBound
		if !byMessage && !byBound {
			h.fail("READY for epoch %d without proof: predecessor %q stopped=%v, commit at %v, now %v", x, h.pred[x], h.stopped[x], at, h.now)
		}
		if !p.Current || p.AppliedPos.Less(p.Target) {
			h.fail("READY while not current through the target: %+v", *p)
		}
		h.mustReady = false
		switch ev.(type) {
		case HandoffStoppedArrived:
			c.readyByStopped++
		case CatchUpDone:
			c.readyByCatchUp++
		case RetryTick:
			c.readyByBound++
		case EpochCommitted:
			c.readyByCommit++
		}
	}
	if _, isReq := ev.(HandoffReqArrived); isReq && act.SendReady {
		c.resentReady++
	}
	if e, ok := ev.(EpochCommitted); ok && e.Epoch > pre.Epoch {
		selfOwner := e.SelfID == e.Rank1ID
		h.mustReady = selfOwner && (e.Moved || pre.Hold != HoldNone) && p.Hold == HoldAwaiting
		if p.Hold == HoldSending {
			c.sendingEntered++
		}
		if p.Hold == HoldAwaiting {
			c.awaitingEntered++
		}
		if pre.Hold != HoldNone {
			c.holdsDropped++
		}
	}
	if h.mustReady && p.Rank1 && p.Hold == HoldNone && !p.ReadySent {
		h.fail("the new owner reached the ordering state without READY: %+v", *p)
	}
	if _, ok := ev.(HandoffReadyArrived); ok && pre.Hold == HoldSending && p.Hold == HoldNone {
		c.readyReceived++
	}
	if _, ok := ev.(ReadArrived); ok && act.Refuse != 0 {
		c.refused++
	}
	if _, ok := ev.(HandoffReqArrived); ok && pre.Hold == HoldNone && p.Hold == HoldAwaiting {
		c.preempted++
	}

	// No operation is both answered and forwarded; an operation that arrives
	// during a hold is never answered.
	var arrived uint16
	var isOp bool
	switch e := ev.(type) {
	case WriteArrived:
		arrived, isOp = opIDOf(e.RequestID), true
	case ReadArrived:
		arrived, isOp = opIDOf(e.RequestID), true
	}
	if isOp {
		heldNow := false
		for _, id := range idsOf(p.Held) {
			if id == arrived {
				heldNow = true
			}
		}
		switch {
		case pre.Hold != HoldNone && act.Refuse == 0 && !heldNow:
			h.fail("op %d arrived during a hold and was neither held nor refused", arrived)
		case pre.Hold == HoldNone && heldNow:
			h.fail("op %d arrived with no hold and was held", arrived)
		case heldNow:
			h.pending[arrived] = true
			c.held++
		case act.Refuse == 0:
			h.answered[arrived] = true
		}
	}
	for _, id := range idsOf(act.ForwardHeld) {
		if !h.pending[id] || h.answered[id] {
			h.fail("op %d was forwarded without being held (or after being answered)", id)
		}
		h.returned[id]++
		if h.returned[id] != 1 {
			h.fail("op %d was returned %d times", id, h.returned[id])
		}
		delete(h.pending, id)
	}
	held := map[uint16]bool{}
	for _, id := range idsOf(p.Held) {
		held[id] = true
	}
	if len(held) != len(h.pending) {
		h.fail("held ops %v and pending ops %v differ", idsOf(p.Held), h.pending)
	}
	for id := range h.pending {
		if !held[id] {
			h.fail("op %d is pending but not held", id)
		}
	}

	// A duplicated or delayed barrier message changes nothing once the hold
	// ended: nothing for this barrier is left to change.
	if to, ok := isBarrierMessage(ev); ok && pre.Hold == HoldNone && to <= pre.Epoch {
		c.duplicatesAfterEnd++
		if !reflect.DeepEqual(*p, pre) {
			h.fail("a barrier message for epoch %d after the hold ended changed the pair:\nbefore %+v\nafter  %+v", to, pre, *p)
		}
	}

	// A hold always ends: the next epoch that is not about this node drops it,
	// returns every held op, and leaves no hold behind.
	if p.Hold != HoldNone {
		probe := clonePair(*p)
		next := p.Epoch
		if p.ToEpoch > next {
			next = p.ToEpoch
		}
		probeAct := probe.Apply(EpochCommitted{At: h.now, Epoch: next + 1, SelfID: nodeA, OldRank1ID: nodeC, Rank1ID: nodeC, Bound: stopBound})
		if probe.Hold != HoldNone || len(probe.Held) != 0 || !probeAct.ClearHold || !reflect.DeepEqual(idsOf(probeAct.ForwardHeld), idsOf(p.Held)) {
			h.fail("a further epoch did not end the hold: %+v act=%+v", probe, probeAct)
		}
	}
}

func runBarrierModelSeed(t *testing.T, seed int64, c *modelCounts) {
	rng := rand.New(rand.NewSource(seed))
	h := &harness{
		t: t, seed: seed, count: c, owner: nodeC, now: t0,
		ref:   refModel{phase: "idle"},
		epoch: 10, commitAt: map[uint64]time.Duration{}, pred: map[uint64]string{}, stopped: map[uint64]map[string]bool{},
		answered: map[uint16]bool{}, pending: map[uint16]bool{}, returned: map[uint16]int{},
	}
	for h.step = 0; h.step < 600; h.step++ {
		var ev BarrierEvent
		if rng.Intn(100) < 12 && len(h.history) > 0 {
			// A delayed or duplicated barrier message: the channel replays an old one.
			ev = h.history[rng.Intn(len(h.history))]
		} else {
			ev = h.newEvent(rng)
		}
		if _, ok := isBarrierMessage(ev); ok {
			h.history = append(h.history, ev)
		}
		h.apply(ev)
	}
	// Close any hold and prove every held op came back exactly once.
	h.step++
	h.apply(EpochCommitted{At: h.now, Epoch: max(h.pair.Epoch, h.pair.ToEpoch) + 1, SelfID: nodeA, OldRank1ID: nodeC, Rank1ID: nodeC, Bound: stopBound})
	if h.pair.Hold != HoldNone || len(h.pending) != 0 {
		h.fail("a hold or held ops survived the closing epoch: %+v pending=%v", h.pair, h.pending)
	}
}

// TestBarrierMachineAgainstTheReferenceModel runs twenty fixed seeds of random
// events, with replayed and duplicated barrier messages, through BarrierPair
// and the plain reference side by side. Identical outputs and state are
// asserted at every step, and so are the invariants of the brief: the new
// owner never orders before stopProved, no operation is both answered and
// forwarded, a hold always ends, held operations come back exactly once, and
// a barrier message after the hold ended changes nothing.
func TestBarrierMachineAgainstTheReferenceModel(t *testing.T) {
	var c modelCounts
	for seed := int64(1); seed <= 20; seed++ {
		runBarrierModelSeed(t, seed, &c)
	}
	// The sequences must actually exercise every path, or the agreement is vacuous.
	for name, n := range map[string]int{
		"sending entered": c.sendingEntered, "awaiting entered": c.awaitingEntered,
		"READY by STOPPED": c.readyByStopped, "READY by catch-up": c.readyByCatchUp, "READY by the bound": c.readyByBound,
		"holds dropped by a further epoch": c.holdsDropped, "held ops": c.held, "refused reads": c.refused,
		"READY received": c.readyReceived, "duplicates after the end": c.duplicatesAfterEnd,
		"REQ ahead of the commit": c.preempted, "READY resent on a REQ": c.resentReady,
	} {
		if n == 0 {
			t.Errorf("the model never exercised: %s", name)
		}
	}
	t.Logf("coverage over 20 seeds: %+v", c)
}
