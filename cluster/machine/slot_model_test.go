package machine

import (
	"fmt"
	"math/rand"
	"slices"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// The model test of SlotRecord: 20 fixed seeds of random events against a
// reference model that shares no code with slot.go, and a ghost of the real
// world (what the other nodes really hold, what the range on disk really
// holds) that the safety properties are checked against. The harness plays the
// caller (U44, U46): it persists, executes each action and reloads after a
// crash at any step.

// slotRef is the reference: the table of seeding §3 and §8 written as a plain
// switch over the persisted state and a handful of facts, with its own
// bookkeeping of what was attested.
type slotRef struct {
	// persisted, loaded from the record before each step
	state SlotState
	phase Phase
	cut   cluster.Position
	appl  cluster.Position
	// memory
	known, inT, rank1, current bool
	epoch                      uint64
	copies                     []string
	fence, hint                cluster.Position
	hasFence                   bool
	att                        map[string]cluster.Position
	attEpoch                   uint64
	attCopies                  []string
	deleted, rebuild, intact   bool
}

func (m *slotRef) load(r *SlotRecord) {
	m.state, m.phase, m.cut, m.appl = r.State, r.Phase, r.Cut, r.Applied
}

func (m *slotRef) forget() {
	*m = slotRef{state: m.state, phase: m.phase, cut: m.cut, appl: m.appl}
}

func slotRefCovers(p, f cluster.Position) bool {
	if p.Epoch != f.Epoch {
		return p.Epoch > f.Epoch
	}
	return p.Seq >= f.Seq
}

func (m *slotRef) gate() bool {
	if !m.known || !m.hasFence || len(m.copies) == 0 {
		return false
	}
	for _, c := range m.copies {
		if p, ok := m.att[c]; !ok || !slotRefCovers(p, m.fence) {
			return false
		}
	}
	return true
}

// waiting names the members that have not attested a position covering the
// fence the gate is about to use: the one on disk, or the one a retire would write.
func (m *slotRef) waiting() []string {
	fence := m.fence
	if !m.hasFence {
		fence = m.appl
		if fence.Less(m.hint) {
			fence = m.hint
		}
	}
	var w []string
	for _, c := range m.copies {
		if p, ok := m.att[c]; !ok || !slotRefCovers(p, fence) {
			w = append(w, c)
		}
	}
	return w
}

func (m *slotRef) posture() (bool, bool) {
	if m.state != SlotReady || m.rebuild || !m.known || !m.inT {
		return false, false
	}
	return !m.rank1 || m.current, true
}

// retiredStep is the decision for a retired record.
func (m *slotRef) retiredStep() SlotAction {
	switch {
	case m.deleted && m.inT:
		m.state, m.deleted = SlotAbsent, false
		return SlotAction{Do: SlotRestartSeed, Restart: 4, RemoveRecord: true}
	case m.deleted:
		return SlotAction{ProposeRetire: true}
	case m.gate() && m.inT:
		return SlotAction{Do: SlotDeleteRangeThenSeed}
	case m.gate():
		return SlotAction{Do: SlotDeleteRange}
	}
	a := SlotAction{Do: SlotGate, Waiting: m.waiting()}
	if m.known && !m.hasFence {
		a.WriteFence, a.Fence = true, m.appl
		if m.appl.Less(m.hint) {
			a.Fence = m.hint
		}
	}
	return a
}

// step returns the expected action and updates the model's state.
func (m *slotRef) step(ev SlotEvent) SlotAction {
	act := m.decide(ev)
	act.Serve, act.CountsAsCopy = m.posture()
	return act
}

func (m *slotRef) decide(ev SlotEvent) SlotAction {
	if m.state > SlotRetiring {
		switch ev.(type) {
		case SeedSourceAsked:
			return SlotAction{SeedRefused: 4}
		case SeedRetryAsked:
			return SlotAction{Refusal: cluster.CodeSeedNotHeld}
		}
		return SlotAction{}
	}
	switch e := ev.(type) {
	case Reconciled:
		return m.reconcile(e)
	case SeedFinished:
		if m.state == SlotSeeding && m.phase == PhaseNone {
			m.state, m.rebuild = SlotReady, false
			if m.known && m.inT {
				return SlotAction{Do: SlotReportBit}
			}
		}
		return SlotAction{}
	case RetireStarted:
		switch m.state {
		case SlotRetired:
			if !m.deleted && m.known && m.hasFence && e.Fence == m.fence && m.gate() {
				m.state, m.intact = SlotRetiring, false
				return SlotAction{Do: SlotDeleteRange}
			}
			return SlotAction{Do: SlotGate, Waiting: m.waiting()}
		case SlotRetiring:
			return SlotAction{Do: SlotFinishDeleteFirst}
		}
		return SlotAction{}
	case RetireDeleted:
		if m.state != SlotRetiring {
			return SlotAction{}
		}
		if m.known && m.inT {
			m.state = SlotAbsent
			return SlotAction{Do: SlotRestartSeed, Restart: 4, RemoveRecord: true}
		}
		m.state, m.deleted, m.intact = SlotRetired, true, false
		return SlotAction{ProposeRetire: true}
	case RetireCommitted:
		if m.state == SlotRetired && m.deleted {
			m.state, m.deleted = SlotAbsent, false
			return SlotAction{RemoveRecord: true}
		}
		return SlotAction{}
	case SeedSourceAsked:
		switch {
		case m.state == SlotReady, m.state == SlotRetired && m.intact:
			return SlotAction{}
		case m.state == SlotAbsent:
			return SlotAction{SeedRefused: 3}
		}
		return SlotAction{SeedRefused: 4}
	case SeedRetryAsked:
		if m.state == SlotSeeding {
			return SlotAction{Do: SlotRestartSeed, Restart: 4}
		}
		return SlotAction{Refusal: cluster.CodeSeedNotHeld}
	case LogBelowHorizon:
		if m.state == SlotReady && m.known && m.inT {
			m.rebuild = true
			return SlotAction{Do: SlotRestartSeed, Restart: 2}
		}
		return SlotAction{}
	case PositionReplied:
		if m.state != SlotRetired || !m.known || e.Epoch != m.epoch || !slices.Contains(m.copies, e.Node) {
			return SlotAction{}
		}
		if e.State == SlotReady {
			m.att[e.Node] = e.Applied
		} else {
			delete(m.att, e.Node)
		}
		return m.retiredStep()
	}
	return SlotAction{} // PositionClaimed
}

func (m *slotRef) reconcile(e Reconciled) SlotAction {
	v := e.View
	copies := slices.Compact(slices.Sorted(slices.Values(v.Copies)))
	// A view older than the one held is dropped whole. Within one epoch the
	// member set, the target membership and a fence once known cannot change,
	// so a view that says otherwise is a queued old one.
	if m.known && (e.Epoch < m.epoch ||
		e.Epoch == m.epoch && (!slices.Equal(copies, m.copies) || v.InTargetSet != m.inT || m.hasFence && (!v.HasFence || v.Fence != m.fence))) {
		return SlotAction{Ignored: true}
	}
	// The committed epoch moved since the last view this process held: an
	// attempt that registered a cut under the old epoch is stale (seeding §6).
	moved := m.known && m.epoch < e.Epoch
	if !m.known || m.attEpoch != e.Epoch || !slices.Equal(m.attCopies, copies) || m.att == nil {
		m.att, m.attEpoch, m.attCopies = map[string]cluster.Position{}, e.Epoch, copies
	}
	m.known, m.epoch, m.inT, m.rank1, m.current = true, e.Epoch, v.InTargetSet, v.Rank1, v.Current
	m.copies, m.fence, m.hasFence, m.hint = copies, v.Fence, v.HasFence, v.FenceHint
	switch m.state {
	case SlotAbsent:
		if m.inT {
			if v.Joining || v.OthersHoldBit || v.RangeHoldsKeys {
				return SlotAction{Do: SlotQueueSeed}
			}
			return SlotAction{Do: SlotWriteReadyEmpty}
		}
	case SlotSeeding:
		switch {
		case !m.inT:
			// The record stays seeding; the caller removes it and then the range.
			return SlotAction{Do: SlotDeleteRangeAndForget}
		case m.phase == PhaseReset || m.phase > PhaseBuffer:
			return SlotAction{Do: SlotRestartSeed, Restart: 4}
		case !m.cut.Zero() && m.cut.Epoch <= e.Epoch && !moved:
			return SlotAction{Do: SlotResumeSeed}
		}
		return SlotAction{Do: SlotRestartSeed, Restart: 3}
	case SlotReady:
		switch {
		case !m.inT:
			f := m.appl
			if f.Less(m.hint) {
				f = m.hint
			}
			m.state, m.deleted, m.rebuild, m.intact = SlotRetired, false, false, true
			return SlotAction{Do: SlotRetire, WriteFence: true, Fence: f, Waiting: m.copies}
		case v.OwnBit || !v.Current:
			return SlotAction{Do: SlotCatchUp}
		}
		return SlotAction{Do: SlotReportBit}
	case SlotRetired:
		return m.retiredStep()
	case SlotRetiring:
		return SlotAction{Do: SlotFinishDeleteFirst}
	}
	return SlotAction{}
}

// slotRange is what the pair's range on disk really holds.
type slotRange uint8

const (
	slotRangeEmpty slotRange = iota
	slotRangePartial
	slotRangeFull
)

type slotTruth struct {
	state   SlotState
	applied cluster.Position
}

type slotPast struct {
	node  string
	reply PositionReplied
}

type slotCoverage struct {
	gateOpened, rejoinReseeds, staleIgnored, seedingIgnored, claims                   int
	reloadMidRetire, prematureRefused, removedAfterRetire, horizonRestarts, dropped   int
	retiredBack, seedsFinished, reconciles, reloads, emptyGateRefused, reusedIdentity int
	staleViews, retiredSources                                                        int
	earlierCutResumes, laterCutRestarts, movedEpochRestarts, equalCutResumes          int
}

type slotSim struct {
	t   *testing.T
	rnd *rand.Rand
	rec SlotRecord
	ref slotRef

	fenceDisk   cluster.Position // the caller's 's' record
	hasFence    bool
	rangeOnDisk slotRange

	deletePending, deleteRunning bool

	epoch                        uint64
	inTarget, ownBit, rank1, cur bool
	joining, othersHold          bool
	copies                       []string
	hint                         cluster.Position
	truth                        map[string]slotTruth
	past                         []slotPast
	views                        []Reconciled
	cov                          *slotCoverage
	step                         int
}

var slotPeerNames = []string{"node-b", "node-c", "node-d"}

func (s *slotSim) fail(format string, args ...any) {
	s.t.Helper()
	s.t.Fatalf("step %d: %s\n  record %+v range %d world epoch %d inTarget %v copies %v truth %v", s.step, fmt.Sprintf(format, args...), s.rec.State, s.rangeOnDisk, s.epoch, s.inTarget, s.copies, s.truth)
}

func (s *slotSim) position() cluster.Position {
	menu := []cluster.Position{slotPos(1, 10), slotPos(1, 50), slotPos(1, 60), slotPos(2, 1), slotPos(0, 9999), slotPos(s.epoch, 1), slotPos(s.epoch+1, 1)}
	return menu[s.rnd.Intn(len(menu))]
}

func (s *slotSim) rerollTruth(node string) {
	switch s.rnd.Intn(5) {
	case 0, 1:
		s.truth[node] = slotTruth{SlotReady, slotPos(s.epoch, 1000)} // covers any fence made so far
	case 2:
		s.truth[node] = slotTruth{SlotReady, s.position()}
	case 3:
		s.truth[node] = slotTruth{SlotSeeding, slotPos(s.epoch, 5000)} // its cut: a claim, not a copy
	default:
		s.truth[node] = slotTruth{SlotAbsent, cluster.Position{}}
	}
}

// feed gives the event to the machine and to the reference, compares them and
// checks every property that does not depend on the reference.
func (s *slotSim) feed(ev SlotEvent) SlotAction {
	s.step++
	before := s.rec.State
	cutBefore, phaseBefore := s.rec.Cut, s.rec.Phase
	s.ref.load(&s.rec)
	want := s.ref.step(ev)
	got := s.rec.Apply(ev)
	s.countEpochRule(ev, got, before, phaseBefore, cutBefore)
	if !slotSame(got, want) || s.rec.State != s.ref.state {
		s.fail("%T %+v: machine %+v state %d, reference %+v state %d", ev, ev, got, s.rec.State, want, s.ref.state)
	}
	s.legal(before, s.rec.State, ev)
	if s.rec.State == SlotReady && s.rangeOnDisk != slotRangeFull {
		s.fail("a record is ready over a range that is not whole (range %d)", s.rangeOnDisk)
	}
	if (got.Serve || got.CountsAsCopy) && (s.rec.State != SlotReady || !s.inTarget) {
		s.fail("a record that is not ready serves or counts: %+v", got)
	}
	if _, asked := ev.(SeedSourceAsked); asked && got.SeedRefused == 0 && before == SlotRetired {
		// Property: a retired holder lends out its range only while the range is whole.
		if s.rangeOnDisk != slotRangeFull {
			s.fail("a retired record served a seed over a range that is not whole (range %d)", s.rangeOnDisk)
		}
		s.cov.retiredSources++
	}
	s.exec(got, ev, before)
	return got
}

// countEpochRule records which side of the epoch rule a seeding record met, so
// the test can require every side to be reached.
func (s *slotSim) countEpochRule(ev SlotEvent, act SlotAction, before SlotState, phase Phase, cut cluster.Position) {
	rc, ok := ev.(Reconciled)
	if !ok || act.Ignored || before != SlotSeeding || !rc.View.InTargetSet || phase == PhaseReset || phase > PhaseBuffer || cut.Zero() {
		return
	}
	switch {
	case act.Do == SlotResumeSeed && cut.Epoch < rc.Epoch:
		s.cov.earlierCutResumes++
	case act.Do == SlotResumeSeed:
		s.cov.equalCutResumes++
	case act.Restart == 3 && cut.Epoch > rc.Epoch:
		s.cov.laterCutRestarts++
	case act.Restart == 3:
		s.cov.movedEpochRestarts++
	}
}

// legal is the transition graph of the record: the only edges the table allows.
func (s *slotSim) legal(from, to SlotState, ev SlotEvent) {
	if from == to {
		return
	}
	ok := false
	switch ev.(type) {
	case Reconciled:
		ok = (from == SlotReady && to == SlotRetired) || (from == SlotRetired && to == SlotAbsent)
	case SeedFinished:
		ok = from == SlotSeeding && to == SlotReady
	case RetireStarted:
		ok = from == SlotRetired && to == SlotRetiring
	case RetireDeleted:
		ok = from == SlotRetiring && (to == SlotRetired || to == SlotAbsent)
	case RetireCommitted:
		ok = from == SlotRetired && to == SlotAbsent
	}
	if !ok {
		s.fail("%T moved the record %d -> %d, an edge the table does not have (a retire was cancelled or skipped)", ev, from, to)
	}
}

func (s *slotSim) truthCovers(node string, fence cluster.Position) bool {
	tr := s.truth[node]
	return tr.state == SlotReady && ((tr.applied.Epoch == fence.Epoch && tr.applied.Seq >= fence.Seq) || tr.applied.Epoch > fence.Epoch)
}

// exec is the caller's duty for one action.
func (s *slotSim) exec(act SlotAction, ev SlotEvent, before SlotState) {
	if act.WriteFence {
		s.fenceDisk, s.hasFence = act.Fence, true
	}
	if act.RemoveRecord {
		if s.rangeOnDisk != slotRangeEmpty {
			s.fail("the record was removed over a range that still holds data (%d)", s.rangeOnDisk)
		}
		if before == SlotRetired || before == SlotRetiring {
			s.cov.removedAfterRetire++
		}
	}
	if act.Do == SlotDeleteRangeAndForget {
		// The caller removed the record and then dropped the range.
		s.rangeOnDisk = slotRangeEmpty
		s.cov.dropped++
		// A replaced record holds no world memory (as after a reload), so the
		// reference forgets its own.
		s.rec = SlotRecord{}
		s.ref.forget()
	}
	_, isStart := ev.(RetireStarted)
	_, isDeleted := ev.(RetireDeleted)
	switch {
	case isStart && act.Do == SlotDeleteRange:
		// Property: a delete starts only when the gate holds on what the other
		// nodes really hold.
		fence := s.fenceDisk
		ok := s.hasFence && len(s.copies) > 0
		for _, c := range s.copies {
			ok = ok && s.truthCovers(c, fence)
		}
		if !ok {
			s.fail("the first delete batch was allowed without every member of %v attested ready at %v (truth %v)", s.copies, fence, s.truth)
		}
		s.cov.gateOpened++
		s.rangeOnDisk, s.deleteRunning, s.deletePending = slotRangePartial, true, false
	case isStart:
		s.cov.prematureRefused++
		if len(s.copies) == 0 {
			s.cov.emptyGateRefused++
		}
	case isDeleted:
		s.deleteRunning = false
	}
	switch ev.(type) {
	case Reconciled, PositionReplied:
		s.deletePending = act.Do == SlotDeleteRange || act.Do == SlotDeleteRangeThenSeed
	}
	if act.Do == SlotFinishDeleteFirst {
		s.deleteRunning = true
	}
	if act.Do == SlotRestartSeed && act.Restart == 2 {
		s.cov.horizonRestarts++
	}
	switch act.Do {
	case SlotQueueSeed, SlotRestartSeed:
		if s.rnd.Intn(10) < 8 {
			s.beginAttempt(act, before)
		}
	case SlotWriteReadyEmpty:
		// The caller writes the interim: ready, nothing applied, no seed
		// identity. The machine's world memory stays; an empty pair is whole.
		s.rec.State, s.rec.Phase, s.rec.Applied, s.rec.SeedID = SlotReady, PhaseNone, cluster.Position{}, [16]byte{}
		s.rangeOnDisk = slotRangeFull
	}
}

// beginAttempt mirrors U12's AttemptWritten: a fresh attempt record, allowed
// only over an absent, seeding or ready record.
func (s *slotSim) beginAttempt(act SlotAction, before SlotState) {
	switch s.rec.State {
	case SlotAbsent:
		if s.rangeOnDisk != slotRangeEmpty {
			s.fail("an attempt began over a range that holds data (%d)", s.rangeOnDisk)
		}
		if act.Do == SlotRestartSeed && act.RemoveRecord {
			s.cov.rejoinReseeds++
		}
	case SlotSeeding, SlotReady:
	default:
		return // U12 refuses an attempt on a retired record
	}
	s.rec.State, s.rec.Phase, s.rec.Cut, s.rec.Applied, s.rec.Cursor = SlotSeeding, PhaseReset, cluster.Position{}, cluster.Position{}, nil
	s.rangeOnDisk = slotRangePartial
}

func (s *slotSim) seedProgress() {
	if s.rec.State != SlotSeeding {
		s.feed(SeedFinished{Pos: slotPos(s.epoch, 20)}) // premature: nothing may change
		return
	}
	switch s.rec.Phase {
	case PhaseReset:
		s.rec.Phase = PhaseSnapshot
		s.rec.Cut = slotPos(s.epoch, 10)
		switch n := s.rnd.Intn(8); {
		case n < 2 && s.epoch > 1:
			s.rec.Cut = slotPos(s.epoch-1, 10) // a quiet pair's cut: its last position, in an earlier epoch
		case n == 2:
			s.rec.Cut = slotPos(s.epoch+1, 10) // a source that is ahead of this node's view
		}
		s.rec.Applied = s.rec.Cut
	case PhaseSnapshot:
		s.rec.Phase = PhaseBuffer
	case PhaseBuffer:
		s.rec.Phase = PhaseNone
	default:
		s.rangeOnDisk = slotRangeFull // the drain and the sync are done: the range really is whole
		s.feed(SeedFinished{Pos: slotPos(s.epoch, 20)})
		if s.rec.State == SlotReady {
			s.cov.seedsFinished++
		}
	}
}

func (s *slotSim) reconcile(newEpoch bool) {
	if newEpoch {
		for _, node := range s.copies {
			if s.rnd.Intn(2) == 0 {
				tr := s.truth[node]
				s.past = append(s.past, slotPast{node, PositionReplied{Node: node, Epoch: s.epoch, State: tr.state, Applied: tr.applied}})
			}
		}
		s.epoch++
		if s.rnd.Intn(2) == 0 {
			s.inTarget = !s.inTarget
		}
		s.copies = nil
		for _, p := range slotPeerNames {
			if s.rnd.Intn(5) > 0 {
				s.copies = append(s.copies, p)
			}
		}
		if s.rnd.Intn(15) == 0 {
			s.copies = nil
		}
		for _, p := range slotPeerNames {
			if s.rnd.Intn(2) == 0 {
				s.rerollTruth(p)
			}
		}
		if len(s.copies) > 0 && s.rnd.Intn(5) == 0 {
			s.rerollTruth(s.copies[s.rnd.Intn(len(s.copies))]) // a reused identity: same name, a different disk
			s.cov.reusedIdentity++
		}
		s.ownBit, s.rank1, s.cur = s.rnd.Intn(2) == 0, s.rnd.Intn(3) == 0, s.rnd.Intn(4) > 0
		// Seeding stays the common outcome for an absent pair (about 94%) so the
		// seedsFinished floor holds; the ready-empty row still fires every seed.
		s.joining, s.othersHold = s.rnd.Intn(4) > 0, s.rnd.Intn(4) > 0
		s.hint = cluster.Position{}
		if s.rnd.Intn(3) == 0 {
			s.hint = s.position()
		}
	}
	v := SlotView{InTargetSet: s.inTarget, OwnBit: s.ownBit, Rank1: s.rank1, Current: s.cur, Joining: s.joining, OthersHoldBit: s.othersHold, Copies: slices.Clone(s.copies),
		Fence: s.fenceDisk, HasFence: s.hasFence, FenceHint: s.hint}
	s.cov.reconciles++
	if s.rec.State == SlotRetired && s.inTarget {
		s.cov.retiredBack++
	}
	ev := Reconciled{View: v, Epoch: s.epoch}
	s.views = append(s.views, ev)
	s.feed(ev)
}

// replayReconciled delivers a Reconciled that was queued behind newer ones.
// Only a machine that already holds a view can tell it is old.
func (s *slotSim) replayReconciled() {
	if !s.ref.known || len(s.views) == 0 {
		return
	}
	ev := s.views[s.rnd.Intn(len(s.views))]
	if ev.Epoch < s.epoch {
		s.cov.staleViews++
	}
	s.feed(ev)
}

func (s *slotSim) evidence() {
	nodes := append(slices.Clone(slotPeerNames), "node-z")
	node := nodes[s.rnd.Intn(len(nodes))]
	tr := s.truth[node]
	switch s.rnd.Intn(6) {
	case 0, 1, 2: // an honest reply, in this epoch, from the target's own record
		s.feed(PositionReplied{Node: node, Epoch: s.epoch, State: tr.state, Applied: tr.applied})
	case 3: // a reply gathered in an earlier epoch, delivered late
		if len(s.past) > 0 {
			p := s.past[s.rnd.Intn(len(s.past))]
			if p.reply.Epoch != s.epoch {
				s.cov.staleIgnored++
				s.feed(p.reply)
			}
		}
	case 4: // a seeding target reports its cut, the highest position it could claim
		s.cov.seedingIgnored++
		s.feed(PositionReplied{Node: node, Epoch: s.epoch, State: SlotSeeding, Applied: slotPos(s.epoch+1, 99999)})
	default: // a claim by somebody else, a bit, the old owner
		s.cov.claims++
		s.feed(PositionClaimed{Node: node, From: "node-x", Applied: slotPos(s.epoch+1, 99999)})
	}
}

func (s *slotSim) reload() {
	if s.rec.State == SlotRetired || s.rec.State == SlotRetiring {
		s.cov.reloadMidRetire++
	}
	s.cov.reloads++
	s.rec = slotReload(s.rec)
	s.ref.forget()
	s.deletePending = false
	if s.rec.State != SlotRetiring {
		s.deleteRunning = false
	}
}

func (s *slotSim) run(steps int) {
	for i := 0; i < steps; i++ {
		switch n := s.rnd.Intn(100); {
		case n < 12:
			s.reconcile(true)
		case n < 20:
			s.reconcile(false)
		case n < 48:
			s.evidence()
		case n < 58:
			if s.deletePending && s.rnd.Intn(5) > 0 {
				s.feed(RetireStarted{Fence: s.fenceDisk})
			} else {
				fence := s.fenceDisk
				if s.rnd.Intn(3) == 0 {
					fence = s.position()
				}
				s.feed(RetireStarted{Fence: fence})
			}
		case n < 66:
			if s.deleteRunning && s.rec.State == SlotRetiring && s.rnd.Intn(4) > 0 {
				s.rangeOnDisk = slotRangeEmpty
				s.feed(RetireDeleted{})
			} else if s.rec.State != SlotRetiring {
				s.feed(RetireDeleted{})
			}
		case n < 71:
			s.feed(RetireCommitted{})
		case n < 85:
			s.seedProgress()
		case n < 88:
			s.feed(SeedSourceAsked{Requester: "node-r"})
		case n < 90:
			s.feed(SeedRetryAsked{})
		case n < 92:
			s.feed(LogBelowHorizon{})
		case n < 97:
			s.replayReconciled()
		default:
			s.reload()
		}
	}
}

// TestSlotModel runs 20 fixed seeds. Properties checked at every step: the
// machine equals the reference; only the table's edges are taken (so a retire
// that has begun deleting is never cancelled and never returns to ready); a
// record is ready only over a whole range; a delete starts only when every
// member of the new copy set really holds the fence; a record is removed only
// over an empty range; a slot that rejoins re-seeds from nothing; a reload at
// any step lands on a state the table allows. [guard: cancel a retire, open the
// gate on a claim, a stale reply or a seeding reply, or remove a record early]
func TestSlotModel(t *testing.T) {
	total := slotCoverage{}
	for seed := int64(1); seed <= 20; seed++ {
		cov := &slotCoverage{}
		s := &slotSim{t: t, rnd: rand.New(rand.NewSource(seed)), cov: cov, epoch: 1, inTarget: true, truth: map[string]slotTruth{}, rangeOnDisk: slotRangeFull}
		s.rec = SlotRecord{State: SlotReady, Applied: slotPos(1, 50), Source: "src", SeedID: slotSeedID}
		for _, p := range slotPeerNames {
			s.rerollTruth(p)
		}
		s.copies = slices.Clone(slotPeerNames[:2])
		s.run(5000)
		total.gateOpened += cov.gateOpened
		total.rejoinReseeds += cov.rejoinReseeds
		total.staleIgnored += cov.staleIgnored
		total.seedingIgnored += cov.seedingIgnored
		total.claims += cov.claims
		total.reloadMidRetire += cov.reloadMidRetire
		total.prematureRefused += cov.prematureRefused
		total.removedAfterRetire += cov.removedAfterRetire
		total.horizonRestarts += cov.horizonRestarts
		total.dropped += cov.dropped
		total.retiredBack += cov.retiredBack
		total.seedsFinished += cov.seedsFinished
		total.reconciles += cov.reconciles
		total.reloads += cov.reloads
		total.emptyGateRefused += cov.emptyGateRefused
		total.reusedIdentity += cov.reusedIdentity
		total.staleViews += cov.staleViews
		total.retiredSources += cov.retiredSources
		total.earlierCutResumes += cov.earlierCutResumes
		total.laterCutRestarts += cov.laterCutRestarts
		total.movedEpochRestarts += cov.movedEpochRestarts
		total.equalCutResumes += cov.equalCutResumes
	}
	t.Logf("coverage over 20 seeds: %+v", total)
	if total.gateOpened < 50 || total.removedAfterRetire < 30 || total.seedsFinished < 30 {
		t.Errorf("the 20 seeds are too thin: %d deletes allowed, %d records removed after a retire, %d seeds finished", total.gateOpened, total.removedAfterRetire, total.seedsFinished)
	}
	for name, n := range map[string]int{
		"deletes allowed by the gate": total.gateOpened, "rejoin re-seeds": total.rejoinReseeds, "stale replies": total.staleIgnored,
		"seeding replies": total.seedingIgnored, "claims": total.claims, "reloads mid-retire": total.reloadMidRetire,
		"premature deletes refused": total.prematureRefused, "records removed after a retire": total.removedAfterRetire,
		"horizon restarts": total.horizonRestarts, "partial seeds dropped": total.dropped, "retired and back in target": total.retiredBack,
		"seeds finished": total.seedsFinished, "empty member sets refused": total.emptyGateRefused, "reused identities": total.reusedIdentity, "stale Reconciled views": total.staleViews,
		"retired holders served as a seed source": total.retiredSources, "committed-epoch moves restarted": total.movedEpochRestarts,
		"earlier-epoch cuts resumed": total.earlierCutResumes, "later-epoch cuts restarted": total.laterCutRestarts,
		"equal-epoch cuts resumed": total.equalCutResumes,
	} {
		if n == 0 {
			t.Errorf("the 20 seeds never reached: %s", name)
		}
	}
}
