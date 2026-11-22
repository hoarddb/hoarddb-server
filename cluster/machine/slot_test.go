package machine

import (
	"bytes"
	"slices"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// Every package-level identifier in this file and slot_model_test.go starts
// with "slot": all machine units share one test package.

const slotEpoch = 7

func slotPos(epoch, seq uint64) cluster.Position { return cluster.Position{Epoch: epoch, Seq: seq} }

var (
	slotFence    = slotPos(slotEpoch, 100)
	slotPeers    = []string{"node-b", "node-c"}
	slotSeedID   = [16]byte{9}
	slotAllWorld = []SlotState{SlotAbsent, SlotSeeding, SlotReady, SlotRetired, SlotRetiring}
)

// slotView is a view of a pair with two other copies, a fence on disk and a
// current node; mods adjust it.
func slotView(mods ...func(*SlotView)) SlotView {
	v := SlotView{Copies: slices.Clone(slotPeers), Fence: slotFence, HasFence: true, Current: true}
	for _, m := range mods {
		m(&v)
	}
	return v
}

func slotIn(v *SlotView)         { v.InTargetSet = true }
func slotBit(v *SlotView)        { v.OwnBit = true }
func slotJoining(v *SlotView)    { v.Joining = true }
func slotOthersHold(v *SlotView) { v.OthersHoldBit = true }
func slotRangeHolds(v *SlotView) { v.RangeHoldsKeys = true }
func slotBehind(v *SlotView)     { v.Current = false }
func slotRank1(v *SlotView)      { v.Rank1 = true }
func slotNoFence(v *SlotView)    { v.HasFence = false }
func slotNoCopies(v *SlotView)   { v.Copies = nil }
func slotWithHint(p cluster.Position) func(*SlotView) {
	return func(v *SlotView) { v.FenceHint = p }
}

// slotRec builds a record. A seeding record past the reset is registered in
// the current epoch; mods adjust it.
func slotRec(state SlotState, mods ...func(*SlotRecord)) SlotRecord {
	r := SlotRecord{State: state, Applied: slotFence, Source: "src", SeedID: slotSeedID}
	switch state {
	case SlotSeeding:
		r.Phase, r.Cut, r.Applied = PhaseSnapshot, slotPos(slotEpoch, 50), slotPos(slotEpoch, 50)
	case SlotAbsent:
		r.Applied, r.Source, r.SeedID = cluster.Position{}, "", [16]byte{}
	}
	for _, m := range mods {
		m(&r)
	}
	return r
}

func slotPhase(p Phase) func(*SlotRecord) { return func(r *SlotRecord) { r.Phase = p } }
func slotCut(p cluster.Position) func(*SlotRecord) {
	return func(r *SlotRecord) { r.Cut = p }
}

func slotReconcile(r *SlotRecord, v SlotView) SlotAction { return slotReconcileAt(r, v, slotEpoch) }

func slotReconcileAt(r *SlotRecord, v SlotView, epoch uint64) SlotAction {
	return r.Apply(Reconciled{View: v, Epoch: epoch})
}

func slotReplyAt(r *SlotRecord, node string, epoch uint64, state SlotState, applied cluster.Position) SlotAction {
	return r.Apply(PositionReplied{Node: node, Epoch: epoch, State: state, Applied: applied})
}

func slotReply(r *SlotRecord, node string, state SlotState, applied cluster.Position) SlotAction {
	return r.Apply(PositionReplied{Node: node, Epoch: slotEpoch, State: state, Applied: applied})
}

// slotOpenGate reconciles r against v and then has every other copy attest it
// is ready at the fence; it returns the last action.
func slotOpenGate(r *SlotRecord, v SlotView) SlotAction {
	act := slotReconcile(r, v)
	for _, node := range v.Copies {
		act = slotReply(r, node, SlotReady, slotFence)
	}
	return act
}

// slotReload is a crash and a restart: only the persisted fields survive.
func slotReload(r SlotRecord) SlotRecord {
	return SlotRecord{State: r.State, Phase: r.Phase, Applied: r.Applied, Cut: r.Cut, Cursor: bytes.Clone(r.Cursor), Source: r.Source, SeedID: r.SeedID}
}

// slotPersisted compares the persisted fields only.
func slotPersisted(a, b SlotRecord) bool {
	return a.State == b.State && a.Phase == b.Phase && a.Applied == b.Applied && a.Cut == b.Cut &&
		bytes.Equal(a.Cursor, b.Cursor) && a.Source == b.Source && a.SeedID == b.SeedID
}

func slotSame(a, b SlotAction) bool {
	return a.Do == b.Do && a.Restart == b.Restart && a.Serve == b.Serve && a.CountsAsCopy == b.CountsAsCopy &&
		a.Refusal == b.Refusal && a.SeedRefused == b.SeedRefused && a.WriteFence == b.WriteFence && a.Fence == b.Fence &&
		a.ProposeRetire == b.ProposeRetire && a.Ignored == b.Ignored && a.RemoveRecord == b.RemoveRecord && slices.Equal(a.Waiting, b.Waiting)
}

func slotCheck(t *testing.T, name string, got, want SlotAction) {
	t.Helper()
	if !slotSame(got, want) {
		t.Errorf("%s: action = %+v, want %+v", name, got, want)
	}
}

// slotShape compares the parts of an action a table line is about; Waiting is
// compared only when the line names it.
func slotShape(a SlotAction) SlotAction {
	a.Waiting = nil
	return a
}

// TestSlotStatesAreSeedStates pins that the lifecycle and the seed machine
// persist the same state byte (conv §12).
func TestSlotStatesAreSeedStates(t *testing.T) {
	for _, c := range []struct {
		got  SlotState
		want uint8
	}{{SlotSeeding, SeedSeeding}, {SlotReady, SeedReady}, {SlotRetired, SeedRetired}, {SlotRetiring, SeedRetiring}, {SlotAbsent, 0}} {
		if uint8(c.got) != c.want {
			t.Errorf("SlotState %d != seed state %d", c.got, c.want)
		}
	}
}

// TestSlotReconciliationTable is one case per line of the table in build doc
// §4.10 / seeding §3, plus the lines the gate adds. [guard: change any row's
// action, or let a retired slot that comes back stay ready]
func TestSlotReconciliationTable(t *testing.T) {
	nothing := SlotAction{}
	cases := []struct {
		name      string
		rec       SlotRecord
		view      SlotView
		evidence  bool // every other copy attested ready at the fence before the checked reconcile
		want      SlotAction
		wantState SlotState
	}{
		{"absent, not in target", slotRec(SlotAbsent), slotView(), false, nothing, SlotAbsent},
		{"absent, in target", slotRec(SlotAbsent), slotView(slotIn), false, SlotAction{Do: SlotWriteReadyEmpty}, SlotAbsent},
		{"absent, in target, stale bit set", slotRec(SlotAbsent), slotView(slotIn, slotBit), false, SlotAction{Do: SlotWriteReadyEmpty}, SlotAbsent},
		{"absent, in target, joining", slotRec(SlotAbsent), slotView(slotIn, slotJoining), false, SlotAction{Do: SlotQueueSeed}, SlotAbsent},
		{"absent, in target, another member holds the bit", slotRec(SlotAbsent), slotView(slotIn, slotOthersHold), false, SlotAction{Do: SlotQueueSeed}, SlotAbsent},
		{"absent, in target, joining and another member holds the bit", slotRec(SlotAbsent), slotView(slotIn, slotJoining, slotOthersHold), false, SlotAction{Do: SlotQueueSeed}, SlotAbsent},
		{"absent, in target, no bit set, the range holds keys (BUG-0819)", slotRec(SlotAbsent), slotView(slotIn, slotRangeHolds), false, SlotAction{Do: SlotQueueSeed}, SlotAbsent},
		{"seeding, same cut and epoch", slotRec(SlotSeeding), slotView(slotIn), false, SlotAction{Do: SlotResumeSeed}, SlotSeeding},
		{"seeding in buffer phase, same cut and epoch", slotRec(SlotSeeding, slotPhase(PhaseBuffer)), slotView(slotIn), false, SlotAction{Do: SlotResumeSeed}, SlotSeeding},
		{"seeding, a quiet pair's cut from an earlier epoch resumes", slotRec(SlotSeeding, slotCut(slotPos(slotEpoch-1, 50))), slotView(slotIn), false, SlotAction{Do: SlotResumeSeed}, SlotSeeding},
		{"seeding, a cut from a later epoch restarts", slotRec(SlotSeeding, slotCut(slotPos(slotEpoch+1, 50))), slotView(slotIn), false, SlotAction{Do: SlotRestartSeed, Restart: 3}, SlotSeeding},
		{"seeding, registered but the cut was never written", slotRec(SlotSeeding, slotCut(cluster.Position{})), slotView(slotIn), false, SlotAction{Do: SlotRestartSeed, Restart: 3}, SlotSeeding},
		{"seeding, attempt written but never registered", slotRec(SlotSeeding, slotPhase(PhaseReset), slotCut(cluster.Position{})), slotView(slotIn), false, SlotAction{Do: SlotRestartSeed, Restart: 4}, SlotSeeding},
		{"seeding, a phase nobody can read", slotRec(SlotSeeding, slotPhase(Phase(9))), slotView(slotIn), false, SlotAction{Do: SlotRestartSeed, Restart: 4}, SlotSeeding},
		{"seeding, not in target", slotRec(SlotSeeding), slotView(), false, SlotAction{Do: SlotDeleteRangeAndForget}, SlotSeeding},
		{"ready, in target, own bit set", slotRec(SlotReady), slotView(slotIn, slotBit), false, SlotAction{Do: SlotCatchUp, Serve: true, CountsAsCopy: true}, SlotReady},
		{"ready, in target, own bit clear, current", slotRec(SlotReady), slotView(slotIn), false, SlotAction{Do: SlotReportBit, Serve: true, CountsAsCopy: true}, SlotReady},
		{"ready, in target, own bit clear, behind", slotRec(SlotReady), slotView(slotIn, slotBehind), false, SlotAction{Do: SlotCatchUp, Serve: true, CountsAsCopy: true}, SlotReady},
		{"ready, behind, rank 1: catches up and answers no read", slotRec(SlotReady), slotView(slotIn, slotBit, slotBehind, slotRank1), false, SlotAction{Do: SlotCatchUp, CountsAsCopy: true}, SlotReady},
		{"ready, current rank 1 serves", slotRec(SlotReady), slotView(slotIn, slotBit, slotRank1), false, SlotAction{Do: SlotCatchUp, Serve: true, CountsAsCopy: true}, SlotReady},
		{"ready, not in target: retire with the fence", slotRec(SlotReady), slotView(), false, SlotAction{Do: SlotRetire, WriteFence: true, Fence: slotFence}, SlotRetired},
		{"ready, not in target, handoff fence is larger", slotRec(SlotReady), slotView(slotWithHint(slotPos(slotEpoch, 130))), false, SlotAction{Do: SlotRetire, WriteFence: true, Fence: slotPos(slotEpoch, 130)}, SlotRetired},
		{"ready, not in target, handoff fence is smaller", slotRec(SlotReady), slotView(slotWithHint(slotPos(slotEpoch, 3))), false, SlotAction{Do: SlotRetire, WriteFence: true, Fence: slotFence}, SlotRetired},
		{"retired, not in target, no evidence: gate", slotRec(SlotRetired), slotView(), false, SlotAction{Do: SlotGate}, SlotRetired},
		{"retired, not in target, 's' missing: write it", slotRec(SlotRetired), slotView(slotNoFence), false, SlotAction{Do: SlotGate, WriteFence: true, Fence: slotFence}, SlotRetired},
		{"retired, not in target, every member attested: delete", slotRec(SlotRetired), slotView(), true, SlotAction{Do: SlotDeleteRange}, SlotRetired},
		{"retired, back in target, no evidence: not cancelled, gate", slotRec(SlotRetired), slotView(slotIn, slotBit), false, SlotAction{Do: SlotGate}, SlotRetired},
		{"retired, back in target, evidence: delete range then seed", slotRec(SlotRetired), slotView(slotIn, slotBit), true, SlotAction{Do: SlotDeleteRangeThenSeed}, SlotRetired},
		{"retiring, not in target", slotRec(SlotRetiring), slotView(), false, SlotAction{Do: SlotFinishDeleteFirst}, SlotRetiring},
		{"retiring, back in target", slotRec(SlotRetiring), slotView(slotIn, slotBit), false, SlotAction{Do: SlotFinishDeleteFirst}, SlotRetiring},
		{"a record nobody can read is left alone", slotRec(SlotState(9)), slotView(slotIn), false, nothing, SlotState(9)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := c.rec
			if c.evidence {
				slotOpenGate(&r, c.view)
			}
			got := slotReconcile(&r, c.view)
			slotCheck(t, c.name, slotShape(got), slotShape(c.want))
			if r.State != c.wantState {
				t.Errorf("state = %d, want %d", r.State, c.wantState)
			}
			if c.want.Do == SlotGate && !c.want.WriteFence && len(got.Waiting) != len(slotPeers) {
				t.Errorf("gate waits for %v, want both peers", got.Waiting)
			}
		})
	}
}

// TestSlotLinesFedByOtherEvents is the table's non-reconcile lines: each event
// of build doc §4.10 against the records it can meet.
func TestSlotLinesFedByOtherEvents(t *testing.T) {
	retiredOpen := func(mods ...func(*SlotView)) SlotRecord {
		r := slotRec(SlotRetired)
		slotOpenGate(&r, slotView(mods...))
		return r
	}
	reconciled := func(state SlotState, mods ...func(*SlotView)) SlotRecord {
		r := slotRec(state)
		slotReconcile(&r, slotView(mods...))
		return r
	}
	cases := []struct {
		name      string
		rec       SlotRecord
		ev        SlotEvent
		want      SlotAction
		wantState SlotState
	}{
		{"source asked while ready", slotRec(SlotReady), SeedSourceAsked{Requester: "x"}, SlotAction{}, SlotReady},
		{"source asked while absent: not a holder", slotRec(SlotAbsent), SeedSourceAsked{}, SlotAction{SeedRefused: 3}, SlotAbsent},
		{"source asked while seeding: never a source", slotRec(SlotSeeding), SeedSourceAsked{}, SlotAction{SeedRefused: 4}, SlotSeeding},
		{"source asked while retired", slotRec(SlotRetired), SeedSourceAsked{}, SlotAction{SeedRefused: 4}, SlotRetired},
		{"source asked while retiring", slotRec(SlotRetiring), SeedSourceAsked{}, SlotAction{SeedRefused: 4}, SlotRetiring},
		{"source asked of a record nobody can read", slotRec(SlotState(9)), SeedSourceAsked{}, SlotAction{SeedRefused: 4}, SlotState(9)},
		{"retry while seeding restarts on a fresh cut", slotRec(SlotSeeding), SeedRetryAsked{}, SlotAction{Do: SlotRestartSeed, Restart: 4}, SlotSeeding},
		{"retry while ready is refused", slotRec(SlotReady), SeedRetryAsked{}, SlotAction{Refusal: cluster.CodeSeedNotHeld}, SlotReady},
		{"retry while absent is refused", slotRec(SlotAbsent), SeedRetryAsked{}, SlotAction{Refusal: cluster.CodeSeedNotHeld}, SlotAbsent},
		{"retry while retired is refused", slotRec(SlotRetired), SeedRetryAsked{}, SlotAction{Refusal: cluster.CodeSeedNotHeld}, SlotRetired},
		{"retry while retiring is refused", slotRec(SlotRetiring), SeedRetryAsked{}, SlotAction{Refusal: cluster.CodeSeedNotHeld}, SlotRetiring},
		{"seed finished: ready, bit reported", func() SlotRecord {
			r := slotRec(SlotSeeding, slotPhase(PhaseNone))
			slotReconcile(&r, slotView(slotIn))
			return r
		}(), SeedFinished{Pos: slotFence}, SlotAction{Do: SlotReportBit, Serve: true, CountsAsCopy: true}, SlotReady},
		{"seed finished before the drain is done is ignored", slotRec(SlotSeeding, slotPhase(PhaseBuffer)), SeedFinished{Pos: slotFence}, SlotAction{}, SlotSeeding},
		{"seed finished on a retired record is ignored", slotRec(SlotRetired), SeedFinished{Pos: slotFence}, SlotAction{}, SlotRetired},
		{"horizon on a ready copy restarts the seed and serves nothing", reconciled(SlotReady, slotIn, slotBit), LogBelowHorizon{}, SlotAction{Do: SlotRestartSeed, Restart: 2}, SlotReady},
		{"horizon on a copy out of the set does nothing", reconciled(SlotReady), LogBelowHorizon{}, SlotAction{}, SlotRetired},
		{"first delete batch writes retiring", retiredOpen(), RetireStarted{Fence: slotFence}, SlotAction{Do: SlotDeleteRange}, SlotRetiring},
		{"delete started on a ready copy is refused outright", slotRec(SlotReady), RetireStarted{Fence: slotFence}, SlotAction{}, SlotReady},
		{"delete finished out of the set proposes RETIRE", func() SlotRecord {
			r := retiredOpen()
			r.Apply(RetireStarted{Fence: slotFence})
			return r
		}(), RetireDeleted{}, SlotAction{ProposeRetire: true}, SlotRetired},
		{"delete finished on a record that never started is ignored", retiredOpen(), RetireDeleted{}, SlotAction{}, SlotRetired},
		{"RETIRE committed before the range is deleted keeps the record", retiredOpen(), RetireCommitted{}, SlotAction{}, SlotRetired},
		{"a claim changes nothing", retiredOpen(), PositionClaimed{Node: "node-b", From: "node-c", Applied: slotFence}, SlotAction{}, SlotRetired},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := c.rec
			got := r.Apply(c.ev)
			slotCheck(t, c.name, slotShape(got), slotShape(c.want))
			if r.State != c.wantState {
				t.Errorf("state = %d, want %d", r.State, c.wantState)
			}
		})
	}
}

// TestSlotRetiredCopyIsCancelledWhenTheSlotComesBack: the retire is not
// cancellable. A retired slot that rejoins the copy set deletes and seeds from
// scratch and is never `ready` over its old range, whatever the evidence.
// [guard: in retiredAction, when the slot is back in the target set, set State to SlotReady and return SlotCatchUp]
func TestSlotRetiredCopyIsCancelledWhenTheSlotComesBack(t *testing.T) {
	for _, evidence := range []bool{false, true} {
		r := slotRec(SlotRetired)
		back := slotView(slotIn, slotBit, slotOthersHold, slotNoCopies, func(v *SlotView) { v.Copies = slices.Clone(slotPeers) })
		if evidence {
			slotOpenGate(&r, back)
		}
		act := slotReconcile(&r, back)
		if r.State == SlotReady || act.Serve || act.CountsAsCopy || act.Do == SlotCatchUp || act.Do == SlotReportBit {
			t.Fatalf("evidence=%v: a retired slot came back as %+v in state %d", evidence, act, r.State)
		}
		want := SlotGate
		if evidence {
			want = SlotDeleteRangeThenSeed
		}
		if act.Do != want {
			t.Errorf("evidence=%v: Do = %d, want %d", evidence, act.Do, want)
		}
	}
	// The whole way back: delete, record removed, seed again from nothing.
	r := slotRec(SlotRetired)
	back := slotView(slotIn, slotOthersHold)
	slotOpenGate(&r, back)
	if act := r.Apply(RetireStarted{Fence: slotFence}); act.Do != SlotDeleteRange || r.State != SlotRetiring {
		t.Fatalf("delete start: %+v state %d", act, r.State)
	}
	act := r.Apply(RetireDeleted{})
	if !act.RemoveRecord || act.Do != SlotRestartSeed || act.Restart != 4 || r.State != SlotAbsent {
		t.Fatalf("delete finished while back in the set: %+v state %d, want the record removed and a seed from reset", act, r.State)
	}
	if act := slotReconcile(&r, back); act.Do != SlotQueueSeed {
		t.Errorf("the removed record queues a seed, got Do=%d", act.Do)
	}
}

// TestSlotASeedingSlotIsNeverACopy: a slot serves nothing until ready. Any
// record that is not ready and in the copy set counts for nothing, whatever
// event it meets. [guard: count a seeding slot for the concern]
func TestSlotASeedingSlotIsNeverACopy(t *testing.T) {
	events := func() []SlotEvent {
		return []SlotEvent{
			Reconciled{View: slotView(slotIn, slotBit, slotRank1), Epoch: slotEpoch},
			Reconciled{View: slotView(), Epoch: slotEpoch},
			SeedSourceAsked{}, SeedRetryAsked{}, LogBelowHorizon{}, RetireStarted{Fence: slotFence}, RetireDeleted{},
			RetireCommitted{}, PositionReplied{Node: "node-b", Epoch: slotEpoch, State: SlotReady, Applied: slotFence},
			PositionClaimed{Node: "node-b", Applied: slotFence},
		}
	}
	for _, state := range []SlotState{SlotAbsent, SlotSeeding, SlotRetired, SlotRetiring} {
		for _, phase := range []Phase{PhaseNone, PhaseReset, PhaseSnapshot, PhaseBuffer} {
			for i, ev := range events() {
				r := slotRec(state, slotPhase(phase))
				act := r.Apply(ev)
				if act.Serve || act.CountsAsCopy {
					t.Errorf("state %d phase %d event %d (%T): serve=%v counts=%v", state, phase, i, ev, act.Serve, act.CountsAsCopy)
				}
			}
		}
	}
	// The one record that does count: ready, in the copy set, reconciled.
	ready := slotRec(SlotReady)
	if act := slotReconcile(&ready, slotView(slotIn, slotBit)); !act.Serve || !act.CountsAsCopy {
		t.Errorf("a ready copy in the set must serve and count: %+v", act)
	}
	// A ready copy that has not been reconciled in this process serves nothing either.
	r := slotRec(SlotReady)
	if act := r.Apply(SeedSourceAsked{}); act.Serve || act.CountsAsCopy {
		t.Errorf("an unreconciled ready record serves: %+v", act)
	}
}

// TestSlotASeedingPairNotInTargetKeepsItsRecord: the row answers the executor
// and leaves the record seeding, so a lost answer is asked again on the next
// pass; the executor removes the record, and removes it before the range, so a
// crash can never leave a resumable cut over a deleted range.
// [guard: call r.remove() in the row]
func TestSlotASeedingPairNotInTargetKeepsItsRecord(t *testing.T) {
	r := slotRec(SlotSeeding)
	for pass := 1; pass <= 2; pass++ {
		act := slotReconcile(&r, slotView())
		slotCheck(t, "seeding, not in target", slotShape(act), SlotAction{Do: SlotDeleteRangeAndForget})
		if r.State != SlotSeeding {
			t.Fatalf("pass %d: state = %d after the row, want seeding (the executor removes the record)", pass, r.State)
		}
		if act.RemoveRecord {
			t.Fatalf("pass %d: the row set RemoveRecord; the record must outlive the answer", pass)
		}
	}
}

// TestSlotRetiringIsWrittenWithTheFirstDelete: the event that carries the first
// delete batch is the one that flips the state, and nothing earlier does.
// [guard: flip the state after the delete]
func TestSlotRetiringIsWrittenWithTheFirstDelete(t *testing.T) {
	r := slotRec(SlotReady)
	slotReconcile(&r, slotView())
	if r.State != SlotRetired {
		t.Fatalf("after the retire order the state = %d, want retired", r.State)
	}
	slotOpenGate(&r, slotView())
	if r.State != SlotRetired {
		t.Fatalf("a passed gate must not flip the state by itself: %d", r.State)
	}
	act := r.Apply(RetireStarted{Fence: slotFence})
	if act.Do != SlotDeleteRange || r.State != SlotRetiring {
		t.Fatalf("RetireStarted: %+v state %d, want a delete order with state=retiring in the same step", act, r.State)
	}
}

// TestSlotNoDeleteWithoutEveryMembersAttestation is the gate. [guard: accept a
// seeding reply, skip a member, or pass on an empty member set]
func TestSlotNoDeleteWithoutEveryMembersAttestation(t *testing.T) {
	type step func(r *SlotRecord)
	reply := func(node string, state SlotState, pos cluster.Position) step {
		return func(r *SlotRecord) { slotReply(r, node, state, pos) }
	}
	cases := []struct {
		name  string
		view  SlotView
		steps []step
		open  bool
	}{
		{"every member ready at the fence", slotView(), []step{reply("node-b", SlotReady, slotFence), reply("node-c", SlotReady, slotFence)}, true},
		{"every member ready past the fence", slotView(), []step{reply("node-b", SlotReady, slotPos(slotEpoch, 101)), reply("node-c", SlotReady, slotPos(slotEpoch, 900))}, true},
		{"a later epoch covers the fence", slotView(), []step{reply("node-b", SlotReady, slotPos(slotEpoch+1, 1)), reply("node-c", SlotReady, slotFence)}, true},
		{"one member silent", slotView(), []step{reply("node-b", SlotReady, slotFence)}, false},
		{"one member one position short", slotView(), []step{reply("node-b", SlotReady, slotFence), reply("node-c", SlotReady, slotPos(slotEpoch, 99))}, false},
		{"one member in an earlier epoch is not evidence", slotView(), []step{reply("node-b", SlotReady, slotFence), reply("node-c", SlotReady, slotPos(slotEpoch-1, 9999))}, false},
		{"one member seeding claims the fence as its cut", slotView(), []step{reply("node-b", SlotReady, slotFence), reply("node-c", SlotSeeding, slotPos(slotEpoch, 5000))}, false},
		{"a ready reply followed by a seeding one withdraws it", slotView(), []step{reply("node-b", SlotReady, slotFence), reply("node-c", SlotReady, slotFence), reply("node-c", SlotSeeding, slotFence)}, false},
		{"a retired member is not a copy", slotView(), []step{reply("node-b", SlotReady, slotFence), reply("node-c", SlotRetired, slotFence)}, false},
		{"an outsider's reply counts for nobody", slotView(), []step{reply("node-b", SlotReady, slotFence), reply("node-z", SlotReady, slotFence)}, false},
		{"no member set at all", slotView(slotNoCopies), nil, false},
		{"no fence on disk", slotView(slotNoFence), []step{reply("node-b", SlotReady, slotFence), reply("node-c", SlotReady, slotFence)}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := slotRec(SlotRetired)
			slotReconcile(&r, c.view)
			for _, s := range c.steps {
				s(&r)
			}
			act := r.Apply(RetireStarted{Fence: slotFence})
			opened := act.Do == SlotDeleteRange && r.State == SlotRetiring
			if opened != c.open {
				t.Errorf("delete allowed = %v, want %v (action %+v, state %d)", opened, c.open, act, r.State)
			}
			if !c.open && r.State != SlotRetired {
				t.Errorf("a refused delete changed the state to %d", r.State)
			}
		})
	}
	t.Run("another fence than the one the gate passed for", func(t *testing.T) {
		r := slotRec(SlotRetired)
		slotOpenGate(&r, slotView())
		act := r.Apply(RetireStarted{Fence: slotPos(slotEpoch, 1)})
		if act.Do == SlotDeleteRange || r.State != SlotRetired {
			t.Errorf("a delete started on a fence the evidence was not gathered for: %+v state %d", act, r.State)
		}
	})
	t.Run("a restarted node has no evidence and no view", func(t *testing.T) {
		r := slotRec(SlotRetired)
		slotOpenGate(&r, slotView())
		reloaded := slotReload(r)
		if act := reloaded.Apply(RetireStarted{Fence: slotFence}); act.Do == SlotDeleteRange || reloaded.State != SlotRetired {
			t.Errorf("a reloaded record deleted before it reconciled: %+v", act)
		}
		slotReply(&reloaded, "node-b", SlotReady, slotFence)
		slotReply(&reloaded, "node-c", SlotReady, slotFence)
		if act := reloaded.Apply(RetireStarted{Fence: slotFence}); act.Do == SlotDeleteRange {
			t.Errorf("replies without a view opened the gate: %+v", act)
		}
	})
}

// TestSlotAClaimedPositionIsNeverEvidence: a position a node has not itself
// applied, told by a peer, a bit or the old owner, never opens the gate.
// [guard: count a claim as an attestation]
func TestSlotAClaimedPositionIsNeverEvidence(t *testing.T) {
	r := slotRec(SlotRetired)
	slotReconcile(&r, slotView())
	for _, node := range slotPeers {
		for _, from := range []string{"node-z", node, ""} {
			r.Apply(PositionClaimed{Node: node, From: from, Applied: slotPos(slotEpoch+5, 1)})
		}
	}
	if act := r.Apply(RetireStarted{Fence: slotFence}); act.Do == SlotDeleteRange || r.State != SlotRetired {
		t.Fatalf("claims opened the gate: %+v", act)
	}
	// One attested member plus one claim still waits for the claimed one.
	slotReply(&r, "node-b", SlotReady, slotFence)
	r.Apply(PositionClaimed{Node: "node-c", Applied: slotFence})
	act := r.Apply(RetireStarted{Fence: slotFence})
	if act.Do == SlotDeleteRange || !slices.Equal(act.Waiting, []string{"node-c"}) {
		t.Errorf("one attested and one claimed: %+v, want a wait naming node-c", act)
	}
}

// TestSlotStaleEvidenceIsIgnored: a reply from another epoch, or gathered
// before the epoch or the member set changed, is not evidence now.
// [guard: ignore the envelope's epoch, or keep evidence across a reconcile]
func TestSlotStaleEvidenceIsIgnored(t *testing.T) {
	r := slotRec(SlotRetired)
	slotReconcile(&r, slotView())
	r.Apply(PositionReplied{Node: "node-b", Epoch: slotEpoch - 1, State: SlotReady, Applied: slotFence})
	r.Apply(PositionReplied{Node: "node-c", Epoch: slotEpoch + 1, State: SlotReady, Applied: slotFence})
	if act := r.Apply(RetireStarted{Fence: slotFence}); act.Do == SlotDeleteRange {
		t.Fatalf("replies from other epochs opened the gate: %+v", act)
	}
	// Evidence gathered, then the epoch moves: the gate starts over.
	slotOpenGate(&r, slotView())
	act := r.Apply(Reconciled{View: slotView(), Epoch: slotEpoch + 1})
	if act.Do != SlotGate || len(act.Waiting) != 2 {
		t.Fatalf("a new epoch must ask again: %+v", act)
	}
	// Evidence gathered, then a member is replaced in the set (a new epoch): same.
	r2 := slotRec(SlotRetired)
	slotOpenGate(&r2, slotView())
	act = slotReconcileAt(&r2, slotView(func(v *SlotView) { v.Copies = []string{"node-b", "node-d"} }), slotEpoch+1)
	if act.Do != SlotGate || !slices.Equal(act.Waiting, []string{"node-b", "node-d"}) {
		t.Fatalf("a changed member set must ask again: %+v", act)
	}
	// The same view again keeps what was gathered.
	r3 := slotRec(SlotRetired)
	slotOpenGate(&r3, slotView())
	if act := slotReconcile(&r3, slotView()); act.Do != SlotDeleteRange {
		t.Errorf("an unchanged view lost its evidence: %+v", act)
	}
}

// TestSlotRecordIsRemovedOnlyAfterTheRangeIsDeleted is seeding §8's tail: RETIRE
// first, then the record goes, and never over data. [guard: remove on
// RetireCommitted without the deleted range, or before RETIRE is committed]
func TestSlotRecordIsRemovedOnlyAfterTheRangeIsDeleted(t *testing.T) {
	r := slotRec(SlotRetired)
	slotOpenGate(&r, slotView())
	if act := r.Apply(RetireCommitted{}); act.RemoveRecord || r.State != SlotRetired {
		t.Fatalf("the record was removed over a range that was never deleted: %+v", act)
	}
	r.Apply(RetireStarted{Fence: slotFence})
	if act := r.Apply(RetireCommitted{}); act.RemoveRecord || r.State != SlotRetiring {
		t.Fatalf("the record was removed while the delete runs: %+v", act)
	}
	act := r.Apply(RetireDeleted{})
	if !act.ProposeRetire || act.RemoveRecord || r.State != SlotRetired {
		t.Fatalf("a finished delete proposes RETIRE and keeps the record until it commits: %+v state %d", act, r.State)
	}
	// RETIRE is proposed again on every reconcile until it commits.
	if again := slotReconcile(&r, slotView()); !again.ProposeRetire || again.Do == SlotDeleteRange {
		t.Fatalf("a reconcile after the delete: %+v", again)
	}
	act = r.Apply(RetireCommitted{})
	if !act.RemoveRecord || r.State != SlotAbsent || r.Applied != (cluster.Position{}) || r.Source != "" {
		t.Fatalf("RETIRE committed: %+v record %+v, want the record gone", act, r)
	}
	// After a crash the delete is forgotten, and the gate must pass again before a repeat delete.
	r2 := slotRec(SlotRetired)
	slotOpenGate(&r2, slotView())
	r2.Apply(RetireStarted{Fence: slotFence})
	r2.Apply(RetireDeleted{})
	reloaded := slotReload(r2)
	if act := reloaded.Apply(RetireCommitted{}); act.RemoveRecord {
		t.Errorf("a reloaded record removed itself without knowing its range is empty: %+v", act)
	}
}

// TestSlotRetiringThatIsBackInTheSetSeedsFromReset: seeding §3's last line.
func TestSlotRetiringThatIsBackInTheSetSeedsFromReset(t *testing.T) {
	r := slotRec(SlotRetiring)
	if act := slotReconcile(&r, slotView(slotIn, slotBit)); act.Do != SlotFinishDeleteFirst || act.Serve || act.CountsAsCopy {
		t.Fatalf("reconcile: %+v", act)
	}
	act := r.Apply(RetireDeleted{})
	if act.Do != SlotRestartSeed || act.Restart != 4 || !act.RemoveRecord || r.State != SlotAbsent || act.ProposeRetire {
		t.Fatalf("delete finished back in the set: %+v state %d", act, r.State)
	}
	// Out of the set it proposes RETIRE instead, and never seeds.
	r2 := slotRec(SlotRetiring)
	slotReconcile(&r2, slotView())
	act = r2.Apply(RetireDeleted{})
	if act.Do != SlotDoNothing || !act.ProposeRetire || act.RemoveRecord || r2.State != SlotRetired {
		t.Fatalf("delete finished out of the set: %+v state %d", act, r2.State)
	}
}

// TestSlotRefusalsChangeNothing: a refusal, an ignored event and a gate that
// waits leave the persisted record exactly as it was.
func TestSlotRefusalsChangeNothing(t *testing.T) {
	events := []SlotEvent{
		SeedSourceAsked{}, SeedRetryAsked{}, LogBelowHorizon{}, RetireCommitted{}, RetireDeleted{}, RetireStarted{Fence: slotPos(1, 1)},
		SeedFinished{Pos: slotFence}, PositionClaimed{Node: "node-b", Applied: slotFence},
		PositionReplied{Node: "node-z", Epoch: slotEpoch, State: SlotReady, Applied: slotFence},
	}
	for _, state := range append(slices.Clone(slotAllWorld), SlotState(9)) {
		for i, ev := range events {
			r := slotRec(state, slotPhase(PhaseBuffer))
			slotReconcile(&r, slotView(slotIn)) // may legitimately change it; the refusals below must not
			before := r
			before.Cursor = bytes.Clone(r.Cursor)
			act := r.Apply(ev)
			_, finishes := ev.(RetireDeleted) // a delete that was running does finish
			if !slotPersisted(before, r) && (!finishes || state != SlotRetiring) {
				t.Errorf("state %d event %d (%T): record changed %+v -> %+v on %+v", state, i, ev, before, r, act)
			}
		}
	}
}

// slotFlagPairs lists the (Joining, OthersHoldBit) values a view is crossed
// with: all four for a pair this node is in, only (false, false) otherwise.
func slotFlagPairs(inTarget bool) [][2]bool {
	if !inTarget {
		return [][2]bool{{false, false}}
	}
	return [][2]bool{{false, false}, {false, true}, {true, false}, {true, true}}
}

// TestSlotEveryEventClassHasExactlyOneRow enumerates the cross product of
// event kind, record state, phase, cut, Raft view and gate evidence, and fails
// if any class matches zero rows or two. [guard: widen or narrow one row's
// condition]
func TestSlotEveryEventClassHasExactlyOneRow(t *testing.T) {
	states := append(slices.Clone(slotAllWorld), SlotState(9))
	phases := []Phase{PhaseNone, PhaseReset, PhaseSnapshot, PhaseBuffer, Phase(9)}
	// The cut-epoch dimension is earlier, equal and later than the committed
	// epoch (and no cut at all): a quiet pair's cut may be earlier, and only a
	// later one is a mismatch.
	cuts := []cluster.Position{{}, slotPos(slotEpoch, 50), slotPos(slotEpoch-1, 50), slotPos(slotEpoch+1, 50)}
	type world struct {
		known, inTarget, hasFence bool
		evidence                  string // "", "open", "deleted"
	}
	worlds := []world{{}}
	for _, in := range []bool{false, true} {
		for _, fence := range []bool{false, true} {
			for _, ev := range []string{"", "open", "deleted"} {
				worlds = append(worlds, world{true, in, fence, ev})
			}
		}
	}
	// The Reconciled events cross the view with the epoch relation to a world
	// at slotEpoch (lower, equal, higher) and with the same or another member set.
	reconciled := func() []SlotEvent {
		var evs []SlotEvent
		for _, epoch := range []uint64{slotEpoch - 1, slotEpoch, slotEpoch + 1} {
			for _, other := range []bool{false, true} {
				for _, in := range []bool{false, true} {
					for _, bit := range []bool{false, true} {
						for _, cur := range []bool{false, true} {
							for _, rank1 := range []bool{false, true} {
								for _, fence := range []bool{false, true} {
									// Joining and OthersHoldBit only steer a pair this node is in; out of
									// the target set they stay false so the product does not double for nothing.
									for _, flags := range slotFlagPairs(in) {
										v := slotView()
										v.InTargetSet, v.OwnBit, v.Current, v.Rank1, v.HasFence = in, bit, cur, rank1, fence
										v.Joining, v.OthersHoldBit = flags[0], flags[1]
										if other {
											v.Copies = []string{"node-b"}
										}
										evs = append(evs, Reconciled{View: v, Epoch: epoch})
									}
								}
							}
						}
					}
				}
			}
		}
		return evs
	}()
	others := []SlotEvent{
		SeedFinished{Pos: slotFence},
		RetireStarted{Fence: slotFence}, RetireStarted{Fence: slotPos(1, 1)},
		RetireDeleted{}, RetireCommitted{}, SeedSourceAsked{Requester: "x"}, SeedRetryAsked{}, LogBelowHorizon{},
		PositionClaimed{Node: "node-b", From: "node-c", Applied: slotFence},
	}
	for _, node := range []string{"node-b", "node-z"} {
		for _, epoch := range []uint64{slotEpoch, slotEpoch + 1} {
			for _, st := range []SlotState{SlotAbsent, SlotSeeding, SlotReady, SlotRetired, SlotRetiring, SlotState(9)} {
				others = append(others, PositionReplied{Node: node, Epoch: epoch, State: st, Applied: slotFence})
			}
		}
	}
	events := append(reconciled, others...)

	classes := 0
	for _, state := range states {
		for _, phase := range phases {
			for _, cut := range cuts {
				for _, w := range worlds {
					for _, intact := range []bool{false, true} {
						if intact && (state != SlotRetired || w.evidence == "deleted") {
							continue // only a retired record whose delete has not finished is ever intact
						}
						r := slotRec(state, slotPhase(phase), slotCut(cut))
						r.intact = intact
						if w.known {
							v := slotView()
							v.InTargetSet, v.HasFence = w.inTarget, w.hasFence
							switch w.evidence {
							case "open":
								slotOpenGate(&r, v)
							case "deleted":
								slotOpenGate(&r, v)
								r.deleted = true
							default:
								r.adopt(Reconciled{View: v, Epoch: slotEpoch})
							}
						}
						for _, ev := range events {
							classes++
							if rows := matchingSlotRows(&r, ev); len(rows) != 1 {
								t.Fatalf("state %d intact %v phase %d cut %v world %+v event %T %+v: %d rows %v, want exactly 1", state, intact, phase, cut, w, ev, ev, len(rows), rows)
							}
						}
					}
				}
			}
		}
	}
	t.Logf("%d event classes, each matched exactly one row (%d rows in the table)", classes, len(slotRows))
	if classes < 50000 {
		t.Fatalf("enumerated only %d classes", classes)
	}
}

// TestSlotAStaleReconciledCannotReopenTheGate is the verifier's sequence: the
// machine holds epoch 8 with {b, c}; a queued Reconciled from epoch 7 with a
// smaller set arrives; a reply from epoch 7 must not become evidence.
// [guard: remove the epoch guard so a Reconciled replaces the world whatever its epoch]
func TestSlotAStaleReconciledCannotReopenTheGate(t *testing.T) {
	r := slotRec(SlotRetired)
	slotReconcileAt(&r, slotView(), slotEpoch+1)
	slotReplyAt(&r, "node-b", slotEpoch+1, SlotReady, slotFence)
	stale := slotView(func(v *SlotView) { v.Copies = []string{"node-c"} })
	if act := slotReconcileAt(&r, stale, slotEpoch); !act.Ignored || act.Do != SlotDoNothing {
		t.Fatalf("a Reconciled from an older epoch was not ignored: %+v", act)
	}
	slotReplyAt(&r, "node-c", slotEpoch, SlotReady, slotFence)
	act := r.Apply(RetireStarted{Fence: slotFence})
	if act.Do == SlotDeleteRange || r.State != SlotRetired {
		t.Fatalf("an old-epoch reply passed the gate after a stale Reconciled: %+v state %d", act, r.State)
	}
	if !slices.Equal(act.Waiting, []string{"node-c"}) {
		t.Errorf("the gate still waits for node-c at epoch %d, got %v", slotEpoch+1, act.Waiting)
	}
	slotReplyAt(&r, "node-c", slotEpoch+1, SlotReady, slotFence)
	if act := r.Apply(RetireStarted{Fence: slotFence}); act.Do != SlotDeleteRange {
		t.Errorf("the current-epoch reply must complete the gate: %+v", act)
	}
}

// TestSlotReconciledNeverLowersTheEpoch: a Reconciled older than the held view
// is ignored whole (no state change, no action); the same epoch with another
// member set, another membership of the target set or a fence that differs
// from the one known is ignored too; a higher epoch replaces the world and
// discards the stored evidence. [guard: remove the epoch guard]
func TestSlotReconciledNeverLowersTheEpoch(t *testing.T) {
	held := func() SlotRecord {
		r := slotRec(SlotRetired)
		slotOpenGate(&r, slotView(func(v *SlotView) { v.InTargetSet = false }))
		return r
	}
	cases := []struct {
		name    string
		epoch   uint64
		view    SlotView
		ignored bool
		state   SlotState
		do      uint8
	}{
		{"older epoch, same view", slotEpoch - 1, slotView(), true, SlotRetired, SlotDoNothing},
		{"older epoch, back in target", slotEpoch - 1, slotView(slotIn), true, SlotRetired, SlotDoNothing},
		{"same epoch, another member set", slotEpoch, slotView(func(v *SlotView) { v.Copies = []string{"node-b"} }), true, SlotRetired, SlotDoNothing},
		{"same epoch, target membership flipped", slotEpoch, slotView(slotIn), true, SlotRetired, SlotDoNothing},
		{"same epoch, the fence on disk vanished", slotEpoch, slotView(slotNoFence), true, SlotRetired, SlotDoNothing},
		{"same epoch, another fence", slotEpoch, slotView(func(v *SlotView) { v.Fence = slotPos(slotEpoch, 5) }), true, SlotRetired, SlotDoNothing},
		{"same epoch, same view keeps the evidence", slotEpoch, slotView(), false, SlotRetired, SlotDeleteRange},
		{"higher epoch discards the evidence", slotEpoch + 1, slotView(), false, SlotRetired, SlotGate},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := held()
			act := slotReconcileAt(&r, c.view, c.epoch)
			if act.Ignored != c.ignored || act.Do != c.do || r.State != c.state {
				t.Errorf("ignored=%v do=%d state=%d, want ignored=%v do=%d state=%d (%+v)", act.Ignored, act.Do, r.State, c.ignored, c.do, c.state, act)
			}
			if c.ignored && (len(act.Waiting) != 0 || act.WriteFence || act.ProposeRetire || act.RemoveRecord) {
				t.Errorf("an ignored Reconciled asked for something: %+v", act)
			}
		})
	}
	// An ignored Reconciled leaves the record's own transitions alone: a ready
	// copy is not retired by a stale view that says it left the set.
	r := slotRec(SlotReady)
	slotReconcileAt(&r, slotView(slotIn, slotBit), slotEpoch)
	if act := slotReconcileAt(&r, slotView(), slotEpoch-1); !act.Ignored || r.State != SlotReady || !act.Serve {
		t.Errorf("a stale view retired or silenced a ready copy: %+v state %d", act, r.State)
	}
	// An unknown world (a restart) accepts the first view it is given.
	fresh := slotRec(SlotReady)
	if act := slotReconcileAt(&fresh, slotView(slotIn, slotBit), 3); act.Ignored || act.Do != SlotCatchUp {
		t.Errorf("the first Reconciled after a restart must be taken: %+v", act)
	}
}

// slotRegisteredPhases are the phases a registered attempt can be in: the
// record has a cut, so the resume-or-restart decision looks at it.
var slotRegisteredPhases = []Phase{PhaseSnapshot, PhaseBuffer, PhaseNone}

// slotSeedingAt builds a seeding record in phase with cut as its registered cut.
func slotSeedingAt(phase Phase, cut cluster.Position) SlotRecord {
	return slotRec(SlotSeeding, slotPhase(phase), slotCut(cut), func(r *SlotRecord) { r.Applied = cut })
}

// TestSlotAQuietPairWithAnEarlierEpochCutResumes: a quiet pair's cut is the last
// position that exists (conv §10 rule 5) and may belong to an earlier epoch, so
// a record that reloads with the cut (e-1, 5) under committed epoch e resumes.
// Restarting it would restart every attempt forever, because the next attempt
// registers the same quiet cut (seeding test 19, the bug U12 fixed).
// [guard: require Cut.Epoch == the committed epoch in slotResumable]
func TestSlotAQuietPairWithAnEarlierEpochCutResumes(t *testing.T) {
	for _, phase := range slotRegisteredPhases {
		for _, behind := range []uint64{1, 2, slotEpoch - 1} {
			cut := slotPos(slotEpoch-behind, 5)
			rec := slotSeedingAt(phase, cut)
			reloaded := slotReload(rec)
			// The reload is a restart: no view yet. Reconciling again, as the
			// caller does after every commit, must resume every time.
			for i := 0; i < 3; i++ {
				slotCheck(t, "reload", slotShape(slotReconcile(&reloaded, slotView(slotIn))), SlotAction{Do: SlotResumeSeed})
			}
			if !slotPersisted(slotReload(reloaded), slotReload(rec)) || reloaded.State != SlotSeeding {
				t.Errorf("phase %d cut %v: the resume changed the record: %+v", phase, cut, reloaded)
			}
		}
	}
}

// TestSlotALaterEpochCutRestarts: a cut from an epoch later than the committed
// one is a source ahead of this node's view; the history cannot be compared, so
// the attempt restarts with reason 3 (epoch changed), whatever the phase.
// [guard: resume when Cut.Epoch > the committed epoch]
func TestSlotALaterEpochCutRestarts(t *testing.T) {
	for _, phase := range slotRegisteredPhases {
		for _, ahead := range []uint64{1, 5} {
			cut := slotPos(slotEpoch+ahead, 5)
			rec := slotSeedingAt(phase, cut)
			reloaded := slotReload(rec)
			slotCheck(t, "reload", slotShape(slotReconcile(&reloaded, slotView(slotIn))), SlotAction{Do: SlotRestartSeed, Restart: 3})
			live := rec
			slotCheck(t, "live", slotShape(slotReconcile(&live, slotView(slotIn))), SlotAction{Do: SlotRestartSeed, Restart: 3})
		}
	}
}

// TestSlotAnEqualEpochCutResumes is the regression for the ordinary case: a
// cut registered in the committed epoch resumes, before and after a reload.
// [guard: restart when Cut.Epoch equals the committed epoch]
func TestSlotAnEqualEpochCutResumes(t *testing.T) {
	for _, phase := range slotRegisteredPhases {
		rec := slotSeedingAt(phase, slotPos(slotEpoch, 5))
		live := rec
		slotCheck(t, "live", slotShape(slotReconcile(&live, slotView(slotIn))), SlotAction{Do: SlotResumeSeed})
		reloaded := slotReload(rec)
		slotCheck(t, "reload", slotShape(slotReconcile(&reloaded, slotView(slotIn))), SlotAction{Do: SlotResumeSeed})
	}
}

// TestSlotACommittedEpochMoveAfterRegistrationRestarts is the slot machine's
// counterpart of SeedRecord.RegEpoch (seeding §6: "epoch changes mid-seed: the
// same attempt, restarted with a fresh cut"). The view the machine holds is the
// epoch it last reconciled under; a Reconciled at a later epoch for a record
// with a registered cut means the committed epoch moved while the attempt ran,
// so it restarts with reason 3 even though the cut itself is not later than the
// committed epoch. A reload holds no view and resumes: the caller's
// RegisterAccepted on the same cut is what registers it again.
// [guard: drop the moved-epoch test from slotResumable's caller]
func TestSlotACommittedEpochMoveAfterRegistrationRestarts(t *testing.T) {
	for _, phase := range slotRegisteredPhases {
		for _, cutEpoch := range []uint64{slotEpoch - 1, slotEpoch} {
			rec := slotSeedingAt(phase, slotPos(cutEpoch, 5))
			slotCheck(t, "before the move", slotShape(slotReconcile(&rec, slotView(slotIn))), SlotAction{Do: SlotResumeSeed})
			slotCheck(t, "after the move", slotShape(slotReconcileAt(&rec, slotView(slotIn), slotEpoch+1)), SlotAction{Do: SlotRestartSeed, Restart: 3})

			reloaded := slotReload(rec)
			slotCheck(t, "reloaded after the move", slotShape(slotReconcileAt(&reloaded, slotView(slotIn), slotEpoch+1)), SlotAction{Do: SlotResumeSeed})
		}
	}
}

// slotAgreeSeed is the seed machine's side of the epoch rule for a seeding
// record in phase with cut, under committed epoch committed. A record that was
// reloaded (heldAt == 0) is resumed by the caller's RegisterAccepted on the same
// cut; a record the machine registered itself in epoch heldAt meets its next
// attempt event (a SyncDone) in the committed epoch. It returns the
// SEED_RESTART reason, 0 for "proceeds".
func slotAgreeSeed(phase Phase, cut cluster.Position, cursor []byte, committed, heldAt uint64) uint8 {
	rec := SeedRecord{State: SeedSeeding, Phase: phase, Cut: cut, Applied: cut, Cursor: cursor, RegEpoch: heldAt}
	cfg := SeedConfig{Epoch: committed}
	if heldAt == 0 {
		return rec.Apply(RegisterAccepted{Cut: cut}, cfg).Restart
	}
	return rec.Apply(SyncDone{}, cfg).Restart
}

// slotAgreeSlot is the slot machine's side: the same record reconciled under
// the committed epoch, after a first Reconciled at heldAt when the machine held
// a view. It returns 0 for SlotResumeSeed and the restart reason otherwise.
func slotAgreeSlot(t *testing.T, phase Phase, cut cluster.Position, cursor []byte, committed, heldAt uint64) uint8 {
	t.Helper()
	rec := slotSeedingAt(phase, cut)
	rec.Cursor = cursor
	if heldAt != 0 {
		slotReconcileAt(&rec, slotView(slotIn), heldAt)
	}
	act := slotReconcileAt(&rec, slotView(slotIn), committed)
	switch act.Do {
	case SlotResumeSeed:
		return 0
	case SlotRestartSeed:
		return act.Restart
	}
	t.Fatalf("phase %d cut %v committed %d held %d: Do = %d, want resume or restart", phase, cut, committed, heldAt, act.Do)
	return 0
}

// TestSlotSeedAndSlotAgreeOnTheEpochRule feeds the same (cut epoch, committed
// epoch, phase) combinations to both machines and requires the same answer
// (proceed, or the same SEED_RESTART reason). The two machines each decide
// whether an attempt registered in an earlier epoch may go on, and they drifted
// once already; this is the table that stops it. It covers a reloaded record
// (RegisterAccepted on the same cut) and a live one whose attempt registered in
// the epoch before the committed one. [guard: restore the equality test in
// slotResumable, or accept a later epoch in either machine]
func TestSlotSeedAndSlotAgreeOnTheEpochRule(t *testing.T) {
	const committed = slotEpoch
	phases := []Phase{PhaseSnapshot, PhaseBuffer, PhaseNone, Phase(9)}
	cursors := [][]byte{nil, []byte("k")}
	combos := 0
	for _, phase := range phases {
		for _, cursor := range cursors {
			for _, cutEpoch := range []uint64{committed - 3, committed - 1, committed, committed + 1, committed + 4} {
				cut := slotPos(cutEpoch, 5)
				// heldAt 0 is a reload; a live attempt registered in heldAt, which
				// cannot be below the cut's epoch and is never ahead of committed.
				for _, heldAt := range []uint64{0, committed - 1, committed} {
					if heldAt != 0 && cutEpoch > heldAt {
						continue // the machine never registers a cut ahead of its own epoch
					}
					want := uint8(0)
					switch {
					case phase > PhaseBuffer:
						want = 4
					case cutEpoch > committed || heldAt != 0 && heldAt != committed:
						want = 3
					}
					seed := slotAgreeSeed(phase, cut, cursor, committed, heldAt)
					slot := slotAgreeSlot(t, phase, cut, cursor, committed, heldAt)
					combos++
					if seed != slot || slot != want {
						t.Errorf("phase %d cursor %q cut epoch %d (committed %d, held at %d): seed machine %d, slot machine %d, want %d",
							phase, cursor, cutEpoch, committed, heldAt, seed, slot, want)
					}
				}
			}
		}
	}
	t.Logf("%d combinations, the two machines agree on each", combos)

	// The two cases that have no counterpart in the seed machine, pinned here so
	// the difference is a decision and not an accident. A record with no cut was
	// never registered: the seed machine takes its first registration as an
	// ordinary one, while a reloaded record has lost the attempt and the slot
	// machine starts a new one. A phase `reset` record owes a range delete.
	for _, phase := range slotRegisteredPhases {
		if got := slotAgreeSlot(t, phase, cluster.Position{}, nil, committed, 0); got != 3 {
			t.Errorf("phase %d with no cut: slot machine %d, want a restart with reason 3", phase, got)
		}
	}
	if got := slotAgreeSlot(t, PhaseReset, cluster.Position{}, nil, committed, 0); got != 4 {
		t.Errorf("phase reset: slot machine %d, want a restart with reason 4", got)
	}
}

// TestSlotRecordWaitingForNamesTheMembersWithoutAnAttestation: the operator's view names
// the member the gate still waits for, and only that one. [guard: return every copy]
func TestSlotRecordWaitingForNamesTheMembersWithoutAnAttestation(t *testing.T) {
	r := slotRec(SlotRetired)
	if got := r.WaitingFor(); len(got) != 0 {
		t.Fatalf("a record never reconciled waits for %v, want nobody", got)
	}
	slotReconcile(&r, slotView(func(v *SlotView) { v.Copies = []string{"node-a", "node-b"} }))
	slotReply(&r, "node-a", SlotReady, slotFence)
	if got := r.WaitingFor(); !slices.Equal(got, []string{"node-b"}) {
		t.Fatalf("WaitingFor = %v, want [node-b]", got)
	}
	slotReply(&r, "node-b", SlotReady, slotFence)
	if got := r.WaitingFor(); len(got) != 0 {
		t.Errorf("both attested, WaitingFor = %v, want nobody", got)
	}
}

// TestSlotRecordAttestedCopiesCountsTheMembersThatCoverTheFence: the count is the copy-set
// members whose attestation covers the fence, so an unreconciled record, an empty copy set and
// a set nobody has attested all count 0. [BREAK: return len(copies)]
func TestSlotRecordAttestedCopiesCountsTheMembersThatCoverTheFence(t *testing.T) {
	r := slotRec(SlotRetired)
	if got := r.AttestedCopies(); got != 0 {
		t.Fatalf("a record never reconciled counts %d, want 0", got)
	}
	empty := slotRec(SlotRetired)
	slotReconcile(&empty, slotView(func(v *SlotView) { v.Copies = nil }))
	if got := empty.AttestedCopies(); got != 0 {
		t.Fatalf("an empty copy set counts %d, want 0", got)
	}
	slotReconcile(&r, slotView(func(v *SlotView) { v.Copies = []string{"node-a", "node-b"} }))
	if got := r.AttestedCopies(); got != 0 {
		t.Fatalf("nobody attested counts %d, want 0", got)
	}
	slotReply(&r, "node-a", SlotReady, slotFence)
	if got := r.AttestedCopies(); got != 1 {
		t.Errorf("one attested counts %d, want 1", got)
	}
	slotReply(&r, "node-b", SlotReady, slotFence)
	if got := r.AttestedCopies(); got != 2 {
		t.Errorf("both attested counts %d, want 2", got)
	}
}

// TestSlotARetiredRecordWithAWholeRangeIsASeedSource: a record that retires from ready in
// this process keeps a whole range until its delete starts, and answers a seed source request
// (BUG-0835). After a restart (the flag is memory), once the delete has started and once it
// has finished, it is refused as before. [guard: keep retired in source/not-current; keep the
// flag through the delete start; persist the flag across a reload]
func TestSlotARetiredRecordWithAWholeRangeIsASeedSource(t *testing.T) {
	r := slotRec(SlotReady)
	if act := slotReconcile(&r, slotView()); act.Do != SlotRetire || r.State != SlotRetired {
		t.Fatalf("a ready record out of the set: %+v state %d, want it retired", act, r.State)
	}
	if !r.RetiredIntact() {
		t.Fatal("a record retired from ready in this process does not read intact")
	}
	slotCheck(t, "retired in this process, delete not started", slotShape(r.Apply(SeedSourceAsked{Requester: "x"})), SlotAction{})
	reloaded := slotReload(r)
	if reloaded.RetiredIntact() {
		t.Error("a reloaded retired record reads intact; after a restart it cannot know")
	}
	slotCheck(t, "retired, after a restart", slotShape(reloaded.Apply(SeedSourceAsked{Requester: "x"})), SlotAction{SeedRefused: 4})

	slotOpenGate(&r, slotView())
	if act := r.Apply(RetireStarted{Fence: slotFence}); act.Do != SlotDeleteRange || r.State != SlotRetiring {
		t.Fatalf("delete start: %+v state %d", act, r.State)
	}
	if r.RetiredIntact() {
		t.Error("a retiring record reads intact")
	}
	slotCheck(t, "delete started", slotShape(r.Apply(SeedSourceAsked{Requester: "x"})), SlotAction{SeedRefused: 4})
	if act := r.Apply(RetireDeleted{}); !act.ProposeRetire || r.State != SlotRetired {
		t.Fatalf("delete finished out of the set: %+v state %d", act, r.State)
	}
	if r.RetiredIntact() {
		t.Error("a retired record whose delete finished reads intact")
	}
	slotCheck(t, "delete finished, awaiting RETIRE", slotShape(r.Apply(SeedSourceAsked{Requester: "x"})), SlotAction{SeedRefused: 4})
}
