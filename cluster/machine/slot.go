package machine

import (
	"slices"
	"strings"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// SlotState is the persisted state of one (slot, database) record (conv §12).
// The values are U12's SeedSeeding..SeedRetiring, with 0 for "no record".
type SlotState uint8

// The record states. SlotRetired means "out of the copy set, nothing deleted
// yet" until the range delete has finished, and then "deleted, awaiting the
// RETIRE commit" (SlotRecord.RetiredIntact tells the two apart, in the process
// that retired the record only); SlotRetiring means the range delete has started
// and can never be cancelled (seeding §8).
const (
	SlotAbsent SlotState = iota
	SlotSeeding
	SlotReady
	SlotRetired
	SlotRetiring
)

// The Do codes of a SlotAction. 1 to 10 are the build doc's (§4.10); 11 is
// added because the doc has no word for "the gate has passed, start deleting".
const (
	SlotDoNothing   uint8 = 0
	SlotQueueSeed   uint8 = 1 // queue a seed attempt for an absent record
	SlotResumeSeed  uint8 = 2 // resume on the pinned snapshot
	SlotRestartSeed uint8 = 3 // write a fresh attempt; Restart says why
	// 4 is unassigned: it was SlotDropPartial, which S3b replaced with 12.
	SlotCatchUp              uint8 = 5  // catch up from the owner's log
	SlotReportBit            uint8 = 6  // re-report the SEEDED bit
	SlotRetire               uint8 = 7  // state=3 and the 's' record, one batch
	SlotDeleteRangeThenSeed  uint8 = 8  // the slot came back: delete, then seed from reset
	SlotGate                 uint8 = 9  // ask POSITION_QUERY and wait; nothing may be deleted
	SlotFinishDeleteFirst    uint8 = 10 // a delete has started: finish it before anything else
	SlotDeleteRange          uint8 = 11 // the gate passed: write the first delete batch with state=4
	SlotDeleteRangeAndForget uint8 = 12 // a seeding pair left the copy set: delete its range and forget its record
	SlotWriteReadyEmpty      uint8 = 13 // an active node's absent in-target pair that no other member holds, over an empty range: write it ready and empty (the interim's signature)
)

// SlotRecord is the per-(slot, database) record of conv §12 as the lifecycle
// sees it. Persist the first seven fields after every Apply (the record and the
// data it describes move in one batch, seeding §2). The rest is memory the
// machine rebuilds after a restart: nothing in it may outlive the process,
// because evidence gathered before a crash is exactly the kind a gate must
// not trust.
type SlotRecord struct {
	State   SlotState
	Phase   Phase // U12's phase, persisted in the same record
	Applied cluster.Position
	Cut     cluster.Position
	Cursor  []byte
	Source  string
	SeedID  [16]byte

	world     slotWorld
	attest    map[string]cluster.Position // target -> position it attested as ready, under attestKey
	attestKey slotKey
	deleted   bool // the range delete finished in this process
	rebuild   bool // LOG_BELOW_HORIZON: serves nothing while the re-seed is asked for
	intact    bool // retired from ready in this process, and its range delete has not started (RetiredIntact)
}

// SlotView is what committed state and the caller's disk say about one pair.
// The world reaches the machine only through Reconciled.View: every decision
// after it reads the snapshot that event took, so a view that is stale by the
// time a delete starts cannot steer one.
type SlotView struct {
	InTargetSet bool // this node is in copies(s, d, e_current)
	OwnBit      bool // the committed SEEDED bit for this node
	Rank1       bool
	Current     bool // a completed catch-up at this epoch

	// OthersHoldBit is true when any other member of the pair's copy set
	// holds the committed SEEDED bit: some other node has the data, so an
	// absent record here must seed rather than start empty.
	OthersHoldBit bool
	// Joining is true when this node is joining in the committed state: a
	// joiner's pairs are always seeded, never written empty.
	Joining bool
	// RangeHoldsKeys is true when the pair's key range on this node's disk holds
	// a key. Under an absent record that is a range `seeding forget` kept
	// (BUG-0819): the pair is not a new, empty one, so it seeds, and the seed's
	// range reset deletes what was kept. The caller reads it for an absent
	// in-target pair only.
	RangeHoldsKeys bool

	// Copies is copies(s, d, e_current) minus this node: the members whose
	// own records the deletion gate asks about (seeding §8). A `down` member
	// stays in it, so a gate never passes without it.
	Copies []string
	// Fence and HasFence are the 's' reclaim record (conv §12).
	Fence    cluster.Position
	HasFence bool
	// FenceHint is the appliedPos of the HANDOFF_READY this node received as the
	// old owner, zero when it received none; the fence is the larger of it and
	// the record's applied position.
	FenceHint cluster.Position
}

// SlotEvent is one input to SlotRecord.Apply.
type SlotEvent interface{ isSlotEvent() }

// Reconciled says committed state moved, or the node booted: run seeding §3's
// table. Epoch is the committed epoch.
type Reconciled struct {
	View  SlotView
	Epoch uint64
}

// SeedFinished says U12 reported MarkReady: the sync returned and Pos is the
// final applied position.
type SeedFinished struct{ Pos cluster.Position }

// RetireStarted says the caller is about to write the first range-delete
// batch. Fence is the 's' record's fence the caller read; the machine accepts
// the event only when the gate passed for exactly that fence. Feed it BEFORE
// writing the batch and write the batch only if the action says SlotDeleteRange.
type RetireStarted struct{ Fence cluster.Position }

// SeedSourceAsked says a peer asked this node to be its seed source.
type SeedSourceAsked struct{ Requester string }

// SeedRetryAsked says the operator ran `seeding retry` for this pair.
type SeedRetryAsked struct{}

// LogBelowHorizon says a catch-up for this pair answered LOG_BELOW_HORIZON.
type LogBelowHorizon struct{}

// RetireCommitted says the RETIRE this node proposed is committed and its
// SEEDED bit is clear.
type RetireCommitted struct{}

// PositionReplied says a POSITION_REPLY arrived from Node, answered from its
// own record: its state and applied position, in the envelope's Epoch. It is
// the only evidence the deletion gate accepts (seeding §8, conv §10.1).
type PositionReplied struct {
	Node    string
	Epoch   uint64
	State   SlotState
	Applied cluster.Position
}

// PositionClaimed says somebody told this node a position that Node did not
// attest to itself: a peer's report, a SEEDED bit, an old owner's word. It is
// never evidence and changes nothing; it is an event so that the rule is a row.
type PositionClaimed struct {
	Node, From string
	Applied    cluster.Position
}

func (Reconciled) isSlotEvent()      {}
func (SeedFinished) isSlotEvent()    {}
func (RetireStarted) isSlotEvent()   {}
func (RetireDeleted) isSlotEvent()   {} // U12's type: the same fact, "the range delete finished"
func (SeedSourceAsked) isSlotEvent() {}
func (SeedRetryAsked) isSlotEvent()  {}
func (LogBelowHorizon) isSlotEvent() {}
func (RetireCommitted) isSlotEvent() {}
func (PositionReplied) isSlotEvent() {}
func (PositionClaimed) isSlotEvent() {}

// SlotAction is what one event asks the caller to do.
type SlotAction struct {
	Do      uint8
	Restart uint8 // SEED_RESTART reason for SlotRestartSeed: 2 log floor, 3 epoch changed, 4 range reset
	// Serve says this pair may answer a client read; CountsAsCopy says it counts
	// for the write concern, the gate and the bitmap. Both are false for every
	// record that is not ready and in the copy set. The read gate asks the same rule through
	// SlotRecord.Serves, so a copy answers on what this field says and not on the disk record.
	Serve        bool
	CountsAsCopy bool
	Refusal      cluster.Code // SEED_NOT_HELD when a retry is asked for a slot that is not seeding
	// SeedRefused is the SEED_REFUSED reason for SeedSourceAsked (3 not a
	// holder, 4 not current), 0 when this node may be the source. SEED_REFUSED
	// is a wire kind, not a cluster.Code, so it has its own field.
	SeedRefused uint8
	// WriteFence says write the 's' record with Fence: with the state change for
	// SlotRetire, alone for SlotGate. It must be durable before any delete.
	WriteFence bool
	Fence      cluster.Position
	// ProposeRetire says propose RETIRE now; send it again on every Reconciled
	// until RetireCommitted arrives.
	ProposeRetire bool
	// RemoveRecord says the record is now absent: delete its key. The machine
	// decides this and only after the range is empty (seeding §8).
	RemoveRecord bool
	// Waiting names the members the gate still waits for (seeding §8, §11).
	Waiting []string
	// Ignored says a Reconciled event was dropped because it is older than the
	// view the machine already holds; nothing changed and nothing is asked.
	Ignored bool
}

// slotWorld is the snapshot of the last Reconciled. It is memory only, so a
// restarted node decides nothing about a delete until it has reconciled again.
type slotWorld struct {
	known                    bool
	epoch                    uint64
	inTarget, rank1, current bool
	copies                   []string // sorted, no duplicates
	fence                    cluster.Position
	hasFence                 bool
	hint                     cluster.Position
}

// slotKey is what attested positions are valid under: the epoch and the
// members they were attested for.
type slotKey struct {
	epoch  uint64
	copies string
}

func (w slotWorld) key() slotKey { return slotKey{w.epoch, strings.Join(w.copies, "\x00")} }

func slotWorldOf(ev Reconciled) slotWorld {
	copies := slices.Clone(ev.View.Copies)
	slices.Sort(copies)
	return slotWorld{
		known: true, epoch: ev.Epoch,
		inTarget: ev.View.InTargetSet, rank1: ev.View.Rank1, current: ev.View.Current,
		copies: slices.Compact(copies),
		fence:  ev.View.Fence, hasFence: ev.View.HasFence, hint: ev.View.FenceHint,
	}
}

// adopt takes the event's view as the snapshot. Attested positions survive
// only while the epoch and the member set are the ones they were gathered for.
func (r *SlotRecord) adopt(ev Reconciled) slotWorld {
	w := slotWorldOf(ev)
	if r.attestKey != w.key() {
		r.attest, r.attestKey = nil, w.key()
	}
	r.world = w
	return w
}

// slotCovers is conv §10 rule 6: p covers fence f in the same epoch at or past
// it, or in a later epoch (reachable only through the boundary check). Two
// positions in other epochs are not ordered evidence.
func slotCovers(p, f cluster.Position) bool {
	return (p.Epoch == f.Epoch && p.Seq >= f.Seq) || p.Epoch > f.Epoch
}

// fenceFor is the fence a retire writes: the larger of the record's own applied
// position and the HANDOFF_READY position it received (seeding §8).
func (r *SlotRecord) fenceFor(w slotWorld) cluster.Position {
	if r.Applied.Less(w.hint) {
		return w.hint
	}
	return r.Applied
}

// gateHolds is the deletion gate: a fence is on disk, the member set is not
// empty, and every member attested ready, in this epoch, at a position that
// covers the fence. An empty set holds nothing: "every member" of nobody is
// not evidence that somebody else has the data.
func (r *SlotRecord) gateHolds(w slotWorld) bool {
	if !w.known || !w.hasFence || len(w.copies) == 0 || r.attestKey != w.key() {
		return false
	}
	for _, node := range w.copies {
		pos, ok := r.attest[node]
		if !ok || !slotCovers(pos, w.fence) {
			return false
		}
	}
	return true
}

// waiting names the members without an attestation that covers the fence.
func (r *SlotRecord) waiting(w slotWorld) []string {
	fence := w.fence
	if !w.hasFence {
		fence = r.fenceFor(w)
	}
	var names []string
	for _, node := range w.copies {
		if pos, ok := r.attest[node]; !ok || r.attestKey != w.key() || !slotCovers(pos, fence) {
			names = append(names, node)
		}
	}
	return names
}

// WaitingFor names the members the deletion gate still waits for, by the world of the
// last Reconciled; nil when it waits for nobody or the record never reconciled. It is the
// operator's view of what Apply's SlotAction.Waiting says at an event.
func (r *SlotRecord) WaitingFor() []string { return r.waiting(r.world) }

// AttestedCopies counts the members of the copy set whose attestation covers the fence, by the
// world of the last Reconciled: 0 when the record never reconciled, the copy set is empty or no
// member has attested. It is how an operator surface tells a retired record that other members
// back from one this node may hold the only copy of.
func (r *SlotRecord) AttestedCopies() int {
	return len(r.world.copies) - len(r.waiting(r.world))
}

// posture is whether the pair serves and counts. Only a ready record in the
// copy set, reconciled in this process, does; an owner that is not current has
// not finished its catch-up and answers no read yet.
func (r *SlotRecord) posture() (serve, counts bool) {
	if r.State != SlotReady || r.rebuild || !r.world.known || !r.world.inTarget {
		return false, false
	}
	return !r.world.rank1 || r.world.current, true
}

// Serves is posture's serve half for a pair that is not the owner: the record is ready, not
// asked to rebuild, reconciled in this process and in the target set. A copy may answer a
// read only when it is true.
func (r *SlotRecord) Serves() bool {
	serve, _ := r.posture()
	return serve && !r.world.rank1
}

// remove makes the record absent. Memory about the world stays, because the
// next Reconciled would rebuild it anyway.
func (r *SlotRecord) remove() {
	r.State, r.Phase, r.Applied, r.Cut, r.Cursor, r.Source, r.SeedID = SlotAbsent, PhaseNone, cluster.Position{}, cluster.Position{}, nil, "", [16]byte{}
	r.deleted, r.rebuild, r.intact = false, false, false
}

// RetiredIntact reports a retired record whose range is whole: the record left the copy set
// from ready in this process and its range delete has not started, so the range still holds
// the copy it held when it was ready (BUG-0835). It is memory only, like deleted: a record
// reloaded after a restart reads false, because a persisted SlotRetired cannot tell an intact
// range from a finished delete that awaits its RETIRE. False is the safe reading. Nothing
// that keeps or deletes the range reads it (the gate alone decides that), so reading false
// for an intact range only refuses it as a seed source; reading true for a deleted one would
// lend out a copy that is gone.
func (r *SlotRecord) RetiredIntact() bool { return r.State == SlotRetired && r.intact }

// SourceStanding is the part of a record that SeedSourceAsked reads, for a caller that answers
// from its node's reading of the record (the persisted state and RetiredIntact) rather than
// from the record itself, as the seed source does. retiredIntact counts only with SlotRetired.
func SourceStanding(state SlotState, retiredIntact bool) SlotRecord {
	return SlotRecord{State: state, intact: retiredIntact && state == SlotRetired}
}

// slotPlan is what a retired record does next.
type slotPlan uint8

const (
	slotPlanGate            slotPlan = iota // wait for the evidence
	slotPlanDelete                          // the gate passed and the range still holds data
	slotPlanProposeRetire                   // deleted, out of the set: RETIRE, then remove
	slotPlanSeedAfterDelete                 // deleted and back in the set: remove, seed again
)

func (r *SlotRecord) retiredPlan(w slotWorld) slotPlan {
	switch {
	case r.deleted && w.inTarget:
		return slotPlanSeedAfterDelete
	case r.deleted:
		return slotPlanProposeRetire
	case r.gateHolds(w):
		return slotPlanDelete
	}
	return slotPlanGate
}

// retiredAction is the whole decision for a retired record against world w.
func (r *SlotRecord) retiredAction(w slotWorld) SlotAction {
	switch r.retiredPlan(w) {
	case slotPlanSeedAfterDelete:
		r.remove()
		return SlotAction{Do: SlotRestartSeed, Restart: 4, RemoveRecord: true}
	case slotPlanProposeRetire:
		return SlotAction{ProposeRetire: true}
	case slotPlanDelete:
		if w.inTarget {
			return SlotAction{Do: SlotDeleteRangeThenSeed}
		}
		return SlotAction{Do: SlotDeleteRange}
	}
	act := SlotAction{Do: SlotGate, Waiting: r.waiting(w)}
	if w.known && !w.hasFence {
		act.WriteFence, act.Fence = true, r.fenceFor(w)
	}
	return act
}

// mayDelete is the one authority for starting a delete: the gate passed for
// exactly the fence the caller is about to rely on, and the range still holds
// data. A deleted range needs no delete.
func (r *SlotRecord) mayDelete(fence cluster.Position) bool {
	w := r.world
	return !r.deleted && w.known && w.hasFence && fence == w.fence && r.gateHolds(w)
}

// slotRow is one line of the decision table. Rows are mutually exclusive and
// together cover every event against every record, so which row fires never
// depends on their order; a test enumerates the classes and requires exactly
// one match for each.
type slotRow struct {
	name  string
	match func(*SlotRecord, SlotEvent) bool
	run   func(*SlotRecord, SlotEvent) SlotAction
}

func slotOn[E SlotEvent](name string, match func(*SlotRecord, E) bool, run func(*SlotRecord, E) SlotAction) slotRow {
	return slotRow{
		name: name,
		match: func(r *SlotRecord, ev SlotEvent) bool {
			e, ok := ev.(E)
			return ok && slotReadable(r) && match(r, e)
		},
		run: func(r *SlotRecord, ev SlotEvent) SlotAction { return run(r, ev.(E)) },
	}
}

// slotOnAny is slotOn for the two events that answer even for a record the
// machine cannot read, because a wrong answer there lends out a source or
// restarts a pull.
func slotOnAny[E SlotEvent](name string, match func(*SlotRecord, E) bool, run func(*SlotRecord, E) SlotAction) slotRow {
	return slotRow{
		name: name,
		match: func(r *SlotRecord, ev SlotEvent) bool {
			e, ok := ev.(E)
			return ok && match(r, e)
		},
		run: func(r *SlotRecord, ev SlotEvent) SlotAction { return run(r, ev.(E)) },
	}
}

func slotReadable(r *SlotRecord) bool { return r.State <= SlotRetiring }

func slotNothing(*SlotRecord, SlotEvent) SlotAction { return SlotAction{} }

// slotResumable is a seeding record whose attempt registered a cut that is not
// later than the committed epoch and that the committed epoch has not left
// behind: it can resume on the pinned snapshot. It is the slot machine's side of
// SeedRecord's epoch rule (conv §12 contract 3) and the two must agree:
//
//   - a cut from an equal or EARLIER epoch proceeds, because a quiet pair's cut
//     is the last position that exists (conv §10 rule 5) and may belong to an
//     earlier epoch; requiring equality restarts every attempt forever;
//   - a cut from a LATER epoch is a source ahead of this node's view, SEED_RESTART
//     reason 3;
//   - a zero cut was never registered;
//   - a committed epoch that moved after the attempt registered is stale
//     (seeding §6, "epoch changes mid-seed"). SeedRecord.RegEpoch records when it
//     registered; this machine's equivalent is the epoch of the last view it
//     held in this process. A reload holds no view, and the caller's
//     RegisterAccepted on the same cut registers it again (conv §12 contract 4).
func slotResumable(r *SlotRecord, epoch uint64) bool {
	registered := (r.Phase == PhaseSnapshot || r.Phase == PhaseBuffer || r.Phase == PhaseNone) && !r.Cut.Zero()
	return registered && r.Cut.Epoch <= epoch && (!r.world.known || r.world.epoch >= epoch)
}

func slotNeedsFreshAttempt(r *SlotRecord) bool { return r.Phase == PhaseReset || r.Phase > PhaseBuffer }

// slotStaleView is a Reconciled that is older than the view the machine holds.
// Epochs only grow, so a lower one is a queued old event. Within one epoch the
// member set, whether this node is in the target set, and a fence once known
// cannot change, so a view that says otherwise is stale too, and the safe
// answer to either is to keep the view already held: adopting it would let a
// reply from the epoch it names pass the epoch check. A machine with no view
// (a restart) takes the first one it is given.
func slotStaleView(r *SlotRecord, ev Reconciled) bool {
	w := r.world
	if !w.known {
		return false
	}
	if ev.Epoch != w.epoch {
		return ev.Epoch < w.epoch
	}
	v := ev.View
	return !slices.Equal(slotWorldOf(ev).copies, w.copies) || v.InTargetSet != w.inTarget ||
		(w.hasFence && (!v.HasFence || v.Fence != w.fence))
}

func slotReconciledOn(name string, match func(*SlotRecord, Reconciled) bool, run func(*SlotRecord, Reconciled, slotWorld) SlotAction) slotRow {
	return slotOn(name, func(r *SlotRecord, ev Reconciled) bool { return !slotStaleView(r, ev) && match(r, ev) },
		func(r *SlotRecord, ev Reconciled) SlotAction { return run(r, ev, r.adopt(ev)) })
}

func slotReplyAdmissible(r *SlotRecord, ev PositionReplied) bool {
	if r.State != SlotRetired || !r.world.known || ev.Epoch != r.world.epoch {
		return false
	}
	_, member := slices.BinarySearch(r.world.copies, ev.Node)
	return member
}

// slotRows is the whole decision table (build doc §4.10, seeding §3 and §8).
// A row marked "gap" fills a case the brief is silent on, with the reading
// that never lets a node delete data it is the last full holder of and never
// lets a half-run retire be cancelled.
var slotRows = []slotRow{
	{"any/unreadable-record", // gap: a state the machine cannot read is never touched
		func(r *SlotRecord, ev SlotEvent) bool {
			if _, ok := ev.(SeedSourceAsked); ok {
				return false
			}
			if _, ok := ev.(SeedRetryAsked); ok {
				return false
			}
			return !slotReadable(r)
		}, slotNothing},

	// Reconciled: seeding §3, one row per table line.
	slotOn("reconciled/older-than-the-held-view", // gap: a stale event changes nothing and asks for nothing
		func(r *SlotRecord, ev Reconciled) bool { return slotStaleView(r, ev) },
		func(*SlotRecord, Reconciled) SlotAction { return SlotAction{Ignored: true} }),
	slotReconciledOn("reconciled/absent-not-in-target",
		func(r *SlotRecord, ev Reconciled) bool { return r.State == SlotAbsent && !ev.View.InTargetSet },
		func(*SlotRecord, Reconciled, slotWorld) SlotAction { return SlotAction{} }),
	slotReconciledOn("reconciled/absent-in-target", // a set bit here is stale and is cleared by the joining commit
		func(r *SlotRecord, ev Reconciled) bool { return r.State == SlotAbsent && ev.View.InTargetSet },
		func(_ *SlotRecord, ev Reconciled, _ slotWorld) SlotAction {
			// A joiner always seeds, and so does any node when another member
			// holds the data. Only an active node whose pair nobody else holds
			// starts it ready and empty, and only over an empty range: keys under
			// an absent record are a retired copy that was forgotten, and a ready
			// record over them would serve that old copy as the pair (BUG-0819).
			if ev.View.Joining || ev.View.OthersHoldBit || ev.View.RangeHoldsKeys {
				return SlotAction{Do: SlotQueueSeed}
			}
			return SlotAction{Do: SlotWriteReadyEmpty}
		}),
	slotReconciledOn("reconciled/seeding-resumable",
		func(r *SlotRecord, ev Reconciled) bool {
			return r.State == SlotSeeding && ev.View.InTargetSet && slotResumable(r, ev.Epoch)
		},
		func(*SlotRecord, Reconciled, slotWorld) SlotAction { return SlotAction{Do: SlotResumeSeed} }),
	slotReconciledOn("reconciled/seeding-not-resumable",
		func(r *SlotRecord, ev Reconciled) bool {
			return r.State == SlotSeeding && ev.View.InTargetSet && !slotNeedsFreshAttempt(r) && !slotResumable(r, ev.Epoch)
		},
		func(*SlotRecord, Reconciled, slotWorld) SlotAction {
			return SlotAction{Do: SlotRestartSeed, Restart: 3}
		}),
	slotReconciledOn("reconciled/seeding-attempt-never-registered", // gap: phase reset has no cut to resume
		func(r *SlotRecord, ev Reconciled) bool {
			return r.State == SlotSeeding && ev.View.InTargetSet && slotNeedsFreshAttempt(r)
		},
		func(*SlotRecord, Reconciled, slotWorld) SlotAction {
			return SlotAction{Do: SlotRestartSeed, Restart: 4}
		}),
	slotReconciledOn("reconciled/seeding-not-in-target",
		func(r *SlotRecord, ev Reconciled) bool { return r.State == SlotSeeding && !ev.View.InTargetSet },
		func(*SlotRecord, Reconciled, slotWorld) SlotAction {
			// The record stays seeding: the row fires again on every pass until
			// the executor has removed it, so a lost answer is asked again.
			return SlotAction{Do: SlotDeleteRangeAndForget}
		}),
	slotReconciledOn("reconciled/ready-in-target-bit-set",
		func(r *SlotRecord, ev Reconciled) bool {
			return r.State == SlotReady && ev.View.InTargetSet && ev.View.OwnBit
		},
		func(*SlotRecord, Reconciled, slotWorld) SlotAction { return SlotAction{Do: SlotCatchUp} }),
	slotReconciledOn("reconciled/ready-in-target-bit-clear-current",
		func(r *SlotRecord, ev Reconciled) bool {
			return r.State == SlotReady && ev.View.InTargetSet && !ev.View.OwnBit && ev.View.Current
		},
		func(*SlotRecord, Reconciled, slotWorld) SlotAction { return SlotAction{Do: SlotReportBit} }),
	slotReconciledOn("reconciled/ready-in-target-bit-clear-behind", // gap: confirm current first, so catch up
		func(r *SlotRecord, ev Reconciled) bool {
			return r.State == SlotReady && ev.View.InTargetSet && !ev.View.OwnBit && !ev.View.Current
		},
		func(*SlotRecord, Reconciled, slotWorld) SlotAction { return SlotAction{Do: SlotCatchUp} }),
	slotReconciledOn("reconciled/ready-not-in-target",
		func(r *SlotRecord, ev Reconciled) bool { return r.State == SlotReady && !ev.View.InTargetSet },
		func(r *SlotRecord, _ Reconciled, w slotWorld) SlotAction {
			fence := r.fenceFor(w)
			r.State, r.Phase, r.Cut, r.Cursor, r.deleted, r.rebuild = SlotRetired, PhaseNone, cluster.Position{}, nil, false, false
			r.intact = true // a ready range is whole, and this retire deletes nothing
			return SlotAction{Do: SlotRetire, WriteFence: true, Fence: fence, Waiting: r.waiting(w)}
		}),
	slotReconciledOn("reconciled/retired", // retired in or out of the set: the gate decides, never the view alone
		func(r *SlotRecord, _ Reconciled) bool { return r.State == SlotRetired },
		func(r *SlotRecord, _ Reconciled, w slotWorld) SlotAction { return r.retiredAction(w) }),
	slotReconciledOn("reconciled/retiring", // gap: out of the set the delete still finishes, and nothing seeds after it
		func(r *SlotRecord, _ Reconciled) bool { return r.State == SlotRetiring },
		func(*SlotRecord, Reconciled, slotWorld) SlotAction { return SlotAction{Do: SlotFinishDeleteFirst} }),

	// SeedFinished.
	slotOn("finished/seeding-complete",
		func(r *SlotRecord, _ SeedFinished) bool { return r.State == SlotSeeding && r.Phase == PhaseNone },
		func(r *SlotRecord, ev SeedFinished) SlotAction {
			r.State, r.Applied, r.Cut, r.Cursor, r.rebuild = SlotReady, ev.Pos, cluster.Position{}, nil, false
			if r.world.known && r.world.inTarget {
				return SlotAction{Do: SlotReportBit}
			}
			return SlotAction{}
		}),
	slotOn("finished/not-complete", // gap: a stale event, or a seed whose drain is not done
		func(r *SlotRecord, _ SeedFinished) bool { return r.State != SlotSeeding || r.Phase != PhaseNone },
		func(*SlotRecord, SeedFinished) SlotAction { return SlotAction{} }),

	// RetireStarted.
	slotOn("delete-start/gate-passed",
		func(r *SlotRecord, ev RetireStarted) bool { return r.State == SlotRetired && r.mayDelete(ev.Fence) },
		func(r *SlotRecord, _ RetireStarted) SlotAction {
			// The flag drops with the state, before the caller writes the first delete batch,
			// so no reader of RetiredIntact sees true once a key may be gone.
			r.State, r.intact = SlotRetiring, false
			return SlotAction{Do: SlotDeleteRange}
		}),
	slotOn("delete-start/refused", // gap: no evidence, another fence, or an empty range: nothing is deleted
		func(r *SlotRecord, ev RetireStarted) bool { return r.State == SlotRetired && !r.mayDelete(ev.Fence) },
		func(r *SlotRecord, _ RetireStarted) SlotAction {
			return SlotAction{Do: SlotGate, Waiting: r.waiting(r.world)}
		}),
	slotOn("delete-start/already-retiring",
		func(r *SlotRecord, _ RetireStarted) bool { return r.State == SlotRetiring },
		func(*SlotRecord, RetireStarted) SlotAction { return SlotAction{Do: SlotFinishDeleteFirst} }),
	slotOn("delete-start/not-retired", // a ready or seeding copy is never deleted
		func(r *SlotRecord, _ RetireStarted) bool { return r.State != SlotRetired && r.State != SlotRetiring },
		func(*SlotRecord, RetireStarted) SlotAction { return SlotAction{} }),

	// RetireDeleted.
	slotOn("deleted/finished-and-back-in-target",
		func(r *SlotRecord, _ RetireDeleted) bool {
			return r.State == SlotRetiring && r.world.known && r.world.inTarget
		},
		func(r *SlotRecord, _ RetireDeleted) SlotAction {
			r.remove()
			return SlotAction{Do: SlotRestartSeed, Restart: 4, RemoveRecord: true}
		}),
	slotOn("deleted/finished-out-of-target",
		func(r *SlotRecord, _ RetireDeleted) bool {
			return r.State == SlotRetiring && (!r.world.known || !r.world.inTarget)
		},
		func(r *SlotRecord, _ RetireDeleted) SlotAction {
			r.State, r.Phase, r.Cut, r.Cursor, r.deleted, r.intact = SlotRetired, PhaseNone, cluster.Position{}, nil, true, false
			return SlotAction{ProposeRetire: true}
		}),
	slotOn("deleted/not-retiring", // gap: only a delete that started can finish
		func(r *SlotRecord, _ RetireDeleted) bool { return r.State != SlotRetiring },
		func(*SlotRecord, RetireDeleted) SlotAction { return SlotAction{} }),

	// RetireCommitted.
	slotOn("committed/remove-the-record", // seeding §8: RETIRE first, then the record goes
		func(r *SlotRecord, _ RetireCommitted) bool { return r.State == SlotRetired && r.deleted },
		func(r *SlotRecord, _ RetireCommitted) SlotAction {
			r.remove()
			return SlotAction{RemoveRecord: true}
		}),
	slotOn("committed/range-not-deleted", // gap: a record over data is never removed
		func(r *SlotRecord, _ RetireCommitted) bool { return r.State != SlotRetired || !r.deleted },
		func(*SlotRecord, RetireCommitted) SlotAction { return SlotAction{} }),

	// SeedSourceAsked: a seeding node is never a source, and a retired one only while its range
	// is whole (BUG-0835).
	slotOnAny("source/ready",
		func(r *SlotRecord, _ SeedSourceAsked) bool { return r.State == SlotReady },
		func(*SlotRecord, SeedSourceAsked) SlotAction { return SlotAction{} }),
	slotOnAny("source/retired-intact", // retired from ready in this process, delete not started
		func(r *SlotRecord, _ SeedSourceAsked) bool { return r.RetiredIntact() },
		func(*SlotRecord, SeedSourceAsked) SlotAction { return SlotAction{} }),
	slotOnAny("source/not-a-holder",
		func(r *SlotRecord, _ SeedSourceAsked) bool { return r.State == SlotAbsent },
		func(*SlotRecord, SeedSourceAsked) SlotAction { return SlotAction{SeedRefused: 3} }),
	slotOnAny("source/not-current", // seeding, retired and not intact, retiring, or a record nobody can read
		func(r *SlotRecord, _ SeedSourceAsked) bool {
			return r.State != SlotReady && r.State != SlotAbsent && !r.RetiredIntact()
		},
		func(*SlotRecord, SeedSourceAsked) SlotAction { return SlotAction{SeedRefused: 4} }),

	// SeedRetryAsked.
	slotOnAny("retry/seeding", // gap: a fresh cut on another source, never over a mixed range
		func(r *SlotRecord, _ SeedRetryAsked) bool { return r.State == SlotSeeding },
		func(*SlotRecord, SeedRetryAsked) SlotAction { return SlotAction{Do: SlotRestartSeed, Restart: 4} }),
	slotOnAny("retry/not-seeding",
		func(r *SlotRecord, _ SeedRetryAsked) bool { return r.State != SlotSeeding },
		func(*SlotRecord, SeedRetryAsked) SlotAction { return SlotAction{Refusal: cluster.CodeSeedNotHeld} }),

	// LogBelowHorizon: conv §10 rule 3.
	slotOn("horizon/ready-in-target",
		func(r *SlotRecord, _ LogBelowHorizon) bool {
			return r.State == SlotReady && r.world.known && r.world.inTarget
		},
		func(r *SlotRecord, _ LogBelowHorizon) SlotAction {
			r.rebuild = true
			return SlotAction{Do: SlotRestartSeed, Restart: 2}
		}),
	slotOn("horizon/nothing-to-catch-up",
		func(r *SlotRecord, _ LogBelowHorizon) bool {
			return r.State != SlotReady || !r.world.known || !r.world.inTarget
		},
		func(*SlotRecord, LogBelowHorizon) SlotAction { return SlotAction{} }),

	// PositionReplied: the only evidence the gate takes.
	slotOn("reply/not-retired", // gap: only a retired record runs the gate
		func(r *SlotRecord, ev PositionReplied) bool { return r.State != SlotRetired },
		func(*SlotRecord, PositionReplied) SlotAction { return SlotAction{} }),
	slotOn("reply/no-view", // gap: a restarted node decides nothing before it has reconciled
		func(r *SlotRecord, _ PositionReplied) bool { return r.State == SlotRetired && !r.world.known },
		func(*SlotRecord, PositionReplied) SlotAction { return SlotAction{} }),
	slotOn("reply/stale-epoch",
		func(r *SlotRecord, ev PositionReplied) bool {
			return r.State == SlotRetired && r.world.known && ev.Epoch != r.world.epoch
		},
		func(*SlotRecord, PositionReplied) SlotAction { return SlotAction{} }),
	slotOn("reply/not-a-member",
		func(r *SlotRecord, ev PositionReplied) bool {
			return r.State == SlotRetired && r.world.known && ev.Epoch == r.world.epoch && !slotReplyAdmissible(r, ev)
		},
		func(*SlotRecord, PositionReplied) SlotAction { return SlotAction{} }),
	slotOn("reply/target-not-ready", // a seeding target's applied position is its cut: a claim, not a copy
		func(r *SlotRecord, ev PositionReplied) bool {
			return slotReplyAdmissible(r, ev) && ev.State != SlotReady
		},
		func(r *SlotRecord, ev PositionReplied) SlotAction {
			delete(r.attest, ev.Node)
			return r.retiredAction(r.world)
		}),
	slotOn("reply/target-ready",
		func(r *SlotRecord, ev PositionReplied) bool {
			return slotReplyAdmissible(r, ev) && ev.State == SlotReady
		},
		func(r *SlotRecord, ev PositionReplied) SlotAction {
			if r.attest == nil {
				r.attest = map[string]cluster.Position{}
			}
			r.attest[ev.Node] = ev.Applied
			return r.retiredAction(r.world)
		}),

	// PositionClaimed.
	slotOn("claim/never-evidence",
		func(*SlotRecord, PositionClaimed) bool { return true },
		func(*SlotRecord, PositionClaimed) SlotAction { return SlotAction{} }),
}

// matchingSlotRows names every row whose condition holds for ev.
func matchingSlotRows(r *SlotRecord, ev SlotEvent) []string {
	var names []string
	for _, row := range slotRows {
		if row.match(r, ev) {
			names = append(names, row.name)
		}
	}
	return names
}

// Apply feeds one event to the record and says what the caller must do. The
// caller persists State, Phase, Applied, Cut, Cursor, Source and SeedID after
// every Apply, in the batch of the data work the action asks for. A refusal
// leaves the record exactly as it was.
//
// CONTRACT (the table relies on all of this; U44 and U46 must keep it):
//
//  1. The first event after a start is Reconciled. Everything the machine knows
//     about the world, the attested positions and "the range is deleted" is
//     memory; until a Reconciled arrives no delete is authorised. The world
//     arrives only through Reconciled.View. A later Reconciled with a lower
//     epoch, or the same epoch with another member set, another target
//     membership or a different known fence, is answered Ignored and changes
//     nothing; a higher epoch replaces the world and discards the evidence.
//     Feed the first Reconciled after a restart from the latest committed state:
//     a machine with no view cannot tell that one is old.
//  2. Feed RetireStarted before the first delete batch and write that batch,
//     with state=4, only when the action says SlotDeleteRange. Feed
//     RetireDeleted when the delete is complete.
//  3. The deletion gate is PositionReplied events: each target's own POSITION_REPLY
//     in the envelope's epoch. Anything else a node hears about a position is
//     PositionClaimed, which is never evidence.
//  4. RemoveRecord means the key is gone and the record is absent. U12 refuses an
//     attempt on a retired record, so a pair seeds again only after that.
//  5. After a crash, reload the persisted fields only and start again at 1.
//  6. SlotView.Copies excludes this node, and a record the caller cannot decode
//     is State>SlotRetiring: it is left alone and never offered as a source.
//  7. The epoch rule is SeedRecord's (conv §12 contract 3): a registered cut from
//     an equal or earlier epoch than Reconciled.Epoch resumes, a later one
//     restarts with reason 3, and so does a Reconciled at a later epoch than the
//     view this process already holds (the epoch moved after the attempt
//     registered). After SlotResumeSeed the caller registers again on the same
//     cut (RegisterAccepted), which is what sets SeedRecord.RegEpoch.
func (r *SlotRecord) Apply(ev SlotEvent) SlotAction {
	for _, row := range slotRows {
		if row.match(r, ev) {
			out := row.run(r, ev)
			out.Serve, out.CountsAsCopy = r.posture()
			return out
		}
	}
	// Unreachable: TestSlotEveryEventClassHasExactlyOneRow proves every event has a row.
	return SlotAction{}
}
