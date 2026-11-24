package wire

import (
	"context"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The tests below are BUG-0926's second round: the verifier's attacks on the first fix. A hold
// that waits for the copy that ordered an epoch its owner missed must survive a further epoch
// committed before it ends, a node whose own hold is open must not answer for the pair as a
// ready copy, and a new owner's catch-up must not call the pair current while a live copy is
// silent.

// bug0926SixthEpoch is epoch 6 over A, B, C at RF 2 with B in bState and C in cState: a commit
// that leaves the pair's copy set (A, C) as it was, so only a hold that remembers what it was
// waiting for still waits.
func bug0926SixthEpoch(bState, cState cluster.MemberState) cluster.ClusterState {
	return u33State(6, 2, []cluster.Member{u33Member("A", cluster.MemberActive), u33Member("B", bState), u33Member("C", cState)})
}

// bug0926HeldAtFive is node A returned at epoch 5 and held for C, the owner of epoch 4, which is
// silent; B answers for the other pairs A takes back.
func bug0926HeldAtFive(t *testing.T) (*u33Node, uint16) {
	t.Helper()
	slot, states := bug0926Slot(t)
	node := newU33Node("A")
	u33SetApplied(node.pairs, slot, u33DB, cluster.Position{Epoch: 3, Seq: 5})
	for _, state := range states {
		node.h.OnEpoch(state)
	}
	node.link.silent["C"] = true
	node.link.replies["B"] = PositionReply{State: PositionStateReady}
	if target, err := node.h.Target(context.Background(), slot, u33DB, 4); err == nil {
		t.Fatalf("precondition: the epoch-5 barrier took %s with C silent", target)
	}
	return node, slot
}

// bug0926TickEveryPair runs enough Ticks for every awaiting pair to be advanced: epoch 6 hands
// node A the pairs of the member it commits down, and one Tick advances handoffAdvancePerTick of
// them in rotation. The Ticks' errors are those other pairs' and are not looked at.
func bug0926TickEveryPair(node *u33Node) {
	for range barrierSlots/handoffAdvancePerTick + 1 {
		_ = node.h.Tick(context.Background())
	}
}

// TestAHoldOpenAcrossAFurtherEpochStillWaitsForTheCopyThatOrdered: A returns at epoch 5 and waits
// for C, which ordered epoch 4 alone. Before that hold ends an unrelated epoch 6 commits (B down;
// the pair's copy set is still A, C). The new barrier moves the pair from epoch 5, where A is the
// owner by the table, so the first fix named no predecessor and A's own epoch-3 record met the RF 2
// quorum of one: C's degraded writes were lost while C was alive.
func TestAHoldOpenAcrossAFurtherEpochStillWaitsForTheCopyThatOrdered(t *testing.T) {
	node, slot := bug0926HeldAtFive(t)
	node.h.OnEpoch(bug0926SixthEpoch(cluster.MemberDown, cluster.MemberActive))
	if target, err := node.h.Target(context.Background(), slot, u33DB, 5); err == nil {
		t.Fatalf("after an unrelated epoch 6, A took target %s from its own epoch-3 record while C, which ordered epoch 4 and holds its degraded writes, is silent", target)
	}
	node.link.silent["C"] = false
	node.link.replies["C"] = PositionReply{State: PositionStateReady, AppliedPos: cluster.Position{Epoch: 4, Seq: 2}}
	target, err := node.h.Target(context.Background(), slot, u33DB, 5)
	if err != nil {
		t.Fatalf("C answered and the hold still holds: %v", err)
	}
	if target != (cluster.Position{Epoch: 4, Seq: 2}) {
		t.Fatalf("target %s, want C's 4:2", target)
	}
}

// TestAHoldOpenAcrossAFurtherEpochDoesNotOpenThePair is the same through Tick: the pair stays in
// its awaiting hold while C is silent.
func TestAHoldOpenAcrossAFurtherEpochDoesNotOpenThePair(t *testing.T) {
	node, slot := bug0926HeldAtFive(t)
	node.h.OnEpoch(bug0926SixthEpoch(cluster.MemberDown, cluster.MemberActive))
	node.clock.set(u33Bound)
	bug0926TickEveryPair(node)
	if flags := u33Flags(node.pairs, slot); flags&flagAwaiting == 0 {
		target, _ := node.h.CachedTarget(slot, u33DB)
		t.Fatalf("the pair opened (flags %05b) on target %s with C, the epoch-4 owner, silent", flags, target)
	}
}

// TestARestartHoldOpenAcrossAFurtherEpochStillWaitsForEveryCopy: a restart hold cannot name the
// epochs its node missed, so it waits for every live copy. An epoch 6 that commits before C
// answers re-enters the hold as an ordinary move, which the first fix gave no such rule.
func TestARestartHoldOpenAcrossAFurtherEpochStillWaitsForEveryCopy(t *testing.T) {
	node, slot := bug0926Restarted(t, cluster.MemberActive)
	node.link.silent["C"] = true
	node.link.replies["B"] = PositionReply{State: PositionStateReady}
	if err := node.h.Tick(context.Background()); err == nil {
		t.Fatal("precondition: the restart hold did not hold with C silent")
	}
	node.h.OnEpoch(bug0926SixthEpoch(cluster.MemberDown, cluster.MemberActive))
	node.clock.set(5 * u33Bound)
	bug0926TickEveryPair(node)
	if flags := u33Flags(node.pairs, slot); flags&flagAwaiting == 0 {
		target, _ := node.h.CachedTarget(slot, u33DB)
		t.Fatalf("the restart-held pair opened (flags %05b) on target %s after epoch 6 with C silent", flags, target)
	}
}

// TestAHoldOpenAcrossAFurtherEpochEndsWhenTheCopyIsCommittedDown is the way out as it happens: A
// waits for C, then the detector commits C down at epoch 6. The pair opens on A's record and is
// marked degraded, since it opened without what only C held.
func TestAHoldOpenAcrossAFurtherEpochEndsWhenTheCopyIsCommittedDown(t *testing.T) {
	node, slot := bug0926HeldAtFive(t)
	node.h.OnEpoch(bug0926SixthEpoch(cluster.MemberActive, cluster.MemberDown))
	node.clock.set(5 * u33Bound)
	bug0926TickEveryPair(node)
	if flags := u33Flags(node.pairs, slot); flags&flagAwaiting != 0 {
		t.Fatalf("the pair is still held (flags %05b) after C was committed down", flags)
	}
	if !node.h.Degraded(slot, u33DB) {
		t.Fatal("the pair opened without C, committed down at epoch 6, and is not marked degraded")
	}
}

// TestAHoldOpenAcrossAFurtherEpochAtRF3WaitsForTheCopyThatOrdered: RF 3 over A, B, C. At epoch 4
// A is down and B orders; C holds B's majority writes through 4:3 and B alone a degraded one at
// 4:4. At epoch 5 A is back and waits for B. An unrelated epoch 6 (D committed down) commits
// before B answers. The first fix re-entered the hold with no predecessor, A's own record and C
// met the quorum of two, and A opened at C's 4:3 without B's write.
func TestAHoldOpenAcrossAFurtherEpochAtRF3WaitsForTheCopyThatOrdered(t *testing.T) {
	ring := func(epoch int64, aState, dState cluster.MemberState) cluster.ClusterState {
		return u33State(epoch, 3, []cluster.Member{u33Member("A", aState), u33Member("B", cluster.MemberActive), u33Member("C", cluster.MemberActive), u33Member("D", dState)})
	}
	states := []cluster.ClusterState{ring(3, cluster.MemberActive, cluster.MemberActive), ring(4, cluster.MemberDown, cluster.MemberActive), ring(5, cluster.MemberActive, cluster.MemberActive)}
	slot := bug0926RankedSlot(t, states[0], "A", "B", "C")
	node := newU33Node("A")
	u33SetApplied(node.pairs, slot, u33DB, cluster.Position{Epoch: 3, Seq: 5})
	for _, state := range states {
		node.h.OnEpoch(state)
	}
	node.link.silent["B"] = true
	node.link.replies["C"] = PositionReply{State: PositionStateReady, AppliedPos: cluster.Position{Epoch: 4, Seq: 3}}
	node.link.replies["D"] = PositionReply{State: PositionStateReady}
	if target, err := node.h.Target(context.Background(), slot, u33DB, 4); err == nil {
		t.Fatalf("precondition: the epoch-5 barrier took %s with B silent", target)
	}
	node.h.OnEpoch(ring(6, cluster.MemberActive, cluster.MemberDown))
	if target, err := node.h.Target(context.Background(), slot, u33DB, 5); err == nil {
		t.Fatalf("after an unrelated epoch 6, A took target %s (C's) while B, which alone holds the degraded write it ordered at epoch 4, is live and silent", target)
	}
}

// TestAHoldOpenedByAnEarlyRequestKeepsTheQuorumRule guards the fix against holding an ordinary
// handoff for every copy: a HANDOFF_REQ that arrives before its commit opens the hold, and the
// commit that follows is that same barrier, not a further epoch. At RF 3 the new owner D takes
// its target from the quorum of two, B and C, while A is silent and was not the owner.
func TestAHoldOpenedByAnEarlyRequestKeepsTheQuorumRule(t *testing.T) {
	first := u33State(1, 3, u33Active("A", "B", "C"))
	second := u33State(2, 3, u33Active("A", "B", "C", "D"))
	slot := u33Moved(t, first, second, 3, "B", "D")[0]
	node := newU33Node("D")
	node.h.OnEpoch(first)
	req := HandoffReq{FromEpoch: 1, ToEpoch: 2, Bitmap: BitmapOfSlots([]uint16{slot})}
	if err := node.h.OnHandoffReq("B", u33Env(kindHandoffReq, 1, u33DB), req); err != nil {
		t.Fatalf("OnHandoffReq: %v", err)
	}
	node.h.OnEpoch(second)
	u33Stopped(t, node, "B", 2, slot)
	ready := PositionReply{State: PositionStateReady, AppliedPos: cluster.Position{Epoch: 1, Seq: 3}}
	node.link.replies["B"], node.link.replies["C"] = ready, ready
	node.link.silent["A"] = true
	target, err := node.h.Target(context.Background(), slot, u33DB, 1)
	if err != nil {
		t.Fatalf("B, the predecessor, and C answered and the ordinary handoff still holds: %v", err)
	}
	if target != ready.AppliedPos {
		t.Fatalf("target %s, want %s", target, ready.AppliedPos)
	}
}

// TestACatchUpWithASilentCopyTakesNoSourceForAnOwner: the new owner J's catch-up finds C ahead
// and B silent. B may be further ahead (a degraded write only it holds), so the owner's catch-up
// takes no source until B answers: reaching C's position would mark the pair current.
func TestACatchUpWithASilentCopyTakesNoSourceForAnOwner(t *testing.T) {
	state := u33State(6, 3, u33Active("B", "C", "J"))
	slot := bug0926RankedSlot(t, state, "J", "B", "C")
	node, link := bug0834CatchUpNode(state)
	link.silent["B"] = true
	link.replies["C"] = PositionReply{State: PositionStateReady, AppliedPos: cluster.Position{Epoch: 4, Seq: 3}}
	if source, err := node.catchUpSourceVia(link, slot, u33DB); err == nil {
		t.Fatalf("the owner's catch-up chose %s while B, a live copy that may be further ahead, was silent", source)
	}
	link.silent["B"] = false
	link.replies["B"] = PositionReply{State: PositionStateReady, AppliedPos: cluster.Position{Epoch: 4, Seq: 4}}
	if source, err := node.catchUpSourceVia(link, slot, u33DB); err != nil || source != "B" {
		t.Fatalf("with every copy answering the source is %q (%v), want B, the furthest", source, err)
	}
}

// TestACopysCatchUpReadsFromTheOwnerWhileAnotherCopyIsSilent guards the catch-up fix against
// stalling every copy behind a silent peer: a copy that is not the owner reads from the owner,
// which ordered every write of the epoch, whatever another copy does.
func TestACopysCatchUpReadsFromTheOwnerWhileAnotherCopyIsSilent(t *testing.T) {
	state := u33State(6, 3, u33Active("B", "C", "J"))
	slot := bug0926RankedSlot(t, state, "B", "J", "C")
	node, link := bug0834CatchUpNode(state)
	link.silent["C"] = true
	link.replies["B"] = PositionReply{State: PositionStateReady, AppliedPos: cluster.Position{Epoch: 6, Seq: 3}}
	if source, err := node.catchUpSourceVia(link, slot, u33DB); err != nil || source != "B" {
		t.Fatalf("a copy's catch-up with the owner answering chose %q (%v), want the owner B", source, err)
	}
}

// TestAPairInItsAwaitingHoldIsNotAReadyCopy: while this node's own awaiting hold is open its
// record may lack what the copy it waits for ordered, so a POSITION_QUERY is answered as
// evidence (retired: the position raises a target, it never counts for a quorum), not as a ready
// copy. The first fix answered ready, so another node's quorum could count this record for the
// epoch it missed. Once the hold ends the pair answers ready again.
func TestAPairInItsAwaitingHoldIsNotAReadyCopy(t *testing.T) {
	node := s0OpenClusterNode(t, t.TempDir(), nil)
	dbID := bug0760Box(t, node, u30iDB)
	slot := bug0760Slot(t, u30iDB)
	ask := func() PositionReply {
		t.Helper()
		body, err := EncodePositionQuery(PositionQuery{Slot: slot, DB: dbID})
		if err != nil {
			t.Fatal(err)
		}
		env := transport.Envelope{Version: transport.EnvelopeVersion, Kind: kindPositionQuery, Epoch: node.topology.Committed(), Slot: slot, DBID: dbID}
		reply, err := node.serveData(context.Background(), transport.Handshake{NodeID: "peer"}, transport.Message{Env: env, Body: body})
		if err != nil {
			t.Fatalf("POSITION_QUERY: %v", err)
		}
		answered, err := DecodePositionReply(reply.Body)
		if err != nil {
			t.Fatal(err)
		}
		return answered
	}
	if settled := ask(); settled.State != PositionStateReady {
		t.Fatalf("precondition: the settled pair answered state %d, want ready", settled.State)
	}
	node.pairs.With(slot, dbID, func(p *cluster.Pair) { p.Flags |= flagAwaiting })
	held := ask()
	if held.State == PositionStateReady {
		t.Fatalf("the pair answered ready at %s while its own awaiting hold is open", held.AppliedPos)
	}
	if held.State != PositionStateRetired || held.AppliedPos != node.pairs.Snapshot(slot, dbID).AppliedPos {
		t.Fatalf("the held pair answered state %d at %s, want retired (evidence) at its applied position", held.State, held.AppliedPos)
	}
	node.pairs.With(slot, dbID, func(p *cluster.Pair) { p.Flags &^= flagAwaiting })
	if settled := ask(); settled.State != PositionStateReady {
		t.Fatalf("after the hold the pair answered state %d, want ready", settled.State)
	}
}
