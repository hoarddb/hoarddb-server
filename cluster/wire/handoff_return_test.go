package wire

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// bug0926Slot returns a slot whose RF 2 copy set at a three-member cluster is A then C, and
// the three states of A's return: all active (A owns it), A committed down (C orders it with
// one copy), A active again (A owns it again).
func bug0926Slot(t *testing.T) (slot uint16, states [3]cluster.ClusterState) {
	t.Helper()
	down := []cluster.Member{u33Member("A", cluster.MemberDown), u33Member("B", cluster.MemberActive), u33Member("C", cluster.MemberActive)}
	states[0] = u33State(3, 2, u33Active("A", "B", "C"))
	states[1] = u33State(4, 2, down)
	states[2] = u33State(5, 2, u33Active("A", "B", "C"))
	table := cluster.BuildTable(states[0])
	for candidate := range uint16(barrierSlots) {
		copies := table.Copies(candidate, 2)
		if len(copies) == 2 && copies[0].ID == "A" && copies[1].ID == "C" {
			return candidate, states
		}
	}
	t.Fatal("no slot ranks A, then C")
	return 0, states
}

// bug0926Returned is node A after its return: fed the three epochs in order, holding its own
// copy at its last position of epoch 3, with C's HANDOFF_STOPPED for epoch 5 already in.
func bug0926Returned(t *testing.T, own cluster.Position) (*u33Node, uint16) {
	t.Helper()
	slot, states := bug0926Slot(t)
	node := newU33Node("A")
	u33SetApplied(node.pairs, slot, u33DB, own)
	for _, state := range states {
		node.h.OnEpoch(state)
	}
	u33Stopped(t, node, "C", 5, slot)
	return node, slot
}

// TestAReturningOwnerDoesNotAnswerForTheEpochItWasDownIn is BUG-0926. At RF 2 the returning
// owner A needs RF - majority + 1 = 1 answer from the copy set of epoch 4. A was committed down
// at epoch 4, so its own record holds nothing C ordered then; counted as that answer, it let A
// open on its epoch-3 position while C, the only copy that ordered epoch 4, did not answer. A's
// first record of epoch 5 then names an epoch-3 prevEnd, C's epoch-4 tail reads as one nobody
// owns, C is reset to A's copy, and every write C acknowledged at epoch 4 is gone from both.
func TestAReturningOwnerDoesNotAnswerForTheEpochItWasDownIn(t *testing.T) {
	own := cluster.Position{Epoch: 3, Seq: 5}
	node, slot := bug0926Returned(t, own)
	node.link.silent["C"] = true
	target, err := node.h.Target(context.Background(), slot, u33DB, 4)
	if err == nil {
		t.Fatalf("A took target %s from its own record while C, the copy that ordered epoch 4, was silent; want a hold", target)
	}
	if err := node.h.MaybeSendReady(context.Background(), slot, u33DB); err == nil {
		t.Fatal("MaybeSendReady reported no error while C was silent")
	}
	if _, _, readies := node.link.counts(); readies != 0 {
		t.Fatal("A sent READY, and so opened the pair, without hearing from C")
	}
	if flags := u33Flags(node.pairs, slot); flags&flagAwaiting == 0 {
		t.Fatalf("A's pair flags are %05b while C is silent, want the awaiting hold kept", flags)
	}
}

// TestAReturningOwnerTakesTheTargetOfTheCopyThatOrderedWhileItWasDown: once C answers, the target
// is C's epoch-4 position, so A catches up through it before its first record. [GUARD: the fix must
// not hold a pair whose live old copy answers]
func TestAReturningOwnerTakesTheTargetOfTheCopyThatOrderedWhileItWasDown(t *testing.T) {
	node, slot := bug0926Returned(t, cluster.Position{Epoch: 3, Seq: 5})
	tail := cluster.Position{Epoch: 4, Seq: 2}
	node.link.replies["C"] = PositionReply{State: PositionStateReady, AppliedPos: tail}
	target, err := node.h.Target(context.Background(), slot, u33DB, 4)
	if err != nil {
		t.Fatal(err)
	}
	if target != tail {
		t.Fatalf("target %s, want C's epoch-4 position %s", target, tail)
	}
	if node.h.Degraded(slot, u33DB) {
		t.Fatal("the target was marked degraded although the only live copy of epoch 4 answered")
	}
}

// bug0926RankedSlot returns a slot whose ranking at state starts with order.
func bug0926RankedSlot(t *testing.T, state cluster.ClusterState, order ...string) uint16 {
	t.Helper()
	return bug0834RankedSlot(t, state, order...)
}

// TestAReturningOwnerAtRF3WaitsForTheCopyThatOrderedADegradedWrite: RF 3 over A, B, C. A is
// committed down at epoch 4 and B orders the pair; C is slow, so B's write is acknowledged with
// one copy (applied_degraded) and only B holds it. At epoch 5 A is back. The quorum is two
// answers, and C alone is short of it once A's own record does not count, so the old rule (A
// plus C) opened behind B's write. B, live and silent, must keep the hold.
func TestAReturningOwnerAtRF3WaitsForTheCopyThatOrderedADegradedWrite(t *testing.T) {
	down := []cluster.Member{u33Member("A", cluster.MemberDown), u33Member("B", cluster.MemberActive), u33Member("C", cluster.MemberActive)}
	states := []cluster.ClusterState{u33State(3, 3, u33Active("A", "B", "C")), u33State(4, 3, down), u33State(5, 3, u33Active("A", "B", "C"))}
	slot := bug0926RankedSlot(t, states[0], "A", "B", "C")
	node := newU33Node("A")
	u33SetApplied(node.pairs, slot, u33DB, cluster.Position{Epoch: 3, Seq: 5})
	for _, state := range states {
		node.h.OnEpoch(state)
	}
	u33Stopped(t, node, "B", 5, slot)
	node.link.silent["B"] = true
	node.link.replies["C"] = PositionReply{State: PositionStateReady, AppliedPos: cluster.Position{Epoch: 3, Seq: 5}}
	if target, err := node.h.Target(context.Background(), slot, u33DB, 4); err == nil {
		t.Fatalf("A took target %s while B, the owner that ordered epoch 4, was silent; want a hold", target)
	}
	degradedWrite := cluster.Position{Epoch: 4, Seq: 1}
	node.link.silent["B"] = false
	node.link.replies["B"] = PositionReply{State: PositionStateReady, AppliedPos: degradedWrite}
	target, err := node.h.Target(context.Background(), slot, u33DB, 4)
	if err != nil {
		t.Fatal(err)
	}
	if target != degradedWrite {
		t.Fatalf("target %s, want B's degraded write at %s", target, degradedWrite)
	}
}

// TestAJoinerTakingAPairWaitsForThePredecessorAtRF3: RF 3 over A, B, C, D joins and takes a pair
// from B. A and C answer, which met the old quorum of two, but B ordered every write of the epoch,
// a degraded one only it holds included, and B is live: the new owner waits for it.
func TestAJoinerTakingAPairWaitsForThePredecessorAtRF3(t *testing.T) {
	first := u33State(1, 3, u33Active("A", "B", "C"))
	second := u33State(2, 3, u33Active("A", "B", "C", "D"))
	slot := u33Moved(t, first, second, 3, "B", "D")[0]
	node := newU33Node("D")
	node.h.OnEpoch(first)
	node.h.OnEpoch(second)
	u33Stopped(t, node, "B", 2, slot)
	ready := PositionReply{State: PositionStateReady, AppliedPos: cluster.Position{Epoch: 1, Seq: 3}}
	node.link.replies["A"], node.link.replies["C"] = ready, ready
	node.link.silent["B"] = true
	if target, err := node.h.Target(context.Background(), slot, u33DB, 1); err == nil {
		t.Fatalf("D took target %s from A and C while B, the predecessor, was silent; want a hold", target)
	}
}

// bug0926Restarted is node A of an RF 2 pair with C, restarted straight into epoch 5: the handoff's
// first state is already epoch 5 (the boot never applied the epochs in between), so no move is
// seen and the first touch opens a restart hold. cState is C's state at epoch 5.
func bug0926Restarted(t *testing.T, cState cluster.MemberState) (*u33Node, uint16) {
	t.Helper()
	members := []cluster.Member{u33Member("A", cluster.MemberActive), u33Member("B", cluster.MemberActive), u33Member("C", cState)}
	state := u33State(5, 2, members)
	slot := bug0926RankedSlot(t, u33State(5, 2, u33Active("A", "B", "C")), "A", "C")
	node := newU33Node("A")
	u33SetApplied(node.pairs, slot, u33DB, cluster.Position{Epoch: 3, Seq: 5})
	node.h.OnEpoch(state)
	if held, decided := node.h.RestartHold(slot, u33DB, 2, 5); !held || !decided {
		t.Fatalf("RestartHold = held %v decided %v, want both", held, decided)
	}
	node.clock.set(u33Bound)
	return node, slot
}

// TestARestartHoldDoesNotAnswerForItself is the restart-hold edge of BUG-0926: the node cannot
// tell which epochs it missed, so its own epoch-3 record does not stand in for C, which may have
// ordered the pair while it was away.
func TestARestartHoldDoesNotAnswerForItself(t *testing.T) {
	node, slot := bug0926Restarted(t, cluster.MemberActive)
	node.link.silent["C"] = true
	if err := node.h.Tick(context.Background()); err == nil {
		t.Fatal("Tick reported no error while C was silent")
	}
	if flags := u33Flags(node.pairs, slot); flags&flagAwaiting == 0 {
		t.Fatalf("the restarted pair opened on its own record with C silent: flags %05b", flags)
	}
	node.link.silent["C"] = false
	node.link.replies["C"] = PositionReply{State: PositionStateReady, AppliedPos: cluster.Position{Epoch: 4, Seq: 2}}
	if err := node.h.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if target, _ := node.h.CachedTarget(slot, u33DB); target != (cluster.Position{Epoch: 4, Seq: 2}) {
		t.Fatalf("target %s, want C's 4:2", target)
	}
}

// TestAPairWhoseOtherCopyIsGoneStillOpens is the way out of the hold: once the silent copy is
// committed down (the detector does it after failover_ms) the rule no longer waits for it, and
// the returning owner and the restarted owner both open on their own record, and a pair opened
// without the copy is marked degraded (before the fix the returning owner's own record counted as
// the answer, so it opened silently, not degraded). It also guards the fix against holding a pair
// forever.
func TestAPairWhoseOtherCopyIsGoneStillOpens(t *testing.T) {
	t.Run("returning owner", func(t *testing.T) {
		slot, states := bug0926Slot(t)
		states[2] = u33State(5, 2, []cluster.Member{u33Member("A", cluster.MemberActive), u33Member("B", cluster.MemberActive), u33Member("C", cluster.MemberDown)})
		node := newU33Node("A")
		own := cluster.Position{Epoch: 3, Seq: 5}
		u33SetApplied(node.pairs, slot, u33DB, own)
		for _, state := range states {
			node.h.OnEpoch(state)
		}
		// B answers for the other pairs A takes back; this pair's copies are A and C only.
		node.link.replies["B"] = PositionReply{State: PositionStateReady}
		node.clock.set(u33Bound)
		if err := node.h.Tick(context.Background()); err != nil {
			t.Fatal(err)
		}
		if flags := u33Flags(node.pairs, slot); flags&flagAwaiting != 0 {
			t.Fatalf("A's pair is still held with C committed down: flags %05b", flags)
		}
		if !node.h.Degraded(slot, u33DB) {
			t.Fatal("the pair opened without C and is not marked degraded")
		}
	})
	t.Run("restarted owner", func(t *testing.T) {
		node, slot := bug0926Restarted(t, cluster.MemberDown)
		if err := node.h.Tick(context.Background()); err != nil {
			t.Fatal(err)
		}
		if flags := u33Flags(node.pairs, slot); flags&flagAwaiting != 0 {
			t.Fatalf("the restarted pair is still held with C committed down: flags %05b", flags)
		}
	})
}

// TestTheHoldNamesTheWayOut: the refusal of a pair held for a silent copy says how it ends.
func TestTheHoldNamesTheWayOut(t *testing.T) {
	node, slot := bug0926Returned(t, cluster.Position{Epoch: 3, Seq: 5})
	node.link.silent["C"] = true
	_, err := node.h.Target(context.Background(), slot, u33DB, 4)
	if err == nil {
		t.Fatal("no hold with C silent")
	}
	for _, want := range []string{"'nodes'", "failover_ms", "'replace-node"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the hold %q does not name %s", err, want)
		}
	}
}

// TestACatchUpWithASilentCopyIsNotCurrent: "no source ahead" is evidence only when every live copy
// answered. A silent one may be ahead, so the catch-up source is an error, not ErrNoCatchUpSource,
// which would mark the pair current and let a new owner open behind it.
func TestACatchUpWithASilentCopyIsNotCurrent(t *testing.T) {
	state := u33State(5, 2, u33Active("C", "J"))
	slot := bug0926RankedSlot(t, state, "J", "C")
	node, link := bug0834CatchUpNode(state)
	link.silent["C"] = true
	if _, err := node.catchUpSourceVia(link, slot, u33DB); err == nil || errors.Is(err, ErrNoCatchUpSource) {
		t.Fatalf("catch-up source with C silent: %v; want an error that is not ErrNoCatchUpSource", err)
	}
	link.silent["C"] = false
	link.replies["C"] = PositionReply{State: PositionStateReady}
	if _, err := node.catchUpSourceVia(link, slot, u33DB); !errors.Is(err, ErrNoCatchUpSource) {
		t.Fatalf("catch-up source with C answering and not ahead: %v; want ErrNoCatchUpSource", err)
	}
}
