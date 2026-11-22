package wire

import (
	"context"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// u57r19 tests the restart barrier (BUG-0743, SPEC-GAP U57-19): a node that booted into a
// committed epoch holds every pair it owns until the stop bound counted from boot has
// passed and it caught up to its copies.

// u57r19Bound is step_down_ms + heartbeat_ms of u33State.
const u57r19Bound = u33Bound

// u57r19Node is node B of a three-node cluster at epoch 1, one slot it owns, and the applied
// position its two other copies report. The handoff saw the epoch (the supervisor feeds it
// before the first touch), and the clock stands at boot.
func u57r19Node(t *testing.T, applied cluster.Position) (*u33Node, uint16) {
	t.Helper()
	state := u33State(1, 3, u33Active("A", "B", "C"))
	table := cluster.BuildTable(state)
	var slot uint16
	for candidate := range uint16(barrierSlots) {
		if owner, _ := table.Owner(candidate, 3); owner.ID == "B" {
			slot = candidate
			break
		}
	}
	node := newU33Node("B")
	for _, peer := range []string{"A", "C"} {
		node.link.replies[peer] = PositionReply{State: PositionStateReady, AppliedPos: applied}
	}
	u33SetApplied(node.pairs, slot, u33DB, applied)
	node.h.OnEpoch(state)
	return node, slot
}

// TestRestartHoldWaitsTheBoundThenCatchesUp: the pair is unreadable until the bound from boot
// has passed, then asks its copies and catches up, and no barrier message is sent because
// there is no peer. [GUARD: the bound; the catch-up; no REQ/STOPPED/READY]
func TestRestartHoldWaitsTheBoundThenCatchesUp(t *testing.T) {
	applied := cluster.Position{Epoch: 1, Seq: 4}
	node, slot := u57r19Node(t, applied)
	held, decided := node.h.RestartHold(slot, u33DB, 3, 1)
	if !held || !decided {
		t.Fatalf("RestartHold = held %v decided %v, want both", held, decided)
	}
	if flags := u33Flags(node.pairs, slot); flags&flagAwaiting == 0 || flags&flagStopProved != 0 {
		t.Fatalf("flags %05b after the restart hold, want awaiting and not stop-proved", flags)
	}
	node.clock.set(u57r19Bound - time.Millisecond)
	if err := node.h.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if flags := u33Flags(node.pairs, slot); flags&flagAwaiting == 0 || node.catch.callCount() != 0 {
		t.Fatalf("flags %05b and %d catch-ups just before the bound, want the hold kept and no catch-up", flags, node.catch.callCount())
	}
	node.clock.set(u57r19Bound)
	if err := node.h.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	flags := u33Flags(node.pairs, slot)
	if flags&flagAwaiting != 0 || flags&flagStopProved == 0 || flags&flagReplayed == 0 {
		t.Fatalf("flags %05b at the bound, want the hold ended with stop proof and replayed", flags)
	}
	if node.catch.callCount() != 1 {
		t.Fatalf("%d catch-ups, want one", node.catch.callCount())
	}
	if target, known := node.h.CachedTarget(slot, u33DB); !known || target != applied {
		t.Fatalf("target %+v known %v, want the copies' position %+v", target, known, applied)
	}
	if reqs, stops, readies := node.link.counts(); reqs+stops+readies != 0 {
		t.Fatalf("%d REQ, %d STOPPED, %d READY sent; a peer-less hold sends none", reqs, stops, readies)
	}
}

// TestRestartHoldAsksTheCopiesBeforeServing: a copy that is silent keeps the pair held after
// the bound. [GUARD: serving on the bound alone]
func TestRestartHoldAsksTheCopiesBeforeServing(t *testing.T) {
	node, slot := u57r19Node(t, cluster.Position{Epoch: 1, Seq: 4})
	node.link.silent["A"], node.link.silent["C"] = true, true
	node.h.RestartHold(slot, u33DB, 3, 1)
	node.clock.set(u57r19Bound)
	if err := node.h.Tick(context.Background()); err == nil {
		t.Fatal("Tick reported no error with both copies silent")
	}
	if flags := u33Flags(node.pairs, slot); flags&flagAwaiting == 0 || flags&flagStopProved != 0 {
		t.Fatalf("flags %05b with the copies silent, want the hold kept", flags)
	}
}

// TestRestartHoldBoundCountsFromBootNotFirstTouch: a pair first touched long after boot is
// proved on the first Tick. [GUARD: At = first touch]
func TestRestartHoldBoundCountsFromBootNotFirstTouch(t *testing.T) {
	node, slot := u57r19Node(t, cluster.Position{Epoch: 1, Seq: 4})
	node.clock.set(10 * time.Second)
	node.h.RestartHold(slot, u33DB, 3, 1)
	if err := node.h.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if flags := u33Flags(node.pairs, slot); flags&flagAwaiting != 0 || flags&flagStopProved == 0 {
		t.Fatalf("flags %05b on the first Tick after a late touch, want the hold ended", flags)
	}
}

// TestRestartHoldIsOnceAndAnswersNotMine: a second touch keeps the first hold's timing, a
// pair this node does not own is not held, and an epoch the handoff has not seen is
// undecided. [GUARD: idempotence; owner check; epoch check]
func TestRestartHoldIsOnceAndAnswersNotMine(t *testing.T) {
	node, slot := u57r19Node(t, cluster.Position{Epoch: 1, Seq: 4})
	node.h.RestartHold(slot, u33DB, 3, 1)
	node.clock.set(time.Second)
	if held, decided := node.h.RestartHold(slot, u33DB, 3, 1); !held || !decided {
		t.Fatalf("second touch = held %v decided %v", held, decided)
	}
	node.clock.set(u57r19Bound)
	if err := node.h.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if flags := u33Flags(node.pairs, slot); flags&flagAwaiting != 0 {
		t.Fatalf("flags %05b: the second touch restarted the bound", flags)
	}

	// Once the pair opened, a late second touch neither reopens the barrier nor drops the target.
	if held, decided := node.h.RestartHold(slot, u33DB, 3, 1); held || !decided {
		t.Fatalf("a touch after the pair opened = held %v decided %v, want decided and not held", held, decided)
	}
	if flags := u33Flags(node.pairs, slot); flags&flagAwaiting != 0 || flags&flagStopProved == 0 {
		t.Fatalf("flags %05b: a touch after the pair opened reopened it", flags)
	}
	if _, known := node.h.CachedTarget(slot, u33DB); !known {
		t.Fatal("a touch after the pair opened dropped its target")
	}

	table := cluster.BuildTable(u33State(1, 3, u33Active("A", "B", "C")))
	var other uint16
	for candidate := range uint16(barrierSlots) {
		if owner, _ := table.Owner(candidate, 3); owner.ID != "B" {
			other = candidate
			break
		}
	}
	if held, decided := node.h.RestartHold(other, u33DB, 3, 1); held || !decided {
		t.Fatalf("a pair B does not own = held %v decided %v, want decided and not held", held, decided)
	}
	if flags := u33Flags(node.pairs, other); flags != 0 {
		t.Fatalf("a pair B does not own got flags %05b", flags)
	}
	if held, decided := node.h.RestartHold(slot, u33DB, 3, 2); held || decided {
		t.Fatalf("an unseen epoch = held %v decided %v, want undecided", held, decided)
	}
	if _, decided := newU33Node("B").h.RestartHold(slot, u33DB, 3, 1); decided {
		t.Fatal("a handoff that saw no epoch decided")
	}
}

// TestRestartHoldNeverReadsTheStaleRFMap: a database created at the current epoch may not be in
// the handoff's rf map yet; the hold must still open and must survive the next epoch.
// [GUARD: taking rf from h.rf]
func TestRestartHoldNeverReadsTheStaleRFMap(t *testing.T) {
	node, _ := u57r19Node(t, cluster.Position{Epoch: 1, Seq: 4})
	const newDB uint64 = 7
	table := cluster.BuildTable(u33State(1, 3, u33Active("A", "B", "C")))
	var slot uint16
	for candidate := range uint16(barrierSlots) {
		if owner, _ := table.Owner(candidate, 3); owner.ID == "B" {
			slot = candidate
			break
		}
	}
	if held, decided := node.h.RestartHold(slot, newDB, 3, 1); !held || !decided {
		t.Fatalf("a database absent from the handoff's rf map = held %v decided %v, want held", held, decided)
	}
	// The next epoch lists the database: the hold must stay.
	node.h.OnEpoch(u33State(2, 3, u33Active("A", "B", "C"), int64(u33DB), int64(newDB)))
	if flags := node.pairs.Snapshot(slot, newDB).Flags; flags&flagAwaiting == 0 {
		t.Fatalf("flags %05b after the next epoch, want the restart hold kept", flags)
	}
}

// TestRestartHoldOfASingleMemberClusterHasNoBound: with one member there is no other owner,
// so the pair opens on the first Tick after the catch-up. [GUARD: bound 0 for one member]
func TestRestartHoldOfASingleMemberClusterHasNoBound(t *testing.T) {
	state := u33State(1, 1, u33Active("B"))
	node := newU33Node("B")
	node.h.OnEpoch(state)
	if held, decided := node.h.RestartHold(5, u33DB, 1, 1); !held || !decided {
		t.Fatalf("RestartHold = held %v decided %v", held, decided)
	}
	if err := node.h.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if flags := u33Flags(node.pairs, 5); flags&flagAwaiting != 0 || flags&flagStopProved == 0 {
		t.Fatalf("flags %05b on the first Tick of a one-member cluster, want the hold ended", flags)
	}
}

// TestRestartHoldOpensOnTheSurvivorsWhenAMajorityIsGone: the degraded rule (U57-17) applies
// to a restart: the pair does not hang when two of three copies are down.
// [GUARD: Target's structural branch]
func TestRestartHoldOpensOnTheSurvivorsWhenAMajorityIsGone(t *testing.T) {
	applied := cluster.Position{Epoch: 1, Seq: 7}
	slot, states := u5717Slot(t)
	node := newU33Node("C")
	u33SetApplied(node.pairs, slot, u33DB, applied)
	node.h.OnEpoch(states[2])
	if held, decided := node.h.RestartHold(slot, u33DB, 3, 3); !held || !decided {
		t.Fatalf("RestartHold = held %v decided %v", held, decided)
	}
	node.clock.set(u57r19Bound)
	u5717AssertOpened(t, node, slot, applied)
}

// TestAnEpochChangeDuringTheRestartHoldRestartsTheBound: a real epoch ends the restart hold
// and the pair waits again from that commit (machine row E4). [GUARD: feedEpoch reaching it]
func TestAnEpochChangeDuringTheRestartHoldRestartsTheBound(t *testing.T) {
	node, slot := u57r19Node(t, cluster.Position{Epoch: 1, Seq: 4})
	node.h.RestartHold(slot, u33DB, 3, 1)
	node.clock.set(time.Second)
	next := u33State(2, 3, u33Active("A", "B", "C", "D"))
	if owner, _ := cluster.BuildTable(next).Owner(slot, 3); owner.ID != "B" {
		t.Skipf("slot %d moved to %s with D; the scenario needs B to keep it", slot, owner.ID)
	}
	node.h.OnEpoch(next)
	node.clock.set(u57r19Bound)
	if err := node.h.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if flags := u33Flags(node.pairs, slot); flags&flagAwaiting == 0 {
		t.Fatalf("flags %05b: the original boot bound ended a hold the epoch change restarted", flags)
	}
	node.clock.set(time.Second + u57r19Bound)
	if err := node.h.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if flags := u33Flags(node.pairs, slot); flags&flagAwaiting != 0 {
		t.Fatalf("flags %05b after the new bound, want the hold ended", flags)
	}
}

// TestAStaleRequestAndAStoppedFromTheOldOwnerProveNothingToARestartHold: with no peer named,
// only the bound proves the stop. [GUARD: Peer == "" ignores STOPPED]
func TestAStaleRequestAndAStoppedFromTheOldOwnerProveNothingToARestartHold(t *testing.T) {
	node, slot := u57r19Node(t, cluster.Position{Epoch: 1, Seq: 4})
	node.h.RestartHold(slot, u33DB, 3, 1)
	req := HandoffReq{FromEpoch: 0, ToEpoch: 1, Bitmap: BitmapOfSlots([]uint16{slot})}
	if err := node.h.OnHandoffReq("A", u33Env(kindHandoffReq, 1, u33DB), req); err != nil {
		t.Fatal(err)
	}
	u33Stopped(t, node, "A", 1, slot)
	if err := node.h.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if flags := u33Flags(node.pairs, slot); flags&flagAwaiting == 0 || node.catch.callCount() != 0 {
		t.Fatalf("flags %05b, %d catch-ups: a STOPPED from a named stranger proved the stop", flags, node.catch.callCount())
	}
	if reqs, stops, readies := node.link.counts(); reqs+stops+readies != 0 {
		t.Fatal("a peer-less hold answered a stale request")
	}
}
