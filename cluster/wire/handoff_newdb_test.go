package wire

import (
	"reflect"
	"slices"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
)

const bug0747OtherDB = 5

// TestADatabaseCreatedAtTheCurrentEpochGetsABarrierAtTheNextMove (BUG-0747): the database
// was created after the epoch began, so only a state at the same epoch tells the handoff
// of it. Its new owner must hold when the epoch then moves. [GUARD: refreshing rf on a
// state at the current epoch]
func TestADatabaseCreatedAtTheCurrentEpochGetsABarrierAtTheNextMove(t *testing.T) {
	first := u33State(1, 2, u33Active("A"), bug0747OtherDB)
	created := u33State(1, 2, u33Active("A"), bug0747OtherDB, int64(u33DB))
	created.StateSeq = 2
	second := u33State(2, 2, u33Active("A", "B"), bug0747OtherDB, int64(u33DB))
	slots := u33Moved(t, first, second, 2, "A", "B")
	node := newU33Node("B")
	node.h.OnEpoch(first)
	node.h.OnEpoch(created)
	node.h.OnEpoch(second)
	for _, slot := range slots {
		if u33Flags(node.pairs, slot)&flagAwaiting == 0 {
			t.Fatalf("slot %d of a database created after the epoch began opened with no barrier (flags %05b)", slot, u33Flags(node.pairs, slot))
		}
	}
}

// TestAnRFChangeAtTheCurrentEpochIsTheRFTheNextMoveStartsFrom (BUG-0747): the copies whose
// answers the new owner needs are those of the RF the database had when the epoch moved,
// not the RF it was created with. [GUARD: refreshing rf on a state at the current epoch]
func TestAnRFChangeAtTheCurrentEpochIsTheRFTheNextMoveStartsFrom(t *testing.T) {
	members := []cluster.Member{u33Member("A", cluster.MemberActive), u33Member("B", cluster.MemberActive),
		u33Member("C", cluster.MemberActive), u33Member("D", cluster.MemberActive)}
	created := u33State(1, 2, members)
	altered := u33State(1, 3, members)
	altered.StateSeq = 2
	down := u33State(2, 3, []cluster.Member{u33Member("A", cluster.MemberDown), members[1], members[2], members[3]})
	before, after := cluster.BuildTable(altered), cluster.BuildTable(down)
	var slot uint16
	var wantCopies []string
	for candidate := range uint16(barrierSlots) {
		oldOwner, _ := before.Owner(candidate, 3)
		newOwner, _ := after.Owner(candidate, 3)
		if oldOwner.ID != "A" || newOwner.ID != "B" {
			continue
		}
		var alive, atCreation []string
		for _, copyMember := range before.Copies(candidate, 3) {
			if copyMember.ID != "A" {
				alive = append(alive, copyMember.ID)
			}
		}
		for _, copyMember := range before.Copies(candidate, 2) {
			if copyMember.ID != "A" {
				atCreation = append(atCreation, copyMember.ID)
			}
		}
		if !slices.Equal(alive, atCreation) {
			slot, wantCopies = candidate, alive
			break
		}
	}
	if wantCopies == nil {
		t.Fatal("no slot whose copy set differs between RF 2 and RF 3")
	}
	node := newU33Node("B")
	node.h.OnEpoch(created)
	node.h.OnEpoch(altered)
	node.h.OnEpoch(down)
	node.h.mu.Lock()
	opened := node.h.pairs[pairKey{slot, u33DB}]
	var got []string
	if opened != nil {
		got = slices.Clone(opened.oldCopies)
	}
	node.h.mu.Unlock()
	if opened == nil {
		t.Fatalf("slot %d opened with no barrier", slot)
	}
	if !slices.Equal(got, wantCopies) {
		t.Fatalf("slot %d asked the copies %v, want those of the altered RF: %v", slot, got, wantCopies)
	}
}

// TestADatabaseDroppedAtTheCurrentEpochReleasesItsOpenBarrier (BUG-0747 review F4): a pair
// in a barrier whose database is dropped by a commit that moves no epoch must not wait
// for an epoch move that may never come. [GUARD: releasing pairs of a database the
// refresh no longer lists]
func TestADatabaseDroppedAtTheCurrentEpochReleasesItsOpenBarrier(t *testing.T) {
	node, slots := u33Awaiting(t, cluster.Position{})
	dropped := u33State(2, 2, u33Active("A", "B"), bug0747OtherDB)
	dropped.StateSeq = 3
	node.h.OnEpoch(dropped)
	node.h.mu.Lock()
	_, stillOpen := node.h.pairs[pairKey{slots[0], u33DB}]
	node.h.mu.Unlock()
	if stillOpen {
		t.Fatal("a dropped database kept its barrier")
	}
}

// TestAnUnchangedDatabaseListKeepsTheRFMap: most commits at an epoch change no database,
// and they run on the supervisor's path before the lease is renewed, so they must not
// rebuild the map. [GUARD: skipping the rebuild when nothing changed]
func TestAnUnchangedDatabaseListKeepsTheRFMap(t *testing.T) {
	node, _ := u33Awaiting(t, cluster.Position{})
	same := u33State(2, 2, u33Active("A", "B"))
	same.StateSeq = 3
	node.h.mu.Lock()
	before := reflect.ValueOf(node.h.rf).Pointer()
	node.h.mu.Unlock()
	node.h.OnEpoch(same)
	node.h.mu.Lock()
	after := reflect.ValueOf(node.h.rf).Pointer()
	node.h.mu.Unlock()
	if before != after {
		t.Fatal("a state with the same databases rebuilt the rf map")
	}
}

// TestAReplayedMoveOnARestartedNodeIsDescribedAsTheRestartHold (BUG-0747): once queued
// states reach the handoff, a restarted node's replayed epochs open a real barrier with
// the old owner as peer; inside the stop bound after boot the refusal must still say
// "restarted" (BUG-0743 wording). [GUARD: the restart window on the barrier]
func TestAReplayedMoveOnARestartedNodeIsDescribedAsTheRestartHold(t *testing.T) {
	for _, restarted := range []bool{true, false} {
		first, second := u33State(1, 2, u33Active("A")), u33State(2, 2, u33Active("A", "B"))
		slots := u33Moved(t, first, second, 2, "A", "B")
		node := newU33Node("B")
		node.h.cfg.Restarted = restarted
		node.h.OnEpoch(first)
		node.h.OnEpoch(second)
		want := handoffHoldCause
		if restarted {
			want = restartHoldCause
		}
		if got := node.h.HoldCause(slots[0], u33DB); got != want {
			t.Fatalf("restarted %v: hold cause %q, want %q", restarted, got, want)
		}
		if got := awaitingCause(node.pairs.Snapshot(slots[0], u33DB)); got != want {
			t.Fatalf("restarted %v: the read gate's cause %q, want %q", restarted, got, want)
		}
	}
}
