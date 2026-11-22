package wire

import (
	"context"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
)

// u5717Slot returns a slot whose three copies rank A, B, C at the first state of a
// three-member cluster, and the states of the two-step failover: A down, then A and B down.
func u5717Slot(t *testing.T) (slot uint16, states [3]cluster.ClusterState) {
	t.Helper()
	states[0] = u33State(1, 3, u33Active("A", "B", "C"))
	states[1] = u33State(2, 3, []cluster.Member{u33Member("A", cluster.MemberDown), u33Member("B", cluster.MemberActive), u33Member("C", cluster.MemberActive)})
	states[2] = u33State(3, 3, []cluster.Member{u33Member("A", cluster.MemberDown), u33Member("B", cluster.MemberDown), u33Member("C", cluster.MemberActive)})
	table := cluster.BuildTable(states[0])
	for candidate := range uint16(barrierSlots) {
		copies := table.Copies(candidate, 3)
		if len(copies) == 3 && copies[0].ID == "A" && copies[1].ID == "B" && copies[2].ID == "C" {
			return candidate, states
		}
	}
	t.Fatal("no slot ranks A, B, C")
	return 0, states
}

// u5717Survivor is node C holding applied on the slot, fed the failover states. skip leaves
// out the intermediate epoch, as a slow supervisor would.
func u5717Survivor(t *testing.T, applied cluster.Position, skip bool) (*u33Node, uint16) {
	t.Helper()
	slot, states := u5717Slot(t)
	node := newU33Node("C")
	u33SetApplied(node.pairs, slot, u33DB, applied)
	node.h.OnEpoch(states[0])
	node.clock.set(0)
	if !skip {
		node.h.OnEpoch(states[1])
	}
	node.h.OnEpoch(states[2])
	return node, slot
}

func u5717AssertOpened(t *testing.T, node *u33Node, slot uint16, applied cluster.Position) {
	t.Helper()
	if err := node.h.Tick(context.Background()); err != nil {
		text := err.Error()
		if len(text) > 300 {
			text = text[:300] + "..."
		}
		t.Fatalf("Tick after the bound: %s", text)
	}
	flags := u33Flags(node.pairs, slot)
	if flags&flagAwaiting != 0 || flags&flagStopProved == 0 {
		t.Fatalf("the survivor's pair flags are %05b after the bound, want the awaiting hold cleared and the stop proof set", flags)
	}
	target, known := node.h.CachedTarget(slot, u33DB)
	if !known || target != applied {
		t.Fatalf("cached target %+v known=%v, want the survivor's own position %+v", target, known, applied)
	}
	if !node.h.Degraded(slot, u33DB) {
		// The pair is freed once READY is queued only when nothing can ask for it again; here
		// the machine keeps it until the next epoch, so the mark must still be readable.
		t.Fatal("the pair opened on the survivor alone and was not marked degraded")
	}
	node.catch.mu.Lock()
	defer node.catch.mu.Unlock()
	ran := false
	for _, call := range node.catch.calls {
		ran = ran || call == (pairKey{slot, u33DB})
	}
	if !ran {
		t.Fatal("no catch-up ran for the pair")
	}
}

// TestATwoStepFailoverOpensOnTheSurvivingCopy: the owner and the first copy are both down,
// so only this node is left of the three copies. The old rule asked for two answers and held
// forever; the pair now opens on what the survivor has. [GUARD: the structural branch of Target]
func TestATwoStepFailoverOpensOnTheSurvivingCopy(t *testing.T) {
	applied := cluster.Position{Epoch: 1, Seq: 7}
	node, slot := u5717Survivor(t, applied, false)
	node.clock.set(u33Bound)
	u5717AssertOpened(t, node, slot, applied)
}

// TestSkippedEpochsStillOpenOnTheSurvivors: the intermediate epoch, which only changes a
// member's state and not the copy set, is not fed. The pair still opens. OnEpoch's contract
// stays "every committed epoch, in order": a skipped epoch that changed the copy set would
// make the old copies come from the last epoch fed, and this test does not cover that.
func TestSkippedEpochsStillOpenOnTheSurvivors(t *testing.T) {
	applied := cluster.Position{Epoch: 1, Seq: 4}
	node, slot := u5717Survivor(t, applied, true)
	node.clock.set(u33Bound)
	u5717AssertOpened(t, node, slot, applied)
}

// TestAShortfallOfOneAnswerStillHolds: three copies are not down and one answers; the other
// two are silent but alive, so the quorum is only not yet met and the pair keeps holding.
// [GUARD: loosening the structural test to answers < need opens this pair]
func TestAShortfallOfOneAnswerStillHolds(t *testing.T) {
	first := u33State(1, 3, u33Active("A", "C", "D"))
	second := u33State(2, 3, u33Active("A", "B", "C", "D"))
	slot := u33Moved(t, first, second, 3, "A", "B")[0]
	node := newU33Node("B")
	node.h.OnEpoch(first)
	node.clock.set(0)
	node.h.OnEpoch(second)
	node.link.replies["A"] = PositionReply{State: PositionStateReady, AppliedPos: cluster.Position{Epoch: 1, Seq: 9}}
	node.link.silent["C"], node.link.silent["D"] = true, true
	node.clock.set(u33Bound)
	if err := node.h.Tick(context.Background()); err == nil {
		t.Fatal("Tick reported no error while two live copies were silent")
	}
	if flags := u33Flags(node.pairs, slot); flags&flagAwaiting == 0 {
		t.Fatalf("a live silent copy let the pair open: flags %05b", flags)
	}
	if _, known := node.h.CachedTarget(slot, u33DB); known {
		t.Fatal("a target was recorded from one answer of the two needed")
	}
	if node.catch.callCount() != 0 {
		t.Fatal("a catch-up ran before the quorum of answers existed")
	}
}

// TestTheNewOwnersPairIsReplayedAfterReady: a pair that became owner by handoff has no
// Acquire to mark its log tail replayed; READY proves the same fact.
func TestTheNewOwnersPairIsReplayedAfterReady(t *testing.T) {
	node, slots := u33Awaiting(t, cluster.Position{})
	slot := slots[0]
	key := pairKey{slot, u33DB}
	node.h.mu.Lock()
	node.h.apply(key, node.h.pairs[key], machine.CatchUpDone{Current: true})
	node.h.mu.Unlock()
	u33Stopped(t, node, "A", 2, slot)
	flags := u33Flags(node.pairs, slot)
	if flags&(flagSending|flagAwaiting) != 0 || flags&flagStopProved == 0 {
		t.Fatalf("the barrier did not end: flags %05b", flags)
	}
	if flags&flagReplayed == 0 {
		t.Fatalf("flags %05b after READY: the new owner would be refused with 'log tail has not been replayed'", flags)
	}
}

// TestNoTargetIsAskedBeforeTheBound: until the old owner is proved stopped no copy is
// asked for its position.
func TestNoTargetIsAskedBeforeTheBound(t *testing.T) {
	node, slots := u33Awaiting(t, cluster.Position{})
	node.clock.set(u33Bound - 1)
	if err := node.h.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(node.link.asked) != 0 || node.catch.callCount() != 0 {
		t.Fatalf("copies asked %v and %d catch-ups ran before the bound", node.link.asked, node.catch.callCount())
	}
	if u33Flags(node.pairs, slots[0])&flagAwaiting == 0 {
		t.Fatal("the pair opened before the bound")
	}
}

// TestCachedTargetDoesNotTakeTheHandoffLockUnderAPairLock: the write path asks for the target
// while it holds a pair's lock, and Tick holds the Handoff's lock while it takes each pair's.
// If CachedTarget needs the Handoff's lock the two wait for each other (BUG-0744).
func TestCachedTargetDoesNotTakeTheHandoffLockUnderAPairLock(t *testing.T) {
	node, slots := u33Awaiting(t, cluster.Position{})
	slot := slots[0]
	inside, proceed, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		defer close(finished)
		node.pairs.With(slot, u33DB, func(*cluster.Pair) {
			close(inside)
			<-proceed
			node.h.CachedTarget(slot, u33DB)
		})
	}()
	<-inside
	node.clock.set(u33Bound)
	go func() { _ = node.h.Tick(context.Background()) }()
	// Tick holds the Handoff's lock until it has taken this pair's lock, which is held above.
	for node.h.mu.TryLock() {
		node.h.mu.Unlock()
		time.Sleep(time.Millisecond)
	}
	close(proceed)
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("CachedTarget under a pair lock deadlocked with Tick: lock order inverted")
	}
}

// TestADegradedTargetAccountsForEveryLiveCopy: RF 5, copies A..E, A, B and C committed down,
// D the new owner. The quorum (3) cannot exist, but E is alive: a write acknowledged by
// A, B and E is on E alone, so the pair may open only once E is accounted for, either by its
// position or by saying it is still seeding (it then holds no acknowledged write).
func TestADegradedTargetAccountsForEveryLiveCopy(t *testing.T) {
	first := u33State(1, 5, u33Active("A", "B", "C", "D", "E"))
	second := u33State(2, 5, []cluster.Member{u33Member("A", cluster.MemberDown), u33Member("B", cluster.MemberDown),
		u33Member("C", cluster.MemberDown), u33Member("D", cluster.MemberActive), u33Member("E", cluster.MemberActive)})
	var slot uint16
	found := false
	table := cluster.BuildTable(first)
	for candidate := range uint16(barrierSlots) {
		copies := table.Copies(candidate, 5)
		if len(copies) == 5 && copies[0].ID == "A" && copies[1].ID == "B" && copies[2].ID == "C" && copies[3].ID == "D" && copies[4].ID == "E" {
			slot, found = candidate, true
			break
		}
	}
	if !found {
		t.Fatal("no slot ranks A to E in order")
	}
	own := cluster.Position{Epoch: 1, Seq: 3}
	ahead := cluster.Position{Epoch: 1, Seq: 7}
	cases := []struct {
		name     string
		reply    *PositionReply
		silent   bool
		wantOpen bool
		want     cluster.Position
	}{
		{name: "E alive but silent holds", silent: true},
		{name: "E answers ahead, the target is its position", reply: &PositionReply{State: PositionStateReady, AppliedPos: ahead}, wantOpen: true, want: ahead},
		{name: "E still seeding is excluded", reply: &PositionReply{State: PositionStateSeeding, AppliedPos: ahead}, wantOpen: true, want: own},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			node := newU33Node("D")
			u33SetApplied(node.pairs, slot, u33DB, own)
			node.h.OnEpoch(first)
			node.clock.set(0)
			node.h.OnEpoch(second)
			if tc.reply != nil {
				node.link.replies["E"] = *tc.reply
			}
			node.link.silent["E"] = tc.silent
			target, err := node.h.Target(context.Background(), slot, u33DB, 1)
			if !tc.wantOpen {
				if err == nil {
					t.Fatalf("a live silent copy was skipped: target %+v", target)
				}
				if _, known := node.h.CachedTarget(slot, u33DB); known || node.h.Degraded(slot, u33DB) {
					t.Fatal("a target or a degraded mark was recorded while a live copy was unaccounted for")
				}
				return
			}
			if err != nil || target != tc.want {
				t.Fatalf("target %+v, error %v; want %+v", target, err, tc.want)
			}
			if !node.h.Degraded(slot, u33DB) {
				t.Fatal("the pair opened on fewer than the quorum and was not marked degraded")
			}
		})
	}
}
