package wire

import (
	"context"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// BUG-0813: the stop bound of a barrier (step_down_ms + heartbeat_ms) must come from the
// committed settings, also when they changed in a state that moved no epoch. u33State's
// settings give 1500 + 600 = 2100 ms.

// u0813Raised and u0813Lowered are the step_down_ms values the tests commit; with the 600 ms
// heartbeat of u33State the bounds are 12.6 s and 2.1 s.
const (
	u0813Raised      = 12000
	u0813RaisedBound = (u0813Raised + 600) * time.Millisecond
)

// u0813WithStepDown is state with step_down_ms set to stepDown, committed at seq.
func u0813WithStepDown(state cluster.ClusterState, seq, stepDown int64) cluster.ClusterState {
	state.StateSeq = seq
	state.Settings.StepDownMs = stepDown
	return state
}

// u0813Owner is node B of a three-node cluster at epoch 1, fed the given states (all at
// epoch 1, in commit order), with a slot it owns whose two other copies report applied.
func u0813Owner(t *testing.T, states ...cluster.ClusterState) (*u33Node, uint16) {
	t.Helper()
	table := cluster.BuildTable(states[0])
	var slot uint16
	for candidate := range uint16(barrierSlots) {
		if owner, _ := table.Owner(candidate, 3); owner.ID == "B" {
			slot = candidate
			break
		}
	}
	node := newU33Node("B")
	applied := cluster.Position{Epoch: 1, Seq: 4}
	for _, peer := range []string{"A", "C"} {
		node.link.replies[peer] = PositionReply{State: PositionStateReady, AppliedPos: applied}
	}
	u33SetApplied(node.pairs, slot, u33DB, applied)
	for _, state := range states {
		node.h.OnEpoch(state)
	}
	return node, slot
}

// u0813Bound is the Bound the barrier of a pair was opened with.
func u0813Bound(t *testing.T, node *u33Node, slot uint16) time.Duration {
	t.Helper()
	node.h.mu.Lock()
	defer node.h.mu.Unlock()
	hp := node.h.pairs[pairKey{slot, u33DB}]
	if hp == nil {
		t.Fatalf("slot %d has no barrier", slot)
	}
	return hp.barrier.Bound
}

// u0813HeldUntil checks that the restart hold of slot is kept 1 ms before bound and ends at it.
func u0813HeldUntil(t *testing.T, node *u33Node, slot uint16, bound time.Duration) {
	t.Helper()
	node.clock.set(bound - time.Millisecond)
	if err := node.h.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if flags := u33Flags(node.pairs, slot); flags&flagAwaiting == 0 {
		t.Fatalf("flags %05b %s after boot: the hold ended before the committed stop bound %s", flags, bound-time.Millisecond, bound)
	}
	node.clock.set(bound)
	if err := node.h.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if flags := u33Flags(node.pairs, slot); flags&flagAwaiting != 0 || flags&flagStopProved == 0 {
		t.Fatalf("flags %05b at the stop bound %s, want the hold ended with stop proof", flags, bound)
	}
}

// TestRestartHoldFollowsAStepDownRaisedAfterTheEpoch: step_down_ms raised by a state that
// moved no epoch is the bound a later restart hold waits. [BUG-0813: the hold kept 2.1 s]
func TestRestartHoldFollowsAStepDownRaisedAfterTheEpoch(t *testing.T) {
	state := u33State(1, 3, u33Active("A", "B", "C"))
	node, slot := u0813Owner(t, state, u0813WithStepDown(state, 2, u0813Raised))
	if held, decided := node.h.RestartHold(slot, u33DB, 3, 1); !held || !decided {
		t.Fatalf("RestartHold = held %v decided %v, want both", held, decided)
	}
	if bound := u0813Bound(t, node, slot); bound != u0813RaisedBound {
		t.Fatalf("the restart hold opened with bound %s, want the committed %s", bound, u0813RaisedBound)
	}
	u0813HeldUntil(t, node, slot, u0813RaisedBound)
}

// TestRestartHoldKeepsTheEpochBoundWhenStepDownIsLowered: a step_down_ms lowered after the
// epoch commit does not shorten the hold, because the owner before that commit was granted
// its leases under the longer value. [GUARD: a fix that only follows the latest state]
func TestRestartHoldKeepsTheEpochBoundWhenStepDownIsLowered(t *testing.T) {
	state := u0813WithStepDown(u33State(1, 3, u33Active("A", "B", "C")), 1, u0813Raised)
	node, slot := u0813Owner(t, state, u0813WithStepDown(state, 2, 1500))
	node.h.RestartHold(slot, u33DB, 3, 1)
	if bound := u0813Bound(t, node, slot); bound != u0813RaisedBound {
		t.Fatalf("the restart hold opened with bound %s after step_down_ms was lowered, want the epoch's %s", bound, u0813RaisedBound)
	}
	u0813HeldUntil(t, node, slot, u0813RaisedBound)
}

// TestRestartHoldFollowsARaiseAfterALowering: lowered and then raised again at the same epoch,
// the larger of the epoch's and the latest value holds. [BUG-0813]
func TestRestartHoldFollowsARaiseAfterALowering(t *testing.T) {
	state := u33State(1, 3, u33Active("A", "B", "C"))
	node, slot := u0813Owner(t, state, u0813WithStepDown(state, 2, 1000), u0813WithStepDown(state, 3, u0813Raised))
	node.h.RestartHold(slot, u33DB, 3, 1)
	if bound := u0813Bound(t, node, slot); bound != u0813RaisedBound {
		t.Fatalf("the restart hold opened with bound %s, want the committed %s", bound, u0813RaisedBound)
	}
}

// TestAFailoverAfterARaisedStepDownWaitsTheRaisedBound: a raise committed at epoch 1, then
// the owner committed down at epoch 2. The new owner's barrier proves the old owner stopped
// only after the raised bound. [GUARD: the epoch-move barrier; it read the epoch's own state
// before the fix too]
func TestAFailoverAfterARaisedStepDownWaitsTheRaisedBound(t *testing.T) {
	first := u33State(1, 2, u33Active("A", "B"))
	raised := u0813WithStepDown(first, 2, u0813Raised)
	second := u0813WithStepDown(u33State(2, 2, []cluster.Member{u33Member("A", cluster.MemberDown), u33Member("B", cluster.MemberActive)}), 3, u0813Raised)
	slot := u33Moved(t, first, second, 2, "A", "B")[0]
	node := newU33Node("B")
	node.link.replies["B"] = PositionReply{State: PositionStateReady}
	node.h.OnEpoch(first)
	node.h.OnEpoch(raised)
	node.h.OnEpoch(second)
	if bound := u0813Bound(t, node, slot); bound != u0813RaisedBound {
		t.Fatalf("the failover barrier opened with bound %s, want the committed %s", bound, u0813RaisedBound)
	}
	node.clock.set(u0813RaisedBound - time.Millisecond)
	if err := node.h.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if flags := u33Flags(node.pairs, slot); flags&flagAwaiting == 0 || flags&flagStopProved != 0 {
		t.Fatalf("flags %05b before the raised bound, want the old owner not yet proved stopped", flags)
	}
	node.clock.set(u0813RaisedBound)
	if err := node.h.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if flags := u33Flags(node.pairs, slot); flags&flagStopProved == 0 {
		t.Fatalf("flags %05b at the raised bound, want the old owner proved stopped", flags)
	}
}

// TestHandoffStopBoundIsTheLargerOfTheEpochAndTheLatestState is the bound computation alone:
// a state at the current epoch can only lengthen the bound the epoch's commit set, a new
// epoch sets it afresh, and a state of an older epoch changes nothing. [BUG-0813]
func TestHandoffStopBoundIsTheLargerOfTheEpochAndTheLatestState(t *testing.T) {
	members := u33Active("A", "B", "C")
	at := func(epoch, seq, stepDown int64) cluster.ClusterState {
		return u0813WithStepDown(u33State(epoch, 3, members), seq, stepDown)
	}
	ms := func(stepDown int64) time.Duration { return time.Duration(stepDown+600) * time.Millisecond }
	for _, tc := range []struct {
		name   string
		states []cluster.ClusterState
		want   time.Duration
	}{
		{"raised at the epoch", []cluster.ClusterState{at(1, 1, 1500), at(1, 2, 9000)}, ms(9000)},
		{"lowered at the epoch", []cluster.ClusterState{at(1, 1, 9000), at(1, 2, 1500)}, ms(9000)},
		// 3000 is above the epoch's 1500 and below the 9000 seen in between: an epoch-only bound
		// gives 1500 and a bound that remembers the largest value gives 9000.
		{"raised, then lowered to a value above the epoch's", []cluster.ClusterState{at(1, 1, 1500), at(1, 2, 9000), at(1, 3, 3000)}, ms(3000)},
		{"a new epoch sets it afresh", []cluster.ClusterState{at(1, 1, 9000), at(2, 2, 1500)}, ms(1500)},
		{"an older epoch changes nothing", []cluster.ClusterState{at(2, 2, 1500), at(1, 3, 9000)}, ms(1500)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			node, slot := u0813Owner(t, tc.states...)
			epoch := uint64(tc.states[0].Epoch)
			for _, state := range tc.states {
				epoch = max(epoch, uint64(state.Epoch))
			}
			if _, decided := node.h.RestartHold(slot, u33DB, 3, epoch); !decided {
				t.Fatal("RestartHold is undecided at the epoch it was fed")
			}
			if bound := u0813Bound(t, node, slot); bound != tc.want {
				t.Fatalf("bound %s, want %s", bound, tc.want)
			}
		})
	}
}

// TestAFailoverSoonAfterALoweredStepDownWaitsTheOldLeases is the hazard of lowering
// step_down_ms, which BUG-0813's fix does not change: the old owner accepted its last grant
// under the step_down_ms it had applied, and nothing makes it apply the lowered value before
// it is replaced. Here 12000 is lowered to 1500 at 1 s and the owner is committed down at 2 s;
// the new owner proves the old one stopped at 2 s + 2.1 s, while a lease accepted under 12000
// just before the commit runs to about 2 s + 12 s.
func TestAFailoverSoonAfterALoweredStepDownWaitsTheOldLeases(t *testing.T) {
	t.Skip("FINDING: a step_down_ms lowered shortly before a failover lets the new owner prove the old one stopped while a lease granted under the old value may still run; not BUG-0813, reported with its fix")
	first := u0813WithStepDown(u33State(1, 2, u33Active("A", "B")), 1, u0813Raised)
	lowered := u0813WithStepDown(first, 2, 1500)
	second := u0813WithStepDown(u33State(2, 2, []cluster.Member{u33Member("A", cluster.MemberDown), u33Member("B", cluster.MemberActive)}), 3, 1500)
	slot := u33Moved(t, first, second, 2, "A", "B")[0]
	node := newU33Node("B")
	node.link.replies["B"] = PositionReply{State: PositionStateReady}
	node.h.OnEpoch(first)
	node.clock.set(time.Second)
	node.h.OnEpoch(lowered)
	node.clock.set(2 * time.Second)
	node.h.OnEpoch(second)
	node.clock.set(2*time.Second + u0813RaisedBound - time.Millisecond)
	if err := node.h.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if flags := u33Flags(node.pairs, slot); flags&flagStopProved != 0 {
		t.Fatalf("flags %05b: the old owner was proved stopped %s after the failover, inside a lease it may have accepted under step_down_ms %d", flags, u0813RaisedBound-time.Millisecond, u0813Raised)
	}
}

// TestSawStateFollowsEveryStateFed: the restart hold's caller waits for the state it judged
// by, including one that moved no epoch. [BUG-0813: the boot ordering]
func TestSawStateFollowsEveryStateFed(t *testing.T) {
	node := newU33Node("B")
	if node.h.SawState(0) {
		t.Fatal("SawState is true before any state was fed")
	}
	state := u33State(1, 3, u33Active("A", "B", "C"))
	node.h.OnEpoch(state)
	if !node.h.SawState(1) || node.h.SawState(2) {
		t.Fatalf("after state_seq 1: SawState(1) %v, SawState(2) %v; want true, false", node.h.SawState(1), node.h.SawState(2))
	}
	node.h.OnEpoch(u0813WithStepDown(state, 2, u0813Raised))
	if !node.h.SawState(2) {
		t.Fatal("a state at the current epoch was fed but SawState(2) is false")
	}
}
