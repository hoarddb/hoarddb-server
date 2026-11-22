package wire

import (
	"context"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
)

// TestADownLeaderActivatesItself: a member committed down that is itself the Raft leader asks
// itself back instead of returning, as a joiner on a one-voter cluster would (BUG-0754).
func TestADownLeaderActivatesItself(t *testing.T) {
	node := bug0754OpenNode(t, false)
	if err := node.setMemberState(context.Background(), "s0", "down"); err != nil {
		t.Fatalf("committing s0 down on a one-voter node: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		row, _ := reconcileMember(node.raft.State(), "s0")
		if row.State == cluster.MemberActive && node.ActivationAsks() >= 1 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("s0 is %s with %d asks after 5s, want active and at least one ask", row.State, node.ActivationAsks())
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestADownMemberWithAPairStillSeedingDoesNotAsk is the negative twin of the test above: a down
// member whose pair is not ready stays down and never asks (invariant I2). Without it a pass of
// the first test could mean the node asks whatever it holds.
func TestADownMemberWithAPairStillSeedingDoesNotAsk(t *testing.T) {
	node := bug0754OpenNode(t, false)
	dbID := s0Schema(t, node)
	const slot = uint16(0)
	// The first reconcile pass of the new database writes the pair's record asynchronously.
	deadline := time.Now().Add(10 * time.Second)
	for node.SlotRecordState(slot, dbID) != machine.SlotReady {
		if time.Now().After(deadline) {
			t.Fatalf("pair (%d, %d) is %v after 10s, want ready before the reseed", slot, dbID, node.SlotRecordState(slot, dbID))
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err := node.ReseedPair(slot, dbID); err != nil {
		t.Fatalf("ReseedPair: %v", err)
	}
	if got := node.SlotRecordState(slot, dbID); got != machine.SlotSeeding {
		t.Fatalf("pair (%d, %d) is %v after ReseedPair, want seeding", slot, dbID, got)
	}
	if err := node.setMemberState(context.Background(), "s0", "down"); err != nil {
		t.Fatalf("committing s0 down on a one-voter node: %v", err)
	}
	time.Sleep(2 * time.Second) // four ask periods
	if got := node.SlotRecordState(slot, dbID); got != machine.SlotSeeding {
		t.Fatalf("pair (%d, %d) is %v after 2s, want it still seeding: a one-voter cluster has no source", slot, dbID, got)
	}
	row, _ := reconcileMember(node.raft.State(), "s0")
	if row.State != cluster.MemberDown || node.ActivationAsks() != 0 {
		t.Fatalf("s0 is %s with %d asks, want down and none", row.State, node.ActivationAsks())
	}
}

// TestOnlyAJoiningOrDownMemberAsksToBeActivated pins the states a member asks to leave: joining
// and down, never a witness.
func TestOnlyAJoiningOrDownMemberAsksToBeActivated(t *testing.T) {
	asks := map[cluster.MemberState]bool{
		cluster.MemberJoining: true,
		cluster.MemberDown:    true,
		cluster.MemberActive:  false,
		cluster.MemberLeaving: false,
		cluster.MemberDead:    false,
	}
	for state, want := range asks {
		for _, witness := range []bool{false, true} {
			wantNow := want && !witness
			if got := activationAskable(cluster.Member{State: state, Witness: witness}); got != wantNow {
				t.Errorf("activationAskable(state %v, witness %v) = %v, want %v", state, witness, got, wantNow)
			}
		}
	}
}
