package server

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// TestAProposersLateCommitDoesNotMoveItsViewBack: node 1 proposes a join at
// epoch 2 and a majority accepts it; node 1 is held after the set is chosen
// and before it adopts it. Meanwhile a member that holds the acceptance
// proposes, finishes the same join at epoch 2 and commits it to everyone,
// node 1 included, and then commits epoch 3, the joiner removed again. When
// node 1 is released, its adoption of epoch 2 must not move its view back
// from epoch 3. Guard: the forward-only gate in adoptMembershipLocked.
func TestAProposersLateCommitDoesNotMoveItsViewBack(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	ports := reserveNodePorts(t)
	startJoinTarget(t, tc, ports, tc.token)
	proposer := clusterOf(tc, 0)
	founders := proposer.Members()

	chosenHeld := make(chan struct{})
	release := make(chan struct{})
	var heldOnce, releasedOnce sync.Once
	hold := func(nodeID, phase, _ string, _ uint64) error {
		if nodeID == proposer.self.ID && phase == "chosen" {
			heldOnce.Do(func() { close(chosenHeld) })
			<-release
		}
		return nil
	}
	membershipPhaseHook.Store(&hold)
	t.Cleanup(func() { membershipPhaseHook.Store(nil) })
	// Deferred after the cluster's stop, so it runs first: a node held in the
	// hook would keep Stop waiting.
	defer releasedOnce.Do(func() { close(release) })

	committed := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		committed <- proposer.proposeJoin(ctx, ports.ringNode())
	}()
	select {
	case <-chosenHeld:
	case <-time.After(30 * time.Second):
		t.Fatal("test setup: node 1's join was never chosen")
	}

	// The finisher is a member whose record holds node 1's acceptance, so its
	// ballot is above node 1's and the value rule gives it node 1's join.
	var finisher *Cluster
	if !holdsWithin(10*time.Second, func() bool {
		for _, index := range []int{1, 2} {
			if pending := pendingOf(clusterOf(tc, index)); pending != nil && pending.Accepted != nil {
				finisher = clusterOf(tc, index)
				return true
			}
		}
		return false
	}) {
		t.Fatal("test setup: no member holds node 1's acceptance")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	fabricated := fabricatedMemberAddr(t)
	err := finisher.proposeJoin(ctx, ringNodeAt(fabricated))
	if !errors.Is(err, ErrAnotherMembershipChangeFinished) {
		t.Fatalf("test setup: %s's change answered %v; it should have finished node 1's join", finisher.self.ID, err)
	}
	if epoch := proposer.Epoch(); epoch != InitialEpoch+1 {
		t.Fatalf("test setup: node 1 is at epoch %d after the finished join, want %d", epoch, InitialEpoch+1)
	}
	if err := finisher.proposeRemoval(ctx, ports.ringNode().ID); err != nil {
		t.Fatalf("test setup: removing the joiner at epoch %d: %v", InitialEpoch+2, err)
	}
	later := InitialEpoch + 2
	if epoch := proposer.Epoch(); epoch != later {
		t.Fatalf("test setup: node 1 is at epoch %d after the joiner's removal, want %d", epoch, later)
	}

	releasedOnce.Do(func() { close(release) })
	select {
	case err := <-committed:
		if err != nil {
			t.Logf("the late commit answered: %v", err)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("the proposal never returned after it was released")
	}
	if epoch, members := proposer.epochMembers(); epoch != later || len(members) != len(founders) {
		t.Fatalf("after its late commit node 1 is at epoch %d with %d members; it had caught up to epoch %d with %d: "+
			"the commit moved its view back", epoch, len(members), later, len(founders))
	}
}
