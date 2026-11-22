package server

import (
	"context"
	"testing"
	"time"
)

// Dispatch 6, step 6.4: every member hears the change (spec §19.3.7, T-101).

// TestEveryReachableMemberHasAdoptedWhenTheProposerReturns is T-101. Five
// nodes; when proposeJoin returns, all five are on the new epoch: prepare and
// accept return at a majority, but the commit goes to every member and the
// proposer waits for each reachable member's answer. With one member stopped
// the join still commits, and the four that answer have adopted. Guard: the
// wait for the commit answers in deliverCommits.
func TestEveryReachableMemberHasAdoptedWhenTheProposerReturns(t *testing.T) {
	t.Run("every member answers", func(t *testing.T) {
		tc := newTestCluster(t, 5, 3)
		defer tc.stop()
		proposeJoinAndCheckAdoption(t, tc, nil)
	})
	t.Run("one member is stopped", func(t *testing.T) {
		tc := newTestCluster(t, 5, 3)
		defer tc.stop()
		tc.stopNodeKeepingData(4)
		proposeJoinAndCheckAdoption(t, tc, map[int]bool{4: true})
	})
}

// proposeJoinAndCheckAdoption proposes a join through node 1 and fails the
// test unless every running member is on the new epoch when it returns.
func proposeJoinAndCheckAdoption(t *testing.T, tc *testCluster, stopped map[int]bool) {
	t.Helper()
	proposer := tc.nodes[0].server.core.cluster
	before := proposer.Epoch()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	started := time.Now()
	if err := proposer.proposeJoin(ctx, reserveNodePorts(t).ringNode()); err != nil {
		t.Fatalf("proposeJoin: %v", err)
	}
	took := time.Since(started)
	adopted := 0
	for index, node := range tc.nodes {
		if stopped[index] {
			continue
		}
		if epoch := node.server.core.cluster.Epoch(); epoch == before+1 {
			adopted++
		} else {
			t.Errorf("node %d is on epoch %d when the proposer returns, want %d", index+1, epoch, before+1)
		}
	}
	t.Logf("%d running members adopted epoch %d; proposeJoin took %s", adopted, before+1, took.Round(time.Millisecond))
}
