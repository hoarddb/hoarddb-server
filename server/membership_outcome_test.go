package server

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/transport"
)

// Defence in depth and the outcome counter (spec §6.6, §6.2.4).

// TestNoTermOpensWhileMembershipDisagrees: a bucket's owner that disagrees
// with a member about the set at its epoch opens no term on it, and opens it
// once it agrees again. Guard: the mismatch check in openTerm.
func TestNoTermOpensWhileMembershipDisagrees(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, testBucket, 3)
	key := placementOf(testDatabase, testBucket)
	owner, err := clusterOf(tc, 0).Owner(key)
	if err != nil {
		t.Fatalf("placing the bucket: %v", err)
	}
	c := clusterOf(tc, indexOf(t, tc, owner.ID))
	held := heldBucket{database: testDatabase, bucket: testBucket}

	c.mismatch.Store(true)
	if err := c.openTerm(held, key); !errors.Is(err, errNotYetDecided) {
		t.Errorf("the owner, disagreeing with a member about its membership, opened a term: %v", err)
	}
	c.mismatch.Store(false)
	if err := c.openTerm(held, key); err != nil {
		t.Errorf("the owner, agreeing again, could not open the term: %v", err)
	}
}

// membershipOutcomeCounts reads the membership change counter of every node of
// tc, summed by outcome.
func membershipOutcomeCounts(t *testing.T, tc *testCluster) map[string]float64 {
	t.Helper()
	counts := make(map[string]float64)
	for index := range tc.nodes {
		families, err := clusterOf(tc, index).metrics.Gather()
		if err != nil {
			t.Fatalf("gathering node %d's metrics: %v", index+1, err)
		}
		for _, family := range families {
			if family.GetName() != "hoarddb_membership_changes_total" {
				continue
			}
			for _, metric := range family.GetMetric() {
				for _, label := range metric.GetLabel() {
					if label.GetName() == "outcome" {
						counts[label.GetValue()] += metric.GetCounter().GetValue()
					}
				}
			}
		}
	}
	return counts
}

// TestEveryMembershipOutcomeIsCounted drives one change to each of the six
// outcomes and reads the counter. Guard: changeMembership counts every
// outcome.
func TestEveryMembershipOutcomeIsCounted(t *testing.T) {
	setRecoveryDelays(t, time.Hour, 0, time.Hour)
	tc := newTestCluster(t, 5, 3)
	defer tc.stop()
	n1, n2, n5 := tc.addrs[0], tc.addrs[1], tc.addrs[4]
	first, second := clusterOf(tc, 0), clusterOf(tc, 1)

	first.transition.Store(true)
	if err := first.proposeRemoval(commandContext(t), n5); !errors.Is(err, ErrEpochInProgress) {
		t.Fatalf("refused: %v", err)
	}
	first.transition.Store(false)

	refuseEveryPrepare := func(_, phase, _ string, _ uint64) error {
		if phase == phasePrepare {
			return errors.New("test seam: every member refuses the prepare")
		}
		return nil
	}
	membershipPhaseHook.Store(&refuseEveryPrepare)
	if err := first.proposeRemoval(commandContext(t), n5); !errors.Is(err, ErrJoinFailedMajority) {
		t.Fatalf("no_majority: %v", err)
	}
	setPhaseHook(t, func(nodeID, phase, fromNode string, _ uint64) error {
		if fromNode == n1 && phase == phaseAccept && nodeID != n2 {
			time.Sleep(acceptRefusalDelay)
			return fmt.Errorf("test seam: %s refuses node 1's accept", nodeID)
		}
		return nil
	})
	if err := first.proposeRemoval(commandContext(t), n5); !errors.Is(err, ErrMembershipChangeIndeterminate) {
		t.Fatalf("indeterminate: %v", err)
	}
	requireAcceptance(t, second)

	// Node 2 holds node 1's acceptance, and finishes it while node 1 hears
	// nothing.
	setGossipSkip(t, func(nodeID, peerID, _ string) bool { return nodeID == n1 || peerID == n1 })
	setSendHook(t, func(_ context.Context, fromNode, toNode, _ string) error {
		if fromNode != n1 && toNode == n1 {
			return errors.New("test seam: node 1 hears nothing")
		}
		return nil
	})
	if err := second.proposeRemoval(commandContext(t), tc.addrs[3]); !errors.Is(err, ErrAnotherMembershipChangeFinished) {
		t.Fatalf("finished_another: %v", err)
	}
	if outcome, err := first.proposeRemovalOutcome(commandContext(t), n5); err != nil || outcome.kind != changeAlreadyCommitted {
		t.Fatalf("already_committed: %q, %v", outcome.kind, err)
	}
	if err := second.proposeRemoval(commandContext(t), tc.addrs[3]); err != nil {
		t.Fatalf("committed: %v", err)
	}

	counts := membershipOutcomeCounts(t, tc)
	for _, outcome := range []changeOutcome{changeCommitted, changeFinishedAnother, changeAlreadyCommitted,
		changeRefused, changeNoMajority, changeIndeterminate} {
		if counts[string(outcome)] < 1 {
			t.Errorf("the counter has %v for outcome %s, want at least 1 (all: %v)", counts[string(outcome)], outcome, counts)
		}
	}
}
