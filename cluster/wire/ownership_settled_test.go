package wire

import (
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// TestAcquireBeforeTheHandoffHasTheCommittedEpochClaimsNothing (BUG-0758): a pair first
// touched after the committed state moved but before the Handoff was fed that epoch must not
// be marked replayed and stopProved, or a new owner serves before its barrier opens.
func TestAcquireBeforeTheHandoffHasTheCommittedEpochClaimsNothing(t *testing.T) {
	settled := false
	pairs := &cluster.PairSet{}
	own := NewRestartedPairOwnership(pairs, PairOwnershipConfig{Settled: func() bool { return settled }})
	own.Acquire(5, testDB)
	if flags := pairs.Snapshot(5, testDB).Flags; flags != 0 {
		t.Fatalf("the pair has flags %#b before the Handoff saw the epoch, want 0", flags)
	}
	settled = true
	own.Acquire(5, testDB)
	if flags := pairs.Snapshot(5, testDB).Flags; flags != flagReplayed|flagStopProved {
		t.Fatalf("after the Handoff saw the epoch the pair has flags %#b, want replayed and stopProved", flags)
	}
}

// TestHandoffAppliedFollowsTheEpochItWasFed (BUG-0758).
func TestHandoffAppliedFollowsTheEpochItWasFed(t *testing.T) {
	node := newU33Node("n1")
	node.h.OnEpoch(u33State(1, 2, u33Active("n1", "n2")))
	if !node.h.Applied(1) {
		t.Fatal("Applied(1) is false after epoch 1 was fed")
	}
	if node.h.Applied(2) {
		t.Fatal("Applied(2) is true before epoch 2 was fed")
	}
	node.h.OnEpoch(u33State(2, 2, u33Active("n1", "n2")))
	if !node.h.Applied(2) {
		t.Fatal("Applied(2) is false after epoch 2 was fed")
	}
}
