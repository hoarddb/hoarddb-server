package wire

import (
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// TestADatabaseCreatedAndAnEpochMovedInOneBurstStillOpensTheBarrier (BUG-0747): a follower
// applies the create database commit and the membership commit before its supervisor
// runs. The queue must hand the handoff the state in between, or the database is missing
// from the previous rf and its new owner opens with no barrier. [GUARD: queueing a state
// whose database list changed]
func TestADatabaseCreatedAndAnEpochMovedInOneBurstStillOpensTheBarrier(t *testing.T) {
	first := u33State(1, 2, u33Active("A"), bug0747OtherDB)
	created := u33State(1, 2, u33Active("A"), bug0747OtherDB, int64(u33DB))
	created.StateSeq = 2
	second := u33State(2, 2, u33Active("A", "B"), bug0747OtherDB, int64(u33DB))
	slots := u33Moved(t, first, second, 2, "A", "B")
	node := newU33Node("B")
	node.h.OnEpoch(first)

	var parts clusterParts
	parts.queueState(first)
	parts.takeEpochs() // the supervisor fed it
	for _, state := range []cluster.ClusterState{created, second} {
		parts.queueState(state)
	}
	for _, queued := range parts.takeEpochs() {
		node.h.OnEpoch(queued)
	}
	node.h.OnEpoch(second)
	for _, slot := range slots {
		if u33Flags(node.pairs, slot)&flagAwaiting == 0 {
			t.Fatalf("slot %d of a database created in the same burst as the epoch move opened with no barrier (flags %05b)", slot, u33Flags(node.pairs, slot))
		}
	}
}
