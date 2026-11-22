package wire

import (
	"sync/atomic"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// countingState is a StateSource that counts how often State, which clones the whole
// committed state, is called.
type countingState struct {
	state cluster.ClusterState
	calls atomic.Int64
}

func (s *countingState) State() cluster.ClusterState {
	s.calls.Add(1)
	return s.state
}

// TestStaleReadsFlagIsReadWithoutCloningTheClusterState: [GUARD cluster.stale_reads costs no
// state clone on a keyed read (82 us and 329 KB per read at 100 databases)].
func TestStaleReadsFlagIsReadWithoutCloningTheClusterState(t *testing.T) {
	source := &countingState{state: cluster.ClusterState{Epoch: 1, StateSeq: 1, Members: u30iMembers("n1")}}
	source.state.Settings.StaleReads = true
	topo := NewClusterTopology("n1", source)
	before := source.calls.Load()
	if !topo.StaleReads() {
		t.Fatal("the committed setting is on and StaleReads says off")
	}
	if got := testing.AllocsPerRun(100, func() { topo.StaleReads() }); got != 0 {
		t.Fatalf("StaleReads allocated %v times per call, want 0", got)
	}
	if calls := source.calls.Load() - before; calls != 0 {
		t.Fatalf("StaleReads cloned the cluster state %d times", calls)
	}
	source.state.StateSeq, source.state.Settings.StaleReads = 2, false
	topo.Refresh()
	if topo.StaleReads() {
		t.Fatal("after the setting committed off, StaleReads still says on")
	}
}
