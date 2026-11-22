package wire

import (
	"sync/atomic"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// lazyTopologySource is a state a follower learns from its peers: it moves without the
// topology being told.
type lazyTopologySource struct {
	state atomic.Pointer[cluster.ClusterState]
}

func (s *lazyTopologySource) State() cluster.ClusterState { return *s.state.Load() }
func (s *lazyTopologySource) StateSeq() int64             { return s.state.Load().StateSeq }

// TestAFollowerSeesCommittedStateItNeverProposed: a node that only learns state from
// the leader reads the new database through the topology without anyone calling Refresh.
func TestAFollowerSeesCommittedStateItNeverProposed(t *testing.T) {
	source := &lazyTopologySource{}
	first := cluster.ClusterState{StateSeq: 1}
	source.state.Store(&first)
	topology := NewClusterTopology("n2", source)
	if _, ok := topology.DatabaseID("app"); ok {
		t.Fatal("a database that was never committed resolves")
	}
	moved := cluster.ClusterState{StateSeq: 2, Epoch: 1, Databases: []cluster.Database{{Name: "app", ID: 1, RF: 2, SchemaVersion: 1}}}
	source.state.Store(&moved)
	id, ok := topology.DatabaseID("app")
	if !ok || id != 1 {
		t.Fatalf("after the state moved the topology resolves app to %d, %v; want 1, true without a Refresh", id, ok)
	}
	if topology.Committed() != 1 {
		t.Fatalf("committed epoch %d, want 1", topology.Committed())
	}
}
