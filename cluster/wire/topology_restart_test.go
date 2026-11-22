package wire

import (
	"sync/atomic"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// u57movingState is a source whose committed state moves on every read: state k has epoch k
// and replication factor 2 when k is odd, 3 when it is even, as a cluster under
// "alter database ... replication_factor" does between two reads of a view.
type u57movingState struct{ reads atomic.Int64 }

func (s *u57movingState) StateSeq() int64 { return s.reads.Add(1) }

func (s *u57movingState) State() cluster.ClusterState {
	k := s.reads.Load()
	rf := int32(3)
	if k%2 == 1 {
		rf = 2
	}
	return cluster.ClusterState{
		Epoch: k, StateSeq: k, Members: u30iMembers("n1"),
		Databases: []cluster.Database{{Name: "db", ID: 1, RF: rf}},
	}
}

// TestDatabaseAtEpochReadsOneView: the replication factor and the epoch a restart hold is
// opened with belong to the same committed state. Two reads of a moving view (Database, then
// Committed) pair an RF of one epoch with another. [GUARD: two reads instead of one]
func TestDatabaseAtEpochReadsOneView(t *testing.T) {
	topo := NewClusterTopology("n1", &u57movingState{})
	for range 20 {
		info, epoch, ok := topo.DatabaseAtEpoch(1)
		if !ok {
			t.Fatal("database 1 is not in the view")
		}
		want := 3
		if epoch%2 == 1 {
			want = 2
		}
		if info.RF != want {
			t.Fatalf("RF %d came with epoch %d, which is RF %d: the two belong to different states", info.RF, epoch, want)
		}
	}
}
