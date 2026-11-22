package harness_test

import (
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/harness"
)

func TestHarnessBootstrapsThreeVoters(t *testing.T) {
	c := harness.NewCluster(t, 3, harness.Options{})
	state := c.AwaitConverged(20 * time.Second)
	if len(state.Members) != 3 || len(state.Voters) != 3 {
		t.Fatalf("members %d voters %d, want 3 and 3: %+v", len(state.Members), len(state.Voters), state)
	}
	for _, member := range state.Members {
		if member.State != cluster.MemberActive {
			t.Fatalf("member %s is %v, want active", member.ID, member.State)
		}
	}
	leaders := 0
	for _, id := range c.IDs() {
		if c.Node(id).IsLeader() {
			leaders++
		}
	}
	if leaders != 1 {
		t.Fatalf("%d leaders among %v, want exactly 1", leaders, c.IDs())
	}
}
