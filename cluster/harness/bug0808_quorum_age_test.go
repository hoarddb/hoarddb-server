package harness_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/harness"
)

// TestOnlyLeaseLapsedAndNoQuorumAreTransientRefusals pins the classification the harness
// tests retry on (BUG-0808): the code decides, never the words of the message, and a
// refusal that is about something else is not retried.
func TestOnlyLeaseLapsedAndNoQuorumAreTransientRefusals(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"lease lapsed", &cluster.Error{Code: cluster.CodeLeaseLapsed, Message: "x"}, true},
		{"no quorum, text without the word lease", &cluster.Error{Code: cluster.CodeNoQuorum, Message: "this node cannot see a majority"}, true},
		{"no quorum, wrapped", fmt.Errorf("inserting: %w", &cluster.Error{Code: cluster.CodeNoQuorum, Message: "x"}), true},
		{"not owner", &cluster.Error{Code: cluster.CodeNotOwner, Message: "x"}, false},
		{"owner down", &cluster.Error{Code: cluster.CodeOwnerDown, Message: "x"}, false},
		{"uncoded error that says lease", errors.New("lease"), false},
		{"nil", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := harness.IsTransientRefusal(tc.err); got != tc.want {
				t.Fatalf("IsTransientRefusal(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// TestALeaseCanBeValidWhileTheQuorumRoundIsStale shows why the settle step must wait for a
// fresh quorum round and not only for a lease: a follower whose two peers go away keeps a
// valid lease for up to step_down_ms but refuses NO_QUORUM once its last confirmed round is
// older than three heartbeats. AwaitLeases waited on the lease alone, so it returned for a
// node that was about to refuse every write.
func TestALeaseCanBeValidWhileTheQuorumRoundIsStale(t *testing.T) {
	c := harness.NewCluster(t, 3, harness.Options{NoDetection: true, NoProbes: true})
	leaderID := c.AwaitLeader(10 * time.Second)
	c.AwaitLeases(30 * time.Second)
	var survivor string
	for _, id := range c.IDs() {
		if id != leaderID && survivor == "" {
			survivor = id
		}
	}
	for _, id := range c.IDs() {
		if id != survivor {
			c.Kill(id)
		}
	}
	node := c.Node(survivor)
	c.Await("the survivor's lease to be valid while its quorum round is stale", 5*time.Second, func() bool {
		return node.LeaseValid() && node.QuorumLost()
	})
}
