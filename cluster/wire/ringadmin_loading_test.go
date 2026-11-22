package wire

import (
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
)

type loadingLiveness struct {
	u83Liveness
	stage string
}

func (l loadingLiveness) Loading() string { return l.stage }

// TestDescribeNodeDoesNotSayOKWhileReplayingOrDiverged (review n1): a node that has not loaded
// its log, or that refused a committed entry, serves no client frame, so describe_node must not
// say serving ok, on one member or on four, and diverged wins over replaying. Break: drop the
// Loading case from describeNode.
func TestDescribeNodeDoesNotSayOKWhileReplayingOrDiverged(t *testing.T) {
	for _, members := range [][]string{{"node-a"}, {"node-a", "node-b", "node-c", "node-d"}} {
		state := u83State(cluster.MemberActive, members...)
		for stage, want := range map[string]string{ServingReplaying: ServingReplaying, ServingDiverged: ServingDiverged, "": ServingOK} {
			node := u83Fake(state, u83Liveness{"node-a", true})
			node.live = loadingLiveness{u83Liveness{"node-a", true}, stage}
			if got := node.describeNode(RingNodeFacts{Version: "v", StartedAt: time.Now()}); got.Serving != want {
				t.Errorf("%d members, loading %q: serving %q, want %q", len(members), stage, got.Serving, want)
			}
		}
	}
	if err := checkAdminNode(&RingAdminNode{NodeID: "n", Version: "v", DefaultRF: 1, Serving: ServingReplaying}); err != nil {
		t.Errorf("a replaying reply is refused by the check: %v", err)
	}
}
