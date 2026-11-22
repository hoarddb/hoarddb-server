package wire

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// bug0754OpenNode is s0OpenClusterNode with the activation ask held when hold is true, so the
// ask loop cannot race a test that calls activateOnLeader by hand; a test of the ask itself
// passes false.
func bug0754OpenNode(t *testing.T, hold bool) *Node {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	opts := &ClusterOptions{Listener: listener, InsecureTestOnly: true, RaftLogger: hclog.NewNullLogger(), HoldActivation: hold}
	node, err := OpenNode(ctx, NodeConfig{Dir: t.TempDir(), NodeID: "s0", Cluster: opts})
	if err != nil {
		t.Fatalf("opening the cluster node: %v", err)
	}
	t.Cleanup(func() { _ = node.Close() })
	return node
}

// TestActivateOnLeaderTakesADownMember: a member committed down that asks to come back is
// committed active (BUG-0754).
func TestActivateOnLeaderTakesADownMember(t *testing.T) {
	node := bug0754OpenNode(t, true)
	ctx := context.Background()
	if err := node.setMemberState(ctx, "s0", "down"); err != nil {
		t.Fatalf("committing s0 down on a one-voter node: %v", err)
	}
	before := node.raft.State()

	out, err := node.activateOnLeader(ctx, "s0")
	if err != nil {
		t.Fatalf("activateOnLeader of a down member: %v", err)
	}
	if !strings.HasPrefix(out.Result, "Activated s0") {
		t.Fatalf("Result = %q, want it to start with %q", out.Result, "Activated s0")
	}
	after := node.raft.State()
	row, _ := reconcileMember(after, "s0")
	if row.State != cluster.MemberActive {
		t.Fatalf("s0 is %s after the activation, want active", row.State)
	}
	if after.Epoch != before.Epoch+1 || out.Epoch != after.Epoch {
		t.Fatalf("epoch %d (reply %d), want %d: rank 1 becomes rank 2", after.Epoch, out.Epoch, before.Epoch+1)
	}
}

// TestActivateOnLeaderAnswersTheFactsForEveryOtherState: an ask for a member that is neither
// joining nor down commits nothing and moves no state_seq (invariant I5).
func TestActivateOnLeaderAnswersTheFactsForEveryOtherState(t *testing.T) {
	for _, state := range []string{"active", "leaving", "dead"} {
		t.Run(state, func(t *testing.T) {
			node := bug0754OpenNode(t, true)
			ctx := context.Background()
			if state != "active" {
				if err := node.setMemberState(ctx, "s0", state); err != nil {
					t.Fatalf("committing s0 %s on a one-voter node: %v", state, err)
				}
			}
			before := node.raft.State()

			out, err := node.activateOnLeader(ctx, "s0")
			if err != nil {
				t.Fatalf("activateOnLeader of a %s member: %v", state, err)
			}
			if !strings.Contains(out.Result, "already "+state) {
				t.Fatalf("Result = %q, want it to contain %q", out.Result, "already "+state)
			}
			after := node.raft.State()
			if after.StateSeq != before.StateSeq || after.Epoch != before.Epoch || out.StateSeq != before.StateSeq || out.Epoch != before.Epoch {
				t.Fatalf("state_seq %d epoch %d (reply %d, %d), want %d and %d unchanged", after.StateSeq, after.Epoch, out.StateSeq, out.Epoch, before.StateSeq, before.Epoch)
			}
		})
	}
}
