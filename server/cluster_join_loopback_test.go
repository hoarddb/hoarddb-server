package server

import (
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/transport"
)

// TestJoinFromLoopbackNodeIsRefused is SEC-1 F1: a node whose ring address is
// loopback would commit an epoch naming itself as 127.0.0.1:<port>, which the
// remote node refuses to adopt every two seconds, leaving the membership
// stuck (remove-node then fails for want of a majority). The join is refused
// before any membership change, naming the setting that fixes it.
func TestJoinFromLoopbackNodeIsRefused(t *testing.T) {
	tc := newTestCluster(t, 1, 2)
	defer tc.stop()

	cluster := tc.nodes[0].server.core.cluster
	epochBefore := cluster.Epoch()

	respFrame := tc.frame(0, transport.OpClusterAdd, &ClusterAddRequest{Address: "remote@192.0.2.9:4433"})
	if respFrame.Opcode != transport.OpError {
		t.Fatalf("join of a remote address from a loopback node answered %s; want a refusal", respFrame.Opcode)
	}
	for _, want := range []string{"HOARDDB_LISTEN", "127.0.0.1", "192.0.2.9:4433", "membership.json", "HOARDDB_SEED_NODES"} {
		if !strings.Contains(string(respFrame.Payload), want) {
			t.Errorf("refusal %q does not mention %q", respFrame.Payload, want)
		}
	}
	if cluster.Epoch() != epochBefore {
		t.Errorf("epoch moved from %d to %d on a refused join", epochBefore, cluster.Epoch())
	}
}
