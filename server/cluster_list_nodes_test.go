package server

import "testing"

// TestListNodesReportsTheMemberCountNotTheKnownPeerCount is BUG-0708: the node
// list answers the known-peer table, a cache of who has gossiped, so its length
// is not the member count. Node 1's table is emptied to stand for a node that
// has not heard from anyone, and a member is added that has never gossiped to
// it; the count must still be the epoch record's.
func TestListNodesReportsTheMemberCountNotTheKnownPeerCount(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	node := tc.nodes[0].server.core
	node.knownNodesMu.Lock()
	node.knownNodes = make(map[string]*NodeInfo)
	node.knownNodesMu.Unlock()

	resp, _ := listNodes(t, tc, 0, false)
	if len(resp.Nodes) != 0 || resp.MemberCount != 3 || resp.Standalone {
		t.Fatalf("with an empty peer table over 3 members, got %d rows, MemberCount %d, Standalone %v; want 0 rows, 3, false",
			len(resp.Nodes), resp.MemberCount, resp.Standalone)
	}

	fabricated := fabricatedMemberAddr(t)
	if err := clusterOf(tc, 0).proposeJoin(commandContext(t), ringNodeAt(fabricated)); err != nil {
		t.Fatalf("joining %s: %v", fabricated, err)
	}
	resp, _ = listNodes(t, tc, 0, false)
	if resp.MemberCount != 4 {
		t.Errorf("after a fourth member joined that has not gossiped, MemberCount = %d, want 4", resp.MemberCount)
	}
	if want := clusterOf(tc, 0).Epoch(); resp.Epoch != want {
		t.Errorf("Epoch = %d, want the record's %d", resp.Epoch, want)
	}
}
