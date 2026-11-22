package server

import (
	"testing"

	"github.com/hoarddb/hoarddb-server/ring"
)

// peerDriverGuardCluster is a three-node cluster whose nodes 1 and 2 are
// stopped, so no gossip of theirs reaches node 0 and the test alone writes its
// peerDrivers table.
func peerDriverGuardCluster(t *testing.T) (*testCluster, *Cluster, [2]string) {
	t.Helper()
	tc := newTestCluster(t, 3, 3)
	tc.stopNodeKeepingData(1)
	tc.stopNodeKeepingData(2)
	c := tc.nodes[0].server.core.cluster
	// What the live gossip taught node 0 before the stop is not under test.
	c.peerDrivers.record(tc.addrs[1], "")
	c.peerDrivers.record(tc.addrs[2], "")
	return tc, c, [2]string{tc.addrs[1], tc.addrs[2]}
}

// A node speaks only for itself: a Driver on any entry of a gossip request but
// the sender's own is not stored (BUG-0709, rule 1 of sealed_peers.go).
func TestGossipDriverOfAnotherNodeIsNotTaken(t *testing.T) {
	tc, c, peers := peerDriverGuardCluster(t)
	defer tc.stop()

	c.learnStoreFromGossip(GossipRequest{NodeID: peers[0], Nodes: []Peer{
		{ID: peers[1], Driver: "192.0.2.99:7433"},
		{ID: peers[0], Driver: "192.0.2.10:7433"},
	}})

	if got := c.peerDrivers.address(peers[0]); got != "192.0.2.10:7433" {
		t.Errorf("the sender's own driver address = %q, want it stored", got)
	}
	if got := c.peerDrivers.address(peers[1]); got != "" {
		t.Errorf("a driver address the sender claimed for another node was stored: %q", got)
	}
}

// A peer that stops sending the field (a downgrade, or an older build) must
// not keep a stale address, on either side of an exchange.
func TestGossipWithoutADriverClearsTheStoredAddress(t *testing.T) {
	tc, c, peers := peerDriverGuardCluster(t)
	defer tc.stop()
	peer, _ := c.memberByID(peers[0])

	c.peerDrivers.record(peers[0], "192.0.2.10:7433")
	c.learnStoreFromGossip(GossipRequest{NodeID: peers[0], Nodes: []Peer{{ID: peers[0]}}})
	if got := c.peerDrivers.address(peers[0]); got != "" {
		t.Errorf("after a request without the field the address = %q, want it cleared", got)
	}

	c.peerDrivers.record(peers[0], "192.0.2.10:7433")
	c.learnStoreFromGossipResponse(peer, GossipResponse{Success: true, Self: Peer{ID: peers[0]}})
	if got := c.peerDrivers.address(peers[0]); got != "" {
		t.Errorf("after a response without the field the address = %q, want it cleared", got)
	}

	c.peerDrivers.record(peers[0], "192.0.2.10:7433")
	c.learnStoreFromGossipResponse(peer, GossipResponse{Success: true})
	if got := c.peerDrivers.address(peers[0]); got != "" {
		t.Errorf("after a response with no self record the address = %q, want it cleared", got)
	}
}

// The table holds current members only, however many nodes come and go.
func TestPeerDriversHoldCurrentMembersOnly(t *testing.T) {
	tc, c, peers := peerDriverGuardCluster(t)
	defer tc.stop()

	c.peerDrivers.record(peers[0], "192.0.2.10:7433")
	c.peerDrivers.record(peers[1], "192.0.2.11:7433")
	var kept []ring.Node
	for _, member := range c.Members() {
		if member.ID != peers[1] {
			kept = append(kept, member)
		}
	}
	if err := c.adoptMembership(c.Epoch()+1, kept); err != nil {
		t.Fatalf("adoptMembership: %v", err)
	}

	if got := c.peerDrivers.address(peers[1]); got != "" {
		t.Errorf("a removed member's address is still held: %q", got)
	}
	if got := c.peerDrivers.address(peers[0]); got != "192.0.2.10:7433" {
		t.Errorf("a remaining member's address = %q, want it kept", got)
	}
}
