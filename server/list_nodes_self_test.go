package server

import (
	"testing"
	"time"
)

// TestListNodesIncludesTheAnsweringNode guards the legacy ring's LIST_NODES
// table: a member lists the whole cluster, itself included, with every
// fingerprint. The seed join that used to put the node in its own table was
// removed, so Start must record it.
func TestListNodesIncludesTheAnsweringNode(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	deadline := time.Now().Add(30 * time.Second)
	for {
		complete := true
		for _, n := range tc.nodes {
			known := n.server.core.KnownNodes()
			if len(known) != len(tc.nodes) {
				complete = false
			}
			for _, entry := range known {
				if entry.Fingerprint == "" {
					complete = false
				}
			}
		}
		if complete {
			break
		}
		if time.Now().After(deadline) {
			for i, n := range tc.nodes {
				t.Logf("node %d knows %d nodes", i, len(n.server.core.KnownNodes()))
			}
			t.Fatalf("not every node lists all %d members with a fingerprint", len(tc.nodes))
		}
		time.Sleep(100 * time.Millisecond)
	}
	for i, n := range tc.nodes {
		self := n.server.core.cluster.self.ID
		entry, ok := n.server.core.KnownNodes()[self]
		if !ok || entry.Fingerprint != n.server.core.fingerprint {
			t.Errorf("node %d does not list itself with its own fingerprint: %+v", i, entry)
		}
	}
}
