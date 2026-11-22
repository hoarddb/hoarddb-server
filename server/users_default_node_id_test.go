package server

import (
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/transport"
)

// TestUserChangesFollowTheRingIdentityWhenNodeIDIsDefaulted is BUG-0592: a
// cluster whose seeds are listed by address and whose nodes never set a node
// id (config.Load defaults it to the hostname, shared by every node on one
// host) must still let the master accept user changes and refuse the others
// naming the master.
func TestUserChangesFollowTheRingIdentityWhenNodeIDIsDefaulted(t *testing.T) {
	const defaultedHostname = "build-host"
	tc := newTestClusterWith(t, 3, 3, func(_ int, cfg *config.Config) {
		cfg.Server.NodeID = defaultedHostname
	})
	defer tc.stop()

	master, err := tc.nodes[0].server.core.authMaster()
	if err != nil {
		t.Fatalf("authMaster: %v", err)
	}
	masterIndex := -1
	for i, addr := range tc.addrs {
		if addr == master.ID {
			masterIndex = i
		}
	}
	if masterIndex < 0 {
		t.Fatalf("auth master %q is none of the nodes %v", master.ID, tc.addrs)
	}

	for index := range tc.nodes {
		frame := tc.frame(index, transport.OpUserCreate, clusterCreateRequest(t, "bug0592-"+tc.addrs[index], "user"+string(rune('a'+index))))
		if index == masterIndex {
			if frame.Opcode == transport.OpError {
				t.Fatalf("create user through the master %s was refused: %s", tc.addrs[index], frame.Payload)
			}
			continue
		}
		if frame.Opcode != transport.OpError {
			t.Fatalf("create user through non-master %s answered %s, want a refusal", tc.addrs[index], frame.Opcode)
		}
		refusal := string(frame.Payload)
		if !strings.Contains(refusal, "master:    "+master.ID) {
			t.Errorf("refusal on %s does not name the master %s: %s", tc.addrs[index], master.ID, refusal)
		}
		if !strings.Contains(refusal, "this node: "+tc.addrs[index]) || strings.Contains(refusal, defaultedHostname) {
			t.Errorf("refusal on %s should name the node's ring id, not the hostname: %s", tc.addrs[index], refusal)
		}
	}
}
