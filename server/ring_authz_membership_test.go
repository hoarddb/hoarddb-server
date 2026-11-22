package server

import (
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
)

// TestRingAuthzMembershipThroughTheRunningServer: [GUARD on the real server a root user and a
// cluster-manager user are authorized and get the one typed refusal that a node without
// membership gives, with the two settings and the restart; every other user, a reader, a
// db-manager and one with no grants, gets one PERMISSION_DENIED text that names no node and
// no setting].
func TestRingAuthzMembershipThroughTheRunningServer(t *testing.T) {
	qs, _, root := ringComposeServer(t)
	const disabled = "membership changes are not available on this node yet: set cluster.enabled: true and restart; the join, promote, leave, remove-node and replace-node commands arrive with the cluster membership work"
	commands := []wire.RingAdminRequest{
		{Cmd: wire.AdminJoin, NodeID: "node-b", DataAddr: "node-b.example:4433"},
		{Cmd: wire.AdminPromote, NodeID: "node-b"},
		{Cmd: wire.AdminLeave, NodeID: "node-b"},
		{Cmd: wire.AdminRemoveNode, NodeID: "node-b"},
		{Cmd: wire.AdminReplaceNode, NodeID: "node-b", DataAddr: "node-b.example:4433"},
	}
	manager := ringAdminUser(t, qs, "clustermgr", Grant{Role: roleClusterManager, DB: "*"})
	for name, client := range map[string]*tcpServerClient{"root": root, "cluster-manager": manager} {
		for _, req := range commands {
			refusal := ringAdminRefusal(t, ringAdminFrame(t, client, req), cluster.CodeInvalidArgument)
			if refusal.Message != disabled {
				t.Errorf("%s, %s: refusal %q, want %q", name, req.Cmd, refusal.Message, disabled)
			}
		}
	}
	denied := map[string]*tcpServerClient{
		"cluster-manager@db": ringAdminUser(t, qs, "cmdb", Grant{Role: roleClusterManager, DB: ringComposeDB}),
		"root@db":            ringAdminUser(t, qs, "rootdb", Grant{Role: roleRoot, DB: ringComposeDB}),
		"nobody":             ringAdminUser(t, qs, "nobody"),
		"reader":             ringAdminUser(t, qs, "reader", Grant{Role: roleReader, DB: ringComposeDB}),
		"db-manager":         ringAdminUser(t, qs, "dbmgr", Grant{Role: roleDBManager, DB: "*"}),
	}
	var first string
	for name, client := range denied {
		for _, req := range commands {
			refusal := ringAdminRefusal(t, ringAdminFrame(t, client, req), cluster.CodePermissionDenied)
			if first == "" {
				first = refusal.Message
			}
			if refusal.Message != first {
				t.Errorf("%s, %s: denial %q differs from %q", name, req.Cmd, refusal.Message, first)
			}
			for _, leak := range []string{"node-b", "cluster.enabled", "allow_membership"} {
				if strings.Contains(refusal.Message, leak) {
					t.Errorf("%s, %s: the denial names %q: %s", name, req.Cmd, leak, refusal.Message)
				}
			}
		}
	}
}
