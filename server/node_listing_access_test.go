package server

import (
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// TestListNodesNeedsTheClusterRole: the node list carries every member's
// address, so it is answered to root and to cluster-manager, the role that runs
// the cluster, and refused to everyone else with the role to ask for. 'status'
// does not use it and stays open to every user.
func TestListNodesNeedsTheClusterRole(t *testing.T) {
	_, addr := newTCPEnabledServer(t, nil)
	root := dialTCPServer(t, addr)
	root.login()
	root.call(transport.OpCreateDB, &CreateDatabaseRequest{Name: "app"}, nil)
	const password = "node-listing-password-123"
	cases := []struct {
		user    string
		roles   []UserRole
		allowed bool
	}{
		{"operator", []UserRole{{Role: "cluster-manager", DB: "*"}}, true},
		{"app-writer", []UserRole{{Role: "writer", DB: "app"}}, false},
		{"all-reader", []UserRole{{Role: "reader", DB: "*"}}, false},
		{"accounts", []UserRole{{Role: "user-manager", DB: "*"}}, false},
	}
	for _, tc := range cases {
		var created UserMutationResponse
		root.call(transport.OpUserCreate, userCreateFor(t, tc.user, password, tc.roles), &created)
	}
	if frame := root.send(transport.OpListNodes, &NodeListRequest{}); frame.Opcode == transport.OpError {
		t.Fatalf("root's node list answered %s", frame.Opcode)
	}
	for _, tc := range cases {
		client := dialTCPServer(t, addr)
		client.loginAs(tc.user, password)
		frame := client.send(transport.OpListNodes, &NodeListRequest{})
		if tc.allowed {
			if frame.Opcode == transport.OpError {
				t.Errorf("%s (%v) could not list nodes: answered %s", tc.user, tc.roles, frame.Opcode)
			}
			continue
		}
		if frame.Opcode != transport.OpError {
			t.Errorf("%s (%v) listed the nodes; want PERMISSION_DENIED", tc.user, tc.roles)
			continue
		}
		var refusal ErrorResponse
		_ = bson.Unmarshal(frame.Payload, &refusal)
		if refusal.Code != int(transport.ErrPermissionDenied) || !strings.Contains(refusal.Message, "cluster-manager") {
			t.Errorf("%s's refusal is code %d %q; want PERMISSION_DENIED naming cluster-manager", tc.user, refusal.Code, refusal.Message)
		}
		if frame := client.send(transport.OpHealthCheck, &HealthRequest{}); frame.Opcode == transport.OpError {
			t.Errorf("%s cannot run status: %s", tc.user, frame.Payload)
		}
	}
}
