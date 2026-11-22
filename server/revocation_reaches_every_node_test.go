package server

import (
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// served reports whether the connection still answers a request: a closed
// connection, or a refusal, is not service.
func (c *tcpServerClient) served() bool {
	c.t.Helper()
	payload, err := bson.Marshal(&ListDatabasesRequest{})
	if err != nil {
		c.t.Fatalf("marshal: %v", err)
	}
	_ = c.conn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := c.conn.Write(transport.NewFrame(transport.OpListDBs, payload).Encode()); err != nil {
		return false
	}
	frame, err := transport.DecodeFrame(c.br)
	if err != nil {
		return false
	}
	return frame.Opcode == transport.OpListDBs
}

// TestRevokingAUserEndsItsSessionsOnEveryNode: dropping or disabling a user,
// or replacing its password, ends that user's open connections on every node
// of the cluster, not only on the node that took the change. A role change
// does not: it reaches an open connection at its next login.
func TestRevokingAUserEndsItsSessionsOnEveryNode(t *testing.T) {
	cluster := newTestCluster(t, 2, 2)
	defer cluster.stop()
	master := clusterMasterIndex(t, cluster)
	cluster.ok(0, transport.OpCreateDB, &CreateDatabaseRequest{Name: "app"})
	const password = "revocation-password-123"

	userVersion := func() uint64 {
		var listing UserListResponse
		frame := cluster.ok(master, transport.OpUserList, &UserListRequest{})
		if err := bson.Unmarshal(frame.Payload, &listing); err != nil {
			t.Fatalf("decode user list: %v", err)
		}
		return listing.Version
	}
	disabled := false
	cases := []struct {
		name     string
		change   func(user string) (transport.Opcode, any)
		revoking bool
	}{
		{"drop", func(user string) (transport.Opcode, any) {
			return transport.OpUserDrop, &UserDropRequest{RequestID: "drop-" + user, Username: user}
		}, true},
		{"disable", func(user string) (transport.Opcode, any) {
			return transport.OpUserAlter, &UserAlterRequest{RequestID: "disable-" + user, Username: user, Enabled: &disabled, Version: userVersion()}
		}, true},
		{"password", func(user string) (transport.Opcode, any) {
			material, err := NewCredential("a-replacement-password-123", DefaultArgon2Params())
			if err != nil {
				t.Fatalf("NewCredential: %v", err)
			}
			credential := &UserCredential{Algo: material.Algo, Salt: material.Salt, Hash: material.Hash, Params: material.Params}
			return transport.OpUserAlter, &UserAlterRequest{RequestID: "password-" + user, Username: user, Credentials: credential, Version: userVersion()}
		}, true},
		{"grant", func(user string) (transport.Opcode, any) {
			return transport.OpUserGrant, &UserGrantRequest{RequestID: "grant-" + user, Username: user, Role: UserRole{Role: "writer", DB: "app"}}
		}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			user := fmt.Sprintf("%s-user", tc.name)
			cluster.ok(master, transport.OpUserCreate, userCreateFor(t, user, password, []UserRole{{Role: "reader", DB: "app"}}))
			sessions := make([]*tcpServerClient, len(cluster.driverAddrs))
			for i, addr := range cluster.driverAddrs {
				waitForLogin(t, addr, user, password)
				sessions[i] = dialTCPServer(t, addr)
				sessions[i].loginAs(user, password)
				if !sessions[i].served() {
					t.Fatalf("node %d does not serve %s before the change", i+1, user)
				}
			}

			op, req := tc.change(user)
			cluster.ok(master, op, req)

			for i, session := range sessions {
				role := "the node that took the change"
				if i != master {
					role = "another node"
				}
				if !tc.revoking {
					time.Sleep(time.Second)
					if !session.served() {
						t.Errorf("a %s ended %s's session on %s; a role change applies at the next login", tc.name, user, role)
					}
					continue
				}
				deadline := time.Now().Add(10 * time.Second)
				for session.served() {
					if time.Now().After(deadline) {
						t.Errorf("after a %s, %s's open session on %s is still served 10s later", tc.name, user, role)
						break
					}
					time.Sleep(100 * time.Millisecond)
				}
			}
		})
	}
}

// TestNodesOwnRootSessionSurvivesTheFirstPush: each node of a new cluster
// creates its own root account at its first start, and the harness logs in to
// every node with it straight away. When the master's config replaces that
// account with the cluster's root account of the same name, those sessions
// keep working: it is not a revocation.
func TestNodesOwnRootSessionSurvivesTheFirstPush(t *testing.T) {
	cluster := newTestCluster(t, 2, 2)
	defer cluster.stop()
	master := clusterMasterIndex(t, cluster)
	masterRoot, ok := cluster.nodes[master].server.core.auth.recordForTest(testRootUser)
	if !ok {
		t.Fatalf("the master holds no %s account", testRootUser)
	}
	for i, node := range cluster.nodes {
		if i == master {
			continue
		}
		deadline := time.Now().Add(30 * time.Second)
		for {
			record, ok := node.server.core.auth.recordForTest(testRootUser)
			if ok && record.CreatedAt.Equal(masterRoot.CreatedAt) {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("node %d never took the master's %s account", i+1, testRootUser)
			}
			time.Sleep(50 * time.Millisecond)
		}
		if frame := cluster.frame(i, transport.OpListDBs, &ListDatabasesRequest{}); frame.Opcode == transport.OpError {
			t.Fatalf("node %d's root session, opened before the first push, was refused after it: %s", i+1, frame.Payload)
		}
	}
}

// TestIdleSessionOnAnotherNodeIsClosedWhenTheUserIsDropped: a member closes a
// dropped user's connections as it applies the change, so an idle connection
// is closed without waiting for the client to send anything.
func TestIdleSessionOnAnotherNodeIsClosedWhenTheUserIsDropped(t *testing.T) {
	cluster := newTestCluster(t, 2, 2)
	defer cluster.stop()
	master := clusterMasterIndex(t, cluster)
	other := 1 - master
	const password = "idle-session-password-123"
	cluster.ok(master, transport.OpUserCreate, userCreateFor(t, "idle-user", password, nil))
	addr := cluster.driverAddrs[other]
	waitForLogin(t, addr, "idle-user", password)
	session := dialTCPServer(t, addr)
	session.loginAs("idle-user", password)

	cluster.ok(master, transport.OpUserDrop, &UserDropRequest{RequestID: "drop-idle-user", Username: "idle-user"})

	// Nothing is sent: the read ends only if the node closes the connection.
	_ = session.conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	_, err := session.br.ReadByte()
	var netErr net.Error
	if err == nil || (errors.As(err, &netErr) && netErr.Timeout()) {
		t.Fatalf("the dropped user's idle connection on another node was still open 10s after the drop (read: %v)", err)
	}
}
