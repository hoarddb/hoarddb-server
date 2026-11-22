package server

import (
	"context"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// TestSingleNodeQuorumPasses is the case that breaks every local dev box when
// it is wrong: ⌊1/2⌋+1 = 1, itself, so a standalone node always has a quorum.
func TestSingleNodeQuorumPasses(t *testing.T) {
	s := userTestServer(t)
	if err := s.requireQuorum(context.Background()); err != nil {
		t.Fatalf("standalone requireQuorum = %v, want nil", err)
	}
}

// clusterMasterIndex returns the index of the node that owns the reserved
// auth key.
func clusterMasterIndex(t *testing.T, tc *testCluster) int {
	t.Helper()
	master, err := tc.nodes[0].server.core.authMaster()
	if err != nil {
		t.Fatalf("authMaster: %v", err)
	}
	for i, n := range tc.nodes {
		if n.cfg.Server.NodeID == master.ID {
			return i
		}
	}
	t.Fatalf("no node owns the auth master key")
	return -1
}

func clusterCreateRequest(t *testing.T, requestID, username string) *UserCreateRequest {
	t.Helper()
	credential, err := NewCredential("quorum-password-123", DefaultArgon2Params())
	if err != nil {
		t.Fatalf("NewCredential: %v", err)
	}
	return &UserCreateRequest{
		RequestID:  requestID,
		Username:   username,
		Credential: UserCredential{Algo: credential.Algo, Salt: credential.Salt, Hash: credential.Hash, Params: credential.Params},
		Roles:      []UserRole{{Role: "reader", DB: "app"}},
	}
}

// TestNonMasterRefusesMutation is the step-8 cluster test: on one node the
// answer is always "you are the master", so the refusal needs a real cluster.
func TestNonMasterRefusesMutation(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()

	masterIndex := clusterMasterIndex(t, tc)
	follower := (masterIndex + 1) % 3
	master, err := tc.nodes[masterIndex].server.core.authMaster()
	if err != nil {
		t.Fatalf("authMaster: %v", err)
	}

	frame := tc.frame(follower, transport.OpUserCreate, clusterCreateRequest(t, "step8-follower", "bob"))
	if frame.Opcode != transport.OpError {
		t.Fatalf("a follower accepted a user mutation (%s), want a refusal", frame.Opcode)
	}
	var errResp ErrorResponse
	_ = bson.Unmarshal(frame.Payload, &errResp)
	if !strings.Contains(errResp.Message, "cluster master") {
		t.Fatalf("refusal %q does not name the master", errResp.Message)
	}
	if !strings.Contains(errResp.Message, master.Addr) {
		t.Fatalf("refusal %q does not carry the master's address %s", errResp.Message, master.Addr)
	}
}

// TestNoQuorumRefusesMutation stops a majority of the members and requires the
// master to refuse rather than commit a config a healed partition would
// discard. It also asserts the error names how many it reached.
func TestNoQuorumRefusesMutation(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()

	masterIndex := clusterMasterIndex(t, tc)
	for i, n := range tc.nodes {
		if i == masterIndex {
			continue
		}
		if err := n.server.Stop(); err != nil {
			t.Fatalf("stop node %d: %v", i, err)
		}
		_ = n.client.Close()
		n.client = nil
	}

	frame := tc.frame(masterIndex, transport.OpUserCreate, clusterCreateRequest(t, "step8-quorum", "carol"))
	if frame.Opcode != transport.OpError {
		t.Fatalf("master accepted a mutation with no quorum (%s), want a refusal", frame.Opcode)
	}
	var errResp ErrorResponse
	_ = bson.Unmarshal(frame.Payload, &errResp)
	if !strings.Contains(errResp.Message, ErrNoQuorum.Error()) {
		t.Fatalf("refusal %q does not name %q", errResp.Message, ErrNoQuorum)
	}
	if !strings.Contains(errResp.Message, "could be reached") {
		t.Fatalf("refusal %q does not say how many members it reached", errResp.Message)
	}
}

// TestQuorumAcceptsTwoOfThreeReachable is the mis-count that turns user
// management off cluster-wide if the arithmetic is wrong: a strict majority of
// 3 is 2, so one stopped member must not refuse the mutation.
func TestQuorumAcceptsTwoOfThreeReachable(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()

	masterIndex := clusterMasterIndex(t, tc)
	for i, n := range tc.nodes {
		if i == masterIndex {
			continue
		}
		if err := n.server.Stop(); err != nil {
			t.Fatalf("stop node %d: %v", i, err)
		}
		_ = n.client.Close()
		n.client = nil
		break
	}

	reachable, needed, total := tc.nodes[masterIndex].server.core.cluster.QuorumReachable(context.Background())
	if total != 3 || needed != 2 || reachable != 2 {
		t.Fatalf("QuorumReachable = reachable %d / needed %d of %d, want 2/2 of 3", reachable, needed, total)
	}

	frame := tc.frame(masterIndex, transport.OpUserCreate, clusterCreateRequest(t, "step8-twoofthree", "dave"))
	if frame.Opcode == transport.OpError {
		t.Fatalf("the master refused with two of three members reachable: %s", frame.Payload)
	}
}
