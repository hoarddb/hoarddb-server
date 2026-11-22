package server

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/transport"
)

// statusMembershipOf asks node for its status and returns the membership it
// reports.
func statusMembershipOf(t *testing.T, tc *testCluster, node *testNode) string {
	t.Helper()
	frame := tc.frameOn(node, transport.OpHealthCheck, HealthRequest{})
	if frame.Opcode == transport.OpError {
		t.Fatalf("status through %s was refused: %s; it is served while a node waits for 'join'", node.addr, string(frame.Payload))
	}
	var health HealthResponse
	if err := bson.Unmarshal(frame.Payload, &health); err != nil {
		t.Fatalf("decoding %s's status: %v", node.addr, err)
	}
	return health.Membership
}

// TestStatusSaysWhetherTheNodeHasBeenAdded: status is where a node says it
// has not been added (confirmation pass §9.3): "not added yet" from a node
// started to join, "confirmed" once it has adopted the join, and nothing from
// a standalone server.
func TestStatusSaysWhetherTheNodeHasBeenAdded(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	ports := reserveNodePorts(t)
	newcomer := startJoinTarget(t, tc, ports, tc.token)
	if got := statusMembershipOf(t, tc, newcomer); got != MembershipNotAddedYet {
		t.Errorf("status of a node waiting for 'join' says membership %q, want %q", got, MembershipNotAddedYet)
	}
	if got := statusMembershipOf(t, tc, tc.nodes[0]); got != MembershipConfirmed {
		t.Errorf("status of a founder says membership %q, want %q", got, MembershipConfirmed)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := clusterOf(tc, 0).proposeJoin(ctx, ports.ringNode()); err != nil {
		t.Fatalf("proposeJoin: %v", err)
	}
	deadline := time.Now().Add(20 * time.Second)
	for statusMembershipOf(t, tc, newcomer) != MembershipConfirmed {
		if time.Now().After(deadline) {
			t.Fatalf("status of the newcomer never said %q after its join", MembershipConfirmed)
		}
		time.Sleep(50 * time.Millisecond)
	}

	health, err := newTestServer(t).HealthCheck()
	if err != nil {
		t.Fatalf("a standalone server's health check: %v", err)
	}
	if health.Membership != "" {
		t.Errorf("a standalone server reports membership %q; it has no member list to confirm", health.Membership)
	}
}

// TestTheClusterFixtureWaitsForConfirmation: newTestCluster hands back a
// cluster only when no node is still bootstrapped, so the first request of
// every cluster test is never answered "not added yet". Confirmation is held
// back for a few seconds by a seam: left alone it lands in the same gossip
// exchange that makes the nodes known to each other, and a fixture that waited
// only for that would pass by luck.
func TestTheClusterFixtureWaitsForConfirmation(t *testing.T) {
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseAll := func() { releaseOnce.Do(func() { close(release) }) }
	hold := func(string) { <-release }
	confirmMembershipHook.Store(&hold)
	defer confirmMembershipHook.Store(nil)
	defer releaseAll()
	time.AfterFunc(3*time.Second, releaseAll)

	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	for index := range tc.nodes {
		if clusterOf(tc, index).bootstrapped.Load() {
			t.Errorf("newTestCluster returned with node %d still bootstrapped", index+1)
		}
	}
}

// TestAFounderWaitingOnAnAbsentFounderSaysWhichAndHowOut is D1 of the
// join-freeze verification: on a new cluster's first boot a founder serves
// nothing until every founder has answered. With one founder never started
// the others are not newcomers, and "run 'join' on a member" is the wrong
// advice: the refusal and status name the founder that has not answered and
// the two ways out — start it, or 'remove-node' it — and removing it lets the
// others confirm.
func TestAFounderWaitingOnAnAbsentFounderSaysWhichAndHowOut(t *testing.T) {
	const absent = 2
	release := make(chan struct{})
	defer close(release)
	held := func(inner peerConn) peerConn { return &heldAllConn{peerConn: inner, release: release} }
	tc := startTestClusterTuned(t, 3, 3, nil, func(index int, c *Cluster) {
		// Nothing reaches the absent founder or leaves it before it is stopped.
		absentID := c.s.config.HashRing.SeedNodes[absent]
		for _, seed := range c.s.config.HashRing.SeedNodes {
			if seed != c.self.ID && (index == absent || seed == absentID) {
				wrapPeerConn(c.peerFor(ring.Node{ID: seed, Addr: seed}), held)
			}
		}
	})
	defer tc.stop()
	_ = tc.nodes[absent].client.Close()
	tc.nodes[absent].client = nil
	if err := tc.nodes[absent].server.Stop(); err != nil {
		t.Fatalf("stopping founder %d: %v", absent+1, err)
	}
	tc.nodes[absent].server = nil
	absentID, presentID := tc.addrs[absent], tc.addrs[1]

	waiting := clusterOf(tc, 0)
	deadline := time.Now().Add(20 * time.Second)
	for !agreedWith(waiting, presentID) {
		if time.Now().After(deadline) {
			t.Fatalf("test setup: founder 1 never heard founder 2")
		}
		time.Sleep(20 * time.Millisecond)
	}

	frame := tc.frameOn(tc.nodes[0], transport.OpCreateDB, CreateDatabaseRequest{Name: newcomerDatabase})
	if frame.Opcode != transport.OpError {
		t.Fatalf("a founder answered a create database while founder %d has never answered: %s", absent+1, string(frame.Payload))
	}
	var refused ErrorResponse
	if err := bson.Unmarshal(frame.Payload, &refused); err != nil {
		t.Fatalf("decoding the refusal: %v", err)
	}
	if strings.Contains(refused.Message, "'join ") || !strings.Contains(refused.Message, absentID) ||
		!strings.Contains(refused.Message, "remove-node") || strings.Contains(refused.Message, presentID) {
		t.Errorf("a founder waiting on %s refuses with %q; want %s named as not answered, 'remove-node' offered, no 'join' advice",
			absentID, refused.Message, absentID)
	}
	health, err := tc.nodes[0].server.core.HealthCheck()
	if err != nil {
		t.Fatalf("health check: %v", err)
	}
	if health.Membership != MembershipWaitingForFounders || !strings.Contains(health.MembershipDetail, absentID) ||
		strings.Contains(health.MembershipDetail, presentID) {
		t.Errorf("status says membership %q (%q); want %q naming only %s", health.Membership, health.MembershipDetail,
			MembershipWaitingForFounders, absentID)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := waiting.proposeRemoval(ctx, absentID); err != nil {
		t.Fatalf("remove-node %s: %v", absentID, err)
	}
	deadline = time.Now().Add(20 * time.Second)
	for index := 0; index < absent; index++ {
		for clusterOf(tc, index).bootstrapped.Load() {
			if time.Now().After(deadline) {
				t.Fatalf("founder %d is still bootstrapped after the absent founder was removed", index+1)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
}
