package harness_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster/harness"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The BUG-0808 renewal tests commit heartbeat_ms 250 instead of the default 100, so the quorum
// bound (three heartbeats) is 750 ms and so is the lease clock check (three heartbeats between
// renewer ticks, lease-renewer-cadence R2). The rule is the product's; only the scale is larger,
// so that the renewer keeps ample room on a loaded race-detector VM while a slow step of 1.5 s is
// still twice the bound: the slow-step test is red on a renewer inside the step by construction.
const (
	renewalHeartbeatMs = 250
	renewalBound       = 3 * renewalHeartbeatMs * time.Millisecond
	renewalSlowStep    = 1500 * time.Millisecond
	// renewalStepDown is the committed step_down_ms (the default): a lease outlives its last
	// renewal by up to this, and the quorum bound exists to stop serving well before it.
	renewalStepDown = 2000 * time.Millisecond
)

// renewalCluster forms three voters with an RF=2 database and one bucket, no failure detection,
// and heartbeat_ms renewalHeartbeatMs committed.
func renewalCluster(t *testing.T) *harness.Cluster {
	t.Helper()
	return renewalClusterWith(t, harness.Options{NoDetection: true, NoProbes: true})
}

// renewalClusterWith is renewalCluster with the harness options opts.
func renewalClusterWith(t *testing.T, opts harness.Options) *harness.Cluster {
	t.Helper()
	c := proofSetup(t, opts)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := c.Node(c.AwaitLeader(10*time.Second)).SetSetting(ctx, "heartbeat_ms", renewalHeartbeatMs); err != nil {
		t.Fatalf("committing heartbeat_ms %d: %v", renewalHeartbeatMs, err)
	}
	c.AwaitConverged(10 * time.Second)
	c.AwaitLeases(30 * time.Second)
	return c
}

// TestASlowSupervisorStepDoesNotStopANodeServing is BUG-0808: a supervisor step slowed by
// something that is not a quorum round (an activation ask, a voter reconcile, a disk sync on a
// loaded host) must not make a healthy node refuse NO_QUORUM or LEASE_LAPSED. Every step of
// every node waits 1.5 s, twice the quorum bound, while the network and the quorum rounds are
// untouched. A renewer that runs inside the step renews once per 1.5 s and the node refuses;
// a renewer of its own keeps every node servable and every write is acknowledged on the first
// try, with no retry (lease-renewer-cadence H1, R3; mutation M6 turns it red).
func TestASlowSupervisorStepDoesNotStopANodeServing(t *testing.T) {
	c := renewalCluster(t)
	for _, id := range c.IDs() {
		c.SlowSupervisor(id, renewalSlowStep)
	}
	deadline := time.Now().Add(3 * renewalSlowStep)
	for sample := 0; time.Now().Before(deadline); sample++ {
		for _, id := range c.IDs() {
			node := c.Node(id)
			if !node.LeaseValid() || node.QuorumLost() {
				t.Fatalf("sample %d: %s stopped serving while only its supervisor steps were slow: lease valid %v, quorum lost %v (background: %v)",
					sample, id, node.LeaseValid(), node.QuorumLost(), c.BackgroundErrors())
			}
		}
		if sample%25 == 0 {
			via := c.IDs()[(sample/25)%len(c.IDs())]
			id := fmt.Sprintf("slow-step-%d", sample)
			if _, err := c.Node(via).Insert(proofCtx(t), proofDB, proofBucket, bson.D{{Key: "_id_", Value: id}, {Key: "name", Value: "x"}}); err != nil {
				t.Fatalf("insert of %s through %s while only supervisor steps were slow: %v", id, via, err)
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestTheQuorumBoundStillHoldsWhenTheRoundFails is the safety half of BUG-0808: the renewer
// may only stamp a quorum round that a voter majority confirmed. When the round cannot finish
// (a node cut from the others) or takes longer than the bound, the node must report the quorum
// lost, and it must do so before step_down_ms has passed since the cut, which is what the
// bound is for: a lease can outlive its granting majority by up to step_down_ms. A renewer
// that stamped on a timer, without a round, never reports it and fails each case.
func TestTheQuorumBoundStillHoldsWhenTheRoundFails(t *testing.T) {
	cases := []struct {
		name string
		// cut isolates or slows one node and returns the node whose quorum must be lost.
		cut func(c *harness.Cluster, leader, follower string) string
	}{
		{"a follower cut from both voters", func(c *harness.Cluster, _, follower string) string {
			c.Pause(follower)
			return follower
		}},
		{"a leader cut from both voters", func(c *harness.Cluster, leader, _ string) string {
			c.Pause(leader)
			return leader
		}},
		{"a follower whose lease round outlasts the bound", func(c *harness.Cluster, leader, follower string) string {
			c.DelayChannel(follower, leader, transport.ChannelControl, renewalBound+renewalBound/2)
			return follower
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := renewalCluster(t)
			leader := c.AwaitLeader(10 * time.Second)
			var follower string
			for _, id := range c.IDs() {
				if id != leader {
					follower = id
					break
				}
			}
			watched := tc.cut(c, leader, follower)
			cutAt := time.Now()
			node := c.Node(watched)
			for !node.QuorumLost() {
				if time.Since(cutAt) >= renewalStepDown {
					t.Fatalf("%s still reports a fresh quorum %v after the cut: its lease could outlive the majority (bound %v)", watched, time.Since(cutAt), renewalBound)
				}
				time.Sleep(5 * time.Millisecond)
			}
			t.Logf("%s reported the quorum lost %v after the cut (bound %v)", watched, time.Since(cutAt).Round(time.Millisecond), renewalBound)
		})
	}
}
