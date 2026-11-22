package harness_test

import (
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster/harness"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The harness halves of spec lease-renewer-cadence section 7 (H2, H3): a lease round between two
// and three heartbeats long keeps the node serving. They run at heartbeat_ms 250 (renewalCluster),
// so a 600 ms round sits between two heartbeats (500 ms) and the tick bound of three (750 ms), with
// a margin of 150 ms. Today's serial renewer adds a heartbeat of sleep after the round and voids.
// They are integration evidence on a loaded VM; the renewer's unit tests are the deterministic proof.

const (
	// slowRound is the delay that makes one lease round about 600 ms long.
	slowRound = 600 * time.Millisecond
	// slowRoundWatch is how long every sample must find the lease valid.
	slowRoundWatch = 4 * time.Second
)

// leaseHoldsFor samples node's lease every 10 ms for d and fails at the first sample that finds it
// not valid.
func leaseHoldsFor(t *testing.T, c *harness.Cluster, id string, d time.Duration, what string) {
	t.Helper()
	deadline := time.Now().Add(d)
	for sample := 0; time.Now().Before(deadline); sample++ {
		if !c.Node(id).LeaseValid() {
			t.Fatalf("sample %d: %s stopped serving while %s (background: %v)", sample, id, what, c.BackgroundErrors())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestASlowFollowerLeaseRoundDoesNotStopItServing is H2: every LEASE_REQ the follower writes to the
// leader is delayed 600 ms, so each of its rounds takes between two and three heartbeats. Its lease
// must hold at every sample: the next round starts as the slow one ends, so ticks are about 600 ms
// apart, inside the three-heartbeat bound.
//
// The seed reporter is held (HoldSeedReports): its relay calls share the follower's control
// connection with the lease request, and the delay above would hold each relay for 600 ms too, so
// a LEASE_REQ queued behind one would wait its turn (about 600 ms) before its own 600 ms and the
// round would take about 1.2 s. That turn wait is candidate T2 of the spec, parked with the
// control-connection half (d3, OQ-2), and this test measures the renewer's cadence, not it.
func TestASlowFollowerLeaseRoundDoesNotStopItServing(t *testing.T) {
	c := renewalClusterWith(t, harness.Options{NoDetection: true, NoProbes: true, HoldSeedReports: true})
	leader := c.AwaitLeader(10 * time.Second)
	follower := otherThan(c, leader)
	c.DelayChannel(follower, leader, transport.ChannelControl, slowRound)
	leaseHoldsFor(t, c, follower, slowRoundWatch, "its lease rounds took about 600 ms")
}

// h3RaftTimeout keeps the cluster from electing while every Raft write of the leader is delayed
// 600 ms: a follower hears from the leader about every 900 ms, and the leader's lease of the
// library (half of this) still sees its followers' answers in time.
const h3RaftTimeout = 3 * time.Second

// TestASlowLeaderQuorumRoundDoesNotStopItServing is H3: every Raft write from the leader to both
// followers is delayed 600 ms, so its VerifyLeader, and with it its own lease round, takes up to
// about 600 ms. The fault layer delays only the dialer's writes and the leader dials its
// followers, so the leader's side is delayed; a response travels on the same connection. The
// Raft timeout is raised so the delay elects no one; the test fails if the leader changes.
func TestASlowLeaderQuorumRoundDoesNotStopItServing(t *testing.T) {
	c := renewalClusterWith(t, harness.Options{NoDetection: true, NoProbes: true, RaftTimeout: h3RaftTimeout})
	leader := c.AwaitLeader(10 * time.Second)
	for _, id := range c.IDs() {
		if id != leader {
			c.DelayChannel(leader, id, transport.ChannelRaft, slowRound)
		}
	}
	leaseHoldsFor(t, c, leader, slowRoundWatch, "its own quorum rounds took up to about 600 ms")
	if now := c.AwaitLeader(10 * time.Second); now != leader {
		t.Fatalf("the leader changed from %s to %s under the delay: raise h3RaftTimeout, the test proves nothing with an election", leader, now)
	}
}

// otherThan returns a member of c that is not id.
func otherThan(c *harness.Cluster, id string) string {
	for _, other := range c.IDs() {
		if other != id {
			return other
		}
	}
	return ""
}
