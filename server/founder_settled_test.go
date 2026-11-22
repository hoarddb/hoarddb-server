package server

import (
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// Owner ruling 52: founders record Settled at the first epoch when they
// bootstrap. A founder holds every copy the first epoch placed and no change
// has moved one, so it is settled there by construction; without the record
// join_freeze.go:settledFloor reads 0 on a cluster that never changed
// membership, and ring 0 lies outside the history (the 10a.7 builder's
// finding: one unreachable member then refused every create and drop of a
// name the owner does not hold).

// ownSettledFloor is c's settled floor over its current members.
func ownSettledFloor(c *Cluster) uint64 {
	_, members := c.epochMembers()
	return settledFloor(c.self.ID, c.settled.Load(), c.settledOfSnapshot(), members)
}

// TestFoundersAreSettledAtTheFirstEpoch: on a fresh three-node cluster that
// never changed membership, every member's own Settled epoch and its settled
// floor over the members is the first epoch, and both survive a restart.
// Guard: record nothing at the founders' confirmation — every floor is 0.
func TestFoundersAreSettledAtTheFirstEpoch(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	settledEverywhere := func() bool {
		for _, node := range tc.nodes {
			c := node.server.core.cluster
			if c.settled.Load() != InitialEpoch || ownSettledFloor(c) != InitialEpoch {
				return false
			}
		}
		return true
	}
	report := func(when string) {
		t.Helper()
		for index, node := range tc.nodes {
			c := node.server.core.cluster
			t.Errorf("%s: node %d is settled at %d, its floor over the members is %d; want both %d",
				when, index+1, c.settled.Load(), ownSettledFloor(c), InitialEpoch)
		}
	}
	if !eventually(settledEverywhere) {
		report("a fresh cluster")
		t.Fatal("a founder of a cluster that never changed membership is not settled at the first epoch (ruling 52)")
	}
	tc.restartNode(1)
	if !eventually(settledEverywhere) {
		report("after a restart")
	}
}

// TestAJoinerForgetsTheViewItStartedWith: a newcomer starts on a provisional
// view of the first epoch — its seeds and itself — that no member ever held,
// and its history records it. Its join is its first adoption, and from then
// the history holds no record of that epoch that lists the joiner: with
// founders settled at the first epoch (ruling 52), the joiner's settled floor
// is that epoch, and a ring built from its provisional view would make a name
// it owns read as unmoved since the floor (R8-D (b)), passing over the members
// that hold it. The record it may hold is the cluster's, carried in the answer
// it adopted its join from (on-demand pull spec §6.7, O-7 (b); owner ruling
// 2026-10-20). The founders keep their record of the first epoch. Guard: keep
// the provisional record — the joiner's first epoch lists the joiner.
func TestAJoinerForgetsTheViewItStartedWith(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	ports := reserveNodePorts(t)
	joiner := joinNewcomer(t, tc, 0, ports)
	defer joiner.stop()
	joined := joiner.node.server.core.cluster
	if !eventually(func() bool { return !joined.bootstrapped.Load() && joined.Epoch() > InitialEpoch }) {
		t.Fatal("setup: the newcomer never adopted its join")
	}
	if members, recorded := joined.membersAt(InitialEpoch); recorded && nodeListed(members, joined.self.ID) {
		t.Errorf("the joiner's history records the first epoch as %v, a view it started with and no member held", memberIDsOf(members))
	}
	for index := range tc.nodes {
		members, recorded := clusterOf(tc, index).membersAt(InitialEpoch)
		if !recorded || len(members) != len(tc.nodes) || nodeListed(members, joined.self.ID) {
			t.Errorf("founder %d's record of the first epoch is %v (%v), want the three founders", index+1, memberIDsOf(members), recorded)
		}
	}
}

// TestAMemberDownDoesNotRefuseNewNamesOnAClusterThatNeverChanged: three
// founders at factor 2, never a membership change, one member stopped. At the
// owner of a new name, a create succeeds and a drop of a name no member holds
// answers that the bucket is not found: the names' sets are unchanged since
// the floor, the first epoch, so the unheard member is passed over (R8-D (b)).
// Guard: record nothing at the founders' confirmation — the floor is 0, which
// is outside the history, and both are refused with retry naming the stopped
// member.
func TestAMemberDownDoesNotRefuseNewNamesOnAClusterThatNeverChanged(t *testing.T) {
	tc := newTestCluster(t, 3, 2)
	defer tc.stop()
	const down = 2
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitListedBy(t, tc, testDatabase, 0, 1, 2)
	placed := ringWith(t, 2, tc.addrs...)
	running := func(name string) bool { return ownerOn(placed, name) != tc.addrs[down] }
	created := bucketNamed(t, "fresh", running)
	absent := bucketNamed(t, "absent", func(name string) bool { return running(name) && name != created })
	// Waited for, not required: the assertions below say what is wrong when
	// the floor never reaches the first epoch.
	eventually(func() bool {
		return ownSettledFloor(clusterOf(tc, 0)) == InitialEpoch && ownSettledFloor(clusterOf(tc, 1)) == InitialEpoch
	})
	tc.stopNodeKeepingData(down)
	owner := func(name string) int { return nodeIndexByAddr(t, tc.addrs, ownerOn(placed, name)) }

	frame := tc.frame(owner(created), transport.OpCreateBucket, bson.M{"database": testDatabase, "name": created, "type": string(BucketTypeHash),
		"write_concern": "one"})
	if frame.Opcode == transport.OpError {
		t.Errorf("a create of a new name with one member down was refused: %s", refusalOf(t, frame).Message)
	}
	frame = tc.frame(owner(absent), transport.OpDeleteBucket, bson.M{"database": testDatabase, "name": absent, "write_concern": "one"})
	if frame.Opcode != transport.OpError {
		t.Fatalf("a drop of a name no member holds was answered %s", frame.Opcode)
	}
	if refused := refusalOf(t, frame); refused.Code != int(transport.ErrNotFound) {
		t.Errorf("a drop of a name no member holds, with one member down, answered code %d, want not found: %s", refused.Code, refused.Message)
	}
}

// TestAFounderThatLostItsSettledRecordSettlesAgainAtStart (the verifier of
// part 2, fix 2a): persistSettled writes the record unsynced and once, so a
// founder can lose it — a power loss before a later sync, or a crash between
// confirming its view and recording Settled — and restart confirmed at the
// first epoch with Settled 0. Nothing re-settled it: the confirmation runs only
// while bootstrapped, and resumeSettling needs the ring before the first epoch.
// Here node 2's record is zeroed and the node restarted; it is settled at the
// first epoch again. Guard: no founding settle at start — it stays at 0.
func TestAFounderThatLostItsSettledRecordSettlesAgainAtStart(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	lost := clusterOf(tc, 1)
	if !eventually(func() bool { return lost.settled.Load() == InitialEpoch }) {
		t.Fatal("setup: node 2 never settled at the first epoch")
	}
	lost.settled.Store(0)
	if err := lost.persistSettled(); err != nil {
		t.Fatalf("setup: zeroing node 2's settled record: %v", err)
	}
	tc.restartNode(1)
	restarted := clusterOf(tc, 1)
	if !eventually(func() bool { return restarted.settled.Load() == InitialEpoch }) {
		t.Fatalf("a founder restarted without its settled record is settled at %d, want the first epoch %d (ruling 52)",
			restarted.settled.Load(), InitialEpoch)
	}
}
