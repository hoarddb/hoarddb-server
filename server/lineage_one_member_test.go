package server

import (
	"testing"

	"github.com/hoarddb/hoarddb-server/transport"
)

// oneMemberBucket creates a database and a hash bucket through a client on a
// one-member cluster and returns the server and the bucket's state document.
func oneMemberBucket(t *testing.T, tc *testCluster, bucket string) (*Server, uint64) {
	t.Helper()
	core := tc.nodes[0].server.core
	if core.cluster == nil || core.cluster.Enabled() {
		t.Fatalf("the cluster is not a one-member one: cluster %v", core.cluster)
	}
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	return core, core.cluster.Epoch()
}

// A bucket a client creates on a one-member cluster takes the standalone door,
// and must still be stamped as the owner's own create is (BUG-0624).
func TestACreateOnAOneMemberClusterIsStampedWithAnIncarnation(t *testing.T) {
	tc := newTestCluster(t, 1, 1)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	core, epoch := oneMemberBucket(t, tc, "founder")

	state := lineageOf(t, core, testDatabase, "founder")
	if state.Incarnation == 0 {
		t.Errorf("incarnation = 0, want a minted one")
	}
	if state.Term != epoch {
		t.Errorf("term = %d, want the membership epoch %d", state.Term, epoch)
	}
	if !state.Opened {
		t.Errorf("the copy is not marked opened")
	}
	if _, opened, err := core.bucketLineage(testDatabase, "founder"); err != nil || !opened {
		t.Errorf("bucketLineage opened = %v, err %v, want opened", opened, err)
	}
}

// A drop and a re-create at the lone node are two buckets, told apart by their
// incarnations (BUG-0624; BUG-0500's drop record is keyed on the number).
func TestARecreateOnAOneMemberClusterGetsANewIncarnation(t *testing.T) {
	tc := newTestCluster(t, 1, 1)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	core, _ := oneMemberBucket(t, tc, "again")
	dropped := lineageOf(t, core, testDatabase, "again").Incarnation

	tc.ok(0, transport.OpDeleteBucket, map[string]any{"database": testDatabase, "name": "again"})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: "again", Type: BucketTypeHash})

	successor := lineageOf(t, core, testDatabase, "again").Incarnation
	if successor == 0 || successor == dropped {
		t.Errorf("successor incarnation = %d, dropped = %d, want two different non-zero numbers", successor, dropped)
	}
}

// The founder's bucket keeps the incarnation it was minted with when a second
// node joins, so the cluster tells it from any other bucket of its name.
func TestAOneMemberFoundersBucketKeepsItsIncarnationAcrossAJoin(t *testing.T) {
	tc := newTestClusterWith(t, 1, 1, autoRebalanceOffOnNodeZero)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	core, _ := oneMemberBucket(t, tc, "founder")
	before := lineageOf(t, core, testDatabase, "founder")

	ports := reserveNodePorts(t)
	joinLiveReplica(t, tc, ports, nil)

	after := lineageOf(t, core, testDatabase, "founder")
	if before.Incarnation == 0 || after.Incarnation != before.Incarnation || !after.Opened {
		t.Errorf("incarnation %d before the join, %d after (opened %v), want the same non-zero number", before.Incarnation, after.Incarnation, after.Opened)
	}
}
