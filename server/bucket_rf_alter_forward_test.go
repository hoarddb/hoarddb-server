package server

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// BUG-0643: an alter the owner refuses for a reason a retry clears (R4, R7, R8,
// R11) must reach the client in the same wire class whichever node it asked.
// These tests send the alter to node 2, which does not own the bucket, so it
// forwards, and hold the answer to the one the owner gives its own client
// (TestEveryAlterRefusalTravelsInItsWireClass): UNAVAILABLE, not_applied, the
// owner's sentence unchanged.

// requireForwardedAlterRefusal asks a non-owner to alter and holds the answer
// to the owner's own: the same class, outcome and sentence.
func requireForwardedAlterRefusal(t *testing.T, tc *testCluster, req AlterBucketRequest, want ...string) {
	t.Helper()
	viaOwner := requireUnavailableNotApplied(t, alterFrame(t, tc, 0, req), want...)
	viaCoordinator := requireUnavailableNotApplied(t, alterFrame(t, tc, 1, req), want...)
	if viaCoordinator.Message != viaOwner.Message {
		t.Errorf("the owner's sentence changed on the forward:\n owner:       %q\n coordinator: %q", viaOwner.Message, viaCoordinator.Message)
	}
	requireNoAlterLogged(t, tc, req.Name)
}

func TestAnAlterRefusedForAMemberWithoutTheFeatureIsUnavailableThroughANonOwner(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitForAlterFeature(t, tc)
	bucket := bucketOwnedBy(t, tc, 0, "sessions")
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	owner := tc.nodes[0].server.core.cluster
	markStale(owner, tc.addrs[2])
	defer owner.recordMemberFeatures(tc.addrs[2], clusterFeatures())
	requireForwardedAlterRefusal(t, tc, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 2},
		"does not change factors", "Upgrade "+tc.addrs[2])
}

func TestAnAlterRefusedForAnUnfinishedPlacementPassIsUnavailableThroughANonOwner(t *testing.T) {
	tc := newTestCluster(t, 4, 1)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitForAlterFeature(t, tc)
	removed := tc.addrs[3]
	stopNode(t, tc, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := clusterOf(tc, 1).proposeRemoval(ctx, removed); err != nil {
		t.Fatalf("removing node 4: %v", err)
	}
	owner := clusterOf(tc, 0)
	epoch := clusterOf(tc, 1).Epoch()
	pollUntil(t, "nodes 1 and 2 adopt the removal and finish their placement pass", func() bool {
		return owner.Epoch() == epoch && owner.placementPassFinished() && clusterOf(tc, 1).placementPassFinished()
	})
	bucket := bucketOwnedBy(t, tc, 0, "sessions")
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})

	// Only the owner's pass is unfinished: the coordinator's own R7 check must
	// pass, so the refusal can only be the owner's, forwarded.
	setPassFinished(owner, epoch-1)
	defer setPassFinished(owner, epoch)
	requireForwardedAlterRefusal(t, tc, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 2},
		"is still recording which buckets the last membership change moved")
}

func TestAnAlterRefusedForAPendingCopyIsUnavailableThroughANonOwner(t *testing.T) {
	// Auto-rebalance off: the pending-copy worker would otherwise land the copy
	// to node 2 between the two alters, clear its awaiting record, and make the
	// second alter correctly acceptable instead of refused.
	tc := newTestClusterWith(t, 3, 1, func(_ int, cfg *config.Config) { cfg.Server.AutoRebalance = false })
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitForAlterFeature(t, tc)
	bucket := bucketOwnedBy(t, tc, 0, "sessions")
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	alterAccepted(t, alterFrame(t, tc, 0, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 2}))

	req := AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 3}
	viaOwner := requireUnavailableNotApplied(t, alterFrame(t, tc, 0, req), "is still copying to", "(to 2)", "or lower replication_factor to 1")
	viaCoordinator := requireUnavailableNotApplied(t, alterFrame(t, tc, 1, req), "is still copying to", "(to 2)", "or lower replication_factor to 1")
	if viaCoordinator.Message != viaOwner.Message {
		t.Errorf("the owner's sentence changed on the forward:\n owner:       %q\n coordinator: %q", viaOwner.Message, viaCoordinator.Message)
	}
}

func TestAnAlterRefusedForAnUncoveredDecreaseIsUnavailableThroughANonOwner(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitForAlterFeature(t, tc)
	lowered := bucketOwnedBy(t, tc, 0, "lowered")
	createBucketAt(tc, 0, testDatabase, lowered, 3)
	waitForServing(t, tc, testDatabase, lowered, 3)
	owner := tc.nodes[0].server.core
	failNextApplyOn(t, owner.kv, anySeq)
	tc.frame(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: lowered, Key: "failed", Data: map[string]interface{}{"n": 1}})
	store.ApplyFaultHook.Store(nil)
	putDocument(tc, 0, testDatabase, lowered, "after")
	if ownerCopy, err := owner.cluster.answerCopy(ReplBucketRef{Database: testDatabase, Bucket: lowered}); err != nil || !ownerCopy.Owes {
		t.Fatalf("setup: the owner's copy is %+v (%v), want it owing the apply its store failed", ownerCopy, err)
	}
	requireForwardedAlterRefusal(t, tc, AlterBucketRequest{Database: testDatabase, Name: lowered, ReplicationFactor: 1},
		fmt.Sprintf("bucket %q cannot be lowered from replication_factor 3 to 1", lowered),
		fmt.Sprintf("Run 'show buckets' on %s and retry when the row reads 3/3.", tc.addrs[0]))
}
