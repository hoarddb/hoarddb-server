package server

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/transport"
)

// R7, R8 and the supersede (bucket-replication-rev8-spec §4.3, §5.2 step 4,
// §6.3; owner R9-Q1; build doc step 2.7).

// setPassFinished sets what c's write path reads as the epoch its placement
// pass last finished for: below the current epoch it is the state a pass that
// is still running leaves.
func setPassFinished(c *Cluster, epoch uint64) {
	c.placementMu.Lock()
	defer c.placementMu.Unlock()
	c.passFinished = epoch
	c.noteUnsettledLocked()
}

// TestAnAlterIsRefusedUntilThePlacementPassFinishes is T-162 and T-184's R7
// row: after a membership change, an alter is refused UNAVAILABLE and
// not_applied while its owner's placement pass for the new epoch has not
// finished, and proceeds once it has. The pass's unfinished state is set
// directly on the owner after a real removal moved the cluster to a new epoch.
func TestAnAlterIsRefusedUntilThePlacementPassFinishes(t *testing.T) {
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
	pollUntil(t, "node 1 adopts the removal and finishes its placement pass", func() bool {
		return owner.Epoch() == epoch && owner.placementPassFinished()
	})
	bucket := bucketOwnedBy(t, tc, 0, "sessions")
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})

	setPassFinished(owner, epoch-1)
	frame := alterFrame(t, tc, 0, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 2})
	setPassFinished(owner, epoch)
	requireUnavailableNotApplied(t, frame,
		fmt.Sprintf("bucket %q cannot change its replication_factor yet: %s is still recording which buckets the last membership change moved (epoch %d). Run the alter again.",
			bucket, owner.self.ID, epoch))
	requireNoAlterLogged(t, tc, bucket)

	alterAccepted(t, alterFrame(t, tc, 0, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 2}))
}

// TestASecondAlterIsRefusedWhileACopyIsPending is T-163 (R8 as narrowed,
// owner R9-Q1) with its owner-side supersede case (build doc SA-5). An
// increase from 3 to 5 leaves the fourth and fifth members of the bucket's
// walk awaiting a copy, which G2 never clears. An alter to 4, which still
// places the fourth, is refused UNAVAILABLE and not_applied; an alter to 3,
// which places neither, proceeds (a decrease, so with the switch), and its
// apply deletes both awaiting records and drops them from the map.
func TestASecondAlterIsRefusedWhileACopyIsPending(t *testing.T) {
	// Auto-rebalance off: the copy stays pending, which the pending-copy
	// worker would otherwise make at once.
	tc := newTestClusterWith(t, 5, 1, func(_ int, cfg *config.Config) { cfg.Server.AutoRebalance = false })
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitForAlterFeature(t, tc)
	bucket := bucketOwnedBy(t, tc, 0, "sessions")
	createBucketAt(tc, 0, testDatabase, bucket, 3)
	waitForServing(t, tc, testDatabase, bucket, 3)
	owner := tc.nodes[0].server.core
	key := placementOf(testDatabase, bucket)
	walk := mustReplicaIDsAt(t, owner.cluster, testDatabase, bucket, 5)

	alterAccepted(t, alterFrame(t, tc, 0, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 5}))
	if pending := sortedAwaiting(owner.cluster, key); len(pending) != 2 {
		t.Fatalf("after the increase the owner awaits %v, want the two members it placed", pending)
	}

	frame := alterFrame(t, tc, 0, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 4})
	requireUnavailableNotApplied(t, frame,
		fmt.Sprintf("bucket %q is still copying to %s from its last replication_factor change (to 5), and replication_factor 4 keeps %s in its set.",
			bucket, walk[3], walk[3]),
		"retry when the row reads 5/5", fmt.Sprintf("or lower replication_factor to 3 to stop waiting for %s.", walk[3]))
	if logged := walOpCount(t, owner.wal, transport.OpAlterBucket, bucket); logged != 1 {
		t.Fatalf("the owner logged %d alters, want the increase alone", logged)
	}

	alterAccepted(t, alterFrame(t, tc, 0, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 3}))
	if pending := awaitingInMap(owner.cluster, key); len(pending) != 0 {
		t.Errorf("after the alter to 3 the owner's map still awaits %v", pending)
	}
	if onDisk := awaitingOnDisk(t, owner, key); len(onDisk) != 0 {
		t.Errorf("after the alter to 3 the owner's store still holds awaiting records for %v", onDisk)
	}
}

// TestAPendingCopyRefusesAnAlterThatPlacesItInItsWireClass is T-184's R8 row
// on three nodes: 1 to 2 leaves the second member awaiting, and 2 to 3, which
// still places it, is UNAVAILABLE and not_applied.
func TestAPendingCopyRefusesAnAlterThatPlacesItInItsWireClass(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitForAlterFeature(t, tc)
	bucket := bucketOwnedBy(t, tc, 0, "sessions")
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	alterAccepted(t, alterFrame(t, tc, 0, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 2}))

	requireUnavailableNotApplied(t, alterFrame(t, tc, 0, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 3}),
		"is still copying to", "(to 2)", "or lower replication_factor to 1")
	if logged := walOpCount(t, tc.nodes[0].server.core.wal, transport.OpAlterBucket, bucket); logged != 1 {
		t.Fatalf("the owner logged %d alters, want the first alone", logged)
	}
}
