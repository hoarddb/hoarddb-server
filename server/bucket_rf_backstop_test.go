package server

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/transport"
)

// The backstop rule (bucket-replication-rev8-spec §11.3; G3 build doc step
// 6). A member that missed an alter whose entry was lost with a departed
// owner learns the factor from its new owner's answer at the next follow,
// ReplCopy.Factor, and applies the placement effect at it: its catalog
// entry, record and registry at the learned factor, and an outside mark when
// its membership differs between the two factors, without moving its lineage.

// demotedAcrossTheOwnersRemoval names a bucket owned by node 1 whose fourth
// holder at 4 is not in its set at 2 once node 1 is removed.
func demotedAcrossTheOwnersRemoval(t *testing.T, tc *testCluster) string {
	t.Helper()
	before := ringWith(t, 4, tc.addrs...)
	after := ringWith(t, 2, tc.addrs[1:]...)
	for attempt := 0; attempt < 8000; attempt++ {
		name := fmt.Sprintf("sessions%04d", attempt)
		key := placementOf(testDatabase, name).encoded
		walk, err := before.Replicas(key, 4)
		if err != nil {
			t.Fatalf("placing %s: %v", name, err)
		}
		if walk[0].ID != tc.addrs[0] {
			continue
		}
		kept, err := after.Replicas(key, 2)
		if err != nil {
			t.Fatalf("placing %s without node 1: %v", name, err)
		}
		if !nodeListed(kept, walk[3].ID) {
			return name
		}
	}
	t.Fatal("no bucket name of node 1's leaves its fourth holder out of its set at 2 once node 1 is removed")
	return ""
}

// lostDecrease is T-57's state: five nodes; a bucket at 4, owned by node 1,
// lowered to 2 while the owner's stream to the fourth node of its walk is
// frozen, so that node never receives the decrease; then the owner stopped and
// removed, and its log, the decrease's only way to that node, gone with it. On
// the new ring the node is in the set at 4 and not at 2. before is its copy
// before the decrease.
type lostDecrease struct {
	tc       *testCluster
	bucket   string
	survivor int
	demoted  int
	before   ReplCopy
}

func loseADecrease(t *testing.T) lostDecrease {
	t.Helper()
	tc := newTestCluster(t, 5, 1)
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitForAlterFeature(t, tc)
	bucket := demotedAcrossTheOwnersRemoval(t, tc)
	createBucketAt(tc, 0, testDatabase, bucket, 4)
	waitForServing(t, tc, testDatabase, bucket, 4)
	owner := clusterOf(tc, 0)
	walk := mustReplicaIDsAt(t, owner, testDatabase, bucket, 4)
	lost := lostDecrease{tc: tc, bucket: bucket, survivor: nodeIndexByAddr(t, tc.addrs, walk[1]), demoted: nodeIndexByAddr(t, tc.addrs, walk[3])}
	putDocumentAt(tc, 0, testDatabase, bucket, "before", "all")
	demotedCore := tc.nodes[lost.demoted].server.core
	var err error
	if lost.before, err = demotedCore.cluster.answerCopy(ReplBucketRef{Database: testDatabase, Bucket: bucket}); err != nil {
		t.Fatalf("reading node %d's copy: %v", lost.demoted+1, err)
	}

	unfreeze := freezePeer(owner.peerFor(mustNode(t, owner, tc.addrs[lost.demoted])))
	defer unfreeze()
	alterWithin(t, tc, 0, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 2})
	if logged := walOpCount(t, demotedCore.wal, transport.OpAlterBucket, bucket); logged != 0 {
		t.Fatalf("setup: node %d logged the decrease its stream was frozen for", lost.demoted+1)
	}
	stopNode(t, tc, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := tc.nodes[lost.survivor].server.core.cluster.proposeRemoval(ctx, owner.self.ID); err != nil {
		t.Fatalf("removing the owner: %v", err)
	}
	if after, err := demotedCore.cluster.replicasAtFactor(placementOf(testDatabase, bucket), 2); err != nil || nodeListed(after, tc.addrs[lost.demoted]) {
		t.Fatalf("setup: on the new ring node %d is in the set at 2 (%v, %v); the backstop would have nothing to mark", lost.demoted+1, nodeIDs(after), err)
	}
	return lost
}

// TestADemotedHolderWhoseOwnerWasRemovedLearnsTheFactorAtTheFollow is T-57.
// In lostDecrease's state the demoted node, at its next follow, learns 2 from
// the new owner: it holds the bucket at 2 in all three stores, its copy is
// marked outside with its lineage where it was, and its reads are forwarded.
// Its release is G5's (OQ-G3-4): the removal's sweep ran before the mark.
func TestADemotedHolderWhoseOwnerWasRemovedLearnsTheFactorAtTheFollow(t *testing.T) {
	lost := loseADecrease(t)
	defer lost.tc.stop()
	tc, bucket, demoted := lost.tc, lost.bucket, lost.demoted
	demotedCore := tc.nodes[demoted].server.core
	pollUntil(t, "the demoted node learns the factor at its follow", func() bool {
		return heldFactorOf(t, demotedCore, bucket).record == 2
	})
	requireHeldAt(t, demotedCore, bucket, 2, "after the follow")
	after, err := demotedCore.cluster.answerCopy(ReplBucketRef{Database: testDatabase, Bucket: bucket})
	if err != nil {
		t.Fatalf("reading node %d's copy: %v", demoted+1, err)
	}
	if after.Outside == 0 {
		t.Errorf("node %d's copy, outside the set at the learned factor, carries no outside mark: %+v", demoted+1, after)
	}
	if after.Term != lost.before.Term || after.Through != lost.before.Through {
		t.Errorf("learning the factor moved node %d's lineage from (%d, %d) to (%d, %d)",
			demoted+1, lost.before.Term, lost.before.Through, after.Term, after.Through)
	}
	requireReadsFound(t, tc, demoted, bucket, []string{"before"}, "once the demoted node learned the factor")
	t.Logf("node %d's copy after the follow: held %v, marks %+v (its release is G5's)",
		demoted+1, demotedCore.bucketStoreOpen(testDatabase, bucket), demotedCore.cluster.placementMarksOf(placementOf(testDatabase, bucket)))
}
