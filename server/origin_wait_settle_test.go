package server

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/transport"
)

// A wait for an origin's stream is kept in memory only while a copy waits
// (rule 11): every way a take-over or a follower settles a bucket, and the
// release of a copy outside its set, forgets it. Each case seeds a wait for a
// bucket that then settles by that path, and asserts the wait is gone.

// seedOriginWait records a wait for key's origin stream on c, as a round
// that met a stream still delivering would.
func seedOriginWait(c *Cluster, key placementKey) {
	now := time.Now()
	c.takeOver.mu.Lock()
	c.takeOver.waiting[key] = originWait{began: now, advanced: now}
	c.takeOver.mu.Unlock()
}

// holdsOriginWait reports whether c still records a wait for key.
func holdsOriginWait(c *Cluster, key placementKey) bool {
	c.takeOver.mu.Lock()
	defer c.takeOver.mu.Unlock()
	_, waiting := c.takeOver.waiting[key]
	return waiting
}

// TestEverySettledTakeOverForgetsItsOriginWait: on a settled three-node
// cluster, the owner of an opened bucket (takeOverOwned's opened-and-unmoved
// case), a replica that follows its term (followOwner's settled case), and
// the owner of a name no node holds (completeTakeOver's nothing case) each
// settle the bucket a wake names, and each forgets the wait it held. Guard:
// forget the wait only in finishTakeOver — all three keep it.
func TestEverySettledTakeOverForgetsItsOriginWait(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	const bucket = "settled"
	key := placementOf(testDatabase, bucket)
	owner, err := clusterOf(tc, 0).Owner(key)
	if err != nil {
		t.Fatalf("placing %s: %v", bucket, err)
	}
	ownerIndex := nodeIndexByAddr(t, tc.addrs, owner.ID)
	tc.ok(ownerIndex, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(ownerIndex, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, bucket, 3)
	follower := (ownerIndex + 1) % len(tc.nodes)

	const unheld = "neverheld"
	unheldKey := placementOf(testDatabase, unheld)
	unheldOwner, err := clusterOf(tc, 0).Owner(unheldKey)
	if err != nil {
		t.Fatalf("placing %s: %v", unheld, err)
	}

	for _, settle := range []struct {
		name   string
		index  int
		bucket string
		key    placementKey
	}{
		{name: "the owner of an opened bucket", index: ownerIndex, bucket: bucket, key: key},
		{name: "a replica that follows its owner's term", index: follower, bucket: bucket, key: key},
		{name: "the owner of a name no node holds", index: nodeIndexByAddr(t, tc.addrs, unheldOwner.ID), bucket: unheld, key: unheldKey},
	} {
		c := clusterOf(tc, settle.index)
		seedOriginWait(c, settle.key)
		if !holdsWithin(15*time.Second, func() bool {
			c.wakeTakeOver(testDatabase, settle.bucket)
			return !holdsOriginWait(c, settle.key)
		}) {
			t.Errorf("node %d, %s, still holds a wait for %s's origin stream after its take-over settled it", settle.index+1, settle.name, settle.bucket)
		}
	}
}

// TestAReleasedCopyForgetsItsOriginWait: a node that leaves a bucket's set
// while it holds a wait for the old owner's stream releases its copy once
// every member covers it, and forgets the wait with it. The wait is seeded as
// the leaver's sweep for the join begins its pass, before any release. Guard:
// release the copy without ending its take-over — the wait stays.
func TestAReleasedCopyForgetsItsOriginWait(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	tc := newTestClusterWith(t, 3, 2, autoRebalanceOffOnNodeZero)
	t.Cleanup(tc.stop)
	ports := reserveNodePorts(t)
	seeded := findSeededReplicaScenario(t, tc, ports.ringNode(), "released")
	sc := leavingHolderScenario{tc: tc, bucket: seeded.bucket, ownerIndex: seeded.ownerIndex, leaverIndex: leaverOf(t, tc, seeded.bucket)}
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(sc.ownerIndex, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: sc.bucket, Type: BucketTypeHash})
	sc.putEach("k0", "k1")
	leaver := tc.nodes[sc.leaverIndex].server.core
	key := placementOf(testDatabase, sc.bucket)
	joinedAt := leaver.cluster.Epoch() + 1
	var seededWait atomic.Bool
	hook := func(nodeID string, epoch uint64, stage sweepStage) {
		if nodeID == leaver.cluster.self.ID && epoch >= joinedAt && stage == sweepPassing && leaver.bucketStoreOpen(testDatabase, sc.bucket) &&
			seededWait.CompareAndSwap(false, true) {
			seedOriginWait(leaver.cluster, key)
		}
	}
	sweepStageHook.Store(&hook)
	t.Cleanup(func() { sweepStageHook.Store(nil) })
	sc.joiner = joinLiveReplica(t, tc, ports, nil)
	if leaver.cluster.Hosts(key) {
		t.Fatalf("precondition: node %d is still in %s's replica set after the join", sc.leaverIndex+1, sc.bucket)
	}
	sc.sweepAll(ctx, true)
	if !seededWait.Load() {
		t.Fatalf("precondition: node %d's sweep for the join never began a pass while it held %s", sc.leaverIndex+1, sc.bucket)
	}
	if leaver.bucketStoreOpen(testDatabase, sc.bucket) {
		t.Fatalf("precondition: node %d left %s's set and every member covers it, and it did not release its copy", sc.leaverIndex+1, sc.bucket)
	}
	if holdsOriginWait(leaver.cluster, key) {
		t.Errorf("node %d released its copy of %s outside the set and still holds a wait for its origin stream", sc.leaverIndex+1, sc.bucket)
	}
}
