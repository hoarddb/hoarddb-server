package server

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Dispatch 6, step 6.5a: the membership history, and ownership judged since
// the entry was logged (A-6.2; review F-2).

// ringOfIDs is the placement ring at factor 3 of the given member ids.
func ringOfIDs(t *testing.T, ids ...string) *ring.Ring {
	t.Helper()
	placement := ring.New(3)
	for _, id := range ids {
		if err := placement.AddNode(ring.Node{ID: id, Addr: id}); err != nil {
			t.Fatalf("building a ring: %v", err)
		}
	}
	return placement
}

// bucketOwnedThroughAJoinAndARemoval returns the index of the node X of tc
// and a bucket name such that: X owns it on tc's two nodes; n owns it once n
// joins; n still owns it, and r and X hold copies, once r joins; and X owns it again
// once n is removed. It returns an empty name when there is none.
func bucketOwnedThroughAJoinAndARemoval(t *testing.T, tc *testCluster, n, r string) (int, string) {
	t.Helper()
	first := ringOfIDs(t, tc.addrs...)
	second := ringOfIDs(t, tc.addrs[0], tc.addrs[1], n)
	third := ringOfIDs(t, tc.addrs[0], tc.addrs[1], n, r)
	fourth := ringOfIDs(t, tc.addrs[0], tc.addrs[1], r)
	for i := 0; i < 2000; i++ {
		name := fmt.Sprintf("regained%04d", i)
		key := placementOf(testDatabase, name).encoded
		owner, _ := first.Owner(key)
		atSecond, _ := second.Owner(key)
		atThird, _ := third.Replicas(key, 3)
		atFourth, _ := fourth.Owner(key)
		// X stays in the set at every epoch, so it keeps its copy and, owning
		// the bucket again, holds it: a node that released its copy is
		// addressed nothing for the bucket whatever it logged.
		if atSecond.ID == n && atThird[0].ID == n && nodeListed(atThird, r) && nodeListed(atThird, owner.ID) && atFourth.ID == owner.ID {
			return nodeIndexByAddr(t, tc.addrs, owner.ID), name
		}
	}
	return 0, ""
}

// joinersForARegainedBucket reserves the ports of two joiners N and R, trying
// a few pairs, for which bucketOwnedThroughAJoinAndARemoval finds a bucket: the
// placement of some address pairs leaves none.
func joinersForARegainedBucket(t *testing.T, tc *testCluster) (n, r *testNodePorts, xIndex int, bucket string) {
	t.Helper()
	candidates := make([]*testNodePorts, 0, 6)
	for len(candidates) < cap(candidates) {
		candidates = append(candidates, reserveNodePorts(t))
	}
	for _, n := range candidates {
		for _, r := range candidates {
			if n == r {
				continue
			}
			if xIndex, bucket := bucketOwnedThroughAJoinAndARemoval(t, tc, n.ringAddr, r.ringAddr); bucket != "" {
				return n, r, xIndex, bucket
			}
		}
	}
	t.Fatalf("no pair of six reserved addresses places a bucket X → N → X with R on N's set")
	return nil, nil, 0, ""
}

// waitHolds polls until s holds bucket.
func waitHolds(t *testing.T, s *Server, bucket string, what string) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for !s.bucketStoreOpen(testDatabase, bucket) {
		if time.Now().After(deadline) {
			t.Fatalf("%s never holds %s", what, bucket)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestAnOwnerThatRegainsABucketDoesNotShipWhatItLoggedBeforeItLostIt is step
// 6.5a's test (review F-2): X and S at factor 3, five pushes to a FIFO bucket
// X owns, retention off; N joins and takes the bucket; R joins and copies it
// from N; N is removed and X owns it again. R holds five elements, not ten,
// and X's stranded counter is five: X did not own the bucket at every epoch
// since it logged the pushes. Guard: judge ownership as it stands, and X
// ships its first-era pushes to R, which already holds them by its copy.
func TestAnOwnerThatRegainsABucketDoesNotShipWhatItLoggedBeforeItLostIt(t *testing.T) {
	tc := newTestClusterWith(t, 2, 3, withoutRetentionLoop)
	defer tc.stop()
	noRetention := func(cfg *config.Config) { withoutRetentionLoop(0, cfg) }
	nPorts, rPorts, xIndex, bucket := joinersForARegainedBucket(t, tc)
	x := tc.nodes[xIndex].server.core.cluster

	tc.ok(xIndex, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(xIndex, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeFIFO})
	waitForServing(t, tc, testDatabase, bucket, 2)
	for n := 1; n <= 5; n++ {
		tc.ok(xIndex, transport.OpFIFOPush, PushRequest{Database: testDatabase, Bucket: bucket, Data: map[string]interface{}{"n": n}})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := x.proposeJoin(ctx, nPorts.ringNode()); err != nil {
		t.Fatalf("joining N: %v", err)
	}
	n := startJoinTarget(t, tc, nPorts, tc.token, noRetention)
	waitHolds(t, n.server.core, bucket, "N")
	// R is seeded with N and pins it, and N pins R, as with any member.
	tc.nodes = append(tc.nodes, n)
	tc.addrs = append(tc.addrs, nPorts.ringAddr)
	tc.fingerprints = append(tc.fingerprints, preGeneratedNodeFingerprints(t, []string{n.cfg.Storage.DataDir})...)

	if err := x.proposeJoin(ctx, rPorts.ringNode()); err != nil {
		t.Fatalf("joining R: %v", err)
	}
	r := startJoinTarget(t, tc, rPorts, tc.token, noRetention)
	defer func() { _ = r.client.Close() }()
	waitHolds(t, r.server.core, bucket, "R")
	if got := localCountOf(t, r.server.core, bucket); got != 5 {
		t.Fatalf("setup: R's copy from N holds %d elements, want 5", got)
	}

	if err := x.proposeRemoval(ctx, nPorts.ringAddr); err != nil {
		t.Fatalf("removing N: %v", err)
	}
	if owner, err := x.Owner(placementOf(testDatabase, bucket)); err != nil || owner.ID != x.self.ID {
		t.Fatalf("setup: X does not own %s again after N's removal (%v, %v)", bucket, owner, err)
	}
	caughtUpWithin(t, x.peerFor(mustNode(t, x, rPorts.ringAddr)), 30*time.Second)

	if got := localCountOf(t, r.server.core, bucket); got != 5 {
		t.Errorf("R holds %d elements of the queue after X regained it, want 5: X shipped what it logged before it lost the bucket", got)
	}
	if got := x.stranded.total.Load(); got != 5 {
		t.Errorf("X counts %d stranded entries, want 5", got)
	}
}
