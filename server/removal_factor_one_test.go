package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/transport"
)

// TestARunningOwnerRemovedAtFactorOneHandsItsBucketOver is BUG-0500: at
// factor 1, removing the bucket's owner while it runs is a clean hand-off
// (bucket-replication spec §20.8.7): the survivor that the ring now names as
// owner serves every acknowledged write without an operator's migrate, and the
// removed node keeps its copy. Guard: nothing names the bucket to the new
// owner, so it answers "bucket not found" for good.
func TestARunningOwnerRemovedAtFactorOneHandsItsBucketOver(t *testing.T) {
	tc := newTestCluster(t, 2, 1)
	defer tc.stop()
	survivor := tc.nodes[0].server.core.cluster
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	bucket := bucketNameOwnedBy(t, survivor, testDatabase, tc.addrs[1])
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	for i := 0; i < 5; i++ {
		tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: fmt.Sprintf("k%02d", i),
			Data: map[string]interface{}{"n": i}, WriteConcern: "all"})
	}
	// The survivor's catalog never lists a factor-1 bucket it does not hold:
	// its own sweep cannot name the bucket to itself.
	requireCatalogLacks(t, survivor, testDatabase, bucket)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := survivor.proposeRemoval(ctx, tc.addrs[1]); err != nil {
		t.Fatalf("removing the running owner: %v", err)
	}

	served := awaitHandOverKeepingTheSource(t, tc, 0, 1, bucket)
	if !served {
		frame := tc.frame(0, transport.OpGet, FindRequest{Database: testDatabase, Bucket: bucket, Key: "k00"})
		t.Fatalf("%s after removing its only holder while it ran, the survivor never took %s/%s over within %s; a read answers %s: %q",
			survivor.self.ID, testDatabase, bucket, takeOverWait, frame.Opcode, string(frame.Payload))
	}
	for i := 0; i < 5; i++ {
		if key := fmt.Sprintf("k%02d", i); !holdsKeyIn(tc.nodes[0], testDatabase, bucket, key) {
			t.Errorf("the survivor lacks acknowledged write %s", key)
		}
	}
	if !tc.nodes[1].server.core.bucketStoreOpen(testDatabase, bucket) {
		t.Error("the removed node deleted its copy")
	}
}

// TestARunningNodeRemovedAtFactorOneHandsEachBucketToItsOwnNewOwner is
// BUG-0500's three-node variant: one of three nodes is removed while it runs,
// and the factor-1 buckets it owned go to both survivors. Each survivor serves
// the bucket the ring now gives it, and the removed node keeps every copy.
// Guard: a fix that names the buckets only to one survivor, or only to the
// last one.
func TestARunningNodeRemovedAtFactorOneHandsEachBucketToItsOwnNewOwner(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()
	const removedIndex = 2
	proposer := clusterOf(tc, 0)
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	after := ringWith(t, 1, tc.addrs[0], tc.addrs[1])
	buckets := make([]string, 2)
	for survivorIndex := range buckets {
		buckets[survivorIndex] = bucketNamed(t, fmt.Sprintf("handed%d-", survivorIndex), func(name string) bool {
			owner, err := proposer.Owner(placementOf(testDatabase, name))
			return err == nil && owner.ID == tc.addrs[removedIndex] && ownerOn(after, name) == tc.addrs[survivorIndex]
		})
	}
	for _, bucket := range buckets {
		tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
		for i := 0; i < 5; i++ {
			tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: fmt.Sprintf("k%02d", i),
				Data: map[string]interface{}{"n": i}, WriteConcern: "all"})
		}
	}
	for survivorIndex, bucket := range buckets {
		requireCatalogLacks(t, clusterOf(tc, survivorIndex), testDatabase, bucket)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := proposer.proposeRemoval(ctx, tc.addrs[removedIndex]); err != nil {
		t.Fatalf("removing the running owner: %v", err)
	}

	for survivorIndex, bucket := range buckets {
		if !awaitHandOverKeepingTheSource(t, tc, survivorIndex, removedIndex, bucket) {
			frame := tc.frame(survivorIndex, transport.OpGet, FindRequest{Database: testDatabase, Bucket: bucket, Key: "k00"})
			t.Fatalf("node %d, the new owner of %s, never took it over within %s; a read answers %s: %q",
				survivorIndex+1, bucket, takeOverWait, frame.Opcode, string(frame.Payload))
		}
		for i := 0; i < 5; i++ {
			if key := fmt.Sprintf("k%02d", i); !holdsKeyIn(tc.nodes[survivorIndex], testDatabase, bucket, key) {
				t.Errorf("node %d lacks acknowledged write %s of %s", survivorIndex+1, key, bucket)
			}
		}
		if !tc.nodes[removedIndex].server.core.bucketStoreOpen(testDatabase, bucket) {
			t.Errorf("the removed node deleted its copy of %s", bucket)
		}
	}
}

// requireCatalogLacks fails unless c's catalog has no entry for the bucket.
func requireCatalogLacks(t *testing.T, c *Cluster, database, bucket string) {
	t.Helper()
	listed := false
	c.forEachKnownBucket(func(knownDatabase, knownBucket string) {
		listed = listed || (knownDatabase == database && knownBucket == bucket)
	})
	if listed {
		t.Fatalf("setup: %s's catalog lists %s/%s, which it does not hold at factor 1", c.self.ID, database, bucket)
	}
}

// awaitHandOverKeepingTheSource waits up to takeOverWait for the node at
// ownerIndex to serve the bucket, and fails at once if the removed node's copy
// is gone before then: the removed node may hold the only copy, so it must
// never give it up before a member has it (and, being removed, never after).
func awaitHandOverKeepingTheSource(t *testing.T, tc *testCluster, ownerIndex, removedIndex int, bucket string) bool {
	t.Helper()
	deadline := time.Now().Add(takeOverWait)
	for time.Now().Before(deadline) {
		ownerServes := tc.nodes[ownerIndex].server.core.bucketStoreOpen(testDatabase, bucket)
		if !tc.nodes[removedIndex].server.core.bucketStoreOpen(testDatabase, bucket) {
			t.Fatalf("the removed node no longer holds %s (the new owner serves it: %v)", bucket, ownerServes)
		}
		if ownerServes {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false
}

// TestARemovedNodeStoppedMidHandOffHandsItsBucketsOverWhenItStarts is
// BUG-0500 across a restart: the removed node's asks are refused until it
// stops, so the survivor has not taken the bucket over; started again, still
// removed, and with a ring of one member, it names the bucket to the survivor,
// which then serves it. Guard: a hand-off launched only on adopting the
// removal, so a removed node restarted mid-way leaves the bucket unreachable.
func TestARemovedNodeStoppedMidHandOffHandsItsBucketsOverWhenItStarts(t *testing.T) {
	tc := newTestCluster(t, 2, 1)
	defer tc.stop()
	survivor := clusterOf(tc, 0)
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	bucket := bucketNameOwnedBy(t, survivor, testDatabase, tc.addrs[1])
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	for i := 0; i < 5; i++ {
		tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: fmt.Sprintf("k%02d", i),
			Data: map[string]interface{}{"n": i}, WriteConcern: "all"})
	}

	var asked atomic.Int64
	refuse := func(fromID, _, _, name string) error {
		if fromID != tc.addrs[1] || name != bucket {
			return nil
		}
		asked.Add(1)
		return errors.New("test: the removed node stops before the survivor answers")
	}
	askNodeToMigrateHook.Store(&refuse)
	t.Cleanup(func() { askNodeToMigrateHook.Store(nil) })

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := survivor.proposeRemoval(ctx, tc.addrs[1]); err != nil {
		t.Fatalf("removing the running owner: %v", err)
	}
	if !holdsWithin(10*time.Second, func() bool { return asked.Load() > 0 }) {
		t.Fatal("the removed node never asked the survivor to take its bucket")
	}
	if tc.nodes[0].server.core.bucketStoreOpen(testDatabase, bucket) {
		t.Fatal("setup: the survivor took the bucket over although every ask was refused")
	}

	askNodeToMigrateHook.Store(nil)
	tc.restartNode(1)
	if !clusterOf(tc, 1).removed() {
		t.Fatal("setup: the restarted node does not start removed")
	}
	if !awaitHandOverKeepingTheSource(t, tc, 0, 1, bucket) {
		frame := tc.frame(0, transport.OpGet, FindRequest{Database: testDatabase, Bucket: bucket, Key: "k00"})
		t.Fatalf("the removed node, restarted mid-way, never handed %s over within %s; a read on the survivor answers %s: %q",
			bucket, takeOverWait, frame.Opcode, string(frame.Payload))
	}
	for i := 0; i < 5; i++ {
		if key := fmt.Sprintf("k%02d", i); !holdsKeyIn(tc.nodes[0], testDatabase, bucket, key) {
			t.Errorf("the survivor lacks acknowledged write %s", key)
		}
	}
}

// TestARemovedNodeNeverCallsItsCopyOfAnotherIncarnationHandedOver is
// BUG-0500's rework, item 2: the removed node holds incarnation 1 of a
// factor-1 bucket, and the name is created again on its new owner before the
// removed node's ask gets through. The owner answers that it already serves
// the bucket, but it serves incarnation 2, and incarnation 1's writes exist only
// on the removed node. The pass must not log that its data directory can be
// emptied; it warns, naming the bucket and what the operator can do, and ends.
// Guard: "already held" taken as handed over.
func TestARemovedNodeNeverCallsItsCopyOfAnotherIncarnationHandedOver(t *testing.T) {
	logger, captured := newLogCapture()
	tc := newTestClusterTuned(t, 2, 1, nil, func(index int, c *Cluster) {
		if index == 1 {
			c.logger = logger
		}
	})
	defer tc.stop()
	survivor := clusterOf(tc, 0)
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	bucket := bucketNameOwnedBy(t, survivor, testDatabase, tc.addrs[1])
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "old",
		Data: map[string]interface{}{"n": 1}, WriteConcern: "all"})

	var refuse atomic.Bool
	refuse.Store(true)
	asked := countRemovedNodeAsks(t, tc.addrs[1], bucket, &refuse)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := survivor.proposeRemoval(ctx, tc.addrs[1]); err != nil {
		t.Fatalf("removing the running owner: %v", err)
	}
	if !holdsWithin(10*time.Second, func() bool { return asked.Load() > 0 }) {
		t.Fatal("setup: the removed node never asked")
	}
	recreateUnheard(t, tc, tc.addrs[1], bucket)
	before := asked.Load()
	refuse.Store(false)

	// Either the pass asks again once the hold lifts, or an ask that landed in
	// the window between the recreation and the lift has already settled the
	// bucket: the owner answered that it serves another incarnation, which ends
	// the pass with nothing left to ask. Waiting for an ask alone fails in the
	// second case whenever the setup is slow enough to span a retry (BUG-0487).
	// The assertions below hold in both.
	askedAgainOrEnded := func() bool {
		return asked.Load() > before || !clusterOf(tc, 1).removedHandOffRunning.Load()
	}
	if !holdsWithin(40*time.Second, askedAgainOrEnded) {
		t.Fatalf("setup: the removed node neither asked again after the hold lifted nor ended its pass (asks %d, before the lift %d)",
			asked.Load(), before)
	}
	if !holdsWithin(10*time.Second, func() bool { return !clusterOf(tc, 1).removedHandOffRunning.Load() }) {
		t.Fatal("the removed node's pass never ended although the owner answered for the bucket")
	}
	if lines := captured.messages(slog.LevelInfo, "can be emptied"); len(lines) > 0 {
		t.Errorf("the removed node said its data directory can be emptied while it holds the only copy of incarnation 1: %q", lines)
	}
	if captured.count(slog.LevelWarn, bucket, "another incarnation") == 0 {
		t.Errorf("the removed node did not warn that %s was not handed over because its owner holds another incarnation; warnings: %q",
			bucket, captured.messages(slog.LevelWarn))
	}
	if holdsKeyIn(tc.nodes[0], testDatabase, bucket, "old") || !holdsKeyIn(tc.nodes[0], testDatabase, bucket, "fresh") {
		t.Error("the recreated bucket on the owner was replaced by the removed node's incarnation")
	}
	if !holdsKeyIn(tc.nodes[1], testDatabase, bucket, "old") {
		t.Error("the removed node lost its copy")
	}
}
