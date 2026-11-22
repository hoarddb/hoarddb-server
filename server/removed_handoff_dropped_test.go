package server

import (
	"context"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/transport"
)

// removedOwnerWithOneKey builds BUG-0512's cluster: two nodes at factor 1,
// node 1's log captured, and a bucket owned by node 1 holding key "old". It
// returns the bucket's name and node 1's incarnation of it.
func removedOwnerWithOneKey(t *testing.T) (*testCluster, *capturedLog, string, uint64) {
	t.Helper()
	logger, captured := newLogCapture()
	tc := newTestClusterTuned(t, 2, 1, nil, func(index int, c *Cluster) {
		if index == 1 {
			c.logger = logger
		}
	})
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	bucket := bucketNameOwnedBy(t, clusterOf(tc, 0), testDatabase, tc.addrs[1])
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "old",
		Data: map[string]interface{}{"n": 1}, WriteConcern: "all"})
	return tc, captured, bucket, lineageOf(t, clusterOf(tc, 1).s, testDatabase, bucket).Incarnation
}

// TestARemovedNodeIgnoresADroppedAnswerForAnotherIncarnation is BUG-0512: a
// member answers the removed node's ask that it dropped an incarnation that
// is not the one the removed node holds. That answer is not about the removed
// node's copy, so the node keeps handing it over and never says its data
// directory can be emptied while it holds the only copy; once the member
// takes the bucket, it serves the removed node's writes. Guard: take any
// dropped answer as this copy's drop — the pass ends at once with "can be
// emptied", and the owner never gets "old".
func TestARemovedNodeIgnoresADroppedAnswerForAnotherIncarnation(t *testing.T) {
	tc, captured, bucket, own := removedOwnerWithOneKey(t)
	defer tc.stop()
	survivor := clusterOf(tc, 0)

	var refusing atomic.Bool
	var refused atomic.Int64
	refusing.Store(true)
	refuseMigrateOn(t, survivor.self.ID, bucket, func() error {
		if !refusing.Load() {
			return nil
		}
		refused.Add(1)
		return &pullOfDroppedIncarnationError{database: testDatabase, bucket: bucket, incarnation: own + 7}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := survivor.proposeRemoval(ctx, tc.addrs[1]); err != nil {
		t.Fatalf("removing the running owner: %v", err)
	}
	// Two answers: the removed node asked again after the first one, so it did
	// not take the first as its copy's drop.
	if !holdsWithin(20*time.Second, func() bool { return refused.Load() >= 2 }) {
		t.Fatalf("the removed node stopped asking after %d answers that another incarnation (%d, it holds %d) was dropped; info: %q",
			refused.Load(), own+7, own, captured.messages(slog.LevelInfo))
	}
	if lines := captured.messages(slog.LevelInfo, "can be emptied"); len(lines) > 0 {
		t.Fatalf("the removed node said its data directory can be emptied while it holds the only copy of %s: %q", bucket, lines)
	}
	if lines := captured.messages(slog.LevelInfo, "dropped the incarnation"); len(lines) > 0 {
		t.Errorf("the removed node noted its incarnation dropped when the member dropped another: %q", lines)
	}
	refusing.Store(false)

	if !holdsWithin(40*time.Second, func() bool { return !clusterOf(tc, 1).removedHandOffRunning.Load() }) {
		t.Fatal("the removed node's pass never ended after the member stopped refusing")
	}
	if !holdsKeyIn(tc.nodes[0], testDatabase, bucket, "old") {
		t.Errorf("the owner does not serve the removed node's write: the hand-off stopped at the answer about another incarnation")
	}
	if !holdsKeyIn(tc.nodes[1], testDatabase, bucket, "old") {
		t.Error("the removed node lost its copy")
	}
}

// TestARemovedNodeTakesADroppedAnswerForItsOwnIncarnationAsDropped is
// BUG-0512's intended case (R8-C): a member answers that it dropped the
// incarnation the removed node holds. The node notes it, stops handing the
// bucket over, and keeps its copy (a removed node deletes nothing). Guard:
// ignore the answer — the pass keeps asking and never ends.
func TestARemovedNodeTakesADroppedAnswerForItsOwnIncarnationAsDropped(t *testing.T) {
	tc, captured, bucket, own := removedOwnerWithOneKey(t)
	defer tc.stop()
	survivor := clusterOf(tc, 0)

	refuseMigrateOn(t, survivor.self.ID, bucket, func() error {
		return &pullOfDroppedIncarnationError{database: testDatabase, bucket: bucket, incarnation: own}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := survivor.proposeRemoval(ctx, tc.addrs[1]); err != nil {
		t.Fatalf("removing the running owner: %v", err)
	}
	if !holdsWithin(20*time.Second, func() bool { return captured.count(slog.LevelInfo, "dropped the incarnation") > 0 }) {
		t.Fatalf("the removed node did not note that the member dropped its incarnation; info: %q", captured.messages(slog.LevelInfo))
	}
	if !holdsWithin(10*time.Second, func() bool { return !clusterOf(tc, 1).removedHandOffRunning.Load() }) {
		t.Fatal("the removed node's pass did not end although the member dropped its incarnation")
	}
	if holdsKeyIn(tc.nodes[0], testDatabase, bucket, "old") {
		t.Error("the member took over an incarnation it answered dropped")
	}
	if !holdsKeyIn(tc.nodes[1], testDatabase, bucket, "old") {
		t.Error("the removed node lost its copy")
	}
}
