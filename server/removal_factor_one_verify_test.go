package server

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/transport"
)

// BUG-0500 verifier's safety checks on the removed node's hand-off pass
// (removed_handoff.go): it never copies one incarnation over another, never
// re-seeds a bucket the cluster dropped, and never deletes its own copy.

// countRemovedNodeAsks installs an askNodeToMigrateHook that counts the
// removed node's asks for bucket and refuses them while refuse is set.
func countRemovedNodeAsks(t *testing.T, fromID, bucket string, refuse *atomic.Bool) *atomic.Int64 {
	t.Helper()
	var asked atomic.Int64
	hook := func(askerID, _, _, name string) error {
		if askerID != fromID || name != bucket {
			return nil
		}
		asked.Add(1)
		if refuse.Load() {
			return errors.New("verifier: the ask is held")
		}
		return nil
	}
	askNodeToMigrateHook.Store(&hook)
	t.Cleanup(func() { askNodeToMigrateHook.Store(nil) })
	return &asked
}

// recreateUnheard creates bucket again on the survivor at index 0 while its
// take-over cannot hear removedID, and writes "fresh" to it: the way two
// incarnations of a name still arise once a create on a node a removal left
// alone waits for the take-over (BUG-0625). Heard, the removed node's copy is
// copied in and the create is answered from it, so there is no second
// incarnation for the tests of the removed node's pass to meet.
func recreateUnheard(t *testing.T, tc *testCluster, removedID, bucket string) {
	t.Helper()
	silence := func(askerID, nodeID string) error {
		if askerID == tc.addrs[0] && nodeID == removedID {
			return errors.New("test: the removed node is not heard during the recreate")
		}
		return nil
	}
	askHolderHook.Store(&silence)
	defer askHolderHook.Store(nil)
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "fresh",
		Data: map[string]interface{}{"n": 99}, WriteConcern: "all"})
}

// roundGate holds the removed node's asks for one bucket: the first is
// refused at once, the second blocks inside the hook until released, and every
// ask after it goes through. The second ask is the first one of the pass's
// second round, so when reached closes, round one has ended with the bucket
// pending, and the pass is held before it asks anyone anything (BUG-0523).
type roundGate struct {
	asked, through atomic.Int64
	reached        chan struct{}
	release        chan struct{}
	releaseOnce    sync.Once
}

// holdSecondRound installs an askNodeToMigrateHook that follows roundGate for
// the asks fromID makes for bucket.
func holdSecondRound(t *testing.T, fromID, bucket string) *roundGate {
	t.Helper()
	gate := &roundGate{reached: make(chan struct{}), release: make(chan struct{})}
	hook := func(askerID, _, _, name string) error {
		if askerID != fromID || name != bucket {
			return nil
		}
		switch gate.asked.Add(1) {
		case 1:
			return errors.New("verifier: the ask is held")
		case 2:
			close(gate.reached)
			<-gate.release
		}
		gate.through.Add(1)
		return nil
	}
	askNodeToMigrateHook.Store(&hook)
	t.Cleanup(func() { askNodeToMigrateHook.Store(nil) })
	return gate
}

// open lets the held ask go through; it is safe to call twice.
func (g *roundGate) open() { g.releaseOnce.Do(func() { close(g.release) }) }

// TestVerifyRemovedHandOffNeverCopiesOverARecreatedBucket: the removed node
// holds incarnation 1 of a factor-1 bucket; before its ask gets through, an
// operator creates the name again on the new owner (incarnation 2) and writes
// to it. The ask must not copy incarnation 1 over it (R8-A), and the removed
// node keeps its copy.
//
// The recreate is made while the pass is held inside round two's ask, not
// "after the first ask arrived": a round asks the member and then asks the
// owner what its copy is, and a create landing between those two ends the pass
// as "superseded" for good (TestARecreateInRoundOneEndsTheRemovedNodesPass).
// Waiting for a second ask after that left the old version empty-handed
// whenever the pass was slow to reach its question (BUG-0523).
func TestVerifyRemovedHandOffNeverCopiesOverARecreatedBucket(t *testing.T) {
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
	gate := holdSecondRound(t, tc.addrs[1], bucket)
	defer gate.open() // runs before tc.stop, so a failed test never leaves the pass blocked in the hook

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := survivor.proposeRemoval(ctx, tc.addrs[1]); err != nil {
		t.Fatalf("removing the running owner: %v", err)
	}
	select {
	case <-gate.reached:
	case <-time.After(40 * time.Second):
		t.Fatalf("setup: the removed node's second round never started (asks %d)", gate.asked.Load())
	}
	if tc.nodes[0].server.core.bucketStoreOpen(testDatabase, bucket) {
		t.Fatal("setup: the survivor took the bucket although every ask was held")
	}
	recreateUnheard(t, tc, tc.addrs[1], bucket)

	gate.open()
	if !holdsWithin(40*time.Second, func() bool { return gate.through.Load() > 0 }) {
		t.Fatalf("the removed node's held ask never went through after the hold lifted (asks %d)", gate.asked.Load())
	}
	time.Sleep(3 * time.Second)

	if !holdsKeyIn(tc.nodes[0], testDatabase, bucket, "fresh") {
		t.Error("the recreated bucket on the survivor lost its own write")
	}
	if holdsKeyIn(tc.nodes[0], testDatabase, bucket, "k00") {
		t.Error("the removed node's older incarnation was copied over the recreated bucket")
	}
	if !holdsKeyIn(tc.nodes[1], testDatabase, bucket, "k00") {
		t.Error("the removed node lost its copy")
	}
	t.Logf("asks that went through after the hold lifted: %d", gate.through.Load())
}

// TestARecreateInRoundOneEndsTheRemovedNodesPass is the window the test above
// stays out of: the name is created again on the owner while round one is
// between its ask and its question to the owner. The owner then answers with
// another incarnation, so the pass calls the bucket superseded (R8-A), ends,
// and never asks a second time. That is the designed outcome, not a defect. A
// test that waits for a second ask once "the first ask arrived" fails whenever
// the create wins that race; this one makes it win every time (BUG-0523).
func TestARecreateInRoundOneEndsTheRemovedNodesPass(t *testing.T) {
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
	inRoundOne := make(chan struct{})
	recreated := make(chan struct{})
	var recreatedOnce sync.Once
	markRecreated := func() { recreatedOnce.Do(func() { close(recreated) }) }
	defer markRecreated() // runs before tc.stop, so a failed test never leaves the pass blocked in the hook
	hook := func(askerID, _, _, name string) error {
		if askerID != tc.addrs[1] || name != bucket {
			return nil
		}
		if asked.Add(1) == 1 {
			close(inRoundOne)
			<-recreated
		}
		return errors.New("verifier: the ask is held")
	}
	askNodeToMigrateHook.Store(&hook)
	t.Cleanup(func() { askNodeToMigrateHook.Store(nil) })

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := survivor.proposeRemoval(ctx, tc.addrs[1]); err != nil {
		t.Fatalf("removing the running owner: %v", err)
	}
	select {
	case <-inRoundOne:
	case <-time.After(20 * time.Second):
		t.Fatal("setup: the removed node never asked")
	}
	recreateUnheard(t, tc, tc.addrs[1], bucket)
	markRecreated()

	if !holdsWithin(10*time.Second, func() bool { return !clusterOf(tc, 1).removedHandOffRunning.Load() }) {
		t.Fatal("the removed node's pass kept running although its owner serves another incarnation of the name")
	}
	if got := asked.Load(); got != 1 {
		t.Errorf("the removed node asked %d times, want 1: a superseded bucket is not asked again", got)
	}
	if !holdsKeyIn(tc.nodes[0], testDatabase, bucket, "fresh") {
		t.Error("the recreated bucket on the survivor lost its own write")
	}
	if holdsKeyIn(tc.nodes[0], testDatabase, bucket, "k00") {
		t.Error("the removed node's older incarnation was copied over the recreated bucket")
	}
	if !holdsKeyIn(tc.nodes[1], testDatabase, bucket, "k00") {
		t.Error("the removed node lost its copy")
	}
}

// TestVerifyRemovedHandOffNeverReseedsADroppedBucket: the hand-off completes,
// the cluster drops the bucket, and the removed node (which deletes nothing)
// restarts and runs its pass again. The dropped bucket must not come back.
func TestVerifyRemovedHandOffNeverReseedsADroppedBucket(t *testing.T) {
	for _, shape := range []struct{ nodes, rf int }{{2, 1}, {3, 2}} {
		t.Run(fmt.Sprintf("%d nodes factor %d", shape.nodes, shape.rf), func(t *testing.T) {
			tc := newTestCluster(t, shape.nodes, shape.rf)
			defer tc.stop()
			removed := shape.nodes - 1
			proposer := clusterOf(tc, 0)
			tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
			bucket := bucketNameOwnedBy(t, proposer, testDatabase, tc.addrs[removed])
			tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
			for i := 0; i < 5; i++ {
				tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: fmt.Sprintf("k%02d", i),
					Data: map[string]interface{}{"n": i}, WriteConcern: "all"})
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			if err := proposer.proposeRemoval(ctx, tc.addrs[removed]); err != nil {
				t.Fatalf("removing the running owner: %v", err)
			}
			survivorsServe := func() (int, bool) {
				for i := 0; i < removed; i++ {
					if tc.nodes[i].server.core.bucketStoreOpen(testDatabase, bucket) {
						return i, true
					}
				}
				return -1, false
			}
			if !holdsWithin(takeOverWait, func() bool { _, ok := survivorsServe(); return ok }) {
				t.Fatal("setup: the hand-off never completed")
			}
			// Let the pass end before the drop.
			time.Sleep(2 * time.Second)
			tc.ok(0, transport.OpDeleteBucket, DeleteBucketRequest{Database: testDatabase, Name: bucket})
			if !holdsWithin(20*time.Second, func() bool { _, ok := survivorsServe(); return !ok }) {
				t.Fatal("setup: the drop did not reach every survivor")
			}
			if !tc.nodes[removed].server.core.bucketStoreOpen(testDatabase, bucket) {
				t.Fatal("setup: the removed node no longer holds its copy")
			}

			var refuse atomic.Bool
			asked := countRemovedNodeAsks(t, tc.addrs[removed], bucket, &refuse)
			tc.restartNode(removed)
			if !clusterOf(tc, removed).removed() {
				t.Fatal("setup: the restarted node does not start removed")
			}
			if !holdsWithin(20*time.Second, func() bool { return asked.Load() > 0 }) {
				t.Fatal("the restarted removed node never asked: the re-seed path is not exercised")
			}
			deadline := time.Now().Add(20 * time.Second)
			for time.Now().Before(deadline) {
				if i, ok := survivorsServe(); ok {
					t.Fatalf("node %d serves the dropped bucket again, re-seeded from the removed node", i+1)
				}
				time.Sleep(100 * time.Millisecond)
			}
			if !tc.nodes[removed].server.core.bucketStoreOpen(testDatabase, bucket) {
				t.Error("the removed node deleted its copy")
			}
			// The pass ends: a dropped bucket is nothing to hand over, so it
			// is not asked about every 30 s for good.
			if !holdsWithin(10*time.Second, func() bool { return !clusterOf(tc, removed).removedHandOffRunning.Load() }) {
				t.Error("the removed node's hand-off pass never ended after the bucket was dropped")
			}
			t.Logf("asks in 20 s after the restart: %d (pass still running: %v)", asked.Load(),
				clusterOf(tc, removed).removedHandOffRunning.Load())
		})
	}
}
