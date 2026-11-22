package server

import (
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/transport"
)

// The take-over opens no term before the start hold opens (on-demand pull
// build doc step 10; spec §9.2): a term opened before the hold has moved the
// log past what the peers hold would stamp the copy with an applied sequence
// below the fast-forwarded tail.

// TestATakeOverOpensNoTermBeforeTheStartHoldOpens: three nodes at factor 3;
// the bucket's owner is removed, and the survivor that now owns it, holding a
// replica, has its take-over paused. The other survivor is stopped and the
// new owner restarted, so its start hold has no peer to hear from and stays
// shut. Its take-over, let run, does not open the copy while the hold is
// shut. Once the other survivor is back the hold opens, the worker is woken,
// and the copy opens with an applied sequence at or past the log's tail.
// Guard: the hold check in completeTakeOver.
func TestATakeOverOpensNoTermBeforeTheStartHoldOpens(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	t.Cleanup(tc.stop)
	before := ringWith(t, 3, tc.addrs...)
	after := ringWith(t, 3, tc.addrs[1], tc.addrs[2])
	bucket := bucketNamed(t, "held", func(name string) bool { return ownerOn(before, name) == tc.addrs[0] })
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, bucket, 3)
	putHeld(tc, bucket)
	newOwnerID := ownerOn(after, bucket)
	newOwner := nodeIndexByAddr(t, tc.addrs, newOwnerID)
	other := 3 - newOwner

	paused := func(nodeID string) bool { return nodeID == newOwnerID }
	takeOverPausedHook.Store(&paused)
	t.Cleanup(func() { takeOverPausedHook.Store(nil) })
	removeAndTell(t, tc, newOwner, 0)
	via := clusterOf(tc, newOwner)
	if err := clusterOf(tc, other).reconcileMembership(via.self.ID, via.memberNodes(), via.memberIDs(), via.Epoch()); err != nil {
		t.Fatalf("bringing node %d to the removal: %v", other+1, err)
	}
	ref := ReplBucketRef{Database: testDatabase, Bucket: bucket}
	if own, err := via.answerCopy(ref); err != nil || own.Opened && own.Moved == 0 {
		t.Fatalf("setup: the new owner took the bucket over before the restart (copy %+v, err %v)", own, err)
	}

	otherBase := tc.stopNodeKeepingData(other)
	restarted := tc.restartNode(newOwner).server.core.cluster
	if restarted.startHold.open.Load() {
		t.Fatalf("setup: the restarted owner's start hold opened with its only peer stopped")
	}
	takeOverPausedHook.Store(nil)
	restarted.signalTakeOver()
	if holdsWithin(2*time.Second, func() bool {
		own, err := restarted.answerCopy(ref)
		return err == nil && own.Opened && own.Moved == 0
	}) {
		t.Errorf("the restarted owner opened the copy of %s while its start hold was shut", bucket)
	}

	tc.nodes[other] = tc.startNode(other, otherBase, tc.token)
	if !holdsWithin(takeOverWait, restarted.startHold.open.Load) {
		t.Fatalf("the restarted owner's start hold never opened once its peer was back")
	}
	tailAtOpen := restarted.walLastSequence()
	if !holdsWithin(takeOverWait, func() bool {
		own, err := restarted.answerCopy(ref)
		return err == nil && own.Opened && own.Moved == 0
	}) {
		t.Fatalf("the restarted owner did not open the copy of %s once its start hold opened", bucket)
	}
	state, err := restarted.s.bucketStateNamed(testDatabase, bucket)
	if err != nil {
		t.Fatalf("reading the opened copy's state: %v", err)
	}
	if state.AppliedSeq < tailAtOpen {
		t.Errorf("the copy opened with applied sequence %d, below the log's tail %d once the hold had moved it", state.AppliedSeq, tailAtOpen)
	}
	if !foundThrough(tc, newOwner, testDatabase, bucket, "held") {
		t.Errorf("\"held\" does not read back through the new owner once its copy opened")
	}
}
