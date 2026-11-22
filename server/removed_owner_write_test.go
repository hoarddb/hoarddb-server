package server

import (
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// BUG-0693, owner ruling R8 extended to writes (2026-10-23): inside the
// removal window every mutation of a name a removed node owned — a bucket
// whose only copy is on that node, or a name never created — answers the
// retry a read gets: UNAVAILABLE, write_outcome not_applied, naming the
// removed node. Once that node's hand-off has reached this node or it is
// confirmed gone, a mutation of a name that still is not here is not found
// with the note naming the node.

// retryNamingRemovedNode fails the test unless frame is the removed owner's
// retry: UNAVAILABLE, nothing applied, naming node, and never not found.
func retryNamingRemovedNode(t *testing.T, what string, frame *transport.Frame, node string) {
	t.Helper()
	var refusal ErrorResponse
	if frame.Opcode != transport.OpError || bson.Unmarshal(frame.Payload, &refusal) != nil {
		t.Errorf("%s was answered %s, want a retry naming %s: %q", what, frame.Opcode, node, frame.Payload)
		return
	}
	if transport.ErrorCode(refusal.Code) != transport.ErrUnavailable || strings.Contains(refusal.Message, "not found") ||
		!strings.Contains(refusal.Message, node) {
		t.Errorf("%s was answered code %d %q; want UNAVAILABLE naming the removed node %s", what, refusal.Code, refusal.Message, node)
	}
	if outcome := writeOutcomeOf(t, frame); outcome != outcomeNotApplied {
		t.Errorf("%s says write_outcome %q, want %q", what, outcome, outcomeNotApplied)
	}
}

// loggedCounts is how many entries of each mutation for bucket every running
// node's log holds.
func loggedCounts(t *testing.T, tc *testCluster, bucket string) map[string]int {
	t.Helper()
	counts := make(map[string]int)
	for index, node := range tc.nodes {
		if node.server == nil {
			continue
		}
		for _, mutation := range missingBucketMutations(bucket) {
			counts[fmt.Sprintf("node %d %s", index+1, mutation.name)] = walOpCount(t, node.server.core.wal, mutation.op, bucket)
		}
	}
	return counts
}

// requireNothingLoggedSince fails for any entry of a mutation of bucket that a
// node logged since before.
func requireNothingLoggedSince(t *testing.T, tc *testCluster, bucket string, before map[string]int) {
	t.Helper()
	for where, count := range loggedCounts(t, tc, bucket) {
		if count != before[where] {
			t.Errorf("%s of bucket %q: %d entries logged, %d before the refused writes", where, bucket, count, before[where])
		}
	}
}

// refuseEveryMutationNamingRemovedNode sends every mutation of each name to
// node index and requires the removed owner's retry, with nothing logged.
func refuseEveryMutationNamingRemovedNode(t *testing.T, tc *testCluster, index int, removedID string, names ...string) {
	t.Helper()
	for _, name := range names {
		before := loggedCounts(t, tc, name)
		for _, mutation := range missingBucketMutations(name) {
			frame := tc.frame(index, mutation.op, mutation.req)
			retryNamingRemovedNode(t, fmt.Sprintf("a %s of %s inside the removal window", mutation.name, name), frame, removedID)
		}
		requireNothingLoggedSince(t, tc, name, before)
	}
}

// TestAWriteOfARemovedOwnersNameAtALoneSurvivor is BUG-0693 at the one-member
// door. The survivor's take-over is busy with the first bucket the removed
// node holds, while the removed node's hand-off is held: every mutation of a
// second bucket whose only copy is on the removed node, and of a name it owned
// that exists nowhere, is the retry naming it, logged nowhere. Once the
// hand-off goes through the second bucket lands and a put applies, and the
// never-created name is not found with the note. Guard: answerUnheldDataOp
// leaves a name the busy worker recorded nothing for to the handler, which
// answers "not found ... create it" without the note.
func TestAWriteOfARemovedOwnersNameAtALoneSurvivor(t *testing.T) {
	s := restartedLoneSurvivorWith(t, loneSurvivorSetup{second: true})
	survivorID, removedID := s.tc.addrs[0], s.tc.addrs[1]
	survivor := clusterOf(s.tc, 0)
	busy := holdTakeOverOf(t, survivorID, s.bucket)
	s.tc.frame(0, transport.OpGet, FindRequest{Database: testDatabase, Bucket: s.bucket, Key: "k00"})
	busy.waitReached(t, "the survivor's take-over of the first bucket")
	nowhere := bucketNamed(t, "never", func(name string) bool { return survivor.ownedByARemovedNode(placementOf(testDatabase, name)) })

	refuseEveryMutationNamingRemovedNode(t, s.tc, 0, removedID, s.second, nowhere)

	busy.release()
	s.refuse.Store(false)
	if !holdsWithin(takeOverWait, func() bool { return holdsKeyIn(s.tc.nodes[0], testDatabase, s.second, "s0") }) {
		t.Fatalf("the survivor never took %s over once the hand-off went through", s.second)
	}
	if !holdsWithin(takeOverWait, func() bool {
		return !survivor.readWindowOpen(survivor.Epoch()) && survivor.awaitedRemovedOwnerOf(placementOf(testDatabase, nowhere)) == ""
	}) {
		t.Fatalf("the survivor's window never closed once the removed node's hand-off went through")
	}
	s.tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: s.second, Key: "after", Data: map[string]interface{}{"n": 1}})
	for _, mutation := range missingBucketMutations(nowhere) {
		missNamingRemovedNode(t, fmt.Sprintf("a %s of a never-created name after the hand-off", mutation.name),
			s.tc.frame(0, mutation.op, mutation.req), removedID)
	}
}

// TestAWriteOfARemovedOwnersNameAtARoutedOwner is BUG-0693 at a routed owner:
// three nodes at factor 1, the owner of a bucket removed while it runs, its
// asks held so its hand-off has not reached the successor, and the successor's
// take-over paused. Every mutation of that bucket, and of a never-created name
// the removed node owned and the successor owns now, is the retry naming the
// removed node, logged nowhere. Once the hand-off goes through, the bucket's
// put applies and the never-created name is not found with the note. Guard:
// fenceUnheldBucket answers missingBucketErr, the join fence's "has no local
// copy" retry, which names no node and never ends.
func TestAWriteOfARemovedOwnersNameAtARoutedOwner(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	t.Cleanup(tc.stop)
	removedID := tc.addrs[0]
	before := ringWith(t, 1, tc.addrs...)
	after := ringWith(t, 1, tc.addrs[1], tc.addrs[2])
	bucket := bucketNamed(t, "r8write", func(name string) bool { return ownerOn(before, name) == removedID })
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, bucket, 1)
	tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "held",
		Data: map[string]interface{}{"n": 1}, WriteConcern: "all"})
	successor := nodeIndexByAddr(t, tc.addrs, ownerOn(after, bucket))
	nowhere := bucketNamed(t, "r8never", func(name string) bool {
		return ownerOn(before, name) == removedID && ownerOn(after, name) == tc.addrs[successor]
	})

	var refuse atomic.Bool
	refuse.Store(true)
	hook := func(fromID, _, _, _ string) error {
		if fromID == removedID && refuse.Load() {
			return errors.New("test: the removed node's ask is held")
		}
		return nil
	}
	askNodeToMigrateHook.Store(&hook)
	t.Cleanup(func() { askNodeToMigrateHook.Store(nil) })
	resume := pauseWorkerOf(t, clusterOf(tc, successor))
	removeAndTell(t, tc, successor, 0)
	via, other := clusterOf(tc, successor), 3-successor
	if err := clusterOf(tc, other).reconcileMembership(via.self.ID, via.memberNodes(), via.memberIDs(), via.Epoch()); err != nil {
		t.Fatalf("bringing node %d to the removal: %v", other+1, err)
	}

	refuseEveryMutationNamingRemovedNode(t, tc, successor, removedID, bucket, nowhere)

	refuse.Store(false)
	resume()
	via.signalTakeOver()
	if !holdsWithin(takeOverWait, func() bool { return foundThrough(tc, successor, testDatabase, bucket, "held") }) {
		t.Fatalf("\"held\" never read back through the successor once the hand-off went through")
	}
	if !holdsWithin(takeOverWait, func() bool {
		return !via.readWindowOpen(via.Epoch()) && via.awaitedRemovedOwnerOf(placementOf(testDatabase, nowhere)) == ""
	}) {
		t.Fatalf("the successor's window never closed once the removed node's hand-off went through")
	}
	tc.ok(successor, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "after", Data: map[string]interface{}{"n": 1}})
	for _, mutation := range missingBucketMutations(nowhere) {
		missNamingRemovedNode(t, fmt.Sprintf("a %s of a never-created name after the hand-off", mutation.name),
			tc.frame(successor, mutation.op, mutation.req), removedID)
	}
}

// TestAfterTheWindowAFreshTakeOverRoundDoesNotTurnAMissIntoARetry is the
// verifier's observation on BUG-0692, pinned under BUG-0693: after the removed
// node's hand-off has reached a lone survivor and its window has closed, a get
// and a put of a never-created name the removed node owned are the miss with
// the note (R8), even while the take-over round the request wakes is still
// looking for a copy. The round is held at its question to the removed node,
// which makes the one-in-150 interleaving certain. Guard: the one-member door
// waits on the round it woke, sees its claim, and answers the hand-off's
// "moving to this node ... Retry in a moment" instead of not found.
func TestAfterTheWindowAFreshTakeOverRoundDoesNotTurnAMissIntoARetry(t *testing.T) {
	s := restartedLoneSurvivor(t)
	survivorID, removedID := s.tc.addrs[0], s.tc.addrs[1]
	survivor := clusterOf(s.tc, 0)
	nowhere := bucketNamed(t, "never", func(name string) bool { return survivor.ownedByARemovedNode(placementOf(testDatabase, name)) })
	s.refuse.Store(false)
	if !holdsWithin(takeOverWait, func() bool { return s.tc.nodes[0].server.core.bucketStoreOpen(testDatabase, s.bucket) }) {
		t.Fatalf("setup: the survivor never took %s over", s.bucket)
	}
	if !holdsWithin(takeOverWait, func() bool {
		return !survivor.readWindowOpen(survivor.Epoch()) && survivor.awaitedRemovedOwnerOf(placementOf(testDatabase, nowhere)) == ""
	}) {
		t.Fatal("setup: the survivor's window never closed once the hand-off reached it")
	}
	held := make(chan struct{})
	t.Cleanup(func() { close(held) })
	hook := func(askerID, nodeID string) error {
		if askerID == survivorID && nodeID == removedID {
			<-held
		}
		return nil
	}
	askHolderHook.Store(&hook)
	t.Cleanup(func() { askHolderHook.Store(nil) })

	missNamingRemovedNode(t, "a get of a never-created name after the window, while a take-over round looks for it",
		s.tc.frame(0, transport.OpGet, FindRequest{Database: testDatabase, Bucket: nowhere, Key: "k"}), removedID)
	missNamingRemovedNode(t, "a put of a never-created name after the window, while a take-over round looks for it",
		s.tc.frame(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: nowhere, Key: "k", Data: map[string]interface{}{"n": 1}}), removedID)
}
