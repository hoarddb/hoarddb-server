package server

import (
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/transport"
)

// BUG-0688: a removed node that runs names each bucket it holds to the bucket's
// new owner (removed_handoff.go). Until that bucket lands at the owner, the
// owner knows the removed node holds it: a read there, or through a node that
// forwards to it, is answered from the copy or with the hand-off's retry,
// never "not found" (on-demand pull spec §6.8, owner rulings R7 and R8).

// TestAReadAtTheNewOwnerBeforeARemovedNodesBucketLandsNeverAnswersNotFound is
// BUG-0688: three nodes at factor 1, and the owner of a bucket is removed while
// it runs. Its successor's questions to it about the copies it holds go
// unanswered (the hook), while every other exchange with it works: the
// removed node names the bucket to its successor and answers its probes, so it
// is neither silent nor gone, and the bucket has not landed. A read at the
// successor and one through the other survivor must answer the retry, never
// "not found". Red before the fix: the successor's take-over took the
// unanswered question for "no copy anywhere", told the removed node's ask
// "not found", the ask counted as named, the removed node's notice that it
// had named every bucket let both survivors settle, and once the window had
// closed both reads answered `bucket "…" not found … to create it. This name
// was placed on … before … was removed`. Guard: the successor keeps the
// names a removed node handed it, unlanded, as awaited.
func TestAReadAtTheNewOwnerBeforeARemovedNodesBucketLandsNeverAnswersNotFound(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	t.Cleanup(tc.stop)
	before := ringWith(t, 1, tc.addrs...)
	after := ringWith(t, 1, tc.addrs[1], tc.addrs[2])
	bucket := bucketNamed(t, "r0688", func(name string) bool { return ownerOn(before, name) == tc.addrs[0] })
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, bucket, 1)
	tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "held",
		Data: map[string]interface{}{"n": 1}, WriteConcern: "all"})
	removedID := tc.addrs[0]
	successor := nodeIndexByAddr(t, tc.addrs, ownerOn(after, bucket))
	other := 3 - successor
	successorID := tc.addrs[successor]

	var unheard atomic.Bool
	unheard.Store(true)
	silence := func(askerID, nodeID string) error {
		if unheard.Load() && askerID == successorID && nodeID == removedID {
			return errors.New("test: the removed node's answer about its copies is lost")
		}
		return nil
	}
	askHolderHook.Store(&silence)
	t.Cleanup(func() { askHolderHook.Store(nil) })
	var asks atomic.Int64
	countAsks := func(askerID, _, _, name string) error {
		if askerID == removedID && name == bucket {
			asks.Add(1)
		}
		return nil
	}
	askNodeToMigrateHook.Store(&countAsks)
	t.Cleanup(func() { askNodeToMigrateHook.Store(nil) })

	removeAndTell(t, tc, successor, 0)
	via := clusterOf(tc, successor)
	if err := clusterOf(tc, other).reconcileMembership(via.self.ID, via.memberNodes(), via.memberIDs(), via.Epoch()); err != nil {
		t.Fatalf("bringing node %d to the removal: %v", other+1, err)
	}
	// The second ask comes once the first has been answered and the removed
	// node's pass has backed off: the successor has decided about the bucket
	// at least once by then.
	if !holdsWithin(takeOverWait, func() bool { return asks.Load() >= 2 }) {
		t.Fatalf("setup: the removed node asked its successor to take %s %d times, want at least 2", bucket, asks.Load())
	}
	c := clusterOf(tc, successor)
	settled := holdsWithin(2*time.Second, func() bool { return !c.readWindowOpen(c.Epoch()) })
	t.Logf("the settling window at the successor closed before the reads: %v", settled)

	get := FindRequest{Database: testDatabase, Bucket: bucket, Key: "held"}
	for _, door := range []struct {
		what  string
		index int
	}{{"a get at the successor", successor}, {"a get through the other survivor", other}} {
		frame := tc.frame(door.index, transport.OpGet, get)
		if frame.Opcode == transport.OpError {
			refusedAwaitingCopyOrRetry(t, door.what+" before the removed node's bucket landed", frame)
			if text := errorFrameText(frame); !strings.Contains(text, removedID) {
				t.Errorf("%s was answered %q; want the retry to name the removed node %s", door.what, text, removedID)
			}
			continue
		}
		foundHeld(t, door.what+" before the removed node's bucket landed", frame)
	}

	unheard.Store(false)
	if !holdsWithin(takeOverWait, func() bool { return foundThrough(tc, successor, testDatabase, bucket, "held") }) {
		t.Errorf("\"held\" never read back through the successor once it heard the removed node")
	}
	if !holdsWithin(takeOverWait, func() bool { return c.settled.Load() >= c.Epoch() }) {
		t.Errorf("the successor never settled at epoch %d once the removed node's bucket landed", c.Epoch())
	}
}

// TestASuccessorDoesNotSettleWhileARemovedNodesBucketIsCopying is BUG-0688's
// settle half (owner ruling R7): the removed node names its bucket to its
// successor, whose take-over decides through the fallback (the removed node's
// first answer is lost) and then holds before it copies the bucket in. The
// removed node's ask is refused past the write-concern timeout, which counts
// as named, and its notice that it named every bucket arrives. The successor
// must not report itself settled while the bucket has not landed. Red before
// the fix: a round clears the name when it takes it and a fallback-decided
// copy records no status, so the successor settled mid-copy. Guard: the
// awaited hand-off counts in takeOverUnfound.
func TestASuccessorDoesNotSettleWhileARemovedNodesBucketIsCopying(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	t.Cleanup(tc.stop)
	before := ringWith(t, 1, tc.addrs...)
	after := ringWith(t, 1, tc.addrs[1], tc.addrs[2])
	bucket := bucketNamed(t, "s0688", func(name string) bool { return ownerOn(before, name) == tc.addrs[0] })
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, bucket, 1)
	tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "held",
		Data: map[string]interface{}{"n": 1}, WriteConcern: "all"})
	removedID := tc.addrs[0]
	successor := nodeIndexByAddr(t, tc.addrs, ownerOn(after, bucket))
	other := 3 - successor
	successorID := tc.addrs[successor]

	var questions atomic.Int64
	loseFirst := func(askerID, nodeID string) error {
		if askerID == successorID && nodeID == removedID && questions.Add(1) == 1 {
			return errors.New("test: the removed node's first answer about its copies is lost")
		}
		return nil
	}
	askHolderHook.Store(&loseFirst)
	t.Cleanup(func() { askHolderHook.Store(nil) })
	reached := make(chan struct{})
	release := make(chan struct{})
	var reachedOnce, releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	holdCopy := func(nodeID, _, name string) {
		if nodeID == successorID && name == bucket {
			reachedOnce.Do(func() { close(reached) })
			<-release
		}
	}
	takeOverBeforeOpenHook.Store(&holdCopy)
	t.Cleanup(func() { takeOverBeforeOpenHook.Store(nil) })

	removeAndTell(t, tc, successor, 0)
	via := clusterOf(tc, successor)
	if err := clusterOf(tc, other).reconcileMembership(via.self.ID, via.memberNodes(), via.memberIDs(), via.Epoch()); err != nil {
		t.Fatalf("bringing node %d to the removal: %v", other+1, err)
	}
	select {
	case <-reached:
	case <-time.After(takeOverWait):
		t.Fatalf("setup: the successor's take-over of %s never reached its copy", bucket)
	}
	c := clusterOf(tc, successor)
	if !holdsWithin(takeOverWait, func() bool {
		c.removedHandOffMu.Lock()
		defer c.removedHandOffMu.Unlock()
		state, heard := c.removedHandOffs[removedID]
		return heard && state.namedAllAt != 0
	}) {
		t.Fatalf("setup: the removed node's notice that it named every bucket never reached its successor")
	}
	if holdsWithin(2*time.Second, func() bool { return c.settled.Load() >= c.Epoch() }) {
		t.Errorf("the successor reported itself settled at epoch %d while its copy of %s, which the removed node handed it, had not landed",
			c.Epoch(), bucket)
	}
	releaseOnce.Do(func() { close(release) })
	if !holdsWithin(takeOverWait, func() bool { return foundThrough(tc, successor, testDatabase, bucket, "held") }) {
		t.Errorf("\"held\" never read back through the successor once its copy ran")
	}
	if !holdsWithin(takeOverWait, func() bool { return c.settled.Load() >= c.Epoch() }) {
		t.Errorf("the successor never settled at epoch %d once the removed node's bucket landed", c.Epoch())
	}
}

// TestARemovedNodeThatDiesAfterItsHandOffEndsTheWaitWithinTheBound is owner
// ruling R8 after BUG-0688: the removed node hands its bucket to its successor,
// which cannot hear its answer about the copy (as in the read test above),
// and then dies. The successor waits for it no longer than the removed-node
// bound: it settles, and a get of the bucket is the ordinary miss that names
// the removed node, never a retry forever. Guard: the probe that runs on while
// a handed bucket is awaited, and the entries it forgets once it finds the
// node gone.
func TestARemovedNodeThatDiesAfterItsHandOffEndsTheWaitWithinTheBound(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	t.Cleanup(tc.stop)
	before := ringWith(t, 1, tc.addrs...)
	after := ringWith(t, 1, tc.addrs[1], tc.addrs[2])
	bucket := bucketNamed(t, "g0688", func(name string) bool { return ownerOn(before, name) == tc.addrs[0] })
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, bucket, 1)
	tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "held",
		Data: map[string]interface{}{"n": 1}, WriteConcern: "all"})
	removedID := tc.addrs[0]
	successor := nodeIndexByAddr(t, tc.addrs, ownerOn(after, bucket))
	other := 3 - successor
	successorID := tc.addrs[successor]

	silence := func(askerID, nodeID string) error {
		if askerID == successorID && nodeID == removedID {
			return errors.New("test: the removed node's answer about its copies is lost")
		}
		return nil
	}
	askHolderHook.Store(&silence)
	t.Cleanup(func() { askHolderHook.Store(nil) })
	c := clusterOf(tc, successor)
	key := placementOf(testDatabase, bucket)

	removeAndTell(t, tc, successor, 0)
	via := clusterOf(tc, successor)
	if err := clusterOf(tc, other).reconcileMembership(via.self.ID, via.memberNodes(), via.memberIDs(), via.Epoch()); err != nil {
		t.Fatalf("bringing node %d to the removal: %v", other+1, err)
	}
	if !holdsWithin(takeOverWait, func() bool { return c.handedOffBy(key) == removedID }) {
		t.Fatalf("setup: the removed node never handed %s to its successor", bucket)
	}
	// The node dies after its notice has arrived and a probe has found it
	// answering since: what ended the probe before.
	if !holdsWithin(takeOverWait, func() bool {
		c.removedHandOffMu.Lock()
		defer c.removedHandOffMu.Unlock()
		state, heard := c.removedHandOffs[removedID]
		return heard && state.namedAllAt != 0
	}) {
		t.Fatalf("setup: the removed node's notice that it named every bucket never reached its successor")
	}
	time.Sleep(removedNodeAnswerWait + time.Second)
	tc.stopNodeKeepingData(0)
	began := time.Now()
	if !holdsWithin(takeOverWait, func() bool { return c.settled.Load() >= c.Epoch() }) {
		t.Fatalf("the successor never settled at epoch %d once the removed node died", c.Epoch())
	}
	if took := time.Since(began); took > 2*removedNodeAnswerWait+5*time.Second {
		t.Errorf("the successor settled %s after the removed node died; it waits about %s for a removed node that does not answer", took, removedNodeAnswerWait)
	}
	frame := tc.frame(successor, transport.OpGet, FindRequest{Database: testDatabase, Bucket: bucket, Key: "held"})
	missNamingRemovedNode(t, "a get of the bucket whose only copy is on the dead removed node, after the bound", frame, removedID)
}
