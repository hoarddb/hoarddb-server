package server

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/transport"
)

// BUG-0702: a removed node's notice that it has named every bucket it holds
// was sent to each member once, and a member keeps it in memory only. A
// member restarted after the notice and before it settled the removal forgot
// it, and the removed node, which counted that member as told, never told it
// again: the member awaited the removed node for as long as it answered, its
// settle mark stayed down, and a read of a never-created name the removed
// node owned answered a retry for good instead of R8's miss.

// TestASuccessorRestartedBeforeItsCopyLandsIsToldTheNoticeAgain is BUG-0702
// at the successor: three nodes at factor 1. R is removed; its successor S
// records R's hand-off of a bucket, but S's take-over is paused so the copy
// does not land, and R's notice reaches S. S is restarted, keeping its data,
// and its take-over runs again; R asks again and the bucket lands. S must then
// settle the removal, and a get of a never-created name R owned must be the
// miss naming R. Red on the tree before BUG-0701's fix: S forgot the notice in
// the restart, R had already counted S as told and ended its pass once the
// bucket landed, so S never settled and the get answered the retry naming R.
// BUG-0701's fix already turns it green (the notice's rise to namedAllEnded
// tells every member again); the third-member test below is the one only the
// probe's answer fixes.
func TestASuccessorRestartedBeforeItsCopyLandsIsToldTheNoticeAgain(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	t.Cleanup(tc.stop)
	before := ringWith(t, 1, tc.addrs...)
	after := ringWith(t, 1, tc.addrs[1], tc.addrs[2])
	bucket := bucketNamed(t, "r0702", func(name string) bool { return ownerOn(before, name) == tc.addrs[0] })
	createHeld(t, tc, bucket)
	removedID := tc.addrs[0]
	successor := nodeIndexByAddr(t, tc.addrs, ownerOn(after, bucket))
	other := 3 - successor
	successorID := tc.addrs[successor]

	var paused atomic.Bool
	paused.Store(true)
	pauseSuccessor := func(nodeID string) bool { return nodeID == successorID && paused.Load() }
	takeOverPausedHook.Store(&pauseSuccessor)
	t.Cleanup(func() { takeOverPausedHook.Store(nil) })

	removeAndTell(t, tc, other, 0)
	via := clusterOf(tc, other)
	if err := clusterOf(tc, successor).reconcileMembership(via.self.ID, via.memberNodes(), via.memberIDs(), via.Epoch()); err != nil {
		t.Fatalf("bringing the successor to the removal: %v", err)
	}
	key := placementOf(testDatabase, bucket)
	if !holdsWithin(takeOverWait, func() bool { return clusterOf(tc, successor).handedOffBy(key) == removedID }) {
		t.Fatalf("setup: the successor never recorded the removed node's hand-off of %s", bucket)
	}
	if !holdsWithin(takeOverWait, func() bool { return heardNamedAll(clusterOf(tc, successor), removedID) }) {
		t.Fatalf("setup: the removed node's notice that it named every bucket never reached the successor")
	}
	if c := clusterOf(tc, successor); c.settled.Load() >= c.Epoch() {
		t.Fatalf("setup: the successor settled epoch %d before its restart, with the bucket's copy not landed", c.Epoch())
	}

	tc.restartNode(successor)
	paused.Store(false)
	c := clusterOf(tc, successor)
	if heardNamedAll(c, removedID) {
		t.Fatalf("setup: the successor remembered the removed node's notice across its restart")
	}
	if !holdsWithin(takeOverWait, func() bool { return foundThrough(tc, successor, testDatabase, bucket, "held") }) {
		t.Fatalf("setup: \"held\" in %s never read back through the restarted successor", bucket)
	}

	// Two probe intervals and the hand-off's back-off are ample: the removed
	// node answers every probe.
	if !holdsWithin(20*time.Second, func() bool { return c.settled.Load() >= c.Epoch() }) {
		t.Errorf("the successor, restarted after the removed node's notice, never settled epoch %d (settled %d) once the bucket landed",
			c.Epoch(), c.settled.Load())
	}
	nowhere := ""
	for i := 0; nowhere == ""; i++ {
		if name := fmt.Sprintf("never-made-%d", i); c.ownedByARemovedNode(placementOf(testDatabase, name)) && ownerOn(after, name) == successorID {
			nowhere = name
		}
		if i > 4096 {
			t.Fatal("setup: no name in 4096 tries was owned by the removed node and then by the successor")
		}
	}
	get := FindRequest{Database: testDatabase, Bucket: nowhere, Key: "k"}
	if !holdsWithin(10*time.Second, func() bool {
		text := errorFrameText(tc.frame(successor, transport.OpGet, get))
		return strings.Contains(text, "not found") && strings.Contains(text, removedID)
	}) {
		frame := tc.frame(successor, transport.OpGet, get)
		t.Errorf("a get at the restarted successor of %s, a name the removed node owned that exists nowhere, was answered %s: %q; "+
			"want the miss naming %s", nowhere, frame.Opcode, errorFrameText(frame), removedID)
	}
}

// TestAMemberRestartedAfterTheRemovedNodesNoticeStillSettlesTheRemoval is
// BUG-0702: three nodes at factor 1, and two buckets only R holds: one moves
// to S, one to the third member M. Both survivors' take-overs are paused, so
// neither copy lands and R's pass stays pending; R names both and its notice
// reaches M. M is restarted, keeping its data, and its take-over runs again;
// S's stays paused, so R's pass never ends. M must settle the removal once its
// bucket lands, and a get at M of a never-created name R owned (and M owns
// now) must be the miss naming R. Red before the fix: M forgot the notice in
// its restart, R had counted M as told and only tells again when its notice
// rises, which it does not while S's bucket is pending, so M awaited R for as
// long as R ran, never settled, and the get answered the retry naming R.
// Guard: a probe from a member without the notice makes R name that member's
// buckets to it again and tell it again.
func TestAMemberRestartedAfterTheRemovedNodesNoticeStillSettlesTheRemoval(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	t.Cleanup(tc.stop)
	before := ringWith(t, 1, tc.addrs...)
	after := ringWith(t, 1, tc.addrs[1], tc.addrs[2])
	toS := bucketNamed(t, "s0702", func(name string) bool { return ownerOn(before, name) == tc.addrs[0] })
	successor := nodeIndexByAddr(t, tc.addrs, ownerOn(after, toS))
	member := 3 - successor
	memberID, successorID, removedID := tc.addrs[member], tc.addrs[successor], tc.addrs[0]
	toM := bucketNamed(t, "m0702", func(name string) bool {
		return ownerOn(before, name) == removedID && ownerOn(after, name) == memberID
	})
	createHeld(t, tc, toS)
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: toM, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, toM, 1)
	tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: toM, Key: "held",
		Data: map[string]interface{}{"n": 1}, WriteConcern: "all"})

	var memberPaused atomic.Bool
	memberPaused.Store(true)
	pause := func(nodeID string) bool { return nodeID == successorID || nodeID == memberID && memberPaused.Load() }
	takeOverPausedHook.Store(&pause)
	t.Cleanup(func() { takeOverPausedHook.Store(nil) })

	removeAndTell(t, tc, successor, 0)
	via := clusterOf(tc, successor)
	if err := clusterOf(tc, member).reconcileMembership(via.self.ID, via.memberNodes(), via.memberIDs(), via.Epoch()); err != nil {
		t.Fatalf("bringing the third member to the removal: %v", err)
	}
	for _, recorded := range []struct {
		index  int
		bucket string
	}{{successor, toS}, {member, toM}} {
		c, key := clusterOf(tc, recorded.index), placementOf(testDatabase, recorded.bucket)
		if !holdsWithin(takeOverWait, func() bool { return c.handedOffBy(key) == removedID }) {
			t.Fatalf("setup: node %d never recorded the removed node's hand-off of %s", recorded.index+1, recorded.bucket)
		}
	}
	if !holdsWithin(takeOverWait, func() bool { return heardNamedAll(clusterOf(tc, member), removedID) }) {
		t.Fatalf("setup: the removed node's notice that it named every bucket never reached the third member")
	}
	if c := clusterOf(tc, member); c.settled.Load() >= c.Epoch() {
		t.Fatalf("setup: the third member settled epoch %d before its restart, with its bucket's copy not landed", c.Epoch())
	}

	tc.restartNode(member)
	memberPaused.Store(false)
	c := clusterOf(tc, member)
	if heardNamedAll(c, removedID) {
		t.Fatalf("setup: the third member remembered the removed node's notice across its restart")
	}
	if !holdsWithin(takeOverWait, func() bool { return foundThrough(tc, member, testDatabase, toM, "held") }) {
		t.Fatalf("setup: \"held\" in %s never read back through the restarted third member", toM)
	}
	if !holdsWithin(20*time.Second, func() bool { return c.settled.Load() >= c.Epoch() }) {
		t.Errorf("the third member, restarted after the removed node's notice, never settled epoch %d (settled %d) once its bucket landed, "+
			"while the removed node's other bucket is still copying", c.Epoch(), c.settled.Load())
	}
	nowhere := ""
	for i := 0; nowhere == ""; i++ {
		if name := fmt.Sprintf("never-made-%d", i); c.ownedByARemovedNode(placementOf(testDatabase, name)) && ownerOn(after, name) == memberID {
			nowhere = name
		}
		if i > 4096 {
			t.Fatal("setup: no name in 4096 tries was owned by the removed node and then by the third member")
		}
	}
	get := FindRequest{Database: testDatabase, Bucket: nowhere, Key: "k"}
	if !holdsWithin(10*time.Second, func() bool {
		text := errorFrameText(tc.frame(member, transport.OpGet, get))
		return strings.Contains(text, "not found") && strings.Contains(text, removedID)
	}) {
		frame := tc.frame(member, transport.OpGet, get)
		t.Errorf("a get at the restarted third member of %s, a name the removed node owned that exists nowhere, was answered %s: %q; "+
			"want the miss naming %s", nowhere, frame.Opcode, errorFrameText(frame), removedID)
	}
}

// TestARestartedMemberIsNamedItsPendingBucketAgainBeforeItIsToldAgain is
// BUG-0702's re-tell: three nodes at factor 1 and two buckets only R holds,
// one moving to S and one to the third member M; both take-overs are paused,
// so neither copy lands, and R's notice reaches M. M is restarted and its
// take-over stays paused, so its own bucket stays pending. After the restart
// R's first ask of M for that bucket is refused without the hand-off taken
// (as an ask M answers before it is ready is), and the next is held. While it
// is held, M must not hold R's notice: it has no record of the bucket, and
// the notice would let it answer the bucket as missing. Once the ask goes
// through, M must be told the notice, with the bucket recorded first. Red
// without the re-tell: R counted M as told and never tells it again while the
// bucket is pending (answerRemovedProbe's forgot branch), or tells it while
// the bucket is unnamed (retellForgotten's un-naming).
func TestARestartedMemberIsNamedItsPendingBucketAgainBeforeItIsToldAgain(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	t.Cleanup(tc.stop)
	before := ringWith(t, 1, tc.addrs...)
	after := ringWith(t, 1, tc.addrs[1], tc.addrs[2])
	toS := bucketNamed(t, "s0702r", func(name string) bool { return ownerOn(before, name) == tc.addrs[0] })
	successor := nodeIndexByAddr(t, tc.addrs, ownerOn(after, toS))
	member := 3 - successor
	memberID, successorID, removedID := tc.addrs[member], tc.addrs[successor], tc.addrs[0]
	toM := bucketNamed(t, "m0702r", func(name string) bool {
		return ownerOn(before, name) == removedID && ownerOn(after, name) == memberID
	})
	createHeld(t, tc, toS)
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: toM, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, toM, 1)
	tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: toM, Key: "held",
		Data: map[string]interface{}{"n": 1}, WriteConcern: "all"})

	pause := func(nodeID string) bool { return nodeID == successorID || nodeID == memberID }
	takeOverPausedHook.Store(&pause)
	t.Cleanup(func() { takeOverPausedHook.Store(nil) })
	var armed atomic.Bool
	var asks atomic.Int64
	refused := make(chan struct{})
	held := make(chan struct{})
	release := make(chan struct{})
	var refusedOnce, heldOnce, releaseOnce sync.Once
	letGo := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(letGo)
	setAskNodeToMigrateHook(t, func(fromID, toID, _, name string) error {
		if !armed.Load() || fromID != removedID || toID != memberID || name != toM {
			return nil
		}
		switch asks.Add(1) {
		case 1:
			refusedOnce.Do(func() { close(refused) })
			return &migrateRefusedError{message: "node " + toID + ": not ready for bucket " + name + " (test: the ask came before the restarted member was)"}
		case 2:
			heldOnce.Do(func() { close(held) })
			<-release
		}
		return nil
	})

	removeAndTell(t, tc, successor, 0)
	via := clusterOf(tc, successor)
	if err := clusterOf(tc, member).reconcileMembership(via.self.ID, via.memberNodes(), via.memberIDs(), via.Epoch()); err != nil {
		t.Fatalf("bringing the third member to the removal: %v", err)
	}
	keyM := placementOf(testDatabase, toM)
	if !holdsWithin(takeOverWait, func() bool { return clusterOf(tc, member).handedOffBy(keyM) == removedID }) {
		t.Fatalf("setup: the third member never recorded the removed node's hand-off of %s", toM)
	}
	if !holdsWithin(takeOverWait, func() bool { return heardNamedAll(clusterOf(tc, member), removedID) }) {
		t.Fatalf("setup: the removed node's notice that it named every bucket never reached the third member")
	}

	tc.restartNode(member)
	armed.Store(true)
	c := clusterOf(tc, member)
	for _, step := range []struct {
		what string
		ch   chan struct{}
	}{{"refused ask", refused}, {"held ask", held}} {
		select {
		case <-step.ch:
		case <-time.After(takeOverWait):
			t.Fatalf("setup: the removed node never made its %s of %s to the restarted third member", step.what, toM)
		}
	}
	if heardNamedAll(c, removedID) {
		t.Errorf("the restarted third member was told the removed node's notice while %s, which it owns and no longer has a record of, was unnamed "+
			"(the removed node's ask of it was refused and the next one is held)", toM)
	}

	letGo()
	if !holdsWithin(takeOverWait, func() bool { return heardNamedAll(c, removedID) }) {
		t.Fatalf("the restarted third member was never told the removed node's notice again once %s was named to it, "+
			"while that bucket stays pending", toM)
	}
	if handing := c.handedOffBy(keyM); handing != removedID {
		t.Errorf("the restarted third member holds the removed node's notice but its record of %s's hand-off is %q; want %s", toM, handing, removedID)
	}
}
