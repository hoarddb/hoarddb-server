package server

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// BUG-0513: the owner's read gate looked only at the placement marks, so a
// bucket that moved to this node with no mark was served for reads from a copy
// this node had not taken over, which can lack a write the previous owner
// acknowledged. The gate now also reads the copy's lineage, as the write gate
// does, without the bucket's lock.

// movedBucket is a bucket whose ownership a removal moved: its old owner, the
// node that owns it after the removal, and the other survivor.
type movedBucket struct {
	tc                           *testCluster
	key                          placementKey
	oldOwner, newOwner, survivor int
}

// futureOwnerAfterRemoving is the index of the node that owns key on the ring
// of every node but removed.
func futureOwnerAfterRemoving(t *testing.T, tc *testCluster, key placementKey, removed int) int {
	t.Helper()
	survivors := ring.New(3)
	for i, addr := range tc.addrs {
		if i == removed {
			continue
		}
		if err := survivors.AddNode(ring.Node{ID: addr, Addr: addr}); err != nil {
			t.Fatalf("setup: building the survivors' ring: %v", err)
		}
	}
	next, err := survivors.Replicas(key.encoded, 3)
	if err != nil {
		t.Fatalf("setup: placing the bucket on the survivors' ring: %v", err)
	}
	return nodeIndexByAddr(t, tc.addrs, next[0].ID)
}

// removeOldOwner removes the bucket's old owner through another node and
// brings the third node to the removal's epoch.
func (m *movedBucket) removeOldOwner(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	proposer := clusterOf(m.tc, m.survivor)
	if err := proposer.proposeRemoval(ctx, m.tc.addrs[m.oldOwner]); err != nil {
		t.Fatalf("setup: removing the old owner: %v", err)
	}
	if err := clusterOf(m.tc, m.newOwner).reconcileMembership(m.tc.addrs[m.survivor], proposer.memberNodes(), proposer.memberIDs(), proposer.Epoch()); err != nil {
		t.Fatalf("setup: bringing the new owner to the removal's epoch: %v", err)
	}
	if owner, err := proposer.Owner(m.key); err != nil || owner.ID != m.tc.addrs[m.newOwner] {
		t.Fatalf("setup: the owner after the removal is %v (err %v), want %s", owner.ID, err, m.tc.addrs[m.newOwner])
	}
}

// assertReadsRefused asserts that every read of the bucket at its new owner,
// direct or forwarded to it, is refused as a take-over in progress: never an
// answer from the copy it has not taken over.
func (m *movedBucket) assertReadsRefused(t *testing.T, acked string) {
	t.Helper()
	t.Logf("the new owner's marks: %+v", clusterOf(m.tc, m.newOwner).placementMarksOf(m.key))
	assertHandoffRefusal(t, "a get of "+acked, m.tc.frame(m.newOwner, transport.OpGet,
		FindRequest{Database: testDatabase, Bucket: testBucket, Key: acked}))
	assertHandoffRefusal(t, "a scan", m.tc.frame(m.newOwner, transport.OpScan,
		ScanRequest{Database: testDatabase, Bucket: testBucket}))
	assertHandoffRefusal(t, "a count", m.tc.frame(m.newOwner, transport.OpCount,
		CountRequest{Database: testDatabase, Bucket: testBucket}))

	payload, err := bson.Marshal(FindRequest{Database: testDatabase, Bucket: testBucket, Key: acked})
	if err != nil {
		t.Fatalf("encoding the forwarded get: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	survivor := clusterOf(m.tc, m.survivor)
	resp, err := survivor.forward(ctx, mustNode(t, survivor, m.tc.addrs[m.newOwner]),
		ReplEntry{Op: int32(transport.OpGet), Bucket: testBucket, Payload: payload}, 0, readForward{})
	switch {
	case err != nil:
		if !strings.Contains(err.Error(), "is moving to this node") {
			t.Errorf("a get forwarded to the new owner was refused with %v, want the take-over's refusal", err)
		}
	default:
		assertHandoffRefusal(t, "a get forwarded to the new owner", transport.NewFrame(transport.Opcode(resp.Opcode), resp.Payload))
	}
}

// assertHandoffRefusal asserts that frame is the refusal of a read at an
// owner that has not taken the bucket over: a retryable UNAVAILABLE that says
// ownership is moving here.
func assertHandoffRefusal(t *testing.T, what string, frame *transport.Frame) {
	t.Helper()
	if frame.Opcode != transport.OpError {
		t.Errorf("%s at the new owner, which has not taken the bucket over, answered %s %.200q; want the take-over's refusal",
			what, frame.Opcode, frame.Payload)
		return
	}
	refusal := refusalOf(t, frame)
	if transport.ErrorCode(refusal.Code) != transport.ErrUnavailable || !strings.Contains(refusal.Message, "is moving to this node") {
		t.Errorf("%s at the new owner was refused with code %d %q, want UNAVAILABLE saying ownership is moving to this node",
			what, refusal.Code, refusal.Message)
	}
}

// foundAt reports whether node index answers a get of key with the document.
func foundAt(t *testing.T, tc *testCluster, index int, key string) (bool, string) {
	t.Helper()
	frame := tc.frame(index, transport.OpGet, FindRequest{Database: testDatabase, Bucket: testBucket, Key: key})
	if frame.Opcode == transport.OpError {
		return false, string(frame.Payload)
	}
	var answer FindResponse
	if err := bson.Unmarshal(frame.Payload, &answer); err != nil {
		t.Fatalf("decoding the get of %s: %v", key, err)
	}
	return answer.Found, answer.Message
}

// unmarkedMovedBucket builds BUG-0513's state as a skipped pass leaves it: the
// old owner acknowledges 'acked' at majority with its stream to the future
// owner frozen, it is removed, the new owner's pass for the removal finishes,
// and the bucket's mark is forgotten. The take-over worker is off throughout.
func unmarkedMovedBucket(t *testing.T) (*movedBucket, func()) {
	t.Helper()
	tc := newTestCluster(t, 3, 3)
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	if frame := tc.writeConcerned(0, "before", "all"); frame.Opcode == transport.OpError {
		t.Fatalf("setup: writing 'before' at all: %s", frame.Payload)
	}
	m := &movedBucket{tc: tc, key: placementOf(testDatabase, testBucket)}
	owner, err := clusterOf(tc, 0).Owner(m.key)
	if err != nil {
		t.Fatalf("setup: reading the owner: %v", err)
	}
	m.oldOwner = nodeIndexByAddr(t, tc.addrs, owner.ID)
	m.newOwner = futureOwnerAfterRemoving(t, tc, m.key, m.oldOwner)
	m.survivor = 3 - m.oldOwner - m.newOwner
	oc := clusterOf(tc, m.oldOwner)
	unfreeze := freezePeer(oc.peerFor(mustNode(t, oc, tc.addrs[m.newOwner])))
	if frame := tc.writeConcerned(m.oldOwner, "acked", "majority"); frame.Opcode == transport.OpError {
		t.Fatalf("setup: writing 'acked' at majority: %s", frame.Payload)
	}
	m.removeOldOwner(t)
	c := clusterOf(tc, m.newOwner)
	waitForPlacementPassOf(t, c)
	if term, opened, err := c.s.bucketLineage(testDatabase, testBucket); err != nil || opened || term == 0 {
		t.Fatalf("setup: the new owner's copy has term %d opened %v (err %v), want a term it has not opened", term, opened, err)
	}
	c.forgetPlacementMarks(m.key)
	return m, func() {
		unfreeze()
		tc.stop()
	}
}

// TestAnUnmarkedMovedBucketIsNotServedForReadsBeforeItsTakeOver is BUG-0513's
// acceptance test: a bucket that moved to this node and carries no placement
// mark is refused for reads, direct and forwarded, never answered "not found"
// for a write the previous owner acknowledged. Once the take-over worker runs,
// the refusal has named the bucket to it, and reads answer again with the
// acknowledged write, with no restart. Guard: return only the marks' answer
// from ownerReadAwaitsTakeOver.
func TestAnUnmarkedMovedBucketIsNotServedForReadsBeforeItsTakeOver(t *testing.T) {
	takeOverOffHook.Store(true)
	defer takeOverOffHook.Store(false)
	m, stop := unmarkedMovedBucket(t)
	defer stop()
	m.assertReadsRefused(t, "acked")
	if t.Failed() {
		return
	}

	// The worker's idle wait is takeOverRetryCeiling (30 s); a read answered
	// well inside it was answered because a refused read woke the worker.
	takeOverOffHook.Store(false)
	deadline := time.Now().Add(takeOverRetryCeiling / 3)
	for {
		found, message := foundAt(t, m.tc, m.newOwner, "acked")
		if found {
			break
		}
		if strings.Contains(message, "document not found") {
			t.Fatalf("a read at the new owner answered %q for the acknowledged 'acked' while it took the bucket over", message)
		}
		if time.Now().After(deadline) {
			t.Fatalf("reads at the new owner still answer %q %s after the take-over worker was let run: the refused reads did not start its take-over",
				message, takeOverRetryCeiling/3)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TestAnOpenedOwnersCopyIsServedForReads: the green path. An owner that opened
// its copy, with no mark, serves reads from it at once. Guard: answer true
// from ownerReadAwaitsTakeOver for every held bucket.
func TestAnOpenedOwnersCopyIsServedForReads(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	if frame := tc.writeConcerned(0, "present", "all"); frame.Opcode == transport.OpError {
		t.Fatalf("setup: writing 'present' at all: %s", frame.Payload)
	}
	key := placementOf(testDatabase, testBucket)
	owner, err := clusterOf(tc, 0).Owner(key)
	if err != nil {
		t.Fatalf("setup: reading the owner: %v", err)
	}
	index := nodeIndexByAddr(t, tc.addrs, owner.ID)
	if found, message := foundAt(t, tc, index, "present"); !found {
		t.Errorf("a get at the owner of its opened copy answered %q, want the document", message)
	}
	// The same when the lineage cache is empty and the gate reads the state
	// document.
	frame := readUnderLockOnAMiss(t, tc, index, "present")
	var answer FindResponse
	if frame.Opcode == transport.OpError {
		t.Errorf("on a cache miss, a get at the owner of its opened copy was refused: %s", answerText(frame))
	} else if err := bson.Unmarshal(frame.Payload, &answer); err != nil || !answer.Found {
		t.Errorf("on a cache miss, a get at the owner of its opened copy answered %+v (err %v), want the document", answer, err)
	}
}

// readUnderLockOnAMiss empties node index's lineage cache of the bucket, as
// an attach leaves it, and sends it a get of key with the bucket's lock held,
// as a writer holds it through an apply. It fails the test when the read
// waits on the lock (the bound of TestReadPathsTakeNoBucketLock) or fills the
// cache, which only a writer does, under the lock.
func readUnderLockOnAMiss(t *testing.T, tc *testCluster, index int, key string) *transport.Frame {
	t.Helper()
	served, err := clusterOf(tc, index).s.resolveBucket(testDatabase, testBucket)
	if err != nil {
		t.Fatalf("setup: resolving the bucket: %v", err)
	}
	payload, err := bson.Marshal(FindRequest{Database: testDatabase, Bucket: testBucket, Key: key})
	if err != nil {
		t.Fatalf("encoding the get: %v", err)
	}
	served.rec.mu.Lock()
	defer served.rec.mu.Unlock()
	served.rec.lineageFlags.Store(0)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	frame, err := tc.nodes[index].client.SendRequest(ctx, transport.OpGet, payload)
	if err != nil {
		t.Fatalf("on a cache miss, a get did not answer within 5 s while the bucket's lock was held (it waits on the lock): %v", err)
	}
	if served.rec.lineageFlags.Load()&lineageKnown != 0 {
		t.Errorf("the read filled the lineage cache; only a writer, under the bucket's lock, fills it")
	}
	return frame
}

// TestTheReadGateOnACacheMissTakesNoBucketLock: with the new owner's lineage
// cache empty, as an attach leaves it, and the bucket's lock held, a read
// still answers, refused from the state document's lineage, without waiting
// on the lock or filling the cache; the write gate agrees. Guard: read the
// lineage through bucketLineage, which fills the cache under the lock.
func TestTheReadGateOnACacheMissTakesNoBucketLock(t *testing.T) {
	takeOverOffHook.Store(true)
	defer takeOverOffHook.Store(false)
	m, stop := unmarkedMovedBucket(t)
	defer stop()
	assertHandoffRefusal(t, "on a cache miss, a get of acked", readUnderLockOnAMiss(t, m.tc, m.newOwner, "acked"))
	c := clusterOf(m.tc, m.newOwner)
	writeGate, err := c.ownerAwaitsTakeOver(m.key, c.Epoch(), testDatabase, testBucket)
	if err != nil {
		t.Fatalf("the write gate: %v", err)
	}
	if !writeGate {
		t.Errorf("the write gate says the copy is taken over; the gates disagree")
	}
}

// TestTheReadGateFillsItsCacheOnTheFirstMissWithoutWaiting: the read gate
// reads the state document once for a bucket whose lineage cache is empty and
// fills the cache without waiting on the bucket's lock, so the next read is a
// hit (BUG-0566). Guard: leave the cache to the writers, which makes every
// read a miss and allocates on each.
func TestTheReadGateFillsItsCacheOnTheFirstMissWithoutWaiting(t *testing.T) {
	takeOverOffHook.Store(true)
	defer takeOverOffHook.Store(false)
	m, stop := unmarkedMovedBucket(t)
	defer stop()
	c := clusterOf(m.tc, m.newOwner)
	served, err := c.s.resolveBucket(testDatabase, testBucket)
	if err != nil {
		t.Fatalf("setup: resolving the bucket: %v", err)
	}
	// Empty the lineage cache, as an attach leaves it.
	served.rec.lineageFlags.Store(0)

	if _, err := c.ownerReadAwaitsTakeOver(m.key, c.Epoch(), testDatabase, testBucket); err != nil {
		t.Fatalf("the first read: %v", err)
	}
	if served.rec.lineageFlags.Load()&lineageKnown == 0 {
		t.Fatalf("the first read left the lineage cache empty: the next read reads the state document again")
	}

	var readErr error
	allocs := minAllocs(7, 100, func() {
		if _, err := c.ownerReadAwaitsTakeOver(m.key, c.Epoch(), testDatabase, testBucket); err != nil {
			readErr = err
		}
	})
	if readErr != nil {
		t.Fatalf("a read with the cache filled: %v", readErr)
	}
	if allocs != 0 {
		t.Errorf("a read with the filled lineage cache allocated %.1f times per run, want 0: it still reads the state document", allocs)
	}
}

// TestAConsistentGetAtTheOwnerReadsTheStateOnce: versionedRead loads the
// copy's state document from its read's snapshot and hands it to the owner
// read gate, so the gate does not read the same document a second time
// (BUG-0566). Guard: let the gate load its own document on a cache miss.
func TestAConsistentGetAtTheOwnerReadsTheStateOnce(t *testing.T) {
	takeOverOffHook.Store(true)
	defer takeOverOffHook.Store(false)
	m, stop := unmarkedMovedBucket(t)
	defer stop()
	c := clusterOf(m.tc, m.newOwner)
	served, err := c.s.resolveBucket(testDatabase, testBucket)
	if err != nil {
		t.Fatalf("setup: resolving the bucket: %v", err)
	}
	// Empty the lineage cache, as an attach leaves it: the gate would load a
	// document itself if versionedRead did not hand it one.
	served.rec.lineageFlags.Store(0)

	gateLoads := 0
	hook := func(string, string) { gateLoads++ }
	readGateLineageLoadHook.Store(&hook)
	defer readGateLineageLoadHook.Store(nil)

	if _, err := c.s.versionedRead(testDatabase, testBucket, "acked"); err != nil {
		t.Fatalf("the consistent get: %v", err)
	}
	if gateLoads != 0 {
		t.Errorf("the read gate loaded the state document %d times during a consistent get that had already read it, want 0", gateLoads)
	}
}

// TestTheCacheOpenedRuleMatchesTheDocumentRule: the cache keeps the term, the
// opened flag and whether the position is zero, and cacheIsOpened must answer
// as copyIsOpened does from the document for every combination (BUG-0566).
func TestTheCacheOpenedRuleMatchesTheDocumentRule(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	c := clusterOf(tc, 0)
	key := placementOf(testDatabase, testBucket)
	for _, arm := range []struct {
		name    string
		term    uint64
		opened  bool
		through uint64
	}{
		{"term 0, position 0, unopened", 0, false, 0},
		{"term 0, position set, unopened", 0, false, 7},
		{"term set, position 0, unopened", 3, false, 0},
		{"opened", 0, true, 0},
		{"opened with term and position", 3, true, 5},
	} {
		flags := lineageKnown
		if arm.opened {
			flags |= lineageOpened
		}
		if arm.through == 0 {
			flags |= lineagePositionZero
		}
		st := store.BucketState{Term: arm.term, Opened: arm.opened, Through: arm.through}
		want := !c.copyIsOpened(key, st, placementMarks{})
		if got := !c.cacheIsOpened(key, arm.term, flags, placementMarks{}); got != want {
			t.Errorf("%s: the cache says awaits=%v, the document says awaits=%v", arm.name, got, want)
		}
	}
}

// TestACreateReplayedOntoTheNewOwnerAfterItsPassIsNotServedForReads is route
// R1 of the BUG-0513 analysis, end to end with no shortcut: the old owner
// creates the bucket with its stream to the future owner frozen, so the future
// owner holds no copy when its pass for the removal runs and the pass marks
// nothing. The stream is let go after the pass, and the old owner, removed,
// delivers the create in its own term. The new owner's copy is unopened and
// unmarked, and must not be served for reads. Guard: return only the marks'
// answer from ownerReadAwaitsTakeOver.
func TestACreateReplayedOntoTheNewOwnerAfterItsPassIsNotServedForReads(t *testing.T) {
	takeOverOffHook.Store(true)
	defer takeOverOffHook.Store(false)
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	// Every node holds the database before a stream is frozen: an old owner
	// that has not learned it asks the members, and its question to a frozen
	// future owner, which may be the one node that holds it, goes unanswered
	// (BUG-0487; TestACreateAtANodeThatHasNotLearnedItsDatabaseIsRefused-
	// WhileItsHolderIsFrozen).
	waitListedBy(t, tc, testDatabase, 0, 1, 2)
	m := &movedBucket{tc: tc, key: placementOf(testDatabase, testBucket)}
	owner, err := clusterOf(tc, 0).Owner(m.key)
	if err != nil {
		t.Fatalf("setup: reading the owner: %v", err)
	}
	m.oldOwner = nodeIndexByAddr(t, tc.addrs, owner.ID)
	m.newOwner = futureOwnerAfterRemoving(t, tc, m.key, m.oldOwner)
	m.survivor = 3 - m.oldOwner - m.newOwner
	oc := clusterOf(tc, m.oldOwner)
	unfreeze := freezePeer(oc.peerFor(mustNode(t, oc, tc.addrs[m.newOwner])))
	defer unfreeze()
	tc.ok(m.oldOwner, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	if frame := tc.writeConcerned(m.oldOwner, "acked", "majority"); frame.Opcode == transport.OpError {
		t.Fatalf("setup: writing 'acked' at majority: %s", frame.Payload)
	}
	c := clusterOf(tc, m.newOwner)
	if c.s.bucketServed(testDatabase, testBucket) {
		t.Fatalf("setup: the future owner holds the bucket although its old owner's stream to it is frozen")
	}

	m.removeOldOwner(t)
	waitForPlacementPassOf(t, c)
	// The old owner's write is held at the new owner's admission, as a
	// stopped old owner never sends it: the copy lacks the acknowledged write.
	var createdAfterPass, createdBeforePass atomic.Int32
	writeHeld := make(chan struct{})
	defer close(writeHeld)
	admitted := func(at *Cluster, entry ReplEntry) {
		if at != c || entry.Bucket != testBucket {
			return
		}
		if transport.Opcode(entry.Op) == transport.OpPut {
			<-writeHeld
			return
		}
		if transport.Opcode(entry.Op) != transport.OpCreateBucket {
			return
		}
		if at.placementPassFinished() {
			createdAfterPass.Add(1)
		} else {
			createdBeforePass.Add(1)
		}
	}
	afterReplicaAdmittedHook.Store(&admitted)
	defer afterReplicaAdmittedHook.Store(nil)
	unfreeze()
	if !holdsWithin(20*time.Second, func() bool { return c.s.bucketServed(testDatabase, testBucket) }) {
		t.Fatalf("route R1 not reached: the removed old owner did not deliver the create to the new owner within 20 s")
	}
	if createdAfterPass.Load() == 0 || createdBeforePass.Load() != 0 {
		t.Fatalf("route R1 not reached: the create was admitted %d times after the new owner's pass and %d before",
			createdAfterPass.Load(), createdBeforePass.Load())
	}
	if marks := c.placementMarksOf(m.key); marks.Moved != 0 {
		t.Fatalf("route R1 not reached: the copy the replay landed carries a mark %+v", marks)
	}
	if term, opened, err := c.s.bucketLineage(testDatabase, testBucket); err != nil || opened || term == 0 {
		t.Fatalf("route R1 not reached: the landed copy has term %d opened %v (err %v), want a term it has not opened", term, opened, err)
	}
	stored, err := c.s.Find(FindRequest{Database: testDatabase, Bucket: testBucket, Key: "acked"})
	if err != nil {
		t.Fatalf("setup: reading the new owner's own copy: %v", err)
	}
	if stored.Found {
		t.Fatalf("setup: the new owner's copy holds 'acked', whose delivery the test holds")
	}
	m.assertReadsRefused(t, "acked")
}

// TestACreateAtANodeThatHasNotLearnedItsDatabaseIsRefusedWhileItsHolderIsFrozen
// (BUG-0487) forces the order that failed the setup of
// TestACreateReplayedOntoTheNewOwnerAfterItsPassIsNotServedForReads: the
// future owner is node 1, the only node that holds the database (its streams
// to the other two are held), and the old owner has not learned it when its
// stream to the future owner is frozen. The
// create is refused, as designed: the old owner asks the members, and the
// frozen one, the holder, does not answer. Once the old owner lists the
// database, the wait that setup now makes, the same create goes through.
func TestACreateAtANodeThatHasNotLearnedItsDatabaseIsRefusedWhileItsHolderIsFrozen(t *testing.T) {
	takeOverOffHook.Store(true)
	defer takeOverOffHook.Store(false)
	tc := newTestClusterPlaced(t, 3, 3, func(addrs []string) bool {
		owner := ownerOn(ringWith(t, 3, addrs...), testBucket)
		if owner == addrs[0] {
			return false
		}
		survivors := make([]string, 0, 2)
		for _, addr := range addrs {
			if addr != owner {
				survivors = append(survivors, addr)
			}
		}
		return ownerOn(ringWith(t, 3, survivors...), testBucket) == addrs[0]
	})
	defer tc.stop()
	key := placementOf(testDatabase, testBucket)
	owner, err := clusterOf(tc, 0).Owner(key)
	if err != nil {
		t.Fatalf("setup: reading the owner: %v", err)
	}
	oldOwner := nodeIndexByAddr(t, tc.addrs, owner.ID)
	if next := futureOwnerAfterRemoving(t, tc, key, oldOwner); next != 0 {
		t.Fatalf("setup: the future owner is node %d, want node 1", next+1)
	}
	survivor := 3 - oldOwner
	learnOldOwner := freezePeer(peerOf(t, tc, 0, oldOwner))
	defer learnOldOwner()
	learnSurvivor := freezePeer(peerOf(t, tc, 0, survivor))
	defer learnSurvivor()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	for _, index := range []int{oldOwner, survivor} {
		if listsDatabase(t, tc.nodes[index], testDatabase) {
			t.Fatalf("setup: node %d lists the database although node 1's stream to it is held", index+1)
		}
	}
	unfreeze := freezePeer(peerOf(t, tc, oldOwner, 0))
	defer unfreeze()
	create := CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash}
	frame := tc.frame(oldOwner, transport.OpCreateBucket, create)
	if frame.Opcode != transport.OpError || !strings.Contains(answerText(frame), "has not learned database") {
		t.Fatalf("a create at a node that has not learned its database, with the holder frozen, answered %s %s; want the refusal the setup met",
			frame.Opcode, answerText(frame))
	}
	learnOldOwner()
	learnSurvivor()
	waitListedBy(t, tc, testDatabase, oldOwner)
	tc.ok(oldOwner, transport.OpCreateBucket, create)
}
