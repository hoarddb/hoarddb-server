package server

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// BUG-0701: a node removed at factor 1 names each bucket it holds to the
// bucket's owner, and tells every member once it has named them all. Both
// facts were tied to the owner of that moment: a second membership change that
// moves the bucket on before its copy lands (the first successor removed too,
// or a join that takes the bucket) left the new owner with the notice and no
// record of the hand-off, so it stopped awaiting the removed node and answered
// reads of the bucket "not found" until the removed node asked it, up to 30
// seconds later. A read must be the retry naming the removed node until the
// copy lands, never "not found" (owner ruling R8).

// secondChangeFixture is a cluster at factor 1 and a bucket whose only copy
// is on R (node 1), which is removed first; S owns the bucket once R is
// removed, and a second membership change moves it on to the new owner before
// S's copy lands. S's copy is held before it opens, and every ask R makes of
// the new owner for the bucket is held until releaseAsksOfNewOwner. doors are
// the members a test reads the bucket through.
type secondChangeFixture struct {
	tc         *testCluster
	bucket     string
	rID        string
	sID        string
	newOwnerID string
	doors      []readDoor

	releaseAsksOfNewOwner func()
}

// readDoor is a member a read is sent through.
type readDoor struct {
	what  string
	frame func(op transport.Opcode, req any) *transport.Frame
}

// doorAt is the read door of node index.
func doorAt(tc *testCluster, what string, index int) readDoor {
	return readDoor{what: what, frame: func(op transport.Opcode, req any) *transport.Frame { return tc.frame(index, op, req) }}
}

// errTestLostAnswer is the seam's loss of a removed node's first answer about
// its copies.
var errTestLostAnswer = errors.New("test: the removed node's first answer about its copies is lost")

// createHeld creates bucket through node 1 with one acknowledged entry, "held".
func createHeld(t *testing.T, tc *testCluster, bucket string) {
	t.Helper()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, bucket, 1)
	tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "held",
		Data: map[string]interface{}{"n": 1}, WriteConcern: "all"})
}

// newSecondChangeFixture holds R's asks of newOwnerID for bucket, which node 1
// (R) holds the only copy of.
func newSecondChangeFixture(t *testing.T, tc *testCluster, bucket, sID, newOwnerID string) *secondChangeFixture {
	t.Helper()
	f := &secondChangeFixture{tc: tc, bucket: bucket, rID: tc.addrs[0], sID: sID, newOwnerID: newOwnerID}
	release := make(chan struct{})
	var releaseOnce sync.Once
	f.releaseAsksOfNewOwner = func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(f.releaseAsksOfNewOwner)
	setAskNodeToMigrateHook(t, func(fromID, memberID, _, name string) error {
		if fromID == f.rID && memberID == f.newOwnerID && name == bucket {
			<-release
		}
		return nil
	})
	return f
}

// fourNodeSecondRemoval is the four-node layout: R (node 1), S, T (the owner
// once R and S are removed) and U, which proposes both removals, by index.
type fourNodeSecondRemoval struct {
	*secondChangeFixture
	s, owner, u int
}

// newFourNodeSecondRemoval builds the four-node layout for the first name R
// owns: S and T are that name's owners once R, then S, are removed, so every
// port draw has one, read through T and U.
func newFourNodeSecondRemoval(t *testing.T, prefix string) *fourNodeSecondRemoval {
	t.Helper()
	tc := newTestCluster(t, 4, 1)
	t.Cleanup(tc.stop)
	before := ringWith(t, 1, tc.addrs...)
	withoutR := ringWith(t, 1, tc.addrs[1:]...)
	bucket := bucketNamed(t, prefix, func(name string) bool { return ownerOn(before, name) == tc.addrs[0] })
	sID := ownerOn(withoutR, bucket)
	withoutRS := ringWith(t, 1, addrsWithout(tc.addrs, tc.addrs[0], sID)...)
	ownerID := ownerOn(withoutRS, bucket)
	layout := &fourNodeSecondRemoval{s: nodeIndexByAddr(t, tc.addrs, sID), owner: nodeIndexByAddr(t, tc.addrs, ownerID)}
	layout.u = 6 - layout.s - layout.owner
	createHeld(t, tc, bucket)
	layout.secondChangeFixture = newSecondChangeFixture(t, tc, bucket, sID, ownerID)
	layout.doors = []readDoor{doorAt(tc, "a get at T, the bucket's owner", layout.owner), doorAt(tc, "a get through U", layout.u)}
	return layout
}

// holdSuccessorsCopy makes S's take-over of the bucket decide through the
// fallback (R's first answer about its copies is lost, as in BUG-0688's test)
// and then hold before it copies the bucket in, so R's ask of S is refused
// with the hand-off recorded and the copy never lands at S.
func (f *secondChangeFixture) holdSuccessorsCopy(t *testing.T) *takeOverHold {
	t.Helper()
	var lostOnce sync.Once
	loseFirst := func(askerID, nodeID string) error {
		lost := false
		if askerID == f.sID && nodeID == f.rID {
			lostOnce.Do(func() { lost = true })
		}
		if lost {
			return errTestLostAnswer
		}
		return nil
	}
	askHolderHook.Store(&loseFirst)
	t.Cleanup(func() { askHolderHook.Store(nil) })
	return holdTakeOverOf(t, f.sID, f.bucket)
}

// tell brings every node in indexes to the proposer's membership, as gossip
// would.
func (f *secondChangeFixture) tell(t *testing.T, proposer *Cluster, indexes ...int) {
	t.Helper()
	for _, index := range indexes {
		if err := clusterOf(f.tc, index).reconcileMembership(proposer.self.ID, proposer.memberNodes(), proposer.memberIDs(), proposer.Epoch()); err != nil {
			t.Fatalf("bringing node %d to epoch %d: %v", index+1, proposer.Epoch(), err)
		}
	}
}

// removeRAndWaitForTheNotice removes R through node via, tells the members in
// others, and waits until S has recorded R's hand-off of the bucket and R's
// notice that it named every bucket has reached every member but S: the state
// BUG-0698's fix leaves behind.
func (f *secondChangeFixture) removeRAndWaitForTheNotice(t *testing.T, copying *takeOverHold, via int, others ...int) {
	t.Helper()
	removeAndTell(t, f.tc, via, 0)
	f.tell(t, clusterOf(f.tc, via), others...)
	if copying != nil {
		copying.waitReached(t, "S's take-over of the removed node's bucket")
	}
	successor := clusterOf(f.tc, nodeIndexByAddr(t, f.tc.addrs, f.sID))
	key := placementOf(testDatabase, f.bucket)
	if !holdsWithin(takeOverWait, func() bool { return successor.handedOffBy(key) == f.rID }) {
		t.Fatalf("setup: S never recorded the removed node's hand-off of %s", f.bucket)
	}
	for index := range f.tc.nodes {
		if index == 0 || f.tc.addrs[index] == f.sID {
			continue
		}
		c := clusterOf(f.tc, index)
		if !holdsWithin(takeOverWait, func() bool { return heardNamedAll(c, f.rID) }) {
			t.Fatalf("setup: the removed node's notice that it named every bucket never reached node %d", index+1)
		}
	}
}

// heardNamedAll reports whether c holds removedID's notice that it named every
// bucket it holds.
func heardNamedAll(c *Cluster, removedID string) bool {
	c.removedHandOffMu.Lock()
	defer c.removedHandOffMu.Unlock()
	state, heard := c.removedHandOffs[removedID]
	return heard && state.namedAllAt != 0
}

// readsAreRetryOrEntry reads the bucket through every door and fails on any
// answer but the entry or a retry.
func (f *secondChangeFixture) readsAreRetryOrEntry(t *testing.T, when string) {
	t.Helper()
	get := FindRequest{Database: testDatabase, Bucket: f.bucket, Key: "held"}
	for _, door := range f.doors {
		frame := door.frame(transport.OpGet, get)
		if frame.Opcode == transport.OpError {
			refusedAwaitingCopyOrRetry(t, door.what+" "+when, frame)
			if text := errorFrameText(frame); !strings.Contains(text, "Retry in a moment") {
				t.Errorf("%s %s was answered %q; want a retry", door.what, when, text)
			}
			continue
		}
		foundHeld(t, door.what+" "+when, frame)
	}
}

// landsThroughEveryDoor fails unless the entry reads back through every door
// once R's asks of the new owner are released.
func (f *secondChangeFixture) landsThroughEveryDoor(t *testing.T) {
	t.Helper()
	f.releaseAsksOfNewOwner()
	get := FindRequest{Database: testDatabase, Bucket: f.bucket, Key: "held"}
	for _, door := range f.doors {
		if !holdsWithin(takeOverWait, func() bool {
			frame := door.frame(transport.OpGet, get)
			var found FindResponse
			return frame.Opcode != transport.OpError && bson.Unmarshal(frame.Payload, &found) == nil && found.Found
		}) {
			t.Errorf("\"held\" in %s never read back (%s) once the removed node's ask of its new owner went through", f.bucket, door.what)
		}
	}
}

// removeSAndTell removes S through U and brings R, S and T to the removal.
func (f *fourNodeSecondRemoval) removeSAndTell(t *testing.T) {
	t.Helper()
	proposer := clusterOf(f.tc, f.u)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := proposer.proposeRemoval(ctx, f.sID); err != nil {
		t.Fatalf("removing S through U: %v", err)
	}
	f.tell(t, proposer, 0, f.s, f.owner)
	if !clusterOf(f.tc, f.owner).actsAsOwner(placementOf(testDatabase, f.bucket)) {
		t.Fatalf("setup: T does not own %s after S's removal", f.bucket)
	}
}

// removeR removes R through U and waits for the state BUG-0698's fix leaves.
func (f *fourNodeSecondRemoval) removeR(t *testing.T) {
	t.Helper()
	f.removeRAndWaitForTheNotice(t, f.holdSuccessorsCopy(t), f.u, f.s, f.owner)
}

// TestAHandOffTheFirstSuccessorTookIsStillAwaitedAfterItIsRemoved is BUG-0701:
// four nodes at factor 1. R is removed; S, the bucket's new owner, records R's
// hand-off but its copy is held before it lands; R's notice that it named
// every bucket reaches T and U. Then S is removed too, which moves the bucket
// to T, and R's ask of T is held. A read of the bucket through T and through U
// must be a retry until the copy lands, then the entry. Red before the fix: T
// held R's notice and no record of the hand-off (it went with S), so T and U
// settled the second removal, the window closed, and both reads answered
// `bucket "…" not found … to create it. This name was placed on … before …
// was removed`. Guard: a notice counts only for the epoch it was given at.
func TestAHandOffTheFirstSuccessorTookIsStillAwaitedAfterItIsRemoved(t *testing.T) {
	f := newFourNodeSecondRemoval(t, "s0701")
	f.removeR(t)
	f.removeSAndTell(t)

	owner := clusterOf(f.tc, f.owner)
	closed := holdsWithin(3*time.Second, func() bool { return !owner.readWindowOpen(owner.Epoch()) })
	t.Logf("the settling window at T closed before the reads: %v", closed)
	f.readsAreRetryOrEntry(t, "while the removed node's ask of T is held")
	f.landsThroughEveryDoor(t)
}

// TestAHandOffIsStillAwaitedAfterAJoinMovesTheBucket is BUG-0701's join half:
// three nodes at factor 1. R is removed; S records R's hand-off but its
// take-over is paused, so the copy does not land; R's notice reaches the third
// member. Then a fourth node J joins and takes the bucket from S, S's
// take-over runs again and drops its record (the bucket is no longer S's),
// and R's ask of J is held. A read through J and through the third member
// must be a retry until the copy lands, then the entry, never "not found".
// Red before the fix: R told J its notice as soon as J was a member, before
// it had named the bucket to J, so J held the notice and no record of the
// hand-off, every member settled, and the reads answered the removed-node
// miss (`database "…" not found … This name was placed on …`).
func TestAHandOffIsStillAwaitedAfterAJoinMovesTheBucket(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	t.Cleanup(tc.stop)
	ports := reserveNodePorts(t)
	joinerID := ports.ringNode().ID
	before := ringWith(t, 1, tc.addrs...)
	withoutR := ringWith(t, 1, tc.addrs[1], tc.addrs[2])
	joined := ringWith(t, 1, tc.addrs[1], tc.addrs[2], joinerID)
	bucket := bucketNamed(t, "j0701", func(name string) bool {
		return ownerOn(before, name) == tc.addrs[0] && ownerOn(joined, name) == joinerID
	})
	successor := nodeIndexByAddr(t, tc.addrs, ownerOn(withoutR, bucket))
	other := 3 - successor
	createHeld(t, tc, bucket)
	f := newSecondChangeFixture(t, tc, bucket, tc.addrs[successor], joinerID)
	// S's take-over is paused: R's ask of S is recorded and refused past the
	// write-concern timeout, which counts as named, and nothing lands at S.
	var pausedS atomic.Bool
	pausedS.Store(true)
	pauseS := func(nodeID string) bool { return nodeID == f.sID && pausedS.Load() }
	takeOverPausedHook.Store(&pauseS)
	t.Cleanup(func() { takeOverPausedHook.Store(nil) })
	f.removeRAndWaitForTheNotice(t, nil, other, successor)

	proposer := clusterOf(tc, other)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := proposer.proposeJoin(ctx, ports.ringNode()); err != nil {
		t.Fatalf("joining J through the third member: %v", err)
	}
	joinerNode := startJoinTarget(t, tc, ports, tc.token)
	t.Cleanup(func() { _ = joinerNode.client.Close() })
	joiner := joinerNode.server.core.cluster
	adoptedByGossip(t, joiner, proposer.Epoch())
	f.tell(t, proposer, 0, successor)
	if !holdsWithin(takeOverWait, func() bool { return joiner.actsAsOwner(placementOf(testDatabase, bucket)) }) {
		t.Fatalf("setup: J does not own %s after its join", bucket)
	}
	// S's take-over runs again on the membership with J, where S no longer
	// owns the bucket: it drops its record of R's hand-off.
	pausedS.Store(false)
	clusterOf(tc, successor).signalTakeOver()
	key := placementOf(testDatabase, bucket)
	if !holdsWithin(takeOverWait, func() bool { return clusterOf(tc, successor).handedOffBy(key) == "" }) {
		t.Fatalf("setup: S kept its record of R's hand-off of %s after the join moved it to J", bucket)
	}
	f.doors = []readDoor{
		{what: "a get at J, the bucket's owner", frame: func(op transport.Opcode, req any) *transport.Frame { return tc.frameOn(joinerNode, op, req) }},
		doorAt(tc, "a get through the third member", other),
	}

	closed := holdsWithin(3*time.Second, func() bool { return !joiner.readWindowOpen(joiner.Epoch()) })
	t.Logf("the settling window at J closed before the reads: %v", closed)
	f.readsAreRetryOrEntry(t, "while the removed node's ask of J is held")
	f.landsThroughEveryDoor(t)
}

// TestACreateInTheGapAfterASecondChangeNeverSupersedesTheRemovedNodesCopy is a
// guard for BUG-0701, not a red test: in the gap of the four-node second
// removal, a create of the bucket at T asks every node removed within the
// history whether it holds a copy (BUG-0625, BUG-0627) and R answers, so the
// create is never answered as a new incarnation and the entry written before
// the removals reads back once the copy lands. It passes on the tree before
// the fix too; it is here so a change to the awaited state cannot make the
// gap lose the copy silently.
func TestACreateInTheGapAfterASecondChangeNeverSupersedesTheRemovedNodesCopy(t *testing.T) {
	f := newFourNodeSecondRemoval(t, "c0701")
	f.removeR(t)
	f.removeSAndTell(t)

	frame := f.tc.frame(f.owner, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: f.bucket, Type: BucketTypeHash})
	t.Logf("a create of %s at T in the gap was answered %s: %s", f.bucket, frame.Opcode, errorFrameText(frame))
	f.landsThroughEveryDoor(t)
}

// TestARefusedAskOfTheNewOwnerDoesNotRaiseTheNotice is BUG-0701's naming
// guard: in the four-node second removal, R's first ask of T after S is
// removed is refused without the hand-off taken (as an ask that reached T
// before T adopted the removal is), so that round leaves the bucket unnamed;
// R's next ask of T is held. The round must not raise R's notice to the new
// epoch: reads through T and U stay a retry until the copy lands. Guard: only
// a round that named every pending bucket to its owner at one epoch raises
// the notice to that epoch.
func TestARefusedAskOfTheNewOwnerDoesNotRaiseTheNotice(t *testing.T) {
	f := newFourNodeSecondRemoval(t, "n0701")
	asked := make(chan struct{})
	release := make(chan struct{})
	var mu sync.Mutex
	asks := 0
	var askedOnce, releaseOnce sync.Once
	f.releaseAsksOfNewOwner = func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(f.releaseAsksOfNewOwner)
	setAskNodeToMigrateHook(t, func(fromID, memberID, _, name string) error {
		if fromID != f.rID || memberID != f.newOwnerID || name != f.bucket {
			return nil
		}
		mu.Lock()
		asks++
		first := asks == 1
		mu.Unlock()
		if first {
			return &migrateRefusedError{message: "node " + memberID + ": seeding this replica of bucket " + name +
				" from its owner (test: the ask reached T before it adopted the removal)"}
		}
		askedOnce.Do(func() { close(asked) })
		<-release
		return nil
	})
	f.removeR(t)
	f.removeSAndTell(t)
	select {
	case <-asked:
	case <-time.After(takeOverWait):
		t.Fatalf("setup: R never asked T for %s a second time", f.bucket)
	}

	owner := clusterOf(f.tc, f.owner)
	closed := holdsWithin(3*time.Second, func() bool { return !owner.readWindowOpen(owner.Epoch()) })
	t.Logf("the settling window at T closed before the reads: %v", closed)
	f.readsAreRetryOrEntry(t, "after R's refused ask of T, while its next ask is held")
	f.landsThroughEveryDoor(t)
}
