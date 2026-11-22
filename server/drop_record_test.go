package server

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Dispatch 10a, step 10a.5 (R8-C): a drop names the
// incarnation it removes, reaches every member, clears what is pending for the
// name, and leaves a record that keeps a copy of that incarnation from landing
// again until no node that could hand it back still holds it.

// foundThrough reads key of database/bucket through node index's client door:
// true only when the node answered that the document exists.
func foundThrough(tc *testCluster, index int, database, bucket, key string) bool {
	frame := tc.frame(index, transport.OpGet, FindRequest{Database: database, Bucket: bucket, Key: key})
	if frame.Opcode == transport.OpError {
		return false
	}
	var resp FindResponse
	if len(frame.Payload) > 0 {
		if err := bson.Unmarshal(frame.Payload, &resp); err != nil {
			return false
		}
	}
	return resp.Found
}

// dropAtOne drops database/bucket through node index at write concern one. A
// node that owns the bucket but has not taken it over refuses the drop with
// the designed "ownership is moving to this node ... Retry in a moment"
// answer (UNAVAILABLE; nothing was stored), so the drop is sent again while
// that answer comes back, as a driver does (BUG-0529).
func dropAtOne(tc *testCluster, index int, database, bucket string) {
	tc.t.Helper()
	frame, _ := dropRetryingAHandOff(tc, index, database, bucket)
	if frame.Opcode == transport.OpError {
		tc.t.Fatalf("node %d answered %s with an error: %s", index+1, transport.OpDeleteBucket, string(frame.Payload))
	}
}

// dropRetryingAHandOff is dropAtOne's send: it returns the last answer and the
// number of sends.
func dropRetryingAHandOff(tc *testCluster, index int, database, bucket string) (*transport.Frame, int) {
	return sendWhileOwnershipMoves(func() *transport.Frame {
		return tc.frame(index, transport.OpDeleteBucket, bson.M{"database": database, "name": bucket, "write_concern": "one"})
	})
}

// isAHandOffRefusal reports whether frame is the refusal handoffInProgress
// builds that ends in "Retry in a moment": UNAVAILABLE, nothing stored.
func isAHandOffRefusal(frame *transport.Frame) bool {
	if frame.Opcode != transport.OpError {
		return false
	}
	var answer ErrorResponse
	if err := bson.Unmarshal(frame.Payload, &answer); err != nil {
		return false
	}
	return transport.ErrorCode(answer.Code) == transport.ErrUnavailable &&
		strings.Contains(answer.Message, "is moving to this node") && strings.Contains(answer.Message, "Retry in a moment")
}

// sendWhileOwnershipMoves calls send until its answer is not that refusal, for
// as long as the suite's usual 15 s wait, and returns the last answer with the
// number of sends. It waits on the condition, the refusal clearing, and
// retries only that refusal: any other error comes straight back.
func sendWhileOwnershipMoves(send func() *transport.Frame) (*transport.Frame, int) {
	var last *transport.Frame
	attempts := 0
	eventually(func() bool {
		last = send()
		attempts++
		return !isAHandOffRefusal(last)
	})
	return last, attempts
}

// shortenSweeps makes sweep retries fast for one test.
func shortenSweeps(t *testing.T, giveUp time.Duration) {
	t.Helper()
	floor, ceiling, gaveUp := sweepRetryFloor.get(), sweepRetryCeiling.get(), sweepRetryGiveUp.get()
	sweepRetryFloor.set(100 * time.Millisecond)
	sweepRetryCeiling.set(500 * time.Millisecond)
	if giveUp > 0 {
		sweepRetryGiveUp.set(giveUp)
	}
	t.Cleanup(func() { sweepRetryFloor.set(floor); sweepRetryCeiling.set(ceiling); sweepRetryGiveUp.set(gaveUp) })
}

// leftOutside is a bucket whose set lost a holder to a join that the holder's
// hand-off could not confirm: the joiner never answers that it holds the
// bucket, so the old holder keeps a copy outside the set.
type leftOutside struct {
	tc      *testCluster
	joiner  *restartableJoiner
	bucket  string
	owner   int
	outside int
	// asked counts the hand-off questions to the joiner about the bucket, by
	// the asking node.
	asked sync.Map
}

func (s *leftOutside) askedBy(index int) int64 {
	count, _ := s.asked.Load(s.tc.addrs[index])
	if count == nil {
		return 0
	}
	return count.(*atomic.Int64).Load()
}

// setUpACopyLeftOutsideTheSet starts three nodes at factor 2, creates a bucket
// whose owner keeps it through a join and whose other holder leaves its set,
// and joins the fourth node with every hand-off of the bucket to it refused.
// giveUp, when non-zero, is how soon a sweep stops retrying.
func setUpACopyLeftOutsideTheSet(t *testing.T, giveUp time.Duration) *leftOutside {
	t.Helper()
	tc := newTestCluster(t, 3, 2)
	ports := reserveNodePorts(t)
	newcomer := ports.ringNode()
	before := ringWith(t, 2, tc.addrs...)
	after := ringWith(t, 2, append(append([]string(nil), tc.addrs...), newcomer.ID)...)
	var outsideID string
	bucket := bucketNamed(t, "left", func(name string) bool {
		key := placementOf(testDatabase, name).encoded
		old, _ := before.Replicas(key, 2)
		joined, _ := after.Replicas(key, 2)
		if old[0].ID != joined[0].ID || joined[1].ID != newcomer.ID {
			return false
		}
		outsideID = old[1].ID
		return true
	})
	s := &leftOutside{tc: tc, bucket: bucket, owner: nodeIndexByAddr(t, tc.addrs, ownerOn(before, bucket)),
		outside: nodeIndexByAddr(t, tc.addrs, outsideID)}

	tc.ok(s.owner, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(s.owner, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, bucket, 2)
	for i := 0; i < 5; i++ {
		tc.ok(s.owner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: fmt.Sprintf("k%d", i),
			Data: map[string]interface{}{"n": i}, WriteConcern: "all"})
	}

	shortenSweeps(t, giveUp)
	refuse := func(fromID, memberID, database, name string) error {
		if memberID != newcomer.ID || name != bucket {
			return nil
		}
		counter, _ := s.asked.LoadOrStore(fromID, &atomic.Int64{})
		counter.(*atomic.Int64).Add(1)
		return errors.New("test: the joiner does not confirm this bucket")
	}
	askNodeToMigrateHook.Store(&refuse)
	t.Cleanup(func() { askNodeToMigrateHook.Store(nil) })
	// The sweeps never settle, so the join freeze would refuse the drop and
	// the create for good.
	joinFreezeOffHook.Store(true)
	t.Cleanup(func() { joinFreezeOffHook.Store(false) })
	s.joiner = joinNewcomer(t, tc, s.owner, ports)
	return s
}

// TestADropReachesACopyLeftOutsideTheSet covers a sweep that gives up
// leaves a copy outside the set. The bucket is dropped; the leftover is gone
// within a bounded wait, and a create of the name then succeeds. Guard:
// address the drop to the set only — the leftover is never dropped.
func TestADropReachesACopyLeftOutsideTheSet(t *testing.T) {
	s := setUpACopyLeftOutsideTheSet(t, time.Second)
	defer s.tc.stop()
	outside := s.tc.nodes[s.outside].server.core
	if !eventually(func() bool { return outside.cluster.sweepLeftCount() > 0 }) {
		t.Fatal("setup: the outside holder's sweep never gave up on the bucket")
	}
	if !outside.bucketStoreOpen(testDatabase, s.bucket) {
		t.Fatal("setup: the holder outside the set released its copy although the hand-off was never confirmed")
	}

	dropAtOne(s.tc, s.owner, testDatabase, s.bucket)
	nodes := append(append([]*testNode(nil), s.tc.nodes...), s.joiner.node)
	gone := func() bool {
		for _, node := range nodes {
			if node.server.core.bucketStoreOpen(testDatabase, s.bucket) {
				return false
			}
		}
		return true
	}
	if !eventually(gone) {
		for index, node := range nodes {
			if node.server.core.bucketStoreOpen(testDatabase, s.bucket) {
				t.Errorf("node %d still serves bucket %s after its drop", index+1, s.bucket)
			}
		}
		t.Fatalf("the drop never reached the copy left outside the set on node %d, or a pull of it brought the bucket back", s.outside+1)
	}
	s.tc.ok(s.owner, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: s.bucket, Type: BucketTypeHash})
	if foundThrough(s.tc, s.owner, testDatabase, s.bucket, "k0") {
		t.Error("the re-created bucket serves the dropped bucket's document k0")
	}
}

// TestADropClearsWhatIsPendingForTheName: before the drop, the owner and the
// holder outside the set both retry the bucket's hand-off (a sweep pending
// list), and the outside holder has an 'h' record for it. After the drop,
// neither the pending lists, the residue nor the marks name it, and no node
// starts a pull of it. Guard: leave the pending list — the owner's next retry
// pulls the dropped name back from wherever it finds it.
func TestADropClearsWhatIsPendingForTheName(t *testing.T) {
	s := setUpACopyLeftOutsideTheSet(t, 0)
	defer s.tc.stop()
	key := placementOf(testDatabase, s.bucket)
	owner, outside := s.tc.nodes[s.owner].server.core, s.tc.nodes[s.outside].server.core
	if !eventually(func() bool { return s.askedBy(s.owner) >= 2 && s.askedBy(s.outside) >= 2 }) {
		t.Fatalf("setup: the hand-off was retried %d times by the owner and %d by the outside holder; want both retrying",
			s.askedBy(s.owner), s.askedBy(s.outside))
	}
	if marks := outside.cluster.placementMarksOf(key); marks.Outside == 0 {
		t.Fatal("setup: the outside holder has no 'outside' mark for the bucket")
	}
	if !outside.cluster.retryingNames(s.bucket) || !owner.cluster.retryingNames(s.bucket) {
		t.Fatal("setup: the bucket is not on both nodes' sweep pending lists")
	}

	var pulls atomic.Int64
	countPulls := func(database, bucket string) {
		if bucket == s.bucket {
			pulls.Add(1)
		}
	}
	pullStartedHook.Store(&countPulls)
	defer pullStartedHook.Store(nil)
	dropAtOne(s.tc, s.owner, testDatabase, s.bucket)
	if !eventually(func() bool { return !outside.bucketStoreOpen(testDatabase, s.bucket) }) {
		t.Fatal("the drop never reached the holder outside the set")
	}
	// Each node clears its own lists: the owner learns of the drop on its own
	// schedule. Waiting for the outside holder's store to close says nothing
	// about either node's lists, so wait for each. A pass in flight when the
	// drop landed no longer writes its older list back (BUG-0659); before that
	// fix the name stayed until the sweep gave up, and for a ring owner with no
	// store it stayed for good, since the retry of a name no member holds
	// answers not-found and counts as unsettled. The wait covers only the
	// owner learning of the drop late (BUG-0487).
	for _, node := range []*Server{owner, outside} {
		if !eventually(func() bool { return !node.cluster.retryingNames(s.bucket) }) {
			t.Errorf("%s: the sweep's pending list still names the dropped bucket", node.cluster.self.ID)
		}
		if !eventually(func() bool { return !node.cluster.residueNames(s.bucket) }) {
			t.Errorf("%s: the sweep's residue still names the dropped bucket", node.cluster.self.ID)
		}
	}
	if marks := outside.cluster.placementMarksOf(key); marks != (placementMarks{}) {
		t.Errorf("the outside holder still marks the dropped bucket %+v in memory", marks)
	}
	if held := storedPlacementMarks(t, outside, key); held {
		t.Error("the outside holder still has the dropped bucket's 'h' record on disk")
	}
	time.Sleep(4 * sweepRetryCeiling.get())
	if started := pulls.Load(); started != 0 {
		t.Errorf("%d pulls of the dropped bucket started after its drop; a retry left on a pending list pulls it back", started)
	}
}

// storedPlacementMarks reports whether s has an 'h' record for key on disk.
func storedPlacementMarks(t *testing.T, s *Server, key placementKey) bool {
	t.Helper()
	snapshot := s.kv.NewSnapshot()
	defer snapshot.Discard()
	_, err := snapshot.Get(placementMarkKey(key))
	return err == nil
}

// removedHolder is a bucket at factor 2 whose owner R is removed while
// running and keeps its copy; the new owner pulled the bucket from the old
// replica X, which is outside the new set and still holds its copy; the new
// set's other member Y has not pulled it. Auto-rebalance is off everywhere,
// so nothing moves but what the test moves.
type removedHolder struct {
	tc                                *testCluster
	bucket                            string
	removed, outside, newOwner, other int
}

func setUpARemovedHolder(t *testing.T) *removedHolder {
	t.Helper()
	tc := newTestClusterWith(t, 4, 2, func(_ int, cfg *config.Config) { cfg.Server.AutoRebalance = false })
	full := ringWith(t, 2, tc.addrs...)
	var s removedHolder
	s.tc = tc
	s.bucket = bucketNamed(t, "handed", func(name string) bool {
		key := placementOf(testDatabase, name).encoded
		set, _ := full.Replicas(key, 2)
		var rest []string
		for _, addr := range tc.addrs {
			if addr != set[0].ID {
				rest = append(rest, addr)
			}
		}
		after, _ := ringWith(t, 2, rest...).Replicas(key, 2)
		if after[0].ID == set[1].ID || after[1].ID == set[1].ID {
			return false
		}
		s.removed, s.outside = nodeIndexByAddr(t, tc.addrs, set[0].ID), nodeIndexByAddr(t, tc.addrs, set[1].ID)
		s.newOwner, s.other = nodeIndexByAddr(t, tc.addrs, after[0].ID), nodeIndexByAddr(t, tc.addrs, after[1].ID)
		return true
	})
	tc.ok(s.newOwner, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(s.newOwner, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: s.bucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, s.bucket, 2)
	for i := 0; i < 5; i++ {
		tc.ok(s.newOwner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: s.bucket, Key: fmt.Sprintf("k%d", i),
			Data: map[string]interface{}{"n": i}, WriteConcern: "all"})
	}

	via := clusterOf(tc, s.newOwner)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := via.proposeRemoval(ctx, tc.addrs[s.removed]); err != nil {
		t.Fatalf("setup: removing node %d: %v", s.removed+1, err)
	}
	for index := range tc.nodes {
		if index == s.newOwner {
			continue
		}
		if err := clusterOf(tc, index).reconcileMembership(via.self.ID, via.memberNodes(), via.memberIDs(), via.Epoch()); err != nil {
			t.Fatalf("setup: bringing node %d to the removal: %v", index+1, err)
		}
	}
	if clusterOf(tc, s.removed).removedAt.Load() == 0 {
		t.Fatal("setup: the removed node does not know it was removed")
	}
	// The new owner copies from the removed node, which keeps its copy, as its
	// take-over does: a hand-off pull, which a removed node serves (10b.5).
	removedNode := ring.Node{ID: tc.addrs[s.removed], Addr: tc.addrs[s.removed]}
	if err := via.pullBucketWith(ctx, pullPlan{database: testDatabase, bucket: s.bucket, source: removedNode, takeOver: true}); err != nil {
		t.Fatalf("setup: the new owner pulling from the removed node: %v", err)
	}
	if !via.s.bucketStoreOpen(testDatabase, s.bucket) {
		t.Fatal("setup: the new owner does not hold the bucket after its pull")
	}
	if !tc.nodes[s.outside].server.core.bucketStoreOpen(testDatabase, s.bucket) {
		t.Fatal("setup: the old replica outside the new set released its copy")
	}
	return &s
}

// ownerHasOpened reports what refuseAnUnsettledSource reads before it serves a
// replica's pull: the owner's copy is opened and not marked moved.
func ownerHasOpened(owner *Cluster, database, bucket string) bool {
	state, err := owner.s.bucketStateNamed(database, bucket)
	if err != nil {
		return false
	}
	key := placementOf(database, bucket)
	marks := owner.placementMarksOf(key)
	return owner.copyIsOpened(key, state, marks) && marks.Moved == 0
}

// waitForTheOwnerToOpen waits until owner serves its copy of database/bucket
// as a settled source, so a replica's seed is not refused for the owner's
// take-over still being in flight.
func waitForTheOwnerToOpen(t *testing.T, owner *Cluster, database, bucket string) {
	t.Helper()
	pollUntil(t, fmt.Sprintf("%s has opened its copy of %s/%s", owner.self.ID, database, bucket), func() bool {
		return ownerHasOpened(owner, database, bucket)
	})
}

// TestAReplicaSeedWaitsForTheOwnersTakeOver is the deterministic form of
// BUG-0524. The owner's placement pass runs when it runs: one that marks the
// copy the owner just pulled as moved leaves it unsettled until the take-over
// worker opens a term and clears the mark, and until then the owner refuses a
// replica's seed as "not the bucket's settled owner". The test holds every
// worker and records that mark itself: ownerHasOpened is false and the seed is
// refused. Once the workers run, the owner settles and the same seed lands.
// A waitForTheOwnerToOpen that returned early, or a condition that says true
// while the seed is still refused, fails here on every run, where
// TestADroppedBucketIsNotTakenBackFromARemovedNode failed about twice in 480.
func TestAReplicaSeedWaitsForTheOwnersTakeOver(t *testing.T) {
	held := func(string) bool { return true }
	takeOverPausedHook.Store(&held)
	defer takeOverPausedHook.Store(nil)
	s := setUpARemovedHolder(t)
	defer s.tc.stop()
	owner := clusterOf(s.tc, s.newOwner)
	replica := clusterOf(s.tc, s.other)
	key := placementOf(testDatabase, s.bucket)

	owner.placementMu.Lock()
	owner.unsettled[key] = placementMarks{Moved: owner.Epoch()}
	owner.placementMu.Unlock()
	if ownerHasOpened(owner, testDatabase, s.bucket) {
		t.Fatal("the owner counts its copy settled while it is marked moved")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	err := replica.ensureMigrated(ctx, testDatabase, s.bucket)
	if err == nil || !strings.Contains(err.Error(), errSourceNotSettled.Error()) {
		t.Fatalf("the replica's seed from an owner marked moved answered %v; want the %q refusal", err, errSourceNotSettled)
	}
	if replica.s.bucketStoreOpen(testDatabase, s.bucket) {
		t.Fatal("the replica holds the bucket although its owner refused to seed it")
	}

	takeOverPausedHook.Store(nil)
	owner.wakeTakeOver(testDatabase, s.bucket)
	waitForTheOwnerToOpen(t, owner, testDatabase, s.bucket)
	pullNow(t, replica, testDatabase, s.bucket)
}

// TestADropRefusedWhileOwnershipMovesIsSentAgain is the deterministic form of
// BUG-0529. The new owner's take-over is held and its copy is marked moved, as
// a placement pass that has not finished leaves it: a drop sent to it is then
// refused with the designed answer, UNAVAILABLE and "ownership of ... is moving
// to this node ... Retry in a moment", which CI met as a hard failure of
// TestADropDuringAHandOffIsNotUndone. A single send (what dropAtOne was) gets
// that refusal on every run; the helper sends again, so once the workers run
// the same drop lands, after more than one send. A dropAtOne that sent once
// fails here with the CI's message.
func TestADropRefusedWhileOwnershipMovesIsSentAgain(t *testing.T) {
	held := func(string) bool { return true }
	takeOverPausedHook.Store(&held)
	defer takeOverPausedHook.Store(nil)
	s := setUpARemovedHolder(t)
	defer s.tc.stop()
	owner := clusterOf(s.tc, s.newOwner)
	key := placementOf(testDatabase, s.bucket)
	owner.placementMu.Lock()
	owner.unsettled[key] = placementMarks{Moved: owner.Epoch()}
	owner.noteUnsettledLocked() // the write path reads the published count, not the map
	owner.placementMu.Unlock()

	single := s.tc.frame(s.newOwner, transport.OpDeleteBucket, bson.M{"database": testDatabase, "name": s.bucket, "write_concern": "one"})
	if !isAHandOffRefusal(single) {
		t.Fatalf("a drop at the new owner while its take-over is held answered opcode %s %q; want the hand-off refusal",
			single.Opcode, string(single.Payload))
	}
	t.Logf("a single send is refused: %s", string(single.Payload))

	type outcome struct {
		frame    *transport.Frame
		attempts int
	}
	done := make(chan outcome, 1)
	firstAnswered := make(chan struct{})
	var firstOnce sync.Once
	go func() {
		frame, attempts := sendWhileOwnershipMoves(func() *transport.Frame {
			answer := s.tc.frame(s.newOwner, transport.OpDeleteBucket, bson.M{"database": testDatabase, "name": s.bucket, "write_concern": "one"})
			firstOnce.Do(func() { close(firstAnswered) })
			return answer
		})
		done <- outcome{frame, attempts}
	}()
	<-firstAnswered // the hold lifts only after the helper has met the refusal once
	takeOverPausedHook.Store(nil)
	owner.wakeTakeOver(testDatabase, s.bucket)
	waitForTheOwnerToOpen(t, owner, testDatabase, s.bucket)
	got := <-done
	if got.frame.Opcode == transport.OpError {
		t.Fatalf("the drop was not sent again once ownership settled: %s", string(got.frame.Payload))
	}
	if got.attempts < 2 {
		t.Fatalf("the drop landed after %d send; want a refusal first and a second send", got.attempts)
	}
}

// TestADropDuringAHandOffIsNotUndone covers the root cause of a drop being undone: the new owner
// O' has pulled the bucket from X, which is outside the new set and keeps its
// copy until the set confirms. The bucket is dropped while X's stream is held,
// and O' and Y then try to pull it. Afterwards, a GET at O', at Y and at the
// removed node R finds nothing, and X's copy is gone. Guard: no drop-record
// check in the materialise — O' pulls X's dropped copy back.
func TestADropDuringAHandOffIsNotUndone(t *testing.T) {
	s := setUpARemovedHolder(t)
	defer s.tc.stop()
	outsideID := s.tc.addrs[s.outside]
	release := make(chan struct{})
	var once sync.Once
	releaseX := func() { once.Do(func() { close(release) }) }
	defer releaseX()
	hold := func(peerID string) func() {
		if peerID == outsideID {
			<-release
		}
		return func() {}
	}
	beforeShipHook.Store(&hold)
	defer beforeShipHook.Store(nil)

	dropAtOne(s.tc, s.newOwner, testDatabase, s.bucket)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for _, index := range []int{s.newOwner, s.other} {
		_ = clusterOf(s.tc, index).ensureMigrated(ctx, testDatabase, s.bucket)
	}
	releaseX()
	beforeShipHook.Store(nil)

	outside := s.tc.nodes[s.outside].server.core
	if !eventually(func() bool { return !outside.bucketStoreOpen(testDatabase, s.bucket) }) {
		t.Errorf("node %d (outside the new set) still holds the dropped bucket", s.outside+1)
	}
	for _, index := range []int{s.newOwner, s.other} {
		if s.tc.nodes[index].server.core.bucketStoreOpen(testDatabase, s.bucket) {
			t.Errorf("node %d serves bucket %s after its drop was acknowledged: the drop was undone by a pull of a copy outside the set",
				index+1, s.bucket)
		}
	}
	for _, index := range []int{s.newOwner, s.other, s.removed} {
		if foundThrough(s.tc, index, testDatabase, s.bucket, "k0") {
			t.Errorf("a GET of k0 through node %d finds the dropped bucket's document", index+1)
		}
	}
}

// TestADroppedBucketIsNotTakenBackFromARemovedNode (review M-2): the removed
// node R keeps its copy of a bucket that is then dropped. A pull from R lands
// nothing, a catalog answer of R's names a holder that is not counted, and the
// drop record stays while R answers that it holds the incarnation; once R ages
// out of the membership history, the tombstone sweep deletes it. Guard: delete
// the record at the first acknowledgement — the pull from R then lands. And
// the fallback arm of step 10b.5: the owner's take-over of the name finds R's
// copy of the dropped incarnation ineligible and answers not found. Guard:
// take a removed node's copy without the drop check — the take-over copies
// it, and only the materialise's own refusal stops it.
func TestADroppedBucketIsNotTakenBackFromARemovedNode(t *testing.T) {
	s := setUpARemovedHolder(t)
	defer s.tc.stop()
	owner := clusterOf(s.tc, s.newOwner)
	// The owner's pull from the removed node lands the copy, but its take-over
	// worker opens the copy's term afterwards, and a source seeds a replica
	// only once it has (refuseAnUnsettledSource, BUG-0524).
	waitForTheOwnerToOpen(t, owner, testDatabase, s.bucket)
	pullNow(t, clusterOf(s.tc, s.other), testDatabase, s.bucket)
	dropped := lineageOf(t, owner.s, testDatabase, s.bucket).Incarnation
	dropAtOne(s.tc, s.newOwner, testDatabase, s.bucket)
	if !eventually(func() bool {
		return !s.tc.nodes[s.outside].server.core.bucketStoreOpen(testDatabase, s.bucket) &&
			!s.tc.nodes[s.other].server.core.bucketStoreOpen(testDatabase, s.bucket)
	}) {
		t.Fatal("setup: the drop did not reach every member")
	}
	if !s.tc.nodes[s.removed].server.core.bucketStoreOpen(testDatabase, s.bucket) {
		t.Fatal("setup: the removed node no longer holds its copy")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, err := owner.sweepTombstones(ctx); err != nil {
		t.Fatalf("the tombstone sweep: %v", err)
	}
	if recorded, err := owner.s.dropRecorded(testDatabase, s.bucket, dropped); err != nil || !recorded {
		t.Fatalf("the drop record of incarnation %d is gone (%v) while the removed node still holds that incarnation", dropped, err)
	}

	removedNode := ring.Node{ID: s.tc.addrs[s.removed], Addr: s.tc.addrs[s.removed]}
	err := owner.pullBucketWith(ctx, pullPlan{database: testDatabase, bucket: s.bucket, source: removedNode, takeOver: true})
	if owner.s.bucketStoreOpen(testDatabase, s.bucket) {
		t.Fatalf("a pull from the removed node landed the dropped bucket again (%v)", err)
	}
	if !errors.Is(err, errPullOfDroppedIncarnation) {
		t.Errorf("the pull from the removed node failed with %v; want the dropped-incarnation refusal", err)
	}
	// The fallback arm (step 10b.5): the owner's take-over, asked for the name,
	// finds the removed node's copy of the dropped incarnation and takes
	// nothing — the bucket is not found.
	if err := owner.ensureMigrated(ctx, testDatabase, s.bucket); !errors.Is(err, ErrMigrateBucketNotFound) {
		t.Errorf("the owner's take-over of the dropped name answered %v; want not found: the removed node's copy is of the dropped incarnation", err)
	}
	if owner.s.bucketStoreOpen(testDatabase, s.bucket) {
		t.Fatal("the owner's take-over landed the dropped bucket from the removed node")
	}
	answer, err := owner.askCatalog(ctx, removedNode, ReplCatalogAsk{Database: testDatabase, Bucket: s.bucket})
	if err != nil {
		t.Fatalf("asking the removed node the catalog question: %v", err)
	}
	if answer.HeldIncarnation != dropped {
		t.Fatalf("setup: the removed node answers incarnation %d, want the dropped %d", answer.HeldIncarnation, dropped)
	}
	survey := catalogSurvey{holders: []catalogHolder{{member: removedNode.ID, generation: answer.HeldGeneration, incarnation: answer.HeldIncarnation}}}
	if holders, err := owner.liveHolders(testDatabase, s.bucket, survey); err != nil || len(holders) != 0 {
		t.Errorf("the removed node's copy of a dropped incarnation counts as a live holder: %v %v", holders, err)
	}

	// Swap runs now and returns the old limit; the deferred Store restores it.
	defer gossipHistoryLimit.Store(gossipHistoryLimit.Swap(1))
	if _, err := owner.sweepTombstones(ctx); err != nil {
		t.Fatalf("the tombstone sweep after the removed node aged out: %v", err)
	}
	if recorded, err := owner.s.dropRecorded(testDatabase, s.bucket, dropped); err != nil || recorded {
		t.Errorf("the drop record outlived the removed node's place in the history (recorded %v, %v)", recorded, err)
	}
}

// TestAnOldDropDoesNotRemoveANewIncarnation: a drop of incarnation I is pushed
// to a member that holds J; J is untouched. A second arm pushes a drop whose
// incarnation is 0 (a drop logged before incarnations, review B-2). Guard:
// apply by name, or let 0 match.
func TestAnOldDropDoesNotRemoveANewIncarnation(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	const bucket = "renewed"
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, bucket, 3)
	tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "k", Data: map[string]interface{}{"n": 1}, WriteConcern: "all"})
	owner := ownerIndexOf(t, tc, bucket)
	replica := (owner + 1) % 3
	held := tc.nodes[replica].server.core
	live := lineageOf(t, held, testDatabase, bucket).Incarnation
	older := live - 1
	if live == 1 {
		older = math.MaxInt64
	}
	for arm, incarnation := range map[string]uint64{"an older incarnation": older, "incarnation 0": 0} {
		t.Run(arm, func(t *testing.T) {
			drop := bson.M{"database": testDatabase, "name": bucket}
			if incarnation != 0 {
				drop["incarnation"] = int64(incarnation)
			}
			payload, err := bson.Marshal(drop)
			if err != nil {
				t.Fatal(err)
			}
			entry := ReplEntry{Seq: 1 << 40, Op: int32(transport.OpDeleteBucket), Bucket: bucket, Payload: payload}
			if _, err := clusterOf(tc, replica).applyAsReplica(context.Background(), entry, true, tc.addrs[owner], false); err != nil {
				t.Fatalf("pushing the drop: %v", err)
			}
			if !held.bucketStoreOpen(testDatabase, bucket) {
				t.Fatalf("a drop of incarnation %d removed the live incarnation %d", incarnation, live)
			}
			if got := lineageOf(t, held, testDatabase, bucket).Incarnation; got != live {
				t.Fatalf("the held copy is incarnation %d after the drop, want %d", got, live)
			}
			if resp, err := held.Find(FindRequest{Database: testDatabase, Bucket: bucket, Key: "k"}); err != nil || !resp.Found {
				t.Fatalf("the live incarnation lost its document: %v", err)
			}
		})
	}
}

// TestAReleasedCopyIsSeededAgainWhenItsNodeReentersTheSet (review M-3): a
// holder releases its copy when a join takes it out of the set; the joiner is
// removed again, the holder is back in the set, and it pulls the live
// incarnation with no refusal. Guard: write the drop record in every
// commitDropBatch — the release then records the live incarnation dropped.
func TestAReleasedCopyIsSeededAgainWhenItsNodeReentersTheSet(t *testing.T) {
	tc := newTestCluster(t, 3, 2)
	defer tc.stop()
	ports := reserveNodePorts(t)
	newcomer := ports.ringNode()
	before := ringWith(t, 2, tc.addrs...)
	after := ringWith(t, 2, append(append([]string(nil), tc.addrs...), newcomer.ID)...)
	var outsideID string
	bucket := bucketNamed(t, "released", func(name string) bool {
		key := placementOf(testDatabase, name).encoded
		old, _ := before.Replicas(key, 2)
		joined, _ := after.Replicas(key, 2)
		if old[0].ID != joined[0].ID || joined[1].ID != newcomer.ID {
			return false
		}
		outsideID = old[1].ID
		return true
	})
	owner, outside := nodeIndexByAddr(t, tc.addrs, ownerOn(before, bucket)), nodeIndexByAddr(t, tc.addrs, outsideID)
	tc.ok(owner, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(owner, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, bucket, 2)
	tc.ok(owner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "k", Data: map[string]interface{}{"n": 1}, WriteConcern: "all"})
	live := lineageOf(t, tc.nodes[owner].server.core, testDatabase, bucket).Incarnation
	shortenSweeps(t, 0)

	joiner := joinNewcomer(t, tc, owner, ports)
	pullNow(t, joiner.cluster(), testDatabase, bucket)
	holder := tc.nodes[outside].server.core
	if !eventually(func() bool { return !holder.bucketStoreOpen(testDatabase, bucket) }) {
		t.Fatal("setup: the holder outside the set never released its copy")
	}

	via := clusterOf(tc, owner)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := via.proposeRemoval(ctx, newcomer.ID); err != nil {
		t.Fatalf("removing the joiner: %v", err)
	}
	for index := range tc.nodes {
		if index != owner {
			if err := clusterOf(tc, index).reconcileMembership(via.self.ID, via.memberNodes(), via.memberIDs(), via.Epoch()); err != nil {
				t.Fatalf("bringing node %d to the removal: %v", index+1, err)
			}
		}
	}
	pullNow(t, holder.cluster, testDatabase, bucket)
	if got := lineageOf(t, holder, testDatabase, bucket).Incarnation; got != live {
		t.Errorf("the holder re-entered the set with incarnation %d, want the live %d", got, live)
	}
}

// retryingNames reports whether the sweep's retry list names bucket.
func (c *Cluster) retryingNames(bucket string) bool {
	c.sweepLeftMu.Lock()
	defer c.sweepLeftMu.Unlock()
	for _, pending := range c.retrying.list {
		if pending.bucket == bucket {
			return true
		}
	}
	return false
}

// residueNames reports whether the sweep's residue names bucket.
func (c *Cluster) residueNames(bucket string) bool {
	c.sweepLeftMu.Lock()
	defer c.sweepLeftMu.Unlock()
	for _, pending := range c.residue.list {
		if pending.bucket == bucket {
			return true
		}
	}
	return false
}
