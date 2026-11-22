package server

import (
	"context"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Step 10b.5 (rev 7 step 10.5): the take-over's choice of source. A new owner
// opens from the most complete copy any node that may hold one has — waiting
// for a member it cannot reach rather than opening without it — and opens a
// term of its own whenever a node it could not ask may have opened one.

// lostOwnerSet is a bucket whose owner is stopped and removed at factor 3:
// the members of its set before the removal, and its owner after.
type lostOwnerSet struct {
	tc       *testCluster
	bucket   string
	owner    int
	newOwner int
	// replicas are the two other members of the set before the removal.
	replicas [2]int
}

// pickLostOwnerSet chooses a bucket whose new owner after the removal of its
// owner is one of its replicas, when replicaOwns, or the node outside its set.
func pickLostOwnerSet(t *testing.T, tc *testCluster, prefix string, replicaOwns bool) *lostOwnerSet {
	t.Helper()
	before := ringWith(t, 3, tc.addrs...)
	var lost *lostOwnerSet
	bucketNamed(t, prefix, func(name string) bool {
		key := placementOf(testDatabase, name).encoded
		set, err := before.Replicas(key, 3)
		if err != nil {
			return false
		}
		owner := nodeIndexByAddr(t, tc.addrs, set[0].ID)
		var rest []string
		for index, addr := range tc.addrs {
			if index != owner {
				rest = append(rest, addr)
			}
		}
		next, err := ringWith(t, 3, rest...).Owner(key)
		if err != nil || nodeListed(set, next.ID) != replicaOwns {
			return false
		}
		lost = &lostOwnerSet{tc: tc, bucket: name, owner: owner, newOwner: nodeIndexByAddr(t, tc.addrs, next.ID),
			replicas: [2]int{nodeIndexByAddr(t, tc.addrs, set[1].ID), nodeIndexByAddr(t, tc.addrs, set[2].ID)}}
		return true
	})
	return lost
}

// create creates the bucket through its owner and writes count documents at
// all, keyed prefix0, prefix1, ...
func (l *lostOwnerSet) create(t *testing.T, prefix string, count int) {
	t.Helper()
	l.tc.ok(l.owner, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	l.tc.ok(l.owner, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: l.bucket, Type: BucketTypeHash})
	waitForServing(t, l.tc, testDatabase, l.bucket, 3)
	l.write(t, l.owner, prefix, count, "all")
}

// write writes count documents through node index at concern.
func (l *lostOwnerSet) write(t *testing.T, index int, prefix string, count int, concern string) {
	t.Helper()
	for i := 0; i < count; i++ {
		frame := l.tc.frame(index, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: l.bucket,
			Key: fmt.Sprintf("%s%d", prefix, i), Data: map[string]interface{}{"n": i}, WriteConcern: concern})
		if frame.Opcode == transport.OpError {
			t.Fatalf("write %s%d through node %d at %s: %s", prefix, i, index+1, concern, frame.Payload)
		}
	}
}

// removeOwner removes the stopped owner through the new owner and brings
// every running node to the removal.
func (l *lostOwnerSet) removeOwner(t *testing.T) {
	t.Helper()
	removeAndForget(t, l.tc, l.newOwner, l.tc.addrs[l.owner])
}

// holdsAll fails unless node index's own store holds prefix0..prefix(count-1).
func (l *lostOwnerSet) holdsAll(t *testing.T, index int, prefix string, count int, who string) {
	t.Helper()
	var missing []string
	for i := 0; i < count; i++ {
		if key := fmt.Sprintf("%s%d", prefix, i); !holdsKeyIn(l.tc.nodes[index], testDatabase, l.bucket, key) {
			missing = append(missing, key)
		}
	}
	if len(missing) > 0 {
		t.Errorf("%s, node %d, lacks %d acknowledged writes of %s: %v", who, index+1, len(missing), l.bucket, missing)
	}
}

// TestABucketWaitsForAMemberThatMayHoldACopy (rev 7 step 10.5): the owner is
// stopped and removed while one of its replicas is stopped. The new owner
// cannot tell whether the stopped replica holds writes no one else does, so
// for ten seconds every write is refused with a message that names it; the
// replica is started and the writes succeed. Guards: take the first holder in
// member order; open without waiting; complete on a majority of answers —
// each opens without the stopped replica and the write succeeds.
func TestABucketWaitsForAMemberThatMayHoldACopy(t *testing.T) {
	// Five nodes: the removal needs a majority with two of them stopped.
	tc := newTestCluster(t, 5, 3)
	defer tc.stop()
	l := pickLostOwnerSet(t, tc, "waits", true)
	l.create(t, "w", 5)
	stopped := l.replicas[0]
	if stopped == l.newOwner {
		stopped = l.replicas[1]
	}
	startStopped := stopNode(t, tc, stopped)
	tc.stopNodeKeepingData(l.owner)
	l.removeOwner(t)

	refusal := func(attempt int) ErrorResponse {
		frame := tc.frame(l.newOwner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: l.bucket,
			Key: fmt.Sprintf("refused%d", attempt), Data: map[string]interface{}{"n": attempt}, WriteConcern: "majority"})
		return requireRetryRefusal(t, frame, fmt.Sprintf("write %d at the new owner while node %d may hold a copy", attempt, stopped+1))
	}
	attempt := 0
	// The first refusals may come before the worker has asked the members.
	if !holdsWithin(takeOverWait, func() bool {
		attempt++
		return strings.Contains(refusal(attempt).Message, tc.addrs[stopped])
	}) {
		t.Fatalf("no refusal at the new owner names the stopped replica %s", tc.addrs[stopped])
	}
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(time.Second) {
		attempt++
		if refused := refusal(attempt); !strings.Contains(refused.Message, tc.addrs[stopped]) {
			t.Fatalf("refusal %d does not name the stopped replica %s: %s", attempt, tc.addrs[stopped], refused.Message)
		}
	}

	startStopped()
	tc.bringMembershipTo(t, clusterOf(tc, l.newOwner))
	if !holdsWithin(takeOverWait, func() bool {
		frame := tc.frame(l.newOwner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: l.bucket,
			Key: "after", Data: map[string]interface{}{"n": 0}, WriteConcern: "majority"})
		return frame.Opcode != transport.OpError
	}) {
		t.Fatalf("writes at the new owner are still refused after the stopped replica, node %d, was started", stopped+1)
	}
	l.holdsAll(t, l.newOwner, "w", 5, "the new owner")
}

// owingOwner is a bucket at factor 2 whose owner's store failed one apply —
// its copy owes the entry, which its replica applied — and took a second
// write.
type owingOwner struct {
	tc      *testCluster
	bucket  string
	owner   int
	replica int
}

// setUpAnOwingOwner creates bucket through owner, fails the owner's apply of
// the first write and makes the second.
func setUpAnOwingOwner(t *testing.T, tc *testCluster, bucket string, owner int) *owingOwner {
	t.Helper()
	set, err := ringWith(t, 2, tc.addrs...).Replicas(placementOf(testDatabase, bucket).encoded, 2)
	if err != nil {
		t.Fatalf("placing %s: %v", bucket, err)
	}
	o := &owingOwner{tc: tc, bucket: bucket, owner: owner, replica: nodeIndexByAddr(t, tc.addrs, set[1].ID)}
	tc.ok(owner, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(owner, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, bucket, 2)
	failed := failNextApplyOn(t, tc.nodes[owner].server.core.kv, anySeq)
	tc.frame(owner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "owed",
		Data: map[string]interface{}{"n": 1}, WriteConcern: "all"})
	if failed.Load() == 0 {
		t.Fatal("setup: the owner's store failed no apply")
	}
	store.ApplyFaultHook.Store(nil)
	tc.ok(owner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "second",
		Data: map[string]interface{}{"n": 2}, WriteConcern: "all"})
	own, err := tc.nodes[owner].server.core.cluster.answerCopy(ReplBucketRef{Database: testDatabase, Bucket: bucket})
	if err != nil || !own.Owes {
		t.Fatalf("setup: the owner's copy of %s does not owe the failed apply (%+v, %v)", bucket, own, err)
	}
	if !holdsKeyIn(tc.nodes[o.replica], testDatabase, bucket, "owed") {
		t.Fatalf("setup: the replica, node %d, does not hold the write the owner owes", o.replica+1)
	}
	return o
}

// requireBoth fails unless node holds both writes.
func (o *owingOwner) requireBoth(t *testing.T, node *testNode, who string) {
	t.Helper()
	for _, key := range []string{"owed", "second"} {
		if !holdsKeyIn(node, testDatabase, o.bucket, key) {
			t.Errorf("%s lacks %q of %s: its copy was taken from one that owed it", who, key, o.bucket)
		}
	}
}

// TestACopyThatOwesAnApplyIsNotTheSourceOfAHandOff (A-10.3, T-141): the
// owner's store fails one apply and a second write succeeds; the bucket then
// moves to a node that holds nothing, which must hold both writes: a copy that
// owes an apply is not the source while a copy in its term at or past its
// position owes nothing. On a join the joiner, which has no record of the
// membership before it, asks every holder; on the removal of the running owner
// the new owner asks the previous owner first, which answers opened. Guard:
// take the previous owner's copy — the new owner lacks the write it owes.
func TestACopyThatOwesAnApplyIsNotTheSourceOfAHandOff(t *testing.T) {
	t.Run("a join moves the bucket to the joiner", func(t *testing.T) {
		tc := newTestCluster(t, 3, 2)
		defer tc.stop()
		ports := reserveNodePorts(t)
		newcomer := ports.ringNode()
		after := ringWith(t, 2, append(append([]string(nil), tc.addrs...), newcomer.ID)...)
		bucket := bucketNamed(t, "owed", func(name string) bool { return ownerOn(after, name) == newcomer.ID })
		o := setUpAnOwingOwner(t, tc, bucket, nodeIndexByAddr(t, tc.addrs, ownerOn(ringWith(t, 2, tc.addrs...), bucket)))
		joiner := joinNewcomer(t, tc, o.owner, ports)
		waitTakenOver(t, joiner.node.server.core, testDatabase, bucket)
		o.requireBoth(t, joiner.node, "the joiner")
	})
	t.Run("the removal of the running owner moves the bucket to the node outside its set", func(t *testing.T) {
		tc := newTestCluster(t, 3, 2)
		defer tc.stop()
		before := ringWith(t, 2, tc.addrs...)
		var owner, newOwner int
		bucket := bucketNamed(t, "owed", func(name string) bool {
			key := placementOf(testDatabase, name).encoded
			set, err := before.Replicas(key, 2)
			if err != nil {
				return false
			}
			owner = nodeIndexByAddr(t, tc.addrs, set[0].ID)
			var rest []string
			for index, addr := range tc.addrs {
				if index != owner {
					rest = append(rest, addr)
				}
			}
			next := ownerOn(ringWith(t, 2, rest...), name)
			newOwner = nodeIndexByAddr(t, tc.addrs, next)
			return !nodeListed(set, next)
		})
		o := setUpAnOwingOwner(t, tc, bucket, owner)
		removeAndTell(t, tc, newOwner, owner)
		waitTakenOver(t, tc.nodes[newOwner].server.core, testDatabase, bucket)
		o.requireBoth(t, tc.nodes[newOwner], fmt.Sprintf("the new owner, node %d,", newOwner+1))
	})
}

// removeThrough removes node removed through node via and brings every
// running node of h, the joiner included, to the removal.
func (h *heldTakeOver) removeThrough(t *testing.T, via int, removed string) {
	t.Helper()
	proposer := clusterOf(h.tc, via)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := proposer.proposeRemoval(ctx, removed); err != nil {
		t.Fatalf("removing %s: %v", removed, err)
	}
	for _, node := range h.nodes() {
		if node.server == nil || node.server.core.cluster == proposer {
			continue
		}
		if err := node.server.core.cluster.reconcileMembership(proposer.self.ID, proposer.memberNodes(), proposer.memberIDs(), proposer.Epoch()); err != nil {
			t.Fatalf("bringing %s to the removal of %s: %v", node.server.core.cluster.self.ID, removed, err)
		}
	}
}

// TestTwoMembershipChangesInARowStillTakeTheNewestCopy (rev 7 step 10.5): a
// take-over that spans a second membership change still opens from the newest
// copy. A join's take-over is held, a third node is removed, and released the
// joiner opens from the old owner's copy; and a joiner that took a bucket over,
// took ten writes at all and was removed while stopped is succeeded by an
// owner that holds the ten.
func TestTwoMembershipChangesInARowStillTakeTheNewestCopy(t *testing.T) {
	t.Run("a join's take-over held across the removal of a third node", func(t *testing.T) {
		h := setUpAHeldTakeOver(t, removableKeepsTheJoiner)
		defer h.tc.stop()
		joinerID := h.joiner.cluster().self.ID
		removed := -1
		for index := range h.tc.addrs {
			if index == h.oldOwner {
				continue
			}
			rest := []string{joinerID}
			for other, otherAddr := range h.tc.addrs {
				if other != index {
					rest = append(rest, otherAddr)
				}
			}
			if ownerOn(ringWith(t, 2, rest...), h.bucket) == joinerID {
				removed = index
				break
			}
		}
		if removed < 0 {
			t.Fatalf("setup: the removal of every node but the old owner moves %s away from the joiner", h.bucket)
		}
		h.removeThrough(t, h.oldOwner, h.tc.addrs[removed])
		h.release()
		waitTakenOver(t, h.joiner.node.server.core, testDatabase, h.bucket)
		if !holdsKeyIn(h.joiner.node, testDatabase, h.bucket, "held") {
			t.Errorf("the joiner opened %s across the removal of node %d without the old owner's write", h.bucket, removed+1)
		}
	})
	t.Run("an owner that opened and was removed while stopped", func(t *testing.T) {
		h := setUpAHeldTakeOver(t)
		defer h.tc.stop()
		h.release()
		waitTakenOver(t, h.joiner.node.server.core, testDatabase, h.bucket)
		joinerID := h.joiner.cluster().self.ID
		// The ten are on the joiner's whole set: its replica follows the
		// joiner's term before they are written, and each is answered with
		// both copies. A write the joiner alone confirmed is conceded when it
		// is removed while stopped (spec §20.6), which is not this test.
		set, err := h.joiner.cluster().Replicas(placementOf(testDatabase, h.bucket))
		if err != nil {
			t.Fatalf("placing %s on the joiner: %v", h.bucket, err)
		}
		for _, member := range set[1:] {
			waitFollowing(t, h.tc.nodes[nodeIndexByAddr(t, h.tc.addrs, member.ID)].server.core, h.joiner.node.server.core, testDatabase, h.bucket)
		}
		for i := 0; i < 10; i++ {
			frame := h.tc.frameOn(h.joiner.node, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: h.bucket,
				Key: fmt.Sprintf("ten%d", i), Data: map[string]interface{}{"n": i}, WriteConcern: "all"})
			if frame.Opcode == transport.OpError {
				t.Fatalf("write %d at all through the joiner: %s", i, frame.Payload)
			}
			if confirmed, ok := bson.Raw(frame.Payload).Lookup("copies_confirmed").AsInt64OK(); !ok || int(confirmed) != len(set) {
				t.Fatalf("write %d at all through the joiner was answered copies_confirmed %d (%v), want %d", i, confirmed, ok, len(set))
			}
		}
		h.joiner.stop()
		h.removeThrough(t, h.oldOwner, joinerID)
		next := nodeIndexByAddr(t, h.tc.addrs, ownerOn(ringWith(t, 2, h.tc.addrs...), h.bucket))
		waitTakenOver(t, h.tc.nodes[next].server.core, testDatabase, h.bucket)
		for i := 0; i < 10; i++ {
			if key := fmt.Sprintf("ten%d", i); !holdsKeyIn(h.tc.nodes[next], testDatabase, h.bucket, key) {
				t.Errorf("the next owner, node %d, lacks %s, acknowledged at all by the removed joiner", next+1, key)
			}
		}
	})
}

// TestAnUnrelatedChangeDoesNotSendAHandOffToTheFallback (A-10.1, T-140): the
// running owner of a bucket is removed, and the bucket's new owner holds no
// copy; before it takes the bucket over, a member that holds no copy is
// stopped and an unrelated node joins. The previous owner is the owner on the
// ring before the removal, not the one before the join — the new owner itself —
// so the take-over asks it, and opens without waiting for the stopped member.
// Guard: take the previous owner from ringAt(epoch − 1) — the new owner finds
// none, falls back, and waits for the stopped member.
func TestAnUnrelatedChangeDoesNotSendAHandOffToTheFallback(t *testing.T) {
	tc := newTestCluster(t, 4, 2)
	defer tc.stop()
	before := ringWith(t, 2, tc.addrs...)
	var owner, newOwner, bystander int
	bucket := bucketNamed(t, "unrelated", func(name string) bool {
		set, err := before.Replicas(placementOf(testDatabase, name).encoded, 2)
		if err != nil {
			return false
		}
		owner = nodeIndexByAddr(t, tc.addrs, set[0].ID)
		var rest []string
		for index, addr := range tc.addrs {
			if index != owner {
				rest = append(rest, addr)
			}
		}
		next := ownerOn(ringWith(t, 2, rest...), name)
		newOwner = nodeIndexByAddr(t, tc.addrs, next)
		for index, addr := range tc.addrs {
			if index != owner && index != newOwner && !nodeListed(set, addr) {
				bystander = index
			}
		}
		return !nodeListed(set, next)
	})
	tc.ok(owner, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(owner, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, bucket, 2)
	tc.ok(owner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "held",
		Data: map[string]interface{}{"n": 1}, WriteConcern: "all"})

	takeOverOffHook.Store(true)
	t.Cleanup(func() { takeOverOffHook.Store(false) })
	removeAndTell(t, tc, newOwner, owner)
	tc.bringMembershipTo(t, clusterOf(tc, newOwner))
	stopNode(t, tc, bystander)
	var remaining []string
	for index, addr := range tc.addrs {
		if index != owner {
			remaining = append(remaining, addr)
		}
	}
	var ports *testNodePorts
	for attempt := 0; ; attempt++ {
		ports = reserveNodePorts(t)
		if ownerOn(ringWith(t, 2, append(append([]string(nil), remaining...), ports.ringNode().ID)...), bucket) == tc.addrs[newOwner] {
			break
		}
		if attempt == 50 {
			t.Fatalf("setup: no newcomer's address leaves %s with node %d", bucket, newOwner+1)
		}
	}
	joinNewcomer(t, tc, newOwner, ports)

	takeOverOffHook.Store(false)
	taker := clusterOf(tc, newOwner)
	taker.wakeTakeOver(testDatabase, bucket)
	waitTakenOver(t, tc.nodes[newOwner].server.core, testDatabase, bucket)
	if !holdsKeyIn(tc.nodes[newOwner], testDatabase, bucket, "held") {
		t.Errorf("the new owner, node %d, took %s over without the previous owner's write", newOwner+1, bucket)
	}
}

// pauseTakeOvers stops the take-over workers of nodes until the returned
// function is called; it wakes them.
func pauseTakeOvers(t *testing.T, nodes ...*Cluster) (resume func()) {
	t.Helper()
	paused := make(map[string]bool, len(nodes))
	for _, c := range nodes {
		paused[c.self.ID] = true
	}
	hook := func(nodeID string) bool { return paused[nodeID] }
	takeOverPausedHook.Store(&hook)
	t.Cleanup(func() { takeOverPausedHook.Store(nil) })
	return func() {
		takeOverPausedHook.Store(nil)
		for _, c := range nodes {
			c.signalTakeOver()
		}
	}
}

// removeRunningThrough removes node removed through the running node via and
// brings every other running node of tc to the removal.
func removeRunningThrough(t *testing.T, tc *testCluster, via int, removed string) {
	t.Helper()
	proposer := clusterOf(tc, via)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := proposer.proposeRemoval(ctx, removed); err != nil {
		t.Fatalf("removing %s: %v", removed, err)
	}
	tc.bringMembershipTo(t, proposer)
}

// TestANodeThatRegainsABucketOpensANewTermWhenItsSuccessorIsSilent (A-10.1,
// T-139) is review F-1's six steps. A owns a bucket at factor 3; a join moves
// it to the joiner B, which opens it, takes a write at one, and is stopped
// before any replica has asked it (the replicas' workers are paused); B is
// removed, and A regains the bucket. B does not answer, so A cannot tell
// whether B opened a term above its own: it opens a new term rather than
// resume its old one. A hundred writes at all go through A; B is started
// again, and answers as a removed node holding the term it opened; A is
// stopped and removed. The next owner holds the hundred: the replicas' term is
// A's new one, above B's. Guard: resume on the fallback — A's writes stay in
// its old term, B's term is above it, and the next owner takes B's copy,
// without the hundred.
func TestANodeThatRegainsABucketOpensANewTermWhenItsSuccessorIsSilent(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	ports := reserveNodePorts(t)
	joinerID := ports.ringNode().ID
	before := ringWith(t, 3, tc.addrs...)
	after := ringWith(t, 3, append(append([]string(nil), tc.addrs...), joinerID)...)
	bucket := bucketNamed(t, "regained", func(name string) bool { return ownerOn(after, name) == joinerID })
	a := nodeIndexByAddr(t, tc.addrs, ownerOn(before, bucket))
	tc.ok(a, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(a, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, bucket, 3)
	tc.ok(a, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "seed",
		Data: map[string]interface{}{"n": -1}, WriteConcern: "all"})

	var members []*Cluster
	for index := range tc.nodes {
		members = append(members, clusterOf(tc, index))
	}
	resume := pauseTakeOvers(t, members...)
	b := joinNewcomer(t, tc, a, ports)
	waitTakenOver(t, b.node.server.core, testDatabase, bucket)
	opened, err := b.cluster().answerCopy(ReplBucketRef{Database: testDatabase, Bucket: bucket})
	if err != nil || !opened.Opened {
		t.Fatalf("setup: the joiner did not open %s (%+v, %v)", bucket, opened, err)
	}
	// An entry in B's term, which only B holds: a term with no entry is
	// transparent (A-10.2), and the next owner would open past it either way.
	if frame := tc.frameOn(b.node, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "only-b",
		Data: map[string]interface{}{"n": 0}, WriteConcern: "one"}); frame.Opcode == transport.OpError {
		t.Fatalf("setup: a write at one through the joiner: %s", frame.Payload)
	}
	b.stop()
	removeRunningThrough(t, tc, a, joinerID)
	resume()

	waitTakenOver(t, tc.nodes[a].server.core, testDatabase, bucket)
	regained, err := clusterOf(tc, a).answerCopy(ReplBucketRef{Database: testDatabase, Bucket: bucket})
	if err != nil {
		t.Fatalf("reading node %d's copy: %v", a+1, err)
	}
	if regained.Term <= opened.Term {
		t.Errorf("node %d regained %s in term %d, not above the term %d its silent successor opened", a+1, bucket, regained.Term, opened.Term)
	}
	for i := 0; i < 100; i++ {
		frame := tc.frame(a, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket,
			Key: fmt.Sprintf("h%03d", i), Data: map[string]interface{}{"n": i}, WriteConcern: "all"})
		if frame.Opcode == transport.OpError {
			t.Fatalf("write %d at all through node %d: %s", i, a+1, frame.Payload)
		}
	}

	b.ports = b.ports.rebind(t)
	b.start(t, b.node.cfg)
	if err := b.cluster().reconcileMembership(clusterOf(tc, a).self.ID, clusterOf(tc, a).memberNodes(), clusterOf(tc, a).memberIDs(), clusterOf(tc, a).Epoch()); err != nil {
		t.Fatalf("telling the restarted joiner of its removal: %v", err)
	}
	if !b.cluster().removed() {
		t.Fatal("setup: the restarted joiner does not know it was removed")
	}
	via := (a + 1) % 3
	tc.stopNodeKeepingData(a)
	removeRunningThrough(t, tc, via, tc.addrs[a])
	// A removed node follows later memberships as gossip brings them, and
	// answers questions only from its own epoch.
	viaCluster := clusterOf(tc, via)
	if err := b.cluster().reconcileMembership(viaCluster.self.ID, viaCluster.memberNodes(), viaCluster.memberIDs(), viaCluster.Epoch()); err != nil {
		t.Fatalf("bringing the removed joiner to the removal of node %d: %v", a+1, err)
	}
	var rest []string
	for index, addr := range tc.addrs {
		if index != a {
			rest = append(rest, addr)
		}
	}
	next := nodeIndexByAddr(t, tc.addrs, ownerOn(ringWith(t, 3, rest...), bucket))
	waitTakenOver(t, tc.nodes[next].server.core, testDatabase, bucket)
	var missing int
	for i := 0; i < 100; i++ {
		if !holdsKeyIn(tc.nodes[next], testDatabase, bucket, fmt.Sprintf("h%03d", i)) {
			missing++
		}
	}
	if missing > 0 {
		t.Errorf("the next owner, node %d, lacks %d of the hundred writes acknowledged at all through node %d", next+1, missing, a+1)
	}
}

// killAtHook stops node index of tc while its worker is held in a hook — as a
// kill there would leave it: the hook returns only once the node's context is
// cancelled, so the worker does nothing more that lasts — and starts it again
// on its data directory.
func killAtHook(t *testing.T, tc *testCluster, index int, held chan struct{}) {
	t.Helper()
	node := tc.nodes[index]
	base := filepath.Dir(node.cfg.Storage.DataDir)
	c := node.server.core.cluster
	if node.client != nil {
		_ = node.client.Close()
		node.client = nil
	}
	stopped := make(chan error, 1)
	go func() { stopped <- node.server.Stop() }()
	if !holdsWithin(10*time.Second, func() bool { return c.ctx.Err() != nil }) {
		t.Fatalf("node %d's cluster never stopped", index+1)
	}
	close(held)
	if err := <-stopped; err != nil {
		t.Fatalf("stopping node %d: %v", index+1, err)
	}
	node.server = nil
	tc.nodes[index] = tc.startNode(index, base, tc.token)
}

// TestANewOwnerThatCrashesMidHandOffResumes (rev 7 step 10.5): a new owner
// killed in the middle of its take-over finishes it once restarted, and holds
// every acknowledged write — killed after it deleted its own copy to copy a
// more complete one over it; while it waits for a member that may hold the
// newest copy; and between the commit of the copy it took and its next
// request.
func TestANewOwnerThatCrashesMidHandOffResumes(t *testing.T) {
	t.Run("after it dropped its copy", func(t *testing.T) {
		loss := setUpAnOwnerLossWithALaggingReplica(t)
		defer loss.tc.stop()
		bucket := loss.toLagging[0]
		laggingID := loss.tc.addrs[loss.lagging]
		reached, held := make(chan struct{}), make(chan struct{})
		var reachedOnce sync.Once
		hook := func(nodeID, database, name string) {
			if nodeID != laggingID || name != bucket {
				return
			}
			reachedOnce.Do(func() { close(reached) })
			<-held
		}
		takeOverAfterReleaseHook.Store(&hook)
		t.Cleanup(func() { takeOverAfterReleaseHook.Store(nil) })
		loss.removeTheStoppedOwner(t)
		select {
		case <-reached:
		case <-time.After(takeOverWait):
			t.Fatalf("setup: the lagging replica, node %d, never deleted its copy of %s to take it over", loss.lagging+1, bucket)
		}
		killAtHook(t, loss.tc, loss.lagging, held)
		takeOverAfterReleaseHook.Store(nil)

		waitTakenOver(t, loss.tc.nodes[loss.lagging].server.core, testDatabase, bucket)
		if missing := loss.missingWrites(loss.lagging, bucket); len(missing) > 0 {
			t.Errorf("the restarted new owner, node %d, lacks %v of %s", loss.lagging+1, missing, bucket)
		}
	})
	t.Run("while it waits for a member that may hold the newest copy", func(t *testing.T) {
		tc := newTestCluster(t, 3, 2)
		defer tc.stop()
		ports := reserveNodePorts(t)
		newcomer := ports.ringNode()
		after := ringWith(t, 2, append(append([]string(nil), tc.addrs...), newcomer.ID)...)
		bucket := bucketNamed(t, "waited", func(name string) bool { return ownerOn(after, name) == newcomer.ID })
		oldOwner := nodeIndexByAddr(t, tc.addrs, ownerOn(ringWith(t, 2, tc.addrs...), bucket))
		tc.ok(oldOwner, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
		tc.ok(oldOwner, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
		waitForServing(t, tc, testDatabase, bucket, 2)
		for i := 0; i < 5; i++ {
			tc.ok(oldOwner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: fmt.Sprintf("w%d", i),
				Data: map[string]interface{}{"n": i}, WriteConcern: "all"})
		}
		startOldOwner := stopNode(t, tc, oldOwner)
		joiner := joinNewcomer(t, tc, (oldOwner+1)%3, ports)
		key := placementOf(testDatabase, bucket)
		if !holdsWithin(takeOverWait, func() bool {
			return slices.Contains(joiner.cluster().takeOverStatusOf(key).waitingOn, tc.addrs[oldOwner])
		}) {
			t.Fatalf("setup: the joiner never waited for the stopped old owner, node %d", oldOwner+1)
		}
		joiner.restart(t)
		startOldOwner()
		// A node stopped across a join learns the joiner's key when it returns.
		trustJoinerFingerprint(t, tc, joiner.node.cfg.Server.Listen, joiner.node.cfg.Storage.DataDir)
		tc.bringMembershipTo(t, joiner.cluster())
		waitTakenOver(t, joiner.node.server.core, testDatabase, bucket)
		for i := 0; i < 5; i++ {
			if key := fmt.Sprintf("w%d", i); !holdsKeyIn(joiner.node, testDatabase, bucket, key) {
				t.Errorf("the restarted joiner lacks %s of %s, acknowledged at all", key, bucket)
			}
		}
	})
	t.Run("between the commit of its copy and its next request", func(t *testing.T) {
		h := setUpAHeldTakeOver(t)
		defer h.tc.stop()
		h.release()
		waitTakenOver(t, h.joiner.node.server.core, testDatabase, h.bucket)
		h.joiner.restart(t)
		waitTakenOver(t, h.joiner.node.server.core, testDatabase, h.bucket)
		if found, err := h.joiner.node.server.core.Find(FindRequest{Database: testDatabase, Bucket: h.bucket, Key: "held"}); err != nil || !found.Found {
			t.Errorf("the restarted joiner does not hold %q of %s (%v)", "held", h.bucket, err)
		}
		frame := h.tc.frameOn(h.joiner.node, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: h.bucket, Key: "next",
			Data: map[string]interface{}{"n": 2}, WriteConcern: "all"})
		if frame.Opcode == transport.OpError {
			t.Errorf("the restarted joiner refused the next write: %s", frame.Payload)
		}
	})
}

// removableKeepsTheJoiner reports whether some node other than name's owner
// before the join can be removed with name still owned by the joiner, so the
// held-take-over-across-a-removal case can always be set up whatever ports the
// test drew.
func removableKeepsTheJoiner(addrs []string, newcomerID, name string) bool {
	place := func(nodes []string) *ring.Ring {
		placed := ring.New(2)
		for _, addr := range nodes {
			if placed.AddNode(ring.Node{ID: addr, Addr: addr}) != nil {
				return nil
			}
		}
		return placed
	}
	before := place(addrs)
	if before == nil {
		return false
	}
	oldOwner := ownerOn(before, name)
	for _, candidate := range addrs {
		if candidate == oldOwner {
			continue
		}
		rest := []string{newcomerID}
		for _, other := range addrs {
			if other != candidate {
				rest = append(rest, other)
			}
		}
		if placement := place(rest); placement != nil && ownerOn(placement, name) == newcomerID {
			return true
		}
	}
	return false
}
