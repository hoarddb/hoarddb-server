package server

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Step 10b.5 under I-H1: a take-over compares copies only within one
// incarnation of a name. A stray — a bucket created beside a copy nobody could
// see — needs a seam (ownerQuestionPassOverHook), as dispatch 10a's tests of
// it do (stop-and-ask S-8).

// strayCase is a name with two live incarnations: I1, created by its first
// owner and held by the members of its first set, and I2, created beside it
// by a joiner that owns the name, with the door's question passed over, and
// held by the joiner and the members of its set that held nothing.
type strayCase struct {
	tc     *testCluster
	rf     int
	bucket string
	// first are the founders that hold I1, replicas those that hold I2 beside
	// the stray's joiner.
	first, replicas []int
	stray           *restartableJoiner
	later           *testNodePorts
}

// strayLayout is a name's place in the construction: its sets on the
// founders' ring and once the stray's joiner has joined, and the identities
// of the founders, the stray's joiner and the later node.
type strayLayout struct {
	name          string
	founders      []string
	stray, later  string
	first, joined []string
}

// setUpAStray builds the two incarnations. final reports whether a name's
// place in the ring the test ends on suits the test.
func setUpAStray(t *testing.T, founders, rf int, final func(layout strayLayout) bool) *strayCase {
	t.Helper()
	switchJoinFreezeOff(t)
	tc := newTestClusterWith(t, founders, rf, func(_ int, cfg *config.Config) { cfg.Server.AutoRebalance = false })
	before := ringWith(t, rf, tc.addrs...)
	var (
		strayPorts, laterPorts *testNodePorts
		first, replicas        []int
		bucket                 string
	)
	// Some pairs of joiner addresses admit no name the construction needs:
	// the random ports are drawn again until one does.
	for attempt := 0; attempt < 20 && bucket == ""; attempt++ {
		strayPorts, laterPorts = reserveNodePorts(t), reserveNodePorts(t)
		strayID, laterID := strayPorts.ringNode().ID, laterPorts.ringNode().ID
		joined := ringWith(t, rf, append(append([]string(nil), tc.addrs...), strayID)...)
		bucket = strayNamed(func(name string) bool {
			key := placementOf(testDatabase, name).encoded
			firstSet, _ := before.Replicas(key, rf)
			joinedSet, _ := joined.Replicas(key, rf)
			if joinedSet[0].ID != strayID {
				return false
			}
			// A member of the stray's set that holds I1 refuses its create (10a.6);
			// at least one holds nothing, and takes I2.
			var takers []string
			for _, member := range joinedSet[1:] {
				if !nodeListed(firstSet, member.ID) {
					takers = append(takers, member.ID)
				}
			}
			if len(takers) == 0 {
				return false
			}
			layout := strayLayout{name: name, founders: tc.addrs, stray: strayID, later: laterID,
				first: nodeIDs(firstSet), joined: nodeIDs(joinedSet)}
			if !final(layout) {
				return false
			}
			first, replicas = nil, nil
			for _, member := range firstSet {
				first = append(first, nodeIndexByAddr(t, tc.addrs, member.ID))
			}
			for _, member := range takers {
				replicas = append(replicas, nodeIndexByAddr(t, tc.addrs, member))
			}
			return true
		})
	}
	if bucket == "" {
		t.Fatal("setup: no pair of joiner addresses of twenty admits a bucket name stray00000..stray49999 that satisfies the test's placement")
	}
	s := &strayCase{tc: tc, rf: rf, bucket: bucket, first: first, replicas: replicas, later: laterPorts}
	tc.ok(first[0], transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(first[0], transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, bucket, rf)
	tc.ok(first[0], transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "i1", Data: map[string]interface{}{"n": 1}, WriteConcern: "all"})

	s.stray = joinNewcomer(t, tc, first[0], strayPorts)
	ownerQuestionPassOverHook.Store(true)
	created := holdsWithin(takeOverWait, func() bool {
		frame := tc.frameOn(s.stray.node, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
		return frame.Opcode != transport.OpError && s.stray.node.server.core.bucketStoreOpen(testDatabase, bucket)
	})
	ownerQuestionPassOverHook.Store(false)
	if !created {
		t.Fatalf("setup: the joiner never created the stray incarnation of %s", bucket)
	}
	if !holdsWithin(takeOverWait, func() bool {
		frame := tc.frameOn(s.stray.node, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "i2",
			Data: map[string]interface{}{"n": 2}, WriteConcern: "majority"})
		if frame.Opcode == transport.OpError {
			return false
		}
		for _, index := range replicas {
			if !holdsKeyIn(tc.nodes[index], testDatabase, bucket, "i2") {
				return false
			}
		}
		return true
	}) {
		t.Fatalf("setup: the stray's write never reached its replicas, nodes %v", replicas)
	}
	strayIncarnation := lineageOf(t, s.stray.node.server.core, testDatabase, bucket).Incarnation
	firstIncarnation := lineageOf(t, tc.nodes[first[0]].server.core, testDatabase, bucket).Incarnation
	if strayIncarnation == firstIncarnation {
		t.Fatalf("setup: the stray holds the first incarnation %d", firstIncarnation)
	}
	for _, index := range replicas {
		if held := lineageOf(t, tc.nodes[index].server.core, testDatabase, bucket).Incarnation; held != strayIncarnation {
			t.Fatalf("setup: node %d holds incarnation %d, not the stray's %d", index+1, held, strayIncarnation)
		}
	}
	return s
}

// joinLater joins the later node through a founder, and returns it.
func (s *strayCase) joinLater(t *testing.T) *restartableJoiner {
	t.Helper()
	return joinNewcomer(t, s.tc, s.first[0], s.later)
}

// requireIncarnationKept fails unless node's own copy of the bucket still
// holds key, a document of its incarnation, and not other.
func requireIncarnationKept(t *testing.T, node *testNode, bucket, key, other, who string) {
	t.Helper()
	if !holdsKeyIn(node, testDatabase, bucket, key) || holdsKeyIn(node, testDatabase, bucket, other) {
		t.Errorf("%s no longer holds its own incarnation of %s (%s: %v, %s: %v): a copy of another incarnation was put over it",
			who, bucket, key, holdsKeyIn(node, testDatabase, bucket, key), other, holdsKeyIn(node, testDatabase, bucket, other))
	}
}

// TestATakeOverNeverTakesACopyOfAnotherIncarnation is the fallback with two
// members' live copies of two incarnations (review m-4): the stray's joiner
// is stopped and removed, and a later node that holds nothing becomes the
// name's owner. Its previous owners do not answer that they opened — the
// stray's joiner is gone, and the first owner regained the name and never
// settled — so the fallback asks every member, and finds I1 on two of them
// and I2 on a third: it takes neither, the bucket stays refused with a message
// that names the holders, and every copy keeps its incarnation. Guard:
// compare across incarnations — the later owner takes I2, the newer term.
func TestATakeOverNeverTakesACopyOfAnotherIncarnation(t *testing.T) {
	s := setUpAStray(t, 4, 2, func(layout strayLayout) bool {
		return ownerOfOn(t, layout.name, append(append([]string(nil), layout.founders...), layout.later)...) == layout.later
	})
	defer s.tc.stop()
	s.stray.stop()
	removeAndForget(t, s.tc, s.first[0], s.stray.ports.ringAddr)
	later := s.joinLater(t)
	ctx, cancel := context.WithTimeout(context.Background(), takeOverWait)
	defer cancel()
	if err := later.cluster().ensureMigrated(ctx, testDatabase, s.bucket); err == nil {
		t.Fatalf("the later owner took %s over although two members hold live copies of two incarnations of it", s.bucket)
	}
	time.Sleep(2 * time.Second)
	if later.node.server.core.bucketStoreOpen(testDatabase, s.bucket) {
		t.Fatalf("the later owner holds a copy of %s: it took one incarnation over the other", s.bucket)
	}
	requireRefusedNaming(t, s.tc, later.node, s.bucket, s.tc.addrs[s.replicas[0]])
	for _, index := range s.first {
		requireIncarnationKept(t, s.tc.nodes[index], s.bucket, "i1", "i2", fmt.Sprintf("node %d", index+1))
	}
	requireIncarnationKept(t, s.tc.nodes[s.replicas[0]], s.bucket, "i2", "i1", fmt.Sprintf("node %d", s.replicas[0]+1))
	if seen := later.cluster().divergencesSeen.Load(); seen == 0 {
		t.Error("the later owner counted no divergence for the two incarnations it found")
	}
}

// TestAnOrdinaryTakeOverOfAStrayIsReported is the ordinary path's case
// (review m-4): a later node becomes the name's owner while its previous
// owner, which created the stray, still answers that it opened: the stray is
// served — nothing is lost, its incarnation is the one the previous owner
// held — and a follower that holds the first incarnation is not copied over:
// it keeps its copy, and the divergence is counted. Guard: copy the stray
// over the followers — the follower loses I1.
//
// The stray's creator is a founder: a joiner takes no bucket from a node that
// joined before it on this ring. Five founders at factor 2; the name's first
// owner A is removed while it runs, which moves the name to two founders that
// hold nothing; with auto_rebalance off the new owner has not copied it, and
// creates it again, the door's question passed over; then a later node joins
// and takes the name, with a first holder of I1 back in its set.
func TestAnOrdinaryTakeOverOfAStrayIsReported(t *testing.T) {
	switchJoinFreezeOff(t)
	tc := newTestClusterWith(t, 5, 2, func(_ int, cfg *config.Config) { cfg.Server.AutoRebalance = false })
	defer tc.stop()
	founders := ringWith(t, 2, tc.addrs...)
	var (
		laterPorts                           *testNodePorts
		later, bucket                        string
		first, removed, strayOwner, follower int
	)
	// Some joiner addresses admit no name the construction needs: the random
	// ports are drawn again until one does.
	for attempt := 0; attempt < 20 && bucket == ""; attempt++ {
		laterPorts = reserveNodePorts(t)
		later = laterPorts.ringNode().ID
		bucket = strayNamed(func(name string) bool {
			key := placementOf(testDatabase, name).encoded
			firstSet, _ := founders.Replicas(key, 2)
			var rest []string
			for _, addr := range tc.addrs {
				if addr != firstSet[0].ID {
					rest = append(rest, addr)
				}
			}
			strayRing := ringWith(t, 2, rest...)
			straySet, _ := strayRing.Replicas(key, 2)
			if nodeListed(firstSet, straySet[0].ID) || nodeListed(firstSet, straySet[1].ID) {
				return false
			}
			finalRing := ringWith(t, 2, append(rest, later)...)
			finalSet, _ := finalRing.Replicas(key, 2)
			if finalSet[0].ID != later || finalSet[1].ID != firstSet[1].ID {
				return false
			}
			removed, first = nodeIndexByAddr(t, tc.addrs, firstSet[0].ID), nodeIndexByAddr(t, tc.addrs, firstSet[1].ID)
			strayOwner, follower = nodeIndexByAddr(t, tc.addrs, straySet[0].ID), first
			return true
		})
	}
	if bucket == "" {
		t.Fatal("setup: no joiner address of twenty admits a bucket name stray00000..stray49999 that satisfies the test's placement")
	}
	tc.ok(removed, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(removed, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, bucket, 2)
	tc.ok(removed, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "i1", Data: map[string]interface{}{"n": 1}, WriteConcern: "all"})
	removeAndTell(t, tc, strayOwner, removed)
	for index := range tc.nodes {
		if index == removed || index == strayOwner {
			continue
		}
		via := clusterOf(tc, strayOwner)
		if err := clusterOf(tc, index).reconcileMembership(via.self.ID, via.memberNodes(), via.memberIDs(), via.Epoch()); err != nil {
			t.Fatalf("bringing node %d to the removal: %v", index+1, err)
		}
	}
	ownerQuestionPassOverHook.Store(true)
	created := holdsWithin(takeOverWait, func() bool {
		frame := tc.frame(strayOwner, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
		return frame.Opcode != transport.OpError && tc.nodes[strayOwner].server.core.bucketStoreOpen(testDatabase, bucket)
	})
	ownerQuestionPassOverHook.Store(false)
	if !created {
		t.Fatalf("setup: node %d never created the stray incarnation of %s", strayOwner+1, bucket)
	}
	tc.ok(strayOwner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "i2", Data: map[string]interface{}{"n": 2}, WriteConcern: "one"})

	joiner := joinNewcomer(t, tc, strayOwner, laterPorts)
	ctx, cancel := context.WithTimeout(context.Background(), takeOverWait)
	defer cancel()
	if err := joiner.cluster().ensureMigrated(ctx, testDatabase, bucket); err != nil {
		t.Fatalf("the later owner's take-over of %s from the stray's creator: %v", bucket, err)
	}
	if !holdsKeyIn(joiner.node, testDatabase, bucket, "i2") || holdsKeyIn(joiner.node, testDatabase, bucket, "i1") {
		t.Fatalf("the later owner does not serve the stray incarnation its previous owner held")
	}
	followerNode := tc.nodes[follower]
	if !holdsWithin(takeOverWait, func() bool { return followerNode.server.core.cluster.divergencesSeen.Load() > 0 }) {
		t.Errorf("the follower, node %d, which holds the first incarnation, never counted the divergence", follower+1)
	}
	requireIncarnationKept(t, followerNode, bucket, "i1", "i2", fmt.Sprintf("the follower, node %d,", follower+1))
}

// ownerOfOn is the owner of name in testDatabase on the ring of addrs.
func ownerOfOn(t *testing.T, name string, addrs ...string) string {
	t.Helper()
	return ownerOn(ringWith(t, 2, addrs...), name)
}

// requireRefusedNaming fails unless a write through node to the bucket is
// refused UNAVAILABLE with a message naming every one of names.
func requireRefusedNaming(t *testing.T, tc *testCluster, node *testNode, bucket string, names ...string) {
	t.Helper()
	frame := tc.frameOn(node, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "refused", Data: map[string]interface{}{"n": 0}})
	refused := requireRetryRefusal(t, frame, "a write to a name with two live incarnations")
	for _, name := range names {
		if !strings.Contains(refused.Message, name) {
			t.Errorf("the refusal does not name %s: %s", name, refused.Message)
		}
	}
}

// removeAndForget removes a stopped node through node via and brings every
// running node to the removal.
func removeAndForget(t *testing.T, tc *testCluster, via int, removed string) {
	t.Helper()
	proposer := clusterOf(tc, via)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := proposer.proposeRemoval(ctx, removed); err != nil {
		t.Fatalf("removing %s: %v", removed, err)
	}
	for index, node := range tc.nodes {
		if index == via || node.server == nil {
			continue
		}
		if err := clusterOf(tc, index).reconcileMembership(proposer.self.ID, proposer.memberNodes(), proposer.memberIDs(), proposer.Epoch()); err != nil {
			t.Fatalf("bringing node %d to the removal of %s: %v", index+1, removed, err)
		}
	}
}

// strayNamed is bucketNamed over more names, the construction constraining a
// name on three rings; empty when none satisfies want.
func strayNamed(want func(name string) bool) string {
	for i := 0; i < 50000; i++ {
		if name := fmt.Sprintf("stray%05d", i); want(name) {
			return name
		}
	}
	return ""
}

// TestAFallbackCopiesFromARemovedNodeThatStillAnswers is review requirement 5:
// a node that left the membership within the history and still answers is a
// source. At factor 1 the only copy is on the owner, removed while it runs:
// the new owner copies it from the removed node, which keeps its copy. At
// factor 3 with every member's copy lost — one member's dropped locally, the
// other never made with auto_rebalance off — and the owner stopped and removed,
// the fallback finds the copy on the replica removed earlier, which still
// answers. Guard: skip nodes that left — the bucket is not found.
func TestAFallbackCopiesFromARemovedNodeThatStillAnswers(t *testing.T) {
	writeFive := func(t *testing.T, tc *testCluster, through int, bucket string) {
		t.Helper()
		for i := 0; i < 5; i++ {
			tc.ok(through, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: fmt.Sprintf("w%d", i),
				Data: map[string]interface{}{"n": i}, WriteConcern: "all"})
		}
	}
	requireFive := func(t *testing.T, node *testNode, bucket, who string) {
		t.Helper()
		for i := 0; i < 5; i++ {
			if key := fmt.Sprintf("w%d", i); !holdsKeyIn(node, testDatabase, bucket, key) {
				t.Errorf("%s lacks %s of %s", who, key, bucket)
			}
		}
	}

	t.Run("factor 1", func(t *testing.T) {
		tc := newTestCluster(t, 3, 1)
		defer tc.stop()
		bucket := bucketOwnedByAt(t, tc.addrs, tc.addrs[0], "only")
		tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
		tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash, ReplicationFactor: 1})
		waitForServing(t, tc, testDatabase, bucket, 1)
		writeFive(t, tc, 0, bucket)
		removeAndTell(t, tc, 1, 0)
		via := clusterOf(tc, 1)
		if err := clusterOf(tc, 2).reconcileMembership(via.self.ID, via.memberNodes(), via.memberIDs(), via.Epoch()); err != nil {
			t.Fatalf("bringing node 3 to the removal: %v", err)
		}
		newOwner := nodeIndexByAddr(t, tc.addrs, ownerOfOn(t, bucket, tc.addrs[1], tc.addrs[2]))
		ctx, cancel := context.WithTimeout(context.Background(), takeOverWait)
		defer cancel()
		if err := clusterOf(tc, newOwner).ensureMigrated(ctx, testDatabase, bucket); err != nil {
			t.Fatalf("the new owner's take-over of %s, whose only copy is on the removed node: %v", bucket, err)
		}
		requireFive(t, tc.nodes[newOwner], bucket, fmt.Sprintf("node %d, the new owner,", newOwner+1))
		if !tc.nodes[0].server.core.bucketStoreOpen(testDatabase, bucket) {
			t.Error("the removed node deleted its copy after the new owner copied it")
		}
	})

	t.Run("factor 3, every member's copy lost", func(t *testing.T) {
		tc := newTestClusterWith(t, 4, 3, func(_ int, cfg *config.Config) { cfg.Server.AutoRebalance = false })
		defer tc.stop()
		before := ringWith(t, 3, tc.addrs...)
		var owner, replica, member, outside int
		bucket := bucketNamed(t, "lost", func(name string) bool {
			set, _ := before.Replicas(placementOf(testDatabase, name).encoded, 3)
			owner = nodeIndexByAddr(t, tc.addrs, set[0].ID)
			replica, member = nodeIndexByAddr(t, tc.addrs, set[1].ID), nodeIndexByAddr(t, tc.addrs, set[2].ID)
			for index, addr := range tc.addrs {
				if !nodeListed(set, addr) {
					outside = index
				}
			}
			return true
		})
		tc.ok(owner, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
		tc.ok(owner, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
		waitForServing(t, tc, testDatabase, bucket, 3)
		writeFive(t, tc, owner, bucket)
		// The replica leaves while it runs, and keeps its copy.
		removeAndForget(t, tc, owner, tc.addrs[replica])
		if !tc.nodes[replica].server.core.bucketStoreOpen(testDatabase, bucket) {
			t.Fatal("setup: the removed replica no longer holds its copy")
		}
		// Every member's copy but the owner's is lost: the remaining replica's,
		// and the one the owner's stream gave the node that entered the set
		// when the replica left (its replay of the create from the log).
		holdsWithin(5*time.Second, func() bool { return tc.nodes[outside].server.core.bucketStoreOpen(testDatabase, bucket) })
		for _, index := range []int{member, outside} {
			if !tc.nodes[index].server.core.bucketStoreOpen(testDatabase, bucket) {
				continue
			}
			if resp, err := tc.nodes[index].server.core.DeleteBucket(DeleteBucketRequest{Database: testDatabase, Name: bucket}); err != nil || !resp.Success {
				t.Fatalf("setup: losing node %d's copy: %+v, %v", index+1, resp, err)
			}
		}
		// The owner is stopped and removed.
		tc.stopNodeKeepingData(owner)
		removeAndForget(t, tc, member, tc.addrs[owner])
		newOwner := nodeIndexByAddr(t, tc.addrs, ownerOfOn(t, bucket, tc.addrs[member], tc.addrs[outside]))
		for _, index := range []int{member, outside} {
			if tc.nodes[index].server.core.bucketStoreOpen(testDatabase, bucket) {
				t.Fatalf("setup: node %d, a member, still holds a copy", index+1)
			}
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*takeOverWait)
		defer cancel()
		if err := clusterOf(tc, newOwner).ensureMigrated(ctx, testDatabase, bucket); err != nil {
			t.Fatalf("the new owner's take-over of %s, whose only copy is on a node removed earlier: %v", bucket, err)
		}
		requireFive(t, tc.nodes[newOwner], bucket, fmt.Sprintf("node %d, the new owner,", newOwner+1))
		if !tc.nodes[replica].server.core.bucketStoreOpen(testDatabase, bucket) {
			t.Error("the removed replica deleted its copy after the new owner copied it")
		}
	})
}

// TestARejoinedNodeServesNothingItHeldBeforeItWasRemoved is review
// requirement 6: the owner of a bucket is stopped and removed, its successor
// takes the bucket over and takes writes, and the node is added back and
// started on its data directory. It catches up through the epoch that left it
// out and the one that lists it again; the placement passes over them mark its
// copy, and a marked copy counts as not opened: with its take-over worker
// stopped it refuses both a read and a write of the bucket it owns again,
// rather than serve what it held before its removal. Started, the worker takes
// the bucket over, and the node serves the writes its successor took. Guard:
// count a held copy as opened — the rejoined node answers the read from its
// copy from before the removal.
func TestARejoinedNodeServesNothingItHeldBeforeItWasRemoved(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	rejoined := ownerIndexOf(t, tc, testBucket)
	survivorIndex := (rejoined + 1) % 3
	rejoinedID := tc.addrs[rejoined]
	tc.ok(survivorIndex, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(rejoined, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	for i := 0; i < 3; i++ {
		if frame := tc.writeConcerned(rejoined, fmt.Sprintf("before%d", i), "all"); frame.Opcode == transport.OpError {
			t.Fatalf("write %d before the removal: %s", i, frame.Payload)
		}
	}

	base := tc.stopNodeKeepingData(rejoined)
	survivor := clusterOf(tc, survivorIndex)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := survivor.proposeRemoval(ctx, rejoinedID); err != nil {
		t.Fatalf("removing the stopped owner: %v", err)
	}
	tc.bringMembershipTo(t, survivor)
	var remaining []string
	for index, addr := range tc.addrs {
		if index != rejoined {
			remaining = append(remaining, addr)
		}
	}
	successor := nodeIndexByAddr(t, tc.addrs, ownerOfOn(t, testBucket, remaining...))
	waitTakenOver(t, tc.nodes[successor].server.core, testDatabase, testBucket)
	for i := 0; i < 3; i++ {
		if frame := tc.writeConcerned(successor, fmt.Sprintf("away%d", i), "majority"); frame.Opcode == transport.OpError {
			t.Fatalf("write %d while the owner was removed: %s", i, frame.Payload)
		}
	}

	if err := survivor.proposeJoin(ctx, ring.Node{ID: rejoinedID, Addr: rejoinedID}); err != nil {
		t.Fatalf("adding the removed node back: %v", err)
	}
	tc.bringMembershipTo(t, survivor)
	takeOverOffHook.Store(true)
	t.Cleanup(func() { takeOverOffHook.Store(false) })
	tc.nodes[rejoined] = tc.startNode(rejoined, base, tc.token)
	tc.bringMembershipTo(t, survivor)
	back := tc.nodes[rejoined].server.core.cluster
	if back.removed() || back.Epoch() != survivor.Epoch() {
		t.Fatalf("setup: the re-added node is at epoch %d (removed %v), want the cluster's %d", back.Epoch(), back.removed(), survivor.Epoch())
	}
	if owner, err := back.Owner(placementOf(testDatabase, testBucket)); err != nil || owner.ID != rejoinedID {
		t.Fatalf("setup: the re-added node does not own %s again (%v, %v)", testBucket, owner.ID, err)
	}

	read := tc.frame(rejoined, transport.OpGet, FindRequest{Database: testDatabase, Bucket: testBucket, Key: "before0"})
	if read.Opcode != transport.OpError {
		t.Fatalf("a read at the rejoined owner before it took the bucket over was answered %s, from the copy it held before its removal", read.Opcode)
	}
	if refused := refusalOf(t, read); refused.Code != int(transport.ErrUnavailable) || !strings.Contains(refused.Message, "moving to this node") {
		t.Errorf("a read at the rejoined owner before it took the bucket over: code %d; want UNAVAILABLE, ownership moving: %s", refused.Code, refused.Message)
	}
	write := tc.writeConcerned(rejoined, "refused", "one")
	requireRetryRefusal(t, write, "a write at the rejoined owner before it took the bucket over")

	takeOverOffHook.Store(false)
	back.wakeTakeOver(testDatabase, testBucket)
	waitTakenOver(t, tc.nodes[rejoined].server.core, testDatabase, testBucket)
	for _, key := range []string{"before0", "before2", "away0", "away2"} {
		if found, message := tc.readFromNode(rejoined, key); !found {
			t.Errorf("the rejoined owner does not serve %s after its take-over: %s", key, message)
		}
	}
}
