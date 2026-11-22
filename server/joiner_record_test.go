package server

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The joiner learns its previous owner (on-demand pull build doc step 9; spec
// §6.7, O-7 (b)): the answer a joiner adopts its join from — a gossip answer
// or the join's commit — carries the cluster's record of the joiner's
// provisional epoch, which the joiner writes over its provisional record, so
// its take-over asks the bucket's previous owner first instead of every
// member.

// founderRecordAt fails the test unless c's history records epoch with
// exactly the member IDs want.
func founderRecordAt(t *testing.T, c *Cluster, epoch uint64, want []string) {
	t.Helper()
	members, recorded := c.membersAt(epoch)
	if !recorded {
		t.Fatalf("%s's history does not record epoch %d; want the cluster's record %v", c.self.ID, epoch, want)
	}
	got := memberIDsOf(members)
	slices.Sort(got)
	sorted := slices.Sorted(slices.Values(want))
	if !slices.Equal(got, sorted) {
		t.Errorf("%s's history records epoch %d as %v; want the cluster's record %v", c.self.ID, epoch, got, sorted)
	}
}

// askedPreviousOwnerFirst names bucket to c's worker with the open held, and
// fails the test unless the take-over published previous as its final
// previous owner after one question, with no fallback.
func askedPreviousOwnerFirst(t *testing.T, c *Cluster, bucket, previous string) *workerGate {
	t.Helper()
	held := newWorkerGate(t)
	onBucketHook(t, &takeOverBeforeOpenHook, c.self.ID, func(name string) {
		if name == bucket {
			held.hold()
		}
	})
	asked := countQuestions(t, c.self.ID)
	fallbacksBefore := c.takeOverFallbacks.Load()
	if !c.wakeTakeOverCapped(testDatabase, bucket) {
		t.Fatalf("setup: %s was refused a place in the take-over queue", bucket)
	}
	held.await(t, "the open of the joiner's take-over", takeOverWait)
	status := c.takeOverStatusOf(placementOf(testDatabase, bucket))
	if !status.sourceKnown || status.source.ID != previous {
		t.Errorf("the joiner's take-over of %s published %+v; want the previous owner %s as its source", bucket, status, previous)
	}
	if n := asked.Load(); n != 1 {
		t.Errorf("the joiner's take-over asked %d questions; want one, to the previous owner", n)
	}
	held.release()
	if !servedWithin(c, bucket, takeOverWait) {
		t.Fatalf("the joiner's take-over of %s did not land once released", bucket)
	}
	if n := c.takeOverFallbacks.Load() - fallbacksBefore; n != 0 {
		t.Errorf("the joiner's take-over ran the fallback %d times; the ordinary path decides", n)
	}
	return held
}

// previousOwnerOf is the owner of bucket on the ring of members.
func previousOwnerOf(t *testing.T, members []ring.Node, bucket string) string {
	t.Helper()
	return ownerOn(ringOf(t, members, 3), bucket)
}

// TestAJoinersTakeOverAsksItsPreviousOwnerFirst is §6.7 through a gossip
// answer: at the first join of a cluster founded and never changed (J = 2),
// the joiner adopts its join from a member's gossip answer, which carries the
// cluster's record of epoch 1, and its take-over asks the bucket's previous
// owner once and publishes it as the source, with no fallback. The J = 3
// variant guards the re-entering branch: the first joiner takes the bucket
// over, and a second joiner adopts epoch 3 from a gossip answer that also
// carries epochs 1 and 2, and asks the first joiner. Guard: the answer to a
// provisional asker is historyAbove(req.Epoch - 1).
func TestAJoinersTakeOverAsksItsPreviousOwnerFirst(t *testing.T) {
	t.Run("J = 2", func(t *testing.T) {
		s := setUpJoinerOwningUnpulledWith(t, joinerSetup{prefix: "learned", count: 1, adoptByGossip: true})
		founders := memberIDsOf(s.members[:len(s.members)-1])
		founderRecordAt(t, s.joiner, InitialEpoch, founders)
		askedPreviousOwnerFirst(t, s.joiner, s.buckets[0], previousOwnerOf(t, s.members[:len(s.members)-1], s.buckets[0]))
	})
	t.Run("J = 3", func(t *testing.T) {
		secondPorts := reserveNodePorts(t)
		s := setUpJoinerOwningUnpulledWith(t, joinerSetup{prefix: "learned3", count: 1, adoptByGossip: true, also: func(joined []ring.Node, name string) bool {
			return ownerOn(ringOf(t, append(append([]ring.Node{}, joined...), secondPorts.ringNode()), 3), name) == secondPorts.ringAddr
		}, reseed: func() { secondPorts = reserveNodePorts(t) }})
		takenOverBy(t, s.joiner, s.buckets[0])
		second := joinSecond(t, s, secondPorts, nil)
		c := second.server.core.cluster
		founderRecordAt(t, c, InitialEpoch, memberIDsOf(s.members[:len(s.members)-1]))
		founderRecordAt(t, c, InitialEpoch+1, memberIDsOf(s.members))
		askedPreviousOwnerFirst(t, c, s.buckets[0], previousOwnerOf(t, s.members, s.buckets[0]))
	})
}

// takenOverBy has c take bucket over and land it, so c is the bucket's final
// previous owner when the next join moves it on.
func takenOverBy(t *testing.T, c *Cluster, bucket string) {
	t.Helper()
	if !c.wakeTakeOverCapped(testDatabase, bucket) || !servedWithin(c, bucket, takeOverWait) {
		t.Fatalf("setup: %s did not take %s over", c.self.ID, bucket)
	}
}

// joinSecond joins a second newcomer on secondPorts to s's cluster, which J1
// is a member of now, and waits for it to adopt the join by gossip. prepare,
// when set, runs before the second newcomer starts.
func joinSecond(t *testing.T, s joinerOwningUnpulled, secondPorts *testNodePorts, prepare func()) *testNode {
	t.Helper()
	s.tc.nodes = append(s.tc.nodes, s.joinerNode)
	s.tc.addrs = append(s.tc.addrs, s.joiner.self.ID)
	s.tc.fingerprints = append(s.tc.fingerprints, preGeneratedNodeFingerprints(t, []string{s.joinerNode.cfg.Storage.DataDir})...)
	proposer := s.tc.nodes[0].server.core.cluster
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := proposer.proposeJoin(ctx, secondPorts.ringNode()); err != nil {
		t.Fatalf("proposing the second join: %v", err)
	}
	for _, node := range s.tc.nodes {
		t.Cleanup(holdStreamPushesOnly(node.server.core.cluster.peerFor(secondPorts.ringNode())))
	}
	if prepare != nil {
		prepare()
	}
	second := startJoinTarget(t, s.tc, secondPorts, s.tc.token)
	t.Cleanup(func() { _ = second.client.Close() })
	adoptedByGossip(t, second.server.core.cluster, proposer.Epoch())
	return second
}

// adoptedByGossip waits until c is at epoch.
func adoptedByGossip(t *testing.T, c *Cluster, epoch uint64) {
	t.Helper()
	if !holdsWithin(takeOverWait, func() bool { return c.Epoch() >= epoch }) {
		t.Fatalf("setup: %s never adopted membership epoch %d (it is at %d)", c.self.ID, epoch, c.Epoch())
	}
}

// TestAJoinersTakeOverAsksItsPreviousOwnerFirstThroughTheCommit is §6.7 on
// the commit path (ODP-1, stop-and-ask 15): the newcomer runs before the join
// is proposed and its gossip reconciliation is skipped, so the join's commit
// is what it adopts from. The commit carries historyAbove(InitialEpoch - 1) to
// the newcomer alone, and its take-over asks the previous owner first. In the
// J = 3 variant the commit carries every epoch, so the newcomer adopts the
// whole history from it, with no gossip. Guard: the commit's History.
func TestAJoinersTakeOverAsksItsPreviousOwnerFirstThroughTheCommit(t *testing.T) {
	t.Run("J = 2", func(t *testing.T) {
		s := setUpJoinerOwningUnpulledWith(t, joinerSetup{prefix: "committed", count: 1, newcomerFirst: true})
		founders := memberIDsOf(s.members[:len(s.members)-1])
		founderRecordAt(t, s.joiner, InitialEpoch, founders)
		askedPreviousOwnerFirst(t, s.joiner, s.buckets[0], previousOwnerOf(t, s.members[:len(s.members)-1], s.buckets[0]))
	})
	t.Run("J = 3", func(t *testing.T) {
		secondPorts := reserveNodePorts(t)
		s := setUpJoinerOwningUnpulledWith(t, joinerSetup{prefix: "committed3", count: 1, adoptByGossip: true, also: func(joined []ring.Node, name string) bool {
			return ownerOn(ringOf(t, append(append([]ring.Node{}, joined...), secondPorts.ringNode()), 3), name) == secondPorts.ringAddr
		}, reseed: func() { secondPorts = reserveNodePorts(t) }})
		takenOverBy(t, s.joiner, s.buckets[0])
		second := joinSecondThroughTheCommit(t, s, secondPorts)
		c := second.server.core.cluster
		founderRecordAt(t, c, InitialEpoch, memberIDsOf(s.members[:len(s.members)-1]))
		founderRecordAt(t, c, InitialEpoch+1, memberIDsOf(s.members))
		askedPreviousOwnerFirst(t, c, s.buckets[0], previousOwnerOf(t, s.members, s.buckets[0]))
	})
}

// skipGossipOf skips nodeID's gossip reconciliations, both sides, until the
// test ends: what it adopts, it adopts from a membership change's commit.
func skipGossipOf(t *testing.T, nodeID string) {
	t.Helper()
	skip := func(at, _, _ string) bool { return at == nodeID }
	gossipReconcileHook.Store(&skip)
	t.Cleanup(func() { gossipReconcileHook.Store(nil) })
}

// joinSecondThroughTheCommit is joinSecond with the second newcomer running
// before the join is proposed and its gossip skipped: it adopts the join from
// the commit.
func joinSecondThroughTheCommit(t *testing.T, s joinerOwningUnpulled, secondPorts *testNodePorts) *testNode {
	t.Helper()
	s.tc.nodes = append(s.tc.nodes, s.joinerNode)
	s.tc.addrs = append(s.tc.addrs, s.joiner.self.ID)
	s.tc.fingerprints = append(s.tc.fingerprints, preGeneratedNodeFingerprints(t, []string{s.joinerNode.cfg.Storage.DataDir})...)
	skipGossipOf(t, secondPorts.ringAddr)
	second := startJoinTarget(t, s.tc, secondPorts, s.tc.token)
	t.Cleanup(func() { _ = second.client.Close() })
	for _, node := range s.tc.nodes {
		t.Cleanup(holdStreamPushesOnly(node.server.core.cluster.peerFor(secondPorts.ringNode())))
	}
	proposer := s.tc.nodes[0].server.core.cluster
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := proposer.proposeJoin(ctx, secondPorts.ringNode()); err != nil {
		t.Fatalf("proposing the second join: %v", err)
	}
	adoptedByGossip(t, second.server.core.cluster, proposer.Epoch())
	return second
}

// TestAPeekAtAJoinerPastTheBoundIsForwardedToThePreviousOwner is the §6.4
// join-case probe under O-7 (b): the joiner, which knows its previous owner,
// holds its take-over before the open; a peek there is forwarded to the
// previous owner and answers the element the joiner's first pop takes once
// released. Guard: the provisional record is overwritten, not forgotten.
func TestAPeekAtAJoinerPastTheBoundIsForwardedToThePreviousOwner(t *testing.T) {
	s := setUpJoinerOwningUnpulledWith(t, joinerSetup{prefix: "joinpeek", count: 1, adoptByGossip: true, storeType: BucketTypeFIFO})
	bucket := s.buckets[0]
	held := newWorkerGate(t)
	onBucketHook(t, &takeOverBeforeOpenHook, s.joiner.self.ID, func(name string) {
		if name == bucket {
			held.hold()
		}
	})
	if !s.joiner.wakeTakeOverCapped(testDatabase, bucket) {
		t.Fatalf("setup: %s was refused a place in the take-over queue", bucket)
	}
	held.await(t, "the open of the joiner's take-over", takeOverWait)
	frame := s.tc.frameOn(s.joinerNode, transport.OpFIFOPeek, PeekRequest{Database: testDatabase, Bucket: bucket})
	answered(t, "a peek at the joiner mid take-over", frame)
	var peeked PeekResponse
	if err := bson.Unmarshal(frame.Payload, &peeked); err != nil || !peeked.Found || len(peeked.Items) == 0 {
		t.Fatalf("the peek at the joiner found no element (err %v): %s", err, frame.Payload)
	}
	held.release()
	var popped PopResponse
	if !holdsWithin(takeOverWait, func() bool {
		frame := s.tc.frameOn(s.joinerNode, transport.OpFIFOPop, PopRequest{Database: testDatabase, Bucket: bucket})
		return frame.Opcode != transport.OpError && bson.Unmarshal(frame.Payload, &popped) == nil && popped.Found
	}) {
		t.Fatal("no pop at the joiner succeeded once its take-over was released")
	}
	if len(popped.Items) == 0 || elementID(popped.Items[0]) != elementID(peeked.Items[0]) {
		t.Errorf("the peek at the joiner answered %s, and its first pop took %v", peeked.Items[0], popped.Items)
	}
}

// TestAJoinerRestartedMidJoinKeepsTheFoundingRecord is §6.7's restart seam: a
// newcomer that has written the cluster's record of its provisional epoch over
// its own, and crashed before membership.json moved, starts again on its
// provisional view. Loading the history at start keeps the cluster's record
// of that epoch (it is the current epoch's record), so ringAt(1) is the
// cluster's ring, not the seed view. Guard: the start does not write the seed
// view back over an existing record.
func TestAJoinerRestartedMidJoinKeepsTheFoundingRecord(t *testing.T) {
	tc := newTestCluster(t, 2, 3)
	t.Cleanup(tc.stop)
	ports := reserveNodePorts(t)
	skipGossipOf(t, ports.ringAddr)
	newcomer := startJoinTarget(t, tc, ports, tc.token)
	t.Cleanup(func() { _ = newcomer.client.Close() })
	c := newcomer.server.core.cluster
	if !c.bootstrapped.Load() || c.Epoch() != InitialEpoch {
		t.Fatalf("setup: the newcomer is not provisional at epoch %d (bootstrapped %t, epoch %d)", InitialEpoch, c.bootstrapped.Load(), c.Epoch())
	}
	founders := peersToNodes(tc.nodes[0].server.core.cluster.memberNodes())
	if err := c.overwriteMembership(InitialEpoch, founders); err != nil {
		t.Fatalf("writing the cluster's record of epoch %d: %v", InitialEpoch, err)
	}
	// What a start does with the node store it finds.
	if err := c.loadMembershipHistory(); err != nil {
		t.Fatalf("loading the history as a start does: %v", err)
	}
	founderRecordAt(t, c, InitialEpoch, memberIDsOf(founders))
	placed, recorded := c.ringAt(InitialEpoch)
	if !recorded {
		t.Fatalf("ringAt(%d) is not recorded after the restart", InitialEpoch)
	}
	if nodeListed(placed.Nodes(), c.self.ID) {
		t.Errorf("ringAt(%d) places on the newcomer itself: the seed view came back", InitialEpoch)
	}
}

// TestAJoinerAdoptingFromAnOlderResponderStaysOnTheFallback is §6.7's
// fail-safe: a joiner that adopts its join from an answer that carries no
// record of its provisional epoch (a responder of an older build; here the
// test's adoption, which carries no history) forgets its provisional record,
// as before, and its take-over decides by the fallback, publishing no source.
// Guard: no record, no provisional record kept.
func TestAJoinerAdoptingFromAnOlderResponderStaysOnTheFallback(t *testing.T) {
	s := setUpJoinerOwningUnpulledWith(t, joinerSetup{prefix: "older", count: 1})
	if _, recorded := s.joiner.membersAt(InitialEpoch); recorded {
		t.Errorf("the joiner kept a record of epoch %d with no record carried; its provisional view is never history", InitialEpoch)
	}
	bucket := s.buckets[0]
	fallbacksBefore := s.joiner.takeOverFallbacks.Load()
	if !s.joiner.wakeTakeOverCapped(testDatabase, bucket) {
		t.Fatalf("setup: %s was refused a place in the take-over queue", bucket)
	}
	if !servedWithin(s.joiner, bucket, takeOverWait) {
		t.Fatalf("the joiner's take-over of %s did not land", bucket)
	}
	if n := s.joiner.takeOverFallbacks.Load() - fallbacksBefore; n != 1 {
		t.Errorf("the joiner's take-over ran the fallback %d times; with no previous owner known it decides by the fallback once", n)
	}
}
