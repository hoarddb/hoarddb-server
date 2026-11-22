package server

import (
	"context"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/ring"
)

// BUG-0511: a node restarted with its placement pass unfinished adopted a later
// epoch from gossip before Start resumed the pass, and a pass for an older
// epoch that finished last set the finished epoch back.

// rewindPassRecord sets node index's placement pass record back to the epoch
// before its current one, as a crash in the middle of the pass leaves it on
// disk (BUG-0506's setup).
func rewindPassRecord(t *testing.T, tc *testCluster, index int) {
	t.Helper()
	c := clusterOf(tc, index)
	if !holdsWithin(10*time.Second, c.placementPassFinished) {
		t.Fatal("setup: the pass of the join never finished")
	}
	c.passRecordMu.Lock()
	defer c.passRecordMu.Unlock()
	c.placementMu.Lock()
	c.passFinished = c.Epoch() - 1
	c.placementMu.Unlock()
	encoded, err := c.passRecordLocked(c.Epoch() - 1)
	if err != nil {
		t.Fatalf("setup: encoding the pass record: %v", err)
	}
	batch := c.s.kv.NewBatch()
	defer batch.Discard()
	if err := batch.Set(placementPrefix(), encoded); err != nil {
		t.Fatalf("setup: writing the pass record: %v", err)
	}
	if err := batch.Commit(true); err != nil {
		t.Fatalf("setup: committing the pass record: %v", err)
	}
}

// joinedFourth joins a fabricated member from node 2 and brings every node to
// the join's epoch. It returns the members before the join.
func joinedFourth(t *testing.T, tc *testCluster) []ring.Node {
	t.Helper()
	proposer := clusterOf(tc, 1)
	before := proposer.Members()
	newcomer := ring.Node{ID: fabricatedMemberAddr(t)}
	newcomer.Addr = newcomer.ID
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := proposer.proposeJoin(ctx, newcomer); err != nil {
		t.Fatalf("setup: join the newcomer: %v", err)
	}
	for i := range tc.nodes {
		if i == 1 {
			continue
		}
		if err := clusterOf(tc, i).reconcileMembership(tc.addrs[1], proposer.memberNodes(), proposer.memberIDs(), proposer.Epoch()); err != nil {
			t.Fatalf("setup: member %d resync: %v", i+1, err)
		}
	}
	return before
}

func peersOf(members []ring.Node) []Peer {
	peers := make([]Peer, 0, len(members))
	for _, m := range members {
		peers = append(peers, Peer{ID: m.ID, Addr: m.Addr})
	}
	return peers
}

// recordedPassFinished is the finished epoch of the pass record on disk.
func recordedPassFinished(t *testing.T, c *Cluster) uint64 {
	t.Helper()
	snapshot := c.s.kv.NewSnapshot()
	defer snapshot.Discard()
	stored, err := snapshot.Get(placementPrefix())
	if err != nil {
		t.Fatalf("reading the pass record: %v", err)
	}
	var record placementPassRecord
	if err := bson.Unmarshal(stored, &record); err != nil {
		t.Fatalf("decoding the pass record: %v", err)
	}
	return record.Finished
}

// TestAGossipAdoptionBeforeStartWaitsForTheUnfinishedPlacementPass: node 1
// restarts with its placement pass for epoch E unfinished on disk. A gossip
// request from a member at E+1 is delivered before Start (in the tune hook,
// which runs between NewService and Service.Start, as the internode listener
// would). The node must not adopt E+1 before the pass of E has finished
// (step 9.4); once Start has resumed and finished that pass, the same request
// takes it to E+1, and the pass of E+1 finishes. Guard: drop the
// placementPassFinished check from adoptInOrder and the node adopts E+1 in the
// hook with passFinished at E−1.
func TestAGossipAdoptionBeforeStartWaitsForTheUnfinishedPlacementPass(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	before := joinedFourth(t, tc)
	epoch := clusterOf(tc, 0).Epoch()
	rewindPassRecord(t, tc, 0)

	ahead := GossipRequest{NodeID: tc.addrs[1], Addr: tc.addrs[1], Epoch: epoch + 1, Nodes: peersOf(before)}
	var adoptedEpoch, finishedAt uint64
	var unfinishedBefore bool
	tc.tune = func(index int, c *Cluster) {
		if index != 0 {
			return
		}
		unfinishedBefore = !c.placementPassFinished()
		if err := c.reconcileRequest(ahead); err != nil {
			t.Logf("reconcileRequest: %v", err)
		}
		adoptedEpoch = c.Epoch()
		c.placementMu.Lock()
		finishedAt = c.passFinished
		c.placementMu.Unlock()
	}
	tc.restartNode(0)
	tc.tune = nil
	if !unfinishedBefore {
		t.Fatal("setup: the restarted node's pass for the join's epoch was already finished")
	}
	t.Logf("before Start: epoch %d -> %d by gossip, pass finished at %d", epoch, adoptedEpoch, finishedAt)
	if adoptedEpoch > epoch {
		t.Fatalf("a gossip adoption delivered before Start took the node from epoch %d to %d while the placement pass of %d had not finished (passFinished %d)",
			epoch, adoptedEpoch, epoch, finishedAt)
	}

	c := clusterOf(tc, 0)
	if !holdsWithin(10*time.Second, c.placementPassFinished) {
		t.Fatalf("Start never finished the resumed placement pass of epoch %d", epoch)
	}
	if err := c.reconcileRequest(ahead); err != nil {
		t.Fatalf("the same gossip after the pass finished: %v", err)
	}
	if got := c.Epoch(); got != epoch+1 {
		t.Fatalf("after its pass of %d finished the node is at epoch %d, not %d", epoch, got, epoch+1)
	}
	if !holdsWithin(10*time.Second, c.placementPassFinished) {
		t.Fatalf("the placement pass of epoch %d never finished", epoch+1)
	}
	c.placementMu.Lock()
	finished := c.passFinished
	c.placementMu.Unlock()
	if finished != epoch+1 {
		t.Errorf("passFinished is %d after the adoption of %d", finished, epoch+1)
	}
}

// TestAnOlderPlacementPassFinishingLastDoesNotSetTheFinishedEpochBack: the
// sink of BUG-0511's outcome 2. Once two passes run at once — Start's resume of
// E and an adoption's pass of E+1 — and E's finishes last, the finished epoch
// fell below the epoch in memory and on disk, and nothing in the process ran
// the pass again: membership changes were refused and the take-over worker
// waited for good. The late pass is run by a direct call here. Guard: set
// passFinished and the record to the pass's own epoch in placementPass and
// both fall back to E.
func TestAnOlderPlacementPassFinishingLastDoesNotSetTheFinishedEpochBack(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	joinedFourth(t, tc)
	c := clusterOf(tc, 0)
	epoch := c.Epoch()
	if !holdsWithin(10*time.Second, c.placementPassFinished) {
		t.Fatal("setup: the pass of the join never finished")
	}
	if err := c.placementPass(epoch - 1); err != nil {
		t.Fatalf("the late pass of epoch %d: %v", epoch-1, err)
	}
	c.placementMu.Lock()
	finished := c.passFinished
	c.placementMu.Unlock()
	if finished < epoch {
		err := c.awaitPlacementPass()
		rounds, waiting := c.takeOverRound(context.Background())
		t.Errorf("an older pass that finished last set passFinished to %d, below epoch %d: awaitPlacementPass=%v, takeOverRound=(%d, %v)",
			finished, epoch, err, rounds, waiting)
	}
	if recorded := recordedPassFinished(t, c); recorded < epoch {
		t.Errorf("an older pass that finished last recorded finished epoch %d on disk, below epoch %d", recorded, epoch)
	}
}
