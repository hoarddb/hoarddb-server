package server

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The membership change wired across real nodes: T1 to T4 of the
// one-set-per-epoch spec, the opposite case of T1, and the proposer's and
// acceptor's rules against real acceptors.

// ringNodeAt is a member whose identity is its address.
func ringNodeAt(addr string) ring.Node {
	return ring.Node{ID: addr, Addr: addr}
}

// heldSendDeadline bounds how long the schedule holds a proposer's message: it
// then counts as unreachable, so a held send can never hang the test.
const heldSendDeadline = 2 * time.Second

// lostProposerRun is the schedule of a proposer lost mid-change: node 3
// proposes removing node 5 and only node 4 answers it; node 1 then removes
// node 2 without hearing from nodes 3 and 4. It records the member set every
// node adopts at the epoch after the first.
type lostProposerRun struct {
	tc                     *testCluster
	n1, n2, n3, n4, n5     string
	skipStrayGossip        atomic.Bool
	holdFirstPrepares      atomic.Bool
	adoptedMu              sync.Mutex
	adoptedAtNext          map[string][][]string
	lostCommandErr         error
	committedWithoutRemove []string
}

// newLostProposerRun installs the seams of the schedule on a running cluster
// of five; the seams are removed when the test ends.
func newLostProposerRun(t *testing.T, tc *testCluster) *lostProposerRun {
	t.Helper()
	run := &lostProposerRun{
		tc: tc, n1: tc.addrs[0], n2: tc.addrs[1], n3: tc.addrs[2], n4: tc.addrs[3], n5: tc.addrs[4],
		adoptedAtNext: make(map[string][][]string),
	}
	run.skipStrayGossip.Store(true)
	run.holdFirstPrepares.Store(true)
	// No recovery on node 3 or node 4 may race node 1's change: the schedule
	// is about what node 1's change does with their acceptances.
	recoveryAfter := membershipRecoveryAfter.get()
	membershipRecoveryAfter.set(time.Hour)
	t.Cleanup(func() { membershipRecoveryAfter.set(recoveryAfter) })
	clusters := make(map[string]*Cluster, len(tc.nodes))
	for index := range tc.nodes {
		clusters[tc.addrs[index]] = clusterOf(tc, index)
	}
	recordAdoption := func(nodeID string, epoch uint64) {
		if epoch != InitialEpoch+1 {
			return
		}
		_, members := clusters[nodeID].epochMembers()
		ids := memberIDsOf(members)
		slices.Sort(ids)
		run.adoptedMu.Lock()
		run.adoptedAtNext[nodeID] = append(run.adoptedAtNext[nodeID], ids)
		run.adoptedMu.Unlock()
	}
	viewAdoptedHook.Store(&recordAdoption)
	// Nodes 1, 2 and 5 refuse node 3's change at the phase that would commit
	// it; node 4 answers it.
	refuseLostChange := func(nodeID, phase, fromNode string, _ uint64) error {
		if fromNode != run.n3 || phase != phaseAccept {
			return nil
		}
		if nodeID == run.n1 || nodeID == run.n2 || nodeID == run.n5 {
			return fmt.Errorf("test seam: %s refuses %s's change at %s", nodeID, fromNode, phase)
		}
		return nil
	}
	membershipPhaseHook.Store(&refuseLostChange)
	// Node 1 never reaches nodes 3 and 4 while it prepares: its majority is
	// nodes 1, 2 and 5, none of which holds node 3's change.
	holdPrepare := func(ctx context.Context, fromNode, toNode, phase string) error {
		if !run.holdFirstPrepares.Load() || fromNode != run.n1 || phase != phasePrepare || (toNode != run.n3 && toNode != run.n4) {
			return nil
		}
		select {
		case <-ctx.Done():
		case <-time.After(heldSendDeadline):
		}
		return fmt.Errorf("test seam: %s's prepare to %s is held", fromNode, toNode)
	}
	membershipSendHook.Store(&holdPrepare)
	// No gossip touches node 4 while the schedule runs, so its view moves only
	// by the membership change itself.
	skipStray := func(nodeID, peerID, _ string) bool {
		return run.skipStrayGossip.Load() && (nodeID == run.n4 || peerID == run.n4)
	}
	gossipReconcileHook.Store(&skipStray)
	t.Cleanup(func() {
		viewAdoptedHook.Store(nil)
		membershipPhaseHook.Store(nil)
		membershipSendHook.Store(nil)
		gossipReconcileHook.Store(nil)
	})
	for _, addr := range tc.addrs {
		if addr != run.n2 {
			run.committedWithoutRemove = append(run.committedWithoutRemove, addr)
		}
	}
	slices.Sort(run.committedWithoutRemove)
	return run
}

// runLostChange runs node 3's change, which three of the five members refuse
// at the phase that would commit it.
func (run *lostProposerRun) runLostChange(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	run.lostCommandErr = clusterOf(run.tc, 2).proposeRemoval(ctx, run.n5)
	if !errors.Is(run.lostCommandErr, ErrMembershipChangeIndeterminate) {
		t.Fatalf("test setup: node 3's removal of node 5, refused at accept by three of five members, answered %v; want %v",
			run.lostCommandErr, ErrMembershipChangeIndeterminate)
	}
}

// runCommittedChange runs node 1's removal of node 2, which must commit.
func (run *lostProposerRun) runCommittedChange(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := clusterOf(run.tc, 0).proposeRemoval(ctx, run.n2); err != nil {
		t.Fatalf("node 1's removal of node 2, with nodes 1, 2 and 5 answering: %v", err)
	}
	run.holdFirstPrepares.Store(false)
}

// createBuckets creates the test database and the named buckets, and waits
// until each is served by its replica set.
func createBuckets(t *testing.T, tc *testCluster, names []string) {
	t.Helper()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	for _, name := range names {
		tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: name, Type: BucketTypeHash})
	}
	for _, name := range names {
		waitForServing(t, tc, testDatabase, name, tc.rf)
	}
}

// waitStrayOpened gives node 4 time to open what it would after node 1's
// change, and returns what it opened that the committed ring gives another
// owner.
func (run *lostProposerRun) waitStrayOpened(t *testing.T, names []string) []string {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	opened := run.strayOpenedTerms(t, names)
	for time.Now().Before(deadline) && len(opened) < len(names) {
		time.Sleep(100 * time.Millisecond)
		opened = run.strayOpenedTerms(t, names)
	}
	return opened
}

// assertOneSetAtNextEpoch fails the test for any node that adopted, at the
// epoch after the first, a set other than the one node 1 committed.
func (run *lostProposerRun) assertOneSetAtNextEpoch(t *testing.T) {
	t.Helper()
	run.adoptedMu.Lock()
	defer run.adoptedMu.Unlock()
	for nodeID, adoptions := range run.adoptedAtNext {
		for _, adopted := range adoptions {
			if !slices.Equal(adopted, run.committedWithoutRemove) {
				t.Errorf("node %s adopted membership epoch %d with %v; the set committed at that epoch is %v",
					nodeID, InitialEpoch+1, adopted, run.committedWithoutRemove)
			}
		}
	}
}

// strayOpenedTerms lists the buckets node 4 holds open at the epoch after the
// first that the committed ring gives another owner.
func (run *lostProposerRun) strayOpenedTerms(t *testing.T, names []string) []string {
	t.Helper()
	committed := ringOfAddrs(t, run.tc.rf, run.committedWithoutRemove)
	stray := run.tc.nodes[3].server.core
	var opened []string
	for _, name := range names {
		state, err := stray.bucketStateNamed(testDatabase, name)
		if err != nil || !state.Opened || state.Term != InitialEpoch+1 {
			continue
		}
		owner, err := committed.Owner(placementOf(testDatabase, name).encoded)
		if err != nil {
			t.Fatalf("placing %s on the committed ring: %v", name, err)
		}
		if owner.ID != run.n4 {
			opened = append(opened, fmt.Sprintf("%s (owner %s)", name, owner.ID))
		}
	}
	return opened
}

// ringOfAddrs builds the placement ring of a member set given by address.
func ringOfAddrs(t *testing.T, rf int, addrs []string) *ring.Ring {
	t.Helper()
	placement := ring.New(rf)
	for _, addr := range addrs {
		if err := placement.AddNode(ring.Node{ID: addr, Addr: addr}); err != nil {
			t.Fatalf("building a ring of %v: %v", addrs, err)
		}
	}
	return placement
}

// addrsWithout is addrs less the ones named, in order.
func addrsWithout(addrs []string, without ...string) []string {
	var kept []string
	for _, addr := range addrs {
		if !slices.Contains(without, addr) {
			kept = append(kept, addr)
		}
	}
	return kept
}

// TestALostProposerLeavesNoNodeOnItsSet: five nodes; node 3 proposes removing
// node 5, and three of the five members refuse it at accept, node 4 accepting
// it. Node 1 then removes node 2 with nodes 2 and 5. Every node that adopts
// the next epoch adopts node 1's set, and node 4 opens no bucket at that epoch
// that node 1's set gives another owner. Red where a member's answer to a
// proposal adopts the proposed set: node 4 adopted node 3's. Guard: the accept
// handler adopts.
func TestALostProposerLeavesNoNodeOnItsSet(t *testing.T) {
	tc := newTestCluster(t, 5, 3)
	defer tc.stop()
	var names []string
	for index := 0; index < 12; index++ {
		names = append(names, fmt.Sprintf("lost%02d", index))
	}
	createBuckets(t, tc, names)
	run := newLostProposerRun(t, tc)
	run.runLostChange(t)
	run.runCommittedChange(t)
	opened := run.waitStrayOpened(t, names)
	run.assertOneSetAtNextEpoch(t)
	if len(opened) > 0 {
		t.Errorf("node 4 opened at membership epoch %d buckets the committed set gives another owner: %v",
			InitialEpoch+1, opened)
	}
}

// TestAStrayTermNeverSupersedesAnAcknowledgedWrite: the schedule of
// TestALostProposerLeavesNoNodeOnItsSet, on buckets node 5 owns before and
// after node 1's change, node 4 owns on node 3's set, and node 5 does not own
// once a fabricated node joins. Node 5 acknowledges writes at majority and at
// one; the join, then the fabricated node's removal, give the buckets back to
// node 5, whose take-over must keep every write it acknowledged. A regression
// test, with no red requirement (owner ruling D7): on the tree before the fix
// the stray opened these buckets only through a create racing its adoption,
// and the deterministic failing-first tests are
// TestALostProposerLeavesNoNodeOnItsSet and TestGossipNeverSpreadsAnAcceptedSet.
func TestAStrayTermNeverSupersedesAnAcknowledgedWrite(t *testing.T) {
	var loss strayLoss
	tc := newTestClusterPlaced(t, 5, 3, func(addrs []string) bool {
		var found bool
		loss, found = strayLossPlacement(t, addrs, 3, func() string { return fabricatedMemberAddr(t) })
		return found
	})
	defer tc.stop()
	names, fabricated := loss.names, loss.fabricated
	createBuckets(t, tc, names)
	run := newLostProposerRun(t, tc)
	run.runLostChange(t)
	run.runCommittedChange(t)
	t.Logf("node 4 holds open at membership epoch %d: %v", InitialEpoch+1, run.waitStrayOpened(t, names))

	acknowledged := make(map[string][]string)
	for _, name := range names {
		for _, concern := range []string{"majority", "one"} {
			key := "owner-" + concern
			if ackedAtOwner(t, tc, 4, name, key, concern) {
				acknowledged[name] = append(acknowledged[name], key)
			}
		}
	}
	if len(acknowledged) == 0 {
		t.Fatalf("test setup: node 5 acknowledged no write on %v", names)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	proposer := clusterOf(tc, 0)
	if err := proposer.proposeJoin(ctx, ring.Node{ID: fabricated, Addr: fabricated}); err != nil {
		t.Fatalf("joining the fabricated node %s: %v", fabricated, err)
	}
	run.skipStrayGossip.Store(false)
	tc.bringMembershipTo(t, proposer)
	var removeErr error
	for attempt := 0; attempt < 50; attempt++ {
		if removeErr = proposer.proposeRemoval(ctx, fabricated); removeErr == nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if removeErr != nil {
		t.Fatalf("removing the fabricated node %s: %v", fabricated, removeErr)
	}
	tc.bringMembershipTo(t, proposer)

	for _, name := range names {
		waitTakeOverSettled(t, tc.nodes[4].server.core, testDatabase, name)
		for _, key := range acknowledged[name] {
			frame := tc.frame(4, transport.OpGet, FindRequest{Database: testDatabase, Bucket: name, Key: key})
			if frame.Opcode == transport.OpError {
				t.Errorf("%s/%s, acknowledged by node 5 as owner, is gone after node 5 took the bucket back: %s",
					name, key, string(frame.Payload))
			}
		}
	}
}

// strayOwnedNames chooses up to three bucket names that node 4 owns on the
// ring without node 5 and does not own on the ring without node 2. also, when
// set, is a further condition.
func strayOwnedNames(t *testing.T, addrs []string, rf int, also func(name string) bool) []string {
	t.Helper()
	n2, n4, n5 := addrs[1], addrs[3], addrs[4]
	committedRing := ringOfAddrs(t, rf, addrsWithout(addrs, n2))
	strayRing := ringOfAddrs(t, rf, addrsWithout(addrs, n5))
	var names []string
	for index := 0; index < 20000 && len(names) < 3; index++ {
		name := fmt.Sprintf("stray%05d", index)
		if ownerOnRing(t, strayRing, name) == n4 && ownerOnRing(t, committedRing, name) != n4 && (also == nil || also(name)) {
			names = append(names, name)
		}
	}
	return names
}

// ownerOnRing is the owner of bucket name of the test database on placement.
func ownerOnRing(t *testing.T, placement *ring.Ring, name string) string {
	t.Helper()
	owner, err := placement.Owner(placementOf(testDatabase, name).encoded)
	if err != nil {
		t.Fatalf("placing %s: %v", name, err)
	}
	return owner.ID
}

// strayLoss is the placement TestAStrayTermNeverSupersedesAnAcknowledgedWrite
// needs: its bucket names and the fabricated node whose join takes them from
// node 5.
type strayLoss struct {
	names      []string
	fabricated string
}

// strayLossPlacement chooses the names of strayOwnedNames that node 5 owns on
// the first ring of addrs and on the ring without node 2, and another node
// owns once a fabricated node joins; it returns that fabricated node too.
// Which names qualify depends on the addresses, so it tries several
// fabricated nodes, drawn from nextFabricated; found is false when none
// qualifies.
func strayLossPlacement(t *testing.T, addrs []string, rf int, nextFabricated func() string) (loss strayLoss, found bool) {
	t.Helper()
	n2, n5 := addrs[1], addrs[4]
	firstRing := ringOfAddrs(t, rf, addrs)
	committedRing := ringOfAddrs(t, rf, addrsWithout(addrs, n2))
	for attempt := 0; attempt < 128; attempt++ {
		fabricated := nextFabricated()
		joinedRing := ringOfAddrs(t, rf, append(addrsWithout(addrs, n2), fabricated))
		names := strayOwnedNames(t, addrs, rf, func(name string) bool {
			return ownerOnRing(t, firstRing, name) == n5 && ownerOnRing(t, committedRing, name) == n5 &&
				ownerOnRing(t, joinedRing, name) != n5
		})
		if len(names) > 0 {
			return strayLoss{names: names, fabricated: fabricated}, true
		}
	}
	return strayLoss{}, false
}

// waitTakeOverSettled waits until s, the bucket's owner, has finished its
// take-over after the last membership change, however it finished: by a copy,
// by a term of its own, or by resuming the term it held, which nobody opened
// since. Its placement pass for the epoch is done, so the marks are the
// recorded ones, and no mark or take-over is left.
func waitTakeOverSettled(t *testing.T, s *Server, database, bucket string) {
	t.Helper()
	key := placementOf(database, bucket)
	if !holdsWithin(takeOverWait, func() bool {
		_, opened, err := s.bucketLineage(database, bucket)
		return err == nil && opened && s.cluster.placementPassFinished() && s.bucketStoreOpen(database, bucket) &&
			s.cluster.placementMarksOf(key).Moved == 0 && !s.cluster.takeOverStatusOf(key).recorded()
	}) {
		term, opened, err := s.bucketLineage(database, bucket)
		t.Fatalf("%s never finished taking %s/%s over at membership epoch %d: term %d, opened %v, marks %+v, error %v",
			s.cluster.self.ID, database, bucket, s.cluster.Epoch(), term, opened, s.cluster.placementMarksOf(key), err)
	}
}

// ackedAtOwner writes key into bucket name through node index at the write
// concern given, retrying while the owner refuses, and reports whether the
// write was acknowledged.
func ackedAtOwner(t *testing.T, tc *testCluster, index int, name, key, concern string) bool {
	t.Helper()
	for attempt := 0; attempt < 100; attempt++ {
		frame := tc.frame(index, transport.OpPut, InsertRequest{
			Database: testDatabase, Bucket: name, Key: key,
			Data: map[string]interface{}{"written_by": "owner"}, WriteConcern: concern,
		})
		if frame.Opcode != transport.OpError {
			return true
		}
		if attempt == 99 {
			t.Logf("node %d refused %s/%s at %s: %s", index+1, name, key, concern, strings.TrimSpace(string(frame.Payload)))
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}
