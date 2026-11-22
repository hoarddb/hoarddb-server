package server

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Dispatch 9, step 9.4: one view of the membership, and one epoch at a time
// (T-118).

// historyEpochs reports which epochs a node's membership history holds.
func historyEpochs(c *Cluster) map[uint64]bool {
	c.historyMu.Lock()
	defer c.historyMu.Unlock()
	held := make(map[uint64]bool, len(c.history))
	for epoch := range c.history {
		held[epoch] = true
	}
	return held
}

// TestANodeAdoptsEveryEpochItMissedInOrder is T-118. A node stopped through a
// join and the removal of the node that joined comes back: its history holds
// both epochs, and its marks are those of adopting them in turn — the buckets
// that moved to the joiner and back — although the ring it ends on is the one
// it stopped on. A node stopped through more changes than its peer remembers
// adopts none of them and says what to do. Guard: adopt the peer's epoch
// directly — the node then has no record of the epoch in between, and marks
// nothing.
func TestANodeAdoptsEveryEpochItMissedInOrder(t *testing.T) {
	// The marks are read as the passes recorded them, before the take-over
	// worker settles the copies and clears them (step 10b.5).
	takeOverOffHook.Store(true)
	defer takeOverOffHook.Store(false)
	t.Run("a join and a removal", func(t *testing.T) {
		missed := stopThroughAJoinAndARemoval(t)
		returned := missed.start().server.core.cluster
		missed.assertAdoptedInTurn(t, returned)
	})

	t.Run("more changes than the peer remembers", func(t *testing.T) {
		defer gossipHistoryLimit.Store(gossipHistoryLimit.Swap(2))
		tc := newTestClusterWith(t, 4, 2, func(_ int, cfg *config.Config) { cfg.Server.AutoRebalance = false })
		defer tc.stop()
		const stoppedIndex = 3
		start := stopNode(t, tc, stoppedIndex)
		for changes := 0; changes < 3; changes++ {
			joiner := joinNewcomer(t, tc, 0, reserveNodePorts(t))
			for index := 0; index < stoppedIndex; index++ {
				waitForPlacementPassOf(t, clusterOf(tc, index))
			}
			waitForPlacementPassOf(t, joiner.cluster())
		}
		clusterEpoch := clusterOf(tc, 0).Epoch()

		logs := recordClusterLogs(t)
		returned := start().server.core.cluster
		pollUntil(t, "the returned node says it missed more changes than the cluster remembers", func() bool {
			return len(logs.matching(slog.LevelError, "missed more membership changes than the cluster remembers")) > 0
		})
		if epoch := returned.Epoch(); epoch != InitialEpoch {
			t.Errorf("the returned node is at epoch %d; it missed %d changes, its peer remembers %d, and it must adopt none of them",
				epoch, clusterEpoch-InitialEpoch, gossipHistoryLimit.Load())
		}
		if !returned.mismatch.Load() {
			t.Error("the returned node accepts writes on a membership it knows to be stale")
		}
	})
}

// heldBy returns the names of the buckets a node holds.
func heldBy(c *Cluster, names []string) []string {
	var held []string
	for _, name := range names {
		if c.s.bucketStoreOpen(testDatabase, name) {
			held = append(held, name)
		}
	}
	return held
}

// missedJoinAndRemoval is a four-node cluster at factor 2 whose fourth node
// was stopped through a join and the removal of the node that joined.
type missedJoinAndRemoval struct {
	tc *testCluster
	// held is the buckets the stopped node holds; start starts it again.
	held  []string
	start func() *testNode
	// joinerAddr is the node that joined and was removed; finalEpoch is the
	// cluster's epoch after the removal, and the join's is the one before.
	joinerAddr string
	finalEpoch uint64
}

// stopThroughAJoinAndARemoval builds T-118's cluster: 200 buckets, the fourth
// node stopped, a fifth node joined and then removed.
func stopThroughAJoinAndARemoval(t *testing.T) missedJoinAndRemoval {
	t.Helper()
	tc := newTestClusterWith(t, 4, 2, func(_ int, cfg *config.Config) { cfg.Server.AutoRebalance = false })
	const stoppedIndex = 3
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	var names []string
	for i := 0; i < 200; i++ {
		name := fmt.Sprintf("missed%03d", i)
		names = append(names, name)
		tc.ok(i%3, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: name, Type: BucketTypeHash})
	}
	stopped := clusterOf(tc, stoppedIndex)
	pollUntil(t, "every copy of every bucket is in place", func() bool {
		copies := 0
		for index := range tc.nodes {
			copies += len(heldBy(clusterOf(tc, index), names))
		}
		return copies == 2*len(names)
	})
	held := heldBy(stopped, names)
	start := stopNode(t, tc, stoppedIndex)

	ports := reserveNodePorts(t)
	joiner := joinNewcomer(t, tc, 0, ports)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for index := 0; index < stoppedIndex; index++ {
		waitForPlacementPassOf(t, clusterOf(tc, index))
	}
	waitForPlacementPassOf(t, joiner.cluster())
	if err := clusterOf(tc, 0).proposeRemoval(ctx, joiner.cluster().self.ID); err != nil {
		t.Fatalf("removing the joiner: %v", err)
	}
	return missedJoinAndRemoval{tc: tc, held: held, start: start, joinerAddr: ports.ringAddr, finalEpoch: clusterOf(tc, 0).Epoch()}
}

// assertAdoptedInTurn checks the returned node against T-118: it reaches the
// cluster's epoch, its history holds every epoch it missed, and its marks are
// those of adopting the join and then the removal.
func (missed missedJoinAndRemoval) assertAdoptedInTurn(t *testing.T, returned *Cluster) {
	t.Helper()
	pollUntil(t, "the returned node reaches the cluster's epoch", func() bool { return returned.Epoch() == missed.finalEpoch })
	waitForPlacementPassOf(t, returned)
	epochs := historyEpochs(returned)
	for epoch := InitialEpoch + 1; epoch <= missed.finalEpoch; epoch++ {
		if !epochs[epoch] {
			t.Errorf("the returned node's history lacks epoch %d (it holds %v): it did not adopt each epoch it missed", epoch, epochs)
		}
	}
	withJoiner := factorTwoRingOf(t, append(append([]string(nil), missed.tc.addrs...), missed.joinerAddr))
	original := factorTwoRingOf(t, missed.tc.addrs)
	want := expectedPlacementMarks(t, missed.held, returned.self.ID, original, withJoiner, missed.finalEpoch-1)
	for key, mark := range expectedPlacementMarks(t, missed.held, returned.self.ID, withJoiner, original, missed.finalEpoch) {
		want[key] = want[key].merged(mark)
	}
	got := recordedPlacementMarks(t, returned.s.kv)
	if len(want) == 0 {
		t.Fatal("test setup: neither change moved a bucket the stopped node holds")
	}
	mismatched := 0
	for key, mark := range want {
		if got[key] != mark {
			mismatched++
		}
	}
	if mismatched > 0 || len(got) != len(want) {
		t.Errorf("the returned node holds %d buckets; adopting the join and the removal in turn marks %d of them, and it recorded %d marks, %d of them not as expected",
			len(missed.held), len(want), len(got), mismatched)
	}
}

// TestAGossipRequestDuringACatchUpWaitsForTheEpochBeingAdopted: A
// node that missed two epochs learns both from a peer's answer and adopts the
// first. A gossip request from another member, carrying the second, arrives
// after the node's view has moved to the first and before that epoch's
// placement pass has started: it finds no pass to wait for, adopts the second
// epoch at once, and that pass marks the buckets moved at the second epoch.
// The first epoch's pass comes later and keeps those marks, as a mark is never
// overwritten. The node is held at that point (viewAdoptedHook) so the request
// lands there every time. Guard: adopt without waiting for the catch-up in
// progress.
func TestAGossipRequestDuringACatchUpWaitsForTheEpochBeingAdopted(t *testing.T) {
	// The marks are read as the passes recorded them, before the take-over
	// worker settles the copies and clears them (step 10b.5).
	takeOverOffHook.Store(true)
	defer takeOverOffHook.Store(false)
	missed := stopThroughAJoinAndARemoval(t)
	stoppedID := missed.tc.addrs[3]
	joinEpoch := missed.finalEpoch - 1

	inBetween := make(chan struct{})
	release := make(chan struct{})
	var reached, released sync.Once
	hold := func(nodeID string, epoch uint64) {
		if nodeID != stoppedID || epoch != joinEpoch {
			return
		}
		reached.Do(func() { close(inBetween) })
		<-release
	}
	viewAdoptedHook.Store(&hold)
	// Registered after the cluster's stop, so it runs first: a node held in
	// the hook would keep Stop waiting for ever.
	t.Cleanup(func() {
		released.Do(func() { close(release) })
		viewAdoptedHook.Store(nil)
	})

	returned := missed.start().server.core.cluster
	select {
	case <-inBetween:
	case <-time.After(30 * time.Second):
		t.Fatal("test setup: the returned node never adopted the join's epoch")
	}
	// What a member's gossip request carries: its epoch and members, no history.
	member := clusterOf(missed.tc, 0)
	asked := make(chan error, 1)
	go func() {
		asked <- returned.reconcileMembership(member.self.ID, member.memberNodes(), member.memberIDs(), member.Epoch())
	}()
	// Without the wait the request adopts the removal's epoch, and its pass
	// finishes, while the join's view is held. With it, the request is still
	// waiting when this stops looking.
	answered := false
	var requestErr error
	select {
	case requestErr = <-asked:
		answered = true
		pollUntil(t, "the request's adoption finishes its placement pass", returned.placementPassFinished)
	case <-time.After(2 * time.Second):
	}
	released.Do(func() { close(release) })
	if !answered {
		requestErr = <-asked
	}
	if requestErr != nil {
		t.Fatalf("the member's gossip request: %v", requestErr)
	}
	missed.assertAdoptedInTurn(t, returned)
}

// TestConcurrentCatchUpsAdoptEachMissedEpochOnceAndNeverGoBack is the other half of the
// catch-up ordering check. Two catch-ups that both read the node at the epoch it stopped on
// run one after the other; the second must start from where the first left
// the node, not from the epoch it read. The first adoption is held until the
// second catch-up has read the node's epoch, so the two always overlap.
// Guard: start the second catch-up from the epoch it read — it adopts the
// join's epoch again after the removal's, moving the node's view back.
func TestConcurrentCatchUpsAdoptEachMissedEpochOnceAndNeverGoBack(t *testing.T) {
	// The marks are read as the passes recorded them, before the take-over
	// worker settles the copies and clears them (step 10b.5).
	takeOverOffHook.Store(true)
	defer takeOverOffHook.Store(false)
	missed := stopThroughAJoinAndARemoval(t)
	stoppedID := missed.tc.addrs[3]

	firstHeld := make(chan struct{})
	release := make(chan struct{})
	var held, released sync.Once
	var adoptionsMu sync.Mutex
	var adoptions []uint64
	count := func(nodeID string, epoch uint64) error {
		if nodeID != stoppedID {
			return nil
		}
		adoptionsMu.Lock()
		adoptions = append(adoptions, epoch)
		adoptionsMu.Unlock()
		first := false
		held.Do(func() { first = true; close(firstHeld) })
		if first {
			<-release
		}
		return nil
	}
	adoptMembershipHook.Store(&count)
	// Registered after the cluster's stop, so it runs first: a node held in
	// the hook would keep Stop waiting for ever.
	t.Cleanup(func() {
		released.Do(func() { close(release) })
		adoptMembershipHook.Store(nil)
	})

	returned := missed.start().server.core.cluster
	select {
	case <-firstHeld:
	case <-time.After(30 * time.Second):
		t.Fatal("test setup: the returned node never began catching up")
	}
	// A second catch-up from a member's answer, while the first is held
	// before its first adoption: it reads the epoch the node stopped on.
	member := clusterOf(missed.tc, 0)
	answer := peerGossip{records: member.historyAbove(InitialEpoch), answered: true}
	caughtUp := make(chan error, 1)
	go func() {
		caughtUp <- returned.reconcileMembershipWith(member.self.ID, member.memberNodes(), member.memberIDs(), member.Epoch(), answer)
	}()
	time.Sleep(time.Second)
	released.Do(func() { close(release) })
	if err := <-caughtUp; err != nil {
		t.Fatalf("the second catch-up: %v", err)
	}
	pollUntil(t, "the returned node reaches the cluster's epoch", func() bool { return returned.Epoch() == missed.finalEpoch })
	waitForPlacementPassOf(t, returned)

	adoptionsMu.Lock()
	recorded := append([]uint64(nil), adoptions...)
	adoptionsMu.Unlock()
	perEpoch := make(map[uint64]int)
	for index, epoch := range recorded {
		perEpoch[epoch]++
		if index > 0 && epoch <= recorded[index-1] {
			t.Errorf("the returned node adopted epoch %d after epoch %d (adoptions in order: %v): its view moved back", epoch, recorded[index-1], recorded)
		}
	}
	for epoch := InitialEpoch + 1; epoch <= missed.finalEpoch; epoch++ {
		if perEpoch[epoch] != 1 {
			t.Errorf("the returned node adopted epoch %d %d times, want once (per epoch: %v)", epoch, perEpoch[epoch], perEpoch)
		}
	}
	missed.assertAdoptedInTurn(t, returned)
}
