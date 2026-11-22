package server

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/transport"
)

// The shared bound (on-demand pull build doc step 3; spec §6.1, R5, F-1,
// F-2, R-1, R-4): one wait per bucket per take-over, created by the first
// waiting read under takeOver.mu and removed when the take-over ends.

// parkTakeOverBeforeRead holds c's worker just before it reads bucket's copy
// in a round, until the test ends, and counts how often it gets there.
func parkTakeOverBeforeRead(t *testing.T, c *Cluster, bucket string) *atomic.Int64 {
	t.Helper()
	release := make(chan struct{})
	var fires atomic.Int64
	hook := func(nodeID, _, held string) {
		if nodeID == c.self.ID && held == bucket {
			fires.Add(1)
			<-release
		}
	}
	takeOverBeforeReadHook.Store(&hook)
	t.Cleanup(func() {
		takeOverBeforeReadHook.Store(nil)
		close(release)
	})
	return &fires
}

// boundOf is c's stored bound for key, if any.
func boundOf(c *Cluster, key placementKey) (waitBound, bool) {
	c.takeOver.mu.Lock()
	defer c.takeOver.mu.Unlock()
	bound, stored := c.takeOver.bounds[key]
	return bound, stored
}

// readsAnsweredBy runs one on-demand read of bucket at c at each offset from
// now, and returns when each was answered.
func readsAnsweredBy(t *testing.T, c *Cluster, bucket string, offsets []time.Duration) []time.Time {
	t.Helper()
	key, epoch := placementOf(testDatabase, bucket), c.Epoch()
	answeredAt := make([]time.Time, len(offsets))
	var wg sync.WaitGroup
	for index, offset := range offsets {
		wg.Add(1)
		go func() {
			defer wg.Done()
			time.Sleep(offset)
			answer := c.onDemandRead(context.Background(), transport.OpGet, key, epoch, testDatabase, bucket)
			answeredAt[index] = time.Now()
			if answer.serve || answer.forward {
				t.Errorf("a read of %s whose take-over is held was answered from a copy (serve %t, forward %t)", bucket, answer.serve, answer.forward)
			}
		}()
	}
	wg.Wait()
	return answeredAt
}

// latestOf is the latest of times.
func latestOf(times []time.Time) time.Time {
	var latest time.Time
	for _, at := range times {
		if at.After(latest) {
			latest = at
		}
	}
	return latest
}

// TestAThousandConcurrentGetsWaitOneBound is R5: a thousand reads of one
// bucket, started over half a second while its take-over is held undecided,
// all answer by the first read's bound, and the worker takes the bucket over
// once. Guard: the shared takeOver.bounds entry (a per-read deadline answers
// the late reads up to half a second later).
func TestAThousandConcurrentGetsWaitOneBound(t *testing.T) {
	s := setUpJoinerBeforeAnyPull(t)
	fires := parkTakeOverBeforeRead(t, s.joiner, s.bucket)
	offsets := make([]time.Duration, 1000)
	for index := range offsets {
		offsets[index] = time.Duration(index) * 500 * time.Microsecond
	}
	first := time.Now()
	latest := latestOf(readsAnsweredBy(t, s.joiner, s.bucket, offsets))
	if late := latest.Sub(first); late > onDemandWait+100*time.Millisecond {
		t.Errorf("the last of 1,000 reads answered %s after the first began; every read shares the first one's bound of %s", late, onDemandWait)
	}
	if n := fires.Load(); n != 1 {
		t.Errorf("the worker reached the bucket's take-over %d times, want once", n)
	}
}

// TestTwoFirstWaitersShareOneBound is R2-1a: the first waiter is parked
// between its read of takeOver.bounds and its store; a second waiter must not
// finish while it is parked (both hold takeOver.mu across the read and the
// store), the store happens once, and both wait until the same instant.
// Guard: the read and the store in one takeOver.mu section.
func TestTwoFirstWaitersShareOneBound(t *testing.T) {
	s := setUpJoinerBeforeAnyPull(t)
	const bucket = "twowaiters"
	key, epoch := placementOf(testDatabase, bucket), s.joiner.Epoch()
	if !s.joiner.readWindowOpen(epoch) {
		t.Fatalf("setup: the joiner's settling window is closed")
	}
	parked, release := make(chan struct{}), make(chan struct{})
	var fires atomic.Int64
	hook := func(_, _, named string) {
		if named == bucket && fires.Add(1) == 1 {
			close(parked)
			<-release
		}
	}
	takeOverBeforeBoundStoreHook.Store(&hook)
	t.Cleanup(func() { takeOverBeforeBoundStoreHook.Store(nil) })

	var firstUntil, secondUntil time.Time
	firstDone, secondDone := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(firstDone)
		firstUntil = s.joiner.readBound(key, epoch)
	}()
	<-parked
	go func() {
		defer close(secondDone)
		secondUntil = s.joiner.readBound(key, epoch)
	}()
	select {
	case <-secondDone:
		t.Errorf("the second waiter finished while the first was parked between its read of the bound and its store")
	case <-time.After(200 * time.Millisecond):
	}
	close(release)
	<-firstDone
	<-secondDone
	if n := fires.Load(); n != 1 {
		t.Errorf("the bound was stored %d times, want once", n)
	}
	if !firstUntil.Equal(secondUntil) {
		t.Errorf("the two first waiters wait until %s and %s, want one instant", firstUntil, secondUntil)
	}
	if _, stored := boundOf(s.joiner, key); !stored {
		t.Errorf("no bound is stored for the bucket")
	}
}

// TestABoundHoldsWhenTheFallbackReruns is F-1: while a member does not
// answer, the fallback records the take-over's status again every round; the
// bound is not part of that status, so a read after several rounds waits until
// the first read's instant. The reads ask readBound directly: from step 4 a
// read is answered at once from the finding that the member did not answer.
// Guard: the bound kept apart from setTakeOverStatus.
func TestABoundHoldsWhenTheFallbackReruns(t *testing.T) {
	s := setUpJoinerBeforeAnyPull(t)
	silent := s.members[1].ID
	var asked atomic.Int64
	hook := func(askerID, nodeID string) error {
		if askerID == s.joiner.self.ID && nodeID == silent {
			asked.Add(1)
			return errSeamFailsHandOff
		}
		return nil
	}
	askHolderHook.Store(&hook)
	t.Cleanup(func() { askHolderHook.Store(nil) })
	key, epoch := placementOf(testDatabase, s.bucket), s.joiner.Epoch()
	first := s.joiner.readBound(key, epoch)
	s.joiner.wakeTakeOverCapped(testDatabase, s.bucket)
	pollUntil(t, "the fallback reruns three rounds", func() bool { return asked.Load() >= 3 })
	if later := s.joiner.readBound(key, epoch); !later.Equal(first) {
		t.Errorf("after %d rounds a read waits until %s, not the first read's %s", asked.Load(), later, first)
	}
}

// TestTheWaitBoundLeavesNoResidue is R2-1b: a read records a bound, the
// take-over lands the copy and ends: no bound is left for the bucket. Guard:
// endTakeOver's delete.
func TestTheWaitBoundLeavesNoResidue(t *testing.T) {
	s := setUpJoinerBeforeAnyPull(t)
	key := placementOf(testDatabase, s.bucket)
	foundHeld(t, "a get at the joiner before its pull", s.tc.frameOn(s.joinerNode, transport.OpGet, FindRequest{Database: testDatabase, Bucket: s.bucket, Key: "held"}))
	pollUntil(t, "the take-over ends", func() bool {
		_, running := s.joiner.takeOverStatusOfPresent(key)
		return !s.joiner.takeOverClaimed(testDatabase, s.bucket) && !running
	})
	if bound, stored := boundOf(s.joiner, key); stored {
		t.Errorf("a bound (until %s, epoch %d) outlives the take-over that ended", bound.until, bound.epoch)
	}
}

// TestASecondTakeOverGetsAFreshBound is F-2 and R-4: take-over 1 of a name
// ends in takeOverRound's copySettled default arm — this node is neither its
// owner nor in its set — which leaves no bound; and a read of a later epoch
// never waits on a bound of an earlier one. Guards: the default arm's delete,
// and the epoch in the bound.
func TestASecondTakeOverGetsAFreshBound(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()
	c := clusterOf(tc, 0)
	bucket := bucketNamed(t, "elsewhere", func(name string) bool {
		owner, err := c.Owner(placementOf(testDatabase, name))
		return err == nil && owner.ID != c.self.ID
	})
	key, epoch := placementOf(testDatabase, bucket), c.Epoch()
	spent := time.Now().Add(onDemandWait)
	c.takeOver.mu.Lock()
	c.takeOver.bounds[key] = waitBound{until: spent, epoch: epoch}
	c.takeOver.mu.Unlock()
	c.wakeTakeOver(testDatabase, bucket)
	if !eventually(func() bool { _, stored := boundOf(c, key); return !stored }) {
		t.Errorf("take-over 1 ended in the default arm and left its bound behind")
	}

	// A bound of epoch 1 that something left behind is not take-over 2's.
	c.takeOver.mu.Lock()
	c.takeOver.bounds[key] = waitBound{until: spent, epoch: epoch}
	c.takeOver.mu.Unlock()
	time.Sleep(10 * time.Millisecond)
	until := c.readBound(key, epoch+1)
	if !until.After(spent) {
		t.Errorf("a read at epoch %d waits until take-over 1's bound %s, want a fresh one", epoch+1, spent)
	}
	if bound, _ := boundOf(c, key); bound.epoch != epoch+1 {
		t.Errorf("the stored bound is of epoch %d, want %d", bound.epoch, epoch+1)
	}
}

// TestTheBoundIsClearedWhenTheWindowCloses is ODP-6: a read records a bound
// in the settling window; once every member has settled, recomputeJoinFreeze
// publishes the closed window and clears every bound. Guard: the clear in
// recomputeJoinFreeze.
func TestTheBoundIsClearedWhenTheWindowCloses(t *testing.T) {
	lowerSweepBounds(t, 10*time.Minute)
	var failing atomic.Bool
	failing.Store(true)
	setAskNodeToMigrateHook(t, func(_, _, _, _ string) error {
		if failing.Load() {
			return errSeamFailsHandOff
		}
		return nil
	})
	s := startSettlingCluster(t)
	s.join(t, 0)
	joiner := s.joiner.server.core.cluster
	epoch := joiner.Epoch()
	if !joiner.readWindowOpen(epoch) {
		t.Fatalf("setup: the joiner's settling window is closed while every hand-off fails")
	}
	key := placementOf(testDatabase, "windowbound")
	joiner.readBound(key, epoch)
	if _, stored := boundOf(joiner, key); !stored {
		t.Fatalf("setup: a read in the window stored no bound")
	}
	failing.Store(false)
	pollUntil(t, "every member settles the join at the joiner", func() bool {
		wakeAll(s.members())
		return !joiner.readWindowOpen(epoch)
	})
	if bound, stored := boundOf(joiner, key); stored {
		t.Errorf("a bound (until %s, epoch %d) outlives the settling window", bound.until, bound.epoch)
	}
}

// takeOverStatusOfPresent is key's take-over status and whether one is
// recorded at all.
func (c *Cluster) takeOverStatusOfPresent(key placementKey) (takeOverStatus, bool) {
	c.takeOver.mu.Lock()
	defer c.takeOver.mu.Unlock()
	status, present := c.takeOver.status[key]
	return status, present
}
