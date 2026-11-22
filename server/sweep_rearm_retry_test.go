package server

import (
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/transport"
)

// BUG-0196: a rearm that takes a newer sweep generation while a full sweep is
// still retrying makes that sweep stop at its next attempt, and the buckets it
// was retrying are recorded nowhere (only the newest generation records its
// residue), so a holder the change moved out of a bucket's set keeps its copy.

// retryingNow reports whether c's newest full sweep is in its retry loop with
// bucket on the list it retries.
func (c *Cluster) retryingNow(database, bucket string) bool {
	c.sweepLeftMu.Lock()
	defer c.sweepLeftMu.Unlock()
	return c.retrying.generation == c.fullSweepGeneration.Load() &&
		slices.Contains(c.retrying.list, heldBucket{database: database, bucket: bucket})
}

// orphanAt records bucket as an orphan create of epoch on c: the one thing
// besides a residue that gives a rearm something to ask about.
func (c *Cluster) orphanAt(database, bucket string, epoch uint64) {
	c.sweepLeftMu.Lock()
	defer c.sweepLeftMu.Unlock()
	if c.orphans == nil {
		c.orphans = make(map[heldBucket]uint64)
	}
	c.orphans[heldBucket{database: database, bucket: bucket}] = epoch
}

// TestRearmDuringARetryDoesNotDropTheRetryingBuckets is the GAP-23 verifier's
// N-5 probe: the join's sweep on a holder the join moved out of a bucket's set
// retries that bucket's hand-off; a rearm with something of its own to ask
// about arrives during the retry; then the hand-off is let through. The
// holder must still release its copy, every member of the new set hold one,
// and the holder settle at the join. Guard: let a rearm start while a full
// sweep that has not recorded its residue runs.
func TestRearmDuringARetryDoesNotDropTheRetryingBuckets(t *testing.T) {
	lowerSweepBounds(t, time.Minute)
	s := startSettlingCluster(t)
	defer s.tc.stop()
	const holderIndex = 0
	holder := clusterOf(s.tc, holderIndex)
	joined := s.joinedRing(t)
	leaving := bucketNamed(t, "leave", func(name string) bool {
		set, err := joined.Replicas(placementOf(testDatabase, name).encoded, 3)
		return err == nil && containsID(nodeIDs(set), s.ports.ringAddr) && !containsID(nodeIDs(set), s.tc.addrs[holderIndex])
	})
	s.tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: leaving, Type: BucketTypeHash})
	s.tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: leaving, Key: "doc", Data: map[string]interface{}{"n": 1}})
	waitForServing(t, s.tc, testDatabase, leaving, 3)
	// The orphan the rearm asks about: a bucket the holder keeps, so its
	// hand-off succeeds at once.
	orphan := s.keptBucket(t, holderIndex, "orphan")

	var failing atomic.Bool
	failing.Store(true)
	failHandOffs(t, holder.self.ID, &failing, leaving)
	joinEpoch := s.join(t, 1)
	pollUntil(t, "the holder's join sweep retries the hand-off of "+leaving, func() bool {
		return holder.retryingNow(testDatabase, leaving)
	})

	holder.orphanAt(testDatabase, orphan, joinEpoch)
	holder.rearmSweep()
	failing.Store(false)

	deadline := time.Now().Add(30 * time.Second)
	for holder.s.bucketStoreOpen(testDatabase, leaving) {
		if time.Now().After(deadline) {
			t.Fatalf("node %s still holds a copy of %s that is no longer placed on it (%d rearms started; residue %+v)",
				holder.self.ID, leaving, holder.sweepRearms.Load(), holder.residueSnapshot())
		}
		holder.wakeGossip()
		time.Sleep(20 * time.Millisecond)
	}
	set, err := holder.Replicas(placementOf(testDatabase, leaving))
	if err != nil {
		t.Fatalf("placing %s after the join: %v", leaving, err)
	}
	for _, member := range s.members() {
		if containsID(nodeIDs(set), member.self.ID) && !member.s.bucketStoreOpen(testDatabase, leaving) {
			t.Errorf("node %s, in %s's set, holds no copy after the holder released its own", member.self.ID, leaving)
		}
	}
	waitSettled(t, holder, joinEpoch)
}

// TestARearmBetweenAFullSweepsNumberAndItsClaimDoesNotDropItsBuckets is
// BUG-0196's residual: a full sweep took its generation and had not yet
// published it as the newest full sweep when a rearm, with something of its
// own to ask about, checked its guard. The rearm saw no full sweep running,
// took a newer generation, and the full sweep stopped at its first retry with
// its pending buckets recorded nowhere. Guard: take the generation and publish
// it as two steps outside sweepLeftMu.
func TestARearmBetweenAFullSweepsNumberAndItsClaimDoesNotDropItsBuckets(t *testing.T) {
	lowerSweepBounds(t, time.Minute)
	s := startSettlingCluster(t)
	defer s.tc.stop()
	const holderIndex = 0
	holder := clusterOf(s.tc, holderIndex)
	joined := s.joinedRing(t)
	leaving := bucketNamed(t, "leave", func(name string) bool {
		set, err := joined.Replicas(placementOf(testDatabase, name).encoded, 3)
		return err == nil && containsID(nodeIDs(set), s.ports.ringAddr) && !containsID(nodeIDs(set), s.tc.addrs[holderIndex])
	})
	s.tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: leaving, Type: BucketTypeHash})
	s.tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: leaving, Key: "doc", Data: map[string]interface{}{"n": 1}})
	waitForServing(t, s.tc, testDatabase, leaving, 3)
	orphan := s.keptBucket(t, holderIndex, "orphan")

	// The holder's hand-offs of the moved bucket fail until its join sweep's
	// first pass has asked once, so that sweep has it to retry.
	var failing atomic.Bool
	var refused atomic.Int64
	failing.Store(true)
	setAskNodeToMigrateHook(t, func(from, _, _, bucket string) error {
		if from == holder.self.ID && bucket == leaving && failing.Load() {
			refused.Add(1)
			return errSeamFailsHandOff
		}
		return nil
	})
	joinEpoch := holder.Epoch() + 1
	var rearmedInWindow atomic.Bool
	hook := func(nodeID string, epoch uint64, stage sweepStage) {
		if nodeID != holder.self.ID || epoch != joinEpoch || stage != sweepNumbered || !rearmedInWindow.CompareAndSwap(false, true) {
			return
		}
		holder.orphanAt(testDatabase, orphan, joinEpoch)
		holder.rearmSweep()
	}
	sweepStageHook.Store(&hook)
	t.Cleanup(func() { sweepStageHook.Store(nil) })

	s.join(t, 1)
	pollUntil(t, "the join sweep's first pass asks the set about "+leaving, func() bool { return refused.Load() > 0 })
	if !rearmedInWindow.Load() {
		t.Fatal("test setup: the holder's join sweep never reached sweepNumbered")
	}
	failing.Store(false)

	deadline := time.Now().Add(30 * time.Second)
	for holder.s.bucketStoreOpen(testDatabase, leaving) {
		if time.Now().After(deadline) {
			t.Fatalf("node %s still holds a copy of %s that is no longer placed on it (%d rearms started; residue %+v)",
				holder.self.ID, leaving, holder.sweepRearms.Load(), holder.residueSnapshot())
		}
		holder.wakeGossip()
		time.Sleep(20 * time.Millisecond)
	}
	waitSettled(t, holder, joinEpoch)
}
