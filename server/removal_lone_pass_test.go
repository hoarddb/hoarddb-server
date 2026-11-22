package server

import (
	"sync/atomic"
	"testing"
	"time"
)

// restartedMidPass builds BUG-0506's scenario on top of BUG-0502's: the lone
// survivor of a 2→1 removal, with its asks still held, has the record of its
// placement pass set back to the previous epoch, as a crash in the middle of
// the pass for the removal's epoch leaves it on disk, and is restarted again.
func restartedMidPass(t *testing.T) *loneSurvivor {
	t.Helper()
	s := restartedLoneSurvivor(t)
	c := clusterOf(s.tc, 0)
	if s.tc.nodes[0].server.core.bucketStoreOpen(testDatabase, s.bucket) {
		t.Fatalf("setup: the survivor took %s over while the removed node's asks were held", s.bucket)
	}
	c.passRecordMu.Lock()
	c.placementMu.Lock()
	c.passFinished = c.Epoch() - 1
	c.placementMu.Unlock()
	encoded, err := c.passRecordLocked(c.Epoch() - 1)
	if err != nil {
		c.passRecordMu.Unlock()
		t.Fatalf("setup: encoding the pass record: %v", err)
	}
	batch := c.s.kv.NewBatch()
	if err := batch.Set(placementPrefix(), encoded); err != nil {
		c.passRecordMu.Unlock()
		t.Fatalf("setup: writing the pass record: %v", err)
	}
	if err := batch.Commit(true); err != nil {
		c.passRecordMu.Unlock()
		t.Fatalf("setup: committing the pass record: %v", err)
	}
	batch.Discard()
	c.passRecordMu.Unlock()
	s.tc.restartNode(0)
	restarted := clusterOf(s.tc, 0)
	if restarted.Enabled() || !restarted.aloneAfterARemoval() {
		t.Fatal("setup: the restarted survivor is not alone on its ring after a removal")
	}
	return s
}

// TestALoneSurvivorRestartedDuringItsPlacementPassTakesTheBucketOver is
// BUG-0506: the lone survivor of a 2→1 removal restarts before its placement
// pass for the removal's epoch has finished. Start's single-node return skipped
// the resume of that pass, and the take-over worker waits on the pass every
// round, so the bucket the removed node asks it to take answers "bucket not
// found" for good. Guard: drop the resume from the lone path and the bucket is
// never served.
func TestALoneSurvivorRestartedDuringItsPlacementPassTakesTheBucketOver(t *testing.T) {
	s := restartedMidPass(t)
	s.refuse.Store(false)
	if !holdsWithin(takeOverWait, func() bool { return s.tc.nodes[0].server.core.bucketStoreOpen(testDatabase, s.bucket) }) {
		c := clusterOf(s.tc, 0)
		t.Fatalf("a lone survivor restarted with its placement pass unfinished never took %s over within %s (pass finished: %v, epoch %d)",
			s.bucket, takeOverWait, c.placementPassFinished(), c.Epoch())
	}
	s.requireEveryWrite(t)
}

// TestALoneSurvivorRestartedDuringItsPlacementPassFinishesItAndGoesIdle is
// BUG-0506's other two effects. A node whose pass has not finished refuses
// every membership change (awaitPlacementPass), so the survivor could not even
// be grown out of the state by a join; and its take-over worker, whose round
// answers "one left, soon" while the pass is unfinished, woke every
// takeOverRetryFloor for good. Guard: drop the resume from the lone path and
// the pass never finishes, and the worker runs about five rounds a second.
func TestALoneSurvivorRestartedDuringItsPlacementPassFinishesItAndGoesIdle(t *testing.T) {
	s := restartedMidPass(t)
	s.refuse.Store(false)
	c := clusterOf(s.tc, 0)
	if err := c.awaitPlacementPass(); err != nil {
		if !holdsWithin(takeOverWait, c.placementPassFinished) {
			t.Fatalf("a lone survivor restarted with its placement pass unfinished never finished it within %s, so it refuses every membership change: %v",
				takeOverWait, err)
		}
	}
	if !holdsWithin(takeOverWait, func() bool { return s.tc.nodes[0].server.core.bucketStoreOpen(testDatabase, s.bucket) }) {
		t.Fatalf("setup: the survivor never took %s over", s.bucket)
	}

	// The round count, read from the hook every round consults. With nothing
	// left the worker waits takeOverRetryCeiling between rounds; a wake from a
	// late ask may add one round to a window, never five.
	var rounds atomic.Int64
	self := c.self.ID
	hook := func(nodeID string) bool {
		if nodeID == self {
			rounds.Add(1)
		}
		return false
	}
	takeOverPausedHook.Store(&hook)
	t.Cleanup(func() { takeOverPausedHook.Store(nil) })
	const window = 2 * time.Second
	idle := false
	var last int64
	for attempt := 0; attempt < 5 && !idle; attempt++ {
		before := rounds.Load()
		time.Sleep(window)
		last = rounds.Load() - before
		idle = last <= 1
	}
	if !idle {
		t.Errorf("the take-over worker of the lone survivor, with nothing left to take over, ran %d rounds in %s: it still wakes every %s",
			last, window, takeOverRetryFloor)
	}
}
