package server

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestADropDuringAPassIsNotWrittenBackByIt: a pass that has already listed the
// bucket as unsettled is still running when the bucket's drop lands. The drop
// takes the name off the sweep's lists; the pass then hands its own, older list
// to setRetrying, and when the give-up bound has passed by then the round ends
// with the dropped name in the residue (BUG-0659). Neither the retry list nor
// the residue may name the bucket once the round ends. Guard: leave setRetrying
// and recordResidue as plain overwrites.
func TestADropDuringAPassIsNotWrittenBackByIt(t *testing.T) {
	s := setUpACopyLeftOutsideTheSet(t, 0)
	defer s.tc.stop()
	owner := s.tc.nodes[s.owner].server.core
	ownerID := owner.cluster.self.ID
	if !eventually(func() bool { return s.askedBy(s.owner) >= 2 && owner.cluster.retryingNames(s.bucket) }) {
		t.Fatal("setup: the owner is not retrying the bucket's hand-off")
	}

	var armed, afterRelease atomic.Bool
	var endedOnce sync.Once
	atPassEnd, release, ended := make(chan struct{}), make(chan struct{}), make(chan struct{})
	hook := func(nodeID string, _ uint64, stage sweepStage) {
		if nodeID != ownerID {
			return
		}
		switch {
		case stage == passFinished && armed.CompareAndSwap(true, false):
			close(atPassEnd)
			<-release
		case stage == sweepEnded && afterRelease.Load():
			endedOnce.Do(func() { close(ended) })
		}
	}
	sweepStageHook.Store(&hook)
	t.Cleanup(func() { sweepStageHook.Store(nil) })
	armed.Store(true)
	select {
	case <-atPassEnd:
	case <-time.After(30 * time.Second):
		t.Fatal("no pass of the owner's sweep finished")
	}

	dropAtOne(s.tc, s.owner, testDatabase, s.bucket)
	if !eventually(func() bool { return !owner.cluster.retryingNames(s.bucket) }) {
		t.Fatal("the drop never took the name off the owner's retry list")
	}
	// The give-up bound has passed by the time the held pass returns, so the
	// round ends on its first look instead of retrying a dropped name.
	sweepRetryGiveUp.set(time.Nanosecond)
	afterRelease.Store(true)
	close(release)
	select {
	case <-ended:
	case <-time.After(30 * time.Second):
		t.Fatal("the owner's round never ended")
	}
	if owner.cluster.retryingNames(s.bucket) {
		t.Error("the pass that was in flight when the drop landed wrote the dropped name back to the retry list")
	}
	if owner.cluster.residueNames(s.bucket) {
		t.Error("the round gave up and recorded the dropped name as residue")
	}
}
