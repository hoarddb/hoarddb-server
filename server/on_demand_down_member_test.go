package server

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/transport"
)

// A member this node records as down (peer.downSince) is still asked the copy
// question, with catalogQuestionWait for its answer rather than the write
// concern's timeout (owner ruling R2, 2026-10-20; on-demand pull spec §6.3). A
// peer's liveness is learned on the gossip lane and can be stale while the
// member answers on the request lane: skipping it changed outcomes
// (TestPullerTriesTheNextMemberAfterARefusal). A member that is really gone
// costs the short wait.

// countAsksOf counts the fallback's questions from asker to nodeID.
func countAsksOf(t *testing.T, asker *Cluster, nodeID string) *atomic.Int64 {
	t.Helper()
	var asked atomic.Int64
	hook := func(askerID, askedID string) error {
		if askerID == asker.self.ID && askedID == nodeID {
			asked.Add(1)
		}
		return nil
	}
	askHolderHook.Store(&hook)
	t.Cleanup(func() { askHolderHook.Store(nil) })
	return &asked
}

// keepRecordedDown records p as unreachable every millisecond until the
// returned function is called, so a gossip round that reaches the live member
// cannot clear the record while the test reads.
func keepRecordedDown(p *peer) (stop func()) {
	p.recordUnreachable(time.Now())
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				p.recordUnreachable(time.Now())
			}
		}
	}()
	var once sync.Once
	return func() { once.Do(func() { close(done); wg.Wait() }) }
}

// TestAMemberRecordedDownThatAnswersStillLandsThePull: a live member the
// joiner records as down answers the take-over's question, so the read lands
// the copy and finds the document, as when the member is not recorded down.
// Guard: asking a member recorded down (a member skipped is unheard, and the
// read answers retry).
func TestAMemberRecordedDownThatAnswersStillLandsThePull(t *testing.T) {
	s := setUpJoinerBeforeAnyPull(t)
	recordedDown := s.members[1]
	asked := countAsksOf(t, s.joiner, recordedDown.ID)
	stop := keepRecordedDown(s.joiner.peerFor(recordedDown))
	t.Cleanup(stop)
	if _, down := s.joiner.peerFor(recordedDown).downSince(); !down {
		t.Fatalf("test setup: the joiner does not record %s as down", recordedDown.ID)
	}
	frame := s.tc.frameOn(s.joinerNode, transport.OpGet, FindRequest{Database: testDatabase, Bucket: s.bucket, Key: "held"})
	stop()
	foundHeld(t, "a get at the joiner while it records a live member as down", frame)
	if asked.Load() == 0 {
		t.Errorf("the fallback did not ask %s, which it records as down", recordedDown.ID)
	}
}

// TestASilentMemberRecordedDownCostsAtMostTheShortWait: a member recorded down
// whose question is never answered is asked once, counts as unheard (wrapping
// errPeerDown), and holds the fallback's question no longer than
// catalogQuestionWait, where an unrecorded member is given the write
// concern's timeout. Guard: the short wait for a member recorded down.
func TestASilentMemberRecordedDownCostsAtMostTheShortWait(t *testing.T) {
	s := setUpJoinerBeforeAnyPull(t)
	silent := s.members[1]
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	p := s.joiner.peerFor(silent)
	wrapPeerConn(p, func(inner peerConn) peerConn { return &heldPushConn{peerConn: inner, release: release} })
	asked := countAsksOf(t, s.joiner, silent.ID)
	if s.joiner.wcTimeout <= catalogQuestionWait+time.Second {
		t.Fatalf("test setup: the write concern's timeout %s is too close to catalogQuestionWait %s to tell them apart",
			s.joiner.wcTimeout, catalogQuestionWait)
	}
	stop := keepRecordedDown(p)
	t.Cleanup(stop)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	began := time.Now()
	nodes := s.joiner.askHolders(ctx, []ReplBucketRef{{Database: testDatabase, Bucket: s.bucket}})
	took := time.Since(began)
	stop()
	t.Logf("the fallback's question took %s with a silent member recorded down (catalogQuestionWait %s, write concern %s)",
		took, catalogQuestionWait, s.joiner.wcTimeout)
	if n := asked.Load(); n != 1 {
		t.Errorf("the fallback asked the silent member recorded down %d times, want once", n)
	}
	if limit := catalogQuestionWait + 750*time.Millisecond; took > limit {
		t.Errorf("the fallback's question took %s, want at most %s for a member recorded down", took, limit)
	}
	var heard bool
	for _, node := range nodes {
		if node.node.ID != silent.ID {
			continue
		}
		heard = true
		if !errors.Is(node.err, errPeerDown) {
			t.Errorf("the silent member recorded down was recorded with %v, want errPeerDown", node.err)
		}
	}
	if !heard {
		t.Errorf("the fallback has no record of the silent member %s", silent.ID)
	}
}
