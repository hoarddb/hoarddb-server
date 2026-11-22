package server

import (
	"fmt"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/transport"
)

// stopMidProbe is the moment BUG-0692 needs, made certain: the survivor's stop
// cancels its context while a probe of the removed node is in flight. The
// probe is started after the cancel, so its question fails on the cancelled
// context exactly as an in-flight one does when Stop lands during its send;
// the restart that follows waits for it (storeWG) before the store closes.
func stopMidProbe(t *testing.T) func(survivor *Cluster, removedID string) {
	return func(survivor *Cluster, removedID string) {
		t.Helper()
		removed, known := survivor.removedNode(removedID)
		if !known {
			t.Fatalf("setup: %s is not a removed node in the survivor's history", removedID)
		}
		if survivor.settled.Load() >= survivor.Epoch() {
			t.Fatalf("setup: the survivor settled epoch %d before its stop, with the removed node's hand-off held", survivor.Epoch())
		}
		survivor.cancel()
		survivor.probeRemoved(removed)
	}
}

// TestAStopDuringARemovedNodeProbeDoesNotSettleTheRemoval is BUG-0692's cause:
// a probe of a removed node that fails because this node is stopping is not
// the removed node's silence. Guard: probeRemoved takes any failed probe for
// "gone" (silent > 3 s, owner ruling R7), so the stopping survivor raises and
// persists its Settled epoch with the hand-off still held; after the restart
// removedSince starts at that epoch, nothing is awaited, and the settling
// window is closed for good.
func TestAStopDuringARemovedNodeProbeDoesNotSettleTheRemoval(t *testing.T) {
	s := restartedLoneSurvivorWith(t, loneSurvivorSetup{beforeRestart: stopMidProbe(t)})
	restarted := clusterOf(s.tc, 0)
	epoch := restarted.Epoch()
	if settled := restarted.settled.Load(); settled >= epoch {
		t.Fatalf("the survivor, stopped while it probed the removed node whose hand-off is held, came back settled at epoch %d (its epoch is %d): "+
			"it took its own stop for the removed node's silence", settled, epoch)
	}
	if !restarted.readWindowOpen(epoch) {
		t.Errorf("after the restart the settling window of epoch %d is closed while the removed node's hand-off is held", epoch)
	}
}

// TestALoneSurvivorStoppedMidProbeStillAwaitsTheRemovedNodesBucket is
// BUG-0692's data-safety half. The survivor was stopped mid-probe and
// restarted; its take-over worker is busy with the first bucket the removed
// node holds (held before its open), as it is while it copies many. A get of a
// second bucket whose only copy (an acknowledged write) is on the removed
// node, which runs and whose hand-off is held, is never "not found" inside the
// window: it is a retry naming the removed node (owner rulings R7, R8). So is
// a get of a name the removed node owned that exists nowhere. Once the hand-off
// goes through, the survivor serves both buckets with every write. Guard: the
// false settle closes the window; the get waits unheldDataOpWait for a worker
// that is busy elsewhere, and answerOnDemand's after-window branch then
// answers both gets "not found ... create it" for a bucket that exists.
func TestALoneSurvivorStoppedMidProbeStillAwaitsTheRemovedNodesBucket(t *testing.T) {
	s := restartedLoneSurvivorWith(t, loneSurvivorSetup{second: true, beforeRestart: stopMidProbe(t)})
	survivorID, removedID := s.tc.addrs[0], s.tc.addrs[1]
	busy := holdTakeOverOf(t, survivorID, s.bucket)
	s.tc.frame(0, transport.OpGet, FindRequest{Database: testDatabase, Bucket: s.bucket, Key: "k00"})
	busy.waitReached(t, "the survivor's take-over of the first bucket")

	survivor := clusterOf(s.tc, 0)
	nowhere := ""
	for i := 0; nowhere == ""; i++ {
		if name := fmt.Sprintf("never-made-%d", i); survivor.ownedByARemovedNode(placementOf(testDatabase, name)) {
			nowhere = name
		}
		if i > 512 {
			t.Fatal("setup: no name in 512 tries was owned by the removed node")
		}
	}
	for _, read := range []struct {
		what   string
		bucket string
		key    string
	}{
		{"a get of a bucket only the removed node holds", s.second, "s0"},
		{"a get of a name the removed node owned that exists nowhere", nowhere, "k"},
	} {
		frame := s.tc.frame(0, transport.OpGet, FindRequest{Database: testDatabase, Bucket: read.bucket, Key: read.key})
		text := string(frame.Payload)
		if frame.Opcode != transport.OpError || strings.Contains(text, "not found") || !strings.Contains(text, removedID) {
			t.Errorf("%s, on the survivor restarted after a stop mid-probe, while the removed node's hand-off is held, was answered %s: %q; "+
				"want a retry naming %s", read.what, frame.Opcode, text, removedID)
		}
	}

	busy.release()
	s.refuse.Store(false)
	for _, landed := range []struct{ bucket, key string }{{s.bucket, "k00"}, {s.second, "s0"}} {
		if !holdsWithin(takeOverWait, func() bool { return holdsKeyIn(s.tc.nodes[0], testDatabase, landed.bucket, landed.key) }) {
			t.Errorf("the survivor never took %s over with its acknowledged write %s once the hand-off went through", landed.bucket, landed.key)
		}
	}
	s.requireEveryWrite(t)
}
