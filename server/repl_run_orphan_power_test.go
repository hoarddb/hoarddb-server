package server

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage/kvfault"
	"github.com/hoarddb/hoarddb-server/transport"
)

// A run stopped by a store failure leaves its later entries logged and
// unapplied, and the owner's re-send of them is logged a second time. These
// two tests drive a replica on the fault store by hand: one frame of two FIFO
// pushes whose first apply fails, then re-sends, then a crash. Neither the
// restart's position nor its replay may treat the first copies as applied or
// owed.

// queuePush is a push of id into app/queue at the sender's sequence seq.
func queuePush(t *testing.T, seq uint64, id string) ReplEntry {
	t.Helper()
	payload, err := bson.Marshal(PushRequest{Database: "app", Bucket: "queue", Data: map[string]interface{}{"_id_": id}})
	if err != nil {
		t.Fatalf("encoding the push of %q: %v", id, err)
	}
	return ReplEntry{Seq: seq, Op: int32(transport.OpFIFOPush), Bucket: "queue", Payload: payload}
}

// appQueueOrder is app/queue's elements in pop order.
func appQueueOrder(t *testing.T, s *Server) []string {
	t.Helper()
	resp, err := s.Peek(PeekRequest{Database: "app", Bucket: "queue", Limit: maxPeekLimit})
	if err != nil {
		t.Fatalf("peeking app/queue: %v", err)
	}
	order := make([]string, 0, len(resp.Items))
	for _, element := range resp.Items {
		id, ok := element.Lookup("_id_").StringValueOK()
		if !ok {
			t.Fatalf("an element of app/queue has no _id_: %v", element)
		}
		order = append(order, id)
	}
	return order
}

// stopARunAtItsFirstEntry sends one frame of two pushes whose first apply the
// store fails, so the second is logged and never applied.
func stopARunAtItsFirstEntry(t *testing.T, c *Cluster) {
	t.Helper()
	failed := failNextApplyOn(t, c.s.kv, anySeq)
	loggedBefore := c.s.wal.LastSequence()
	resp := pushToReplica(t, c, frameSyncSource, []ReplEntry{queuePush(t, 1, "a"), queuePush(t, 2, "b")})
	if resp.Success || resp.AppliedThrough != 0 || failed.Load() == 0 {
		t.Fatalf("setup: the frame was answered success=%t applied_through=%d (store failures %d); want the first push failed",
			resp.Success, resp.AppliedThrough, failed.Load())
	}
	if logged := c.s.wal.LastSequence() - loggedBefore; logged != 2 {
		t.Fatalf("setup: the frame put %d entries in the replica's log, want both, as one run", logged)
	}
}

// TestAPowerCutAfterARunsOrphanWasReSentAppliesItOnce: the owner re-sends
// both pushes and they apply, logged a second time. The power then takes the
// store's unsynced applies, and replay must apply each push once: the first
// copy of the second push was never applied, and is not the one to replay.
func TestAPowerCutAfterARunsOrphanWasReSentAppliesItOnce(t *testing.T) {
	s, kv, c, dataDir := newReplicaOnFaultStore(t)
	stopARunAtItsFirstEntry(t, c)
	if resp := pushToReplica(t, c, frameSyncSource, []ReplEntry{queuePush(t, 1, "a"), queuePush(t, 2, "b")}); !resp.Success || resp.AppliedThrough != 2 {
		t.Fatalf("the re-send was answered success=%t applied_through=%d error=%q", resp.Success, resp.AppliedThrough, resp.Error)
	}
	if order := appQueueOrder(t, s); !slices.Equal(order, []string{"a", "b"}) {
		t.Fatalf("setup: after the re-send the queue is %v", order)
	}

	restarted := killedAfterPowerLoss(t, dataDir, kv, s)
	if err := restarted.s.replayWAL(context.Background()); err != nil {
		t.Fatalf("replaying the log: %v", err)
	}
	if order := appQueueOrder(t, restarted.s); !slices.Equal(order, []string{"a", "b"}) {
		t.Fatalf("after a power cut and the replay the queue is %v, want [a b]: a push was applied from both of its copies", order)
	}
	if got := appliedSnapshot(restarted)[frameSyncSource]; got != 2 {
		t.Fatalf("after the replay the position for the sender is %d, want 2", got)
	}
}

// TestACrashAfterAPartialReSendKeepsTheRestOwed: the owner re-sends only the
// first push, which applies, so the queue's mark passes the first copy of the
// second. The node then dies. Restarted, it must still owe the second push:
// replay passes over its first copy, below the mark, so a position rebuilt
// from it would say applied and the owner would never send it again.
func TestACrashAfterAPartialReSendKeepsTheRestOwed(t *testing.T) {
	s, kv, c, dataDir := newReplicaOnFaultStore(t)
	stopARunAtItsFirstEntry(t, c)
	if resp := pushToReplica(t, c, frameSyncSource, []ReplEntry{queuePush(t, 1, "a")}); !resp.Success || resp.AppliedThrough != 1 {
		t.Fatalf("the re-send of the first push was answered success=%t applied_through=%d error=%q", resp.Success, resp.AppliedThrough, resp.Error)
	}
	if err := kv.Sync(); err != nil {
		t.Fatalf("syncing the store: %v", err)
	}

	restarted := killedAfterPowerLoss(t, dataDir, kv, s)
	if err := restarted.s.replayWAL(context.Background()); err != nil {
		t.Fatalf("replaying the log: %v", err)
	}
	if !restarted.shouldApply(frameSyncSource, 2) {
		t.Fatalf("restarted, the replica counts the second push applied (position %d) and holds %v: the owner would never send it again",
			appliedSnapshot(restarted)[frameSyncSource], appQueueOrder(t, restarted.s))
	}
	if resp := pushToReplica(t, restarted, frameSyncSource, []ReplEntry{queuePush(t, 2, "b")}); !resp.Success || resp.AppliedThrough != 2 {
		t.Fatalf("the re-send of the second push was answered success=%t applied_through=%d error=%q", resp.Success, resp.AppliedThrough, resp.Error)
	}
	if order := appQueueOrder(t, restarted.s); !slices.Equal(order, []string{"a", "b"}) {
		t.Fatalf("after the restart and the re-send the queue is %v, want [a b]", order)
	}
}

// TestAVoidWhoseFirstSequenceReadFailsIsStillRecorded is BUG-0569: the stopped
// run's void reads the log's first sequence, and here that read fails, the
// log's oldest segment being unreadable for the length of the stop. The void
// must still be made and recorded before the owner's re-send is logged: a void
// forgotten there lets the re-send in beside the first copies, and a power cut
// then replays both.
func TestAVoidWhoseFirstSequenceReadFailsIsStillRecorded(t *testing.T) {
	s, kv, c, dataDir := newReplicaOnFaultStore(t)
	withTheLogsFirstSequenceUnreadableInTheVoid(t, s, dataDir, func() { stopARunAtItsFirstEntry(t, c) })

	if resp := pushToReplica(t, c, frameSyncSource, []ReplEntry{queuePush(t, 1, "a"), queuePush(t, 2, "b")}); !resp.Success || resp.AppliedThrough != 2 {
		t.Fatalf("the re-send was answered success=%t applied_through=%d error=%q", resp.Success, resp.AppliedThrough, resp.Error)
	}
	restarted := killedAfterPowerLoss(t, dataDir, kv, s)
	if err := restarted.s.replayWAL(context.Background()); err != nil {
		t.Fatalf("replaying the log: %v", err)
	}
	if order := appQueueOrder(t, restarted.s); !slices.Equal(order, []string{"a", "b"}) {
		t.Fatalf("after a power cut and the replay the queue is %v, want [a b]: a void its failed read skipped let the re-send in beside the first copies", order)
	}
}

// TestAWidenedVoidWhoseFirstSequenceReadFailsIsStillRecorded is BUG-0569 for
// a source that already has a void: the first sequence decides whether the
// new void widens the earlier one or replaces it, and here the read fails at
// the second stop. The second void must still be recorded, widened, before
// the re-send of the second run is logged.
func TestAWidenedVoidWhoseFirstSequenceReadFailsIsStillRecorded(t *testing.T) {
	s, kv, c, dataDir := newReplicaOnFaultStore(t)
	stopARunAtItsFirstEntry(t, c)
	if resp := pushToReplica(t, c, frameSyncSource, []ReplEntry{queuePush(t, 1, "a"), queuePush(t, 2, "b")}); !resp.Success || resp.AppliedThrough != 2 {
		t.Fatalf("the re-send of the first run was answered success=%t applied_through=%d error=%q", resp.Success, resp.AppliedThrough, resp.Error)
	}

	withTheLogsFirstSequenceUnreadableInTheVoid(t, s, dataDir, func() {
		failed := failNextApplyOn(t, c.s.kv, anySeq)
		resp := pushToReplica(t, c, frameSyncSource, []ReplEntry{queuePush(t, 3, "c"), queuePush(t, 4, "d")})
		if resp.Success || resp.AppliedThrough != 0 || failed.Load() == 0 {
			t.Fatalf("setup: the second run was answered success=%t applied_through=%d (store failures %d); want its first push failed",
				resp.Success, resp.AppliedThrough, failed.Load())
		}
	})
	if resp := pushToReplica(t, c, frameSyncSource, []ReplEntry{queuePush(t, 3, "c"), queuePush(t, 4, "d")}); !resp.Success || resp.AppliedThrough != 4 {
		t.Fatalf("the re-send of the second run was answered success=%t applied_through=%d error=%q", resp.Success, resp.AppliedThrough, resp.Error)
	}

	restarted := killedAfterPowerLoss(t, dataDir, kv, s)
	if err := restarted.s.replayWAL(context.Background()); err != nil {
		t.Fatalf("replaying the log: %v", err)
	}
	if order := appQueueOrder(t, restarted.s); !slices.Equal(order, []string{"a", "b", "c", "d"}) {
		t.Fatalf("after a power cut and the replay the queue is %v, want [a b c d]", order)
	}
	if got := appliedSnapshot(restarted)[frameSyncSource]; got != 4 {
		t.Fatalf("after the replay the position for the sender is %d, want 4", got)
	}
}

// TestAFailedLastEntryReSentAndAppliedThenAPowerCutAppliesItOnce is BUG-0570
// for a run: a push of one entry whose apply the store fails stops the run on
// its last entry, with nothing logged after it. The owner re-sends it, it
// applies and is logged a second time, and the power goes. The failed copy
// was never applied and must not be replayed beside the re-send.
func TestAFailedLastEntryReSentAndAppliedThenAPowerCutAppliesItOnce(t *testing.T) {
	s, kv, c, dataDir := newReplicaOnFaultStore(t)
	failed := failNextApplyOn(t, c.s.kv, anySeq)
	if resp := pushToReplica(t, c, frameSyncSource, []ReplEntry{queuePush(t, 1, "a")}); resp.Success || failed.Load() == 0 {
		t.Fatalf("setup: the push was answered success=%t (store failures %d); want it failed", resp.Success, failed.Load())
	}
	if resp := pushToReplica(t, c, frameSyncSource, []ReplEntry{queuePush(t, 1, "a")}); !resp.Success || resp.AppliedThrough != 1 {
		t.Fatalf("the re-send was answered success=%t applied_through=%d error=%q", resp.Success, resp.AppliedThrough, resp.Error)
	}
	requireOnePushAfterAPowerCut(t, dataDir, kv, s)
}

// TestAFailedEntryAppliedOnItsOwnReSentThenAPowerCutAppliesItOnce is BUG-0570
// for an entry that does not join a run and is logged and applied on its own
// (applyAsReplica, as handleReplPush calls it under the sender's stream lock).
func TestAFailedEntryAppliedOnItsOwnReSentThenAPowerCutAppliesItOnce(t *testing.T) {
	s, kv, c, dataDir := newReplicaOnFaultStore(t)
	entry := queuePush(t, 1, "a")
	term, _, err := c.s.bucketLineage("app", "queue")
	if err != nil {
		t.Fatalf("reading app/queue's lineage: %v", err)
	}
	entry.Term = term
	streamOf(t, c, frameSyncSource)
	failed := failNextApplyOn(t, c.s.kv, anySeq)
	unlock := c.lockStreamFrom(frameSyncSource)
	_, applyErr := c.applyAsReplica(context.Background(), entry, true, frameSyncSource, true)
	unlock()
	if applyErr == nil || failed.Load() == 0 {
		t.Fatalf("setup: the entry applied on its own was answered %v (store failures %d); want it failed", applyErr, failed.Load())
	}
	if resp := pushToReplica(t, c, frameSyncSource, []ReplEntry{queuePush(t, 1, "a")}); !resp.Success || resp.AppliedThrough != 1 {
		t.Fatalf("the re-send was answered success=%t applied_through=%d error=%q", resp.Success, resp.AppliedThrough, resp.Error)
	}
	requireOnePushAfterAPowerCut(t, dataDir, kv, s)
}

// requireOnePushAfterAPowerCut cuts the power under s, replays its log, and
// requires app/queue to hold the one push "a" and the sender's position to
// stand at it.
func requireOnePushAfterAPowerCut(t *testing.T, dataDir string, kv *kvfault.Store, s *Server) {
	t.Helper()
	restarted := killedAfterPowerLoss(t, dataDir, kv, s)
	if err := restarted.s.replayWAL(context.Background()); err != nil {
		t.Fatalf("replaying the log: %v", err)
	}
	if order := appQueueOrder(t, restarted.s); !slices.Equal(order, []string{"a"}) {
		t.Fatalf("after a power cut and the replay the queue is %v, want [a]: the copy whose apply failed was replayed beside the re-send", order)
	}
	if got := appliedSnapshot(restarted)[frameSyncSource]; got != 1 {
		t.Fatalf("after the replay the position for the sender is %d, want 1", got)
	}
}

// withTheLogsFirstSequenceUnreadableInTheVoid runs stop, which must stop a run
// and void what it logged, with the log's first sequence unreadable from the
// moment the void begins until stop returns. The unreadable segment is one
// older than every real one (the log starts at index 1) whose first entry has
// an impossible length: FirstSequence reads the oldest segment first and fails
// there. A file mode would not do, as root reads through it.
func withTheLogsFirstSequenceUnreadableInTheVoid(t *testing.T, s *Server, dataDir string, stop func()) {
	t.Helper()
	unreadable := filepath.Join(dataDir, "wal", "wal_00000.log")
	var readFailed atomic.Bool
	voidHook := func(*Cluster, string) error {
		if err := os.WriteFile(unreadable, make([]byte, 16), 0o600); err != nil {
			return fmt.Errorf("planting an unreadable oldest segment: %w", err)
		}
		_, err := s.wal.FirstSequence()
		readFailed.Store(err != nil)
		return nil
	}
	orphanVoidFaultHook.Store(&voidHook)
	t.Cleanup(func() { orphanVoidFaultHook.Store(nil) })
	stop()
	orphanVoidFaultHook.Store(nil)
	if err := os.Remove(unreadable); err != nil {
		t.Fatalf("removing the unreadable segment: %v", err)
	}
	if !readFailed.Load() {
		t.Fatal("setup: the log's first sequence was readable during the void")
	}
}
