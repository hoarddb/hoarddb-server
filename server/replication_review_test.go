package server

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/storage/kvfault"
	"github.com/hoarddb/hoarddb-server/transport"
)

// killReplica stops the replica without its clean-stop checkpoint and starts it
// again over the same data directory.
func (f *fifoCluster) killReplica(t *testing.T) *Cluster {
	t.Helper()
	node := f.tc.nodes[f.replicaIndex]
	node.server.core.cluster.skipStopCheckpoint = true
	dataDir := node.cfg.Storage.DataDir
	if node.client != nil {
		_ = node.client.Close()
		node.client = nil
	}
	if err := node.server.Stop(); err != nil {
		t.Fatalf("stop node %d: %v", f.replicaIndex+1, err)
	}
	node.server = nil
	f.tc.nodes[f.replicaIndex] = f.tc.startNode(f.replicaIndex, filepath.Dir(dataDir), f.tc.token)
	return f.replica()
}

// TestRetentionCannotTruncateAnEntryThePositionHasNotReached: a retention pass
// run the moment a replicated entry leaves the unapplied registry must find the
// node's position already past it. If the position moves later, the pass sees
// the entry as neither unapplied nor applied: it checkpoints a position without
// the entry, syncs, and truncates the entry out of the log, and after a kill
// neither holds it, so the owner's re-send lands a second time. The pass runs
// from afterUnappliedReleaseHook, inside applyAsReplica, so a position raised
// anywhere after the release (in handleReplPush, as it was at 0d4766f) is caught.
func TestRetentionCannotTruncateAnEntryThePositionHasNotReached(t *testing.T) {
	const (
		base          = 3
		sealingSource = "other-node"
	)
	f := newFIFOCluster(t, base)
	replica := f.replica()
	replica.s.wal.SetMaxSegmentBytes(1) // every entry is its own segment, so a pass can truncate all but the last
	template := ownEntriesOf(t, f.tc.nodes[f.ownerIndex].server.core.cluster, testBucket)[0]
	held, sealing := template, template
	held.Seq, sealing.Seq = template.Seq+1000, template.Seq+1001
	heldLocalSeq := replica.s.wal.LastSequence() + 1

	prune := func(entry ReplEntry) {
		if entry.Seq != held.Seq {
			return
		}
		// A later entry seals the held one's segment, then a pass runs. It comes
		// from another source: from the same one it would raise the position
		// past the held entry and hide the gap.
		if resp := pushToReplica(t, replica, sealingSource, []ReplEntry{sealing}); !resp.Success {
			t.Errorf("the entry that seals the segment: %s", resp.Error)
			return
		}
		// The bound is over every member's cursor of this node's own
		// stream, which carries nothing here: advanced, it no longer holds
		// the log, and the pass can truncate what the window is about.
		advanceEveryStream(t, replica)
		if err := replica.pruneWAL(); err != nil {
			t.Errorf("retention pass inside the window: %v", err)
		}
	}
	afterUnappliedReleaseHook.Store(&prune)
	t.Cleanup(func() { afterUnappliedReleaseHook.Store(nil) })
	if resp := pushToReplica(t, replica, f.ownerID, []ReplEntry{held}); !resp.Success {
		t.Fatalf("the held entry: %s", resp.Error)
	}
	afterUnappliedReleaseHook.Store(nil)
	if first, err := replica.s.wal.FirstSequence(); err != nil || first <= heldLocalSeq {
		t.Fatalf("test setup: the log still holds the held entry (local sequence %d, first retained %d, %v)", heldLocalSeq, first, err)
	}

	restarted := f.killReplica(t)
	if resp := pushToReplica(t, restarted, f.ownerID, []ReplEntry{held}); !resp.Success {
		t.Fatalf("the re-send of the held entry: %s", resp.Error)
	}
	if resp := pushToReplica(t, restarted, sealingSource, []ReplEntry{sealing}); !resp.Success {
		t.Fatalf("the re-send of the sealing entry: %s", resp.Error)
	}
	if got, want := f.tc.localCount(f.replicaIndex), int64(base+2); got != want {
		t.Fatalf("the queue holds %d elements after the owner re-sent two entries it had already acknowledged; want %d", got, want)
	}
}

// TestACheckpointCannotDeleteAFloorCommittedAfterItsSnapshot: a snapshot position
// recorded while a checkpoint is between reading the store and committing must
// survive the checkpoint. Lost, the materialized copy stays and its history is
// replayed over it after a restart.
func TestACheckpointCannotDeleteAFloorCommittedAfterItsSnapshot(t *testing.T) {
	_, _, c, _ := newReplicaOnFaultStore(t)
	if err := c.recordSeeded("src", "app", "b", 10); err != nil {
		t.Fatalf("recording the first position: %v", err)
	}
	c.markApplied("src", 30) // past the floor of 10, so the checkpoint deletes it
	recorded := make(chan error, 1)
	c.beforeCheckpointCommit = func() {
		go func() { recorded <- c.recordSeeded("src", "app", "b", 50) }()
		time.Sleep(300 * time.Millisecond)
	}
	if err := c.checkpointPosition("src", true); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	c.beforeCheckpointCommit = nil
	select {
	case err := <-recorded:
		if err != nil {
			t.Fatalf("recording the later position: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the later position was never recorded")
	}
	restarted := restartPositionTestCluster(t, c)
	if err := restarted.loadPositions(); err != nil {
		t.Fatalf("restart: %v", err)
	}
	if got := restarted.seededThroughFor("src", "app", "b"); got != 50 {
		t.Fatalf("after a restart the snapshot position of app/b from src is %d, want 50", got)
	}
}

// TestASnapshotPositionIsDurableWhenRecorded: one sync per pull, because a
// flush of a rotated memtable can make the copy's writes durable without any
// sync, and a position not yet durable would then be lost while the copy it
// describes stays (Cluster.recordSeeded). A later sync does carry an earlier
// unsynced write: the patched Badger's DB.Sync syncs rotated memtables too.
func TestASnapshotPositionIsDurableWhenRecorded(t *testing.T) {
	_, kv, c, dataDir := newReplicaOnFaultStore(t)
	if err := c.recordSeeded("src", "app", "b", 50); err != nil {
		t.Fatalf("recording: %v", err)
	}
	restarted := killedAfterPowerLoss(t, dataDir, kv, c.s)
	if got := restarted.seededThroughFor("src", "app", "b"); got != 50 {
		t.Fatalf("after a power loss the snapshot position is %d, want 50", got)
	}
}

// TestPositionLocksDoNotDeadlockUnderLoad pins the order positionMu, then
// appliedMu: every writer of a position runs against every other at once, and
// the run must finish. An inversion anywhere shows as a deadlock here (and as a
// race report under -race).
func TestPositionLocksDoNotDeadlockUnderLoad(t *testing.T) {
	_, _, c, _ := newReplicaOnFaultStore(t)
	const rounds = 200
	var running sync.WaitGroup
	work := []func(int){
		func(i int) { _ = c.recordSeeded("src", "app", fmt.Sprintf("b%d", i%7), uint64(i+1)) },
		func(i int) { _ = c.forgetSeeded("src", "app", fmt.Sprintf("b%d", i%7)) },
		func(i int) { c.markApplied("src", uint64(i+1)); _ = c.shouldApply("src", uint64(i)) },
		func(int) { _, _ = c.checkpointAllPositions(false) },
		func(i int) { _ = c.checkpointPosition("src", i%10 == 0) },
	}
	for _, step := range work {
		running.Add(1)
		go func() {
			defer running.Done()
			for i := 0; i < rounds; i++ {
				step(i)
			}
		}()
	}
	finished := make(chan struct{})
	go func() { running.Wait(); close(finished) }()
	select {
	case <-finished:
	case <-time.After(60 * time.Second):
		t.Fatal("the position writers did not finish in 60 s: a lock-order inversion")
	}
}

// TestAPushThatLandsOnTheOwnerKeepsItsPositionAcrossAKill: ownership moved while
// the old owner still streams, so its push lands here as the bucket's owner.
// applyAsOwner logs the entry as this node's own, so after a kill the log cannot
// give that source's position back: it must be checkpointed, synced, before the
// push is acknowledged, or the old owner's re-send is applied a second time.
func TestAPushThatLandsOnTheOwnerKeepsItsPositionAcrossAKill(t *testing.T) {
	s, kv, c, dataDir := newReplicaOnFaultStore(t)
	const (
		oldOwner = "old-owner"
		pushes   = 4
	)
	if !c.ownsBucket(placementOf("app", "queue")) {
		t.Fatal("test setup: the node does not own app/queue, so the push would not land on an owner")
	}
	stream := make([]ReplEntry, 0, pushes)
	for seq := uint64(1); seq <= pushes; seq++ {
		payload, err := bson.Marshal(PushRequest{Database: "app", Bucket: "queue", Data: map[string]interface{}{"n": seq}})
		if err != nil {
			t.Fatalf("encoding push %d: %v", seq, err)
		}
		stream = append(stream, ReplEntry{Seq: seq, Op: int32(transport.OpFIFOPush), Bucket: "queue", Payload: payload})
	}
	if resp := pushToReplica(t, c, oldOwner, stream); !resp.Success || resp.AppliedThrough != pushes {
		t.Fatalf("the old owner's pushes were not acknowledged through %d: %+v", pushes, resp)
	}

	restarted := killedAfterPowerLoss(t, dataDir, kv, s)
	if err := restarted.s.replayWAL(context.Background()); err != nil {
		t.Fatalf("replaying the log after the kill: %v", err)
	}
	if resp := pushToReplica(t, restarted, oldOwner, stream); !resp.Success {
		t.Fatalf("the re-send was refused: %s", resp.Error)
	}
	counted, err := restarted.s.Count(CountRequest{Database: "app", Bucket: "queue"})
	if err != nil {
		t.Fatalf("counting the queue: %v", err)
	}
	if counted.Count != pushes {
		t.Fatalf("the queue holds %d elements after the old owner re-sent the %d it had acknowledged before the kill; want %d",
			counted.Count, pushes, pushes)
	}
}

// syncFailingKV is a store that cannot make anything durable while fail is set:
// Sync fails and so does a synced commit. An unsynced commit still lands, in
// memory, as it does on a disk that has stopped taking flushes.
type syncFailingKV struct {
	storage.KV
	fail atomic.Bool
}

func (kv *syncFailingKV) NewBatch() storage.KVBatch {
	return &syncFailingBatch{KVBatch: kv.KV.NewBatch(), owner: kv}
}

func (kv *syncFailingKV) Sync() error {
	if kv.fail.Load() {
		return errors.New("the store could not sync")
	}
	return kv.KV.Sync()
}

type syncFailingBatch struct {
	storage.KVBatch
	owner *syncFailingKV
}

func (batch *syncFailingBatch) Commit(sync bool) error {
	if sync && batch.owner.fail.Load() {
		return errors.New("the store could not sync the commit")
	}
	return batch.KVBatch.Commit(sync)
}

// TestARetentionPassThatCannotSyncLeavesTheCheckpointOwed: a retention pass
// writes its checkpoint unsynced and then syncs the store. The debt a skip left
// is paid only once that sync succeeds: settled on the unsynced commit, a sync
// that then fails leaves the debt clear, the retry is acknowledged with no
// checkpoint, and a power loss puts the position below what was acknowledged.
func TestARetentionPassThatCannotSyncLeavesTheCheckpointOwed(t *testing.T) {
	dataDir := t.TempDir()
	inner := kvfault.New()
	store := &syncFailingKV{KV: inner}
	s := newKVFaultServer(dataDir, store)
	if resp, err := s.CreateBucket(CreateBucketRequest{Database: "app", Name: "held", Type: BucketTypeHash}); err != nil || !resp.Success {
		t.Fatalf("creating the bucket: %+v, %v", resp, err)
	}
	if err := inner.Sync(); err != nil {
		t.Fatalf("syncing the catalog: %v", err)
	}
	c := retentionTestCluster(t, s, dataDir)
	t.Cleanup(c.cancel)
	const floor = 40
	if err := c.recordSeeded("test-owner", "app", "held", floor); err != nil {
		t.Fatalf("recording the snapshot position: %v", err)
	}
	// An entry in the log, so a retention pass has a bound above zero and runs.
	if err := putThrough(c, asOwner, "held", "filler", "x"); err != nil {
		t.Fatalf("the write that gives retention a bound: %v", err)
	}
	covered := []ReplEntry{
		{Seq: floor - 5, Op: int32(transport.OpPut), Bucket: "held", Payload: putPayload(t, "app", "held", "covered")},
		{Seq: floor - 1, Op: int32(transport.OpPut), Bucket: "held", Payload: putPayload(t, "app", "held", "covered2")},
	}

	// The stream opens while the store still syncs: the frames below are the
	// pushes of a stream that has opened.
	streamOf(t, c, "test-owner")
	store.fail.Store(true)
	if resp := pushToReplica(t, c, "test-owner", covered); resp.Success {
		t.Fatalf("the frame was acknowledged although its checkpoint could not sync: %+v", resp)
	}
	if err := c.pruneWAL(); err == nil {
		t.Fatal("test setup: the retention pass succeeded although the store cannot sync")
	}
	if retry := pushToReplica(t, c, "test-owner", covered); retry.Success {
		restarted := killedAfterPowerLoss(t, dataDir, inner, s)
		if restarted.shouldApply("test-owner", floor-1) {
			t.Fatalf("the retry was acknowledged through %d while the store could not sync, and after a power loss the position "+
				"of test-owner is below it: the retention pass's unsynced checkpoint settled the debt", floor-1)
		}
		t.Fatalf("the retry was acknowledged through %d while the store could not sync", floor-1)
	}

	store.fail.Store(false)
	if retry := pushToReplica(t, c, "test-owner", covered); !retry.Success || retry.AppliedThrough != floor-1 {
		t.Fatalf("once the store syncs again the retry was not acknowledged through %d: %+v", floor-1, retry)
	}
	restarted := killedAfterPowerLoss(t, dataDir, inner, s)
	if restarted.shouldApply("test-owner", floor-1) {
		t.Fatalf("after a power loss behind the acknowledged retry the position of test-owner is below %d", floor-1)
	}
}

// TestARetentionPassThatSyncsClearsTheCheckpointOwed: a skip under a snapshot
// position whose own synced checkpoint failed leaves the debt owed. A retention
// pass over a healthy store writes its checkpoint unsynced and then syncs the
// store; that sync, not the unsynced commit, is what pays the debt. This is the
// "is cleared after one whose sync succeeded" half of BUG-0209's first
// criterion on the retention path: no push runs between the refused skip that
// records the debt and the pass that clears it, so removing
// c.settleOwedCheckpoints(checkpointed) from pruneWAL leaves the debt owed and
// fails here.
func TestARetentionPassThatSyncsClearsTheCheckpointOwed(t *testing.T) {
	dataDir := t.TempDir()
	inner := kvfault.New()
	store := &syncFailingKV{KV: inner}
	s := newKVFaultServer(dataDir, store)
	if resp, err := s.CreateBucket(CreateBucketRequest{Database: "app", Name: "held", Type: BucketTypeHash}); err != nil || !resp.Success {
		t.Fatalf("creating the bucket: %+v, %v", resp, err)
	}
	if err := inner.Sync(); err != nil {
		t.Fatalf("syncing the catalog: %v", err)
	}
	c := retentionTestCluster(t, s, dataDir)
	t.Cleanup(c.cancel)
	const floor = 40
	if err := c.recordSeeded("test-owner", "app", "held", floor); err != nil {
		t.Fatalf("recording the snapshot position: %v", err)
	}
	// An entry in the log, so a retention pass has a bound above zero and runs.
	if err := putThrough(c, asOwner, "held", "filler", "x"); err != nil {
		t.Fatalf("the write that gives retention a bound: %v", err)
	}
	covered := []ReplEntry{
		{Seq: floor - 5, Op: int32(transport.OpPut), Bucket: "held", Payload: putPayload(t, "app", "held", "covered")},
		{Seq: floor - 1, Op: int32(transport.OpPut), Bucket: "held", Payload: putPayload(t, "app", "held", "covered2")},
	}
	streamOf(t, c, "test-owner")
	// Every frame is covered by the snapshot position, so the position moves
	// with no log entry behind it and owes a synced checkpoint. The store
	// refuses that checkpoint, so the frame is refused and the debt stays owed.
	store.fail.Store(true)
	if resp := pushToReplica(t, c, "test-owner", covered); resp.Success {
		t.Fatalf("the frame was acknowledged although its checkpoint could not sync: %+v", resp)
	}
	if !c.checkpointIsOwed("test-owner") {
		t.Fatal("test setup: the refused skip left no checkpoint debt")
	}

	// A healthy store: the pass's unsynced checkpoint commits, its sync
	// succeeds, and the debt is paid. No push runs in between.
	store.fail.Store(false)
	if err := c.pruneWAL(); err != nil {
		t.Fatalf("the retention pass over a healthy store: %v", err)
	}
	if c.checkpointIsOwed("test-owner") {
		t.Fatal("a retention pass whose store sync succeeded left the checkpoint debt owed")
	}
}

// TestACheckpointBelowTheDebtDoesNotSettleIt pins the comparison that pays a
// debt: a checkpoint that read the position before a skip raised it commits a
// position below the debt, and must leave the debt owed.
func TestACheckpointBelowTheDebtDoesNotSettleIt(t *testing.T) {
	_, _, c, _ := newReplicaOnFaultStore(t)
	c.markApplied("src", 10)
	c.beforeCheckpointCommit = func() { c.raisePositionOwingCheckpoint("src", 50) }
	if err := c.checkpointPosition("src", true); err != nil {
		t.Fatalf("the checkpoint that read position 10: %v", err)
	}
	c.beforeCheckpointCommit = nil
	if !c.checkpointIsOwed("src") {
		t.Fatal("a checkpoint that committed position 10 settled a debt of 50")
	}
	if err := c.checkpointPosition("src", true); err != nil {
		t.Fatalf("the checkpoint that reads position 50: %v", err)
	}
	if c.checkpointIsOwed("src") {
		t.Fatal("a synced checkpoint that committed position 50 left a debt of 50 owed")
	}
}

// TestAFailedFloorSkipCheckpointIsNotUndoneByTheRetry: the skip raises the
// in-memory position, the checkpoint fails and the frame is refused; the
// owner's retry is then skipped as already applied, and must still be
// checkpointed before it is acknowledged.
func TestAFailedFloorSkipCheckpointIsNotUndoneByTheRetry(t *testing.T) {
	dataDir := t.TempDir()
	inner := kvfault.New()
	refusing := &commitRefusingKV{KV: inner}
	s := newKVFaultServer(dataDir, refusing)
	if resp, err := s.CreateBucket(CreateBucketRequest{Database: "app", Name: "held", Type: BucketTypeHash}); err != nil || !resp.Success {
		t.Fatalf("creating the bucket: %+v, %v", resp, err)
	}
	if err := inner.Sync(); err != nil {
		t.Fatalf("syncing the catalog: %v", err)
	}
	c := retentionTestCluster(t, s, dataDir)
	t.Cleanup(c.cancel)
	const floor = 40
	if err := c.recordSeeded("test-owner", "app", "held", floor); err != nil {
		t.Fatalf("recording the snapshot position: %v", err)
	}
	covered := []ReplEntry{
		{Seq: floor - 5, Op: int32(transport.OpPut), Bucket: "held", Payload: putPayload(t, "app", "held", "covered")},
		{Seq: floor - 1, Op: int32(transport.OpPut), Bucket: "held", Payload: putPayload(t, "app", "held", "covered2")},
	}
	streamOf(t, c, "test-owner")
	refusing.refuse.Store(true)
	if resp := pushToReplica(t, c, "test-owner", covered); resp.Success {
		t.Fatalf("the frame was acknowledged although its checkpoint failed: %+v", resp)
	}
	refusing.refuse.Store(false)
	if resp := pushToReplica(t, c, "test-owner", covered); !resp.Success || resp.AppliedThrough != floor-1 {
		t.Fatalf("the retry was not acknowledged through %d: %+v", floor-1, resp)
	}
	restarted := killedAfterPowerLoss(t, dataDir, inner, s)
	if restarted.shouldApply("test-owner", floor-1) {
		t.Fatalf("after a kill behind the retry's acknowledgement the position is below %d: the retry was acknowledged with no checkpoint", floor-1)
	}
}
