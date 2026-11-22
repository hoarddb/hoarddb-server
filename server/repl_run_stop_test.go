package server

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/storage/kvfault"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// A replica logs a run of a stream push's entries, syncs the log once and then
// applies them in order (BUG-0521). When an apply stops the run (a batch too
// large for this node, a divergence, a store failure), the entries after it
// are in the log and were never applied: the run's orphans. These tests pin
// that no orphan is ever applied, by a restart's replay or as a second copy of
// the owner's re-send. Each puts the orphan in a FIFO bucket, where a second
// apply is a second element.

// otherBucketOwnedBy is a bucket name in database, other than taken, that
// nodeID owns on c's ring.
func otherBucketOwnedBy(t *testing.T, c *Cluster, database, nodeID, taken string) string {
	t.Helper()
	for i := 0; i < 512; i++ {
		name := fmt.Sprintf("queue-%d", i)
		owner, err := c.Owner(placementOf(database, name))
		if err != nil {
			t.Fatalf("ring owner of %q: %v", name, err)
		}
		if owner.ID == nodeID && name != taken {
			return name
		}
	}
	t.Fatalf("no queue name in 512 tries is placed on %q", nodeID)
	return ""
}

// waitPastOwnerSeq waits up to 60 s for node index to have applied the
// owner's stream through seq.
func (tc *testCluster) waitPastOwnerSeq(t *testing.T, index int, seq uint64, when string) {
	t.Helper()
	for deadline := time.Now().Add(60 * time.Second); tc.nodes[index].server.core.cluster.shouldApply(tc.addrs[0], seq); time.Sleep(200 * time.Millisecond) {
		if time.Now().After(deadline) {
			owner := tc.nodes[0].server.core.cluster
			state, reason := owner.peerFor(mustNode(t, owner, tc.addrs[index])).streamStatus()
			t.Fatalf("%s: node %d never moved past the owner's sequence %d: stream %v, %q", when, index+1, seq, state, reason)
		}
	}
}

// TestARunStoppedByAStoreFailureAppliesItsOrphanOnce: the 32 MiB replica is
// down while its owner logs a batch the owner itself refuses (too many
// entries) and then pushes to a FIFO bucket. Back up, the replica's catch-up carries both in
// one frame, so both join one run. The replica's store fails the batch's apply,
// which stops the run with the push logged and unapplied. Restarted, the
// replica's replay refuses the logged batch as too large and holds the
// owner's stream below it, so the push logged after it must not be applied by
// that replay: it would land ahead of an entry of its source this node has not
// applied. The owner's re-send of the batch is then consumed (the owner
// refused it too) and the stream moves on, and the FIFO must hold the push
// once, as its owner does.
func TestARunStoppedByAStoreFailureAppliesItsOrphanOnce(t *testing.T) {
	tc := memtableMixedCluster(t)
	defer tc.stop()
	owner := tc.nodes[0].server.core.cluster
	bucket := bucketNameOwnedBy(t, owner, "app", tc.addrs[0])
	queue := otherBucketOwnedBy(t, owner, "app", tc.addrs[0], bucket)
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: bucket, Type: BucketTypeHash})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: queue, Type: BucketTypeFIFO})
	waitForServing(t, tc, "app", bucket, 3)
	waitForServing(t, tc, "app", queue, 3)

	smallDir := tc.stopNodeKeepingData(2)
	// 60,000 documents are 120,001 keys live, over the 64 MiB owner's bound of
	// 104,857 and about 2.4 MB, inside one catch-up read with the push.
	refused := tc.frame(0, transport.OpBatchPut, prefixedSmallBatch("app", bucket, "r", 60_000))
	if !strings.Contains(string(refused.Payload), "none of it was written") {
		t.Fatalf("setup: the owner did not refuse 60,000 documents (an answer of %d bytes)", len(refused.Payload))
	}
	refusedSeq := owner.walLastSequence()
	tc.ok(0, transport.OpFIFOPush, PushRequest{Database: "app", Bucket: queue, Data: map[string]interface{}{"_id_": "after"}, WriteConcern: "one"})
	pushSeq := owner.walLastSequence()

	// Every apply on the replica fails while armed: the only store neither
	// running node owns is the replica's.
	running := []storage.KV{tc.nodes[0].server.core.kv, tc.nodes[1].server.core.kv}
	var armed atomic.Bool
	var failed atomic.Int64
	armed.Store(true)
	hook := func(kv storage.KV, _ uint16, _ uint64, _ uint64) error {
		if !armed.Load() || slices.Contains(running, kv) {
			return nil
		}
		failed.Add(1)
		return errors.New("injected store failure on the replica")
	}
	store.ApplyFaultHook.Store(&hook)
	defer store.ApplyFaultHook.Store(nil)

	tc.nodes[2] = tc.startNode(2, smallDir, tc.token)
	small := tc.nodes[2].server.core.cluster
	loggedAtStart := small.walLastSequence()
	for deadline := time.Now().Add(30 * time.Second); failed.Load() == 0 || small.walLastSequence() < loggedAtStart+2; time.Sleep(20 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("setup: the replica never logged the batch and the push and failed the batch (failures %d, logged %d past %d)",
				failed.Load(), small.walLastSequence()-loggedAtStart, loggedAtStart)
		}
	}
	smallDir = tc.stopNodeKeepingData(2)
	armed.Store(false)

	tc.nodes[2] = tc.startNode(2, smallDir, tc.token)
	// The count is read before the position: once the re-send of the batch
	// is consumed the push may land, and the position never goes back.
	if held := tc.bucketCountOn(2, "app", queue); held != 0 && tc.nodes[2].server.core.cluster.shouldApply(tc.addrs[0], refusedSeq) {
		t.Fatalf("the restarted replica holds %d elements of the FIFO pushed after the owner's sequence %d, which it has not applied", held, refusedSeq)
	}
	tc.waitPastOwnerSeq(t, 2, pushSeq, "after the replica restarted")
	if tc.nodes[2].server.core.cluster.shouldApply(tc.addrs[0], refusedSeq) {
		t.Fatalf("the replica moved past the push without consuming the refused batch at %d", refusedSeq)
	}
	if held, owned := tc.popOrderLocal(t, 2, queue), tc.popOrderLocal(t, 0, queue); !slices.Equal(held, owned) {
		t.Fatalf("the replica holds the FIFO as %v, its owner %v: the push logged after the stopped batch was applied more than once", held, owned)
	}
	if held := tc.bucketCountOn(2, "app", bucket); held != 0 {
		t.Fatalf("the replica holds %d documents of a batch its owner refused", held)
	}
}

// TestASmallerReplicaCrashingBeforeItsRefusalRecordAppliesNothingLoggedAfterTheBatch
// is BUG-0248's crash with a run: the 32 MiB replica's catch-up carries a
// batch it cannot commit and a FIFO push after it, in one frame, one run. Its
// apply refuses the batch, and the node dies before the refusal is recorded
// and before anything else is: both fault hooks fail. Restarted unresized, its
// replay refuses the logged batch again and holds the owner's stream below it,
// and must not apply the push logged after it: the stream stalls on the
// batch, so nothing of the source past it may reach the bucket. Resized, the
// replica must hold the batch and the push once each.
func TestASmallerReplicaCrashingBeforeItsRefusalRecordAppliesNothingLoggedAfterTheBatch(t *testing.T) {
	tc := memtableMixedCluster(t)
	defer tc.stop()
	owner := tc.nodes[0].server.core.cluster
	bucket := bucketNameOwnedBy(t, owner, "app", tc.addrs[0])
	queue := otherBucketOwnedBy(t, owner, "app", tc.addrs[0], bucket)
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: bucket, Type: BucketTypeHash})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: queue, Type: BucketTypeFIFO})
	tc.ok(0, transport.OpPut, InsertRequest{Database: "app", Bucket: bucket, Key: "seed", Data: map[string]interface{}{"n": 1}, WriteConcern: "all"})
	waitForServing(t, tc, "app", queue, 3)

	smallDir := tc.stopNodeKeepingData(2)
	const documents = 27_000
	batch := smallDocumentBatch("app", bucket, documents)
	batch.WriteConcern = "one"
	frame := tc.ok(0, transport.OpBatchPut, batch)
	var answer BatchInsertResponse
	if err := bson.Unmarshal(frame.Payload, &answer); err != nil || !answer.Success {
		t.Fatalf("setup: the 64 MiB owner did not commit %d small documents: %s (%v)", documents, frame.Payload, err)
	}
	batchSeq := owner.walLastSequence()
	tc.ok(0, transport.OpFIFOPush, PushRequest{Database: "app", Bucket: queue, Data: map[string]interface{}{"_id_": "after"}, WriteConcern: "one"})
	pushSeq := owner.walLastSequence()

	smallID := tc.addrs[2]
	var crashed atomic.Bool
	recordHook := func(c *Cluster, _ string, _ uint64) error {
		if c.self.ID != smallID {
			return nil
		}
		crashed.Store(true)
		return errors.New("injected crash before the size refusal record commits")
	}
	sizeRefusalRecordFaultHook.Store(&recordHook)
	defer sizeRefusalRecordFaultHook.Store(nil)
	voidHook := func(c *Cluster, _ string) error {
		if c.self.ID != smallID {
			return nil
		}
		return errors.New("injected crash before the stopped run's record commits")
	}
	orphanVoidFaultHook.Store(&voidHook)
	defer orphanVoidFaultHook.Store(nil)

	tc.nodes[2] = tc.startNode(2, smallDir, tc.token)
	for deadline := time.Now().Add(30 * time.Second); !crashed.Load(); time.Sleep(20 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("setup: the 32 MiB replica never reached its size refusal record for the owner's batch")
		}
	}
	smallDir = tc.stopNodeKeepingData(2)
	sizeRefusalRecordFaultHook.Store(nil)
	orphanVoidFaultHook.Store(nil)

	tc.nodes[2] = tc.startNode(2, smallDir, tc.token)
	if held := tc.bucketCountOn(2, "app", queue); held != 0 {
		t.Fatalf("the restarted 32 MiB replica holds %d elements of the FIFO, logged after the batch its stream stalls on", held)
	}
	tc.stallStaysOnSmallReplica(t, bucket, 1, batchSeq, "after the replica crashed before its refusal record and restarted unresized")
	if held := tc.bucketCountOn(2, "app", queue); held != 0 {
		t.Fatalf("the stalled 32 MiB replica holds %d elements of the FIFO, logged after the batch its stream stalls on", held)
	}

	tc.configure = func(index int, cfg *config.Config) {
		if index == 2 {
			cfg.Storage.MemtableMB = 64
		}
	}
	tc.restartNode(2)
	tc.waitPastOwnerSeq(t, 2, pushSeq, "after the replica was resized and restarted")
	tc.requireCounts(t, bucket, documents+1, "after the replica was resized and restarted")
	if held, owned := tc.popOrderLocal(t, 2, queue), tc.popOrderLocal(t, 0, queue); !slices.Equal(held, owned) {
		t.Fatalf("the resized replica holds the FIFO as %v, its owner %v", held, owned)
	}
}

// TestAReplicaCrashingBeforeItsRunsVoidHoldsTheBatchAndThePushOnceResized:
// the 32 MiB replica records its refusal of the owner's batch and dies before
// it voids the push its run logged after the batch. It comes back resized, so
// its replay applies the batch from its own log, settles the record, and must
// not apply the push: the start counted it, the stalled stream never
// acknowledged it, and the owner re-sends it. The replica must hold each once.
func TestAReplicaCrashingBeforeItsRunsVoidHoldsTheBatchAndThePushOnceResized(t *testing.T) {
	tc := memtableMixedCluster(t)
	defer tc.stop()
	owner := tc.nodes[0].server.core.cluster
	bucket := bucketNameOwnedBy(t, owner, "app", tc.addrs[0])
	queue := otherBucketOwnedBy(t, owner, "app", tc.addrs[0], bucket)
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: bucket, Type: BucketTypeHash})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: queue, Type: BucketTypeFIFO})
	tc.ok(0, transport.OpPut, InsertRequest{Database: "app", Bucket: bucket, Key: "seed", Data: map[string]interface{}{"n": 1}, WriteConcern: "all"})
	waitForServing(t, tc, "app", queue, 3)

	smallDir := tc.stopNodeKeepingData(2)
	const documents = 27_000
	batch := smallDocumentBatch("app", bucket, documents)
	batch.WriteConcern = "one"
	tc.ok(0, transport.OpBatchPut, batch)
	tc.ok(0, transport.OpFIFOPush, PushRequest{Database: "app", Bucket: queue, Data: map[string]interface{}{"_id_": "after"}, WriteConcern: "one"})
	pushSeq := owner.walLastSequence()

	smallID := tc.addrs[2]
	var crashed atomic.Bool
	voidHook := func(c *Cluster, _ string) error {
		if c.self.ID != smallID {
			return nil
		}
		crashed.Store(true)
		return errors.New("injected crash before the stopped run's record commits")
	}
	orphanVoidFaultHook.Store(&voidHook)
	defer orphanVoidFaultHook.Store(nil)

	tc.nodes[2] = tc.startNode(2, smallDir, tc.token)
	for deadline := time.Now().Add(30 * time.Second); !crashed.Load(); time.Sleep(20 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("setup: the 32 MiB replica never stopped a run on the owner's batch")
		}
	}
	if records := tc.sizeRefusalRecords(2); records != 1 {
		t.Fatalf("setup: the replica holds %d size refusal records, want the one its apply wrote", records)
	}
	smallDir = tc.stopNodeKeepingData(2)
	orphanVoidFaultHook.Store(nil)

	tc.configure = func(index int, cfg *config.Config) {
		if index == 2 {
			cfg.Storage.MemtableMB = 64
		}
	}
	tc.nodes[2] = tc.startNode(2, smallDir, tc.token)
	tc.waitPastOwnerSeq(t, 2, pushSeq, "after the replica was resized and restarted")
	tc.requireCounts(t, bucket, documents+1, "after the replica was resized and restarted")
	if held, owned := tc.popOrderLocal(t, 2, queue), tc.popOrderLocal(t, 0, queue); !slices.Equal(held, owned) {
		t.Fatalf("the resized replica holds the FIFO as %v, its owner %v", held, owned)
	}
}

// TestADivergedRunAppliesNothingOfTheSourcePastItAcrossARestart: a replica
// whose copy of one bucket diverged is down while its owner writes to that
// bucket and then pushes to a FIFO bucket. Its catch-up carries both in one
// run; the first is refused at apply and recorded diverged, which stalls the
// stream from that owner, so nothing of it past the refused entry may reach
// this node (divergence.go), not after a restart either. Once migrate re-seeds
// the diverged copy, the FIFO must hold the push once.
func TestADivergedRunAppliesNothingOfTheSourcePastItAcrossARestart(t *testing.T) {
	s := setUpDivergedDeclaration(t)
	defer s.tc.stop()
	tc := s.tc
	full := ringWith(t, 3, tc.addrs...)
	queue := bucketNamed(t, "queue", func(name string) bool {
		return ownerOn(full, name) == tc.addrs[s.owner] && name != s.bucket && name != s.other
	})
	tc.ok(s.owner, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: queue, Type: BucketTypeFIFO})
	waitForServing(t, tc, testDatabase, queue, 3)

	base := tc.stopNodeKeepingData(s.replica)
	tc.ok(s.owner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: s.bucket, Key: "k",
		Data: map[string]interface{}{"Age": 1}, WriteConcern: "majority"})
	tc.ok(s.owner, transport.OpFIFOPush, PushRequest{Database: testDatabase, Bucket: queue, Data: map[string]interface{}{"_id_": "after"}, WriteConcern: "majority"})
	pushSeq := tc.nodes[s.owner].server.core.cluster.walLastSequence()
	tc.nodes[s.replica] = tc.startNode(s.replica, base, tc.token)
	s.divergedFrom(t)
	if held := tc.bucketCountOn(s.replica, testDatabase, queue); held != 0 {
		t.Fatalf("the diverged replica holds %d elements of the FIFO pushed after the refused entry", held)
	}

	tc.restartNode(s.replica)
	tc.waitReady()
	if held := tc.bucketCountOn(s.replica, testDatabase, queue); held != 0 {
		t.Fatalf("the diverged replica, restarted, holds %d elements of the FIFO pushed after the refused entry: its replay applied what the stall holds back", held)
	}

	resp := decodedMigrate(t, tc.frame(s.replica, transport.OpMigrate, MigrateRequest{Database: testDatabase, Bucket: s.bucket}))
	if !resp.Success {
		t.Fatalf("migrate on the diverged replica answered %+v", resp)
	}
	for deadline := time.Now().Add(60 * time.Second); clusterOf(tc, s.replica).shouldApply(tc.addrs[s.owner], pushSeq); time.Sleep(200 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("the re-seeded replica never moved past the owner's push at %d", pushSeq)
		}
	}
	if held, owned := tc.popOrderLocalIn(t, s.replica, testDatabase, queue), tc.popOrderLocalIn(t, s.owner, testDatabase, queue); !slices.Equal(held, owned) {
		t.Fatalf("the re-seeded replica holds the FIFO as %v, its owner %v", held, owned)
	}
}

// popOrderLocalIn is popOrderLocal for a bucket of any database.
func (tc *testCluster) popOrderLocalIn(t *testing.T, index int, database, bucket string) []string {
	t.Helper()
	resp, err := tc.nodes[index].server.core.Peek(PeekRequest{Database: database, Bucket: bucket, Limit: maxPeekLimit})
	if err != nil {
		t.Fatalf("node %d: local peek of %q: %v", index+1, bucket, err)
	}
	order := make([]string, 0, len(resp.Items))
	for _, element := range resp.Items {
		id, ok := element.Lookup("_id_").StringValueOK()
		if !ok {
			t.Fatalf("node %d: an element of %q has no _id_: %v", index+1, bucket, element)
		}
		order = append(order, id)
	}
	return order
}

// A run stopped by a term-ahead refusal or by a divergence has its stopped
// entry consumed by the store: the bucket's mark moves past it, and replay
// passes over it. A crash after that and before the run's void is durable left
// the stopped entry counted by the start and skipped by replay, while the
// orphans logged after it were replayed ahead of it, and the owner's re-send
// was passed over as a duplicate (BUG-0571). These two tests crash a replica
// on the fault store in that window: the void, and the divergence record,
// fail as the crash leaves them, and the store keeps what it committed, as a
// process crash does.

// failTheStopsRecords fails, on c, every record a stopped run writes after
// its stopped entry's apply returned: the void, and a divergence record.
func failTheStopsRecords(t *testing.T, c *Cluster) {
	t.Helper()
	voidHook := func(failing *Cluster, _ string) error {
		if failing != c {
			return nil
		}
		return errors.New("injected crash before the stopped run's void commits")
	}
	orphanVoidFaultHook.Store(&voidHook)
	recordHook := func(failing *Cluster, _ string, _ uint64) error {
		if failing != c {
			return nil
		}
		return errors.New("injected crash before the divergence record commits")
	}
	divergenceRecordFaultHook.Store(&recordHook)
	t.Cleanup(func() {
		orphanVoidFaultHook.Store(nil)
		divergenceRecordFaultHook.Store(nil)
	})
}

// crashedKeepingTheStore restarts the replica over its store and log as a
// process crash leaves them: what the store committed without a sync is kept
// (the fault store is synced first, then loses nothing). The hooks are cleared
// first, and the restarted node has replayed its log.
func crashedKeepingTheStore(t *testing.T, dataDir string, kv *kvfault.Store, s *Server) *Cluster {
	t.Helper()
	orphanVoidFaultHook.Store(nil)
	divergenceRecordFaultHook.Store(nil)
	if err := kv.Sync(); err != nil {
		t.Fatalf("syncing the store as a process crash keeps it: %v", err)
	}
	restarted := killedAfterPowerLoss(t, dataDir, kv, s)
	if err := restarted.s.replayWAL(context.Background()); err != nil {
		t.Fatalf("replaying the log: %v", err)
	}
	return restarted
}

// TestACrashAfterATermAheadStopKeepsTheRunInOrder: one frame of two pushes of
// a term above the term the queue's state document holds. The copy's term in
// memory is raised to it, as when the check before the log misses what the
// store then refuses (lineage_rule.go:Cluster.passedOverByLineage), so both
// are logged in one run, and the store refuses the first as term-ahead and
// consumes its local sequence. The node dies before the run's void is
// durable. Restarted, it must not apply the second push ahead of the first,
// and once the owner re-sends both, the queue must hold [a b].
func TestACrashAfterATermAheadStopKeepsTheRunInOrder(t *testing.T) {
	s, kv, c, dataDir := newReplicaOnFaultStore(t)
	term, opened, err := s.bucketLineage("app", "queue")
	if err != nil {
		t.Fatalf("reading app/queue's lineage: %v", err)
	}
	rec := s.lockServedRecord("app", "queue")
	if rec == nil {
		t.Fatal("setup: app/queue is not served")
	}
	rec.setLineageLocked(term+1, opened)
	rec.mu.Unlock()

	failTheStopsRecords(t, c)
	first, second := queuePush(t, 1, "a"), queuePush(t, 2, "b")
	first.Term, second.Term = term+1, term+1
	loggedBefore := c.s.wal.LastSequence()
	resp := pushToReplica(t, c, frameSyncSource, []ReplEntry{first, second})
	if resp.Success || resp.AppliedThrough != 0 || !strings.Contains(resp.Error, "term") {
		t.Fatalf("setup: the frame was answered success=%t applied_through=%d error=%q; want the first push refused as term-ahead",
			resp.Success, resp.AppliedThrough, resp.Error)
	}
	if logged := c.s.wal.LastSequence() - loggedBefore; logged != 2 {
		t.Fatalf("setup: the frame put %d entries in the replica's log, want both, as one run", logged)
	}

	restarted := crashedKeepingTheStore(t, dataDir, kv, s)
	if order := appQueueOrder(t, restarted.s); len(order) > 0 && order[0] != "a" {
		t.Fatalf("restarted, the replica holds the queue as %v: the push logged after the term-ahead stop was replayed ahead of it", order)
	}
	if resp := pushToReplica(t, restarted, frameSyncSource, []ReplEntry{queuePush(t, 1, "a"), queuePush(t, 2, "b")}); !resp.Success || resp.AppliedThrough != 2 {
		t.Fatalf("the re-send was answered success=%t applied_through=%d error=%q", resp.Success, resp.AppliedThrough, resp.Error)
	}
	if order := appQueueOrder(t, restarted.s); !slices.Equal(order, []string{"a", "b"}) {
		t.Fatalf("after the restart and the owner's re-send the queue is %v, want [a b]", order)
	}
}

// TestACrashAfterAnUnrecordedDivergenceKeepsTheRunInOrder: one frame of a put
// its owner applied, which this replica's store refuses (the replica declares
// an index on the field, and NaN is no index key: a refusal inside the batch,
// which the store consumes), and a push after it. The refusal is a
// divergence, and the node dies before the divergence record and the run's
// void are durable. Restarted, nothing of the source past the diverged entry
// may reach the copy, and the entry must not be counted applied: its owner
// re-sends it, and the re-send must stall on it again, as the record makes it.
func TestACrashAfterAnUnrecordedDivergenceKeepsTheRunInOrder(t *testing.T) {
	stopARunOnADivergenceThenRestart(t, crashedKeepingTheStore)
}

// TestAPowerCutAfterAnUnrecordedDivergenceKeepsTheRunInOrder is the same stop
// with the power cut instead: what the store committed without a sync is
// gone. The divergence's record and the void must be synced with the stopped
// entry's consumption, or the cut takes them and leaves the push to replay.
func TestAPowerCutAfterAnUnrecordedDivergenceKeepsTheRunInOrder(t *testing.T) {
	stopARunOnADivergenceThenRestart(t, func(t *testing.T, dataDir string, kv *kvfault.Store, s *Server) *Cluster {
		t.Helper()
		orphanVoidFaultHook.Store(nil)
		divergenceRecordFaultHook.Store(nil)
		restarted := killedAfterPowerLoss(t, dataDir, kv, s)
		if err := restarted.s.replayWAL(context.Background()); err != nil {
			t.Fatalf("replaying the log: %v", err)
		}
		return restarted
	})
}

// stopARunOnADivergenceThenRestart is the divergence tests' body: restart
// brings the node back after the stop, as a crash or a power cut leaves it.
func stopARunOnADivergenceThenRestart(t *testing.T, restart func(*testing.T, string, *kvfault.Store, *Server) *Cluster) {
	t.Helper()
	dataDir := t.TempDir()
	kv := kvfault.New()
	s := newKVFaultServer(dataDir, kv)
	for _, create := range []CreateBucketRequest{
		{Database: "app", Name: "indexed", Type: BucketTypeHash, Options: map[string]interface{}{"indices": []interface{}{"n"}}},
		{Database: "app", Name: "queue", Type: BucketTypeFIFO},
	} {
		if resp, err := s.CreateBucket(create); err != nil || !resp.Success {
			t.Fatalf("creating bucket %q: %+v, %v", create.Name, resp, err)
		}
	}
	if err := kv.Sync(); err != nil {
		t.Fatalf("syncing the catalog: %v", err)
	}
	c := retentionTestCluster(t, s, dataDir)
	t.Cleanup(c.cancel)

	put, err := bson.Marshal(InsertRequest{Database: "app", Bucket: "indexed", Key: "k", Data: map[string]interface{}{"n": math.NaN()}})
	if err != nil {
		t.Fatalf("encoding the put: %v", err)
	}
	diverged := ReplEntry{Seq: 1, Op: int32(transport.OpPut), Bucket: "indexed", Key: []byte("k"), Payload: put, OwnerOutcome: ownerOutcomeApplied}
	after := queuePush(t, 2, "a")
	after.OwnerOutcome = ownerOutcomeApplied

	failTheStopsRecords(t, c)
	loggedBefore := c.s.wal.LastSequence()
	resp := pushToReplica(t, c, frameSyncSource, []ReplEntry{diverged, after})
	if resp.Success || resp.AppliedThrough != 0 || !strings.Contains(resp.Error, "sequence 1") {
		t.Fatalf("setup: the frame was answered success=%t applied_through=%d error=%q; want the put refused as diverged",
			resp.Success, resp.AppliedThrough, resp.Error)
	}
	if logged := c.s.wal.LastSequence() - loggedBefore; logged != 2 {
		t.Fatalf("setup: the frame put %d entries in the replica's log, want both, as one run", logged)
	}

	restarted := restart(t, dataDir, kv, s)
	if order := appQueueOrder(t, restarted.s); len(order) != 0 {
		t.Fatalf("restarted, the replica holds the queue as %v: the push logged after the diverged entry was replayed past it", order)
	}
	if !restarted.shouldApply(frameSyncSource, 1) {
		t.Fatalf("restarted, the replica counts the diverged entry applied (position %d): its owner would never send it again",
			appliedSnapshot(restarted)[frameSyncSource])
	}
	if resp := pushToReplica(t, restarted, frameSyncSource, []ReplEntry{diverged, after}); resp.Success || resp.AppliedThrough != 0 {
		t.Fatalf("the re-send was answered success=%t applied_through=%d error=%q; want it refused at the diverged entry",
			resp.Success, resp.AppliedThrough, resp.Error)
	}
	if order := appQueueOrder(t, restarted.s); len(order) != 0 {
		t.Fatalf("after the owner's re-send the replica holds the queue as %v; the stream stalls on the diverged entry", order)
	}
	if _, recorded := restarted.divergenceFrom(frameSyncSource); !recorded {
		t.Fatal("after the owner's re-send no divergence from it is recorded")
	}
}

// A divergence the store never reached, a refusal the handler gave before the
// store or a create of another incarnation, is recorded after its apply, in a
// synced write of its own, while the entry it refuses is already in the log.
// A crash between the two left the entry counted applied and no record, and
// replay applied it again, refused again and recorded nothing: the copy went
// on without the owner's write, unrepaired (BUG-0581). The log now carries the
// owner's outcome, so replay judges the entry as the live apply did.

// updateOfMissing is an update of key "missing" in app/held, which this
// replica does not hold, at the sender's sequence seq.
func updateOfMissing(t *testing.T, seq uint64, ownerOutcome int32) ReplEntry {
	t.Helper()
	payload, err := bson.Marshal(UpdateRequest{Database: "app", Bucket: "held", Key: "missing", Set: map[string]interface{}{"n": 2}})
	if err != nil {
		t.Fatalf("encoding the update: %v", err)
	}
	return ReplEntry{Seq: seq, Op: int32(transport.OpUpdate), Bucket: "held", Key: []byte("missing"), Payload: payload, OwnerOutcome: ownerOutcome}
}

// requireDivergenceStallsAt requires restarted to hold source's divergence at
// seq, of reason, with its position below it and the queue empty, and the
// owner's re-send of the refused entry to be refused there.
func requireDivergenceStallsAt(t *testing.T, restarted *Cluster, seq uint64, reason string, resend []ReplEntry) {
	t.Helper()
	if order := appQueueOrder(t, restarted.s); len(order) != 0 {
		t.Fatalf("restarted, the replica holds the queue as %v: the push logged after the refused entry was replayed past it", order)
	}
	if !restarted.shouldApply(frameSyncSource, seq) {
		t.Fatalf("restarted, the replica counts the refused entry at %d applied (position %d): its owner would never send it again",
			seq, appliedSnapshot(restarted)[frameSyncSource])
	}
	if record, recorded := restarted.divergenceFrom(frameSyncSource); !recorded || record.Seq != seq || record.Reason != reason {
		t.Fatalf("restarted, the divergence from %s is %+v (recorded %t); want one at sequence %d, reason %q", frameSyncSource, record, recorded, seq, reason)
	}
	if resp := pushToReplica(t, restarted, frameSyncSource, resend); resp.Success || resp.AppliedThrough >= seq {
		t.Fatalf("the re-send was answered success=%t applied_through=%d error=%q; want it refused at the entry at %d",
			resp.Success, resp.AppliedThrough, resp.Error, seq)
	}
	if order := appQueueOrder(t, restarted.s); len(order) != 0 {
		t.Fatalf("after the owner's re-send the replica holds the queue as %v; the stream stalls on the refused entry", order)
	}
}

// TestACrashBeforeAHandlerRefusedDivergenceIsRecordedKeepsTheDivergence: one
// frame of an update its owner applied of a key this replica lacks, which the
// handler refuses before the store, and a push. The node dies before the
// divergence record and the run's void commit. Restarted, the divergence must
// be recorded, the entry not counted, nothing past it applied, and the
// owner's re-send must stall on it.
func TestACrashBeforeAHandlerRefusedDivergenceIsRecordedKeepsTheDivergence(t *testing.T) {
	s, kv, c, dataDir := newReplicaOnFaultStore(t)
	after := queuePush(t, 2, "a")
	after.OwnerOutcome = ownerOutcomeApplied
	frame := []ReplEntry{updateOfMissing(t, 1, ownerOutcomeApplied), after}

	failTheStopsRecords(t, c)
	resp := pushToReplica(t, c, frameSyncSource, frame)
	if resp.Success || resp.AppliedThrough != 0 || !strings.Contains(resp.Error, "sequence 1") {
		t.Fatalf("setup: the frame was answered success=%t applied_through=%d error=%q; want the update refused as diverged",
			resp.Success, resp.AppliedThrough, resp.Error)
	}

	restarted := crashedKeepingTheStore(t, dataDir, kv, s)
	requireDivergenceStallsAt(t, restarted, 1, refusalDiverged, frame)
}

// TestACrashKeepsAnAgreedRefusalAppliedAndRecordsTheDivergedOne: the same
// handler refusal twice from one owner. The first its owner refused too (its
// outcome unknown), so the replica agrees with it and acknowledges it; the
// second its owner applied, and the node dies before that divergence is
// recorded. The two leave the same log entry but for the owner's outcome.
// Restarted, the agreed refusal must still count applied (it was
// acknowledged, and the owner may have pruned it), and the diverged one must
// be recorded and not count.
func TestACrashKeepsAnAgreedRefusalAppliedAndRecordsTheDivergedOne(t *testing.T) {
	s, kv, c, dataDir := newReplicaOnFaultStore(t)
	if resp := pushToReplica(t, c, frameSyncSource, []ReplEntry{updateOfMissing(t, 1, ownerOutcomeUnknown)}); !resp.Success || resp.AppliedThrough != 1 {
		t.Fatalf("setup: the agreed refusal was answered success=%t applied_through=%d error=%q; want it acknowledged",
			resp.Success, resp.AppliedThrough, resp.Error)
	}
	after := queuePush(t, 3, "a")
	after.OwnerOutcome = ownerOutcomeApplied
	frame := []ReplEntry{updateOfMissing(t, 2, ownerOutcomeApplied), after}

	failTheStopsRecords(t, c)
	if resp := pushToReplica(t, c, frameSyncSource, frame); resp.Success || resp.AppliedThrough != 0 {
		t.Fatalf("setup: the diverged frame was answered success=%t applied_through=%d error=%q; want it refused",
			resp.Success, resp.AppliedThrough, resp.Error)
	}

	restarted := crashedKeepingTheStore(t, dataDir, kv, s)
	if restarted.shouldApply(frameSyncSource, 1) {
		t.Fatalf("restarted, the replica no longer counts the agreed refusal at 1 applied (position %d), which it acknowledged",
			appliedSnapshot(restarted)[frameSyncSource])
	}
	requireDivergenceStallsAt(t, restarted, 2, refusalDiverged, frame)
}

// createOfAnotherIncarnationOfHeld is a frame of one create, owner-applied at
// the sender's sequence 1, of the incarnation after the one of app/held s
// holds, which it returns.
func createOfAnotherIncarnationOfHeld(t *testing.T, s *Server) ([]ReplEntry, uint64) {
	t.Helper()
	generation, live, err := s.openBucketIdentity("app", "held")
	if err != nil || live == 0 {
		t.Fatalf("setup: app/held's identity: generation %d incarnation %d, %v", generation, live, err)
	}
	payload, err := bson.Marshal(bson.D{
		{Key: "database", Value: "app"}, {Key: "name", Value: "held"}, {Key: "type", Value: string(BucketTypeHash)},
		{Key: "db_gen", Value: generation}, {Key: "dropped_generations", Value: []int64{}},
		{Key: "term", Value: int64(InitialEpoch)}, {Key: "incarnation", Value: int64(live + 1)},
	})
	if err != nil {
		t.Fatalf("encoding the create: %v", err)
	}
	return []ReplEntry{{Seq: 1, Op: int32(transport.OpCreateBucket), Bucket: "held", Payload: payload, OwnerOutcome: ownerOutcomeApplied}}, live
}

// TestACrashBeforeAnotherIncarnationIsRecordedKeepsTheDivergence: a create of
// another incarnation of app/held, which this replica holds, is refused and
// the node dies before the refusal is recorded. Restarted, the refusal must be
// recorded, the create not counted, and the owner's re-send refused.
func TestACrashBeforeAnotherIncarnationIsRecordedKeepsTheDivergence(t *testing.T) {
	s, kv, c, dataDir := newReplicaOnFaultStore(t)
	frame, live := createOfAnotherIncarnationOfHeld(t, s)

	failTheStopsRecords(t, c)
	if resp := pushToReplica(t, c, frameSyncSource, frame); resp.Success || resp.AppliedThrough != 0 || !strings.Contains(resp.Error, "sequence 1") {
		t.Fatalf("setup: the create was answered success=%t applied_through=%d error=%q; want it refused as another incarnation",
			resp.Success, resp.AppliedThrough, resp.Error)
	}

	restarted := crashedKeepingTheStore(t, dataDir, kv, s)
	requireDivergenceStallsAt(t, restarted, 1, refusalAnotherIncarnation, frame)
	if _, held, err := restarted.s.openBucketIdentity("app", "held"); err != nil || held != live {
		t.Fatalf("restarted, app/held is incarnation %d (%v), want the %d it held", held, err, live)
	}
}

// TestACleanRunAddsNoSyncedWriteAndADivergenceOne: logging the owner's outcome
// rides in the log entry, so a frame of clean entries costs the store no
// synced write and the log one fsync, as before (BUG-0521), and a divergence
// costs one synced write, its record.
func TestACleanRunAddsNoSyncedWriteAndADivergenceOne(t *testing.T) {
	_, kv, c, _ := newReplicaOnFaultStore(t)
	var fsyncs atomic.Int64
	c.s.wal.SetFsyncHook(func() { fsyncs.Add(1) })
	defer c.s.wal.SetFsyncHook(nil)

	// The first push of a new source is checkpointed once; the frames after it
	// are what the cost is measured on.
	warm := queuePush(t, 1, "warm")
	warm.OwnerOutcome = ownerOutcomeApplied
	if resp := pushToReplica(t, c, frameSyncSource, []ReplEntry{warm}); !resp.Success {
		t.Fatalf("setup: the first push was answered %+v", resp)
	}
	fsyncs.Store(0)

	const clean = 16
	frame := make([]ReplEntry, 0, clean)
	for seq := uint64(2); seq <= clean+1; seq++ {
		push := queuePush(t, seq, fmt.Sprintf("e%d", seq))
		push.OwnerOutcome = ownerOutcomeApplied
		frame = append(frame, push)
	}
	syncsBefore := kv.Syncs()
	if resp := pushToReplica(t, c, frameSyncSource, frame); !resp.Success || resp.AppliedThrough != clean+1 {
		t.Fatalf("setup: the clean frame was answered success=%t applied_through=%d error=%q", resp.Success, resp.AppliedThrough, resp.Error)
	}
	if added := kv.Syncs() - syncsBefore; added != 0 {
		t.Errorf("a frame of %d clean entries added %d synced writes to the store, want none", clean, added)
	}
	if got := fsyncs.Load(); got != 1 {
		t.Errorf("a frame of %d clean entries cost the log %d fsyncs, want 1", clean, got)
	}

	syncsBefore = kv.Syncs()
	if resp := pushToReplica(t, c, frameSyncSource, []ReplEntry{updateOfMissing(t, clean+2, ownerOutcomeApplied)}); resp.Success {
		t.Fatalf("setup: the diverged entry was answered success: %+v", resp)
	}
	if added := kv.Syncs() - syncsBefore; added != 1 {
		t.Errorf("a divergence added %d synced writes to the store, want its record's one", added)
	}
}

// Replay refuses entries that applied when they were live for reasons that
// are no divergence: a later entry the store already holds changed what the
// handler reads, the bucket has gone since, or the source has gone back on the
// log they came from. None of them may be recorded as diverged; each would
// stall the stream on a copy that agrees with its owner.

// ownerApplied is entry with the owner's outcome applied.
func ownerApplied(entry ReplEntry) ReplEntry {
	entry.OwnerOutcome = ownerOutcomeApplied
	return entry
}

// requireNoDivergenceAfterRestart requires restarted to record no divergence
// and to count source's entries through seq applied.
func requireNoDivergenceAfterRestart(t *testing.T, restarted *Cluster, seq uint64) {
	t.Helper()
	if record, recorded := restarted.divergenceFrom(frameSyncSource); recorded {
		t.Fatalf("restarted, the replica records a divergence from %s, %+v, though it applied what its owner applied", frameSyncSource, record)
	}
	if restarted.shouldApply(frameSyncSource, seq) {
		t.Fatalf("restarted, the replica no longer counts %s's entry at %d applied (position %d)", frameSyncSource, seq, appliedSnapshot(restarted)[frameSyncSource])
	}
}

// TestAReplayedUpdateOfAKeyDeletedSinceIsNoDivergence: a put, an update and a
// delete of one key, all applied. Replayed, the update finds the key gone, as
// the delete left the store, and refuses: it is at or below the bucket's
// applied mark, so it applied when live.
func TestAReplayedUpdateOfAKeyDeletedSinceIsNoDivergence(t *testing.T) {
	s, kv, c, dataDir := newReplicaOnFaultStore(t)
	update, err := bson.Marshal(UpdateRequest{Database: "app", Bucket: "held", Key: "k", Set: map[string]interface{}{"n": 2}})
	if err != nil {
		t.Fatalf("encoding the update: %v", err)
	}
	remove, err := bson.Marshal(DeleteRequest{Database: "app", Bucket: "held", Key: "k"})
	if err != nil {
		t.Fatalf("encoding the delete: %v", err)
	}
	for _, entry := range []ReplEntry{
		{Seq: 1, Op: int32(transport.OpPut), Bucket: "held", Key: []byte("k"), Payload: putPayload(t, "app", "held", "k")},
		{Seq: 2, Op: int32(transport.OpUpdate), Bucket: "held", Key: []byte("k"), Payload: update},
		{Seq: 3, Op: int32(transport.OpDelete), Bucket: "held", Key: []byte("k"), Payload: remove},
	} {
		if resp := pushToReplica(t, c, frameSyncSource, []ReplEntry{ownerApplied(entry)}); !resp.Success || resp.AppliedThrough != entry.Seq {
			t.Fatalf("setup: the entry at %d was answered success=%t applied_through=%d error=%q", entry.Seq, resp.Success, resp.AppliedThrough, resp.Error)
		}
	}
	requireNoDivergenceAfterRestart(t, crashedKeepingTheStore(t, dataDir, kv, s), 3)
}

// TestAReplayedWriteToABucketDroppedSinceIsNoDivergence: a put applied to
// app/held, which is then dropped here. Replayed, the put finds no bucket and
// refuses; the bucket is not held, so no divergence is judged.
func TestAReplayedWriteToABucketDroppedSinceIsNoDivergence(t *testing.T) {
	s, kv, c, dataDir := newReplicaOnFaultStore(t)
	put := ownerApplied(ReplEntry{Seq: 1, Op: int32(transport.OpPut), Bucket: "held", Key: []byte("k"), Payload: putPayload(t, "app", "held", "k")})
	if resp := pushToReplica(t, c, frameSyncSource, []ReplEntry{put}); !resp.Success || resp.AppliedThrough != 1 {
		t.Fatalf("setup: the put was answered success=%t applied_through=%d error=%q", resp.Success, resp.AppliedThrough, resp.Error)
	}
	if resp, err := s.DeleteBucket(DeleteBucketRequest{Database: "app", Name: "held"}); err != nil || !resp.Success {
		t.Fatalf("setup: dropping app/held: %+v, %v", resp, err)
	}
	requireNoDivergenceAfterRestart(t, crashedKeepingTheStore(t, dataDir, kv, s), 1)
}

// TestAReplayedEntryOfALogItsSourceWentBackOnIsNoDivergence: the handler
// refused an entry its owner applied and the record was lost, but the source
// has since gone back on that log (its rebased_at is past the entry), so the
// entry is no longer the source's to judge.
func TestAReplayedEntryOfALogItsSourceWentBackOnIsNoDivergence(t *testing.T) {
	s, kv, c, dataDir := newReplicaOnFaultStore(t)
	failTheStopsRecords(t, c)
	if resp := pushToReplica(t, c, frameSyncSource, []ReplEntry{updateOfMissing(t, 1, ownerOutcomeApplied)}); resp.Success {
		t.Fatalf("setup: the update was answered success: %+v", resp)
	}
	c.appliedMu.Lock()
	c.rebasedAt[frameSyncSource] = c.s.wal.LastSequence()
	c.appliedMu.Unlock()
	if err := c.checkpointPosition(frameSyncSource, true); err != nil {
		t.Fatalf("setup: checkpointing the rebase: %v", err)
	}
	restarted := crashedKeepingTheStore(t, dataDir, kv, s)
	if record, recorded := restarted.divergenceFrom(frameSyncSource); recorded {
		t.Fatalf("restarted, the replica records a divergence from %s, %+v, on an entry of a log the source went back on", frameSyncSource, record)
	}
}

// TestARestartWithAStandingDivergenceDoesNotWriteItAgain: a divergence the
// live apply recorded, on an entry nothing was logged after, so nothing voided
// it. Each restart replays the refused entry and judges it diverged again; the
// record stands, so the replay must not write it a second time, or every
// restart of a stalled replica costs a synced write.
func TestARestartWithAStandingDivergenceDoesNotWriteItAgain(t *testing.T) {
	s, kv, c, dataDir := newReplicaOnFaultStore(t)
	if resp := pushToReplica(t, c, frameSyncSource, []ReplEntry{updateOfMissing(t, 1, ownerOutcomeApplied)}); resp.Success {
		t.Fatalf("setup: the update was answered success: %+v", resp)
	}
	if _, recorded := c.divergenceFrom(frameSyncSource); !recorded {
		t.Fatal("setup: the live apply did not record the divergence")
	}
	if err := kv.Sync(); err != nil {
		t.Fatalf("syncing the store as a process crash keeps it: %v", err)
	}
	restarted := killedAfterPowerLoss(t, dataDir, kv, s)
	syncsBefore := kv.Syncs()
	if err := restarted.s.replayWAL(context.Background()); err != nil {
		t.Fatalf("replaying the log: %v", err)
	}
	if added := kv.Syncs() - syncsBefore; added != 0 {
		t.Errorf("the restart's replay added %d synced writes, want none: the divergence's record already stands", added)
	}
	if record, recorded := restarted.divergenceFrom(frameSyncSource); !recorded || record.Seq != 1 {
		t.Fatalf("restarted, the divergence is %+v (recorded %t), want the one at 1", record, recorded)
	}
}

// TestAReplayedEntryWhoseStoreFailsIsNoDivergence: a push applied live whose
// store commit a power cut took, replayed while the store fails. The failure
// is owed to a later replay, not a refusal of the owner's write, so it must
// not be recorded as diverged.
func TestAReplayedEntryWhoseStoreFailsIsNoDivergence(t *testing.T) {
	s, kv, c, dataDir := newReplicaOnFaultStore(t)
	push := ownerApplied(queuePush(t, 1, "a"))
	if resp := pushToReplica(t, c, frameSyncSource, []ReplEntry{push}); !resp.Success || resp.AppliedThrough != 1 {
		t.Fatalf("setup: the push was answered success=%t applied_through=%d error=%q", resp.Success, resp.AppliedThrough, resp.Error)
	}
	restarted := killedAfterPowerLoss(t, dataDir, kv, s)
	if order := appQueueOrder(t, restarted.s); len(order) != 0 {
		t.Fatalf("setup: the power cut left the queue as %v; want the push's commit gone, for replay to apply", order)
	}
	var failed atomic.Int64
	hook := func(storage.KV, uint16, uint64, uint64) error {
		failed.Add(1)
		return errors.New("injected store failure during replay")
	}
	store.ApplyFaultHook.Store(&hook)
	err := restarted.s.replayWAL(context.Background())
	store.ApplyFaultHook.Store(nil)
	if err != nil {
		t.Fatalf("replaying the log: %v", err)
	}
	if failed.Load() == 0 {
		t.Fatal("setup: replay never reached the store")
	}
	if record, recorded := restarted.divergenceFrom(frameSyncSource); recorded {
		t.Fatalf("restarted, the replica records a divergence from %s, %+v, for a replay its store failed", frameSyncSource, record)
	}
}

// TestAReplayedDivergenceTheStreamMovedPastIsNotRecordedAgain: a divergence
// recorded live and settled by a later entry of its source, in another
// bucket, that applied. Replayed, the refused entry is refused again, above its
// bucket's mark; the source's later entry is in the store, so the stream moved past it, and the divergence must not be
// recorded again nor the position lowered below what was acknowledged.
func TestAReplayedDivergenceTheStreamMovedPastIsNotRecordedAgain(t *testing.T) {
	s, kv, c, dataDir := newReplicaOnFaultStore(t)
	if resp := pushToReplica(t, c, frameSyncSource, []ReplEntry{updateOfMissing(t, 1, ownerOutcomeApplied)}); resp.Success {
		t.Fatalf("setup: the update was answered success: %+v", resp)
	}
	if _, recorded := c.divergenceFrom(frameSyncSource); !recorded {
		t.Fatal("setup: the live apply did not record the divergence")
	}
	// Another bucket's entry, as when the stall ends and the source writes on:
	// the refused entry stays above its own bucket's mark.
	if resp := pushToReplica(t, c, frameSyncSource, []ReplEntry{ownerApplied(queuePush(t, 2, "a"))}); !resp.Success || resp.AppliedThrough != 2 {
		t.Fatalf("setup: the push past the divergence was answered success=%t applied_through=%d error=%q", resp.Success, resp.AppliedThrough, resp.Error)
	}
	if _, recorded := c.divergenceFrom(frameSyncSource); recorded {
		t.Fatal("setup: the push past the divergence did not settle it")
	}
	requireNoDivergenceAfterRestart(t, crashedKeepingTheStore(t, dataDir, kv, s), 2)
}
