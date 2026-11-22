package server

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
	dto "github.com/prometheus/client_model/go"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Spec §4.4 rule 5 (rev 5.2) and the build doc's SA-27 and SA-28: a write
// whose store apply fails is recorded, retried by replay, kept in the log until
// then, and applied beneath the mark only where that gives the replicas' result.

var errInjectedStoreFailure = errors.New("injected store failure")

// failNextApplyOn makes the store fail, once, the commit of the first apply on
// kv that match accepts — inside store.Apply, where the carry runs (build doc
// §1 trap 4). It returns the sequence it failed, 0 until it has fired.
func failNextApplyOn(t *testing.T, kv storage.KV, match func(seq uint64) bool) *atomic.Uint64 {
	t.Helper()
	var failedSeq atomic.Uint64
	var fired atomic.Bool
	hook := func(target storage.KV, _ uint16, _ uint64, seq uint64) error {
		if target != kv || !match(seq) || !fired.CompareAndSwap(false, true) {
			return nil
		}
		failedSeq.Store(seq)
		return errInjectedStoreFailure
	}
	store.ApplyFaultHook.Store(&hook)
	t.Cleanup(func() { store.ApplyFaultHook.Store(nil) })
	return &failedSeq
}

// anySeq matches every apply.
func anySeq(uint64) bool { return true }

// putValue writes key=value to testBucket through node index.
func putValue(t *testing.T, tc *testCluster, index int, key, value, wc string) *transport.Frame {
	t.Helper()
	return tc.frame(index, transport.OpPut, InsertRequest{
		Database:     testDatabase,
		Bucket:       testBucket,
		Key:          key,
		Data:         map[string]interface{}{"v": value},
		WriteConcern: wc,
	})
}

// valueOnNode reads field "v" of key from node index's own copy.
func valueOnNode(t *testing.T, tc *testCluster, index int, key string) (string, bool) {
	t.Helper()
	frame := tc.frame(index, transport.OpGet, FindRequest{Database: testDatabase, Bucket: testBucket, Key: key})
	if frame.Opcode == transport.OpError {
		t.Fatalf("reading %q from node %d: %s", key, index+1, string(frame.Payload))
	}
	var resp FindResponse
	if err := bson.Unmarshal(frame.Payload, &resp); err != nil {
		t.Fatalf("decoding the read of %q (opcode %s, %d bytes): %v", key, frame.Opcode, len(frame.Payload), err)
	}
	if !resp.Found {
		return "", false
	}
	var doc struct {
		V string `bson:"v"`
	}
	if err := bson.Unmarshal(resp.Data, &doc); err != nil {
		t.Fatalf("decoding %q: %v", key, err)
	}
	return doc.V, true
}

// TestReplicaDoesNotRecordItsFailedApply is §1 trap 4 (d): a replica's live
// apply records nothing. Its retry is the owner's redelivery, which it applies
// under a new local sequence; recording the first attempt too would make replay
// apply the write a second time.
func TestReplicaDoesNotRecordItsFailedApply(t *testing.T) {
	tc := newTestCluster(t, 2, 2)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	owner, err := tc.nodes[0].server.core.cluster.Owner(placementOf(testDatabase, testBucket))
	if err != nil {
		t.Fatalf("owner: %v", err)
	}
	ownerIndex := nodeIndexByAddr(t, tc.addrs, owner.ID)
	replicaIndex := 1 - ownerIndex

	failed := failNextApplyOn(t, tc.nodes[replicaIndex].server.core.kv, anySeq)
	if frame := putValue(t, tc, ownerIndex, "x", "x1", "majority"); frame.Opcode == transport.OpError {
		t.Fatalf("write X: %s", string(frame.Payload))
	}
	if failed.Load() == 0 {
		t.Fatal("the replica's apply of X never reached the store; the test failed nothing")
	}
	if frame := putValue(t, tc, ownerIndex, "y", "y1", "majority"); frame.Opcode == transport.OpError {
		t.Fatalf("write Y: %s", string(frame.Payload))
	}

	if st := bucketStateOf(t, tc.nodes[replicaIndex].server.core, testDatabase, testBucket); len(st.FailedSeqs) != 0 {
		t.Fatalf("the replica recorded its failed apply: failed_seqs = %v; the owner re-sent X, so replay would apply it twice", st.FailedSeqs)
	}
	// Since D-2 a replica whose apply failed is not in sync, so `majority`
	// was answered by the owner alone; the owner's redelivery brings X
	// (re-aimed by replication-stream step 7.6).
	pollUntil(t, "the owner's redelivery of X reaches the replica", func() bool {
		return holdsKeyLocallyIn(tc.nodes[replicaIndex], testBucket, "x")
	})
	if st := bucketStateOf(t, tc.nodes[replicaIndex].server.core, testDatabase, testBucket); len(st.FailedSeqs) != 0 {
		t.Fatalf("the replica recorded its failed apply: failed_seqs = %v; the owner re-sent X, so replay would apply it twice", st.FailedSeqs)
	}
	tc.restartNode(replicaIndex)
	if _, found := valueOnNode(t, tc, replicaIndex, "x"); !found {
		t.Fatal("X is missing on the replica after its restart")
	}
	if st := bucketStateOf(t, tc.nodes[replicaIndex].server.core, testDatabase, testBucket); len(st.FailedSeqs) != 0 {
		t.Fatalf("after the restart the replica's failed_seqs = %v, want none", st.FailedSeqs)
	}
}

// TestOwedApplyHoldsWALRetention is SA-28: the replicas acknowledge an owed
// write, so retention would prune the only copy of it before the restart that
// retries it; the owed registry holds the bound below it until then.
func TestOwedApplyHoldsWALRetention(t *testing.T) {
	tc, ownerIndex := applyOrderCluster(t)
	defer tc.stop()
	ownerCore := tc.nodes[ownerIndex].server.core

	failed := failNextApplyOn(t, ownerCore.kv, anySeq)
	putValue(t, tc, ownerIndex, "a", "a1", "majority")
	seqA := failed.Load()
	if seqA == 0 {
		t.Fatal("A's apply never reached the store; the test failed nothing")
	}
	if frame := putValue(t, tc, ownerIndex, "b", "b1", "majority"); frame.Opcode == transport.OpError {
		t.Fatalf("write B: %s", string(frame.Payload))
	}
	seqB := ownerCore.wal.LastSequence()

	if bound := ownerCore.cluster.retentionBound(); bound >= seqA {
		t.Fatalf("retention may prune to %d while this node still owes A at %d: the only copy would go", bound, seqA)
	}
	if err := ownerCore.cluster.pruneWAL(); err != nil {
		t.Fatalf("retention pass: %v", err)
	}

	restarted := tc.restartNode(ownerIndex).server.core
	if _, found := valueOnNode(t, tc, ownerIndex, "a"); !found {
		t.Fatalf("after the restart A (seq %d) is missing: replay did not retry it", seqA)
	}
	if n := restarted.owed.count(); n != 0 {
		t.Fatalf("after the restart the registry owes %d sequences, want none", n)
	}
	// The bound is over every member's cursor; the other node hosts nothing
	// of this one's at factor 1, so its cursor moves only by an advance.
	advanceEveryStream(t, restarted.cluster)
	if bound := restarted.cluster.retentionBound(); bound < seqB {
		t.Fatalf("after the retry the retention bound is %d, still held below B's %d", bound, seqB)
	}
}

// TestReplayDoesNotApplyAFailedPutBeneathALaterPutOfTheSameKey is SA-27, the
// test that fails on rule 5 as the owner first took it: a failed put of v1
// replayed over an applied put of v2 would leave v1 where every replica has v2.
// Replay takes the loud path instead: nothing applied, the record settled, the
// counter moved.
func TestReplayDoesNotApplyAFailedPutBeneathALaterPutOfTheSameKey(t *testing.T) {
	tc, ownerIndex := applyOrderCluster(t)
	defer tc.stop()

	failed := failNextApplyOn(t, tc.nodes[ownerIndex].server.core.kv, anySeq)
	putValue(t, tc, ownerIndex, "k", "v1", "majority")
	if failed.Load() == 0 {
		t.Fatal("A's apply never reached the store; the test failed nothing")
	}
	if frame := putValue(t, tc, ownerIndex, "k", "v2", "majority"); frame.Opcode == transport.OpError {
		t.Fatalf("write B: %s", string(frame.Payload))
	}
	if st := bucketStateOf(t, tc.nodes[ownerIndex].server.core, testDatabase, testBucket); !reflect.DeepEqual(st.FailedSeqs, []uint64{failed.Load()}) {
		t.Fatalf("before the restart failed_seqs = %v, want [%d]: the test did not build the recorded failure", st.FailedSeqs, failed.Load())
	}

	before := notReplayedCount(t)
	restarted := tc.restartNode(ownerIndex).server.core
	if value, found := valueOnNode(t, tc, ownerIndex, "k"); !found || value != "v2" {
		t.Fatalf("after replay k = %q (found %v), want v2: the failed put was applied over the later one", value, found)
	}
	if st := bucketStateOf(t, restarted, testDatabase, testBucket); len(st.FailedSeqs) != 0 {
		t.Fatalf("after replay failed_seqs = %v, want none: the loud path must settle the record", st.FailedSeqs)
	}
	if moved := notReplayedCount(t) - before; moved != 1 {
		t.Fatalf("hoarddb_apply_failures_not_replayed_total moved by %v, want 1", moved)
	}
}

// waitForSequence waits until w's last sequence is above seq.
func waitForSequence(t *testing.T, w *storage.WAL, seq uint64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for w.LastSequence() <= seq {
		if ctx.Err() != nil {
			t.Fatalf("the WAL never passed sequence %d", seq)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// notReplayedCount reads hoarddb_apply_failures_not_replayed_total.
func notReplayedCount(t *testing.T) float64 {
	t.Helper()
	var metric dto.Metric
	if err := applyFailuresNotReplayed.Write(&metric); err != nil {
		t.Fatalf("reading the not-replayed counter: %v", err)
	}
	return metric.GetCounter().GetValue()
}
