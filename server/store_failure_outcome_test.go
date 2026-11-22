package server

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// storeFailureAnswer is one data write whose handler answers a store failure
// on the owner with success false, not with an error frame.
type storeFailureAnswer struct {
	what    string
	op      transport.Opcode
	request func(via int) any
	// response decodes the answer, so a typed client is shown to keep the
	// outcome too.
	response func() any
}

// TestAStoreFailureAnsweredSuccessFalseSaysUnknown is the acceptance
// test for store-failure outcomes. When the owner's own store fails to apply a logged write, the entry is
// still shipped, the replicas apply it, and the owner owes it and applies it on
// replay (owed.go:noteApplyOutcome). A put, a delete, a FIFO push, a field
// push and a batch put answer that failure as a response with success false.
// Each must say write_outcome unknown and carry its copy count — through the
// owner and through a node that forwards to it — or a client retries a push
// that is already stored and duplicates it.
func TestAStoreFailureAnsweredSuccessFalseSaysUnknown(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	ownerIndex, _ := ownerAndReplica(t, tc)
	queue := bucketOwnedBy(t, tc, ownerIndex, "queue")
	tc.ok(ownerIndex, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: queue, Type: BucketTypeFIFO})
	posts := bucketOwnedBy(t, tc, ownerIndex, "posts")
	createFieldBucket(t, tc, ownerIndex, posts)
	waitForServing(t, tc, testDatabase, queue, 3)
	waitForServing(t, tc, testDatabase, posts, 3)
	ownerKV := tc.nodes[ownerIndex].server.core.kv

	answers := []storeFailureAnswer{
		{"a put", transport.OpPut, func(via int) any {
			return testBucketPut(fmt.Sprintf("put-%d", via), "all")
		}, func() any { return &InsertResponse{} }},
		{"a delete", transport.OpDelete, func(via int) any {
			key := fmt.Sprintf("doomed-%d", via)
			tc.ok(ownerIndex, transport.OpPut, testBucketPut(key, "all"))
			return DeleteRequest{Database: testDatabase, Bucket: testBucket, Key: key}
		}, func() any { return &DeleteResponse{} }},
		{"a FIFO push", transport.OpFIFOPush, func(via int) any {
			return PushRequest{Database: testDatabase, Bucket: queue, Data: map[string]interface{}{"via": via}, WriteConcern: "all"}
		}, func() any { return &PushResponse{} }},
		{"a field push", transport.OpFieldPush, func(via int) any {
			return FieldRequest{Database: testDatabase, Bucket: posts, Key: "p1", Field: "Comments",
				Data: map[string]interface{}{"via": via}, WriteConcern: "all"}
		}, func() any { return &FieldResponse{} }},
		{"a batch put", transport.OpBatchPut, func(via int) any {
			return BatchInsertRequest{Database: testDatabase, Bucket: testBucket, WriteConcern: "all",
				Data: []map[string]interface{}{{"_id_": fmt.Sprintf("batch-a-%d", via)}, {"_id_": fmt.Sprintf("batch-b-%d", via)}}}
		}, func() any { return &BatchInsertResponse{} }},
	}

	for _, answer := range answers {
		for _, via := range []int{ownerIndex, (ownerIndex + 1) % 3} {
			what := fmt.Sprintf("%s with the owner's store failing, through node %d", answer.what, via+1)
			request := answer.request(via)
			failed := failNextApplyOn(t, ownerKV, anySeq)
			frame := tc.ok(via, answer.op, request)
			if failed.Load() == 0 {
				t.Fatalf("test setup: %s failed no apply", what)
			}
			raw := bson.Raw(frame.Payload)
			if success, ok := raw.Lookup("success").BooleanOK(); !ok || success {
				t.Fatalf("test setup: %s was answered without success false, so it is not this bug's class: %s", what, raw)
			}
			outcome, _ := raw.Lookup("write_outcome").StringValueOK()
			if outcome != outcomeUnknown {
				t.Errorf("%s was answered write_outcome %q; want %q: the entry is logged, shipped and owed: %s", what, outcome, outcomeUnknown, raw)
			}
			confirmed, factor, hasConfirmed, hasFactor := copiesConfirmedOf(frame.Payload)
			if !hasConfirmed || !hasFactor {
				t.Errorf("%s was answered without copies_confirmed or replication_factor: %s", what, raw)
			}
			// Spec §19.4 P3: a count of N means at least N nodes hold the
			// write. The owner does not hold it until a replay, so only the
			// replicas' acknowledgements count.
			if confirmed > factor-1 {
				t.Errorf("%s was answered copies_confirmed %d of %d: that counts the owner, whose store failed the write", what, confirmed, factor)
			}
			typed := answer.response()
			if err := bson.Unmarshal(frame.Payload, typed); err != nil {
				t.Fatalf("%s: decoding the answer into %T: %v", what, typed, err)
			}
			again, err := bson.Marshal(typed)
			if err != nil {
				t.Fatalf("%s: encoding %T: %v", what, typed, err)
			}
			if kept, _ := bson.Raw(again).Lookup("write_outcome").StringValueOK(); kept != outcome {
				t.Errorf("%T does not declare write_outcome: a typed client loses %q", typed, outcome)
			}
		}
	}
}

// refuseNextCommitAsTooLarge makes the next apply on kv fail its commit with
// storage.ErrBatchTooLarge, as a store whose memtable is too small for the
// batch does. store.ApplyFaultHook's error goes through the same marking as a
// real commit's, so the store takes it as the operation's refusal. A real one
// needs a field operation of some 52,000 entries at the memtable floor: a
// single element large enough goes to the value log and fits.
func refuseNextCommitAsTooLarge(t *testing.T, kv storage.KV) *atomic.Bool {
	t.Helper()
	var fired atomic.Bool
	hook := func(target storage.KV, _ uint16, _ uint64, _ uint64) error {
		if target != kv || !fired.CompareAndSwap(false, true) {
			return nil
		}
		return fmt.Errorf("%w: injected for a smaller memtable", storage.ErrBatchTooLarge)
	}
	store.ApplyFaultHook.Store(&hook)
	t.Cleanup(func() { store.ApplyFaultHook.Store(nil) })
	return &fired
}

// TestAFieldWriteTooLargeForTheOwnerSaysUnknown: a field write the owner's
// store refuses as too large for one batch (storage.ErrBatchTooLarge) is
// answered success false. The bound comes from each node's memtable, which
// follows its host's memory, so a replica with a larger one applies the entry
// the owner refused (a mixed cluster). The answer must say unknown,
// never leave a client to read success false as nothing stored.
func TestAFieldWriteTooLargeForTheOwnerSaysUnknown(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	ownerIndex, _ := ownerAndReplica(t, tc)
	posts := bucketOwnedBy(t, tc, ownerIndex, "posts")
	createFieldBucket(t, tc, ownerIndex, posts)
	waitForServing(t, tc, testDatabase, posts, 3)

	for _, via := range []int{ownerIndex, (ownerIndex + 1) % 3} {
		what := fmt.Sprintf("a field push the owner refused as too large for its batch, through node %d", via+1)
		refused := refuseNextCommitAsTooLarge(t, tc.nodes[ownerIndex].server.core.kv)
		frame := tc.ok(via, transport.OpFieldPush, FieldRequest{Database: testDatabase, Bucket: posts, Key: "p1", Field: "Comments",
			WriteConcern: "all", Data: map[string]interface{}{"via": via}})
		if !refused.Load() {
			t.Fatalf("test setup: %s refused no commit", what)
		}
		raw := bson.Raw(frame.Payload)
		if message, _ := raw.Lookup("message").StringValueOK(); !strings.Contains(message, storage.ErrBatchTooLarge.Error()) {
			t.Fatalf("test setup: %s was not answered with the batch bound: %s", what, raw)
		}
		if outcome, _ := raw.Lookup("write_outcome").StringValueOK(); outcome != outcomeUnknown {
			t.Errorf("%s was answered write_outcome %q; want %q: a replica with a larger memtable applies it: %s", what, outcome, outcomeUnknown, raw)
		}
	}
}

// TestAnErrorFrameForAWriteTooLargeForTheOwnerSaysUnknown is the error-frame
// half of the too-large case: an update and a blob write answer the owner's
// batch-bound refusal as an error frame, not success false, and that frame
// said not_applied. A replica with a larger memtable applies the entry, so it
// must say unknown.
func TestAnErrorFrameForAWriteTooLargeForTheOwnerSaysUnknown(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	ownerIndex, _ := ownerAndReplica(t, tc)
	blobs := bucketOwnedBy(t, tc, ownerIndex, "blobs")
	tc.ok(ownerIndex, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: blobs, Type: BucketTypeBlob})
	waitForServing(t, tc, testDatabase, blobs, 3)
	tc.ok(ownerIndex, transport.OpPut, testBucketPut("doc", "all"))
	ownerKV := tc.nodes[ownerIndex].server.core.kv

	for _, via := range []int{ownerIndex, (ownerIndex + 1) % 3} {
		for _, write := range []struct {
			what    string
			op      transport.Opcode
			request any
		}{
			{"a $set", transport.OpUpdate, UpdateRequest{Database: testDatabase, Bucket: testBucket, Key: "doc",
				Set: map[string]interface{}{"n": via}}},
			{"a blob write", transport.OpBlobWrite, BlobWriteRequest{Database: testDatabase, Bucket: blobs,
				Key: fmt.Sprintf("b%d", via), Data: []byte("bytes")}},
		} {
			what := fmt.Sprintf("%s the owner refused as too large for its batch, through node %d", write.what, via+1)
			refused := refuseNextCommitAsTooLarge(t, ownerKV)
			refusal := errorAnswerOf(t, tc.frame(via, write.op, write.request), what)
			if !refused.Load() {
				t.Fatalf("test setup: %s refused no commit", what)
			}
			if !strings.Contains(refusal.Message, storage.ErrBatchTooLarge.Error()) {
				t.Fatalf("test setup: %s was not answered with the batch bound: %s", what, refusal.Message)
			}
			if refusal.WriteOutcome != outcomeUnknown {
				t.Errorf("%s was answered code %d, write_outcome %q; want %q: a replica with a larger memtable applies it: %s",
					what, refusal.Code, refusal.WriteOutcome, outcomeUnknown, refusal.Message)
			}
		}
	}
}

// TestAStoreFailureWhoseConcernTimesOutSaysUnknown is the timeout half of
// the same rule. A write answered success false for the owner's store failure,
// whose write concern then times out, was answered as the timeout of a write
// "stored on the owner" (outcome applied): the owner does not hold it until a
// replay applies it. The answer must say unknown. The setup is T-108's
// write-concern timeout: a held replica stays in sync and confirms nothing.
func TestAStoreFailureWhoseConcernTimesOutSaysUnknown(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	ownerIndex, heldIndex := ownerAndReplica(t, tc)
	otherIndex := 3 - ownerIndex - heldIndex
	ownerCluster := tc.nodes[ownerIndex].server.core.cluster
	unfreeze := freezePeer(ownerCluster.peerFor(mustNode(t, ownerCluster, tc.addrs[heldIndex])))
	for i := 0; i < 5; i++ {
		putDocumentAt(tc, ownerIndex, testDatabase, testBucket, fmt.Sprintf("behind-%d", i), "one")
	}
	heldID := tc.addrs[heldIndex]
	holding, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	hold := func(peerID string, _ []ReplEntry) {
		if peerID != heldID {
			return
		}
		once.Do(func() { close(holding) })
		<-release
	}
	beforeReplayShipHook.Store(&hold)
	defer beforeReplayShipHook.Store(nil)
	defer close(release)
	defer unfreeze()
	restarted := tc.restartNode(ownerIndex)
	select {
	case <-holding:
	case <-time.After(30 * time.Second):
		t.Fatal("test setup: the restarted owner never replayed its log to the held replica")
	}
	ownerCluster = restarted.server.core.cluster
	toOther := ownerCluster.peerFor(mustNode(t, ownerCluster, tc.addrs[otherIndex]))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := catchUpPeer(ctx, toOther); err != nil {
		t.Fatalf("test setup: %v", err)
	}
	if state, _ := ownerCluster.peerFor(mustNode(t, ownerCluster, heldID)).streamStatus(); state != streamConnecting {
		t.Fatalf("test setup: the held replica's stream is %s, want connecting", state)
	}

	failed := failNextApplyOn(t, restarted.server.core.kv, anySeq)
	what := "an all write whose owner-side apply failed and which the held replica cannot confirm"
	refusal := errorAnswerOf(t, tc.frame(ownerIndex, transport.OpPut, testBucketPut("failed-and-timed-out", "all")), what)
	if failed.Load() == 0 {
		t.Fatalf("test setup: %s failed no apply", what)
	}
	if refusal.WriteOutcome != outcomeUnknown {
		t.Errorf("%s was answered code %d, write_outcome %q; want %q: the owner does not hold it until a replay: %s",
			what, refusal.Code, refusal.WriteOutcome, outcomeUnknown, refusal.Message)
	}
}
