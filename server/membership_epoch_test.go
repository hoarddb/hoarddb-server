package server

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Dispatch 6 of the replication stream: one epoch per exchange (spec §19.3.6
// R-1 and R-2, §19.3.7).

// sendReplPush hands a replication push to a node's handler as the wire
// would, and returns its answer.
func sendReplPush(t *testing.T, receiver *Cluster, push ReplPushRequest) ReplPushResponse {
	t.Helper()
	encoded, err := bson.Marshal(push)
	if err != nil {
		t.Fatalf("marshal replication push: %v", err)
	}
	respFrame, err := handleReplPush(context.Background(), transport.NewFrame(transport.OpReplPush, encoded), receiver.s)
	if err != nil {
		t.Fatalf("replication push: %v", err)
	}
	var resp ReplPushResponse
	if err := bson.Unmarshal(respFrame.Payload, &resp); err != nil {
		t.Fatalf("decode replication ack: %v", err)
	}
	return resp
}

// queueOfThreeAtAll creates the FIFO test bucket on three nodes at factor 3
// and pushes three elements at `all`. It returns the owner's index and the
// indexes of the two other nodes.
func queueOfThreeAtAll(t *testing.T, tc *testCluster) (owner, replica, third int) {
	t.Helper()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeFIFO})
	waitForServing(t, tc, testDatabase, testBucket, 3)
	for n := 1; n <= 3; n++ {
		tc.ok(0, transport.OpFIFOPush, PushRequest{Database: testDatabase, Bucket: testBucket,
			Data: map[string]interface{}{"n": n}, WriteConcern: "all"})
	}
	owner = ownerIndexOf(t, tc, testBucket)
	return owner, (owner + 1) % 3, (owner + 2) % 3
}

// requireCountsOf fails the test unless each node's own copy of the test
// bucket holds want elements.
func requireCountsOf(t *testing.T, tc *testCluster, want int64, when string) {
	t.Helper()
	for index := range tc.nodes {
		if got := tc.localCount(index); got != want {
			t.Errorf("%s: node %d holds %d elements of the queue, want %d", when, index+1, got, want)
		}
	}
}

// TestAForwardedWriteIsRefusedByANodeThatIsNotTheOwner is T-98, spec §19.12
// probe 3 (W2): a forwarded push that reaches a replica is refused
// "not_owner", and nothing is logged or applied there. On the starting tree it
// was answered success=true, the replica's log grew by one, and the holders
// counted 4, 3 and 3. Guard: main's `owned` test, which sent every request for
// a bucket this node does not own to applyAsReplica.
func TestAForwardedWriteIsRefusedByANodeThatIsNotTheOwner(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	_, replica, third := queueOfThreeAtAll(t, tc)
	replicaCluster := tc.nodes[replica].server.core.cluster
	logged := replicaCluster.s.wal.LastSequence()

	resp := sendReplPush(t, replicaCluster, ReplPushRequest{
		Token:          replicaCluster.localToken(),
		FromNode:       tc.addrs[third],
		ReturnResponse: true,
		WriteConcern:   int(WcAll),
		Epoch:          replicaCluster.Epoch(),
		Entries: []ReplEntry{{Op: int32(transport.OpFIFOPush), Bucket: testBucket,
			Payload: requestBody(t, PushRequest{Database: testDatabase, Bucket: testBucket, Data: map[string]interface{}{"n": 4}})}},
	})
	if resp.Success || resp.Refusal != refusalNotOwner {
		t.Errorf("a forwarded push to a node that does not own the bucket was answered success=%v refusal=%q error=%q; want refused %q",
			resp.Success, resp.Refusal, resp.Error, refusalNotOwner)
	}
	if now := replicaCluster.s.wal.LastSequence(); now != logged {
		t.Errorf("the replica's log went from %d to %d for a write it does not own", logged, now)
	}
	requireCountsOf(t, tc, 3, "after a forward to a replica")

	// The coordinator's side: the refusal reaches the client as "the
	// membership is changing", unavailable, retry.
	thirdCluster := tc.nodes[third].server.core.cluster
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := thirdCluster.forward(ctx, mustNode(t, thirdCluster, tc.addrs[replica]), ReplEntry{Op: int32(transport.OpFIFOPush), Bucket: testBucket,
		Payload: requestBody(t, PushRequest{Database: testDatabase, Bucket: testBucket, Data: map[string]interface{}{"n": 5}})}, WcAll, readForward{})
	if !errors.Is(err, ErrNotOwner) || codeFor(err) != transport.ErrUnavailable {
		t.Errorf("a forward to a node that does not own the bucket returned %v (code %v); want ErrNotOwner, unavailable", err, codeFor(err))
	}
	requireCountsOf(t, tc, 3, "after a coordinator's forward to a replica")
}

// TestAPushOnAnotherEpochIsRefused is T-100's other half (spec §19.3.7): a
// push that changes state, from a node on another membership epoch, is
// refused "epoch" with the receiver's epoch and nothing logged — a stream
// push, a forwarded write and an advance notice alike — and a coordinator
// turns the refusal into ErrEpochMismatch, unavailable. Guard: no check in
// handleReplPush.
func TestAPushOnAnotherEpochIsRefused(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	owner, sender, third := queueOfThreeAtAll(t, tc)
	ownerCluster := tc.nodes[owner].server.core.cluster
	senderID := tc.addrs[sender]
	current := ownerCluster.Epoch()
	logged := ownerCluster.s.wal.LastSequence()
	push := func() []byte {
		return requestBody(t, PushRequest{Database: testDatabase, Bucket: testBucket, Data: map[string]interface{}{"n": 4}})
	}
	stream := streamOf(t, ownerCluster, senderID)
	seq := sourcePosition(tc.nodes[owner], senderID) + 1
	for name, request := range map[string]ReplPushRequest{
		"a stream push": {FromNode: senderID, Epoch: current + 1, Stream: stream,
			Entries: []ReplEntry{{Seq: seq, Op: int32(transport.OpFIFOPush), Bucket: testBucket, Payload: push()}}},
		"a forwarded write": {FromNode: tc.addrs[third], Epoch: current - 1, ReturnResponse: true,
			Entries: []ReplEntry{{Op: int32(transport.OpFIFOPush), Bucket: testBucket, Payload: push()}}},
		"an advance notice": {FromNode: senderID, Epoch: current + 1,
			Stream: &ReplStream{Incarnation: stream.Incarnation, AdvanceTo: seq + 100}},
	} {
		request.Token = ownerCluster.localToken()
		resp := sendReplPush(t, ownerCluster, request)
		if resp.Success || resp.Refusal != refusalEpoch || resp.Epoch != current {
			t.Errorf("%s on another epoch was answered success=%v refusal=%q epoch=%d; want refused %q with epoch %d",
				name, resp.Success, resp.Refusal, resp.Epoch, refusalEpoch, current)
		}
	}
	if now := ownerCluster.s.wal.LastSequence(); now != logged {
		t.Errorf("the owner's log went from %d to %d for pushes on another epoch", logged, now)
	}
	if position := sourcePosition(tc.nodes[owner], senderID); position >= seq {
		t.Errorf("the owner's position for %s moved to %d on pushes on another epoch", senderID, position)
	}
	requireCountsOf(t, tc, 3, "after pushes on another epoch")

	// What the coordinator answers its client with (A-6.5).
	if code := codeFor(fmt.Errorf("forwarding: %w", ErrEpochMismatch)); code != transport.ErrUnavailable {
		t.Errorf("ErrEpochMismatch reaches the wire as %v, want unavailable", code)
	}
}

// TestAStreamPushIsNeverReOriginated is T-99 (R-1, W3): a stream push that
// reaches the node that owns the bucket is applied as a replica — one
// `replica` entry in its log — and shipped to nobody. On the starting tree the
// owner took it through applyAsOwner, logged it as its own and shipped it to
// its set, the sender included. Guard: main's `owned` test.
func TestAStreamPushIsNeverReOriginated(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	owner, sender, third := queueOfThreeAtAll(t, tc)
	ownerCluster := tc.nodes[owner].server.core.cluster
	senderID := tc.addrs[sender]
	logged := ownerCluster.s.wal.LastSequence()
	stream := streamOf(t, ownerCluster, senderID)
	seq := sourcePosition(tc.nodes[owner], senderID) + 1

	resp := sendReplPush(t, ownerCluster, ReplPushRequest{
		Token:    ownerCluster.localToken(),
		FromNode: senderID,
		Epoch:    ownerCluster.Epoch(),
		Stream:   stream,
		Entries: []ReplEntry{{Seq: seq, Op: int32(transport.OpFIFOPush), Bucket: testBucket,
			Term:    bucketTermOn(t, ownerCluster.s, testDatabase, testBucket),
			Payload: requestBody(t, PushRequest{Database: testDatabase, Bucket: testBucket, Data: map[string]interface{}{"n": 4}})}},
	})
	if !resp.Success {
		t.Fatalf("the owner refused a stream push: %s (refusal %q)", resp.Error, resp.Refusal)
	}
	entries, err := ownerCluster.s.wal.ReadFrom(logged + 1)
	if err != nil {
		t.Fatalf("reading the owner's log: %v", err)
	}
	if len(entries) != 1 || entries[0].Origin.Kind != storage.WALOriginReplica || entries[0].Origin.Source != senderID || entries[0].Origin.Seq != seq {
		var origins []storage.WALOrigin
		for _, entry := range entries {
			origins = append(origins, entry.Origin)
		}
		t.Errorf("the owner's log gained %d entries with origins %+v; want one `replica` entry of %s's sequence %d", len(entries), origins, senderID, seq)
	}
	caughtUpWithin(t, streamTo(t, tc, owner, third), 20*time.Second)
	caughtUpWithin(t, streamTo(t, tc, owner, sender), 20*time.Second)
	if got := tc.localCount(third); got != 3 {
		t.Errorf("the third node holds %d elements of the queue after a stream push to the owner, want 3: the owner shipped it on", got)
	}
	if got := tc.localCount(sender); got != 3 {
		t.Errorf("the sender holds %d elements of the queue, want 3: the owner sent its own entry back to it", got)
	}
}
