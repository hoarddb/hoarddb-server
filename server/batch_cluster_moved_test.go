package server

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// TestClusterBatchOwnershipMovedInFlight is T-C5's first half: a batch item
// whose bucket moves to another owner while the item is held inside the
// owner's fence, after it decided it owns the bucket and before it is logged,
// is answered unknown with ErrOwnershipMoved's class, because the owner has
// ceded the bucket and the entry may still reach the copies. The second half,
// a local chain item that is never forwarded, is
// TestClusterBatchLocalChainNeverForwarded. Break: answer the item applied or
// not_applied when the owner moved.
func TestClusterBatchOwnershipMovedInFlight(t *testing.T) {
	tc := newTestClusterWith(t, 3, 3, autoRebalanceOff)
	sc := findCededWriteScenario(t, tc)
	tc.ok(sc.owner, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(sc.owner, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: sc.moved, Type: BucketTypeHash,
		Options: storesOption(StoreDeclaration{Name: "Feed", Engine: "fifoStore"})})
	waitForServing(t, tc, testDatabase, sc.moved, 3)
	tc.ok(sc.owner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: sc.moved, Key: "doc",
		Data: map[string]interface{}{"n": 0}, WriteConcern: "all"})

	ownerCluster := tc.nodes[sc.owner].server.core.cluster
	replicaCluster := tc.nodes[sc.replica].server.core.cluster
	reached := make(chan struct{})
	release := make(chan struct{})
	var holdOnce, releaseOnce sync.Once
	hold := func(nodeID string, op transport.Opcode, _, bucket string, _ uint64) {
		if nodeID != ownerCluster.self.ID || op != transport.OpFieldPush || bucket != sc.moved {
			return
		}
		held := false
		holdOnce.Do(func() { held = true })
		if held {
			close(reached)
			<-release
		}
	}
	afterOwnerViewHook.Store(&hold)
	t.Cleanup(func() { afterOwnerViewHook.Store(nil) })
	// Registered after the cluster's own cleanup, so it runs first: an item
	// held in the hook keeps its node from stopping.
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })

	push := batchItemOf(t, transport.OpFieldPush, FieldRequest{Database: testDatabase, Bucket: sc.moved, Key: "doc", Field: "Feed",
		Data: map[string]interface{}{"n": 1}})
	payload := mustMarshalBatch(t, BatchRequest{Items: []BatchItem{push}})
	replies := make(chan *transport.Frame, 1)
	client := tc.dial(sc.owner)
	t.Cleanup(func() { _ = client.Close() })
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		// A send that fails leaves the channel empty; the test then fails on
		// the wait below, not from this goroutine.
		if frame, err := client.SendRequest(ctx, transport.OpBatch, payload); err == nil {
			replies <- frame
		}
	}()
	select {
	case <-reached:
	case <-time.After(15 * time.Second):
		t.Fatal("setup: the batch item never reached its owner's fence")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := ownerCluster.proposeJoin(ctx, sc.newcomer); err != nil {
		t.Fatalf("join %s: %v", sc.newcomer.ID, err)
	}
	pollUntil(t, "the replica adopts the join", func() bool { return replicaCluster.Epoch() == ownerCluster.Epoch() })
	if owner, err := ownerCluster.Owner(placementOf(testDatabase, sc.moved)); err != nil || owner.ID != sc.newcomer.ID {
		t.Fatalf("setup: %s is owned by %v (%v) after the join, not by the newcomer", sc.moved, owner, err)
	}
	releaseOnce.Do(func() { close(release) })

	var reply *transport.Frame
	select {
	case reply = <-replies:
	case <-time.After(30 * time.Second):
		t.Fatal("the held batch was never answered")
	}
	if reply.Opcode != transport.OpBatchOK {
		t.Fatalf("the batch was answered %s: %s", reply.Opcode, reply.Payload)
	}
	var resp BatchResponse
	if err := bson.Unmarshal(reply.Payload, &resp); err != nil || len(resp.Answers) != 1 {
		t.Fatalf("decoding the batch reply: %v (%d answers)", err, len(resp.Answers))
	}
	moved := batchAnswerError(t, resp.Answers[0], "the item whose bucket moved in flight")
	requireOutcome(t, moved, "the item whose bucket moved in flight", outcomeUnknown, transport.ErrUnavailable)
	if !strings.Contains(moved.Message, "changed while the write was in flight") {
		t.Errorf("the item reads %q, want ErrOwnershipMoved's sentence", moved.Message)
	}
}
