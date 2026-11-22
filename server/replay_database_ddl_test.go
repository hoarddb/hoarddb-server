package server

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/transport"
)

// joinerOfAPulledBucket is a three-node, factor-3 cluster in which the test
// database was created, given a bucket, dropped and created again with the
// bucket, all through the bucket's owner; a fourth node has joined and pulled
// the bucket while the owner's replication pushes to it were frozen, so the
// owner's stream to it still has the database's whole history to replay.
type joinerOfAPulledBucket struct {
	tc        *testCluster
	owner     int
	joiner    *testNode
	toJoiner  *peer
	bucket    string
	keys      []string
	unfreeze  func()
	snapshots uint64
}

func newJoinerOfAPulledBucket(t *testing.T) *joinerOfAPulledBucket {
	t.Helper()
	tc := newTestCluster(t, 3, 3)
	t.Cleanup(tc.stop)

	ports := reserveNodePorts(t)
	newcomer := ports.ringNode()
	members := tc.nodes[0].server.core.cluster.Members()
	after := ringOf(t, append(append([]ring.Node{}, members...), newcomer), 3)
	bucket, ownerID := "", ""
	for attempt := 0; attempt < 1000 && bucket == ""; attempt++ {
		name := fmt.Sprintf("snap%03d", attempt)
		key := placementOf(testDatabase, name)
		was, err := tc.nodes[0].server.core.cluster.Owner(key)
		if err != nil {
			t.Fatalf("owner of %s: %v", name, err)
		}
		set, err := after.Replicas(key.encoded, 3)
		if err != nil {
			t.Fatalf("replicas of %s after the join: %v", name, err)
		}
		// The owner keeps the bucket and the newcomer is placed on it, so the
		// newcomer pulls it from the owner and the owner adopts the position.
		if set[0].ID == was.ID && containsID(nodeIDs(set), newcomer.ID) {
			bucket, ownerID = name, was.ID
		}
	}
	if bucket == "" {
		t.Fatal("setup: no bucket name in snap000..snap999 keeps its owner and is placed on the newcomer")
	}
	owner := nodeIndexByAddr(t, tc.addrs, ownerID)
	ownerCluster := tc.nodes[owner].server.core.cluster

	tc.ok(owner, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	createBucketAt(tc, owner, testDatabase, bucket, 0)
	waitForServing(t, tc, testDatabase, bucket, 3)
	putDocumentAt(tc, owner, testDatabase, bucket, "gen1", "all")
	tc.ok(owner, transport.OpDeleteDB, DeleteDatabaseRequest{Name: testDatabase})
	waitForServing(t, tc, testDatabase, bucket, 0)
	tc.ok(owner, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	createBucketAt(tc, owner, testDatabase, bucket, 0)
	waitForServing(t, tc, testDatabase, bucket, 3)
	keys := make([]string, 0, 20)
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("gen2-%02d", i)
		keys = append(keys, key)
		putDocumentAt(tc, owner, testDatabase, bucket, key, "all")
	}

	toJoiner := ownerCluster.peerFor(newcomer)
	// Only the stream's entries are held: its handshake passes, because a
	// source serves a pull only over a stream that has opened (A-4.2).
	unfreeze := holdStreamEntries(toJoiner)
	t.Cleanup(unfreeze)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := ownerCluster.proposeJoin(ctx, newcomer); err != nil {
		t.Fatalf("proposeJoin: %v", err)
	}
	joiner := startJoinTarget(t, tc, ports, tc.token)
	if err := joiner.server.core.cluster.reconcileMembership(ownerCluster.self.ID, ownerCluster.memberNodes(), ownerCluster.memberIDs(), ownerCluster.Epoch()); err != nil {
		t.Fatalf("resync the joiner's membership: %v", err)
	}
	pollUntil(t, "the joiner pulled the bucket", func() bool { return joiner.server.core.bucketStoreOpen(testDatabase, bucket) })
	pollUntil(t, "the joiner recorded the snapshot position", func() bool {
		return joiner.server.core.cluster.seededThroughFor(ownerID, testDatabase, bucket) > 0
	})
	for _, key := range keys {
		if !holdsKeyLocallyIn(joiner, bucket, key) {
			t.Fatalf("setup: the pulled copy lacks %s", key)
		}
	}
	return &joinerOfAPulledBucket{
		tc: tc, owner: owner, joiner: joiner, toJoiner: toJoiner, bucket: bucket, keys: keys, unfreeze: unfreeze,
		snapshots: joiner.server.core.cluster.seededThroughFor(ownerID, testDatabase, bucket),
	}
}

// TestAReplayedDropDatabaseDoesNotDeleteAPulledCopy: the owner's replay to a
// joiner starts at the joiner's acknowledgement, which is below a snapshot
// the joiner pulled. Between the two lie the database's first create, its
// drop and its second create. Bucket entries there are passed over by the
// snapshot position; the database DDL is not bucket-scoped, so before the
// fix the drop was applied on the joiner and deleted the copy it had pulled —
// a copy the owner counts as holding everything through the snapshot.
func TestAReplayedDropDatabaseDoesNotDeleteAPulledCopy(t *testing.T) {
	jp := newJoinerOfAPulledBucket(t)
	acked := jp.toJoiner.ackedThrough(jp.tc.nodes[jp.owner].server.core.cluster.self.ID)
	t.Logf("joiner pulled %s at sequence %d; the owner's stream to it is acknowledged through %d", jp.bucket, jp.snapshots, acked)

	jp.unfreeze()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := catchUpPeer(ctx, jp.toJoiner); err != nil {
		t.Fatalf("catch the joiner up: %v", err)
	}
	// catchUpPeer returns once the joiner has acknowledged the replay, and a
	// replica acknowledges an entry after applying it.
	if !jp.joiner.server.core.bucketStoreOpen(testDatabase, jp.bucket) {
		t.Fatalf("the replay deleted the joiner's pulled copy of %s/%s: a drop database logged below the snapshot at %d was applied over it",
			testDatabase, jp.bucket, jp.snapshots)
	}
	missing := 0
	for _, key := range jp.keys {
		if !holdsKeyLocallyIn(jp.joiner, jp.bucket, key) {
			missing++
		}
	}
	if missing > 0 {
		t.Fatalf("the replay deleted %d of the %d keys of the joiner's pulled copy of %s/%s", missing, len(jp.keys), testDatabase, jp.bucket)
	}
}

// pushToReceiver delivers entries to receiver as a replication push from
// source and fails the test unless the receiver accepts them.
func pushToReceiver(t *testing.T, receiver *testNode, source string, entries []ReplEntry) {
	t.Helper()
	core := receiver.server.core
	push, err := bson.Marshal(ReplPushRequest{Token: core.cluster.localToken(), FromNode: source, Entries: entries,
		Epoch: core.cluster.Epoch(), Stream: streamOf(t, core.cluster, source)})
	if err != nil {
		t.Fatalf("marshal replication push: %v", err)
	}
	respFrame, err := handleReplPush(context.Background(), transport.NewFrame(transport.OpReplPush, push), core)
	if err != nil {
		t.Fatalf("replication push: %v", err)
	}
	var resp ReplPushResponse
	if err := bson.Unmarshal(respFrame.Payload, &resp); err != nil {
		t.Fatalf("decode replication ack: %v", err)
	}
	if !resp.Success {
		t.Fatalf("the receiver refused the push: %s", resp.Error)
	}
}
