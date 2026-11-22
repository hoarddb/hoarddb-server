package server

import (
	"context"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/transport"
)

// TestANewcomerBootedPastEpochOneIsNotMarkedRemovedAndJoins: on a
// cluster that has had a membership change, a node started the documented way
// gossips at epoch 1, a member answers with its history, and the newcomer took
// the first epoch that does not list it as its own removal — and stayed
// removed after 'join', refusing every write and pulling nothing. While it is
// bootstrapped it adopts nothing from a history that omits it, and the join
// then adds it like any newcomer.
func TestANewcomerBootedPastEpochOneIsNotMarkedRemovedAndJoins(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	via := clusterOf(tc, 0)

	// Epoch 3: a node joins and is removed again.
	passing := joinNewcomer(t, tc, 0, reserveNodePorts(t))
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	// The newcomer's ports are reserved while the passing node still holds its own, so the
	// kernel cannot hand the passing node's internode port out again: the members still pin
	// that port's key from the passing node.
	ports := reserveNodePorts(t)
	if err := via.proposeRemoval(ctx, passing.cluster().self.ID); err != nil {
		t.Fatalf("removing the passing node: %v", err)
	}
	passing.stop()
	if via.Epoch() != InitialEpoch+2 {
		t.Fatalf("test setup: the cluster is at epoch %d, want %d", via.Epoch(), InitialEpoch+2)
	}

	// A bucket the newcomer will own once it joins, with a document in it.
	joined := ringOf(t, append(via.Members(), ports.ringNode()), 3)
	bucket := bucketNamed(t, "gained", func(name string) bool { return ownerOn(joined, name) == ports.ringNode().ID })
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "k", Data: map[string]interface{}{"n": 1}})

	newcomer := startJoinTarget(t, tc, ports, tc.token)
	waiting := newcomer.server.core.cluster
	deadline := time.Now().Add(20 * time.Second)
	for !waiting.viewDisputed.Load() {
		if time.Now().After(deadline) {
			t.Fatalf("test setup: the newcomer never heard a member answer at epoch %d", via.Epoch())
		}
		time.Sleep(20 * time.Millisecond)
	}
	// Two more woken rounds of its gossip, each answered with the history.
	for round := 0; round < 2; round++ {
		waiting.wakeGossip()
		time.Sleep(300 * time.Millisecond)
	}
	if _, removed, err := readRemovalRecord(newcomer.server.core.kv); err != nil || removed {
		t.Fatalf("before 'join', the newcomer recorded its own removal (removed=%v, err=%v): it adopted a history that never listed it", removed, err)
	}
	if waiting.removed() || !waiting.bootstrapped.Load() || waiting.Epoch() != InitialEpoch {
		t.Fatalf("before 'join', the newcomer is removed=%v (at %d), bootstrapped=%v, epoch %d; want not removed, bootstrapped, epoch %d",
			waiting.removed(), waiting.removedAt.Load(), waiting.bootstrapped.Load(), waiting.Epoch(), InitialEpoch)
	}

	if err := via.proposeJoin(ctx, ports.ringNode()); err != nil {
		t.Fatalf("proposeJoin: %v", err)
	}
	deadline = time.Now().Add(30 * time.Second)
	for waiting.bootstrapped.Load() || waiting.Epoch() != InitialEpoch+3 {
		if time.Now().After(deadline) {
			t.Fatalf("the newcomer did not adopt its join: epoch %d, bootstrapped %v, removed %v",
				waiting.Epoch(), waiting.bootstrapped.Load(), waiting.removed())
		}
		time.Sleep(20 * time.Millisecond)
	}
	if waiting.removed() {
		t.Fatalf("the newcomer was joined at epoch %d and is still removed (at %d)", waiting.Epoch(), waiting.removedAt.Load())
	}

	// The holders hand the bucket it owns over, and it serves it.
	for !holdsKeyIn(newcomer, testDatabase, bucket, "k") {
		if time.Now().After(deadline.Add(30 * time.Second)) {
			t.Fatalf("the holders never handed bucket %s to the newcomer that owns it", bucket)
		}
		time.Sleep(50 * time.Millisecond)
	}
	frame := tc.frameOn(newcomer, transport.OpGet, FindRequest{Database: testDatabase, Bucket: bucket, Key: "k"})
	if frame.Opcode == transport.OpError {
		t.Fatalf("the newcomer does not serve the bucket it owns and holds: %s", string(frame.Payload))
	}
}

// TestARealMemberThatMissedItsRemovalStillMarksItselfRemoved: the rule above
// is for a node no member has confirmed. A confirmed member partitioned
// across its own removal learns it on reconnect from the members' history,
// and marks itself removed, as before.
func TestARealMemberThatMissedItsRemovalStillMarksItselfRemoved(t *testing.T) {
	tc := newTestCluster(t, 4, 3)
	defer tc.stop()
	const removedIndex = 3
	removedID := tc.addrs[removedIndex]
	missing := clusterOf(tc, removedIndex)

	release := make(chan struct{})
	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()
	held := func(inner peerConn) peerConn { return &heldAllConn{peerConn: inner, release: release} }
	for index := range tc.nodes {
		if index == removedIndex {
			continue
		}
		wrapPeerConn(clusterOf(tc, index).peerFor(ring.Node{ID: removedID, Addr: removedID}), held)
		wrapPeerConn(missing.peerFor(ring.Node{ID: tc.addrs[index], Addr: tc.addrs[index]}), held)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := clusterOf(tc, 0).proposeRemoval(ctx, removedID); err != nil {
		t.Fatalf("removing node %d: %v", removedIndex+1, err)
	}
	if missing.removed() || missing.bootstrapped.Load() {
		t.Fatalf("test setup: the partitioned member is removed=%v bootstrapped=%v before it reconnects", missing.removed(), missing.bootstrapped.Load())
	}

	close(release)
	released = true
	deadline := time.Now().Add(20 * time.Second)
	for !missing.removed() {
		if time.Now().After(deadline) {
			t.Fatalf("a member that missed its own removal reconnected and did not mark itself removed (epoch %d)", missing.Epoch())
		}
		missing.wakeGossip()
		time.Sleep(50 * time.Millisecond)
	}
}
