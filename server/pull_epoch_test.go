package server

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Dispatch 6, step 6.3: a pull across membership epochs (spec §19.3.7).

// holdOnOldEpoch makes the node nodeID refuse to adopt any epoch after from
// until the returned release is called: a member that has not learned a
// membership change yet.
func holdOnOldEpoch(t *testing.T, nodeID string, from uint64) (release func()) {
	t.Helper()
	var held atomic.Bool
	held.Store(true)
	refuse := func(adopting string, epoch uint64) error {
		if held.Load() && adopting == nodeID && epoch > from {
			return fmt.Errorf("test seam: %s is held on membership epoch %d", nodeID, from)
		}
		return nil
	}
	adoptMembershipHook.Store(&refuse)
	t.Cleanup(func() { adoptMembershipHook.Store(nil) })
	return func() { held.Store(false) }
}

// adoptFrom brings target's membership to source's through the gossip
// reconciliation a member runs when it hears of a newer epoch.
func adoptFrom(t *testing.T, target, source *Cluster) {
	t.Helper()
	if err := target.reconcileMembership(source.self.ID, source.memberNodes(), source.memberIDs(), source.Epoch()); err != nil {
		t.Fatalf("bringing %s to %s's membership: %v", target.self.ID, source.self.ID, err)
	}
}

// replacedReplicaOf reports which node enters a bucket's replica set when the
// set's second member is removed, or ok=false when the removal also moves the
// owner. The consistent-hash library redistributes every partition under
// fresh load limits whenever a member leaves, so removing the second replica
// moves the owner of a few partitions; which bucket name hashes to one of the
// others depends on the node identities the drawn ports fix.
func replacedReplicaOf(addrs []string, key string) (ownerID, removedID, enteringID string, ok bool) {
	members := ring.New(2)
	for _, addr := range addrs {
		if err := members.AddNode(ring.Node{ID: addr, Addr: addr}); err != nil {
			return "", "", "", false
		}
	}
	before, err := members.Replicas(key, 2)
	if err != nil {
		return "", "", "", false
	}
	after := ring.New(2)
	for _, addr := range addrs {
		if addr == before[1].ID {
			continue
		}
		if err := after.AddNode(ring.Node{ID: addr, Addr: addr}); err != nil {
			return "", "", "", false
		}
	}
	placed, err := after.Replicas(key, 2)
	if err != nil || placed[0].ID != before[0].ID {
		return "", "", "", false
	}
	return before[0].ID, before[1].ID, placed[1].ID, true
}

// TestAPullAcrossEpochsIsRefusedAndRetried is T-100 (spec §19.3.7, W1). Five
// nodes at factor 2; the replica of the test bucket is removed, so another
// member Y enters the bucket's set and pulls it from the owner A, which a test
// seam holds on the old epoch. The pull is refused. A keeps taking writes on
// its old set; once it adopts, the sweep's retry lands the copy on Y, and
// every write A took in between is on Y. Guard: no check in
// handleMigrateStart — Y pulls under A's old ring, A's writes go to the old
// set, A's stream to Y moves Y's cursor past them, and Y is short by them.
func TestAPullAcrossEpochsIsRefusedAndRetried(t *testing.T) {
	tc := newTestClusterWith(t, 5, 2, withoutRetentionLoop)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	// Which bucket name has an owner the removal leaves in place depends on the
	// partition the name hashes to, and which partitions a removal moves depends
	// on the node identities the drawn ports fix, so search names rather than
	// assert it of testBucket (BUG-0487 family). bucketNamed returns at the
	// first qualifying name, so these are that bucket's nodes.
	var ownerID, removedID, enteringID string
	bucket := bucketNamed(t, "pulled", func(name string) bool {
		var ok bool
		ownerID, removedID, enteringID, ok = replacedReplicaOf(tc.addrs, placementOf(testDatabase, name).encoded)
		return ok
	})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, bucket, 2)

	ownerIndex := nodeIndexByAddr(t, tc.addrs, ownerID)
	removedIndex := nodeIndexByAddr(t, tc.addrs, removedID)
	enteringIndex := nodeIndexByAddr(t, tc.addrs, enteringID)
	proposerIndex := 0
	for proposerIndex == ownerIndex || proposerIndex == removedIndex {
		proposerIndex++
	}
	owner := tc.nodes[ownerIndex].server.core.cluster
	entering := tc.nodes[enteringIndex].server.core.cluster
	proposer := tc.nodes[proposerIndex].server.core.cluster

	release := holdOnOldEpoch(t, owner.self.ID, owner.Epoch())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := proposer.proposeRemoval(ctx, tc.addrs[removedIndex]); err != nil {
		t.Fatalf("removing node %d: %v", removedIndex+1, err)
	}
	if entering.Epoch() != proposer.Epoch() {
		adoptFrom(t, entering, proposer)
	}

	if err := entering.ensureMigrated(ctx, testDatabase, bucket); err == nil {
		t.Errorf("node %d pulled the test bucket from its owner while the owner is on epoch %d and it is on %d; want the pull refused",
			enteringIndex+1, owner.Epoch(), entering.Epoch())
	}

	var between []string
	for n := 0; n < 5; n++ {
		written := fmt.Sprintf("between-%d", n)
		tc.ok(ownerIndex, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: written,
			Data: map[string]interface{}{"n": n}})
		between = append(between, written)
	}
	// What would carry the entering node's cursor past those writes on the
	// old ring: an advance notice from the owner's stream.
	owner.askForAdvances()
	waitCursorAtLeast(owner.peerFor(mustNode(t, owner, tc.addrs[enteringIndex])), owner.s.wal.SyncedSequence(), 2*time.Second)

	release()
	adoptFrom(t, owner, proposer)
	deadline := time.Now().Add(60 * time.Second)
	for !entering.s.bucketStoreOpen(testDatabase, bucket) {
		if time.Now().After(deadline) {
			t.Fatalf("node %d never holds the test bucket after its owner adopted the removal", enteringIndex+1)
		}
		time.Sleep(50 * time.Millisecond)
	}
	caughtUpWithin(t, owner.peerFor(mustNode(t, owner, tc.addrs[enteringIndex])), 20*time.Second)
	for _, written := range between {
		if !holdsKeyLocally(tc.nodes[enteringIndex], bucket, written) {
			t.Errorf("node %d, which entered the set, does not hold %q, written on the owner while it was on the old epoch", enteringIndex+1, written)
		}
	}
}
