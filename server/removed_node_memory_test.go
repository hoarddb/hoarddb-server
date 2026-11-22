package server

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Dispatch 9, step 9.3: a removed node remembers, and stays a source (T-117;
// A-9.3).

// heldAllConn holds every request sent on it until release is closed or the
// request's context ends: a connection a partition holds, gossip included.
type heldAllConn struct {
	peerConn
	release <-chan struct{}
}

func (h *heldAllConn) SendRequest(ctx context.Context, lane transport.Lane, op transport.Opcode, payload []byte) (*transport.Frame, error) {
	select {
	case <-h.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return h.peerConn.SendRequest(ctx, lane, op, payload)
}

// bucketOwnedByAt returns a bucket name whose owner on the ring of addrs at
// factor is owner.
func bucketOwnedByAt(t *testing.T, addrs []string, owner, prefix string) string {
	t.Helper()
	placement := ring.New(3)
	for _, id := range addrs {
		if err := placement.AddNode(ring.Node{ID: id, Addr: id}); err != nil {
			t.Fatalf("building a ring: %v", err)
		}
	}
	for i := 0; i < 1000; i++ {
		name := fmt.Sprintf("%s%03d", prefix, i)
		placed, err := placement.Owner(placementOf(testDatabase, name).encoded)
		if err != nil {
			t.Fatalf("placing %s: %v", name, err)
		}
		if placed.ID == owner {
			return name
		}
	}
	t.Fatalf("no bucket name of 1000 is owned by %s", owner)
	return ""
}

// removeAndTell removes the node at removedIndex through via and hands it what
// the next gossip round would, so a test does not wait twenty seconds for it.
func removeAndTell(t *testing.T, tc *testCluster, via, removedIndex int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	proposer := clusterOf(tc, via)
	if err := proposer.proposeRemoval(ctx, tc.addrs[removedIndex]); err != nil {
		t.Fatalf("removing node %d: %v", removedIndex+1, err)
	}
	if err := clusterOf(tc, removedIndex).reconcileMembership(proposer.self.ID, proposer.memberNodes(), proposer.memberIDs(), proposer.Epoch()); err != nil {
		t.Fatalf("telling node %d of its removal: %v", removedIndex+1, err)
	}
}

// TestARemovedNodeRemembersItsRemovalAcrossARestart is T-117: a node removed
// by the others is stopped and started again with every connection between it
// and the members held, so nothing tells it again. A write to it is refused
// with step 6.5b's message, and its membership.json still lists it. Guard: no
// record of the removal.
func TestARemovedNodeRemembersItsRemovalAcrossARestart(t *testing.T) {
	tc := newTestCluster(t, 4, 3)
	defer tc.stop()
	const removedIndex = 3
	removedID := tc.addrs[removedIndex]
	bucket := bucketOwnedByAt(t, tc.addrs, removedID, "remembered")
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, bucket, 3)
	epochBefore := clusterOf(tc, removedIndex).Epoch()

	removeAndTell(t, tc, 0, removedIndex)
	if clusterOf(tc, removedIndex).removedAt.Load() == 0 {
		t.Fatal("test setup: the removed node does not know it was removed")
	}

	release := make(chan struct{})
	defer close(release)
	held := func(inner peerConn) peerConn { return &heldAllConn{peerConn: inner, release: release} }
	for index := range tc.nodes {
		if index == removedIndex {
			continue
		}
		member := clusterOf(tc, index)
		wrapPeerConn(member.peerFor(ring.Node{ID: removedID, Addr: removedID}), held)
	}
	tc.tune = func(index int, c *Cluster) {
		if index != removedIndex {
			return
		}
		for _, member := range tc.addrs {
			if member != removedID {
				wrapPeerConn(c.peerFor(ring.Node{ID: member, Addr: member}), held)
			}
		}
	}
	defer func() { tc.tune = nil }()
	tc.restartNode(removedIndex)

	frame := tc.frame(removedIndex, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "after-restart",
		Data: map[string]interface{}{"n": 1}, WriteConcern: "one"})
	if frame.Opcode != transport.OpError {
		t.Fatalf("a removed node that restarted answered a write with %s: it forgot that it was removed", frame.Opcode)
	}
	if text := string(frame.Payload); !strings.Contains(text, "removed from the cluster") || !strings.Contains(text, "connect to") {
		t.Errorf("the restarted removed node's refusal does not say it was removed and where to connect: %s", text)
	}
	restarted := clusterOf(tc, removedIndex)
	if epoch, members, found, err := loadMembership(restarted.membershipPath()); err != nil || !found || epoch != epochBefore || !nodeListed(members, removedID) {
		t.Errorf("the removed node's membership.json was rewritten: epoch %d (was %d), members %v, found %v, %v", epoch, epochBefore, memberIDsOf(members), found, err)
	}
}

// TestAMigrateDoneOnARemovedNodeDeletesNothing: a member of a bucket's set
// completes a pull from the removed node, whose copy is outside every set: the
// removed node answers the pull's last step, MIGRATE_DONE, and still holds its
// copy. The source's side of the pull is driven directly — its snapshot, then
// the target's done with the snapshot's digest — because a removed node also
// keeps shipping what it originated, and its stream can seed the member
// before a pull runs. Guard: no removed-node return in handleMigrateDone.
func TestAMigrateDoneOnARemovedNodeDeletesNothing(t *testing.T) {
	tc := newTestClusterWith(t, 4, 2, func(_ int, cfg *config.Config) { cfg.Server.AutoRebalance = false })
	defer tc.stop()
	const removedIndex = 3
	removedID := tc.addrs[removedIndex]
	bucket := bucketOwnedByAt(t, tc.addrs, removedID, "sourced")
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, bucket, 2)
	for i := 0; i < 3; i++ {
		tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: fmt.Sprintf("k%d", i),
			Data: map[string]interface{}{"n": i}, WriteConcern: "all"})
	}

	via := (removedIndex + 1) % len(tc.nodes)
	removeAndTell(t, tc, via, removedIndex)
	removed := clusterOf(tc, removedIndex)
	member := clusterOf(tc, via)
	snapshot, err := removed.snapshotBucket(testDatabase, bucket, "")
	if err != nil {
		t.Fatalf("the removed node's snapshot of %s: %v", bucket, err)
	}
	snapshot.target, snapshot.transfer = member.self.ID, "a-test-pull"
	removed.setPending(migrateKey(testDatabase, bucket), snapshot)
	done, err := bson.Marshal(MigrateDoneRequest{Token: member.localToken(), Database: testDatabase, Bucket: bucket,
		Digest: hex.EncodeToString(snapshot.digest[:]), Transfer: snapshot.transfer})
	if err != nil {
		t.Fatalf("encoding the done: %v", err)
	}
	answerFrame, err := handleMigrateDone(context.Background(), transport.NewFrame(transport.OpMigrateDone, done), removed.s)
	if err != nil {
		t.Fatalf("MIGRATE_DONE on the removed node: %v", err)
	}
	var answer MigrateDoneResponse
	if err := bson.Unmarshal(answerFrame.Payload, &answer); err != nil || !answer.Ok {
		t.Fatalf("the removed node answered the pull's done with %+v (%v)", answer, err)
	}
	if !removed.s.bucketStoreOpen(testDatabase, bucket) {
		t.Error("the removed node deleted its copy when a member's pull of it completed: a removed node never deletes a copy")
	}
}

// TestARemovedNodeFollowsALaterEpoch: a removed node shown the next epoch by a
// member of the membership it recorded rewrites its record and its view, so a
// member's pull from it on that epoch is not refused across epochs. Guard: no
// rewrite.
func TestARemovedNodeFollowsALaterEpoch(t *testing.T) {
	tc := newTestCluster(t, 5, 3)
	defer tc.stop()
	const removedIndex, laterIndex = 4, 3
	removeAndTell(t, tc, 0, removedIndex)
	removed := clusterOf(tc, removedIndex)
	firstRemoval := removed.Epoch()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	proposer := clusterOf(tc, 0)
	if err := proposer.proposeRemoval(ctx, tc.addrs[laterIndex]); err != nil {
		t.Fatalf("removing node %d: %v", laterIndex+1, err)
	}
	later := proposer.Epoch()
	if err := removed.reconcileMembership(proposer.self.ID, proposer.memberNodes(), proposer.memberIDs(), later); err != nil {
		t.Fatalf("showing the removed node epoch %d: %v", later, err)
	}

	record, found, err := readRemovalRecord(removed.s.kv)
	if err != nil || !found || record.Epoch != later {
		t.Errorf("the removed node's record of its removal is at epoch %d (found %v, %v); it was shown epoch %d (it was removed at %d)",
			record.Epoch, found, err, later, firstRemoval)
	}
	if refusal := removed.refuseAPullAcrossEpochs(MigrateStartRequest{FromNode: proposer.self.ID, Database: testDatabase, Bucket: testBucket, Epoch: later}); refusal != nil {
		t.Errorf("the removed node refuses a member's pull on epoch %d across epochs: %v", later, refusal)
	}
}

// TestARemovalTellsAReachableRemovedNode: a reachable, idle node refuses a
// write as soon as its removal has committed, without having been asked
// anything — the proposer told it. Guard: no frame after the commit; the node
// then learns at its next gossip round, twenty seconds away.
func TestARemovalTellsAReachableRemovedNode(t *testing.T) {
	tc := newTestCluster(t, 4, 3)
	defer tc.stop()
	const removedIndex = 3
	removedID := tc.addrs[removedIndex]
	bucket := bucketOwnedByAt(t, tc.addrs, removedID, "told")
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, bucket, 3)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := clusterOf(tc, 0).proposeRemoval(ctx, removedID); err != nil {
		t.Fatalf("removing node %d: %v", removedIndex+1, err)
	}
	frame := tc.frame(removedIndex, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "after-removal",
		Data: map[string]interface{}{"n": 1}, WriteConcern: "one"})
	if frame.Opcode != transport.OpError || !strings.Contains(string(frame.Payload), "removed from the cluster") {
		t.Errorf("the removed node, reachable and idle, answered a write right after its removal committed with %s %s: nobody told it",
			frame.Opcode, string(frame.Payload))
	}
}
