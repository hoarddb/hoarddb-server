package server

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/transport"
)

// TestRemoveNodeByMajorityClusterSurvives removes a member by majority vote:
// the epoch advances, the remaining nodes agree, the removed node is gone from
// membership, and the cluster keeps serving the buckets the survivors hold.
func TestRemoveNodeByMajorityClusterSurvives(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	// Every node holds the bucket (w=all).
	for i := 0; i < 5; i++ {
		tc.frame(0, transport.OpPut, InsertRequest{
			Database: testDatabase, Bucket: testBucket, Key: fmt.Sprintf("k%02d", i),
			Data: map[string]interface{}{"n": i}, WriteConcern: "all",
		})
	}

	removeID := tc.addrs[1]
	oc0 := tc.nodes[0].server.core.cluster
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := oc0.proposeRemoval(ctx, removeID); err != nil {
		t.Fatalf("proposeRemoval: %v", err)
	}
	if got := oc0.Epoch(); got != InitialEpoch+1 {
		t.Fatalf("epoch after removal = %d, want %d", got, InitialEpoch+1)
	}
	ids := map[string]bool{}
	for _, m := range oc0.Members() {
		ids[m.ID] = true
	}
	if ids[removeID] || len(ids) != 2 {
		t.Fatalf("removed node still a member, or wrong size: %v", ids)
	}

	// The other survivor converges; neither has the divergence flag.
	n2 := tc.nodes[2].server.core.cluster
	peer := n2.peerFor(mustNode(t, n2, tc.addrs[0]))
	if err := peer.gossip(ctx); err != nil {
		t.Fatalf("survivor gossip: %v", err)
	}
	for _, i := range []int{0, 2} {
		if tc.nodes[i].server.core.cluster.mismatch.Load() {
			t.Fatalf("survivor %d flagged divergence after a clean removal", i+1)
		}
	}
	if n2.Epoch() != InitialEpoch+1 {
		t.Fatalf("survivor epoch = %d, want %d", n2.Epoch(), InitialEpoch+1)
	}

	// The cluster still serves a bucket both survivors hold, once its owner has
	// taken it over (step 10b.5) when the removal moved it.
	owner, err := oc0.Owner(placementOf(testDatabase, testBucket))
	if err != nil {
		t.Fatalf("the owner after the removal: %v", err)
	}
	waitTakenOver(t, tc.nodes[nodeIndexByAddr(t, tc.addrs, owner.ID)].server.core, testDatabase, testBucket)
	if frame := tc.writeConcerned(0, "still-here", "majority"); frame.Opcode == transport.OpError {
		t.Fatalf("write after removal was rejected: %s", string(frame.Payload))
	}
}

// TestRemoveNodeRejectsInvalidTargets pins the removal edges.
func TestRemoveNodeRejectsInvalidTargets(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()

	oc0 := tc.nodes[0].server.core.cluster
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// A non-member cannot be removed.
	if err := oc0.proposeRemoval(ctx, "ghost@1.2.3.4:1"); err == nil || !strings.Contains(err.Error(), ErrNotMember.Error()) {
		t.Fatalf("expected %q, got %v", ErrNotMember, err)
	}
	// A node cannot remove itself.
	if err := oc0.proposeRemoval(ctx, oc0.self.ID); err == nil || !strings.Contains(err.Error(), ErrRemoveSelf.Error()) {
		t.Fatalf("expected %q, got %v", ErrRemoveSelf, err)
	}
	// A removal is refused while another transition is in progress.
	oc0.transition.Store(true)
	if err := oc0.proposeRemoval(ctx, tc.addrs[1]); err == nil || !strings.Contains(err.Error(), ErrEpochInProgress.Error()) {
		t.Fatalf("expected %q during a held transition, got %v", ErrEpochInProgress, err)
	}
	oc0.transition.Store(false)
	if oc0.Epoch() != InitialEpoch {
		t.Fatalf("epoch changed despite rejected operations: %d", oc0.Epoch())
	}
}
