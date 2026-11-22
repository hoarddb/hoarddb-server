package server

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// TestACreateOfANameARemovedNodeOwnedGoesOnWhileAMemberIsDown is the
// deterministic guard on the waitsOnMembersOnly deferral of
// Cluster.awaitCopyARemovedNodeMayHold (BUG-0627). Five members at factor 2
// remove node 5 and settle; node 4 is then stopped. The name is picked from
// the rings, not by chance: node 5 owned it before the removal, so the owner
// question runs its take-over round, and the round is blocked only by the
// unheard node 4, which no removed node holds a copy against. The create goes
// on to the members' survey, which finds the name's set unchanged since the
// settled floor and creates it. Guard: drop the waitsOnMembersOnly deferral,
// and the round's hand-off refusal is the create's answer (or the create waits
// out its take-over bound), so the create is not answered "created" promptly.
func TestACreateOfANameARemovedNodeOwnedGoesOnWhileAMemberIsDown(t *testing.T) {
	switchJoinFreezeOff(t)
	tc := newTestCluster(t, 5, 2)
	defer tc.stop()
	const via, removed, down = 0, 4, 3
	viaCluster := clusterOf(tc, via)
	removedID, downID := tc.addrs[removed], tc.addrs[down]
	full := ringWith(t, 2, tc.addrs...)
	reduced := ringWith(t, 2, tc.addrs[0], tc.addrs[1], tc.addrs[2], tc.addrs[3])
	// A name node 5 owned at the founders' epoch, whose owner now is neither
	// the stopped node nor a member the create would be routed away from.
	name := bucketNamed(t, "removedowned", func(candidate string) bool {
		return ownerOn(full, candidate) == removedID && ownerOn(reduced, candidate) != downID &&
			!slices.Contains(replicaIDs(t, reduced, 2, candidate), downID)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := viaCluster.proposeRemoval(ctx, removedID); err != nil {
		t.Fatalf("setup: removing node %d: %v", removed+1, err)
	}
	for other := 1; other < 5; other++ {
		if err := clusterOf(tc, other).reconcileMembership(viaCluster.self.ID, viaCluster.memberNodes(), viaCluster.memberIDs(), viaCluster.Epoch()); err != nil {
			t.Fatalf("setup: bringing node %d to the removal: %v", other+1, err)
		}
	}
	settledAt := viaCluster.Epoch()
	heard := func() bool {
		viaCluster.settleMu.Lock()
		defer viaCluster.settleMu.Unlock()
		if viaCluster.settled.Load() < settledAt {
			return false
		}
		for _, index := range []int{1, 2, 3} {
			if viaCluster.settledOf[tc.addrs[index]] < settledAt {
				return false
			}
		}
		return true
	}
	if !eventually(heard) {
		t.Fatalf("setup: node 1 never heard every member settle at epoch %d", settledAt)
	}
	tc.ok(via, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitListedBy(t, tc, testDatabase, 0, 1, 2, 3)
	tc.stopNodeKeepingData(down)

	at := nodeIndexByAddr(t, tc.addrs, ownerOn(reduced, name))
	if !clusterOf(tc, at).ownedByARemovedNode(placementOf(testDatabase, name)) {
		t.Fatalf("setup: the history does not say a removed node owned %s, so the create would not reach the owner question", name)
	}
	began := time.Now()
	frame := tc.frame(at, transport.OpCreateBucket, bson.M{"database": testDatabase, "name": name, "type": string(BucketTypeHash), "write_concern": "one"})
	took := time.Since(began).Round(time.Millisecond)
	if text := string(frame.Payload); frame.Opcode == transport.OpError || !strings.Contains(text, "created") {
		t.Fatalf("a create of %s, which removed node %d owned, with member %s unheard, was answered %s after %s: %q", name, removed+1, downID, frame.Opcode, took, text)
	}
	if took > DefaultWriteConcernTimeout {
		t.Errorf("a create of %s with an unheard member took %s, longer than the write-concern timeout", name, took)
	}
}

// TestALoneSurvivorCreatesAnUnheldNameTheRemovedNodeOwned pins the
// found-nothing side of BUG-0625's wait for a name picked to reach it. The
// survivor of a 2-to-1 removal restarts alone; the name is one the removed
// node owned at the founders' epoch and never held, so the create runs
// Cluster.awaitCopyARemovedNodeMayHold's take-over round (the setup asserts
// ownedByARemovedNode), which finds the name nowhere. The create is answered
// "created" within the write-concern timeout. Guard: treat
// ErrMigrateBucketNotFound as a refusal, and the create is refused; the
// phase-named buckets of TestALoneSurvivorStillCreatesANewName reach the round
// only when the removed node happened to own them.
func TestALoneSurvivorCreatesAnUnheldNameTheRemovedNodeOwned(t *testing.T) {
	s := restartedLoneSurvivor(t)
	founders := ringWith(t, 1, s.tc.addrs...)
	name := bucketNamed(t, "removedowned", func(candidate string) bool {
		return candidate != s.bucket && ownerOn(founders, candidate) == s.tc.addrs[1]
	})
	lone := clusterOf(s.tc, 0)
	if !lone.ownedByARemovedNode(placementOf(testDatabase, name)) {
		t.Fatalf("setup: the lone survivor does not remember that the removed node owned %s, so the create would not reach the owner question", name)
	}
	if s.tc.nodes[0].server.core.bucketStoreOpen(testDatabase, name) {
		t.Fatalf("setup: the survivor holds %s before the create", name)
	}
	began := time.Now()
	frame := s.tc.frame(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: name, Type: BucketTypeHash})
	took := time.Since(began).Round(time.Millisecond)
	if text := string(frame.Payload); frame.Opcode == transport.OpError || !strings.Contains(text, "created") {
		t.Fatalf("a create of %s, a name the removed node owned and no node holds, on the lone survivor was answered %s after %s: %q", name, frame.Opcode, took, text)
	}
	if took > DefaultWriteConcernTimeout {
		t.Errorf("the create of %s on the lone survivor took %s, longer than the write-concern timeout", name, took)
	}
	t.Logf("%s was created in %s", name, took)
}
