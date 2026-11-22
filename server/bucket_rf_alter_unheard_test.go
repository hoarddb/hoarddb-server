package server

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/transport"
)

// unheardMemberFixture is the setup of the BUG-0685 unheard-member test, shared
// with the data writes' (BUG-0686): six members at factor 2, one stopped after a
// removal moved a name's replica set, so the settled floor is behind the change.
// It returns the cluster, the index of the owner of a name the removal moved
// that no node holds, that name, and the stopped member's ID.
func unheardMemberFixture(t *testing.T) (tc *testCluster, owner int, moved, downID string) {
	t.Helper()
	switchJoinFreezeOff(t)
	tc = newTestCluster(t, 6, 2)
	t.Cleanup(tc.stop)
	const via = 0
	// Which members play the removed, stopped and first-removed parts is chosen
	// by the ring the run's random addresses make: a name no removed node ever
	// owned, whose set a removal moved, exists for most but not every choice.
	without := func(drop ...int) []int {
		var kept []int
		for index := range tc.addrs {
			if !slices.Contains(drop, index) {
				kept = append(kept, index)
			}
		}
		return kept
	}
	ringOf := func(indexes []int) *ring.Ring {
		addrs := make([]string, 0, len(indexes))
		for _, index := range indexes {
			addrs = append(addrs, tc.addrs[index])
		}
		return ringWith(t, 2, addrs...)
	}
	everyone := ringOf(without())
	var removed, down, first int
	var full, reduced *ring.Ring
	for first = 1; first < len(tc.addrs) && moved == ""; first++ {
		for removed = 1; removed < len(tc.addrs) && moved == ""; removed++ {
			for down = 1; down < len(tc.addrs) && moved == ""; down++ {
				if removed == first || down == first || down == removed {
					continue
				}
				full, reduced = ringOf(without(first)), ringOf(without(first, removed))
				moved, _ = findBucketName("moved", func(name string) bool {
					now := replicaIDs(t, reduced, 2, name)
					for _, held := range [][]string{replicaIDs(t, everyone, 2, name), replicaIDs(t, full, 2, name), now} {
						if held[0] == tc.addrs[first] || held[0] == tc.addrs[removed] {
							return false
						}
					}
					return !slices.Equal(replicaIDs(t, full, 2, name), now) && now[0] != tc.addrs[down]
				})
			}
		}
	}
	if moved == "" {
		t.Fatal("setup: no choice of removed and stopped members gives a name the removal moved and no removed node owned")
	}
	first, removed, down = first-1, removed-1, down-1
	downID = tc.addrs[down]
	viaCluster := clusterOf(tc, via)
	removeNode := func(index int, others []int) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := viaCluster.proposeRemoval(ctx, tc.addrs[index]); err != nil {
			t.Fatalf("setup: removing node %d: %v", index+1, err)
		}
		for _, other := range others {
			if err := clusterOf(tc, other).reconcileMembership(viaCluster.self.ID, viaCluster.memberNodes(), viaCluster.memberIDs(), viaCluster.Epoch()); err != nil {
				t.Fatalf("setup: bringing node %d to the removal of node %d: %v", other+1, index+1, err)
			}
		}
	}
	removeNode(first, without(via))
	settledAt := viaCluster.Epoch()
	heard := func() bool {
		c := viaCluster
		if c.settled.Load() < settledAt {
			return false
		}
		c.settleMu.Lock()
		defer c.settleMu.Unlock()
		for _, index := range without(via, first) {
			if c.settledOf[tc.addrs[index]] < settledAt {
				return false
			}
		}
		return true
	}
	if !eventually(heard) {
		t.Fatalf("setup: node 1 never heard every member settle at epoch %d", settledAt)
	}
	tc.ok(via, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitListedBy(t, tc, testDatabase, without(first)...)
	waitForAlterFeature(t, tc)

	tc.stopNodeKeepingData(down)
	removeNode(removed, without(via, down, first))
	if floor := viaCluster.settledFloorNow(); floor != settledAt {
		t.Fatalf("setup: the settled floor is %d, want %d (the epoch the stopped member settled at)", floor, settledAt)
	}

	owner = nodeIndexByAddr(t, tc.addrs, ownerOn(reduced, moved))
	return tc, owner, moved, downID
}

// TestAnAlterOfAMissingNameIsRetriedNotNotFoundWhileAMemberIsUnheard pins the
// fix's other edge (BUG-0685, the verifier's added test): an owner that holds
// no copy and cannot hear from every member must not call the bucket not
// found, since the unheard member may hold it. The setup is the create's
// M-5 one (TestANewBucketIsCreatedWhileAMemberIsDownAfterAnUnrelatedChange):
// five members at factor 2, one stopped after a removal moved the name's set,
// so the settled floor is behind the change. The alter is refused UNAVAILABLE,
// not_applied, naming the unheard member. Guard: read an unheard member as
// "holds none" and the alter answers NOT_FOUND for a bucket that may exist.
func TestAnAlterOfAMissingNameIsRetriedNotNotFoundWhileAMemberIsUnheard(t *testing.T) {
	tc, owner, moved, downID := unheardMemberFixture(t)
	refused := requireRetryRefusal(t, alterFrame(t, tc, owner, AlterBucketRequest{Database: testDatabase, Name: moved, ReplicationFactor: 3}),
		"an alter of a name whose set changed while a member is unheard")
	if !strings.Contains(refused.Message, downID) {
		t.Errorf("the refusal does not name the unheard member %s: %s", downID, refused.Message)
	}
}
