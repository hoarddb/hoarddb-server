package server

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// Dispatch 10a, step 10a.5 (R8-C, "the concern is not widened with the
// route"): a drop is routed to every member, but its write concern counts only
// the members of its set at the factor it was placed at. A member outside the
// set acknowledges a copy the client never counted.

// dropConcernCase holds one member's apply of a drop for a while and reports
// whether the drop was answered before that hold ended.
type dropConcernCase struct {
	concern string
	// held picks the member whose apply is held: the set's replica, or a
	// member outside the set.
	held func(setReplica, outside int) int
	// answeredWhileHeld is what a drop of this concern must do: answer while
	// the hold is on (true), or only after it ends (false).
	answeredWhileHeld bool
	why               string
}

// TestADropsConcernCountsOnlyItsSet: four nodes at factor 2, so a bucket's set
// is its owner and one replica, and two members are outside it. The drop
// reaches all three others.
//
//   - At majority, with the set's replica holding its apply, the drop is not
//     answered until that replica confirms: the two members outside the set
//     acknowledge at once, and they do not make a majority of the set.
//   - At all, with a member outside the set holding its apply, the drop is
//     answered as soon as the set's replica confirms: the member outside the
//     set is not one of "all".
//
// Guard: count every addressee (applyAsOwner's `concernSet == nil ||
// nodeListed(concernSet, n.ID)` made always true) — the majority arm is
// answered while the set's replica still holds, and the all arm waits out the
// member outside the set.
func TestADropsConcernCountsOnlyItsSet(t *testing.T) {
	cases := []dropConcernCase{
		{concern: "majority", held: func(setReplica, _ int) int { return setReplica }, answeredWhileHeld: false,
			why: "the members outside the set made its majority"},
		{concern: "all", held: func(_, outside int) int { return outside }, answeredWhileHeld: true,
			why: "it waited for a member outside the set"},
	}
	for _, dropCase := range cases {
		t.Run(dropCase.concern, func(t *testing.T) { runDropConcernCase(t, dropCase) })
	}
}

func runDropConcernCase(t *testing.T, dropCase dropConcernCase) {
	// The hold is far shorter than a ship's timeout (half of this), so the held
	// member stays in sync throughout and the concern is not degraded.
	tc := newTestClusterTuned(t, 4, 2, nil, func(_ int, c *Cluster) { c.wcTimeout = 20 * time.Second })
	defer tc.stop()
	placed := ringWith(t, 2, tc.addrs...)
	bucket := bucketNamed(t, "concern", func(string) bool { return true })
	set, err := placed.Replicas(placementOf(testDatabase, bucket).encoded, 2)
	if err != nil {
		t.Fatalf("setup: the set of %s: %v", bucket, err)
	}
	owner, setReplica := nodeIndexByAddr(t, tc.addrs, set[0].ID), nodeIndexByAddr(t, tc.addrs, set[1].ID)
	var outside int
	for index := range tc.addrs {
		if index != owner && index != setReplica {
			outside = index
			break
		}
	}
	held := dropCase.held(setReplica, outside)

	tc.ok(owner, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(owner, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, bucket, 2)
	tc.ok(owner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "k", Data: map[string]interface{}{"n": 1},
		WriteConcern: "all"})

	heldID := tc.addrs[held]
	var entered, released atomic.Bool
	finished := make(chan struct{})
	hold := func(c *Cluster, entry ReplEntry) {
		if c.self.ID != heldID || transport.Opcode(entry.Op) != transport.OpDeleteBucket || entry.Bucket != bucket {
			return
		}
		if !entered.CompareAndSwap(false, true) {
			return
		}
		time.Sleep(2 * time.Second)
		released.Store(true)
		close(finished)
	}
	afterReplicaAdmittedHook.Store(&hold)
	defer afterReplicaAdmittedHook.Store(nil)

	frame := tc.frame(owner, transport.OpDeleteBucket, bson.M{"database": testDatabase, "name": bucket, "write_concern": dropCase.concern})
	answeredWhileHeld := !released.Load()
	if frame.Opcode == transport.OpError {
		var refusal ErrorResponse
		if err := bson.Unmarshal(frame.Payload, &refusal); err != nil {
			t.Fatalf("the drop at %s was refused, and its refusal does not decode: %v", dropCase.concern, err)
		}
		t.Fatalf("the drop at %s was refused: %s", dropCase.concern, refusal.Message)
	}
	select {
	case <-finished:
	case <-time.After(30 * time.Second):
		t.Fatalf("node %d never applied the drop of %s", held+1, bucket)
	}
	if !entered.Load() {
		t.Fatalf("setup: node %d's apply of the drop was never held", held+1)
	}
	if answeredWhileHeld != dropCase.answeredWhileHeld {
		t.Errorf("the drop at %s was answered while node %d held its apply: %v, want %v — %s (R8-C: the concern is the set's at the placed factor)",
			dropCase.concern, held+1, answeredWhileHeld, dropCase.answeredWhileHeld, dropCase.why)
	}
}
