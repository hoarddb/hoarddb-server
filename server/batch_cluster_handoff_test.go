package server

import (
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/transport"
)

// TestClusterBatchBucketInHandoff is T-C3, and the hand-off half of T-14: an
// item whose bucket is moving to this node, which has not taken it over, is
// refused not_applied with the hand-off's class and sentence before anything
// is logged; the later items of that bucket are not_applied naming it; the
// items of a bucket this node already serves are applied. Break: let a write
// through a bucket awaiting its take-over, or let the chain go on after a
// refused item.
func TestClusterBatchBucketInHandoff(t *testing.T) {
	takeOverOffHook.Store(true)
	t.Cleanup(func() { takeOverOffHook.Store(false) })
	const users = 24
	tc := newBatchCluster(t, 3, 3, users, nil)
	moving := &movedBucket{tc: tc, key: placementOf(testDatabase, userBucket(1))}
	moving.oldOwner = ownerOfUser(t, tc, 1)
	moving.newOwner = futureOwnerAfterRemoving(t, tc, moving.key, moving.oldOwner)
	moving.survivor = 3 - moving.oldOwner - moving.newOwner
	// A bucket the new owner already owns is not moved by the removal.
	kept := usersOwnedBy(t, tc, moving.newOwner, users, 1)[0]
	moving.removeOldOwner(t)
	c := clusterOf(tc, moving.newOwner)
	waitForPlacementPassOf(t, c)
	for _, check := range []struct {
		user   int
		awaits bool
	}{{1, true}, {kept, false}} {
		key := placementOf(testDatabase, userBucket(check.user))
		_, epoch, err := c.ownerView(key)
		if err != nil {
			t.Fatalf("setup: reading the owner of %s: %v", userBucket(check.user), err)
		}
		awaits, err := c.ownerAwaitsTakeOver(key, epoch, testDatabase, userBucket(check.user))
		if err != nil || awaits != check.awaits {
			t.Fatalf("setup: %s awaits its take-over: %v (err %v), want %v", userBucket(check.user), awaits, err, check.awaits)
		}
	}

	answers := sendClusterBatch(t, tc, moving.newOwner, BatchRequest{Items: []BatchItem{
		clusterPush(t, 1, map[string]interface{}{"n": 1}),
		clusterPush(t, kept, map[string]interface{}{"n": 1}),
		clusterPush(t, 1, map[string]interface{}{"n": 2}),
		clusterPush(t, kept, map[string]interface{}{"n": 2}),
	}})
	refused := batchAnswerError(t, answers[0], "item 1, the bucket in hand-off")
	requireOutcome(t, refused, "item 1", outcomeNotApplied, transport.ErrUnavailable)
	if !strings.Contains(refused.Message, "is moving to this node") {
		t.Errorf("item 1 reads %q, want the hand-off's sentence", refused.Message)
	}
	stopped := batchAnswerError(t, answers[2], "item 3, later in the bucket in hand-off")
	requireOutcome(t, stopped, "item 3", outcomeNotApplied, transport.ErrUnavailable)
	if !strings.Contains(stopped.Message, "item 1 of 4") {
		t.Errorf("item 3 reads %q, want it to name item 1", stopped.Message)
	}
	batchAnswerField(t, answers[1], "item 2, a bucket this node serves")
	batchAnswerField(t, answers[3], "item 4, a bucket this node serves")
	if n := clusterFeedLen(t, tc, moving.newOwner, kept); n != 2 {
		t.Errorf("the bucket this node serves holds %d pushes, want 2", n)
	}
}
