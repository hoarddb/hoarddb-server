package server

import (
	"strings"
	"sync/atomic"
	"testing"

	"github.com/hoarddb/hoarddb-server/transport"
)

// TestClusterBatchMutationCheckOnRemoteItems is T-C14's remote half: a
// coordinator whose membership mismatch is set refuses a write item bound for
// another node before it sends anything (batchCoordinator.refuseBeforeSend),
// though that owner would itself accept it. The refused item is not_applied,
// the later items of its bucket's chain are not run and name it, nothing is
// forwarded for either bucket, and a read for a third owner, which a lone read
// never meets the check on, is still served.
func TestClusterBatchMutationCheckOnRemoteItems(t *testing.T) {
	tc := newBatchCluster(t, 3, 1, 32, nil)
	chain := usersOwnedBy(t, tc, 1, 32, 2)
	reader := usersOwnedBy(t, tc, 2, 32, 1)[0]
	c := tc.nodes[0].server.core.cluster

	var sentForChain atomic.Int64
	isChainBucket := func(bucket string) bool {
		return bucket == userBucket(chain[0]) || bucket == userBucket(chain[1])
	}
	batchHook := func(entries []ReplEntry, target string) {
		for _, entry := range entries {
			if isChainBucket(entry.Bucket) {
				sentForChain.Add(1)
			}
		}
	}
	forwardBatchTargetHook.Store(&batchHook)
	t.Cleanup(func() { forwardBatchTargetHook.Store(nil) })
	loneHook := func(entry ReplEntry, target string) {
		if isChainBucket(entry.Bucket) {
			sentForChain.Add(1)
		}
	}
	forwardTargetHook.Store(&loneHook)
	t.Cleanup(func() { forwardTargetHook.Store(nil) })

	c.mismatch.Store(true)
	t.Cleanup(func() { c.mismatch.Store(false) })
	answers := sendClusterBatch(t, tc, 0, BatchRequest{Items: []BatchItem{
		clusterPush(t, chain[0], map[string]interface{}{"n": 1}),
		clusterPush(t, chain[0], map[string]interface{}{"n": 2}),
		batchItemOf(t, transport.OpFieldLen, clusterUserField(reader, "Feed")),
		clusterPush(t, chain[1], map[string]interface{}{"n": 1}),
	}})
	c.mismatch.Store(false)

	first := batchAnswerError(t, answers[0], "item 1")
	if first.WriteOutcome != outcomeNotApplied || transport.ErrorCode(first.Code) != transport.ErrConflict {
		t.Errorf("item 1 was answered code %d, write_outcome %q; want the membership refusal, %q: %s",
			first.Code, first.WriteOutcome, outcomeNotApplied, first.Message)
	}
	if !strings.Contains(first.Message, "nodes history") {
		t.Errorf("item 1's refusal does not name the command to run: %s", first.Message)
	}
	stopped := batchAnswerError(t, answers[1], "item 2")
	requireOutcome(t, stopped, "item 2", outcomeNotApplied, transport.ErrUnavailable)
	if !strings.Contains(stopped.Message, "Not run: item 1 of 4") {
		t.Errorf("item 2 does not name item 1: %s", stopped.Message)
	}
	if got := batchAnswerField(t, answers[2], "item 3, a read of another owner").Len; got != 0 {
		t.Errorf("the read answered length %d, want 0", got)
	}
	other := batchAnswerError(t, answers[3], "item 4")
	if other.WriteOutcome != outcomeNotApplied || transport.ErrorCode(other.Code) != transport.ErrConflict {
		t.Errorf("item 4, the other bucket's write, was answered code %d, write_outcome %q; want the membership refusal, %q: %s",
			other.Code, other.WriteOutcome, outcomeNotApplied, other.Message)
	}
	if got := sentForChain.Load(); got != 0 {
		t.Fatalf("%d entries for the refused buckets were sent to their owner; the coordinator refuses before it sends", got)
	}
	for _, u := range chain {
		if n := clusterFeedLen(t, tc, ownerOfUser(t, tc, u), u); n != 0 {
			t.Errorf("User_%d holds %d pushes; a refused batch applies none", u, n)
		}
	}
}
