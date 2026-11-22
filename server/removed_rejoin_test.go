package server

import (
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// joinThrough sends the operator's `join` for address to node index and
// returns the refusal's text, or "" when the join was accepted.
func joinThrough(t *testing.T, tc *testCluster, index int, address string) string {
	t.Helper()
	frame := tc.frame(index, transport.OpClusterAdd, &ClusterAddRequest{Address: address})
	if frame.Opcode == transport.OpError {
		return string(frame.Payload)
	}
	var resp ClusterAddResponse
	if err := bson.Unmarshal(frame.Payload, &resp); err != nil {
		t.Fatalf("decoding the join's answer: %v", err)
	}
	if !resp.Success {
		return resp.Error
	}
	return ""
}

// TestAJoinOfANodeThatRemembersItsRemovalIsRefused: a node removed from the
// cluster keeps its record of the removal and refuses every write while it
// has it. Added back as it is, it would be a member that refuses writes to
// every bucket it owns. The join is refused, and says to empty its data
// directory first; the membership does not change. Guard: no check — the join
// commits.
func TestAJoinOfANodeThatRemembersItsRemovalIsRefused(t *testing.T) {
	tc := newTestCluster(t, 4, 3)
	defer tc.stop()
	const removedIndex = 3
	removeAndTell(t, tc, 0, removedIndex)
	proposer := clusterOf(tc, 0)
	epoch := proposer.Epoch()

	removedID := tc.addrs[removedIndex]
	refusal := joinThrough(t, tc, 0, removedID+"@"+removedID)
	if refusal == "" {
		t.Fatalf("a node that was removed and keeps its data directory was added back: it refuses every write to the buckets it now owns")
	}
	for _, want := range []string{"empty its data directory", "once the members hold its buckets"} {
		if !strings.Contains(refusal, want) {
			t.Errorf("the refusal does not say %q: %s", want, refusal)
		}
	}
	if got := proposer.Epoch(); got != epoch {
		t.Errorf("the refused join moved the membership from epoch %d to %d", epoch, got)
	}
}

// TestAJoinOfANodeThatCannotBeAskedIsRefused: a join adds a running node. One
// that does not answer cannot say whether it remembers a removal, and on a
// plaintext cluster nothing else asks it anything, so the join is refused
// with what to do — start it and join again — and the membership does not
// change. Guard: let an unanswered probe through — the join commits.
func TestAJoinOfANodeThatCannotBeAskedIsRefused(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	proposer := clusterOf(tc, 0)
	epoch := proposer.Epoch()
	absent := fabricatedMemberAddr(t)

	refusal := joinThrough(t, tc, 0, "absent@"+absent)
	if refusal == "" {
		t.Fatalf("a join of %s, which nothing answers at, was accepted", absent)
	}
	if !strings.Contains(refusal, "start it") {
		t.Errorf("the refusal does not say to start the node and join again: %s", refusal)
	}
	if got := proposer.Epoch(); got != epoch {
		t.Errorf("the refused join moved the membership from epoch %d to %d", epoch, got)
	}
}
