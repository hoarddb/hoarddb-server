package server

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// 'nodes history' on the server: OpListNodes with history asked (spec §6.5.2).

// listNodes asks node index for its nodes, with or without its history.
func listNodes(t *testing.T, tc *testCluster, index int, history bool) (NodeListResponse, []byte) {
	t.Helper()
	frame := tc.ok(index, transport.OpListNodes, NodeListRequest{History: history})
	var resp NodeListResponse
	if err := bson.Unmarshal(frame.Payload, &resp); err != nil {
		t.Fatalf("decoding node %d's list: %v", index+1, err)
	}
	return resp, frame.Payload
}

// TestNodesHistoryListsEveryRecordedEpoch: after a join, a removal of the
// joiner and a removal of node 3, node 1 answers every epoch with its set,
// oldest first; node 3, removed and running, still answers its history.
// Guard: handleListNodes fills the history when asked.
func TestNodesHistoryListsEveryRecordedEpoch(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	proposer := clusterOf(tc, 0)
	fabricated := fabricatedMemberAddr(t)
	if err := proposer.proposeJoin(commandContext(t), ringNodeAt(fabricated)); err != nil {
		t.Fatalf("joining %s: %v", fabricated, err)
	}
	if err := proposer.proposeRemoval(commandContext(t), fabricated); err != nil {
		t.Fatalf("removing %s: %v", fabricated, err)
	}
	if err := proposer.proposeRemoval(commandContext(t), tc.addrs[2]); err != nil {
		t.Fatalf("removing node 3: %v", err)
	}
	want := [][]string{
		tc.addrs,
		append(slices.Clone(tc.addrs), fabricated),
		tc.addrs,
		tc.addrs[:2],
	}
	resp, _ := listNodes(t, tc, 0, true)
	if !resp.HistoryReported || len(resp.MembershipHistory) != len(want) {
		t.Fatalf("node 1 answered history_reported=%v with %d records, want %d", resp.HistoryReported, len(resp.MembershipHistory), len(want))
	}
	for index, record := range resp.MembershipHistory {
		var ids []string
		for _, peer := range record.Members {
			ids = append(ids, peer.ID)
		}
		if record.Epoch != InitialEpoch+uint64(index) || !sameMemberIDs(ids, want[index]) {
			t.Errorf("record %d is epoch %d %v, want epoch %d %v", index, record.Epoch, ids, InitialEpoch+uint64(index), want[index])
		}
	}
	if !holdsWithin(10*time.Second, clusterOf(tc, 2).removed) {
		t.Fatal("node 3 never learned of its removal")
	}
	removedAnswer, _ := listNodes(t, tc, 2, true)
	if !removedAnswer.HistoryReported || len(removedAnswer.MembershipHistory) == 0 {
		t.Errorf("node 3, removed and running, answered history_reported=%v with %d records",
			removedAnswer.HistoryReported, len(removedAnswer.MembershipHistory))
	}
}

// TestNodesWithoutHistoryAnswersAsBefore: a list not asking for the history
// carries neither history field, so the answer's bytes are what they were.
// Guard: the history is filled only when asked.
func TestNodesWithoutHistoryAnswersAsBefore(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	_, payload := listNodes(t, tc, 0, false)
	for _, key := range []string{"membership_history", "history_reported"} {
		if _, err := bson.Raw(payload).LookupErr(key); err == nil {
			t.Errorf("a list that did not ask for the history carries %q", key)
		}
	}
}

// TestNodesAcceptsAnEmptyPayload: a list with an empty payload, as older
// clients send, is answered, not refused for a payload that does not decode.
// Guard: handleListNodes decodes only a non-empty payload.
func TestNodesAcceptsAnEmptyPayload(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	frame, err := tc.nodes[0].client.SendRequest(ctx, transport.OpListNodes, nil)
	if err != nil {
		t.Fatalf("sending an empty list request: %v", err)
	}
	if frame.Opcode != transport.OpGetResponse {
		t.Errorf("an empty list request was answered %s: %s", frame.Opcode, string(frame.Payload))
	}
}
