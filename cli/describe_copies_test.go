package cli

import (
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/server"
)

// What `describe bucket` says of a bucket's copies (bucket-replication-rev8-spec
// §12.2; the G4 to G7 build doc step 10): the owner's count, who is in the
// set, why the count is missing, and a member's own pending copy.

func TestDescribeBucketSaysWhatTheOwnerKnowsOfItsCopies(t *testing.T) {
	now := time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)
	nodes := []string{"node-a", "node-b", "node-c"}
	cases := []struct {
		name string
		resp server.BucketStatsResponse
		want []fact
	}{
		{"healthy", server.BucketStatsResponse{ReplicationFactor: 3, InSync: 3, State: server.StateOK, Nodes: nodes},
			[]fact{{"Copies", "3 of 3 in sync"}, {"Nodes", "node-a (owner), node-b, node-c"}}},
		{"a copy pending", server.BucketStatsResponse{
			ReplicationFactor: 3, InSync: 1, State: server.StateCopying, Nodes: nodes,
			PendingCopies: []server.PendingCopy{{Node: "node-b", Since: now.Add(-4 * time.Hour).Unix()}, {Node: "node-c", Since: now.Add(-12 * time.Minute).Unix()}},
		}, []fact{{"Copies", "1 of 3 in sync, copying to node-b (4h), node-c (12m)"}, {"Nodes", "node-a (owner), node-b, node-c"}}},
		{"a stalled addressee", server.BucketStatsResponse{
			ReplicationFactor: 3, InSync: 2, State: server.StateUnderReplicated, Nodes: nodes, NotInSync: []string{"node-c"},
		}, []fact{{"Copies", "2 of 3 in sync, node-c not in sync"}, {"Nodes", "node-a (owner), node-b, node-c"}}},
		{"fewer members than the factor", server.BucketStatsResponse{
			ReplicationFactor: 3, InSync: 2, State: server.StateUnderReplicated, Nodes: nodes[:2],
		}, []fact{{"Copies", "2 of 3 in sync, under-replicated: 2 members"}, {"Nodes", "node-a (owner), node-b"}}},
		{"owner unreachable", server.BucketStatsResponse{ReplicationFactor: 3, State: server.StateOwnerUnreachable},
			[]fact{{"Copies", "unknown: the owner did not answer, so this node cannot count the copies"}}},
		{"taking over", server.BucketStatsResponse{ReplicationFactor: 3, State: server.StateTakingOver, PreviousOwner: "node-x"},
			[]fact{{"Copies", "unknown: the owner is taking this bucket over from node-x and cannot count the copies yet"}}},
		{"taking over, previous owner unknown", server.BucketStatsResponse{ReplicationFactor: 3, State: server.StateTakingOver},
			[]fact{{"Copies", "unknown: the owner is taking this bucket over and cannot count the copies yet"}}},
		{"a member's own pending copy", server.BucketStatsResponse{
			ReplicationFactor: 3, InSync: 1, State: server.StateCopying, Nodes: nodes,
			PendingCopies: []server.PendingCopy{{Node: "node-b", Since: now.Add(-4 * time.Hour).Unix()}},
			PendingCopy:   "pending copy here: since 2026-10-20T08:00:00Z, not tried yet, next retry 2026-10-20T08:00:00Z",
		}, []fact{
			{"Copies", "1 of 3 in sync, copying to node-b (4h)"}, {"Nodes", "node-a (owner), node-b, node-c"},
			{"Pending copy here", "since 2026-10-20T08:00:00Z, not tried yet, next retry 2026-10-20T08:00:00Z"},
		}},
		{"a node with no one to copy to", server.BucketStatsResponse{ReplicationFactor: 3, InSync: 1, State: server.StateStandalone},
			[]fact{{"Copies", "1 (standalone: no other member to copy to)"}}},
		{"a node with no one to copy to, at one", server.BucketStatsResponse{ReplicationFactor: 1, InSync: 1, State: server.StateStandalone}, nil},
		{"a server that sends no view", server.BucketStatsResponse{ReplicationFactor: 3}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := copyFacts(tc.resp, now)
			if len(got) != len(tc.want) {
				t.Fatalf("describe prints %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Errorf("row %d is %q: %q, want %q: %q", i, got[i].label, got[i].value, tc.want[i].label, tc.want[i].value)
				}
			}
		})
	}
}
