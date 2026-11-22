package server

import (
	"slices"
	"testing"
	"time"
)

// TestJudgeJoinFreeze is the judge of join-freeze spec §3.2, §6.1 and §8.4 on
// its own: the pause, who is waited on and why, who is stalled, and the
// counts. Q1's guard is here (review F-5): a member down across the join keeps
// the pause; exempting unreachable members fails that row.
func TestJudgeJoinFreeze(t *testing.T) {
	const join = 5
	members := []string{"n1", "n2", "n3", "n4"}
	joined := map[string]uint64{"n4": join}
	cases := []struct {
		name          string
		in            joinFreezeInput
		paused        bool
		joiners       []string
		waitingOn     []waitedOn
		stalled       []string
		copied, total int
		complete      bool
	}{
		{
			name:   "founders of a cluster that never changed membership",
			in:     joinFreezeInput{epoch: 1, self: "n1", members: []string{"n1", "n2", "n3"}},
			paused: false,
		},
		{
			name: "a join is paused until every member has settled it",
			in: joinFreezeInput{epoch: join, self: "n1", members: members, joinedAt: joined, ownSettled: join,
				settledOf: map[string]uint64{"n2": join, "n3": join - 1, "n4": join},
				handOff:   map[string]HandOffProgress{"n3": {Epoch: join, Total: 10, Done: 4}}},
			paused:    true,
			joiners:   []string{"n4"},
			waitingOn: []waitedOn{{Node: "n3", State: "still handing off (6 left)"}},
			copied:    4, total: 10,
		},
		{
			name: "every member settled: allowed",
			in: joinFreezeInput{epoch: join, self: "n1", members: members, joinedAt: joined, ownSettled: join,
				settledOf: map[string]uint64{"n2": join, "n3": join, "n4": join}},
			paused: false,
		},
		{
			name: "a member settled and then down is not waited on by itself",
			in: joinFreezeInput{epoch: join, self: "n1", members: members, joinedAt: joined, ownSettled: join - 1,
				settledOf: map[string]uint64{"n2": join, "n3": join, "n4": join},
				handOff:   map[string]HandOffProgress{"n1": {Epoch: join, Total: 3, Done: 2}},
				downFor:   map[string]time.Duration{"n3": time.Minute}},
			paused:    true,
			joiners:   []string{"n4"},
			waitingOn: []waitedOn{{Node: "n1", State: "still handing off (1 left)"}},
			copied:    2, total: 3,
		},
		{
			name: "a member down before it settled is waited on as unreachable, and the holders waiting on it are stalled",
			in: joinFreezeInput{epoch: join, self: "n1", members: members, joinedAt: joined, ownSettled: join - 1,
				settledOf: map[string]uint64{"n2": join - 1, "n3": join - 1, "n4": join},
				handOff: map[string]HandOffProgress{
					"n1": {Epoch: join, Total: 4, Done: 1, WaitingOn: []string{"n3"}},
					"n2": {Epoch: join, Total: 6, Done: 2, WaitingOn: []string{"n3"}},
				},
				downFor: map[string]time.Duration{"n3": 312 * time.Second}},
			paused:    true,
			joiners:   []string{"n4"},
			waitingOn: []waitedOn{{Node: "n3", State: "unreachable for 5m12s", Down: true}},
			stalled:   []string{"n1", "n2"},
			copied:    3, total: 10,
		},
		{
			name: "Q1: a member down across the join keeps it paused",
			in: joinFreezeInput{epoch: join, self: "n1", members: members, joinedAt: joined, ownSettled: join,
				settledOf: map[string]uint64{"n2": join, "n3": join - 2, "n4": join},
				downFor:   map[string]time.Duration{"n3": time.Hour}},
			paused:    true,
			joiners:   []string{"n4"},
			waitingOn: []waitedOn{{Node: "n3", State: "unreachable for 1h0m0s", Down: true}},
		},
		{
			name: "a removal after a settled join is allowed",
			in: joinFreezeInput{epoch: join + 1, self: "n1", members: []string{"n1", "n2", "n4"}, joinedAt: joined, ownSettled: join,
				settledOf: map[string]uint64{"n2": join, "n4": join}},
			paused: false,
		},
		{
			name: "the joiner removed while members are still below its join stays paused",
			in: joinFreezeInput{epoch: join + 1, self: "n1", members: []string{"n1", "n2", "n3"}, joinedAt: joined, ownSettled: join - 1,
				settledOf: map[string]uint64{"n2": join - 1, "n3": join - 1}},
			paused:  true,
			joiners: []string{"n4"},
			waitingOn: []waitedOn{
				{Node: "n1", State: "has not reported since n4 joined"},
				{Node: "n2", State: "has not reported since n4 joined"},
				{Node: "n3", State: "has not reported since n4 joined"},
			},
		},
		{
			name: "two joiners: the first settled, the second not",
			in: joinFreezeInput{epoch: join + 1, self: "n1", members: append(slices.Clone(members), "n5"),
				joinedAt: map[string]uint64{"n4": join, "n5": join + 1}, ownSettled: join + 1,
				settledOf: map[string]uint64{"n2": join + 1, "n3": join, "n4": join + 1, "n5": join + 1},
				handOff:   map[string]HandOffProgress{"n3": {Epoch: join + 1, Total: 2, Done: 0, GaveUp: true}}},
			paused:    true,
			joiners:   []string{"n5"},
			waitingOn: []waitedOn{{Node: "n3", State: "gave up handing off 2 buckets for now; retrying while its members answer"}},
			total:     2,
		},
		{
			name: "every member reported: the counts are complete",
			in: joinFreezeInput{epoch: join, self: "n1", members: members, joinedAt: joined, ownSettled: join - 1,
				settledOf: map[string]uint64{"n2": join, "n3": join, "n4": join},
				handOff: map[string]HandOffProgress{
					"n1": {Epoch: join, Total: 5, Done: 3}, "n2": {Epoch: join, Total: 2, Done: 2},
					"n3": {Epoch: join, Total: 1, Done: 1}, "n4": {Epoch: join},
				}},
			paused:    true,
			joiners:   []string{"n4"},
			waitingOn: []waitedOn{{Node: "n1", State: "still handing off (2 left)"}},
			copied:    6, total: 8, complete: true,
		},
		{
			name: "a value relayed for this node is not its own",
			in: joinFreezeInput{epoch: join, self: "n1", members: members, joinedAt: joined, ownSettled: join - 1,
				settledOf: map[string]uint64{"n1": join, "n2": join, "n3": join, "n4": join}},
			paused:    true,
			joiners:   []string{"n4"},
			waitingOn: []waitedOn{{Node: "n1", State: "has not reported since n4 joined"}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			report := judgeJoinFreeze(tc.in)
			if report.Epoch != tc.in.epoch {
				t.Errorf("report for epoch %d, want %d", report.Epoch, tc.in.epoch)
			}
			if report.Paused != tc.paused {
				t.Fatalf("paused %v, want %v (report %+v)", report.Paused, tc.paused, report)
			}
			if !slices.Equal(report.Joiners, tc.joiners) {
				t.Errorf("joiners %v, want %v", report.Joiners, tc.joiners)
			}
			if !slices.Equal(report.WaitingOn, tc.waitingOn) {
				t.Errorf("waiting on %+v, want %+v", report.WaitingOn, tc.waitingOn)
			}
			if !slices.Equal(report.Stalled, tc.stalled) {
				t.Errorf("stalled %v, want %v", report.Stalled, tc.stalled)
			}
			if tc.paused && (report.Copied != tc.copied || report.Total != tc.total || report.CountsComplete != tc.complete) {
				t.Errorf("counts %d of %d (complete %v), want %d of %d (complete %v)",
					report.Copied, report.Total, report.CountsComplete, tc.copied, tc.total, tc.complete)
			}
		})
	}
}
