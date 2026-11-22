package cluster

import (
	"slices"
	"strings"
	"testing"
	"time"

	hraft "github.com/hashicorp/raft"
)

func c5Join(t *testing.T, leader *Raft, id string) *Raft {
	t.Helper()
	cfg := quickRaft(t, t.TempDir(), id)
	cfg.Bootstrap = false
	joined := openOrFail(t, cfg)
	if _, err := leader.Join(JoinRequest{Member: Member{ID: id, Incarnation: 1, DataAddr: cfg.Listener.Addr().String()},
		RaftAddr: cfg.Listener.Addr().String(), PlacingSet: make([]byte, 1024)}); err != nil {
		t.Fatalf("join %s: %v", id, err)
	}
	return joined
}

func c5AwaitVoters(t *testing.T, r *Raft, want ...string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !slices.Equal(r.State().Voters, want) {
		if time.Now().After(deadline) {
			t.Fatalf("voters are %v, want %v", r.State().Voters, want)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestC5ReconcileVotersRepairsADriftedState: [GUARD a leader proposes SET_VOTERS for every difference between the
// state's voters and the Raft configuration, in both directions, and proposes nothing when they agree].
func TestC5ReconcileVotersRepairsADriftedState(t *testing.T) {
	leader := openOrFail(t, quickRaft(t, t.TempDir(), "n1"))
	second := c5Join(t, leader, "n2")
	conf, err := leader.configuration()
	if err != nil {
		t.Fatal(err)
	}
	var addr hraft.ServerAddress
	for _, server := range conf.Servers {
		if server.ID == "n2" {
			addr = server.Address
		}
	}
	if changed, err := leader.ReconcileVoters(); err != nil || changed != 0 {
		t.Fatalf("a joined non-voter made reconcile change %d (%v)", changed, err)
	}
	if err := leader.raft.AddVoter("n2", addr, 0, raftApplyTimeout).Error(); err != nil {
		t.Fatal(err)
	}
	for end := time.Now().Add(5 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		seen, err := second.configuration()
		if err == nil && slices.ContainsFunc(seen.Servers, func(s hraft.Server) bool { return s.ID == "n2" && s.Suffrage == hraft.Voter }) {
			break
		}
		if time.Now().After(end) {
			t.Fatal("the follower never saw n2 become a voter")
		}
	}
	if changed, err := second.ReconcileVoters(); err != nil || changed != 0 {
		t.Fatalf("a follower proposed %d changes (%v)", changed, err)
	}
	if got := leader.State().Voters; !slices.Equal(got, []string{"n1"}) {
		t.Fatalf("the test needs a drifted state, voters are %v", got)
	}
	changed, err := leader.ReconcileVoters()
	if err != nil || changed != 1 {
		t.Fatalf("reconcile changed %d, %v", changed, err)
	}
	c5AwaitVoters(t, leader, "n1", "n2")
	if row := leader.State().Members[1]; row.Suffrage != "voter" {
		t.Errorf("n2 is %q after the repair", row.Suffrage)
	}
	seq := leader.State().StateSeq
	if changed, err = leader.ReconcileVoters(); err != nil || changed != 0 || leader.State().StateSeq != seq {
		t.Fatalf("a state that agrees proposed %d changes (%v), state_seq %d -> %d", changed, err, seq, leader.State().StateSeq)
	}
	if err := leader.raft.RemoveServer("n2", 0, raftApplyTimeout).Error(); err != nil {
		t.Fatal(err)
	}
	if changed, err = leader.ReconcileVoters(); err != nil || changed != 1 {
		t.Fatalf("reconcile after a removal changed %d, %v", changed, err)
	}
	c5AwaitVoters(t, leader, "n1")
}

// TestC5ANewLeaderRepairsVotersLeftByADeadLeader: [GUARD the leader dies after AddVoter and before SET_VOTERS; after the
// election the new leader's reconcile makes the state's voters equal the configuration].
func TestC5ANewLeaderRepairsVotersLeftByADeadLeader(t *testing.T) {
	first := openOrFail(t, quickRaft(t, t.TempDir(), "n1"))
	second := c5Join(t, first, "n2")
	third := c5Join(t, first, "n3")
	if _, err := first.Promote("n2"); err != nil {
		t.Fatal(err)
	}
	conf, err := first.configuration()
	if err != nil {
		t.Fatal(err)
	}
	var addr hraft.ServerAddress
	for _, server := range conf.Servers {
		if server.ID == "n3" {
			addr = server.Address
		}
	}
	// Promote's first half: the library has n3 as a voter, SET_VOTERS never comes.
	if err := first.raft.AddVoter("n3", addr, 0, raftApplyTimeout).Error(); err != nil {
		t.Fatal(err)
	}
	// n3 must hold the entry that made it a voter, or the survivors cannot form a majority of three.
	for end := time.Now().Add(10 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		seen, err := third.configuration()
		if err == nil && slices.ContainsFunc(seen.Servers, func(s hraft.Server) bool { return s.ID == "n3" && s.Suffrage == hraft.Voter }) {
			break
		}
		if time.Now().After(end) {
			t.Fatal("n3 never saw itself become a voter")
		}
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	var leader *Raft
	deadline := time.Now().Add(15 * time.Second)
	for leader == nil {
		for _, r := range []*Raft{second, third} {
			if r.IsLeader() {
				leader = r
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("no new leader was elected")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := leader.State().Voters; slices.Contains(got, "n3") {
		t.Fatalf("the state already lists n3 as a voter: %v", got)
	}
	for end := time.Now().Add(10 * time.Second); ; {
		if _, err := leader.ReconcileVoters(); err == nil && slices.Contains(leader.State().Voters, "n3") {
			break
		}
		if time.Now().After(end) {
			t.Fatalf("voters are %v, want n3 among them", leader.State().Voters)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := leader.State().Voters; !slices.Equal(got, []string{"n1", "n2", "n3"}) {
		t.Errorf("voters are %v, want n1 n2 n3", got)
	}
}

// TestC5ReconcileLeavesAWitnessVoterAlone: [GUARD a witness is a state voter by spec (it joins as a Raft non-voter and
// is granted its vote by SET_VOTERS), so the reconciler neither removes it nor re-adds it].
func TestC5ReconcileLeavesAWitnessVoterAlone(t *testing.T) {
	leader := openOrFail(t, quickRaft(t, t.TempDir(), "n1"))
	cfg := quickRaft(t, t.TempDir(), "w")
	cfg.Bootstrap = false
	openOrFail(t, cfg)
	if _, err := leader.Join(JoinRequest{Member: Member{ID: "w", Incarnation: 1, DataAddr: cfg.Listener.Addr().String(), Witness: true},
		RaftAddr: cfg.Listener.Addr().String(), PlacingSet: make([]byte, 1024)}); err != nil {
		t.Fatal(err)
	}
	if _, err := leader.propose(CommandSetVoters, SetVotersBody("w", true)); err != nil {
		t.Fatal(err)
	}
	seq := leader.State().StateSeq
	for range 3 {
		if changed, err := leader.ReconcileVoters(); err != nil || changed != 0 {
			t.Fatalf("reconcile changed %d (%v) with a witness voter", changed, err)
		}
	}
	if got := leader.State(); !slices.Equal(got.Voters, []string{"n1", "w"}) || got.StateSeq != seq {
		t.Fatalf("the witness vote was undone: voters %v, state_seq %d -> %d", got.Voters, seq, got.StateSeq)
	}
}

// TestC5ReconcileReportsAVoterWithNoMemberRowOnce: [GUARD a configured voter with no member row is reported by name, once,
// not skipped silently and not on every step].
func TestC5ReconcileReportsAVoterWithNoMemberRowOnce(t *testing.T) {
	leader := openOrFail(t, quickRaft(t, t.TempDir(), "n1"))
	c5Join(t, leader, "n2")
	if _, err := leader.Promote("n2"); err != nil {
		t.Fatal(err)
	}
	if _, err := leader.propose(CommandRemoveMember, fsmDoc(fsmStr("id", "n2"))); err != nil {
		t.Fatal(err)
	}
	_, err := leader.ReconcileVoters()
	if err == nil || !strings.Contains(err.Error(), "n2") || !strings.Contains(err.Error(), "member row") {
		t.Fatalf("the orphan was not reported by name: %v", err)
	}
	if _, err := leader.ReconcileVoters(); err != nil {
		t.Fatalf("the same orphan was reported twice: %v", err)
	}
}
