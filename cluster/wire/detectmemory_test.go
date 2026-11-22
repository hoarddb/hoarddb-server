package wire

import (
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"slices"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
)

func stepTwoBState(states map[string]cluster.MemberState) cluster.ClusterState {
	var state cluster.ClusterState
	for id, memberState := range states {
		state.Members = append(state.Members, cluster.Member{ID: id, State: memberState})
	}
	return state
}

// TestAReportMadeToAGoneLeaderIsMadeAgainToTheNewOne: D1. The detector returns a report once
// per failure run. The ledger keeps it until the subject answers or the committed state
// shows it down, so a leader change can send it to whoever leads now.
// [GUARD: forget the report when it is sent]
func TestAReportMadeToAGoneLeaderIsMadeAgainToTheNewOne(t *testing.T) {
	ledger := newFailureLedger()
	ledger.noteReported("n3")
	ledger.noteReported("n4")
	ledger.noteAnswered("n4")
	if got := ledger.reportsForNewLeader(); !slices.Equal(got, []string{"n3"}) {
		t.Fatalf("after n4 answered, a new leader is owed %v, want [n3]", got)
	}
	ledger.noteState(stepTwoBState(map[string]cluster.MemberState{"n3": cluster.MemberDown}))
	if got := ledger.reportsForNewLeader(); len(got) != 0 {
		t.Fatalf("once n3 is committed down it is owed nothing, got %v", got)
	}
}

// TestADownProposalThatWasLostIsRetriedUntilTheStateShowsIt: D1, second half. [GUARD: drop
// the entry when it is attempted; ignore the throttle]
func TestADownProposalThatWasLostIsRetriedUntilTheStateShowsIt(t *testing.T) {
	ledger := newFailureLedger()
	silent := func(string) bool { return false }
	ledger.noteProposedDown("n3", time.Second)
	if got := ledger.downToRetry(time.Second+50*time.Millisecond, 100*time.Millisecond, silent); len(got) != 0 {
		t.Fatalf("a proposal attempted 50 ms ago was retried: %v", got)
	}
	if got := ledger.downToRetry(2*time.Second, 100*time.Millisecond, silent); !slices.Equal(got, []string{"n3"}) {
		t.Fatalf("a lost proposal was not retried: %v", got)
	}
	if got := ledger.downToRetry(2*time.Second+10*time.Millisecond, 100*time.Millisecond, silent); len(got) != 0 {
		t.Fatalf("the retry did not restart the throttle: %v", got)
	}
	if got := ledger.downToRetry(5*time.Second, 100*time.Millisecond, func(string) bool { return true }); len(got) != 0 {
		t.Fatalf("a subject that answers again was proposed down: %v", got)
	}
	ledger.noteProposedDown("n5", time.Second)
	ledger.noteState(stepTwoBState(map[string]cluster.MemberState{"n5": cluster.MemberDown}))
	if got := ledger.downToRetry(9*time.Second, 100*time.Millisecond, silent); slices.Contains(got, "n5") {
		t.Fatalf("a member the state shows down was proposed down again: %v", got)
	}
}

// TestAProbeThatSpannedAStallIsNotEvidence: a long gap between supervisor steps means this
// process was away; the probe that timed out across it says nothing about the subject.
// [GUARD: never mark a stall; discard answered probes too]
func TestAProbeThatSpannedAStallIsNotEvidence(t *testing.T) {
	var guard stallGuard
	limit := 3 * time.Second
	guard.observe(10*time.Second, limit)
	guard.observe(10*time.Second+100*time.Millisecond, limit)
	if guard.unreliable(10*time.Second, false) {
		t.Fatal("a probe was discarded although the steps were 100 ms apart")
	}
	started := 10*time.Second + 150*time.Millisecond
	guard.observe(40*time.Second, limit) // the process was stopped for 30 s
	if !guard.unreliable(started, false) {
		t.Fatal("a failed probe that began before the stall was kept")
	}
	if guard.unreliable(started, true) {
		t.Fatal("an answered probe was discarded: an answer needs no excuse")
	}
	if guard.unreliable(40*time.Second+time.Millisecond, false) {
		t.Fatal("a failed probe that began after the stall was discarded")
	}
}

// TestTheDownTimeIsTheEarliestAcrossLeaderChanges (pi F14): feeding the detector "down now"
// at every election would restart the stage-2 timer each time, so a leader flapping faster
// than dead_ms would keep a down member from ever reaching dead. [GUARD: return now]
func TestTheDownTimeIsTheEarliestAcrossLeaderChanges(t *testing.T) {
	ledger := newFailureLedger()
	first := ledger.noteDown("n3", 10*time.Second)
	for election := 1; election <= 5; election++ {
		if got := ledger.noteDown("n3", 10*time.Second+time.Duration(election)*time.Minute); got != first {
			t.Fatalf("election %d fed %s, want the first time %s", election, got, first)
		}
	}
	ledger.forgetDown(stepTwoBState(map[string]cluster.MemberState{"n3": cluster.MemberActive}))
	if got := ledger.noteDown("n3", time.Hour); got != time.Hour {
		t.Fatalf("a member that came back and went down again kept the old time: %s", got)
	}
}

type stepTwoBClock struct{ now time.Duration }

func (c *stepTwoBClock) BootNow() time.Duration { return c.now }

// stepTwoBSupervisor is a supervisor with no node behind it: enough to drive the parts that
// decide, which is where the wiring between the detector and the ledger lives.
func stepTwoBSupervisor(clock *stepTwoBClock, members map[string]cluster.MemberState) *supervisor {
	s := newSupervisor(nil)
	s.clock = clock
	for id, state := range members {
		s.state.Members = append(s.state.Members, cluster.Member{ID: id, State: state})
	}
	s.state.Settings.ProbeIntervalMs, s.state.Settings.HeartbeatMs = 1000, 100
	s.cfg = machine.DetectorConfig{
		SelfID: "n1", ProbeInterval: time.Second, ProbeTimeout: 500 * time.Millisecond, Failures: 3,
		Failover: 5 * time.Second, Dead: 600 * time.Second, Voter: true,
		MemberIDs: []string{"n1", "n2", "n3"}, VoterIDs: []string{"n1", "n2", "n3"},
	}
	return s
}

// TestALeaderChangeOwesTheNewLeaderEveryReport (pi: the wiring, not the ledger). [GUARD: drop
// the resend loop in onLeaderLocked]
func TestALeaderChangeOwesTheNewLeaderEveryReport(t *testing.T) {
	s := stepTwoBSupervisor(&stepTwoBClock{now: time.Second}, map[string]cluster.MemberState{"n1": cluster.MemberActive, "n2": cluster.MemberActive, "n3": cluster.MemberActive})
	s.onLeaderLocked("n2")
	s.ledger.noteReported("n3")
	s.onLeaderLocked("n1")
	if !s.unsent["n3"] {
		t.Fatal("a leader change did not mark the report owed to the new leader")
	}
}

// TestALeaderThatChangesFasterThanDeadMsStillReachesDead (pi F14, through the supervisor):
// every election feeds the detector the time the member was first seen down. [GUARD: feed
// now instead]
func TestALeaderThatChangesFasterThanDeadMsStillReachesDead(t *testing.T) {
	clock := &stepTwoBClock{now: 10 * time.Second}
	s := stepTwoBSupervisor(clock, map[string]cluster.MemberState{"n1": cluster.MemberActive, "n2": cluster.MemberActive, "n3": cluster.MemberDown})
	s.onLeaderLocked("n1")
	s.refeedDownLocked() // the state applied: n3 went down at 10 s
	for _, leader := range []string{"n2", "n1", "n2", "n1"} {
		clock.now += 200 * time.Second
		s.onLeaderLocked(leader)
	}
	act := s.det.Apply(machine.ProbeTick{At: clock.now}, s.cfg)
	if len(act.ProposeDead) != 1 || act.ProposeDead[0] != "n3" {
		t.Fatalf("after four elections 800 s on, ProposeDead = %v, want [n3]: the down time was reset", act.ProposeDead)
	}
}

// TestAProbeFailureAcrossAStallIsDropped (pi: the wiring). [GUARD: skip the stall check in
// probeVerdictLocked]
func TestAProbeFailureAcrossAStallIsDropped(t *testing.T) {
	clock := &stepTwoBClock{now: 10 * time.Second}
	s := stepTwoBSupervisor(clock, map[string]cluster.MemberState{"n1": cluster.MemberActive, "n2": cluster.MemberActive, "n3": cluster.MemberActive})
	s.stall.observe(10*time.Second, 3*time.Second)
	s.stall.observe(60*time.Second, 3*time.Second) // the process was away for 50 s
	if _, ok := s.probeVerdictLocked(machine.ProbeResult{Subject: "n3", StartedAt: 59 * time.Second, At: 60 * time.Second}); ok {
		t.Fatal("a failed probe that was in flight across a stall was fed to the detector")
	}
	if _, known := s.reachable["n3"]; known {
		t.Fatal("a dropped probe changed the subject's reachability")
	}
	if _, ok := s.probeVerdictLocked(machine.ProbeResult{Subject: "n3", StartedAt: 61 * time.Second, At: 62 * time.Second}); !ok {
		t.Fatal("a failed probe started after the stall was dropped")
	}
	if s.reachable["n3"] {
		t.Fatal("a real failure did not mark the subject unreachable")
	}
}

// TestALostDownProposalIsListedAgainByTheSupervisor (pi: the wiring). [GUARD: return nothing]
func TestALostDownProposalIsListedAgainByTheSupervisor(t *testing.T) {
	clock := &stepTwoBClock{now: 10 * time.Second}
	s := stepTwoBSupervisor(clock, map[string]cluster.MemberState{"n1": cluster.MemberActive, "n2": cluster.MemberActive, "n3": cluster.MemberActive})
	s.ledger.noteProposedDown("n3", 5*time.Second)
	s.reachable["n3"] = false
	if got := s.lostDownLocked(); !slices.Equal(got, []string{"n3"}) {
		t.Fatalf("the supervisor lists %v to propose down again, want [n3]", got)
	}
	s.reachable["n3"] = true
	s.ledger.noteProposedDown("n3", 5*time.Second)
	if got := s.lostDownLocked(); len(got) != 0 {
		t.Fatalf("a member that answers was proposed down: %v", got)
	}
}
