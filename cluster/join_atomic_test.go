package cluster

import (
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	hraft "github.com/hashicorp/raft"
)

// configuredServers lists the ids the Raft configuration holds, voters and nonvoters.
func configuredServers(t *testing.T, r *Raft) map[string]hraft.ServerSuffrage {
	t.Helper()
	conf, err := r.configuration()
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]hraft.ServerSuffrage{}
	for _, server := range conf.Servers {
		out[string(server.ID)] = server.Suffrage
	}
	return out
}

func openJoinerAt(t *testing.T, id string) RaftConfig {
	t.Helper()
	cfg := quickRaft(t, t.TempDir(), id)
	cfg.Bootstrap = false
	openOrFail(t, cfg)
	return cfg
}

// TestAFailedAddNonvoterLeavesNothing: a join entered where the library cannot add a server (a
// node that does not lead) changes no configuration and leaves no in-flight mark behind.
func TestAFailedAddNonvoterLeavesNothing(t *testing.T) {
	leader := openOrFail(t, quickRaft(t, t.TempDir(), "n1"))
	follower, _ := joinSecond(t, leader)
	cfg := openJoinerAt(t, "n3")
	_, err := follower.Join(JoinRequest{
		Member: Member{ID: "n3", Incarnation: 1, DataAddr: cfg.Listener.Addr().String()}, RaftAddr: cfg.Listener.Addr().String(), PlacingSet: make([]byte, 1024),
	})
	var notLeader *NotLeaderError
	if !errors.As(err, &notLeader) {
		t.Fatalf("a join on a follower: %v, want a not-leader refusal", err)
	}
	if servers := configuredServers(t, leader); len(servers) != 2 {
		t.Fatalf("a failed add changed the configuration: %v", servers)
	}
	if follower.joinInFlight("n3") || leader.joinInFlight("n3") {
		t.Fatal("a failed add left a join marked in flight")
	}
}

// TestARefusedRegistrationRollsBackTheNonvoter: the registration is refused after the library took
// the server, and the server is taken back out in the same call. [BREAK: drop the RemoveServer in
// RegisterJoiner].
func TestARefusedRegistrationRollsBackTheNonvoter(t *testing.T) {
	leader := openOrFail(t, quickRaft(t, t.TempDir(), "n1"))
	cfg := openJoinerAt(t, "n2")
	_, err := leader.Join(JoinRequest{
		Member: Member{ID: "n2", Incarnation: 1, DataAddr: "n1:1"}, RaftAddr: cfg.Listener.Addr().String(), PlacingSet: make([]byte, 1024),
	})
	if err == nil {
		t.Fatal("a registration at an address another member holds was accepted")
	}
	if servers := configuredServers(t, leader); len(servers) != 1 {
		t.Fatalf("the refused joiner stayed in the configuration: %v", servers)
	}
	if leader.joinInFlight("n2") {
		t.Fatal("a refused join stayed marked in flight")
	}
}

// TestAnOrphanNonvoterIsRemovedByTheLeadersSweep: a nonvoter with no member row and no join running
// here is removed by the leader's next ReconcileVoters; one whose join is running is left alone.
// [BREAK: the sweep looks at voters only].
func TestAnOrphanNonvoterIsRemovedByTheLeadersSweep(t *testing.T) {
	leader := openOrFail(t, quickRaft(t, t.TempDir(), "n1"))
	cfg := openJoinerAt(t, "n2")
	if err := leader.AddJoiner("n2", cfg.Listener.Addr().String()); err != nil {
		t.Fatal(err)
	}
	if _, err := leader.ReconcileVoters(); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if _, kept := configuredServers(t, leader)["n2"]; !kept {
		t.Fatal("the sweep removed a nonvoter whose join is still running")
	}
	leader.EndJoin("n2")
	if changed, err := leader.ReconcileVoters(); err != nil || changed != 1 {
		t.Fatalf("reconcile changed %d, %v; want it to remove the orphan", changed, err)
	}
	if servers := configuredServers(t, leader); len(servers) != 1 {
		t.Fatalf("the orphan nonvoter is still configured: %v", servers)
	}
}

// TestLeaderLossBetweenAddNonvoterAndRegisterLeavesNoPhantom: the leader dies after AddNonvoter and
// before REGISTER_MEMBER; the next process of that seat sweeps the phantom, and the operator's
// retry of the join registers the same id and address cleanly.
func TestLeaderLossBetweenAddNonvoterAndRegisterLeavesNoPhantom(t *testing.T) {
	dir := t.TempDir()
	leader, err := OpenRaft(quickRaft(t, dir, "n1"))
	if err != nil {
		t.Fatal(err)
	}
	cfg := openJoinerAt(t, "n2")
	addr := cfg.Listener.Addr().String()
	if err := leader.AddJoiner("n2", addr); err != nil {
		t.Fatal(err)
	}
	if err := leader.Close(); err != nil { // the join's goroutine dies with the process: no EndJoin
		t.Fatal(err)
	}

	second := quickRaft(t, dir, "n1")
	second.Bootstrap = false
	restarted := openOrFail(t, second)
	for end := time.Now().Add(10 * time.Second); !restarted.IsLeader(); {
		if time.Now().After(end) {
			t.Fatal("the restarted seat never led")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, kept := configuredServers(t, restarted)["n2"]; !kept {
		t.Fatal("the phantom was not in the persisted configuration; the test proves nothing")
	}
	if _, err := restarted.ReconcileVoters(); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if _, kept := configuredServers(t, restarted)["n2"]; kept {
		t.Fatal("the new leader left the phantom nonvoter configured")
	}
	if _, err := restarted.Join(JoinRequest{Member: Member{ID: "n2", Incarnation: 1, DataAddr: addr}, RaftAddr: addr, PlacingSet: make([]byte, 1024)}); err != nil {
		t.Fatalf("the operator's retry of the join: %v", err)
	}
}

// steadyTimeouts gives a node election and lease timeouts a starved host cannot overrun. quickRaft's
// 50 ms lets a leader lose its lease, or a promoted voter campaign, whenever the scheduler pauses the
// test process for that long (BUG-0937); the cap tests promote real voters, so they need the margin.
func steadyTimeouts(cfg *RaftConfig) {
	cfg.HeartbeatTimeout = 500 * time.Millisecond
	cfg.ElectionTimeout = 500 * time.Millisecond
	cfg.LeaderLease = 500 * time.Millisecond
}

// capCluster is a leader, elected before it returns, with every id in joiners registered as a nonvoter.
func capCluster(t *testing.T, joiners ...string) *Raft {
	t.Helper()
	leaderConfig := quickRaft(t, t.TempDir(), "n1")
	steadyTimeouts(&leaderConfig)
	leader := openOrFail(t, leaderConfig)
	waitFor(t, leader.IsLeader)
	for _, id := range joiners {
		c5JoinTimed(t, leader, id, steadyTimeouts)
	}
	waitFor(t, leader.IsLeader)
	return leader
}

func assertCapRefusal(t *testing.T, err error, id string, would, limit int) {
	t.Helper()
	var refusal *Error
	want := "promote " + id + " would make"
	if !errors.As(err, &refusal) || refusal.Code != CodeInvalidArgument || !strings.Contains(refusal.Message, want) {
		t.Fatalf("promotion of %s: %v, want INVALID_ARGUMENT %q", id, err, want)
	}
	for _, text := range []string{strings.Join([]string{"would make", itoa(would), "voters and cluster.voters is", itoa(limit)}, " "), "'leave <voter>'", "'remove-node <dead voter>'", "arrive in the next release"} {
		if !strings.Contains(refusal.Message, text) {
			t.Errorf("P1 text lacks %q:\n%s", text, refusal.Message)
		}
	}
}

func itoa(n int) string { return string(rune('0' + n)) }

// TestVoterCapHoldsAcrossALeaderChangeMidPromote: a leader that died after AddVoter and before
// SET_VOTERS leaves the configuration one voter ahead of the state; a promotion at the next leader
// counts the configuration and is refused P1. The state list is simulated behind the configuration
// by adding the voter through the library alone, which is exactly what the next leader finds.
// [BREAK: count len(state.Voters)].
func TestVoterCapHoldsAcrossALeaderChangeMidPromote(t *testing.T) {
	leader := capCluster(t, "n2", "n3", "n4")
	if _, err := leader.PromoteWithin("n2", 3); err != nil {
		t.Fatalf("the second voter: %v", err)
	}
	if err := leader.raft.AddVoter("n3", hraft.ServerAddress(mustMemberAddr(t, leader, "n3")), 0, raftApplyTimeout).Error(); err != nil {
		t.Fatal(err)
	}
	if got := len(leader.State().Voters); got != 2 {
		t.Fatalf("the state lists %d voters, the test needs it behind the configuration at 2", got)
	}
	_, err := leader.PromoteWithin("n4", 3)
	assertCapRefusal(t, err, "n4", 4, 3)
	if got := configuredServers(t, leader)["n4"]; got == hraft.Voter {
		t.Fatal("the refused promotion still added n4 as a voter")
	}
}

func mustMemberAddr(t *testing.T, r *Raft, id string) string {
	t.Helper()
	addr, ok := r.MemberAddr(id)
	if !ok {
		t.Fatalf("no member %s", id)
	}
	return addr
}

// TestVoterCapCountsWitnesses: two data voters and a witness are three voters, so a fourth
// promotion is refused. [BREAK: leave the witnesses out of the count].
func TestVoterCapCountsWitnesses(t *testing.T) {
	leader := capCluster(t, "n2", "n3")
	witness := openJoinerAt(t, "w")
	addr := witness.Listener.Addr().String()
	if _, err := leader.Join(JoinRequest{Member: Member{ID: "w", Incarnation: 1, DataAddr: addr, Witness: true}, RaftAddr: addr, PlacingSet: make([]byte, 1024)}); err != nil {
		t.Fatal(err)
	}
	if _, err := leader.propose(CommandSetVoters, SetVotersBody("w", true)); err != nil {
		t.Fatal(err)
	}
	if _, err := leader.PromoteWithin("n2", 3); err != nil {
		t.Fatalf("the data voter beside n1 and the witness: %v", err)
	}
	_, err := leader.PromoteWithin("n3", 3)
	assertCapRefusal(t, err, "n3", 4, 3)
}

// TestPromoteOfAVoterIsNotCountedTwice: promoting a node that already votes returns the committed
// facts, even at the cap.
func TestPromoteOfAVoterIsNotCountedTwice(t *testing.T) {
	leader := capCluster(t, "n2")
	if _, err := leader.PromoteWithin("n2", 2); err != nil {
		t.Fatal(err)
	}
	if _, err := leader.PromoteWithin("n2", 2); err != nil {
		t.Fatalf("a repeat promotion at the cap: %v", err)
	}
}

// orphanLeader is a leader whose configuration holds n2 as a nonvoter no row names and no join
// marks: what a leader that died between AddNonvoter and REGISTER_MEMBER leaves behind.
func orphanLeader(t *testing.T) (leader *Raft, addr string) {
	t.Helper()
	leader = openOrFail(t, quickRaft(t, t.TempDir(), "n1"))
	addr = openJoinerAt(t, "n2").Listener.Addr().String()
	if err := leader.AddJoiner("n2", addr); err != nil {
		t.Fatal(err)
	}
	leader.EndJoin("n2")
	return leader, addr
}

// TestTheSweepRemovesAnOrphanWhileHoldingTheJoinsLock: between the sweep's check and its
// RemoveServer a join can neither mark nor add, because AddJoiner takes joinsMu first. The seam
// runs in that window and finds the lock held. [BREAK: take joinsMu in removeOrphan after the
// afterOrphanCheck seam, or not at all]
func TestTheSweepRemovesAnOrphanWhileHoldingTheJoinsLock(t *testing.T) {
	leader, addr := orphanLeader(t)
	held := false
	leader.afterOrphanCheck = func(string) {
		if leader.joinsMu.TryLock() {
			leader.joinsMu.Unlock()
			return
		}
		held = true
	}
	if changed, err := leader.ReconcileVoters(); err != nil || changed != 1 {
		t.Fatalf("reconcile changed %d, %v; want it to remove the orphan", changed, err)
	}
	if !held {
		t.Fatal("the sweep ran its check and RemoveServer without holding the joins lock, so a join could mark in between")
	}
	// The join that waited for the removal adds the server again and registers it.
	if _, err := leader.Join(JoinRequest{Member: Member{ID: "n2", Incarnation: 1, DataAddr: addr}, RaftAddr: addr, PlacingSet: make([]byte, 1024)}); err != nil {
		t.Fatalf("the join after the sweep: %v", err)
	}
	if got := configuredServers(t, leader)["n2"]; got != hraft.Nonvoter {
		t.Fatalf("n2 is %v in the configuration after its join", got)
	}
}

// TestTheSweepDoesNotRemoveAJoinerThatRegisteredAfterItReadItsState: the sweep read the state with
// no row for n2; a whole join (mark, add, register, unmark) then ran before the sweep's check. The
// check reads the row afresh and leaves the server. [BREAK: judge by the state the caller read,
// not a fresh one]
func TestTheSweepDoesNotRemoveAJoinerThatRegisteredAfterItReadItsState(t *testing.T) {
	leader, addr := orphanLeader(t)
	leader.beforeOrphanCheck = func(id string) {
		if _, err := leader.Join(JoinRequest{Member: Member{ID: id, Incarnation: 1, DataAddr: addr}, RaftAddr: addr, PlacingSet: make([]byte, 1024)}); err != nil {
			t.Errorf("the interleaved join: %v", err)
		}
	}
	if _, err := leader.ReconcileVoters(); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if _, kept := configuredServers(t, leader)["n2"]; !kept {
		t.Fatal("the sweep removed a joiner whose row had committed: a registered member with no Raft server")
	}
	if _, member := leader.MemberAddr("n2"); !member {
		t.Fatal("the interleaved join left no row")
	}
}

// TestVoterCapCountsAWitnessWhoseVoteIsCommittedButNotApplied: a witness votes by SET_VOTERS alone,
// so the cap reads it from the applied state. A just-elected leader can hold that entry committed and
// not yet applied; PromoteWithin waits for the log to be applied before it counts, and refuses P1.
// The FSM is held on the witness's SET_VOTERS, so the entry is committed and unapplied for as long as
// the test wants. The test releases the hold when the count has been taken, or after half a second
// when the code is (correctly) still waiting for the log. [BREAK: drop the Barrier in PromoteWithin]
func TestVoterCapCountsAWitnessWhoseVoteIsCommittedButNotApplied(t *testing.T) {
	leader := capCluster(t, "n2", "n3")
	witness := openJoinerAt(t, "w")
	addr := witness.Listener.Addr().String()
	if _, err := leader.Join(JoinRequest{Member: Member{ID: "w", Incarnation: 1, DataAddr: addr, Witness: true}, RaftAddr: addr, PlacingSet: make([]byte, 1024)}); err != nil {
		t.Fatal(err)
	}
	if _, err := leader.PromoteWithin("n2", 3); err != nil {
		t.Fatalf("the data voter beside n1: %v", err)
	}
	reached, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	gate := func(kind byte) {
		if kind == CommandSetVoters {
			once.Do(func() { close(reached); <-release })
		}
	}
	leader.fsm.applyGate.Store(&gate)
	proposed := make(chan error, 1)
	go func() {
		_, err := leader.propose(CommandSetVoters, SetVotersBody("w", true))
		proposed <- err
	}()
	<-reached
	counted := make(chan int, 1)
	leader.afterVoterCount = func(voters int) { counted <- voters }
	promoted := make(chan error, 1)
	go func() {
		_, err := leader.PromoteWithin("n3", 3)
		promoted <- err
	}()
	select {
	case voters := <-counted:
		t.Logf("the cap counted %d voters while the witness's vote was unapplied", voters)
	case <-time.After(500 * time.Millisecond):
	}
	close(release)
	if err := <-proposed; err != nil {
		t.Fatalf("the witness's vote: %v", err)
	}
	assertCapRefusal(t, <-promoted, "n3", 4, 3)
	if got := configuredServers(t, leader)["n3"]; got == hraft.Voter {
		t.Fatal("the promotion past the cap still added n3 as a voter")
	}
}

// TestTheSweepWaitsForTheLogBeforeJudgingAServer: a leader that has not applied the log yet holds a
// REGISTER_MEMBER committed and unapplied, so the state shows no row for a server the configuration
// names. The sweep waits for the log (the Barrier in ReconcileVoters) and then sees the row; without
// it the sweep removes a registered joiner. The FSM is held on REGISTER_MEMBER, and released after
// 100 ms or when the sweep returns. [BREAK: drop the Barrier in ReconcileVoters]
func TestTheSweepWaitsForTheLogBeforeJudgingAServer(t *testing.T) {
	leader, addr := orphanLeader(t)
	reached, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	gate := func(kind byte) {
		if kind == CommandRegisterMember {
			once.Do(func() { close(reached); <-release })
		}
	}
	leader.fsm.applyGate.Store(&gate)
	registered := make(chan error, 1)
	go func() {
		_, err := leader.propose(CommandRegisterMember, RegisterMemberBody(Member{ID: "n2", Incarnation: 1, DataAddr: addr}, make([]byte, 1024), BuildStateRange()))
		registered <- err
	}()
	<-reached
	type sweep struct {
		changed int
		err     error
	}
	swept := make(chan sweep, 1)
	go func() {
		changed, err := leader.ReconcileVoters()
		swept <- sweep{changed, err}
	}()
	var got sweep
	select {
	case got = <-swept:
		close(release)
	case <-time.After(100 * time.Millisecond):
		close(release)
		got = <-swept
	}
	if err := <-registered; err != nil {
		t.Fatalf("the registration: %v", err)
	}
	if got.err != nil || got.changed != 0 {
		t.Fatalf("the sweep changed %d (%v) while a registration was committed and unapplied", got.changed, got.err)
	}
	if _, kept := configuredServers(t, leader)["n2"]; !kept {
		t.Fatal("the sweep removed a joiner whose registration was committed: a row with no Raft server")
	}
}

// holdTheFSM blocks the apply of the next command of kind, so a Barrier taken meanwhile waits.
func holdTheFSM(t *testing.T, leader *Raft, kind byte) (release func()) {
	t.Helper()
	reached, hold := make(chan struct{}), make(chan struct{})
	var once sync.Once
	gate := func(got byte) {
		if got == kind {
			once.Do(func() { close(reached); <-hold })
		}
	}
	leader.fsm.applyGate.Store(&gate)
	go func() { _, _ = leader.propose(kind, SetVotersBody("n9", false)) }()
	<-reached
	return func() { close(hold) }
}

// TestTheSweepDoesNotWaitForTheLogForAnOrphanItAlreadyReported (pi S6): a voter with no row is
// reported once and stays unrecorded; every later supervisor step must not pay a barrier for it,
// because the step also renews leases. With the FSM held, a step that waited would take the whole
// barrier. [BREAK: hasUnrecordedServer stops skipping reported orphan voters]
func TestTheSweepDoesNotWaitForTheLogForAnOrphanItAlreadyReported(t *testing.T) {
	leader := openOrFail(t, quickRaft(t, t.TempDir(), "n1"))
	c5Join(t, leader, "n2")
	if _, err := leader.Promote("n2"); err != nil {
		t.Fatal(err)
	}
	if _, err := leader.propose(CommandRemoveMember, fsmDoc(fsmStr("id", "n2"))); err != nil {
		t.Fatal(err)
	}
	if _, err := leader.ReconcileVoters(); err == nil {
		t.Fatal("the first step did not report the orphan voter")
	}
	release := holdTheFSM(t, leader, CommandSetVoters)
	defer release()
	started := time.Now()
	if _, err := leader.ReconcileVoters(); err != nil {
		t.Fatalf("the second step: %v", err)
	}
	if took := time.Since(started); took > reconcileBarrier/2 {
		t.Fatalf("the step waited %v for the log on behalf of an orphan it had already reported", took)
	}
}

// TestTheSweepsWaitForTheLogIsBounded (pi S6): for a server it has not judged yet, the sweep waits
// at most the barrier bound while the FSM is held, and the bound is a fraction of a lease.
// [BREAK: Barrier(raftApplyTimeout) in ReconcileVoters]
func TestTheSweepsWaitForTheLogIsBounded(t *testing.T) {
	leader, _ := orphanLeader(t)
	if reconcileBarrier > 500*time.Millisecond {
		t.Fatalf("the barrier bound is %v, which holds a lease grant for longer than a lease", reconcileBarrier)
	}
	release := holdTheFSM(t, leader, CommandSetVoters)
	defer release()
	started := time.Now()
	if changed, err := leader.ReconcileVoters(); err != nil || changed != 0 {
		t.Fatalf("a step whose barrier timed out changed %d (%v), want it to look again next step", changed, err)
	}
	if took := time.Since(started); took > reconcileBarrier+reconcileBarrier/2+250*time.Millisecond {
		t.Fatalf("the step took %v, the bound is %v", took, reconcileBarrier)
	}
}
