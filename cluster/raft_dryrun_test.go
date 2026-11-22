package cluster

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// unreadFieldBody is a CREATE_DATABASE body with a field this build does not read, as a newer
// build's leader would write it.
func unreadFieldBody(name string, id int64) []byte {
	return fsmDoc(fsmStr("name", name), fsmI64("id", id), fsmI32("rf", 1), fsmI64("schema_version", 1), fsmStr("storagex", "memory"))
}

func lastLogIndex(t *testing.T, r *Raft) uint64 {
	t.Helper()
	last, err := r.store.LastIndex()
	if err != nil {
		t.Fatal(err)
	}
	return last
}

func wantRefusal(t *testing.T, what string, err error, code Code) {
	t.Helper()
	var refusal *Error
	if !errors.As(err, &refusal) || refusal.Code != code {
		t.Fatalf("%s: got %v, want a %v refusal", what, err, code)
	}
}

// TestTheDryRunCoversEveryAppendPath: a body this build refuses returns the refusal to the
// proposer and the log gains no entry, through each way into the library's Apply that lives in
// package cluster: Propose and ProposeContext, and Raft.propose (register member and set
// voters). The wire package's applyAsLeader and proposeAtSeq are covered in its own tests, and
// bootstrap goes through Propose. A restamp of a stale CREATE_DATABASE id is refused CONFLICT at
// its own dry run. Break: take the dry run out of ProposeContext (the committed refusal then
// grows the log and sets the flag, so every assertion below goes red).
func TestTheDryRunCoversEveryAppendPath(t *testing.T) {
	r := openOrFail(t, quickRaft(t, t.TempDir(), "n1"))
	createDatabase(t, r, "app", 1)
	stamp := func() uint64 { return uint64(r.State().StateSeq) }
	before := lastLogIndex(t, r)
	check := func(what string, err error, code Code) {
		t.Helper()
		wantRefusal(t, what, err, code)
		if got := lastLogIndex(t, r); got != before {
			t.Fatalf("%s: the log grew from %d to %d: a refusal reached the log", what, before, got)
		}
		if r.IsDiverged() {
			t.Fatalf("%s: a refusal reached the log and set the flag", what)
		}
	}

	_, err := r.Propose(Command{Version: CommandVersion, Kind: CommandCreateDatabase, BaseSeq: stamp(), Body: unreadFieldBody("x", 2)})
	check("Propose with an unread field", err, CodeInvalidArgument)
	_, err = r.ProposeContext(context.Background(), Command{Version: CommandVersion, Kind: CommandCreateDatabase, BaseSeq: stamp(), Body: unreadFieldBody("x", 2)})
	check("ProposeContext with an unread field", err, CodeInvalidArgument)
	_, err = r.propose(CommandRegisterMember, fsmDoc(fsmStr("id", "n9"), fsmI64("incarnation", 1), fsmStr("data_addr", "n9:1"), fsmStr("extra", "x")))
	check("propose (register member) with an unread field", err, CodeInvalidArgument)
	_, err = r.propose(CommandSetVoters, fsmDoc(fsmStr("id", "n1"), fsmBool("voting", true), fsmStr("extra", "x")))
	check("propose (set voters) with an unread field", err, CodeInvalidArgument)

	// The restamp of a stale create: the id was taken, so every node would refuse it CONFLICT.
	_, err = r.Propose(Command{Version: CommandVersion, Kind: CommandCreateDatabase, BaseSeq: stamp(), Body: fsmCreateDatabaseBody("app2", 1, 1)})
	check("a restamped CREATE_DATABASE with a taken id", err, CodeConflict)
	// A stale stamp is STATE_MOVED at the dry run, and nothing is appended either.
	_, err = r.Propose(Command{Version: CommandVersion, Kind: CommandCreateDatabase, BaseSeq: stamp() - 1, Body: fsmCreateDatabaseBody("app3", 2, 1)})
	check("a stale stamp", err, CodeStateMoved)
	// An ahead stamp is STATE_MOVED for the proposer, never a divergence.
	_, err = r.Propose(Command{Version: CommandVersion, Kind: CommandCreateDatabase, BaseSeq: stamp() + 4, Body: fsmCreateDatabaseBody("app3", 2, 1)})
	check("an ahead stamp", err, CodeStateMoved)
	var ahead *Diverged
	if errors.As(err, &ahead) {
		t.Fatal("an ahead stamp reached the proposer as a *Diverged")
	}

	// And a good command still commits.
	if res, err := r.Propose(Command{Version: CommandVersion, Kind: CommandCreateDatabase, BaseSeq: stamp(), Body: fsmCreateDatabaseBody("app4", 2, 1)}); err != nil || res.StateSeq != 3 {
		t.Fatalf("a valid command: %+v, %v", res, err)
	}
}

// TestADryRunAtAStaleStateAfterALeaderChangeIsNotAFinalRefusal is section 7 M5: a leader whose
// FSM trails the committed log (a new leader) must not answer a final NOT_FOUND or CONFLICT that
// the cluster would not give. The held entry creates database 1, which the stale state lacks,
// so the dry run of an alter of database 1 stamped at the stale state_seq would say NOT_FOUND;
// the truth is STATE_MOVED, which tells the proposer to stamp again. Break: return the first
// dry run's refusal without waiting for the FSM.
func TestADryRunAtAStaleStateAfterALeaderChangeIsNotAFinalRefusal(t *testing.T) {
	cfg := quickRaft(t, t.TempDir(), "n1")
	var hold atomic.Bool
	release := make(chan struct{})
	cfg.ApplyGate = func(kind byte) {
		if kind == CommandCreateDatabase && hold.Load() {
			<-release
		}
	}
	r := openOrFail(t, cfg)
	stale := uint64(r.State().StateSeq)
	hold.Store(true)
	var releaseOnce sync.Once
	letGo := func() { releaseOnce.Do(func() { hold.Store(false); close(release) }) }
	t.Cleanup(letGo) // a failed assertion must not leave the FSM goroutine held through Close
	logBefore := lastLogIndex(t, r)
	held := r.raft.Apply(divergedRaw(t, Command{Version: CommandVersion, Kind: CommandCreateDatabase, BaseSeq: stale, Body: fsmCreateDatabaseBody("app", 1, 1)}), time.Second)
	for lastLogIndex(t, r) == logBefore { // the append is asynchronous: the entry is in the log, unapplied
		time.Sleep(time.Millisecond)
	}

	answer := make(chan error, 1)
	go func() {
		_, err := r.Propose(Command{Version: CommandVersion, Kind: CommandAlterDatabase, BaseSeq: stale, Body: fsmDoc(fsmI64("id", 1), fsmI32("rf", 1), fsmI64("schema_version", 1))})
		answer <- err
	}()
	select {
	case err := <-answer:
		t.Fatalf("the proposal answered %v while the FSM trailed the log: a refusal computed at a stale state was returned", err)
	case <-time.After(300 * time.Millisecond):
	}
	letGo()
	if err := held.Error(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-answer:
		wantRefusal(t, "an alter stamped at a state the log had moved past", err, CodeStateMoved)
	case <-time.After(5 * time.Second):
		t.Fatal("the proposal never answered after the FSM caught up")
	}
	if r.IsDiverged() {
		t.Fatal("a proposer's stale stamp set the flag")
	}
}

// TestADivergedNodeRefusesToPropose: a node that has recorded a divergence appends nothing, so
// it cannot make healthy nodes refuse its entry. It answers with text 4 and the log is the
// same length after. Break: remove the refusal in DryRun.
func TestADivergedNodeRefusesToPropose(t *testing.T) {
	r := openOrFail(t, quickRaft(t, t.TempDir(), "n1"))
	createDatabase(t, r, "app", 1)
	if err := r.raft.Apply(divergedRaw(t, Command{Version: CommandVersion, Kind: CommandCreateDatabase, BaseSeq: uint64(r.State().StateSeq), Body: unreadFieldBody("x", 2)}), time.Second).Error(); err != nil {
		t.Fatal(err)
	}
	if !r.IsDiverged() {
		t.Fatal("the raw-appended refused entry did not set the flag")
	}
	before := lastLogIndex(t, r)
	_, err := r.Propose(Command{Version: CommandVersion, Kind: CommandCreateDatabase, BaseSeq: uint64(r.State().StateSeq), Body: fsmCreateDatabaseBody("fine", 2, 1)})
	wantRefusal(t, "a valid command on a diverged leader", err, CodeUnavailable)
	if !strings.Contains(err.Error(), "disagrees with the cluster's") || !strings.Contains(err.Error(), "Send the command to another member") {
		t.Fatalf("the refusal is not text 4: %v", err)
	}
	if got := lastLogIndex(t, r); got != before {
		t.Fatalf("a diverged node appended: the log grew from %d to %d", before, got)
	}
}

// TestADryRunThatTimesOutWaitingForTheFSMIsAPlainUnavailable: on a sitting leader whose FSM
// trails the log, the catch-up wait that runs out of time answers UNAVAILABLE as a plain
// *Error. A *NotLeaderError would make proposeAtSeq retry the same wait for thirty seconds and a
// relay report NO_QUORUM about a leader that is not wrong. Break: return a *NotLeaderError.
func TestADryRunThatTimesOutWaitingForTheFSMIsAPlainUnavailable(t *testing.T) {
	r := openOrFail(t, quickRaft(t, t.TempDir(), "n1"))
	createDatabase(t, r, "app", 1)
	release := make(chan struct{})
	held := func(byte) { <-release }
	r.fsm.applyGate.Store(&held)
	defer func() { r.fsm.applyGate.Store(nil) }()
	var once sync.Once
	releaseFSM := func() { once.Do(func() { close(release) }) }
	defer releaseFSM()
	raw := divergedRaw(t, Command{Version: CommandVersion, Kind: CommandCreateDatabase, BaseSeq: uint64(r.State().StateSeq), Body: fsmCreateDatabaseBody("later", 2, 1)})
	before := lastLogIndex(t, r)
	future := r.raft.Apply(raw, time.Second) // committed, held unapplied by the gate
	for lastLogIndex(t, r) == before {
		time.Sleep(time.Millisecond)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	// A refusal by the stale state sends the dry run to the catch-up wait.
	err := r.DryRun(ctx, Command{Version: CommandVersion, Kind: CommandCreateDatabase, BaseSeq: uint64(r.State().StateSeq), Body: unreadFieldBody("x", 3)})
	var notLeader *NotLeaderError
	if errors.As(err, &notLeader) {
		t.Fatalf("a sitting leader's catch-up timeout is a NotLeaderError: %v", err)
	}
	wantRefusal(t, "the catch-up timeout", err, CodeUnavailable)
	releaseFSM()
	if err := future.Error(); err != nil {
		t.Fatal(err)
	}
}

// TestADryRunClonesTheStateOnce: ApplyCommand clones before it mutates and assigns only to its
// own receiver, so the dry run hands it a shallow copy taken under the read lock instead of a
// full State() clone first. At 1,000 databases the second clone was 3.3 MB and 1.1 ms of leader
// CPU per cluster command. The allocations of one dry run must stay near one clone's. Break: call
// r.State() in dryRunOnce (about two clones).
func TestADryRunClonesTheStateOnce(t *testing.T) {
	r := openOrFail(t, quickRaft(t, t.TempDir(), "n1"))
	for i := range 60 {
		createDatabase(t, r, fmt.Sprintf("db%d", i), int64(i+1))
		if _, err := r.Propose(Command{Version: CommandVersion, Kind: CommandSeeded, BaseSeq: uint64(r.State().StateSeq),
			Body: fsmNodeBitsBody("n1", int64(i+1), fsmBitsAt(5, 6))}); err != nil {
			t.Fatalf("seed bits of database %d: %v", i+1, err)
		}
	}
	state := r.State()
	cmd := Command{Version: CommandVersion, Kind: CommandCreateDatabase, BaseSeq: uint64(state.StateSeq), Body: fsmCreateDatabaseBody("probe", state.NextDatabaseID, 1)}
	oneClone := testing.AllocsPerRun(20, func() { _ = state.clone() })
	dryRun := testing.AllocsPerRun(20, func() {
		if err := r.dryRunOnce(cmd); err != nil {
			t.Fatal(err)
		}
	})
	if oneClone < 60 {
		t.Fatalf("a clone of 60 databases allocates only %.0f times: the state is not what the test needs", oneClone)
	}
	if dryRun > oneClone*1.4 {
		t.Fatalf("one dry run allocates %.0f times, one clone %.0f: the state is cloned more than once", dryRun, oneClone)
	}
}
