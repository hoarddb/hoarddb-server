package cluster

import (
	"errors"
	"strings"
	"testing"
	"time"

	hraft "github.com/hashicorp/raft"
)

// hideStorage stands in for a build that predates the storage field.
func hideStorage(field string) bool { return field == "storage" }

// storageBody is a CREATE_DATABASE body for a build that reads storage.
func storageBody(name string, id int64) []byte {
	return fsmDoc(fsmStr("name", name), fsmI64("id", id), fsmI32("rf", 1), fsmI64("schema_version", 1), fsmStr("storage", "memory"))
}

// joinSecondHiding is joinSecond with the new node's FSM standing in for an older build from its
// first entry (hideField is set before the node can see one).
func joinSecondHiding(t *testing.T, leader *Raft, hide func(string) bool) *Raft {
	t.Helper()
	cfg := quickRaft(t, t.TempDir(), "n2")
	cfg.Bootstrap = false
	second := openOrFail(t, cfg)
	second.fsm.hideField = hide
	if _, err := leader.Join(JoinRequest{
		Member:     Member{ID: "n2", Incarnation: 1, DataAddr: cfg.Listener.Addr().String()},
		RaftAddr:   cfg.Listener.Addr().String(),
		PlacingSet: make([]byte, 1024),
	}); err != nil {
		t.Fatalf("join: %v", err)
	}
	return second
}

func waitDiverged(t *testing.T, r *Raft) Divergence {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if d, ok := r.Divergence(); ok {
			return d
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("the node never recorded a divergence")
	return Divergence{}
}

// TestAFollowerThatRefusesACommittedCommandFailsStopAtOnce is BUG-0781's test: a follower whose
// build does not read storage (the hide hook), a leader that proposes CREATE_DATABASE with
// storage: memory. The refused entry itself records the divergence (no second command), with
// the reworded refusal as its reason; the follower's state is frozen and the leader's is not.
// (The exit on Raft.Diverged() is F2's.) Red before F1: STATE_MOVED forever, no record. Break:
// do not record the divergence in Apply.
func TestAFollowerThatRefusesACommittedCommandFailsStopAtOnce(t *testing.T) {
	leader := openOrFail(t, quickRaft(t, t.TempDir(), "n1"))
	follower := joinSecondHiding(t, leader, hideStorage)
	frozen := leader.State().StateSeq

	res, err := leader.Propose(Command{Version: CommandVersion, Kind: CommandCreateDatabase, BaseSeq: uint64(frozen), Body: storageBody("cache", 1)})
	if err != nil {
		t.Fatalf("the leader refused its own command: %v", err)
	}
	got := waitDiverged(t, follower)
	if got.Kind != CommandCreateDatabase || got.StateSeq != frozen || got.BaseSeq != uint64(frozen) {
		t.Fatalf("the record is %+v, want the create at stamp and state %d", got, frozen)
	}
	for _, want := range []string{`field "storage"`, "does not read", "do not remove the field"} {
		if !strings.Contains(got.Reason, want) {
			t.Errorf("the reason %q lacks %q", got.Reason, want)
		}
	}
	if strings.Contains(got.Reason, ": remove it") {
		t.Errorf("the reason tells the operator to remove the field: %q", got.Reason)
	}
	if follower.StateSeq() != frozen {
		t.Errorf("the follower's state moved to %d, want it frozen at %d", follower.StateSeq(), frozen)
	}
	if leader.IsDiverged() || leader.StateSeq() != res.StateSeq {
		t.Errorf("the healthy leader diverged=%v at %d, want applied at %d", leader.IsDiverged(), leader.StateSeq(), res.StateSeq)
	}
}

// TestADivergedNodeElectedLeaderProposesNothingAndNoHealthyNodeExits is round-4 B1: the node that
// recorded a divergence is made leader and asked to propose; every proposal is refused with
// text 4, nothing is appended and the healthy node records no divergence. (The relay and
// applyAsLeader paths are covered in cluster/wire by TestADivergedNodeProposesNothingOnAnyPath.)
// Break: remove the diverged refusal in DryRun.
func TestADivergedNodeElectedLeaderProposesNothingAndNoHealthyNodeExits(t *testing.T) {
	// The seat is the dialable address, so the diverged node can ask the healthy one for a vote.
	cfg0 := quickRaft(t, t.TempDir(), "n1")
	cfg0.DataAddr = cfg0.Listener.Addr().String()
	healthy := openOrFail(t, cfg0)
	diverging := joinSecondHiding(t, healthy, hideStorage)
	if _, err := healthy.Promote("n2"); err != nil {
		t.Fatalf("promote: %v", err)
	}
	if _, err := healthy.Propose(Command{Version: CommandVersion, Kind: CommandCreateDatabase, BaseSeq: uint64(healthy.State().StateSeq), Body: storageBody("cache", 1)}); err != nil {
		t.Fatal(err)
	}
	waitDiverged(t, diverging)
	// The library gives a transfer a short election-scaled window and answers "leadership
	// transfer timeout" when a loaded host misses it. The transfer is not what this test checks, so
	// it is asked again a few times, and a refusal that is not a timeout still fails at once.
	var transferErr error
	for attempt := 0; attempt < 5; attempt++ {
		transferErr = healthy.raft.LeadershipTransferToServer(hraft.ServerID("n2"), hraft.ServerAddress(diverging.cfg.Listener.Addr().String())).Error()
		if transferErr == nil || !errors.Is(transferErr, hraft.ErrLeadershipTransferInProgress) && !strings.Contains(transferErr.Error(), "timeout") {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if transferErr != nil {
		t.Fatalf("transfer leadership to the diverged node: %v", transferErr)
	}
	deadline := time.Now().Add(5 * time.Second)
	for !diverging.IsLeader() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if !diverging.IsLeader() {
		t.Fatal("the diverged node was not elected")
	}
	before := lastLogIndex(t, diverging)
	_, err := diverging.Propose(Command{Version: CommandVersion, Kind: CommandCreateDatabase, BaseSeq: uint64(diverging.State().StateSeq), Body: fsmCreateDatabaseBody("fine", 2, 1)})
	var refusal *Error
	if !errors.As(err, &refusal) || refusal.Code != CodeUnavailable || !strings.Contains(err.Error(), "Send the command to another member") {
		t.Fatalf("a diverged leader answered %v, want text 4", err)
	}
	if got := lastLogIndex(t, diverging); got != before {
		t.Fatalf("the diverged leader appended: log %d to %d", before, got)
	}
	if healthy.IsDiverged() {
		t.Fatal("a healthy node recorded a divergence")
	}
}

// TestAnyRefusalExceptStateMovedIsATrigger covers the refusals the hide hook and the value
// domains add to TestEveryRefusalButStateMovedSetsTheFlag: an unread field, a storage word, a
// member-state word and a setting key this build does not accept each set the flag when
// committed, and STATE_MOVED does not. Break: exempt INVALID_ARGUMENT from the classification.
func TestAnyRefusalExceptStateMovedIsATrigger(t *testing.T) {
	seq := func(f *clusterFSM) uint64 { return uint64(f.state.StateSeq) }
	cases := []struct {
		name string
		hide func(string) bool
		cmd  func(f *clusterFSM) Command
		want bool
	}{
		{"an unread field", hideStorage, func(f *clusterFSM) Command {
			return Command{Version: CommandVersion, Kind: CommandCreateDatabase, BaseSeq: seq(f), Body: storageBody("x", f.state.NextDatabaseID)}
		}, true},
		{"a storage word", nil, func(f *clusterFSM) Command {
			return Command{Version: CommandVersion, Kind: CommandCreateDatabase, BaseSeq: seq(f), Body: fsmDoc(fsmStr("name", "x"), fsmI64("id", f.state.NextDatabaseID), fsmI32("rf", 1), fsmI64("schema_version", 1), fsmStr("storage", "ram"))}
		}, true},
		{"a member-state word", nil, func(f *clusterFSM) Command {
			return Command{Version: CommandVersion, Kind: CommandSetMemberState, BaseSeq: seq(f), Body: fsmSetMemberStateBody("n2", "sideways")}
		}, true},
		{"a setting key", nil, func(f *clusterFSM) Command {
			return Command{Version: CommandVersion, Kind: CommandSetSetting, BaseSeq: seq(f), Body: fsmDoc(fsmStr("key", "no_such_setting"), fsmBool("value", true))}
		}, true},
		{"STATE_MOVED", nil, func(f *clusterFSM) Command {
			return Command{Version: CommandVersion, Kind: CommandCreateDatabase, BaseSeq: seq(f) - 1, Body: fsmCreateDatabaseBody("x", f.state.NextDatabaseID, 1)}
		}, false},
	}
	for _, tc := range cases {
		f := divergedFSM(t)
		f.hideField = tc.hide
		out := f.Apply(divergedEntry(20, divergedRaw(t, tc.cmd(f)))).(applyOutcome)
		if (f.diverged.Load() != nil) != tc.want {
			t.Errorf("%s (outcome error %v): flag set = %v, want %v", tc.name, out.err, f.diverged.Load() != nil, tc.want)
		}
	}
}

// TestALogWrittenBeforeTheDryRunFlagsTheNodeOnReplay is the flag half of test 10 (the exit and
// text 1's start-fresh sentence are F2's): a log holding a committed CONFLICT (a restamped
// stale CREATE_DATABASE id, as builds before the dry run commit) flags the node when it is
// replayed after a restart. Break: classify CONFLICT as harmless.
func TestALogWrittenBeforeTheDryRunFlagsTheNodeOnReplay(t *testing.T) {
	dir := t.TempDir()
	first := openOrFail(t, quickRaft(t, dir, "n1"))
	createDatabase(t, first, "app", 1)
	conflict := divergedRaw(t, Command{Version: CommandVersion, Kind: CommandCreateDatabase, BaseSeq: uint64(first.State().StateSeq), Body: fsmCreateDatabaseBody("app", 2, 1)})
	if err := first.AppendWithoutDryRun(conflict); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second := openOrFail(t, quickRaft(t, dir, "n1"))
	got := waitDiverged(t, second)
	if got.Kind != CommandCreateDatabase {
		t.Fatalf("the record is %+v, want the refused create", got)
	}
	if second.LogApplied() {
		t.Fatal("a node that replayed a committed refusal reports its log applied")
	}
}

// TestARefusedLastEntryFlagsAMemberOnReplay is the flag half of test 12 (round-4 M3; serving
// nothing before the exit is F2's): the refused entry is the last one in the log, with nothing
// stamped above it, so the BaseSeq comparison cannot see it and only the refusal does. Break:
// fire only on BaseSeq above StateSeq, as rev 2 and rev 3 did.
func TestARefusedLastEntryFlagsAMemberOnReplay(t *testing.T) {
	dir := t.TempDir()
	first := openOrFail(t, quickRaft(t, dir, "n1"))
	createDatabase(t, first, "app", 1)
	if err := first.AppendWithoutDryRun(divergedRaw(t, Command{Version: CommandVersion, Kind: CommandCreateDatabase, BaseSeq: uint64(first.State().StateSeq), Body: unreadFieldBody("x", 2)})); err != nil {
		t.Fatal(err)
	}
	last := lastLogIndex(t, first)
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second := openOrFail(t, quickRaft(t, dir, "n1"))
	got := waitDiverged(t, second)
	if got.Index != last {
		t.Fatalf("the record names index %d, want the last entry, %d", got.Index, last)
	}
	if second.LogApplied() {
		t.Fatal("LogApplied is true on a node whose last entry was refused")
	}
}
