package cluster

import (
	"net"
	"reflect"
	"strconv"
	"testing"
	"time"
)

// divergedFollower is the setup of the F5 recovery tests: a healthy leader, a follower whose
// build does not read storage (the hide hook), the leader's CREATE_DATABASE with storage that
// diverges the follower, and manyAfter more commands (each accepted by the leader, each
// refused by the frozen follower). The follower is then closed with its directory kept.
type divergedFollower struct {
	leader *Raft
	dir    string
	addr   string
	frozen int64
}

func openDivergedFollower(t *testing.T, manyAfter int) *divergedFollower {
	t.Helper()
	cfg0 := quickRaft(t, t.TempDir(), "n1")
	cfg0.DataAddr = cfg0.Listener.Addr().String()
	leader := openOrFail(t, cfg0)

	cfg := quickRaft(t, t.TempDir(), "n2")
	cfg.Bootstrap = false
	addr := cfg.Listener.Addr().String()
	follower := openOrFail(t, cfg)
	follower.fsm.hideField = hideStorage
	if _, err := leader.Join(JoinRequest{
		Member:     Member{ID: "n2", Incarnation: 1, DataAddr: addr},
		RaftAddr:   addr,
		PlacingSet: make([]byte, 1024),
	}); err != nil {
		t.Fatalf("join: %v", err)
	}
	frozen := leader.State().StateSeq
	if _, err := leader.Propose(Command{Version: CommandVersion, Kind: CommandCreateDatabase, BaseSeq: uint64(frozen), Body: storageBody("cache", 1)}); err != nil {
		t.Fatal(err)
	}
	waitDiverged(t, follower)
	for i := range manyAfter {
		createDatabase(t, leader, "d"+strconv.Itoa(i), int64(2+i))
	}
	// The leader's last entry has reached the follower's log (it refused it), so the frozen
	// follower holds the whole retained log the restart will replay.
	deadline := time.Now().Add(5 * time.Second)
	for lastLogIndex(t, follower) < lastLogIndex(t, leader) && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if lastLogIndex(t, follower) < lastLogIndex(t, leader) {
		t.Fatalf("the follower's log ends at %d, the leader's at %d", lastLogIndex(t, follower), lastLogIndex(t, leader))
	}
	if follower.StateSeq() != frozen {
		t.Fatalf("the follower's state moved from %d to %d", frozen, follower.StateSeq())
	}
	if err := follower.TakeSnapshot(); err == nil {
		t.Fatal("the diverged follower wrote a snapshot of its frozen state")
	}
	dir := follower.cfg.Dir
	if err := follower.Close(); err != nil {
		t.Fatal(err)
	}
	return &divergedFollower{leader: leader, dir: dir, addr: addr, frozen: frozen}
}

// reopen starts the follower's directory again on the same raft address, on the build that
// wrote the log (it reads storage).
func (d *divergedFollower) reopen(t *testing.T) *Raft {
	t.Helper()
	listener, err := net.Listen("tcp", d.addr)
	if err != nil {
		t.Fatalf("listen on the follower's old address: %v", err)
	}
	cfg := quickRaft(t, d.dir, "n2")
	_ = cfg.Listener.Close()
	cfg.Listener, cfg.Bootstrap = listener, false
	return openOrFail(t, cfg)
}

// TestARestartOnTheFixedBuildAfter128EntriesCatchesUp is test 17, part one (mixed-build spec
// 6.2, failure mode 3): a follower that fail-stopped on a refused entry, with 130 more entries
// committed after it (past the library's snapshot threshold of 128), is restarted on the build
// that wrote the log. No snapshot of the frozen state exists (Snapshot was refused), so the
// retained log still holds the refused entry; the restart replays it, applies it, catches up to
// the leader's state exactly and serves, with nothing lost and no flag. The leader is forced to
// compact its own log first so the catch-up cannot lean on the leader's copy of the old entries.
// Break: persist the divergence across restarts (the restarted node would stay flagged and
// LogApplied false), or let a flagged node snapshot (the refused entry is compacted and the
// restart is frozen).
func TestARestartOnTheFixedBuildAfter128EntriesCatchesUp(t *testing.T) {
	d := openDivergedFollower(t, 130)
	if err := d.leader.TakeSnapshot(); err != nil {
		t.Fatalf("the healthy leader's snapshot: %v", err)
	}
	createDatabase(t, d.leader, "while-down", 132) // committed while the follower is down

	fixed := d.reopen(t)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && (fixed.StateSeq() != d.leader.StateSeq() || !fixed.LogApplied()) {
		time.Sleep(10 * time.Millisecond)
	}
	if fixed.IsDiverged() {
		t.Fatal("the restart on the build that wrote the log recorded a divergence")
	}
	if !fixed.LogApplied() {
		t.Fatalf("LogApplied is false after catching up (state_seq %d, leader %d)", fixed.StateSeq(), d.leader.StateSeq())
	}
	if got, want := fixed.State(), d.leader.State(); !reflect.DeepEqual(got, want) {
		t.Fatalf("the recovered state is not the leader's:\nrecovered %+v\nleader    %+v", got, want)
	}
	names := map[string]bool{}
	for _, db := range fixed.State().Databases {
		names[db.Name] = true
	}
	for _, want := range []string{"cache", "d0", "d129", "while-down"} {
		if !names[want] {
			t.Errorf("database %q is missing from the recovered state: nothing may be lost", want)
		}
	}
	// It serves: a command through the recovered node's own state is accepted by the cluster.
	createDatabase(t, d.leader, "after", 133)
	for fixed.StateSeq() != d.leader.StateSeq() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if fixed.StateSeq() != d.leader.StateSeq() || fixed.IsDiverged() {
		t.Fatalf("the recovered node stopped following (state_seq %d, leader %d, diverged %v)", fixed.StateSeq(), d.leader.StateSeq(), fixed.IsDiverged())
	}
}

// TestARestartOnTheSameBuildFiresTheTriggerAgain is test 17, part two (failure mode 10): a
// directory whose log holds an entry this build refuses, with 130 entries stamped above it, is
// restarted on the same build. The replay hits the same entry and records the same divergence at
// the same index, the state still frozen, snapshots still refused, the log not reported applied
// and the refused entry still retained: it neither recovers nor gets worse. (The build that
// diverged is this build: the refused entry carries a field no build of this tree reads, because
// the hide hook of the old-build seam cannot be set before a restart's replay without a race.)
// Break: persist the divergence across restarts (the record would not be fired by the replay),
// or let a flagged node snapshot (the refused entry is compacted away and the restart is frozen
// and unflagged).
func TestARestartOnTheSameBuildFiresTheTriggerAgain(t *testing.T) {
	dir := t.TempDir()
	first := openOrFail(t, quickRaft(t, dir, "n1"))
	createDatabase(t, first, "app", 1)
	frozen := first.State().StateSeq
	if err := first.AppendWithoutDryRun(divergedRaw(t, Command{Version: CommandVersion, Kind: CommandCreateDatabase, BaseSeq: uint64(frozen), Body: unreadFieldBody("x", 2)})); err != nil {
		t.Fatal(err)
	}
	refused := waitDiverged(t, first)
	for i := range 130 {
		raw := divergedRaw(t, Command{Version: CommandVersion, Kind: CommandCreateDatabase, BaseSeq: uint64(frozen) + 1 + uint64(i), Body: fsmCreateDatabaseBody("d"+strconv.Itoa(i), int64(3+i), 1)})
		if err := first.AppendWithoutDryRun(raw); err != nil {
			t.Fatal(err)
		}
	}
	if err := first.TakeSnapshot(); err == nil {
		t.Fatal("the diverged node wrote a snapshot of its frozen state")
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	noRaftGoroutines(t)

	again := openOrFail(t, quickRaft(t, dir, "n1"))
	record := waitDiverged(t, again)
	if record.Index != refused.Index || record.StateSeq != frozen || record.Kind != CommandCreateDatabase {
		t.Fatalf("the second run recorded %+v, want the same entry (index %d) at the frozen state_seq %d", record, refused.Index, frozen)
	}
	time.Sleep(300 * time.Millisecond)
	if again.StateSeq() != frozen {
		t.Fatalf("the state moved from %d to %d on a node that diverged again", frozen, again.StateSeq())
	}
	if again.LogApplied() {
		t.Fatal("LogApplied is true on the restarted diverged node")
	}
	if err := again.TakeSnapshot(); err == nil {
		t.Fatal("the restarted diverged node wrote a snapshot of its frozen state")
	}
	if first, err := again.store.FirstIndex(); err != nil || first > refused.Index {
		t.Fatalf("the refused entry %d is no longer in the retained log (first index %d, %v)", refused.Index, first, err)
	}
}
