package cluster

import (
	"net"
	"path/filepath"
	"testing"
	"time"

	hraft "github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

// TestAnUncommittedTrailingCommandReplacedByANoopStillLoadsTheState (test 13, boot shape, review
// B1): a healthy follower restarts with one uncommitted trailing command from an older term in
// its log (the pi S-1 boot shape) and the leader then commits a non-command entry at that index,
// so the FSM never reaches openedLastCommand and LogApplied stays false for good on a quiet
// cluster. StateLoaded, which the server's replay gate waits on, must open anyway: the library
// handed the whole log to the FSM and the state_seq is stable. Break: make StateLoaded return
// LogApplied() alone (the gate then never opens).
func TestAnUncommittedTrailingCommandReplacedByANoopStillLoadsTheState(t *testing.T) {
	cfg0 := quickRaft(t, t.TempDir(), "n1")
	cfg0.DataAddr = cfg0.Listener.Addr().String()
	leader := openOrFail(t, cfg0)
	cfg := quickRaft(t, t.TempDir(), "n2")
	cfg.Bootstrap = false
	addr := cfg.Listener.Addr().String()
	follower := openOrFail(t, cfg)
	if _, err := leader.Join(JoinRequest{Member: Member{ID: "n2", Incarnation: 1, DataAddr: addr}, RaftAddr: addr, PlacingSet: make([]byte, 1024)}); err != nil {
		t.Fatalf("join: %v", err)
	}
	createDatabase(t, leader, "d0", 1)
	deadline := time.Now().Add(5 * time.Second)
	for lastLogIndex(t, follower) < lastLogIndex(t, leader) && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond)
	dir := follower.cfg.Dir
	if err := follower.Close(); err != nil {
		t.Fatal(err)
	}
	// Append a stale-term command at last+1 to the follower's log.
	store, err := raftboltdb.NewBoltStore(filepath.Join(dir, "raft", "raft.db"))
	if err != nil {
		t.Fatal(err)
	}
	last, _ := store.LastIndex()
	var tail hraft.Log
	if err := store.GetLog(last, &tail); err != nil {
		t.Fatal(err)
	}
	raw, err := EncodeCommand(Command{Version: CommandVersion, Kind: CommandCreateDatabase, BaseSeq: uint64(leader.State().StateSeq), Body: fsmDoc(fsmStr("name", "ghost"), fsmI64("id", 2), fsmI32("rf", 1), fsmI64("schema_version", 1))})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.StoreLog(&hraft.Log{Index: last + 1, Term: tail.Term - 1, Type: hraft.LogCommand, Data: raw}); err != nil {
		t.Fatal(err)
	}
	t.Logf("injected index %d term %d (tail term %d)", last+1, tail.Term-1, tail.Term)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	cfg2 := quickRaft(t, dir, "n2")
	_ = cfg2.Listener.Close()
	cfg2.Listener, cfg2.Bootstrap = listener, false
	reopened := openOrFail(t, cfg2)
	t.Logf("openedLastCommand=%d", reopened.openedLastCommand)
	// The new leader's no-op stands in as a barrier: a committed non-command entry at last+1.
	if err := leader.raft.Barrier(5 * time.Second).Error(); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(10 * time.Second)
	for !reopened.StateLoaded() && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if reopened.LogApplied() {
		t.Fatal("the shape did not reproduce: LogApplied is true, so this test proves nothing")
	}
	if reopened.IsDiverged() {
		t.Fatal("the node diverged: an uncommitted entry replaced by the leader is not a divergence")
	}
	if !reopened.StateLoaded() {
		t.Fatalf("StateLoaded never opened on a healthy caught-up node (LogHandedToFSM=%v, stateSeq %d)", reopened.LogHandedToFSM(), reopened.StateSeq())
	}
}
