package wire

import (
	"context"
	"errors"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/transport"
)

// divergedRaftConfig is a one-node Raft seat with short timers on a fresh listener.
func divergedRaftConfig(t *testing.T, dir string) cluster.RaftConfig {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return cluster.RaftConfig{
		NodeID: "n1", Dir: dir, Listener: listener, DataAddr: "n1:1", Incarnation: 1,
		Bootstrap: true, Logger: hclog.NewNullLogger(),
		HeartbeatTimeout: 50 * time.Millisecond, ElectionTimeout: 50 * time.Millisecond, LeaderLease: 50 * time.Millisecond,
	}
}

func openDivergedRaft(t *testing.T, dir string) *cluster.Raft {
	t.Helper()
	cfg := divergedRaftConfig(t, dir)
	raft, err := cluster.OpenRaft(cfg)
	if err != nil {
		_ = cfg.Listener.Close()
		t.Fatalf("OpenRaft: %v", err)
	}
	t.Cleanup(func() { _ = raft.Close() })
	return raft
}

// bareNode is a Node with only what the proposal and seeding paths read: the Raft seat, its
// topology and an empty cluster part.
func bareNode(raft *cluster.Raft) *Node {
	return &Node{cfg: NodeConfig{NodeID: "n1"}, raft: raft, topology: NewClusterTopology("n1", raft), cl: &clusterParts{}}
}

func divergedCommand(t *testing.T, kind byte, baseSeq uint64, body []byte) []byte {
	t.Helper()
	raw, err := cluster.EncodeCommand(cluster.Command{Version: cluster.CommandVersion, Kind: kind, BaseSeq: baseSeq, Body: body})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// unreadFieldCreate is a CREATE_DATABASE body carrying a field this build does not read.
func unreadFieldCreate(name string, id int64) []byte {
	return bsonDoc(bsonStr("name", name), bsonI64("id", id), bsonI32("rf", 1), bsonI64("schema_version", 1), bsonStr("storagex", "memory"))
}

func plainCreate(name string, id int64) []byte {
	return bsonDoc(bsonStr("name", name), bsonI64("id", id), bsonI32("rf", 1), bsonI64("schema_version", 1))
}

// flagNode commits, by raw append, an entry this build refuses and one stamped above the frozen
// state, as the leader of a build that read the first would append; the node is then diverged
// and its state is frozen at the state_seq it had.
func flagNode(t *testing.T, raft *cluster.Raft) {
	t.Helper()
	frozen := uint64(raft.State().StateSeq)
	if err := raft.AppendWithoutDryRun(divergedCommand(t, cluster.CommandCreateDatabase, frozen, unreadFieldCreate("x", 1))); err != nil {
		t.Fatal(err)
	}
	if err := raft.AppendWithoutDryRun(divergedCommand(t, cluster.CommandCreateDatabase, frozen+1, plainCreate("app", 2))); err != nil {
		t.Fatal(err)
	}
	if !raft.IsDiverged() {
		t.Fatal("the refused entry did not set the flag")
	}
}

// outsideTargetRecord writes the ready record of a pair of a database the frozen state does not
// hold, the pair the section 4 hazard retires.
func outsideTargetRecord(t *testing.T, g *rig) uint16 {
	t.Helper()
	const slot = 5
	putRecord(t, g.kv, slot, readyRecord(cluster.Position{Epoch: 1, Seq: 3}))
	return slot
}

// pairStillReady fails when the record of (slot, reconDB) is not ready and unfenced, or when
// anything was acted on.
func pairStillReady(t *testing.T, g *rig, slot uint16) {
	t.Helper()
	rec, ok := g.rec.Record(slot, reconDB)
	if !ok || rec.State != machine.SeedReady {
		t.Fatalf("the record of slot %d is %+v (present %v), want ready", slot, rec, ok)
	}
	if disk, ok := diskRecord(t, g.kv, slot); !ok || disk.State != machine.SeedReady {
		t.Fatalf("the record of slot %d on disk is %+v (present %v), want ready", slot, disk, ok)
	}
	if fence, ok := hasFence(t, g.kv, slot); ok {
		t.Fatalf("slot %d is fenced at %+v: its pair is unreadable", slot, fence)
	}
	if len(g.sink.actions) != 0 {
		t.Fatalf("the reconciler acted on a frozen state: %+v", g.sink.actions)
	}
}

// seedingRig builds the node's reconciler over a Badger store with the node's own Raft as its
// state, the way newSeedParts wires it; withHold says whether the reconciler's own guard is wired.
func seedingRig(t *testing.T, n *Node, withHold bool) (*rig, *seedParts) {
	t.Helper()
	kv := openKV(t, filepath.Join(t.TempDir(), "data"))
	t.Cleanup(func() { _ = kv.Close() })
	g := newRig(t, kv, cluster.ClusterState{}, "n1", false)
	g.rec.cfg.State = raftState{n: n}
	if withHold {
		g.rec.cfg.Diverged = n.raft.IsDiverged
	}
	return g, newSeedPartsFor(g.rec, n.raft.State(), "n1")
}

func driveTicks(t *testing.T, n *Node, ticks int) {
	t.Helper()
	for range ticks {
		if err := n.driveSeeding(context.Background()); err != nil {
			t.Fatalf("driveSeeding: %v", err)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// TestARefusedEntryDoesNotLetTheReconcilerRetireAPair is spec test 3a: a node holds the pair of
// a database whose CREATE follows a refused entry in the log; it restarts, replays, and is driven
// for more ticks than any latch needs. The slot record stays ready and
// unfenced and the reconciler acts on nothing. Break: remove all four guards (today's
// behaviour); the pass runs against the frozen state and retires the pair.
func TestARefusedEntryDoesNotLetTheReconcilerRetireAPair(t *testing.T) {
	dir := t.TempDir()
	first := openDivergedRaft(t, dir)
	flagNode(t, first)
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	again := openDivergedRaft(t, dir)
	n := bareNode(again)
	g, parts := seedingRig(t, n, true)
	slot := outsideTargetRecord(t, g)
	n.cl.seeding.Store(parts)
	if err := g.rec.LoadRecords(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for !again.IsDiverged() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !again.IsDiverged() {
		t.Fatal("the replay never set the flag again")
	}
	driveTicks(t, n, 8)
	pairStillReady(t, g, slot)
	if got := parts.passes.Load(); got != 0 {
		t.Fatalf("%d reconcile passes ran on a diverged node", got)
	}
}

// TestDriveSeedingChecksTheFlagBeforeAnythingElse is test 3c: with the other guards out of the
// way (the state is already latched loaded and the reconciler's own guard is not wired), the
// check at the top of driveSeeding alone stops the pass. Break: drop that check.
func TestDriveSeedingChecksTheFlagBeforeAnythingElse(t *testing.T) {
	raft := openDivergedRaft(t, t.TempDir())
	flagNode(t, raft)
	n := bareNode(raft)
	g, parts := seedingRig(t, n, false)
	slot := outsideTargetRecord(t, g)
	n.cl.seeding.Store(parts)
	if err := g.rec.LoadRecords(); err != nil {
		t.Fatal(err)
	}
	parts.latch.Loaded = true // stateLoaded no longer stands in the way
	driveTicks(t, n, 4)
	pairStillReady(t, g, slot)
	if got := parts.passes.Load(); got != 0 {
		t.Fatalf("%d reconcile passes ran on a diverged node", got)
	}
}

// TestStateLoadedDoesNotLatchWhileDiverged is test 3d: a diverged node holds a frozen state, so
// the latch must not open on a loaded answer while the flag is set. Break: ignore the flag in
// LoadLatch.Step.
func TestStateLoadedDoesNotLatchWhileDiverged(t *testing.T) {
	parts := &seedParts{}
	for tick := range 8 {
		if parts.stateLoaded(true, true) {
			t.Fatalf("tick %d: the latch opened on a diverged node", tick)
		}
	}
	if parts.latch.Loaded {
		t.Fatal("loaded was latched while diverged")
	}
	// The same answer on a healthy node latches, so the test above can fail.
	healthy := &seedParts{}
	if !healthy.stateLoaded(true, false) {
		t.Fatal("the latch no longer opens on a healthy node: the diverged case proves nothing")
	}
}

// TestTheReconcilersOwnPassStopsOnTheFlag is test 3f: a reconciler told its node is diverged
// decides nothing, whatever drove it. Break: drop the check in reconcile.
func TestTheReconcilersOwnPassStopsOnTheFlag(t *testing.T) {
	st := threeNodes(t)
	slots := findSlots(t, st)
	for _, flagged := range []bool{false, true} {
		kv := openKV(t, filepath.Join(t.TempDir(), "data"))
		g := newRig(t, kv, st, testSelf, false)
		g.rec.cfg.Diverged = func() bool { return flagged }
		putRecord(t, kv, slots.outside, readyRecord(cluster.Position{Epoch: uint64(st.Epoch), Seq: 3}))
		g.boot(t)
		retired := false
		for _, a := range g.sink.actions {
			retired = retired || (a.Slot == slots.outside && a.Do == machine.SlotRetire)
		}
		if retired == flagged {
			t.Fatalf("diverged=%v: retired=%v; a healthy pass retires a ready pair outside the target set and a diverged one does nothing", flagged, retired)
		}
		if flagged {
			pairStillReady(t, g, slots.outside)
		}
		if err := kv.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

const divergedSender = "n1"

func relayFromVoter(kind byte, body []byte) transport.RelayProposal {
	return transport.RelayProposal{RequestID: [transport.RelayRequestIDLen]byte{1}, Kind: kind, Body: body}
}

// TestADivergedNodeProposesNothingOnAnyPath: a flagged leader refuses a relayed command, a local
// command (applyAsLeader) and a command at a chosen stamp (proposeAtSeq) with text 4, and
// appends nothing: its state_seq does not move and no entry reaches the log. Break: remove the
// refusal in Raft.DryRun.
func TestADivergedNodeProposesNothingOnAnyPath(t *testing.T) {
	raft := openDivergedRaft(t, t.TempDir())
	flagNode(t, raft)
	n := bareNode(raft)
	frozen := raft.State().StateSeq
	body := plainCreate("fine", raft.State().NextDatabaseID)
	text4 := func(what string, err error) {
		t.Helper()
		if err == nil || !strings.Contains(err.Error(), "disagrees with the cluster's") || !strings.Contains(err.Error(), "Send the command to another member") {
			t.Fatalf("%s: got %v, want text 4", what, err)
		}
	}

	reply := n.serveRelay(context.Background(), transport.Handshake{NodeID: divergedSender}, relayFromVoter(cluster.CommandCreateDatabase, body))
	if reply.Status != transport.RelayRefused || !strings.Contains(reply.Message, "Send the command to another member") {
		t.Fatalf("serveRelay answered %+v, want a refusal with text 4", reply)
	}
	_, err := n.applyAsLeader(context.Background(), cluster.CommandCreateDatabase, body)
	text4("applyAsLeader", err)
	_, err = n.proposeAtSeq(context.Background(), cluster.CommandCreateDatabase, body, uint64(frozen))
	text4("proposeAtSeq", err)
	time.Sleep(100 * time.Millisecond)
	if got := raft.State().StateSeq; got != frozen {
		t.Fatalf("a diverged node appended: state_seq moved from %d to %d", frozen, got)
	}
}

// TestTheDryRunCoversApplyAsLeaderProposeAtSeqAndTheRelay is spec tests 1 and 2 for the wire
// paths: a body this build refuses, through applyAsLeader (with its restamp loop), proposeAtSeq
// and a relayed command, returns the refusal and appends nothing: a committed refusal would
// set the flag. A restamp of a stale CREATE_DATABASE id is refused CONFLICT. Break: move the dry
// run out of ProposeContext (the relay's own check still catches the relay; the other two go red).
func TestTheDryRunCoversApplyAsLeaderProposeAtSeqAndTheRelay(t *testing.T) {
	raft := openDivergedRaft(t, t.TempDir())
	n := bareNode(raft)
	if _, err := n.applyAsLeader(context.Background(), cluster.CommandCreateDatabase, plainCreate("app", 1)); err != nil {
		t.Fatal(err)
	}
	seq := raft.State().StateSeq
	want := func(what string, err error, code cluster.Code) {
		t.Helper()
		var refusal *cluster.Error
		if !errors.As(err, &refusal) || refusal.Code != code {
			t.Fatalf("%s: got %v, want %v", what, err, code)
		}
		if raft.IsDiverged() {
			t.Fatalf("%s: the refusal reached the log and set the flag", what)
		}
		if got := raft.State().StateSeq; got != seq {
			t.Fatalf("%s: state_seq moved from %d to %d", what, seq, got)
		}
	}
	_, err := n.applyAsLeader(context.Background(), cluster.CommandCreateDatabase, unreadFieldCreate("x", 2))
	want("applyAsLeader with an unread field", err, cluster.CodeInvalidArgument)
	_, err = n.proposeAtSeq(context.Background(), cluster.CommandCreateDatabase, unreadFieldCreate("x", 2), uint64(seq))
	want("proposeAtSeq with an unread field", err, cluster.CodeInvalidArgument)
	_, err = n.applyAsLeader(context.Background(), cluster.CommandCreateDatabase, plainCreate("again", 1))
	want("applyAsLeader restamping a taken id", err, cluster.CodeConflict)
	reply := n.serveRelay(context.Background(), transport.Handshake{NodeID: divergedSender}, relayFromVoter(cluster.CommandCreateDatabase, unreadFieldCreate("y", 2)))
	if reply.Status != transport.RelayRefused || cluster.Code(reply.Code) != cluster.CodeInvalidArgument {
		t.Fatalf("a relayed body the leader does not read: %+v, want INVALID_ARGUMENT refused", reply)
	}
	if raft.IsDiverged() || raft.State().StateSeq != seq {
		t.Fatal("a relayed refusal reached the log")
	}
}
