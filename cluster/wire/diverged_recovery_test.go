package wire

import (
	"slices"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage"
)

// ownedAtFactorOne is a slot whose only copy, with replication factor 1, is testSelf.
func ownedAtFactorOne(t *testing.T, st cluster.ClusterState) uint16 {
	t.Helper()
	table := reconcilePlacingTable(st, testSelf)
	for slot := 0; slot < storage.SlotCount; slot++ {
		if copies := table.Copies(uint16(slot), 1); len(copies) == 1 && copies[0].ID == testSelf {
			return uint16(slot)
		}
	}
	t.Fatal("no slot is owned by the test node at replication factor 1")
	return 0
}

// retiredInTheWindow writes what a build from before the mixed-build spec left behind when its
// reconciler ran on a frozen state (section 4): the pair's record retired, the fence durable,
// the data untouched.
func retiredInTheWindow(t *testing.T, g *rig, slot uint16, fence cluster.Position) {
	t.Helper()
	rec := retiredRecord()
	rec.Applied = fence
	putRecord(t, g.kv, slot, rec)
	putFence(t, g.kv, slot, fence)
}

// requireStillRetiredAndFenced is the state failure mode 16 names: on disk and in the
// reconciler the record is retired (the slot record state, not only the bytes) and the fence is
// still written.
func requireStillRetiredAndFenced(t *testing.T, g *rig, slot uint16, fence cluster.Position) {
	t.Helper()
	disk, ok := diskRecord(t, g.kv, slot)
	if !ok || disk.State != machine.SeedRetired {
		t.Fatalf("the record of slot %d on disk is %+v (present %v), want retired", slot, disk, ok)
	}
	if rec, ok := g.rec.Record(slot, reconDB); !ok || rec.State != machine.SeedRetired {
		t.Fatalf("the reconciler's record of slot %d is %+v (present %v), want retired", slot, rec, ok)
	}
	if got, ok := hasFence(t, g.kv, slot); !ok || got != fence {
		t.Fatalf("the fence of slot %d is %v (present %v), want %v", slot, got, ok, fence)
	}
}

// TestAfterRecoveryAReplicationFactorOnePairIsStillReady is test 17, part three (spec section 4,
// BUG-0798, failure mode 16's other side): the record store of a replication factor 1 node
// holds a ready pair of database 1. While the node is diverged and restarted, its frozen state
// lacks database 1 and every guard keeps the pair's slot record ready and unfenced with no pass
// run; after the node is recovered on the build that wrote the log (a second Raft seat whose
// replayed log holds database 1's CREATE, over the same record store) the pass runs against the
// real state and the record is still ready and unfenced: nothing was retired, nothing deleted.
// The two Raft seats share one Badger store because the old build's refusal cannot be
// reproduced on a directory this build then reads (the hide hook is set only on a fresh node);
// the record store is the part that recovery must not have changed. Break (run): remove the
// four guards, and the first phase retires and fences the pair (today's behaviour: SlotRetired
// with a fence, red).
func TestAfterRecoveryAReplicationFactorOnePairIsStillReady(t *testing.T) {
	kv := openKV(t, t.TempDir()+"/data")
	t.Cleanup(func() { _ = kv.Close() })
	const slot = 5

	// Run one: the diverged build, restarted, with its frozen state lacking database 1.
	diverged := t.TempDir()
	first := openDivergedRaft(t, diverged)
	flagNode(t, first)
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	again := openDivergedRaft(t, diverged)
	n := bareNode(again)
	g, parts := seedingRigOn(t, kv, n)
	putRecord(t, kv, slot, readyRecord(cluster.Position{Epoch: 1, Seq: 3}))
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
		t.Fatalf("%d reconcile passes ran on the diverged node", got)
	}

	// Run two: recovered. The log holds database 1 (stamped at the state it was written at).
	recovered := openDivergedRaft(t, t.TempDir())
	if err := recovered.AppendWithoutDryRun(divergedCommand(t, cluster.CommandCreateDatabase, uint64(recovered.State().StateSeq), plainCreate("app", int64(reconDB)))); err != nil {
		t.Fatal(err)
	}
	if recovered.IsDiverged() || !recovered.LogApplied() || len(recovered.State().Databases) != 1 {
		t.Fatalf("the recovered seat is not caught up: diverged %v, applied %v, databases %d", recovered.IsDiverged(), recovered.LogApplied(), len(recovered.State().Databases))
	}
	n2 := bareNode(recovered)
	g2, parts2 := seedingRigOn(t, kv, n2)
	n2.cl.seeding.Store(parts2)
	if err := g2.rec.LoadRecords(); err != nil {
		t.Fatal(err)
	}
	driveTicks(t, n2, 8)
	if got := parts2.passes.Load(); got == 0 {
		t.Fatal("no reconcile pass ran on the recovered node: the test proves nothing about the pass")
	}
	// A healthy pass acts on the pairs it owns (report bits, catch up); what it must not do is
	// retire, fence or delete any pair, this one above all.
	for _, a := range g2.sink.actions {
		switch a.Do {
		case machine.SlotRetire, machine.SlotGate, machine.SlotDeleteRange, machine.SlotDeleteRangeThenSeed, machine.SlotRestartSeed:
			t.Fatalf("the recovered node's pass took a retiring action: %+v", a)
		}
	}
	if rec, ok := g2.rec.Record(slot, reconDB); !ok || rec.State != machine.SeedReady {
		t.Fatalf("the record of slot %d is %+v (present %v), want ready", slot, rec, ok)
	}
	if disk, ok := diskRecord(t, kv, slot); !ok || disk.State != machine.SeedReady {
		t.Fatalf("the record of slot %d on disk is %+v (present %v), want ready", slot, disk, ok)
	}
	if fence, ok := hasFence(t, kv, slot); ok {
		t.Fatalf("slot %d is fenced at %+v after recovery: its pair is unreadable", slot, fence)
	}
}

// seedingRigOn is seedingRig over a record store the caller owns, with the reconciler's own
// guard wired, so two Raft seats can be driven over one store.
func seedingRigOn(t *testing.T, kv *storage.BadgerKV, n *Node) (*rig, *seedParts) {
	t.Helper()
	g := newRig(t, kv, cluster.ClusterState{}, "n1", false)
	g.rec.cfg.State = raftState{n: n}
	g.rec.cfg.Diverged = n.raft.IsDiverged
	return g, newSeedPartsFor(g.rec, n.raft.State(), "n1")
}

// TestAfterRecoveryAReplicationFactorOnePairRetiredInTheWindowStaysRetired is test 17, part
// four (section 4, failure mode 16, check 18): a build from before the spec retired and fenced a
// replication factor 1 pair while its state was frozen. After recovery the database is back,
// this node is the pair's only copy, and nothing brings the pair back: the pass leaves the
// record SeedRetired and the fence written, the gate waits for nobody and deletes nothing,
// 'seeding retry' (SeedRetryAsked) is refused SEED_NOT_HELD and changes nothing, and the
// record state the read path and POSITION_REPLY use is retired. This records what the code does
// (BUG-0798's open part), not what is wanted. Break (run): let the gate pass without an
// attestation (retiredPlan returns slotPlanDelete): the pass then orders DeleteRangeThenSeed
// on the only copy.
func TestAfterRecoveryAReplicationFactorOnePairRetiredInTheWindowStaysRetired(t *testing.T) {
	st := threeNodes(t)
	alterRF(t, &st, 1)
	slot := ownedAtFactorOne(t, st)
	kv := openKV(t, t.TempDir())
	t.Cleanup(func() { _ = kv.Close() })
	g := newRig(t, kv, st, testSelf, true)
	fence := cluster.Position{Epoch: uint64(st.Epoch), Seq: 30}
	retiredInTheWindow(t, g, slot, fence)
	g.boot(t)

	acts := g.sink.of(slot)
	if len(acts) != 1 || acts[0].Do != machine.SlotGate || len(acts[0].Waiting) != 0 || acts[0].Serve {
		t.Fatalf("a retired replication factor 1 pair got %+v, want the gate naming nobody and serving nothing", acts)
	}
	requireStillRetiredAndFenced(t, g, slot, fence)
	if slices.ContainsFunc(g.sink.actions, func(a Action) bool {
		return a.Do == machine.SlotDeleteRange || a.Do == machine.SlotDeleteRangeThenSeed || a.Do == machine.SlotRestartSeed
	}) {
		t.Fatalf("the pass deleted or restarted the only copy: %+v", g.sink.actions)
	}
	if got := g.rec.WaitingFor(slot, reconDB); len(got) != 0 {
		t.Fatalf("the gate waits for %v, but there is nobody else who holds the pair", got)
	}

	act, err := g.rec.Feed(slot, reconDB, machine.SeedRetryAsked{})
	if err != nil || act.Refusal != cluster.CodeSeedNotHeld || act.Do != 0 {
		t.Fatalf("'seeding retry' on the retired pair gave %+v, %v, want SEED_NOT_HELD and no action", act, err)
	}
	requireStillRetiredAndFenced(t, g, slot, fence)

	// A restart of the process over the same store boots a fresh reconciler: still retired.
	restarted := newRig(t, kv, st, testSelf, true)
	restarted.boot(t)
	requireStillRetiredAndFenced(t, restarted, slot, fence)
	if positionState(machine.SlotRetired) == PositionStateReady {
		t.Fatal("a retired pair is reported as ready to a peer's position query")
	}
}

// TestAfterRecoveryAReplicationFactorTwoPairRetiredInTheWindowIsReseededFromACopy is test 17,
// part five (the replication factor 2 and above path, section 4 and 6.2): the same record and
// fence as the test above, but another member holds a copy. The gate names that member and
// deletes nothing until it attests a position that covers the fence; only then the retired
// range is deleted and the pair is seeded again from reset, which is how an RF 2 node recovers
// a pair a pre-spec build retired. Break: open the gate before the attestation covers the fence
// (the "behind" reply would then return DeleteRangeThenSeed).
func TestAfterRecoveryAReplicationFactorTwoPairRetiredInTheWindowIsReseededFromACopy(t *testing.T) {
	st := threeNodes(t)
	slots := findSlots(t, st)
	kv := openKV(t, t.TempDir())
	t.Cleanup(func() { _ = kv.Close() })
	g := newRig(t, kv, st, testSelf, true)
	epoch := uint64(st.Epoch)
	fence := cluster.Position{Epoch: epoch, Seq: 30}
	retiredInTheWindow(t, g, slots.copyOnly, fence)
	g.boot(t)

	acts := g.sink.of(slots.copyOnly)
	if len(acts) != 1 || acts[0].Do != machine.SlotGate || len(acts[0].Waiting) != 1 || acts[0].Serve {
		t.Fatalf("a retired replication factor 2 pair got %+v, want the gate naming one member", acts)
	}
	requireStillRetiredAndFenced(t, g, slots.copyOnly, fence)
	other := acts[0].Waiting[0]

	behind := machine.PositionReplied{Node: other, Epoch: epoch, State: machine.SlotReady, Applied: cluster.Position{Epoch: epoch, Seq: 29}}
	if act, err := g.rec.Feed(slots.copyOnly, reconDB, behind); err != nil || act.Do != machine.SlotGate {
		t.Fatalf("a copy behind the fence gave %+v, %v, want the gate to stay shut", act, err)
	}
	requireStillRetiredAndFenced(t, g, slots.copyOnly, fence)

	covers := behind
	covers.Applied.Seq = 30
	if act, err := g.rec.Feed(slots.copyOnly, reconDB, covers); err != nil || act.Do != machine.SlotDeleteRangeThenSeed {
		t.Fatalf("a covering copy gave %+v, %v, want DeleteRangeThenSeed", act, err)
	}
	if act, err := g.rec.Feed(slots.copyOnly, reconDB, machine.RetireStarted{Fence: fence}); err != nil || act.Do != machine.SlotDeleteRange {
		t.Fatalf("RetireStarted gave %+v, %v, want DeleteRange", act, err)
	}
	if act, err := g.rec.Feed(slots.copyOnly, reconDB, machine.RetireDeleted{}); err != nil || act.Do != machine.SlotRestartSeed {
		t.Fatalf("RetireDeleted gave %+v, %v, want RestartSeed", act, err)
	}
	if disk, _ := diskRecord(t, kv, slots.copyOnly); disk.State != machine.SeedSeeding || disk.Phase != machine.PhaseReset {
		t.Fatalf("after the delete the record is %+v, want seeding from reset", disk)
	}
}
