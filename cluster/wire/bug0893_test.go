package wire

import (
	"errors"
	"slices"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
)

// TestAStalePassDoesNotRetireAPairWrittenBeforeItReadItsGeneration is BUG-0893, the window
// BUG-0757's guard left open. The driver reads the state a pass will judge
// (Node.driveSeeding) well before the pass starts to sweep. Here the database is committed and
// its first write creates the pair's ready record after that read and before the pass is
// called, so the record is older than anything the pass reads for itself: the pass judged the
// pair a leftover of a database that is gone and retired it, although the committed table
// keeps it on this node. On a pair whose copy set is this node alone the retired record never
// became ready again.
func TestAStalePassDoesNotRetireAPairWrittenBeforeItReadItsGeneration(t *testing.T) {
	before := threeNodesBeforeTheDatabase(t)
	live := threeNodes(t)
	if live.StateSeq != before.StateSeq+1 || len(before.Databases) != 0 || len(live.Databases) != 1 {
		t.Fatalf("the states are not one create apart: seq %d and %d, %d and %d databases", before.StateSeq, live.StateSeq, len(before.Databases), len(live.Databases))
	}
	slots := findSlots(t, live)
	e := uint64(live.Epoch)
	kv := openKV(t, t.TempDir())
	t.Cleanup(func() { _ = kv.Close() })
	g := newRig(t, kv, before, testSelf, true)
	g.boot(t)

	// The driver has read the older state. Before it calls the pass, the database is committed
	// and a write to the owner's pair creates its record.
	g.state = live
	if err := g.rec.ApplyAdvance(slots.owner, reconDB, advanceTo(kv, slots.owner, 1, e)); err != nil {
		t.Fatalf("the first write to the new database's pair: %v", err)
	}
	if err := g.rec.OnStateSeq(t.Context(), before); err != nil {
		t.Fatalf("the pass over the older state: %v", err)
	}

	for _, act := range g.sink.of(slots.owner) {
		if act.Do == machine.SlotRetire {
			t.Fatalf("the pass over a state that predates the database retired a pair of it: %+v", act)
		}
	}
	if rec, ok := diskRecord(t, kv, slots.owner); !ok || rec.State != machine.SeedReady {
		t.Fatalf("after the stale pass the record is %+v (found %v), want ready", rec, ok)
	}
	slotsView := recordSlots{parts: newSeedPartsFor(g.rec, live, testSelf)}
	if !slotsView.Admits(slots.owner, reconDB) {
		t.Fatalf("the pair is %q on its owner after the pass, so its writes are refused: want ready",
			notReadyCause(slotsView.State(slots.owner, reconDB)))
	}
	if err := g.rec.ApplyAdvance(slots.owner, reconDB, advanceTo(kv, slots.owner, 2, e)); err != nil {
		t.Fatalf("the second write to the pair (ErrPairNotReady %v): %v", errors.Is(err, ErrPairNotReady), err)
	}

	// The next pass runs on the live state and keeps the pair as it is.
	if err := g.rec.OnStateSeq(t.Context(), live); err != nil {
		t.Fatalf("the pass over the live state: %v", err)
	}
	if rec, ok := diskRecord(t, kv, slots.owner); !ok || rec.State != machine.SeedReady || rec.Applied.Seq != 2 {
		t.Fatalf("after the live pass the record is %+v (found %v), want ready at seq 2", rec, ok)
	}
}

// threeNodesAndASecondDatabase is threeNodes one commit later: database "other" (id 2) exists
// too, and nothing about "app" or the members changed.
func threeNodesAndASecondDatabase(t *testing.T, st cluster.ClusterState) cluster.ClusterState {
	t.Helper()
	st.Members = slices.Clone(st.Members)
	st.Databases = slices.Clone(st.Databases)
	apply(t, &st, cluster.CommandCreateDatabase, bsonDoc(bsonStr("name", "other"), bsonI64("id", 2),
		bsonI32("rf", 2), bsonI64("schema_version", 1)))
	return st
}

// TestARetireAStalePassLeftIsDoneByTheNextPass is the other side of BUG-0893's fix: a pass
// over a state that was moved past leaves a ready pair that is out of the set alone, and the
// retire is deferred, not lost. The pass on the newer state, which the driver runs because the
// state moved, retires the pair.
func TestARetireAStalePassLeftIsDoneByTheNextPass(t *testing.T) {
	older := threeNodes(t)
	live := threeNodesAndASecondDatabase(t, older)
	if live.StateSeq != older.StateSeq+1 {
		t.Fatalf("the states are not one commit apart: seq %d and %d", older.StateSeq, live.StateSeq)
	}
	slots := findSlots(t, live)
	kv := openKV(t, t.TempDir())
	t.Cleanup(func() { _ = kv.Close() })
	putRecord(t, kv, slots.outside, readyRecord(cluster.Position{Epoch: uint64(older.Epoch), Seq: 7}))
	g := newRig(t, kv, live, testSelf, true)
	if err := g.rec.LoadRecords(); err != nil {
		t.Fatalf("LoadRecords: %v", err)
	}

	if err := g.rec.OnStateSeq(t.Context(), older); err != nil {
		t.Fatalf("the pass over the older state: %v", err)
	}
	if rec, ok := diskRecord(t, kv, slots.outside); !ok || rec.State != machine.SeedReady {
		t.Fatalf("after the stale pass the record is %+v (found %v), want ready: the pass judged a state that is not the committed one", rec, ok)
	}
	if acts := g.sink.of(slots.outside); len(acts) != 0 {
		t.Fatalf("the stale pass asked for %+v on a pair it must leave alone", acts)
	}

	if err := g.rec.OnStateSeq(t.Context(), live); err != nil {
		t.Fatalf("the pass over the live state: %v", err)
	}
	if rec, ok := diskRecord(t, kv, slots.outside); !ok || rec.State != machine.SeedRetired {
		t.Fatalf("after the pass on the live state the record is %+v (found %v), want retired", rec, ok)
	}
	retires := 0
	for _, act := range g.sink.of(slots.outside) {
		if act.Do == machine.SlotRetire {
			retires++
		}
	}
	if retires != 1 {
		t.Fatalf("the pass on the live state emitted %d retires for the pair, want 1: %+v", retires, g.sink.of(slots.outside))
	}
}

// TestAStaleResettingPassStillRetiresARecordFromDisk pins the leg of the guard that a
// resetting pass (a joiner's first) keeps. Such a pass leaves alone only a pair that changed
// since it began: the pass after it resets nothing, so a record loaded from disk that it
// skipped while out of the set would stay ready, with what it held before the join, if the
// next commit put the pair back in the set.
func TestAStaleResettingPassStillRetiresARecordFromDisk(t *testing.T) {
	joining := joiningNodes(t)
	live := threeNodesAndASecondDatabase(t, joining)
	slots := findSlots(t, joining)
	kv := openKV(t, t.TempDir())
	t.Cleanup(func() { _ = kv.Close() })
	putRecord(t, kv, slots.outside, readyRecord(cluster.Position{Epoch: 1, Seq: 7}))
	g := newRig(t, kv, live, testSelf, true)
	if err := g.rec.LoadRecords(); err != nil {
		t.Fatalf("LoadRecords: %v", err)
	}
	if err := g.rec.OnJoiningCommit(joining); err != nil {
		t.Fatalf("the joining pass over the older state: %v", err)
	}
	if rec, ok := diskRecord(t, kv, slots.outside); !ok || rec.State != machine.SeedRetired {
		t.Fatalf("after the stale joining pass the record is %+v (found %v), want retired", rec, ok)
	}
}

// TestAResettingPassLeavesAPairWrittenSinceItBegan pins the leg of the guard a resetting pass
// keeps: a pair that changed since the pass began is left alone when the state was moved past.
// A joiner's first pass starts on the state before the database; while it sweeps, the database
// is committed and a live step creates the pair's ready record on the joiner, which its own
// placing set holds. The pass must not retire that record as a leftover of a database that is
// gone (BUG-0757, on the pass that resets).
func TestAResettingPassLeavesAPairWrittenSinceItBegan(t *testing.T) {
	before := threeNodesBeforeTheDatabase(t)
	setState(t, &before, testSelf, "down")
	replace(t, &before, testSelf, 2)
	live := before
	live.Members = slices.Clone(before.Members)
	apply(t, &live, cluster.CommandCreateDatabase, bsonDoc(bsonStr("name", "app"), bsonI64("id", int64(reconDB)),
		bsonI32("rf", 2), bsonI64("schema_version", 1)))
	if live.StateSeq != before.StateSeq+1 || len(before.Databases) != 0 || len(live.Databases) != 1 {
		t.Fatalf("the states are not one create apart: seq %d and %d, %d and %d databases", before.StateSeq, live.StateSeq, len(before.Databases), len(live.Databases))
	}
	slots := findSlots(t, live)
	e := uint64(live.Epoch)
	kv := openKV(t, t.TempDir())
	t.Cleanup(func() { _ = kv.Close() })
	store := &snapshotHookStore{Store: kv}
	g := newRigOn(t, kv, store, before, testSelf, true)
	if err := g.rec.LoadRecords(); err != nil {
		t.Fatalf("LoadRecords: %v", err)
	}

	// The pass takes its snapshot after it read its generation, so the record is newer than it.
	store.onSnapshot = func() {
		g.state = live
		if err := g.rec.ApplyAdvance(slots.owner, reconDB, advanceTo(kv, slots.owner, 1, e)); err != nil {
			t.Errorf("the first write to the new database's pair: %v", err)
		}
	}
	if err := g.rec.OnJoiningCommit(before); err != nil {
		t.Fatalf("the joining pass over the older state: %v", err)
	}
	if store.onSnapshot != nil {
		t.Fatal("the pass took no snapshot, so the write never ran inside it")
	}

	for _, act := range g.sink.of(slots.owner) {
		if act.Do == machine.SlotRetire {
			t.Fatalf("the joining pass over a state that predates the database retired a pair of it: %+v", act)
		}
	}
	if rec, ok := diskRecord(t, kv, slots.owner); !ok || rec.State != machine.SeedReady || rec.Applied.Seq != 1 {
		t.Fatalf("after the stale joining pass the record is %+v (found %v), want ready at seq 1", rec, ok)
	}
}
