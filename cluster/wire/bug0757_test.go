package wire

import (
	"bytes"
	"errors"
	"testing"

	"github.com/hoarddb/hoarddb-bson/x/bsonx/bsoncore"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage"
)

// threeNodesBeforeTheDatabase is threeNodes at the state_seq before "app" was created: the
// state a reconcile pass that started a moment earlier still holds.
func threeNodesBeforeTheDatabase(t *testing.T) cluster.ClusterState {
	t.Helper()
	var st cluster.ClusterState
	self := bsonDoc(bsonStr("id", "n1"), bsonI64("incarnation", 1), bsonStr("data_addr", "n1:1"))
	apply(t, &st, cluster.CommandBootstrap, bsonDoc(bsonBin("cluster_id", bytes.Repeat([]byte{7}, 16)), bsonVersion(),
		bsoncore.AppendDocumentElement(nil, "self", self)))
	for _, id := range []string{"n2", "n3"} {
		register(t, &st, id, 1)
		setState(t, &st, id, "active")
		apply(t, &st, cluster.CommandSetVoters, bsonDoc(bsonStr("id", id), bsonBool("voting", true)))
	}
	return st
}

// snapshotHookStore runs onSnapshot once, the first time the reconciler takes its snapshot:
// the moment a pass has fixed its state and starts to sweep.
type snapshotHookStore struct {
	Store
	onSnapshot func()
}

func (h *snapshotHookStore) NewSnapshot() storage.KVSnapshot {
	if hook := h.onSnapshot; hook != nil {
		h.onSnapshot = nil
		hook()
	}
	return h.Store.NewSnapshot()
}

// TestAWriteToARetiredPairInTheTargetSetIsNotRefused is BUG-0757. A reconcile pass takes its
// state when it starts and sweeps 8,192 slots per database, a long time on a disk that
// syncs in 8 ms. A database created after the pass started is not in that state, and a
// write to it in the meantime creates the pair's ready record (ApplyAdvance judges the
// pair against the live state). The pass's last step judged every held pair of a database
// its state does not list as "nothing is in its target set" and retired the new record,
// although the live committed table keeps the pair in the copy set: the owner then refused
// every write to the pair ("is retired (out of the copy set) on this node, so it orders no
// write") until a later state_seq commit gave the machine the real view.
func TestAWriteToARetiredPairInTheTargetSetIsNotRefused(t *testing.T) {
	before := threeNodesBeforeTheDatabase(t)
	live := threeNodes(t)
	if live.StateSeq != before.StateSeq+1 || len(before.Databases) != 0 || len(live.Databases) != 1 {
		t.Fatalf("the states are not one create apart: seq %d and %d, %d and %d databases", before.StateSeq, live.StateSeq, len(before.Databases), len(live.Databases))
	}
	slots := findSlots(t, live)
	e := uint64(live.Epoch)
	kv := openKV(t, t.TempDir())
	t.Cleanup(func() { _ = kv.Close() })
	store := &snapshotHookStore{Store: kv}
	g := newRigOn(t, kv, store, before, testSelf, true)
	g.boot(t)

	// The pass starts on the older state. While it sweeps, the database is committed and a
	// write to the owner's pair creates its record.
	store.onSnapshot = func() {
		g.state = live
		if err := g.rec.ApplyAdvance(slots.owner, reconDB, advanceTo(kv, slots.owner, 1, e)); err != nil {
			t.Errorf("the first write to the new database's pair: %v", err)
		}
	}
	if err := g.rec.OnStateSeq(t.Context(), before); err != nil {
		t.Fatalf("the pass over the older state: %v", err)
	}
	if store.onSnapshot != nil {
		t.Fatal("the pass took no snapshot, so the write never ran inside it")
	}

	for _, act := range g.sink.of(slots.owner) {
		if act.Do == machine.SlotRetire {
			t.Fatalf("the pass over a state that predates the database retired a pair of it: %+v", act)
		}
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

// TestAPairOfADatabaseNoStateListsIsStillRetired is the other side of BUG-0757's fix: the
// pass leaves a pair alone only while the live state is newer than the pass's own. A ready
// record of a database that the live state does not list either is retired as before.
func TestAPairOfADatabaseNoStateListsIsStillRetired(t *testing.T) {
	live := threeNodesBeforeTheDatabase(t)
	withDatabase := threeNodes(t)
	slots := findSlots(t, withDatabase)
	kv := openKV(t, t.TempDir())
	t.Cleanup(func() { _ = kv.Close() })
	putRecord(t, kv, slots.owner, readyRecord(cluster.Position{Epoch: uint64(withDatabase.Epoch), Seq: 7}))
	g := newRig(t, kv, live, testSelf, true)
	g.boot(t)
	if rec, ok := diskRecord(t, kv, slots.owner); !ok || rec.State != machine.SeedRetired {
		t.Fatalf("a ready pair of a database no state lists is %+v (found %v), want retired", rec, ok)
	}
}
