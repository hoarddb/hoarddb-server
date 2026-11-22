package wire

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson/bsontype"
	"github.com/hoarddb/hoarddb-bson/x/bsonx/bsoncore"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage"
)

const (
	testSelf = "n2"
	reconDB  = uint64(1)
)

// --- ClusterState built through the commands themselves -------------------

func bsonDoc(elems ...[]byte) []byte   { return bsoncore.BuildDocument(nil, bytes.Join(elems, nil)) }
func bsonStr(k, v string) []byte       { return bsoncore.AppendStringElement(nil, k, v) }
func bsonI64(k string, v int64) []byte { return bsoncore.AppendInt64Element(nil, k, v) }
func bsonI32(k string, v int32) []byte { return bsoncore.AppendInt32Element(nil, k, v) }

// bsonVersion is BOOTSTRAP's cluster_version at this build's level.
func bsonVersion() []byte { return bsonI32("cluster_version", int32(cluster.StateLevel)) }

// bsonLevels is REGISTER_MEMBER's min_level and max_level: the joiner runs this build's range.
func bsonLevels() []byte {
	levels := cluster.BuildStateRange()
	return append(bsonI32("min_level", int32(levels.Min)), bsonI32("max_level", int32(levels.Max))...)
}
func bsonBool(k string, v bool) []byte {
	element := bsoncore.AppendHeader(nil, bsontype.Boolean, k)
	if v {
		return append(element, 1)
	}
	return append(element, 0)
}
func bsonBin(k string, b []byte) []byte {
	return bsoncore.AppendBinaryElement(nil, k, bsontype.BinaryGeneric, b)
}

func apply(t *testing.T, st *cluster.ClusterState, kind byte, body []byte) {
	t.Helper()
	if _, err := st.ApplyCommand(cluster.Command{Version: cluster.CommandVersion, Kind: kind, BaseSeq: uint64(st.StateSeq), Body: body}); err != nil {
		t.Fatalf("command 0x%02x refused: %v", kind, err)
	}
}

func register(t *testing.T, st *cluster.ClusterState, id string, inc int64) {
	t.Helper()
	addr := id + ":1"
	if len(id) > 200 {
		addr = "long-id-host:1" // the cluster keeps a data address to 256 bytes, so a 256-byte id cannot also be its host
	}
	apply(t, st, cluster.CommandRegisterMember, bsonDoc(bsonStr("id", id), bsonI64("incarnation", inc),
		bsonStr("data_addr", addr), bsonBool("witness", false), bsonBin("placing_set", make([]byte, 1024)), bsonLevels()))
}

func setState(t *testing.T, st *cluster.ClusterState, id, word string) {
	t.Helper()
	apply(t, st, cluster.CommandSetMemberState, bsonDoc(bsonStr("id", id), bsonStr("state", word)))
}

func replace(t *testing.T, st *cluster.ClusterState, id string, inc int64) {
	t.Helper()
	apply(t, st, cluster.CommandSetMemberState, bsonDoc(bsonStr("id", id), bsonStr("state", "joining"),
		bsonI64("incarnation", inc), bsonBool("clear_bits", true)))
}

// threeNodes is n1, n2, n3 active and voters, and database "app" (id 1) at RF 2.
func threeNodes(t *testing.T) cluster.ClusterState {
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
	apply(t, &st, cluster.CommandCreateDatabase, bsonDoc(bsonStr("name", "app"), bsonI64("id", int64(reconDB)),
		bsonI32("rf", 2), bsonI64("schema_version", 1)))
	return st
}

// joiningNodes is threeNodes with n2 replaced, so n2 is joining: the member whose absent
// in-target pairs the reconciler seeds (an active member gets the interim, seeding S0).
func joiningNodes(t *testing.T) cluster.ClusterState {
	t.Helper()
	st := threeNodes(t)
	setState(t, &st, testSelf, "down")
	replace(t, &st, testSelf, 2)
	return st
}

func withBit(t *testing.T, st *cluster.ClusterState, slot int) {
	t.Helper()
	bits := make([]byte, 1024)
	bits[slot/8] |= 1 << (slot % 8)
	apply(t, st, cluster.CommandSeeded, bsonDoc(bsonStr("node", testSelf), bsonI64("db_id", int64(reconDB)), bsonBin("bits", bits)))
}

// slotsOf finds a slot where n2 is a non-owner copy, one where it owns, and
// one where it holds nothing.
type slotsOf struct{ copyOnly, owner, outside uint16 }

func findSlots(t *testing.T, st cluster.ClusterState) slotsOf {
	t.Helper()
	table := reconcilePlacingTable(st, testSelf) // a joining n2 ranks itself in its own placing set
	found := [3]bool{}
	var s slotsOf
	for slot := 0; slot < storage.SlotCount && (!found[0] || !found[1] || !found[2]); slot++ {
		copies := table.Copies(uint16(slot), 2)
		owner, _ := table.Owner(uint16(slot), 2)
		in := copies[0].ID == testSelf || copies[1].ID == testSelf
		switch {
		case in && owner.ID == testSelf && !found[1]:
			s.owner, found[1] = uint16(slot), true
		case in && owner.ID != testSelf && !found[0]:
			s.copyOnly, found[0] = uint16(slot), true
		case !in && !found[2]:
			s.outside, found[2] = uint16(slot), true
		}
	}
	return s
}

// --- storage, clock, sink -------------------------------------------------

func openKV(t *testing.T, dir string) *storage.BadgerKV {
	t.Helper()
	kv, err := storage.OpenBadgerKV(dir, storage.BadgerTuning{MemTableMB: 32}, nil)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	return kv
}

type reconClock struct{ at time.Time }

func (c *reconClock) Now() time.Time { c.at = c.at.Add(time.Second); return c.at }

type stateFn func() cluster.ClusterState

func (f stateFn) State() cluster.ClusterState { return f() }

// recordingSink remembers every action; hook runs at the moment of the call.
type recordingSink struct {
	actions []Action
	hook    func(Action)
	fail    error // returned from Do after the hook, for a crash at that point
}

func (s *recordingSink) Do(a Action) error {
	if s.hook != nil {
		s.hook(a)
	}
	s.actions = append(s.actions, a)
	return s.fail
}

func (s *recordingSink) of(slot uint16) []Action {
	var out []Action
	for _, a := range s.actions {
		if a.Slot == slot {
			out = append(out, a)
		}
	}
	return out
}

type rig struct {
	kv    *storage.BadgerKV
	rec   *Reconciler
	sink  *recordingSink
	pairs *cluster.PairSet
	state cluster.ClusterState
}

func newRig(t *testing.T, kv *storage.BadgerKV, st cluster.ClusterState, self string, current bool) *rig {
	t.Helper()
	return newRigOn(t, kv, kv, st, self, current)
}

// newRigOn is newRig with the reconciler reading and writing through store, so
// a test can fail a commit.
func newRigOn(t *testing.T, kv *storage.BadgerKV, store Store, st cluster.ClusterState, self string, current bool) *rig {
	t.Helper()
	g := &rig{kv: kv, sink: &recordingSink{}, pairs: &cluster.PairSet{}, state: st}
	g.rec = NewReconciler(Config{
		NodeID: self, Store: store, Clock: &reconClock{at: time.Unix(1_700_000_000, 0)},
		State: stateFn(func() cluster.ClusterState { return g.state }), Sink: g.sink, Pairs: g.pairs,
		Current: func(uint16, uint64) bool { return current }, WindowBytes: 1 << 20,
	})
	return g
}

func (g *rig) boot(t *testing.T) {
	t.Helper()
	if err := g.rec.OnBoot(t.Context()); err != nil {
		t.Fatalf("OnBoot: %v", err)
	}
}

func putRecord(t *testing.T, kv *storage.BadgerKV, slot uint16, rec SeedRecord) {
	t.Helper()
	b := kv.NewBatch()
	defer b.Discard()
	if err := b.Set(SeedRecordKey(slot, reconDB), mustEncode(t, rec)); err != nil {
		t.Fatal(err)
	}
	if err := b.Commit(true); err != nil {
		t.Fatal(err)
	}
}

func putFence(t *testing.T, kv *storage.BadgerKV, slot uint16, fence cluster.Position) {
	t.Helper()
	b := kv.NewBatch()
	defer b.Discard()
	if err := b.Set(reconcileFenceKey(reconcileKey{slot, reconDB}, fence), []byte{}); err != nil {
		t.Fatal(err)
	}
	if err := b.Commit(true); err != nil {
		t.Fatal(err)
	}
}

func diskRecord(t *testing.T, kv *storage.BadgerKV, slot uint16) (SeedRecord, bool) {
	t.Helper()
	snap := kv.NewSnapshot()
	defer snap.Discard()
	raw, err := snap.Get(SeedRecordKey(slot, reconDB))
	if errors.Is(err, storage.ErrKeyNotFound) {
		return SeedRecord{}, false
	}
	if err != nil {
		t.Fatal(err)
	}
	rec, err := DecodeSeedRecord(raw)
	if err != nil {
		t.Fatal(err)
	}
	return rec, true
}

func hasFence(t *testing.T, kv *storage.BadgerKV, slot uint16) (cluster.Position, bool) {
	t.Helper()
	snap := kv.NewSnapshot()
	defer snap.Discard()
	pos, ok, err := reconcileReadFence(snap, reconcileKey{slot, reconDB})
	if err != nil {
		t.Fatal(err)
	}
	return pos, ok
}

func readyRecord(applied cluster.Position) SeedRecord {
	rec := SeedRecord{WALCursor: 11, Ordinal: 22}
	rec.State, rec.Applied, rec.Source = machine.SeedReady, applied, "n1"
	rec.SeedID = [16]byte{9}
	return rec
}

func seedingRecord(phase machine.Phase, cut cluster.Position, cursor string) SeedRecord {
	rec := SeedRecord{}
	rec.State, rec.Phase, rec.Cut, rec.Applied, rec.Source = machine.SeedSeeding, phase, cut, cut, "n1"
	rec.SeedID = [16]byte{8}
	rec.Cursor = []byte(cursor)
	return rec
}

func retiredRecord() SeedRecord {
	rec := SeedRecord{}
	rec.State, rec.Applied = machine.SeedRetired, cluster.Position{Epoch: 1, Seq: 4}
	return rec
}

// --- the nine tests of the U44 row ----------------------------------------

// TestSeedRecordResumesAtItsCutOnDisk kills a seeding node and restarts it on
// the same store: the resumed attempt keeps the seed id, the cut and the cursor
// that were written with the chunk.
func TestSeedRecordResumesAtItsCutOnDisk(t *testing.T) {
	st := threeNodes(t)
	slots := findSlots(t, st)
	dir := t.TempDir()
	kv := openKV(t, dir)
	cut := cluster.Position{Epoch: uint64(st.Epoch), Seq: 40}
	want := seedingRecord(machine.PhaseSnapshot, cut, "key-0042")
	putRecord(t, kv, slots.copyOnly, want)
	if err := kv.Close(); err != nil {
		t.Fatal(err)
	}

	kv = openKV(t, dir)
	t.Cleanup(func() { _ = kv.Close() })
	g := newRig(t, kv, st, testSelf, true)
	g.boot(t)

	acts := g.sink.of(slots.copyOnly)
	if len(acts) != 1 || acts[0].Do != machine.SlotResumeSeed {
		t.Fatalf("actions for the seeding slot = %+v, want one ResumeSeed", acts)
	}
	got, ok := g.rec.Record(slots.copyOnly, reconDB)
	if !ok || got.SeedID != want.SeedID || got.Cut != cut || !bytes.Equal(got.Cursor, []byte("key-0042")) {
		t.Fatalf("resumed record = %+v, want seed id %x cut %v cursor key-0042", got, want.SeedID, cut)
	}
}

// TestReconciliationTable runs seeding §3's table, one subtest per line.
func TestReconciliationTable(t *testing.T) {
	base := threeNodes(t)
	slots := findSlots(t, base)
	joiningBase := joiningNodes(t)
	joiningSlots := findSlots(t, joiningBase)
	e := uint64(base.Epoch)
	cases := []struct {
		name    string
		slot    uint16
		joining bool // n2 is joining (joiningBase), else active (base)
		prep    func(t *testing.T, kv *storage.BadgerKV, slot uint16)
		bit     bool
		// otherBit commits the SEEDED bit of the slot's other copy-set member.
		otherBit bool
		current  bool
		do       uint8
		restart  uint8
		state    uint8 // record state on disk afterwards, 0 for absent
		phase    machine.Phase
	}{
		{name: "absent, not in target: nothing", slot: slots.outside, do: machine.SlotDoNothing},
		{name: "absent, in target, joining: seeding reset, queued", slot: joiningSlots.copyOnly, joining: true, do: machine.SlotQueueSeed,
			state: machine.SeedSeeding, phase: machine.PhaseReset},
		// Seeding S0's interim (plan §0.3): until the committed bits exist, an active member
		// writes an absent in-target pair ready empty instead of seeding it.
		{name: "absent, in target, not joining: ready empty, the interim", slot: slots.copyOnly, do: machine.SlotReportBit,
			state: machine.SeedReady, phase: machine.PhaseNone},
		{name: "absent, in target, not joining, another member holds the bit: seeding reset, queued", slot: slots.copyOnly, otherBit: true,
			do: machine.SlotQueueSeed, state: machine.SeedSeeding, phase: machine.PhaseReset},
		{name: "seeding, same cut: resume", slot: slots.copyOnly, do: machine.SlotResumeSeed, state: machine.SeedSeeding, phase: machine.PhaseSnapshot,
			prep: func(t *testing.T, kv *storage.BadgerKV, slot uint16) {
				putRecord(t, kv, slot, seedingRecord(machine.PhaseSnapshot, cluster.Position{Epoch: e, Seq: 5}, "k"))
			}},
		{name: "seeding, later cut epoch: restart 3 with a reset", slot: slots.copyOnly, do: machine.SlotRestartSeed, restart: 3,
			state: machine.SeedSeeding, phase: machine.PhaseReset,
			prep: func(t *testing.T, kv *storage.BadgerKV, slot uint16) {
				putRecord(t, kv, slot, seedingRecord(machine.PhaseSnapshot, cluster.Position{Epoch: e + 5, Seq: 5}, "k"))
			}},
		// SPEC-GAP: seeding section 3 says a seeding record from an older epoch or
		// cut restarts; conv section 12 contract 3 and slot.go resume a cut from
		// an equal or earlier epoch. The code follows conv section 12, and this row
		// records that. cluster/machine is not edited.
		{name: "seeding, older cut epoch: resumes (SPEC-GAP)", slot: slots.copyOnly, do: machine.SlotResumeSeed, state: machine.SeedSeeding, phase: machine.PhaseSnapshot,
			prep: func(t *testing.T, kv *storage.BadgerKV, slot uint16) {
				putRecord(t, kv, slot, seedingRecord(machine.PhaseSnapshot, cluster.Position{Epoch: e - 1, Seq: 5}, "k"))
			}},
		{name: "seeding, attempt never registered: restart 4", slot: slots.copyOnly, do: machine.SlotRestartSeed, restart: 4,
			state: machine.SeedSeeding, phase: machine.PhaseReset,
			prep: func(t *testing.T, kv *storage.BadgerKV, slot uint16) {
				putRecord(t, kv, slot, seedingRecord(machine.PhaseReset, cluster.Position{}, ""))
			}},
		{name: "seeding, not in target: delete the range and forget the record, the record stays", slot: slots.outside, do: machine.SlotDeleteRangeAndForget,
			state: machine.SeedSeeding, phase: machine.PhaseSnapshot,
			prep: func(t *testing.T, kv *storage.BadgerKV, slot uint16) {
				putRecord(t, kv, slot, seedingRecord(machine.PhaseSnapshot, cluster.Position{Epoch: e, Seq: 5}, "k"))
			}},
		{name: "ready, in target, bit 1: catch up", slot: slots.copyOnly, bit: true, do: machine.SlotCatchUp, state: machine.SeedReady,
			prep: func(t *testing.T, kv *storage.BadgerKV, slot uint16) {
				putRecord(t, kv, slot, readyRecord(cluster.Position{Epoch: e, Seq: 7}))
			}},
		{name: "ready, in target, bit 0, current: re-report", slot: slots.copyOnly, current: true, do: machine.SlotReportBit, state: machine.SeedReady,
			prep: func(t *testing.T, kv *storage.BadgerKV, slot uint16) {
				putRecord(t, kv, slot, readyRecord(cluster.Position{Epoch: e, Seq: 7}))
			}},
		{name: "ready, behind, rank 1: catch up first", slot: slots.owner, do: machine.SlotCatchUp, state: machine.SeedReady,
			prep: func(t *testing.T, kv *storage.BadgerKV, slot uint16) {
				putRecord(t, kv, slot, readyRecord(cluster.Position{Epoch: e, Seq: 7}))
			}},
		{name: "ready, not in target: retired with the fence", slot: slots.outside, do: machine.SlotRetire, state: machine.SeedRetired,
			prep: func(t *testing.T, kv *storage.BadgerKV, slot uint16) {
				putRecord(t, kv, slot, readyRecord(cluster.Position{Epoch: e, Seq: 7}))
			}},
		{name: "retired, not in target: write the fence, then the gate", slot: slots.outside, do: machine.SlotGate, state: machine.SeedRetired,
			prep: func(t *testing.T, kv *storage.BadgerKV, slot uint16) { putRecord(t, kv, slot, retiredRecord()) }},
		{name: "retired, in target: the gate, never a delete on sight", slot: slots.copyOnly, do: machine.SlotGate, state: machine.SeedRetired,
			prep: func(t *testing.T, kv *storage.BadgerKV, slot uint16) { putRecord(t, kv, slot, retiredRecord()) }},
		{name: "retiring: finish the delete first", slot: slots.outside, do: machine.SlotFinishDeleteFirst, state: machine.SeedRetiring,
			prep: func(t *testing.T, kv *storage.BadgerKV, slot uint16) {
				rec := retiredRecord()
				rec.State = machine.SeedRetiring
				putRecord(t, kv, slot, rec)
			}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st := base
			if tc.joining {
				st = joiningBase
			}
			if tc.bit {
				withBit(t, &st, int(tc.slot))
			}
			if tc.otherBit {
				withBitOf(t, &st, otherCopyOf(t, st, tc.slot), int(tc.slot))
			}
			kv := openKV(t, t.TempDir())
			t.Cleanup(func() { _ = kv.Close() })
			if tc.prep != nil {
				tc.prep(t, kv, tc.slot)
			}
			g := newRig(t, kv, st, testSelf, tc.current)
			g.boot(t)

			acts := g.sink.of(tc.slot)
			var got uint8
			if len(acts) == 1 {
				got = acts[0].Do
			} else if len(acts) > 1 {
				t.Fatalf("slot got %d actions: %+v", len(acts), acts)
			}
			if got != tc.do {
				t.Fatalf("action = %d, want %d (%+v)", got, tc.do, acts)
			}
			if tc.restart != 0 && acts[0].Restart != tc.restart {
				t.Fatalf("restart reason = %d, want %d", acts[0].Restart, tc.restart)
			}
			rec, present := diskRecord(t, kv, tc.slot)
			if tc.state == 0 && present {
				t.Fatalf("record %+v is on disk, want none", rec)
			}
			if tc.state != 0 && (!present || rec.State != tc.state || rec.Phase != tc.phase) {
				t.Fatalf("disk record = %+v (present %v), want state %d phase %d", rec, present, tc.state, tc.phase)
			}
			if tc.state == machine.SeedRetired {
				if _, ok := hasFence(t, kv, tc.slot); !ok {
					t.Fatal("a retired record has no 's' fence record: the gate has nothing to compare against")
				}
			}
		})
	}
}

// TestASeedingPairThatLeftTheTargetKeepsItsRecordAtDispatch: the pass answers
// a seeding pair that left the target with SlotDeleteRangeAndForget and leaves
// its record seeding, on disk, when the sink is called; the sink voids it (an
// attempt in phase reset) before the range is deleted and removes it after, so a
// crash can never leave a resumable cut over a deleted range nor a pair with no
// record over a range that still holds keys (BUG-0817), and a pass that is asked
// again asks again. (The design's name ends "UntilDeleted", which the interim arm
// now makes true; the database DDL plan's D4b executor replaces it with retiring.)
// [guard: remove the record in the row]
func TestASeedingPairThatLeftTheTargetKeepsItsRecordAtDispatch(t *testing.T) {
	st := threeNodes(t)
	slots := findSlots(t, st)
	e := uint64(st.Epoch)
	kv := openKV(t, t.TempDir())
	t.Cleanup(func() { _ = kv.Close() })
	putRecord(t, kv, slots.outside, seedingRecord(machine.PhaseSnapshot, cluster.Position{Epoch: e, Seq: 5}, "k"))
	g := newRig(t, kv, st, testSelf, true)
	atDispatch := map[bool]int{}
	g.sink.hook = func(a Action) {
		if a.Slot == slots.outside && a.Do == machine.SlotDeleteRangeAndForget {
			rec, present := diskRecord(t, kv, a.Slot)
			atDispatch[present && rec.State == machine.SeedSeeding && rec.Phase == machine.PhaseSnapshot]++
		}
	}
	g.boot(t)
	if err := g.rec.OnStateSeq(t.Context(), st); err != nil {
		t.Fatal(err)
	}
	if atDispatch[true] != 2 || atDispatch[false] != 0 {
		t.Fatalf("the record was seeding at phase snapshot on disk at %d of 2 dispatches (%v): a crash between the removal and the drop would be unsafe, and the row must be idempotent", atDispatch[true], atDispatch)
	}
	if rec, ok := g.rec.Record(slots.outside, reconDB); !ok || rec.State != machine.SeedSeeding {
		t.Fatalf("the record in memory is %+v (present %v), want seeding", rec, ok)
	}
}

// TestRetiredCopyIsCancelledWhenTheSlotComesBack is the opposite of its name:
// the retire is never cancelled. The slot returns to the copy set while the
// other copy is behind the fence: nothing is deleted and nothing serves; the
// range delete runs only once that copy's own reply covers the fence, and the
// pair then seeds from reset.
func TestRetiredCopyIsCancelledWhenTheSlotComesBack(t *testing.T) {
	st := threeNodes(t)
	slots := findSlots(t, st)
	kv := openKV(t, t.TempDir())
	t.Cleanup(func() { _ = kv.Close() })
	e := uint64(st.Epoch)
	fence := cluster.Position{Epoch: e, Seq: 30}
	rec := retiredRecord()
	rec.Applied = fence
	putRecord(t, kv, slots.copyOnly, rec)
	putFence(t, kv, slots.copyOnly, fence)
	g := newRig(t, kv, st, testSelf, true)
	g.boot(t)

	acts := g.sink.of(slots.copyOnly)
	if len(acts) != 1 || acts[0].Do != machine.SlotGate || len(acts[0].Waiting) != 1 || acts[0].Serve || acts[0].CountsAsCopy {
		t.Fatalf("a returned retired slot = %+v, want the gate naming one member, serving nothing", acts)
	}
	if disk, _ := diskRecord(t, kv, slots.copyOnly); disk.State != machine.SeedRetired {
		t.Fatalf("the retire was cancelled: record is %+v", disk)
	}
	other := acts[0].Waiting[0]

	behind := machine.PositionReplied{Node: other, Epoch: e, State: machine.SlotReady, Applied: cluster.Position{Epoch: e, Seq: 29}}
	act, err := g.rec.Feed(slots.copyOnly, reconDB, behind)
	if err != nil || act.Do != machine.SlotGate {
		t.Fatalf("a copy behind the fence opened the gate: %+v, %v", act, err)
	}
	covers := behind
	covers.Applied.Seq = 30
	act, err = g.rec.Feed(slots.copyOnly, reconDB, covers)
	if err != nil || act.Do != machine.SlotDeleteRangeThenSeed {
		t.Fatalf("a covering reply = %+v, %v, want DeleteRangeThenSeed", act, err)
	}
	act, err = g.rec.Feed(slots.copyOnly, reconDB, machine.RetireStarted{Fence: fence})
	if err != nil || act.Do != machine.SlotDeleteRange {
		t.Fatalf("RetireStarted = %+v, %v, want DeleteRange", act, err)
	}
	if disk, _ := diskRecord(t, kv, slots.copyOnly); disk.State != machine.SeedRetiring {
		t.Fatalf("record after RetireStarted = %+v, want retiring", disk)
	}
	act, err = g.rec.Feed(slots.copyOnly, reconDB, machine.RetireDeleted{})
	if err != nil || act.Do != machine.SlotRestartSeed {
		t.Fatalf("RetireDeleted = %+v, %v, want RestartSeed", act, err)
	}
	if disk, _ := diskRecord(t, kv, slots.copyOnly); disk.State != machine.SeedSeeding || disk.Phase != machine.PhaseReset {
		t.Fatalf("after the delete the record = %+v, want seeding from reset", disk)
	}
}

// targetSlots lists the slots where self is in the copy set of the state with
// the joiner(s) filtered out of the ranking, which is what "never ranked" means.
func targetSlots(st cluster.ClusterState, self string, unranked ...string) []uint16 {
	ranked := st
	ranked.Members = nil
	for _, m := range st.Members {
		if !slices.Contains(unranked, m.ID) {
			if m.ID == self {
				m.State = cluster.MemberActive // a joiner ranks itself in its own placing set
			}
			ranked.Members = append(ranked.Members, m)
		}
	}
	table := cluster.BuildTable(ranked)
	var out []uint16
	for slot := 0; slot < storage.SlotCount; slot++ {
		for _, m := range table.Copies(uint16(slot), 2) {
			if m.ID == self {
				out = append(out, uint16(slot))
			}
		}
	}
	return out
}

// heldSlots lists the slots the boot left a record for: a seeding attempt on a joiner, the
// interim on an active member (seeding S0); either is the pass's evidence of the target set.
func heldSlots(g *rig) []uint16 {
	var out []uint16
	for slot := 0; slot < storage.SlotCount; slot++ {
		if _, found := g.rec.Record(uint16(slot), reconDB); found {
			out = append(out, uint16(slot))
		}
	}
	return out
}

// TestAJoiningIdentityIsNeverRanked: n3 and n2 are both joining. n1's target
// sets ignore both, and each joiner's placing set holds itself and not the
// other joiner (seeding §9, several joins before any completes).
func TestAJoiningIdentityIsNeverRanked(t *testing.T) {
	st := threeNodes(t)
	setState(t, &st, "n2", "down")
	replace(t, &st, "n2", 2)
	setState(t, &st, "n3", "down")
	replace(t, &st, "n3", 2)

	cases := []struct {
		self     string
		unranked []string
	}{
		{"n1", []string{"n2", "n3"}}, // an active node ranks neither joiner
		{"n2", []string{"n3"}},       // the joiner ranks itself and not the other joiner
		{"n3", []string{"n2"}},
	}
	for _, tc := range cases {
		t.Run(tc.self, func(t *testing.T) {
			kv := openKV(t, t.TempDir())
			t.Cleanup(func() { _ = kv.Close() })
			g := newRig(t, kv, st, tc.self, true)
			g.boot(t)
			want := targetSlots(st, tc.self, tc.unranked...)
			slices.Sort(want)
			if got := heldSlots(g); !slices.Equal(got, want) {
				t.Fatalf("%s holds %d slots, want the %d slots of a ranking without %v", tc.self, len(got), len(want), tc.unranked)
			}
		})
	}
}

// TestAReplaceFromDownBumpsTwice: n2 is down (ranked), replaced (joining: it
// leaves the ranking, epoch +1) and then active (epoch +1 again). The
// reconciler sees both epochs.
func TestAReplaceFromDownBumpsTwice(t *testing.T) {
	st := threeNodes(t)
	setState(t, &st, "n2", "down")
	down := st
	if !rankedIn(cluster.BuildTable(down), "n2") {
		t.Fatal("a down member must keep its rank")
	}
	replace(t, &st, "n2", 2)
	joining := st
	setState(t, &st, "n2", "active")
	active := st
	if joining.Epoch != down.Epoch+1 || active.Epoch != joining.Epoch+1 {
		t.Fatalf("epochs %d -> %d -> %d, want two bumps", down.Epoch, joining.Epoch, active.Epoch)
	}
	if rankedIn(cluster.BuildTable(joining), "n2") {
		t.Fatal("a joining identity is still ranked")
	}

	kv := openKV(t, t.TempDir())
	t.Cleanup(func() { _ = kv.Close() })
	g := newRig(t, kv, joining, testSelf, true)
	g.boot(t)
	if err := g.rec.OnJoiningCommit(joining); err != nil {
		t.Fatal(err)
	}
	for _, a := range g.sink.actions {
		if a.Epoch != uint64(joining.Epoch) {
			t.Fatalf("an action at epoch %d during the joining epoch %d", a.Epoch, joining.Epoch)
		}
	}
	g.sink.actions = nil
	if err := g.rec.OnStateSeq(t.Context(), active); err != nil {
		t.Fatal(err)
	}
	for _, a := range g.sink.actions {
		if a.Epoch != uint64(active.Epoch) {
			t.Fatalf("an action at epoch %d after the active commit at %d", a.Epoch, active.Epoch)
		}
	}
}

func rankedIn(table *cluster.SlotTable, id string) bool {
	return slices.ContainsFunc(table.Members(), func(m cluster.Member) bool { return m.ID == id })
}

// TestAJoiningNodeResetsItsOwnRecordsToReset: a returning node still holds
// ready records from before its wipe. Its joining commit rewrites the ones in
// its placing set to seeding from reset and forgets their position.
func TestAJoiningNodeResetsItsOwnRecordsToReset(t *testing.T) {
	st := threeNodes(t)
	setState(t, &st, "n2", "down")
	replace(t, &st, "n2", 2)
	placing := st
	setState(t, &placing, "n2", "active")
	slots := findSlots(t, placing)

	kv := openKV(t, t.TempDir())
	t.Cleanup(func() { _ = kv.Close() })
	at := cluster.Position{Epoch: 1, Seq: 50}
	putRecord(t, kv, slots.copyOnly, readyRecord(at))
	putRecord(t, kv, slots.owner, readyRecord(at))
	// Boot before the joining commit lands (the node is down and ranked), so the
	// reset can only come from OnJoiningCommit and not from a joining boot.
	before := threeNodes(t)
	setState(t, &before, "n2", "down")
	g := newRig(t, kv, before, testSelf, true)
	g.boot(t)
	for _, slot := range []uint16{slots.copyOnly, slots.owner} {
		if rec, _ := diskRecord(t, kv, slot); rec.State != machine.SeedReady {
			t.Fatalf("setup: the down boot changed slot %d to %+v", slot, rec)
		}
	}
	g.state = st
	if err := g.rec.OnJoiningCommit(st); err != nil {
		t.Fatal(err)
	}
	for _, slot := range []uint16{slots.copyOnly, slots.owner} {
		rec, ok := diskRecord(t, kv, slot)
		if !ok || rec.State != machine.SeedSeeding || rec.Phase != machine.PhaseReset {
			t.Fatalf("slot %d record = %+v, want seeding from reset", slot, rec)
		}
		if rec.Applied.Seq == 50 || rec.Ordinal != 0 || rec.WALCursor != 0 {
			t.Fatalf("slot %d keeps its pre-wipe position: %+v", slot, rec)
		}
		if p := g.pairs.Snapshot(slot, reconDB); p.AppliedPos.Seq == 50 || p.Ordinal != 0 {
			t.Fatalf("slot %d pair still claims %+v", slot, p)
		}
	}
	if err := g.rec.OnJoiningCommit(placing); err == nil {
		t.Fatal("OnJoiningCommit accepted a state in which the node is active")
	}
}

// TestARestartReRegistersOnTheSameCut: after a restart the resumed seed
// machine has registered again on its cut. Without that, every attempt event
// is stale and answers SEED_RESTART 3.
func TestARestartReRegistersOnTheSameCut(t *testing.T) {
	st := threeNodes(t)
	slots := findSlots(t, st)
	kv := openKV(t, t.TempDir())
	t.Cleanup(func() { _ = kv.Close() })
	e := uint64(st.Epoch)
	cut := cluster.Position{Epoch: e, Seq: 5}
	putRecord(t, kv, slots.copyOnly, seedingRecord(machine.PhaseSnapshot, cut, "k1"))
	g := newRig(t, kv, st, testSelf, true)
	g.boot(t)

	if _, ok := g.rec.Seed(slots.copyOnly, reconDB); !ok {
		t.Fatal("no resumed seed machine for a seeding record")
	}
	cfg := machine.SeedConfig{SyncBytes: 1 << 20, Epoch: e}
	out, err := g.rec.ApplySeedChunk(slots.copyOnly, reconDB, chunkReq(g.kv, slots.copyOnly, "k2", false, cfg))
	if err != nil {
		t.Fatal(err)
	}
	if out.Restart != 0 || !out.ApplyChunk {
		t.Fatalf("a chunk on the resumed attempt = %+v, want it applied", out)
	}
	if seed, _ := g.rec.Seed(slots.copyOnly, reconDB); seed.Cut != cut {
		t.Fatalf("the resumed cut moved to %v", seed.Cut)
	}

	// The failure the registration prevents: a record loaded from disk is stale.
	loaded, err := DecodeSeedRecord(mustEncode(t, seedingRecord(machine.PhaseSnapshot, cut, "k1")))
	if err != nil {
		t.Fatal(err)
	}
	stale := loaded.SeedRecord
	if out := stale.Apply(machine.ChunkStored{Cursor: []byte("k2"), Bytes: 10, Entries: 1}, cfg); out.Restart != 3 {
		t.Fatalf("an unregistered record answered %+v, want Restart=3", out)
	}
}

func alterRF(t *testing.T, st *cluster.ClusterState, rf int32) {
	t.Helper()
	apply(t, st, cluster.CommandAlterDatabase, bsonDoc(bsonI64("id", int64(reconDB)), bsonI32("rf", rf)))
}

// TestAnRFChangeInsideOneEpochIsReconciled: an RF change bumps state_seq and
// not the epoch (conv §7), so the machine sees the same epoch with another
// target set. The reconciler must still run the table: RF 2 to 1 retires the
// copy that left the set and drops a partial seed, and RF 1 to 2 queues the
// copy that entered it.
func TestAnRFChangeInsideOneEpochIsReconciled(t *testing.T) {
	st := threeNodes(t)
	slots := findSlots(t, st)
	e := uint64(st.Epoch)
	shrunk := st
	alterRF(t, &shrunk, 1)
	if shrunk.Epoch != st.Epoch {
		t.Fatalf("an RF change moved the epoch %d -> %d: this test assumes it does not", st.Epoch, shrunk.Epoch)
	}
	// The slots where n2 is a copy at RF 2 and not at RF 1 are the non-owner copies.
	t.Run("ready copy leaves", func(t *testing.T) {
		kv := openKV(t, t.TempDir())
		t.Cleanup(func() { _ = kv.Close() })
		putRecord(t, kv, slots.copyOnly, readyRecord(cluster.Position{Epoch: e, Seq: 7}))
		g := newRig(t, kv, st, testSelf, true)
		g.boot(t)
		g.sink.actions = nil
		if err := g.rec.OnStateSeq(t.Context(), shrunk); err != nil {
			t.Fatal(err)
		}
		acts := g.sink.of(slots.copyOnly)
		if len(acts) != 1 || acts[0].Do != machine.SlotRetire {
			t.Fatalf("after RF 2 to 1 the leaving copy got %+v, want Retire", acts)
		}
		if rec, _ := diskRecord(t, kv, slots.copyOnly); rec.State != machine.SeedRetired {
			t.Fatalf("record = %+v, want retired", rec)
		}
		if acts[0].Serve || acts[0].CountsAsCopy {
			t.Fatal("a copy that left the set still serves or counts")
		}
	})
	t.Run("partial seed leaves", func(t *testing.T) {
		kv := openKV(t, t.TempDir())
		t.Cleanup(func() { _ = kv.Close() })
		putRecord(t, kv, slots.copyOnly, seedingRecord(machine.PhaseSnapshot, cluster.Position{Epoch: e, Seq: 5}, "k"))
		g := newRig(t, kv, st, testSelf, true)
		g.boot(t)
		g.sink.actions = nil
		if err := g.rec.OnStateSeq(t.Context(), shrunk); err != nil {
			t.Fatal(err)
		}
		if acts := g.sink.of(slots.copyOnly); len(acts) != 1 || acts[0].Do != machine.SlotDeleteRangeAndForget {
			t.Fatalf("after RF 2 to 1 the partial seed got %+v, want DeleteRangeAndForget", acts)
		}
		if rec, ok := diskRecord(t, kv, slots.copyOnly); !ok || rec.State != machine.SeedSeeding {
			t.Fatalf("the partial seed's record after the pass is %+v (present %v), want it kept, seeding, for the sink to remove", rec, ok)
		}
	})
	t.Run("copy enters", func(t *testing.T) {
		kv := openKV(t, t.TempDir())
		t.Cleanup(func() { _ = kv.Close() })
		g := newRig(t, kv, shrunk, testSelf, true)
		g.boot(t)
		if len(g.sink.of(slots.copyOnly)) != 0 {
			t.Fatal("a slot outside the RF 1 set was queued")
		}
		if err := g.rec.OnStateSeq(t.Context(), st); err != nil {
			t.Fatal(err)
		}
		// Seeding S0: on an active member the entering copy is ready empty (the interim) and
		// is repaired as any lagging ready copy is (plan §0.3); a joiner would be queued.
		if acts := g.sink.of(slots.copyOnly); len(acts) != 1 || acts[0].Do != machine.SlotReportBit {
			t.Fatalf("after RF 1 to 2 the entering copy got %+v, want only the report of its bit: the interim is written, not seeded", acts)
		}
		if rec, found := diskRecord(t, kv, slots.copyOnly); !found || rec.State != machine.SeedReady || !rec.Applied.Zero() {
			t.Fatalf("after RF 1 to 2 the entering copy's record is %+v (found %v), want ready empty at 0:0", rec, found)
		}
	})
}

// otherCopy is the one copy that is not n2 at RF 1 or 2 for a slot n2 holds as a
// non-owner copy.
func otherCopy(t *testing.T, st cluster.ClusterState, slot uint16) string {
	t.Helper()
	for _, m := range cluster.BuildTable(st).Copies(slot, 2) {
		if m.ID != testSelf {
			return m.ID
		}
	}
	t.Fatal("no other copy")
	return ""
}

// TestEventsBeforeBootAreRefused: before OnBoot every pair looks absent, so a
// state commit would rewrite a ready record to seeding and drop it.
func TestEventsBeforeBootAreRefused(t *testing.T) {
	st := threeNodes(t)
	slots := findSlots(t, st)
	kv := openKV(t, t.TempDir())
	t.Cleanup(func() { _ = kv.Close() })
	putRecord(t, kv, slots.copyOnly, readyRecord(cluster.Position{Epoch: uint64(st.Epoch), Seq: 7}))
	g := newRig(t, kv, st, testSelf, true)
	if err := g.rec.OnStateSeq(t.Context(), st); err == nil {
		t.Fatal("OnStateSeq ran before OnBoot")
	}
	joining := st
	setState(t, &joining, "n2", "down")
	replace(t, &joining, "n2", 2)
	if err := g.rec.OnJoiningCommit(joining); err == nil {
		t.Fatal("OnJoiningCommit ran before OnBoot")
	}
	if _, err := g.rec.Feed(slots.copyOnly, reconDB, machine.SeedRetryAsked{}); err == nil {
		t.Fatal("Feed ran before OnBoot")
	}
	if rec, _ := diskRecord(t, kv, slots.copyOnly); rec.State != machine.SeedReady {
		t.Fatalf("the ready record became %+v", rec)
	}
	if len(g.sink.actions) != 0 {
		t.Fatalf("actions before boot: %+v", g.sink.actions)
	}
}

// TestABootAsAJoiningNodeResetsItsRecords: a crash between the joining commit
// and OnJoiningCommit leaves a ready record. The boot must treat it exactly as
// OnJoiningCommit does.
func TestABootAsAJoiningNodeResetsItsRecords(t *testing.T) {
	st := threeNodes(t)
	setState(t, &st, "n2", "down")
	replace(t, &st, "n2", 2)
	placing := st
	setState(t, &placing, "n2", "active")
	slots := findSlots(t, placing)
	kv := openKV(t, t.TempDir())
	t.Cleanup(func() { _ = kv.Close() })
	putRecord(t, kv, slots.copyOnly, readyRecord(cluster.Position{Epoch: 1, Seq: 50}))
	g := newRig(t, kv, st, testSelf, true)
	g.boot(t)
	rec, ok := diskRecord(t, kv, slots.copyOnly)
	if !ok || rec.State != machine.SeedSeeding || rec.Phase != machine.PhaseReset {
		t.Fatalf("a joining node booted with %+v, want seeding from reset", rec)
	}
	for _, a := range g.sink.of(slots.copyOnly) {
		if a.Serve || a.CountsAsCopy {
			t.Fatalf("a joining node serves: %+v", a)
		}
	}
}

func advanceTo(kv *storage.BadgerKV, slot uint16, seq uint64, e uint64) AdvanceRequest {
	return AdvanceRequest{
		Stage: func(batch storage.KVBatch, _ *cluster.Pair) (Advance, error) {
			key := storage.Key(slot, reconDB, 5, storage.NSDocuments, []byte("d1"))
			if err := batch.Set(key, []byte("v")); err != nil {
				return Advance{}, err
			}
			return Advance{Applied: cluster.Position{Epoch: e, Seq: seq}, WALCursor: 99, Ordinal: 23}, nil
		},
		Commit: func(batch storage.KVBatch, _ bool) error { return batch.Commit(true) },
	}
}

// TestRecordAdvancesInTheDataBatch: the record rides the batch the caller
// stages and commits, and nothing in memory or on disk moves unless that commit
// succeeds.
func TestRecordAdvancesInTheDataBatch(t *testing.T) {
	st := threeNodes(t)
	slots := findSlots(t, st)
	kv := openKV(t, t.TempDir())
	t.Cleanup(func() { _ = kv.Close() })
	e := uint64(st.Epoch)
	putRecord(t, kv, slots.owner, readyRecord(cluster.Position{Epoch: e, Seq: 7}))
	g := newRig(t, kv, st, testSelf, true)
	g.boot(t)
	memory := func() uint64 {
		rec, ok := g.rec.Record(slots.owner, reconDB)
		if !ok {
			t.Fatal("no record")
		}
		return rec.Applied.Seq
	}
	dataKey := storage.Key(slots.owner, reconDB, 5, storage.NSDocuments, []byte("d1"))

	// A commit that fails leaves everything where it was.
	failing := advanceTo(kv, slots.owner, 8, e)
	failing.Commit = func(batch storage.KVBatch, _ bool) error {
		if rec, _ := diskRecord(t, kv, slots.owner); rec.Applied.Seq != 7 {
			t.Errorf("the record moved to %v before the data batch committed", rec.Applied)
		}
		if memory() != 7 {
			t.Error("Record names a position that is only staged")
		}
		batch.Discard()
		return errors.New("disk full")
	}
	if err := g.rec.ApplyAdvance(slots.owner, reconDB, failing); err == nil {
		t.Fatal("a failed commit was reported as success")
	}
	if rec, _ := diskRecord(t, kv, slots.owner); rec.Applied.Seq != 7 || rec.Ordinal != 22 || memory() != 7 {
		t.Fatalf("a failed commit moved the record to %+v (memory %d)", rec, memory())
	}
	if p := g.pairs.Snapshot(slots.owner, reconDB); p.AppliedPos.Seq == 8 {
		t.Fatal("a failed commit advanced the pair set")
	}

	// A staging error writes nothing.
	staging := advanceTo(kv, slots.owner, 8, e)
	staging.Stage = func(storage.KVBatch, *cluster.Pair) (Advance, error) { return Advance{}, errors.New("encode failed") }
	if err := g.rec.ApplyAdvance(slots.owner, reconDB, staging); err == nil || memory() != 7 {
		t.Fatalf("a staging error = %v, memory %d", err, memory())
	}

	// The next write is staged from what is committed.
	if err := g.rec.ApplyAdvance(slots.owner, reconDB, advanceTo(kv, slots.owner, 8, e)); err != nil {
		t.Fatal(err)
	}
	rec, _ := diskRecord(t, kv, slots.owner)
	if rec.Applied.Seq != 8 || rec.WALCursor != 99 || rec.Ordinal != 23 || memory() != 8 {
		t.Fatalf("committed record = %+v (memory %d), want 8, cursor 99, ordinal 23", rec, memory())
	}
	if pair := g.pairs.Snapshot(slots.owner, reconDB); pair.AppliedPos.Seq != 8 || pair.Ordinal != 23 {
		t.Fatalf("pair set = %+v, want 8 and ordinal 23", pair)
	}
	snap := kv.NewSnapshot()
	defer snap.Discard()
	if _, err := snap.Get(dataKey); err != nil {
		t.Fatalf("the data key is missing beside its record: %v", err)
	}
}

// TestAnAdvanceIsRefusedOnAPairThatIsNotReady: a pair that is absent, seeding,
// retired or retiring takes no write, and nothing is staged or written.
func TestAnAdvanceIsRefusedOnAPairThatIsNotReady(t *testing.T) {
	st := threeNodes(t)
	slots := findSlots(t, st)
	e := uint64(st.Epoch)
	retiring := retiredRecord()
	retiring.State = machine.SeedRetiring
	cases := map[string]*SeedRecord{
		"absent": nil,
		"seeding": func() *SeedRecord {
			r := seedingRecord(machine.PhaseSnapshot, cluster.Position{Epoch: e, Seq: 5}, "k")
			return &r
		}(),
		"retired":  func() *SeedRecord { r := retiredRecord(); return &r }(),
		"retiring": &retiring,
	}
	for name, rec := range cases {
		t.Run(name, func(t *testing.T) {
			slot := slots.copyOnly
			if rec == nil {
				slot = slots.outside // an absent pair in the target set is queued, not absent
			}
			kv := openKV(t, t.TempDir())
			t.Cleanup(func() { _ = kv.Close() })
			if rec != nil {
				putRecord(t, kv, slot, *rec)
			}
			g := newRig(t, kv, st, testSelf, true)
			g.boot(t)
			req := advanceTo(kv, slot, 8, e)
			staged := false
			req.Stage = func(storage.KVBatch, *cluster.Pair) (Advance, error) { staged = true; return Advance{}, nil }
			err := g.rec.ApplyAdvance(slot, reconDB, req)
			if !errors.Is(err, ErrPairNotReady) || staged {
				t.Fatalf("err = %v, staged %v: want ErrPairNotReady before staging", err, staged)
			}
			before, had := diskRecord(t, kv, slot)
			if had != (rec != nil) || (had && before.State == machine.SeedReady) {
				t.Fatalf("disk record = %+v (%v)", before, had)
			}
		})
	}
}

// TestAReconcileWaitsForAnAdvanceInFlight: the pair lock is held from the
// staging to the install, so a retire cannot run in between. The retire
// reconcile finishes after the advance, and then the record says retired.
func TestAReconcileWaitsForAnAdvanceInFlight(t *testing.T) {
	st := threeNodes(t)
	slots := findSlots(t, st)
	e := uint64(st.Epoch)
	shrunk := st
	alterRF(t, &shrunk, 1)
	kv := openKV(t, t.TempDir())
	t.Cleanup(func() { _ = kv.Close() })
	putRecord(t, kv, slots.copyOnly, readyRecord(cluster.Position{Epoch: e, Seq: 7}))
	g := newRig(t, kv, st, testSelf, true)
	g.boot(t)

	staged, release := make(chan struct{}), make(chan struct{})
	req := advanceTo(kv, slots.copyOnly, 8, e)
	inner := req.Stage
	req.Stage = func(b storage.KVBatch, p *cluster.Pair) (Advance, error) {
		close(staged)
		<-release
		return inner(b, p)
	}
	advanced := make(chan error, 1)
	go func() { advanced <- g.rec.ApplyAdvance(slots.copyOnly, reconDB, req) }()
	<-staged
	reconciled := make(chan error, 1)
	go func() { reconciled <- g.rec.OnStateSeq(t.Context(), shrunk) }()
	select {
	case <-reconciled:
		t.Fatal("the retire ran while a write was staged: the record and the delete are not serialised")
	case <-time.After(300 * time.Millisecond):
	}
	close(release)
	if err := <-advanced; err != nil {
		t.Fatal(err)
	}
	if err := <-reconciled; err != nil {
		t.Fatal(err)
	}
	if rec, _ := diskRecord(t, kv, slots.copyOnly); rec.State != machine.SeedRetired || rec.Applied.Seq != 8 {
		t.Fatalf("disk after the advance and the retire = %+v, want retired at 8", rec)
	}
}

// retryBusy runs op until it stops answering ErrPairBusy.
func retryBusy(op func() error) error {
	for {
		err := op()
		if !errors.Is(err, ErrPairBusy) {
			return err
		}
		runtime.Gosched()
	}
}

// TestSerialisedAdvancesAndReconcilesNeverDisagree runs writers, lifecycle
// events and chunk appliers against reconcile passes that flip RF 2 and 1 on
// many pairs (run under -race, repeatedly). A busy answer is retried, never
// ignored. At the end disk and memory agree for every pair; a lost update shows
// as memory ready over a disk that says seeding, or the reverse.
func TestSerialisedAdvancesAndReconcilesNeverDisagree(t *testing.T) {
	st := threeNodes(t)
	e := uint64(st.Epoch)
	cfg := machine.SeedConfig{SyncBytes: 1 << 20, Epoch: e}
	shrunk := st
	alterRF(t, &shrunk, 1)
	table := cluster.BuildTable(st)
	var slots []uint16
	for slot := 0; slot < storage.SlotCount && len(slots) < 24; slot++ {
		if table.Copies(uint16(slot), 2)[1].ID == testSelf {
			slots = append(slots, uint16(slot))
		}
	}
	kv := openKV(t, t.TempDir())
	t.Cleanup(func() { _ = kv.Close() })
	for i, slot := range slots {
		if i%3 == 2 {
			putRecord(t, kv, slot, seedingRecord(machine.PhaseSnapshot, cluster.Position{Epoch: e, Seq: 5}, "k0"))
			continue
		}
		putRecord(t, kv, slot, readyRecord(cluster.Position{Epoch: e, Seq: 7}))
	}
	g := newRig(t, kv, st, testSelf, true)
	g.boot(t)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i, slot := range slots {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for seq := uint64(8); ; seq++ {
				select {
				case <-stop:
					return
				default:
				}
				var err error
				switch i % 3 {
				case 0:
					err = retryBusy(func() error { return g.rec.ApplyAdvance(slot, reconDB, advanceTo(kv, slot, seq, e)) })
				case 1:
					_, err = g.rec.Feed(slot, reconDB, machine.LogBelowHorizon{})
					if errors.Is(err, ErrPairBusy) {
						err = retryBusy(func() error { _, ferr := g.rec.Feed(slot, reconDB, machine.LogBelowHorizon{}); return ferr })
					}
				default:
					err = retryBusy(func() error {
						_, cerr := g.rec.ApplySeedChunk(slot, reconDB, chunkReq(kv, slot, fmt.Sprintf("k%d", seq), false, cfg))
						return cerr
					})
					if errors.Is(err, ErrPairNotSeeding) {
						err = nil
					}
				}
				if err != nil && !errors.Is(err, ErrPairNotReady) {
					t.Errorf("slot %d: %v", slot, err)
					return
				}
			}
		}()
	}
	for i := 0; i < 6; i++ {
		if err := g.rec.OnStateSeq(t.Context(), shrunk); err != nil {
			t.Error(err)
		}
		if err := g.rec.OnStateSeq(t.Context(), st); err != nil {
			t.Error(err)
		}
	}
	close(stop)
	wg.Wait()
	for _, slot := range slots {
		disk, onDisk := diskRecord(t, kv, slot)
		mem, inMemory := g.rec.Record(slot, reconDB)
		if onDisk != inMemory || (onDisk && !bytes.Equal(mustEncode(t, disk), mustEncode(t, mem))) {
			t.Fatalf("slot %d: disk %+v (%v) and memory %+v (%v) disagree", slot, disk, onDisk, mem, inMemory)
		}
	}
}

// TestAConcurrentEventDuringAPassIsNotLost is the verifier's probe: a pass is
// stalled in its commit while a lifecycle event restarts a pair the pass does
// not rewrite. The pass must not install its older picture over the event's
// result.
func TestAConcurrentEventDuringAPassIsNotLost(t *testing.T) {
	st := threeNodes(t)
	slots := findSlots(t, st)
	e := uint64(st.Epoch)
	shrunk := st
	alterRF(t, &shrunk, 1)
	kv := openKV(t, t.TempDir())
	t.Cleanup(func() { _ = kv.Close() })
	// owner stays ready in the target at RF 1 (the pass does not rewrite it);
	// copyOnly is retired by the pass, which gives the pass something to commit.
	putRecord(t, kv, slots.owner, readyRecord(cluster.Position{Epoch: e, Seq: 7}))
	putRecord(t, kv, slots.copyOnly, readyRecord(cluster.Position{Epoch: e, Seq: 7}))
	var armed atomic.Bool
	store := blockingStore{kv, &armed, make(chan struct{}, 1), make(chan struct{})}
	g := newRigOn(t, kv, store, st, testSelf, true)
	g.boot(t)

	armed.Store(true)
	done := make(chan error, 1)
	go func() { done <- g.rec.OnStateSeq(t.Context(), shrunk) }()
	<-store.started
	act, err := g.rec.Feed(slots.owner, reconDB, machine.LogBelowHorizon{})
	if err != nil || act.Do != machine.SlotRestartSeed {
		close(store.release)
		t.Fatalf("Feed during the pass = %+v, %v", act, err)
	}
	close(store.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	mem, _ := g.rec.Record(slots.owner, reconDB)
	disk, _ := diskRecord(t, kv, slots.owner)
	if mem.State != machine.SeedSeeding || disk.State != machine.SeedSeeding {
		t.Fatalf("the pass undid the event: memory %+v, disk %+v", mem, disk)
	}
	if err := g.rec.ApplyAdvance(slots.owner, reconDB, advanceTo(kv, slots.owner, 9, e)); !errors.Is(err, ErrPairNotReady) {
		t.Fatalf("a write to the restarted pair got %v, want ErrPairNotReady", err)
	}
}

// TestOnBootRunsOnce: a second boot would install older disk state over newer
// memory.
func TestOnBootRunsOnce(t *testing.T) {
	st := threeNodes(t)
	kv := openKV(t, t.TempDir())
	t.Cleanup(func() { _ = kv.Close() })
	g := newRig(t, kv, st, testSelf, true)
	g.boot(t)
	if err := g.rec.OnBoot(t.Context()); err == nil {
		t.Fatal("a second OnBoot was accepted")
	}
}

// TestASinkMayCallBackIntoAPass: actions are handed out after the pass lock is
// released, so a sink that triggers another pass does not deadlock.
func TestASinkMayCallBackIntoAPass(t *testing.T) {
	st := threeNodes(t)
	kv := openKV(t, t.TempDir())
	t.Cleanup(func() { _ = kv.Close() })
	g := newRig(t, kv, st, testSelf, true)
	g.boot(t)
	shrunk := st
	alterRF(t, &shrunk, 1)
	reentered := false
	g.sink.hook = func(Action) {
		if reentered {
			return
		}
		reentered = true
		if err := g.rec.OnStateSeq(t.Context(), st); err != nil {
			t.Error(err)
		}
	}
	done := make(chan error, 1)
	go func() { done <- g.rec.OnStateSeq(t.Context(), shrunk) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("a sink that re-enters a pass deadlocks")
	}
}

// TestTheOrdinalSurvivesARestart: the ordinal written in a data batch is in the
// pair set after the store is closed and reopened.
func TestTheOrdinalSurvivesARestart(t *testing.T) {
	st := threeNodes(t)
	slots := findSlots(t, st)
	dir := t.TempDir()
	kv := openKV(t, dir)
	e := uint64(st.Epoch)
	putRecord(t, kv, slots.owner, readyRecord(cluster.Position{Epoch: e, Seq: 7}))
	g := newRig(t, kv, st, testSelf, true)
	g.boot(t)
	req := advanceTo(kv, slots.owner, 98213, e)
	inner := req.Stage
	req.Stage = func(b storage.KVBatch, p *cluster.Pair) (Advance, error) {
		adv, err := inner(b, p)
		adv.Ordinal, adv.WALCursor = 98213, 0xA1B2
		return adv, err
	}
	if err := g.rec.ApplyAdvance(slots.owner, reconDB, req); err != nil {
		t.Fatal(err)
	}
	if err := kv.Close(); err != nil {
		t.Fatal(err)
	}
	kv = openKV(t, dir)
	t.Cleanup(func() { _ = kv.Close() })
	g = newRig(t, kv, st, testSelf, true)
	g.boot(t)
	pair := g.pairs.Snapshot(slots.owner, reconDB)
	if pair.Ordinal != 98213 || pair.AppliedPos.Seq != 98213 {
		t.Fatalf("pair after restart = %+v, want ordinal 98213 at seq 98213", pair)
	}
}

// TestACrashAtAnyStepNeverServesADeletedRange walks the verifier's sequence
// (advance, RF 2 to 1, retire, gate, delete, delete done, RETIRE committed) and,
// after each step, crashes (reopens the store from what was committed) and boots
// with RF raised back to 2. A write after the retire is refused, and the node
// never serves a pair whose data was deleted: a returned retired slot gates.
func TestACrashAtAnyStepNeverServesADeletedRange(t *testing.T) {
	st := threeNodes(t)
	slots := findSlots(t, st)
	slot := slots.copyOnly
	e := uint64(st.Epoch)
	shrunk := st
	alterRF(t, &shrunk, 1)
	other := otherCopy(t, shrunk, slot)
	fence := cluster.Position{Epoch: e, Seq: 8}
	dir := t.TempDir()
	kv := openKV(t, dir)
	putRecord(t, kv, slot, readyRecord(cluster.Position{Epoch: e, Seq: 7}))
	g := newRig(t, kv, st, testSelf, true)
	g.boot(t)
	t.Cleanup(func() { _ = kv.Close() })

	crashAndBoot := func(step string, mayServe bool) {
		t.Helper()
		if err := kv.Close(); err != nil {
			t.Fatal(err)
		}
		kv = openKV(t, dir)
		fresh := newRig(t, kv, st, testSelf, true) // RF back to 2
		fresh.boot(t)
		for _, a := range fresh.sink.of(slot) {
			if (a.Serve || a.CountsAsCopy) && !mayServe {
				t.Fatalf("after %s and a crash the node serves the pair: %+v", step, a)
			}
		}
		rec, ok := diskRecord(t, kv, slot)
		if !mayServe && ok && rec.State == machine.SeedReady {
			t.Fatalf("after %s and a crash the disk says ready: %+v", step, rec)
		}
		g = newRig(t, kv, shrunk, testSelf, true)
		g.boot(t) // continue the walk at RF 1 on the same store
		g.sink.actions = nil
	}
	refused := func(step string) {
		t.Helper()
		called := false
		req := advanceTo(kv, slot, 9, e)
		req.Commit = func(storage.KVBatch, bool) error { called = true; return nil }
		if err := g.rec.ApplyAdvance(slot, reconDB, req); !errors.Is(err, ErrPairNotReady) || called {
			t.Fatalf("after %s a write was not refused: %v (commit called %v)", step, err, called)
		}
	}
	feed := func(ev machine.SlotEvent) {
		t.Helper()
		if _, err := g.rec.Feed(slot, reconDB, ev); err != nil {
			t.Fatal(err)
		}
	}

	if err := g.rec.ApplyAdvance(slot, reconDB, advanceTo(kv, slot, 8, e)); err != nil {
		t.Fatal(err)
	}
	crashAndBoot("the advance", true)
	g = newRig(t, kv, st, testSelf, true)
	g.boot(t)
	for range 2 { // the second pass reads the fence the retire wrote
		if err := g.rec.OnStateSeq(t.Context(), shrunk); err != nil {
			t.Fatal(err)
		}
	}
	refused("the retire")
	crashAndBoot("the retire", false)
	for range 2 {
		if err := g.rec.OnStateSeq(t.Context(), shrunk); err != nil {
			t.Fatal(err)
		}
	}
	feed(machine.PositionReplied{Node: other, Epoch: e, State: machine.SlotReady, Applied: fence})
	refused("the gate opening")
	crashAndBoot("the gate opening", false)
	for range 2 {
		if err := g.rec.OnStateSeq(t.Context(), shrunk); err != nil {
			t.Fatal(err)
		}
	}
	feed(machine.PositionReplied{Node: other, Epoch: e, State: machine.SlotReady, Applied: fence})
	feed(machine.RetireStarted{Fence: fence})
	refused("the delete starting")
	crashAndBoot("the delete starting", false)
	for range 2 {
		if err := g.rec.OnStateSeq(t.Context(), shrunk); err != nil {
			t.Fatal(err)
		}
	}
	feed(machine.PositionReplied{Node: other, Epoch: e, State: machine.SlotReady, Applied: fence})
	feed(machine.RetireStarted{Fence: fence})
	feed(machine.RetireDeleted{})
	refused("the delete finishing")
	crashAndBoot("the delete finishing", false)
}

// TestAnEmptyOrForeignStateIsRefused: a state that is not loaded yet, or that
// does not list this node, says nothing about what the node should hold. It must
// never turn a ready record into a retire or a seeding record into a drop. Each
// refusal has its own text.
func TestAnEmptyOrForeignStateIsRefused(t *testing.T) {
	st := threeNodes(t)
	slots := findSlots(t, st)
	e := uint64(st.Epoch)
	other := threeNodes(t)
	other.Members = slices.DeleteFunc(slices.Clone(other.Members), func(m cluster.Member) bool { return m.ID == testSelf })
	cases := []struct {
		name string
		bad  cluster.ClusterState
		want string
	}{
		{"empty", cluster.ClusterState{}, "state is empty"},
		{"node not listed", other, "is not a member"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			kv := openKV(t, t.TempDir())
			t.Cleanup(func() { _ = kv.Close() })
			putRecord(t, kv, slots.copyOnly, readyRecord(cluster.Position{Epoch: e, Seq: 7}))
			putRecord(t, kv, slots.outside, seedingRecord(machine.PhaseSnapshot, cluster.Position{Epoch: e, Seq: 5}, "k"))
			g := newRig(t, kv, tc.bad, testSelf, true)
			err := g.rec.OnBoot(t.Context())
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("OnBoot error = %v, want one containing %q", err, tc.want)
			}
			if err := g.rec.OnStateSeq(t.Context(), tc.bad); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("OnStateSeq error = %v, want one containing %q", err, tc.want)
			}
			if len(g.sink.actions) != 0 {
				t.Fatalf("actions for a state that says nothing: %+v", g.sink.actions)
			}
			if rec, _ := diskRecord(t, kv, slots.copyOnly); rec.State != machine.SeedReady {
				t.Fatalf("the ready record became %+v", rec)
			}
			if rec, ok := diskRecord(t, kv, slots.outside); !ok || rec.State != machine.SeedSeeding {
				t.Fatalf("the seeding record became %+v (present %v)", rec, ok)
			}
		})
	}
}

// failingStore fails the Commit of the batch that writes or deletes key while fail is set.
type failingStore struct {
	*storage.BadgerKV
	fail *bool
	key  []byte
}

func (f failingStore) NewBatch() storage.KVBatch {
	return &failingBatch{KVBatch: f.BadgerKV.NewBatch(), store: f}
}

type failingBatch struct {
	storage.KVBatch
	store   failingStore
	matched bool
}

func (b *failingBatch) Delete(key []byte) error {
	if bytes.Equal(key, b.store.key) {
		b.matched = true
	}
	return b.KVBatch.Delete(key)
}

func (b *failingBatch) Set(key, value []byte) error {
	if bytes.Equal(key, b.store.key) {
		b.matched = true
	}
	return b.KVBatch.Set(key, value)
}

func (b *failingBatch) Commit(sync bool) error {
	if *b.store.fail && b.matched {
		b.Discard()
		return errors.New("injected commit failure")
	}
	return b.KVBatch.Commit(sync)
}

// TestACrashAroundAPartialDropNeverResumesADeletedRange: a seeding copy that
// leaves the set has its record voided first (an attempt in phase reset), its
// range deleted second and its record removed last (dropLeftSeed, the drop the
// sink orders for SlotDeleteRangeAndForget). A crash or a failed commit at any
// point leaves the whole seed (nothing dropped), or a voided attempt that a boot
// starts over from a range reset, or, once the range is empty, no record, which
// a boot on an active member writes the interim for (seeding S0, plan §0.3:
// ready empty at 0:0 with no cursor). It never leaves a resumable cut over a
// deleted range, and since BUG-0817 never a pair with no record, or a ready
// one, over keys the delete has not removed.
// [guard: delete the range before the record is voided]
func TestACrashAroundAPartialDropNeverResumesADeletedRange(t *testing.T) {
	st := threeNodes(t)
	slots := findSlots(t, st)
	slot := slots.copyOnly
	key := reconcileKey{slot, reconDB}
	e := uint64(st.Epoch)
	shrunk := st
	alterRF(t, &shrunk, 1)
	seeding := seedingRecord(machine.PhaseSnapshot, cluster.Position{Epoch: e, Seq: 5}, "k")

	bootAtRF2 := func(t *testing.T, dir string, kv *storage.BadgerKV) *rig {
		t.Helper()
		if err := kv.Close(); err != nil {
			t.Fatal(err)
		}
		kv = openKV(t, dir)
		t.Cleanup(func() { _ = kv.Close() })
		g := newRig(t, kv, st, testSelf, true)
		g.boot(t)
		return g
	}
	noResume := func(t *testing.T, g *rig, what string) {
		t.Helper()
		for _, a := range g.sink.of(slot) {
			if a.Do == machine.SlotResumeSeed {
				t.Fatalf("%s: the boot resumes a cut over a dropped range", what)
			}
		}
	}
	// startsOver: the boot found the voided attempt and seeds the pair again from a range
	// reset; it does not call a range that may still hold the old seed's keys ready.
	startsOver := func(t *testing.T, g *rig, what string) {
		t.Helper()
		noResume(t, g, what)
		if acts := g.sink.of(slot); len(acts) != 1 || acts[0].Do != machine.SlotRestartSeed {
			t.Fatalf("%s: the boot answered %+v, want one RestartSeed", what, acts)
		}
		rec, ok := diskRecord(t, g.kv, slot)
		if !ok || rec.State != machine.SeedSeeding || rec.Phase != machine.PhaseReset || len(rec.Cursor) != 0 || !rec.Cut.Zero() {
			t.Fatalf("%s: the boot left %+v (present %v), want a seeding attempt in phase reset with no cut and no cursor", what, rec, ok)
		}
	}
	// leave runs the pass that makes the pair leave the target and checks that it
	// only asks: the record is still there for dropLeftSeed to void.
	leave := func(t *testing.T, g *rig) {
		t.Helper()
		g.sink.actions = nil
		if err := g.rec.OnStateSeq(t.Context(), shrunk); err != nil {
			t.Fatal(err)
		}
		if acts := g.sink.of(slot); len(acts) != 1 || acts[0].Do != machine.SlotDeleteRangeAndForget {
			t.Fatalf("the pass answered %+v, want one DeleteRangeAndForget", acts)
		}
	}

	t.Run("a failed void commit drops nothing", func(t *testing.T) {
		dir := t.TempDir()
		kv := openKV(t, dir)
		t.Cleanup(func() { _ = kv.Close() })
		putRecord(t, kv, slot, seeding)
		fail := false
		g := newRigOn(t, kv, failingStore{kv, &fail, SeedRecordKey(slot, reconDB)}, st, testSelf, true)
		g.boot(t)
		leave(t, g)
		fail = true
		deleted := false
		err := dropLeftSeed(t.Context(), g.rec, key, func(context.Context, reconcileKey) error {
			deleted = true
			return nil
		})
		if err == nil {
			t.Fatal("a failed commit was reported as success")
		}
		if deleted {
			t.Fatal("the range was deleted although the void of the record did not commit")
		}
		if rec, ok := diskRecord(t, kv, slot); !ok || rec.State != machine.SeedSeeding || rec.Cursor == nil {
			t.Fatalf("the seeding record changed: %+v (%v)", rec, ok)
		}
		if rec, ok := g.rec.Record(slot, reconDB); !ok || rec.Phase != machine.PhaseSnapshot {
			t.Fatalf("the record in memory changed although its commit failed: %+v (%v)", rec, ok)
		}
	})
	t.Run("crash after the void, before the drop", func(t *testing.T) {
		dir := t.TempDir()
		kv := openKV(t, dir)
		putRecord(t, kv, slot, seeding)
		g := newRig(t, kv, st, testSelf, true)
		g.boot(t)
		leave(t, g)
		crash := errors.New("crash before the drop")
		err := dropLeftSeed(t.Context(), g.rec, key, func(context.Context, reconcileKey) error { return crash })
		if !errors.Is(err, crash) {
			t.Fatalf("dropLeftSeed = %v, want the delete's error", err)
		}
		startsOver(t, bootAtRF2(t, dir, kv), "crash before the drop")
	})
	t.Run("a failed removal commit after the drop keeps the voided attempt", func(t *testing.T) {
		dir := t.TempDir()
		kv := openKV(t, dir)
		putRecord(t, kv, slot, seeding)
		fail := false
		g := newRigOn(t, kv, failingStore{kv, &fail, SeedRecordKey(slot, reconDB)}, st, testSelf, true)
		g.boot(t)
		leave(t, g)
		err := dropLeftSeed(t.Context(), g.rec, key, func(context.Context, reconcileKey) error {
			fail = true // the void is committed; the removal is the next write of the record's key
			return nil
		})
		if err == nil {
			t.Fatal("a failed removal commit was reported as success")
		}
		if rec, ok := g.rec.Record(slot, reconDB); !ok || rec.State != machine.SeedSeeding || rec.Phase != machine.PhaseReset {
			t.Fatalf("the record in memory after the failed removal is %+v (present %v), want the voided attempt", rec, ok)
		}
		startsOver(t, bootAtRF2(t, dir, kv), "a failed removal")
	})
	t.Run("crash after the drop", func(t *testing.T) {
		dir := t.TempDir()
		kv := openKV(t, dir)
		putRecord(t, kv, slot, seeding)
		g := newRig(t, kv, st, testSelf, true)
		g.boot(t)
		leave(t, g)
		if err := dropLeftSeed(t.Context(), g.rec, key, func(context.Context, reconcileKey) error { return nil }); err != nil {
			t.Fatal(err)
		}
		booted := bootAtRF2(t, dir, kv)
		noResume(t, booted, "crash after the drop")
		rec, ok := diskRecord(t, booted.kv, slot)
		if !ok || rec.State != machine.SeedReady || !rec.Applied.Zero() || len(rec.Cursor) != 0 || rec.SeedID != [16]byte{} {
			t.Fatalf("crash after the drop: the boot left %+v (present %v), want the interim: ready empty with no cursor to resume", rec, ok)
		}
	})
}

// TestTheLeftSeedsRecordIsVoidedBeforeItsFirstDeleteAndGoneAfterIt: the delete
// function reads the record the first time it is called, and it is a seeding
// attempt in phase reset, in memory and on disk: nothing resumes it and it admits
// no write. The record is gone only after the delete returned. (Until BUG-0817
// the record was absent at the first delete, which an active member admits.)
// [guard: call the delete function first; remove the record before the delete]
func TestTheLeftSeedsRecordIsVoidedBeforeItsFirstDeleteAndGoneAfterIt(t *testing.T) {
	st := threeNodes(t)
	slots := findSlots(t, st)
	slot := slots.outside
	kv := openKV(t, t.TempDir())
	t.Cleanup(func() { _ = kv.Close() })
	pulled := seedingRecord(machine.PhaseSnapshot, cluster.Position{Epoch: uint64(st.Epoch), Seq: 5}, "k")
	putRecord(t, kv, slot, pulled)
	g := newRig(t, kv, st, testSelf, true)
	g.boot(t)
	view := recordSlots{parts: newSeedPartsFor(g.rec, st, testSelf)}
	voided := func(where string, rec SeedRecord, ok bool) {
		t.Helper()
		if !ok || rec.State != machine.SeedSeeding || rec.Phase != machine.PhaseReset || len(rec.Cursor) != 0 || !rec.Cut.Zero() || rec.SeedID == pulled.SeedID {
			t.Errorf("at the first delete the record %s is %+v (present %v), want a new seeding attempt in phase reset with no cut and no cursor", where, rec, ok)
		}
	}
	calls := 0
	err := dropLeftSeed(t.Context(), g.rec, reconcileKey{slot, reconDB}, func(context.Context, reconcileKey) error {
		calls++
		rec, ok := g.rec.Record(slot, reconDB)
		voided("in memory", rec, ok)
		rec, ok = diskRecord(t, kv, slot)
		voided("on disk", rec, ok)
		if view.Admits(slot, reconDB) {
			t.Error("the pair admits a write at its first delete")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("the range was deleted %d times, want once", calls)
	}
	if rec, ok := g.rec.Record(slot, reconDB); ok {
		t.Errorf("the record is still in memory after the drop: %+v", rec)
	}
	if rec, ok := diskRecord(t, kv, slot); ok {
		t.Errorf("the record is still on disk after the drop: %+v", rec)
	}
}

// snapshotHookKV runs afterSnapshot once, right after the first snapshot is taken: the point
// at which Puller.resetRange has fixed the set of keys it will delete and has deleted none.
type snapshotHookKV struct {
	*storage.BadgerKV
	afterSnapshot func()
}

func (k *snapshotHookKV) NewSnapshot() storage.KVSnapshot {
	snap := k.BadgerKV.NewSnapshot()
	if hook := k.afterSnapshot; hook != nil {
		k.afterSnapshot = nil
		hook()
	}
	return snap
}

// leftSeed is a seeding pair of n2 (an active member) with part of its range pulled, after
// the pass that saw the pair leave the copy set (RF 2 to 1) answered DeleteRangeAndForget.
// back is the state that names n2 a copy again (RF 1 to 2); d1 and d2 are the partial seed's
// keys, d1 the one advanceTo rewrites.
type leftSeed struct {
	g            *rig
	kv           *storage.BadgerKV
	slot         uint16
	key          reconcileKey
	epoch        uint64
	shrunk, back cluster.ClusterState
	d1, d2       []byte
}

func newLeftSeed(t *testing.T, dir string) *leftSeed {
	t.Helper()
	return newLeftSeedOn(t, dir, func(kv *storage.BadgerKV) Store { return kv })
}

// newLeftSeedOn is newLeftSeed with the reconciler writing through the store wrap returns,
// so a test can fail one of the drop's record commits.
func newLeftSeedOn(t *testing.T, dir string, wrap func(*storage.BadgerKV) Store) *leftSeed {
	t.Helper()
	st := threeNodes(t)
	slot := findSlots(t, st).copyOnly
	l := &leftSeed{slot: slot, key: reconcileKey{slot, reconDB}, epoch: uint64(st.Epoch)}
	l.shrunk = st
	alterRF(t, &l.shrunk, 1)
	l.back = l.shrunk
	alterRF(t, &l.back, 2)
	l.kv = openKV(t, dir)
	putRecord(t, l.kv, slot, seedingRecord(machine.PhaseSnapshot, cluster.Position{Epoch: l.epoch, Seq: 5}, "k"))
	l.d1 = storage.Key(slot, reconDB, 5, storage.NSDocuments, []byte("d1"))
	l.d2 = storage.Key(slot, reconDB, 5, storage.NSDocuments, []byte("d2"))
	seeded := l.kv.NewBatch()
	defer seeded.Discard()
	for _, k := range [][]byte{l.d1, l.d2} {
		if err := seeded.Set(k, []byte("seeded")); err != nil {
			t.Fatal(err)
		}
	}
	if err := seeded.Commit(true); err != nil {
		t.Fatal(err)
	}
	l.g = newRigOn(t, l.kv, wrap(l.kv), st, testSelf, true)
	l.g.boot(t)
	l.g.sink.actions = nil
	l.g.state = l.shrunk
	if err := l.g.rec.OnStateSeq(t.Context(), l.shrunk); err != nil {
		t.Fatal(err)
	}
	if acts := l.g.sink.of(slot); len(acts) != 1 || acts[0].Do != machine.SlotDeleteRangeAndForget {
		t.Fatalf("the pass answered %+v, want one DeleteRangeAndForget", acts)
	}
	return l
}

// rangeKeys is how many keys the pair's range holds.
func rangeKeys(t *testing.T, kv *storage.BadgerKV, slot uint16) int {
	t.Helper()
	prefix := storage.PairPrefix(slot, reconDB)
	snap := kv.NewSnapshot()
	defer snap.Discard()
	it := snap.NewIterator(storage.IterOptions{Lower: prefix, Upper: keyRangeEnd(prefix)})
	defer it.Close()
	held := 0
	for it.Seek(prefix); it.Valid(); it.Next() {
		held++
	}
	if err := it.Err(); err != nil {
		t.Fatal(err)
	}
	return held
}

// TestAWriteIsNotAdmittedWhileALeftSeedsRangeIsBeingDeleted (BUG-0817): the range of a
// seeding pair that left the copy set is deleted by the real Puller.resetRange, and the copy
// set names the node again after the delete has fixed its keys and before it removed one. A
// pair with no record is "never written here, so empty" on an active member, which is false
// for as long as the delete runs: the pair must admit no write in that window, whatever the
// committed state says by then, and no ready record may be left over a range the delete
// emptied behind it. [guard: remove the record before the range is deleted]
func TestAWriteIsNotAdmittedWhileALeftSeedsRangeIsBeingDeleted(t *testing.T) {
	l := newLeftSeed(t, t.TempDir())
	t.Cleanup(func() { _ = l.kv.Close() })
	parts := newSeedPartsFor(l.g.rec, l.back, testSelf) // an active member that has observed the state
	view := recordSlots{parts: parts}
	if view.Admits(l.slot, reconDB) {
		t.Fatal("a seeding pair admits a write before the drop")
	}

	var admitted bool
	var advanceErr error
	hooked := &snapshotHookKV{BadgerKV: l.kv}
	hooked.afterSnapshot = func() {
		l.g.state = l.back
		admitted = view.Admits(l.slot, reconDB)
		advanceErr = l.g.rec.ApplyAdvance(l.slot, reconDB, advanceTo(l.kv, l.slot, 6, l.epoch)) // writes d1 = "v"
	}
	puller := &Puller{cfg: PullerConfig{KV: hooked, Budget: newReclaimBudget(reclaimBudgetConfig{})}}
	if err := dropLeftSeed(t.Context(), l.g.rec, l.key, puller.resetRange); err != nil {
		t.Fatalf("dropLeftSeed: %v", err)
	}
	if hooked.afterSnapshot != nil {
		t.Fatal("the range delete never took its snapshot, so the window was not opened")
	}

	if admitted {
		t.Error("the pair admitted a write while its range was being deleted")
	}
	if !errors.Is(advanceErr, ErrPairNotReady) {
		t.Errorf("a live step during the delete returned %v, want ErrPairNotReady", advanceErr)
	}
	if rec, ok := l.g.rec.Record(l.slot, reconDB); ok && rec.State == machine.SeedReady {
		t.Errorf("the delete left a ready record at %s over a range it emptied (%d keys left)", rec.Applied, rangeKeys(t, l.kv, l.slot))
	}
	if held := rangeKeys(t, l.kv, l.slot); held != 0 {
		t.Errorf("the left seed's range still holds %d keys after its delete", held)
	}
}

// TestACrashInsideALeftSeedsDeleteStillRefusesWritesAndRedoesTheDelete (BUG-0817): the node
// stops after the delete removed part of the range. What is on disk must still say "not
// admitted" at the next boot, before any pass runs, and the pass must start the pair over
// from a range reset (in the copy set) or order the delete again (out of it): never a ready
// record, written by a live step or by the interim rule, over the keys the delete left.
// [guard: remove the record before the range is deleted]
func TestACrashInsideALeftSeedsDeleteStillRefusesWritesAndRedoesTheDelete(t *testing.T) {
	// crashed runs the drop until its delete has removed d1 and fails there, then reopens the
	// store under a reconciler that has loaded its records and run no pass.
	crashed := func(t *testing.T, at func(*leftSeed) cluster.ClusterState) (*leftSeed, *rig) {
		t.Helper()
		dir := t.TempDir()
		l := newLeftSeed(t, dir)
		crash := errors.New("the node stopped inside the delete")
		err := dropLeftSeed(t.Context(), l.g.rec, l.key, func(context.Context, reconcileKey) error {
			half := l.kv.NewBatch()
			defer half.Discard()
			if err := half.Delete(l.d1); err != nil {
				return err
			}
			if err := half.Commit(true); err != nil {
				return err
			}
			return crash
		})
		if !errors.Is(err, crash) {
			t.Fatalf("dropLeftSeed = %v, want the delete's error", err)
		}
		if err := l.kv.Close(); err != nil {
			t.Fatal(err)
		}
		l.kv = openKV(t, dir)
		t.Cleanup(func() { _ = l.kv.Close() })
		state := at(l)
		g := newRig(t, l.kv, state, testSelf, true)
		if err := g.rec.LoadRecords(); err != nil {
			t.Fatal(err)
		}
		if held := rangeKeys(t, l.kv, l.slot); held != 1 {
			t.Fatalf("the half-deleted range holds %d keys, want the one the delete had not reached", held)
		}
		view := recordSlots{parts: newSeedPartsFor(g.rec, state, testSelf)}
		if view.Admits(l.slot, reconDB) {
			t.Error("after the restart the pair admits a write over its half-deleted range")
		}
		if err := g.rec.ApplyAdvance(l.slot, reconDB, advanceTo(l.kv, l.slot, 6, l.epoch)); !errors.Is(err, ErrPairNotReady) {
			t.Errorf("after the restart a live step returned %v, want ErrPairNotReady", err)
		}
		return l, g
	}

	t.Run("back in the copy set: the pair starts over from a range reset", func(t *testing.T) {
		l, g := crashed(t, func(l *leftSeed) cluster.ClusterState { return l.back })
		if err := g.rec.OnStateSeq(t.Context(), l.back); err != nil {
			t.Fatal(err)
		}
		if acts := g.sink.of(l.slot); len(acts) != 1 || acts[0].Do != machine.SlotRestartSeed {
			t.Fatalf("the first pass answered %+v, want one RestartSeed: a half-deleted range is neither resumed nor called ready", acts)
		}
		rec, ok := diskRecord(t, l.kv, l.slot)
		if !ok || rec.State != machine.SeedSeeding || rec.Phase != machine.PhaseReset || len(rec.Cursor) != 0 {
			t.Fatalf("the first pass left %+v (present %v), want a seeding attempt that owes the range reset", rec, ok)
		}
	})
	t.Run("still out of the copy set: the delete is ordered again and finishes", func(t *testing.T) {
		l, g := crashed(t, func(l *leftSeed) cluster.ClusterState { return l.shrunk })
		if err := g.rec.OnStateSeq(t.Context(), l.shrunk); err != nil {
			t.Fatal(err)
		}
		if acts := g.sink.of(l.slot); len(acts) != 1 || acts[0].Do != machine.SlotDeleteRangeAndForget {
			t.Fatalf("the first pass answered %+v, want one DeleteRangeAndForget", acts)
		}
		puller := &Puller{cfg: PullerConfig{KV: l.kv, Budget: newReclaimBudget(reclaimBudgetConfig{})}}
		if err := dropLeftSeed(t.Context(), g.rec, l.key, puller.resetRange); err != nil {
			t.Fatalf("dropLeftSeed: %v", err)
		}
		if rec, ok := diskRecord(t, l.kv, l.slot); ok {
			t.Fatalf("the finished drop left the record %+v", rec)
		}
		if held := rangeKeys(t, l.kv, l.slot); held != 0 {
			t.Fatalf("the finished drop left %d keys in the range", held)
		}
	})
}

// seedWalk drives a pair's seed machine through the events of one complete
// pull: registered at cut, the final chunk, the drain and the sync.
func seedWalk(t *testing.T, g *rig, slot uint16, cut cluster.Position, cfg machine.SeedConfig, register bool) machine.SeedOutcome {
	t.Helper()
	feed := func(ev machine.SeedEvent) machine.SeedOutcome {
		t.Helper()
		out, err := g.rec.FeedSeed(slot, reconDB, FeedSeedRequest{Event: ev, Cfg: cfg, DataDurable: true})
		if err != nil {
			t.Fatalf("%T: %v", ev, err)
		}
		if out.Restart != 0 {
			t.Fatalf("%T asked for a restart: %+v", ev, out)
		}
		return out
	}
	if register {
		feed(machine.RegisterAccepted{Cut: cut, WindowBytes: 1 << 20})
	}
	if out, err := g.rec.ApplySeedChunk(slot, reconDB, chunkReq(g.kv, slot, "k9", true, cfg)); err != nil || !out.ApplyChunk {
		t.Fatalf("the final chunk = %+v, %v", out, err)
	}
	// The slot record must carry the seed machine's phase after the final
	// chunk, in memory and on disk, or a restart resumes the wrong phase.
	if rec, _ := g.rec.Record(slot, reconDB); rec.Phase != machine.PhaseBuffer {
		t.Fatalf("memory phase after the final chunk = %v, want buffer", rec.Phase)
	}
	if disk, _ := diskRecord(t, g.kv, slot); disk.Phase != machine.PhaseBuffer {
		t.Fatalf("disk phase after the final chunk = %v, want buffer", disk.Phase)
	}
	feed(machine.BufferDrained{Last: cut})
	return feed(machine.SyncDone{})
}

func assertServes(t *testing.T, g *rig, st cluster.ClusterState, slot uint16, wantApplied cluster.Position) {
	t.Helper()
	rec, ok := g.rec.Record(slot, reconDB)
	if !ok || rec.State != machine.SeedReady || rec.Applied != wantApplied {
		t.Fatalf("record = %+v (%v), want ready at %v", rec, ok, wantApplied)
	}
	if disk, _ := diskRecord(t, g.kv, slot); disk.State != machine.SeedReady || disk.Applied != wantApplied {
		t.Fatalf("disk record = %+v, want ready at %v", disk, wantApplied)
	}
	g.sink.actions = nil
	if err := g.rec.OnStateSeq(t.Context(), st); err != nil {
		t.Fatal(err)
	}
	acts := g.sink.of(slot)
	if len(acts) != 1 || !acts[0].Serve || !acts[0].CountsAsCopy {
		t.Fatalf("a ready copy in the set = %+v, want it to serve and count", acts)
	}
	if p := g.pairs.Snapshot(slot, reconDB); p.AppliedPos != wantApplied {
		t.Fatalf("pair set = %+v, want %v", p, wantApplied)
	}
}

// TestAQueuedSeedBecomesReady: queued, pulled, synced: the record is ready,
// serves and counts as a copy. This is the path every slot the reconciler
// queues takes; on an active member the boot writes the interim (seeding S0),
// so the queue here is the horizon row's restart, which the interim answers.
func TestAQueuedSeedBecomesReady(t *testing.T) {
	st := threeNodes(t)
	slot := findSlots(t, st).copyOnly
	e := uint64(st.Epoch)
	cut := cluster.Position{Epoch: e, Seq: 5}
	kv := openKV(t, t.TempDir())
	t.Cleanup(func() { _ = kv.Close() })
	g := newRig(t, kv, st, testSelf, true)
	g.boot(t)
	if act, err := g.rec.Feed(slot, reconDB, machine.LogBelowHorizon{}); err != nil || act.Do != machine.SlotRestartSeed {
		t.Fatalf("setup: %+v, %v", act, err)
	}
	out := seedWalk(t, g, slot, cut, machine.SeedConfig{SyncBytes: 1 << 20, Epoch: e}, true)
	if !out.MarkReady {
		t.Fatalf("the sync did not mark the copy ready: %+v", out)
	}
	assertServes(t, g, st, slot, cut)
}

// TestAResumedSeedBecomesReady: a restart in the middle of the snapshot
// resumes, registers again on its cut and completes the same way.
func TestAResumedSeedBecomesReady(t *testing.T) {
	st := threeNodes(t)
	slot := findSlots(t, st).copyOnly
	e := uint64(st.Epoch)
	cut := cluster.Position{Epoch: e, Seq: 5}
	kv := openKV(t, t.TempDir())
	t.Cleanup(func() { _ = kv.Close() })
	putRecord(t, kv, slot, seedingRecord(machine.PhaseSnapshot, cut, "k1"))
	g := newRig(t, kv, st, testSelf, true)
	g.boot(t)
	if acts := g.sink.of(slot); len(acts) != 1 || acts[0].Do != machine.SlotResumeSeed {
		t.Fatalf("setup: %+v", acts)
	}
	out := seedWalk(t, g, slot, cut, machine.SeedConfig{SyncBytes: 1 << 20, Epoch: e}, false)
	if !out.MarkReady {
		t.Fatalf("the sync did not mark the copy ready: %+v", out)
	}
	assertServes(t, g, st, slot, cut)
}

// TestAStaleSeedCompletionIsIgnored: a seed that restarted is a new attempt; the
// events of the old one change nothing, and SeedFinished for it is a no-op.
func TestAStaleSeedCompletionIsIgnored(t *testing.T) {
	st := threeNodes(t)
	slot := findSlots(t, st).copyOnly
	e := uint64(st.Epoch)
	cut := cluster.Position{Epoch: e, Seq: 5}
	cfg := machine.SeedConfig{SyncBytes: 1 << 20, Epoch: e}
	kv := openKV(t, t.TempDir())
	t.Cleanup(func() { _ = kv.Close() })
	g := newRig(t, kv, st, testSelf, true)
	g.boot(t)
	// An active member's boot writes the interim (seeding S0); the attempt starts from the
	// horizon row's restart.
	if act, err := g.rec.Feed(slot, reconDB, machine.LogBelowHorizon{}); err != nil || act.Do != machine.SlotRestartSeed {
		t.Fatalf("setup: %+v, %v", act, err)
	}
	feed := func(ev machine.SeedEvent) {
		t.Helper()
		if _, err := g.rec.FeedSeed(slot, reconDB, FeedSeedRequest{Event: ev, Cfg: cfg, DataDurable: true}); err != nil {
			t.Fatal(err)
		}
	}
	feed(machine.RegisterAccepted{Cut: cut, WindowBytes: 1 << 20})
	if _, err := g.rec.ApplySeedChunk(slot, reconDB, chunkReq(g.kv, slot, "k9", true, cfg)); err != nil {
		t.Fatal(err)
	}
	feed(machine.BufferDrained{Last: cut}) // the sync is now owed
	before, _ := g.rec.Record(slot, reconDB)

	act, err := g.rec.Feed(slot, reconDB, machine.SeedRetryAsked{}) // the operator restarts it
	if err != nil || act.Do != machine.SlotRestartSeed {
		t.Fatalf("restart = %+v, %v", act, err)
	}
	restarted, _ := g.rec.Record(slot, reconDB)
	if restarted.SeedID == before.SeedID || restarted.Phase != machine.PhaseReset {
		t.Fatalf("the restart kept the old attempt: %+v", restarted)
	}
	feed(machine.SyncDone{}) // the old attempt's sync lands late
	if _, err := g.rec.Feed(slot, reconDB, machine.SeedFinished{Pos: cut}); err != nil {
		t.Fatal(err)
	}
	if rec, _ := g.rec.Record(slot, reconDB); rec.State != machine.SeedSeeding {
		t.Fatalf("a stale completion made the restarted seed %+v", rec)
	}
	// The new attempt, driven to the end, does become ready.
	out := seedWalk(t, g, slot, cut, cfg, true)
	if !out.MarkReady {
		t.Fatalf("the restarted attempt did not complete: %+v", out)
	}
	assertServes(t, g, st, slot, cut)
}

func chunkKey(slot uint16, cursor string) []byte {
	return storage.Key(slot, reconDB, 5, storage.NSDocuments, []byte(cursor))
}

// chunkReq stages one chunk whose last key is cursor, with its data in the batch.
func chunkReq(kv *storage.BadgerKV, slot uint16, cursor string, final bool, cfg machine.SeedConfig) SeedChunkRequest {
	return SeedChunkRequest{
		Cfg: cfg,
		Stage: func(batch storage.KVBatch, _ machine.SeedRecord) (machine.SeedEvent, error) {
			if err := batch.Set(chunkKey(slot, cursor), []byte("v")); err != nil {
				return nil, err
			}
			return machine.ChunkStored{Cursor: []byte(cursor), Bytes: 10, Entries: 1, Final: final}, nil
		},
		Commit: func(batch storage.KVBatch, sync bool) error { return batch.Commit(sync) },
	}
}

func hasKey(t *testing.T, kv *storage.BadgerKV, key []byte) bool {
	t.Helper()
	snap := kv.NewSnapshot()
	defer snap.Discard()
	_, err := snap.Get(key)
	if errors.Is(err, storage.ErrKeyNotFound) {
		return false
	}
	if err != nil {
		t.Fatal(err)
	}
	return true
}

// failAllStore fails every Commit while fail is set.
type failAllStore struct {
	*storage.BadgerKV
	fail *bool
}

func (f failAllStore) NewBatch() storage.KVBatch {
	return &failAllBatch{KVBatch: f.BadgerKV.NewBatch(), fail: f.fail}
}

type failAllBatch struct {
	storage.KVBatch
	fail *bool
}

func (b *failAllBatch) Commit(sync bool) error {
	if *b.fail {
		b.Discard()
		return errors.New("injected ENOSPC")
	}
	return b.KVBatch.Commit(sync)
}

// TestAFailedCommitLeavesMemoryAndDiskOnTheOldState: at every site that changes
// a pair, a commit that fails (ENOSPC) leaves memory equal to disk equal to the
// old state, a not-ready pair still refuses writes, and a retry from the old
// state succeeds.
func TestAFailedCommitLeavesMemoryAndDiskOnTheOldState(t *testing.T) {
	st := threeNodes(t)
	slots := findSlots(t, st)
	slot := slots.copyOnly
	e := uint64(st.Epoch)
	cut := cluster.Position{Epoch: e, Seq: 5}
	cfg := machine.SeedConfig{SyncBytes: 1 << 20, Epoch: e}
	shrunk := st
	alterRF(t, &shrunk, 1)
	joining := st
	setState(t, &joining, "n2", "down")
	replace(t, &joining, "n2", 2)

	cases := []struct {
		name  string
		boot  cluster.ClusterState
		prep  func(t *testing.T, g *rig)
		put   *SeedRecord
		op    func(t *testing.T, g *rig) error
		after func(t *testing.T, g *rig) // checked after the retry
	}{
		{name: "write the interim", boot: shrunk, // an active member's entering copy (seeding S0)
			op:    func(t *testing.T, g *rig) error { return g.rec.OnStateSeq(t.Context(), st) },
			after: func(t *testing.T, g *rig) { wantState(t, g, slot, machine.SeedReady) }},
		{name: "retire a ready copy", boot: st, put: ptr(readyRecord(cluster.Position{Epoch: e, Seq: 7})),
			op:    func(t *testing.T, g *rig) error { return g.rec.OnStateSeq(t.Context(), shrunk) },
			after: func(t *testing.T, g *rig) { wantState(t, g, slot, machine.SeedRetired) }},
		// A seeding pair that leaves the copy set is no longer dropped by the
		// pass: it answers SlotDeleteRangeAndForget and keeps the record, and the
		// sink voids the record first (seedpull_node.go:queueLeftSeed), so the
		// pass commits nothing here for a failed commit to leave half done.
		{name: "reset at a joining commit", boot: st, put: ptr(readyRecord(cluster.Position{Epoch: e, Seq: 7})),
			op:    func(t *testing.T, g *rig) error { return g.rec.OnJoiningCommit(joining) },
			after: func(t *testing.T, g *rig) { wantState(t, g, slot, machine.SeedSeeding) }},
		{name: "restart on request", boot: st, put: ptr(seedingRecord(machine.PhaseSnapshot, cut, "k")),
			op: func(t *testing.T, g *rig) error {
				_, err := g.rec.Feed(slot, reconDB, machine.SeedRetryAsked{})
				return err
			},
			after: func(t *testing.T, g *rig) {
				if rec, _ := g.rec.Record(slot, reconDB); rec.Phase != machine.PhaseReset {
					t.Fatalf("the retry left %+v", rec)
				}
			}},
		{name: "finish a seed", boot: st, put: ptr(seedingRecord(machine.PhaseNone, cut, "k9")),
			prep: func(t *testing.T, g *rig) {
				// resume registers the cut; the drain is already done.
				if _, ok := g.rec.Seed(slot, reconDB); !ok {
					t.Fatal("no resumed seed")
				}
			},
			op: func(t *testing.T, g *rig) error {
				_, err := g.rec.FeedSeed(slot, reconDB, FeedSeedRequest{Event: machine.SyncDone{}, Cfg: cfg, DataDurable: true})
				return err
			},
			after: func(t *testing.T, g *rig) { wantState(t, g, slot, machine.SeedReady) }},
		{name: "apply a chunk", boot: st, put: ptr(seedingRecord(machine.PhaseSnapshot, cut, "k1")),
			op: func(t *testing.T, g *rig) error {
				_, err := g.rec.ApplySeedChunk(slot, reconDB, chunkReq(g.kv, slot, "k2", false, cfg))
				return err
			},
			after: func(t *testing.T, g *rig) {
				if rec, _ := g.rec.Record(slot, reconDB); string(rec.Cursor) != "k2" || !hasKey(t, g.kv, chunkKey(slot, "k2")) {
					t.Fatalf("the retry left %+v", rec)
				}
			}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			kv := openKV(t, t.TempDir())
			t.Cleanup(func() { _ = kv.Close() })
			if tc.put != nil {
				putRecord(t, kv, slot, *tc.put)
			}
			fail := false
			g := newRigOn(t, kv, failAllStore{kv, &fail}, tc.boot, testSelf, true)
			g.boot(t)
			if tc.prep != nil {
				tc.prep(t, g)
			}
			memBefore, hadMem := g.rec.Record(slot, reconDB)
			diskBefore, hadDisk := diskRecord(t, kv, slot)
			pairBefore := g.pairs.Snapshot(slot, reconDB)

			fail = true
			if err := tc.op(t, g); err == nil || !strings.Contains(err.Error(), "slot") {
				t.Fatalf("the failed commit returned %v, want an error naming the pair", err)
			}
			memAfter, hasMem := g.rec.Record(slot, reconDB)
			diskAfter, hasDisk := diskRecord(t, kv, slot)
			if hasMem != hadMem || hasDisk != hadDisk || (hasMem && !bytes.Equal(mustEncode(t, memAfter), mustEncode(t, memBefore))) ||
				(hasDisk && !bytes.Equal(mustEncode(t, diskAfter), mustEncode(t, diskBefore))) {
				t.Fatalf("a failed commit moved the record: memory %+v (%v) -> %+v (%v), disk %+v (%v) -> %+v (%v)",
					memBefore, hadMem, memAfter, hasMem, diskBefore, hadDisk, diskAfter, hasDisk)
			}
			if hasMem && hasDisk && !bytes.Equal(mustEncode(t, memAfter), mustEncode(t, diskAfter)) {
				t.Fatalf("memory %+v and disk %+v disagree", memAfter, diskAfter)
			}
			if p := g.pairs.Snapshot(slot, reconDB); p != pairBefore {
				t.Fatalf("a failed commit moved the pair set: %+v -> %+v", pairBefore, p)
			}
			if hasMem && memAfter.State != machine.SeedReady {
				req := advanceTo(kv, slot, 9, e)
				if err := g.rec.ApplyAdvance(slot, reconDB, req); !errors.Is(err, ErrPairNotReady) {
					t.Fatalf("a write to a pair that is not ready got %v", err)
				}
			}

			fail = false
			if err := tc.op(t, g); err != nil {
				t.Fatalf("the retry from the old state failed: %v", err)
			}
			tc.after(t, g)
		})
	}
}

func ptr[T any](v T) *T { return &v }

func wantState(t *testing.T, g *rig, slot uint16, want uint8) {
	t.Helper()
	rec, ok := g.rec.Record(slot, reconDB)
	if !ok || rec.State != want {
		t.Fatalf("record = %+v (%v), want state %d", rec, ok, want)
	}
	if disk, _ := diskRecord(t, g.kv, slot); disk.State != want {
		t.Fatalf("disk record = %+v, want state %d", disk, want)
	}
}

// TestAFailedReadyCommitCannotBeFollowedByAWrite is the verifier's probe: the
// sync that would make a seed ready fails, then a write arrives. The pair is
// still seeding in memory and on disk, so the write is refused and no ready
// record lands over the half-seeded range.
func TestAFailedReadyCommitCannotBeFollowedByAWrite(t *testing.T) {
	st := threeNodes(t)
	slot := findSlots(t, st).copyOnly
	e := uint64(st.Epoch)
	cut := cluster.Position{Epoch: e, Seq: 5}
	cfg := machine.SeedConfig{SyncBytes: 1 << 20, Epoch: e}
	kv := openKV(t, t.TempDir())
	t.Cleanup(func() { _ = kv.Close() })
	putRecord(t, kv, slot, seedingRecord(machine.PhaseNone, cut, "k9"))
	fail := false
	g := newRigOn(t, kv, failAllStore{kv, &fail}, st, testSelf, true)
	g.boot(t)
	fail = true
	if _, err := g.rec.FeedSeed(slot, reconDB, FeedSeedRequest{Event: machine.SyncDone{}, Cfg: cfg, DataDurable: true}); err == nil {
		t.Fatal("the failed ready commit was reported as success")
	}
	if err := g.rec.ApplyAdvance(slot, reconDB, advanceTo(kv, slot, 9, e)); !errors.Is(err, ErrPairNotReady) {
		t.Fatalf("a write after the failed ready commit got %v, want ErrPairNotReady", err)
	}
	fail = false
	if rec, _ := diskRecord(t, kv, slot); rec.State != machine.SeedSeeding {
		t.Fatalf("disk = %+v, want the seeding record", rec)
	}
	// A reconcile pass over the same state must not hide the divergence either.
	if err := g.rec.OnStateSeq(t.Context(), st); err != nil {
		t.Fatal(err)
	}
	if rec, _ := g.rec.Record(slot, reconDB); rec.State != machine.SeedSeeding {
		t.Fatalf("memory = %+v after the pass", rec)
	}
}

// TestTheRecordAdvancesOnlyInTheChunksOwnBatch: ApplySeedChunk adds the cursor to
// the data's batch, so a failed commit leaves neither and a crash after the
// commit leaves both. FeedSeed refuses the events that carry data, and refuses
// to mark a seed ready unless the caller says its data is durable.
func TestTheRecordAdvancesOnlyInTheChunksOwnBatch(t *testing.T) {
	st := threeNodes(t)
	slot := findSlots(t, st).copyOnly
	e := uint64(st.Epoch)
	cut := cluster.Position{Epoch: e, Seq: 5}
	cfg := machine.SeedConfig{SyncBytes: 1 << 20, Epoch: e}
	dir := t.TempDir()
	kv := openKV(t, dir)
	putRecord(t, kv, slot, seedingRecord(machine.PhaseSnapshot, cut, "k1"))
	g := newRig(t, kv, st, testSelf, true)
	g.boot(t)

	// The record-only entry takes no chunk.
	if _, err := g.rec.FeedSeed(slot, reconDB, FeedSeedRequest{Event: machine.ChunkStored{Cursor: []byte("k9"), Entries: 1}, Cfg: cfg}); err == nil {
		t.Fatal("FeedSeed accepted a chunk: a record could lead its data")
	}
	if rec, _ := diskRecord(t, kv, slot); string(rec.Cursor) != "k1" {
		t.Fatalf("a refused chunk moved the cursor: %+v", rec)
	}

	// A chunk whose commit fails leaves record and data unchanged.
	failing := chunkReq(kv, slot, "k2", false, cfg)
	failing.Commit = func(batch storage.KVBatch, _ bool) error { batch.Discard(); return errors.New("disk full") }
	if _, err := g.rec.ApplySeedChunk(slot, reconDB, failing); err == nil {
		t.Fatal("a failed chunk commit was reported as success")
	}
	if rec, _ := diskRecord(t, kv, slot); string(rec.Cursor) != "k1" || hasKey(t, kv, chunkKey(slot, "k2")) {
		t.Fatalf("a failed chunk left %+v, data %v", rec, hasKey(t, kv, chunkKey(slot, "k2")))
	}
	if seed, _ := g.rec.Seed(slot, reconDB); string(seed.Cursor) != "k1" {
		t.Fatalf("a failed chunk moved the seed machine to %q", seed.Cursor)
	}

	// A crash after the commit and before the install: record and data are both
	// there, and a boot resumes from that cursor.
	crash := chunkReq(kv, slot, "k2", false, cfg)
	crash.Commit = func(batch storage.KVBatch, sync bool) error {
		if err := batch.Commit(sync); err != nil {
			return err
		}
		return errors.New("crashed before the install")
	}
	if _, err := g.rec.ApplySeedChunk(slot, reconDB, crash); err == nil {
		t.Fatal("the crash was swallowed")
	}
	if err := kv.Close(); err != nil {
		t.Fatal(err)
	}
	kv = openKV(t, dir)
	t.Cleanup(func() { _ = kv.Close() })
	g = newRig(t, kv, st, testSelf, true)
	g.boot(t)
	rec, _ := g.rec.Record(slot, reconDB)
	if string(rec.Cursor) != "k2" || !hasKey(t, kv, chunkKey(slot, "k2")) {
		t.Fatalf("after the crash the record is %+v and the data present is %v: they must agree", rec, hasKey(t, kv, chunkKey(slot, "k2")))
	}

	// A chunk the machine does not want (a duplicate) commits nothing.
	dup := chunkReq(kv, slot, "k1", false, cfg)
	committed := false
	dup.Commit = func(batch storage.KVBatch, sync bool) error { committed = true; return batch.Commit(sync) }
	if out, err := g.rec.ApplySeedChunk(slot, reconDB, dup); err != nil || out.ApplyChunk || committed {
		t.Fatalf("a duplicate chunk = %+v, %v, committed %v", out, err, committed)
	}
	if hasKey(t, kv, chunkKey(slot, "k1")) {
		t.Fatal("a refused chunk's data was written")
	}

	// MarkReady needs the caller to say the data is durable.
	if _, err := g.rec.ApplySeedChunk(slot, reconDB, chunkReq(kv, slot, "k9", true, cfg)); err != nil {
		t.Fatal(err)
	}
	if _, err := g.rec.FeedSeed(slot, reconDB, FeedSeedRequest{Event: machine.BufferDrained{Last: cut}, Cfg: cfg}); err != nil {
		t.Fatal(err)
	}
	if _, err := g.rec.FeedSeed(slot, reconDB, FeedSeedRequest{Event: machine.SyncDone{}, Cfg: cfg}); err == nil {
		t.Fatal("SyncDone marked a seed ready without DataDurable")
	}
	if rec, _ := g.rec.Record(slot, reconDB); rec.State != machine.SeedSeeding {
		t.Fatalf("a refused ready left %+v", rec)
	}
	out, err := g.rec.FeedSeed(slot, reconDB, FeedSeedRequest{Event: machine.SyncDone{}, Cfg: cfg, DataDurable: true})
	if err != nil || !out.MarkReady {
		t.Fatalf("SyncDone with durable data = %+v, %v", out, err)
	}
}

// blockingStore holds every Commit until release is closed, once armed.
type blockingStore struct {
	*storage.BadgerKV
	armed   *atomic.Bool
	started chan struct{}
	release chan struct{}
}

func (b blockingStore) NewBatch() storage.KVBatch {
	return &blockingBatch{KVBatch: b.BadgerKV.NewBatch(), store: b}
}

type blockingBatch struct {
	storage.KVBatch
	store blockingStore
}

func (b *blockingBatch) Commit(sync bool) error {
	if b.store.armed.CompareAndSwap(true, false) {
		select {
		case b.store.started <- struct{}{}:
		default:
		}
		<-b.store.release
	}
	return b.KVBatch.Commit(sync)
}

// TestAPairWithAStagedChangeRefusesWrites: between a retire being staged and its
// batch landing, memory still says ready; a write must not slip in with a ready
// record computed from that old state.
func TestAPairWithAStagedChangeRefusesWrites(t *testing.T) {
	st := threeNodes(t)
	slot := findSlots(t, st).copyOnly
	e := uint64(st.Epoch)
	shrunk := st
	alterRF(t, &shrunk, 1)
	kv := openKV(t, t.TempDir())
	t.Cleanup(func() { _ = kv.Close() })
	putRecord(t, kv, slot, readyRecord(cluster.Position{Epoch: e, Seq: 7}))
	var armed atomic.Bool
	store := blockingStore{kv, &armed, make(chan struct{}, 1), make(chan struct{})}
	g := newRigOn(t, kv, store, st, testSelf, true)
	g.boot(t)

	armed.Store(true)
	done := make(chan error, 1)
	go func() { done <- g.rec.OnStateSeq(t.Context(), shrunk) }()
	<-store.started
	admitted := make(chan error, 1)
	go func() { admitted <- g.rec.ApplyAdvance(slot, reconDB, advanceTo(kv, slot, 8, e)) }()
	select {
	case err := <-admitted:
		if !errors.Is(err, ErrPairBusy) {
			t.Fatalf("a write while the retire was staged got %v, want ErrPairBusy", err)
		}
	case <-time.After(3 * time.Second):
		close(store.release)
		t.Fatal("a write was admitted while the retire was staged: it reached the blocked commit")
	}
	if _, err := g.rec.Feed(slot, reconDB, machine.SeedRetryAsked{}); !errors.Is(err, ErrPairBusy) {
		t.Fatalf("Feed while the retire was staged got %v, want ErrPairBusy", err)
	}
	close(store.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := g.rec.ApplyAdvance(slot, reconDB, advanceTo(kv, slot, 8, e)); !errors.Is(err, ErrPairNotReady) {
		t.Fatalf("a write after the retire landed got %v, want ErrPairNotReady", err)
	}
}

// TestAReadySeedRecordGivesItsPairTheTailCursorAtBoot (BUG-0746): a pair that was seeded keeps a
// seed-shaped record, not the 32-byte local one, and the node that must send that pair's tail
// reads the log cursor from it. [GUARD: do not copy WALCursor into the pair at load]
func TestAReadySeedRecordGivesItsPairTheTailCursorAtBoot(t *testing.T) {
	st := threeNodes(t)
	slots := findSlots(t, st)
	dir := t.TempDir()
	kv := openKV(t, dir)
	applied := cluster.Position{Epoch: uint64(st.Epoch), Seq: 9}
	putRecord(t, kv, slots.copyOnly, readyRecord(applied))
	if err := kv.Close(); err != nil {
		t.Fatal(err)
	}
	kv = openKV(t, dir)
	t.Cleanup(func() { _ = kv.Close() })
	g := newRig(t, kv, st, testSelf, true)
	g.boot(t)
	got := g.pairs.Snapshot(slots.copyOnly, reconDB)
	if got.AppliedPos != applied || got.TailCursor != 11 {
		t.Fatalf("the seeded pair holds applied %s tail cursor %d, want %s and 11", got.AppliedPos, got.TailCursor, applied)
	}
}

// TestARetiredPairsGateOpensWithoutAnotherPass: the pass that retires a copy writes its fence,
// and the machine must know the fence in that same pass. Otherwise the answers of the other
// copies arrive, cover the fence, and the gate still says "write the fence" until some later
// commit runs another pass; an idle cluster has none, so the departed copy was never deleted.
func TestARetiredPairsGateOpensWithoutAnotherPass(t *testing.T) {
	st := threeNodes(t)
	slots := findSlots(t, st)
	kv := openKV(t, t.TempDir())
	t.Cleanup(func() { _ = kv.Close() })
	applied := cluster.Position{Epoch: uint64(st.Epoch), Seq: 7}
	putRecord(t, kv, slots.copyOnly, readyRecord(applied))
	shrunk := st
	alterRF(t, &shrunk, 1)
	g := newRig(t, kv, st, testSelf, true)
	g.boot(t)
	if err := g.rec.OnStateSeq(t.Context(), shrunk); err != nil {
		t.Fatal(err)
	}
	if disk, _ := diskRecord(t, kv, slots.copyOnly); disk.State != machine.SeedRetired {
		t.Fatalf("the pass left the record %+v, want retired", disk)
	}
	acts := g.sink.of(slots.copyOnly)
	if len(acts) == 0 || !acts[len(acts)-1].WriteFence || len(acts[len(acts)-1].Waiting) != 1 {
		t.Fatalf("the retire pass answered %+v, want the fence written and one member awaited", acts)
	}
	covers := machine.PositionReplied{Node: acts[len(acts)-1].Waiting[0], Epoch: uint64(shrunk.Epoch), State: machine.SlotReady, Applied: applied}
	act, err := g.rec.Feed(slots.copyOnly, reconDB, covers)
	if err != nil || act.Do != machine.SlotDeleteRange {
		t.Fatalf("a covering reply after the retire pass = %+v, %v, want DeleteRange with no second pass", act, err)
	}
}
