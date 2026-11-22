package wire

import (
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
)

// otherCopyOf is the member of the slot's copy set that is not testSelf.
func otherCopyOf(t *testing.T, st cluster.ClusterState, slot uint16) string {
	t.Helper()
	for _, m := range reconcilePlacingTable(st, testSelf).Copies(slot, 2) {
		if m.ID != testSelf {
			return m.ID
		}
	}
	t.Fatalf("slot %d has no copy besides %s", slot, testSelf)
	return ""
}

// interimRecord is the record an active node writes for an absent in-target pair: ready with
// every field zero, so it carries no position, no seed id and no source.
func interimRecord() SeedRecord {
	rec := SeedRecord{}
	rec.State = machine.SeedReady
	return rec
}

// bitsView is reconcileViewOf for testSelf on the slot, over the state's own placing table.
func bitsView(st cluster.ClusterState, slot uint16, joining bool) reconcileView {
	table := reconcilePlacingTable(st, testSelf)
	return reconcileViewOf(reconcileViewRequest{
		State: st, Self: testSelf, Key: reconcileKey{slot, reconDB},
		Copies: table.Copies(slot, 2), Joining: joining, Table: table, RF: 2,
	})
}

// bitsBoot boots a reconciler on the state with the record (if any) on disk and returns it.
func bitsBoot(t *testing.T, st cluster.ClusterState, slot uint16, rec *SeedRecord) *rig {
	t.Helper()
	kv := openKV(t, t.TempDir())
	t.Cleanup(func() { _ = kv.Close() })
	if rec != nil {
		putRecord(t, kv, slot, *rec)
	}
	g := newRig(t, kv, st, testSelf, true)
	g.boot(t)
	return g
}

func wantOneAction(t *testing.T, g *rig, slot uint16, do, restart uint8) {
	t.Helper()
	acts := g.sink.of(slot)
	if len(acts) != 1 || acts[0].Do != do || acts[0].Restart != restart {
		t.Fatalf("slot %d actions = %+v, want one with Do %d Restart %d", slot, acts, do, restart)
	}
}

func wantDisk(t *testing.T, g *rig, slot uint16, state uint8, phase machine.Phase) SeedRecord {
	t.Helper()
	rec, ok := diskRecord(t, g.kv, slot)
	if !ok || rec.State != state || rec.Phase != phase {
		t.Fatalf("slot %d disk record = %+v (present %v), want state %d phase %d", slot, rec, ok, state, phase)
	}
	return rec
}

func TestTheViewCarriesWhetherOthersHoldTheBit(t *testing.T) {
	st := threeNodes(t)
	slot := findSlots(t, st).copyOnly
	other := otherCopyOf(t, st, slot)
	var outside string
	for _, id := range []string{"n1", "n2", "n3"} {
		if id != testSelf && id != other {
			outside = id
		}
	}
	for _, tc := range []struct {
		name                string
		bitOf               string
		wantOthers, wantOwn bool
	}{
		{"another copy's bit", other, true, false},
		{"only the own bit", testSelf, false, true},
		{"a bit of a node outside the copy set", outside, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			withBitOf(t, &st, tc.bitOf, int(slot))
			defer func() { st = threeNodes(t) }()
			view := bitsView(st, slot, false)
			if view.othersHoldBit != tc.wantOthers || view.ownBit != tc.wantOwn {
				t.Fatalf("othersHoldBit %v ownBit %v, want %v and %v", view.othersHoldBit, view.ownBit, tc.wantOthers, tc.wantOwn)
			}
		})
	}
}

func TestTheViewCarriesJoining(t *testing.T) {
	st := joiningNodes(t)
	slot := findSlots(t, st).copyOnly
	if !bitsView(st, slot, true).joining {
		t.Fatal("a request with Joining true gave a view with joining false")
	}
	if bitsView(st, slot, false).joining {
		t.Fatal("a request with Joining false gave a view with joining true")
	}
	// The machine sees it: a joiner's absent pair seeds although no bit is set.
	g := bitsBoot(t, st, slot, nil)
	wantOneAction(t, g, slot, machine.SlotQueueSeed, 0)
}

func TestANewDatabasesPairIsReadyEmptyAndAShiftedPairSeeds(t *testing.T) {
	st := threeNodes(t)
	slot := findSlots(t, st).copyOnly
	fresh := bitsBoot(t, st, slot, nil)
	wantOneAction(t, fresh, slot, machine.SlotReportBit, 0) // the ready-empty write asks for its bit
	if rec := wantDisk(t, fresh, slot, machine.SeedReady, machine.PhaseNone); !rec.Applied.Zero() || rec.SeedID != [16]byte{} {
		t.Fatalf("interim record = %+v, want no position and no seed id", rec)
	}

	withBitOf(t, &st, otherCopyOf(t, st, slot), int(slot))
	shifted := bitsBoot(t, st, slot, nil)
	wantOneAction(t, shifted, slot, machine.SlotQueueSeed, 0)
	wantDisk(t, shifted, slot, machine.SeedSeeding, machine.PhaseReset)
}

func TestAJoinersAbsentPairSeedsEvenWhenNoBitIsSet(t *testing.T) {
	st := joiningNodes(t)
	slot := findSlots(t, st).copyOnly
	g := bitsBoot(t, st, slot, nil)
	wantOneAction(t, g, slot, machine.SlotQueueSeed, 0)
	wantDisk(t, g, slot, machine.SeedSeeding, machine.PhaseReset)
}

func TestAnActiveNodesAbsentPairIsReadyEmptyWhenNoBitIsSet(t *testing.T) {
	st := threeNodes(t)
	for name, slot := range map[string]uint16{"copy": findSlots(t, st).copyOnly, "owner": findSlots(t, st).owner} {
		t.Run(name, func(t *testing.T) {
			g := bitsBoot(t, st, slot, nil)
			wantOneAction(t, g, slot, machine.SlotReportBit, 0)
			wantDisk(t, g, slot, machine.SeedReady, machine.PhaseNone)
		})
	}
}

func TestASeededEmptyPairIsNotReseededByTheBits(t *testing.T) {
	st := threeNodes(t)
	slot := findSlots(t, st).copyOnly
	withBitOf(t, &st, otherCopyOf(t, st, slot), int(slot))
	rec := readyRecord(cluster.Position{}) // seeded from an empty range: Applied zero, SeedID set
	g := bitsBoot(t, st, slot, &rec)
	for _, act := range g.sink.of(slot) {
		if act.Do == machine.SlotRestartSeed {
			t.Fatalf("a pair seeded from an empty range was restarted: %+v", act)
		}
	}
	wantOneAction(t, g, slot, machine.SlotReportBit, 0) // the bit rows decide: own bit clear, current
	wantDisk(t, g, slot, machine.SeedReady, machine.PhaseNone)
}

func TestAnInterimPairOfTheOwnerIsNotReseededByTheBits(t *testing.T) {
	st := threeNodes(t)
	slot := findSlots(t, st).owner
	withBitOf(t, &st, otherCopyOf(t, st, slot), int(slot))
	rec := interimRecord()
	g := bitsBoot(t, st, slot, &rec)
	for _, act := range g.sink.of(slot) {
		if act.Do == machine.SlotRestartSeed {
			t.Fatalf("the owner's interim pair was restarted: %+v", act)
		}
	}
	wantDisk(t, g, slot, machine.SeedReady, machine.PhaseNone)
}
