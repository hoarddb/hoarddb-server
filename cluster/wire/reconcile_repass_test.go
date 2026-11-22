package wire

import (
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster/machine"
)

// TestAPassOverUnchangedQueuedPairsRewritesNothing: a joiner's pairs wait in phase reset
// for a pull slot while the cluster commits seed bits, and every commit runs a pass. A
// pass whose view of a queued pair is the one the last pass saw must leave its attempt
// alone: a new seed id per pass is a record write per queued pair per commit, and it voids
// the registration a puller has in flight on the old id.
func TestAPassOverUnchangedQueuedPairsRewritesNothing(t *testing.T) {
	st := joiningNodes(t)
	kv := openKV(t, t.TempDir())
	t.Cleanup(func() { _ = kv.Close() })
	g := newRig(t, kv, st, testSelf, true)
	g.boot(t)

	type queued struct {
		slot uint16
		id   [16]byte
	}
	var queuedPairs []queued
	for _, action := range g.sink.actions {
		if action.Do != machine.SlotQueueSeed {
			continue
		}
		record, found := g.rec.Record(action.Slot, action.DBID)
		if !found || record.State != machine.SeedSeeding || record.Phase != machine.PhaseReset {
			t.Fatalf("slot %d after the boot pass: %+v (found %v), want seeding from reset", action.Slot, record, found)
		}
		queuedPairs = append(queuedPairs, queued{action.Slot, record.SeedID})
	}
	if len(queuedPairs) == 0 {
		t.Fatal("the boot pass queued no pair; the joiner has nothing to seed")
	}

	for pass := range 3 {
		g.sink.actions = nil
		g.state.StateSeq++
		if err := g.rec.OnStateSeq(t.Context(), g.state); err != nil {
			t.Fatalf("pass %d: %v", pass, err)
		}
		for _, action := range g.sink.actions {
			if action.Do == machine.SlotRestartSeed {
				t.Fatalf("pass %d: slot %d got a fresh attempt although nothing about it changed", pass, action.Slot)
			}
		}
		for _, pair := range queuedPairs {
			record, found := g.rec.Record(pair.slot, reconDB)
			if !found || record.SeedID != pair.id {
				t.Fatalf("pass %d: slot %d seed id %x, want the boot pass's %x", pass, pair.slot, record.SeedID, pair.id)
			}
		}
	}
}
