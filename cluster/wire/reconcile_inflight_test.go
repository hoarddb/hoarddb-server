package wire

import (
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster/machine"
)

// TestAPassNeverRestartsAnAttemptThatIsRunningOnAnEmptyRange: a seed of an empty pair is
// cut at 0:0, and the slot machine reads a zero cut as "never registered", so a pass over
// the unchanged view used to answer a restart that threw the running attempt away: a new
// seed id, the partial range reset and the source's pin orphaned. A joiner passes once per
// committed seed report, so each of its in-flight pulls was restarted about once in twelve
// passes.
func TestAPassNeverRestartsAnAttemptThatIsRunningOnAnEmptyRange(t *testing.T) {
	st := joiningNodes(t)
	kv := openKV(t, t.TempDir())
	t.Cleanup(func() { _ = kv.Close() })
	g := newRig(t, kv, st, testSelf, true)
	g.boot(t)
	slot := findSlots(t, st).copyOnly

	cfg := machine.SeedConfig{SyncBytes: 1 << 20, Epoch: uint64(st.Epoch)}
	out, err := g.rec.FeedSeed(slot, reconDB, FeedSeedRequest{Event: machine.RegisterAccepted{WindowBytes: 1 << 20}, Cfg: cfg})
	if err != nil || out.Restart != 0 {
		t.Fatalf("registering the attempt: outcome %+v, error %v", out, err)
	}
	running, found := g.rec.Record(slot, reconDB)
	if !found || running.Phase != machine.PhaseSnapshot {
		t.Fatalf("after the registration the record is %+v (found %v), want seeding in the snapshot phase", running, found)
	}

	for pass := range 3 {
		g.sink.actions = nil
		g.state.StateSeq++
		if err := g.rec.OnStateSeq(t.Context(), g.state); err != nil {
			t.Fatalf("pass %d: %v", pass, err)
		}
		for _, action := range g.sink.of(slot) {
			if action.Do == machine.SlotRestartSeed {
				t.Fatalf("pass %d restarted the running attempt of slot %d", pass, slot)
			}
		}
		after, _ := g.rec.Record(slot, reconDB)
		if after.SeedID != running.SeedID || after.Phase != machine.PhaseSnapshot {
			t.Fatalf("pass %d changed the running attempt: seed id %x phase %v, want %x phase %v", pass, after.SeedID, after.Phase, running.SeedID, machine.PhaseSnapshot)
		}
	}
}
