package wire

import (
	"errors"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
)

// TestFeedSeedRefusesAnAcceptForAnAttemptTheRecordNoLongerHolds (BUG-0818): FeedSeed applies
// an event that names its attempt only while the record still holds that attempt. A
// RegisterAccepted for the attempt a restart replaced changes nothing, on disk or in memory,
// and one for the attempt the record holds registers it.
// [guard: feedSeedLocked ignores FeedSeedRequest.Attempt]
func TestFeedSeedRefusesAnAcceptForAnAttemptTheRecordNoLongerHolds(t *testing.T) {
	st := joiningNodes(t)
	kv := openKV(t, t.TempDir())
	t.Cleanup(func() {
		if err := kv.Close(); err != nil {
			t.Errorf("closing the store: %v", err)
		}
	})
	g := newRig(t, kv, st, testSelf, true)
	g.boot(t)
	slot := findSlots(t, st).copyOnly
	cfg := machine.SeedConfig{SyncBytes: 1 << 20, Epoch: uint64(st.Epoch)}
	accepted := machine.RegisterAccepted{Cut: cluster.Position{Epoch: uint64(st.Epoch), Seq: 40}, WindowBytes: 1 << 20}

	replaced, found := g.rec.Record(slot, reconDB)
	if !found || replaced.Phase != machine.PhaseReset {
		t.Fatalf("the booted record is %+v (found %v), want an attempt in the reset phase", replaced, found)
	}
	if err := g.rec.RestartSeed(slot, reconDB); err != nil {
		t.Fatalf("RestartSeed: %v", err)
	}
	held, _ := g.rec.Record(slot, reconDB)
	if held.SeedID == replaced.SeedID {
		t.Fatalf("the restart kept seed id %x", held.SeedID)
	}

	_, err := g.rec.FeedSeed(slot, reconDB, FeedSeedRequest{Event: accepted, Cfg: cfg, Attempt: &replaced.SeedID})
	if !errors.Is(err, ErrSeedAttemptReplaced) {
		t.Fatalf("an accept for the replaced attempt answered %v, want ErrSeedAttemptReplaced", err)
	}
	after, _ := g.rec.Record(slot, reconDB)
	if after.SeedID != held.SeedID || after.Phase != machine.PhaseReset || after.Cut != (cluster.Position{}) {
		t.Errorf("the refused accept changed the record: attempt %x phase %d cut %v, want %x in the reset phase with no cut", after.SeedID, after.Phase, after.Cut, held.SeedID)
	}
	onDisk, ok := diskRecord(t, kv, slot)
	if !ok || onDisk.SeedID != held.SeedID || onDisk.Phase != machine.PhaseReset {
		t.Errorf("the refused accept changed the record on disk: %+v (found %v)", onDisk, ok)
	}

	out, err := g.rec.FeedSeed(slot, reconDB, FeedSeedRequest{Event: accepted, Cfg: cfg, Attempt: &held.SeedID})
	if err != nil || out.Restart != 0 {
		t.Fatalf("an accept for the attempt the record holds: outcome %+v, error %v", out, err)
	}
	registered, _ := g.rec.Record(slot, reconDB)
	if registered.SeedID != held.SeedID || registered.Phase != machine.PhaseSnapshot || registered.Cut != accepted.Cut {
		t.Errorf("the record after its own accept is attempt %x phase %d cut %v, want %x in the snapshot phase at %v", registered.SeedID, registered.Phase, registered.Cut, held.SeedID, accepted.Cut)
	}
}
