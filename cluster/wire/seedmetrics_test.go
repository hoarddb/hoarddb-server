package wire

import (
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
)

// TestStateCountsCountEachStateOnce loads one record of each state from disk
// and checks the three counts. Retired and retiring both count as retired, so
// the two of them make 2.
func TestStateCountsCountEachStateOnce(t *testing.T) {
	st := threeNodes(t)
	kv := openKV(t, t.TempDir())
	t.Cleanup(func() { _ = kv.Close() })
	applied := cluster.Position{Epoch: uint64(st.Epoch), Seq: 4}
	putRecord(t, kv, 1, readyRecord(applied))
	putRecord(t, kv, 2, seedingRecord(machine.PhaseSnapshot, applied, "key-0001"))
	putRecord(t, kv, 3, retiredRecord())
	retiring := retiredRecord()
	retiring.State = machine.SeedRetiring
	putRecord(t, kv, 4, retiring)

	rig := newRig(t, kv, st, testSelf, true)
	if err := rig.rec.LoadRecords(); err != nil {
		t.Fatalf("LoadRecords: %v", err)
	}
	ready, seeding, retired := rig.rec.StateCounts()
	if ready != 1 || seeding != 1 || retired != 2 {
		t.Fatalf("StateCounts() = %d ready, %d seeding, %d retired; want 1, 1, 2", ready, seeding, retired)
	}
}

// TestStateCountsLeaveOutAPairWithNoRecord holds a pair in memory whose record
// is not on disk. Its stale retired state must not be counted: a pair with no
// record is not a copy this node holds.
func TestStateCountsLeaveOutAPairWithNoRecord(t *testing.T) {
	st := threeNodes(t)
	kv := openKV(t, t.TempDir())
	t.Cleanup(func() { _ = kv.Close() })
	putRecord(t, kv, 1, readyRecord(cluster.Position{Epoch: uint64(st.Epoch), Seq: 4}))

	rig := newRig(t, kv, st, testSelf, true)
	if err := rig.rec.LoadRecords(); err != nil {
		t.Fatalf("LoadRecords: %v", err)
	}
	rig.rec.pairs[reconcileKey{slot: 2, db: reconDB}] = &reconcilePair{disk: retiredRecord()}

	ready, seeding, retired := rig.rec.StateCounts()
	if ready != 1 || seeding != 0 || retired != 0 {
		t.Fatalf("StateCounts() = %d ready, %d seeding, %d retired; want 1, 0, 0: the pair with no record on disk is not held", ready, seeding, retired)
	}
}

func TestTheBytesCounterCountsTheChunksApplied(t *testing.T) {
	rig := newT3Rig(t, t3Options{})
	owner := t3NewSource(t, "x", "x", uint64(rig.state.Epoch), SeedOptions{ChunkEntries: 3})
	rig.owner, rig.sources = "x", map[string]*t3Source{"x": owner}
	owner.fill(t, rig.slot, reconDB, 10, t3Cut(t, rig), 0)
	var want uint64
	for _, entry := range owner.dump(t, rig.slot, reconDB) {
		want += uint64(entryBytes(entry.Key, entry.Value))
	}

	rig.pullUntil("the pair to be ready", rig.isReady)

	if bytes, _ := rig.puller.Counters(); bytes != want {
		t.Errorf("the bytes counter is %d, want %d, the entry bytes of the range", bytes, want)
	}
}

func TestTheRetriesCounterCountsAMoveToAnotherSource(t *testing.T) {
	pull := newFailoverPull(t)
	pull.run(t)
	if _, retries := pull.rig.puller.Counters(); retries < 1 {
		t.Errorf("the retries counter is %d after the pull moved from x to y, want at least 1", retries)
	}
}
