package wire

import (
	"errors"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Seeding slice S4, step B4: a receiver pauses its pulls while its disk is at or above
// replication.seed_disk_high_water_pct, keeps their data, and resumes by itself. An error
// reading the disk does not pause a pull (owner question 3, 2026-11-04).

// s4HighWater is the receiver's replication.seed_disk_high_water_pct in these tests.
const s4HighWater = 85

// errStatfs is the error the disk read returns when the test turns the failure on.
var errStatfs = errors.New("statfs of the data directory failed")

// s4Disk is the receiver's disk as the pull reads it: a percentage the test moves, and a
// failure it can switch on.
type s4Disk struct {
	pct    atomic.Int64
	failed atomic.Bool
}

func (d *s4Disk) read() (int, error) {
	if d.failed.Load() {
		return 0, errStatfs
	}
	return int(d.pct.Load()), nil
}

// s4Paused reports whether the pull's status row says it is paused for the disk and names the fix.
func s4Paused(rig *t3Rig) bool {
	status, ok := rig.puller.Status(rig.slot, reconDB)
	return ok && strings.Contains(status.Detail, "seed_disk_high_water_pct")
}

// s4DiskRig is a receiver rig whose pair holds ten keys the owner does not have yet, and the
// owner, which holds ten keys of its own.
func s4DiskRig(t *testing.T, disk *s4Disk) (*t3Rig, *t3Source) {
	t.Helper()
	rig := newT3Rig(t, t3Options{seed: SeedOptions{ChunkEntries: 3, DiskHighWaterPct: s4HighWater}, DiskUsedPct: disk.read})
	owner := t3NewSource(t, "x", "x", uint64(rig.state.Epoch), SeedOptions{ChunkEntries: 3})
	rig.owner, rig.sources = "x", map[string]*t3Source{"x": owner}
	owner.fill(t, rig.slot, reconDB, 10, t3Cut(t, rig), 0)
	return rig, owner
}

// TestAPullAboveTheDiskHighWaterMarkPausesAndKeepsItsData: a pull that starts on a full disk sends
// nothing, deletes nothing and stays seeding, its status names the setting; once the disk has room
// it finishes. [BREAK: move the disk check in Puller.attempt below the reset of the range]
func TestAPullAboveTheDiskHighWaterMarkPausesAndKeepsItsData(t *testing.T) {
	var disk s4Disk
	disk.pct.Store(90)
	rig, owner := s4DiskRig(t, &disk)
	s1FillRange(t, rig.kv, rig.slot, reconDB, 10)
	record, _ := rig.rec.Record(rig.slot, reconDB)
	if record.State != machine.SeedSeeding || record.Phase != machine.PhaseReset {
		t.Fatalf("the pair starts %v in phase %v, want seeding in phase reset", record.State, record.Phase)
	}

	rig.pullUntil("the pull to pause on the disk", func() bool { return s4Paused(rig) || rig.isReady() })
	if rig.isReady() {
		t.Fatal("the pull finished on a full disk")
	}
	rig.mu.Lock()
	sent := len(rig.seen)
	rig.mu.Unlock()
	if sent != 0 {
		t.Errorf("the paused pull sent %d messages, want none", sent)
	}
	kept := t3Dump(t, rig.kv, rig.slot, reconDB)
	if len(kept) != 10 {
		t.Fatalf("%d keys on a paused receiver, want the 10 it held", len(kept))
	}
	for _, entry := range kept {
		if string(entry.Value) != "old" {
			t.Errorf("key %x holds %q after the pause, want the value it had", entry.Key, entry.Value)
		}
	}
	record, _ = rig.rec.Record(rig.slot, reconDB)
	if record.State != machine.SeedSeeding {
		t.Errorf("the record is %v during the pause, want seeding", record.State)
	}
	status, _ := rig.puller.Status(rig.slot, reconDB)
	if !strings.Contains(status.Detail, "replication.seed_disk_high_water_pct") || !strings.Contains(status.Detail, "resumes by itself") {
		t.Errorf("the pause says %q, want the fix and that it resumes by itself", status.Detail)
	}

	disk.pct.Store(10)
	rig.pullUntil("the pull to resume and finish", rig.isReady)
	t3SameEntries(t, t3Dump(t, rig.kv, rig.slot, reconDB), owner.dump(t, rig.slot, reconDB))
}

// TestAPullPausesBetweenChunksAndResumes: the disk fills while a pull is running; the next chunk is
// not asked for until the disk has room again, and then the pull finishes. [BREAK: remove the disk
// check at the top of the loop in Puller.stream]
func TestAPullPausesBetweenChunksAndResumes(t *testing.T) {
	var disk s4Disk
	rig, owner := s4DiskRig(t, &disk)
	acks := 0
	rig.wrap = func(_ string, msg transport.Message, reply *transport.Message, err error) (*transport.Message, error) {
		if msg.Env.Kind == transport.KindSeedAck {
			acks++
			if acks == 1 {
				disk.pct.Store(90)
			}
		}
		return reply, err
	}

	rig.pullUntil("the pull to pause on the disk", func() bool { return s4Paused(rig) || rig.isReady() })
	if rig.isReady() {
		t.Fatal("the pull finished while the disk was full")
	}
	if acks != 1 {
		t.Errorf("%d SEED_ACKs while the disk was full, want 1: the next chunk waits for the disk", acks)
	}

	disk.pct.Store(10)
	rig.pullUntil("the pull to resume and finish", rig.isReady)
	t3SameEntries(t, t3Dump(t, rig.kv, rig.slot, reconDB), owner.dump(t, rig.slot, reconDB))
}

// TestAFailedDiskReadDoesNotPauseThePull: a disk read that fails is reported and the pull goes on
// to ready. [BREAK: return a pause from Puller.diskPaused when the read fails]
func TestAFailedDiskReadDoesNotPauseThePull(t *testing.T) {
	var disk s4Disk
	disk.failed.Store(true)
	rig, owner := s4DiskRig(t, &disk)

	rig.pullUntil("the pull to finish", func() bool { return rig.isReady() || s4Paused(rig) })
	if !rig.isReady() {
		t.Fatal("a failed disk read paused the pull; it must go on")
	}
	t3SameEntries(t, t3Dump(t, rig.kv, rig.slot, reconDB), owner.dump(t, rig.slot, reconDB))
	rig.mu.Lock()
	reported := rig.reported
	rig.mu.Unlock()
	if !slices.ContainsFunc(reported, func(err error) bool { return errors.Is(err, errStatfs) }) {
		t.Errorf("the failed disk read was not reported; reported %v", reported)
	}
}
