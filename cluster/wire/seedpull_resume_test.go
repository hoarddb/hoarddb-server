package wire

import (
	"context"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

// p6Rig boots a rig on kv the way production wires the pull: a real node-wide buffer, the
// drain's applier and the dedupe window. The caller wires the source, because the source
// outlives the first process exactly as another node's pin does.
func p6Rig(t *testing.T, kv storage.KV, applier Applier) *t3Rig {
	t.Helper()
	return newT3Rig(t, t3Options{
		kv: kv, Buffers: newSeedBuffers(1 << 20), Applier: applier, Window: p4WindowConfig,
		seed: SeedOptions{ChunkEntries: 3},
	})
}

// p6DieAtTheFinalChunk runs one pull and cancels its context the moment the source's final
// chunk arrives, before the chunk is applied: the record reaches the buffer phase on disk
// and neither the drain nor the closing round runs. It reports whether the final chunk
// arrived, and returns once Run has exited.
func p6DieAtTheFinalChunk(t *testing.T, rig *t3Rig) bool {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	final := false
	rig.wrap = func(_ string, _ transport.Message, reply *transport.Message, err error) (*transport.Message, error) {
		if reply != nil && reply.Env.Kind == transport.KindSeedChunk {
			if chunk, decodeErr := transport.DecodeSeedChunk(payloadOf(*reply)); decodeErr == nil && chunk.Final {
				final = true
				cancel()
			}
		}
		return reply, err
	}
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		rig.puller.Run(ctx)
	}()
	rig.puller.Queue(rig.slot, reconDB)
	select {
	case <-finished:
	case <-time.After(20 * time.Second):
		t.Fatal("the pull did not stop after the final chunk")
	}
	return final
}

// TestARestartInBufferNeverMarksReadyWithoutDraining: a crash after the final chunk and
// before the drain leaves a seeding record in the buffer phase, and its in-memory buffer is
// gone. The restart resumes as a snapshot on the same cut (the resume mask and the crashed
// cursor go out), the source's empty final chunk turns the phase back to the buffer, and the
// writes the lost buffer held are filled by the closing round before the pair is ready.
// [BREAK: feed BufferDrained from memory, without the closing round]
func TestARestartInBufferNeverMarksReadyWithoutDraining(t *testing.T) {
	badger := t3OpenKV(t, t.TempDir())
	t.Cleanup(func() {
		if err := badger.Close(); err != nil {
			t.Errorf("closing the store: %v", err)
		}
	})
	applier := &p4Applier{}
	crashed := p6Rig(t, badger, applier)
	owner := t3NewSource(t, "x", "x", uint64(crashed.state.Epoch), SeedOptions{ChunkEntries: 3})
	crashed.owner, crashed.sources = "x", map[string]*t3Source{"x": owner}
	cut := t3Cut(t, crashed)
	owner.fill(t, crashed.slot, reconDB, 6, cut, 0)
	body := p4Insert(t)
	// The live writes that were in the buffer when the process died: the owner holds them,
	// the copy never drained them.
	for seq := cut.Seq + 1; seq <= cut.Seq+2; seq++ {
		owner.hold(p4Frame(t, crashed, seq, body, uint8(seq)).PairRecord)
	}

	if !p6DieAtTheFinalChunk(t, crashed) {
		t.Fatal("the first process never saw a final chunk")
	}
	onDisk, found := diskRecord(t, badger, crashed.slot)
	if !found || onDisk.Phase != machine.PhaseBuffer || onDisk.State != machine.SeedSeeding {
		t.Fatalf("the crashed record is %+v, want a seeding record in the buffer phase", onDisk)
	}
	if len(onDisk.Cursor) == 0 {
		t.Fatal("the crashed record has no cursor; the snapshot's last key is what a resume continues after")
	}

	// The process starts again on the same store. The record is still in the buffer phase in
	// memory and on disk, and its buffer is empty: nothing may make it ready but a drain.
	rebooted := p6Rig(t, badger, applier)
	rebooted.owner, rebooted.sources = "x", map[string]*t3Source{"x": owner}
	if loaded, _ := rebooted.rec.Record(rebooted.slot, reconDB); loaded.Phase != machine.PhaseBuffer {
		t.Fatalf("the reloaded record is in phase %v, want %v: the buffer is runtime state", loaded.Phase, machine.PhaseBuffer)
	}
	rebooted.pullUntil("the resumed attempt to become ready", rebooted.isReady)

	registers := rebooted.requests(transport.KindSeedRegister)
	if len(registers) == 0 {
		t.Fatal("the resumed attempt never registered; a buffer-phase record resumes the snapshot it was in")
	}
	reg, err := transport.DecodeSeedRegister(payloadOf(registers[0]))
	if err != nil {
		t.Fatal(err)
	}
	if reg.Mask&seedMaskResume == 0 || len(reg.Cursor) == 0 || reg.SeedID != onDisk.SeedID {
		t.Fatalf("the restart registered seed %x mask %d cursor %x, want seed %x, the resume bit and a cursor",
			reg.SeedID, reg.Mask, reg.Cursor, onDisk.SeedID)
	}
	want := []uint64{cut.Seq + 1, cut.Seq + 2}
	if got := applier.filled(); len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("the closing round applied %v, want %v: the writes the lost buffer held are filled before ready", got, want)
	}
	record, _ := rebooted.rec.Record(rebooted.slot, reconDB)
	if record.State != machine.SeedReady {
		t.Fatalf("the resumed record is %v, want ready", record.State)
	}
	if record.Applied.Seq != cut.Seq+2 {
		t.Fatalf("the ready record is at %v, want %d: the closing round's writes are applied", record.Applied, cut.Seq+2)
	}
	if rebooted.ready.Load() != 1 {
		t.Fatalf("OnReady ran %d times, want 1", rebooted.ready.Load())
	}
}

// TestARestartAfterTheDrainStillBecomesReady: a crash between drained/done's commit and
// sync/ready leaves a seeding record in PhaseNone whose range is complete. The restart owes
// only the sync contract 2 names: it does not register, does not pull again, and the sync
// plus SyncDone make the pair ready. [BREAK: leave the PhaseNone record with no sync]
func TestARestartAfterTheDrainStillBecomesReady(t *testing.T) {
	badger := t3OpenKV(t, t.TempDir())
	t.Cleanup(func() {
		if err := badger.Close(); err != nil {
			t.Errorf("closing the store: %v", err)
		}
	})
	first := p6Rig(t, badger, &p4Applier{})
	owner := t3NewSource(t, "x", "x", uint64(first.state.Epoch), SeedOptions{ChunkEntries: 3})
	first.owner, first.sources = "x", map[string]*t3Source{"x": owner}
	cut := t3Cut(t, first)
	owner.fill(t, first.slot, reconDB, 6, cut, 0)

	// The crash: the drain committed drained/done (PhaseNone, the range complete through
	// its last applied position) and the process died before the sync and SyncDone.
	drained := cluster.Position{Epoch: cut.Epoch, Seq: cut.Seq + 3}
	crashed := seedingRecord(machine.PhaseNone, cut, "cursor-key")
	crashed.Applied = drained
	putRecord(t, badger, first.slot, crashed)

	rebooted := p6Rig(t, badger, &p4Applier{})
	rebooted.owner, rebooted.sources = "x", map[string]*t3Source{"x": owner}
	rebooted.pullUntil("the pair to become ready on its own sync", rebooted.isReady)

	record, _ := rebooted.rec.Record(rebooted.slot, reconDB)
	if record.State != machine.SeedReady {
		t.Fatalf("the record is %v, want ready", record.State)
	}
	if record.Applied != drained {
		t.Fatalf("the ready record is at %v, want %v: its own sync kept the drained position", record.Applied, drained)
	}
	if got := len(rebooted.requests(transport.KindSeedRegister)); got != 0 {
		t.Fatalf("%d registrations after a restart in PhaseNone, want 0: the range is complete and owes only its sync", got)
	}
	if rebooted.ready.Load() != 1 {
		t.Fatalf("OnReady ran %d times, want 1", rebooted.ready.Load())
	}
}
