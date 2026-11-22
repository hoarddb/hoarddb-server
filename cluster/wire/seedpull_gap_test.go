package wire

import (
	"slices"
	"sync"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

// p5Records is the source's log: one insert at each sequence, under a request id of its own so
// a catch-up hands the copy writes it has not seen in the buffer.
func p5Records(t *testing.T, rig *t3Rig, seqs ...uint64) []PairRecord {
	t.Helper()
	body := p4Insert(t)
	records := make([]PairRecord, 0, len(seqs))
	for _, seq := range seqs {
		records = append(records, PairRecord{
			Pos:    cluster.Position{Epoch: uint64(rig.state.Epoch), Seq: seq},
			Cursor: seq,
			Record: storage.WALRecord{Class: cluster.OpInsert, RequestID: [16]byte{byte(100 + seq)}, Body: body},
		})
	}
	return records
}

// p5Position is the position at sequence seq of the rig's epoch.
func p5Position(rig *t3Rig, seq uint64) cluster.Position {
	return cluster.Position{Epoch: uint64(rig.state.Epoch), Seq: seq}
}

// TestABufferGapAsksForCatchUpAndNeverSmooths: a buffered write above the next position is a
// hole. The drain asks the pinned source for the hole, applies what the source hands back in
// order, and applies nothing above the hole first. [BREAK: on a hole, apply the write above it
// without asking for the catch-up]
func TestABufferGapAsksForCatchUpAndNeverSmooths(t *testing.T) {
	applier := &p4Applier{}
	rig, buffers := p4Rig(t, applier, nil, 0, SeedOptions{ChunkEntries: 3})
	cut := t3Cut(t, rig)
	rig.sources["x"].log = p5Records(t, rig, cut.Seq+2, cut.Seq+3)
	body := p4Insert(t)
	p4OfferAtTheFinalChunk(rig, buffers, p4Frame(t, rig, cut.Seq+3, body, 3), p4Frame(t, rig, cut.Seq+1, body, 1))
	rig.pullUntil("the pull to finish with the hole filled from the source", rig.isReady)
	if got, want := applier.filled(), []uint64{cut.Seq + 1, cut.Seq + 2, cut.Seq + 3}; !slices.Equal(got, want) {
		t.Fatalf("the drain applied positions %v, want %v: the hole is filled before anything above it", got, want)
	}
	if !rig.askedFrom("x", p5Position(rig, cut.Seq+2)) {
		t.Fatalf("the Replicator never asked the pinned source for the hole at %s", p5Position(rig, cut.Seq+2))
	}
}

// TestAGapFillAdvancesTheSeedRecordInItsOwnBatch: a record the catch-up hands back for a hole
// is staged in the batch that also carries the seed record's step, one batch per record, as a
// drained write is. [BREAK: stage each gap-filled record in its own batch, committed before the
// seed record's step]
func TestAGapFillAdvancesTheSeedRecordInItsOwnBatch(t *testing.T) {
	counted := &p4BatchTally{KV: t3OpenKV(t, t.TempDir())}
	t.Cleanup(func() {
		if err := counted.Close(); err != nil {
			t.Errorf("closing the receiver store: %v", err)
		}
	})
	applier := &p4Applier{}
	rig, buffers := p4Rig(t, applier, counted, 0, SeedOptions{ChunkEntries: 3})
	cut := t3Cut(t, rig)
	rig.sources["x"].log = p5Records(t, rig, cut.Seq+2, cut.Seq+3)
	body := p4Insert(t)
	p4OfferAtTheFinalChunk(rig, buffers, p4Frame(t, rig, cut.Seq+1, body, 1), p4Frame(t, rig, cut.Seq+3, body, 3))
	rig.pullUntil("the pull to finish", rig.isReady)
	counted.mu.Lock()
	defer counted.mu.Unlock()
	if counted.dataCommits != 3 {
		t.Fatalf("the data committed in %d batches, want one per applied position (3: one drained, two gap-filled)", counted.dataCommits)
	}
	if counted.withoutRec != 0 {
		t.Fatalf("%d batch(es) of gap-filled or drained data committed without the seed record, want the record in each", counted.withoutRec)
	}
}

// TestAHorizonDuringTheDrainRestartsWithAFreshCut: the source no longer holds what the copy
// needs to fill a hole (LOG_BELOW_HORIZON). Nothing is smoothed over: the attempt restarts on a
// fresh cut, and the write above the hole is not applied by the attempt that met the horizon.
// [BREAK: on a horizon, wait and ask for the same gap again]
func TestAHorizonDuringTheDrainRestartsWithAFreshCut(t *testing.T) {
	applier := &p4Applier{}
	rig, buffers := p4Rig(t, applier, nil, 0, SeedOptions{ChunkEntries: 3})
	rig.sourceLacksOffered = true
	cut := t3Cut(t, rig)
	hole := p5Position(rig, cut.Seq+2)
	rig.sources["x"].horizonFrom = &hole
	body := p4Insert(t)
	p4OfferAtTheFinalChunk(rig, buffers, p4Frame(t, rig, cut.Seq+1, body, 1), p4Frame(t, rig, cut.Seq+3, body, 3))
	rig.pullUntil("the restarted pull to become ready", rig.isReady)
	if got, want := applier.filled(), []uint64{cut.Seq + 1}; !slices.Equal(got, want) {
		t.Fatalf("the drain applied positions %v, want %v: nothing above the horizon may apply", got, want)
	}
	if registers := len(rig.requests(transport.KindSeedRegister)); registers != 2 {
		t.Fatalf("the pull registered %d times, want 2: the horizon restarts the attempt on a fresh cut", registers)
	}
}

// TestTheDrainEndsOnlyWithACurrentCatchUp: a buffer that is empty is not the end of the pull.
// The drain ends with a catch-up from the next position that answers current, so writes the
// source appended after the cut reach the copy before it is ready. [BREAK: end the drain as
// soon as the buffer is empty, without the closing catch-up]
func TestTheDrainEndsOnlyWithACurrentCatchUp(t *testing.T) {
	applier := &p4Applier{}
	rig, _ := p4Rig(t, applier, nil, 0, SeedOptions{ChunkEntries: 3})
	cut := t3Cut(t, rig)
	rig.sources["x"].log = p5Records(t, rig, cut.Seq+1, cut.Seq+2, cut.Seq+3, cut.Seq+4)
	rig.pullUntil("the pull to finish", rig.isReady)
	if got, want := applier.filled(), []uint64{cut.Seq + 1, cut.Seq + 2, cut.Seq + 3, cut.Seq + 4}; !slices.Equal(got, want) {
		t.Fatalf("the pull applied positions %v before it was ready, want %v from the closing catch-up", got, want)
	}
	record, _ := rig.rec.Record(rig.slot, reconDB)
	if record.Applied != p5Position(rig, cut.Seq+4) {
		t.Fatalf("the ready record is at %s, want the source's tail %s", record.Applied, p5Position(rig, cut.Seq+4))
	}
}

// TestAFrameOfferedDuringTheClosingRoundIsKeptOrReshipped: a write the owner ships while the
// closing catch-up runs is kept by the buffer, because the phase mirror is still listening, and
// the drain applies it before the pull can end. [BREAK: stop listening after the buffer is seen
// empty and before the phase mirror is set; the window is inside EndIfEmpty, so this test does
// not reach it on every run]
func TestAFrameOfferedDuringTheClosingRoundIsKeptOrReshipped(t *testing.T) {
	applier := &p4Applier{}
	rig, buffers := p4Rig(t, applier, nil, 0, SeedOptions{ChunkEntries: 3})
	cut := t3Cut(t, rig)
	source := rig.sources["x"]
	source.log = p5Records(t, rig, cut.Seq+1, cut.Seq+2)
	late := p4Frame(t, rig, cut.Seq+3, p4Insert(t), 3)
	lateRecord := p5Records(t, rig, cut.Seq+3)
	var once sync.Once
	rig.catchHook = func(string, CatchUpRequest, CatchUpChunk) {
		// The owner appends position 3 and ships it after the first closing answer went out.
		once.Do(func() {
			source.log = append(source.log, lateRecord...)
			if !buffers.Offer(rig.slot, reconDB, late) {
				t.Error("the buffer refused a write offered during the closing round")
			}
		})
	}
	rig.pullUntil("the pull to finish", rig.isReady)
	if got, want := applier.filled(), []uint64{cut.Seq + 1, cut.Seq + 2, cut.Seq + 3}; !slices.Equal(got, want) {
		t.Fatalf("the pull applied positions %v, want %v: a write offered during the closing round is applied", got, want)
	}
}

// TestAGapTheSourceCannotFillRestartsWithAFreshCut: the source never held the position after
// the one the copy holds, so its current answer leaves the hole open. The attempt restarts on a
// fresh cut rather than asking the same gap again, and the write above the hole is not applied
// by the attempt that met it. [BREAK: when the hole stays open after a current catch-up, ask for
// the same gap again]
func TestAGapTheSourceCannotFillRestartsWithAFreshCut(t *testing.T) {
	applier := &p4Applier{}
	rig, buffers := p4Rig(t, applier, nil, 0, SeedOptions{ChunkEntries: 3})
	rig.sourceLacksOffered = true
	cut := t3Cut(t, rig)
	rig.sources["x"].log = p5Records(t, rig, cut.Seq+1)
	body := p4Insert(t)
	p4OfferAtTheFinalChunk(rig, buffers, p4Frame(t, rig, cut.Seq+1, body, 1), p4Frame(t, rig, cut.Seq+3, body, 3))
	rig.pullUntil("the restarted pull to become ready", rig.isReady)
	if slices.Contains(applier.filled(), cut.Seq+3) {
		t.Fatalf("the drain applied position %d above the hole at %d: %v", cut.Seq+3, cut.Seq+2, applier.filled())
	}
	if registers := len(rig.requests(transport.KindSeedRegister)); registers != 2 {
		t.Fatalf("the pull registered %d times, want 2: an unfilled hole restarts the attempt on a fresh cut", registers)
	}
}
