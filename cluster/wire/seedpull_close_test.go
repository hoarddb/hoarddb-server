package wire

import (
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

// closeEarlierEpochRig is a rig whose source cut the snapshot at (epoch-1, 40) and has appended up
// to tail in that same epoch while the committed epoch is epoch: the shape of a seed that crosses
// an epoch change.
func closeEarlierEpochRig(t *testing.T, tail uint64, applier Applier) (*t3Rig, *seedBuffers, uint64) {
	t.Helper()
	rig, buffers := p4Rig(t, applier, nil, 0, SeedOptions{ChunkEntries: 3})
	epoch := uint64(rig.state.Epoch)
	source := rig.sources["x"]
	source.pairs.With(rig.slot, reconDB, func(pair *cluster.Pair) { pair.AppliedPos = cluster.Position{Epoch: epoch - 1, Seq: 40} })
	if tail > 40 {
		source.hold(closeRecordAt(t, cluster.Position{Epoch: epoch - 1, Seq: tail}))
	}
	return rig, buffers, epoch
}

// closeRecordAt is a write of the source's log at pos.
func closeRecordAt(t *testing.T, pos cluster.Position) PairRecord {
	t.Helper()
	return PairRecord{
		Pos: pos, Cursor: pos.Seq,
		Record: storage.WALRecord{Class: cluster.OpInsert, RequestID: [16]byte{byte(pos.Seq)}, Body: p4Insert(t)},
	}
}

// closeSeedIDs counts the distinct seeds the pull registered: a retry of an attempt keeps its
// seed id, and an attempt on a fresh cut takes a new one.
func closeSeedIDs(t *testing.T, rig *t3Rig) int {
	t.Helper()
	ids := make(map[[16]byte]struct{})
	for _, msg := range rig.requests(transport.KindSeedRegister) {
		register, err := transport.DecodeSeedRegister(payloadOf(msg))
		if err != nil {
			t.Fatalf("decoding a SEED_REGISTER the pull sent: %v", err)
		}
		ids[register.SeedID] = struct{}{}
	}
	return len(ids)
}

// TestAClosingRoundBehindTheSourceTailRestartsOnAFreshCut: the snapshot was cut in an earlier
// epoch than the committed one, the source appended past the cut in that earlier epoch, and the
// receiver never held those writes. The closing round asks from the committed epoch's opener and
// is answered current at a tail above the record's applied position. The machine refuses ready
// there, so the attempt restarts on a fresh cut instead of looping on the same answer.
// [BREAK: complete the attempt without checking the record against the source's tail]
func TestAClosingRoundBehindTheSourceTailRestartsOnAFreshCut(t *testing.T) {
	applier := &p4Applier{}
	rig, _, _ := closeEarlierEpochRig(t, 100, applier)
	rig.pullUntil("the pull to take a second seed on a fresh cut", func() bool { return closeSeedIDs(t, rig) >= 2 })
	if rig.isReady() {
		t.Fatal("the pair became ready behind the source's tail")
	}
}

// TestAnEpochJumpHoleRestartsOnAFreshCut: a write is buffered in the cut's epoch, the committed
// epoch has moved on, and the source is quiet in it. The next position is the committed epoch's
// opener, which no catch-up can reach from the buffered write, so the hole cannot close; the
// attempt restarts on a fresh cut rather than asking the same question for ever.
// [BREAK: treat the hole as closed whenever the next position is not below the lowest write]
func TestAnEpochJumpHoleRestartsOnAFreshCut(t *testing.T) {
	applier := &p4Applier{}
	rig, buffers, epoch := closeEarlierEpochRig(t, 42, applier)
	body := p4Insert(t)
	frames := make([]WriteFrame, 0, 2)
	for seq := uint64(41); seq <= 42; seq++ {
		frame := p4Frame(t, rig, seq, body, byte(seq))
		frame.Epoch, frame.Pos.Epoch = epoch-1, epoch-1
		frames = append(frames, frame)
	}
	rig.sourceLacksOffered = true
	p4OfferAtTheFinalChunk(rig, buffers, frames...)
	rig.pullUntil("the pull to take a second seed on a fresh cut", func() bool { return closeSeedIDs(t, rig) >= 2 })
	if got := applier.filled(); len(got) != 0 {
		t.Fatalf("the drain applied positions %v, want none: nothing may apply past the hole", got)
	}
}
