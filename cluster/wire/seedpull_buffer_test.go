package wire

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

// p2LiveFrame is one WRITE above the cut of the rig's pair, as the owner sends it during a seed.
func p2LiveFrame(rig *t3Rig, seq uint64, bodyBytes int) WriteFrame {
	return WriteFrame{
		Slot: rig.slot, DB: reconDB, Epoch: uint64(rig.state.Epoch),
		PairRecord: PairRecord{
			Pos:    cluster.Position{Epoch: uint64(rig.state.Epoch), Seq: seq},
			Record: storage.WALRecord{Body: make([]byte, bodyBytes)},
		},
	}
}

// p2MirrorOf reads the phase mirror of the open attempt of the rig's pair, or mirrorNone when
// no attempt is open.
func p2MirrorOf(buffers *seedBuffers, slot uint16, db uint64) seedPhaseMirror {
	buffers.mu.Lock()
	a := buffers.attempts[pairKey{slot, db}]
	buffers.mu.Unlock()
	if a == nil {
		return mirrorNone
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.phase
}

// A live write that does not fit the node's buffer restarts the pull as a fresh attempt: a new
// seed_id and no cursor, so the range is reset and filled again from a new cut.
func TestABufferOverflowRestartsAtReset(t *testing.T) {
	buffers := newSeedBuffers(64)
	rig := newT3Rig(t, t3Options{Buffers: buffers})
	owner := t3NewSource(t, "x", "x", uint64(rig.state.Epoch), SeedOptions{ChunkEntries: 3})
	rig.owner, rig.sources = "x", map[string]*t3Source{"x": owner}
	cut := t3Cut(t, rig)
	owner.fill(t, rig.slot, reconDB, 10, cut, 0)

	// One write above the cut, far larger than the 64-byte bound, arrives while the first
	// chunks are still streaming.
	var sent atomic.Bool
	rig.wrap = func(_ string, msg transport.Message, reply *transport.Message, err error) (*transport.Message, error) {
		if msg.Env.Kind == transport.KindSeedAck && sent.CompareAndSwap(false, true) {
			buffers.Offer(rig.slot, reconDB, p2LiveFrame(rig, cut.Seq+1, 4096))
		}
		return reply, err
	}
	rig.pullUntil("the pull to finish after the overflow restart", rig.isReady)

	registers := rig.requests(transport.KindSeedRegister)
	if len(registers) < 2 {
		t.Fatalf("the pull registered %d time(s), want a second registration after the overflow", len(registers))
	}
	first, err := transport.DecodeSeedRegister(payloadOf(registers[0]))
	if err != nil {
		t.Fatal(err)
	}
	again, err := transport.DecodeSeedRegister(payloadOf(registers[1]))
	if err != nil {
		t.Fatal(err)
	}
	if again.SeedID == first.SeedID {
		t.Errorf("the restarted attempt keeps seed %x, want a fresh seed_id", again.SeedID)
	}
	if again.Mask&seedMaskResume != 0 {
		t.Errorf("the restarted attempt registers with resume set and cursor %x, want no cursor", again.Cursor)
	}
}

// The puller's mirror of the record's phase follows the record: a snapshot while the chunks
// stream, and the buffer once the final chunk is in. The closing acknowledgement is sent after
// the drain has moved the record on, so it is where the buffer phase is read.
func TestThePhaseMirrorFollowsTheRecord(t *testing.T) {
	buffers := newSeedBuffers(1 << 20)
	rig := newT3Rig(t, t3Options{Buffers: buffers})
	owner := t3NewSource(t, "x", "x", uint64(rig.state.Epoch), SeedOptions{ChunkEntries: 3})
	rig.owner, rig.sources = "x", map[string]*t3Source{"x": owner}
	owner.fill(t, rig.slot, reconDB, 9, t3Cut(t, rig), 0)

	var mu sync.Mutex
	var mirrors []seedPhaseMirror
	rig.wrap = func(_ string, msg transport.Message, reply *transport.Message, err error) (*transport.Message, error) {
		if msg.Env.Kind == transport.KindSeedAck {
			mu.Lock()
			mirrors = append(mirrors, p2MirrorOf(buffers, rig.slot, reconDB))
			mu.Unlock()
		}
		return reply, err
	}
	rig.pullUntil("the pull to finish", rig.isReady)

	mu.Lock()
	defer mu.Unlock()
	if len(mirrors) < 2 {
		t.Fatalf("the pull sent %d acknowledgement(s), want one before the first chunk and one closing the seed", len(mirrors))
	}
	if first := mirrors[0]; first != mirrorSnapshot {
		t.Errorf("the mirror before the first chunk is %d, want mirrorSnapshot (%d)", first, mirrorSnapshot)
	}
	// The closing acknowledgement leaves after the buffer ended empty behind a current catch-up
	// (EndIfEmpty), so the mirror no longer listens: a frame offered now is refused and the owner
	// reships it (seeding completion 2.3, step 3).
	if last := mirrors[len(mirrors)-1]; last != mirrorNone {
		t.Errorf("the mirror at the closing acknowledgement is %d, want mirrorNone (%d): the buffer has ended", last, mirrorNone)
	}
}
