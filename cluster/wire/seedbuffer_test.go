package wire

import (
	"sync"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/storage"
)

// bufferTestFrame is a WRITE for pair (1, 7) at seq, carrying a record body of bodyLen bytes.
func bufferTestFrame(seq uint64, bodyLen int) WriteFrame {
	return WriteFrame{
		Slot: 1,
		DB:   7,
		PairRecord: PairRecord{
			Pos:    cluster.Position{Epoch: 1, Seq: seq},
			Record: storage.WALRecord{Body: make([]byte, bodyLen)},
		},
	}
}

// bufferTestBody gives a frame a cost of 200 bytes: the 18-byte record header, the body and
// the 48-byte per-frame overhead.
const bufferTestBody = 200 - storage.WALRecordHeaderLen - 48

func TestOfferDropsAFrameOutsideSnapshotAndBuffer(t *testing.T) {
	b := newSeedBuffers(1 << 20)
	a := b.Begin(1, 7)
	defer b.End(1, 7)

	if b.Offer(1, 7, bufferTestFrame(1, 0)) {
		t.Fatal("a frame offered while the mirror is none was kept")
	}
	a.SetPhase(mirrorSnapshot)
	if !b.Offer(1, 7, bufferTestFrame(2, 0)) {
		t.Fatal("a frame offered in the snapshot was refused")
	}
	a.SetPhase(mirrorBuffer)
	if !b.Offer(1, 7, bufferTestFrame(3, 0)) {
		t.Fatal("a frame offered in the buffer was refused")
	}
	a.SetPhase(mirrorNone)
	if b.Offer(1, 7, bufferTestFrame(4, 0)) {
		t.Fatal("a frame offered after the mirror went none was kept")
	}
	if b.Offer(2, 7, bufferTestFrame(5, 0)) {
		t.Fatal("a frame for a pair with no attempt was kept")
	}
}

func TestTheBufferBoundIsNodeWideAndDividedOverThePulls(t *testing.T) {
	b := newSeedBuffers(1000)
	first := b.Begin(1, 7)
	second := b.Begin(2, 7)
	first.SetPhase(mirrorSnapshot)
	second.SetPhase(mirrorSnapshot)

	// Two pulls in flight share 1000 bytes: each takes 500, two frames of 200 each.
	for seq := uint64(1); seq <= 2; seq++ {
		if !b.Offer(1, 7, bufferTestFrame(seq, bufferTestBody)) {
			t.Fatalf("first pull refused frame %d inside its 500-byte share", seq)
		}
	}
	if b.Offer(1, 7, bufferTestFrame(3, bufferTestBody)) {
		t.Fatal("first pull kept a third frame past its 500-byte share")
	}
	for seq := uint64(11); seq <= 12; seq++ {
		if !b.Offer(2, 7, bufferTestFrame(seq, bufferTestBody)) {
			t.Fatalf("second pull refused frame %d inside its 500-byte share", seq)
		}
	}

	// When the first pull ends, the second may take the whole 1000 bytes: 400 held, 600 free.
	b.End(1, 7)
	for seq := uint64(13); seq <= 15; seq++ {
		if !b.Offer(2, 7, bufferTestFrame(seq, bufferTestBody)) {
			t.Fatalf("second pull refused frame %d after the first ended", seq)
		}
	}
	if b.Offer(2, 7, bufferTestFrame(16, bufferTestBody)) {
		t.Fatal("second pull kept a frame past the node-wide 1000 bytes")
	}
	b.End(2, 7)
}

func TestABufferOverflowMarksTheAttempt(t *testing.T) {
	b := newSeedBuffers(300)
	a := b.Begin(1, 7)
	defer b.End(1, 7)
	a.SetPhase(mirrorSnapshot)

	if !b.Offer(1, 7, bufferTestFrame(5, bufferTestBody)) {
		t.Fatal("the first frame inside the 300-byte share was refused")
	}
	if a.Overflowed() {
		t.Fatal("the attempt reports overflow before any frame was refused")
	}
	if b.Offer(1, 7, bufferTestFrame(9, bufferTestBody)) {
		t.Fatal("a frame past the 300-byte share was kept")
	}
	if b.Offer(1, 7, bufferTestFrame(4, bufferTestBody)) {
		t.Fatal("a second frame past the share was kept")
	}
	if !a.Overflowed() {
		t.Fatal("a refused frame did not mark the attempt overflowed")
	}
	if got := a.OverflowPos(); got != (cluster.Position{Epoch: 1, Seq: 9}) {
		t.Fatalf("OverflowPos = %v, want the first refused frame 1:9", got)
	}
	if _, ok := a.Pop(); !ok {
		t.Fatal("the frame kept inside the share is gone")
	}
	if !a.Empty() {
		t.Fatal("a refused frame was kept in the buffer")
	}
}

func TestThePopOrderIsPositionOrder(t *testing.T) {
	b := newSeedBuffers(1 << 20)
	a := b.Begin(1, 7)
	defer b.End(1, 7)
	a.SetPhase(mirrorSnapshot)

	for _, seq := range []uint64{5, 3, 4} {
		if !b.Offer(1, 7, bufferTestFrame(seq, 0)) {
			t.Fatalf("frame %d refused inside a 1 MiB bound", seq)
		}
	}
	for want := uint64(3); want <= 5; want++ {
		got, ok := a.Pop()
		if !ok {
			t.Fatalf("Pop ran dry before seq %d", want)
		}
		if got.frame.Pos.Seq != want {
			t.Fatalf("Pop returned seq %d, want %d: the buffer must drain in position order", got.frame.Pos.Seq, want)
		}
	}
	if _, ok := a.Pop(); ok {
		t.Fatal("Pop returned a frame from an empty buffer")
	}
	if !a.Empty() {
		t.Fatal("Empty reports a held frame after every Pop")
	}
}

func TestEndIfEmptyIsOneCriticalSection(t *testing.T) {
	// Each round races one Offer against EndIfEmpty. A kept frame must never sit in a buffer
	// whose mirror has gone none: no drain would ever apply it.
	for round := 0; round < 5000; round++ {
		b := newSeedBuffers(1 << 20)
		a := b.Begin(1, 7)
		a.SetPhase(mirrorBuffer)

		var kept bool
		var wg sync.WaitGroup
		start := make(chan struct{})
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			kept = b.Offer(1, 7, bufferTestFrame(uint64(round)+1, 0))
		}()
		close(start)
		ended := a.EndIfEmpty()
		wg.Wait()

		if ended && kept {
			t.Fatalf("round %d: EndIfEmpty ended the attempt and a frame was kept after its check", round)
		}
		if ended && !a.Empty() {
			t.Fatalf("round %d: EndIfEmpty returned true with a frame held", round)
		}
		if !ended && kept && a.Empty() {
			t.Fatalf("round %d: a kept frame was lost while the attempt stayed open", round)
		}
		b.End(1, 7)
	}
}
