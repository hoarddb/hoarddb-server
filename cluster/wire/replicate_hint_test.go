package wire

import (
	"context"
	"slices"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// hintOwner is an owner of one pair (u32Slot) whose log other pairs also write to, with a counting
// log under its replicator.
type hintOwner struct {
	*u32Node
	counting *scanCountingLog
	t        *testing.T
	nextSlot uint16
}

func newHintOwner(t *testing.T) *hintOwner {
	t.Helper()
	owner := &hintOwner{t: t, nextSlot: 1000}
	owner.u32Node = newU32Node(t, "owner", func(cfg *ReplicatorConfig) {
		owner.counting = &scanCountingLog{Log: cfg.Log}
		cfg.Log = owner.counting
	})
	owner.copies = []string{"copy"}
	return owner
}

// write appends position seq of the pair and queues it, as the write path does.
func (o *hintOwner) write(seq uint64) {
	o.t.Helper()
	pos := u32Pos(u32Epoch, seq)
	o.ownAppend(u32Slot, pos, cluster.Position{})
	o.rep.Enqueue(u32Slot, u32DB, pos, cluster.Position{}, o.wal.LastSequence())
}

// writeUnannounced appends position seq of the pair and queues nothing: an entry of the pair the
// write path never told the replicator about.
func (o *hintOwner) writeUnannounced(seq uint64) {
	o.t.Helper()
	o.ownAppend(u32Slot, u32Pos(u32Epoch, seq), cluster.Position{})
}

// others appends one record for each of count other pairs.
func (o *hintOwner) others(count int) {
	o.t.Helper()
	for range count {
		o.ownAppend(o.nextSlot, u32Pos(u32Epoch, 1), cluster.Position{})
		o.nextSlot++
	}
}

func (o *hintOwner) ship() {
	o.t.Helper()
	if err := o.flush(context.Background()); err != nil {
		o.t.Fatal(err)
	}
}

// ackApplied is the copy answering that it applied everything up to seq.
func (o *hintOwner) ackApplied(seq uint64) {
	o.t.Helper()
	o.rep.OnWriteAck(WriteAck{From: "copy", Slot: u32Slot, DB: u32DB, Pos: u32Pos(u32Epoch, seq), Outcome: AckApplied})
	o.ship()
}

// sentSeqs lists the positions sent to the copy, in order.
func (o *hintOwner) sentSeqs() []uint64 {
	var seqs []uint64
	for _, frame := range o.link.sent("copy") {
		seqs = append(seqs, frame.Pos.Seq)
	}
	return seqs
}

const hintReadBudget = 16

// hintGap is more entries than a read will skip from a remembered tail (maxTailLead), so a read
// that starts at a hint is forced to start exactly there.
const hintGap = maxTailLead + 904

// TestAPairsReadStartsNearTheTailNotAtItsOldCursor (BUG-0745): a pair that wrote before has a log
// cursor of its own, and a read from it walks every entry other pairs wrote since. The pair's
// next record is read from where it was appended. [GUARD: ignore the hint]
func TestAPairsReadStartsNearTheTailNotAtItsOldCursor(t *testing.T) {
	owner := newHintOwner(t)
	owner.write(1)
	owner.ship()
	owner.ackApplied(1)
	owner.others(5000)

	owner.counting.entries.Store(0)
	owner.write(2)
	owner.ship()
	if got := owner.counting.entries.Load(); got > hintReadBudget {
		t.Fatalf("shipping a pair's second record read %d log entries after 5000 of other pairs, want at most %d", got, hintReadBudget)
	}
	if got, want := owner.sentSeqs(), []uint64{1, 2}; !slices.Equal(got, want) {
		t.Fatalf("positions sent = %v, want %v", got, want)
	}
}

// TestAReadNeverSkipsAnUnreadRecordOfThePair: a read may start later than the copy's cursor only
// past other pairs' entries. Here the copy refuses the pair's second record after the third was
// queued; the rewind must read the second again, not start at the third. Records of other pairs
// lie between every pair record. [GUARD: keep the hint across a rewind]
func TestAReadNeverSkipsAnUnreadRecordOfThePair(t *testing.T) {
	owner := newHintOwner(t)
	owner.others(9)
	owner.write(1) // log sequence 10
	owner.ship()
	owner.ackApplied(1)

	owner.others(1)
	owner.write(2) // 12
	owner.ship()
	owner.rep.OnWriteAck(WriteAck{From: "copy", Slot: u32Slot, DB: u32DB, Pos: u32Pos(u32Epoch, 1), Outcome: AckRefused})
	owner.others(1)
	owner.write(3) // 14, queued while the copy has not answered
	owner.ship()
	if got, want := owner.sentSeqs(), []uint64{1, 2}; !slices.Equal(got, want) {
		t.Fatalf("positions sent before the copy answered = %v, want %v: nothing ships to a copy that refused", got, want)
	}

	owner.ackApplied(1) // the copy is ready again at the record it holds: the pair is rewound
	if got, want := owner.sentSeqs(), []uint64{1, 2, 2, 3}; !slices.Equal(got, want) {
		t.Fatalf("positions sent after the rewind = %v, want %v: position 2 never applied and must be read again", got, want)
	}
	owner.ackApplied(3)
	owner.others(1)
	owner.write(4)
	owner.ship()
	if got, want := owner.sentSeqs(), []uint64{1, 2, 2, 3, 4}; !slices.Equal(got, want) {
		t.Fatalf("positions sent = %v, want %v", got, want)
	}
}

// TestAnEmptyReadClearsTheHint: once a read finds nothing new the pair's old hint is spent, so
// the next record is read from its own position and the one after that again, round after round,
// not only the first. [GUARD: renew the hint only when there is none]
func TestAnEmptyReadClearsTheHint(t *testing.T) {
	owner := newHintOwner(t)
	for seq := uint64(1); seq <= 4; seq++ {
		owner.others(5000)
		owner.counting.entries.Store(0)
		owner.write(seq)
		owner.ship()
		if got := owner.counting.entries.Load(); seq > 1 && got > hintReadBudget {
			t.Fatalf("round %d: shipping the pair's record read %d log entries, want at most %d", seq, got, hintReadBudget)
		}
		owner.ackApplied(seq) // the read after the ack finds nothing new
	}
	if got, want := owner.sentSeqs(), []uint64{1, 2, 3, 4}; !slices.Equal(got, want) {
		t.Fatalf("positions sent = %v, want %v", got, want)
	}
}

// TestAnEnqueueThatIsNotTheNextPositionSetsNoHint: the write path queues every position of an
// owned pair in turn. A queue for a position that does not follow the last one (a shipped tail, a
// new epoch) says nothing about what lies between, so the read starts at the cursor and finds the
// unannounced entry. [GUARD: take any Enqueue as the next record]
func TestAnEnqueueThatIsNotTheNextPositionSetsNoHint(t *testing.T) {
	owner := newHintOwner(t)
	owner.write(1)
	owner.ship()
	owner.ackApplied(1)
	owner.others(hintGap)
	owner.writeUnannounced(2)
	owner.others(hintGap)
	owner.write(3)
	owner.ship()
	if got, want := owner.sentSeqs(), []uint64{1, 2, 3}; !slices.Equal(got, want) {
		t.Fatalf("positions sent = %v, want %v: position 2 sits between the cursor and the queued record", got, want)
	}
}

// TestAHintNeverMovesPastARecordQueuedButNotYetRead: two records of the pair are queued before
// the copy's read gets to either, with a long run of other pairs' entries between them. The hint
// stays on the first. [GUARD: move the hint to every queued record]
func TestAHintNeverMovesPastARecordQueuedButNotYetRead(t *testing.T) {
	owner := newHintOwner(t)
	owner.write(1)
	owner.ship()
	owner.ackApplied(1)
	owner.others(hintGap)
	owner.write(2)
	owner.others(hintGap)
	owner.write(3)
	owner.ship()
	if got, want := owner.sentSeqs(), []uint64{1, 2, 3}; !slices.Equal(got, want) {
		t.Fatalf("positions sent = %v, want %v: position 2 was queued before position 3", got, want)
	}
}
