package wire

import (
	"context"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// Seeding slice S1, step 4: what the owner's replicator believes about a copy that reset
// itself (the entry ring seeding completion spec §1.4).

// s1Owner is an owner with one copy, "copy", that was shipped nine records of epoch 7.
func s1Owner(t *testing.T) *u32Node {
	t.Helper()
	owner := newU32Node(t, "owner", nil)
	owner.copies = []string{"copy"}
	owner.ownRun(u32Epoch, 1, 9)
	owner.rep.Push(u32Slot, u32DB, u32Pos(u32Epoch, 9))
	if err := owner.flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := len(owner.link.sent("copy")); got != 9 {
		t.Fatalf("the first shipment sent %d frames, want 9", got)
	}
	return owner
}

func s1Ack(owner *u32Node, outcome AckOutcome, pos cluster.Position) {
	owner.rep.OnWriteAck(WriteAck{From: "copy", Slot: u32Slot, DB: u32DB, Pos: pos, Outcome: outcome})
}

// s1View reads the owner's view of the copy.
func s1View(owner *u32Node) (acked cluster.Position, holds bool, generation uint64) {
	owner.rep.mu.Lock()
	defer owner.rep.mu.Unlock()
	st := owner.rep.pairs[pairKey{u32Slot, u32DB}].peers["copy"]
	return st.acked, st.holds, st.generation
}

// TestAPositionlessAckNeverLowersTheOwnersView: a not-applied or refused ack with no position
// (0:0 on the wire) leaves st.acked where it was. [BREAK: lower on any lower position]
func TestAPositionlessAckNeverLowersTheOwnersView(t *testing.T) {
	owner := s1Owner(t)
	s1Ack(owner, AckApplied, u32Pos(u32Epoch, 9))
	for _, outcome := range []AckOutcome{AckNotApplied, AckRefused} {
		s1Ack(owner, outcome, cluster.Position{})
		if acked, _, _ := s1View(owner); acked != u32Pos(u32Epoch, 9) {
			t.Fatalf("a positionless answer %v lowered the owner's view to %s", outcome, acked)
		}
	}
}

// TestAZeroPositionAckNeverLowers: even an applied ack that carries 0:0 is no position.
// [BREAK: treat 0:0 as a position]
func TestAZeroPositionAckNeverLowers(t *testing.T) {
	owner := s1Owner(t)
	s1Ack(owner, AckApplied, u32Pos(u32Epoch, 9))
	s1Ack(owner, AckNotApplied, cluster.Position{})
	s1Ack(owner, AckApplied, cluster.Position{})
	acked, _, _ := s1View(owner)
	if acked != u32Pos(u32Epoch, 9) {
		t.Fatalf("an applied ack at 0:0 lowered the owner's view to %s", acked)
	}
}

// TestARefusedAckNeverLowersTheOwnersView: a refusal at a position below acked, with the
// copy's previous answer not applied, still changes nothing. [BREAK: lower on any non-zero
// lower position]
func TestARefusedAckNeverLowersTheOwnersView(t *testing.T) {
	owner := s1Owner(t)
	s1Ack(owner, AckApplied, u32Pos(u32Epoch, 9))
	s1Ack(owner, AckNotApplied, cluster.Position{})
	s1Ack(owner, AckRefused, u32Pos(u32Epoch, 4))
	if acked, _, _ := s1View(owner); acked != u32Pos(u32Epoch, 9) {
		t.Fatalf("a refusal at 7:4 lowered the owner's view to %s", acked)
	}
}

// TestAGapRefusalTrailingACatchUpAckKeepsTheOwnersView is the crossing of pi BL-2: the copy
// asks for a catch-up and then answers the gap, so its cumulative applied ack for b arrives
// before the refusal at a < b. [BREAK: lower on any non-zero lower position]
func TestAGapRefusalTrailingACatchUpAckKeepsTheOwnersView(t *testing.T) {
	owner := s1Owner(t)
	s1Ack(owner, AckApplied, u32Pos(u32Epoch, 3))
	s1Ack(owner, AckApplied, u32Pos(u32Epoch, 9)) // the catch-up's cumulative ack
	s1Ack(owner, AckRefused, u32Pos(u32Epoch, 3)) // the gap refusal, at the position it held
	acked, _, _ := s1View(owner)
	if acked != u32Pos(u32Epoch, 9) {
		t.Fatalf("the trailing gap refusal lowered the owner's view to %s", acked)
	}
	s1Ack(owner, AckApplied, u32Pos(u32Epoch, 9))
	if acked, holds, _ := s1View(owner); acked != u32Pos(u32Epoch, 9) || !holds {
		t.Fatalf("after the copy's next applied ack the view is %s holds %v", acked, holds)
	}
}

// TestACrossingCatchUpAckIsIgnoredWhileTheCopyHolds: an applied ack below acked whose copy
// answered applied last is a catch-up ack that crossed a later live ack: no lowering, no
// rewind. [BREAK: lower on every applied ack below acked]
func TestACrossingCatchUpAckIsIgnoredWhileTheCopyHolds(t *testing.T) {
	owner := s1Owner(t)
	s1Ack(owner, AckApplied, u32Pos(u32Epoch, 9))
	_, _, before := s1View(owner)
	s1Ack(owner, AckApplied, u32Pos(u32Epoch, 5))
	acked, holds, generation := s1View(owner)
	if acked != u32Pos(u32Epoch, 9) || !holds || generation != before {
		t.Fatalf("a crossing ack left the view at %s (holds %v, rewinds %d), want 7:9, holds, no rewind", acked, holds, generation-before)
	}
}

// TestTheFirstAppliedAckAfterAResetIsHonoured: a copy that answered not applied and then acks
// below its old position was reset: the owner takes the new position and rewinds to it.
// [BREAK: ignore every applied ack below acked]
func TestTheFirstAppliedAckAfterAResetIsHonoured(t *testing.T) {
	owner := s1Owner(t)
	s1Ack(owner, AckApplied, u32Pos(u32Epoch, 9))
	s1Ack(owner, AckNotApplied, cluster.Position{})
	_, _, before := s1View(owner)
	s1Ack(owner, AckApplied, u32Pos(u32Epoch, 3)) // the new cut
	acked, holds, generation := s1View(owner)
	if acked != u32Pos(u32Epoch, 3) || !holds || generation == before {
		t.Fatalf("after the first applied ack of a reset copy the view is %s (holds %v, rewound %v), want 7:3, holds, rewound", acked, holds, generation != before)
	}
	if got := owner.rep.Acked(u32Slot, u32DB, u32Pos(u32Epoch, 9)); got != 0 {
		t.Fatalf("the reset copy is counted for 7:9, which it no longer holds (%d)", got)
	}
	if got := owner.rep.Acked(u32Slot, u32DB, u32Pos(u32Epoch, 3)); got != 1 {
		t.Fatalf("the reset copy is not counted for its cut 7:3 (%d)", got)
	}
}

// TestAResetCopyIsNotCountedAfterItsFirstNotAppliedAck: a retry at a position at or below the
// stale acked, after the copy's 0:0 not-applied answer, does not count the copy. [BREAK:
// ignore not-applied acks for holds]
func TestAResetCopyIsNotCountedAfterItsFirstNotAppliedAck(t *testing.T) {
	owner := s1Owner(t)
	s1Ack(owner, AckApplied, u32Pos(u32Epoch, 9))
	if got := owner.rep.Acked(u32Slot, u32DB, u32Pos(u32Epoch, 5)); got != 1 {
		t.Fatalf("a healthy copy counts %d for 7:5, want 1", got)
	}
	s1Ack(owner, AckNotApplied, cluster.Position{})
	if got := owner.rep.Acked(u32Slot, u32DB, u32Pos(u32Epoch, 5)); got != 0 {
		t.Fatalf("a copy that answered not applied still counts %d for 7:5", got)
	}
}

// TestAQueueFullRefusalDropsTheCopyUntilItsNextAppliedAck pins the owner's ruling and its
// accepted price: a healthy copy that refuses one frame is not counted for a retried
// request until its next applied ack. [BREAK: keep holds on a positionless refusal]
func TestAQueueFullRefusalDropsTheCopyUntilItsNextAppliedAck(t *testing.T) {
	owner := s1Owner(t)
	s1Ack(owner, AckApplied, u32Pos(u32Epoch, 9))
	s1Ack(owner, AckNotApplied, cluster.Position{}) // the queue-full refusal carries no position
	if got := owner.rep.Acked(u32Slot, u32DB, u32Pos(u32Epoch, 9)); got != 0 {
		t.Fatalf("a copy whose last answer was a refusal counts %d for 7:9, want the accepted under-report of 0", got)
	}
	s1Ack(owner, AckApplied, u32Pos(u32Epoch, 9))
	if got := owner.rep.Acked(u32Slot, u32DB, u32Pos(u32Epoch, 9)); got != 1 {
		t.Fatalf("after its next applied ack the copy counts %d for 7:9, want 1", got)
	}
}

// TestAReseedingCopyIsNotCountedForANewWrite is a regression guard with no break: a new
// position exceeds every acked position, so a reset copy is not counted for it by positions
// alone.
func TestAReseedingCopyIsNotCountedForANewWrite(t *testing.T) {
	owner := s1Owner(t)
	s1Ack(owner, AckApplied, u32Pos(u32Epoch, 9))
	s1Ack(owner, AckNotApplied, cluster.Position{})
	if got := owner.rep.Acked(u32Slot, u32DB, u32Pos(u32Epoch, 10)); got != 0 {
		t.Fatalf("a reseeding copy is counted for the new write 7:10 (%d)", got)
	}
}

// TestTheOwnerPushesAgainAfterTheCopyIsReady: after the reset copy's first applied ack at its
// cut, the owner ships what follows the cut again, instead of waiting for acks that already
// went by. [BREAK: skip the rewind]
func TestTheOwnerPushesAgainAfterTheCopyIsReady(t *testing.T) {
	owner := s1Owner(t)
	s1Ack(owner, AckApplied, u32Pos(u32Epoch, 9))
	s1Ack(owner, AckNotApplied, cluster.Position{})
	owner.link.mu.Lock()
	owner.link.frames = map[string][]WriteFrame{}
	owner.link.mu.Unlock()
	s1Ack(owner, AckApplied, u32Pos(u32Epoch, 3))
	if err := owner.flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	shipped := map[cluster.Position]bool{}
	for _, frame := range owner.link.sent("copy") {
		shipped[frame.Pos] = true
	}
	for seq := uint64(4); seq <= 9; seq++ {
		if !shipped[u32Pos(u32Epoch, seq)] {
			t.Fatalf("the owner did not ship 7:%d again after the copy's cut at 7:3; shipped %v", seq, shipped)
		}
	}
}

// TestAStaleTailCursorBelowTheHorizonDoesNotWedgeTheShipment: the owner remembers the cursor where
// its last read of the log stopped and starts a later read of another pair from it. After the log
// is truncated past that cursor, a read from the first retained record that started from the
// stale cursor failed with the horizon again, and the owner re-armed the same read forever,
// so a copy below the horizon was never shipped the record that would make it ask for a reseed.
// The F7 harness test found it. [BREAK: keep the stale tails when the horizon is found]
func TestAStaleTailCursorBelowTheHorizonDoesNotWedgeTheShipment(t *testing.T) {
	owner := newU32Node(t, "owner", nil)
	owner.wal.SetMaxSegmentBytes(512)
	owner.copies = []string{"copy"}
	owner.ownRun(u32Epoch, 1, 30)
	owner.rep.Push(u32Slot, u32DB, u32Pos(u32Epoch, 30))
	if err := owner.flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	s1Ack(owner, AckApplied, u32Pos(u32Epoch, 2))
	owner.rep.rewindAfterSendFailure(pairKey{u32Slot, u32DB}, "copy") // the copy was away
	owner.ownRun(u32Epoch, 31, 60)
	if err := owner.wal.Truncate(owner.wal.LastSequence() - 4); err != nil {
		t.Fatal(err)
	}
	first, err := owner.wal.FirstSequence()
	if err != nil || first <= 31 {
		t.Fatalf("the log starts at %d (%v), want it past the remembered tail at 31", first, err)
	}
	owner.floor.Store(first)
	owner.link.mu.Lock()
	owner.link.frames = map[string][]WriteFrame{}
	owner.link.mu.Unlock()

	for range 6 {
		owner.rep.Retry()
		if err := owner.flush(context.Background()); err != nil {
			t.Fatal(err)
		}
		if len(owner.link.sent("copy")) > 0 {
			break
		}
	}
	sent := owner.link.sent("copy")
	if len(sent) == 0 {
		t.Fatal("the copy below the horizon was never shipped the first retained record: the stale tail cursor wedged the read")
	}
	if sent[0].Pos.Seq <= 3 {
		t.Fatalf("the shipment starts at %s, want the first retained record", sent[0].Pos)
	}
}
