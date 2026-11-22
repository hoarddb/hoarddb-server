package wire

import (
	"strings"
	"testing"
)

// ackBatchOwner is an owner node whose pair has the copies named.
func ackBatchOwner(t *testing.T, copies ...string) *u32Node {
	t.Helper()
	owner := newU32Node(t, "owner", nil)
	owner.copies = copies
	return owner
}

// TestAnAckBatchCountsOnlyEntriesFromACopyOfThePair: an entry for a pair the sender is not a
// copy of is refused and named, and every other entry of the batch is still counted (I4).
// [GUARD: skip the isCopy check]
func TestAnAckBatchCountsOnlyEntriesFromACopyOfThePair(t *testing.T) {
	const strangerSlot = 7
	owner := ackBatchOwner(t, "copy")
	isCopy := func(slot uint16, _ uint64, node string) bool { return node == "copy" && slot != strangerSlot }
	acks := []WriteAck{
		{Slot: strangerSlot, DB: u32DB, Pos: u32Pos(u32Epoch, 2), Outcome: AckApplied},
		{Slot: u32Slot, DB: u32DB, Pos: u32Pos(u32Epoch, 3), Outcome: AckApplied},
	}
	err := owner.rep.OnWriteAckBatch("copy", acks, isCopy)
	if err == nil || !strings.Contains(err.Error(), "slot 7") {
		t.Fatalf("a batch with an entry from a non-copy returned %v, want an error naming slot 7", err)
	}
	if got := owner.rep.Acked(strangerSlot, u32DB, u32Pos(u32Epoch, 2)); got != 0 {
		t.Fatalf("an entry from a node that is not a copy of its pair counted: %d copies confirmed, want 0", got)
	}
	if got := owner.rep.Acked(u32Slot, u32DB, u32Pos(u32Epoch, 3)); got != 1 {
		t.Fatalf("the entry after the refused one was not counted: %d copies confirmed, want 1", got)
	}
	if err := owner.rep.OnWriteAckBatch("copy", acks[1:], isCopy); err != nil {
		t.Fatalf("a batch of only valid entries returned %v, want nil", err)
	}
}

// TestAnAckBatchIsAppliedEntryByEntryInOrder: each entry goes through OnWriteAck in the order
// sent, so a not-applied answer followed by an applied one ends applied and the reverse ends
// held, and the entries of other pairs in the same batch all count.
// [GUARD: apply only the last entry]
func TestAnAckBatchIsAppliedEntryByEntryInOrder(t *testing.T) {
	const slotB, slotC, slotA = 11, 12, 13
	owner := ackBatchOwner(t, "copy")
	everyone := func(uint16, uint64, string) bool { return true }
	acks := []WriteAck{
		{Slot: slotB, DB: u32DB, Pos: u32Pos(u32Epoch, 4), Outcome: AckApplied},
		{Slot: slotC, DB: u32DB, Outcome: AckNotApplied},
		{Slot: slotC, DB: u32DB, Pos: u32Pos(u32Epoch, 2), Outcome: AckApplied},
		{Slot: slotA, DB: u32DB, Pos: u32Pos(u32Epoch, 3), Outcome: AckApplied},
		{Slot: slotA, DB: u32DB, Outcome: AckNotApplied},
	}
	if err := owner.rep.OnWriteAckBatch("copy", acks, everyone); err != nil {
		t.Fatalf("OnWriteAckBatch: %v", err)
	}
	for _, want := range []struct {
		slot  uint16
		seq   uint64
		count int
		why   string
	}{
		{slotB, 4, 1, "an entry of another pair before the last one"},
		{slotC, 2, 1, "not applied then applied ends applied"},
		{slotA, 3, 0, "applied then not applied ends held"},
	} {
		if got := owner.rep.Acked(want.slot, u32DB, u32Pos(u32Epoch, want.seq)); got != want.count {
			t.Errorf("slot %d (%s): %d copies confirmed, want %d", want.slot, want.why, got, want.count)
		}
	}
}
