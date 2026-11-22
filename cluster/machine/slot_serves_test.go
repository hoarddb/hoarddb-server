package machine

import "testing"

// TestSlotRecordServesIsTheMachinesPostureForACopy: [GUARD a copy answers a read only when the
// record is ready, not rebuilding, reconciled in this process and in the target set].
func TestSlotRecordServesIsTheMachinesPostureForACopy(t *testing.T) {
	record := slotRec(SlotReady)
	if record.Serves() {
		t.Fatal("a ready record that has not been reconciled in this process serves")
	}
	slotReconcile(&record, slotView(slotIn, slotBit))
	if !record.Serves() {
		t.Fatal("a ready copy, reconciled and in the target set, does not serve")
	}
	rebuilding := slotRec(SlotReady)
	slotReconcile(&rebuilding, slotView(slotIn, slotBit))
	rebuilding.Apply(LogBelowHorizon{})
	if rebuilding.Serves() {
		t.Fatal("a copy asked to rebuild (LOG_BELOW_HORIZON) serves")
	}
	outside := slotRec(SlotReady)
	slotReconcile(&outside, slotView(slotBit))
	if outside.Serves() {
		t.Fatal("a copy outside the target set serves")
	}
	owner := slotRec(SlotReady)
	slotReconcile(&owner, slotView(slotIn, slotBit, slotRank1))
	if owner.Serves() {
		t.Fatal("Serves is the copy rule: the owner is answered by the gate, not by it")
	}
}
