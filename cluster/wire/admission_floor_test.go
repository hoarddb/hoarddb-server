package wire

import (
	"slices"
	"testing"
)

// TestAnAdmissionFromABehindMemberDoesNotLowerTheFloor: [GUARD BUG-0877 review F4 (B): the floor
// keeps the highest admitting state_seq; a second admit from a member at a lower state_seq leaves
// it where it is, so the driver never judges states the first admitter had already passed].
// BREAK: make noteAdmission store the last value instead of keeping the highest.
func TestAnAdmissionFromABehindMemberDoesNotLowerTheFloor(t *testing.T) {
	var parts clusterParts
	parts.noteAdmission(40)
	parts.noteAdmission(12)
	if got := parts.admittedAt.Load(); got != 40 {
		t.Fatalf("after admits at state_seq 40 then 12 the floor is %d, want 40", got)
	}
	parts.noteAdmission(55)
	if got := parts.admittedAt.Load(); got != 55 {
		t.Fatalf("after a third admit at state_seq 55 the floor is %d, want 55", got)
	}
}

// TestTheFloorIsRecordedBeforeTheAdmitOpensRaft: [GUARD BUG-0877 review F4 (A): on a node that holds
// no cluster state the floor is recorded before Raft.Admit opens the Raft channel, so no entry can
// apply (and no seeding pass judge it) before the floor exists; a node that holds state records no
// floor and is still admitted]. BREAK: call admit before note in admitWithFloor.
func TestTheFloorIsRecordedBeforeTheAdmitOpensRaft(t *testing.T) {
	var parts clusterParts
	var calls []string
	note := func(seq uint64) { parts.noteAdmission(seq); calls = append(calls, "note") }
	admit := func() {
		if parts.admittedAt.Load() != 31 {
			t.Errorf("Admit ran with the floor at %d, want 31 already recorded", parts.admittedAt.Load())
		}
		calls = append(calls, "admit")
	}
	admitWithFloor(true, 31, note, admit)
	if !slices.Equal(calls, []string{"note", "admit"}) {
		t.Fatalf("a fresh node's admission ran %v, want [note admit]", calls)
	}

	calls = nil
	var held clusterParts
	admitWithFloor(false, 99, func(seq uint64) { held.noteAdmission(seq); calls = append(calls, "note") }, func() { calls = append(calls, "admit") })
	if !slices.Equal(calls, []string{"admit"}) || held.admittedAt.Load() != 0 {
		t.Fatalf("a node that holds state ran %v with floor %d, want [admit] and no floor", calls, held.admittedAt.Load())
	}
}
