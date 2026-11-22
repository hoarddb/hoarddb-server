package wire

import (
	"strings"
	"testing"
)

// Seeding slice S1, step 7: the guarded truncation seam (the entry ring seeding completion spec §1.7).

// TestTruncateLogBelowRefusesAtOrAboveAPinnedFloor: a catch-up pins the log from its floor.
// A truncation at the floor is refused and names the pin's kind, slot and database; one
// below it is allowed. The boundary is one rule: seq >= floor refuses. [BREAK: drop the
// check, or refuse strictly above only]
func TestTruncateLogBelowRefusesAtOrAboveAPinnedFloor(t *testing.T) {
	node := s0OpenClusterNode(t, t.TempDir(), nil)
	repl := node.cl.repl
	const floor = 50
	repl.mu.Lock()
	repl.pins[1] = PinnedFloor{Kind: PinCatchUp, Slot: 9, DB: 4, Floor: floor}
	repl.mu.Unlock()

	for _, seq := range []uint64{floor, floor + 1, floor + 1000} {
		err := node.TruncateLogBelow(seq)
		if err == nil {
			t.Fatalf("a truncation below %d was allowed while a catch-up pins %d", seq, floor)
		}
		for _, want := range []string{"catch-up of slot 9 of database 4 pins 50", "'status seeding'"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("the refusal for %d is %q, want it to contain %q", seq, err, want)
			}
		}
	}
	if err := node.TruncateLogBelow(floor - 1); err != nil {
		t.Fatalf("a truncation one below the floor was refused: %v", err)
	}

	repl.mu.Lock()
	delete(repl.pins, 1)
	repl.mu.Unlock()
	if err := node.TruncateLogBelow(floor + 1000); err != nil {
		t.Fatalf("a truncation with no pin was refused: %v", err)
	}
}

// TestTruncateLogBelowTakesTheLowestOfSeveralPins: the lowest floor decides, whichever pair
// holds it, and the error names that one.
func TestTruncateLogBelowTakesTheLowestOfSeveralPins(t *testing.T) {
	node := s0OpenClusterNode(t, t.TempDir(), nil)
	repl := node.cl.repl
	repl.mu.Lock()
	repl.pins[1] = PinnedFloor{Kind: PinCatchUp, Slot: 1, DB: 1, Floor: 90}
	repl.pins[2] = PinnedFloor{Kind: PinCatchUp, Slot: 2, DB: 2, Floor: 40}
	repl.mu.Unlock()
	err := node.TruncateLogBelow(60)
	if err == nil || !strings.Contains(err.Error(), "slot 2 of database 2 pins 40") {
		t.Fatalf("a truncation at 60 under pins 90 and 40 answered %v, want a refusal naming the pin at 40", err)
	}
}
