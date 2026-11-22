package server

import "testing"

// TestUnappliedMarkKeepsASharedFloorUntilBothClose: two writers that open
// before either appends share a floor, and the mark must hold it until the
// second of them has applied, not the first.
func TestUnappliedMarkKeepsASharedFloorUntilBothClose(t *testing.T) {
	var mark unappliedMark
	lastSeq := uint64(7)
	readLast := func() uint64 { return lastSeq }

	first := mark.open(readLast)
	second := mark.open(readLast)
	lastSeq = 9
	later := mark.open(readLast)

	for _, step := range []struct {
		name      string
		close     uint64
		wantFloor uint64
		wantOpen  bool
	}{
		{name: "first of the shared pair applied", close: first, wantFloor: 8, wantOpen: true},
		{name: "second of the shared pair applied", close: second, wantFloor: 10, wantOpen: true},
		{name: "the later writer applied", close: later, wantFloor: 0, wantOpen: false},
	} {
		mark.close(step.close)
		floor, open := mark.lowest()
		if floor != step.wantFloor || open != step.wantOpen {
			t.Fatalf("%s: lowest() = (%d, %v), want (%d, %v)", step.name, floor, open, step.wantFloor, step.wantOpen)
		}
	}
}
