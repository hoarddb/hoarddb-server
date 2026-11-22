package server

import (
	"errors"
	"testing"
	"time"
)

// TestWaitRetiredWaitsForTheAppliesAtOrBelowIt: a replay reads the
// log up to what is synced, which can be ahead of this node's own apply. It
// waits for every apply at or below the last entry it read, not for one above.
func TestWaitRetiredWaitsForTheAppliesAtOrBelowIt(t *testing.T) {
	var mark unappliedMark
	lastSeq := uint64(9)
	below := mark.open(func() uint64 { return lastSeq })
	lastSeq = 20
	above := mark.open(func() uint64 { return lastSeq })
	stop := make(chan struct{})

	returned := make(chan error, 1)
	go func() { returned <- mark.waitRetired(15, stop, 10*time.Second) }()
	select {
	case err := <-returned:
		t.Fatalf("waitRetired(15) returned while the apply at floor 10 was open: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	mark.close(below)
	select {
	case err := <-returned:
		if err != nil {
			t.Fatalf("waitRetired(15) after the apply at floor 10 closed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("waitRetired(15) still waiting with only the apply at floor 21 open")
	}

	if err := mark.waitRetired(25, stop, 50*time.Millisecond); !errors.Is(err, errUnappliedWait) {
		t.Fatalf("waitRetired(25) with floor 21 open past its limit: %v, want errUnappliedWait", err)
	}
	close(stop)
	if err := mark.waitRetired(25, stop, 10*time.Second); !errors.Is(err, errUnappliedWait) {
		t.Fatalf("waitRetired(25) after stop: %v, want errUnappliedWait", err)
	}
	mark.close(above)
}
