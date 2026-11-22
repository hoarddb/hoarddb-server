package wire

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestTheSeedPullerStartsNothingWhileDiverged: a puller whose node recorded a divergence takes no
// queued pair and sends no request, and once the flag reads clear (a test only: the flag is not
// persisted and never clears in a process) the same queued pair is pulled to ready, so the quiet
// came from the gate and not from a broken rig. Break: drop the check at the top of Puller.next.
func TestTheSeedPullerStartsNothingWhileDiverged(t *testing.T) {
	rig := newT3Rig(t, t3Options{})
	owner := t3NewSource(t, "x", "x", uint64(rig.state.Epoch), SeedOptions{ChunkEntries: 3})
	rig.owner = "x"
	rig.sources = map[string]*t3Source{"x": owner}
	owner.fill(t, rig.slot, reconDB, 6, t3Cut(t, rig), 7)

	var diverged atomic.Bool
	diverged.Store(true)
	rig.puller.cfg.Diverged = diverged.Load

	ctx, cancel := context.WithCancel(context.Background())
	var running sync.WaitGroup
	running.Add(1)
	go func() {
		defer running.Done()
		rig.puller.Run(ctx)
	}()
	t.Cleanup(func() { cancel(); running.Wait() })
	rig.puller.Queue(rig.slot, reconDB)

	time.Sleep(300 * time.Millisecond)
	rig.mu.Lock()
	sent := len(rig.seen)
	rig.mu.Unlock()
	if sent != 0 || rig.isReady() {
		t.Fatalf("a diverged node's puller sent %d requests (ready %v), want none", sent, rig.isReady())
	}
	if pending := rig.puller.Pending(); pending == 0 {
		t.Fatal("the pair left the queue while the node was diverged")
	}

	diverged.Store(false)
	rig.puller.Queue(rig.slot, reconDB) // wakes the idle workers
	deadline := time.Now().Add(20 * time.Second)
	for !rig.isReady() && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if !rig.isReady() {
		status, _ := rig.puller.Status(rig.slot, reconDB)
		t.Fatalf("the pair was not pulled after the flag cleared; status %+v", status)
	}
}

// TestTheRetireGateDecidesNothingWhileDiverged: with every copy ready and the machine ready to
// open the gate, a pass on a diverged node asks nobody and deletes no range, and the same pass
// with the flag clear deletes it. Break: drop the check at the top of gateDriver.Pass.
func TestTheRetireGateDecidesNothingWhileDiverged(t *testing.T) {
	fx := newGateFixture(t)
	asks := &gateAsks{answers: fx.readyAnswers()}
	var errs []error
	deps := fx.deps(asks, &errs)
	var diverged atomic.Bool
	diverged.Store(true)
	deps.Diverged = diverged.Load
	gate := newGateDriver(deps)

	if err := gate.Pass(t.Context()); err != nil {
		t.Fatalf("Pass: %v", err)
	}
	if n := asks.asked(); n != 0 {
		t.Fatalf("a diverged node's gate asked %d members, want none", n)
	}
	fx.requireRetiredAndNothingDeleted(t)

	diverged.Store(false)
	if err := gate.Pass(t.Context()); err != nil {
		t.Fatalf("Pass: %v", err)
	}
	if asks.asked() == 0 || fx.deletes.Load() == 0 {
		t.Fatalf("the clear pass asked %d and deleted %d: the rig cannot open the gate, so the test proves nothing", asks.asked(), fx.deletes.Load())
	}
}
