package wire

import (
	"sync"
	"testing"
	"time"
)

// DrainObserved is called once for each time a write's first section waited for the pending list to
// drain, with how long it waited. Two concurrent writes of one document make the second wait for the
// first's flush; writes of distinct documents never drain, so they add no call. No document or bucket
// is named in the call.
func TestDrainObservedCountsEachSameDocumentWait(t *testing.T) {
	fx := newFixture(t)
	fx.wal.SetFsyncHook(func() { time.Sleep(50 * time.Millisecond) })
	var mu sync.Mutex
	var waits []time.Duration
	fx.w.cfg.DrainObserved = func(waited time.Duration) {
		mu.Lock()
		defer mu.Unlock()
		waits = append(waits, waited)
	}

	fx.writeConcurrently(testSlot, pairLockIDs(4))()
	mu.Lock()
	distinct := len(waits)
	mu.Unlock()
	if distinct != 0 {
		t.Fatalf("four writes of four documents observed %d drains, want none", distinct)
	}

	fx.writeConcurrently(testSlot, []string{"same", "same"})()
	mu.Lock()
	defer mu.Unlock()
	if len(waits) != 1 {
		t.Fatalf("two writes of one document observed %d drains, want 1", len(waits))
	}
	if waits[0] <= 0 {
		t.Fatalf("the drain wait was %v, want a positive duration", waits[0])
	}
}
