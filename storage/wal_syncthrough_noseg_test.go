package storage

import (
	"sync/atomic"
	"testing"
	"time"
)

// TestSyncThroughWithoutASegmentNeedsNothingSynced: [GUARD]. While no segment can be activated, a log has
// nothing above its synced sequence to make durable: SyncThrough answers nil for its last sequence, and no
// append succeeds until a segment is activated.
func TestSyncThroughWithoutASegmentNeedsNothingSynced(t *testing.T) {
	var full atomic.Bool
	full.Store(true)
	clock := newWALTestClock()
	withOpenSyscalls(t, func(sys *walSyscalls) {
		failSpareHeader(&full)(sys)
		sys.now = clock.read
	})
	w, err := OpenWAL(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer closeWAL(t, w)
	if err := w.SyncThrough(w.LastSequence()); err != nil {
		t.Fatalf("SyncThrough(LastSequence) on a log with no segment = %v, want nil: nothing is above the synced sequence", err)
	}
	clock.advance(11 * time.Second)
	if _, err := appendRotationEntry(w, 0); err == nil {
		t.Fatal("an append succeeded while no segment could be activated")
	}
	full.Store(false)
	clock.advance(11 * time.Second) // the refusal clears on its own within 10 s of the last failed start
	if _, err := appendRotationEntry(w, 0); err != nil {
		t.Fatalf("append once a segment can be activated: %v", err)
	}
}
