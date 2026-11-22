package server

import (
	"math"
	"testing"
)

// TestSnapshotCapKeepsTheLowestValidThroughAndEndsWithItsPull: two rebases
// during one pull leave the lowest ValidThrough, a pull that begins after them
// is not capped by them, and a released pull leaves nothing held for the
// source (BUG-0311).
func TestSnapshotCapKeepsTheLowestValidThroughAndEndsWithItsPull(t *testing.T) {
	c := &Cluster{
		recordedStream: map[string]recordedStream{"src": {incarnation: []byte("one")}},
		snapshotCaps:   make(map[string][]*snapshotCap),
	}
	if _, held := c.holdSnapshotCap("src", []byte("other")); held {
		t.Fatal("a cap was held for an incarnation the node has not recorded")
	}
	first, held := c.holdSnapshotCap("src", []byte("one"))
	if !held {
		t.Fatal("no cap held for the recorded incarnation")
	}
	c.capSnapshotsInFlightLocked("src", 50)
	c.capSnapshotsInFlightLocked("src", 80)
	later, _ := c.holdSnapshotCap("src", []byte("one"))
	if first.validThrough != 50 || later.validThrough != math.MaxUint64 {
		t.Errorf("caps are %d and %d, want 50 and the maximum", first.validThrough, later.validThrough)
	}
	c.releaseSnapshotCap("src", first)
	c.releaseSnapshotCap("src", later)
	if len(c.snapshotCaps) != 0 {
		t.Errorf("%d sources still hold caps after every pull ended", len(c.snapshotCaps))
	}
}
