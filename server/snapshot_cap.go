package server

import (
	"bytes"
	"math"
)

// snapshotCap follows one pull in flight across the rebases of its source's
// stream. A pull reads the source's incarnation at MIGRATE_START, and the
// snapshot's position is a sequence of that incarnation's log. If the source
// restarts before the position is recorded, the handshake's rebase has
// already run, with nothing to lower, and the new log is the old one only up
// to the rebase's ValidThrough. The cap is the lowest ValidThrough of every
// rebase since the pull began (BUG-0311).
//
// It is per source and per pull in flight, held in Cluster.snapshotCaps and
// gone when the pull ends: nothing is kept per bucket at rest, and nothing is
// persisted, since a restart of this node ends its pulls.
type snapshotCap struct {
	// validThrough is guarded by Cluster.appliedMu.
	validThrough uint64
}

// holdSnapshotCap starts a cap for a pull that read incarnation from source,
// if that is the incarnation this node has recorded for it, and says whether
// it is. The check and the registration are one step under appliedMu, so no
// rebase can run between them and miss the pull. Nothing matches an empty
// record, as in streamIncarnationIs.
func (c *Cluster) holdSnapshotCap(source string, incarnation []byte) (*snapshotCap, bool) {
	c.appliedMu.Lock()
	defer c.appliedMu.Unlock()
	recorded := c.recordedStream[source].incarnation
	if len(recorded) == 0 || !bytes.Equal(recorded, incarnation) {
		return nil, false
	}
	held := &snapshotCap{validThrough: math.MaxUint64}
	c.snapshotCaps[source] = append(c.snapshotCaps[source], held)
	return held, true
}

// releaseSnapshotCap ends a pull's cap.
func (c *Cluster) releaseSnapshotCap(source string, held *snapshotCap) {
	c.appliedMu.Lock()
	defer c.appliedMu.Unlock()
	remaining := c.snapshotCaps[source][:0]
	for _, other := range c.snapshotCaps[source] {
		if other != held {
			remaining = append(remaining, other)
		}
	}
	if len(remaining) == 0 {
		delete(c.snapshotCaps, source)
		return
	}
	c.snapshotCaps[source] = remaining
}

// capSnapshotsInFlightLocked applies a committed rebase of source to the pulls
// in flight from it: their snapshots are of the log the rebase left, valid
// only through validThrough. The caller holds appliedMu.
func (c *Cluster) capSnapshotsInFlightLocked(source string, validThrough uint64) {
	for _, held := range c.snapshotCaps[source] {
		held.validThrough = min(held.validThrough, validThrough)
	}
}
