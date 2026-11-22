package wire

import (
	"errors"
	"fmt"
	"slices"
	"sync"
)

// The guarded seam to the log's truncation (seeding-completion plan §1.7). storage.WAL.Truncate
// honours no pin, so an exported method that called it directly would let a caller cut the log
// out from under a catch-up or a seed that is reading it. TruncateLogBelow refuses instead, and
// names the pin. Nothing in cluster/wire truncates by itself yet: the retention pass that will
// (storage-format F11) goes through this method, and the legacy server.Cluster loop truncates
// the legacy stack's own log and does not use it.
//
// Cost: one slice of functions per node; a truncation asks each for its floors.

// floorRegistry is the set of sources of log floors on a node: the catch-ups it serves now, and
// later the seeds it serves.
type floorRegistry struct {
	mu      sync.Mutex
	sources []func() []PinnedFloor
}

// add registers a source of floors.
func (f *floorRegistry) add(source func() []PinnedFloor) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sources = append(f.sources, source)
}

// lowest returns the lowest floor any source holds, and false when none holds one.
func (f *floorRegistry) lowest() (PinnedFloor, bool) {
	f.mu.Lock()
	sources := slices.Clone(f.sources)
	f.mu.Unlock()
	var lowest PinnedFloor
	found := false
	for _, source := range sources {
		for _, floor := range source() {
			if !found || floor.Floor < lowest.Floor {
				lowest, found = floor, true
			}
		}
	}
	return lowest, found
}

// TruncateLogBelow drops the log segments that hold only sequences below seq, after the store
// is synced so no unsynced write still depends on them. It is exported because the harness
// lives in another package. A seq at or above the lowest floor a catch-up or a seed holds is
// refused: the boundary is inclusive at the WAL, so the one rule is seq >= floor.
//
// The floor check is advisory. It reads the floors once and then truncates, and WAL.Truncate
// honours no pin, so a catch-up that pins its floor after the read and before the truncation is
// not stopped by the check. What covers that window is the catch-up's own pin: pinFloor records
// its start and reads the first retained sequence again, moving the pin up when a truncation has
// already run. A truncation that lands after that second read finds the records gone, and the
// catch-up answers not caught up, so the requester asks again and gets the horizon. At this
// commit only catch-up pins reach the registry (node_cluster.go): a seed holds a Badger read
// transaction, not a WAL floor, and its closing round is a catch-up that pins through pinFloor.
// No test in this package forces the race between the check and the truncation.
//
// TODO(BUG-0760, retention): a pair whose boot replay waits for a declaration keeps its pair record
// at the cursor of its first unapplied record, and the log must keep everything from there. The
// automatic retention pass must floor at the lowest cursor of the pairs PairOwnership.Defer holds
// (replayDeferrals.stopped gives it); today no automatic truncation runs, and a hand-run one
// below that cursor would drop an acknowledged record the replay still owes.
func (n *Node) TruncateLogBelow(seq uint64) error {
	if floor, pinned := n.floors.lowest(); pinned && seq >= floor.Floor {
		return fmt.Errorf("the log cannot be truncated below %d: a %s of slot %d of database %d pins %d. Wait for it to finish, or run 'status seeding'", seq, floor.Kind, floor.Slot, floor.DB, floor.Floor)
	}
	if n.wal == nil {
		return errors.New("this node has no log to truncate")
	}
	if err := n.kv.Sync(); err != nil {
		return fmt.Errorf("syncing the store before truncating the log below %d: %w", seq, err)
	}
	if seq == 0 {
		return nil
	}
	if err := n.wal.Truncate(seq - 1); err != nil {
		return fmt.Errorf("truncating the log below %d: %w", seq, err)
	}
	return nil
}

// SetLogSegmentBytes lowers the size at which the log rotates, so a test can make a log of a
// few kilobytes that truncation can cut. It is a test seam and production code must not call it.
func (n *Node) SetLogSegmentBytes(size int64) { n.wal.SetMaxSegmentBytes(size) }
