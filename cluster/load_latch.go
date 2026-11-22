package cluster

import (
	"errors"
	"strconv"
	"sync"

	hraft "github.com/hashicorp/raft"
)

// LoadLatch decides when a restarted node's committed state is loaded. It is the one rule the
// seed driver and the server's replay gate share, so they cannot drift: both feed it the answer
// of Raft.LoadedNow, which is exact and reads no clock. Once true it stays true, except that a
// diverged node latches nothing new. The zero value is ready. It is not safe for concurrent
// use: Raft.StateLoaded and the seed driver each guard their own with a mutex.
type LoadLatch struct {
	// Loaded is the latch itself; tests set it to stand in for a settled state.
	Loaded bool
}

// Step takes one observation (loaded is Raft.LoadedNow) and reports whether the state is loaded.
func (l *LoadLatch) Step(loaded, diverged bool) bool {
	if diverged {
		// A frozen state is not a loaded one: nothing is latched. A latch from before the
		// divergence is left to the caller's own first check.
		return l.Loaded
	}
	if loaded {
		l.Loaded = true
	}
	return l.Loaded
}

// loadScanLimit bounds how many log entries LoadedNow reads looking for the last command at or
// below the library's applied index. Configuration and no-op entries are rare, so the scan ends
// within a few entries; past the bound the answer is "not loaded" (fail closed).
const loadScanLimit = 1 << 16

// LoadedNow reports, with no memory, whether the FSM applied every command the library has
// handed it. The floor is what the node holds without a command reaching Apply: the FSM's own
// applied index or the library's last_snapshot_index, which the library sets only after a
// snapshot restore finished (so it is the snapshot actually restored, older than the newest on
// disk when the newest was damaged, and the leader's when one was installed). A floor at or
// above the last command the log held at open is the fast path. Otherwise, once the library
// handed the log to the FSM (its AppliedIndex moves when a batch is queued, before the apply)
// up to the smaller of that index and the last command in the CURRENT log, the target is the
// last command entry at or below that AppliedIndex in the current log, and the state is loaded
// when the floor reaches it. An uncommitted trailing command (one or several) that the next
// leader's no-op replaced is gone from the current log, so the scan finds the earlier command
// the FSM did apply; a command still queued behind a stalled FSM is found and waited for,
// however long the stall. A missing index, a read error or an exhausted scan answer false,
// never true. A diverged node is never loaded.
func (r *Raft) LoadedNow() bool {
	if r.IsDiverged() {
		return false
	}
	floor := r.appliedFloor()
	if floor >= r.openedLastCommand {
		return !r.IsDiverged()
	}
	lastIndex, err := r.store.LastIndex()
	if err != nil {
		return false
	}
	current, err := lastCommandAtOrBelow(r.store, lastIndex)
	if err != nil {
		return false
	}
	if r.raft.AppliedIndex() < min(r.openedLastCommand, current) {
		return false
	}
	target, err := lastCommandAtOrBelow(r.store, r.raft.AppliedIndex())
	if err != nil {
		return false
	}
	return floor >= target && !r.IsDiverged()
}

// appliedFloor is the highest log index whose effect the FSM holds: what it applied itself, or
// what the library's last restored snapshot covers. An unreadable last_snapshot_index counts as
// no snapshot, which can only lower the floor (fail closed).
func (r *Raft) appliedFloor() uint64 {
	restored, err := strconv.ParseUint(r.raft.Stats()["last_snapshot_index"], 10, 64)
	if err != nil {
		restored = 0
	}
	return max(r.fsm.applied.Load(), restored)
}

// lastCommandAtOrBelow is the index of the last command entry at or below limit in the retained
// log, 0 when there is none (everything below the log's first index is covered by a snapshot).
func lastCommandAtOrBelow(store hraft.LogStore, limit uint64) (uint64, error) {
	first, err := store.FirstIndex()
	if err != nil {
		return 0, err
	}
	last, err := store.LastIndex()
	if err != nil {
		return 0, err
	}
	limit = min(limit, last)
	for scanned, index := 0, limit; index >= first && index > 0; index-- {
		if scanned++; scanned > loadScanLimit {
			return 0, errors.New("no command entry within the scan limit")
		}
		var entry hraft.Log
		if err := store.GetLog(index, &entry); err != nil {
			return 0, err
		}
		if entry.Type == hraft.LogCommand {
			return index, nil
		}
	}
	return 0, nil
}

// loadGate is Raft's own latch.
type loadGate struct {
	mu    sync.Mutex
	latch LoadLatch
}

// StateLoaded reports whether this node's committed state is loaded and the node is not
// diverged, by LoadedNow latched through a LoadLatch. The replay gate of the server waits on it;
// the seed driver feeds the same LoadedNow into the same LoadLatch on its own ticks.
func (r *Raft) StateLoaded() bool {
	if r.IsDiverged() {
		return false
	}
	g := &r.loadGate
	g.mu.Lock()
	if g.latch.Loaded {
		g.mu.Unlock()
		return true
	}
	g.mu.Unlock()
	// The scan runs outside the latch's mutex; the apply goroutine never takes it anyway.
	loaded := r.LoadedNow()
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.latch.Step(loaded, false)
}
