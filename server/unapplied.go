package server

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// unappliedMark is the node-wide low-water mark of WAL entries that are
// appended and not yet applied. Retention may not truncate at or
// past such an entry: syncing the store before the truncate cannot flush an
// apply that has not happened, so the log is the entry's only durable copy
// until its apply has returned and a later sync has run.
//
// Every path that appends an entry it is about to apply registers here before
// the append and retires after the apply: Cluster.applyAsOwner,
// Cluster.applyAsReplica and Cluster.broadcastMutation. A registration's floor
// is the WAL's last sequence plus one, read before the append, so it is at or
// below the sequence the append assigns.
//
// It is O(writes in flight), never per bucket, and zero when the node is idle.
// Its mutex is a leaf: nothing else is taken while it is held.
type unappliedMark struct {
	mu sync.Mutex
	// floors counts the open registrations at each floor. Two writers opening
	// before either appends share a floor.
	floors map[uint64]int
	// retired is closed, and cleared, at the next close: what waitRetired
	// waits on.
	retired chan struct{}
}

// open registers an entry about to be appended and returns its floor, which
// the caller passes to close once the entry's apply has returned, whatever its
// outcome.
func (m *unappliedMark) open(lastSeq func() uint64) uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.floors == nil {
		m.floors = make(map[uint64]int)
	}
	floor := lastSeq() + 1
	m.floors[floor]++
	return floor
}

// close retires one registration at floor.
func (m *unappliedMark) close(floor uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.floors[floor]--
	if m.floors[floor] <= 0 {
		delete(m.floors, floor)
	}
	if m.retired != nil {
		close(m.retired)
		m.retired = nil
	}
}

// errUnappliedWait is waitRetired's answer when the entries it waits for are
// still being applied at its limit, or the caller stopped.
var errUnappliedWait = errors.New("entries read from the log are still being applied here")

// waitRetired returns once no entry at or below seq is appended and
// unapplied, so what this node's apply of each wrote is readable. A new
// registration's floor is above every sequence already appended, so the wait
// is bounded by the applies in flight; limit bounds it anyway, and stop ends it.
func (m *unappliedMark) waitRetired(seq uint64, stop <-chan struct{}, limit time.Duration) error {
	timer := time.NewTimer(limit)
	defer timer.Stop()
	for {
		m.mu.Lock()
		pending := false
		for floor := range m.floors {
			if floor <= seq {
				pending = true
				break
			}
		}
		if !pending {
			m.mu.Unlock()
			return nil
		}
		if m.retired == nil {
			m.retired = make(chan struct{})
		}
		retired := m.retired
		m.mu.Unlock()
		select {
		case <-retired:
		case <-stop:
			return fmt.Errorf("%w: the stream stopped", errUnappliedWait)
		case <-timer.C:
			return fmt.Errorf("%w after %v (at or below sequence %d)", errUnappliedWait, limit, seq)
		}
	}
}

// lowest returns the lowest open floor, and false when nothing is in flight.
func (m *unappliedMark) lowest() (uint64, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	lowest, found := uint64(0), false
	for floor := range m.floors {
		if !found || floor < lowest {
			lowest, found = floor, true
		}
	}
	return lowest, found
}
