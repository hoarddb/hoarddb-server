package cluster

import (
	"slices"
	"sync"
)

// Pair is the owner's in-memory state for one (slot, database) pair: 73 bytes
// plus the peer id (write path, Formats 1). It is one value per pair and
// nothing in it is per bucket, per document or per element (AGENTS.md rule 11).
type Pair struct {
	// AppliedPos is the last position applied (write path §3.3).
	AppliedPos Position
	// AppendedSeq is the highest seq appended at AppendedEpoch, 0 when none
	// (write path §3.1); it counts only while AppendedEpoch is the committed epoch.
	AppendedSeq uint64
	// AppendedEpoch is the epoch AppendedSeq was appended at: AppendedSeq counts
	// for an epoch only when they match, which is how an append learned before an
	// epoch move stops counting without a sweep of every pair (write path §3.1).
	AppendedEpoch uint64
	// Ordinal is the cumulative count of applied records, the unit the dedupe
	// window is measured in. An epoch never resets it (conventions §18), so a
	// pair that loses this state would restart the window at 0.
	Ordinal uint64
	// Flags holds bit0 sending, bit1 awaiting, bit2 replayNeeded, bit3
	// replayed, bit4 stopProved (write path §2a, §3.3, §8 step 5), bit5 restarted
	// (an awaiting hold opened inside the stop bound after a restart; wording only),
	// bit6 bootReplay (BUG-0760) and bit7 firstApplyFailed (BUG-0809). Every bit is
	// taken: a ninth flag widens the field.
	Flags uint8
	// DrainTo is the seq at which this node stopped assigning, meaningful only
	// while the sending bit is set.
	DrainTo uint64
	// Peer is the barrier's counterparty.
	Peer string
	// TailCursor is this node's log sequence of the record at AppliedPos, 0 when it is not
	// known (a pair seeded and not written since, a record the log no longer holds). It
	// lets a node that must send the pair's tail read that one record without walking the
	// log (BUG-0746). It lives in memory and is filled wherever AppliedPos moves.
	TailCursor uint64
}

// pairKey names a pair. It has no bucket component by design: a bucket adds
// no entry to the map, so the map is O(slots held x databases).
type pairKey struct {
	slot uint16
	db   uint64
}

// pairEntry is a pair's state with its own lock. dropped is set under mu so a
// caller that fetched the entry just before Drop retries on a fresh one
// instead of mutating state the map no longer holds.
type pairEntry struct {
	mu      sync.Mutex
	dropped bool
	state   Pair

	// viewMu guards view and gone. It is never held across I/O, so a read of the pair waits
	// at most a struct copy. Two paths keep mu across an fsync: an RF 2 and larger write (its
	// AppendFrom runs under With) and a copy apply (replicate_apply.go). An RF 1 owner write does not.
	viewMu sync.Mutex
	// view is state as of the last time a holder of mu released it.
	view Pair
	// gone is dropped, readable without mu.
	gone bool

	// The owner's write pipeline (pair-lock-across-fsync spec 3.1). All of it is guarded by mu.
	//
	// pending is the owner entries appended to the log and not yet applied, in position order,
	// which is also cursor order. failed is the pipeline's own mark that an entry's sync or apply
	// failed, so later entries are not applied; it is not the pair flag, which a refused append
	// also sets. waiters counts callers of With, WithHeld, Snapshot and Drop waiting for the list
	// to empty, and drainTurn the callers entitled to run before the next append (the turn rule
	// that bounds both a waiter and a writer behind a stream of waiters). cond is on mu and is
	// made with the first pending entry.
	pending   []PendingWrite
	failed    bool
	waiters   int
	drainTurn int
	cond      *sync.Cond
}

// PendingWrite is one owner write that is in the log and not yet applied.
type PendingWrite struct {
	// Pos is the position the write was assigned and Cursor its node-local log sequence.
	Pos    Position
	Cursor uint64
	// DocKey is the hash of the document the write changes and DedupeKey the hash of its
	// dedupe key; a later append that shares either waits for the list to drain. A zero DocKey
	// means "not one document" and conflicts with everything. A hash collision only costs a drain.
	DocKey, DedupeKey uint64
	// Work is the writer's own apply state; the cluster package never looks inside it.
	Work any
}

// drainLocked blocks, with mu held, until the pending list is empty and this caller holds a
// turn, so the section that follows sees a pair with nothing in flight. It returns at once when
// the list is empty. The caller must re-check dropped afterwards: mu is released while it waits.
func (e *pairEntry) drainLocked() {
	if len(e.pending) == 0 {
		return
	}
	e.waiters++
	for len(e.pending) > 0 || e.drainTurn == 0 {
		e.cond.Wait()
	}
	e.waiters--
	e.drainTurn--
	if e.drainTurn == 0 {
		// The writers held back by the turns can go once this section ends.
		e.cond.Broadcast()
	}
}

// emptied runs when the last pending entry leaves: the pipeline's failure mark ends with it, and
// every counted drain caller is given a turn before the next append.
func (e *pairEntry) emptied() {
	e.failed = false
	e.drainTurn = e.waiters
	e.cond.Broadcast()
}

// publish makes state what ReadView returns. It is called with mu held, after fn ran.
func (e *pairEntry) publish() {
	e.viewMu.Lock()
	e.view = e.state
	e.viewMu.Unlock()
}

// PairSet holds the pair states of one node. The zero value is ready to use.
// The map lock is held only to find or create an entry; the work itself runs
// under that pair's own lock, so two pairs never wait on each other. It starts
// no goroutine and opens no file.
type PairSet struct {
	mu    sync.RWMutex
	pairs map[pairKey]*pairEntry
	// onDrop, when set, is told of every dropped pair after the set's lock is released.
	onDrop func(slot uint16, db uint64)
}

// OnDrop registers fn to run after Drop has forgotten a pair, with no lock of the set held,
// so a collaborator that keeps per-pair state (the dedupe sweeper's cursor) can forget it
// too. The node registers it after Raft has started, so the field is guarded by the set's
// lock like the map it belongs to.
func (p *PairSet) OnDrop(fn func(slot uint16, db uint64)) {
	p.mu.Lock()
	p.onDrop = fn
	p.mu.Unlock()
}

// With runs fn on the pair's state while holding that pair's lock, creating
// the state on first use. fn must not call back into the same PairSet for the
// same pair; it is where callers keep their critical section.
func (p *PairSet) With(slot uint16, db uint64, fn func(*Pair)) {
	key := pairKey{slot: slot, db: db}
	for {
		entry := p.entry(key)
		entry.mu.Lock()
		entry.drainLocked()
		if entry.dropped {
			entry.mu.Unlock()
			continue
		}
		fn(&entry.state)
		entry.publish()
		entry.mu.Unlock()
		return
	}
}

// WithHeld runs fn on the pair's state under that pair's lock only when this node already
// holds the pair, and reports whether it did. Unlike With it never creates an entry, so a
// caller that cycles the held pairs (the schema bump, conv 20 step 6) cannot grow the map
// or resurrect a pair that Drop just forgot.
func (p *PairSet) WithHeld(slot uint16, db uint64, fn func(*Pair)) bool {
	p.mu.RLock()
	entry := p.pairs[pairKey{slot: slot, db: db}]
	p.mu.RUnlock()
	if entry == nil {
		return false
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	entry.drainLocked()
	if entry.dropped {
		return false
	}
	fn(&entry.state)
	entry.publish()
	return true
}

// HeldSlots lists, ascending, the slots of database db that this node holds a pair for. It
// is one pass over the pair map under the map's read lock and takes no pair lock.
func (p *PairSet) HeldSlots(db uint64) []uint16 {
	p.mu.RLock()
	var slots []uint16
	for key := range p.pairs {
		if key.db == db {
			slots = append(slots, key.slot)
		}
	}
	p.mu.RUnlock()
	slices.Sort(slots)
	return slots
}

// Held calls fn for every pair this node holds, in no order, after one pass over the map. fn
// runs with no lock of the set held, so it may use the set.
func (p *PairSet) Held(fn func(slot uint16, db uint64)) {
	p.mu.RLock()
	keys := make([]pairKey, 0, len(p.pairs))
	for key := range p.pairs {
		keys = append(keys, key)
	}
	p.mu.RUnlock()
	for _, key := range keys {
		fn(key.slot, key.db)
	}
}

// Snapshot returns a copy of the pair's state, or the zero Pair when this node
// holds none. It never creates an entry, so reading cannot grow the map.
func (p *PairSet) Snapshot(slot uint16, db uint64) Pair {
	p.mu.RLock()
	entry := p.pairs[pairKey{slot: slot, db: db}]
	p.mu.RUnlock()
	if entry == nil {
		return Pair{}
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	entry.drainLocked()
	if entry.dropped {
		return Pair{}
	}
	return entry.state
}

// ReadView is Snapshot for a read's admission: it returns the state as of the last
// completed With, and does not wait for a With that is running. An RF 1 owner write
// releases the pair's lock across its WAL fsync, so Snapshot waits only for the pending
// writes to apply; only a locked-path write, RF 2 and larger, holds it through the flush.
// A read has no use for an unacknowledged state, and an acknowledged one published it
// first. Use Snapshot where the answer must include a running step.
func (p *PairSet) ReadView(slot uint16, db uint64) Pair {
	p.mu.RLock()
	entry := p.pairs[pairKey{slot: slot, db: db}]
	p.mu.RUnlock()
	if entry == nil {
		return Pair{}
	}
	entry.viewMu.Lock()
	defer entry.viewMu.Unlock()
	if entry.gone {
		return Pair{}
	}
	return entry.view
}

// Drop forgets the pair when this node stops owning or copying it, so the map
// does not keep a state for every pair it ever held. It waits for a running
// With on that pair to finish and for the owner's pending writes to apply, and only
// then removes the entry from the map: a With between the removal and the lock would
// make a fresh zero Pair that nothing marks dropped. The lock order is the entry's
// mu, then the map's; no path takes the map's lock and then an entry's mu. A later
// With starts from the zero Pair.
func (p *PairSet) Drop(slot uint16, db uint64) {
	key := pairKey{slot: slot, db: db}
	p.mu.RLock()
	entry := p.pairs[key]
	p.mu.RUnlock()
	if entry == nil {
		return
	}
	entry.mu.Lock()
	entry.drainLocked()
	if entry.dropped {
		entry.mu.Unlock()
		return
	}
	p.mu.Lock()
	if p.pairs[key] == entry {
		delete(p.pairs, key)
	}
	onDrop := p.onDrop
	p.mu.Unlock()
	entry.dropped = true
	entry.viewMu.Lock()
	entry.gone = true
	entry.viewMu.Unlock()
	entry.mu.Unlock()
	if onDrop != nil {
		onDrop(slot, db)
	}
}

func (p *PairSet) entry(key pairKey) *pairEntry {
	p.mu.RLock()
	entry := p.pairs[key]
	p.mu.RUnlock()
	if entry != nil {
		return entry
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if entry = p.pairs[key]; entry == nil {
		if p.pairs == nil {
			p.pairs = make(map[pairKey]*pairEntry)
		}
		entry = &pairEntry{}
		p.pairs[key] = entry
	}
	return entry
}

func (p *PairSet) pairCount() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.pairs)
}
