package cluster

import "sync"

// The owner's write pipeline over one pair (pair-lock-across-fsync spec 3). An RF 1 write is two
// sections of the pair's lock around its WAL fsync: the first assigns the position, appends
// unsynced and pushes a PendingWrite; the second, after the fsync, applies the durable entries
// at the head of the list in position order. Every other user of the lock (With, WithHeld,
// Snapshot, Drop) first waits for the list to be empty, so it sees the states it could see
// before the pipeline existed.
//
// Lock order, which no path may invert: a pair entry's mu, then the map lock of its PairSet.
// The second section needs only mu, the WAL's sync lock, the replicator's lock and the
// reconciler's lock; none of those is held by a caller of With, WithHeld, Snapshot or Drop
// while it waits for the list, so a drain wait ends after at most the pending cap of applies
// and one group commit.

// Pipeline is the pending list of one pair, valid only inside a WithPipeline or
// PipelineRef.With callback, that is with the pair's lock held.
type Pipeline struct {
	e *pairEntry
}

// PipelineRef names the pair entry an owner write's first section ran on, so its second
// section locks that entry and never a fresh map lookup: a Drop followed by a re-create
// cannot hand the second section an entry without the pending list.
type PipelineRef struct {
	e *pairEntry
}

// WithPipeline is the owner write's first section: it runs fn on the pair's state under the
// pair's lock like With, but it does not wait for pending writes to drain, and it publishes the
// view on release like With does. fn returns false when it found the entry dropped while it
// waited (WaitTurn, Drain returned false) and did nothing; WithPipeline then runs it again on a
// fresh entry. The returned reference is for the second section.
func (p *PairSet) WithPipeline(slot uint16, db uint64, fn func(*Pair, *Pipeline) bool) *PipelineRef {
	key := pairKey{slot: slot, db: db}
	for {
		entry := p.entry(key)
		entry.mu.Lock()
		if entry.dropped {
			entry.mu.Unlock()
			continue
		}
		if !fn(&entry.state, &Pipeline{e: entry}) {
			entry.mu.Unlock()
			continue
		}
		entry.publish()
		entry.mu.Unlock()
		return &PipelineRef{e: entry}
	}
}

// With is the owner write's second section: fn runs on the entry the first section used,
// under its lock, and the view is published on release, which is what makes an applied write
// readable before its answer. The entry may have been dropped meanwhile only after the list
// emptied, so fn must not assume a pending entry of its own is still there.
func (r *PipelineRef) With(fn func(*Pair, *Pipeline)) {
	r.e.mu.Lock()
	defer r.e.mu.Unlock()
	fn(&r.e.state, &Pipeline{e: r.e})
	r.e.publish()
}

// Len is the number of pending writes.
func (pl *Pipeline) Len() int { return len(pl.e.pending) }

// Epoch is the epoch of the newest pending write, 0 when none is pending.
func (pl *Pipeline) Epoch() uint64 {
	if len(pl.e.pending) == 0 {
		return 0
	}
	return pl.e.pending[len(pl.e.pending)-1].Pos.Epoch
}

// Conflicts reports whether a pending write may change what a write of the document with hash doc
// and the dedupe key with hash dedupe would read. A pending write with no document key
// conflicts with everything.
func (pl *Pipeline) Conflicts(doc, dedupe uint64) bool {
	for i := range pl.e.pending {
		w := &pl.e.pending[i]
		if w.DocKey == 0 || doc == 0 || w.DocKey == doc || w.DedupeKey == dedupe {
			return true
		}
	}
	return false
}

// WaitTurn blocks, with the lock held and released while it waits, until a new write may be
// appended: no drain caller is entitled to run first, none is waiting on a list that still
// holds entries, and the list is shorter than max. It returns false when the entry was dropped
// meanwhile; the caller has decided nothing yet and returns false from its WithPipeline callback.
func (pl *Pipeline) WaitTurn(max int) bool {
	e := pl.e
	max = max0(max)
	for !e.dropped && (e.drainTurn > 0 || (e.waiters > 0 && len(e.pending) > 0) || len(e.pending) >= max) {
		e.cond.Wait()
	}
	return !e.dropped
}

// Drain blocks like the entry of With does: until the list is empty and this caller holds a
// turn. A write whose first section cannot overlap the pending ones calls it, then decides
// again from the start, because the lock was released while it waited. It returns false when
// the entry was dropped meanwhile.
func (pl *Pipeline) Drain() bool {
	pl.e.drainLocked()
	return !pl.e.dropped
}

// Push appends a write that is in the log, after the checks that assigned it, in position order.
func (pl *Pipeline) Push(w PendingWrite) {
	e := pl.e
	if e.cond == nil {
		e.cond = sync.NewCond(&e.mu)
	}
	e.pending = append(e.pending, w)
}

// Head is the oldest pending write, or nil. The pointer is valid until PopHead.
func (pl *Pipeline) Head() *PendingWrite {
	if len(pl.e.pending) == 0 {
		return nil
	}
	return &pl.e.pending[0]
}

// PopHead removes the oldest pending write. When it was the last, the failure mark ends and the
// drain callers counted so far are given their turn.
func (pl *Pipeline) PopHead() {
	e := pl.e
	e.pending[0] = PendingWrite{}
	e.pending = e.pending[1:]
	if len(e.pending) == 0 {
		e.pending = nil
		e.emptied()
		return
	}
	// A writer held back by the cap may go.
	e.cond.Broadcast()
}

// Failed reports whether an entry of this pipeline failed its sync or apply since the list was
// last empty; the entries behind it are not applied.
func (pl *Pipeline) Failed() bool { return pl.e.failed }

// Fail marks the pipeline failed.
func (pl *Pipeline) Fail() { pl.e.failed = true }

// max0 keeps a cap of zero or less from meaning "never append": the pending list needs room for one.
func max0(limit int) int { return max(limit, 1) }
