package wire

import (
	"bytes"
	"container/heap"
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The receiver side of the seed channel (seeding sections 3 to 5 and 7, conventions 6.1 and 12).
//
// # What a pull does
//
// A pair whose record says seeding is pulled by one worker, from the owner first. The
// order is fixed by the record contracts of conventions 12:
//
//  1. The attempt is already on disk (the Reconciler wrote it before the first byte was
//     asked for). In phase reset the worker deletes the pair's range and, when it deleted a
//     key, syncs the store, because the record that follows (an empty cursor in phase
//     snapshot) says the range is empty and a crash must not be able to lose the delete. A
//     range that held no key is not synced: there is no delete to lose, and the record
//     that follows is a synced commit.
//  2. SEED_REGISTER names the seed_id of the record. With a durable cursor it sets mask
//     bit 0 and sends the cursor, which is a resume: the source answers with the pinned
//     cut and resume=1 within seed_resume_hold_seconds, or with SEED_RESTART 1 when the
//     pin is gone. A fresh attempt sends mask 0 and no cursor.
//  3. Each SEED_ACK states the applied position (the cut: nothing after it is durable
//     here) and the absolute free credit, and its reply is the next SEED_CHUNK. Chunks are
//     applied one at a time, so the credit is the whole window at each ack. The ack that
//     follows the final chunk is answered with nothing.
//  4. A chunk goes through Reconciler.ApplySeedChunk: its entries and the record's new
//     cursor are one batch, so the cursor never leads its data and a crash leaves both or
//     neither (the argument is at the end of this comment).
//  5. After the final chunk the live writes buffered during the snapshot are drained in
//     position order, each in the batch that carries the seed record's step, and a hole in
//     them is filled from the pinned source by catch-up. The attempt ends when the buffer is
//     empty behind a catch-up that answers current: BufferDrained at the source's tail, then
//     SyncDone, whose ready record is committed with a sync. That one sync makes the attempt's
//     data, its derived expiry entries and every earlier record durable with the ready record
//     (vouched cut I-4), so the earlier steps commit without one (seedStepCommitsUnsynced, and a
//     final chunk syncs only past seed_sync_bytes like any other). Only after it does the pair
//     serve, count as a copy, or answer POSITION_QUERY as ready.
//
// # Why the cursor cannot lead the data
//
// ApplySeedChunk stages the entries and the record into ONE storage batch and commits it
// once. Badger commits a batch atomically, so after a crash either the chunk's entries and
// the new cursor are both present or neither is. A resume therefore asks the source to
// continue after a key whose entries are all on disk, and never after a key that is not. A
// chunk the machine does not want (a duplicate) commits nothing. The reset is the one step
// that is not in a batch with a record, and it is covered by the order in point 1: the
// record stays in phase reset until the delete is synced.
//
// # Cost
//
// A queued or running pull is one map entry (about 120 bytes) keyed by (slot, database).
// There are SlotsInFlight goroutines in all, started once, however many pairs are queued;
// no file descriptor, snapshot or timer is held per pair beyond the one running call. A
// membership change queues the pairs the Reconciler returns, which is O(slots x databases)
// and was already the Reconciler's cost. Nothing here is per bucket or per store.

// PullerConfig is what a Puller reads from its node.
type PullerConfig struct {
	// Self is this node's id.
	Self string
	// Recon holds the pair records the pull advances.
	Recon *Reconciler
	// KV is the node's store; the pull deletes a reset range through it and syncs it.
	KV storage.KV
	// Options are the replication.seed_* bounds.
	Options SeedOptions
	// Call sends one message on the seed channel to source and returns its reply, nil for
	// an acknowledgement with no message (Node.SeedCall).
	Call func(ctx context.Context, source string, msg transport.Message) (*transport.Message, error)
	// Committed and StateSeq are this node's committed epoch and state sequence.
	Committed, StateSeq func() uint64
	// Sources lists the nodes that may serve the pair, best first: the owner, then the
	// other copies. It never contains this node.
	Sources func(slot uint16, db uint64) []string
	// OnReady runs once when a pair has become ready (the sweeps rerun, the node recounts).
	OnReady func(slot uint16, db uint64)
	// DeriveExpiry builds the node-local expiry entries of the seeded documents and queue
	// elements before the pair becomes ready (expiry spec 3.4): a snapshot copies raw keys and the
	// queue is not among them. An error ends the attempt with a retry. Nil derives nothing.
	DeriveExpiry func(ctx context.Context, slot uint16, db uint64) error
	// OnError receives an error the pull has no caller for; nil drops it.
	OnError func(error)
	// Budget is the node's one reclaim budget: the reset of a range is charged to it, key by
	// key, beside the sweeps. Nil makes the puller its own at the default rate.
	Budget *reclaimBudget
	// Buffers is the node's live-write buffer budget. Nil means the pull buffers nothing.
	Buffers *seedBuffers
	// Pace is the node's seed byte rate. Nil paces nothing.
	Pace *seedPacer
	// DiskUsedPct reads the percentage of this node's data filesystem in use. Nil never
	// pauses. An error does not pause: it is reported and the pull goes on.
	DiskUsedPct func() (int, error)
	// Deficits ranks the queue. Nil keeps arrival order; otherwise it returns a function bound to
	// one read of the committed state, so a re-rank reads the state once.
	Deficits func() func(slot uint16, db uint64) int
	// Applier writes a buffered live write into the pair, the way a copy applies a write it
	// receives; the drain uses it and never Fill alone (seeding section 2.3, step 1).
	Applier Applier
	// Replicator is the node's replicator; a gap in the buffer asks it to catch up (step 2).
	Replicator *Replicator
	// Diverged says this node's cluster state is known to disagree with the cluster's: the
	// workers then start no pair (BUG-0781). Nil means the state is trusted.
	Diverged func() bool
	// DedupeWindow is the window a drained write's dedupe entry is recorded under.
	DedupeWindow machine.WindowConfig
}

// Pull states a status row can show (seeding section 11).
const (
	// PullQueued means the pair waits for a free worker.
	PullQueued = "queued"
	// PullPulling means a worker is on the pair.
	PullPulling = "pulling"
	// PullStalled means no progress for seed_idle_seconds. It is a status, not an error
	// code (owner decision 2026-11-01): the Detail says why and what to run.
	PullStalled = "stalled"
)

// PullStatus is the status row of one pair that is not yet ready.
type PullStatus struct {
	State string
	// Detail is the last reason the pull could not go on, with the fix.
	Detail string
	// Since is the anchor of the idle clock: the last applied chunk, else the first time a
	// worker picked the pair up, else (still queued) when it was queued.
	Since time.Time
	// Source is the node the pull is using or last used.
	Source string
	// Bytes and Entries are what this process has applied of the pair since it picked it up
	// (a fresh attempt on a new cut starts them again), and Idle is the time since the last
	// applied chunk, 0 when none has been applied.
	Bytes, Entries int64
	Idle           time.Duration
}

// Puller pulls seeding pairs from their sources.
type Puller struct {
	cfg PullerConfig
	opt SeedOptions

	mu      sync.Mutex
	entries map[reconcileKey]*pullEntry
	pending *pendingQueue
	// rankedAt is when the queue was last ranked with a pair in it; zero until then. reranking is set while one worker
	// builds the deficits outside the lock, so no second worker builds them too.
	rankedAt  time.Time
	reranking bool
	// retries orders the pairs waiting out a backoff by the time they may run again, so
	// finding the due ones costs O(log n) however many pairs a BUSY source has delayed.
	retries retryHeap
	closed  bool
	wake    chan struct{}
	now     func() time.Time
	// counters is what /metrics reads; its zero value works.
	counters seedCounters
}

type pullEntry struct {
	status  PullStatus
	running bool
	again   bool // Queue was called while the pair was running: look at the record once more
	at      time.Time
	delayed bool
	// redirected and rotation are the source choice of the running attempt.
	redirected bool
	rotation   int
	failures   int
	// bytes and entries are the progress of the current attempt; started is when a worker
	// first took the pair, lastChunk when a chunk was last applied.
	bytes, entries int64
	started        time.Time
	lastChunk      time.Time
}

// anchor is where the idle clock starts: the last applied chunk, else the pickup.
func (e *pullEntry) anchor() time.Time {
	if !e.lastChunk.IsZero() {
		return e.lastChunk
	}
	return e.started
}

// NewPuller returns a Puller over cfg. It starts nothing; call Run.
func NewPuller(cfg PullerConfig) *Puller {
	opt := cfg.Options.withDefaults()
	if cfg.Budget == nil {
		cfg.Budget = newReclaimBudget(reclaimBudgetConfig{})
	}
	return &Puller{cfg: cfg, opt: opt, entries: map[reconcileKey]*pullEntry{}, pending: newPendingQueue(cfg.Deficits != nil), wake: make(chan struct{}, 1), now: opt.Now}
}

// Queue asks for the pair to be pulled. It is idempotent: a pair already queued or running
// is left where it is (a running one looks at its record again when it ends).
func (p *Puller) Queue(slot uint16, db uint64) {
	key := reconcileKey{slot, db}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	if entry := p.entries[key]; entry != nil {
		entry.again = true
		return
	}
	p.entries[key] = &pullEntry{status: PullStatus{State: PullQueued, Since: p.now()}}
	p.pending.Add(key)
	p.signal()
}

func (p *Puller) signal() {
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

// Status is the status row of a pair the pull still has, and false for a pair it has
// finished with or never had.
func (p *Puller) Status(slot uint16, db uint64) (PullStatus, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	entry := p.entries[reconcileKey{slot, db}]
	if entry == nil {
		return PullStatus{}, false
	}
	row := entry.status
	row.Bytes, row.Entries = entry.bytes, entry.entries
	now := p.now()
	if !entry.lastChunk.IsZero() {
		row.Idle = now.Sub(entry.lastChunk)
	}
	if !entry.started.IsZero() {
		row.Since = entry.anchor()
		if row.State != PullStalled && now.Sub(row.Since) > p.opt.Idle {
			row.State = PullStalled
		}
	}
	return row, true
}

// Pending is how many pairs the pull has queued, running or waiting to retry.
func (p *Puller) Pending() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.entries)
}

// Close stops accepting pairs. The workers end with the context given to Run.
func (p *Puller) Close() {
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
	p.signal()
}

// Run starts SlotsInFlight workers and returns when ctx ends and every worker has.
func (p *Puller) Run(ctx context.Context) {
	var workers sync.WaitGroup
	for range p.opt.SlotsInFlight {
		workers.Add(1)
		go func() {
			defer workers.Done()
			p.work(ctx)
		}()
	}
	workers.Wait()
}

// rerankDue reports whether the queue may be re-ranked at now: it has not been ranked with pairs in it yet, or
// rerankMinInterval has passed since it last was. The caller holds p.mu.
func (p *Puller) rerankDue(now time.Time) bool {
	return p.rankedAt.IsZero() || now.Sub(p.rankedAt) >= rerankMinInterval
}

// divergedPullPoll is how long an idle worker of a diverged node sleeps before it looks at the
// flag again. The flag never clears in a running process, so this only bounds the wake-ups.
const divergedPullPoll = time.Second

// next takes the next pair that may run now, or says how long to wait.
func (p *Puller) next() (key reconcileKey, wait time.Duration, ok bool) {
	if p.cfg.Diverged != nil && p.cfg.Diverged() {
		// A pull ends in a ready record and may start a reset of the range, both judged against
		// a state that is frozen. The pair stays queued; a woken worker looks again.
		return reconcileKey{}, divergedPullPoll, false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.now()
	for len(p.retries) > 0 && !p.retries[0].at.After(now) {
		due := heap.Pop(&p.retries).(retryItem)
		if entry := p.entries[due.key]; entry != nil && entry.delayed && entry.at.Equal(due.at) {
			entry.delayed = false
			p.pending.Add(due.key)
		}
	}
	if p.cfg.Deficits != nil && !p.reranking && p.rerankDue(now) {
		seq := p.cfg.StateSeq()
		if p.pending.Stale(seq) {
			// The state read and the slot table behind Deficits are the expensive part; the other workers keep taking pairs meanwhile.
			p.reranking = true
			p.mu.Unlock()
			deficits := p.cfg.Deficits()
			p.mu.Lock()
			p.pending.Rerank(seq, deficits)
			p.reranking = false
			if p.pending.Len() > 0 {
				p.rankedAt = now
			}
		}
	}
	for {
		var queued bool
		key, queued = p.pending.Take()
		if !queued {
			break
		}
		entry := p.entries[key]
		if entry == nil || entry.running {
			continue
		}
		entry.running = true
		entry.again = false
		if entry.started.IsZero() {
			entry.started = now
		}
		entry.status.State = PullPulling
		return key, 0, true
	}
	wait = time.Hour
	if len(p.retries) > 0 {
		wait = max(p.retries[0].at.Sub(now), time.Millisecond)
	}
	return reconcileKey{}, wait, false
}

type retryItem struct {
	key reconcileKey
	at  time.Time
}

// retryHeap is a min-heap of retryItem by time.
type retryHeap []retryItem

func (h retryHeap) Len() int           { return len(h) }
func (h retryHeap) Less(i, j int) bool { return h[i].at.Before(h[j].at) }
func (h retryHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *retryHeap) Push(due any)      { *h = append(*h, due.(retryItem)) }
func (h *retryHeap) Pop() any {
	old := *h
	last := old[len(old)-1]
	*h = old[:len(old)-1]
	return last
}

func (p *Puller) work(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		key, wait, ok := p.next()
		if !ok {
			timer := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-p.wake:
			case <-timer.C:
			}
			timer.Stop()
			continue
		}
		retry, finished := p.pull(ctx, key)
		p.finish(key, retry, finished)
	}
}

// finish records the end of one run: the pair is done, or it waits retry before the next.
func (p *Puller) finish(key reconcileKey, retry time.Duration, finished bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	entry := p.entries[key]
	if entry == nil {
		return
	}
	entry.running = false
	switch {
	case !finished:
		entry.delayed, entry.at = true, p.now().Add(retry)
		heap.Push(&p.retries, retryItem{key: key, at: entry.at})
		if entry.status.State != PullStalled {
			entry.status.State = PullQueued
		}
		p.signal()
	case entry.again:
		entry.status.State = PullQueued
		p.pending.Add(key)
		p.signal()
	default:
		delete(p.entries, key)
	}
}

// note records why a pull is waiting, and marks it stalled when it made no progress for
// seed_idle_seconds.
func (p *Puller) note(key reconcileKey, source, detail string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	entry := p.entries[key]
	if entry == nil {
		return
	}
	entry.status.Detail, entry.status.Source = detail, source
	if p.now().Sub(entry.anchor()) > p.opt.Idle {
		// Every way a pull waits ends here, so a pull that heard nothing useful for
		// seed_idle_seconds asks the next legal source, whatever the wait was.
		entry.status.State = PullStalled
		p.bump(entry)
	}
}

// bump moves the pull on to its next source. The caller holds p.mu.
func (p *Puller) bump(entry *pullEntry) {
	entry.rotation++
	entry.redirected = false
	p.counters.retries.Add(1)
}

// sourced says a source accepted the registration. It never touches the idle clock: a source
// that accepts and then says nothing must not look like progress.
func (p *Puller) sourced(key reconcileKey, source string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if entry := p.entries[key]; entry != nil {
		entry.status.Source = source
		if entry.status.State != PullStalled {
			entry.status.State = PullPulling
			entry.status.Detail = ""
		}
	}
}

// progressed says a chunk of size bytes and entries entries was applied: a stall is over.
func (p *Puller) progressed(key reconcileKey, source string, size int64, entries int) {
	p.counters.bytes.Add(uint64(size))
	p.mu.Lock()
	defer p.mu.Unlock()
	if entry := p.entries[key]; entry != nil {
		entry.status.State, entry.status.Detail, entry.status.Source = PullPulling, "", source
		entry.failures = 0
		entry.bytes += size
		entry.entries += int64(entries)
		entry.lastChunk = p.now()
	}
}

func (p *Puller) report(err error) {
	if p.cfg.OnError != nil && err != nil {
		p.cfg.OnError(err)
	}
}

// diskPaused reports whether this node's disk is at or above its high-water mark, and the text
// the pause shows. A failed read is reported and does not pause: one bad statfs must not stop
// re-replication (owner question 3, 2026-11-04). The source side refuses on that error; the
// receiver does not.
func (p *Puller) diskPaused(key reconcileKey) (string, bool) {
	if p.cfg.DiskUsedPct == nil {
		return "", false
	}
	used, err := p.cfg.DiskUsedPct()
	if err != nil {
		p.report(fmt.Errorf("reading the disk usage of %s before the pull of slot %d of database %d, which goes on unchecked: %w", p.cfg.Self, key.slot, key.db, err))
		return "", false
	}
	if used < p.opt.DiskHighWaterPct {
		return "", false
	}
	return fmt.Sprintf("the disk of %s is %d%% full, at or above seed_disk_high_water_pct %d. The pull of slot %d of database %d is paused and keeps its data and its record. Free space on this node or raise replication.seed_disk_high_water_pct, and it resumes by itself", p.cfg.Self, used, p.opt.DiskHighWaterPct, key.slot, key.db), true
}

// hold shows why a pull is paused. It neither stalls the pull nor moves it to another source:
// the pause is the disk's, not the source's, so the idle clock and the rotation stay as they are.
func (p *Puller) hold(key reconcileKey, source, detail string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if entry := p.entries[key]; entry != nil {
		entry.status.Detail, entry.status.Source = detail, source
	}
}

// seedConfig is the machine's configuration for the committed epoch now.
func (p *Puller) seedConfig() machine.SeedConfig {
	return machine.SeedConfig{
		SyncBytes: p.opt.SyncBytes, BufferBytes: p.opt.BufferBytes, WindowBytes: int64(p.opt.WindowBytes), Epoch: p.cfg.Committed(),
	}
}

func (p *Puller) header(key reconcileKey) transport.Envelope {
	return transport.Envelope{Version: transport.EnvelopeVersion, Epoch: p.cfg.Committed(), StateSeq: p.cfg.StateSeq(), Slot: key.slot, DBID: key.db}
}

// diskPauseRetry is how long a paused pull waits before it reads the disk again.
const diskPauseRetry = 5 * time.Second

// pullWait is how a pull step ends when it cannot go on now.
type pullWait struct {
	after  time.Duration
	detail string
	// stop ends the pull for good: the pair is gone, or is no longer seeding.
	stop bool
	// accepted says a source accepted the registration and the attempt still did not register
	// (its range reset failed): the wait is not for a source.
	accepted bool
}

// pull runs the attempts of one pair until it is ready, gone, or must wait. It returns how
// long to wait and false, or true when there is nothing more to do.
func (p *Puller) pull(ctx context.Context, key reconcileKey) (time.Duration, bool) {
	restarts := 0
	for {
		if ctx.Err() != nil {
			return p.opt.RetryBackoff, false
		}
		step := p.attempt(ctx, key)
		if step.stop {
			return 0, true
		}
		if step.after == 0 {
			// A restart: the record is a fresh attempt. Do not spin on a source that keeps
			// restarting us.
			restarts++
			p.counters.retries.Add(1)
			if restarts > 3 {
				return p.opt.RetryBackoff, false
			}
			continue
		}
		return step.after, false
	}
}

// attempt runs one attempt as far as it goes.
func (p *Puller) attempt(ctx context.Context, key reconcileKey) pullWait {
	if p.cfg.Recon.leftSeed(key.slot, key.db) {
		return p.dropLeft(ctx, key)
	}
	record, found := p.cfg.Recon.Record(key.slot, key.db)
	if !found || record.State != machine.SeedSeeding {
		return pullWait{stop: true}
	}
	if record.Phase == machine.PhaseNone {
		// The drain finished (PhaseNone) and the pair is not ready yet. Two cases reach here:
		// an earlier finishSeed of this process failed after the drained record was installed
		// (the expiry derivation, the ordinal scan or the ready commit), and the retry reads the
		// record from memory; or a restart reloaded a drained record, which the drained step
		// commits without a sync, so it survives a cut only when a later synced commit of the
		// store (another pair's ready record, a reconcile pass) vouched it. The range is
		// complete, so there is nothing to register and no source to ask: finishSeed's synced
		// ready commit alone makes the pair ready (machine seed.go, contract 2).
		return p.finishSeed(ctx, key, record.Source, nil, 0)
	}
	if detail, paused := p.diskPaused(key); paused {
		p.hold(key, record.Source, detail)
		return pullWait{after: diskPauseRetry, detail: detail}
	}
	if record.Phase == machine.PhaseReset {
		p.restartCounts(key)
	}
	source, ok := p.chooseSource(key, record)
	if !ok {
		detail := fmt.Sprintf("slot %d of database %d has no node to pull from: no other copy is in its copy set. Run 'nodes' to see which members are down", key.slot, key.db)
		p.note(key, "", detail)
		p.noteKept(key, record)
		return pullWait{after: p.opt.RetryBackoff, detail: detail}
	}
	accept, step, registered := p.register(ctx, key, source, record)
	if !registered {
		if !step.accepted {
			p.noteKept(key, record)
		}
		return step
	}
	if p.cfg.Recon.leftSeed(key.slot, key.db) {
		// The pair left the copy set while the registration was in flight, and the accept landed
		// on the attempt the pass had voided. No chunk is asked for: the next attempt drops it.
		return pullWait{}
	}
	p.openPace(key)
	defer p.closePace(key)
	buffer := p.openBuffer(key)
	defer p.closeBuffer(key)
	buffer.SetPhase(mirrorSnapshot)
	p.sourced(key, source)
	return p.stream(ctx, key, source, accept, buffer)
}

// keptDetail is added to the status row of an attempt that owes its range reset, has no
// source that accepted it yet, and whose range holds keys: a pull deletes the range only once
// a source accepts (Puller.register), so until then the node keeps what it holds. The one
// other thing that deletes it is the pair leaving this node's copy set (dropLeftSeed), which
// asks no source: the node no longer holds the pair.
const keptDetail = " The data this node holds for the slot is kept until another copy accepts the pull or this node leaves the slot's copy set; either deletes it"

// resetDetail is the status row while the reset that a source's accept started is running.
const resetDetail = "%s accepted the pull; this node is deleting the data it holds for the slot before it seeds it again"

// noteKept adds keptDetail to the pair's status row when record is an attempt that owes its
// reset, no source has accepted it and the range holds a key. One snapshot and one seek per attempt that did not
// register, which is at most one per retry backoff per waiting pair.
func (p *Puller) noteKept(key reconcileKey, record SeedRecord) {
	if record.Phase != machine.PhaseReset {
		return
	}
	held, err := p.cfg.Recon.rangeHoldsKeys(key)
	if err != nil {
		// The row keeps the reason it has; it only cannot say whether data is kept.
		p.report(fmt.Errorf("slot %d of database %d: the status row of its waiting pull: %w", key.slot, key.db, err))
		return
	}
	if !held {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if entry := p.entries[key]; entry != nil && !strings.HasSuffix(entry.status.Detail, keptDetail) {
		entry.status.Detail += "." + keptDetail
	}
}

// leftSeedDetail is the status row of a pair whose partial seed is being deleted.
const leftSeedDetail = "this node is no longer in the pair's copy set: the part of the seed it had pulled is being deleted"

// dropLeft deletes the range of a seeding pair that left the copy set and forgets its record
// (dropLeftSeed). It runs here, as the pair's one run, and not on the seeding driver: the
// queue lets one worker at a time touch a pair, so no pull of the pair is started beside
// the delete, and a rate-limited delete of a large partial seed holds up one
// worker instead of every reconcile decision on the node. The run ends with the drop only
// when the record is gone; a record the removal kept is looked at again in the same run.
func (p *Puller) dropLeft(ctx context.Context, key reconcileKey) pullWait {
	p.mu.Lock()
	if entry := p.entries[key]; entry != nil {
		entry.status.State, entry.status.Detail, entry.status.Source = PullPulling, leftSeedDetail, ""
	}
	p.mu.Unlock()
	if err := dropLeftSeed(ctx, p.cfg.Recon, key, p.resetRange); err != nil {
		if !errors.Is(err, ErrPairBusy) {
			p.report(err)
		}
		// A busy pair is one a reconcile pass has staged in a batch it is committing: the delete
		// no longer runs on the driver, so a pass can run beside it. That is a wait, not a fault.
		return p.waiting(key, "", err.Error())
	}
	if _, kept := p.cfg.Recon.Record(key.slot, key.db); kept {
		// The removal declined: the record is no longer the attempt the delete ran under. It is a
		// new attempt a pass wrote for a pair that came back, or it is still a left seed. Either
		// way the run looks at it again now; ending here would leave it to the next state change.
		return pullWait{}
	}
	return pullWait{stop: true}
}

// chooseSource picks the source of this attempt: the one the record names while it is still
// a candidate, else the first candidate. A refusal moves the rotation on.
func (p *Puller) chooseSource(key reconcileKey, record SeedRecord) (string, bool) {
	candidates := p.cfg.Sources(key.slot, key.db)
	if len(candidates) == 0 {
		return "", false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	entry := p.entries[key]
	if entry != nil && entry.rotation > 0 {
		return candidates[entry.rotation%len(candidates)], true
	}
	for _, candidate := range candidates {
		if candidate == record.Source {
			return candidate, true
		}
	}
	return candidates[0], true
}

// restartCounts zeroes the progress of a pair whose attempt begins again on a reset range.
func (p *Puller) restartCounts(key reconcileKey) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if entry := p.entries[key]; entry != nil {
		entry.bytes, entry.entries = 0, 0
	}
}

func (p *Puller) rotate(key reconcileKey) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if entry := p.entries[key]; entry != nil {
		p.bump(entry)
	}
}

// register sends SEED_REGISTER and handles every answer but the accept, which it returns.
func (p *Puller) register(ctx context.Context, key reconcileKey, source string, record SeedRecord) (transport.SeedAccept, pullWait, bool) {
	register := transport.SeedRegister{Header: p.header(key), SeedID: record.SeedID, MyPosition: seedPosition(record.Applied)}
	// A record reloaded in the snapshot or buffer phase has a cut on disk, so it resumes; a
	// buffer-phase record is a snapshot on load (machine seed.go, contract 9).
	if (record.Phase == machine.PhaseSnapshot || record.Phase == machine.PhaseBuffer) && len(record.Cursor) != 0 {
		register.Mask, register.Cursor = seedMaskResume, record.Cursor
	}
	payload, err := transport.EncodeSeedRegister(register)
	if err != nil {
		p.report(err)
		return transport.SeedAccept{}, pullWait{after: p.opt.RetryBackoff, detail: err.Error()}, false
	}
	reply, err := p.call(ctx, source, payload)
	if err != nil {
		return transport.SeedAccept{}, p.callFailed(key, source, err), false
	}
	if reply == nil {
		return transport.SeedAccept{}, p.waiting(key, source, fmt.Sprintf("%s answered SEED_REGISTER with nothing. It will be asked again", source)), false
	}
	reply.Env.Slot, reply.Env.DBID = key.slot, key.db
	switch reply.Env.Kind {
	case transport.KindSeedAccept:
		accept, err := transport.DecodeSeedAccept(payloadOf(*reply))
		if err != nil {
			return transport.SeedAccept{}, p.waiting(key, source, fmt.Sprintf("the SEED_ACCEPT of %s is damaged (%v). It will be asked again", source, err)), false
		}
		if reply.Env.Epoch > p.cfg.Committed() {
			// The source has committed an epoch this node has not applied yet, so the cut may be
			// in an epoch this node does not know and the machine would restart the attempt on a
			// fresh seed_id, abandoning the pin. Wait for the commit: the same seed_id registers
			// again and gets the same pinned cut (resume=1).
			return transport.SeedAccept{}, p.waiting(key, source, fmt.Sprintf("%s is at epoch %d and this node at %d. The pull waits for this node to apply it", source, reply.Env.Epoch, p.cfg.Committed())), false
		}
		if record.Phase == machine.PhaseReset {
			// The reset waits for this accept (BUG-0819): the range may hold the last copy of
			// the pair's data (a range `seeding forget` kept), and deleting it before a source
			// is known to serve the pair would leave nothing to pull and nothing kept. It still
			// comes before the attempt is registered, so a crash in between leaves an attempt
			// that owes its reset (machine seed.go, contract 1).
			//
			// The record was read before SEED_REGISTER went out, outside the pair lock. Read it
			// again before the delete: the reset runs only for the attempt that was sent. This
			// narrows the window to the delete itself and does not close it (the rate-limited
			// delete cannot hold the pair lock). What closes it is an invariant: only this pair's
			// own puller run takes a record out of "seeding" (carrySeed on MarkReady, dropLeft),
			// there is one run per pair and one puller per node. A change that adds another way
			// out of "seeding" must give this delete a guard of its own (BUG-0830).
			if !p.owesReset(key, record.SeedID) {
				return transport.SeedAccept{}, pullWait{}, false
			}
			p.note(key, source, fmt.Sprintf(resetDetail, source))
			if err := p.resetRange(ctx, key); err != nil {
				p.report(err)
				step := p.waiting(key, source, err.Error())
				step.accepted = true
				return transport.SeedAccept{}, step, false
			}
		}
		out, err := p.feedAccept(key, record.SeedID, machine.RegisterAccepted{Cut: cluster.Position{Epoch: accept.Cut.Epoch, Seq: accept.Cut.Seq}, WindowBytes: int64(accept.WindowBytes)})
		if errors.Is(err, ErrSeedAttemptReplaced) {
			// The record was replaced while SEED_REGISTER was in flight (BUG-0818): this accept
			// answers the old attempt. The new one is untouched and still owes its reset, so the
			// run starts over on it, as after a restart: no wait, and no chunk asked for here.
			return transport.SeedAccept{}, pullWait{}, false
		}
		if err != nil {
			return transport.SeedAccept{}, p.waiting(key, source, err.Error()), false
		}
		if out.Restart != 0 {
			return transport.SeedAccept{}, pullWait{}, false
		}
		return accept, pullWait{}, true
	case transport.KindSeedRedirect:
		return transport.SeedAccept{}, p.redirected(key, source, reply), false
	case transport.KindSeedRefused:
		return transport.SeedAccept{}, p.refused(key, source, reply), false
	case transport.KindSeedBusy:
		busy, err := transport.DecodeSeedBusy(payloadOf(*reply))
		after := p.opt.RetryBackoff
		if err == nil && busy.RetryAfterMs > 0 {
			after = time.Duration(busy.RetryAfterMs) * time.Millisecond
		}
		return transport.SeedAccept{}, p.waiting(key, source, fmt.Sprintf("%s is serving its most seeds already (SEED_BUSY). The pull asks again in %s", source, after), after), false
	case transport.KindSeedRestart:
		return transport.SeedAccept{}, p.sourceRestarted(key, source, reply), false
	}
	return transport.SeedAccept{}, p.waiting(key, source, fmt.Sprintf("%s answered SEED_REGISTER with kind 0x%02x, which is not a registration answer", source, reply.Env.Kind)), false
}

// owesReset says the pair still holds the seeding attempt seedID in the reset phase, the one
// attempt whose accept may delete the range.
func (p *Puller) owesReset(key reconcileKey, seedID [16]byte) bool {
	current, found := p.cfg.Recon.Record(key.slot, key.db)
	return found && current.State == machine.SeedSeeding && current.SeedID == seedID && current.Phase == machine.PhaseReset
}

func (p *Puller) call(ctx context.Context, source string, payload []byte) (*transport.Message, error) {
	env, err := transport.DecodeEnvelope(payload)
	if err != nil {
		return nil, fmt.Errorf("reading back a seed message to %s: %w", source, err)
	}
	return p.cfg.Call(ctx, source, transport.Message{Env: env, Body: payload[transport.EnvelopeSize:]})
}

// waiting records why the pull waits and returns the wait; the backoff is the default.
func (p *Puller) waiting(key reconcileKey, source, detail string, after ...time.Duration) pullWait {
	p.note(key, source, detail)
	wait := p.opt.RetryBackoff
	if len(after) > 0 {
		wait = after[0]
	}
	return pullWait{after: wait, detail: detail}
}

// callFailed handles a call that did not complete: the source is down or slow, or it is up and
// its pair is still replaying its log after a restart (the deferred-pair gate's refusal). The
// pull keeps its source and its cursor (a resume can still reach the pin) until
// seed_idle_seconds pass without progress; note then says STALLED and moves it to the next
// candidate.
func (p *Puller) callFailed(key reconcileKey, source string, err error) pullWait {
	detail := fmt.Sprintf("cannot reach %s on the seed channel (%v). Run 'nodes' to see whether it is up; the pull keeps trying and will use another copy if there is one", source, err)
	if isPairReplaying(err) {
		detail = fmt.Sprintf("%s is up and replaying its log of slot %d of database %d after a restart, so it gives no seed until that has finished (%v). Nothing needs doing: the pull asks again and will use another copy if there is one", source, key.slot, key.db, err)
	}
	p.mu.Lock()
	if entry := p.entries[key]; entry != nil {
		entry.failures++
	}
	p.mu.Unlock()
	return p.waiting(key, source, detail)
}

// feed runs one data-free event on the pair's seed machine.
func (p *Puller) feed(key reconcileKey, event machine.SeedEvent) (machine.SeedOutcome, error) {
	out, err := p.cfg.Recon.FeedSeed(key.slot, key.db, FeedSeedRequest{Event: event, Cfg: p.seedConfig(), DataDurable: false})
	if err != nil {
		return machine.SeedOutcome{}, fmt.Errorf("recording %T for slot %d of database %d: %w", event, key.slot, key.db, err)
	}
	return out, nil
}

// feedAccept runs the RegisterAccepted of a SEED_REGISTER that was sent for the attempt
// sentFor. The call ran outside the pair lock, so the record may hold another attempt by
// now; the reconciler then refuses the event with ErrSeedAttemptReplaced.
func (p *Puller) feedAccept(key reconcileKey, sentFor [16]byte, accepted machine.RegisterAccepted) (machine.SeedOutcome, error) {
	out, err := p.cfg.Recon.FeedSeed(key.slot, key.db, FeedSeedRequest{Event: accepted, Cfg: p.seedConfig(), Attempt: &sentFor})
	if err != nil {
		return machine.SeedOutcome{}, fmt.Errorf("recording the accepted registration of slot %d of database %d: %w", key.slot, key.db, err)
	}
	return out, nil
}

// redirected follows a SEED_REDIRECT once per attempt.
func (p *Puller) redirected(key reconcileKey, source string, reply *transport.Message) pullWait {
	redirect, err := transport.DecodeSeedRedirect(payloadOf(*reply))
	if err != nil {
		return p.waiting(key, source, fmt.Sprintf("the SEED_REDIRECT of %s is damaged (%v). It will be asked again", source, err))
	}
	p.mu.Lock()
	entry := p.entries[key]
	again := entry != nil && entry.redirected
	if entry != nil {
		entry.redirected = true
	}
	p.mu.Unlock()
	if again || redirect.Source == p.cfg.Self || redirect.Source == "" {
		p.rotate(key)
		return p.waiting(key, source, fmt.Sprintf("%s redirected to %q, and a redirect is followed once. The pull tries the next copy", source, redirect.Source))
	}
	if _, err := p.feed(key, machine.RegisterRefused{Reason: seedRefusedDisk, Redirect: redirect.Source}); err != nil {
		return p.waiting(key, source, err.Error())
	}
	return pullWait{}
}

// refused handles SEED_REFUSED: the machine's own rows decide what a refusal means.
func (p *Puller) refused(key reconcileKey, source string, reply *transport.Message) pullWait {
	refusal, err := transport.DecodeSeedRefused(payloadOf(*reply))
	if err != nil {
		return p.waiting(key, source, fmt.Sprintf("the SEED_REFUSED of %s is damaged (%v). It will be asked again", source, err))
	}
	out, err := p.feed(key, machine.RegisterRefused{Reason: refusal.Reason})
	if err != nil {
		return p.waiting(key, source, err.Error())
	}
	switch refusal.Reason {
	case seedRefusedDropped:
		// The database is gone at the source's epoch. The Reconciler removes the partial
		// range when its own state says the same; the pull has nothing to do.
		p.note(key, source, refusal.Detail)
		return pullWait{after: p.opt.RetryBackoff * 4, detail: refusal.Detail}
	case seedRefusedNotHolder, seedRefusedNotCurrent:
		p.rotate(key)
	}
	if out.Restart != 0 {
		return pullWait{}
	}
	return p.waiting(key, source, fmt.Sprintf("%s refused the seed (reason %d): %s", source, refusal.Reason, refusal.Detail))
}

// sourceRestarted handles SEED_RESTART at registration or at an ack: the attempt starts
// again on a fresh cut with a reset range (seeding section 7).
func (p *Puller) sourceRestarted(key reconcileKey, source string, reply *transport.Message) pullWait {
	restart, err := transport.DecodeSeedRestart(payloadOf(*reply))
	if err != nil {
		return p.waiting(key, source, fmt.Sprintf("the SEED_RESTART of %s is damaged (%v). It will be asked again", source, err))
	}
	if err := p.cfg.Recon.RestartSeed(key.slot, key.db); err != nil {
		return p.waiting(key, source, err.Error())
	}
	if restart.Reason == seedRestartEpochChanged {
		// The source's view of the epoch differs from ours. Give the commit time to arrive.
		return p.waiting(key, source, fmt.Sprintf("%s reports a different epoch than this node (%d). The attempt restarts when both agree", source, p.cfg.Committed()))
	}
	return pullWait{}
}

// openPace counts the pair's pull among the pulls the node's seed byte rate is divided over.
func (p *Puller) openPace(key reconcileKey) {
	if p.cfg.Pace != nil {
		p.cfg.Pace.Begin(key.slot, key.db)
	}
}

// closePace ends the pair's pull's share of the rate.
func (p *Puller) closePace(key reconcileKey) {
	if p.cfg.Pace != nil {
		p.cfg.Pace.End(key.slot, key.db)
	}
}

// openBuffer opens the live-write buffer of an attempt. With no node budget the attempt gets a
// private buffer that no Offer reaches, so the stream never has to check for nil.
func (p *Puller) openBuffer(key reconcileKey) *seedAttemptBuffer {
	if p.cfg.Buffers == nil {
		return &seedAttemptBuffer{frames: make(map[cluster.Position]bufferedFrame)}
	}
	return p.cfg.Buffers.Begin(key.slot, key.db)
}

// closeBuffer ends the buffer of an attempt and drops whatever it still holds.
func (p *Puller) closeBuffer(key reconcileKey) {
	if p.cfg.Buffers != nil {
		p.cfg.Buffers.End(key.slot, key.db)
	}
}

// stream runs the ack and chunk loop of an accepted registration.
func (p *Puller) stream(ctx context.Context, key reconcileKey, source string, accept transport.SeedAccept, buffer *seedAttemptBuffer) pullWait {
	sequence := transport.NewChunkSequence()
	// The source names a window up to 0xFFFFFFFF; the credit this node grants, and the chunk
	// it will stage in one batch, are bounded by its own seed_window_bytes.
	window := uint32(min(int64(accept.WindowBytes), int64(p.opt.WindowBytes), int64(1<<32-1)))
	cut := accept.Cut
	for {
		if ctx.Err() != nil {
			return pullWait{after: p.opt.RetryBackoff}
		}
		if buffer.Overflowed() {
			// A live write was refused for size. The machine's over-the-buffer-bound row answers a
			// restart; the refused frame is not kept, and the attempt ends with its buffer.
			out, err := p.feed(key, machine.LiveWriteBuffered{Pos: buffer.OverflowPos(), Bytes: p.opt.BufferBytes + 1})
			if err != nil {
				return p.waiting(key, source, err.Error())
			}
			if out.Restart != 0 {
				return pullWait{}
			}
		}
		if detail, paused := p.diskPaused(key); paused {
			// The stream ends here; the record and its cursor stay, and the next attempt registers
			// again on the same pin.
			p.hold(key, source, detail)
			return pullWait{after: diskPauseRetry, detail: detail}
		}
		if p.cfg.Pace != nil {
			// The idle cap keeps the source from closing a stream it hears nothing on while the
			// share is paid (seed_idle_seconds, Owner question 2).
			if err := p.cfg.Pace.Wait(ctx, key.slot, key.db, p.opt.Idle/2); err != nil {
				return pullWait{after: p.opt.RetryBackoff}
			}
		}
		ack := transport.EncodeSeedAck(transport.SeedAck{Header: p.header(key), SeedID: accept.SeedID, Applied: cut, CreditBytes: window})
		reply, err := p.call(ctx, source, ack)
		if err != nil {
			return p.callFailed(key, source, err)
		}
		if reply == nil {
			return p.waiting(key, source, fmt.Sprintf("%s answered a SEED_ACK with nothing before the final chunk. The pull registers again from its cursor", source))
		}
		reply.Env.Slot, reply.Env.DBID = key.slot, key.db
		switch reply.Env.Kind {
		case transport.KindSeedChunk:
			step, done := p.takeChunk(key, source, reply, sequence, window)
			if done {
				buffer.SetPhase(mirrorBuffer)
				return p.closeSeed(ctx, key, source, accept, buffer, window)
			}
			if step != nil {
				return *step
			}
		case transport.KindSeedRestart:
			return p.sourceRestarted(key, source, reply)
		case transport.KindSeedRefused:
			return p.stalledRefusal(key, source, reply)
		default:
			return p.waiting(key, source, fmt.Sprintf("%s answered a SEED_ACK with kind 0x%02x, which is not a chunk", source, reply.Env.Kind))
		}
	}
}

// stalledRefusal turns the oversized-entry refusal (reason 5 with detail, SPEC-GAP T2-5)
// into a stalled status that keeps its text. The record keeps its cursor.
func (p *Puller) stalledRefusal(key reconcileKey, source string, reply *transport.Message) pullWait {
	refusal, err := transport.DecodeSeedRefused(payloadOf(*reply))
	if err != nil {
		return p.waiting(key, source, fmt.Sprintf("the SEED_REFUSED of %s is damaged (%v). It will be asked again", source, err))
	}
	p.mu.Lock()
	if entry := p.entries[key]; entry != nil {
		entry.status.State, entry.status.Detail, entry.status.Source = PullStalled, refusal.Detail, source
	}
	p.mu.Unlock()
	return pullWait{after: p.opt.Idle, detail: refusal.Detail}
}

// takeChunk verifies and applies one chunk. It returns done when the final chunk is
// applied, or a pullWait when the stream cannot go on (nil when it can).
func (p *Puller) takeChunk(key reconcileKey, source string, reply *transport.Message, sequence *transport.ChunkSequence, window uint32) (*pullWait, bool) {
	chunk, err := transport.DecodeSeedChunk(payloadOf(*reply))
	if err != nil {
		// A damaged chunk is refused whole (the CRC covers its entries). The source has
		// moved its cursor past it, so the way to ask again is to register again from the
		// last key applied: a resume.
		step := p.waiting(key, source, fmt.Sprintf("a chunk from %s failed its check (%v). The pull registers again from its cursor", source, err))
		return &step, false
	}
	var size int64
	for _, entry := range chunk.Entries {
		size += int64(entryBytes(entry.Key, entry.Value))
	}
	if size > int64(window) {
		// The receiver granted this credit and applies a chunk in one batch: a source that sends
		// more is refused whole and asked again from the cursor (T3 review F9a).
		step := p.waiting(key, source, fmt.Sprintf("chunk %d from %s is %d bytes, over the %d-byte window this node granted. The pull registers again from its cursor", chunk.Index, source, size, window))
		return &step, false
	}
	if err := sequence.Accept(chunk); err != nil {
		if restartErr := p.cfg.Recon.RestartSeed(key.slot, key.db); restartErr != nil {
			err = errors.Join(err, restartErr)
		}
		step := p.waiting(key, source, fmt.Sprintf("%v", err))
		step.after = 0
		return &step, false
	}
	outcome, err := p.applyChunk(key, chunk)
	if err != nil {
		step := p.waiting(key, source, err.Error())
		return &step, false
	}
	if outcome.Restart != 0 {
		step := pullWait{}
		return &step, false
	}
	if !outcome.ApplyChunk {
		step := p.waiting(key, source, fmt.Sprintf("chunk %d from %s moved nothing forward. The pull registers again from its cursor", chunk.Index, source))
		return &step, false
	}
	if p.cfg.Pace != nil {
		p.cfg.Pace.Spent(key.slot, key.db, size)
	}
	p.progressed(key, source, size, len(chunk.Entries))
	return nil, chunk.Final
}

// applyChunk puts one chunk's entries and the record's new cursor in one batch.
func (p *Puller) applyChunk(key reconcileKey, chunk transport.SeedChunk) (machine.SeedOutcome, error) {
	prefix := storage.PairPrefix(key.slot, key.db)
	outcome, err := p.cfg.Recon.ApplySeedChunk(key.slot, key.db, SeedChunkRequest{
		Cfg: p.seedConfig(),
		Stage: func(batch storage.KVBatch, seed machine.SeedRecord) (machine.SeedEvent, error) {
			var bytesIn int64
			previous := seed.Cursor
			for _, entry := range chunk.Entries {
				if !bytes.HasPrefix(entry.Key, prefix) {
					return nil, fmt.Errorf("chunk %d carries key %x, which is outside slot %d of database %d. A source may send only keys of the pair it was asked for", chunk.Index, entry.Key, key.slot, key.db)
				}
				if len(previous) != 0 && bytes.Compare(entry.Key, previous) <= 0 {
					return nil, fmt.Errorf("chunk %d carries key %x at or before key %x, so its keys are not in order", chunk.Index, entry.Key, previous)
				}
				previous = entry.Key
				if err := batch.Set(entry.Key, entry.Value); err != nil {
					return nil, fmt.Errorf("staging key %x of chunk %d: %w", entry.Key, chunk.Index, err)
				}
				bytesIn += int64(entryBytes(entry.Key, entry.Value))
			}
			var cursor []byte
			if len(chunk.Entries) != 0 {
				cursor = chunk.Entries[len(chunk.Entries)-1].Key
			}
			return machine.ChunkStored{Cursor: cursor, Bytes: bytesIn, Entries: len(chunk.Entries), Final: chunk.Final}, nil
		},
		Commit: func(batch storage.KVBatch, sync bool) error { return batch.Commit(sync) },
	})
	if err != nil {
		return machine.SeedOutcome{}, fmt.Errorf("applying chunk %d of slot %d of database %d: %w", chunk.Index, key.slot, key.db, err)
	}
	return outcome, nil
}

// complete runs once the buffer is empty behind a current catch-up that reached last, the
// source's tail: the drained signal, then the tail every drain ends with.
func (p *Puller) complete(ctx context.Context, key reconcileKey, source string, accept transport.SeedAccept, window uint32, last cluster.Position) pullWait {
	out, err := p.feed(key, machine.BufferDrained{Last: last})
	if err != nil {
		return p.waiting(key, source, err.Error())
	}
	if out.Restart != 0 {
		return pullWait{}
	}
	return p.finishSeed(ctx, key, source, &accept, window)
}

// finishSeed numbers the pair's writes and marks it ready, then closes the seed on the source and
// runs OnReady. It is the tail of an attempt in both shapes: after the drain, which has an accept
// to close, and on a record reloaded in PhaseNone, which owes only this sync (contract 2) and has
// no accept because it never registered. It makes no sync of its own: the SyncDone's ready
// record is committed with a sync, which makes the data, the expiry entries and the drained
// record durable with it before the record is installed (vouched cut I-4). A separate sync
// before it cost a second sync-marker write per pair and vouched nothing more.
func (p *Puller) finishSeed(ctx context.Context, key reconcileKey, source string, accept *transport.SeedAccept, window uint32) pullWait {
	if p.cfg.DeriveExpiry != nil {
		if err := p.cfg.DeriveExpiry(ctx, key.slot, key.db); err != nil {
			return p.waiting(key, source, fmt.Sprintf("building the expiry queue of slot %d of database %d failed: %v. The pull retries", key.slot, key.db, err))
		}
	}
	ordinal, err := p.highestOrdinal(ctx, key)
	if err != nil {
		return p.waiting(key, source, err.Error())
	}
	out, err := p.cfg.Recon.FeedSeed(key.slot, key.db, FeedSeedRequest{Event: machine.SyncDone{}, Cfg: p.seedConfig(), DataDurable: true, Ordinal: ordinal})
	if err != nil {
		return p.waiting(key, source, err.Error())
	}
	if out.Restart != 0 {
		return pullWait{}
	}
	if !out.MarkReady {
		return p.waiting(key, source, fmt.Sprintf("slot %d of database %d did not become ready after its final sync. The pull retries", key.slot, key.db))
	}
	if accept != nil {
		closing := transport.EncodeSeedAck(transport.SeedAck{Header: p.header(key), SeedID: accept.SeedID, Applied: accept.Cut, CreditBytes: window})
		if _, err := p.call(ctx, source, closing); err != nil {
			// The data is applied; the source drops the pin after the hold on its own.
			p.report(fmt.Errorf("closing the seed of slot %d of database %d with %s: %w", key.slot, key.db, source, err))
		}
	}
	if p.cfg.OnReady != nil {
		p.cfg.OnReady(key.slot, key.db)
	}
	return pullWait{stop: true}
}

// drainStatus says where a drain of the buffer stopped.
type drainStatus uint8

const (
	// drainEmpty means the buffer holds no write above the record's position.
	drainEmpty drainStatus = iota
	// drainHole means the lowest buffered write is above the next position.
	drainHole
	// drainEnded means the attempt ends with the wait the drain returned.
	drainEnded
)

// drainRun is the running dedupe ordinal of one attempt (seeding section 2.3, step 0): the
// highest ordinal on disk, read when the first write is staged, and one more for each write.
type drainRun struct {
	ordinal uint64
	scanned bool
}

// errRestartAttempt is what staging a write returns when the seed machine answers a restart: the
// attempt is over and begins again on a fresh cut.
var errRestartAttempt = errors.New("the seed machine restarts the attempt")

// drain applies the buffered writes that continue the record, lowest position first, each in the
// batch that also carries the seed record's step (seeding section 2.3, steps 0 and 1). A write at
// or below the record's applied position is the snapshot's or already applied, and is dropped. A
// write above the next position is a hole: the drain stops with drainHole and the write goes back
// in the buffer, so nothing is applied past it. An error stops the drain with drainEnded and the
// wait says why.
func (p *Puller) drain(ctx context.Context, key reconcileKey, source string, a *seedAttemptBuffer, run *drainRun) (drainStatus, cluster.Position, pullWait) {
	for {
		if ctx.Err() != nil {
			return drainEnded, cluster.Position{}, pullWait{after: p.opt.RetryBackoff}
		}
		record, found := p.cfg.Recon.Record(key.slot, key.db)
		if !found {
			return drainEnded, cluster.Position{}, pullWait{stop: true}
		}
		frame, ok := a.Pop()
		if !ok {
			return drainEmpty, cluster.Position{}, pullWait{}
		}
		if !record.Applied.Less(frame.frame.Pos) {
			continue
		}
		if next := drainNext(record.Applied, p.cfg.Committed()); frame.frame.Pos != next {
			a.requeue(frame)
			return drainHole, frame.frame.Pos, pullWait{}
		}
		if err := p.stageRecord(ctx, key, run, frame.frame.PairRecord); err != nil {
			if errors.Is(err, errRestartAttempt) {
				return drainEnded, cluster.Position{}, pullWait{}
			}
			a.requeue(frame)
			return drainEnded, cluster.Position{}, p.waiting(key, source, err.Error())
		}
	}
}

// closeSeed drains the buffer and runs the closing catch-up until the buffer ends empty behind a
// catch-up that answers current, then completes the attempt (seeding section 2.3, steps 1 to 3).
// A hole is filled from the pinned source; nothing is applied past it first.
func (p *Puller) closeSeed(ctx context.Context, key reconcileKey, source string, accept transport.SeedAccept, buffer *seedAttemptBuffer, window uint32) pullWait {
	run := &drainRun{}
	for {
		status, lowest, drained := p.drain(ctx, key, source, buffer, run)
		switch status {
		case drainEnded:
			return drained
		case drainHole:
			if held, ended := p.closeHole(ctx, key, source, run, lowest); ended {
				return held
			}
			continue
		}
		tail, closing, ended := p.closingRound(ctx, key, source, run)
		if ended {
			return closing
		}
		if buffer.EndIfEmpty() {
			return p.complete(ctx, key, source, accept, window, tail)
		}
	}
}

// closeHole fills the hole below lowest from the pinned source (seeding section 2.3, step 2). The
// hole is closed when the record has applied the lowest buffered write or the next position
// reaches it. A source that answers current and leaves it open, or a horizon, ends the attempt on
// a fresh cut; so does a next position that is past the lowest write without the record having
// applied it, which is an epoch jump: the next position is the committed epoch's opener and no
// catch-up can reach the buffered write from there. It returns true when the attempt ends.
func (p *Puller) closeHole(ctx context.Context, key reconcileKey, source string, run *drainRun, lowest cluster.Position) (pullWait, bool) {
	from, found := p.nextPosition(key)
	if !found {
		return pullWait{stop: true}, true
	}
	if _, err := p.catchUp(ctx, key, source, from, run); err != nil {
		return p.catchUpEnded(key, source, err), true
	}
	record, found := p.cfg.Recon.Record(key.slot, key.db)
	if !found {
		return pullWait{stop: true}, true
	}
	if record.Applied.Less(lowest) && drainNext(record.Applied, p.cfg.Committed()) != lowest {
		return p.restartOnFreshCut(key, source), true
	}
	return pullWait{}, false
}

// closingRound is the catch-up that ends the drain: from the next position, against the pinned
// source. It returns the source's tail when the answer is current and the record has reached it;
// any other answer ends the attempt. A record behind the tail cannot be made ready (the machine's
// drained/behind-the-source-tail row refuses it), and asking again from the same position gets
// the same answer, so the attempt restarts on a fresh cut.
func (p *Puller) closingRound(ctx context.Context, key reconcileKey, source string, run *drainRun) (cluster.Position, pullWait, bool) {
	from, found := p.nextPosition(key)
	if !found {
		return cluster.Position{}, pullWait{stop: true}, true
	}
	tail, err := p.catchUp(ctx, key, source, from, run)
	if err != nil {
		return cluster.Position{}, p.catchUpEnded(key, source, err), true
	}
	record, found := p.cfg.Recon.Record(key.slot, key.db)
	if !found {
		return cluster.Position{}, pullWait{stop: true}, true
	}
	if record.Applied.Less(tail) {
		return cluster.Position{}, p.restartOnFreshCut(key, source), true
	}
	return tail, pullWait{}, false
}

// catchUp asks the pinned source for the records from from and stages each one it hands back in
// the record's own batch. It returns the source's tail when the answer is current.
func (p *Puller) catchUp(ctx context.Context, key reconcileKey, source string, from cluster.Position, run *drainRun) (cluster.Position, error) {
	return p.cfg.Replicator.CatchUpInto(ctx, CatchUpIntoRequest{
		Slot: key.slot, DB: key.db, From: from, Source: source,
		Sink: func(rec PairRecord) error { return p.stageRecord(ctx, key, run, rec) },
	})
}

// catchUpEnded is how an attempt ends when a catch-up fails. A horizon or a divergent answer
// restarts the attempt on a fresh cut, a write the machine restarts ends it the same way, and any
// other error waits and is asked again.
func (p *Puller) catchUpEnded(key reconcileKey, source string, err error) pullWait {
	switch {
	case errors.Is(err, ErrResetAndSeed):
		return p.restartOnFreshCut(key, source)
	case errors.Is(err, errRestartAttempt):
		return pullWait{}
	}
	return p.waiting(key, source, err.Error())
}

// restartOnFreshCut resets the record to a fresh attempt, which pulls a new snapshot on a new cut.
func (p *Puller) restartOnFreshCut(key reconcileKey, source string) pullWait {
	if err := p.cfg.Recon.RestartSeed(key.slot, key.db); err != nil {
		return p.waiting(key, source, err.Error())
	}
	return pullWait{}
}

// nextPosition is the position the record applies next, and false when the pair has no record.
func (p *Puller) nextPosition(key reconcileKey) (cluster.Position, bool) {
	record, found := p.cfg.Recon.Record(key.slot, key.db)
	return drainNext(record.Applied, p.cfg.Committed()), found
}

// stageRecord applies one buffered or caught-up write at the next position, through the record's
// batch with the ordinal after the running one (seeding section 2.3, step 1).
func (p *Puller) stageRecord(ctx context.Context, key reconcileKey, run *drainRun, rec PairRecord) error {
	if !run.scanned {
		highest, err := p.highestOrdinal(ctx, key)
		if err != nil {
			return err
		}
		run.ordinal, run.scanned = highest, true
	}
	out, err := p.applyBuffered(key, rec, run.ordinal+1)
	if err != nil {
		return err
	}
	if out.Restart != 0 {
		return errRestartAttempt
	}
	if len(out.ApplyBuffered) == 0 {
		return fmt.Errorf("slot %d of database %d did not take the buffered write at %s. The pull registers again from its cursor", key.slot, key.db, rec.Pos)
	}
	run.ordinal++
	return nil
}

// applyBuffered writes one write into the pair. Its data, its dedupe entry and the seed record's
// step are one batch, so a crash leaves all of them or none.
func (p *Puller) applyBuffered(key reconcileKey, rec PairRecord, ordinal uint64) (machine.SeedOutcome, error) {
	out, err := p.cfg.Recon.ApplySeedChunk(key.slot, key.db, SeedChunkRequest{
		Cfg: p.seedConfig(),
		Stage: func(batch storage.KVBatch, _ machine.SeedRecord) (machine.SeedEvent, error) {
			op, err := cluster.DecodeOp(rec.Record.Body)
			if err != nil {
				return nil, fmt.Errorf("decoding the write at %s: %w", rec.Pos, err)
			}
			applied := AppliedRecord{
				Slot: key.slot, DB: key.db, Op: op, Pos: rec.Pos, Cursor: rec.Cursor, Ordinal: ordinal,
				RequestID: rec.Record.RequestID, OpIndex: rec.Record.OpIndex,
			}
			if _, err := fillApplied(batch, p.cfg.Applier, applied, p.cfg.DedupeWindow); err != nil {
				return nil, err
			}
			return machine.BufferApplyStep{Pos: applied.Pos, Bytes: int64(len(rec.Record.Body))}, nil
		},
		Commit: func(batch storage.KVBatch, sync bool) error { return batch.Commit(sync) },
	})
	if err != nil {
		return machine.SeedOutcome{}, fmt.Errorf("applying the write at %s of slot %d of database %d: %w", rec.Pos, key.slot, key.db, err)
	}
	return out, nil
}

// drainNext is the position after applied, in the epoch the seed runs in. It mirrors the
// machine's seedNext, which the drain cannot call: a hole has to be seen before anything is
// staged, since staging is what writes the data.
func drainNext(applied cluster.Position, epoch uint64) cluster.Position {
	if applied.Epoch < epoch {
		return cluster.Position{Epoch: epoch, Seq: 1}
	}
	return cluster.Position{Epoch: applied.Epoch, Seq: applied.Seq + 1}
}

// requeue puts back a write the drain popped and could not apply. It was held when it was
// popped, so it fits and goes back without a share check.
func (a *seedAttemptBuffer) requeue(frame bufferedFrame) {
	a.offer(frame.frame, math.MaxInt64)
}

// highestOrdinal finds the highest dedupe-entry ordinal in the pair's range (seeding section
// 2). It runs before the record becomes ready, while nothing applies to the pair, and the
// ordinal is written in the ready record's own batch. It reads keys only and fetches a value
// just for a dedupe entry, and it stops when ctx ends. It is derived from the data on disk,
// so a pull that was killed and resumed gets the same number.
func (p *Puller) highestOrdinal(ctx context.Context, key reconcileKey) (uint64, error) {
	snapshot := p.cfg.KV.NewSnapshot()
	defer snapshot.Discard()
	prefix := storage.PairPrefix(key.slot, key.db)
	iter := snapshot.NewIterator(storage.IterOptions{Lower: prefix, Upper: keyRangeEnd(prefix)})
	defer iter.Close()
	var highest uint64
	for iter.Seek(prefix); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			return 0, fmt.Errorf("scanning the dedupe entries of slot %d of database %d: %w", key.slot, key.db, err)
		}
		entryKey := iter.Key()
		if len(entryKey) != dedupeKeyLen || entryKey[18] != storage.NSDedupe {
			continue
		}
		raw, err := iter.Value()
		if err != nil {
			return 0, fmt.Errorf("reading the dedupe entry %x of slot %d of database %d: %w", entryKey, key.slot, key.db, err)
		}
		_, entry, err := DecodeDedupeEntry(entryKey, raw)
		if err != nil {
			return 0, fmt.Errorf("decoding the dedupe entry %x of slot %d of database %d: %w", entryKey, key.slot, key.db, err)
		}
		highest = max(highest, entry.Ordinal)
	}
	if err := iter.Err(); err != nil {
		return 0, fmt.Errorf("scanning the dedupe entries of slot %d of database %d: %w", key.slot, key.db, err)
	}
	return highest, nil
}

// resetRangeBatch is how many deletes one batch of a range reset holds.
const resetRangeBatch = 1024

// resetRange deletes every key of the pair and, when it deleted any, syncs the store (seeding
// section 4 step 3).
// Each batch is charged to the node's reclaim budget, so a reseed deletes at
// storage.reclaim_keys_per_second beside the sweeps and never as one call that stops the
// store's compaction and refuses its writers (plan §1.3).
func (p *Puller) resetRange(ctx context.Context, key reconcileKey) error {
	prefix := storage.PairPrefix(key.slot, key.db)
	snapshot := p.cfg.KV.NewSnapshot()
	defer snapshot.Discard()
	iter := snapshot.NewIterator(storage.IterOptions{Lower: prefix, Upper: keyRangeEnd(prefix)})
	defer iter.Close()
	batch := p.cfg.KV.NewBatch()
	defer func() { batch.Discard() }()
	queued := 0
	deletedAny := false
	flush := func() error {
		if err := batch.Commit(false); err != nil {
			return fmt.Errorf("deleting part of slot %d of database %d: %w", key.slot, key.db, err)
		}
		batch.Discard()
		deletedAny = true
		deleted := queued
		batch, queued = p.cfg.KV.NewBatch(), 0
		return p.cfg.Budget.charge(ctx, deleted)
	}
	for iter.Seek(prefix); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("resetting slot %d of database %d: %w", key.slot, key.db, err)
		}
		if err := batch.Delete(iter.Key()); err != nil {
			return fmt.Errorf("staging a delete in slot %d of database %d: %w", key.slot, key.db, err)
		}
		queued++
		if queued >= resetRangeBatch {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := iter.Err(); err != nil {
		return fmt.Errorf("walking slot %d of database %d to reset it: %w", key.slot, key.db, err)
	}
	if queued > 0 {
		if err := flush(); err != nil {
			return err
		}
	}
	if !deletedAny {
		// The walk found no key, so this call wrote nothing and there is no delete to make
		// durable. The vouched cut made each store sync cost a marker write, and a sync here
		// would vouch nothing the attempt's ready record does not (seeding-speed-note.md).
		return nil
	}
	if err := p.cfg.KV.Sync(); err != nil {
		return fmt.Errorf("syncing the reset of slot %d of database %d: %w", key.slot, key.db, err)
	}
	return nil
}
