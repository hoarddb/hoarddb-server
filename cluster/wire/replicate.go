package wire

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage"
)

// Defaults of the stream: replication.repl_window, and the catch-up chunk bounds of
// write path §6 step 3.
const (
	defaultWindow       = 64
	defaultChunkRecords = 1024
	defaultChunkBytes   = 4 << 20
	// defaultAckTimeout is longer than linkCallTimeout, so an ack queued behind a catch-up on
	// the same connection is not mistaken for a lost one.
	defaultAckTimeout = 5 * time.Second
)

// AckOutcome is the outcome byte of WRITE_ACK (conv §6.1).
type AckOutcome byte

// The three WRITE_ACK outcomes, the only value table for the byte.
const (
	// AckNotApplied: the copy has not applied the position because it is seeding or
	// cannot judge it yet. It contributes nothing and is never a reset.
	AckNotApplied AckOutcome = 0
	// AckApplied: fsynced in the copy's log and applied in its Badger batch.
	AckApplied AckOutcome = 1
	// AckRefused: a gap or a divergence (SEQ_GAP); the sender must catch the copy up or
	// reset and seed it.
	AckRefused AckOutcome = 2
)

// PairRecord is one retained-log record of a pair with the position it was ordered at
// and the sender's node-local log sequence. conv §6.1 gives a WRITE frame and a
// CATCHUP_CHUNK record no position of their own, so the byte codec waits on that
// ruling; this is the message the codec will carry.
type PairRecord struct {
	Pos cluster.Position
	// Cursor is the sending node's WAL sequence of the record; a copy stores it as the
	// origin's Seq.
	Cursor uint64
	Record storage.WALRecord
}

// WriteFrame is one WRITE: a record pushed to a copy.
type WriteFrame struct {
	Slot uint16
	DB   uint64
	// From is the sending node.
	From string
	// Epoch is the sender's committed epoch, which the data channel requires equal.
	Epoch uint64
	PairRecord
}

// WriteAck is a copy's cumulative acknowledgement: Pos is the highest position the
// copy has applied, whichever frame prompted the ack.
type WriteAck struct {
	From    string
	Slot    uint16
	DB      uint64
	Pos     cluster.Position
	Outcome AckOutcome
}

// CatchUpRequest is CATCHUP_REQ.
type CatchUpRequest struct {
	Slot uint16
	DB   uint64
	// Node is the requesting node.
	Node string
	// Epoch is the requester's committed epoch.
	Epoch uint64
	// From is the first position the requester still needs.
	From cluster.Position
	// FromCursor is the requester's applied cursor plus one. It indexes the
	// requester's own log, so the source treats it as a hint at most (see Seek).
	FromCursor uint64
}

// CatchUpChunk is CATCHUP_CHUNK.
type CatchUpChunk struct {
	// SourceAppended is the source's appended position when the read started, never
	// the requested position.
	SourceAppended cluster.Position
	CaughtUp       bool
	// Divergent says the requester's position is above the source's tail, so it holds
	// a record nobody owns. The requester resets and seeds; it is never current.
	Divergent bool
	// LastPosition is the position of the last record, or SourceAppended when the
	// requester already holds everything.
	LastPosition cluster.Position
	Records      []PairRecord
}

// Link is the data channel as the stream uses it.
type Link interface {
	// Write sends one WRITE frame to peer. It returns once the frame is handed to the
	// transport; the acknowledgement arrives through OnWriteAck.
	Write(ctx context.Context, peer string, frame WriteFrame) error
	// Ack sends a cumulative acknowledgement from a catch-up to the owner.
	Ack(ctx context.Context, peer string, ack WriteAck) error
	// CatchUp asks source for the records from req.From and returns one chunk.
	CatchUp(ctx context.Context, source string, req CatchUpRequest) (CatchUpChunk, error)
}

// Log is the node's one retained log. *storage.WAL implements it.
type Log interface {
	Appender
	cluster.LogReader
	// FirstSequence is the first sequence the log still retains.
	FirstSequence() (uint64, error)
}

// ReseedCause is why a copy's pair is being reset and seeded again. It picks the
// SEED_RESTART reason and the words of the status row (seeding-completion plan §1.2).
type ReseedCause uint8

const (
	// ReseedHorizon: the copy's catch-up was answered LOG_BELOW_HORIZON, so the records it
	// needs are no longer in the source's log.
	ReseedHorizon ReseedCause = iota + 1
	// ReseedDivergent: the copy holds positions nobody owns (conv §10 rule 5).
	ReseedDivergent
)

// String words the cause for logs and status rows.
func (c ReseedCause) String() string {
	switch c {
	case ReseedHorizon:
		return "the log no longer holds what this copy needs (LOG_BELOW_HORIZON)"
	case ReseedDivergent:
		return "this copy held positions nobody owns (divergent tail)"
	}
	return fmt.Sprintf("reseed cause %d", uint8(c))
}

// Seeder resets and seeds again a pair that cannot be caught up: one whose tail nobody
// owns, or whose need is below the source's log. It is the only place a copy's records are
// discarded (conv §10 rule 5, SEED_RESTART reasons 2 and 4).
type Seeder interface {
	// ResetAndSeed resets the pair's range and seeds it. The stream never deletes a
	// record itself.
	ResetAndSeed(slot uint16, db uint64, cause ReseedCause) error
}

// SeedBuffer takes the frames a pair that is not ready cannot apply, so a live write that
// arrives during a seed is kept for the drain after it instead of being lost to the owner's
// retry alone. The replicator only offers; the buffer's bound, its order and its drain belong
// to the buffer (seeding-completion plan §2).
type SeedBuffer interface {
	// Offer keeps the frame and reports whether it did. The ack to the owner is not applied
	// either way: nothing a buffer holds has been applied or made durable as a copy.
	Offer(slot uint16, db uint64, frame WriteFrame) bool
}

// ReplicatorConfig wires a Replicator to the node. The Replicator keeps no state per
// bucket: its state is per (slot, database) pair and per copy of that pair.
type ReplicatorConfig struct {
	// Self is this node's id, stamped on frames and acks it sends.
	Self string
	// Committed is the committed epoch; the data channel requires frames at an equal one.
	Committed func() uint64
	// MarkBehind, when set, is told the epoch of a frame ahead of Committed: the cluster
	// moved, and this node stops serving until its own epoch catches up (conventions
	// section 7). Behind reports that mark against a committed epoch; while it holds, a
	// pushed record is not applied.
	MarkBehind func(epoch uint64)
	Behind     func(committed uint64) bool
	// Pairs holds the per-pair applied state this node shares with its write path.
	Pairs *cluster.PairSet
	// Log is the node's one retained log.
	Log Log
	// KV starts the Badger batch of an apply.
	KV BatchSource
	// Advancer stages the pair record in the batch of each record the copy applies, under
	// the pair lock the apply already holds (BUG-0752). Required: a networked node passes its
	// Reconciler's, so that the seed-aware gating applies to a copy as it does to the owner,
	// and a default here would let a wiring that forgot it pass every test.
	Advancer LockedAdvancer
	// Applier turns a record into keys.
	Applier Applier
	// Link is the data channel.
	Link Link
	// Seeds resets and seeds a pair that holds a tail nobody owns.
	Seeds Seeder
	// Copies lists the other nodes that hold the pair at the committed epoch.
	Copies func(slot uint16, db uint64) []string
	// Owner is the node that owns the pair now.
	Owner func(slot uint16, db uint64) (string, bool)
	// CatchUpSource is where this node asks for a catch-up.
	CatchUpSource func(slot uint16, db uint64) (string, error)
	// FloorCursor is a log sequence at or before the first unshipped record of the pair.
	FloorCursor func(slot uint16, db uint64) uint64
	// Ready reports whether the pair is ready; a seeding pair answers AckNotApplied.
	// Nil means always ready.
	Ready func(slot uint16, db uint64) bool
	// Deferred reports whether the pair's boot replay has not finished (BUG-0760): its applied
	// position is below records this node acknowledged, so a catch-up is not served from it.
	// Nil means never.
	Deferred func(slot uint16, db uint64) bool
	// Buffer, when set, is offered every frame a pair that is not ready was pushed. Nil here:
	// the live-write buffer sets it (plan §2).
	Buffer SeedBuffer
	// Seek turns a catch-up request into the log sequence to read from. Nil reads from
	// the log's first sequence: a cursor in the requester's log cannot seek this one.
	Seek func(req CatchUpRequest) uint64
	// Window is replication.repl_window; 0 means 64.
	Window int
	// DedupeWindow is replication.dedupe_window and the stored result cap, the same
	// values the node's Writer holds: a copy writes the dedupe entry of every record it
	// applies with them (write §5). Zero takes the Writer's defaults.
	DedupeWindow machine.WindowConfig
	// ChunkRecords and ChunkBytes bound one catch-up chunk; 0 means 1,024 and 4 MiB.
	ChunkRecords int
	ChunkBytes   int
	// SchemaReady, when set, is asked before a replicated record is appended: it returns
	// nil once this copy has published the declaration version the record was written
	// under (waiting a bounded time for the publication), or a retryable refusal. A record
	// whose store field the copy cannot resolve yet must be refused, never appended and
	// then failed (U40-7): the owner retries it, or the copy catches up, later.
	SchemaReady func(db uint64, op cluster.Op) error
	// RetryBackoff is how long a copy that answered "not applied" is left alone before the
	// same frames are shipped again; zero takes 200 ms.
	RetryBackoff time.Duration
	// CopyWorkers is the size of the fixed pool that applies queued WRITEs on a copy; 0 takes
	// the larger of 16 and GOMAXPROCS. The pool is the only goroutines the copy side adds: none
	// per pair or per bucket.
	CopyWorkers int
	// CopyQueueBytes bounds the record bytes queued on a copy for the whole node; 0 takes 32 MiB.
	// A pair is also held to twice the window of queued frames.
	CopyQueueBytes int
	// AckTimeout is how long the owner waits for a copy to acknowledge frames it has in flight
	// before it ships them again from the last acknowledged position; 0 takes 5 s, longer than
	// one link call (a lost acknowledgement leaves nothing else to wake the pair).
	AckTimeout time.Duration
	// Now reads the clock for re-arm and acknowledgement deadlines; nil takes time.Now.
	Now func() time.Time
	// RequestCatchUp, when set, starts a catch-up of one pair from its owner in the
	// background (once at a time per pair): the copy's apply workers call it on a gap.
	RequestCatchUp func(slot uint16, db uint64)
	// OnError receives an error that has no caller: a failed background flush, or an
	// ack that could not be sent. Nil drops it.
	OnError func(error)
}

type pairKey struct {
	slot uint16
	db   uint64
}

type sentFrame struct {
	pos    cluster.Position
	cursor uint64
}

// peerState is the sender's view of one copy of one pair.
type peerState struct {
	inflight []sentFrame // shipped, unacked, at most the window
	acked    cluster.Position
	// ackedCursor is the log sequence of the record at acked, 0 when unknown.
	ackedCursor uint64
	// next is the log sequence the next read starts at.
	next    uint64
	shipped cluster.Position
	// behind: the window filled or the copy refused. Nothing ships until it answers.
	behind bool
	// refused: the copy refused or could not judge a frame, so what was shipped after
	// its last ack never applied and is read again once it answers applied.
	refused bool
	// holds: the copy's latest answer was an applied ack. A not-applied or refused answer, with or
	// without a position, clears it, and ackedLocked counts a copy only while it is set, so a copy
	// that reset itself is not counted at its stale acked position (plan §1.4, rule 2).
	holds   bool
	started bool
	// rearmAt is when a copy that answered "not applied" or refused is pushed to again, the
	// zero time when it is not waiting. Only an applied ack clears the wait for good; this
	// makes sure a copy that recovers without anyone asking it to catch up is not abandoned.
	rearmAt time.Time
	// backoffs counts consecutive not-applied answers, so each wait is longer than the last;
	// an applied ack starts it over. sendFailed: the last send to this copy errored.
	backoffs   int
	sendFailed bool
	// fromFloor: the copy's acknowledged record is below the log horizon, so it is shipped
	// from the first retained record and its own gap check asks for a catch-up or a seed.
	fromFloor bool
	// anchor is the log sequence the first read of the pair started at, 0 when it started at
	// the log's first record. It is where a rewind resumes before any ack has set ackedCursor,
	// and it is not an ack: Acked and Await never count it.
	anchor uint64
	// generation changes whenever the cursor is rewound, so a read made before the
	// rewind is dropped instead of shipped.
	generation uint64
	// cursor is the log cursor where the last read stopped, valid when hasCursor and when its next
	// sequence is st.next: the steady state then reads on without seeking the log again.
	cursor    storage.WALCursor
	hasCursor bool
	// hint is the log sequence of the first record Enqueue queued for this pair after this
	// copy's reads had passed every record queued before it, 0 when there is none. A read starts
	// at the later of st.next and hint, so a pair that wrote long ago does not walk the entries
	// other pairs wrote since (BUG-0745).
	//
	// Safety: a read may start later than st.next only if no record of the history the new record
	// extends, not yet read for this copy, lies between st.next and the start. hint is set only when
	// (1) st.next is past the newest record queued so far, so everything before the new one was
	// read, and (2) the new record is the very next position after the record queued last: the next
	// seq of the same epoch, or the opener of a later epoch whose previous end is that record (the
	// opener's previous end is the applied position it was ordered at, which a later record of that
	// history would have raised; BUG-0872). The entries skipped can still hold a record of the pair
	// that is not in that history: one this node applied as a copy and a reset then discarded. The
	// backstop is the copy's own boundary check: a record skipped wrongly is refused as a gap, or an
	// opener the copy is behind for, and either asks for a catch-up, which reads without hints.
	// hint is cleared wherever st.next moves back (rewindLocked, the horizon reset), because the records between the restored
	// st.next and hint are then unread again. A hint that st.next has passed is inert, which is
	// why a read, empty or not, never needs to clear it. A restart of the owner has no peer state,
	// so it reads from the pair's first hint or the log's first record as before.
	hint uint64
	// ackBy is when the copy must have acknowledged something, or the frames in flight are
	// shipped again from its last acknowledged position: the WRITE reply is only a receipt, so
	// a lost ack leaves them in flight with nothing else to wake the pair. Zero when nothing is
	// in flight. Every applied ack that makes progress renews it.
	ackBy time.Time
	// staleAcks is how many more not-applied answers the copy still owes for frames it had
	// queued when it first refused; they are one answer, not one step up the backoff each.
	staleAcks int
}

// retryKey names one copy of one pair: the unit the retry and ack-deadline sets are kept by,
// so a healthy copy's ack never cancels another copy's wait.
type retryKey struct {
	pair pairKey
	peer string
}

type pairSend struct {
	// hint is the log sequence of the first record the write path queued for this pair.
	hint uint64
	// lastPos and lastCursor are the newest position queued for the pair and the log sequence it
	// was appended at: what Enqueue compares the next record with.
	lastPos    cluster.Position
	lastCursor uint64
	// gapFrom and gapTo are the log sequences of two queued records of the pair, the second the very
	// next position after the first, more than maxChainLead entries apart, zero when there are none:
	// no record of the history the second extends lies strictly between them (peerState.hint has the
	// argument, and the copy's boundary check, then a catch-up, is the backstop). A record of the pair
	// that a reset discarded can lie between them, and is rightly not shipped. It is a fact about the
	// log, not about a copy, so it survives what a copy's hint does not: a rewind to just before the gap or a read
	// that has not reached its start yet (BUG-0872). Only the newest such gap is kept; the log is
	// append-only, so it never goes stale.
	gapFrom, gapTo uint64
	peers          map[string]*peerState
	drained        cluster.Position
	changed        chan struct{}
}

// Replicator is the replication stream of one node: the owner's push from the log, the
// copy's apply, and catch-up in both directions (write path §4 and §6).
type Replicator struct {
	cfg ReplicatorConfig

	mu    sync.Mutex
	pairs map[pairKey]*pairSend
	// tails are log cursors where pairs' reads last stopped at the tail, oldest first (guarded by mu).
	tails   []storage.WALCursor
	dirty   map[pairKey]struct{}
	current map[pairKey]struct{}
	// catching holds the catch-ups in flight, one per pair at most (guarded by mu): an entry
	// lives only while its catch-up runs.
	catching map[pairKey]*catchUpFlight
	// failed holds the pairs whose last send to some copy failed, so Retry knows what to
	// queue again without scanning every pair.
	failed map[retryKey]struct{}
	// awaiting holds the copies with frames in flight and an ack deadline, again so Retry
	// visits only those and never scans every pair.
	awaiting map[retryKey]struct{}
	pins     map[uint64]PinnedFloor
	pinID    uint64
	wake     chan struct{}

	// health is the per-peer breaker record and suspectChanged the channel closed and replaced
	// whenever a peer turns suspect or is cleared (replicate_health.go; guarded by mu).
	health         map[string]*peerHealth
	suspectChanged chan struct{}

	// lanes is one sender per copy, so a copy that stops answering holds up only its own
	// shipments (guarded by mu). laneWG counts the running lane goroutines: at most one per
	// copy, none per pair or bucket.
	lanes  map[string]*sendLane
	laneWG sync.WaitGroup
	// runMu orders Run's wg.Add against Close, so Close is a barrier: a Run that has
	// not started yet never starts after it, and one that has is waited for.
	runMu  sync.Mutex
	closed bool
	wg     sync.WaitGroup
	done   chan struct{}

	// copyQ is the copy's apply queue and its fixed worker pool (replicate_queue.go).
	copyQ copyQueues

	// ackBatch is the link's AckBatcher, nil when it cannot batch. ackMu guards ackLanes (one
	// unsent-ack queue per destination node), ackClosed (Run has ended, so an ack is reported
	// instead of queued) and runCtx (the context Run started with, for AckAsync). ackWG counts the
	// running ack-lane goroutines: at most one per destination, none per pair or bucket.
	ackBatch  AckBatcher
	ackMu     sync.Mutex
	ackLanes  map[string]*ackLane
	ackClosed bool
	runCtx    context.Context
	ackWG     sync.WaitGroup

	// writeBatch is the link's WriteBatcher, nil when it cannot batch: the lane then ships one
	// record per Write.
	writeBatch WriteBatcher

	// catchUpHook runs between the floor check and the read of a served catch-up.
	catchUpHook func()
}

// NewReplicator returns a Replicator over cfg. It starts no goroutine until Run.
func NewReplicator(cfg ReplicatorConfig) (*Replicator, error) {
	if cfg.Advancer == nil {
		return nil, errors.New("a Replicator needs an Advancer: without one a copy applies records and persists no pair record, so a restart replays its log from its seed cut (BUG-0752)")
	}
	if cfg.Window <= 0 {
		cfg.Window = defaultWindow
	}
	if cfg.ChunkRecords <= 0 {
		cfg.ChunkRecords = defaultChunkRecords
	}
	if cfg.ChunkBytes <= 0 {
		cfg.ChunkBytes = defaultChunkBytes
	}
	if cfg.AckTimeout <= 0 {
		cfg.AckTimeout = defaultAckTimeout
	}
	if cfg.DedupeWindow.Window == 0 {
		cfg.DedupeWindow.Window = defaultDedupeWindow
	}
	if cfg.DedupeWindow.MaxResult == 0 {
		cfg.DedupeWindow.MaxResult = maxResultBytes
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	ackBatch, _ := cfg.Link.(AckBatcher)
	writeBatch, _ := cfg.Link.(WriteBatcher)
	return &Replicator{
		cfg:        cfg,
		ackBatch:   ackBatch,
		writeBatch: writeBatch,
		ackLanes:   make(map[string]*ackLane),
		copyQ:      copyQueues{pairs: make(map[pairKey]*pairQueue), wake: make(chan struct{}, 1)},
		awaiting:   make(map[retryKey]struct{}),
		pairs:      make(map[pairKey]*pairSend),
		dirty:      make(map[pairKey]struct{}),
		current:    make(map[pairKey]struct{}),
		catching:   make(map[pairKey]*catchUpFlight),
		lanes:      make(map[string]*sendLane),
		failed:     make(map[retryKey]struct{}),
		pins:       make(map[uint64]PinnedFloor),
		health:     make(map[string]*peerHealth),
		wake:       make(chan struct{}, 1),
		done:       make(chan struct{}),

		suspectChanged: make(chan struct{}),
	}, nil
}

// Run ships whatever Push queued until ctx ends or Close is called.
func (r *Replicator) Run(ctx context.Context) {
	r.runMu.Lock()
	if r.closed {
		r.runMu.Unlock()
		return
	}
	r.wg.Add(1)
	r.runMu.Unlock()
	defer r.wg.Done()
	// The lanes use a context that ends with this loop, whether the caller's context ended or
	// Close was called, so shutdown does not wait on a send to a copy that has stopped
	// answering; Run returns only once they have.
	ctx, endLanes := context.WithCancel(ctx)
	r.ackMu.Lock()
	r.runCtx = ctx
	r.ackMu.Unlock()
	// Deferred first so it runs last: the workers that send acks and the lanes have ended by
	// then, and endLanes has already ended the context the ack lanes send under.
	defer r.closeAckLanes()
	defer r.laneWG.Wait()
	defer r.copyQ.workers.Wait()
	defer endLanes()
	r.startCopyWorkers(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-r.done:
			return
		case <-r.wake:
			// A failed send leaves the peer rewound and the pair dirty, so the next
			// Push or ack retries it. The loop only hands the work to the lanes: a copy
			// that does not answer must not delay the shipment to the others.
			r.dispatch(ctx)
		}
	}
}

// Close ends Run and waits for it.
func (r *Replicator) Close() {
	r.runMu.Lock()
	if !r.closed {
		r.closed = true
		close(r.done)
	}
	r.runMu.Unlock()
	r.wg.Wait()
}

func (r *Replicator) signal() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

// Push queues pos for every copy. It takes one short lock and never waits on a copy,
// so it is safe under the pair lock. The log is the outbound queue: nothing is copied.
func (r *Replicator) Push(slot uint16, db uint64, _ cluster.Position) {
	r.mu.Lock()
	r.dirty[pairKey{slot, db}] = struct{}{}
	r.mu.Unlock()
	r.signal()
}

// Resync queues every pair that has shipped to peer, so a copy that was away gets whatever
// it missed without waiting for the next write of each pair.
func (r *Replicator) Resync(peer string) {
	now := r.cfg.Now()
	r.mu.Lock()
	r.clearSuspectLocked(peer, now, "a probe saw it return")
	for key, ps := range r.pairs {
		if _, shipped := ps.peers[peer]; shipped {
			r.dirty[key] = struct{}{}
		}
	}
	r.mu.Unlock()
	r.signal()
}

// Enqueue is Replication's name for Push, with the log sequence the record was appended at and,
// for an epoch's opener, the previous end it carries (zero otherwise). The first one a pair gets is
// where a copy the stream has not shipped the pair to starts reading: without it a new pair would
// start at the first retained record and read the whole log to find its own. A copy that is further
// behind than that finds a gap and asks for a catch-up.
func (r *Replicator) Enqueue(slot uint16, db uint64, pos, prevEnd cluster.Position, cursor uint64) {
	r.mu.Lock()
	ps := r.pairLocked(pairKey{slot, db})
	if ps.hint == 0 {
		ps.hint = cursor
	}
	// The next owned record of the pair, queued after a read that passed all the earlier ones:
	// each copy's next read may start at it (the safety argument is on peerState.hint). After an
	// epoch move the pair's first write is the opener: without it here, every pair's first write of
	// the epoch read from where its copies last stopped, its old tail, through the whole log the
	// other pairs wrote since (BUG-0872).
	follows := !ps.lastPos.Zero() && (pos.Epoch == ps.lastPos.Epoch && pos.Seq == ps.lastPos.Seq+1 ||
		pos.Seq == 1 && ps.lastPos.Epoch < pos.Epoch && prevEnd == ps.lastPos)
	if ps.lastPos.Less(pos) {
		ps.lastPos = pos
	}
	previous := ps.lastCursor
	ps.lastCursor = max(ps.lastCursor, cursor)
	if follows && cursor > previous && cursor-previous > maxChainLead {
		ps.gapFrom, ps.gapTo = previous, cursor
	}
	if follows && cursor > previous {
		for _, st := range ps.peers {
			if st.started && st.next > previous {
				st.hint = cursor
			}
		}
	}
	r.mu.Unlock()
	r.Push(slot, db, pos)
}

func (r *Replicator) pairLocked(key pairKey) *pairSend {
	ps := r.pairs[key]
	if ps == nil {
		ps = &pairSend{peers: make(map[string]*peerState), changed: make(chan struct{})}
		r.pairs[key] = ps
	}
	return ps
}

// sendLane is the queue of one copy: the pairs still to ship to it, and whether a goroutine
// is draining them.
type sendLane struct {
	busy  bool
	queue map[pairKey]struct{}
}

// dispatch moves every dirty pair onto the lane of each of its copies and starts the lanes
// that are idle. It never waits for a send.
func (r *Replicator) dispatch(ctx context.Context) {
	r.mu.Lock()
	keys := make([]pairKey, 0, len(r.dirty))
	for key := range r.dirty {
		keys = append(keys, key)
	}
	clear(r.dirty)
	r.mu.Unlock()

	for _, key := range keys {
		if r.ownershipMoved(key) {
			continue
		}
		for _, peer := range r.cfg.Copies(key.slot, key.db) {
			r.mu.Lock()
			lane := r.lanes[peer]
			if lane == nil {
				lane = &sendLane{queue: make(map[pairKey]struct{})}
				r.lanes[peer] = lane
			}
			lane.queue[key] = struct{}{}
			start := !lane.busy
			lane.busy = true
			if start {
				r.laneWG.Add(1)
			}
			r.mu.Unlock()
			if start {
				go r.runLane(ctx, peer, lane)
			}
		}
	}
}

// ownershipMoved reports that another node owns the pair now, and forgets the pair's failed
// sends: they were this node's as owner, and the new owner ships its own log. A pair with
// no known owner (its copies are down) is still shipped.
func (r *Replicator) ownershipMoved(key pairKey) bool {
	owner, known := r.cfg.Owner(key.slot, key.db)
	if !known || owner == r.cfg.Self {
		return false
	}
	r.mu.Lock()
	if ps := r.pairs[key]; ps != nil {
		for id := range ps.peers {
			delete(r.failed, retryKey{key, id})
			delete(r.awaiting, retryKey{key, id})
		}
	}
	r.mu.Unlock()
	return true
}

// runLane ships the lane's queue to one copy until it is empty or ctx ends.
func (r *Replicator) runLane(ctx context.Context, peer string, lane *sendLane) {
	defer r.laneWG.Done()
	for {
		r.mu.Lock()
		if len(lane.queue) == 0 || ctx.Err() != nil {
			clear(lane.queue)
			lane.busy = false
			r.mu.Unlock()
			return
		}
		keys := make([]pairKey, 0, len(lane.queue))
		for key := range lane.queue {
			keys = append(keys, key)
		}
		clear(lane.queue)
		r.mu.Unlock()
		r.sortByStart(keys, peer)
		if r.writeBatch != nil {
			r.shipBatched(ctx, peer, keys)
			continue
		}
		chain := &laneChain{}
		for _, key := range keys {
			if ctx.Err() != nil {
				// A stop must not wait for a read of every pair left in the pass; the outer loop ends the lane.
				break
			}
			if err := r.shipPeer(ctx, key, peer, chain); err != nil {
				r.laneError(ctx, err)
			}
		}
	}
}

// laneChain is the log cursor a lane pass carries from one pair's read to the next. The pairs of a
// pass are read in log order, so the cursor one read leaves is at or just before where the next
// read starts, and the pass walks the log once instead of seeking it for every pair (BUG-0865).
// It is the lane's own and not the shared tail cache, because the lanes of two copies read at
// different places at once and the cache keeps only cursors that move forward.
type laneChain struct {
	cursor storage.WALCursor
	ok     bool
}

// sortByStart puts the pairs of a lane pass in the order of the log sequence each one's read
// starts at, so a chained cursor only ever moves forward. A pair whose start is unknown (a copy
// not started for it and no hint: it reads from the log's first record) sorts first.
func (r *Replicator) sortByStart(keys []pairKey, peer string) {
	type startedKey struct {
		key   pairKey
		start uint64
	}
	started := make([]startedKey, len(keys))
	r.mu.Lock()
	for index, key := range keys {
		started[index] = startedKey{key: key, start: r.readStartLocked(key, peer)}
	}
	r.mu.Unlock()
	slices.SortFunc(started, func(a, b startedKey) int {
		switch {
		case a.start != b.start:
			return cmp.Compare(a.start, b.start)
		case a.key.slot != b.key.slot:
			return cmp.Compare(a.key.slot, b.key.slot)
		}
		return cmp.Compare(a.key.db, b.key.db)
	})
	for index := range started {
		keys[index] = started[index].key
	}
}

// readStartLocked is the log sequence readShipment will start looking for the pair's next record
// at, 0 when it is the log's first record.
func (r *Replicator) readStartLocked(key pairKey, peer string) uint64 {
	ps := r.pairs[key]
	if ps == nil {
		return 0
	}
	st := ps.peers[peer]
	if st == nil || !st.started {
		return ps.hint
	}
	return ps.pastGap(max(st.next, st.hint))
}

// pastGap is where a read of the pair that would start at from may start instead: the far end of
// the pair's known gap when from lies inside it, from otherwise.
func (ps *pairSend) pastGap(from uint64) uint64 {
	if ps.gapFrom < from && from < ps.gapTo {
		return ps.gapTo
	}
	return from
}

// laneError reports a failed shipment unless the lane is ending anyway.
func (r *Replicator) laneError(ctx context.Context, err error) {
	if ctx.Err() == nil {
		r.report(err)
	}
}

// shipment is the records of one pair read for one copy and not yet sent.
type shipment struct {
	key        pairKey
	peer       string
	st         *peerState
	generation uint64
	records    []PairRecord
	more       bool // the read filled the window's room, so more may be waiting
}

// shipPeer sends the next records of one pair to one copy, in position order, while
// the window has room. The window is the only buffer: a full one stops the read. chain is the lane
// pass's carried cursor (see readShipmentChained).
func (r *Replicator) shipPeer(ctx context.Context, key pairKey, peer string, chain *laneChain) error {
	sh, ok, err := r.readShipmentChained(ctx, key, peer, chain)
	if err != nil || !ok {
		return err
	}
	return r.sendShipment(ctx, sh)
}

// readShipment is shipPeer up to and including the bookkeeping that follows a read (st.next,
// st.shipped, st.cursor). It sends nothing and appends nothing to
// st.inflight. ok is false when there is nothing to send, including every case in which shipPeer
// returned early with nothing to do (behind, window full, below the horizon, an empty read, a
// rewind during the read).
func (r *Replicator) readShipment(ctx context.Context, key pairKey, peer string) (sh shipment, ok bool, err error) {
	return r.readShipmentChained(ctx, key, peer, nil)
}

// readShipmentChained is readShipment for a lane pass: chain, when not nil, is the cursor the
// pass's previous read left, which this read continues on when it starts within maxChainLead
// entries after it, and which this read replaces with the one it leaves.
func (r *Replicator) readShipmentChained(ctx context.Context, key pairKey, peer string, chain *laneChain) (sh shipment, ok bool, err error) {
	// The tail cursor comes from the pair's published view, which takes no pair lock: the owner's write
	// holds that lock through its apply and queues the record inside it, so a lane that took the lock
	// would ship only after the apply. The view is the state at the last completed With, which can only
	// be older (lower) than the live one, and a lower bound is safe: a record the view does not show was
	// appended by a write whose Enqueue raises lastCursor to it (stage queues it before the apply moves
	// TailCursor) or by a copy apply that ShipTail or recoverPair queues at the tail after it. Deferred
	// is asked before the view is read: a pair that is not deferred now has finished its boot replay,
	// whose Withs, and so its published TailCursor, precede the release. A deferred pair is read to the
	// end of the log: its TailCursor lags the records it has not applied (BUG-0760).
	deferred := r.cfg.Deferred != nil && r.cfg.Deferred(key.slot, key.db)
	var tailCursor uint64
	if !deferred {
		tailCursor = r.cfg.Pairs.ReadView(key.slot, key.db).TailCursor
	}
	r.mu.Lock()
	ps := r.pairLocked(key)
	st := ps.peers[peer]
	if st == nil {
		st = &peerState{}
		ps.peers[peer] = st
	}
	if !st.started {
		start := ps.hint
		if start == 0 {
			// The pair is not one this node has written since it started: read from the first
			// retained record. FloorCursor takes the log's lock, so not under mu.
			r.mu.Unlock()
			start = r.cfg.FloorCursor(key.slot, key.db)
			r.mu.Lock()
		}
		if !st.started {
			st.started, st.next = true, start
			if ps.hint != 0 {
				st.anchor = start
			}
		}
	}
	room := r.cfg.Window - len(st.inflight)
	if st.behind || room <= 0 {
		if room <= 0 {
			st.behind = true
		}
		r.mu.Unlock()
		return shipment{}, false, nil
	}
	from, after, generation := st.next, st.shipped, st.generation
	// Every append of an owned pair is followed by an Enqueue that raises lastCursor, and a pair this
	// node took over is queued at its TailCursor, so no record the pair holds is above through unless
	// its own Enqueue is still to come and will dirty the pair again. Zero (a pair queued with Push
	// and no cursor known) reads to the end, as before.
	var through uint64
	if !deferred {
		through = max(ps.lastCursor, tailCursor)
	}
	var reuse *storage.WALCursor
	readFrom := from
	// A copy rewound into the pair's gap, or one whose read stopped at the gap's start, has no hint
	// that reaches past it: the gap itself says the entries up to its end are other pairs'.
	hint := max(st.hint, ps.pastGap(from))
	start := max(from, hint)
	if hint > from {
		// Everything between st.next and the hint is other pairs' (peerState.hint, pairSend.gapFrom).
		// Start from a remembered tail that is not before st.next, else seek the hint itself: the
		// cursor kept at st.next would walk the whole gap.
		readFrom = hint
		if tail, ok := r.tailBeforeLocked(hint); ok && tail.NextSeq() >= from {
			reuse, readFrom = &tail, tail.NextSeq()
		}
	} else if st.hasCursor && st.cursor.NextSeq() == from {
		c := st.cursor
		reuse = &c
	} else if tail, ok := r.tailBeforeLocked(from); ok {
		// A pair with no cursor of its own starts from a tail some pair's read left: the
		// entries between are other pairs' and are skipped, where seeking the log walks it.
		reuse, readFrom = &tail, tail.NextSeq()
	}
	if chain != nil && chain.ok {
		// Only entries of other pairs and ones this pair's reads have passed lie between the cursor and
		// start (the argument is the tail cache's), so reading on from it finds the same records.
		if next := chain.cursor.NextSeq(); next <= start && start-next <= maxChainLead && (reuse == nil || next > reuse.NextSeq()) {
			kept := chain.cursor
			reuse, readFrom = &kept, next
		}
	}
	gapFrom, gapTo := ps.gapFrom, ps.gapTo
	r.mu.Unlock()

	read, err := readPairRecords(ctx, r.cfg.Log, key, pairRead{from: readFrom, after: after, records: room, bytes: r.cfg.ChunkBytes, cursor: reuse, through: through, gapFrom: gapFrom, gapTo: gapTo})
	if chain != nil {
		chain.cursor, chain.ok = read.cursor, read.cursorOK
	}
	if errors.Is(err, storage.ErrBelowHorizon) {
		floor := r.cfg.FloorCursor(key.slot, key.db)
		// The copy's acknowledged record was truncated away. Ship from the first retained
		// record: the copy finds a gap, asks for a catch-up and is told the horizon, and the
		// catch-up's answer reseeds it (plan §1.1).
		r.mu.Lock()
		// The tails remembered below the horizon point into segments that are gone; a read that
		// started from one would find the horizon again and again.
		r.tails = slices.DeleteFunc(r.tails, func(tail storage.WALCursor) bool { return tail.NextSeq() < floor })
		if st.generation == generation {
			st.fromFloor, st.next, st.hasCursor, st.hint = true, floor, false, 0
			st.generation++
		}
		r.dirty[key] = struct{}{}
		r.mu.Unlock()
		r.signal()
		return shipment{}, false, nil
	}
	if err != nil {
		return shipment{}, false, fmt.Errorf("reading the log for slot %d of database %d to push to %s: %w", key.slot, key.db, peer, err)
	}
	if len(read.records) == 0 {
		r.mu.Lock()
		if st.generation == generation {
			st.next = max(st.next, read.next)
			st.cursor, st.hasCursor = read.cursor, read.cursorOK
		}
		r.noteTailLocked(read)
		r.mu.Unlock()
		return shipment{}, false, nil
	}

	r.mu.Lock()
	if st.generation != generation {
		// An ack rewound this copy while the log was read: the batch starts at a stale
		// cursor, so it is dropped and the pair is read again.
		r.dirty[key] = struct{}{}
		r.mu.Unlock()
		r.signal()
		return shipment{}, false, nil
	}
	st.next, st.shipped = read.next, read.records[len(read.records)-1].Pos
	st.cursor, st.hasCursor = read.cursor, read.cursorOK
	r.noteTailLocked(read)
	// No deadline is armed here: nothing is in flight until the frames are sent, and Retry clears
	// a deadline whose copy has nothing in flight (BUG-0866). armAckLocked runs where they go out.
	more := len(read.records) == room
	r.mu.Unlock()
	return shipment{key: key, peer: peer, st: st, generation: generation, records: read.records, more: more}, true, nil
}

// sendShipment is the rest of shipPeer: the per-record loop (append to st.inflight, Link.Write,
// stop when st.rearmAt is set, rewindAfterSendFailure on an error) and the final re-dirty when
// more.
func (r *Replicator) sendShipment(ctx context.Context, sh shipment) error {
	key, peer, st, generation, more := sh.key, sh.peer, sh.st, sh.generation, sh.more
	committed := r.cfg.Committed()
	for _, rec := range sh.records {
		// A frame is in flight from the moment it is sent, not before: a copy that answers
		// not-applied to the first leaves the rest of the batch unsent and uncounted. It is
		// counted before the send, so an ack that overtakes the reply finds it.
		r.mu.Lock()
		if st.generation != generation {
			r.mu.Unlock()
			return nil
		}
		st.inflight = append(st.inflight, sentFrame{pos: rec.Pos, cursor: rec.Cursor})
		r.armAckLocked(retryKey{key, peer}, st)
		r.mu.Unlock()
		frame := WriteFrame{Slot: key.slot, DB: key.db, From: r.cfg.Self, Epoch: committed, PairRecord: rec}
		if err := r.cfg.Link.Write(ctx, peer, frame); err != nil {
			r.rewindAfterSendFailure(key, peer)
			r.noteSendFailure(ctx, peer, err)
			return fmt.Errorf("sending position %s of slot %d to %s: %w", rec.Pos, key.slot, peer, err)
		}
		r.noteSendSuccess(peer)
		r.mu.Lock()
		waiting := !st.rearmAt.IsZero()
		r.mu.Unlock()
		if waiting {
			// The copy answered not-applied or refused: the rest of the batch would only
			// be refused too. Retry re-arms it after its backoff.
			return nil
		}
	}
	if more {
		r.mu.Lock()
		r.dirty[key] = struct{}{}
		r.mu.Unlock()
		r.signal()
	}
	return nil
}

// armAckLocked starts the copy's ack deadline when a frame goes in flight and none is running. It
// belongs here and not at the read: a batched pass reads every pair before it sends any, and a
// Retry between the two would clear a deadline armed at the read (BUG-0866). r.mu is held.
func (r *Replicator) armAckLocked(rk retryKey, st *peerState) {
	if st.ackBy.IsZero() {
		st.ackBy = r.cfg.Now().Add(r.cfg.AckTimeout)
		r.awaiting[rk] = struct{}{}
	}
}

// rewindAfterSendFailure forgets what was in flight to peer, so the next flush ships
// again from the last acknowledged record. The copy refuses a duplicate harmlessly.
func (r *Replicator) rewindAfterSendFailure(key pairKey, peer string) {
	floor := func() uint64 { return r.cfg.FloorCursor(key.slot, key.db) }
	r.mu.Lock()
	defer r.mu.Unlock()
	if st := r.pairs[key].peers[peer]; st != nil {
		rewindLocked(st, floor)
		st.sendFailed = true
		st.ackBy = time.Time{}
		delete(r.awaiting, retryKey{key, peer})
	}
	r.dirty[key] = struct{}{}
	r.failed[retryKey{key, peer}] = struct{}{}
}

// Retry queues every copy of a pair whose last send failed, re-arms a copy that answered
// "not applied" or refused once its backoff has passed, and ships again to a copy that has
// not acknowledged what is in flight to it within the ack deadline. A failed send leaves the
// pair rewound and a lost ack leaves it waiting, with nothing to wake either, so the caller
// (the supervisor, each heartbeat) calls this; the transport's backoff makes an attempt on a
// copy that is still away cost nothing. It visits only those copies, never every pair.
func (r *Replicator) Retry() {
	now := r.cfg.Now()
	r.mu.Lock()
	woke := false
	copies := make(map[pairKey][]string)
	currentCopies := func(key pairKey) []string {
		if got, seen := copies[key]; seen {
			return got
		}
		got := r.cfg.Copies(key.slot, key.db)
		copies[key] = got
		return got
	}
	for rk := range r.failed {
		st := r.peerStateLocked(rk)
		if st == nil || !slices.Contains(currentCopies(rk.pair), rk.peer) {
			// A copy that left the copy set takes its state and its failure with it.
			r.forgetPeerLocked(rk)
			continue
		}
		if !st.rearmAt.IsZero() && !now.Before(st.rearmAt) {
			st.rearmAt, st.behind, st.staleAcks = time.Time{}, false, 0
			rewindLocked(st, func() uint64 { return r.cfg.FloorCursor(rk.pair.slot, rk.pair.db) })
		}
		r.dirty[rk.pair] = struct{}{}
		woke = true
	}
	for rk := range r.awaiting {
		st := r.peerStateLocked(rk)
		switch {
		case st == nil || !slices.Contains(currentCopies(rk.pair), rk.peer):
			r.forgetPeerLocked(rk)
		case len(st.inflight) == 0 || !st.rearmAt.IsZero():
			// Nothing in flight, or the copy refused and the re-arm owns the wait.
			st.ackBy = time.Time{}
			delete(r.awaiting, rk)
		case !now.Before(st.ackBy):
			st.ackBy, st.behind, st.refused = time.Time{}, false, false
			rewindLocked(st, func() uint64 { return r.cfg.FloorCursor(rk.pair.slot, rk.pair.db) })
			delete(r.awaiting, rk)
			r.dirty[rk.pair] = struct{}{}
			woke = true
		}
	}
	r.sweepHealthLocked(now)
	pending := woke || len(r.failed) > 0
	r.mu.Unlock()
	if pending {
		r.signal()
	}
}

func (r *Replicator) peerStateLocked(rk retryKey) *peerState {
	ps := r.pairs[rk.pair]
	if ps == nil {
		return nil
	}
	return ps.peers[rk.peer]
}

// forgetPeerLocked drops a copy's send state and every wait kept for it.
func (r *Replicator) forgetPeerLocked(rk retryKey) {
	if ps := r.pairs[rk.pair]; ps != nil {
		delete(ps.peers, rk.peer)
	}
	delete(r.failed, rk)
	delete(r.awaiting, rk)
}

// settlePeerLocked takes the copy out of the retry set when it is no longer failing.
func (r *Replicator) settlePeerLocked(rk retryKey, st *peerState) {
	if !st.sendFailed && st.rearmAt.IsZero() {
		delete(r.failed, rk)
	}
}

// retryCeiling caps the wait between pushes to a copy that keeps answering not-applied.
const retryCeiling = 30 * time.Second

func (r *Replicator) retryBackoff() time.Duration {
	if r.cfg.RetryBackoff > 0 {
		return r.cfg.RetryBackoff
	}
	return 200 * time.Millisecond
}

// tailCursors is how many tail cursors are remembered.
const tailCursors = 64

// maxTailLead bounds how many log entries a read may skip from the shared tail cursor to a
// pair's record; past it the log is sought. Seeking is not always cheaper than skipping: it scans
// its segment's entries from the segment's start, which is up to 64 MiB.
const maxTailLead = 4096

// maxChainLead is the same bound for the cursor a lane pass carries between pairs. Walking costs
// about a microsecond an entry and a seek tens of milliseconds on a large segment, so the break-even
// is a few tens of thousands of entries.
const maxChainLead = 16384

// noteTailLocked remembers where a read that reached the log's tail stopped, so the next pair
// to be shipped for the first time starts close to its record.
func (r *Replicator) noteTailLocked(read pairReadResult) {
	if !read.cursorOK {
		return
	}
	if n := len(r.tails); n > 0 && read.cursor.NextSeq() <= r.tails[n-1].NextSeq() {
		return
	}
	if len(r.tails) == tailCursors {
		r.tails = append(r.tails[:0], r.tails[1:]...)
	}
	r.tails = append(r.tails, read.cursor)
}

// tailBeforeLocked returns the latest remembered tail cursor at or before from and within
// maxTailLead entries of it. Several are kept because pairs written at once read to different
// tails, and a cursor can only move forward.
func (r *Replicator) tailBeforeLocked(from uint64) (storage.WALCursor, bool) {
	for i := len(r.tails) - 1; i >= 0; i-- {
		if next := r.tails[i].NextSeq(); next <= from {
			return r.tails[i], from-next <= maxTailLead
		}
	}
	return storage.WALCursor{}, false
}

// rewindLocked forgets what was in flight and resumes after the last acknowledged record. floor
// is asked only when that record's log sequence is unknown, because on a real node it takes the
// log's lock and reads its directory.
func rewindLocked(st *peerState, floor func() uint64) {
	st.inflight = nil
	st.hasCursor, st.hint = false, 0
	st.shipped = st.acked
	if st.ackedCursor > 0 && !st.fromFloor {
		st.next = st.ackedCursor + 1
	} else if st.anchor > 0 && !st.fromFloor {
		// Nothing acknowledged yet: resume at the record the pair's first read started at, not
		// at the log's first record, so a refused or unreachable copy does not make the
		// restarted stream re-read the pair's whole retained history (BUG-0746).
		st.next = st.anchor
	} else {
		st.next = floor()
	}
	st.generation++
}

// Two rules about acknowledgements (seeding-completion plan §1.4). A copy can reset itself
// (the Seeder reseeds it) and the owner is told nothing but the acks that follow, so the owner's
// view of the copy, st.acked, has to be lowered by the right ack and by no other.
//
// Rule 1: only an applied ack carrying a position can change st.acked. A refusal or a
// not-applied ack never touches it, whatever position it carries: a gap refusal carries the
// copy's applied position (replicate_apply.go:ackFor), and it can trail a catch-up's
// cumulative applied ack. An applied ack below st.acked is evidence that the copy's record moved
// backwards, which only a reset does, when the copy's previous answer was not applied
// (st.refused is still set): then the owner takes the lower position and rewinds. If the
// previous answer was applied, it is a catch-up ack that crossed a later live ack and is
// ignored. A zero position never lowers. Residual: a stale cumulative catch-up ack that arrives
// while st.refused is set also lowers, which costs a transient undercount and one rewind and
// never an overcount.
//
// Rule 2: st.holds is set by an applied ack and cleared by any other answer, positionless ones
// included, and ackedLocked counts a copy only while it is set. A reset copy answers not
// applied from its first pushed frame on, so a retried request answered from the dedupe window
// does not count it for a position it no longer holds. The price: a healthy copy that refuses
// one frame on a full queue is left out of a retried request's copies_confirmed until its next
// applied ack, an under-report of a copy that holds the data and never an over-report.
//
// Positions are compared lexicographically, so an ack from a later epoch counts for every
// position of an earlier one. That is sound because a position in a later epoch implies the
// copy applied the boundary before it (the opener carries prevEnd and a copy accepts it only at
// that applied position); peers are trusted before v1.0.
// OnWriteAck records a copy's cumulative acknowledgement. An applied ack frees window
// and resumes a copy that was behind; a refusal or a not-applied ack stops the push to
// that copy until it answers applied, and never resets it.
func (r *Replicator) OnWriteAck(ack WriteAck) {
	floor := func() uint64 { return r.cfg.FloorCursor(ack.Slot, ack.DB) }
	key := pairKey{ack.Slot, ack.DB}
	r.mu.Lock()
	ps := r.pairLocked(key)
	st := ps.peers[ack.From]
	if st == nil {
		// Not started: the first ship picks where to read from.
		st = &peerState{}
		ps.peers[ack.From] = st
	}
	rk := retryKey{key, ack.From}
	if ack.Outcome != AckApplied {
		st.holds = false
		if !st.rearmAt.IsZero() && st.staleAcks > 0 {
			// One of the answers the copy still owed for frames it had accepted when it first
			// refused (staleAcks counts the frames sent and unanswered): the wait is running.
			st.staleAcks--
			r.mu.Unlock()
			return
		}
		if st.rearmAt.IsZero() {
			st.staleAcks = max(len(st.inflight)-1, 0)
		}
		st.behind, st.refused = true, true
		// A refusal asked for a catch-up, whose ack ends the wait; a copy that was merely
		// not applied is pushed to again after the backoff. Either way the pair stays in
		// the retry set until an applied ack arrives.
		wait := r.retryBackoff() << min(st.backoffs, 16)
		if ack.Outcome == AckRefused {
			wait *= 10
		}
		st.backoffs++
		st.rearmAt = r.cfg.Now().Add(min(wait, retryCeiling))
		r.failed[rk] = struct{}{}
		r.mu.Unlock()
		return
	}
	st.rearmAt, st.fromFloor, st.backoffs, st.sendFailed, st.staleAcks = time.Time{}, false, 0, false, 0
	st.holds = true
	r.noteAppliedAckLocked(ack.From, r.cfg.Now())
	r.settlePeerLocked(rk, st)
	if !ack.Pos.Zero() && ack.Pos.Less(st.acked) && st.refused {
		// The copy was answering not applied and now acks below what it acked before: its record
		// moved backwards, so it was reset. Take the lower position; the cursor of the old one
		// says nothing about the new, and the rewind below ships from the pair's first read.
		st.acked, st.ackedCursor = ack.Pos, 0
	}
	progressed := st.acked.Less(ack.Pos)
	if progressed {
		st.acked = ack.Pos
		keep := st.inflight[:0]
		for _, frame := range st.inflight {
			if ack.Pos.Less(frame.pos) {
				keep = append(keep, frame)
				continue
			}
			st.ackedCursor = frame.cursor
		}
		st.inflight = keep
		if st.shipped.Less(ack.Pos) {
			st.shipped = ack.Pos
		}
	}
	if st.refused {
		// The frames shipped before the refusal never applied: read again from the
		// acknowledged record.
		rewindLocked(st, floor)
	}
	st.behind, st.refused = false, false
	if len(st.inflight) == 0 {
		st.ackBy = time.Time{}
		delete(r.awaiting, rk)
	} else if progressed {
		st.ackBy = r.cfg.Now().Add(r.cfg.AckTimeout)
	}
	r.dirty[key] = struct{}{}
	close(ps.changed)
	ps.changed = make(chan struct{})
	r.mu.Unlock()
	r.signal()
}

// Acked is how many copies acked a position at or past pos.
func (r *Replicator) Acked(slot uint16, db uint64, pos cluster.Position) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.ackedLocked(pairKey{slot, db}, pos)
}

func (r *Replicator) ackedLocked(key pairKey, pos cluster.Position) int {
	ps := r.pairs[key]
	if ps == nil {
		return 0
	}
	// Only a node that holds a copy of the pair now counts: a departed copy's old ack, or
	// an ack from a node that never held one, confirms nothing.
	current := r.cfg.Copies(key.slot, key.db)
	count := 0
	for id, st := range ps.peers {
		if st.holds && !st.acked.Less(pos) && slices.Contains(current, id) {
			count++
		}
	}
	return count
}

// Await blocks until copies have acked pos or ctx ends, and returns how many had. It also
// returns when no copy that has not acked is both current and not suspect: what is still missing
// cannot answer in time (a peer the breaker marked, or a member committed down and so no longer a
// copy), and the count it returns is still the real acks, so the answer built from it is no
// stronger than the wait would have made it (the entry ring degraded ack breaker spec).
func (r *Replicator) Await(ctx context.Context, slot uint16, db uint64, pos cluster.Position, copies int) int {
	key := pairKey{slot, db}
	for {
		now := r.cfg.Now()
		r.mu.Lock()
		count := r.ackedLocked(key, pos)
		if count >= copies || r.waitableLocked(key, pos, now) == 0 {
			r.mu.Unlock()
			return count
		}
		changed, suspects := r.pairLocked(key).changed, r.suspectChanged
		r.mu.Unlock()
		select {
		case <-changed:
		case <-suspects:
		case <-ctx.Done():
			return r.Acked(slot, db, pos)
		}
	}
}

// NoteDrained records that a HANDOFF_READY drain covers every position up to upTo.
func (r *Replicator) NoteDrained(slot uint16, db uint64, upTo cluster.Position) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ps := r.pairLocked(pairKey{slot, db})
	if ps.drained.Less(upTo) {
		ps.drained = upTo
	}
}

// Covered reports a drain that covers pos, or an ack at or past pos from the copy that
// owns the pair now.
func (r *Replicator) Covered(slot uint16, db uint64, pos cluster.Position) bool {
	key := pairKey{slot, db}
	owner, known := r.cfg.Owner(slot, db)
	r.mu.Lock()
	defer r.mu.Unlock()
	ps := r.pairs[key]
	if ps == nil {
		return false
	}
	if !ps.drained.Less(pos) && !ps.drained.Zero() {
		return true
	}
	if st := ps.peers[owner]; known && st != nil {
		return !st.acked.Less(pos)
	}
	return false
}
