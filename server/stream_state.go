package server

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The stream: this node's log, delivered to one peer (spec §19.3.5).
//
// One goroutine, peer.runSender, sends this node's entries to a peer. It probes
// the peer before it reads anything, replays the log from what the peer has
// acknowledged, and, once a read has reached the log's synced tail, records a
// boundary and starts taking live entries from the queue in one hold of
// streamMu. It ships the log up to the boundary before it takes anything from
// the queue. Any failure stalls the stream, and the sender starts again from
// the log when the peer answers.
//
// A writer never waits for a peer. After its local apply, holding its
// ship-order ticket, it routes the entry: if the stream is streaming the entry
// goes on the peer's queue without blocking, and a full queue stalls the stream
// instead of the writer. In any other state the writer does nothing, because
// the entry is in the log and the sender delivers it from there.
//
// Why nothing is lost or reordered (I-ORD): a replay reads a prefix of the
// synced log in sequence order; shipOrder puts entries on the queue in
// sequence order and the queue never blocks. An entry routed before the stream
// became streaming was synced before it was routed, so it is at or below the
// boundary and the replay carries it; one routed after is queued, and if it is
// at or below the boundary too, the receiver skips the second copy as already
// applied. A stall empties the queue under the lock a writer queues under, so
// nothing stale is left on it, and everything above the peer's position is read
// again.
//
// streamMu is a leaf: nothing is taken under it but the cluster's
// streamChangedMu, the peer's ackMu and the log's sync mutex, and it is never
// held across a network call, a log read or a store write.

// streamState is where one peer's stream stands.
type streamState int

const (
	// streamConnecting is the state every stream starts in, and the one a
	// membership adoption returns it to: the peer's entries come from the log.
	streamConnecting streamState = iota
	// streamStreaming is a stream whose log has been replayed up to a boundary;
	// entries routed since then are queued.
	streamStreaming
	// streamStalled is a stream that failed, was refused, or could not queue an
	// entry. Its entries come from the log once the peer answers again.
	streamStalled
)

func (s streamState) String() string {
	switch s {
	case streamConnecting:
		return "connecting"
	case streamStreaming:
		return "streaming"
	default:
		return "stalled"
	}
}

// replRetryCeiling is the longest a stalled sender waits before it tries its
// peer again. The wait starts at replRetryInterval and doubles.
const replRetryCeiling = 30 * time.Second

// errStreamLeft is the sender's own signal that its stream changed state under
// it — a stall, or a membership adoption — and that it must start again from
// the log at once.
var errStreamLeft = errors.New("the stream changed state while the sender was serving it")

// beforeReplayShipHook, when set, runs before the sender ships each frame it
// read from the log. Tests use it to hold a replay while live writes are made.
// Nil in production.
var beforeReplayShipHook atomic.Pointer[func(peerID string, frame []ReplEntry)]

// beforeShipHook, when set, runs before a sender sends a frame of entries, and
// the function it returns runs once that ship has returned. Tests use it to
// hold every sender at a checkpoint and to know when the ships already under
// way have finished (T-106). The wait in it is not part of the ship's timeout.
// Nil in production.
var beforeShipHook atomic.Pointer[func(peerID string) (shipped func())]

// minReplShipTimeout is the floor under replShipTimeout: a write-concern
// timeout bounds a client's wait, not a peer's apply of a replication frame.
const minReplShipTimeout = 2 * time.Second

// replShipTimeout bounds one ship and one probe (handshake, advance): half the
// write-concern timeout, so a ship to a silent peer gives up while the write
// waiting on it can still be answered against the copies that remain (spec
// §19.3.5, S-7), but never under minReplShipTimeout (BUG-0245).
func (c *Cluster) replShipTimeout() time.Duration {
	return max(c.wcTimeout/2, minReplShipTimeout)
}

// route offers an entry to the peer's stream. It never blocks.
func (p *peer) route(entry ReplEntry) {
	p.streamMu.Lock()
	defer p.streamMu.Unlock()
	p.noteOwedLocked()
	if p.state != streamStreaming {
		return
	}
	select {
	case p.sendCh <- pendingEntry{entry: entry}:
		p.queuedThrough = max(p.queuedThrough, entry.Seq)
	default:
		p.stallLocked("queue full")
	}
}

// inSync reports whether the peer counts toward a write concern (spec
// §19.3.11).
func (p *peer) inSync() bool {
	p.streamMu.Lock()
	defer p.streamMu.Unlock()
	return p.inSyncLocked()
}

// inSyncLocked reports whether the stream is in the in-sync set: it has not
// left it, or it has returned to it. A stream leaves on every stall and
// returns only once its replay has shipped the log up to the boundary it
// entered streaming at (openStream, returnToSyncLocked). Streaming alone is not
// enough: a replica that comes back with a backlog enters streaming when a read
// reaches the tail, and the range up to the boundary can still be thousands of
// entries, which a write waiting on it would wait out to its timeout
// (BUG-0602). A stream reset to connecting by a membership adoption, without a
// failure, stays in. The caller holds streamMu.
func (p *peer) inSyncLocked() bool {
	return p.outSince.IsZero()
}

// stall moves a streaming stream to stalled. A stream that is not streaming is
// already being read from the log and is left as it is.
func (p *peer) stall(reason string) {
	p.streamMu.Lock()
	defer p.streamMu.Unlock()
	if p.state == streamStreaming {
		p.stallLocked(reason)
	}
}

// stallLocked sets the stream stalled, empties the queue and wakes the sender.
// The caller holds streamMu.
func (p *peer) stallLocked(reason string) {
	p.leaveInSyncLocked(reason, "")
	p.setStateLocked(streamStalled, reason)
	p.wakeSender()
}

// resetStream returns the stream to connecting and wakes the sender: every
// entry is derived from the log again, under the ring as it is now.
func (p *peer) resetStream(reason string) {
	p.streamMu.Lock()
	defer p.streamMu.Unlock()
	p.setStateLocked(streamConnecting, reason)
	p.wakeSender()
}

// reopenUnlessOpening makes a streaming stream open again with a handshake,
// and wakes a sender that is opening it already: its handshake is on the way.
func (p *peer) reopenUnlessOpening(reason string) {
	p.streamMu.Lock()
	defer p.streamMu.Unlock()
	if p.state == streamStreaming {
		p.setStateLocked(streamConnecting, reason)
	}
	p.wakeSender()
}

// resetStreams returns every stream to connecting (spec §19.3.5, S-8). An
// entry on a queue was addressed under the ring that has just been replaced:
// what the new ring no longer addresses to a peer is not shipped to it, and
// what this node no longer owns is not shipped at all.
func (c *Cluster) resetStreams(reason string) {
	c.peersMu.RLock()
	peers := make([]*peer, 0, len(c.peers))
	for _, p := range c.peers {
		peers = append(peers, p)
	}
	c.peersMu.RUnlock()
	for _, p := range peers {
		p.resetStream(reason)
	}
}

// markFailed records the sender's own failure. It does not wake the sender:
// the sender is the caller, and is about to wait before it tries again.
func (p *peer) markFailed(err error) {
	if errors.Is(err, storage.ErrBelowHorizon) {
		p.warnHorizon(err.Error())
	}
	var awaiting *awaitingCopyRefusal
	bucket := ""
	if errors.As(err, &awaiting) {
		bucket = awaiting.bucket
	}
	p.streamMu.Lock()
	defer p.streamMu.Unlock()
	p.refusedForGood = nil
	if errors.Is(err, errHandshakeRefusedForGood) {
		p.refusedForGood = err
	}
	var silent *peerSilentError
	if !p.everStreamed && errors.As(err, &silent) {
		p.leaveQuietlyLocked()
	} else {
		p.leaveInSyncLocked(err.Error(), bucket)
	}
	p.setStateLocked(streamStalled, err.Error())
}

// handshakeRefusedForGood returns the refusal that has stopped this stream
// from opening and that no retry clears, or nil when the last attempt was not
// refused that way.
func (p *peer) handshakeRefusedForGood() error {
	p.streamMu.Lock()
	defer p.streamMu.Unlock()
	return p.refusedForGood
}

// leftInSyncHook, when set, runs each time a stream leaves the in-sync set,
// with the sending node, the peer and the reason, under the peer's streamMu: a
// test counts departures a sampler of inSync could miss between two samples.
// It must not block or take streamMu. Nil in production.
var leftInSyncHook atomic.Pointer[func(from, to, reason string)]

// leaveInSyncLocked logs a stream's leaving the in-sync set, once: one WARN
// with the peer, the reason and, for a refusal that names one, the bucket
// (spec §19.3.13). A stream that is already out — stalled, or reset to
// connecting by an adoption since — logs its later failures at Debug, so a
// peer that stays down is one line however often the sender retries. The
// caller holds streamMu.
func (p *peer) leaveInSyncLocked(reason, bucket string) {
	if !p.outSince.IsZero() && !p.outQuiet {
		p.c.logger.Debug("replication to a peer is still stalled", "node_id", p.node.ID, "reason", reason)
		return
	}
	if p.outSince.IsZero() {
		p.outSince = time.Now()
		p.outReplayed = 0
	}
	p.outQuiet = false
	if hook := leftInSyncHook.Load(); hook != nil {
		(*hook)(p.c.self.ID, p.node.ID, reason)
	}
	p.c.noteReplicationTransition()
	attrs := []any{"node_id", p.node.ID, "reason", reason}
	if bucket != "" {
		attrs = append(attrs, "bucket", bucket)
	}
	p.c.logger.Warn("replication to a peer left the in-sync set; it counts toward no write concern until it returns, "+
		"and what it misses is sent from this node's log when it answers", attrs...)
}

// leaveQuietlyLocked takes a never-streamed stream out of the in-sync set
// without announcing it: its first handshake was not answered, so it was never
// in the set to leave (BUG-0731). It stays out for write concern and the
// replication view, since a copy that cannot answer must not be waited for; only
// the departure and the matching return are not logged. A later failure that
// is not silence (a refusal) announces it. The caller holds streamMu.
func (p *peer) leaveQuietlyLocked() {
	if !p.outSince.IsZero() {
		return
	}
	p.outSince = time.Now()
	p.outReplayed = 0
	p.outQuiet = true
	p.c.noteReplicationTransition()
}

// returnToSyncLocked logs, once, that a stream that had left the in-sync set
// has replayed its log up to its boundary: how long it was out and how many
// entries were replayed to it meanwhile. The caller holds streamMu.
func (p *peer) returnToSyncLocked() {
	if p.outSince.IsZero() {
		return
	}
	if !p.outQuiet {
		p.c.logger.Info("replication to a peer is back in sync", "node_id", p.node.ID,
			"out_for", time.Since(p.outSince).Round(time.Millisecond), "entries_replayed", p.outReplayed)
	}
	p.outQuiet = false
	p.outSince = time.Time{}
	p.outReplayed = 0
	p.c.noteReplicationTransition()
}

// noteReplayed counts entries a replay delivered while the stream is out.
func (p *peer) noteReplayed(entries int) {
	p.streamMu.Lock()
	defer p.streamMu.Unlock()
	p.outReplayed += uint64(entries)
}

// setStateLocked records a transition. Every transition empties the queue,
// because an entry on it was routed under the state that is ending, and starts
// a new generation, so a sender working for the old one notices.
func (p *peer) setStateLocked(state streamState, reason string) {
	p.state = state
	p.stallReason = reason
	p.generation++
	p.boundaryShipped = false
	p.queuedThrough = 0
	p.advanceWanted = false
	// Non-blocking receives: the sender may take the last entry between a
	// length check and a receive, and this goroutine holds streamMu.
	for drained := false; !drained; {
		select {
		case <-p.sendCh:
		default:
			drained = true
		}
	}
	p.c.noteStreamChanged()
}

// wakeSender asks the sender to look at its stream now. A wake that is already
// pending covers this one.
func (p *peer) wakeSender() {
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

// currentGeneration reports the stream's generation and drops a pending wake:
// the caller is about to do what any wake asks for.
func (p *peer) currentGeneration() uint64 {
	select {
	case <-p.wake:
	default:
	}
	p.streamMu.Lock()
	defer p.streamMu.Unlock()
	return p.generation
}

// inGeneration reports whether the stream has not changed state since
// generation.
func (p *peer) inGeneration(generation uint64) bool {
	p.streamMu.Lock()
	defer p.streamMu.Unlock()
	return p.generation == generation
}

// noteStreamChanged wakes everything waiting for any stream to change state.
func (c *Cluster) noteStreamChanged() {
	c.streamChangedMu.Lock()
	defer c.streamChangedMu.Unlock()
	close(c.streamChanged)
	c.streamChanged = make(chan struct{})
}

// streamChangedSignal returns a channel closed at the next change of any
// stream's state.
func (c *Cluster) streamChangedSignal() <-chan struct{} {
	c.streamChangedMu.Lock()
	defer c.streamChangedMu.Unlock()
	return c.streamChanged
}

// wakeSenderOf wakes the sender of the stream to a member, if this node has
// one.
func (c *Cluster) wakeSenderOf(nodeID string) {
	c.peersMu.RLock()
	p, known := c.peers[nodeID]
	c.peersMu.RUnlock()
	if known {
		p.wakeSender()
	}
}

// runSender is the only goroutine that sends this node's entries to the peer.
// It opens the stream, serves it until it fails or changes state, and starts
// again: at once after a change of state, which a stall or an adoption makes and
// which a successful attempt answers, and after a wait that doubles from
// replRetryInterval to replRetryCeiling after a failure. A wake ends the wait.
func (p *peer) runSender() {
	defer p.c.storeWG.Done()
	p.c.logger.Debug("replication sender started", "node_id", p.node.ID)
	retry := replRetryInterval
	for {
		generation, err := p.openStream()
		if err == nil {
			retry = replRetryInterval
			err = p.serveQueue(generation)
		}
		if p.stopping() {
			return
		}
		if errors.Is(err, errStreamLeft) {
			continue
		}
		p.markFailed(err)
		select {
		case <-p.wake:
		case <-time.After(retry):
		case <-p.stopCh:
			return
		case <-p.c.ctx.Done():
			return
		}
		retry = min(2*retry, replRetryCeiling)
	}
}

// stopping reports whether the peer was closed or the cluster is stopping. The
// context counts because a peer added after Stop listed the peers to close is
// never closed, and its sender is waited for without a bound.
func (p *peer) stopping() bool {
	select {
	case <-p.stopCh:
		return true
	case <-p.c.ctx.Done():
		return true
	default:
		return false
	}
}

// openStream opens the stream with a handshake, replays the log to the peer,
// and makes the stream streaming. It returns the generation the stream is
// streaming in. Nothing is read from the log until the peer has answered: a
// peer that is down costs this node no I/O (spec §19.3.5, S-5).
func (p *peer) openStream() (uint64, error) {
	generation := p.currentGeneration()
	if err := p.handshake(); err != nil {
		return 0, fmt.Errorf("opening the stream to %s before replaying the log to it: %w", p.node.ID, err)
	}
	replay, err := p.newReplay()
	if err != nil {
		return 0, err
	}
	defer replay.stranded.warn(p.c.logger, p.node.ID)
	for reached := false; !reached; {
		if p.stopping() {
			return 0, errStreamLeft
		}
		if reached, err = replay.ship(p.c.s.wal.SyncedSequence(), generation); err != nil {
			return 0, err
		}
	}
	generation, boundary, entered := p.enterStreaming(generation)
	if !entered {
		return 0, errStreamLeft
	}
	for reached := false; !reached; {
		if p.stopping() {
			return 0, errStreamLeft
		}
		if reached, err = replay.ship(boundary, generation); err != nil {
			return 0, err
		}
	}
	p.streamMu.Lock()
	defer p.streamMu.Unlock()
	if p.generation != generation {
		return 0, errStreamLeft
	}
	p.boundaryShipped = true
	p.settleIfIdleLocked()
	p.returnToSyncLocked()
	return generation, nil
}

// enterStreaming records the boundary and makes the stream streaming, in one
// hold of streamMu, unless the stream changed state since generation. From the
// moment it returns, writers queue; everything synced before it is at or below
// the boundary.
func (p *peer) enterStreaming(generation uint64) (uint64, uint64, bool) {
	p.streamMu.Lock()
	defer p.streamMu.Unlock()
	if p.generation != generation {
		return 0, 0, false
	}
	boundary := p.c.s.wal.SyncedSequence()
	p.refusedForGood = nil
	p.everStreamed = true
	p.setStateLocked(streamStreaming, "")
	return p.generation, boundary, true
}

// peerSilentError marks a handshake that got no answer (a refused dial, a
// timeout), as opposed to one the peer answered with a refusal. A stream that
// has never streamed and gets no answer was never in the in-sync set, so its
// failure is not a departure (BUG-0731); a refusal still is, because a peer
// that refuses must not count toward a write concern. The text is the wrapped
// error's, unchanged.
type peerSilentError struct{ err error }

func (e *peerSilentError) Error() string { return e.err.Error() }
func (e *peerSilentError) Unwrap() error { return e.err }

// open sends one handshake push and returns the position the peer answers
// with.
func (p *peer) open(ctx context.Context, stream *ReplStream) (ReplPosition, error) {
	resp, err := p.call(ctx, transport.LaneControl, &ReplPushRequest{
		Token:    p.c.localToken(),
		FromNode: p.c.self.ID,
		Epoch:    p.c.Epoch(),
		Stream:   stream,
	})
	if err != nil {
		return ReplPosition{}, &peerSilentError{err: err}
	}
	if !resp.Success {
		return ReplPosition{}, fmt.Errorf("%s refused to open this node's stream: %s", p.node.ID, resp.Error)
	}
	if resp.Position == nil {
		return ReplPosition{}, fmt.Errorf("%s answered this node's stream handshake without a position: it runs a build from before "+
			"the handshake, and is sent nothing until it is upgraded", p.node.ID)
	}
	return *resp.Position, nil
}

// streamReplay is one pass of the log to a peer.
type streamReplay struct {
	p      *peer
	cursor storage.WALCursor
	// next is the sequence the cursor reads next, for the error that names it.
	next uint64
	// stranded is what this replay counted as stranded, for its one WARN.
	stranded strandedNote
}

// noteStranded counts the stranded entries of one read of the log that no
// earlier replay counted. The ledger forgets what lies below the log's first
// sequence; when that cannot be read it forgets nothing this time, which only
// keeps a few ranges a little longer.
func (r *streamReplay) noteStranded(stranded []strandedEntry) {
	seqs := make([]uint64, 0, len(stranded))
	buckets := make([]string, 0, len(stranded))
	for _, entry := range stranded {
		seqs = append(seqs, entry.seq)
		buckets = append(buckets, entry.bucket)
	}
	logFirst, err := r.p.c.s.wal.FirstSequence()
	if err != nil {
		logFirst = 0
	}
	r.stranded.add(r.p.c.stranded.count(seqs, logFirst), buckets)
}

// newReplay opens the log where the peer's replay starts.
func (p *peer) newReplay() (*streamReplay, error) {
	from := p.c.replayStart(p)
	cursor, err := p.c.s.wal.OpenCursor(from)
	if err != nil {
		return nil, p.c.replayReadError(p, from, err)
	}
	return &streamReplay{p: p, cursor: cursor, next: from}, nil
}

// ship reads one piece of the log, at most catchUpReadBytes and nothing above
// upTo, and sends the peer what of it is addressed to it. It reports whether
// the read reached upTo.
func (r *streamReplay) ship(upTo uint64, generation uint64) (bool, error) {
	p := r.p
	p.logReads.Add(1)
	entries, next, err := p.c.s.wal.ReadBatch(r.cursor, upTo, catchUpReadBytes)
	if err != nil {
		return false, p.c.replayReadError(p, r.next, err)
	}
	r.cursor = next
	if len(entries) > 0 {
		r.next = entries[len(entries)-1].Seq + 1
	}
	reached := len(entries) == 0 || entries[len(entries)-1].Seq >= upTo
	// The log is read up to what is synced, which can be ahead of this node's
	// own apply of it; the owner's outcome a re-send carries is read from what
	// that apply wrote, so it is waited for.
	if len(entries) > 0 {
		if err := p.c.unapplied.waitRetired(entries[len(entries)-1].Seq, p.stopCh, unappliedWaitLimit); err != nil {
			return false, fmt.Errorf("replaying the log to %s: %w", p.node.ID, err)
		}
	}
	addressed, stranded, err := p.c.replayableEntries(entries, p)
	if err != nil {
		return false, err
	}
	if len(stranded) > 0 {
		r.noteStranded(stranded)
	}
	if len(addressed) == 0 {
		return reached, nil
	}
	p.noteOwed()
	// Checked before anything is sent, so a plaintext peer is refused the
	// first frame and every one after it (D3).
	if refusal := p.c.refuseReplayToPlaintextPeer(p.node, len(addressed)); refusal != nil {
		return false, fmt.Errorf("%d logged entries are held back from %s: %w", len(addressed), p.node.ID, refusal)
	}
	for start, end := 0, 0; start < len(addressed); start = end {
		end = replBatchEnd(addressed, start)
		frame := addressed[start:end]
		if !p.inGeneration(generation) {
			return false, errStreamLeft
		}
		if hook := beforeReplayShipHook.Load(); hook != nil {
			(*hook)(p.node.ID, frame)
		}
		if err := p.ship(frame); err != nil {
			return false, err
		}
		p.noteReplayed(len(frame))
	}
	return reached, nil
}

// serveQueue ships live entries from the queue, in frames cut by count and by
// bytes, until the stream leaves the generation it was entered in or a ship
// fails.
func (p *peer) serveQueue(generation uint64) error {
	ticker := time.NewTicker(replFlushInterval)
	defer ticker.Stop()
	batch := make([]ReplEntry, 0, maxReplBatch)
	batchBytes := 0
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		// A batch taken from the queue before a stall is still a run of the
		// queue in order; it is dropped only so the replay that follows is the
		// one place the entries come from.
		if !p.inGeneration(generation) {
			return errStreamLeft
		}
		err := p.ship(batch)
		batch, batchBytes = batch[:0], 0
		return err
	}
	for {
		select {
		case <-p.stopCh:
			return errStreamLeft
		case <-p.c.ctx.Done():
			return errStreamLeft
		case <-p.wake:
			if !p.inGeneration(generation) {
				return errStreamLeft
			}
			if p.wantsAdvance() {
				if err := flush(); err != nil {
					return err
				}
				if err := p.advanceIfIdle(generation); err != nil {
					return err
				}
			}
		case pe := <-p.sendCh:
			size := replEntryBytes(pe.entry)
			if len(batch) > 0 && batchBytes+size > maxReplBatchBytes {
				if err := flush(); err != nil {
					return err
				}
			}
			batch = append(batch, pe.entry)
			batchBytes += size
			// Ship now when the queue is idle: a client that writes one batch at
			// a time leaves nothing to coalesce with, and waiting for the ticker
			// would add its interval to every write. Under load the queue is not
			// empty and entries still coalesce.
			if len(batch) >= maxReplBatch || batchBytes >= maxReplBatchBytes || len(p.sendCh) == 0 {
				if err := flush(); err != nil {
					return err
				}
			}
		case <-ticker.C:
			if err := flush(); err != nil {
				return err
			}
		}
	}
}

// ship sends one frame and records the peer's acknowledgement. A failure is
// the caller's to turn into a stall; the entries are in the log, and the
// connection is left alone, because it is shared with other traffic to the
// peer and dial replaces one that has failed.
func (p *peer) ship(entries []ReplEntry) error {
	if hook := beforeShipHook.Load(); hook != nil {
		shipped := (*hook)(p.node.ID)
		defer shipped()
	}
	ctx, cancel := context.WithTimeout(p.c.ctx, p.c.replShipTimeout())
	defer cancel()
	incarnation := p.c.s.wal.Incarnation()
	resp, err := p.call(ctx, transport.LaneControl, &ReplPushRequest{
		Token:    p.c.localToken(),
		FromNode: p.c.self.ID,
		Entries:  entries,
		Epoch:    p.c.Epoch(),
		Stream:   &ReplStream{Incarnation: incarnation[:]},
	})
	last := entries[len(entries)-1].Seq
	if err != nil {
		return fmt.Errorf("shipping %d entries, sequences %d to %d, to %s: %w", len(entries), entries[0].Seq, last, p.node.ID, err)
	}
	// A refusal can carry the drop that caused it: learn it before anything
	// is retried, so the retry is addressed by it (G-10, G-11 (a)).
	p.c.learnFromPeer(p.node.ID, resp.DroppedGenerations)
	if !resp.Success {
		refusal := fmt.Errorf("%s refused %d entries, sequences %d to %d: %s", p.node.ID, len(entries), entries[0].Seq, last, resp.Error)
		if resp.Refusal == refusalAwaitingCopy {
			return &awaitingCopyRefusal{bucket: refusedBucket(entries, resp.AppliedThrough), err: refusal}
		}
		if resp.Refusal == refusalDiverged || resp.Refusal == refusalAnotherIncarnation {
			p.divergedOn.Store(&resp.Error)
		}
		if resp.Refusal == refusalAnotherIncarnation {
			if err := p.noteRefusedCreate(entries, resp.AppliedThrough); err != nil {
				return errors.Join(refusal, err)
			}
		}
		return refusal
	}
	if p.divergedOn.Load() != nil {
		p.divergedOn.Store(nil)
	}
	p.c.logger.Debug("shipped entries to peer", "node_id", p.node.ID, "entries", len(entries), "applied_through", resp.AppliedThrough)
	p.advanceAck(p.c.self.ID, resp.AppliedThrough)
	p.settleRefusedCreate(resp.AppliedThrough)
	if resp.AppliedThrough < last {
		return fmt.Errorf("%s applied through sequence %d of a frame that ends at %d", p.node.ID, resp.AppliedThrough, last)
	}
	p.settleIfIdle()
	return nil
}

// The advance notice (spec §19.3.9). Retention's bound is the lowest cursor of
// every member, and a cursor moves only when its peer acknowledges something,
// so a stream that carries nothing — a member that hosts none of this node's
// buckets, between two database DDLs — would hold the log for ever. A
// retention pass therefore asks every streaming peer whose cursor is below the
// synced tail for a notice, and the sender, once it has nothing in flight,
// tells the peer how far nothing is owed to it. The peer records that
// position durably before it answers (handleReplPush).

// askForAdvances asks the sender of every streaming peer whose cursor is below
// the synced tail to send an advance notice. It is a flag per peer: at most one
// notice per peer per retention pass. O(members).
func (c *Cluster) askForAdvances() {
	synced := c.s.wal.SyncedSequence()
	c.peersMu.RLock()
	peers := make([]*peer, 0, len(c.peers))
	for _, p := range c.peers {
		peers = append(peers, p)
	}
	c.peersMu.RUnlock()
	for _, p := range peers {
		if cursor, opened := p.cursor(c.self.ID); opened && cursor < synced {
			p.askForAdvance()
		}
	}
}

// askForAdvance sets advanceWanted on a streaming stream and wakes its sender.
// A stream that is not streaming is replaying the log, which moves its cursor.
func (p *peer) askForAdvance() {
	p.streamMu.Lock()
	defer p.streamMu.Unlock()
	if p.state == streamStreaming {
		p.advanceWanted = true
		p.wakeSender()
	}
}

func (p *peer) wantsAdvance() bool {
	p.streamMu.Lock()
	defer p.streamMu.Unlock()
	return p.advanceWanted
}

// advanceBound is the highest sequence an advance notice may name: no entry at
// or below it is still on its way to any queue. An entry that will be queued
// holds an open ship ticket whose floor is at or below its sequence, so the
// bound is below the lowest open floor (A-5.1); an entry this node appends as a
// replica holds no ticket and is never shipped, so the synced tail bounds the
// rest (A-5.2).
//
// The synced tail is read before the tickets. Read after, a ticket opened
// between the two reads, appended and synced, could fall at or below the bound
// with its entry not yet routed. Read before, a ticket opened later has a floor
// above every sequence that was synced, and one that closed in between routed
// its entry first.
func (c *Cluster) advanceBound() uint64 {
	through := c.s.wal.SyncedSequence()
	if floor, open := c.shipOrder.lowestOpenFloor(); open {
		through = min(through, floor-1)
	}
	return through
}

// advanceIfIdle sends the notice a retention pass asked for, if the stream has
// shipped its boundary range, its queue is empty and nothing it shipped is
// unacknowledged (the caller has just flushed its batch, and a flush returns
// only once the peer acknowledged it). The bound is read first and the queue
// checked after: every entry at or below the bound had closed its ticket, so it
// was on the queue before the check, and an empty queue means the peer has
// acknowledged it. A stream that is not idle keeps the request for the next
// wake; the next retention pass asks again anyway.
func (p *peer) advanceIfIdle(generation uint64) error {
	through := p.c.advanceBound()
	p.streamMu.Lock()
	idle := p.generation == generation && p.advanceWanted && p.boundaryShipped && len(p.sendCh) == 0
	if idle {
		p.advanceWanted = false
	}
	p.streamMu.Unlock()
	if !idle {
		return nil
	}
	if cursor, _ := p.cursor(p.c.self.ID); through <= cursor {
		return nil
	}
	return p.shipAdvance(through)
}

// shipAdvance sends an empty push that tells the peer no entry at or below
// through is owed to it, and moves the cursor there once the peer has answered
// — which it does only after it has recorded the position durably.
func (p *peer) shipAdvance(through uint64) error {
	ctx, cancel := context.WithTimeout(p.c.ctx, p.c.replShipTimeout())
	defer cancel()
	incarnation := p.c.s.wal.Incarnation()
	resp, err := p.call(ctx, transport.LaneControl, &ReplPushRequest{
		Token:    p.c.localToken(),
		FromNode: p.c.self.ID,
		Epoch:    p.c.Epoch(),
		Stream:   &ReplStream{Incarnation: incarnation[:], AdvanceTo: through},
	})
	if err != nil {
		return fmt.Errorf("telling %s that nothing up to sequence %d is owed to it: %w", p.node.ID, through, err)
	}
	if !resp.Success {
		return fmt.Errorf("%s refused the notice that nothing up to sequence %d is owed to it: %s", p.node.ID, through, resp.Error)
	}
	if resp.AppliedThrough < through {
		return fmt.Errorf("%s answered the notice that nothing up to sequence %d is owed to it with position %d", p.node.ID, through, resp.AppliedThrough)
	}
	p.advanceAck(p.c.self.ID, through)
	return nil
}

// replayStart is the sequence a replay to p reads from: one past the stream's
// cursor — the position p reported at the handshake, and what it has
// acknowledged since. Every entry at or below the cursor that is addressed to
// p has been delivered, because one sender delivers in order, and retention
// never prunes past any member's cursor (Cluster.replicaAckBound). A cursor
// below the log's first entry is a hole, whoever the peer is and whatever it
// hosts: the replay is refused at the horizon (replayReadError), never started
// above it.
func (c *Cluster) replayStart(p *peer) uint64 {
	return p.ackedThrough(c.self.ID) + 1
}

// replayReadError words a failed read of the log for the operator: a sequence
// that has been pruned gets the horizon refusal and its remedy (spec §19.3.3),
// anything else is wrapped as it is.
func (c *Cluster) replayReadError(p *peer, needed uint64, err error) error {
	if !errors.Is(err, storage.ErrBelowHorizon) {
		return fmt.Errorf("reading the log from sequence %d for %s: %w", needed, p.node.ID, err)
	}
	first, firstErr := c.s.wal.FirstSequence()
	if firstErr != nil {
		return errors.Join(err, firstErr)
	}
	return fmt.Errorf("%s needs this node's log from sequence %d and the log starts at %d: "+
		"its copies of the buckets this node owns are older than anything the log can repair. "+
		"Remove it ('remove-node %s'), empty its data directory, and add it again; it is then seeded by snapshot. "+
		"('migrate' does not re-seed a node that already holds a copy.) [%w]",
		p.node.ID, needed, first, p.node.ID, err)
}

// strandedEntry is an entry a replay passed over because this node no longer
// owns its bucket (stranded.go).
type strandedEntry struct {
	seq    uint64
	bucket string
}

// replayableEntries picks, from a read of the log, the entries this node sends
// to p: the ones it originated (or logged before origins were recorded) that
// deliverTo addresses to p. It also returns the data entries this node
// originated that it sends to nobody because ownership of their bucket moved
// (R-OWNS): the stranded ones.
func (c *Cluster) replayableEntries(entries []storage.WALEntry, p *peer) ([]ReplEntry, []strandedEntry, error) {
	ship := make([]ReplEntry, 0, len(entries))
	var stranded []strandedEntry
	states := make(map[loggedBucket]store.BucketState)
	for _, e := range entries {
		// What this node applied for another node's stream is that node's to
		// send: sequence numbers mean something only in the stream that
		// assigned them.
		if e.Origin.Kind == storage.WALOriginReplica {
			continue
		}
		// Database DDL logged before origins were recorded may be another
		// coordinator's broadcast this node applied; it was never replayed, and
		// is not now.
		if e.Bucket == "" && e.Origin.Kind == storage.WALOriginLegacy {
			continue
		}
		op := transport.Opcode(e.Op)
		// The state says what the owner's apply did and which term the copy
		// is in, which the entry ships in (step 9.2), and whether this node may
		// ship the entry at all (the sender's rule, step 10b.4).
		st, err := c.loggedState(states, e)
		if err != nil {
			return nil, nil, fmt.Errorf("reading the state of the bucket of sequence %d: %w", e.Seq, err)
		}
		var (
			targets      []ring.Node
			unshippable  bool
			addressedErr error
		)
		if e.Bucket != "" && op != transport.OpDeleteBucket {
			targets, unshippable, addressedErr = c.bucketEntryAddressing(e, st)
		} else {
			var loggedAt uint64
			if e.Origin.Kind == storage.WALOriginOwn {
				loggedAt = e.Origin.Epoch
			}
			targets, unshippable, addressedErr = c.addressing(op, e.Bucket, e.Data, loggedAt)
		}
		if addressedErr != nil {
			return nil, nil, fmt.Errorf("deciding who sequence %d is addressed to: %w", e.Seq, addressedErr)
		}
		if unshippable && e.Origin.Kind == storage.WALOriginOwn && op != transport.OpCreateBucket && op != transport.OpDeleteBucket {
			stranded = append(stranded, strandedEntry{seq: e.Seq, bucket: e.Bucket})
		}
		// An entry a snapshot the peer pulled already holds is shipped too: the
		// peer keeps its snapshot positions and passes over what they cover
		// (R-4), and this node keeps none (spec §19.3.3).
		if !nodeListed(targets, p.node.ID) {
			continue
		}
		// The peer refused the create of an incarnation this node has since
		// dropped: nothing of that incarnation is its (owner ruling 53).
		if p.skipsRefusedIncarnation(e) {
			continue
		}
		ship = append(ship, ReplEntry{Seq: e.Seq, Op: int32(e.Op), Bucket: e.Bucket, Key: e.Key, Payload: e.Data,
			OwnerOutcome: loggedOwnerOutcome(st, e.Seq), Term: st.Term})
	}
	return ship, stranded, nil
}

// awaitingCopyRefusal is a receiver's awaiting_copy refusal of a frame: it is
// placed on the bucket of the refused entry and does not hold it yet. The
// bucket is what the stream's WARN names (spec §19.3.13).
type awaitingCopyRefusal struct {
	bucket string
	err    error
}

func (r *awaitingCopyRefusal) Error() string { return r.err.Error() }

func (r *awaitingCopyRefusal) Unwrap() error { return r.err }

// refusedBucket is the bucket of the first entry of a frame above what the
// receiver applied: the one it refused. A refusal that claims the whole frame
// applied names none.
func refusedBucket(entries []ReplEntry, appliedThrough uint64) string {
	for _, entry := range entries {
		if entry.Seq > appliedThrough {
			return entry.Bucket
		}
	}
	return ""
}
