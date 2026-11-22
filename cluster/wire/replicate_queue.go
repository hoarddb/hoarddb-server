package wire

import (
	"context"
	"fmt"
	"runtime"
	"slices"
	"sync"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The copy side of the stream (write path section 4). A WRITE is answered with a receipt as
// soon as the copy has admitted and queued it; a fixed pool of workers applies each pair's
// queue in order (append, fsync, Badger batch) and only then sends the cumulative WRITE_ACK
// on the reverse path. The receipt is never an acknowledgement: the owner counts a copy
// only on that ack.
const (
	defaultCopyQueueBytes = 32 << 20
	minCopyWorkers        = 16
	// copyFrameOverhead is what a queued frame costs beyond its record body, in bytes.
	copyFrameOverhead = 256
)

// queuedWrite is an admitted frame waiting for a worker.
type queuedWrite struct {
	frame WriteFrame
	op    cluster.Op
	cost  int
}

// pairQueue is the FIFO of one active (slot, database) pair. It exists only while the pair
// has frames queued or being applied, so an idle pair costs nothing.
type pairQueue struct {
	items []queuedWrite
	// depth counts the queued frames and the ones a worker is applying now.
	depth int
	// active: a worker owns the pair, which keeps its records strictly sequential.
	active bool
}

// copyQueues is the copy's whole apply queue (guarded by mu).
type copyQueues struct {
	mu      sync.Mutex
	pairs   map[pairKey]*pairQueue
	ready   []pairKey
	bytes   int
	wake    chan struct{}
	workers sync.WaitGroup
}

// writeCost is what a frame is charged against the node's byte budget.
func writeCost(frame WriteFrame) int { return len(frame.Record.Body) + copyFrameOverhead }

func (r *Replicator) copyWorkers() int {
	if r.cfg.CopyWorkers > 0 {
		return r.cfg.CopyWorkers
	}
	return max(minCopyWorkers, runtime.GOMAXPROCS(0))
}

func (r *Replicator) copyQueueBytes() int {
	if r.cfg.CopyQueueBytes > 0 {
		return r.cfg.CopyQueueBytes
	}
	return defaultCopyQueueBytes
}

// EnqueueWrite queues an admitted frame for the pair's worker and reports true: the
// connection then answers with a receipt. A pair already holding twice the window of frames,
// or a node over its byte budget, gets the not-applied ack back at once instead, and the
// owner re-arms with its backoff; nothing is buffered without bound.
func (r *Replicator) EnqueueWrite(adm WriteAdmission) (WriteAck, bool) {
	key := pairKey{adm.frame.Slot, adm.frame.DB}
	item := queuedWrite{frame: adm.frame, op: adm.op, cost: writeCost(adm.frame)}
	q := &r.copyQ
	q.mu.Lock()
	pq := q.pairs[key]
	depth := 0
	if pq != nil {
		depth = pq.depth
	}
	if depth >= 2*r.cfg.Window || q.bytes+item.cost > r.copyQueueBytes() {
		q.mu.Unlock()
		// No Pos: reading the applied position takes the pair lock, which applyQueued holds across
		// the AppendFrom fsync of a copy apply, and this runs on the connection. A not-applied ack
		// counts for nothing.
		return WriteAck{From: r.cfg.Self, Slot: key.slot, DB: key.db, Outcome: AckNotApplied}, false
	}
	if pq == nil {
		pq = &pairQueue{}
		q.pairs[key] = pq
	}
	pq.items = append(pq.items, item)
	pq.depth++
	q.bytes += item.cost
	if !pq.active && len(pq.items) == 1 {
		q.ready = append(q.ready, key)
	}
	q.mu.Unlock()
	r.wakeCopyWorker()
	return WriteAck{}, true
}

// DropCopyQueue empties the pair's waiting frames, as a reset does: the pair's records are
// about to be deleted, and a frame queued before it was never applied. Frames a worker has
// already taken meet the readiness check in applyLocked and apply nothing.
func (r *Replicator) DropCopyQueue(slot uint16, db uint64) {
	key := pairKey{slot, db}
	q := &r.copyQ
	q.mu.Lock()
	defer q.mu.Unlock()
	pq := q.pairs[key]
	if pq == nil {
		return
	}
	for _, item := range pq.items {
		pq.depth--
		q.bytes -= item.cost
	}
	pq.items = nil
	if pq.active {
		return // finishPair gives the pair back once its worker's frames are done
	}
	q.ready = slices.DeleteFunc(q.ready, func(queued pairKey) bool { return queued == key })
	delete(q.pairs, key)
}

func (r *Replicator) wakeCopyWorker() {
	select {
	case r.copyQ.wake <- struct{}{}:
	default:
	}
}

// startCopyWorkers runs the fixed pool until ctx ends or Close is called.
func (r *Replicator) startCopyWorkers(ctx context.Context) {
	for range r.copyWorkers() {
		r.copyQ.workers.Add(1)
		go r.copyWorker(ctx)
	}
}

func (r *Replicator) copyWorker(ctx context.Context) {
	defer r.copyQ.workers.Done()
	for {
		key, items, ok := r.takeCopyWork()
		if !ok {
			select {
			case <-ctx.Done():
				return
			case <-r.done:
				return
			case <-r.copyQ.wake:
			}
			continue
		}
		r.drainPair(ctx, key, items)
		if ctx.Err() != nil {
			return
		}
	}
}

// takeCopyWork hands a worker the next pair with queued frames and everything queued for it.
func (r *Replicator) takeCopyWork() (pairKey, []queuedWrite, bool) {
	q := &r.copyQ
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.ready) == 0 {
		return pairKey{}, nil, false
	}
	key := q.ready[0]
	q.ready = q.ready[1:]
	pq := q.pairs[key]
	pq.active = true
	// At most one window per turn: a pair with a full queue goes to the back of the ready list
	// when its turn ends, so sixteen busy pairs cannot hold every worker for 2*Window fsyncs each.
	n := min(len(pq.items), r.cfg.Window)
	items := pq.items[:n:n]
	if pq.items = pq.items[n:]; len(pq.items) == 0 {
		pq.items = nil
	}
	if len(q.ready) > 0 {
		r.wakeCopyWorker()
	}
	return key, items, true
}

// drainPair applies one pair's taken frames in order, acknowledging each after it is durable
// and applied, and stops at the first one that is not applied: the rest of the pair's queue is
// dropped, because the owner ships it again from the last acknowledgement.
func (r *Replicator) drainPair(ctx context.Context, key pairKey, items []queuedWrite) {
	stopped := false
	for _, item := range items {
		if stopped || ctx.Err() != nil {
			break
		}
		ack, applied := r.applyQueued(key, item)
		if !applied {
			stopped = true
		}
		r.sendAck(ctx, item.frame.From, ack)
	}
	r.finishPair(key, items, stopped || ctx.Err() != nil)
}

// finishPair gives the pair back: its charge is released, a queue that is dropped is emptied,
// and a pair with more frames waiting goes to the back of the ready list.
func (r *Replicator) finishPair(key pairKey, taken []queuedWrite, drop bool) {
	q := &r.copyQ
	q.mu.Lock()
	pq := q.pairs[key]
	for _, item := range taken {
		pq.depth--
		q.bytes -= item.cost
	}
	if drop {
		for _, item := range pq.items {
			pq.depth--
			q.bytes -= item.cost
		}
		pq.items = nil
	}
	pq.active = false
	wake := false
	if len(pq.items) > 0 {
		q.ready = append(q.ready, key)
		wake = true
	} else {
		delete(q.pairs, key)
	}
	q.mu.Unlock()
	if wake {
		r.wakeCopyWorker()
	}
}

// applyQueued applies one queued frame and returns the ack to send and whether it was applied
// (or already held). The admission checks run again here: the frame waited in the queue, and
// an epoch change, a seeding pair or a node that has stopped serving since must not see an
// old record applied.
func (r *Replicator) applyQueued(key pairKey, item queuedWrite) (WriteAck, bool) {
	ack := WriteAck{From: r.cfg.Self, Slot: key.slot, DB: key.db}
	if r.cfg.Ready != nil && !r.cfg.Ready(key.slot, key.db) {
		ack.Pos, ack.Outcome = r.cfg.Pairs.Snapshot(key.slot, key.db).AppliedPos, AckNotApplied
		return ack, false
	}
	var out applyOutcome
	var err error
	stale := false
	r.cfg.Pairs.With(key.slot, key.db, func(p *cluster.Pair) {
		// Sampled under the pair lock, the one the promotion and the epoch change take: a frame
		// is never applied after a change this check has not seen.
		committed := r.cfg.Committed()
		if item.frame.Epoch != committed || (r.cfg.Behind != nil && r.cfg.Behind(committed)) {
			stale = true
			ack.Pos = p.AppliedPos
			return
		}
		out, err = r.applyLocked(p, applyInput{source: item.frame.From, committed: committed, rec: item.frame.PairRecord, op: item.op, key: key})
	})
	if stale {
		ack.Outcome = AckNotApplied
		return ack, false
	}
	if err != nil {
		r.report(fmt.Errorf("applying position %s of slot %d from %s: %w", item.frame.Pos, key.slot, item.frame.From, err))
		ack.Pos, ack.Outcome = r.cfg.Pairs.Snapshot(key.slot, key.db).AppliedPos, AckNotApplied
		return ack, false
	}
	ack, applied, err := r.ackFor(key, out)
	if err != nil {
		r.report(err)
	}
	if out.verdict == verdictGap && r.cfg.RequestCatchUp != nil {
		r.cfg.RequestCatchUp(key.slot, key.db)
	}
	return ack, applied
}

// readWriteReply reads the reply to a WRITE sent to target. An empty reply is a receipt: the
// copy queued the frame, which says nothing about durability, so nothing is counted. A 0x02
// body is the copy's answer that it did not queue the frame; it goes to the replicator, which
// stops the batch at it. Anything else is refused.
func (l *peerLinks) readWriteReply(target string, frame WriteFrame, reply *transport.Message) error {
	if reply == nil {
		return nil
	}
	if reply.Env.Kind != 0x02 {
		return fmt.Errorf("%s answered a WRITE with kind 0x%02x, want a receipt or a WRITE_ACK", target, reply.Env.Kind)
	}
	ack, err := DecodeWriteAckBody(reply.Body)
	if err != nil {
		return fmt.Errorf("reading the WRITE_ACK of %s: %w", target, err)
	}
	ack.Slot, ack.DB, ack.From = frame.Slot, frame.DB, target
	l.onAck(ack)
	return nil
}
