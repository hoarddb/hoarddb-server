package wire

import (
	"context"
	"fmt"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// AckBatcher is a Link that can send several acknowledgements in one call. A Link without it is sent one WRITE_ACK per call.
type AckBatcher interface {
	// AckBatch sends acks, in order, in one call. It returns when the peer has answered.
	AckBatch(ctx context.Context, peer string, acks []WriteAck) error
}

// ackLane is the unsent acknowledgements to one destination node and whether a goroutine is sending them (guarded by Replicator.ackMu).
type ackLane struct {
	busy  bool
	queue []WriteAck
	// last is the index in queue of the newest applied ack of a pair with no other ack of that pair after it.
	last map[pairKey]int
}

// sendAck is how a worker acknowledges a record: through the ack lane when the link batches, otherwise one Link.Ack call as before.
// The caller has already applied the record; an ack is never queued before that, so the owner never counts a copy early (I3).
func (r *Replicator) sendAck(ctx context.Context, to string, ack WriteAck) {
	if r.ackBatch == nil {
		if err := r.cfg.Link.Ack(ctx, to, ack); err != nil && ctx.Err() == nil {
			r.report(fmt.Errorf("sending the ack for position %s of slot %d to %s: %w", ack.Pos, ack.Slot, to, err))
		}
		return
	}
	r.queueAck(ctx, to, ack)
}

// AckAsync is sendAck for a caller with no context of its own (a connection goroutine): it uses the context Run was started with.
// Before Run starts or after it ends the ack is not sent and the loss is reported through OnError.
func (r *Replicator) AckAsync(to string, ack WriteAck) {
	r.ackMu.Lock()
	ctx, closed := r.runCtx, r.ackClosed
	r.ackMu.Unlock()
	if ctx == nil || closed {
		r.report(fmt.Errorf("an ack for slot %d to %s was not sent: the replicator is not running", ack.Slot, to))
		return
	}
	r.sendAck(ctx, to, ack)
}

// queueAck puts ack on the lane to the destination and starts the lane's goroutine if none is running. Applied acks of one pair
// coalesce into the highest, which is safe because an ack is cumulative; nothing coalesces across another answer for the
// pair, so a not-applied answer is never overtaken by an applied position that came before it (I6).
func (r *Replicator) queueAck(ctx context.Context, to string, ack WriteAck) {
	r.ackMu.Lock()
	defer r.ackMu.Unlock()
	if r.ackClosed {
		r.report(fmt.Errorf("an ack for slot %d to %s was not sent: the replicator has stopped", ack.Slot, to))
		return
	}
	lane := r.ackLanes[to]
	if lane == nil {
		lane = &ackLane{last: make(map[pairKey]int)}
		r.ackLanes[to] = lane
	}
	pair := pairKey{ack.Slot, ack.DB}
	if ack.Outcome != AckApplied {
		delete(lane.last, pair)
		lane.queue = append(lane.queue, ack)
	} else if i, ok := lane.last[pair]; ok {
		if lane.queue[i].Pos.Less(ack.Pos) {
			lane.queue[i].Pos = ack.Pos
		}
	} else {
		lane.last[pair] = len(lane.queue)
		lane.queue = append(lane.queue, ack)
	}
	if !lane.busy {
		lane.busy = true
		r.ackWG.Add(1)
		go r.runAckLane(ctx, to, lane)
	}
}

// runAckLane sends the lane's queue, a whole pass per call, until it is empty or ctx ends. It is the only goroutine the ack path
// adds: one per destination node while acks wait, none per pair, and none when idle. A failed chunk is reported and dropped,
// not retried: the owner's ack deadline reships what was unacknowledged and the copy answers it with fresh acks (I10).
func (r *Replicator) runAckLane(ctx context.Context, to string, lane *ackLane) {
	defer r.ackWG.Done()
	for {
		r.ackMu.Lock()
		if len(lane.queue) == 0 || ctx.Err() != nil {
			lane.queue, lane.busy = nil, false
			delete(r.ackLanes, to)
			r.ackMu.Unlock()
			return
		}
		pass := lane.queue
		lane.queue = nil
		clear(lane.last)
		r.ackMu.Unlock()
		for len(pass) > 0 {
			chunk := pass[:min(len(pass), maxBatchEntries)]
			pass = pass[len(chunk):]
			if err := r.ackBatch.AckBatch(ctx, to, chunk); err != nil && ctx.Err() == nil {
				r.report(fmt.Errorf("sending %d acknowledgements to %s: %w", len(chunk), to, err))
			}
		}
	}
}

// closeAckLanes is the end of Run for the ack path: it refuses new acks and waits for the lane goroutines, whose context has
// ended, so Close returns only once none is left.
func (r *Replicator) closeAckLanes() {
	r.ackMu.Lock()
	r.ackClosed = true
	r.ackMu.Unlock()
	r.ackWG.Wait()
}

// OnWriteAckBatch hands every entry of a WRITE_ACK_BATCH that comes from a copy of its pair to OnWriteAck, in order, and returns an
// error naming the first entry it refused. isCopy reports whether from is a copy of the pair now; an entry for a pair it is not a
// copy of is skipped, never counted (I4), and the entries after it are still handled so one stale pair cannot hold back the others.
func (r *Replicator) OnWriteAckBatch(from string, acks []WriteAck, isCopy func(slot uint16, db uint64, node string) bool) error {
	var firstRefusal error
	for _, ack := range acks {
		if !isCopy(ack.Slot, ack.DB, from) {
			if firstRefusal == nil {
				firstRefusal = &cluster.Error{Code: cluster.CodeNotOwner, Message: fmt.Sprintf(
					"refused a WRITE_ACK_BATCH entry from %s for slot %d: %s is not a copy of that pair at the committed epoch. Nothing was counted for it", from, ack.Slot, from)}
			}
			continue
		}
		ack.From = from
		r.OnWriteAck(ack)
	}
	return firstRefusal
}
