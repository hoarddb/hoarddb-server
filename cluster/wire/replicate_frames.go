package wire

import (
	"context"
	"fmt"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// WriteBatcher is a Link that can send the records of several pairs in one call.
type WriteBatcher interface {
	// WriteBatch sends frames, in order, in one call and returns when the copy has answered with
	// its receipt. Every frame carries the same Epoch. A frame the copy does not queue is answered
	// on the ack path, never by this call.
	WriteBatch(ctx context.Context, peer string, frames []WriteFrame) error
}

// batchEntry is one record of a chunk and the index of the shipment it was read for.
type batchEntry struct {
	shipment int
	record   PairRecord
}

// shipBatched is one lane pass over keys for a link that batches: every pair is read first, so
// the pass is one call however many pairs it holds, and a read that fails costs only its pair.
func (r *Replicator) shipBatched(ctx context.Context, peer string, keys []pairKey) {
	shipments := make([]shipment, 0, len(keys))
	chain := &laneChain{}
	for _, key := range keys {
		if ctx.Err() != nil {
			// A stop: the lane ends, and what was read is not sent to a link that is stopping.
			return
		}
		sh, ok, err := r.readShipmentChained(ctx, key, peer, chain)
		if err != nil {
			r.laneError(ctx, err)
			continue
		}
		if ok {
			shipments = append(shipments, sh)
		}
	}
	if len(shipments) == 0 || ctx.Err() != nil {
		return
	}
	if err := r.sendShipments(ctx, peer, shipments); err != nil {
		r.laneError(ctx, err)
	}
}

// sendShipments cuts the shipments of one lane pass into frames and sends them to peer with
// WriteBatch, in order. The committed epoch is read once, after every read, so one pass is fenced
// by one epoch. A pair's records stay consecutive and ascending across chunks because the chunks
// are cut from the shipments in sequence, never reordered.
func (r *Replicator) sendShipments(ctx context.Context, peer string, shipments []shipment) error {
	committed := r.cfg.Committed()
	chunks := cutChunks(shipments, r.cfg.ChunkBytes)
	dropped := make([]bool, len(shipments))
	for index, chunk := range chunks {
		frames := r.markInFlight(chunk, shipments, dropped, committed)
		if len(frames) == 0 {
			continue
		}
		if err := r.writeBatch.WriteBatch(ctx, peer, frames); err != nil {
			r.rewindFromChunk(peer, chunks[index:], shipments, dropped)
			r.noteSendFailure(ctx, peer, err)
			return fmt.Errorf("sending %d WRITEs to %s in one frame: %w", len(frames), peer, err)
		}
		r.noteSendSuccess(peer)
	}
	r.mu.Lock()
	redirtied := false
	for index, sh := range shipments {
		if sh.more && !dropped[index] {
			r.dirty[sh.key] = struct{}{}
			redirtied = true
		}
	}
	r.mu.Unlock()
	if redirtied {
		r.signal()
	}
	return nil
}

// cutChunks splits the records of shipments, pair after pair in the order read, into chunks of at
// most maxBatchEntries entries and at most limit bytes of record body (one entry always fits).
func cutChunks(shipments []shipment, limit int) [][]batchEntry {
	var chunks [][]batchEntry
	var chunk []batchEntry
	bodyBytes := 0
	for index, sh := range shipments {
		for _, rec := range sh.records {
			size := len(rec.Record.Body)
			if len(chunk) > 0 && (len(chunk) == maxBatchEntries || bodyBytes+size > limit) {
				chunks = append(chunks, chunk)
				chunk, bodyBytes = nil, 0
			}
			chunk = append(chunk, batchEntry{shipment: index, record: rec})
			bodyBytes += size
		}
	}
	if len(chunk) > 0 {
		chunks = append(chunks, chunk)
	}
	return chunks
}

// markInFlight drops the records of a shipment the copy has refused or a rewind has overtaken
// since it was read (the two stops shipPeer has for each record), and counts the rest as in
// flight, because a record is in flight from the moment its frame is sent and an ack that
// overtakes the receipt must find it. A dropped shipment stays dropped for the rest of the pass,
// so its later records can never be sent past a gap.
func (r *Replicator) markInFlight(chunk []batchEntry, shipments []shipment, dropped []bool, committed uint64) []WriteFrame {
	frames := make([]WriteFrame, 0, len(chunk))
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, entry := range chunk {
		sh := shipments[entry.shipment]
		if sh.st.generation != sh.generation || !sh.st.rearmAt.IsZero() {
			dropped[entry.shipment] = true
		}
		if dropped[entry.shipment] {
			continue
		}
		sh.st.inflight = append(sh.st.inflight, sentFrame{pos: entry.record.Pos, cursor: entry.record.Cursor})
		r.armAckLocked(retryKey{sh.key, sh.peer}, sh.st)
		frames = append(frames, WriteFrame{Slot: sh.key.slot, DB: sh.key.db, From: r.cfg.Self, Epoch: committed, PairRecord: entry.record})
	}
	return frames
}

// rewindFromChunk rewinds every pair with a record in the failed chunk or in a later one: the
// later chunks were read, so st.next moved past them, and nothing of them was sent. A pair wholly
// in earlier chunks is not touched, and a dropped one was already rewound or is waiting on its
// backoff.
func (r *Replicator) rewindFromChunk(peer string, chunks [][]batchEntry, shipments []shipment, dropped []bool) {
	rewound := make(map[int]bool)
	for _, chunk := range chunks {
		for _, entry := range chunk {
			if rewound[entry.shipment] || dropped[entry.shipment] {
				continue
			}
			rewound[entry.shipment] = true
			r.rewindAfterSendFailure(shipments[entry.shipment].key, peer)
		}
	}
}

// batchAdmitBudget is how long one frame may spend admitting: half the link call timeout, so a reply is never later than the caller's wait.
const batchAdmitBudget = linkCallTimeout / 2

// OnWriteBatch admits and queues the entries of one WRITE_BATCH from node from, in order, and answers every entry it did not queue as
// not applied on the ack path (AckAsync). owns reports whether from owns the pair at the committed epoch. A pair stops at its first
// answered entry, so the owner reships from the copy's position and never past a gap (I8); an admission error of one pair never fails
// the frame, because the pairs behind it belong to other entities. Nothing is answered for a queued entry: its ack follows from the worker.
func (r *Replicator) OnWriteBatch(from string, epoch uint64, entries []WriteEntry, owns func(slot uint16, db uint64) bool) {
	start := r.cfg.Now()
	stopped := make(map[pairKey]struct{})
	for _, entry := range entries {
		key := pairKey{entry.Slot, entry.DB}
		if _, isStopped := stopped[key]; isStopped {
			continue
		}
		if ack, queued := r.admitBatchEntry(from, epoch, entry, owns, start); !queued {
			stopped[key] = struct{}{}
			r.AckAsync(from, ack)
		}
	}
}

// admitBatchEntry takes one entry of a frame through the checks of serveWrite and queues it. When it is not queued, the ack to
// answer with comes back: not applied with no position, unless AdmitWrite or EnqueueWrite supplied the answer.
func (r *Replicator) admitBatchEntry(from string, epoch uint64, entry WriteEntry, owns func(slot uint16, db uint64) bool, start time.Time) (WriteAck, bool) {
	notApplied := WriteAck{From: r.cfg.Self, Slot: entry.Slot, DB: entry.DB, Outcome: AckNotApplied}
	if !owns(entry.Slot, entry.DB) {
		r.report(&cluster.Error{Code: cluster.CodeNotOwner, Message: fmt.Sprintf(
			"refused a WRITE_BATCH entry from %s for slot %d: %s is not the owner of that pair at the committed epoch. Nothing was applied for it", from, entry.Slot, from)})
		return notApplied, false
	}
	if r.cfg.Now().Sub(start) > batchAdmitBudget {
		return notApplied, false
	}
	adm, err := r.AdmitWrite(WriteFrame{Slot: entry.Slot, DB: entry.DB, From: from, Epoch: epoch, PairRecord: entry.PairRecord})
	if err != nil {
		r.report(fmt.Errorf("admitting position %s of slot %d from %s in a WRITE_BATCH: %w", entry.Pos, entry.Slot, from, err))
		return notApplied, false
	}
	if !adm.Admitted() {
		return adm.ack, false
	}
	return r.EnqueueWrite(adm)
}
