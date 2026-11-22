package server

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// handleBatch answers a BATCH frame that has passed NewHandler's envelope
// gates (batch-request spec §4.2): it decodes and validates the batch, runs
// every gate a lone request meets on each item, and only then runs the items,
// on a cluster through the coordinator (§5.2) and on a node that does not
// route under one acknowledgeDurably (§5.1). It is not a localHandlers entry:
// replicated entries and replay funnel through Server.dispatch, and a batch
// must never be dispatchable there.
func (s *Server) handleBatch(ctx context.Context, frame *transport.Frame) (*transport.Frame, error) {
	batch, err := decodeBatch(frame.Payload)
	if err == nil {
		err = s.admitBatchItems(ctx, frame, batch)
	}
	if err != nil {
		return s.refuseBatch(frame, err), nil
	}
	var answers []batchAnswer
	if s.cluster != nil && (s.cluster.Enabled() || s.cluster.removed()) {
		answers, err = s.cluster.coordinateBatch(ctx, frame, batch)
		if err != nil {
			return s.refuseBatch(frame, err), nil
		}
	} else {
		answers = s.runStandaloneBatch(ctx, frame, batch)
	}
	recordBatchOutcome(batch.items, answers)
	return encodeBatchReply(frame, answers), nil
}

// refuseBatch answers a whole-batch refusal. It carries write_outcome
// not_applied only when the batch carries a write item (an outcome is never
// set on a read), and is counted refused.
func (s *Server) refuseBatch(frame *transport.Frame, err error) *transport.Frame {
	batchRequestsTotal.WithLabelValues("refused").Inc()
	if batchCarriesWrite(frame.Payload) {
		err = withOutcome(outcomeNotApplied, err)
	}
	return errorFrame(frame, err)
}

// batchCarriesWrite reports whether any item of a BATCH payload names a data
// write, read best-effort from items that do not decode as a whole: a
// refusal of item 3's shape still says not_applied when item 1 is a push.
func batchCarriesWrite(payload []byte) bool {
	items, ok := bson.Raw(payload).Lookup("items").ArrayOK()
	if !ok {
		return false
	}
	values, err := items.Values()
	if err != nil {
		return false
	}
	for _, value := range values {
		if value.Type != bson.TypeEmbeddedDocument {
			continue
		}
		number, ok := value.Document().Lookup("opcode").Int32OK()
		if ok && number >= 0 && number <= 0xFF && isDataWrite(transport.Opcode(number)) {
			return true
		}
	}
	return false
}

// admitBatchItems runs, for every item, every gate NewHandler runs on a lone
// client frame, in the same order, on a synthetic frame: Validate at the
// item's own limit, the authorization gate against the item's own database,
// the size gates (which stamp an update with this node's document limit), and
// the read-repair flags. Any refusal refuses the whole batch, naming the item,
// with the gate's code. The synthetic frame as the gates leave it is the item
// from here on (§3.2): running the item's original bytes would run an update
// with no document limit, or with the client's.
func (s *Server) admitBatchItems(ctx context.Context, batchFrame *transport.Frame, batch *batchRequestDecoded) error {
	for i := range batch.items {
		item := &batch.items[i]
		synthetic := batchItemFrame(batchFrame, *item)
		if _, err := validateRequest(item.op, item.payload); err != nil {
			return itemGateRefusal(i, batch.items, err)
		}
		if err := s.authorize(ctx, synthetic); err != nil {
			return itemGateRefusal(i, batch.items, err)
		}
		// NewHandler's part-name door, per item (B7, spread MVP note rev
		// 2.1): the envelope is unscoped, so it never saw this item's bucket,
		// and an item naming a part would place a document outside its part.
		if err := refuseClientPartName(synthetic); err != nil {
			return itemGateRefusal(i, batch.items, err)
		}
		// The deferred type limit is reported for a put or a B+tree insert
		// only (refuseOversizedWrite), which a batch does not carry, so no item
		// is ever marked.
		if _, err := s.refuseOversizedWrite(synthetic); err != nil {
			return itemGateRefusal(i, batch.items, err)
		}
		if err := refuseClientAdmitted(synthetic); err != nil {
			return itemGateRefusal(i, batch.items, err)
		}
		if err := refuseClientRepairBound(synthetic); err != nil {
			return itemGateRefusal(i, batch.items, err)
		}
		item.payload = synthetic.Payload
	}
	return nil
}

// runStandaloneBatch runs a batch on a node that does not route (§5.1). Each
// item goes through Server.fencedDispatch, the call a lone frame takes after
// its gates, and its local step ends when that returns. The whole batch runs
// under one acknowledgeDurably, as a restore chunk runs under one frame's
// commit note: the items commit concurrently, and one kv.Sync after the last
// of them, before any answer is sent, makes them durable. If that sync fails,
// every write answered a success is answered unknown with the not-durable
// text; reads are unchanged.
func (s *Server) runStandaloneBatch(ctx context.Context, frame *transport.Frame, batch *batchRequestDecoded) []batchAnswer {
	plan := &batchPlan{
		items:       batch.items,
		predecessor: chainPredecessors(batch.items),
		budget:      s.batchReplyBudget(),
		holdLocal:   s.batchGateHolder(batch.items, func(int) bool { return true }),
		run: func(ctx context.Context, i int) batchAnswer {
			item := batch.items[i]
			itemFrame := batchItemFrame(frame, item)
			if !item.write() {
				release, err := s.takeExecuteSlot(ctx)
				if err != nil {
					return answerFromFrame(errorFrame(itemFrame, err))
				}
				// Given back when the answer is produced, not when it is
				// charged: held to the charge, pool + 1 reads in one batch
				// would wait on each other for ever (§4.5, F2).
				defer release()
			}
			reply, err := s.fencedDispatch(ctx, itemFrame)
			if err != nil {
				reply = errorFrame(itemFrame, err)
			}
			return answerFromFrame(reply)
		},
	}
	var answers []batchAnswer
	_, err := acknowledgeDurably(ctx, s, func(ctx context.Context) (struct{}, error) {
		answers = s.runBatchPlan(ctx, plan)
		return struct{}{}, nil
	})
	var notDurable *writeNotDurableError
	if errors.As(err, &notDurable) {
		for i, item := range batch.items {
			if item.write() && classifyAnswer(item, answers[i]) == batchItemOK {
				answers[i] = answerFromFrame(errorFrame(batchItemFrame(frame, item), err))
			}
		}
	}
	return answers
}

// takeExecuteSlot takes one of the node's query execution slots for a batch's
// read item, on the node that executes it (§4.5). A coordinator that forwards
// a read takes none.
func (s *Server) takeExecuteSlot(ctx context.Context) (func(), error) {
	gate := s.queryGate()
	return gate.acquire(ctx, gate.executePool())
}

// batchDepthRefusal is the whole-batch refusal of a BATCH Validate refused.
// The envelope's limit is three levels above an item's, so an item past its
// own limit is what passes the envelope's: the refusal names the first such
// item, found by raw lookups and Validate at the item's limit, never by a
// decode, which is what Validate protects. A batch refused elsewhere is
// refused with Validate's own words.
func batchDepthRefusal(payload []byte, envelopeErr error) error {
	items, ok := bson.Raw(payload).Lookup("items").ArrayOK()
	if ok {
		values, err := items.Values()
		if err == nil {
			for i, value := range values {
				if value.Type != bson.TypeEmbeddedDocument {
					continue
				}
				number, isInt := value.Document().Lookup("opcode").Int32OK()
				itemPayload, isDoc := value.Document().Lookup("payload").DocumentOK()
				if !isInt || !isDoc || number < 0 || number > 0xFF {
					continue
				}
				if _, itemErr := validateRequest(transport.Opcode(number), itemPayload); itemErr != nil {
					return &batchRefusal{code: codeFor(itemErr), cause: itemErr, message: fmt.Sprintf("Item %s of %s: %s. %s",
						groupThousands(i+1), groupThousands(len(values)), strings.TrimSuffix(explainError(itemErr), "."), batchNothingRun)}
				}
			}
		}
	}
	return &batchRefusal{code: codeFor(envelopeErr), cause: envelopeErr, message: fmt.Sprintf("The batch: %s. %s",
		strings.TrimSuffix(explainError(envelopeErr), "."), batchNothingRun)}
}
