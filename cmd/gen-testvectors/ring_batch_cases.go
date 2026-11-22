package main

import (
	"bytes"

	hc "github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/transport"
)

// batchEdgeInputs are the values the RING_BATCH edge vectors are built from: the reply stamper,
// the frame prologue, the lone keyed insert and its applied reply, and the two operations the
// frames repeat.
type batchEdgeInputs struct {
	stamp    func(wire.RingReply) wire.RingReply
	prologue wire.RingPrologue
	insert   wire.RingKeyedRequest
	header   hc.Op
	upsert   hc.Op
	applied  wire.RingReply
}

// batchEdgeVectors are the RING_BATCH cases of spec section 7 that the first vectors leave out: a
// batch of one equal to the lone request, the 255 cap, a reply the budget trimmed, and a tail the
// deadline left not attempted. in.applied is the stamped reply of the lone insert.
func (b *ringBuilder) batchEdgeVectors(in batchEdgeInputs) {
	b.exchange("ring batch: a batch of one answers as the lone request would", "A batch of one is operation 0 under the request id, so its inner reply is byte-equal to the reply of the lone RING_KEYED insert vector, and its dedupe entry is that request's.",
		transport.OpRingBatch, nil, b.must(wire.EncodeRingBatch(wire.RingBatchRequest{Prologue: in.prologue, Ops: []hc.Op{in.insert.Op}})), ringReplyFields,
		b.must(wire.EncodeRingReply(in.stamp(wire.RingReply{Outcome: "ok", Answers: []wire.RingAnswer{{Reply: &in.applied}}}))))

	full := make([]hc.Op, 255)
	answers := make([]wire.RingAnswer, 255)
	for index := range full {
		full[index] = in.upsert
		answers[index] = wire.RingAnswer{Reply: &in.applied}
	}
	b.exchange("ring batch: 255 operations, the most a frame holds", "The count byte is 255: the largest frame, answered with 255 answers in request order. A 256th operation cannot be written (the count is one byte and the index of an operation is one byte of its dedupe key), so a driver refuses it when it queues it, before sending anything: 'at most 255 ... Split it into batches of 255 or fewer'.",
		transport.OpRingBatch, nil, b.must(wire.EncodeRingBatch(wire.RingBatchRequest{Prologue: in.prologue, Ops: full})), ringReplyFields,
		b.must(wire.EncodeRingReply(in.stamp(wire.RingReply{Outcome: "ok", Answers: answers}))))

	claim := in.header
	claim.Class, claim.Fields, claim.Count = hc.OpClaim, []hc.FieldOp{{FieldID: 4}}, 5
	copies := int32(1)
	position := hc.Position{Epoch: 4, Seq: 98215}
	leased := in.stamp(wire.RingReply{WriteOutcome: "applied", CopiesConfirmed: &copies, ServedBy: "node-a", Position: &position, Lease: bytes.Repeat([]byte{0x5c}, 16)})
	b.exchange("ring batch: a reply over the budget drops a claim's elements, not the claim", "The reply budget is min(max_result_bytes, 16 MiB). A recomputable reply that does not fit keeps write_outcome, copies_confirmed, position, deduped, found, id, served_by and the lease and drops elements, keys and priorities: the claim stays applied, its lease id stays, and lease_get reads the leased messages back. A pop or take answer is never trimmed; one the budget has no room for is not run and is answered UNAVAILABLE, not attempted.",
		transport.OpRingBatch, nil, b.must(wire.EncodeRingBatch(wire.RingBatchRequest{Prologue: in.prologue, Ops: []hc.Op{claim}})), ringReplyFields,
		b.must(wire.EncodeRingReply(in.stamp(wire.RingReply{Outcome: "ok", Answers: []wire.RingAnswer{{Reply: &leased}}}))))

	notAttempted := wire.RingError{Code: hc.CodeUnavailable, Message: "operation not attempted: the batch's deadline was spent before it started. It was not applied. Resend the whole frame with the same request id"}
	b.exchange("ring batch: the deadline leaves a not-attempted tail", "deadline_ms is the whole frame's budget. Once it is spent the operations not yet started are answered UNAVAILABLE saying not attempted, not applied, and to resend the whole frame with the same request id; they are never unknown. Which operations fall in the tail is not deterministic when more than one pair is in flight. The caller sends the whole frame again under the same request id: the applied operation answers deduped, the others run.",
		transport.OpRingBatch, nil, b.must(wire.EncodeRingBatch(wire.RingBatchRequest{Prologue: in.prologue, Ops: []hc.Op{in.upsert, in.upsert, in.upsert}})), ringReplyFields,
		b.must(wire.EncodeRingReply(in.stamp(wire.RingReply{Outcome: "partial", Answers: []wire.RingAnswer{{Reply: &in.applied}, {Error: &notAttempted}, {Error: &notAttempted}}}))))
}
