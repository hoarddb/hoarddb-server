package ringclient

import (
	"context"
	"errors"
	"fmt"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Batch sends up to 255 write operations, on any buckets, in one request. Build the
// operations with the write methods of a [Bucket] made by [Bucket.In], then call [Batch.Send].
//
// A batch is not a transaction, and it follows MongoDB's unordered bulkWrite: each operation
// is applied or refused on its own, a refused one does not stop the others, and operations on
// one document apply in the order they were queued. Send returns an answer for every
// operation, in the order queued. To retry after a lost answer, call Send again on the same
// Batch: it reuses the request id, the operations that were applied answer Deduped, and the
// rest run. Retrying a part of a batch is not offered, because the position of an operation
// in the frame is part of its identity.
//
// A Batch is not safe for concurrent use.
type Batch struct {
	client *Client
	ops    []batchedOp
	// id is the request id of the frame, drawn on the first Send and kept for every retry.
	id    [16]byte
	hasID bool
}

type batchedOp struct {
	bucket *Bucket
	build  func(BucketInfo) (wire.RingKeyedRequest, error)
}

// NewBatch starts an empty batch.
func (c *Client) NewBatch() *Batch { return &Batch{client: c} }

// In returns a handle on the same bucket whose write methods (Insert, Upsert, Update,
// Delete, Push, PushHeap, Put, Inc, Pop, PopN, Take, Claim, Ack, Release, Extend) queue their operation on
// batch instead of sending it, and return a zero result: the real answer is in the
// [BatchAnswer] that [Batch.Send] returns. Its read methods are refused, since a batch carries
// writes only. Pass [WithRequestID] and [WithWriteDeadline] to Send, not to the methods.
func (b *Bucket) In(batch *Batch) *Bucket {
	return &Bucket{client: b.client, database: b.database, bucket: b.bucket, batch: batch}
}

// Len is how many operations are queued.
func (batch *Batch) Len() int { return len(batch.ops) }

// queue adds the operation build makes, after the same checks a lone request meets, and
// returns an empty reply: the answer comes with Send.
func (batch *Batch) queue(ctx context.Context, bucket *Bucket, options []CallOption, build func(BucketInfo) (wire.RingKeyedRequest, error)) (wire.RingReply, error) {
	if config := newCallConfig(options); config.hasRequestID || config.deadlineMs != 0 {
		return wire.RingReply{}, errors.New("ringclient: a queued operation takes no WithRequestID or WithWriteDeadline: the whole batch is one request. Pass them to Batch.Send")
	}
	if err := wire.CheckRingBatchSize(len(batch.ops) + 1); err != nil {
		return wire.RingReply{}, fmt.Errorf("ringclient: queuing operation %d of the batch on %s: %w", len(batch.ops), bucket.where(), err)
	}
	request, err := batch.buildOne(ctx, bucket, build, false)
	if err != nil {
		return wire.RingReply{}, err
	}
	if request.Read != nil {
		return wire.RingReply{}, fmt.Errorf("ringclient: a batch carries writes only, and this is a read of %s. Make the read on a Bucket that was not made with In", bucket.where())
	}
	batch.ops = append(batch.ops, batchedOp{bucket: bucket, build: build})
	return wire.RingReply{}, nil
}

// buildOne builds one operation under the bucket's header. A field the cached header does not
// declare relearns the header once, as a lone request does.
func (batch *Batch) buildOne(ctx context.Context, bucket *Bucket, build func(BucketInfo) (wire.RingKeyedRequest, error), refresh bool) (wire.RingKeyedRequest, error) {
	var info BucketInfo
	var err error
	if refresh {
		info, err = batch.client.DescribeBucket(ctx, bucket.database, bucket.bucket)
	} else {
		info, err = bucket.Info(ctx)
	}
	if err != nil {
		return wire.RingKeyedRequest{}, err
	}
	request, err := build(info)
	var undeclared undeclaredField
	if errors.As(err, &undeclared) && !refresh {
		return batch.buildOne(ctx, bucket, build, true)
	}
	return request, err
}

// BatchAnswer is what one operation of a batch came to.
type BatchAnswer struct {
	// Err is the server's refusal of this operation, an *[Error], or nil when it was answered.
	// The other members are zero when Err is set.
	Err error
	// Write is what the owner said about the write.
	Write WriteResult
	// Elements are what a pop, a take or a claim removed or leased.
	Elements []bson.Raw
	// Found says a pop or take found an element, or that a release or extend found its lease live.
	Found bool
	// Lease is the lease id a claim returned; zero when there was nothing to lease.
	Lease [16]byte
}

// BatchResult is the answer to a batch.
type BatchResult struct {
	// Outcome is "ok" when every operation was answered, "partial" when any was refused.
	Outcome string
	// Answers has one entry per operation, in the order they were queued.
	Answers []BatchAnswer
}

// Send sends the queued operations as one request and returns an answer for each. A refusal of
// the whole request (the batch did not decode, the connection failed) is the error; a refusal of
// one operation is that operation's answer, in Err. When an answer says the bucket's schema or id
// moved, Send learns the bucket's header again and sends the whole batch once more under the
// same request id. Pass [WithRequestID] to choose the id and [WithWriteDeadline] to bound the
// whole batch; the deadline is shared, and operations that start after it is spent answer
// UNAVAILABLE and were not applied.
func (batch *Batch) Send(ctx context.Context, options ...CallOption) (BatchResult, error) {
	config := newCallConfig(options)
	if config.hasRequestID {
		batch.id, batch.hasID = config.requestID, true
	}
	if !batch.hasID {
		batch.id, batch.hasID = batch.client.NextRequestID(), true
	}
	refresh := false
	for {
		result, moved, err := batch.sendOnce(ctx, config.deadlineMs, refresh)
		if err != nil || !moved || refresh {
			return result, err
		}
		refresh = true
	}
}

// sendOnce builds and sends the frame. moved says some answer was a refusal that a fresh header
// can fix; the caller then calls it again with refresh set.
func (batch *Batch) sendOnce(ctx context.Context, deadlineMs uint32, refresh bool) (result BatchResult, moved bool, err error) {
	ops := make([]cluster.Op, len(batch.ops))
	described := map[*Bucket]bool{}
	for i, queued := range batch.ops {
		again := refresh && !described[queued.bucket]
		described[queued.bucket] = true
		request, err := batch.buildOne(ctx, queued.bucket, queued.build, again)
		if err != nil {
			return BatchResult{}, false, fmt.Errorf("ringclient: building operation %d of the batch for %s: %w", i, queued.bucket.where(), err)
		}
		ops[i] = request.Op
	}
	body, err := wire.EncodeRingBatch(wire.RingBatchRequest{Prologue: wire.RingPrologue{DeadlineMs: deadlineMs}, Ops: ops})
	if err != nil {
		return BatchResult{}, false, fmt.Errorf("ringclient: encoding the batch: %w", err)
	}
	payload, err := batch.client.call(ctx, transport.OpRingBatch, batch.id, body)
	if err != nil {
		return BatchResult{}, false, err
	}
	reply, err := wire.DecodeRingReply(payload)
	if err != nil {
		return BatchResult{}, false, fmt.Errorf("ringclient: decoding the batch reply: %w", err)
	}
	if len(reply.Answers) != len(ops) {
		return BatchResult{}, false, fmt.Errorf("ringclient: the server answered a batch of %d operations with %d answers", len(ops), len(reply.Answers))
	}
	result = BatchResult{Outcome: reply.Outcome, Answers: make([]BatchAnswer, len(reply.Answers))}
	for i, answer := range reply.Answers {
		if answer.Error != nil {
			refusal := &Error{Code: answer.Error.Code, Message: answer.Error.Message, Detail: answer.Error.Detail}
			result.Answers[i].Err = refusal
			moved = moved || refusal.Code == cluster.CodeSchemaNotCurrent || staleBucketID(refusal)
			continue
		}
		lone := answer.Reply
		result.Answers[i] = BatchAnswer{
			Write: writeResult(*lone), Elements: rawList(lone.Elements), Found: lone.Found != nil && *lone.Found,
		}
		copy(result.Answers[i].Lease[:], lone.Lease)
	}
	return result, moved, nil
}
