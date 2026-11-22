package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/hoarddb/hoarddb-server/bench/social/ringload"
	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/ringclient"
)

const (
	// defaultRingBatchSize is the pushes in one RING_BATCH frame with -ring-batch.
	defaultRingBatchSize = 64
	// maxRingBatchSize is the most operations a frame holds (wire.CheckRingBatchSize).
	maxRingBatchSize = 255
)

// batchFrame is one send of a frame: how many operations it carried, how long the round trip
// took, whether it is a resend of a frame already sent, and how many of its answers were
// refused outright or not attempted.
type batchFrame struct {
	ops          int
	took         time.Duration
	resend       bool
	notAttempted int
	refused      int
}

// batchCounts is the totals of a run, copied out under the lock.
type batchCounts struct {
	frames        int64 // sends, resends included
	ops           int64 // operations of first sends: the distinct pushes
	retriedFrames int64
	refused       int64
	notAttempted  int64
}

// batchTotals gathers what the frame sender did across the workers of a leg: the counts the
// bench must never drop, and the round trips as a per-operation latency histogram.
type batchTotals struct {
	size int

	mutex       sync.Mutex
	counts      batchCounts
	latency     *histogram
	overflowed  int64 // samples above the histogram's ceiling, left out of the quantiles
	first, last time.Time
}

func newBatchTotals(size int) *batchTotals {
	return &batchTotals{size: size, latency: newHistogram()}
}

// record adds one send. A frame's round trip is charged to each of its operations, because the
// caller of a batch cannot tell them apart: it is an upper bound for each, not a service time.
func (t *batchTotals) record(frame batchFrame) {
	now := time.Now()
	t.mutex.Lock()
	defer t.mutex.Unlock()
	if t.first.IsZero() {
		t.first = now.Add(-frame.took)
	}
	t.last = now
	t.counts.frames++
	if frame.resend {
		t.counts.retriedFrames++
	} else {
		t.counts.ops += int64(frame.ops)
	}
	t.counts.refused += int64(frame.refused)
	t.counts.notAttempted += int64(frame.notAttempted)
	for range frame.ops {
		if err := t.latency.record(frame.took); err != nil {
			t.overflowed++
		}
	}
}

func (t *batchTotals) snapshot() batchCounts {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	return t.counts
}

// summary is the block a leg ends with. The frame size is in the header line, so a pasted figure
// never loses it.
func (t *batchTotals) summary() string {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	span := t.last.Sub(t.first)
	rate := 0.0
	if span > 0 {
		rate = float64(t.counts.ops) / span.Seconds()
	}
	var text strings.Builder
	fmt.Fprintf(&text, "ring-batch: frame size %d (internal figures, not for any public page)\n", t.size)
	fmt.Fprintf(&text, "ring-batch: %d ops in %d sends (%d resent), %d refused, %d not attempted\n",
		t.counts.ops, t.counts.frames, t.counts.retriedFrames, t.counts.refused, t.counts.notAttempted)
	if t.overflowed > 0 {
		fmt.Fprintf(&text, "ring-batch: %d latency samples were above the histogram's ceiling and are not in the quantiles\n", t.overflowed)
	}
	fmt.Fprintf(&text, "ring-batch: per-op latency p50 %v p99 %v (a frame's round trip charged to each op), %.0f ops/s over %v\n",
		t.latency.quantile(0.5), t.latency.quantile(0.99), rate, span.Round(time.Millisecond))
	return text.String()
}

// pushFeedFrames pushes element onto the Feed of every follower in frames of at most batchSize
// operations, one frame after another. Followers keep their list order, which a frame applies
// in queue order, so a follower named twice receives its copies in order.
func (o *ringOps) pushFeedFrames(ctx context.Context, followers []int, element any, ledger deliveryLedger) error {
	for start := 0; start < len(followers); start += o.batchSize {
		frame := followers[start:min(start+o.batchSize, len(followers))]
		if err := o.sendFeedFrame(ctx, frame, element, ledger); err != nil {
			return err
		}
	}
	return nil
}

// sendFeedFrame sends one frame and, per spec 7.3 rule 4, sends the whole frame again under the
// same request id while an operation answers UNAVAILABLE (not attempted, or unknown) or the
// answer to the frame was lost, at most feedPushRetries times. The operations already applied
// answer deduped. Any other refusal of an operation is definitive: it is counted, the frame is
// not resent, and the first such refusal is returned naming its follower.
func (o *ringOps) sendFeedFrame(ctx context.Context, frame []int, element any, ledger deliveryLedger) error {
	stores := make([]storeRef, len(frame))
	for index, follower := range frame {
		stores[index] = userStore(follower, ringload.UserFeed)
		ledger.sending(follower)
	}
	id := o.api.nextRequestID()
	var lastFailure error
	var answers []error
	for attempt := 0; attempt <= feedPushRetries; attempt++ {
		started := time.Now()
		var err error
		answers, err = o.api.sendBatch(ctx, stores, element, id)
		sample := batchFrame{ops: len(frame), took: time.Since(started), resend: attempt > 0}
		if err != nil {
			o.batch.record(sample)
			lastFailure = fmt.Errorf("sending a frame of %d feed pushes: %w", len(frame), err)
			if !outcomeUnknown(err) || ctx.Err() != nil {
				return lastFailure
			}
			continue
		}
		var refusal error
		for index, answer := range answers {
			switch {
			case answer == nil:
			case isNotAttempted(answer):
				sample.notAttempted++
				lastFailure = fmt.Errorf("into the feed of user %d: %w", frame[index], answer)
			default:
				sample.refused++
				if refusal == nil {
					refusal = fmt.Errorf("into the feed of user %d: %w", frame[index], answer)
				}
			}
		}
		o.batch.record(sample)
		if refusal != nil {
			deliverApplied(frame, answers, ledger)
			return refusal
		}
		if sample.notAttempted == 0 {
			deliverApplied(frame, answers, ledger)
			return nil
		}
	}
	deliverApplied(frame, answers, ledger)
	return lastFailure
}

// deliverApplied tells the ledger about the operations whose answer was an applied write.
func deliverApplied(frame []int, answers []error, ledger deliveryLedger) {
	for index, answer := range answers {
		if answer == nil {
			ledger.delivered(frame[index])
		}
	}
}

// isNotAttempted is whether an operation's answer says it may be sent again: UNAVAILABLE.
func isNotAttempted(answer error) bool {
	var refusal *ringclient.Error
	return errors.As(answer, &refusal) && refusal.Code == cluster.CodeUnavailable
}

// sendBatch sends the pushes of element onto each store as one frame under id and returns one
// answer per store, in order: nil when applied (or deduped), the refusal otherwise.
func (c *ringConn) sendBatch(ctx context.Context, stores []storeRef, element any, id [16]byte) ([]error, error) {
	batch := c.client.NewBatch()
	for index, store := range stores {
		bucket := c.client.Bucket(c.database, store.bucket).In(batch)
		if _, err := bucket.Push(ctx, store.id, store.field, element); err != nil {
			return nil, fmt.Errorf("queuing push %d of the frame onto %s %s: %w", index, store.bucket, store.id, err)
		}
	}
	c.sent.Add(1)
	sent, err := batch.Send(ctx, ringclient.WithRequestID(id))
	if err != nil {
		return nil, err
	}
	answers := make([]error, len(sent.Answers))
	for index, answer := range sent.Answers {
		switch {
		case answer.Err != nil:
			answers[index] = answer.Err
		case !answer.Write.Applied():
			answers[index] = fmt.Errorf("the server's write outcome was %q, not applied", answer.Write.Outcome)
		}
	}
	return answers, nil
}
