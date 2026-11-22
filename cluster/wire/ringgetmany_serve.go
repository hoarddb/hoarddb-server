package wire

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// getManyFrameInFlight is the most reads one RING_GET_MANY frame has in flight across all its
// owners (spec section 2.3). With ringBatchInFlight per owner it bounds what a frame holds to the
// reply budget plus 16 documents, whatever the documents weigh.
const getManyFrameInFlight = 16

// GetManyStats are the node-wide counters of RING_GET_MANY: frames, keys, keys answered with an
// error and keys a copy on this node answered. There is no per-bucket series, and none is exposed
// this release: tests read them.
type GetManyStats struct {
	Frames, Keys, Errors, ServedLocal atomic.Int64
}

// GetManyStats returns the node's get-many counters.
func (n *Node) GetManyStats() *GetManyStats { return &n.getManyStats }

// getMany answers a RING_GET_MANY frame. A payload that does not decode is one refusal for the
// frame; past that every key is answered on its own, in request order.
func (s *ringServer) getMany(ctx context.Context, frame *transport.Frame) (*transport.Frame, error) {
	req, err := DecodeRingGetMany(frame.Payload)
	if err != nil {
		return ringFailure(frame, err)
	}
	payload, err := EncodeRingReply(s.serveGetMany(ctx, req))
	if err != nil {
		return ringFailure(frame, err)
	}
	return transport.NewFrameWithID(transport.OpRingReply, frame.RequestID, payload), nil
}

// getManyReadKey names one read of a frame: the same document asked twice with the same
// projection and the same local is read once and answers both keys.
type getManyReadKey struct {
	bucketID   uint64
	id         string
	projection string
	local      bool
}

// getManyProjectionKey names a projection for a read key. A nil projection (the whole body) and an
// empty non-nil one (only _id_) both print as [], so the key says which it is.
func getManyProjectionKey(project []uint16) string {
	return fmt.Sprintf("%t%v", project != nil, project)
}

// serveGetMany runs the keys of a decoded frame (spec section 2.4). Each key is admitted on its
// own through the lone request's path, with a memo that also remembers refused bucket ids, so a
// frame never widens a grant and 255 keys on one unknown id cost one lookup. The admitted keys
// are read through the node's read path (gate, holds, the copy read and the one forward to the
// owner all happen inside it), grouped by owner by readMany; a key's error is that key's answer
// and never stops another key.
func (s *ringServer) serveGetMany(ctx context.Context, req RingGetManyRequest) RingReply {
	count := len(req.Reads)
	deadlineMs := req.Prologue.DeadlineMs
	// deadline_ms is the frame's budget from when the node began it (R3). A lone read ignores it,
	// so the handler bounds every key itself with one context, shared by all the keys: a key
	// parked in a hold or forwarded to its owner stops when the frame's time is spent.
	if deadlineMs > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, time.Now().Add(time.Duration(deadlineMs)*time.Millisecond))
		defer cancel()
	}
	s.node.getManyStats.Frames.Add(1)
	s.node.getManyStats.Keys.Add(int64(count))
	answers := make([]RingAnswer, count)
	memo := newAdmitMemo(true)
	var refs []expandRef
	var keysOf [][]int
	byRead := map[getManyReadKey]int{}
	for index := range req.Reads {
		read := &req.Reads[index]
		if getManySpent(ctx) {
			// Admission can take a lookup per key, so the deadline stops it too.
			answers[index] = ringErrorAnswer(getManyDeadline(index, count, deadlineMs, false))
			continue
		}
		ref, err := s.admitIn(ctx, RingKeyedRequest{Prologue: req.Prologue, Read: read}, memo)
		if err != nil && getManySpent(ctx) && errors.Is(err, context.DeadlineExceeded) {
			err = getManyDeadline(index, count, deadlineMs, false)
		}
		if err != nil {
			answers[index] = ringErrorAnswer(err)
			continue
		}
		var project []uint16
		if read.Class == RingClassGetFields {
			if project, err = ref.projection(read.Fields); err != nil {
				answers[index] = ringErrorAnswer(err)
				continue
			}
		}
		key := getManyReadKey{ref.entry.Decl.BucketID, string(read.EncID), getManyProjectionKey(project), read.Options.Local}
		at, shared := byRead[key]
		if !shared {
			at = len(refs)
			byRead[key] = at
			refs = append(refs, expandRef{ref: ref, encID: read.EncID, slot: ref.slot(read.EncID), project: project, local: read.Options.Local, at: at})
			keysOf = append(keysOf, nil)
		}
		keysOf[at] = append(keysOf[at], index)
	}

	ownerOf := s.node.ownerOfSlot
	if s.manyOwner != nil {
		ownerOf = s.manyOwner
	}
	fetch := s.manyFetch
	if fetch == nil {
		fetch = s.node.readDocumentFrom
	}
	// The budget is charged as each answer lands, never after the frame, so the frame holds at
	// most the budget plus the reads in flight (R2). used is the bytes of answers kept; produced
	// also counts the answers dropped for want of room, and once it reaches the budget no further
	// read is launched: otherwise a frame of documents that each just miss would read every key
	// to keep none. readMany calls proceed and settle one at a time, so they share these freely.
	budget := s.resultBudget
	var used, produced int64
	full := false
	results := readMany(ctx, refs, readManyPlan{
		fetch:         fetch,
		ownerOf:       ownerOf,
		frameInFlight: getManyFrameInFlight,
		proceed: func() bool {
			full = full || produced >= budget
			return !full
		},
		settle: func(at int, result manyResult) manyResult {
			ref := refs[at]
			for _, index := range keysOf[at] {
				answer := s.getManyAnswer(ref.ref, req.Reads[index], result)
				if answer.Error != nil && getManySpent(ctx) && getManyRanOutOfTime(result.err) {
					// A read cut off by the frame's deadline ends with the context's error or with
					// the refusal of the hold it was parked in; either way the key ran out of time.
					// Any other error settled on its own account and keeps its own text.
					answer = ringErrorAnswer(getManyDeadline(index, count, deadlineMs, true))
				}
				if answer.Reply != nil && len(answer.Reply.Elements) > 0 {
					size := getManyAnswerBytes(*answer.Reply)
					switch {
					case size > budget:
						// It fits no frame, so it takes no room from the keys after it: it is
						// dropped at once (memory stays bounded) and not counted as produced.
						answer = ringErrorAnswer(getManyTooLarge(index, count, ref, budget))
					case used+size > budget:
						produced += size
						answer = ringErrorAnswer(getManyDoesNotFit(index, count, ref, budget, false))
					default:
						produced += size
						used += size
					}
				}
				answers[index] = answer
			}
			if s.manyHeld != nil {
				s.manyHeld(used)
			}
			// The document now lives only in the answers that kept it.
			return manyResult{read: true}
		},
	})
	for at, result := range results {
		if result.read {
			continue
		}
		for _, index := range keysOf[at] {
			answers[index] = ringErrorAnswer(getManyNotRead(ctx, index, count, refs[at], budget, deadlineMs, full))
		}
	}

	reply := RingReply{Outcome: ringBatchOutcome(answers), Answers: answers}
	s.node.getManyStats.Errors.Add(int64(ringCountErrors(answers)))
	epoch, stateSeq := int64(s.node.topology.Committed()), s.node.topology.seq.Load()
	reply.Epoch, reply.StateSeq = &epoch, &stateSeq
	return reply
}

// getManyAnswer is the answer of one key whose read ended: the lone get's reply (found with the
// document, or found:false for a document that is gone, tombstoned or expired), or its error.
func (s *ringServer) getManyAnswer(ref bucketRef, read RingRead, result manyResult) RingAnswer {
	var coded *cluster.Error
	switch {
	case result.err == nil:
		if result.doc.Served == servedLocal {
			s.node.getManyStats.ServedLocal.Add(1)
		}
		return RingAnswer{Reply: &RingReply{Found: ringTrue(), Elements: [][]byte{result.doc.Body}, Served: servedMember(read.Options.Local, result.doc)}}
	case errors.As(result.err, &coded) && coded.Code == cluster.CodeNotFound:
		return RingAnswer{Reply: &RingReply{Found: ringFalse()}}
	}
	return ringErrorAnswer(fmt.Errorf("reading from %s.%s: %w", ref.db, ref.name, result.err))
}

// getManyNotRead is the answer of a key whose read was never launched: the reply was already
// full, the frame's deadline was spent, or the request ended before its turn.
func getManyNotRead(ctx context.Context, index, count int, ref expandRef, budget int64, deadlineMs uint32, full bool) error {
	switch {
	case getManySpent(ctx):
		return getManyDeadline(index, count, deadlineMs, false)
	case full:
		return getManyDoesNotFit(index, count, ref, budget, true)
	}
	return &cluster.Error{Code: cluster.CodeUnavailable, Message: fmt.Sprintf(
		"key %d of %d was not read: the request ended before its turn. Reads change nothing, so resend it", index, count)}
}

// getManySpent says the frame's deadline has passed.
func getManySpent(ctx context.Context) bool {
	return errors.Is(ctx.Err(), context.DeadlineExceeded)
}

// holdOverdueText is the words every refusal of a hold that ran out of the caller's time shares
// (Handoff.overdue): the read was parked and its deadline passed before the hold lifted.
const holdOverdueText = "deadline passed"

// getManyRanOutOfTime says a read's error is the frame's deadline reaching it: the context's own
// error, the i/o timeout of a forwarded read (the data channel turns the context's deadline into a
// connection deadline), or the UNAVAILABLE a hold gives when its budget (the frame's deadline) runs out.
func getManyRanOutOfTime(err error) bool {
	var coded *cluster.Error
	switch {
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, os.ErrDeadlineExceeded):
		return true
	case errors.As(err, &coded):
		return coded.Code == cluster.CodeUnavailable && strings.Contains(coded.Message, holdOverdueText)
	}
	return false
}

// getManyDeadline is the UNAVAILABLE of a key the frame's deadline reached: before it was
// launched, or while it was being read. Neither says Go's "context deadline exceeded".
func getManyDeadline(index, count int, deadlineMs uint32, inFlight bool) error {
	what := "was not read"
	if inFlight {
		what = "was cut off while it was being read"
	}
	return &cluster.Error{Code: cluster.CodeUnavailable, Message: fmt.Sprintf(
		"key %d of %d %s: the frame's deadline of %d ms was spent. Reads change nothing, so resend it: in a smaller frame or with a longer deadline",
		index, count, what, deadlineMs)}
}

// getManyNamed is how a budget refusal names its key: index, bucket and document id.
func getManyNamed(index, count int, ref expandRef) string {
	id, err := store.DecodeStringID(ref.encID)
	if err != nil {
		id = fmt.Sprintf("%x", ref.encID)
	}
	return fmt.Sprintf("key %d of %d (bucket %q, id %q)", index, count, ref.ref.name, id)
}

// getManyDoesNotFit is RESULT_TOO_LARGE for a key whose answer does not fit what is left of the
// reply, or that was not read because the reply was already full.
func getManyDoesNotFit(index, count int, ref expandRef, budget int64, notRead bool) error {
	why := fmt.Sprintf("its answer does not fit what is left of the %s reply", byteSize(budget))
	if notRead {
		why = fmt.Sprintf("the %s reply was already full, so it was not read", byteSize(budget))
	}
	return &cluster.Error{Code: cluster.CodeResultTooLarge, Message: fmt.Sprintf(
		"%s was not returned: %s. Reads change nothing: ask for it again in a smaller frame, or with GetFields naming only the fields you need",
		getManyNamed(index, count, ref), why)}
}

// getManyTooLarge is RESULT_TOO_LARGE for a document whose answer alone is larger than the whole
// budget: no get-many frame can return it, and a lone Get may (up to the 32 MiB frame cap).
func getManyTooLarge(index, count int, ref expandRef, budget int64) error {
	return &cluster.Error{Code: cluster.CodeResultTooLarge, Message: fmt.Sprintf(
		"%s is larger than the %s a get-many reply can carry, so no frame can return it. Read this document with Get",
		getManyNamed(index, count, ref), byteSize(budget))}
}

// byteSize writes n the way a refusal says a budget: "16 MiB", "256 KiB" or "40660 bytes".
func byteSize(n int64) string {
	switch {
	case n > 0 && n%(1<<20) == 0:
		return fmt.Sprintf("%d MiB", n>>20)
	case n > 0 && n%(1<<10) == 0:
		return fmt.Sprintf("%d KiB", n>>10)
	}
	return fmt.Sprintf("%d bytes", n)
}

// getManyAnswerBytes is what an answer charges to the reply budget: the bytes of its encoded
// reply, as chargeBatchAnswers counts a batch's. It is worked out from the BSON layout, without
// encoding, so a 24 MiB document is not copied once more only to be measured. The handler keeps
// only a found reply: found, one element and served_from; a test pins the sum to EncodeRingReply.
func getManyAnswerBytes(reply RingReply) int64 {
	// A document is its 4-byte length, its members and a 0 terminator. A member is a type byte,
	// the name and its 0 terminator, then the value.
	size := int64(4 + 1)
	if reply.Found != nil {
		size += 1 + int64(len("found")) + 1 + 1
	}
	if len(reply.Elements) > 0 {
		array := int64(4 + 1)
		for index, element := range reply.Elements {
			// A binary value is a 4-byte length, a subtype byte and the bytes; its name is its index.
			array += 1 + int64(len(strconv.Itoa(index))) + 1 + 4 + 1 + int64(len(element))
		}
		size += 1 + int64(len("elements")) + 1 + array
	}
	if reply.Served != "" {
		size += 1 + int64(len("served_from")) + 1 + 4 + int64(len(reply.Served)) + 1
	}
	return size
}
