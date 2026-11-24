package wire

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/transport"
)

// ringBatchInFlight is how many pairs one RING_BATCH frame works on at once. Operations on
// different pairs must overlap so that their log appends share one group commit, the way the
// pipelined single-request fan-out they replace did; a serial loop would pay one fsync per
// pair. The bound keeps one frame from taking every core and every fsync slot, 255 operations
// at most. It is a constant and not a setting because no workload needs another value: past
// the first few the group commit is already one flush.
const ringBatchInFlight = 16

// maxRingBatchBudget caps the answers a batch reply keeps whole at 16 MiB, as the legacy BATCH
// does: with a 32 MiB frame that leaves room for the error answers, which are always kept.
const maxRingBatchBudget = 16 << 20

// batch answers a RING_BATCH frame. A payload that does not decode is one refusal for the
// frame; past that every operation is answered on its own, in request order.
func (s *ringServer) batch(ctx context.Context, frame *transport.Frame) (*transport.Frame, error) {
	req, err := DecodeRingBatch(frame.Payload)
	if err != nil {
		return ringFailure(frame, err)
	}
	reply := s.serveBatch(ctx, frame.RequestID, req)
	payload, err := EncodeRingReply(reply)
	if err != nil {
		return ringFailure(frame, err)
	}
	return transport.NewFrameWithID(transport.OpRingReply, frame.RequestID, payload), nil
}

// batchOp is one operation of a frame that passed admission, with where its answer goes.
type batchOp struct {
	index int
	keyed RingKeyedRequest
	ref   bucketRef
}

// serveBatch runs the operations of a decoded frame. Each operation is admitted on its own
// through the lone request's path (authorization by class, the bucket by id with its grant
// check, the client's epoch), so a frame never widens a session's grant. Operations naming
// one pair then run in request order, one after another; distinct pairs run concurrently,
// ringBatchInFlight at a time. A refused operation does not stop the others, and there is no
// atomicity across them, with one exception: once an operation on a store field of a document is
// refused, or answered with an outcome the owner cannot vouch for, every later operation on that
// same store field is refused too (batchStoreOf, batchHoldsBack). A resend runs the refused
// operation again, and had a later one on its store been applied, the resend would put it behind
// that one: a queue's elements out of order.
func (s *ringServer) serveBatch(ctx context.Context, requestID [16]byte, req RingBatchRequest) RingReply {
	started := time.Now()
	answers := make([]RingAnswer, len(req.Ops))
	groups := make(map[pairKey][]batchOp)
	var order []pairKey
	memo := newAdmitMemo(false)
	// refusedAtAdmission holds each store whose operation was refused here, with that
	// operation's index. All operations on one store fall in one group, run in request order,
	// so a refusal while they run is tracked by the group (run below).
	refusedAtAdmission := make(map[batchStoreKey]int)
	for index, op := range req.Ops {
		storeKey, onStore := batchStoreOf(op)
		if earlier, refused := refusedAtAdmission[storeKey]; onStore && refused {
			answers[index] = ringErrorAnswer(ringHeldBehindRefusal(index, earlier))
			continue
		}
		if ringBatchSpent(req.Prologue.DeadlineMs, started) {
			// Admission can take a remote lookup per operation, so the deadline stops it too.
			answers[index] = ringErrorAnswer(ringNotAttempted(index, req.Prologue.DeadlineMs))
			continue
		}
		keyed := RingKeyedRequest{Prologue: req.Prologue, Op: op}
		ref, err := s.admitIn(ctx, keyed, memo)
		if err != nil {
			answers[index] = ringErrorAnswer(err)
			if onStore {
				refusedAtAdmission[storeKey] = index
			}
			continue
		}
		key := pairKey{slot: ref.slot(op.EncID), db: ref.dbID}
		if _, seen := groups[key]; !seen {
			order = append(order, key)
		}
		groups[key] = append(groups[key], batchOp{index: index, keyed: keyed, ref: ref})
	}
	frame := ringBatchFrame{requestID: requestID, size: len(req.Ops), deadlineMs: req.Prologue.DeadlineMs, started: started,
		gate: newRingBatchGate(s.resultBudget, s.elementCeiling)}
	run := func(group []batchOp) {
		var refused map[batchStoreKey]int
		for _, op := range group {
			storeKey, onStore := batchStoreOf(op.keyed.Op)
			if earlier, held := refused[storeKey]; onStore && held {
				answers[op.index] = ringErrorAnswer(ringHeldBehindRefusal(op.index, earlier))
				continue
			}
			answers[op.index] = s.runBatchOp(ctx, frame, op)
			if onStore && batchHoldsBack(answers[op.index]) {
				if refused == nil {
					refused = make(map[batchStoreKey]int)
				}
				refused[storeKey] = op.index
			}
		}
	}
	if workers := min(ringBatchInFlight, len(order)); workers <= 1 {
		for _, key := range order {
			run(groups[key])
		}
	} else {
		jobs := make(chan []batchOp)
		var wg sync.WaitGroup
		for range workers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for group := range jobs {
					run(group)
				}
			}()
		}
		for _, key := range order {
			jobs <- groups[key]
		}
		close(jobs)
		wg.Wait()
	}
	s.chargeBatchAnswers(req.Ops, answers)
	reply := RingReply{Outcome: ringBatchOutcome(answers), Answers: answers}
	epoch, stateSeq := int64(s.node.topology.Committed()), s.node.topology.seq.Load()
	reply.Epoch, reply.StateSeq = &epoch, &stateSeq
	return reply
}

// ringBatchFrame is the state every operation of one RING_BATCH frame shares.
type ringBatchFrame struct {
	requestID  [16]byte
	size       int
	deadlineMs uint32
	started    time.Time
	gate       *ringBatchGate
}

// deadline is when the frame's deadline ends, or the zero time when it has none.
func (f ringBatchFrame) deadline() time.Time {
	if f.deadlineMs == 0 {
		return time.Time{}
	}
	return f.started.Add(time.Duration(f.deadlineMs) * time.Millisecond)
}

// runBatchOp runs one admitted operation under what is left of the frame's deadline. An
// irreversible operation (a pop, a take, an inc with return:"after") may first wait in the gate, so
// what is left is read after the wait: one that waited must not run with the budget it had before
// it queued.
func (s *ringServer) runBatchOp(ctx context.Context, frame ringBatchFrame, op batchOp) RingAnswer {
	irreversible := ringIrreversible(op.keyed.Op)
	if irreversible {
		switch frame.gate.enter(ctx, frame.deadline()) {
		case ringGateNoRoom:
			return ringErrorAnswer(&cluster.Error{Code: cluster.CodeUnavailable, Message: fmt.Sprintf(
				"operation %d of the batch was not attempted: it %s and the frame's reply has no room left to return it. It was not applied; nothing was changed. Send it alone or in a smaller frame",
				op.index, ringIrreversibleWhat(op.keyed.Op))})
		case ringGateEnded:
			if ctx.Err() != nil {
				return ringErrorAnswer(&cluster.Error{Code: cluster.CodeUnavailable, Message: fmt.Sprintf(
					"operation %d of the batch was not attempted: the request ended while it waited for room in the reply. It was not applied; nothing was changed. Resend the whole frame with the same request id",
					op.index)})
			}
			return ringErrorAnswer(ringNotAttempted(op.index, frame.deadlineMs))
		}
	}
	if frame.deadlineMs > 0 {
		left := time.Duration(frame.deadlineMs)*time.Millisecond - time.Since(frame.started)
		if left <= 0 {
			if irreversible {
				frame.gate.leave(0)
			}
			return ringErrorAnswer(ringNotAttempted(op.index, frame.deadlineMs))
		}
		op.keyed.Prologue.DeadlineMs = uint32(min(int64(frame.deadlineMs), max(1, (left+time.Millisecond-1).Milliseconds())))
	}
	execute := s.execute
	if s.batchExecute != nil {
		execute = s.batchExecute
	}
	reply, err := execute(ctx, op.ref, frame.requestID, op.keyed, ringBatchPos{index: uint8(op.index), size: frame.size})
	if irreversible {
		var used int64
		if err == nil {
			if raw, encodeErr := EncodeRingReply(reply); encodeErr == nil {
				used = int64(len(raw))
			}
		}
		frame.gate.leave(used)
	}
	if err != nil {
		return ringErrorAnswer(err)
	}
	return RingAnswer{Reply: &reply}
}

// batchStoreKey names one store field of one document: the unit whose operations a batch keeps
// in request order across a refusal.
type batchStoreKey struct {
	bucketID uint64
	encID    string
	fieldID  uint16
}

// batchStoreOf names the store field op works on, and says whether it works on one. Every class
// that names one store field is ordered by it: a push or a heap push sets an element's place, a
// put or an inc of one key is last-writer-wins, a pop or a take removes what the operations
// before it left, and a claim leases, and an ack, release or extend acts on, what is there when
// it runs. A document write (insert, upsert, update, delete) is not held back: a batch that
// inserts a document and then pushes into it, or inserts and then updates, relies on the later
// operation running when the earlier one is refused because the document exists, and a store
// verb behind a refused document write fails on its own when the document is missing.
func batchStoreOf(op cluster.Op) (batchStoreKey, bool) {
	switch op.Class {
	case cluster.OpStoreVerb, cluster.OpClaim, cluster.OpAck, cluster.OpRelease, cluster.OpExtend:
		// The decoder refuses any of these classes that does not name exactly one field.
		return batchStoreKey{bucketID: op.BucketID, encID: string(op.EncID), fieldID: op.Fields[0].FieldID}, true
	}
	return batchStoreKey{}, false
}

// batchHoldsBack says answer leaves the later operations on its store unsafe to run: a refusal,
// or a write_outcome of unknown. An unknown outcome is durable on this node but not vouched for
// (machine.Answer rows E and E'): when this node is no longer the owner, the successor may not
// hold it, and a later operation applied there would land in front of its resend.
func batchHoldsBack(answer RingAnswer) bool {
	return answer.Error != nil || answer.Reply.WriteOutcome == outcomeUnknown
}

// ringHeldBehindRefusal is the answer of an operation held back because operation earlier, on
// the same store field of the same document, was refused or has an outcome the owner cannot
// vouch for. It is UNAVAILABLE, a refusal that clears by resending, because nothing is wrong
// with the operation itself.
func ringHeldBehindRefusal(index, earlier int) error {
	return &cluster.Error{Code: cluster.CodeUnavailable, Message: fmt.Sprintf(
		"operation %d of the batch was not attempted: operation %d, on the same store of the same document, was refused or its outcome is unknown, and applying this one first could put the store out of order. It was not applied; nothing was changed. Send both again, in order: resending the whole frame with the same request id does that, and an operation already applied answers deduped",
		index, earlier)}
}

// ringBatchSpent says the frame's deadline, if it has one, is used up.
func ringBatchSpent(deadlineMs uint32, started time.Time) bool {
	return deadlineMs > 0 && time.Since(started) >= time.Duration(deadlineMs)*time.Millisecond
}

// ringNotAttempted is the answer of an operation the frame's deadline reached before it ran.
func ringNotAttempted(index int, deadlineMs uint32) error {
	return &cluster.Error{Code: cluster.CodeUnavailable, Message: fmt.Sprintf(
		"operation %d of the batch was not attempted: the frame's %d ms deadline was spent before its turn. It was not applied; nothing was written. Resend the whole frame with the same request id: the operations that were applied answer deduped and this one runs",
		index, deadlineMs)}
}

// ringErrorAnswer is the answer of an operation that was refused: the error it would have had
// as a lone request, or the short form when the full text does not fit the wire.
func ringErrorAnswer(err error) RingAnswer {
	refusal := ringRefusalOf(err)
	if _, encodeErr := EncodeRingError(refusal); encodeErr != nil {
		refusal = ringShortRefusal(refusal.Code)
	}
	return RingAnswer{Error: &refusal}
}

// ringBatchOutcome is ok when every operation was answered with a reply and partial otherwise.
func ringBatchOutcome(answers []RingAnswer) string {
	if ringCountErrors(answers) > 0 {
		return ringBatchPartial
	}
	return ringBatchOK
}

// ringIrreversible says an operation changes data in a way that only its answer carries: a pop or a
// take removes an element, an inc moves counters whose after-element a replay cannot rebuild. Dropping
// that answer would lose it.
func ringIrreversible(op cluster.Op) bool {
	if op.Class != cluster.OpStoreVerb || len(op.Fields) != 1 {
		return false
	}
	switch op.Fields[0].Verb {
	case cluster.VerbPop, cluster.VerbTake:
		return true
	case cluster.VerbInc:
		// Only return:"after" carries data the reply alone holds; a malformed arg is refused by
		// the apply path, which has nothing to lose.
		arg, err := decodeVerbArg(shapeInc, op.Fields[0].Value)
		return err == nil && arg.Return
	}
	return false
}

// ringIrreversibleWhat says in a refusal why the answer of op cannot be dropped.
func ringIrreversibleWhat(op cluster.Op) string {
	if op.Fields[0].Verb == cluster.VerbInc {
		return "returns the element it changed"
	}
	return "removes an element"
}

// ringBatchGate keeps the answers of the irreversible operations of one frame inside the reply
// budget, so none of them ever has to be dropped. An irreversible operation may start only while
// the answers already produced plus the worst case of the ones in flight plus its own worst case
// fit the budget; otherwise it waits for the ones in flight, and when even the answers already
// produced leave no room it is not run at all. Nothing is removed or moved that cannot be returned.
type ringBatchGate struct {
	mu       sync.Mutex
	room     *sync.Cond
	budget   int64
	element  int64
	used     int64
	inflight int64
}

func newRingBatchGate(budget, element int64) *ringBatchGate {
	g := &ringBatchGate{budget: budget, element: element}
	g.room = sync.NewCond(&g.mu)
	return g
}

// ringGateResult is how a wait in the gate ended.
type ringGateResult int

const (
	// ringGateEntered means the worst case of the answer is reserved; the caller must leave.
	ringGateEntered ringGateResult = iota
	// ringGateNoRoom means the answers already produced leave no room for another.
	ringGateNoRoom
	// ringGateEnded means the context or the frame's deadline ended the wait; nothing is reserved.
	ringGateEnded
)

// enter reserves the worst case of one irreversible answer, waiting for operations in flight to
// settle. It stops waiting when ctx ends or the deadline (zero for none) passes, because a
// frame whose connection dropped or whose time is spent must not keep a goroutine parked or run
// the operation late. A sync.Cond cannot select on a context, so an AfterFunc takes the lock and
// broadcasts; holding the lock orders the broadcast after the waiter has parked, so it is not missed.
func (g *ringBatchGate) enter(ctx context.Context, deadline time.Time) ringGateResult {
	if !deadline.IsZero() {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, deadline)
		defer cancel()
	}
	stop := context.AfterFunc(ctx, func() {
		g.mu.Lock()
		defer g.mu.Unlock()
		g.room.Broadcast()
	})
	defer stop()
	g.mu.Lock()
	defer g.mu.Unlock()
	for {
		if g.used+g.element > g.budget {
			return ringGateNoRoom
		}
		if g.used+g.inflight+g.element <= g.budget {
			if ctx.Err() != nil {
				return ringGateEnded
			}
			g.inflight += g.element
			return ringGateEntered
		}
		if ctx.Err() != nil {
			return ringGateEnded
		}
		g.room.Wait()
	}
}

// leave releases the reservation and records the bytes the answer really took.
func (g *ringBatchGate) leave(used int64) {
	g.mu.Lock()
	g.inflight -= g.element
	g.used += used
	g.mu.Unlock()
	g.room.Broadcast()
}

// chargeBatchAnswers keeps the reply inside its byte budget. Error answers and the answers of
// a pop or a take, which removed its element, and of an inc with return:"after", whose element a
// replay cannot rebuild, are kept whole and charged first, which the gate above made fit. A claim is trimmed like any recomputable reply: its lease id stays in the
// answer and lease_get reads the leased messages back. The rest are charged in request order, so the outcome depends on the batch and not on
// timing: a reply that does not fit keeps what says what happened (write_outcome,
// copies_confirmed, position, deduped, found, the id and the lease) and drops its elements, keys
// and priorities. Only a recomputable reply is ever trimmed.
func (s *ringServer) chargeBatchAnswers(ops []cluster.Op, answers []RingAnswer) {
	var charged int64
	for index, answer := range answers {
		switch {
		case answer.Error != nil:
			raw, err := EncodeRingError(*answer.Error)
			if err != nil {
				short := ringShortRefusal(answer.Error.Code)
				answers[index].Error, raw = &short, nil
			}
			charged += int64(len(raw))
		case ringIrreversible(ops[index]):
			if raw, err := EncodeRingReply(*answer.Reply); err == nil {
				charged += int64(len(raw))
			}
		}
	}
	for index, answer := range answers {
		if answer.Reply == nil || ringIrreversible(ops[index]) {
			continue
		}
		raw, err := EncodeRingReply(*answer.Reply)
		if err == nil && charged+int64(len(raw)) <= s.resultBudget {
			charged += int64(len(raw))
			continue
		}
		slim := *answer.Reply
		slim.Elements, slim.Keys, slim.Priorities, slim.Entries = nil, nil, nil, nil
		answers[index].Reply = &slim
		if raw, err = EncodeRingReply(slim); err == nil {
			charged += int64(len(raw))
		}
	}
}
