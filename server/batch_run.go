package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// The bounds of a batch in flight (batch-request spec §3.4, §4.5, §6.1).
const (
	// batchWindow: an item does not start while the first uncharged item of
	// its batch is this many or more positions before it. It bounds the items
	// of one batch in flight on a node, and the answers it holds uncharged.
	batchWindow = 64
	// batchNodeSlots is the node-wide bound on batch items in flight, shared by
	// local items and the items of forwarded sub-batches. It is not a
	// configuration key (zero config); the fairness measurement revisits it.
	batchNodeSlots = 256
	// batchReplyBudgetCap caps the reply budget so every sub-batch answer and
	// every batch reply fits transport.MaxFrameLength.
	batchReplyBudgetCap = 16 << 20
)

// errBatchShuttingDown is an item that had not started when the server began
// shutting down: nothing of it ran.
var errBatchShuttingDown = errors.New("the server is shutting down")

// errBatchChainStopped is an item of a bucket whose earlier item in the batch
// was not applied: it was not run, because a bucket's items apply in order.
var errBatchChainStopped = errors.New("an earlier item for the same bucket was not applied")

// localStep is the one-shot signal that an item's local step has ended
// (§4.3): on a cluster when Cluster.applyInTurn has returned in applyOwned,
// on a single node when Server.fencedDispatch has returned, and in every case
// when the item's call returns. ok says whether the step was a success; the
// item after it in its chain starts only then.
type localStep struct {
	once sync.Once
	done chan struct{}
	ok   bool
}

func newLocalStep() *localStep { return &localStep{done: make(chan struct{})} }

// end fires the signal once; a later call changes nothing.
func (l *localStep) end(ok bool) {
	l.once.Do(func() {
		l.ok = ok
		close(l.done)
	})
}

type localStepCtxKey struct{}

// withLocalStep puts an item's local-step signal in its context.
func withLocalStep(ctx context.Context, step *localStep) context.Context {
	return context.WithValue(ctx, localStepCtxKey{}, step)
}

// noteLocalStep is the one addition §5.4 permits to Cluster.applyOwned: right
// after applyInTurn returns it signals a batch item's chain that the entry is
// appended, ordered and applied, with whether the apply was a success. It
// moves no step, waits on nothing and shares nothing across items. Without a
// note in the context (every lone write) it does nothing.
func noteLocalStep(ctx context.Context, resp ReplResponse, applyErr error) {
	step, ok := ctx.Value(localStepCtxKey{}).(*localStep)
	if !ok {
		return
	}
	step.end(applyErr == nil && replySucceeded(transport.Opcode(resp.Opcode), resp.Payload))
}

// replySucceeded reports whether a verb's reply says success: not an ERROR,
// and, for a reply that carries success, success true.
func replySucceeded(op transport.Opcode, payload []byte) bool {
	if op == transport.OpError {
		return false
	}
	success, err := bson.Raw(payload).LookupErr("success")
	if err != nil {
		return true
	}
	value, isBool := success.BooleanOK()
	return !isBool || value
}

// batchAnswer is one item's answer: the lone reply's opcode and payload.
type batchAnswer struct {
	opcode  transport.Opcode
	payload []byte
}

// answerFromFrame takes a reply frame's answer, running any hook the frame
// carries now: the answer's bytes are copied into the batch's reply, and what
// the hook gives back (a forwarded read's held bytes) is held by the batch's
// own budget from here.
func answerFromFrame(reply *transport.Frame) batchAnswer {
	if hook := reply.TakeWrittenHook(); hook != nil {
		hook()
	}
	return batchAnswer{opcode: reply.Opcode, payload: reply.Payload}
}

// batchItemOutcome is how an item ended, by §4.1's table; its name is the
// hoarddb_batch_items_total outcome label.
type batchItemOutcome string

const (
	batchItemOK         batchItemOutcome = "ok"
	batchItemNotApplied batchItemOutcome = "not_applied"
	batchItemApplied    batchItemOutcome = "applied"
	batchItemUnknown    batchItemOutcome = "unknown"
	batchItemVerbFailed batchItemOutcome = "verb_failed"
	batchItemReadFailed batchItemOutcome = "read_failed"
)

// classifyAnswer applies §4.1's table to one answer.
func classifyAnswer(item batchItem, answer batchAnswer) batchItemOutcome {
	if answer.opcode == transport.OpError {
		if !item.write() {
			return batchItemReadFailed
		}
		var refusal ErrorResponse
		if err := bson.Unmarshal(answer.payload, &refusal); err != nil {
			return batchItemUnknown
		}
		switch refusal.WriteOutcome {
		case outcomeNotApplied:
			return batchItemNotApplied
		case outcomeApplied:
			return batchItemApplied
		}
		// A standalone node leaves it empty where its store failed the apply:
		// a client reads that as unknown.
		return batchItemUnknown
	}
	if !replySucceeded(answer.opcode, answer.payload) {
		return batchItemVerbFailed
	}
	return batchItemOK
}

// batchPlan is one batch, or one owner's sub-batch, ready to run: its items,
// each item's chain predecessor (-1 for the first of its bucket), the items
// answered elsewhere (a remote sub-batch), the reply budget, and how an item
// runs here.
type batchPlan struct {
	items       []batchItem
	predecessor []int
	remote      []bool
	budget      int64
	run         func(ctx context.Context, i int) batchAnswer
	// sendRemote, when set, sends the remote items and calls produce once for
	// each of them with its answer.
	sendRemote func(ctx context.Context, produce func(i int, answer batchAnswer))
	// holdLocal, when set, is called once, before the first local item
	// starts, and again returns the context the local items run in and a
	// release called after the last of them ends. It is how a router takes
	// the gates of the spread collections whose items it runs (spread_gate.go).
	// Nothing is held while a remote answer is awaited: runBatchPlan calls it
	// only after every remote item has been answered.
	holdLocal func(ctx context.Context) (context.Context, func())
}

// chainPredecessors links each item to the item before it naming the same
// (database, bucket): the chains of §4.3. A map built per batch and dropped
// with it: nothing per bucket outlives the request.
func chainPredecessors(items []batchItem) []int {
	last := make(map[placementKey]int, len(items))
	predecessor := make([]int, len(items))
	for i, item := range items {
		key := placementOf(item.database, item.bucket)
		if previous, ok := last[key]; ok {
			predecessor[i] = previous
		} else {
			predecessor[i] = -1
		}
		last[key] = i
	}
	return predecessor
}

// batchReplyBudget is the budget every answer of a batch is charged against:
// the node's result ceiling, capped at 16 MiB (§4.5).
func (s *Server) batchReplyBudget() int64 {
	return min(s.maxResultBytes(), batchReplyBudgetCap)
}

// batchSlots is the node's batch-item semaphore, built on first use as the
// query gate is (tests build Server literals).
func (s *Server) batchSlots() chan struct{} {
	s.batchSlotsOnce.Do(func() {
		s.batchSlotsCh = make(chan struct{}, batchNodeSlots)
	})
	return s.batchSlotsCh
}

// takeBatchSlot waits for one of the node's batch-item slots. It waits with no
// timeout, under the request's context, which only shutdown cancels: every
// holder gives its slot back without waiting on another one (§6.1).
func (s *Server) takeBatchSlot(ctx context.Context) (func(), error) {
	slots := s.batchSlots()
	select {
	case slots <- struct{}{}:
	case <-ctx.Done():
		return nil, fmt.Errorf("waiting for a batch item slot: %w", ctx.Err())
	}
	batchItemsInFlight.Inc()
	if hook := batchSlotTakenHook.Load(); hook != nil {
		(*hook)(len(slots))
	}
	return sync.OnceFunc(func() {
		<-slots
		batchItemsInFlight.Dec()
	}), nil
}

// batchSlotTakenHook, when set, is called with the node's batch slots in use
// each time one is taken. Nil in production.
var batchSlotTakenHook atomic.Pointer[func(inUse int)]

// batchHeldBytesHook, when set, is called once per batch run with the most
// answer bytes it held at once: charged answers kept plus answers produced and
// not yet charged (§4.5's memory bound, T-15). Nil in production.
var batchHeldBytesHook atomic.Pointer[func(peak int64)]

// batchRun is one plan in progress.
type batchRun struct {
	s    *Server
	plan *batchPlan

	mu       sync.Mutex
	cond     *sync.Cond
	answers  []batchAnswer
	produced []bool
	// charged is the first uncharged item: every answer before it is charged.
	charged int
	spent   int64
	held    int64
	peak    int64

	steps []*localStep
	// stopRoot is, for an item a stopped chain did not run, the item that
	// stopped it; -1 otherwise. Set before the item's step ends.
	stopRoot []int
}

// runBatchPlan runs a plan and returns one answer per item, in item order,
// each charged against the reply budget as a running prefix (§4.5).
func (s *Server) runBatchPlan(ctx context.Context, plan *batchPlan) []batchAnswer {
	n := len(plan.items)
	r := &batchRun{
		s: s, plan: plan,
		answers: make([]batchAnswer, n), produced: make([]bool, n),
		steps: make([]*localStep, n), stopRoot: make([]int, n),
	}
	r.cond = sync.NewCond(&r.mu)
	for i := range r.steps {
		r.steps[i] = newLocalStep()
		r.stopRoot[i] = -1
	}
	var started sync.WaitGroup
	remoteAnswered := make(chan struct{})
	if plan.sendRemote != nil {
		started.Add(1)
		go func() {
			defer started.Done()
			defer close(remoteAnswered)
			plan.sendRemote(ctx, r.produce)
		}()
	} else {
		close(remoteAnswered)
	}
	if plan.holdLocal != nil {
		// A router that holds a gate while it waits for another router's is
		// one side of a cycle through two queued drops (spread_gate.go), so
		// the local items start once the remote ones have answered. The
		// window then waits only on earlier local items, which run in order.
		<-remoteAnswered
		var release func()
		ctx, release = plan.holdLocal(ctx)
		defer release()
	}
	for i := 0; i < n; i++ {
		if plan.remote != nil && plan.remote[i] {
			continue
		}
		r.waitWindow(i)
		started.Add(1)
		go func(i int) {
			defer started.Done()
			r.runItem(ctx, i)
		}(i)
	}
	started.Wait()
	if hook := batchHeldBytesHook.Load(); hook != nil {
		(*hook)(r.peak)
	}
	return r.answers
}

// waitWindow returns once item i may start: the first uncharged item is fewer
// than batchWindow positions before it. Every wait points to an earlier item,
// so the window adds no cycle.
func (r *batchRun) waitWindow(i int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i-r.charged >= batchWindow {
		r.cond.Wait()
	}
}

// runItem runs item i after its chain predecessor's local step, or answers it
// not run when that step failed or the server is shutting down.
func (r *batchRun) runItem(ctx context.Context, i int) {
	step := r.steps[i]
	if p := r.plan.predecessor[i]; p >= 0 {
		// The chain waits here, before the item takes its slot or any fence.
		<-r.steps[p].done
		if !r.steps[p].ok {
			root := p
			r.mu.Lock()
			if r.stopRoot[p] >= 0 {
				root = r.stopRoot[p]
			}
			r.stopRoot[i] = root
			r.mu.Unlock()
			step.end(false)
			r.produce(i, r.chainStoppedAnswer(i, root))
			return
		}
	}
	if ctx.Err() != nil {
		step.end(false)
		r.produce(i, r.shuttingDownAnswer(i))
		return
	}
	release, err := r.s.takeBatchSlot(ctx)
	if err != nil {
		step.end(false)
		r.produce(i, r.shuttingDownAnswer(i))
		return
	}
	answer := r.plan.run(withLocalStep(ctx, step), i)
	// The slot is given back when the item's own answer is produced, before it
	// is charged or sent (§6.1, N1).
	release()
	step.end(classifyAnswer(r.plan.items[i], answer) == batchItemOK)
	r.produce(i, answer)
}

// produce records item i's answer and charges every answer the prefix now
// reaches, in item order.
func (r *batchRun) produce(i int, answer batchAnswer) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.answers[i] = answer
	r.produced[i] = true
	r.held += int64(len(answer.payload))
	r.peak = max(r.peak, r.held)
	for r.charged < len(r.answers) && r.produced[r.charged] {
		r.charge(r.charged)
		r.charged++
	}
	r.cond.Broadcast()
}

// charge charges answer i against the budget, replacing it when it would pass
// the budget: a read's answer becomes RESULT_TOO_LARGE, an update's loses its
// document (doc_omitted). An ERROR answer is always kept and charged, and so
// is any other write's few hundred bytes. Called with r.mu held.
func (r *batchRun) charge(i int) {
	item, answer := r.plan.items[i], r.answers[i]
	size := int64(len(answer.payload))
	if answer.opcode != transport.OpError && r.spent+size > r.plan.budget {
		if replaced, kind, ok := r.trimmed(i, item, answer); ok {
			r.held += int64(len(replaced.payload)) - size
			r.answers[i] = replaced
			size = int64(len(replaced.payload))
			batchAnswersTrimmed.WithLabelValues(kind).Inc()
		}
	}
	r.spent += size
}

// trimmed is the answer that replaces answer i past the budget, and the kind
// for the trimmed-answers counter; ok is false for an answer that is kept.
func (r *batchRun) trimmed(i int, item batchItem, answer batchAnswer) (batchAnswer, string, bool) {
	total := len(r.plan.items)
	if !item.write() {
		refusal := ErrorResponse{
			Code: int(transport.ErrResultTooLarge),
			Message: fmt.Sprintf("Item %s of %s (%s): the batch's reply is full (%s), so this read's answer was left out; nothing changed. "+
				"Send it on its own, or with fewer reads in the batch.",
				groupThousands(i+1), groupThousands(total), describeBatchItem(item.op, item.payload), formatMiB(r.plan.budget)),
			MaxResultBytes: r.plan.budget,
			RetainedBytes:  r.spent,
		}
		payload, err := bson.Marshal(refusal)
		if err != nil {
			// Keep the answer as it is: a read that fit the budget's accounting
			// by luck is better than an unreadable refusal.
			return batchAnswer{}, "", false
		}
		return batchAnswer{opcode: transport.OpError, payload: payload}, "read", true
	}
	if item.op != transport.OpUpdate || answer.opcode != transport.OpUpdate {
		return batchAnswer{}, "", false
	}
	var update UpdateResponse
	if err := bson.Unmarshal(answer.payload, &update); err != nil || update.Doc == nil {
		return batchAnswer{}, "", false
	}
	update.Doc = nil
	update.DocOmitted = true
	payload, err := bson.Marshal(update)
	if err != nil {
		return batchAnswer{}, "", false
	}
	return batchAnswer{opcode: transport.OpUpdate, payload: payload}, "write", true
}

// chainStoppedAnswer answers item i, which was not run because item root, an
// earlier item of its bucket, was not applied (§4.3, OC-1).
func (r *batchRun) chainStoppedAnswer(i, root int) batchAnswer {
	r.mu.Lock()
	for !r.produced[root] {
		r.cond.Wait()
	}
	rootAnswer := r.answers[root]
	r.mu.Unlock()
	rootItem := r.plan.items[root]
	return batchChainStoppedAnswer(r.plan.items, i, root, classifyAnswer(rootItem, rootAnswer) != batchItemUnknown)
}

// shuttingDownAnswer answers item i, which had not started when the server
// began shutting down.
func (r *batchRun) shuttingDownAnswer(i int) batchAnswer {
	return r.notRunAnswer(i, &sentencedError{cause: errBatchShuttingDown,
		message: "Not run: the server is shutting down; retry on another member."})
}

// notRunAnswer is an item's ERROR answer for something that kept it from
// running: not_applied on a write, no outcome on a read.
func (r *batchRun) notRunAnswer(i int, err error) batchAnswer {
	item := r.plan.items[i]
	if item.write() {
		err = withOutcome(outcomeNotApplied, err)
	}
	return answerFromFrame(errorFrame(&transport.Frame{Opcode: item.op}, err))
}

// batchItemFrame is an item's synthetic frame: the item's opcode and payload
// under the batch's request id.
func batchItemFrame(batchFrame *transport.Frame, item batchItem) *transport.Frame {
	return &transport.Frame{Version: batchFrame.Version, Opcode: item.op, RequestID: batchFrame.RequestID, Payload: item.payload}
}

// encodeBatchReply is the BATCH_OK frame of a batch's answers.
func encodeBatchReply(batchFrame *transport.Frame, answers []batchAnswer) *transport.Frame {
	reply := BatchResponse{Answers: make([]BatchAnswer, len(answers))}
	for i, answer := range answers {
		reply.Answers[i] = BatchAnswer{Opcode: int32(answer.opcode), Payload: answer.payload}
	}
	payload, err := bson.Marshal(reply)
	if err != nil {
		return errorFrame(batchFrame, fmt.Errorf("encoding the BATCH_OK reply of %d answers: %w", len(answers), err))
	}
	return transport.NewFrameWithID(transport.OpBatchOK, batchFrame.RequestID, payload)
}

// recordBatchOutcome counts a batch and its items (§6: aggregates only, no
// bucket or database label).
func recordBatchOutcome(items []batchItem, answers []batchAnswer) {
	outcome := "ok"
	for i, answer := range answers {
		itemOutcome := classifyAnswer(items[i], answer)
		batchItemsTotal.WithLabelValues(items[i].op.String(), string(itemOutcome)).Inc()
		if itemOutcome != batchItemOK {
			outcome = "partial"
		}
	}
	batchRequestsTotal.WithLabelValues(outcome).Inc()
	batchItemsPerRequest.Observe(float64(len(items)))
}

// batchItemPosition is "Item 9 of 21 (db.User_42["42"].Feed.push(…))", the
// prefix of a refusal raised by one item's gate.
func batchItemPosition(i int, items []batchItem) string {
	return fmt.Sprintf("Item %s of %s (%s)", groupThousands(i+1), groupThousands(len(items)), describeBatchItem(items[i].op, items[i].payload))
}

// itemGateRefusal refuses the whole batch for item i's gate: the lone
// request's refusal, unchanged, prefixed with the item's position, with the
// gate's code.
func itemGateRefusal(i int, items []batchItem, gateErr error) error {
	return &batchRefusal{code: codeFor(gateErr), cause: gateErr, message: fmt.Sprintf("%s: %s. %s",
		batchItemPosition(i, items), strings.TrimSuffix(explainError(gateErr), "."), batchNothingRun)}
}
