package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/transport"
)

// batchChainOwnerCtxKey carries the owner a batch chain's first item was
// placed on: Cluster.write refuses, never forwards, a chain item that finds
// another owner (batch-request spec §4.3, rev 2.1 F3).
type batchChainOwnerCtxKey struct{}

func withBatchChainOwner(ctx context.Context, ownerID string) context.Context {
	return context.WithValue(ctx, batchChainOwnerCtxKey{}, ownerID)
}

// batchChainOwnerFrom is the chain's owner, when ctx is a batch item's.
func batchChainOwnerFrom(ctx context.Context) (string, bool) {
	owner, ok := ctx.Value(batchChainOwnerCtxKey{}).(string)
	return owner, ok
}

// forwardBatchTargetHook, when set, runs where a coordinator sends one owner
// its sub-batch, with the entries and the owner's ID (T-C1). forwardTargetHook
// fires only for a lone forward. Nil in production.
var forwardBatchTargetHook atomic.Pointer[func(entries []ReplEntry, targetID string)]

// withholdBatchFeatureForTesting makes a coordinator treat every owner as one
// that does not advertise the batch feature (T-C8). False in production.
var withholdBatchFeatureForTesting atomic.Bool

// ignoreBatchFlagForTesting makes an owner run a Batch push through today's
// serial loop, as a build without the batch branch does (T-C11). Nil in
// production.
var ignoreBatchFlagForTesting atomic.Bool

// coordinateBatch runs a batch on a node that routes (batch-request spec
// §5.2): each item is prepared as a lone write is, each chain is placed once by
// its first item's owner, this node's chains run through Cluster.write and
// Cluster.serveRead as lone requests do, and every other owner is sent one
// sub-batch of its items. An error is a whole-batch refusal: nothing ran.
func (c *Cluster) coordinateBatch(ctx context.Context, frame *transport.Frame, batch *batchRequestDecoded) ([]batchAnswer, error) {
	if c.bootstrapped.Load() {
		op := transport.OpFieldGet
		if batch.hasWrite {
			op = transport.OpFieldPush
		}
		refusal := c.notYetAddedRefusal(op)
		return nil, &batchRefusal{code: codeFor(refusal), cause: refusal, message: fmt.Sprintf("%s %s",
			strings.TrimSuffix(explainError(refusal), " Nothing was read or written."), batchNothingRun)}
	}
	items := batch.items
	n := len(items)
	entries := make([]ReplEntry, n)
	prepared := make([]error, n)
	for i, item := range items {
		if !item.write() {
			entries[i] = ReplEntry{Op: int32(item.op), Bucket: item.bucket, Payload: item.payload}
			continue
		}
		entries[i], prepared[i] = c.prepareClientWrite(ctx, batchItemFrame(frame, item), item.bucket, decodeFrameMeta(item.payload))
	}
	predecessor := chainPredecessors(items)
	owners := make([]ring.Node, n)
	ownerErr := make([]error, n)
	remote := make([]bool, n)
	for i, item := range items {
		if p := predecessor[i]; p >= 0 {
			owners[i], ownerErr[i] = owners[p], ownerErr[p]
		} else {
			owners[i], ownerErr[i] = c.Owner(placementOf(item.database, item.bucket))
		}
		remote[i] = ownerErr[i] == nil && owners[i].ID != c.self.ID
	}
	coordinator := &batchCoordinator{c: c, frame: frame, items: items, entries: entries, prepared: prepared,
		predecessor: predecessor, owners: owners, ownerErr: ownerErr, wc: batch.writeConcern}
	plan := &batchPlan{
		items:       items,
		predecessor: predecessor,
		remote:      remote,
		budget:      c.s.batchReplyBudget(),
		run:         coordinator.runLocal,
		sendRemote:  coordinator.sendRemote,
		holdLocal:   c.s.batchGateHolder(items, func(i int) bool { return ownerErr[i] == nil && !remote[i] }),
	}
	return c.s.runBatchPlan(ctx, plan), nil
}

// batchCoordinator is one batch on the node the client reached.
type batchCoordinator struct {
	c           *Cluster
	frame       *transport.Frame
	items       []batchItem
	entries     []ReplEntry
	prepared    []error
	predecessor []int
	owners      []ring.Node
	ownerErr    []error
	wc          WriteConcernLevel
}

// runLocal runs item i, whose chain this node owns, as a lone request of it
// runs past handleClientFrame's decode.
func (b *batchCoordinator) runLocal(ctx context.Context, i int) batchAnswer {
	item := b.items[i]
	itemFrame := batchItemFrame(b.frame, item)
	if b.ownerErr[i] != nil {
		return b.itemError(i, withOutcome(outcomeNotApplied, b.ownerErr[i]))
	}
	if !item.write() {
		// A read takes an execute slot on the node that executes it, and
		// gives it back when its answer is produced (§4.5).
		release, err := b.c.s.takeExecuteSlot(ctx)
		if err != nil {
			return b.itemError(i, err)
		}
		defer release()
		reply, err := b.c.serveRead(ctx, itemFrame, item.bucket, item.database)
		if err != nil {
			return b.itemError(i, err)
		}
		return answerFromFrame(reply)
	}
	if b.prepared[i] != nil {
		return b.itemError(i, b.prepared[i])
	}
	// Asked per write item when its chain reaches it, as handleClientFrame
	// asks it of a lone mutation: as fresh for the thousandth item as the
	// first (§5.2 step 3).
	if err := b.c.refuseClientMutation(item.op, item.bucket); err != nil {
		return b.itemError(i, withOutcome(outcomeNotApplied, err))
	}
	resp, err := b.c.write(withBatchChainOwner(ctx, b.owners[i].ID), b.entries[i], placementOf(item.database, item.bucket), b.wc)
	if err != nil {
		return b.itemError(i, withOutcome(outcomeUnknown, err))
	}
	return batchAnswer{opcode: transport.Opcode(resp.Opcode), payload: resp.Payload}
}

// itemError is item i's ERROR answer for err, as errorFrame gives it.
func (b *batchCoordinator) itemError(i int, err error) batchAnswer {
	return answerFromFrame(errorFrame(batchItemFrame(b.frame, b.items[i]), err))
}

// sendRemote sends each other owner one sub-batch of its items, concurrently,
// and produces an answer for every remote item. A chain whose item was refused
// before it could be sent (its prepare, the mutation check, the plaintext-owner
// check) is sent up to that item, which is answered with its refusal, and its
// later items are answered not run.
func (b *batchCoordinator) sendRemote(ctx context.Context, produce func(i int, answer batchAnswer)) {
	byOwner := make(map[string][]int)
	var order []string
	stoppedAt := make(map[int]int) // chain's first item -> item whose refusal stopped it
	head := make([]int, len(b.items))
	for i := range b.items {
		head[i] = i
		if p := b.predecessor[i]; p >= 0 {
			head[i] = head[p]
		}
	}
	for i := range b.items {
		if b.ownerErr[i] != nil || b.owners[i].ID == b.c.self.ID {
			continue
		}
		if root, stopped := stoppedAt[head[i]]; stopped {
			produce(i, batchChainStoppedAnswer(b.items, i, root, true))
			continue
		}
		if err := b.refuseBeforeSend(ctx, i); err != nil {
			stoppedAt[head[i]] = i
			produce(i, b.itemError(i, err))
			continue
		}
		owner := b.owners[i].ID
		if _, seen := byOwner[owner]; !seen {
			order = append(order, owner)
		}
		byOwner[owner] = append(byOwner[owner], i)
	}
	var sent sync.WaitGroup
	for _, owner := range order {
		indices := byOwner[owner]
		sent.Add(1)
		go func() {
			defer sent.Done()
			if b.c.memberAdvertises(owner, featureBatch) && !withholdBatchFeatureForTesting.Load() {
				b.sendSubBatch(ctx, b.owners[indices[0]], indices, produce)
				return
			}
			b.forwardOneByOne(ctx, indices, produce)
		}()
	}
	sent.Wait()
}

// refuseBeforeSend is what a write item meets on this node before it is sent
// to its owner: its prepare, the cluster's refusals of a client mutation, and
// Cluster.write's checks before a forward. Reads meet none of them, as a lone
// read does not.
func (b *batchCoordinator) refuseBeforeSend(ctx context.Context, i int) error {
	item := b.items[i]
	if !item.write() {
		return nil
	}
	if b.prepared[i] != nil {
		return b.prepared[i]
	}
	if err := b.c.refuseClientMutation(item.op, item.bucket); err != nil {
		return withOutcome(outcomeNotApplied, err)
	}
	if err := b.c.refuseAlterWithoutFeature(b.entries[i]); err != nil {
		return withOutcome(outcomeNotApplied, err)
	}
	if err := b.c.refuseForwardToPlaintextOwner(ctx, b.owners[i], b.entries[i]); err != nil {
		return withOutcome(outcomeNotApplied, err)
	}
	return nil
}

// sendSubBatch sends one owner its items as one REPL_PUSH with Batch set, and
// answers each of them from the owner's answer (§5.2 step 6, §5.3).
func (b *batchCoordinator) sendSubBatch(ctx context.Context, owner ring.Node, indices []int, produce func(i int, answer batchAnswer)) {
	entries := make([]ReplEntry, len(indices))
	var readWait int64
	for j, i := range indices {
		entries[j] = b.entries[i]
		if !b.items[i].write() {
			readWait = clientReadForward.waitMs
		}
	}
	if hook := forwardBatchTargetHook.Load(); hook != nil {
		(*hook)(entries, owner.ID)
	}
	batchForwardsTotal.Inc()
	req := &ReplPushRequest{
		Token:          b.c.localToken(),
		FromNode:       b.c.self.ID,
		Entries:        entries,
		WriteConcern:   int(b.wc),
		ReturnResponse: true,
		Epoch:          b.c.Epoch(),
		ReadWaitMs:     readWait,
		Batch:          true,
	}
	resp, err := b.c.peerFor(owner).call(ctx, transport.LaneRequest, req)
	if err == nil {
		b.c.learnFromPeer(owner.ID, resp.DroppedGenerations)
	}
	for j, answer := range b.subBatchAnswers(owner, indices, req.Epoch, resp, err) {
		produce(indices[j], answer)
	}
}

// subBatchAnswers maps one owner's answer to a sub-batch onto its items (§5.3).
// The whole sub-batch is not_applied only for a refusal that provably came
// before any entry ran: BatchNotRun, or an epoch or member-mismatch refusal.
// Any other refusal, a lost answer, or an answer whose count is not the
// entries' makes every write unknown and every read "nothing changed": an
// owner on a build that ignores Batch applies entries 1 to k-1 and breaks at
// k, and its refusal must not be read as "nothing ran".
func (b *batchCoordinator) subBatchAnswers(owner ring.Node, indices []int, epoch uint64, resp *ReplPushResponse, callErr error) []batchAnswer {
	answers := make([]batchAnswer, len(indices))
	every := func(answer func(i int) error) []batchAnswer {
		for j, i := range indices {
			answers[j] = b.itemError(i, answer(i))
		}
		return answers
	}
	switch {
	case callErr != nil:
		return every(func(i int) error { return b.lostForward(i, owner, callErr) })
	case !resp.Success && (resp.BatchNotRun || resp.Refusal == refusalEpoch || resp.Refusal == refusalMemberMismatch):
		return every(func(i int) error { return b.subBatchNotRun(i, owner, epoch, resp) })
	case !resp.Success:
		return every(func(i int) error {
			return b.lostForward(i, owner, fmt.Errorf("owner %s answered the batch with a refusal that does not say whether its items ran: %s", owner.ID, resp.Error))
		})
	case len(resp.BatchAnswers) != len(indices):
		return every(func(i int) error {
			return b.lostForward(i, owner, fmt.Errorf("owner %s answered %d of the %d items sent to it", owner.ID, len(resp.BatchAnswers), len(indices)))
		})
	}
	for j, i := range indices {
		answer := resp.BatchAnswers[j]
		switch {
		case !answer.Success:
			answers[j] = b.itemError(i, forwardRefusal(owner.ID, b.entries[i], epoch, &answer))
		case len(answer.Responses) != 1:
			answers[j] = b.itemError(i, b.lostForward(i, owner, fmt.Errorf("owner %s returned %d responses for 1 entry", owner.ID, len(answer.Responses))))
		default:
			answers[j] = batchAnswer{opcode: transport.Opcode(answer.Responses[0].Opcode), payload: answer.Responses[0].Payload}
		}
	}
	return answers
}

// subBatchNotRun is item i's answer when its owner refused the sub-batch
// before running any of it: not_applied, with the owner's reason.
func (b *batchCoordinator) subBatchNotRun(i int, owner ring.Node, epoch uint64, resp *ReplPushResponse) error {
	if resp.Refusal == refusalEpoch || resp.Refusal == refusalMemberMismatch {
		return forwardRefusal(owner.ID, b.entries[i], epoch, resp)
	}
	return forwardedOutcome(b.entries[i], outcomeNotApplied, fmt.Errorf("owner %s refused the batch before running any of it, and nothing of %s for bucket %q was applied: %s. Retry it",
		owner.ID, b.items[i].op, b.items[i].bucket, resp.Error))
}

// lostForward is item i's answer when its owner's answer was lost, or did not
// say what ran: a write may be stored there (unknown, with the lone advice),
// a read changed nothing.
func (b *batchCoordinator) lostForward(i int, owner ring.Node, cause error) error {
	item := b.items[i]
	return forwardedOutcome(b.entries[i], outcomeUnknown, fmt.Errorf("%w: %s for bucket %q was sent to its owner %s and no answer came back for it; %s: %w",
		ErrForwardLost, item.op, item.bucket, owner.ID, forwardLostAdvice(item.op, item.database, item.bucket, item.payload), cause))
}

// forwardOneByOne sends an owner that does not advertise the batch feature its
// items one lone forward each, through the lone path (Cluster.write,
// Cluster.serveRead), with the chain rule kept here (§5.2 step 7): same
// answers, more internode requests.
func (b *batchCoordinator) forwardOneByOne(ctx context.Context, indices []int, produce func(i int, answer batchAnswer)) {
	chains := make(map[int][]int)
	var heads []int
	for _, i := range indices {
		h := i
		for b.predecessor[h] >= 0 {
			h = b.predecessor[h]
		}
		if _, seen := chains[h]; !seen {
			heads = append(heads, h)
		}
		chains[h] = append(chains[h], i)
	}
	var sent sync.WaitGroup
	for _, h := range heads {
		chain := chains[h]
		sent.Add(1)
		go func() {
			defer sent.Done()
			runForwardedChain(b.items, chain, func(i int) batchAnswer { return b.forwardOne(ctx, i) }, produce)
		}()
	}
	sent.Wait()
}

// runForwardedChain sends a chain's items one at a time through send and
// stops it at the first item that is not stored or may not be (§4.3). An
// `applied` answer (stored, write concern not met) does not stop it: the item
// is in the owner's log ahead of the next, as noteLocalStep treats it on the
// local path, and telling later items it was "not applied" would make a
// client resend a push or an $inc that is already stored.
func runForwardedChain(items []batchItem, chain []int, send func(i int) batchAnswer, produce func(i int, answer batchAnswer)) {
	for k, i := range chain {
		answer := send(i)
		produce(i, answer)
		outcome := classifyAnswer(items[i], answer)
		if outcome == batchItemOK || outcome == batchItemApplied {
			continue
		}
		for _, later := range chain[k+1:] {
			produce(later, batchChainStoppedAnswer(items, later, i, outcome != batchItemUnknown))
		}
		return
	}
}

// forwardOne is one item of forwardOneByOne: the lone path's forward.
func (b *batchCoordinator) forwardOne(ctx context.Context, i int) batchAnswer {
	item := b.items[i]
	if !item.write() {
		reply, err := b.c.serveRead(ctx, batchItemFrame(b.frame, item), item.bucket, item.database)
		if err != nil {
			return b.itemError(i, err)
		}
		return answerFromFrame(reply)
	}
	resp, err := b.c.write(withBatchChainOwner(ctx, b.owners[i].ID), b.entries[i], placementOf(item.database, item.bucket), b.wc)
	if err != nil {
		return b.itemError(i, withOutcome(outcomeUnknown, err))
	}
	return batchAnswer{opcode: transport.Opcode(resp.Opcode), payload: resp.Payload}
}

// batchChainStoppedAnswer is item i's answer when item root, an earlier item
// of its bucket, was not applied: rootNotApplied says it is known not to be
// stored, else it may be (§4.3, OC-1).
func batchChainStoppedAnswer(items []batchItem, i, root int, rootNotApplied bool) batchAnswer {
	position := fmt.Sprintf("item %s of %s (%s)", groupThousands(root+1), groupThousands(len(items)), describeBatchItem(items[root].op, items[root].payload))
	message := fmt.Sprintf("Not run: %s, an earlier item for the same bucket, was not applied, and items of one bucket apply in order. Resend from item %s.",
		position, groupThousands(root+1))
	if !rootNotApplied {
		message = fmt.Sprintf("Not run: %s, an earlier item for the same bucket, may or may not be stored. Read it back, then resend from item %s.",
			position, groupThousands(root+1))
	}
	var err error = &sentencedError{message: message, cause: errBatchChainStopped}
	if items[i].write() {
		err = withOutcome(outcomeNotApplied, err)
	}
	return answerFromFrame(errorFrame(&transport.Frame{Opcode: items[i].op}, err))
}

// serveBatchPush answers a Batch push on the owner (batch-request spec §5.3):
// every entry is re-validated, grouped into its bucket's chain, run
// concurrently through serveForwardedEntry under the batch bounds, and
// answered in BatchAnswers, charged against the reply budget as a running
// prefix in entry order. No entry stops the others; the shared resp, written
// and take of the serial loop are not used.
func (c *Cluster) serveBatchPush(ctx context.Context, frame *transport.Frame, req *ReplPushRequest) (*transport.Frame, error) {
	if !req.ReturnResponse || req.Stream != nil || req.Catalog != nil || req.Handoff != nil {
		return nodeResponse(transport.OpReplAck, frame, ReplPushResponse{Success: false, BatchNotRun: true,
			Error: "a batch push must ask for responses and carry no stream, catalog or hand-off question; nothing was run"}, nil)
	}
	n := len(req.Entries)
	items := make([]batchItem, n)
	for i, e := range req.Entries {
		op := transport.Opcode(e.Op)
		_, database := decodeFrameMeta(e.Payload).scope(op)
		items[i] = batchItem{op: op, payload: e.Payload, database: database, bucket: e.Bucket}
	}
	// Each slot is written only by its own entry's goroutine, and read after
	// runBatchPlan has waited for every one of them.
	answered := make([]ReplPushResponse, n)
	ran := make([]bool, n)
	wc := WriteConcernLevel(req.WriteConcern)
	plan := &batchPlan{
		items:       items,
		predecessor: chainPredecessors(items),
		budget:      c.s.batchReplyBudget(),
		holdLocal:   c.s.batchGateHolder(items, func(int) bool { return true }),
		run: func(ctx context.Context, i int) batchAnswer {
			ran[i] = true
			return c.serveBatchEntry(ctx, req, i, items[i], wc, &answered[i])
		},
	}
	answers := c.s.runBatchPlan(ctx, plan)
	resp := ReplPushResponse{Success: true, BatchAnswers: make([]ReplPushResponse, n)}
	var written []func()
	for i := range answers {
		entry := answered[i]
		switch {
		case !ran[i] || entry.Success:
			// The answer as charged: the runner's own (a chain stopped, a
			// shutdown) or the entry's, perhaps replaced under the budget.
			relayed := ReplResponse{Opcode: int32(answers[i].opcode), Payload: answers[i].payload}
			if ran[i] && len(entry.Responses) == 1 && entry.Responses[0].onWritten != nil {
				written = append(written, entry.Responses[0].onWritten)
			}
			entry = ReplPushResponse{Success: true, DroppedGenerations: entry.DroppedGenerations, Responses: []ReplResponse{relayed}}
		}
		resp.DroppedGenerations = append(resp.DroppedGenerations, entry.DroppedGenerations...)
		entry.DroppedGenerations = nil
		resp.BatchAnswers[i] = entry
	}
	ack, err := nodeResponse(transport.OpReplAck, frame, resp, nil)
	if err != nil {
		fireWritten(written)
		return ack, err
	}
	for _, hook := range written {
		ack.AddWrittenHook(hook)
	}
	return ack, nil
}

// serveBatchEntry runs one entry of a Batch push and records its forwarded
// answer in answered; the batchAnswer it returns is what the chain and the
// budget read.
func (c *Cluster) serveBatchEntry(ctx context.Context, req *ReplPushRequest, i int, item batchItem, wc WriteConcernLevel, answered *ReplPushResponse) batchAnswer {
	e := req.Entries[i]
	refuse := func(refusal *entryRefusal) batchAnswer {
		refusal.applyTo(answered)
		if !item.write() {
			answered.WriteOutcome = ""
		}
		err := errors.New(refusal.message)
		if item.write() {
			err = withOutcome(refusal.outcome, err)
		}
		return answerFromFrame(errorFrame(&transport.Frame{Opcode: item.op}, err))
	}
	// The owner re-validates every entry, not trusting the coordinator: an
	// entry outside the seven verbs, or naming no bucket, is never run. A
	// spread router's read of a part is the one other entry a push carries
	// (spread MVP note, section 3, B8): a part name, which no client can send
	// (NewHandler, admitBatchItems), and a read of the fan-out class.
	partRead := spreadPartReadCarried(item.op) && hasPartSeparator(e.Bucket)
	if (!batchCarried(item.op) && !partRead) || e.Bucket == "" {
		return refuse(&entryRefusal{outcome: outcomeNotApplied, message: fmt.Sprintf(
			"%s refused entry %d of a batch from %s: %s for bucket %q is not one a batch carries (a store field's push, put, delete, get, len or peek, or an update of one key, naming its bucket); nothing was run",
			c.self.ID, i+1, req.FromNode, item.op, e.Bucket)})
	}
	if err := c.refuseForwardedFromPlaintextCoordinator(*req, e); err != nil {
		return refuse(&entryRefusal{outcome: outcomeNotApplied, message: err.Error()})
	}
	switch {
	case partRead:
		// A spread router's part read (spread_fanout.go, "Slots") holds no
		// slot here while it waits: on a part being taken over its read can
		// wait out the on-demand bound and be sent to the part's previous
		// owner. Its query takes an execute slot only while it executes, in
		// admitQuery, given back as the entry returns (below), and its
		// forward is covered by the fan-out's one admission, so it takes no
		// forward slot. Bounded as the router bounds the push.
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(withFanOutAdmitted(ctx), clusterListingTimeout)
		defer cancel()
	case !item.write():
		release, err := c.s.takeExecuteSlot(ctx)
		if err != nil {
			tagEntryRefusal(answered, e, err, false)
			return answerFromFrame(errorFrame(&transport.Frame{Opcode: item.op}, err))
		}
		defer release()
	}
	entryResp, refused, err := c.serveForwardedEntry(ctx, req, e, wc)
	if refused != nil {
		return refuse(refused)
	}
	c.reportTombstones(answered, e, err)
	if err != nil {
		tagEntryRefusal(answered, e, err, item.write())
		return answerFromFrame(errorFrame(&transport.Frame{Opcode: item.op}, err))
	}
	if partRead && entryResp.onWritten != nil {
		// The part's execute slot comes back now, not when the push's ack
		// is written after every entry (R1).
		entryResp.onWritten()
		entryResp.onWritten = nil
	}
	answered.Success = true
	answered.Responses = []ReplResponse{entryResp}
	return batchAnswer{opcode: transport.Opcode(entryResp.Opcode), payload: entryResp.Payload}
}
