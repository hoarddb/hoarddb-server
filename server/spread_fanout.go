package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"sync"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The reads that cover every part of a spread collection (document placement
// spread MVP note, section 3): find, count, distinct, search, describe and
// info, answered at the router. The router takes its gate shared once, reads
// the born set, asks every part at 2 (created) and no other, and merges the
// answers into the answer a whole bucket of the same documents gives. A part
// at 1 or unborn holds no document and is not asked. Dump fans in on its own
// (spread_dump.go).
//
// Slots (R1). The pools a fan-out touches, and what a holder of each waits on.
//
//   - Forward slot: taken once per client query, at the client's door. That
//     is the router (routeFanOut) when the client is there, else the
//     coordinator (Cluster.forwardRead), whose slot covers the fan-out at the
//     router. Every read made on the fan-out's behalf carries that admission
//     (withFanOutAdmitted): the router's own, a part read local or in a Batch
//     push, and a part read's on-demand forward to a part's previous owner
//     while its new owner takes it over. None takes a second forward slot.
//   - Execute slot: taken only by admitQuery, around the one handler that
//     executes a part's query on the node that holds the part, and given back
//     as that part answers (readPartLocally; serveBatchEntry for a remote
//     part), never after the merge or the push's ack. A part read that waits,
//     for the on-demand bound or on the previous owner, holds none.
//
// So an execute slot's holder runs one handler against its own store and waits
// on nothing else. A forward slot's holder waits on part reads, which wait on
// execute slots (whose holders do not wait) or on another node's execution of
// a ServeFromCopy read, which takes only an execute slot there. No holder of
// either pool waits on a slot of the pool it holds, on any node: there is no
// cycle. It was not so before: a part read held an execute slot while its
// on-demand forward waited for a forward slot the fan-out held, and with a
// pool of one that never ended (TestFanOutAtATakeOverPoolOneLiveRemoval).
// Every wait is bounded besides: a part read by clusterListingTimeout, here
// and in a remote push.
//
// Lock order (spread_gate.go). The gate is taken shared, once, at the top of
// fanOut, by a goroutine that holds no fence (its callers are serveRead's
// owner branch, serveForwardedEntry's forwarded read and the one-member read
// door, none of which holds one). Under it, part reads take no gate (a part
// is never a template) and no fence (readers take none), and the template's
// own answer is a read too. So a fan-out holds one gate, shared, and nothing
// that a drop's exclusive hold or a fence holder waits on.
//
// Cost (rule 11). Per fan-out: one goroutine per remote part owner and up to
// localPartReadsAtOnce for this node's parts, all ended before the answer is
// returned; the parts' answers, held until the merge
// (each remote push bounded by its owner's batch reply budget, the local
// parts' together by the same budget here); a merge of O(parts × (skip +
// limit)) documents. Messages: one Batch push per remote owner. Nothing per
// template or part survives the request; no per-bucket metric.

// partRead is one part's share of a fan-out: its index, its name and the
// frame that reads it.
type partRead struct {
	p     int
	part  string
	frame *transport.Frame
}

// partAnswer is a part's answer, or why there is none, and the node asked.
type partAnswer struct {
	frame  *transport.Frame
	err    error
	holder string
}

// spreadPartReadCarried reports whether op is a read the router sends a part
// in a fan-out (and a Batch push may carry for a part name).
func spreadPartReadCarried(op transport.Opcode) bool {
	switch op {
	case transport.OpScan, transport.OpCount, transport.OpDistinct, transport.OpSearch,
		transport.OpBucketStats, transport.OpBucketInfo:
		return true
	}
	return false
}

// isDumpOpcode reports whether op is one of the three dump opcodes.
func isDumpOpcode(op transport.Opcode) bool {
	return op == transport.OpDumpStart || op == transport.OpDumpChunk || op == transport.OpDumpDone
}

// partOwnerSilentError is a part owner that gave no answer to the router's
// read: the push failed or timed out.
type partOwnerSilentError struct {
	node  string
	cause error
}

func (e *partOwnerSilentError) Error() string {
	return fmt.Sprintf("node %s did not answer: %v", e.node, e.cause)
}

func (e *partOwnerSilentError) Unwrap() error { return e.cause }

// readPartLocally runs one part read on this node, bounded as a remote
// owner's Batch push is (clusterListingTimeout). It holds no query slot of its
// own: its query takes an execute slot in admitQuery only while it executes,
// and that slot, like any forwarded bytes the answer carries, is given back as
// soon as the part answers, never after the merge. On a part this node is
// taking over the read may wait out the on-demand bound and be sent to the
// part's previous owner; it carries the fan-out's admission, so that forward
// takes no forward slot (Cluster.forwardRead). A part that does not answer in
// the bound is answered as a silent owner.
func readPartLocally(ctx context.Context, read partRead, holder string, run func(context.Context, partRead) (*transport.Frame, error)) partAnswer {
	ctx, cancel := context.WithTimeout(withFanOutAdmitted(ctx), clusterListingTimeout)
	defer cancel()
	frame, err := run(ctx, read)
	if frame != nil {
		if hook := frame.TakeWrittenHook(); hook != nil {
			hook()
		}
	}
	if ctx.Err() != nil && (err != nil || frame == nil || frame.Opcode == transport.OpError) {
		return partAnswer{err: &partOwnerSilentError{node: holder, cause: ctx.Err()}, holder: holder}
	}
	return partAnswer{frame: frame, err: err, holder: holder}
}

// localPartReadsAtOnce bounds how many of one fan-out's parts on this node
// are read at once: a part being taken over waits up to the on-demand bound,
// and read one at a time a fan-out over many such parts held its gate and its
// admission for tens of seconds. Each read holds no slot while it waits
// (readPartLocally), so reading several at once adds no wait to the order.
const localPartReadsAtOnce = 16

// errPartNotAsked is the answer of a part that was not read because an earlier
// part's answer already refused the whole read.
var errPartNotAsked = errors.New("the part was not read: another part refused the read first")

// refuses reports whether a part's answer refuses the whole read.
func (a partAnswer) refuses() bool {
	return a.err != nil || a.frame == nil || a.frame.Opcode == transport.OpError || partNotFound(a.frame)
}

// readLocalParts reads the parts of reads at indices on this node, up to
// localPartReadsAtOnce at once, charging their answers to held together, and
// asks no further part once one has refused: the whole read is refused then.
func readLocalParts(ctx context.Context, reads []partRead, indices []int, answers []partAnswer, held *resultBudget, holder string,
	run func(context.Context, partRead) (*transport.Frame, error)) {
	var (
		mu       sync.Mutex
		refused  bool
		finished sync.WaitGroup
	)
	slots := make(chan struct{}, localPartReadsAtOnce)
	stopped := func() bool {
		mu.Lock()
		defer mu.Unlock()
		return refused
	}
	for _, i := range indices {
		slots <- struct{}{}
		if stopped() {
			<-slots
			answers[i] = partAnswer{err: errPartNotAsked, holder: holder}
			continue
		}
		finished.Add(1)
		go func(i int) {
			defer finished.Done()
			defer func() { <-slots }()
			answer := readPartLocally(ctx, reads[i], holder, run)
			mu.Lock()
			defer mu.Unlock()
			if err := chargeHeld(held, &answer); err != nil {
				answer = partAnswer{err: err, holder: holder}
			}
			refused = refused || answer.refuses()
			// Each goroutine writes only its own slot; they are read after
			// Wait.
			answers[i] = answer
		}(i)
	}
	finished.Wait()
}

// chargeHeld charges a local part's answer to the router's held budget: the
// local parts' answers are bounded together as one owner's Batch push is.
// The caller serialises calls.
func chargeHeld(held *resultBudget, answer *partAnswer) error {
	if answer.frame == nil || held == nil {
		return nil
	}
	return held.charge(int64(len(answer.frame.Payload)))
}

// readParts reads every part of reads from its owner: one Batch push per
// remote owner, all at once, while this node's own parts are read here.
func (d clusterSpreadDoor) readParts(ctx context.Context, database string, reads []partRead, held *resultBudget) []partAnswer {
	answers := make([]partAnswer, len(reads))
	byOwner := map[string][]int{}
	var owners []ring.Node
	var local []int
	for i, read := range reads {
		owner, err := d.c.Owner(placementOf(database, read.part))
		if err != nil {
			answers[i] = partAnswer{err: err}
			continue
		}
		answers[i].holder = owner.ID
		if owner.ID == d.c.self.ID {
			local = append(local, i)
			continue
		}
		if _, seen := byOwner[owner.ID]; !seen {
			owners = append(owners, owner)
		}
		byOwner[owner.ID] = append(byOwner[owner.ID], i)
	}
	// Each goroutine writes only its own owner's slots of answers, and they
	// are read after Wait.
	var pushes sync.WaitGroup
	for _, owner := range owners {
		pushes.Add(1)
		go func(owner ring.Node, indices []int) {
			defer pushes.Done()
			d.pushPartReads(ctx, owner, reads, indices, answers)
		}(owner, byOwner[owner.ID])
	}
	readLocalParts(ctx, reads, local, answers, held, d.c.self.ID, func(ctx context.Context, read partRead) (*transport.Frame, error) {
		return d.c.serveRead(ctx, read.frame, read.part, database)
	})
	pushes.Wait()
	return answers
}

// pushPartReads sends one owner its parts' reads as one Batch push (B8),
// bounded by clusterListingTimeout, and records each part's answer. An owner
// that does not advertise batches is asked one part at a time.
func (d clusterSpreadDoor) pushPartReads(ctx context.Context, owner ring.Node, reads []partRead, indices []int, answers []partAnswer) {
	pushCtx, cancel := context.WithTimeout(ctx, clusterListingTimeout)
	defer cancel()
	entries := make([]ReplEntry, len(indices))
	for j, i := range indices {
		entries[j] = ReplEntry{Op: int32(reads[i].frame.Opcode), Bucket: reads[i].part, Payload: reads[i].frame.Payload}
	}
	silent := func(err error) error {
		if errors.Is(err, ErrForwardLost) || errors.Is(err, context.DeadlineExceeded) {
			return &partOwnerSilentError{node: owner.ID, cause: err}
		}
		return err
	}
	if !d.c.memberAdvertises(owner.ID, featureBatch) || withholdBatchFeatureForTesting.Load() {
		for j, i := range indices {
			resp, err := d.c.forward(pushCtx, owner, entries[j], 0, clientReadForward)
			if err != nil {
				answers[i].err = silent(err)
				continue
			}
			answers[i].frame = transport.NewFrame(transport.Opcode(resp.Opcode), resp.Payload)
		}
		return
	}
	if hook := forwardBatchTargetHook.Load(); hook != nil {
		(*hook)(entries, owner.ID)
	}
	req := &ReplPushRequest{
		Token:          d.c.localToken(),
		FromNode:       d.c.self.ID,
		Entries:        entries,
		ReturnResponse: true,
		Epoch:          d.c.Epoch(),
		ReadWaitMs:     clientReadForward.waitMs,
		Batch:          true,
	}
	resp, err := d.c.peerFor(owner).call(pushCtx, transport.LaneRequest, req)
	if err == nil {
		d.c.learnFromPeer(owner.ID, resp.DroppedGenerations)
	}
	every := func(err error) {
		for _, i := range indices {
			answers[i].err = err
		}
	}
	switch {
	case err != nil:
		every(&partOwnerSilentError{node: owner.ID, cause: err})
		return
	case !resp.Success:
		every(forwardRefusal(owner.ID, entries[0], req.Epoch, resp))
		return
	case len(resp.BatchAnswers) != len(indices):
		every(&partOwnerSilentError{node: owner.ID, cause: fmt.Errorf("it answered %d of the %d part reads sent to it", len(resp.BatchAnswers), len(indices))})
		return
	}
	for j, i := range indices {
		answer := resp.BatchAnswers[j]
		switch {
		case !answer.Success:
			answers[i].err = forwardRefusal(owner.ID, entries[j], req.Epoch, &answer)
		case len(answer.Responses) != 1:
			answers[i].err = &partOwnerSilentError{node: owner.ID, cause: fmt.Errorf("it returned %d responses for one part read", len(answer.Responses))}
		default:
			answers[i].frame = transport.NewFrame(transport.Opcode(answer.Responses[0].Opcode), answer.Responses[0].Payload)
		}
	}
}

// readParts at the one-member door: every part is on this node, read through
// the door's read path (readLocalParts).
func (d localSpreadDoor) readParts(ctx context.Context, _ string, reads []partRead, held *resultBudget) []partAnswer {
	answers := make([]partAnswer, len(reads))
	indices := make([]int, len(reads))
	for i := range indices {
		indices[i] = i
	}
	readLocalParts(ctx, reads, indices, answers, held, dumpNodeLabel(d.s), func(ctx context.Context, read partRead) (*transport.Frame, error) {
		return d.s.dispatchInFence(ctx, read.frame)
	})
	return answers
}

// fireAnswerHooks runs the written hooks of part answers that are merged and
// never written themselves: a byte-accounting or slot hook fires once.
func fireAnswerHooks(answers []partAnswer) {
	for _, answer := range answers {
		if answer.frame == nil {
			continue
		}
		if hook := answer.frame.TakeWrittenHook(); hook != nil {
			hook()
		}
	}
}

// routeFanOut answers a read of the fan-out class at the router. A query is
// admitted once, from the forward pool (R1, rule 3), and its slot rides on
// the answer frame. A fan-out another node forwarded here was admitted there,
// by Cluster.forwardRead's forward slot, which it holds until this answer
// reaches it: admitted again here, coordinators on two nodes could hold every
// forward slot of both while each waits on a router that needs one.
func (r *spreadRouter) routeFanOut(ctx context.Context, frame *transport.Frame) *transport.Frame {
	if isDumpOpcode(frame.Opcode) {
		return r.routeDump(ctx, frame)
	}
	var admitted func()
	if isQueryOpcode(frame.Opcode) && !fanOutAdmitted(ctx) {
		gate := r.s.queryGate()
		release, err := gate.acquire(ctx, gate.forwardPool())
		if err != nil {
			return errorFrame(frame, fmt.Errorf("admitting %s of bucket %q (database %q): %w", spreadOpVerb(frame.Opcode), r.t.name, r.t.database, err))
		}
		admitted = release
	}
	answer := r.fanOut(ctx, frame)
	answer.RequestID = frame.RequestID
	if admitted != nil {
		answer.AddWrittenHook(admitted)
	}
	return answer
}

// fanOutAdmittedCtxKey marks a fan-out a coordinator forwarded here, which
// its forward slot there admitted.
type fanOutAdmittedCtxKey struct{}

// withFanOutAdmitted says the read was admitted by the node that forwarded it.
func withFanOutAdmitted(ctx context.Context) context.Context {
	return context.WithValue(ctx, fanOutAdmittedCtxKey{}, true)
}

// fanOutAdmitted reports whether ctx says the read was admitted elsewhere.
func fanOutAdmitted(ctx context.Context) bool {
	admitted, _ := ctx.Value(fanOutAdmittedCtxKey{}).(bool)
	return admitted
}

// fanOut is the fan-out under the router gate, shared, taken once.
func (r *spreadRouter) fanOut(ctx context.Context, frame *transport.Frame) *transport.Frame {
	release, err := r.holdGate(ctx, false, frame.Opcode)
	if err != nil {
		return errorFrame(frame, err)
	}
	defer release()
	born, err := r.s.readBornSet(r.t.database, r.t.name)
	if err != nil {
		return errorFrame(frame, err)
	}
	if born.dropping() {
		return errorFrame(frame, droppingError(r.t.database, r.t.name))
	}
	// The template's own answer: the request's checks, made against the
	// collection's declaration, and the shape of the answer an empty bucket
	// gives. A request it refuses asks no part. The template holds one
	// document, the born set, and its counts are replaced by the parts'.
	skeleton, err := r.templateAnswer(ctx, frame)
	if err != nil {
		return errorFrame(frame, err)
	}
	if skeleton.Opcode == transport.OpError || answeredWithoutParts(skeleton) {
		return skeleton
	}
	reads, err := r.partReads(frame, born)
	if err != nil {
		return errorFrame(frame, err)
	}
	held := r.s.newResultBudgetWithCeiling(fanOutMethod(frame.Opcode), r.s.batchReplyBudget(), true, fanOutRefusalShape(frame, r.t))
	defer held.report()
	answers := r.door.readParts(ctx, r.t.database, reads, held)
	defer fireAnswerHooks(answers)
	frames, refusal := r.partFrames(reads, answers)
	if refusal != nil {
		return refusal
	}
	merged, err := r.merge(frame, skeleton, frames)
	if err != nil {
		return errorFrame(frame, err)
	}
	return merged
}

// fanOutMethod is the budget method of a fan-out's opcode.
func fanOutMethod(op transport.Opcode) string {
	switch op {
	case transport.OpScan:
		return queryMethodScan
	case transport.OpSearch:
		return queryMethodSearch
	case transport.OpDistinct:
		return queryMethodDistinct
	}
	return queryMethodCount
}

// fanOutRefusalShape is the result-too-large text of a fan-out, under the
// collection's name.
func fanOutRefusalShape(frame *transport.Frame, t spreadTemplate) resultTooLargeError {
	switch frame.Opcode {
	case transport.OpDistinct:
		field, _ := bson.Raw(frame.Payload).Lookup("field").StringValueOK()
		return resultTooLargeError{query: fmt.Sprintf("distinct(%q) on %s.%s", field, t.database, t.name), shape: resultShapeDistinct}
	case transport.OpSearch:
		return resultTooLargeError{query: fmt.Sprintf("search() on %s.%s", t.database, t.name), shape: resultShapeFind}
	}
	// The find's own window, so the refusal describes what was asked: a
	// deep page asks every part for its first skip + limit, which a whole
	// bucket never holds at once (N6; a step 7 gate).
	var req ScanRequest
	refusal := resultTooLargeError{query: fmt.Sprintf("find() on %s.%s", t.database, t.name), shape: resultShapeFind}
	if bson.Unmarshal(frame.Payload, &req) == nil {
		refusal.skip, refusal.limit = scanWindow(req)
		refusal.sortField, _, _ = singleSortField(req.Sort)
	}
	return refusal
}

// templateAnswer runs the request's handler against the template's own store
// on this node, past dispatch: it is one document, and the router holds no
// execute slot (R1, rule 3).
func (r *spreadRouter) templateAnswer(ctx context.Context, frame *transport.Frame) (*transport.Frame, error) {
	handle, ok := localHandlers[frame.Opcode]
	if !ok {
		return nil, fmt.Errorf("no handler for %s", frame.Opcode)
	}
	answer, err := handle(ctx, frame, r.s)
	if err != nil {
		return nil, err
	}
	return answer, nil
}

// answeredWithoutParts reports whether the template's answer is the whole
// answer: a scan of a bucket that does not resolve, or a search answered in
// band (no filter, or a failure it describes).
func answeredWithoutParts(skeleton *transport.Frame) bool {
	switch skeleton.Opcode {
	case transport.OpScan:
		found, _ := bson.Raw(skeleton.Payload).Lookup("found").BooleanOK()
		return !found
	case transport.OpSearch:
		message, _ := bson.Raw(skeleton.Payload).Lookup("message").StringValueOK()
		return message != ""
	}
	return false
}

// partReads is the request of every created part, in part order. A find asks
// each part for its first skip + limit, unprojected (B10, R4): the router
// merges, then skips, limits and projects. Every other read names the part
// and is otherwise the client's.
func (r *spreadRouter) partReads(frame *transport.Frame, born bornSet) ([]partRead, error) {
	var stamp bson.D
	if frame.Opcode == transport.OpScan {
		var req ScanRequest
		if err := bson.Unmarshal(frame.Payload, &req); err != nil {
			return nil, fmt.Errorf("decoding the find of bucket %q: %w", r.t.name, err)
		}
		skip, limit := scanWindow(req)
		stamp = bson.D{{Key: "skip", Value: int64(0)}, {Key: "limit", Value: min(int64(skip)+int64(limit), math.MaxInt32)},
			{Key: "projection", Value: bson.D{}}, {Key: "expect_bucket_id", Value: int64(0)}}
	}
	var reads []partRead
	for p := 0; p < r.t.parts; p++ {
		if born.state(p) != partCreated {
			continue
		}
		part := partName(r.t.name, p)
		payload, err := stampPayload(frame.Payload, append(bson.D{{Key: "bucket", Value: part}}, stamp...))
		if err != nil {
			return nil, fmt.Errorf("naming a part of bucket %q in the read: %w", r.t.name, err)
		}
		reads = append(reads, partRead{p: p, part: part, frame: transport.NewFrame(frame.Opcode, payload)})
	}
	return reads, nil
}

// scanWindow is a find's skip and limit as ScanCtx reads them.
func scanWindow(req ScanRequest) (skip, limit int) {
	limit = req.Limit
	if limit <= 0 {
		limit = defaultScanLimit
	}
	return max(req.Skip, 0), limit
}

// partFrames checks every part's answer in part order. The first part that
// did not answer, refused, or no longer exists refuses the whole read: never
// a partial answer.
func (r *spreadRouter) partFrames(reads []partRead, answers []partAnswer) ([]*transport.Frame, *transport.Frame) {
	frames := make([]*transport.Frame, len(reads))
	for i, answer := range answers {
		read := reads[i]
		if errors.Is(answer.err, errPartNotAsked) {
			// Another part refused: its refusal is the answer, wherever it is
			// in part order.
			continue
		}
		if answer.err != nil {
			var silent *partOwnerSilentError
			if errors.As(answer.err, &silent) {
				return nil, errorFrame(read.frame, partOwnerSilent(r.t, silent))
			}
			return nil, errorFrame(read.frame, r.displayed(answer.err, read.part))
		}
		if partNotFound(answer.frame) {
			return nil, errorFrame(read.frame, r.door.lostPart(r.t.database, r.t.name, read.part, false))
		}
		if answer.frame.Opcode == transport.OpError {
			// A frame the client reads: built with the protocol version, as
			// every answer frame is.
			return nil, transport.NewFrameWithID(transport.OpError, read.frame.RequestID, displayedAnswer(answer.frame.Payload, read.part, r.t.name))
		}
		frames[i] = answer.frame
	}
	for i, answer := range answers {
		if errors.Is(answer.err, errPartNotAsked) {
			return nil, errorFrame(reads[i].frame, fmt.Errorf("reading bucket %q (database %q): %w", r.t.name, r.t.database, answer.err))
		}
	}
	return frames, nil
}

// partNotFound reports whether a part's answer says the part does not exist:
// NOT FOUND for the bucket, or a find's in-band "found: false".
func partNotFound(answer *transport.Frame) bool {
	if answerNamesNoBucket(int32(answer.Opcode), answer.Payload) {
		return true
	}
	if answer.Opcode != transport.OpScan {
		return false
	}
	found, ok := bson.Raw(answer.Payload).Lookup("found").BooleanOK()
	return ok && !found
}

// partOwnerSilent is spec section 7.1's refusal of a read a part owner did
// not answer, UNAVAILABLE: nothing is returned.
func partOwnerSilent(t spreadTemplate, silent *partOwnerSilentError) error {
	return &sentencedError{cause: ErrForwardLost, message: fmt.Sprintf(
		"Could not read all of bucket %q (database %q): node %s, which owns some of its documents, did not answer, so nothing was returned. "+
			"Retry once %s is back; 'nodes' shows its state.",
		t.name, t.database, silent.node, silent.node)}
}

// merge folds the parts' answers into the collection's.
func (r *spreadRouter) merge(frame, skeleton *transport.Frame, parts []*transport.Frame) (*transport.Frame, error) {
	switch frame.Opcode {
	case transport.OpScan:
		return r.mergeFind(frame, skeleton, parts)
	case transport.OpCount:
		return r.mergeCount(frame, skeleton, parts)
	case transport.OpDistinct:
		return r.mergeDistinct(frame, skeleton, parts)
	case transport.OpSearch:
		return r.mergeSearch(frame, skeleton, parts)
	case transport.OpBucketStats:
		return r.mergeStats(frame, skeleton, parts)
	case transport.OpBucketInfo:
		return r.mergeInfo(frame, skeleton, parts)
	}
	return nil, fmt.Errorf("%s is not a read the router of bucket %q merges", frame.Opcode, r.t.name)
}

// mergeCount sums the parts' counts.
func (r *spreadRouter) mergeCount(frame, skeleton *transport.Frame, parts []*transport.Frame) (*transport.Frame, error) {
	var answer CountResponse
	if err := bson.Unmarshal(skeleton.Payload, &answer); err != nil {
		return nil, fmt.Errorf("decoding the count of bucket %q: %w", r.t.name, err)
	}
	answer.Count = 0
	for _, part := range parts {
		var counted CountResponse
		if err := bson.Unmarshal(part.Payload, &counted); err != nil {
			return nil, fmt.Errorf("decoding a part's count of bucket %q: %w", r.t.name, err)
		}
		answer.Count += counted.Count
	}
	return respond(frame, transport.OpCount, &answer)
}

// partDocument is one document of a part's find answer, with what the
// merge orders it by.
type partDocument struct {
	document bson.Document
	id       []byte
	key      sortKey
}

// mergeFind merges the parts' first skip + limit documents into the page a
// whole bucket returns: by _id_ when the find has no sort (a hash bucket's
// natural order is ascending key), else by the sort key with ties by _id_
// (the whole bucket's ties are in natural order). Then skip, limit and the
// client's projection, compiled once and applied here (R4). Total is the
// parts' sum.
func (r *spreadRouter) mergeFind(frame, skeleton *transport.Frame, parts []*transport.Frame) (*transport.Frame, error) {
	var req ScanRequest
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return nil, fmt.Errorf("decoding the find of bucket %q: %w", r.t.name, err)
	}
	var answer ScanResponse
	if err := bson.Unmarshal(skeleton.Payload, &answer); err != nil {
		return nil, fmt.Errorf("decoding the find of bucket %q: %w", r.t.name, err)
	}
	skip, limit := scanWindow(req)
	sortField, ascending, err := singleSortField(req.Sort)
	if err != nil {
		return nil, err
	}
	var lookup bson.Path
	if sortField != "" {
		path, err := parseFieldPath(sortField)
		if err != nil {
			return nil, fmt.Errorf("the sort key %w", err)
		}
		lookup = libraryPath(path)
	}
	projection, err := compileProjection(req.Projection)
	if err != nil {
		return nil, err
	}
	var total int64
	runs := make([][]partDocument, len(parts))
	for i, part := range parts {
		var page ScanResponse
		if err := bson.Unmarshal(part.Payload, &page); err != nil {
			return nil, fmt.Errorf("decoding a part's find of bucket %q: %w", r.t.name, err)
		}
		total += page.Total
		run := make([]partDocument, 0, len(page.Documents))
		for _, raw := range page.Documents {
			// A part's answer may come from another node: validated, never
			// trusted as this node's own stored bytes.
			document, err := validateAt(raw, recordDepthLimit(recordStored, BucketTypeHash))
			if err != nil {
				return nil, fmt.Errorf("reading a document of bucket %q from a part's answer: %w", r.t.name, err)
			}
			merged := partDocument{document: document, id: documentIDBytes(raw)}
			if sortField != "" {
				if merged.key, _, err = documentSortKey(document, lookup); err != nil {
					return nil, fmt.Errorf("sorting %s.%s: %w", r.t.database, r.t.name, err)
				}
			}
			run = append(run, merged)
		}
		runs[i] = run
	}
	before := func(a, b partDocument) bool {
		if sortField != "" {
			comparison := compareSortKeys(a.key, b.key)
			if !ascending {
				comparison = -comparison
			}
			if comparison != 0 {
				return comparison < 0
			}
		}
		return bytes.Compare(a.id, b.id) < 0
	}
	ordered := mergeRuns(runs, skip+limit, before)
	budget := r.s.newResultBudget(queryMethodScan, resultTooLargeError{query: fmt.Sprintf("find() on %s.%s", r.t.database, r.t.name),
		shape: resultShapeFind, limit: limit, skip: skip, sortField: sortField})
	defer budget.report()
	var documents []bson.Raw
	if sortField == "" && (len(req.Filter) > 0 || len(req.Projection) > 0) {
		// A filtered or projected find of a whole bucket answers an empty
		// page as an empty array, the others as none.
		documents = []bson.Raw{}
	}
	for index := skip; index < len(ordered); index++ {
		encoded, err := r.projected(ordered[index].document, projection, req, sortField != "")
		if err != nil {
			return nil, err
		}
		if err := budget.chargeDocument(elementHeader(len(documents)) + int64(len(encoded))); err != nil {
			return nil, err
		}
		documents = append(documents, encoded)
	}
	answer.Documents = documents
	answer.Total = total
	answer.HasMore = int64(skip)+int64(limit) < total
	answer.Message = ""
	if sortField != "" && total > 100 {
		answer.Message = fmt.Sprintf("note: sorting %d documents without an index", total)
	}
	return respond(frame, transport.OpScan, &answer)
}

// projected is one page document with the client's projection applied, as
// the whole bucket's sorted or unsorted page applies it.
func (r *spreadRouter) projected(document bson.Document, projection *compiledProjection, req ScanRequest, sorted bool) (bson.Raw, error) {
	if projection == nil {
		return bson.Raw(document.Bytes()), nil
	}
	if sorted {
		return sortedPageDocument(document.Bytes(), projection, req)
	}
	return pageDocument(document, projection, req)
}

// documentIDBytes is a document's _id_ as the hash store keys it: the text's
// bytes, whose order is the store's natural order.
func documentIDBytes(raw bson.Raw) []byte {
	id, ok := raw.Lookup("_id_").StringValueOK()
	if !ok {
		return nil
	}
	return []byte(id)
}

// mergeRuns merges runs, each already in order, into the first n documents in
// order. Equal documents keep the order of their runs.
func mergeRuns(runs [][]partDocument, n int, before func(a, b partDocument) bool) []partDocument {
	heads := make([]int, len(runs))
	var merged []partDocument
	for len(merged) < n {
		best := -1
		for i, run := range runs {
			if heads[i] >= len(run) {
				continue
			}
			if best < 0 || before(run[heads[i]], runs[best][heads[best]]) {
				best = i
			}
		}
		if best < 0 {
			break
		}
		merged = append(merged, runs[best][heads[best]])
		heads[best]++
	}
	return merged
}

// mergeDistinct is the union of the parts' values in part order, first seen,
// each value decoded and keyed as a whole bucket's distinct keys it, and
// charged to this node's result budget: over it the read is refused, as a
// whole bucket refuses (N7).
func (r *spreadRouter) mergeDistinct(frame, skeleton *transport.Frame, parts []*transport.Frame) (*transport.Frame, error) {
	var answer DistinctResponse
	if err := bson.Unmarshal(skeleton.Payload, &answer); err != nil {
		return nil, fmt.Errorf("decoding the distinct of bucket %q: %w", r.t.name, err)
	}
	budget := r.s.newResultBudget(queryMethodDistinct, fanOutRefusalShape(frame, r.t))
	defer budget.report()
	seen := map[string]struct{}{}
	var values []interface{}
	for _, part := range parts {
		array, ok := bson.Raw(part.Payload).Lookup("values").ArrayOK()
		if !ok {
			continue
		}
		elements, err := array.Values()
		if err != nil {
			return nil, fmt.Errorf("decoding a part's distinct values of bucket %q: %w", r.t.name, err)
		}
		for _, element := range elements {
			value, err := store.DecodeStoredValue(element.Type, element.Value)
			if err != nil {
				return nil, fmt.Errorf("decoding a distinct value of bucket %q: %w", r.t.name, err)
			}
			key := valueToDistinctKey(value)
			if _, already := seen[key]; already {
				continue
			}
			if err := budget.chargeDocument(elementHeader(len(values)) + int64(len(element.Value)) + int64(len(key))); err != nil {
				return nil, err
			}
			seen[key] = struct{}{}
			values = append(values, value)
		}
	}
	answer.Values = values
	return respond(frame, transport.OpDistinct, &answer)
}

// mergeSearch is the parts' hits in part order, up to the limit, as a find
// with no sort is merged by section 3: Total is what is returned, and more
// remain when a part said so or the parts held more than the limit.
func (r *spreadRouter) mergeSearch(frame, skeleton *transport.Frame, parts []*transport.Frame) (*transport.Frame, error) {
	var req SearchRequest
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return nil, fmt.Errorf("decoding the search of bucket %q: %w", r.t.name, err)
	}
	var answer SearchResponse
	if err := bson.Unmarshal(skeleton.Payload, &answer); err != nil {
		return nil, fmt.Errorf("decoding the search of bucket %q: %w", r.t.name, err)
	}
	budget := r.s.newResultBudget(queryMethodSearch, fanOutRefusalShape(frame, r.t))
	defer budget.report()
	var results []bson.Raw
	hasMore := false
	for _, part := range parts {
		var hits SearchResponse
		if err := bson.Unmarshal(part.Payload, &hits); err != nil {
			return nil, fmt.Errorf("decoding a part's search of bucket %q: %w", r.t.name, err)
		}
		hasMore = hasMore || hits.HasMore
		for _, hit := range hits.Results {
			if req.Limit > 0 && len(results) == req.Limit {
				hasMore = true
				break
			}
			if err := budget.chargeDocument(elementHeader(len(results)) + int64(len(hit))); err != nil {
				return nil, err
			}
			results = append(results, hit)
		}
	}
	answer.Results, answer.Total, answer.HasMore = results, len(results), hasMore
	return respond(frame, transport.OpSearch, &answer)
}

// mergeStats is the template's describe (its declaration, factor, placement
// and copies) with the entries, bytes and store elements of every part
// summed; the born set is never counted (N8).
func (r *spreadRouter) mergeStats(frame, skeleton *transport.Frame, parts []*transport.Frame) (*transport.Frame, error) {
	var answer BucketStatsResponse
	if err := bson.Unmarshal(skeleton.Payload, &answer); err != nil {
		return nil, fmt.Errorf("decoding the describe of bucket %q: %w", r.t.name, err)
	}
	answer.Entries, answer.LogicalBytes = 0, 0
	elements := map[string]int64{}
	for _, part := range parts {
		var described BucketStatsResponse
		if err := bson.Unmarshal(part.Payload, &described); err != nil {
			return nil, fmt.Errorf("decoding a part's describe of bucket %q: %w", r.t.name, err)
		}
		answer.Entries += described.Entries
		answer.LogicalBytes += described.LogicalBytes
		for _, stats := range described.Stores {
			elements[stats.Parent+"\x00"+stats.Name] += stats.Elements
		}
	}
	for i := range answer.Stores {
		answer.Stores[i].Elements = elements[answer.Stores[i].Parent+"\x00"+answer.Stores[i].Name]
	}
	return respondFrame(frame, transport.OpBucketStats, &answer), nil
}

// mergeInfo is the template's info with every part's entries summed.
func (r *spreadRouter) mergeInfo(frame, skeleton *transport.Frame, parts []*transport.Frame) (*transport.Frame, error) {
	var answer BucketInfoResponse
	if err := bson.Unmarshal(skeleton.Payload, &answer); err != nil {
		return nil, fmt.Errorf("decoding the info of bucket %q: %w", r.t.name, err)
	}
	answer.Bucket.Entries = 0
	for _, part := range parts {
		var info BucketInfoResponse
		if err := bson.Unmarshal(part.Payload, &info); err != nil {
			return nil, fmt.Errorf("decoding a part's info of bucket %q: %w", r.t.name, err)
		}
		answer.Bucket.Entries += info.Bucket.Entries
	}
	return respond(frame, transport.OpBucketInfo, &answer)
}
