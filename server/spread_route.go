package server

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// spreadDoor is what the router needs of the door it runs at: on a node that
// routes, the cluster's write and read paths (clusterSpreadDoor); on a node
// alone on its ring or with no cluster, the one-member door's fenced dispatch
// (localSpreadDoor). The router's logic is the same at both, so the doors
// cannot drift.
type spreadDoor interface {
	// prepare turns a frame the router built into the entry it sends, as a
	// client's write is prepared at this door.
	prepare(ctx context.Context, frame *transport.Frame, bucket string) (ReplEntry, error)
	// markTemplate applies a born-set write to the template at majority and
	// says how many copies confirmed it, of the template's declared factor.
	markTemplate(ctx context.Context, entry ReplEntry, template placementKey) (resp ReplResponse, confirmed, factor int, err error)
	// dropTemplate drops the template itself, the last step of a drop (the
	// gate is held in ctx), sent to every member.
	dropTemplate(ctx context.Context, entry ReplEntry, template placementKey, wc WriteConcernLevel) (ReplResponse, error)
	// writePart sends a write, a create or a drop to a part's owner.
	writePart(ctx context.Context, database string, entry ReplEntry, wc WriteConcernLevel) (ReplResponse, error)
	// readPart answers a read of a part, from its owner.
	readPart(ctx context.Context, frame *transport.Frame, database, part string) (*transport.Frame, error)
	// readParts answers every read of a fan-out from its part's owner, in
	// the order of reads, each part read under one execute slot of its
	// owner's (spread_fanout.go). The answers of the parts on this node are
	// charged to held together.
	readParts(ctx context.Context, database string, reads []partRead, held *resultBudget) []partAnswer
	// recordedPartOf is the part_of of a part this door can read locally
	// before it sends a create, for the one-member door, where no owner
	// precheck runs; known is false where the part's owner judges it.
	recordedPartOf(database, part string) (partOf uint64, known bool, err error)
	// lostPart is the refusal of an operation on a part the born set records
	// created and its owner does not hold (B9), in this door's words.
	lostPart(database, bucket, part string, write bool) error
	// awaitsTakeOver refuses, with the hand-off, a write at a router that has
	// not taken its template over: its copy of the born set may lag the
	// previous owner's, since a mark needs only a majority of the copies.
	awaitsTakeOver(database, template string, key placementKey) error
}

// spreadRouter routes one operation of one template.
type spreadRouter struct {
	s    *Server
	door spreadDoor
	t    spreadTemplate
}

// routerInternalContext is the context of the router's own writes: the
// template's gate is held (so a mark reaches the template instead of being
// routed again), and a batch item's chain owner and local step are cleared,
// so a part on another owner is not refused as outside the chain (N1) and a
// mark does not end the item's local step before its document is written.
func routerInternalContext(ctx context.Context, template placementKey) context.Context {
	ctx = context.WithValue(ctx, batchChainOwnerCtxKey{}, nil)
	ctx = context.WithValue(ctx, localStepCtxKey{}, nil)
	return withRouterGateHeld(ctx, template)
}

// withoutBatchChainOwner clears a batch item's chain owner for the item's
// write to its part, which another node may own (N1). The local step stays:
// the part's owner, when it is this node, ends it after its apply.
func withoutBatchChainOwner(ctx context.Context) context.Context {
	return context.WithValue(ctx, batchChainOwnerCtxKey{}, nil)
}

// displayedError shows a part's name as its collection's in an error that
// came back from the part, and keeps the cause for errors.Is and errors.As.
type displayedError struct {
	err              error
	part, collection string
}

func (e *displayedError) Error() string {
	return displayPartIn(e.err.Error(), e.part, e.collection)
}

// displayPartIn shows part as collection in text, raw or quoted with its
// separator escaped, as %q prints it.
func displayPartIn(text, part, collection string) string {
	quoted := strconv.Quote(part)
	text = strings.ReplaceAll(text, quoted[1:len(quoted)-1], collection)
	return strings.ReplaceAll(text, part, collection)
}

func (e *displayedError) Unwrap() error { return e.err }

// displayed wraps err so no part name reaches a client.
func (r *spreadRouter) displayed(err error, part string) error {
	if err == nil || displayPartIn(err.Error(), part, r.t.name) == err.Error() {
		return err
	}
	return &displayedError{err: err, part: part, collection: r.t.name}
}

// routeWrite routes a client's write of the template (section 2.1, steps 1 to
// 3). Nothing is logged under the template's name for it: a refusal is
// answered here, and the document goes to its part only.
func (r *spreadRouter) routeWrite(ctx context.Context, entry ReplEntry, wc WriteConcernLevel) (ReplResponse, error) {
	op := transport.Opcode(entry.Op)
	class := spreadClassOf(op)
	if op == transport.OpUpdate && decodeFrameMeta(entry.Payload).scansBucket() {
		return ReplResponse{}, withOutcome(outcomeNotApplied, filteredUpdateOnSpreadError(r.t.database, r.t.name))
	}
	switch class {
	case spreadCollectionDrop:
		return r.dropCollection(ctx, entry, wc)
	case spreadDDL:
		return r.declineDDL(entry)
	case spreadKeyedWrite, spreadBulkWrite:
	default:
		return ReplResponse{}, withOutcome(outcomeNotApplied, spreadRefusal(op, class, r.t.database, r.t.name))
	}
	release, err := r.holdGate(ctx, false, op)
	if err != nil {
		return ReplResponse{}, err
	}
	defer release()
	// No answer, a not found or a dropping included, is given from a born set
	// this router has not taken over (reads are checked by serveRead first).
	if err := r.door.awaitsTakeOver(r.t.database, r.t.name, r.t.key); err != nil {
		return ReplResponse{}, err
	}
	born, err := r.s.readBornSet(r.t.database, r.t.name)
	if err != nil {
		return ReplResponse{}, withOutcome(outcomeNotApplied, err)
	}
	if born.dropping() {
		return ReplResponse{}, droppingError(r.t.database, r.t.name)
	}
	if class == spreadBulkWrite {
		return r.routeBulk(ctx, entry, wc, born)
	}
	key, err := spreadDocumentKey(op, entry.Payload)
	if err != nil {
		return ReplResponse{}, withOutcome(outcomeNotApplied, err)
	}
	if key == "" {
		return ReplResponse{}, withOutcome(outcomeNotApplied, &sentencedError{cause: ErrMalformedPayload, message: fmt.Sprintf(
			"%s of spread bucket %q (database %q) names no document: send its _id_ as text", op, r.t.name, r.t.database)})
	}
	p, err := r.t.partOfKey(key)
	if err != nil {
		return ReplResponse{}, withOutcome(outcomeNotApplied, err)
	}
	state := born.state(p)
	if state != partCreated {
		if !createsParts(op) {
			// R3: an unborn part, or one only marked, holds no document;
			// nothing is asked and nothing is marked.
			answer, err := r.unbornAnswer(ctx, transport.NewFrame(op, entry.Payload), key)
			if err != nil {
				return ReplResponse{}, err
			}
			return ReplResponse{Opcode: int32(answer.Opcode), Payload: answer.Payload}, nil
		}
		if err := r.ensureCreated(ctx, p); err != nil {
			return ReplResponse{}, err
		}
	}
	return r.writeToPart(ctx, entry, p, wc)
}

// writeToPart sends entry to part p, rewritten to name it. A created part
// whose owner holds no such bucket is a lost part (B9): never re-created
// (B4.3), refused.
func (r *spreadRouter) writeToPart(ctx context.Context, entry ReplEntry, p int, wc WriteConcernLevel) (ReplResponse, error) {
	part := partName(r.t.name, p)
	op := transport.Opcode(entry.Op)
	payload, err := withPartBucket(op, entry.Payload, part)
	if err != nil {
		return ReplResponse{}, withOutcome(outcomeNotApplied, err)
	}
	routed := entry
	routed.Bucket, routed.Payload = part, payload
	resp, err := r.door.writePart(withoutBatchChainOwner(ctx), r.t.database, routed, wc)
	if errors.Is(err, ErrBucketNotFound) || (err == nil && answerNamesNoBucket(resp.Opcode, resp.Payload)) {
		return ReplResponse{}, r.door.lostPart(r.t.database, r.t.name, part, true)
	}
	if err != nil {
		return resp, r.displayed(err, part)
	}
	resp.Payload = displayedAnswer(resp.Payload, part, r.t.name)
	return resp, nil
}

// routeRead routes a client's read of the template. A keyed read is routed
// to its part, a read of the fan-out class covers every created part
// (spread_fanout.go, spread_dump.go), a pass-through opcode is dispatched
// here as for any bucket, and every other read is refused, never answered
// from the template's store.
func (r *spreadRouter) routeRead(ctx context.Context, frame *transport.Frame) (*transport.Frame, error) {
	op := frame.Opcode
	switch class := spreadClassOf(op); class {
	case spreadPassThrough:
		return r.s.dispatch(ctx, frame)
	case spreadFanOutRead:
		return r.routeFanOut(ctx, frame), nil
	case spreadKeyedRead:
	default:
		return errorFrame(frame, spreadRefusal(op, class, r.t.database, r.t.name)), nil
	}
	release, err := r.holdGate(ctx, false, op)
	if err != nil {
		return errorFrame(frame, err), nil
	}
	defer release()
	born, err := r.s.readBornSet(r.t.database, r.t.name)
	if err != nil {
		return errorFrame(frame, err), nil
	}
	if born.dropping() {
		return errorFrame(frame, droppingError(r.t.database, r.t.name)), nil
	}
	key, err := spreadDocumentKey(op, frame.Payload)
	if err != nil {
		return errorFrame(frame, err), nil
	}
	p, err := r.t.partOfKey(key)
	if err != nil {
		return errorFrame(frame, err), nil
	}
	if born.state(p) != partCreated {
		return r.unbornAnswer(ctx, frame, key)
	}
	part := partName(r.t.name, p)
	payload, err := withPartBucket(op, frame.Payload, part)
	if err != nil {
		return errorFrame(frame, err), nil
	}
	answer, err := r.door.readPart(ctx, transport.NewFrameWithID(op, frame.RequestID, payload), r.t.database, part)
	if err != nil {
		return errorFrame(frame, r.displayed(err, part)), nil
	}
	if answerNamesNoBucket(int32(answer.Opcode), answer.Payload) {
		if hook := answer.TakeWrittenHook(); hook != nil {
			hook()
		}
		return errorFrame(frame, r.door.lostPart(r.t.database, r.t.name, part, false)), nil
	}
	if op != transport.OpGetFast || answer.Opcode == transport.OpError {
		answer.Payload = displayedAnswer(answer.Payload, part, r.t.name)
	}
	answer.RequestID = frame.RequestID
	return answer, nil
}

// unbornAnswer is a keyed operation's answer for a key whose part holds no
// document (R3): the part is unborn or only marked. It is the answer the
// collection would give for an absent key, built from the user's name and
// asking no node. A read is answered by the template itself, which holds no
// document but the born set, so its answer for any other key is exactly the
// miss a whole bucket gives; a write, and a read of the born set's own id,
// are answered from the miss texts the handlers use.
func (r *spreadRouter) unbornAnswer(ctx context.Context, frame *transport.Frame, key string) (*transport.Frame, error) {
	op := frame.Opcode
	if spreadClassOf(op) == spreadKeyedRead && op != transport.OpConsistentGet && key != bornDocumentID {
		return r.s.dispatch(ctx, frame)
	}
	switch op {
	case transport.OpGet:
		return respond(frame, transport.OpGetResponse, FindResponse{Found: false, Message: "document not found"})
	case transport.OpExists:
		return respond(frame, transport.OpExistsResponse, map[string]bool{"exists": false})
	case transport.OpGetFast:
		return transport.NewFrameWithID(transport.OpGetFastResponse, frame.RequestID,
			transport.MarshalGetFastResponse(transport.GetFastNotFound, nil)), nil
	case transport.OpConsistentGet:
		return respond(frame, transport.OpConsistentGet, ConsistentGetResponse{})
	case transport.OpDelete:
		return respond(frame, transport.OpDelete, deleteNotFound("document not found"))
	case transport.OpUpdate:
		return respond(frame, transport.OpUpdate, &UpdateResponse{Success: false, Message: fmt.Sprintf(
			"document %q not found in %s.%s — insert it first or use find to check the key", key, r.t.database, r.t.name)})
	}
	var field FieldRequest
	if err := bson.Unmarshal(frame.Payload, &field); err != nil {
		return errorFrame(frame, withOutcome(outcomeNotApplied, fmt.Errorf("decoding the %s request of bucket %q: %w", op, r.t.name, err))), nil
	}
	refusal := r.unbornFieldRefusal(op, field, key)
	if replicationRequired(op) {
		refusal = withOutcome(outcomeNotApplied, refusal)
	}
	return errorFrame(frame, refusal), nil
}

// unbornFieldRefusal is a field operation's answer for a document in an
// unborn part, as a whole bucket gives it for a missing document: the same
// request checks first (the element, the element key, the field's address
// against the collection's declaration, which the template holds), then the
// missing-parent refusal (requireParent's, whose action is "write" for every
// write).
func (r *spreadRouter) unbornFieldRefusal(op transport.Opcode, req FieldRequest, key string) error {
	verb, action := spreadFieldVerb(op)
	if op == transport.OpFieldPush {
		if _, _, err := fieldPushElement(req); err != nil {
			return err
		}
	}
	if err := fieldElementKeyRefusal(verb, req); err != nil {
		return err
	}
	bucket, unlock, err := r.s.lockBucket(r.t.database, r.t.name)
	if err != nil {
		return err
	}
	defer unlock()
	if _, err := resolveField(bucket, req, verb); err != nil {
		return err
	}
	return fmt.Errorf("%w: No document with key %q in bucket %q, so there is no %q store to %s. "+
		"A store field exists for each document, so the document has to exist first.\n"+
		"Run 'db.%s.find({_id_: %q})' to check the key",
		ErrFieldParentNotFound, key, r.t.name, req.Field, action, r.t.name, key)
}

// spreadFieldVerb is a field opcode's verb, as its handler resolves the
// field with it, and the action requireParent names: "write" for every
// write, "read" for a read.
func spreadFieldVerb(op transport.Opcode) (verb, action string) {
	switch op {
	case transport.OpFieldPush:
		return fieldVerbPush, "write"
	case transport.OpFieldPop:
		return fieldVerbPop, "write"
	case transport.OpFieldPut:
		return fieldVerbPut, "write"
	case transport.OpFieldDelete:
		return fieldVerbDelete, "write"
	case transport.OpFieldPeek:
		return fieldVerbPeek, "read"
	case transport.OpFieldLen:
		return fieldVerbLen, "read"
	case transport.OpFieldList:
		return fieldVerbList, "read"
	}
	return fieldVerbGet, "read"
}

// ensureCreated makes part p exist before the first write that creates it
// (section 2.2): mark it at 1, create it carrying the template's incarnation,
// mark it at 2. Each mark is idempotent ($max), so concurrent first writes to
// one part converge, and a later write re-runs whatever a failed one left.
func (r *spreadRouter) ensureCreated(ctx context.Context, p int) error {
	internal := routerInternalContext(ctx, r.t.key)
	if err := r.mark(internal, p, partMarked); err != nil {
		return err
	}
	if err := r.createPart(internal, p); err != nil {
		return err
	}
	return r.mark(internal, p, partCreated)
}

// mark raises part p to state in the born set, at majority, and requires a
// majority of the template's declared factor to confirm it (R2): with a
// replica stalled, majority counts the copies in sync, which can be the router
// alone. Short, the client's write is refused before its document is sent.
func (r *spreadRouter) mark(ctx context.Context, p int, state int32) error {
	update, err := r.templateFrame(transport.OpUpdate, &UpdateRequest{Database: r.t.database, Bucket: r.t.name, Key: bornDocumentID,
		Max: map[string]interface{}{bornField(p): state}, Admitted: true})
	if err != nil {
		return err
	}
	return r.writeBorn(ctx, update, func(confirmed, factor int, cause error) error {
		return markShortError(r.t.database, r.t.name, confirmed, factor, cause)
	})
}

// writeBorn applies one update of the born set at majority and requires
// QuorumSize of the template's declared factor to confirm it (R2); short is
// the refusal in the words of the caller's operation. A born set that does not
// exist yet (the collection's first mark) is made empty and the update
// applied again.
func (r *spreadRouter) writeBorn(ctx context.Context, update *transport.Frame, short func(confirmed, factor int, cause error) error) error {
	resp, confirmed, factor, err := r.applyMark(ctx, update)
	if err == nil && bornSetMissing(resp) {
		// The first mark of the collection: the born set is made empty, then
		// raised. An insert that finds it made by a concurrent first write is
		// as good.
		if err := r.createBornSet(ctx); err != nil {
			return err
		}
		resp, confirmed, factor, err = r.applyMark(ctx, update)
	}
	if err != nil {
		return short(confirmed, factor, err)
	}
	if transport.Opcode(resp.Opcode) == transport.OpError || !replySucceeded(transport.Opcode(resp.Opcode), resp.Payload) {
		return short(0, factor, errors.New(answerMessage(resp.Payload)))
	}
	if confirmed < QuorumSize(factor) {
		return short(confirmed, factor, nil)
	}
	return nil
}

// applyMark prepares and applies one born-set write.
func (r *spreadRouter) applyMark(ctx context.Context, frame *transport.Frame) (ReplResponse, int, int, error) {
	entry, err := r.door.prepare(ctx, frame, r.t.name)
	if err != nil {
		return ReplResponse{}, 0, 0, err
	}
	return r.door.markTemplate(ctx, entry, r.t.key)
}

// createBornSet inserts the empty born set; one that exists already is kept.
func (r *spreadRouter) createBornSet(ctx context.Context) error {
	insert, err := r.templateFrame(transport.OpPut, &InsertRequest{Database: r.t.database, Bucket: r.t.name, Key: bornDocumentID,
		Data: map[string]interface{}{"_id_": bornDocumentID}, Admitted: true})
	if err != nil {
		return err
	}
	resp, _, factor, err := r.applyMark(ctx, insert)
	if err != nil {
		return markShortError(r.t.database, r.t.name, 0, factor, err)
	}
	if transport.Opcode(resp.Opcode) == transport.OpError {
		var refusal ErrorResponse
		if bson.Unmarshal(resp.Payload, &refusal) == nil && transport.ErrorCode(refusal.Code) == transport.ErrDocumentExists {
			return nil
		}
		return markShortError(r.t.database, r.t.name, 0, factor, errors.New(answerMessage(resp.Payload)))
	}
	return nil
}

// bornSetMissing reports whether a mark found no born set to raise.
func bornSetMissing(resp ReplResponse) bool {
	if transport.Opcode(resp.Opcode) != transport.OpUpdate {
		return false
	}
	var answer UpdateResponse
	if err := bson.Unmarshal(resp.Payload, &answer); err != nil {
		return false
	}
	return !answer.Success && strings.Contains(answer.Message, "not found in")
}

// answerMessage is the message of an answer that refused, for a refusal that
// quotes it.
func answerMessage(payload []byte) string {
	value, err := bson.Raw(payload).LookupErr("message")
	if err != nil {
		return "the request was refused"
	}
	if text, ok := value.StringValueOK(); ok {
		return text
	}
	return "the request was refused"
}

// templateFrame encodes one of the router's own requests.
func (r *spreadRouter) templateFrame(op transport.Opcode, request any) (*transport.Frame, error) {
	payload, err := bson.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("encoding the %s the router of bucket %q sends: %w", op, r.t.name, err)
	}
	return transport.NewFrame(op, payload), nil
}

// createPart creates part p with the template's declaration, carrying its
// incarnation as part_of. A part the owner already holds is adopted only when
// its part_of is this incarnation's (B4.2). Any other is an orphan of a
// dropped or lost incarnation: it is dropped through its owner, an ordinary
// logged drop, and created again, once.
func (r *spreadRouter) createPart(ctx context.Context, p int) error {
	part := partName(r.t.name, p)
	create, err := r.templateFrame(transport.OpCreateBucket, r.partCreateRequest(p))
	if err != nil {
		return withOutcome(outcomeNotApplied, err)
	}
	for attempt := 0; ; attempt++ {
		orphanOf, err := r.createOnce(ctx, create, part)
		if err != nil {
			return err
		}
		if orphanOf == 0 {
			return nil
		}
		if attempt > 0 {
			return withOutcome(outcomeNotApplied, fmt.Errorf("part of bucket %q (database %q) is still held from incarnation %d after it was dropped; nothing was stored, retry the write",
				r.t.name, r.t.database, orphanOf))
		}
		if err := r.dropOrphan(ctx, part); err != nil {
			return err
		}
	}
}

// createOnce sends one create of part and says which incarnation an orphan it
// met belongs to, 0 when the part is created or adopted.
func (r *spreadRouter) createOnce(ctx context.Context, create *transport.Frame, part string) (orphanOf uint64, err error) {
	recorded, known, err := r.door.recordedPartOf(r.t.database, part)
	if err != nil {
		return 0, withOutcome(outcomeNotApplied, err)
	}
	if known && recorded != 0 && recorded != r.t.incarnation {
		return recorded, nil
	}
	entry, err := r.door.prepare(ctx, create, part)
	if err != nil {
		return 0, err
	}
	resp, err := r.door.writePart(ctx, r.t.database, entry, WcMajority)
	if err != nil {
		return 0, withOutcome(outcomeNotApplied, r.displayed(fmt.Errorf("creating a part of bucket %q (database %q) for this document: %w; nothing was stored, retry the write",
			r.t.name, r.t.database, err), part))
	}
	var answer partCreateAnswer
	if transport.Opcode(resp.Opcode) != transport.OpError {
		if err := bson.Unmarshal(resp.Payload, &answer); err != nil {
			return 0, withOutcome(outcomeNotApplied, fmt.Errorf("decoding the create of a part of bucket %q: %w", r.t.name, err))
		}
	}
	switch {
	case answer.Success:
		return 0, nil
	case answer.PartOf != 0 && answer.PartOf != r.t.incarnation:
		return answer.PartOf, nil
	}
	return 0, withOutcome(outcomeNotApplied, r.displayed(fmt.Errorf("creating a part of bucket %q (database %q) for this document was refused: %s; nothing was stored",
		r.t.name, r.t.database, answerMessage(resp.Payload)), part))
}

// dropOrphan drops a part of an earlier incarnation through its owner.
func (r *spreadRouter) dropOrphan(ctx context.Context, part string) error {
	if err := r.dropPartBucket(ctx, part); err != nil {
		return withOutcome(outcomeNotApplied, r.displayed(fmt.Errorf("dropping a part of bucket %q (database %q) left from an earlier incarnation: %w; nothing was stored, retry the write",
			r.t.name, r.t.database, err), part))
	}
	return nil
}

// partCreateRequest is the create of part p: the template's declaration
// (typed fields, store fields, indexes, factor) in the wire form a client's
// create carries, so the logged create replicates and replays as any create
// does, plus part and part_of.
func (r *spreadRouter) partCreateRequest(p int) CreateBucketRequest {
	entry := r.t.entry
	options := map[string]interface{}{"placement": placementWhole}
	if len(entry.Indices) > 0 {
		specs := make([]DumpIndexSpec, 0, len(entry.Indices))
		for position, recorded := range entry.Indices {
			spec := indexSpecOf(position, recorded)
			if len(usableIndexSpecs([]store.IndexSpec{spec})) == 0 {
				continue
			}
			carried := DumpIndexSpec{ID: int32(spec.ID), Name: spec.Name, Fields: spec.FieldNames(), Unique: spec.Unique}
			if spec.TTL != nil {
				expireAfter := spec.TTL.ExpireAfterSeconds
				carried.ExpireAfterSeconds = &expireAfter
			}
			specs = append(specs, carried)
		}
		for key, value := range restoreIndexOptions(nil, specs) {
			options[key] = value
		}
	}
	if len(entry.Stores) > 0 {
		names := make(map[int32]string, len(entry.Stores))
		for _, recorded := range entry.Stores {
			names[recorded.ID] = recorded.Name
		}
		stores := make([]DumpStore, 0, len(entry.Stores))
		for _, recorded := range entry.Stores {
			stores = append(stores, DumpStore{Name: recorded.Name, Engine: recorded.Engine, Parent: names[recorded.Parent]})
		}
		options["stores"] = restoreStoreDeclarations(stores, "")
	}
	if fields := declarationsOf(readableFields(entry.Fields)); len(fields) > 0 || entry.FieldTypes != "" {
		options["fields"] = fields
		if entry.FieldTypes != "" {
			options["field_types"] = entry.FieldTypes
		}
	}
	return CreateBucketRequest{Database: r.t.database, Name: partName(r.t.name, p), Type: BucketTypeHash, Options: options,
		ReplicationFactor: int(entry.ReplicationFactor), Part: int32(p), PartOf: r.t.incarnation}
}
