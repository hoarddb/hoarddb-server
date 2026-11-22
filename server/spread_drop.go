package server

import (
	"context"
	"errors"
	"fmt"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// The drop of a spread collection (document placement spread MVP note, section
// 2.3, B4.3 to B4.5), and the refusals of the declaration changes after create.
//
// Order, under the router gate taken exclusively (spread_gate.go):
//
//  1. `dropping: true` in the born set, at majority, confirmed by a quorum of
//     the template's declared factor (R2). From then on every operation at this
//     router or a later one is refused with the retry line.
//  2. Every part at 1 or 2 in the born set is dropped through its owner, an
//     ordinary logged drop with its record. A part its owner does not hold
//     counts as dropped.
//  3. The template is dropped, sent to every member (Cluster.addressing).
//
// The template's drop is sent only after every part answered dropped or not
// found, and nothing before it removes or rewrites the born set (S4-1): a later
// build that seals the born set destroys the collection's key in the template's
// drop, so a drop that stops at 2 must leave the born set readable for the
// resume. A drop refused partway leaves the collection `dropping`, unreadable
// and unwritable as a whole, and a second drop resumes at 2.
//
// A release after a migration (Cluster.releaseOutsideSet -> Server.DeleteBucket,
// no drop entry in the context) never comes through here: it is a node deleting
// its own copy, not a client's drop of the name, so it neither marks nor drops
// the parts (S4-2). The drop of a part carries no template-level stamp (S4-3):
// only the template's payload is stamped, in Cluster.stampDropIncarnation.
//
// Cost (rule 11): O(parts created) <= K = 256 logged drops and one mark, in the
// collection's own time; nothing per bucket of the node, no goroutine, no file
// descriptor, no per-bucket metric.

// holdGate takes the template's gate for one operation and checks, once held,
// that the template is still the one this operation loaded: the router read its
// catalog entry before the gate, so a drop and a create of the same name can
// have landed while it waited. Held shared or exclusively, no drop can land
// until it is released, so the answer holds for the whole operation. Refused,
// the gate is not held. An item of a BATCH that holds the template's gate
// already (batchGateHeld) takes none of its own, since a second shared hold
// behind a queued exclusive one would wait for the first (never re-entered),
// but is still checked against the template it loaded.
func (r *spreadRouter) holdGate(ctx context.Context, exclusive bool, op transport.Opcode) (release func(), err error) {
	release = func() {}
	if !batchGateHeld(ctx, r.t.key) {
		release = r.s.holdRouterGate(r.t.key, exclusive)
	}
	if err := r.templateUnchanged(op); err != nil {
		release()
		return nil, err
	}
	return release, nil
}

// templateUnchanged is the answer to "is this still the template I loaded?":
// not found once it was dropped, retryable once the name was created again.
func (r *spreadRouter) templateUnchanged(op transport.Opcode) error {
	if !r.s.spreadTemplateHeld(r.t.database, r.t.name) {
		if !r.s.bucketStoreOpen(r.t.database, r.t.name) {
			return bucketNotFoundErr(r.t.database, r.t.name)
		}
		return r.recreatedError(op)
	}
	state, err := r.s.bucketStateNamed(r.t.database, r.t.name)
	if err != nil {
		return withOutcome(outcomeNotApplied, fmt.Errorf("reading the incarnation of bucket %q in database %q: %w", r.t.name, r.t.database, err))
	}
	if state.Incarnation != r.t.incarnation {
		return r.recreatedError(op)
	}
	return nil
}

// recreatedError is the refusal of an operation that waited for the gate while
// its collection was dropped and created again.
func (r *spreadRouter) recreatedError(op transport.Opcode) error {
	err := &sentencedError{cause: ErrSpreadRecreated, message: fmt.Sprintf(
		"Bucket %q (database %q) was dropped and created again while this %s waited; nothing was changed. Retry it.",
		r.t.name, r.t.database, spreadOpVerb(op))}
	if replicationRequired(op) {
		return withOutcome(outcomeNotApplied, err)
	}
	return err
}

// dropCollection is the router's drop of the collection: entry is the client's
// drop of the template's name.
func (r *spreadRouter) dropCollection(ctx context.Context, entry ReplEntry, wc WriteConcernLevel) (ReplResponse, error) {
	release, err := r.holdGate(ctx, true, transport.OpDeleteBucket)
	if err != nil {
		return ReplResponse{}, err
	}
	defer release()
	if err := r.door.awaitsTakeOver(r.t.database, r.t.name, r.t.key); err != nil {
		return ReplResponse{}, err
	}
	born, err := r.s.readBornSet(r.t.database, r.t.name)
	if err != nil {
		return ReplResponse{}, err
	}
	r.endRunningDump()
	internal := routerInternalContext(ctx, r.t.key)
	// A collection with no born set was never written to: it has no part and
	// nothing to hide, and the gate keeps a first write out until the template
	// is gone.
	if born.raw != nil {
		if err := r.markDropping(internal); err != nil {
			return ReplResponse{}, err
		}
	}
	for p := 0; p < r.t.parts; p++ {
		if born.state(p) == 0 {
			continue
		}
		if err := r.dropPart(internal, p); err != nil {
			return ReplResponse{}, r.dropRetryError(err)
		}
	}
	resp, err := r.door.dropTemplate(internal, entry, r.t.key, wc)
	if err != nil {
		return ReplResponse{}, r.dropRetryError(err)
	}
	if refusal := dropRefusal(resp); refusal != nil {
		return ReplResponse{}, r.dropRetryError(refusal)
	}
	return resp, nil
}

// dropRefusal is why a drop's answer is not a success, or nil.
func dropRefusal(resp ReplResponse) error {
	if transport.Opcode(resp.Opcode) == transport.OpError {
		return errors.New(answerMessage(resp.Payload))
	}
	var answer DeleteBucketResponse
	if err := bson.Unmarshal(resp.Payload, &answer); err != nil {
		return fmt.Errorf("decoding the answer to a drop: %w", err)
	}
	if !answer.Success {
		return errors.New(answer.Message)
	}
	return nil
}

// markDropping writes `dropping: true` into the born set, confirmed by a
// quorum of the template's declared factor. A mark that falls short is a
// refused drop: nothing has been dropped, and the retry is safe.
func (r *spreadRouter) markDropping(ctx context.Context) error {
	update, err := r.templateFrame(transport.OpUpdate, &UpdateRequest{Database: r.t.database, Bucket: r.t.name, Key: bornDocumentID,
		Set: map[string]interface{}{bornDroppingField: true}, Admitted: true})
	if err != nil {
		return err
	}
	return r.writeBorn(ctx, update, func(confirmed, factor int, cause error) error {
		message := fmt.Sprintf("Bucket %q (database %q) could not record that it is being dropped on enough copies (%d of %d confirmed); nothing was dropped. "+
			"Retry once every node that holds the bucket is back; 'nodes' shows their state.", r.t.name, r.t.database, confirmed, factor)
		if cause != nil {
			message += " (" + explainError(cause) + ")"
		}
		return &sentencedError{cause: errors.Join(ErrSpreadMarkShort, ErrSpreadDropping), message: message}
	})
}

// dropPart drops part p through its owner.
func (r *spreadRouter) dropPart(ctx context.Context, p int) error {
	part := partName(r.t.name, p)
	if err := r.dropPartBucket(ctx, part); err != nil {
		return r.displayed(fmt.Errorf("dropping a part of bucket %q (database %q): %w", r.t.name, r.t.database, err), part)
	}
	return nil
}

// dropPartBucket drops the named part through its owner, an ordinary logged
// drop. A part its owner does not hold counts as dropped: it was marked and
// never created, or an earlier drop got there.
func (r *spreadRouter) dropPartBucket(ctx context.Context, part string) error {
	drop, err := r.templateFrame(transport.OpDeleteBucket, DeleteBucketRequest{Database: r.t.database, Name: part})
	if err != nil {
		return err
	}
	entry, err := r.door.prepare(ctx, drop, part)
	if err != nil {
		return err
	}
	resp, err := r.door.writePart(ctx, r.t.database, entry, WcMajority)
	if err == nil && answerNamesNoBucket(resp.Opcode, resp.Payload) {
		return nil
	}
	if err == nil {
		err = dropRefusal(resp)
	}
	if errors.Is(err, ErrBucketNotFound) {
		return nil
	}
	return err
}

// dropRetryError is the refusal of a drop that stopped partway: the retry
// line, with the reason kept for errors.Is and the node's log. The collection
// stays `dropping`.
func (r *spreadRouter) dropRetryError(cause error) error {
	r.s.logger.Warn("the drop of a spread collection stopped partway; it stays dropping until a second drop finishes it",
		"database", r.t.database, "bucket", r.t.name, "error", cause)
	return &sentencedError{cause: errors.Join(ErrSpreadDropping, cause), message: fmt.Sprintf(
		"Bucket %q (database %q) is being dropped. Run 'drop bucket %s in %s' again to finish it.",
		r.t.name, r.t.database, r.t.name, r.t.database)}
}

// endRunningDump ends the collection's running dump at this router: its parts
// are about to go.
func (r *spreadRouter) endRunningDump() {
	running := r.s.spreadDumps.get(r.t.key)
	if running == nil {
		return
	}
	running.mu.Lock()
	defer running.mu.Unlock()
	r.endDump(running)
}

// declineDDL answers a declaration change after create (section 2.4): refused
// with the way to do it, except a declare of store fields the collection
// already has, which is an identity as it is on a whole bucket (and is what a
// restore of a dump with store fields does to an existing bucket).
func (r *spreadRouter) declineDDL(entry ReplEntry) (ReplResponse, error) {
	op := transport.Opcode(entry.Op)
	if op == transport.OpDeclareStoreFields {
		identity, conflict := r.declaresNothingNew(entry.Payload)
		if conflict != nil {
			return ReplResponse{}, withOutcome(outcomeNotApplied, conflict)
		}
		if identity {
			return r.declaredAlready(op)
		}
	}
	return ReplResponse{}, withOutcome(outcomeNotApplied, spreadRefusal(op, spreadDDL, r.t.database, r.t.name))
}

// declaredAlready is the answer to a declare of store fields the collection
// has.
func (r *spreadRouter) declaredAlready(op transport.Opcode) (ReplResponse, error) {
	payload, err := bson.Marshal(DeclareStoreFieldsResponse{Success: true,
		Message: fmt.Sprintf("bucket %q already declares these store fields", r.t.name)})
	if err != nil {
		return ReplResponse{}, withOutcome(outcomeNotApplied, fmt.Errorf("encoding the declare store fields response: %w", err))
	}
	return ReplResponse{Opcode: int32(op), Payload: payload}, nil
}

// declaresNothingNew reports whether a declare names only store fields the
// template already declares, with the same engines. A declaration the
// ordinary handler would refuse, another engine for a field it has, comes
// back as that refusal.
func (r *spreadRouter) declaresNothingNew(payload []byte) (identity bool, conflict error) {
	var req DeclareStoreFieldsRequest
	if err := bson.Unmarshal(payload, &req); err != nil {
		return false, nil
	}
	entries := make([]interface{}, 0, len(req.Stores))
	for _, declared := range req.Stores {
		entries = append(entries, declared)
	}
	added, err := parseDeclarationList(entries)
	if err != nil {
		return false, nil
	}
	bucket, unlock, err := r.s.lockBucket(r.t.database, r.t.name)
	if err != nil {
		return false, nil
	}
	defer unlock()
	current := bucket.storeFields()
	if err := ValidateStoreDeclarations(StoreValidation{
		Database: r.t.database, Bucket: r.t.name, BucketType: bucket.bucketType, Existing: current.stores, Added: added,
	}); err != nil {
		return false, err
	}
	grown, err := current.withDeclared(added)
	return err == nil && len(grown.stores) == len(current.stores), nil
}
