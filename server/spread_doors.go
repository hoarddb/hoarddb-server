package server

import (
	"context"
	"errors"
	"fmt"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// The doors of the router (spread MVP note, section 2.1): where a client's
// operation on a spread template is handed to the router instead of being
// applied to, or answered from, the template's store.

// clusterSpreadDoor is the router's door on a node that routes: the router
// is the template's owner, and parts are written through Cluster.write and
// read from their owners.
type clusterSpreadDoor struct {
	c *Cluster
}

func (d clusterSpreadDoor) prepare(ctx context.Context, frame *transport.Frame, bucket string) (ReplEntry, error) {
	return d.c.prepareClientWrite(ctx, frame, bucket, decodeFrameMeta(frame.Payload))
}

// markTemplate applies the mark as the template's owner, which the router
// is: fenceAsSettledOwner refuses it as not owner once a membership change
// has moved the template, so a router that lost the template marks nothing.
func (d clusterSpreadDoor) markTemplate(ctx context.Context, entry ReplEntry, template placementKey) (ReplResponse, int, int, error) {
	resp, err := d.c.applyAsOwner(ctx, entry, template, WcMajority)
	factor := d.c.effectiveRF(template)
	if err != nil {
		var outcome *writeOutcomeError
		confirmed := 0
		if errors.As(err, &outcome) {
			confirmed = outcome.confirmed
		}
		return resp, confirmed, factor, err
	}
	return resp, markCopiesConfirmed(resp.Payload), factor, nil
}

// dropTemplate drops the template as its owner, which the router is, the gate
// held in ctx: the drop is sent to every member, one holding only parts of the
// collection included (Cluster.addressing).
func (d clusterSpreadDoor) dropTemplate(ctx context.Context, entry ReplEntry, template placementKey, wc WriteConcernLevel) (ReplResponse, error) {
	return d.c.applyAsOwner(ctx, entry, template, wc)
}

func (d clusterSpreadDoor) writePart(ctx context.Context, database string, entry ReplEntry, wc WriteConcernLevel) (ReplResponse, error) {
	return d.c.write(ctx, entry, placementOf(database, entry.Bucket), wc)
}

// readPart asks the part's owner, never a replica: a replica that has not
// applied the part's create yet would answer the part not found, and a
// created part with no holder is a lost part. On this node the owner's own
// path answers it (serveRead's owner branch, with its take-over checks).
func (d clusterSpreadDoor) readPart(ctx context.Context, frame *transport.Frame, database, part string) (*transport.Frame, error) {
	owner, err := d.c.Owner(placementOf(database, part))
	if err != nil {
		return nil, err
	}
	if owner.ID == d.c.self.ID {
		return d.c.serveRead(ctx, frame, part, database)
	}
	return d.c.forwardRead(ctx, frame, owner, part, clientReadForward)
}

// recordedPartOf is never known here: the part's owner judges an existing
// part's part_of inside its fence (Cluster.applyOwned).
func (d clusterSpreadDoor) recordedPartOf(string, string) (uint64, bool, error) {
	return 0, false, nil
}

// awaitsTakeOver is fenceAsSettledOwner's question asked of the template at
// the router: a template it owns on the ring and has not taken over is
// answered with the hand-off, and the take-over is woken for it.
func (d clusterSpreadDoor) awaitsTakeOver(database, template string, key placementKey) error {
	awaits, err := d.c.ownerAwaitsTakeOver(key, d.c.Epoch(), database, template)
	if err != nil {
		return withOutcome(outcomeNotApplied, fmt.Errorf("reading whether bucket %q in database %q is taken over: %w", template, database, err))
	}
	if !awaits {
		return nil
	}
	d.c.wakeTakeOver(database, template)
	return withOutcome(outcomeNotApplied, d.c.handoffInProgress(database, template))
}

func (d clusterSpreadDoor) lostPart(database, bucket, part string, write bool) error {
	holder := "the node that owns it"
	if owner, err := d.c.Owner(placementOf(database, part)); err == nil {
		holder = owner.ID
	}
	return lostPartError(database, bucket, holder, write)
}

// markCopiesConfirmed reads a data write's copies_confirmed.
func markCopiesConfirmed(payload []byte) int {
	value, err := bson.Raw(payload).LookupErr("copies_confirmed")
	if err != nil {
		return 0
	}
	number, _ := value.AsInt64OK()
	return int(number)
}

// localSpreadDoor is the router's door on a node that does not route: one with
// no cluster (zero config), or alone on its ring. Every part is on this node
// and is written through Server.fencedDispatch, which takes the part's fence
// (the router holds none), and read through Server.dispatchInFence's read
// door. A mark is confirmed once its local apply is committed: there is no
// other copy to wait for (declared factor 1 on a node with no cluster; a node
// alone on its ring holds the only copy), and the client's answer is synced
// with it, under the frame's one acknowledgeDurably.
type localSpreadDoor struct {
	s *Server
}

func (d localSpreadDoor) prepare(_ context.Context, frame *transport.Frame, bucket string) (ReplEntry, error) {
	return ReplEntry{Op: int32(frame.Opcode), Bucket: bucket, Payload: frame.Payload}, nil
}

func (d localSpreadDoor) markTemplate(ctx context.Context, entry ReplEntry, _ placementKey) (ReplResponse, int, int, error) {
	reply, err := d.s.fencedDispatch(ctx, transport.NewFrame(transport.Opcode(entry.Op), entry.Payload))
	if err != nil {
		return ReplResponse{}, 0, 1, err
	}
	return ReplResponse{Opcode: int32(reply.Opcode), Payload: reply.Payload}, 1, 1, nil
}

func (d localSpreadDoor) dropTemplate(ctx context.Context, entry ReplEntry, _ placementKey, _ WriteConcernLevel) (ReplResponse, error) {
	reply, err := d.s.fencedDispatch(ctx, transport.NewFrame(transport.Opcode(entry.Op), entry.Payload))
	if err != nil {
		return ReplResponse{}, err
	}
	return ReplResponse{Opcode: int32(reply.Opcode), Payload: reply.Payload}, nil
}

func (d localSpreadDoor) writePart(ctx context.Context, _ string, entry ReplEntry, _ WriteConcernLevel) (ReplResponse, error) {
	reply, err := d.s.fencedDispatch(ctx, transport.NewFrame(transport.Opcode(entry.Op), entry.Payload))
	if err != nil {
		return ReplResponse{}, err
	}
	return ReplResponse{Opcode: int32(reply.Opcode), Payload: reply.Payload}, nil
}

func (d localSpreadDoor) readPart(ctx context.Context, frame *transport.Frame, _, _ string) (*transport.Frame, error) {
	return d.s.dispatchInFence(ctx, frame)
}

// recordedPartOf reads a local part's part_of, which no owner precheck reads
// at this door: an existing part of another incarnation is an orphan.
func (d localSpreadDoor) recordedPartOf(database, part string) (uint64, bool, error) {
	if !d.s.bucketStoreOpen(database, part) {
		return 0, true, nil
	}
	_, partOf, err := d.s.partFieldsOf(database, part)
	return partOf, true, err
}

// lostPart at the one-member door: every part is on this node, so there is no
// other node to bring back.
func (d localSpreadDoor) lostPart(database, bucket, _ string, write bool) error {
	nothing := "nothing was returned"
	if write {
		nothing = "nothing was stored"
	}
	return withOutcome(outcomeNotApplied, &sentencedError{cause: ErrSpreadPartLost, message: fmt.Sprintf(
		"Some documents of bucket %q (database %q) are in a part that is not held on this node, the only node that holds the bucket; %s. "+
			"They cannot be read from another node: restore them from a dump of the bucket taken before they were lost.",
		bucket, database, nothing)})
}

// awaitsTakeOver at the one-member door: only a node a removal left alone can
// own a template it has not taken over; a node with no cluster, or always
// alone, holds the only copy.
func (d localSpreadDoor) awaitsTakeOver(database, template string, key placementKey) error {
	if d.s.cluster == nil || !d.s.cluster.aloneAfterARemoval() {
		return nil
	}
	return clusterSpreadDoor{c: d.s.cluster}.awaitsTakeOver(database, template, key)
}

// routeSpreadWrite is the router's write door on a node that routes, at the
// top of Cluster.applyAsOwner: both of its callers (Cluster.write and
// Cluster.serveForwardedEntry) reach it as the template's owner, with no fence
// held and nothing logged. routed is false for anything that is not a client's
// operation on a spread template held here: a part, any other bucket, the
// router's own write to the template (its gate is held), and a create of the
// template's name.
func (c *Cluster) routeSpreadWrite(ctx context.Context, entry ReplEntry, key placementKey, wc WriteConcernLevel) (resp ReplResponse, routed bool, err error) {
	if entry.Bucket == "" || hasPartSeparator(entry.Bucket) || routerGateHeld(ctx, key) {
		return ReplResponse{}, false, nil
	}
	op := transport.Opcode(entry.Op)
	if spreadClassOf(op) == spreadPassThrough {
		return ReplResponse{}, false, nil
	}
	_, database := decodeFrameMeta(entry.Payload).scope(op)
	if !c.s.spreadTemplateHeld(database, entry.Bucket) {
		return ReplResponse{}, false, nil
	}
	template, err := c.s.loadSpreadTemplate(database, entry.Bucket)
	if err != nil {
		return ReplResponse{}, true, withOutcome(outcomeNotApplied, err)
	}
	router := spreadRouter{s: c.s, door: clusterSpreadDoor{c: c}, t: template}
	resp, err = router.routeWrite(ctx, entry, wc)
	if err != nil {
		return refusedResponse(op, err), true, nil
	}
	return resp, true, nil
}

// refusedResponse is the router's refusal as the answer frame an owner's
// handler gives, so its code, its text and its write outcome survive a
// forward to the coordinator unchanged (a Go error is relayed as the owner's
// text under INTERNAL). An error that says nothing of its outcome is unknown,
// as Cluster.handleClientFrame answers it.
func refusedResponse(op transport.Opcode, err error) ReplResponse {
	var outcome *writeOutcomeError
	if !errors.As(err, &outcome) {
		err = withOutcome(outcomeUnknown, err)
	}
	frame := errorFrame(&transport.Frame{Opcode: op}, err)
	return ReplResponse{Opcode: int32(frame.Opcode), Payload: frame.Payload}
}

// routeSpreadRead is the router's read door on a node that routes: serveRead's
// owner branch and a forwarded read this node answers, in place of the local
// dispatch, once the template's take-over checks have passed.
func (c *Cluster) routeSpreadRead(ctx context.Context, frame *transport.Frame, database, bucket string) (*transport.Frame, error) {
	template, err := c.s.loadSpreadTemplate(database, bucket)
	if err != nil {
		return errorFrame(frame, err), nil
	}
	router := spreadRouter{s: c.s, door: clusterSpreadDoor{c: c}, t: template}
	return router.routeRead(ctx, frame)
}

// routeSpreadAtOneMember is the router at the one-member door
// (Server.dispatchInFence): a node with no cluster, or alone on its ring. A
// write comes here after admitAtOneMemberDoor and before the fence, its ids
// minted as Cluster.prepareClientWrite mints them; a read comes here in place
// of the local dispatch. routed is false for anything that is not a client's
// operation on a spread template held here.
func (s *Server) routeSpreadAtOneMember(ctx context.Context, frame *transport.Frame, database, bucket string) (answer *transport.Frame, routed bool) {
	if !s.spreadTemplateHeld(database, bucket) || spreadClassOf(frame.Opcode) == spreadPassThrough {
		return nil, false
	}
	if routerGateHeld(ctx, placementOf(database, bucket)) {
		return nil, false
	}
	template, err := s.loadSpreadTemplate(database, bucket)
	if err != nil {
		return errorFrame(frame, withOutcome(outcomeNotApplied, err)), true
	}
	router := spreadRouter{s: s, door: localSpreadDoor{s: s}, t: template}
	if !replicationRequired(frame.Opcode) {
		reply, err := router.routeRead(ctx, frame)
		if err != nil {
			return errorFrame(frame, err), true
		}
		return reply, true
	}
	payload := frame.Payload
	resolved, err := resolveDocumentIDs(frame.Opcode, payload)
	if err != nil {
		return errorFrame(frame, withOutcome(outcomeNotApplied, err)), true
	}
	if resolved != nil {
		payload = resolved
	}
	resp, err := router.routeWrite(ctx, ReplEntry{Op: int32(frame.Opcode), Bucket: bucket, Payload: payload}, 0)
	if err != nil {
		return errorFrame(frame, err), true
	}
	return transport.NewFrameWithID(transport.Opcode(resp.Opcode), frame.RequestID, resp.Payload), true
}

// dispatchRoutingSpread is Server.dispatch for a read at the one-member door,
// with a spread template's read routed instead.
func (s *Server) dispatchRoutingSpread(ctx context.Context, frame *transport.Frame, meta frameMeta) (*transport.Frame, error) {
	if bucket, database, err := scopeOfFrame(frame, meta); err == nil && bucket != "" {
		if answer, routed := s.routeSpreadAtOneMember(ctx, frame, database, bucket); routed {
			return answer, nil
		}
	}
	return s.dispatch(ctx, frame)
}

// partCreateMeetsOrphan is the owner's precheck of the router's create of a
// part it already holds (B4.2): the part_of the held part records, when the
// create carries another; 0 when the create is not a part's, or the part
// belongs to the create's incarnation and is adopted.
func (s *Server) partCreateMeetsOrphan(database string, entry ReplEntry) (uint64, error) {
	if !hasPartSeparator(entry.Bucket) {
		return 0, nil
	}
	var create struct {
		PartOf uint64 `bson:"part_of"`
	}
	if err := bson.Unmarshal(entry.Payload, &create); err != nil {
		return 0, fmt.Errorf("decoding the create of a part of bucket %q: %w", displayBucket(entry.Bucket), err)
	}
	if create.PartOf == 0 {
		return 0, nil
	}
	_, recorded, err := s.partFieldsOf(database, entry.Bucket)
	if err != nil {
		return 0, err
	}
	if recorded == create.PartOf {
		return 0, nil
	}
	return recorded, nil
}

// partCreateAnswer is the answer to the router's create of a part: a
// CreateBucketResponse's success and message, plus, when the create met a
// part held from another incarnation, that incarnation. It is the router's
// alone, so CreateBucketResponse, which every client reads, gains nothing.
type partCreateAnswer struct {
	Success bool   `bson:"success"`
	Message string `bson:"message,omitempty"`
	PartOf  uint64 `bson:"part_of,omitempty"`
}

// orphanPartAnswer is the create's answer for a part held from another
// incarnation: refused, nothing applied, with that incarnation, which the
// router reads to drop the orphan and create the part again.
func orphanPartAnswer(part string, orphanOf uint64) (ReplResponse, error) {
	payload, err := bson.Marshal(partCreateAnswer{Success: false, PartOf: orphanOf, Message: fmt.Sprintf(
		"a part of bucket %q is held from incarnation %d, an earlier one; it is not adopted", displayBucket(part), orphanOf)})
	if err != nil {
		return ReplResponse{}, fmt.Errorf("encoding the answer to a part's create: %w", err)
	}
	return ReplResponse{Opcode: int32(transport.OpCreateBucket), Payload: payload}, nil
}

// ErrSpreadRecreated is under the refusal of a client's write that passed the
// router's check while its bucket was whole and reached the bucket's fence
// after it was dropped and created again spread: nothing was stored, and a
// retry is routed.
var ErrSpreadRecreated = errors.New("the bucket was created again spread while the write waited")

// spreadTemplateUnderFence is the doors' re-check inside the bucket's fence
// (Cluster.applyOwned, Server.dispatchInFence): the router's own check ran
// before the fence, so a drop and a spread create of the name can land between
// the two. A client's operation that would now meet a spread template is
// refused, retryable and not applied; the router's own template writes carry
// its gate and pass, and so does a create of the name. Under the fence a drop
// cannot land, so the answer holds until the apply. One registry lookup.
func (s *Server) spreadTemplateUnderFence(ctx context.Context, op transport.Opcode, database, bucket string, key placementKey) error {
	if bucket == "" || spreadClassOf(op) == spreadPassThrough || routerGateHeld(ctx, key) || !s.spreadTemplateHeld(database, bucket) {
		return nil
	}
	return withOutcome(outcomeNotApplied, &sentencedError{cause: ErrSpreadRecreated, message: fmt.Sprintf(
		"Bucket %q (database %q) was dropped and created again as a spread bucket while this %s waited; nothing was stored. Retry it.",
		bucket, database, op)})
}
