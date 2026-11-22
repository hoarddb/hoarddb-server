package server

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/ids"
	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The replicated write path.
//
//	A client mutation is handled by whichever node it lands on ("the
//	coordinator"). The coordinator forwards it to the bucket's owner if it is
//	not itself the owner. The owner appends the entry to its WAL, applies it
//	locally, ships it to the remaining replicas, and only then answers the
//	client according to the write concern.
//
// Nothing here acknowledges a write that the required number of replicas did
// not confirm. That is the whole point of the rewrite: the previous code path
// marked entries as acknowledged on the leader without sending them anywhere.

var (
	// ErrWriteConcernTimeout means the requested number of copies did not
	// confirm the write in time. The write may still be present on some
	// replicas; the client is told it did not reach the required number.
	ErrWriteConcernTimeout = errors.New("write concern not satisfied")
	// ErrMemberMismatch means this node and a peer disagree about cluster
	// membership, so they would disagree about placement too. Writes fail
	// closed rather than being scattered across inconsistent replica sets.
	ErrMemberMismatch = errors.New("cluster membership disagrees with a peer")
	// errMembershipDiverged is a member set that differs from a member's at
	// one epoch, which, with one set agreed per epoch, comes only from a node
	// that diverged on an older build or from a configuration error.
	errMembershipDiverged = fmt.Errorf("%w: a member holds another member set at this node's epoch. "+
		"Run 'nodes history' against every member and follow the procedure for a diverged cluster in the operator guide "+
		"on adding and removing nodes", ErrMemberMismatch)
	// ErrOwnerMembershipMismatch means a forwarded request reached an owner
	// whose membership disagrees with a peer's, so it applied nothing (BUG-0704).
	// The owner's disagreement clears by itself, so it reaches the wire as
	// UNAVAILABLE, a retry. It does not wrap ErrMemberMismatch, which is
	// CONFLICT: that is this node's own disagreement or a refused membership
	// change, which a retry does not clear.
	ErrOwnerMembershipMismatch = errors.New("the bucket's owner refused the request while its cluster membership disagrees with a peer's")
	// ErrNoWAL means replication was enabled without a usable WAL.
	ErrNoWAL = errors.New("replication requires an open WAL")
	// ErrNotOwner means a forwarded write reached a node that does not own its
	// bucket (spec §19.3.6, R-2): the coordinator and that node place the
	// bucket differently, which happens while a membership change is being
	// learned. Nothing was applied, so the write is safe to retry.
	ErrNotOwner = errors.New("the cluster's membership is changing and the write was not applied")
	// ErrEpochMismatch means a forwarded write reached a node on another
	// membership epoch than the coordinator's (spec §19.3.7): the two would
	// place the bucket differently. Nothing was applied, so the write is safe
	// to retry, and the refusal has woken the gossip that brings the node that
	// is behind up to date.
	ErrEpochMismatch = errors.New("the cluster's membership is changing and the write was not applied")
	// ErrOwnershipMoved means the owner stopped owning the bucket while a write
	// was in flight (spec §19.3.7): the write is stored on that node and will
	// not be replicated from it. Its outcome is unknown to the client.
	ErrOwnershipMoved = errors.New("the bucket's owner changed while the write was in flight")
	// ErrTooLargeToReplicate means a request with its replication framing
	// cannot fit a frame (S-6). It is refused before anything is logged.
	ErrTooLargeToReplicate = errors.New("the request is too large to replicate")
	// ErrForwardLost means a request forwarded to the bucket's owner got no
	// answer: a write may have applied it, while a read stored nothing. The
	// client is told the outcome is unknown for a write and that nothing
	// changed for a read.
	ErrForwardLost = errors.New("the owner's answer to a forwarded request was lost")
	// ErrHandoffInProgress means this node owns the bucket since a membership
	// change and has not taken it over yet (spec §20.8.4, §20.12.2): its copy
	// is not the newest term's, or it holds none. Nothing was stored, so the
	// write is safe to retry.
	ErrHandoffInProgress = errors.New("ownership of the bucket is moving to this node")
)

// replicaEnqueueHook, when set, runs in applyAsOwner after the bucket's fence is
// released and before the entry is queued for the replicas — the window in which
// a slow writer lets a later sequence reach a peer first. Tests use it
// to hold one writer there. Nil in production. It is atomic rather than a plain
// variable because request goroutines of a running cluster read it while a test
// sets and clears it, and the -race run is part of that test.
var replicaEnqueueHook atomic.Pointer[func(ReplEntry)]

// beforeLocalApplyHook, when set, runs in applyInTurn after the write's apply
// turn has come and immediately before its local apply, on the owner
// (applyAsOwner) and on a replica after its append (applyAsReplica). It is
// given the applying node's ID, so a test can hold one node's apply. Tests use
// it to hold a writer there or to record the order applies happen in. Nil in
// production.
var beforeLocalApplyHook atomic.Pointer[func(nodeID string, repl ReplEntry)]

// afterJoinFenceHook, when set, runs in Cluster.write immediately after a
// client's mutation clears the owner join fence (fenceUnheldBucket) and before
// it is forwarded or applied — the window in which a drop can
// land and finish before this write reaches the apply fence.
// Tests use it to hold one writer there while a drop runs, making that
// otherwise narrow race hit every run instead of about one run in four. Nil
// in production.
var afterJoinFenceHook atomic.Pointer[func(op transport.Opcode, database, bucket string)]

// afterOwnerViewHook, when set, runs in applyAsOwner, inside the bucket's
// fence, after the owner read the membership epoch it logs the entry under
// and the join freeze's binding check passed it, and may block: a test holds a
// create there across a membership adoption. Nil in production.
var afterOwnerViewHook atomic.Pointer[func(nodeID string, op transport.Opcode, database, bucket string, epoch uint64)]

// inOwnerFenceHook, when set, runs in applyAsOwner once the bucket's fence is
// held and the entry addressed, before its apply ticket is opened, and may
// block: a test holds a write there to see whether another write of the bucket
// still gets through the fence (BUG-0603). Nil in production.
var inOwnerFenceHook atomic.Pointer[func(nodeID string, op transport.Opcode, bucket string)]

// beforeOwnerFenceHook, when set, runs in applyAsOwner immediately before
// fenceAsSettledOwner, and may block: a test holds a write there while an
// alter or a seeding snapshot overtakes it (T-150a, T-181). Nothing may go
// between the hook and that call: a test that breaks rule F1 restores the
// addressee read above the hook, and a read placed between the two would run
// after the overtaking change and pass with F1 broken
// (bucket-replication-rev8-spec §14). Nil in production.
var beforeOwnerFenceHook atomic.Pointer[func(nodeID string, entry ReplEntry)]

// beforeOpenCreateApplyHook, when set, runs in applyAsOwner for a create of a
// bucket this node serves, immediately before the create's local apply, and
// may block: a test holds the create there while a drop of the same name
// arrives (BUG-0465). Nil in production.
var beforeOpenCreateApplyHook atomic.Pointer[func(nodeID string, entry ReplEntry)]

// afterMovedRouteHook, when set, runs in applyAsOwner once a write that found
// its bucket moved has released its ship ticket's hold on every peer, and may
// block: a test writes to another bucket there (BUG-0528). Nil in production.
var afterMovedRouteHook atomic.Pointer[func(nodeID, bucket string)]

// replicationRequired reports whether an opcode changes stored state and must
// therefore be placed on the replica set rather than served locally.
//
// This is deliberately keyed off Opcode.IsReadOnly() rather than a "which
// opcodes are writes" list, because the read-only list is short and closed;
// anything not on it (including OpPut/OpBatchPut and the upsert opcodes, which
// are data writes) is routed as a replicated write.
func replicationRequired(op transport.Opcode) bool {
	switch op {
	case transport.OpAuth, transport.OpAuthOK, transport.OpAuthFail,
		transport.OpPong, transport.OpError,
		transport.OpStreamOpen, transport.OpStreamData, transport.OpStreamEnd, transport.OpStreamAck,
		// Dump opcodes are already read-only (IsReadOnly), so they take this
		// path regardless; restore opcodes are writes but must NOT travel as
		// a ReplEntry — a restore applies each document through the ordinary
		// per-document write path (server.InsertCtx etc.), so replaying the
		// whole restore frame as one WAL entry would double-apply it. Both
		// sets are exempted here so every one of the six is routed like a
		// bucket-scoped read (serveRead, forwarding to Cluster.Owner), which
		// is what makes "every chunk of one bucket served by the same node"
		// (dump-restore-spec.md §10.1) fall out of the existing routing
		// instead of needing new code in handleClientFrame.
		transport.OpDumpStart, transport.OpDumpChunk, transport.OpDumpDone,
		transport.OpRestoreStart, transport.OpRestoreChunk, transport.OpRestoreDone:
		return false
	}
	return !op.IsReadOnly()
}

// notYetAddedRefusal is the answer of a bootstrapped node to a request that
// names a database or bucket (join-freeze spec §5.4). It names the address to
// 'join' in the form the join command parses (parseMember).
func (c *Cluster) notYetAddedRefusal(op transport.Opcode) error {
	refusal := &sentencedError{
		message: fmt.Sprintf("This node has not been added to the cluster yet: its member list comes from its seed settings "+
			"and no member has confirmed it. Run 'join %s' on a member, then retry. Nothing was read or written.", c.joinAddress()),
		cause: ErrNotYetAdded,
	}
	// A member it refuses to exchange membership with never confirms it, and
	// 'join' would not change that: name the real cause (build report F-D).
	if cause := c.membershipRefusal(); cause != nil {
		refusal.message = fmt.Sprintf("This node cannot be confirmed as a cluster member, so it serves no database or bucket: %v. "+
			"Nothing was read or written.", cause)
	} else if silent, founder := c.foundersNotAnswered(); founder {
		// No member disputes this node's seed list, so it is a founder of a new
		// cluster waiting for the others, not a newcomer: 'join' would not help.
		refusal.message = foundersWaitingMessage(silent) + " Nothing was read or written."
	}
	if replicationRequired(op) {
		return withOutcome(outcomeNotApplied, refusal)
	}
	return refusal
}

// joinAddress is this node in the form 'join' parses (parseMember).
func (c *Cluster) joinAddress() string {
	if c.self.ID != c.self.Addr {
		return c.self.ID + "@" + c.self.Addr
	}
	return c.self.Addr
}

// foundersWaitingMessage names the founders a node of a new cluster still
// waits for, and the two ways out (D1 of the join-freeze verification).
func foundersWaitingMessage(silent []string) string {
	return fmt.Sprintf("This node is a founder of a new cluster and serves no database or bucket until every founder "+
		"in its seed list has answered it; %s has not answered yet. Start it, or, if it is not coming back, "+
		"run 'remove-node <node-id>' for it on a running founder.", strings.Join(silent, ", "))
}

// typeLimitDeferredCtxKey carries NewHandler's mark that it could not resolve
// a client put's bucket type, so the owner must apply the bucket's real size
// limit (BUG-0582). It lives in the context between the door and the entry
// build; the entry carries it to the owner.
type typeLimitDeferredCtxKey struct{}

// withTypeLimitDeferred marks a request whose put the door could not hold to
// its bucket's real limit.
func withTypeLimitDeferred(ctx context.Context) context.Context {
	return context.WithValue(ctx, typeLimitDeferredCtxKey{}, true)
}

// typeLimitDeferred reports whether the door deferred the put's type-dependent
// limit to the owner.
func typeLimitDeferred(ctx context.Context) bool {
	deferred, _ := ctx.Value(typeLimitDeferredCtxKey{}).(bool)
	return deferred
}

// handleClientFrame routes one client-originated frame on a multi-node server.
func (c *Cluster) handleClientFrame(ctx context.Context, frame *transport.Frame) (*transport.Frame, error) {
	// Dump/restore's root gate is checked here, once, before any possible
	// forward: this is the coordinator, where ctx still carries the real
	// client identity. Cluster.forward ships the frame to the bucket's owner
	// over an internode connection that authenticates with the cluster
	// token and never carries a client Identity, so checking on the owner
	// side instead (as server/dump.go's handlers also do, for the
	// standalone and "this node owns it" cases) would refuse every
	// cross-node dump/restore regardless of who the caller was —
	// requireRootIdentity trusts a peer connection for exactly this reason.
	if isDumpOrRestoreOpcode(frame.Opcode) {
		if err := requireRootIdentity(ctx, c.s); err != nil {
			return errorFrame(frame, err), nil
		}
	}

	// One BSON pass yields scope, write concern and the WAL key; decoding the
	// same payload three times was pure overhead on every request.
	meta := decodeFrameMeta(frame.Payload)
	// An unreadable payload is unscoped here and runs locally, where its
	// handler refuses it in its own words; the gate has refused it already
	// for a session that is subject to it. An opcode with a payload layout of
	// its own is scoped by that layout: read as BSON it names no
	// bucket and was answered from local storage, not routed to the owner.
	bucket, database, scopeErr := scopeOfFrame(frame, meta)
	if scopeErr != nil {
		c.logger.Debug("client frame names no readable scope; running it locally", "op", frame.Opcode, "error", scopeErr)
	}
	// Zero means the request named no concern. The bucket's owner resolves it,
	// after its own apply: a coordinator that never held the bucket cannot know
	// its factor and would choose the node's, and the create that registers a
	// factor would itself be taken at the default of the factor it replaces.
	wc, wcErr := meta.writeConcern()
	if wcErr != nil {
		if replicationRequired(frame.Opcode) {
			wcErr = withOutcome(outcomeNotApplied, wcErr)
		}
		return errorFrame(frame, wcErr), nil
	}

	// A listing of databases or of a database's buckets is the cluster's,
	// not this node's copies (BUG-0468). A node whose member list is not
	// confirmed yet keeps the answers it gave before: its own databases, and
	// the not-yet-added refusal of a bucket listing below.
	if isClusterListingOpcode(frame.Opcode) && !c.bootstrapped.Load() {
		return c.serveClusterListing(ctx, frame)
	}

	// Frames that are not scoped to a bucket (auth, ping, health, node
	// listing) execute locally, exactly as on a single node. A data write
	// here names no bucket its handler can find, so its refusal says
	// not_applied, as fencedDispatch's does.
	if bucket == "" && database == "" {
		return dispatchStampingRefusal(ctx, frame, c.s.dispatch)
	}

	// A node whose member list nobody has confirmed places nothing by the
	// cluster's ring: a read it answered as owner would say "not found" for
	// data the cluster holds, and a create would make it the owner of a
	// bucket it holds no copy of.
	if c.bootstrapped.Load() {
		return errorFrame(frame, c.notYetAddedRefusal(frame.Opcode)), nil
	}

	if frame.Opcode == transport.OpRestoreStart {
		return c.startRestoreUnlessPaused(ctx, frame, bucket, database)
	}
	if !replicationRequired(frame.Opcode) {
		return c.serveRead(ctx, frame, bucket, database)
	}

	// The refusal is the answer, so the error stays nil: a Go error makes the
	// transport drop this frame and fail the whole connection, and with it
	// every request in flight there (BUG-0563).
	if err := c.refuseClientMutation(frame.Opcode, bucket); err != nil {
		return errorFrame(frame, withOutcome(outcomeNotApplied, err)), nil
	}

	// A mutation that names a database and no bucket is database DDL or a
	// mistake. Only the DDL is broadcast: the broadcast logs the payload and
	// sends it to every member before any handler looks at it, so a put with
	// an empty bucket name reached every member's log, a plaintext member's
	// included, while the client was told it had been refused.
	if bucket == "" && frame.Opcode != transport.OpCreateDB && frame.Opcode != transport.OpDeleteDB {
		return errorFrame(frame, withOutcome(outcomeNotApplied, requireNames(database, bucket))), nil
	}

	if refusal := c.s.databaseCreateRefusal(frame); refusal != nil {
		return refusal, nil
	}

	// The generations are stamped here, on the node the client reached, and
	// before anything is logged: the payload logged is the one every applier
	// and every replay reads (G-1, G-3, G-5 with SA-DG-14, G-9).
	if frame.Opcode == transport.OpCreateDB || frame.Opcode == transport.OpDeleteDB {
		c.databaseDDLMu.Lock()
		defer c.databaseDDLMu.Unlock()
	}
	// Database DDL is broadcast and never reaches applyAsOwner, where a
	// bucket's write is asked this (BUG-0576), so it is asked here, before it
	// is stamped or logged. This node logs it first and every member applies
	// it, so this node's view of the members is the one that decides.
	if bucket == "" {
		if refused, ok := c.memberFeaturePrecheck(ReplEntry{Op: int32(frame.Opcode), Payload: frame.Payload}); ok {
			return transport.NewFrameWithID(transport.Opcode(refused.Opcode), frame.RequestID, refused.Payload), nil
		}
	}
	entry, prepareErr := c.prepareClientWrite(ctx, frame, bucket, meta)
	if prepareErr != nil {
		return errorFrame(frame, prepareErr), nil
	}

	// The request's own durability requirement (read above into wc) applies
	// when present; otherwise the bucket's default does, resolved by its owner.
	var (
		resp ReplResponse
		err  error
	)
	if bucket == "" {
		// Database-level DDL has no bucket to place by, so it goes to every
		// member.
		resp, err = c.broadcastMutation(ctx, entry, database)
	} else {
		resp, err = c.write(ctx, entry, placementOf(database, bucket), wc)
	}
	if err != nil {
		// Answer with an error frame instead of a handler error. The transport
		// turns a handler error into a fatal stream reset, which would tear
		// down the client's session over something as ordinary as a write
		// concern that could not be met or a bucket whose copy has not landed
		// (the "read frame header: EOF" cascade on a shared
		// connection). The client gets a defined response carrying the reason,
		// and the session survives.
		if errors.Is(err, ErrJoinSyncIncomplete) || errors.Is(err, ErrBucketNotFound) {
			c.logger.Debug("fencing mutation to an owned bucket with no local copy",
				"op", frame.Opcode, "bucket", bucket, "error", err)
		} else {
			c.logger.Warn("write refused", "op", frame.Opcode, "bucket", bucket, "error", err)
		}
		return errorFrame(frame, withOutcome(outcomeUnknown, err)), nil
	}
	return transport.NewFrameWithID(transport.Opcode(resp.Opcode), frame.RequestID, resp.Payload), nil
}

// prepareClientWrite turns a client's mutation into the entry that is logged,
// shipped and replayed, on the node the client reached and before anything is
// logged: the generations are stamped, document ids and now() resolved once
// for every applier, and an entry no replication frame can carry is refused.
// Cluster.handleClientFrame and a batch's items (Cluster.coordinateBatch) both
// prepare through it, so a batch item and a lone write cannot drift
// (batch-request spec §5.2 step 2). Every refusal is not_applied: nothing was
// logged.
func (c *Cluster) prepareClientWrite(ctx context.Context, frame *transport.Frame, bucket string, meta frameMeta) (ReplEntry, error) {
	// A filtered update of a spread bucket is refused before anything is
	// stamped or logged (build step 12c). A bucket this node does not hold
	// cannot be judged here: the entry is marked and its owner refuses it.
	filterDeferred := false
	if frame.Opcode == transport.OpUpdate && meta.scansBucket() {
		_, database := meta.scope(frame.Opcode)
		refusal, judged := c.s.filteredUpdateRefusal(database, bucket)
		if refusal != nil {
			return ReplEntry{}, withOutcome(outcomeNotApplied, refusal)
		}
		filterDeferred = !judged
	}
	// The generations are stamped before anything is logged: the payload
	// logged is the one every applier and every replay reads (G-1, G-3, G-5
	// with SA-DG-14, G-9).
	stamp, stampErr := c.s.stampGenerations(ctx, frame.Opcode, frame.Payload)
	if stampErr != nil {
		return ReplEntry{}, withOutcome(outcomeNotApplied, fmt.Errorf("stamping %s with its database's incarnation: %w", frame.Opcode, stampErr))
	}
	if stamp.refused {
		return ReplEntry{}, withOutcome(outcomeNotApplied, &sentencedError{message: stamp.message, cause: stamp.cause})
	}

	entry := ReplEntry{
		Op:                int32(frame.Opcode),
		Bucket:            bucket,
		Key:               meta.key(),
		Payload:           stamp.payload,
		TypeLimitDeferred: typeLimitDeferred(ctx),
		FilterDeferred:    filterDeferred,
	}

	// Resolve document ids and now() before the entry is logged and shipped.
	// The payload is what the owner, every replica, and every future replay
	// applies, so a request that does not name an id must have one minted
	// *here*, and every now() must be one instant fixed here — done inside the
	// handler instead, each applier minted its own id and read its own clock.
	resolved, resolveErr := resolveDocumentIDs(frame.Opcode, entry.Payload)
	var repeated *repeatedIDError
	if errors.As(resolveErr, &repeated) && !c.doorRefusesRepeatedIDs(repeated) {
		// The door has no bucket type until a batch repeats an _id_, and then
		// only this one lookup: a queue or heap bucket takes the repeat as two
		// elements, and a bucket this node does not hold is judged by its
		// owner's handler. The upsert resolution is the same one without the
		// refusal.
		resolved, resolveErr = resolveWritePayload(transport.OpBatchUpsert, entry.Payload, true)
	}
	if resolveErr != nil {
		return ReplEntry{}, withOutcome(outcomeNotApplied, resolveErr)
	}
	if resolved != nil {
		entry.Payload = resolved
	}

	// A request the replication frame cannot carry is refused here, before
	// anything is logged: an entry in the owner's log that no frame can hold is
	// re-sent for ever and nothing behind it is delivered.
	if err := checkFitsReplicationFrame(entry); err != nil {
		return ReplEntry{}, withOutcome(outcomeNotApplied, err)
	}
	return entry, nil
}

// doorRefusesRepeatedIDs reports whether the door may refuse a batch that
// repeats an _id_: its bucket is held here and is a hash or btree bucket.
func (c *Cluster) doorRefusesRepeatedIDs(repeated *repeatedIDError) bool {
	facts, held := c.s.peekBucket(repeated.database, repeated.bucket)
	return held && insertRefusesRepeatedIDs(facts.bucketType)
}

// refuseClientMutation is the cluster's refusals of a client's mutation that
// hold at whichever door it came in by, asked before anything is stamped or
// logged: Cluster.handleClientFrame asks them on a node that routes and
// Cluster.admitAtOneMemberDoor on a node alone on its ring, so a refusal added
// here is asked at both (BUG-0625, BUG-0626). Each is an atomic load.
func (c *Cluster) refuseClientMutation(op transport.Opcode, bucket string) error {
	if err := c.refuseWhenRemoved(); err != nil {
		return err
	}
	if c.mismatch.Load() {
		// Refusing to write is a serious condition: the node cannot place data
		// where its peers would agree it belongs, so it must be visible rather
		// than showing up as an unexplained failure at the client.
		c.logger.Warn("refusing mutation: cluster membership disagrees",
			"op", op, "bucket", bucket, "error", errMembershipDiverged)
		return errMembershipDiverged
	}
	// The door of the join freeze (spec §5.1): a schema change is refused here
	// while this node's view pauses them. A bucket's owner checks again under
	// its fence (applyAsOwner).
	return c.refuseSchemaChange(op, c.Epoch())
}

// admitAtOneMemberDoor is what a node alone on its ring asks of a client's
// mutation before it takes the bucket's fence (Server.dispatchInFence), where
// a routed node asks handleClientFrame's refusals and, as the bucket's owner,
// fenceUnheldBucket. A node that was always alone owns nothing another node
// could hold and asks only refuseClientMutation. A node a removal left alone
// owns every bucket the removed nodes held, and may hold none of them yet: a
// schema change of a bucket it does not hold is asked the owner's questions
// (fenceUnheldBucket), a data op of one is answered as the owner answers it
// (answerUnheldDataOp, BUG-0632), and a create of one first waits for the take-over to
// look for the bucket on the removed nodes, so it is answered from the copy
// that lands instead of making an empty bucket of the name (BUG-0625). The
// returned context carries the create's survey epoch.
func (c *Cluster) admitAtOneMemberDoor(ctx context.Context, op transport.Opcode, database, bucket string, payload []byte) (context.Context, error) {
	if err := c.refuseClientMutation(op, bucket); err != nil {
		return ctx, err
	}
	if bucket == "" || !c.aloneAfterARemoval() {
		return ctx, nil
	}
	if !isSchemaChange(op) {
		return ctx, c.answerUnheldDataOp(ctx, database, bucket, true)
	}
	// The owner's questions, which ask a create of a name only a removed node
	// may hold to wait for its copy (Cluster.awaitCopyARemovedNodeMayHold).
	return c.fenceUnheldBucket(ctx, op, database, bucket, payload)
}

// checkFitsReplicationFrame refuses an entry that, with the framing that
// carries it to a replica, cannot fit transport.MaxFrameLength. The client's
// own frame limit is the same number, so a request just under it passes the
// transport and would fail here, at the owner.
func checkFitsReplicationFrame(entry ReplEntry) error {
	if len(entry.Payload)+replFrameOverhead <= transport.MaxFrameLength {
		return nil
	}
	return &sentencedError{cause: ErrTooLargeToReplicate, message: fmt.Sprintf("this request is %d bytes; with replication framing it cannot fit a %d MiB frame. "+
		"Send it in smaller pieces, or store the payload outside the database and keep a pointer (the large-files page of the user docs)",
		len(entry.Payload), transport.MaxFrameLength>>20)}
}

// serveRead answers a read, locally when this node holds a copy of the bucket
// and by asking the owner when it does not.
func (c *Cluster) serveRead(ctx context.Context, frame *transport.Frame, bucket, database string) (*transport.Frame, error) {
	if bucket == "" {
		return c.s.dispatch(ctx, frame)
	}
	key := placementOf(database, bucket)
	// Ownership is read once, and the local answer, its fence and the forward
	// all act on that one decision: read again, a membership adopted in between
	// could make this node skip the fence as a non-owner and then forward to
	// itself as the owner (the same window, for reads).
	owner, epoch, ownerErr := c.ownerView(key)
	if ownerErr == nil && owner.ID == c.self.ID {
		// An owner that has not taken the bucket over answers nothing as its
		// owner (spec §20.8.4): the read names the bucket to the take-over and
		// is answered from the copy it lands, by the previous owner, or with
		// retry (onDemandRead).
		awaits, err := c.ownerReadAwaitsTakeOver(key, epoch, database, bucket)
		if err != nil {
			return errorFrame(frame, fmt.Errorf("serving %s of bucket %q in database %q: %w", frame.Opcode, bucket, database, err)), nil
		}
		if c.readsOnDemand(frame.Opcode, awaits, key, epoch, database, bucket) {
			answer := c.onDemandRead(ctx, frame.Opcode, key, epoch, database, bucket)
			if reply, served, err := c.sendOnDemandAnswer(ctx, frame, bucket, answer); !served {
				return reply, err
			}
		} else if awaits {
			// Only a describe or a restore reaches here (readsOnDemand).
			c.wakeTakeOver(database, bucket)
			// A describe is answered from the copy the owner holds, with the
			// count withheld: it says taking-over, where a read is refused. A
			// spread template's copy is never the collection's (B1): its
			// describe is refused with the hand-off, below.
			if frame.Opcode == transport.OpBucketStats && c.s.bucketStoreOpen(database, bucket) && !c.s.spreadTemplateHeld(database, bucket) {
				return c.describeWhileTakingOver(ctx, frame, key), nil
			}
			// A restore is never sent to the previous owner: only the owner
			// restores (on-demand pull spec §6.8). Nor is a describe of a copy
			// that has not landed: it keeps the join fence's refusal (GAP-23
			// G4-G7 SA-15), now that a joiner knows its previous owner (§6.7).
			if status := c.takeOverStatusOf(key); status.sourceKnown && !isRestoreOpcode(frame.Opcode) && frame.Opcode != transport.OpBucketStats {
				return c.forwardRead(ctx, frame, status.source, bucket, readForward{})
			}
			return errorFrame(frame, c.handoffInProgress(database, bucket)), nil
		}
		if err := c.fenceUnlandedRead(database, bucket); err != nil {
			c.logger.Debug("fencing a read of an owned bucket whose copy has not landed",
				"op", frame.Opcode, "bucket", displayBucket(bucket), "error", err)
			return errorFrame(frame, err), nil
		}
		// A spread template answers no read from its store: the router
		// answers it, below the take-over checks above (B1).
		if c.s.spreadTemplateHeld(database, bucket) {
			return c.routeSpreadRead(ctx, frame, database, bucket)
		}
		return c.s.dispatch(ctx, frame)
	}
	serves, outside := c.holderServes(frame.Opcode, key, database, bucket)
	if serves {
		return c.s.dispatch(ctx, frame)
	}
	if outside {
		// The copy is frozen where it left its set: the read goes to the
		// owner, and the take-over that copies it again is started now
		// rather than at the worker's next idle round.
		c.wakeTakeOver(database, bucket)
	}
	if ownerErr != nil {
		c.logger.Warn("read refused", "op", frame.Opcode, "bucket", displayBucket(bucket), "error", ownerErr)
		return errorFrame(frame, ownerErr), nil
	}
	if err := c.refuseRestoreToPlaintextOwner(ctx, owner, frame.Opcode, bucket); err != nil {
		c.logger.Warn("restore refused", "bucket", bucket, "owner", owner.ID, "error", err)
		return errorFrame(frame, err), nil
	}
	if frame.Opcode == transport.OpBucketStats {
		return c.forwardDescribe(ctx, frame, owner, key, database, bucket), nil
	}
	return c.forwardRead(ctx, frame, owner, bucket, clientReadForward)
}

// readAtOneMemberDoor answers a read at the one-member door
// (Server.dispatchInFence). A node a removal left alone may hold a replica
// copy of a bucket it owns and has not taken over, which lacks the removed
// owner's last writes: it answers the read as a routed owner does, through
// serveRead, which forwards it to the bucket's previous owner when the
// take-over has heard from it and otherwise refuses with the hand-off
// (BUG-0630). A dump or a restore is checked for its root identity first, as
// handleClientFrame does, because a forwarded frame carries none. A restore's
// start is the handler's (the routed door gives it startRestoreUnlessPaused,
// not serveRead). Any other node dispatches the read as it always has: a node
// that was always alone pays one atomic load (aloneAfterARemoval).
func (c *Cluster) readAtOneMemberDoor(ctx context.Context, frame *transport.Frame, meta frameMeta) (*transport.Frame, error) {
	alone, epoch := c.aloneAfterARemovalAt()
	if frame.Opcode == transport.OpRestoreStart || !alone {
		// A node that was always alone is its spread buckets' router.
		return c.s.dispatchRoutingSpread(ctx, frame, meta)
	}
	if isDumpOrRestoreOpcode(frame.Opcode) {
		if err := requireRootIdentity(ctx, c.s); err != nil {
			return errorFrame(frame, err), nil
		}
	}
	bucket, database, err := scopeOfFrame(frame, meta)
	if err != nil || bucket == "" || c.readsOpenedCopyAtRest(epoch, database, bucket) {
		// Unreadable (its handler refuses it in its own words, as on the
		// routed door), scoped to no bucket, or a bucket taken over and at
		// rest, which serveRead would answer from this node's copy too, or
		// route when it is a spread template.
		return c.s.dispatchRoutingSpread(ctx, frame, meta)
	}
	if err := c.answerUnheldDataOp(ctx, database, bucket, false); err != nil {
		return errorFrame(frame, err), nil
	}
	return c.serveRead(ctx, frame, bucket, database)
}

// answerUnheldDataOp is the owner's answer, for a node a removal left alone,
// to a data op of a bucket it holds no store of. When a removed node owned the
// name, that node may hold the bucket and the take-over has not said it does
// not, so the op is not "bucket not found": it names the bucket to the
// take-over worker, as a routed owner does (Cluster.fenceUnheldBucket), and
// answers the hand-off ("moving to this node ... Retry in a moment") once the
// worker records what it waits for. The worker records a moment after the
// wake, so the op looks for up to unheldDataOpWait, holding no fence. When it
// has recorded nothing by then, a read is left to serveRead, whose on-demand
// path gives R8's answers, and a mutation is answered R8's way here
// (refuseRemovedOwnersName, BUG-0693): a retry naming the removed node while
// its hand-off is awaited, then not found with the note. A name no removed
// node owned, and a held bucket, are not asked (BUG-0632).
//
// Cost (rule 11): a held bucket pays one lock-free lookup; no per-bucket state
// beyond the take-over's own entries.
func (c *Cluster) answerUnheldDataOp(ctx context.Context, database, bucket string, mutation bool) error {
	// The held bucket returns before the key is built: placementOf allocates.
	if c.s.bucketServed(database, bucket) || c.s.bucketStoreOpen(database, bucket) {
		return nil
	}
	key := placementOf(database, bucket)
	node := c.removedFormerOwnerOf(key)
	if node == "" {
		return nil
	}
	// After R8's bound a read goes straight to serveRead, whose after-window
	// answer is the miss with the note, and a mutation is answered that way here.
	if c.removedOwnersWindowClosed(key) {
		if !mutation {
			return nil
		}
		return c.refuseRemovedOwnersName(key, database, bucket, node)
	}
	if err := c.wakeAndAwaitTakeOver(ctx, key, database, bucket, unheldDataOpWait); err != nil || !mutation {
		return err
	}
	return c.refuseRemovedOwnersName(key, database, bucket, node)
}

// wakeAndAwaitTakeOver names an unheld bucket to the take-over worker and
// answers the hand-off once the worker records what it waits for or has the
// bucket claimed. The worker records a moment after the wake, so the caller
// looks for up to wait (zero looks once), holding no fence; nil means the
// worker had recorded nothing by then. Both doors that ask it, the one-member
// door (answerUnheldDataOp) and the routed owner's fence (fenceUnheldBucket),
// share it so they cannot answer the same state differently (BUG-0636). The
// caller has already found the bucket unheld.
func (c *Cluster) wakeAndAwaitTakeOver(ctx context.Context, key placementKey, database, bucket string, wait time.Duration) error {
	c.wakeTakeOver(database, bucket)
	deadline := time.Now().Add(wait)
	for {
		if c.takeOverStatusOf(key).recorded() || c.takeOverClaimed(database, bucket) {
			return c.handoffInProgress(database, bucket)
		}
		if !time.Now().Before(deadline) {
			return nil
		}
		step := time.NewTimer(unheldDataOpStep)
		select {
		case <-step.C:
		case <-ctx.Done():
			step.Stop()
			return fmt.Errorf("waiting for this node to take over bucket %q in database %q: %w", bucket, database, ctx.Err())
		}
	}
}

// unheldDataOpWait is how long a data op of a bucket a removed node owned
// waits for the take-over worker to record what it waits for before the
// bucket is answered as not found; unheldDataOpStep is how often it looks.
// One worker retry floor is enough for a round that has a copy to report.
const (
	unheldDataOpWait = takeOverRetryFloor
	unheldDataOpStep = 20 * time.Millisecond
)

// readForward is what a forwarded read carries for the on-demand read path
// (on-demand pull spec §6.2, §8, F-5): the bound left for the owner to wait
// (waitMs), the reroute's mark (rerouted), and the serve-or-refuse mark of a
// forward to the bucket's final previous owner (serveFromCopy). Every other
// forward, and every write, carries the zero value.
type readForward struct {
	waitMs        int64
	rerouted      bool
	serveFromCopy bool
}

// clientReadForward is a coordinator's forward of a client read to the
// bucket's owner: the owner may wait the whole bound (spec §8).
var clientReadForward = readForward{waitMs: onDemandWait.Milliseconds()}

// forwardRead sends a read to the node that answers it for this one: the
// bucket's owner, or, while this node takes the bucket over, its previous
// owner. A forward to the previous owner that it refuses because it released
// its copy is answered with this node's hand-off retry: the copy lands here,
// and the read is never told the bucket does not exist (spec §6.2).
func (c *Cluster) forwardRead(ctx context.Context, frame *transport.Frame, owner ring.Node, bucket string, rf readForward) (*transport.Frame, error) {
	var err error
	// A forwarded query's result is held here, on the coordinator, from the
	// owner's answer until the client's write returns. A forwarded-result
	// slot bounds how many this node holds at once (spec §3.1.1). It is never
	// an execution slot: a coordinator waiting on an owner that is waiting on
	// it would deadlock (RC-2).
	var release func()
	// A spread part's read sent on (to the part's previous owner, while this
	// node takes it over) is covered by its fan-out's one admission, which
	// holds a forward slot until the fan-out answers: a second slot here
	// would wait on the first (spread_fanout.go, "Slots").
	if isQueryOpcode(frame.Opcode) && !fanOutAdmitted(ctx) {
		gate := c.s.queryGate()
		release, err = gate.acquire(ctx, gate.forwardPool())
		if err != nil {
			return nil, fmt.Errorf("forwarding %s for bucket %q: %w", frame.Opcode, bucket, err)
		}
	}
	resp, err := c.forward(ctx, owner, ReplEntry{
		Op:      int32(frame.Opcode),
		Bucket:  bucket,
		Payload: frame.Payload,
	}, 0, rf)
	if err != nil {
		if release != nil {
			release()
		}
		if rf.serveFromCopy && errors.Is(err, ErrJoinSyncIncomplete) {
			_, database := decodeFrameMeta(frame.Payload).scope(frame.Opcode)
			err = c.handoffInProgress(database, bucket)
		}
		c.logger.Warn("read forwarded to owner failed", "bucket", displayBucket(bucket), "owner", owner.ID, "error", err)
		return errorFrame(frame, err), nil
	}
	answer := transport.NewFrameWithID(transport.Opcode(resp.Opcode), frame.RequestID, resp.Payload)
	// Count the answer's bytes while this node holds it, whether or not a
	// forwarded-result slot was taken. The N-4 bound is about what the
	// coordinator holds, so the count must stand on its own even where the
	// slot is (wrongly) absent.
	held := int64(len(answer.Payload))
	c.s.queryGate().forwardedBytes.Add(held)
	answer.AddWrittenHook(func() {
		c.s.queryGate().forwardedBytes.Add(-held)
		if release != nil {
			release()
		}
	})
	return answer, nil
}

// holderServes reports whether this node, which is not the bucket's owner,
// answers a read out of its own storage. Membership of the replica set is not
// enough: a bucket declared below the node default is bucket DDL placed on its
// own, smaller set, so a node that never held it has no factor registered and
// computes the set at its own default — which can include itself. "I am in the
// set I computed" is not "I hold the copy", so a node that is not the owner
// answers only from an open store and otherwise forwards. The owner always
// answers itself (serveRead), because the forward's target is the owner: one
// that does not hold the bucket must give its own refusal, not forward to
// itself.
//
// A restore is served by the owner alone. RESTORE_START creates the bucket it
// writes into, so "this node has no copy yet" is its ordinary starting state —
// but it also decides exists, type mismatch, --drop and merge from this node's
// local state, and on a node that holds nothing that state is empty: --drop
// would drop nothing and the restore would merge into the owner's bucket. The
// owner is the same node at every factor and every node can compute it, so a
// session started through any node lands on it and cannot move.
//
// A queue peek is served by the owner alone too. It names the element the next
// pop takes, and a pop is applied on the owner, so a replica that lags would
// show an element already popped. It is still a read: nothing is
// logged, synced or waited for.
//
// A describe is served by the owner alone as well: the counts it prints are
// the owner's (spec §12.2), so a holder forwards it (Cluster.forwardDescribe).
//
// A copy marked outside does not answer either, and outside reports it. It
// left the bucket's set and takes no entry until it is copied again
// (refuseByLineage), so a membership change that places this node in the set
// again would otherwise serve data that stopped where it left (BUG-0560). The
// mark is not cleared by the placement; the copy again clears it.
func (c *Cluster) holderServes(op transport.Opcode, key placementKey, database, bucket string) (serves, outside bool) {
	if isRestoreOpcode(op) || isPeekOpcode(op) || op == transport.OpBucketStats {
		return false, false
	}
	// A copy of a spread template never answers a client read (B1): its born
	// set may lag its owner's, and its store holds no document. The read goes
	// to the owner, the router.
	if c.s.spreadTemplateHeld(database, bucket) {
		return false, false
	}
	return c.holdsServableCopy(key, database, bucket)
}

// holdsServableCopy reports whether this node holds an open copy of the bucket
// that it is in the set of and that is not marked outside, which is a copy it
// may answer a read from; outside reports a copy marked outside (BUG-0560).
func (c *Cluster) holdsServableCopy(key placementKey, database, bucket string) (serves, outside bool) {
	if !c.Hosts(key) || !c.s.bucketStoreOpen(database, bucket) {
		return false, false
	}
	if c.placementMarksOf(key).Outside != 0 {
		return false, true
	}
	return true, false
}

// isPeekOpcode reports whether op reads the element a queue's next pop takes.
func isPeekOpcode(op transport.Opcode) bool {
	switch op {
	case transport.OpFIFOPeek, transport.OpLIFOPeek, transport.OpFieldPeek, transport.OpDeadList:
		return true
	default:
		return false
	}
}

// write applies a mutation to the replica set of the bucket placed by key,
// which the caller derives from the database and bucket it already decoded.
//
// Ownership is read once. A node that owns a bucket but has no local copy of
// it is refused with a reason rather than writing into an empty store: either
// it is a joiner mid-rebalance (the data lives only on the previous holders)
// or the bucket was dropped. The fence is on the ownership decided
// here: fenced before this read, a write could pass it as a non-owner and be
// applied as the owner under a membership adopted in between.
func (c *Cluster) write(ctx context.Context, entry ReplEntry, key placementKey, wc WriteConcernLevel) (ReplResponse, error) {
	owner, err := c.Owner(key)
	if err != nil {
		return ReplResponse{}, withOutcome(outcomeNotApplied, err)
	}
	op := transport.Opcode(entry.Op)
	_, database := decodeFrameMeta(entry.Payload).scope(op)
	// A batch chain's items all go to the owner looked up for its first item:
	// one that finds another owner now is refused, never forwarded, so a chain
	// never spans two owners (batch-request spec §4.3, rev 2.1 F3).
	if chainOwner, inChain := batchChainOwnerFrom(ctx); inChain && chainOwner != owner.ID {
		return ReplResponse{}, withOutcome(outcomeNotApplied, fmt.Errorf("%w: %s for bucket %q in database %q is part of a batch whose items for this bucket go to %s, "+
			"and the bucket now belongs to %s on this node's view of the membership; nothing was applied. "+
			"Resend it from this item: a join or remove-node is normally learned by every node in under a second",
			ErrNotOwner, op, entry.Bucket, database, chainOwner, owner.ID))
	}
	if owner.ID == c.self.ID {
		fenced, err := c.fenceUnheldBucket(ctx, op, database, entry.Bucket, entry.Payload)
		if err != nil {
			return ReplResponse{}, withOutcome(outcomeNotApplied, err)
		}
		ctx = fenced
	}
	if hook := afterJoinFenceHook.Load(); hook != nil {
		(*hook)(op, database, entry.Bucket)
	}
	if owner.ID != c.self.ID {
		// R4's coordinator half: the only protection against an older owner,
		// which cannot run the check itself (spec §11.2).
		if err := c.refuseAlterWithoutFeature(entry); err != nil {
			return ReplResponse{}, withOutcome(outcomeNotApplied, err)
		}
		if err := c.refuseForwardToPlaintextOwner(ctx, owner, entry); err != nil {
			return ReplResponse{}, withOutcome(outcomeNotApplied, err)
		}
		return c.forward(ctx, owner, entry, wc, readForward{})
	}
	return c.applyAsOwner(ctx, entry, key, wc)
}

// ownerFence is what fenceAsSettledOwner needs of a mutation: its opcode,
// the fence mode it takes, and the bucket it names, already decoded, so a
// door that has decoded them pays no second decode.
type ownerFence struct {
	op       transport.Opcode
	mode     fenceMode
	key      placementKey
	database string
	bucket   string
}

// fenceAsSettledOwner takes the bucket's fence for an owner's write and makes
// the check of spec §20.8.4 inside it, before anything is opened or logged
// (step 10b.3): this node is the ring's owner — ownership and the epoch read
// in one hold, the epoch the entry is logged under — and it has taken the
// bucket over. A write or a drop of a bucket it has not taken over is refused
// with ErrHandoffInProgress, and the worker is woken for it. A create is not
// exempt (review requirement 2): it releases the fence, waits for the
// take-over, and takes the fence again, so that it is answered from the copy
// the take-over landed, or creates the bucket when no live copy was found.
// It never waits under the fence: the take-over's open takes the same fence
// exclusively (trap 14).
func (c *Cluster) fenceAsSettledOwner(ctx context.Context, held ownerFence) (*fenceHold, uint64, error) {
	op, key, database, bucket := held.op, held.key, held.database, held.bucket
	for waited := false; ; waited = true {
		fence := c.s.acquireFence(key, held.mode)
		owner, epoch, err := c.ownerView(key)
		if err != nil {
			fence.release()
			return nil, 0, fmt.Errorf("reading the owner of bucket %q in database %q inside its fence: %w", bucket, database, err)
		}
		if owner.ID != c.self.ID {
			fence.release()
			return nil, 0, fmt.Errorf("%w: %s no longer owns bucket %q in database %q at epoch %d; it now belongs to %s",
				ErrNotOwner, c.self.ID, bucket, database, epoch, owner.ID)
		}
		pending, err := c.ownerAwaitsTakeOver(key, epoch, database, bucket)
		if err != nil {
			fence.release()
			return nil, 0, fmt.Errorf("reading whether bucket %q in database %q is taken over: %w", bucket, database, err)
		}
		// A copy opens in the batch that lands it, a moment before the worker
		// removes the bucket's marker, and a create met by the marker is
		// refused at its apply (refuseUnservable): a create waits it out too.
		if op == transport.OpCreateBucket && c.pullPending(database, bucket) {
			pending = true
		}
		if !pending {
			return fence, epoch, nil
		}
		fence.release()
		c.wakeTakeOver(database, bucket)
		if op != transport.OpCreateBucket || waited {
			return nil, 0, c.handoffInProgress(database, bucket)
		}
		if err := c.awaitTakeOver(ctx, key, database, bucket); err != nil {
			return nil, 0, err
		}
	}
}

// recheckUnheldMutationInFence is an owner's check, inside the bucket's fence,
// of a create, or a drop of a bucket this node does not hold: each was decided
// by the members' answers to a question asked before the fence. Inside it,
// before anything is opened or logged, only local facts are re-checked: no
// pull of the name has registered since (the local apply would refuse the
// create only after it was logged and routed; review R-1), and the epoch is
// the one the question was asked at (R8-D, review M-4). Both doors ask it:
// Cluster.applyAsOwner and Cluster.fenceAtOneMemberDoor.
func (c *Cluster) recheckUnheldMutationInFence(ctx context.Context, op transport.Opcode, key placementKey, database, bucket string) error {
	if op != transport.OpCreateBucket && op != transport.OpDeleteBucket {
		return nil
	}
	if c.s.bucketStoreOpen(database, bucket) {
		return nil
	}
	return c.recheckUnheldInFence(ctx, op, key, database, bucket)
}

// fenceAtOneMemberDoor takes the bucket's fence for a client's mutation at the
// one-member door (Server.dispatchInFence). A node a removal left alone owns
// every bucket the removed nodes held, and may hold a copy of one it has not
// taken over: a replica copy, which the take-over will replace with the
// removed owner's. Written there, an acknowledged write is lost when the copy
// lands, so such a node takes the fence as a routed owner does, through
// fenceAsSettledOwner: a write or a drop of a bucket it has not taken over is
// refused with the hand-off (nothing stored, retry), and a create waits for
// the take-over; then it asks recheckUnheldMutationInFence (BUG-0630). It is
// the routed owner's own path, not a copy, so the doors cannot drift. Any
// other node takes the fence as it always has: a node that was always alone
// pays one atomic load (aloneAfterARemoval).
func (c *Cluster) fenceAtOneMemberDoor(ctx context.Context, op transport.Opcode, database, bucket string, payload []byte) (*fenceHold, error) {
	key := placementOf(database, bucket)
	if bucket == "" || !c.aloneAfterARemoval() {
		return c.s.acquireFence(key, fenceModeFor(op)), nil
	}
	mode := ownerFenceMode(ReplEntry{Op: int32(op), Payload: payload})
	fence, _, err := c.fenceAsSettledOwner(ctx, ownerFence{op: op, mode: mode, key: key, database: database, bucket: bucket})
	if err != nil {
		return nil, err
	}
	if err := c.recheckUnheldMutationInFence(ctx, op, key, database, bucket); err != nil {
		fence.release()
		return nil, err
	}
	return fence, nil
}

// refuseOwnerPutSize holds a deferred client's OpPut to the real size limit
// of the bucket its owner holds: the blob cap for a blob bucket, the document
// limit for every other type (BUG-0582). The door applies this when the
// entering node holds the bucket, but cannot resolve a bucket it has not
// learned, so it applies only the ceiling common to every type there
// (Server.refuseBlobBucketPut) and marks the entry, and the owner applies the
// bucket's own limit here. It runs only for a marked put (entry.TypeLimitDeferred)
// and only as the owner applies a client's write (Cluster.applyAsOwner): read
// repair, restore, a replica applying the owner's entry and a WAL replay never
// set the mark and reach Cluster.applyLocal and Server.dispatch without
// passing here, so none of them is refused for a limit its owner accepted.
func (c *Cluster) refuseOwnerPutSize(entry ReplEntry) error {
	if transport.Opcode(entry.Op) != transport.OpPut || !entry.TypeLimitDeferred {
		return nil
	}
	_, err := c.s.refuseInsertFrame(transport.OpPut, entry.Payload)
	return err
}

// applyAsOwner is the serialisation point for a bucket: it durably records the
// entry, applies it, places it on the other replicas, and waits for the write
// concern.
func (c *Cluster) applyAsOwner(ctx context.Context, entry ReplEntry, key placementKey, wc WriteConcernLevel) (ReplResponse, error) {
	// A client's operation on a spread template is the router's (spread MVP
	// note, section 2.1): routed to its part before anything is logged under
	// the template's name. Both callers reach here holding no fence, which the
	// router gate's lock order needs (spread_gate.go).
	if resp, routed, err := c.routeSpreadWrite(ctx, entry, key, wc); routed {
		return resp, err
	}
	// A create of a bucket this node already serves places nothing: the fenced
	// check below answers it from the bucket, neither logged nor shipped. The
	// plaintext-peer guard is about the data a write places, and for such a
	// create it would judge the wider delivery set of the factor the create
	// declares, refusing for a peer the bucket is not on (BUG-0206). So the
	// guard waits for the fenced check, which stays authoritative; a drop that
	// lands between this hint and the fence sends the create round again with
	// the guard back in its place.
	if transport.Opcode(entry.Op) == transport.OpCreateBucket {
		_, database := decodeFrameMeta(entry.Payload).scope(transport.OpCreateBucket)
		if c.s.bucketStoreOpen(database, entry.Bucket) {
			response, err := c.applyOwned(ctx, entry, key, wc, true)
			if !errors.Is(err, errCreateHintStale) {
				return response, err
			}
		}
	}
	return c.applyOwned(ctx, entry, key, wc, false)
}

// errCreateHintStale tells applyAsOwner that the bucket a create was judged to
// exist on was dropped before the create reached its fence, so the pass ran
// without the plaintext-peer guard and must be redone with it. Nothing was
// logged, applied or shipped.
var errCreateHintStale = errors.New("the bucket a create found open was dropped before its fence")

// applyOwned is applyAsOwner's pass. guardDeferred is true when the pass skipped
// the plaintext-peer guard because the bucket looked open; it ends in
// errCreateHintStale if the fence finds it closed.
func (c *Cluster) applyOwned(ctx context.Context, entry ReplEntry, key placementKey, wc WriteConcernLevel, guardDeferred bool) (ReplResponse, error) {
	// Before anything of the write is read or logged: until a majority of
	// this node's peers has said where its log stands, a sequence it gives
	// could be one a peer holds another write at (start_hold.go).
	if err := c.startHold.admit(ctx); err != nil {
		return ReplResponse{}, withOutcome(outcomeNotApplied, err)
	}
	if !guardDeferred {
		if err := c.refusePlacementOnPlaintextReplica(ctx, entry, key); err != nil {
			return ReplResponse{}, withOutcome(outcomeNotApplied, err)
		}
	}
	if refused, ok := c.uniquePrecheck(entry); ok {
		// Refused before anything is logged.
		refused.Payload = refusalWithOutcome(refused.Payload, outcomeNotApplied)
		return refused, nil
	}
	if refused, ok := c.bucketRFPrecheck(entry); ok {
		return refused, nil
	}
	if refused, ok := c.declareNamePrecheck(entry); ok {
		return refused, nil
	}
	if refused, ok, err := c.alterPrecheck(ctx, entry); ok || err != nil {
		if err != nil {
			return ReplResponse{}, withOutcome(outcomeNotApplied, err)
		}
		return refused, nil
	}
	// Refused before anything is logged, so not_applied (errorFrame).
	if refused, ok := c.typedFieldsPrecheck(entry); ok {
		return refused, nil
	}
	// After the specific prechecks, whose messages say more about the same
	// refusal (an alter names its factor).
	if refused, ok := c.memberFeaturePrecheck(entry); ok {
		return refused, nil
	}
	// A client's OpPut is held to its bucket's real size limit here, on the
	// owner, because the door could not resolve the type when the entering
	// node had not learned the bucket (BUG-0582). Refused before anything is
	// logged, so not_applied; the error frame keeps the sentinel's wire code
	// (ErrBlobTooLarge or ErrDocumentTooLarge -> INVALID_ARGUMENT).
	if err := c.refuseOwnerPutSize(entry); err != nil {
		frame := errorFrame(&transport.Frame{Opcode: transport.OpPut}, err)
		return ReplResponse{Opcode: int32(frame.Opcode), Payload: refusalWithOutcome(frame.Payload, outcomeNotApplied)}, nil
	}
	// A filtered update the entering node could not judge (it holds no copy of
	// the bucket) is judged here, by a node that holds the bucket (build step
	// 12c). Refused before anything is logged, so not_applied.
	if err := c.refuseOwnerFilteredUpdate(entry); err != nil {
		frame := errorFrame(&transport.Frame{Opcode: transport.OpUpdate}, err)
		return ReplResponse{Opcode: int32(frame.Opcode), Payload: refusalWithOutcome(frame.Payload, outcomeNotApplied)}, nil
	}
	// A create's carried drops are applied before anything of the create is
	// opened (SA-DG-14): applying one logs and ships a drop, whose turn to
	// route must not wait behind this create's.
	if transport.Opcode(entry.Op) == transport.OpCreateBucket && !seamsOff().noCarried {
		_, database := decodeFrameMeta(entry.Payload).scope(transport.OpCreateBucket)
		if err := c.s.learnTombstones(ctx, database, createCarried(entry.Payload)); err != nil {
			return ReplResponse{}, withOutcome(outcomeNotApplied, fmt.Errorf("applying the drops the create of bucket %q carries: %w", entry.Bucket, err))
		}
	}
	// The bucket's apply fence is held from the append through the local
	// apply — shared, or exclusively for a drop (fenceModeFor) — and released
	// before the replicas are waited for. A
	// snapshot of this bucket (snapshotBucket) takes it exclusively, so the
	// sequence it reads as its high-water is one every appended entry of the
	// bucket has been applied at, and its backup contains exactly those
	// entries — the position a seeded replica adopts on both sides
	//. Writes to the bucket wait for the backup; they
	// are not refused.
	//
	// The fence does not order the replicas' queues: it is shared by every
	// writer and released before the enqueue. The ship ticket does, and it is
	// opened before the append so its floor is below the sequence the append
	// assigns. It is opened inside the fence so a snapshot holding
	// the fence exclusively does not also hold up other buckets' shipping.
	//
	// The fence does not order two writers of the same bucket either, and the
	// bucket's applied_seq needs its applies in sequence order. The
	// apply ticket does: opened before the append, assigned the append's
	// sequence, and waited on just before the local apply. Nothing is held
	// across the append, so the WAL's group commit is intact.
	// A create is decided under the bucket's fence taken exclusively, so the
	// question "does the name exist?" and the append that follows it cannot be
	// separated by another create or a drop of the same name. One that finds the
	// bucket already served is answered from the bucket and is neither logged nor
	// shipped: logged, it would be delivered to the set at the factor it names,
	// and a node outside the bucket's real set would create an empty copy and
	// answer reads from it (verifier N-1).
	isCreate := transport.Opcode(entry.Op) == transport.OpCreateBucket
	if hook := beforeOwnerFenceHook.Load(); hook != nil {
		(*hook)(c.self.ID, entry)
	}
	_, database := decodeFrameMeta(entry.Payload).scope(transport.Opcode(entry.Op))
	fence, epoch, err := c.fenceAsSettledOwner(ctx, ownerFence{op: transport.Opcode(entry.Op), mode: ownerFenceMode(entry),
		key: key, database: database, bucket: entry.Bucket})
	if err != nil {
		return ReplResponse{}, withOutcome(outcomeNotApplied, err)
	}
	// The router's check (routeSpreadWrite) ran before this fence: a drop and
	// a spread create of the name may have landed since, and a client's write
	// must never be applied to a spread template's store.
	if err := c.s.spreadTemplateUnderFence(ctx, transport.Opcode(entry.Op), database, entry.Bucket, key); err != nil {
		fence.release()
		return refusedResponse(transport.Opcode(entry.Op), err), nil
	}
	addressed, unchanged, err := c.addressInFence(entry, key, epoch)
	if err != nil || unchanged != nil {
		fence.release()
		if err != nil {
			return ReplResponse{}, withOutcome(outcomeNotApplied, err)
		}
		return *unchanged, nil
	}
	entry = addressed.entry
	if hook := inOwnerFenceHook.Load(); hook != nil {
		(*hook)(c.self.ID, transport.Opcode(entry.Op), entry.Bucket)
	}
	// A repair put holds the fence exclusively (ownerFenceMode): every entry
	// of the bucket logged before it has applied, and none is logged until it
	// is, so its check of the log and its append are one step (BUG-0603).
	dropped, err := c.dropSupersededRepair(entry)
	if err != nil || dropped != nil {
		fence.release()
		if err != nil {
			return ReplResponse{}, withOutcome(outcomeNotApplied, err)
		}
		return *dropped, nil
	}
	if isCreate {
		_, database := decodeFrameMeta(entry.Payload).scope(transport.OpCreateBucket)
		if c.s.bucketStoreOpen(database, entry.Bucket) {
			// Answered from the open bucket with the fence still held: released
			// first, a drop of the name could land before this apply, and the
			// create would then make the bucket afresh from the client's
			// payload, unlogged (BUG-0465). The payload is stamped as a logged
			// create's is, so the apply never reads a client's lineage.
			defer fence.release()
			// The router's create of a part it finds held is adopted only
			// when the part belongs to the router's template incarnation
			// (B4.2): judged here, inside the part's fence, before anything
			// is applied, and never by name.
			orphanOf, err := c.s.partCreateMeetsOrphan(database, entry)
			if err != nil {
				return ReplResponse{}, withOutcome(outcomeNotApplied, err)
			}
			if orphanOf != 0 {
				return orphanPartAnswer(entry.Bucket, orphanOf)
			}
			stamped, err := stampCreateLineage(entry.Payload, epoch)
			if err != nil {
				return ReplResponse{}, withOutcome(outcomeNotApplied, err)
			}
			entry.Payload = stamped
			if hook := beforeOpenCreateApplyHook.Load(); hook != nil {
				(*hook)(c.self.ID, entry)
			}
			return c.applyLocal(ctx, entry)
		}
		if guardDeferred {
			fence.release()
			return ReplResponse{}, errCreateHintStale
		}
	}
	if err := c.recheckUnheldMutationInFence(ctx, transport.Opcode(entry.Op), key, database, entry.Bucket); err != nil {
		fence.release()
		return ReplResponse{}, withOutcome(outcomeNotApplied, err)
	}
	// An empty claim is answered here, unlogged (claim-ack-spec 5.3): inside
	// the fence, with ownership settled, and before any ticket below is
	// opened, so it leaves none open. It is answered as it is: the
	// coordinator relays it, and no write outcome is stamped on it.
	if transport.Opcode(entry.Op) == transport.OpQueueClaim {
		unlogged, err := c.s.unloggedEmptyClaim(entry.Payload)
		if err != nil || unlogged != nil {
			fence.release()
			if err != nil {
				return ReplResponse{}, withOutcome(outcomeNotApplied, err)
			}
			return *unlogged, nil
		}
	}
	// A create is recorded as in flight before ownership is read, and kept as
	// an orphan if the re-check below finds a membership change took the
	// bucket: it holds this node's Settled until a sweep hands it off (review
	// F-11).
	appended, orphaned := false, false
	if isCreate {
		_, database := decodeFrameMeta(entry.Payload).scope(transport.OpCreateBucket)
		settleCreate := c.registerCreate(database, entry.Bucket)
		defer func() { settleCreate(appended, orphaned) }()
	}
	applyTicket := fence.entry.openApply(c.walLastSequence)
	ticket := c.shipOrder.open(c.walLastSequence)
	defer c.shipOrder.close(ticket)
	unappliedFloor := c.unapplied.open(c.walLastSequence)
	// The epoch was read once, with the ownership, inside the fence
	// (fenceAsSettledOwner), and is recorded with the entry: it is the
	// membership epoch under which this node decided it was the owner. A
	// create carries it in its payload too, as the bucket's first term.
	// The entry ships in its bucket's term, read here, inside the fence.
	// The binding check of the join freeze (spec §4.3), inside the fence and
	// against the epoch the entry is logged under: a join adopted while this
	// write waited for the fence is caught here, before the append.
	termErr := c.refuseSchemaChange(transport.Opcode(entry.Op), epoch)
	if hook := afterOwnerViewHook.Load(); hook != nil {
		_, database := decodeFrameMeta(entry.Payload).scope(transport.Opcode(entry.Op))
		(*hook)(c.self.ID, transport.Opcode(entry.Op), database, entry.Bucket, epoch)
	}
	var term uint64
	if termErr == nil {
		term, termErr = c.shippedTerm(entry, epoch)
	}
	// A write whose version cannot be packed is refused before it is logged.
	// The log is node-wide, so another bucket's write can still take the last
	// sequence before this append; that entry's apply is refused alike on
	// every node (applyLocal, applyAsReplica).
	if termErr == nil {
		termErr = refuseUnversionable(entry, term, c.walLastSequence()+1)
	}
	if termErr == nil && isCreate {
		entry.Payload, termErr = stampCreateLineage(entry.Payload, epoch)
	}
	if termErr == nil && transport.Opcode(entry.Op) == transport.OpDeleteBucket {
		entry.Payload, termErr = c.stampDropIncarnation(entry)
	}
	// Here and not at the door: a pop or a lease verb forwarded by a member
	// reaches this line too, and is judged by the same node's ceiling and
	// clock as a peek of the bucket.
	if termErr == nil {
		entry.Payload, termErr = c.stampAtOwner(entry)
	}
	if termErr != nil {
		c.unapplied.close(unappliedFloor)
		fence.entry.closeApply(applyTicket)
		fence.release()
		return ReplResponse{}, withOutcome(outcomeNotApplied, termErr)
	}
	entry.Term = term
	seq, err := c.appendWAL(entry, storage.WALOrigin{Kind: storage.WALOriginOwn, Epoch: epoch})
	if err != nil {
		c.unapplied.close(unappliedFloor)
		fence.entry.closeApply(applyTicket)
		fence.release()
		// A failed append may have reached the disk: replay would apply it.
		return ReplResponse{}, withOutcome(outcomeUnknown, err)
	}
	appended = true
	fence.entry.assignApply(applyTicket, seq)
	c.shipOrder.assign(ticket, seq)
	entry.Seq = seq
	c.logger.Debug("applied mutation as replica-set owner",
		"bucket", entry.Bucket, "seq", seq, "write_concern", wc.String())

	// Apply locally through the same dispatch path a client request takes. On
	// the owner the apply sequence is the write's own sequence, and a store
	// failure is recorded (spec §4.4 rule 5): the entry is shipped to the
	// replicas whether or not it applied here, so replay must retry it.
	applyCtx, applied := withApplyOutcome(withLineage(withRecordFailures(ctx), seq, true))
	resp, applyErr := c.applyInTurn(applyCtx, fence.entry, applyTicket, entry, seq)
	// The one addition the batch spec permits here (§5.4): a batch item's
	// chain is told its local step has ended. It moves no step and waits on
	// nothing; a lone write carries no note.
	noteLocalStep(ctx, resp, applyErr)
	entry.OwnerOutcome = applyOutcomeOf(resp, applyErr)
	// A store failure the handler reported inside the response is owed to
	// replay, not unknown: the owner did not commit the batch (BUG-0247).
	if applied != nil && applied.storeFailure != nil {
		entry.OwnerOutcome = ownerOutcomeOwed
	}
	_, appliedDatabase := decodeFrameMeta(entry.Payload).scope(transport.Opcode(entry.Op))
	resp = c.handlerRefusalWithOutcome(resp, transport.Opcode(entry.Op), applied, appliedDatabase, entry.Bucket)
	// A create's concern is resolved at the factor its own apply registered,
	// read before the fence is released (F1).
	if isCreate {
		addressed.factor = c.effectiveRF(key)
	}
	c.unapplied.close(unappliedFloor)
	fence.release()
	if transport.Opcode(entry.Op) == transport.OpDeleteBucket {
		// A peer stalled on a refused create of the bucket may be released by
		// this drop (owner ruling 53).
		c.wakeSendersRefusedOn(appliedDatabase, entry.Bucket)
	}

	if hook := replicaEnqueueHook.Load(); hook != nil {
		(*hook)(entry)
	}

	// G-6 on the owner's own apply: a create whose generation was dropped
	// here before it applied created nothing, and is addressed to nobody — a
	// replica would refuse it and the client is told now (review F-6).
	if isCreate {
		refused, err := c.createInDroppedGeneration(entry)
		if err != nil || refused != nil {
			c.shipOrder.route(ticket, nil)
			return ReplResponse{}, withOutcome(outcomeNotApplied, errors.Join(refused, err))
		}
	}

	// The owner re-checks, after its local apply, that it still owns the
	// bucket (spec §19.3.7): a membership adopted while the write was in
	// flight has moved the bucket, and this node will not ship the entry, so
	// the client must not be told it is replicated.
	if !c.actsAsOwner(key) {
		// A data entry is in this node's term, and a ceded owner finishes
		// delivering its term (spec §20.8.3): it is queued now for the
		// members the sender's rule addresses it to (BUG-0528). A create or
		// a drop is left as it was: an orphaned create is shipped to nobody
		// and handed off (review F-11).
		if !isCreate && transport.Opcode(entry.Op) != transport.OpDeleteBucket {
			c.queueCededEntry(ctx, ticket, entry, epoch)
		}
		c.shipOrder.route(ticket, nil)
		if hook := afterMovedRouteHook.Load(); hook != nil {
			(*hook)(c.self.ID, entry.Bucket)
		}
		orphaned = isCreate
		// Only this node is known to hold the write; a pop's entry is
		// answered all the same (answerShortfall).
		return c.answerShortfall(resp, entry, applied, applyErr, 1, addressed.factor,
			withOutcome(outcomeUnknown, ownershipMovedErr(database, entry.Bucket)))
	}

	replicas, concernSet := addressed.replicas, addressed.concernSet
	c.shipOrder.route(ticket, nodeIDs(replicas))

	// Route to the replicas. The entry is routed with its sequence number, and
	// the write concern is satisfied only by peers confirming that sequence.
	// It is routed behind every lower sequence bound for the same peer, because
	// the peer's acknowledgement is a high-water mark (shipOrder).
	//
	// Routing never waits for a peer: a stream that cannot take the entry now
	// delivers it from the log (stream_state.go). So every replica the entry is
	// addressed to is waited for, queued or not, and a write is never answered
	// with an error because a queue was full. The turn is
	// bounded by the write-concern budget, because it waits for earlier writers
	// and not for a peer, and not by the caller (routeContext).
	routeCtx, routeCancel := c.routeContext(ctx)
	peers := make([]*peer, 0, len(replicas))
	var refusedErr error
	for _, n := range replicas {
		if err := c.refuseShipToPlaintextReplica(n, entry); err != nil {
			refusedErr = errors.Join(refusedErr, err)
			c.holdBackFromRefusedReplica(n, seq, err)
			continue
		}
		p := c.peerFor(n)
		c.enqueueInOrder(routeCtx, ticket, p, entry)
		if concernSet == nil || nodeListed(concernSet, n.ID) {
			peers = append(peers, p)
		}
	}
	routeCancel()
	c.shipOrder.close(ticket)

	// The bucket vanished on this node between the owner join fence
	// (handleClientFrame) and this write's local apply turn — a drop landed
	// in the window. Every replica applies the same bucket's WAL
	// entries in the same sequence order, so none will ever acknowledge this
	// one either: waiting out the write concern timeout only delays an
	// answer the local apply already gave, leaving the client unacknowledged
	// for no reason. The entry was still queued above, so a replica's own
	// apply of it stays consistent with the owner's refusal.
	// A drop is the exception (R8-D (d), spec §19.4 P4): routed to every
	// member, it may still apply on a copy this node no longer holds, so a drop
	// not applied here is unknown, never not_applied.
	if applyErr != nil && transport.Opcode(entry.Op) != transport.OpDeleteBucket &&
		(errors.Is(applyErr, ErrBucketNotFound) || errors.Is(applyErr, ErrJoinSyncIncomplete) || errors.Is(applyErr, ErrVersionLimit)) {
		return resp, withOutcome(outcomeNotApplied, errors.Join(applyErr, refusedErr))
	}

	// The concern is resolved at the factor read with the entry's addressees,
	// inside the fence (F1): a create's is the one it registered, a drop's the
	// one it forgot, any other entry's the one it was logged at, whatever an
	// alter has changed since the fence was released.
	factor := addressed.factor
	if wc == 0 {
		wc = c.defaultWriteConcernFor(factor)
	}
	waitCtx := ctx
	if isCreate {
		// A replica that holds the create's generation dropped refuses it, and
		// its refusal teaches this node the drop (G-11 (a)): the client is then
		// told nothing was created, before any write to the bucket is accepted,
		// not after the concern times out.
		var stopWatching func()
		waitCtx, stopWatching = c.untilCreateRefused(ctx, entry)
		defer stopWatching()
	}
	confirmed, err := c.waitQuorum(waitCtx, quorumWait{database: database, bucket: entry.Bucket, key: key, seq: seq, replicas: peers, wc: wc})
	if err != nil {
		if isCreate {
			if refused, checkErr := c.createInDroppedGeneration(entry); refused != nil || checkErr != nil {
				return ReplResponse{}, withOutcome(outcomeNotApplied, errors.Join(refused, checkErr))
			}
		}
		// The local apply already happened, so report both facts honestly:
		// the caller learns the write is not durable on enough copies. A
		// write the owner failed to apply after logging it may yet be applied
		// by a replay; one whose wait timed out is stored here.
		if applyErr != nil || applied.ownerLacksWrite() {
			return resp, withOutcome(outcomeUnknown, errors.Join(applyErr, refusedErr, err))
		}
		if errors.Is(err, ErrWriteConcernTimeout) {
			return c.answerShortfall(resp, entry, applied, applyErr, confirmed, factor, storedWith(confirmed, factor, errors.Join(err, refusedErr)))
		}
		return c.answerShortfall(resp, entry, applied, applyErr, confirmed, factor, withOutcome(outcomeUnknown, errors.Join(refusedErr, err)))
	}
	// A replica that refuses the create stalls its stream, which takes it out
	// of the in-sync set and can end the wait with the owner alone; the drop
	// its refusal carried was learned before the stall, so it is read here.
	if isCreate {
		if refused, checkErr := c.createInDroppedGeneration(entry); refused != nil || checkErr != nil {
			return ReplResponse{}, withOutcome(outcomeNotApplied, errors.Join(refused, checkErr))
		}
	}
	if applyErr != nil {
		return resp, withOutcome(outcomeUnknown, applyErr)
	}
	// A replica refused for being plaintext is the one routing failure that is
	// still the client's to hear: the entry is applied here and will not reach
	// that peer (D3).
	if refusedErr != nil {
		return c.answerShortfall(resp, entry, applied, applyErr, confirmed, factor, storedWith(confirmed, factor, refusedErr))
	}
	return c.stampWriteOutcome(resp, entry, applied, confirmed, factor)
}

// stampAtOwner is the payload the bucket's owner logs for an entry, with the
// values it fixes for every applier: the payload is what the owner, every
// replica and every replay applies, so a decision an applier makes from a
// node's own setting must be made from the owner's, written here. A pop's
// result_ceiling is set to this node's ceiling, replacing whatever the client
// sent under that name in any case, because the decoder reads a member that
// differs only in case: every copy then refuses the pop, or every copy removes
// the same entries (pop-n spec section 3.3). A bucket pop and the lease verbs
// also carry lease_now, this node's clock, and a claim a lease_nonce drawn
// here, so every copy runs the return step at one time and composes the same
// lease id (claim-ack-spec 5.1, CA-14, CA-18). Any other entry is logged as it
// came.
func (c *Cluster) stampAtOwner(entry ReplEntry) ([]byte, error) {
	var stamps bson.D
	switch transport.Opcode(entry.Op) {
	case transport.OpFieldPop:
		// A store field has no leases (claim-ack-spec D-15): the ceiling alone.
		stamps = bson.D{{Key: "result_ceiling", Value: c.s.maxResultBytes()}}
	case transport.OpFIFOPop, transport.OpLIFOPop, transport.OpHeapPop:
		// Stamped before any state is read: the pop's return step judges
		// every lease by this time on every copy (claim-ack-spec 5.1).
		stamps = bson.D{{Key: "result_ceiling", Value: c.s.maxResultBytes()}, {Key: "lease_now", Value: c.s.leaseClockMillis()}}
	case transport.OpQueueClaim:
		var nonce [8]byte
		if _, err := rand.Read(nonce[:]); err != nil {
			return nil, fmt.Errorf("drawing the lease nonce of a claim of bucket %q: %w", entry.Bucket, err)
		}
		stamps = bson.D{{Key: "lease_now", Value: c.s.leaseClockMillis()}, {Key: "lease_nonce", Value: nonce[:]}, {Key: "result_ceiling", Value: c.s.maxResultBytes()}}
	case transport.OpQueueAck, transport.OpQueueRelease, transport.OpQueueExtend:
		stamps = bson.D{{Key: "lease_now", Value: c.s.leaseClockMillis()}}
	default:
		return entry.Payload, nil
	}
	// stampPayload removes a client's member of each name in any letter case
	// before it appends the owner's, so no applier reads a client's clock,
	// nonce or ceiling (D4 verification finding S1).
	stamped, err := stampPayload(entry.Payload, stamps)
	if err != nil {
		return nil, fmt.Errorf("stamping the %s of bucket %q with its owner's %v: %w", transport.Opcode(entry.Op), entry.Bucket, stampNames(stamps), err)
	}
	return stamped, nil
}

// stampNames is the names of the members stampAtOwner writes, for an error.
func stampNames(stamps bson.D) []string {
	names := make([]string, len(stamps))
	for index, stamp := range stamps {
		names[index] = stamp.Key
	}
	return names
}

// fencedAddressing is who an entry is addressed to and the factor its concern
// resolves at, read inside the bucket's fence (spec §6.2 rule F1), with the
// entry as stamped there.
type fencedAddressing struct {
	entry    ReplEntry
	replicas []ring.Node
	// concernSet is the addressees whose copies count toward the concern;
	// nil: every addressee counts. A drop is routed to every member but its
	// concern is the set's at the factor it was placed at (R8-C): a member
	// outside the set acknowledges a copy the client never counted. An alter
	// is routed to the union of its two sets and its concern is the smaller
	// set's (alterConcernSet).
	concernSet []ring.Node
	// factor is the factor the concern resolves at and the write reports. A
	// create's is read after its own apply, which registers it.
	factor int
}

// addressInFence is rule F1 (spec §6.2): an entry's addressees and the factor
// its concern resolves at are computed inside the bucket's fence, just after
// the ownership check, and every later use in the write's path uses them. An
// alter or a seeding snapshot changes them only under this fence taken
// exclusively, so they are the ones in force when the entry is logged; read
// before the fence, a write held there could be addressed at a factor an alter
// has since changed. A drop and an alter are stamped first, because who they
// are addressed to is read from their stamp. An alter that changes nothing is
// answered here, and nothing of it is logged (R9).
func (c *Cluster) addressInFence(entry ReplEntry, key placementKey, epoch uint64) (fencedAddressing, *ReplResponse, error) {
	addressed := fencedAddressing{}
	var err error
	switch transport.Opcode(entry.Op) {
	case transport.OpAlterBucket:
		var unchanged *ReplResponse
		if entry, unchanged, err = c.stampAlter(entry, epoch); err != nil || unchanged != nil {
			return addressed, unchanged, err
		}
	case transport.OpDeleteBucket:
		// The drop's factor is stamped on it here, before it is logged,
		// because its own apply forgets the factor.
		var dropped int32
		if entry, dropped, err = c.stampDrop(entry, key); err != nil {
			return addressed, nil, err
		}
		addressed.factor = int(dropped)
	}
	addressed.entry = entry
	if addressed.replicas, err = c.deliverTo(transport.Opcode(entry.Op), entry.Bucket, entry.Payload); err != nil {
		return addressed, nil, err
	}
	switch transport.Opcode(entry.Op) {
	case transport.OpDeleteBucket:
		addressed.concernSet, err = c.deliveryTargets(entry, key)
	case transport.OpAlterBucket:
		addressed.factor = c.effectiveRF(key)
		addressed.concernSet, err = c.alterConcernSet(entry, key)
	case transport.OpCreateBucket:
	default:
		addressed.factor = c.effectiveRF(key)
	}
	return addressed, nil, err
}

// answerShortfall answers a write this node logged and applied but cannot
// answer as asked: shortfall says why (the write concern was not met, the
// bucket's owner changed, a replica was refused), and it is the answer for
// every write but two. A pop that removed an entry here is answered with its
// response instead, stamped as a write with fewer copies than its factor is
// (BUG-0548): the removal stands on this node and reaches the log, so the
// response is the only copy of the entry, and an error in its place loses it.
// A claim that took entries is answered with its lease the same way
// (claim-ack-spec 5.4), in its own words: lost, its entries come back only
// when the lease ends.
// confirmed and factor are the copies that hold the removal and the bucket's
// factor; write_outcome applied and the message say what fell short.
//
// A write the handler refused is answered with its refusal too (BUG-0557):
// it wrote nothing, and every applier holding the same state refuses it
// alike, so the shortfall's "stored on the owner" would be false. The refusal
// already carries its outcome (handlerRefusalWithOutcome): not_applied, or
// unknown when this node owes the bucket applies.
func (c *Cluster) answerShortfall(resp ReplResponse, entry ReplEntry, applied *applyOutcome, applyErr error,
	confirmed, factor int, shortfall error) (ReplResponse, error) {
	if applyErr == nil && !applied.ownerLacksWrite() && resp.Opcode == int32(transport.OpError) {
		return resp, nil
	}
	if !poppedAnEntry(entry, resp, applied, applyErr) {
		return resp, shortfall
	}
	message := poppedShortMessage(poppedItemCount(resp.Payload), confirmed, factor, shortfall)
	warning := "a pop is answered with its entry although its write fell short; the removal stands"
	if !isPopOpcode(transport.Opcode(entry.Op)) {
		message = claimedShortMessage(confirmed, factor, shortfall)
		warning = "a claim is answered with its lease although its write concern was not met"
	}
	payload, err := restampedAnswer(resp.Payload, bson.D{
		{Key: "message", Value: message},
		{Key: "copies_confirmed", Value: int32(confirmed)},
		{Key: "replication_factor", Value: int32(factor)},
		{Key: "write_outcome", Value: outcomeApplied}})
	if err != nil {
		return resp, errors.Join(shortfall, fmt.Errorf("stamping the answer to %s on bucket %q, which holds the entries it took: %w",
			transport.Opcode(entry.Op), entry.Bucket, err))
	}
	c.logger.Warn(warning,
		"op", transport.Opcode(entry.Op), "bucket", entry.Bucket, "copies_confirmed", confirmed, "replication_factor", factor, "reason", shortfall)
	resp.Payload = payload
	return resp, nil
}

// poppedAnEntry reports whether a write applied on this node took entries and
// holds them in resp: a bucket pop or a store-field pop, or a claim, whose
// store committed and that answered found.
func poppedAnEntry(entry ReplEntry, resp ReplResponse, applied *applyOutcome, applyErr error) bool {
	if applyErr != nil || applied.ownerLacksWrite() {
		return false
	}
	return answerHoldsPoppedEntry(transport.Opcode(entry.Op), transport.Opcode(resp.Opcode), resp.Payload)
}

// answerHoldsPoppedEntry reports whether an answer of opcode answerOp to a
// request of opcode op holds the entries the write took: a pop's or a
// claim's, not an error frame, and found. A claim answers found only when it
// took an entry, so one that only returned or dead-lettered keeps the error
// any other write gets.
func answerHoldsPoppedEntry(op, answerOp transport.Opcode, payload []byte) bool {
	if !answersWithTakenEntries(op) || answerOp == transport.OpError {
		return false
	}
	found, _ := bson.Raw(payload).Lookup("found").BooleanOK()
	return found
}

// restampedAnswer is a response payload with the members stamps names
// replaced by stamps, appended after the members it keeps.
func restampedAnswer(payload []byte, stamps bson.D) ([]byte, error) {
	var document bson.D
	if err := bson.Unmarshal(payload, &document); err != nil {
		return nil, fmt.Errorf("decoding the answer: %w", err)
	}
	replaced := make(map[string]bool, len(stamps))
	for _, stamp := range stamps {
		replaced[stamp.Key] = true
	}
	restamped := make(bson.D, 0, len(document)+len(stamps))
	for _, member := range document {
		if !replaced[member.Key] {
			restamped = append(restamped, member)
		}
	}
	encoded, err := bson.Marshal(append(restamped, stamps...))
	if err != nil {
		return nil, fmt.Errorf("encoding the answer: %w", err)
	}
	return encoded, nil
}

// answersWithTakenEntries reports whether op's answer can hold entries the
// write took off its queue, so that the answer is their only copy: a pop
// removed them, a claim moved them under a lease nobody else knows. It decides
// whether BUG-0548's rule answers a write that fell short; isPopOpcode decides
// whether the words are pop's (claim-ack-spec 5.4).
func answersWithTakenEntries(op transport.Opcode) bool {
	return isPopOpcode(op) || op == transport.OpQueueClaim
}

// isPopOpcode reports whether op removes an entry and answers with it: the
// bucket pops of a fifo, a lifo and a heap bucket, and the store-field pop.
func isPopOpcode(op transport.Opcode) bool {
	switch op {
	case transport.OpFIFOPop, transport.OpLIFOPop, transport.OpHeapPop, transport.OpFieldPop:
		return true
	}
	return false
}

// poppedItemCount is how many entries a pop's answer holds: the length of its
// items array, and one for an answer that has none (a store-field pop's answer
// until it answers items).
func poppedItemCount(payload []byte) int {
	items, ok := bson.Raw(payload).Lookup("items").ArrayOK()
	if !ok {
		return 1
	}
	elements, err := items.Values()
	if err != nil || len(elements) == 0 {
		return 1
	}
	return len(elements)
}

// poppedShortMessage is the message of a pop answered with its entries by
// answerShortfall. A retry of a pop takes the next entries, so it says they
// are here and nowhere else. At one entry the text is the one BUG-0548 settled.
func poppedShortMessage(entries, confirmed, factor int, shortfall error) string {
	reason := shortfall.Error()
	switch {
	case errors.Is(shortfall, ErrOwnershipMoved):
		reason = "the bucket's owner changed while the pop was in flight"
	case errors.Is(shortfall, ErrWriteConcernTimeout):
		reason = "its write concern was not met in time"
	}
	where := "The entry is in this answer and nowhere else; do not pop again to get it back, because a pop takes the next entry"
	if entries > 1 {
		where = fmt.Sprintf("The %d entries are in this answer and nowhere else; do not pop again to get them back, because a pop takes the next entries", entries)
	}
	return fmt.Sprintf("popped, but %s: %d of %d copies are known to hold the removal. %s", reason, confirmed, factor, where)
}

// claimedShortMessage is the message of a claim answered with its lease by
// answerShortfall. Unlike a pop's entries, a claim's come back when the lease
// ends, so claiming again is safe.
func claimedShortMessage(confirmed, factor int, shortfall error) string {
	reason := shortfall.Error()
	switch {
	case errors.Is(shortfall, ErrOwnershipMoved):
		reason = "the bucket's owner changed while the claim was in flight"
	case errors.Is(shortfall, ErrWriteConcernTimeout):
		reason = "its write concern was not met in time"
	}
	return fmt.Sprintf("claimed, but %s: %d of %d copies are known to hold the lease. The lease and its entries are in this answer: "+
		"process and ack them as usual. If the lease is lost with a copy, the entries return to the queue when the lease ends; claim again",
		reason, confirmed, factor)
}

// handlerRefusalWithOutcome gives the error frame a handler answered a logged
// entry of the bucket with its outcome. A store failure is unknown: the entry
// is shipped, the replicas apply it, and this node owes it and applies it on
// replay (noteApplyOutcome). So is a batch too large for this node's store:
// the bound follows each node's memtable, and a replica with a larger one
// applies the entry. Any other refusal is the handler's own answer to
// the entry — a $inc of a string — which every applier gives alike only if they
// hold the same state: not_applied, unless this node owes an apply of the
// bucket. An owner that owes one lags its replicas, which can apply what it
// refused (a $inc after a $set it failed), so then the outcome is unknown.
func (c *Cluster) handlerRefusalWithOutcome(resp ReplResponse, op transport.Opcode, applied *applyOutcome, database, bucket string) ReplResponse {
	// The owner's own answer to the client, not what it ships: the entry's
	// OwnerOutcome was read from the in-band shape before this call.
	if isOverBoundReply(transport.Opcode(resp.Opcode), applied) {
		resp = ReplResponse{Opcode: int32(transport.OpError), Payload: errorFrame(&transport.Frame{}, applied.overBound).Payload}
	}
	if resp.Opcode != int32(transport.OpError) {
		return resp
	}
	outcome := outcomeNotApplied
	// A drop was routed to every member, which may apply it where this node
	// could not (R8-D (d), spec §19.4 P4).
	if op == transport.OpDeleteBucket || applied.ownerLacksWrite() || (bucket != "" && c.s.bucketOwesApplies(database, bucket)) {
		outcome = outcomeUnknown
	}
	resp.Payload = refusalWithOutcome(resp.Payload, outcome)
	return resp
}

// stampWriteOutcome records on a data mutation's response how many copies
// hold the write and the bucket's stored factor (spec §19.3.11). DDL is not
// stamped, nor an error frame: an error carries its count in its own fields
// (errorFrame). A response that answers the owner's own store failure —
// success false rather than an error frame — also says write_outcome unknown:
// the entry is logged and shipped, the replicas apply it, and this node owes it
// and applies it on replay (noteApplyOutcome), so a retry can duplicate it.
// So does one that answers the store's refusal of a batch over its
// atomic bound: the bound follows each node's memtable, and a replica with a
// larger one applies what this node refused.
func (c *Cluster) stampWriteOutcome(resp ReplResponse, entry ReplEntry, applied *applyOutcome, confirmed, factor int) (ReplResponse, error) {
	if !carriesWriteOutcome(transport.Opcode(entry.Op)) || resp.Opcode == int32(transport.OpError) || len(resp.Payload) == 0 {
		return resp, nil
	}
	var document bson.D
	if err := bson.Unmarshal(resp.Payload, &document); err != nil {
		return resp, fmt.Errorf("decoding the response to %s on bucket %q to record its copy count: %w",
			transport.Opcode(entry.Op), entry.Bucket, err)
	}
	// The wait counts the owner as a copy. One that does not hold the write is
	// left out, so the count still means at least that many nodes hold it
	// (spec §19.4 P3); what the wait needed is unchanged.
	if applied.ownerLacksWrite() {
		confirmed--
	}
	document = append(document,
		bson.E{Key: "copies_confirmed", Value: int32(confirmed)},
		bson.E{Key: "replication_factor", Value: int32(factor)})
	if applied.ownerLacksWrite() {
		document = append(document, bson.E{Key: "write_outcome", Value: outcomeUnknown})
	}
	stamped, err := bson.Marshal(document)
	if err != nil {
		return resp, fmt.Errorf("encoding the response to %s on bucket %q with its copy count: %w",
			transport.Opcode(entry.Op), entry.Bucket, err)
	}
	resp.Payload = stamped
	return resp, nil
}

// carriesWriteOutcome reports whether an opcode's response is a data
// mutation's, the kind that tells the client how many copies hold it. Bucket
// and database DDL are not: their placement is the bucket's set, and their
// response says what was created or dropped.
func carriesWriteOutcome(op transport.Opcode) bool {
	switch op {
	case transport.OpCreateBucket, transport.OpDeleteBucket, transport.OpDeclareStoreFields, transport.OpAlterBucket,
		transport.OpCreateDB, transport.OpDeleteDB:
		return false
	}
	return true
}

// applyInTurn waits for the write's apply turn among its bucket's writes, then
// applies it locally with seq as its apply sequence, and retires the ticket
// whatever the outcome.
//
// The wait takes the cluster's context, not the request's: the entry is in the
// WAL already, and a caller that gave up must not leave it appended and
// unapplied on a node that keeps serving. The wait is bounded by the applies
// ahead of it, and a shutdown still ends it.
func (c *Cluster) applyInTurn(ctx context.Context, entry *fenceEntry, ticket *applyTicket, repl ReplEntry, seq uint64) (ReplResponse, error) {
	defer entry.closeApply(ticket)
	if err := entry.waitApplyTurn(c.ctx, ticket); err != nil {
		return ReplResponse{}, fmt.Errorf("applying %s for bucket %q at seq %d: %w", transport.Opcode(repl.Op), repl.Bucket, seq, err)
	}
	if hook := beforeLocalApplyHook.Load(); hook != nil {
		(*hook)(c.self.ID, repl)
	}
	resp, err := c.applyLocal(withApplySeq(ctx, seq), repl)
	// The origin's refusal is recorded before the turn closes, so the state
	// document's rewrite is ordered with the bucket's other applies (R8-E,
	// N-1): an owner re-sending the entry from its log then reads it.
	if lineageFrom(ctx).originated {
		applied := applyOutcomeFrom(ctx)
		if recordErr := c.s.recordOriginRefusal(transport.Opcode(repl.Op), repl, seq, applyOutcomeOf(resp, err),
			applied != nil && applied.storeFailure != nil); recordErr != nil {
			c.logger.Error("could not record a refused write; a replica that refuses it too may be called diverged when it is re-sent from the log",
				"bucket", repl.Bucket, "seq", seq, "error", recordErr)
		}
	}
	return resp, err
}

// applyLocal executes an entry against this node's own storage.
func (c *Cluster) applyLocal(ctx context.Context, entry ReplEntry) (ReplResponse, error) {
	frame := &transport.Frame{
		Opcode:  transport.Opcode(entry.Op),
		Payload: entry.Payload,
	}
	// Only a mutation is refused for want of a local store. It is what panics
	// deep in the store layer on an engine that is not open; a read reaches its
	// handler, which answers "bucket not found" with the command that fixes it.
	// A read is forwarded here by a node that is in the bucket's replica set by
	// its own arithmetic and holds nothing, so the owner is asked about buckets
	// that may never have existed.
	if replicationRequired(transport.Opcode(entry.Op)) {
		if err := c.refuseUnservable(entry, fromPeer(ctx)); err != nil {
			return ReplResponse{}, err
		}
	}
	// Carry the write's version into the handler so it can stamp the
	// read-repair version side-key: the term the entry was logged in and the
	// origin's sequence for it, the same pair on the owner, every replica and
	// every replay, so all of them write the same version for one logical
	// write. On a replica entry.Term is the sender's term at shipping, not the
	// term the entry was logged in; the two are equal only because a sender
	// ships an own entry only while its copy is opened and the entry is of the
	// copy's term (Cluster.bucketEntryAddressing). Were it to ship an entry of
	// an earlier term, the entry would pack in the later one and outrank every
	// write of the terms between. A version that cannot be packed is refused
	// before the dispatch, alike on every node, and is no store failure: a
	// retry could never succeed. The payload was stamped before it was logged,
	// so its generations are the cluster's word.
	version, err := packVersion(entry.Term, entry.Seq)
	if err != nil {
		_, database := decodeFrameMeta(entry.Payload).scope(transport.Opcode(entry.Op))
		return ReplResponse{}, fmt.Errorf("applying sequence %d: %w", entry.Seq, versionLimitRefusal(database, entry.Bucket, err))
	}
	ctx = withClusterApply(withWriteVersion(ctx, version))
	resp, err := c.s.dispatch(ctx, frame)
	if err != nil {
		return ReplResponse{}, err
	}
	if resp == nil {
		return ReplResponse{}, nil
	}
	return ReplResponse{Opcode: int32(resp.Opcode), Payload: resp.Payload, onWritten: resp.TakeWrittenHook()}, nil
}

// refuseUnversionable refuses an entry of a bucket whose (term, sequence)
// cannot be packed into a version (packVersion), naming the bucket and the
// way out.
func refuseUnversionable(entry ReplEntry, term, seq uint64) error {
	if _, err := packVersion(term, seq); err != nil {
		_, database := decodeFrameMeta(entry.Payload).scope(transport.Opcode(entry.Op))
		return versionLimitRefusal(database, entry.Bucket, err)
	}
	return nil
}

// refuseUnservable returns the refusal for an entry this node cannot apply yet,
// or nil. applyAsReplica asks before it appends — a refused push is re-sent
// every replRetryInterval, and appending each re-send grew a fenced replica's
// WAL by one entry per retry for as long as it stayed fenced — and
// applyLocal asks again, because a drop can close the store in between.
// fromPeer is true for another node's entry applied here as a replica.
func (c *Cluster) refuseUnservable(entry ReplEntry, fromPeer bool) error {
	op := transport.Opcode(entry.Op)
	// This node can be a replica for entry.Bucket without having finished
	// receiving it yet (mid-join, mid-rebalance): the bucket exists in the
	// shared catalog everywhere, but this node's local storage engine for it
	// is not open. The join fence only fences the *owner's* local apply
	// (Cluster.write, handleReplPush); a non-owner replica reaches applyLocal
	// directly from handleReplPush with no equivalent guard, and dispatching
	// into a bucket whose engine is nil panics deep in the store layer
	// instead of failing the entry the way a fenced write does. Bucket/DB
	// DDL is exempt for the same reason fenceUnheldBucket exempts it: a create
	// is what opens the store, so there is nothing to check yet.
	switch op {
	case transport.OpCreateBucket:
		// The one exception to the exemption: while a snapshot pull of that
		// bucket is landing on this node (beginLanding), a replicated
		// CREATE_BUCKET is the bucket's history arriving ahead of the snapshot
		// that will populate it. Creating the bucket here would catalogue it
		// empty, the pull's materialise would then refuse a bucket this node
		// already serves and forget its position, and the entries the
		// position passed over meanwhile would be acknowledged and never
		// applied — for every engine, since queues and blob buckets migrate
		// too (build doc Step 18). It is refused only then: a pull that has
		// not recorded its position yet finds the bucket served, under the
		// same fence, and stops, and the stream delivers the rest. Refusing
		// for the whole pull failed the stream's frame for as long as the
		// pull ran, and took the peer out of the in-sync set for every bucket
		// (BUG-0729). handleReplPush waits for the landing first
		// (awaitLanding), so the refusal is met only by a landing that
		// outlasts that wait: the frame fails, the sender re-sends it, and
		// once the copy is in place the entry is at or below the recorded
		// snapshot position and is skipped as already held. This node's own
		// create keeps the wider refusal: the take-over worker's marker
		// (pullPending) is what its write path waits on
		// (Cluster.fenceAsSettledOwner).
		name, database := decodeFrameMeta(entry.Payload).scope(op)
		pending := c.pullLanding(database, name) != nil || (!fromPeer && c.pullPending(database, name))
		if pending || c.copyBeingReplaced(database, name) {
			return fmt.Errorf("%w: bucket %q in database %q", errSnapshotPending, name, database)
		}
		// G-6 (a) on a replica, before anything is logged: a create in a
		// generation this node knows dropped is refused, never acknowledged —
		// an acknowledgement would count toward the create's concern for a
		// copy that does not exist (rev 3, A-1 (c)). The owner learns the drop
		// from the refusal (G-11 (a)).
		if refused, err := c.createInDroppedGeneration(entry); err != nil || refused != nil {
			return errors.Join(refused, err)
		}
	case transport.OpDeleteBucket, transport.OpCreateDB, transport.OpDeleteDB:
	case transport.OpAlterBucket:
		// A member an increase newly places holds nothing, and is the node that
		// most needs the alter (bucket-replication-rev8-spec §4.4): exempt. A
		// copy that is catalogued here and not open yet is refused as by the
		// default arm, and applies the alter once it has landed.
		bucket, database := decodeFrameMeta(entry.Payload).scope(op)
		if _, held := c.s.peekBucket(database, bucket); held && (!c.s.bucketStoreOpen(database, bucket) || c.copyBeingReplaced(database, bucket)) {
			return c.missingBucketErr(database, bucket)
		}
	default:
		if bucket, database := decodeFrameMeta(entry.Payload).scope(op); bucket != "" {
			// A copy a pull is replacing serves reads and takes no entry: one
			// applied after the source's snapshot would be lost with the copy
			// (copyBeingReplaced). The sender re-sends it.
			if !c.s.bucketStoreOpen(database, bucket) || c.copyBeingReplaced(database, bucket) {
				return c.missingBucketErr(database, bucket)
			}
		}
	}
	return nil
}

// appendWAL records an entry durably before it is applied, and returns the
// sequence number that acknowledgements are measured against. The origin says
// whether this node originated the entry or applied it from another node's
// stream: a replay decides what it may ship by reading it back, so
// no caller may leave it out.
func (c *Cluster) appendWAL(entry ReplEntry, origin storage.WALOrigin) (uint64, error) {
	if c.s.wal == nil {
		return 0, ErrNoWAL
	}
	seq, err := c.s.wal.AppendFrom(entry.Bucket, uint8(entry.Op), entry.Key, entry.Payload, origin)
	if err != nil {
		return 0, fmt.Errorf("append WAL for %s on bucket %q: %w", transport.Opcode(entry.Op), entry.Bucket, err)
	}
	c.noteOriginFormat()
	return seq, nil
}

// appendWALUnsynced is appendWAL without the wait for the fsync: the entry is
// not durable, and must be neither applied nor acknowledged, until
// syncWALThrough has returned for its sequence (replicaRun).
func (c *Cluster) appendWALUnsynced(entry ReplEntry, origin storage.WALOrigin) (uint64, error) {
	if c.s.wal == nil {
		return 0, ErrNoWAL
	}
	seq, err := c.s.wal.AppendUnsyncedFrom(entry.Bucket, uint8(entry.Op), entry.Key, entry.Payload, origin)
	if err != nil {
		return 0, fmt.Errorf("append WAL for %s on bucket %q: %w", transport.Opcode(entry.Op), entry.Bucket, err)
	}
	c.noteOriginFormat()
	return seq, nil
}

// queueCededEntry puts a data entry this node logged in its term, which found
// its bucket moved after it was logged, on the queue of each member the
// sender's rule addresses it to (bucketEntryAddressing, as a replay of the log
// would). A stream that reopened after the adoption took its boundary below
// the entry and reads nothing above it, so the queue is the only way the entry
// reaches a streaming peer. The caller's ticket holds every later write's turn
// until this returns, so the entry is queued in sequence order, and no advance
// notice can pass it before it is on the queue. An entry whose addressing
// cannot be read is left to the log, as before this path queued anything.
func (c *Cluster) queueCededEntry(ctx context.Context, ticket *shipTicket, entry ReplEntry, epoch uint64) {
	targets, err := c.cededTargets(entry, epoch)
	if err != nil {
		c.logger.Warn("a write logged in this node's term finished after its bucket moved, and could not be addressed; "+
			"it is delivered from the log when each stream is next opened",
			"bucket", entry.Bucket, "seq", entry.Seq, "error", err)
		return
	}
	c.shipOrder.route(ticket, nodeIDs(targets))
	routeCtx, cancel := c.routeContext(ctx)
	defer cancel()
	for _, member := range targets {
		if refused := c.refuseShipToPlaintextReplica(member, entry); refused != nil {
			c.logger.Warn("a write logged in this node's term is not sent to a member not known to encrypt its store",
				"bucket", entry.Bucket, "seq", entry.Seq, "node_id", member.ID, "error", refused)
			// Stalled, as applyAsOwner stalls its refused replicas: skipped
			// without a stall, the member's next acknowledgement or advance
			// notice moves its cursor past the entry (BUG-0604).
			c.holdBackFromRefusedReplica(member, entry.Seq, refused)
			c.shipOrder.queued(ticket, member.ID)
			continue
		}
		c.enqueueInOrder(routeCtx, ticket, c.peerFor(member), entry)
	}
}

// cededTargets is who the sender's rule addresses a data entry this node
// logged under epoch to, read as a replay of the log reads it.
func (c *Cluster) cededTargets(entry ReplEntry, epoch uint64) ([]ring.Node, error) {
	logged := storage.WALEntry{Seq: entry.Seq, Op: byte(entry.Op), Bucket: entry.Bucket, Key: entry.Key, Data: entry.Payload,
		Origin: storage.WALOrigin{Kind: storage.WALOriginOwn, Epoch: epoch}}
	st, err := c.loggedState(make(map[loggedBucket]store.BucketState, 1), logged)
	if err != nil {
		return nil, fmt.Errorf("reading the state of the bucket of sequence %d: %w", entry.Seq, err)
	}
	targets, _, err := c.bucketEntryAddressing(logged, st)
	if err != nil {
		return nil, fmt.Errorf("deciding who sequence %d is addressed to: %w", entry.Seq, err)
	}
	return targets, nil
}

// walLastSequence is the WAL's last assigned sequence, or 0 without a WAL; a
// ship ticket opened then is closed at once, because appendWAL refuses the write.
func (c *Cluster) walLastSequence() uint64 {
	if c.s.wal == nil {
		return 0
	}
	return c.s.wal.LastSequence()
}

// routeContext is the context a logged entry waits for its routing turn
// under: the write-concern budget, and not the caller. The entry is in the
// WAL already (applyInTurn's rule), and a turn given up because the caller
// went away stalls the peer's stream, which takes the replica out of the
// in-sync set until the log replay brings it back, and every concurrent
// write's concern is counted without it meanwhile. Only the wait for the
// concern (waitQuorum) ends with the caller.
func (c *Cluster) routeContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), c.wcTimeout)
}

func nodeIDs(nodes []ring.Node) []string {
	ids := make([]string, 0, len(nodes))
	for _, n := range nodes {
		ids = append(ids, n.ID)
	}
	return ids
}

// broadcastMutation applies a mutation on every member. It is used for
// database-level DDL, which has no bucket to place by. Ordering still holds:
// each peer drains a single FIFO queue, so a bucket created after its database
// always arrives after the database creation.
func (c *Cluster) broadcastMutation(ctx context.Context, entry ReplEntry, database string) (ReplResponse, error) {
	// As applyAsOwner: the entry is this node's own and ships under its
	// sequence (start_hold.go).
	if err := c.startHold.admit(ctx); err != nil {
		return ReplResponse{}, withOutcome(outcomeNotApplied, err)
	}
	// A broadcast shares the peers' queues and their high-water apply gate with
	// bucket writes, so it takes a ship ticket like applyAsOwner does.
	ticket := c.shipOrder.open(c.walLastSequence)
	defer c.shipOrder.close(ticket)
	unappliedFloor := c.unapplied.open(c.walLastSequence)
	seq, err := c.appendWAL(entry, storage.WALOrigin{Kind: storage.WALOriginOwn, Epoch: c.Epoch()})
	if err != nil {
		c.unapplied.close(unappliedFloor)
		return ReplResponse{}, err
	}
	c.shipOrder.assign(ticket, seq)
	entry.Seq = seq
	applyCtx, applied := withApplyOutcome(ctx)
	resp, applyErr := c.applyLocal(applyCtx, entry)
	resp = c.handlerRefusalWithOutcome(resp, transport.Opcode(entry.Op), applied, database, entry.Bucket)
	c.unapplied.close(unappliedFloor)

	others, err := c.deliverTo(transport.Opcode(entry.Op), entry.Bucket, entry.Payload)
	if err != nil {
		return resp, errors.Join(applyErr, err)
	}
	c.shipOrder.route(ticket, nodeIDs(others))

	// Broadcast entries are not waited for. A stream that cannot take the entry
	// now delivers it from the log, as it does a bucket's entry.
	bctx, cancel := c.routeContext(ctx)
	defer cancel()
	for _, n := range others {
		c.enqueueInOrder(bctx, ticket, c.peerFor(n), entry)
	}
	c.logger.Debug("broadcast mutation to all members",
		"op", transport.Opcode(entry.Op), "database", database, "seq", seq)
	return resp, applyErr
}

// forwardTargetHook, when set, observes the node a coordinator forwards an
// entry to. A test uses it to record the request's actual forward target,
// which the answer does not carry (BUG-0547). Nil in production.
var forwardTargetHook atomic.Pointer[func(entry ReplEntry, targetID string)]

// forward relays an entry to the node that owns the bucket and returns its
// response. Used both for writes by a non-owner coordinator and for reads that
// this node cannot serve.
func (c *Cluster) forward(ctx context.Context, owner ring.Node, entry ReplEntry, wc WriteConcernLevel, rf readForward) (ReplResponse, error) {
	if hook := forwardTargetHook.Load(); hook != nil {
		(*hook)(entry, owner.ID)
	}
	p := c.peerFor(owner)
	req := &ReplPushRequest{
		Token:          c.localToken(),
		FromNode:       c.self.ID,
		Entries:        []ReplEntry{entry},
		WriteConcern:   int(wc),
		ReturnResponse: true,
		Epoch:          c.Epoch(),
		ReadWaitMs:     rf.waitMs,
		ServeFromCopy:  rf.serveFromCopy,
		Rerouted:       rf.rerouted,
	}

	resp, err := p.call(ctx, transport.LaneRequest, req)
	if err != nil {
		// The owner's answer was lost: a write may have been applied there, a
		// read stored nothing.
		_, database := decodeFrameMeta(entry.Payload).scope(transport.Opcode(entry.Op))
		return ReplResponse{}, forwardedOutcome(entry, outcomeUnknown, fmt.Errorf("%w: %s for bucket %q was sent to its owner %s and no answer came back; "+
			"%s: %w",
			ErrForwardLost, transport.Opcode(entry.Op), entry.Bucket, owner.ID,
			forwardLostAdvice(transport.Opcode(entry.Op), database, entry.Bucket, entry.Payload), err))
	}
	c.learnFromPeer(owner.ID, resp.DroppedGenerations)
	if !resp.Success {
		return ReplResponse{}, forwardRefusal(owner.ID, entry, req.Epoch, resp)
	}
	if len(resp.Responses) != 1 {
		return ReplResponse{}, fmt.Errorf("owner %s returned %d responses for 1 entry",
			owner.ID, len(resp.Responses))
	}
	return resp.Responses[0], nil
}

// forwardLostAdvice is the recovery advice Cluster.forward answers a client
// with when the owner's answer to a forwarded request was lost. A read stores
// nothing, so its advice is the retry, not the write's: the same way out the
// LIST_BUCKETS refusal names (listingIncomplete). A restore is routed like a
// read but the owner applies the documents it carries, so its advice is the
// bucket check (and the merge/append hazard) rather than a push read-back. A
// pop has consumed an entry, so a retried pop takes the next one: its advice is
// to peek and never pop again. A claim's advice is the reverse: claim again.
// Every other write keeps the read-back-and-retry advice, because a push or an $inc applied twice is worse than a retry that
// finds it already there.
//
// Read or write is transport.Opcode.IsReadOnly, not replicationRequired: the
// restore opcodes are routed as forwarded reads (they must not travel as one
// ReplEntry) while each document they carry is applied by the owner through the
// ordinary write path, so a lost RESTORE_CHUNK or RESTORE_DONE may have changed
// the bucket. Only the explicit read-only list may be told nothing changed.
func forwardLostAdvice(op transport.Opcode, database, bucket string, payload []byte) string {
	if op.IsReadOnly() {
		return "the read was not answered and nothing changed. " +
			"Retry in a moment; if the member is down, run 'nodes' to see every member and its state"
	}
	if isRestoreOpcode(op) {
		return fmt.Sprintf("the restore may have been applied there: re-running it merges into what landed, "+
			"but a fifo, lifo or heap field element may be pushed twice. Check the bucket before re-running: "+
			"db.%s.count() (run 'use %s' first), or 'dump -db %s -bucket %s' to look at every document",
			bucket, database, database, bucket)
	}
	if op == transport.OpQueueClaim {
		// A claim's entries are not lost with its answer: they wait out the
		// lease, so a second claim is the recovery, not a peek.
		return fmt.Sprintf("the claim may have been applied there without your receiving its lease: its entries return to the queue "+
			"when that lease ends (at most the lease you asked for) and are delivered again. Claim again: db.%s.claim() (run 'use %s' first)",
			bucket, database)
	}
	if !isPopOpcode(op) {
		return "a write may have been applied there, so read it back before retrying a push or an $inc"
	}
	// A pop of several entries lost them all, so the peek that shows what is
	// left must ask for as many.
	peekArgs := ""
	if limit, _ := bson.Raw(payload).Lookup("limit").AsInt64OK(); limit > 1 {
		peekArgs = fmt.Sprintf("{limit: %d}", limit)
	}
	peek := fmt.Sprintf("db.%s.peek(%s)", bucket, peekArgs)
	if op == transport.OpFieldPop {
		var field FieldRequest
		if err := bson.Unmarshal(payload, &field); err == nil {
			peek = fmt.Sprintf("db.%s[%q].%s.peek(%s)", bucket, field.Key, field.Field, peekArgs)
		}
	}
	return fmt.Sprintf("the entry may be lost: the owner may have removed it (or up to the requested number of entries) "+
		"without your receiving it. Do not pop again to look for it. Peek to see what is left: %s (run 'use %s' first)",
		peek, database)
}

// forwardRefusal is the error a coordinator answers its client with when the
// owner refused a forwarded request: matched by the sentinel of the owner's
// refusal, with the owner's word on whether a write was stored.
func forwardRefusal(owner string, entry ReplEntry, epoch uint64, resp *ReplPushResponse) error {
	op := transport.Opcode(entry.Op)
	switch resp.Refusal {
	case refusalEpoch:
		return forwardedOutcome(entry, outcomeNotApplied, fmt.Errorf("%w: %s for bucket %q was sent to %s, which is on membership epoch %d while this node is on %d. "+
			"Retry it: a join or remove-node is normally learned by every node in under a second",
			ErrEpochMismatch, op, entry.Bucket, owner, resp.Epoch, epoch))
	case refusalNotOwner:
		return forwardedOutcome(entry, outcomeNotApplied, fmt.Errorf("%w: %s for bucket %q was sent to %s, which does not own the bucket on its view of the membership. "+
			"Retry it: a join or remove-node is normally learned by every node in under a second (%s)",
			ErrNotOwner, op, entry.Bucket, owner, resp.Error))
	case refusalMemberMismatch:
		return forwardedOutcome(entry, outcomeNotApplied, fmt.Errorf("%w: %s for bucket %q was sent to %s, whose membership disagrees with a peer's, and was not applied. "+
			"Retry it: the disagreement normally clears once the nodes have exchanged their membership, within a few seconds; "+
			"if it persists, run 'nodes history' against every member",
			ErrOwnerMembershipMismatch, op, entry.Bucket, owner))
	case refusalSchemaPaused:
		return forwardedOutcome(entry, outcomeNotApplied, &sentencedError{message: resp.Error, cause: ErrSchemaChangePaused})
	case refusalHandoffInProgress:
		return forwardedOutcome(entry, outcomeNotApplied, &sentencedError{message: fmt.Sprintf("owner %s: %s", owner, resp.Error), cause: ErrHandoffInProgress})
	case refusalBucketNotFound:
		// The owner's sentence unchanged, as the alter's is below.
		return forwardedOutcome(entry, outcomeNotApplied, &sentencedError{message: resp.Error, cause: ErrBucketNotFound})
	case refusalAwaitingCopy:
		_, database := decodeFrameMeta(entry.Payload).scope(op)
		if op.IsReadOnly() {
			return readAwaitingCopy(owner, database, entry.Bucket)
		}
		return forwardedOutcome(entry, outcomeNotApplied, awaitingCopyAt(owner, database, entry.Bucket))
	}
	if sentinel := alterRefusalSentinel(resp.Refusal); sentinel != nil {
		// The owner's sentence unchanged: it is what the owner's own client reads.
		return forwardedOutcome(entry, outcomeNotApplied, &sentencedError{message: resp.Error, cause: sentinel})
	}
	refused := fmt.Errorf("owner %s refused %s for bucket %q: %s", owner, op, entry.Bucket, resp.Error)
	switch resp.Refusal {
	case refusalConcernNotMet:
		refused = &sentencedError{cause: ErrWriteConcernTimeout, message: fmt.Sprintf("owner %s: %s", owner, resp.Error)}
	case refusalOwnershipMoved:
		refused = &sentencedError{cause: ErrOwnershipMoved, message: fmt.Sprintf("owner %s: %s", owner, resp.Error)}
	}
	outcome := resp.WriteOutcome
	if outcome == "" {
		outcome = outcomeUnknown
	}
	if op.IsReadOnly() {
		return refused
	}
	return &writeOutcomeError{outcome: outcome, confirmed: resp.CopiesConfirmed, factor: resp.ReplicationFactor, err: refused}
}

// forwardedOutcome gives a forwarded request's error its outcome. A read
// stores nothing and carries none; every other opcode — including a restore,
// which is routed as a read but applies documents at the owner — carries the
// outcome its caller needs (BUG-0631).
func forwardedOutcome(entry ReplEntry, outcome string, err error) error {
	if transport.Opcode(entry.Op).IsReadOnly() {
		return err
	}
	return withOutcome(outcome, err)
}

// ownsBucket reports whether this node is the ring owner of the bucket placed
// by key.
func (c *Cluster) ownsBucket(key placementKey) bool {
	owner, err := c.Owner(key)
	return err == nil && owner.ID == c.self.ID
}

// inSyncCount is how many copies a write routed to targets can be confirmed by
// now (spec §19.3.11): this node, and every target that is in sync. A target
// leaves the in-sync set whenever its stream stalls (its peer failed, refused
// or could not be queued for) and counts again only once the owner has shipped
// it the log up to the tail, not merely once the stream is taking live writes
// (BUG-0602). A stream reset to connecting by a membership adoption, without
// a failure, stays in. It reads the targets' states and the ring not at all.
func (c *Cluster) inSyncCount(targets []*peer) int {
	inSync := 1
	for _, p := range targets {
		if p.inSync() {
			inSync++
		}
	}
	return inSync
}

// neededAcks returns how many copies must confirm a write at a write concern
// when inSync copies are in sync (owner decision D-2): one, a majority of
// them, or all of them. It is never more than inSync, so a write is never
// refused for want of copies: it waits for what can confirm it, and its
// response says how many did.
func neededAcks(wc WriteConcernLevel, inSync int) int {
	switch wc {
	case WcAll:
		return inSync
	case WcMajority:
		return QuorumSize(inSync)
	default:
		return 1
	}
}

// quorumWait is one write's wait for its concern: the bucket it wrote, the
// sequence the copies must confirm, and the replicas it was routed to.
type quorumWait struct {
	database, bucket string
	key              placementKey
	seq              uint64
	replicas         []*peer
	wc               WriteConcernLevel
}

// waitQuorum blocks until as many copies as the write concern needs of the
// copies in sync hold the write, or the timeout expires, and returns how many
// hold it. The local apply counts as one copy; each replica counts once its
// cursor is at or past the entry's sequence. On every wake — a confirmation, a
// change of any stream's state (a failed ship, a stream that is streaming
// again, a membership adoption), the timeout — it recomputes what is in sync
// and what is confirmed, and re-checks that this node still owns the bucket
// (spec §19.3.7): copies confirmed on the old set do not make a write this
// node will no longer ship durable. At `one` it returns at once.
func (c *Cluster) waitQuorum(ctx context.Context, wait quorumWait) (int, error) {
	if wait.wc != WcMajority && wait.wc != WcAll {
		return c.confirmedCopies(wait), nil
	}
	wctx, cancel := context.WithTimeout(ctx, c.wcTimeout)
	defer cancel()

	acks := make(chan struct{}, len(wait.replicas))
	for _, p := range wait.replicas {
		go func(p *peer) {
			if err := p.waitForAck(wctx, c.self.ID, wait.seq); err == nil {
				acks <- struct{}{}
			}
		}(p)
	}

	for {
		// Taken before the states are read, so a change between the read and
		// the select wakes it.
		changed := c.streamChangedSignal()
		confirmed := c.confirmedCopies(wait)
		if !c.actsAsOwner(wait.key) {
			return confirmed, ownershipMovedErr(wait.database, wait.bucket)
		}
		need := neededAcks(wait.wc, c.inSyncCount(wait.replicas))
		if confirmed >= need {
			return confirmed, nil
		}
		select {
		case <-acks:
		case <-changed:
		case <-wctx.Done():
			return confirmed, concernNotMet(wait.wc, c.wcTimeout, confirmed, c.inSyncCount(wait.replicas))
		}
	}
}

// confirmedCopies is how many nodes hold the write a wait is for: this node,
// which applied it before it routed it, and every replica it was routed to
// whose cursor is at or past its sequence (spec §19.3.11, §19.4 P3). A
// replica that has not acknowledged is not counted, whatever its state.
func (c *Cluster) confirmedCopies(wait quorumWait) int {
	confirmed := 1
	for _, p := range wait.replicas {
		if cursor, opened := p.cursor(c.self.ID); opened && cursor >= wait.seq {
			confirmed++
		}
	}
	return confirmed
}

// concernNotMet is the write-concern timeout's answer (spec §20.12.3:
// TIMEOUT, write_outcome applied). It is ErrWriteConcernTimeout to errors.Is,
// and its sentence says what a client must know first: the write is stored.
func concernNotMet(wc WriteConcernLevel, timeout time.Duration, confirmed, inSync int) error {
	return &sentencedError{cause: ErrWriteConcernTimeout, message: fmt.Sprintf(
		"write concern %s was not met in %s: the write is stored on the owner and confirmed by %d of %d in-sync copies, "+
			"and will reach the others when they are reachable. Do not retry a push or an $inc — it is already stored",
		wc, timeout, confirmed, inSync)}
}

// ownershipMovedErr is the answer to a write whose bucket moved to another
// owner while it was in flight.
func ownershipMovedErr(database, bucket string) error {
	return fmt.Errorf("%w: this node stopped being the owner of %s.%s while the write was in flight; "+
		"its outcome is unknown. Read it back, then retry if it is missing",
		ErrOwnershipMoved, database, bucket)
}

// OwnershipMovedMessage is ownershipMovedErr's wire text for a database and
// bucket. cmd/gen-testvectors builds the driver vector from it, so the vector
// and the server cannot drift into two hand-written sentences.
func OwnershipMovedMessage(database, bucket string) string {
	return ownershipMovedErr(database, bucket).Error()
}

// resolveDocumentIDs returns the payload every applier of a write applies, or
// nil when the payload as sent is already that and needs no rewrite.
//
// The payload is the only thing that reaches every applier: the owner applies
// it, each replica applies it, and a restart replays it from the write-ahead
// log. So two things are fixed here, once, before the entry is logged:
//
//   - A document that carries no id has one minted. Minting inside the handler
//     instead gave every applier its own id: replicas held the same document
//     under different keys, and each restart duplicated every keyless insert
//     (measured on a three-node cluster: 5 documents became 10 after one
//     restart of one node). For a queue element the same defect is worse — a
//     replayed push added a second element instead of overwriting the one
//     already written.
//   - Every now() becomes one instant, whether or not the write names its id, and in an update's operators and filter. An
//     applier reads no clock, so a now() left in the payload would be stored
//     as the sentinel document, never stamped by each copy.
//
// A request that names its ids and holds no now() is returned untouched, so
// the bulk path pays only the cost of looking, and an update with no now() is
// not decoded at all.
func resolveDocumentIDs(op transport.Opcode, payload []byte) ([]byte, error) {
	return resolveWritePayload(op, payload, true)
}

// resolveNowAtTheDoor is resolveDocumentIDs for a node that logs nothing (a
// standalone node, Server.fencedDispatch): it fixes now() and mints no id,
// which the handler does for itself there. A write without now() costs one
// scan of its bytes.
func resolveNowAtTheDoor(op transport.Opcode, payload []byte) ([]byte, error) {
	if !bytes.Contains(payload, nowSentinelKey) {
		return nil, nil
	}
	return resolveWritePayload(op, payload, false)
}

// resolveWritePayload is resolveDocumentIDs, minting missing ids only when
// mintIDs is set.
func resolveWritePayload(op transport.Opcode, payload []byte, mintIDs bool) ([]byte, error) {
	now := newNowStamp(payload)
	switch op {
	case transport.OpPut, transport.OpUpsert, transport.OpBTreeInsert:
		// A btree insert and an upsert are InsertRequests applied by
		// Server.InsertCtx, as a put is, so their key is minted and their
		// now() fixed by the same rule. A miss here is silent: no id minted, a
		// now() stored as the sentinel.
		return resolveInsert(payload, now, mintIDs)
	case transport.OpBatchPut:
		return resolveBatchInsert(payload, now, mintIDs, true)
	case transport.OpBatchUpsert:
		return resolveBatchInsert(payload, now, mintIDs, false)
	case transport.OpBlobWrite:
		// Blob metadata is stored as sent: no applier ever substituted now()
		// in it, so the door does not either.
		if !mintIDs {
			return nil, nil
		}
		return resolveBlobWrite(payload)
	case transport.OpFieldPush, transport.OpFieldPut:
		return resolveFieldWrite(payload, now, mintIDs)
	case transport.OpFIFOPush, transport.OpLIFOPush, transport.OpHeapPush:
		return resolvePush(payload, now, mintIDs)
	case transport.OpUpdate:
		return resolveUpdate(payload, now)
	case transport.OpFIFOPop, transport.OpLIFOPop, transport.OpHeapPop, transport.OpFieldPop:
		return nil, resolvePop(payload)
	case transport.OpQueueClaim:
		return nil, resolveClaim(payload)
	case transport.OpQueueAck, transport.OpQueueRelease:
		return nil, resolveLease(payload, false)
	case transport.OpQueueExtend:
		return nil, resolveLease(payload, true)
	case transport.OpDeadList:
		return nil, resolveDeadList(payload)
	case transport.OpDeadRequeue:
		return nil, resolveDeadMove(payload, "requeue")
	case transport.OpDeadDiscard:
		return nil, resolveDeadMove(payload, "discard")
	}
	return nil, nil
}

// resolvePop refuses, before anything is logged, a pop whose limit is outside
// 1 to 100: the cap is the same on every node, so the door can apply it, and
// a pop refused only by its handler would be logged and shipped first. It
// reads the limit alone, which a bucket pop and a field pop name alike, and
// stamps nothing: the result ceiling is the bucket owner's to stamp
// (Cluster.stampAtOwner). A limit that is not a number is left to the
// handler, whose decode refuses it in its own words.
func resolvePop(payload []byte) error {
	limit, isNumber := bson.Raw(payload).Lookup("limit").AsInt64OK()
	if !isNumber {
		return nil
	}
	_, err := popLimit(int(limit))
	return err
}

// namesID reports whether document carries a non-empty string _id_.
func namesID(document map[string]interface{}) bool {
	id, ok := document["_id_"].(string)
	return ok && id != ""
}

// emptyDocumentID reports whether document carries an _id_ that is present as
// an empty string. An absent _id_ asks for a generated key; an empty one names
// a key nothing can find, so it is refused rather than silently replaced
// (BUG-0153). A non-string _id_ keeps the documented substitution.
func emptyDocumentID(document map[string]interface{}) bool {
	id, ok := document["_id_"].(string)
	return ok && id == ""
}

// refuseEmptyDocumentID is the refusal of an empty-string _id_, or nil.
// database and bucket name where it was found so the refusal says what to fix.
func refuseEmptyDocumentID(document map[string]interface{}, database, bucket string) error {
	if !emptyDocumentID(document) {
		return nil
	}
	return &emptyIDError{database: database, bucket: bucket}
}

// emptyIDError is the refusal of a document whose _id_ is an empty string. Its
// text is built from the bucket and the database, never from the document's
// content.
type emptyIDError struct {
	database, bucket string
	// batchPrefix locates the document in a batch, set by inBatch.
	batchPrefix string
}

func (e *emptyIDError) Error() string {
	return fmt.Sprintf("%sThe _id_ of a document in bucket %q (database %q) is an empty string. "+
		"An _id_ must be a non-empty string, or omit it to get a generated key.",
		e.batchPrefix, e.bucket, e.database)
}

func (e *emptyIDError) Unwrap() error { return ErrEmptyDocumentID }

// withMintedID returns document with a new _id_, and the id. what names the
// id in the error.
func withMintedID(document map[string]interface{}, what string) (map[string]interface{}, string, error) {
	id, err := ids.NewDocumentID()
	if err != nil {
		return nil, "", fmt.Errorf("generating %s: %w", what, err)
	}
	if document == nil {
		document = make(map[string]interface{})
	}
	document["_id_"] = id
	return document, id, nil
}

// encodeResolved encodes a rewritten request; what names it in the error.
func encodeResolved(request any, what string) ([]byte, error) {
	rewritten, err := bson.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("encoding %s with its resolved ids and now(): %w", what, err)
	}
	return rewritten, nil
}

func resolveInsert(payload []byte, now *nowStamp, mintIDs bool) ([]byte, error) {
	var req InsertRequest
	if err := bson.Unmarshal(payload, &req); err != nil {
		return nil, fmt.Errorf("decoding insert request to resolve its key: %w", err)
	}
	if err := refuseEmptyDocumentID(req.Data, req.Database, req.Bucket); err != nil {
		return nil, err
	}
	req.Data = now.fields(req.Data)
	minting := mintIDs && req.Key == "" && !namesID(req.Data)
	if minting {
		var err error
		if req.Data, req.Key, err = withMintedID(req.Data, "document id"); err != nil {
			return nil, err
		}
	}
	if !minting && !now.replaced {
		return nil, nil
	}
	return encodeResolved(req, "insert request")
}

// resolveBatchInsert fixes a bulk write's ids and now(). refuseRepeats is set
// for BATCH_PUT, whose repeated _id_ is refused here, before the log, and not
// for BATCH_UPSERT, which keeps the last occurrence in the store.
func resolveBatchInsert(payload []byte, now *nowStamp, mintIDs, refuseRepeats bool) ([]byte, error) {
	var req BatchInsertRequest
	if err := bson.Unmarshal(payload, &req); err != nil {
		return nil, fmt.Errorf("decoding batch insert request to resolve its keys: %w", err)
	}
	minted := false
	for position, document := range req.Data {
		if err := refuseEmptyDocumentID(document, req.Database, req.Bucket); err != nil {
			return nil, inBatch(err, position+1, "")
		}
		document = now.fields(document)
		if mintIDs && !namesID(document) {
			var err error
			if document, _, err = withMintedID(document, "document id"); err != nil {
				return nil, err
			}
			minted = true
		}
		req.Data[position] = document
	}
	if refuseRepeats {
		if err := refuseRepeatedIDs(req.Data, req.Database, req.Bucket); err != nil {
			return nil, err
		}
	}
	if !minted && !now.replaced {
		return nil, nil
	}
	return encodeResolved(req, "batch insert request")
}

func resolveBlobWrite(payload []byte) ([]byte, error) {
	var req BlobWriteRequest
	if err := bson.Unmarshal(payload, &req); err != nil {
		return nil, fmt.Errorf("decoding blob write request to resolve its key: %w", err)
	}
	if req.Key != "" {
		return nil, nil
	}
	key, err := ids.NewDocumentID()
	if err != nil {
		return nil, fmt.Errorf("generating blob key: %w", err)
	}
	req.Key = key
	return encodeResolved(req, "blob write request")
}

// resolveFieldWrite mints a field element's _id_ for the same reason as a
// queue element's: a heap field keys its element by it, and minted on each
// replica the instances would diverge. Every field write follows the one rule.
func resolveFieldWrite(payload []byte, now *nowStamp, mintIDs bool) ([]byte, error) {
	var req FieldRequest
	if err := bson.Unmarshal(payload, &req); err != nil {
		return nil, fmt.Errorf("decoding field request to resolve its element id: %w", err)
	}
	if err := refuseEmptyDocumentID(req.Data, req.Database, req.Bucket); err != nil {
		return nil, err
	}
	req.Data = now.fields(req.Data)
	minting := mintIDs && !namesID(req.Data)
	if minting {
		var err error
		if req.Data, _, err = withMintedID(req.Data, "element id"); err != nil {
			return nil, err
		}
	}
	if !minting && !now.replaced {
		return nil, nil
	}
	return encodeResolved(req, "field request")
}

// resolvePush mints a queue element's id inside the document, because that is
// what the stores key an element by: the segment header for FIFO/LIFO writes
// the document as-is, and the heap uses the document's _id_ as its Badger key.
// Without it, each applier mints its own.
func resolvePush(payload []byte, now *nowStamp, mintIDs bool) ([]byte, error) {
	var req PushRequest
	if err := bson.Unmarshal(payload, &req); err != nil {
		return nil, fmt.Errorf("decoding push request to resolve its element id: %w", err)
	}
	if err := refuseEmptyDocumentID(req.Data, req.Database, req.Bucket); err != nil {
		return nil, err
	}
	req.Data = now.fields(req.Data)
	minting := mintIDs && !namesID(req.Data)
	if minting {
		var err error
		if req.Data, _, err = withMintedID(req.Data, "element id"); err != nil {
			return nil, err
		}
	}
	if !minting && !now.replaced {
		return nil, nil
	}
	return encodeResolved(req, "push request")
}

// resolveUpdate fixes now() in every operator that carries a value, and in a
// multi update's filter, which every applier evaluates against its own copy.
// An update has no id to mint, so one without now() is not decoded.
func resolveUpdate(payload []byte, now *nowStamp) ([]byte, error) {
	if !now.present {
		return nil, nil
	}
	var req UpdateRequest
	if err := bson.Unmarshal(payload, &req); err != nil {
		return nil, fmt.Errorf("decoding update request to fix its now(): %w", err)
	}
	req.Set = now.fields(req.Set)
	req.Push = now.fields(req.Push)
	req.AddToSet = now.fields(req.AddToSet)
	req.Min = now.fields(req.Min)
	req.Max = now.fields(req.Max)
	req.Filter = now.fields(req.Filter)
	if !now.replaced {
		return nil, nil
	}
	return encodeResolved(req, "update request")
}

// frameMeta is the routing metadata decoded from a client frame in a single
// BSON pass. The code previously decoded the same payload three times — once
// for ring scope, once for write concern, once for the WAL key column.
type frameMeta struct {
	Bucket        string        `bson:"bucket"`
	Database      string        `bson:"database"`
	Name          string        `bson:"name"`
	Key           bson.RawValue `bson:"key"`
	ID            bson.RawValue `bson:"id"`
	WriteConcern  string        `bson:"writeconcern"`
	WriteConcern2 string        `bson:"write_concern"`
	// Multi and Filter are an UPDATE's scan request (build step 12c), read
	// here so the door judges a filtered update with no second decode. Filter
	// is kept raw: it is only asked whether it holds anything.
	Multi  bool          `bson:"multi"`
	Filter bson.RawValue `bson:"filter"`
}

// decodeFrameMetaErr is decodeFrameMeta with the decode error kept, for the one
// caller that must not fail open: the authorization gate refuses a payload it
// cannot read (BUG-0113), so it cannot use the best-effort form. Both go
// through this single bson.Unmarshal so the two readings cannot drift
// (BUG-0098). site and op are for the request-decode hook.
func decodeFrameMetaErr(site string, op transport.Opcode, payload []byte) (m frameMeta, err error) {
	if len(payload) == 0 {
		return m, nil
	}
	noteRequestDecode(site, op, payload)
	// Split rather than `return m, bson.Unmarshal(payload, &m)`: the spec does
	// not order the read of m against the call that writes it.
	err = bson.Unmarshal(payload, &m)
	return m, err
}

func decodeFrameMeta(payload []byte) (m frameMeta) {
	m, _ = decodeFrameMetaErr("decodeFrameMeta", 0, payload) // best-effort: zero values mean "absent"
	return m
}

// scope reports what a frame is scoped to, so it can be placed on the ring. It
// has to be opcode-aware: DDL requests name a bucket or database in a "name"
// field while data requests use "bucket", and misreading the payload would
// place a bucket creation on the wrong replica set or broadcast a single
// bucket's creation cluster-wide.
func (m frameMeta) scope(op transport.Opcode) (bucket, database string) {
	switch op {
	case transport.OpCreateBucket, transport.OpDeleteBucket, transport.OpDeclareStoreFields, transport.OpAlterBucket:
		return m.Name, m.Database
	case transport.OpCreateDB, transport.OpDeleteDB:
		return "", m.Name
	}
	return m.Bucket, m.Database
}

// writeConcern reports the per-request durability requirement; zero means the
// request named none. Both the BSON default naming ("writeconcern") and the
// underscored form used in JSON config ("write_concern") are accepted, so a
// client need not know which the server's struct tags imply. A value that is
// not a write concern is an error, never a silent `one`.
func (m frameMeta) writeConcern() (WriteConcernLevel, error) {
	switch {
	case strings.TrimSpace(m.WriteConcern) != "":
		return ParseWriteConcern(m.WriteConcern)
	case strings.TrimSpace(m.WriteConcern2) != "":
		return ParseWriteConcern(m.WriteConcern2)
	}
	return 0, nil
}

// key extracts best-effort key metadata for the WAL. It never fails a write:
// the key column is diagnostic, the payload is what is replayed.
func (m frameMeta) key() []byte {
	switch {
	case m.Key.Type != 0:
		return append([]byte(nil), m.Key.Value...)
	case m.ID.Type != 0:
		return append([]byte(nil), m.ID.Value...)
	}
	return nil
}
