package server

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// The apply fence (shared-storage spec §5.2.2, "Where the fence is taken").
//
// One fence per bucket per node, owned by Server so a standalone node has
// one too. Writers hold it shared; a drop holds it exclusively, so it waits
// out every in-flight write to the bucket and no new write starts until the
// drop has returned. No handler takes a fence: handlers run inside one, taken
// at exactly these sites — Cluster.applyAsOwner and Cluster.applyAsReplica on
// a cluster, fencedDispatch for the three standalone entry points,
// Cluster.snapshotBucket (exclusive) and Cluster.materializeBucket (shared).
// Replay runs before the node serves and takes none.
//
// The map is reference-counted and an entry is deleted when its last holder
// or waiter leaves, so it is bounded by operations in flight, not by names
// ever written (N-12). Keyed by the bucket's placement key, as placement is
//: two same-named buckets in different databases have their own
// fences, so a snapshot or a drop of one no longer stalls writes to the other.
//
// Lock order: fence → data.mu → db.mu → bucket.mu; the allocator's mutex is a
// leaf below all four.

// dispatchApplyHook, when set, runs in fencedDispatch inside the fence, just
// before the frame is dispatched. Tests use it to hold a write mid-apply. Nil
// in production.
var dispatchApplyHook atomic.Pointer[func(*transport.Frame)]

// beforeOneMemberFenceHook, when set, runs in Server.dispatchInFence after the
// door's checks and the spread router's, just before the bucket's fence is
// taken, and may block: a test holds a write there while its bucket is dropped
// and created again spread. Nil in production.
var beforeOneMemberFenceHook atomic.Pointer[func(*transport.Frame)]

type fenceMode int

const (
	fenceShared fenceMode = iota
	fenceExclusive
)

// fenceEntry is one bucket's fence.
type fenceEntry struct {
	mu sync.RWMutex
	// refs counts holders and waiters, under Server.fenceMapMu. A waiter holds
	// a reference, so no entry is deleted under anyone.
	refs int

	// applyMu guards the apply tickets below. It is a leaf: nothing else is
	// taken while it is held.
	applyMu sync.Mutex
	// applying holds the tickets of the writes of this bucket between their
	// WAL position being chosen and their local apply returning.
	applying map[*applyTicket]struct{}
}

// applyTicket orders one write's local apply among the same bucket's writes
// (build doc §2.1). Its fields are guarded by fenceEntry.applyMu.
type applyTicket struct {
	// floor is the lowest sequence the write can have until the WAL assigns
	// one, and that sequence afterwards.
	floor uint64
	// assigned is set once floor is the WAL's sequence. Only an assigned
	// ticket is given its turn: two tickets can open with one floor, and the
	// one whose append lands second gets the higher sequence.
	assigned bool
	// turn is closed once the ticket is assigned and no other open ticket has
	// a lower floor. Only the tickets whose turn has come are woken, not every
	// waiter on every change.
	turn chan struct{}
}

// openApply registers a write about to be appended to the WAL, before the
// append. The floor is read under the lock, after every earlier ticket's
// registration, and the WAL's last sequence only grows, so the sequence the
// append assigns is at least it.
func (e *fenceEntry) openApply(lastSeq func() uint64) *applyTicket {
	e.applyMu.Lock()
	defer e.applyMu.Unlock()
	if e.applying == nil {
		e.applying = make(map[*applyTicket]struct{})
	}
	ticket := &applyTicket{floor: lastSeq() + 1, turn: make(chan struct{})}
	e.applying[ticket] = struct{}{}
	return ticket
}

// assignApply records the sequence the WAL gave the write.
func (e *fenceEntry) assignApply(ticket *applyTicket, seq uint64) {
	e.applyMu.Lock()
	defer e.applyMu.Unlock()
	ticket.floor, ticket.assigned = seq, true
	e.releaseTurnsLocked()
}

// waitApplyTurn blocks until no other open ticket of this bucket has a lower
// floor: every same-bucket write that could have a lower sequence has
// applied or given up. A ticket opened after this write's append has a floor
// above its sequence, so latecomers never hold it up. Call it after
// assignApply; an unassigned ticket's turn never comes.
func (e *fenceEntry) waitApplyTurn(ctx context.Context, ticket *applyTicket) error {
	select {
	case <-ticket.turn:
		return nil
	case <-ctx.Done():
		e.applyMu.Lock()
		seq := ticket.floor
		e.applyMu.Unlock()
		return fmt.Errorf("waiting for earlier writes of the bucket to apply before seq %d: %w", seq, ctx.Err())
	}
}

// closeApply retires the ticket, waking the writes behind it. Every path out
// of an ordered write must reach it, so callers defer it and may also call it
// earlier; a second call does nothing.
func (e *fenceEntry) closeApply(ticket *applyTicket) {
	e.applyMu.Lock()
	defer e.applyMu.Unlock()
	if _, open := e.applying[ticket]; !open {
		return
	}
	delete(e.applying, ticket)
	e.releaseTurnsLocked()
}

// releaseTurnsLocked gives their turn to the assigned tickets with the lowest
// floor — the ones no other open ticket, assigned or not, is below. A floor
// only rises, so a turn given is never taken back.
func (e *fenceEntry) releaseTurnsLocked() {
	lowest := uint64(0)
	first := true
	for ticket := range e.applying {
		if first || ticket.floor < lowest {
			lowest, first = ticket.floor, false
		}
	}
	for ticket := range e.applying {
		if !ticket.assigned || ticket.floor != lowest {
			continue
		}
		select {
		case <-ticket.turn:
		default:
			close(ticket.turn)
		}
	}
}

// fenceHold is one acquisition of a fence.
type fenceHold struct {
	s     *Server
	key   placementKey
	entry *fenceEntry
	mode  fenceMode
}

// fenceModeFor is the one place a fence's mode is chosen: exclusive for a
// bucket drop, a bucket create and a factor alter, shared for every other
// mutation. A create decides whether the name exists, and can drop a copy of
// it held from an earlier incarnation of its database (G-6 (d)), so nothing
// else of the bucket may run beside it. An alter changes who every later entry
// of the bucket is addressed to, so every write admitted before it has applied
// and routed, and none is admitted until its factor is in place
// (bucket-replication-rev8-spec §6.2 rule F2).
func fenceModeFor(op transport.Opcode) fenceMode {
	if op == transport.OpDeleteBucket || op == transport.OpCreateBucket || op == transport.OpAlterBucket {
		return fenceExclusive
	}
	return fenceShared
}

// acquireFence takes the fence of the bucket placed by key in mode and returns
// the hold, which the caller must release.
func (s *Server) acquireFence(key placementKey, mode fenceMode) *fenceHold {
	entry := s.referenceFence(key)
	if mode == fenceExclusive {
		entry.mu.Lock()
	} else {
		entry.mu.RLock()
	}
	return &fenceHold{s: s, key: key, entry: entry, mode: mode}
}

// tryAcquireFenceShared takes the fence of the bucket placed by key shared if
// that does not wait, and returns nil if it would: a drop, a create or a
// snapshot holds the fence or waits for it. It is for a caller that already
// holds another bucket's fence, which must never wait for a second one: two
// such callers, each waiting behind a drop queued on the fence the other
// holds, would wait for ever.
func (s *Server) tryAcquireFenceShared(key placementKey) *fenceHold {
	entry := s.referenceFence(key)
	if !entry.mu.TryRLock() {
		s.dereferenceFence(key, entry)
		return nil
	}
	return &fenceHold{s: s, key: key, entry: entry, mode: fenceShared}
}

// referenceFence returns the fence of the bucket placed by key, created if
// nobody holds or waits for it, with one more reference counted.
func (s *Server) referenceFence(key placementKey) *fenceEntry {
	s.fenceMapMu.Lock()
	defer s.fenceMapMu.Unlock()
	if s.fences == nil {
		s.fences = make(map[placementKey]*fenceEntry)
	}
	entry, ok := s.fences[key]
	if !ok {
		entry = &fenceEntry{}
		s.fences[key] = entry
	}
	entry.refs++
	return entry
}

// dereferenceFence drops one reference to entry, deleting it when nobody else
// holds or waits for it.
func (s *Server) dereferenceFence(key placementKey, entry *fenceEntry) {
	s.fenceMapMu.Lock()
	defer s.fenceMapMu.Unlock()
	entry.refs--
	if entry.refs == 0 {
		delete(s.fences, key)
	}
}

// release unlocks the fence and drops the hold's reference, deleting the
// entry when nobody else holds or waits for it.
func (h *fenceHold) release() {
	if h.mode == fenceExclusive {
		h.entry.mu.Unlock()
	} else {
		h.entry.mu.RUnlock()
	}
	h.s.dereferenceFence(h.key, h.entry)
}

// fencedDispatch is Server.dispatch inside the fence, for the standalone entry
// points: NewHandler with clustering off, and restore's routeOrdinaryWrite and
// routeOrdinaryDDL. A mutation scoped to a bucket takes that bucket's fence in
// fenceModeFor's mode; database-level DDL, scoped to no bucket, takes the
// database's own (database, "") fence shared. Reads and unscoped frames pass straight through: readers do
// not take the fence (spec §5.2.2). Never call it from a path already inside a
// fence — Cluster.applyLocal is, and taking it again there is C-01.
//
// A data write's refusal leaves here with write_outcome not_applied when
// nothing of it reached the store (refusedBeforeApply): this is the one
// function every write on a node that does not route passes through, as
// Cluster.handlerRefusalWithOutcome is on a bucket's owner.
func (s *Server) fencedDispatch(ctx context.Context, frame *transport.Frame) (*transport.Frame, error) {
	return dispatchStampingRefusal(ctx, frame, s.dispatchInFence)
}

// dispatchStampingRefusal runs dispatch for frame and, for a data write, gives
// its refusal write_outcome not_applied when nothing of it reached the store:
// the request's applies report to a sink of their own, which refusedBeforeApply
// reads. Any other frame is dispatched as it is, and pays for no sink.
func dispatchStampingRefusal(ctx context.Context, frame *transport.Frame, dispatch func(context.Context, *transport.Frame) (*transport.Frame, error)) (*transport.Frame, error) {
	if !isDataWrite(frame.Opcode) {
		return dispatch(ctx, frame)
	}
	applyCtx, applied := withApplyOutcome(ctx)
	reply, err := dispatch(applyCtx, frame)
	if err != nil {
		return reply, err
	}
	return refusedBeforeApply(frame.Opcode, overBoundAnswer(frame, reply, applied), applied), nil
}

// dispatchInFence is fencedDispatch's dispatch: the request's own refusals,
// then Server.dispatch inside the bucket's fence.
func (s *Server) dispatchInFence(ctx context.Context, frame *transport.Frame) (*transport.Frame, error) {
	meta := decodeFrameMeta(frame.Payload)
	// A write concern means nothing to a node with no one to wait for, but a
	// word it does not know is refused as a cluster refuses it, so an
	// application tested on one node meets no new error on the cluster
	// (Cluster.handleClientFrame).
	if _, wcErr := meta.writeConcern(); wcErr != nil {
		if replicationRequired(frame.Opcode) {
			wcErr = withOutcome(outcomeNotApplied, wcErr)
		}
		return errorFrame(frame, wcErr), nil
	}
	if !replicationRequired(frame.Opcode) {
		if s.cluster != nil {
			return s.cluster.readAtOneMemberDoor(ctx, frame, meta)
		}
		// A node with no cluster (zero config) is its spread buckets'
		// router: their reads are routed, never answered from a template.
		return s.dispatchRoutingSpread(ctx, frame, meta)
	}
	bucket, database := meta.scope(frame.Opcode)
	if bucket == "" && database == "" {
		return s.dispatch(ctx, frame)
	}
	// A filtered update of a spread bucket is refused at the client's door
	// (build step 12c); Cluster.prepareClientWrite asks the same on a node
	// that routes.
	if frame.Opcode == transport.OpUpdate && meta.scansBucket() {
		if refusal, _ := s.filteredUpdateRefusal(database, bucket); refusal != nil {
			return errorFrame(frame, withOutcome(outcomeNotApplied, refusal)), nil
		}
	}
	// A clustered node alone on its ring comes in by this door: a node a
	// removal left alone, or one removed from a two-node cluster. It asks what
	// the routed door asks (BUG-0625, BUG-0626).
	if s.cluster != nil {
		admitted, err := s.cluster.admitAtOneMemberDoor(ctx, frame.Opcode, database, bucket, frame.Payload)
		if err != nil {
			return errorFrame(frame, withOutcome(outcomeNotApplied, err)), nil
		}
		ctx = admitted
	}
	// A node that does not route is its own owner: it judges a create's shape
	// here (the name length and the replication factor), before anything is
	// dispatched, as Cluster.applyAsOwner does for a
	// node that does.
	if frame.Opcode == transport.OpCreateDB {
		if reason := clientNameRefusal(frame.Opcode, frame.Payload); reason != nil {
			return respond(frame, transport.OpCreateDB, &CreateDatabaseResponse{Success: false, Message: reason.Error()})
		}
	}
	if frame.Opcode == transport.OpDeclareStoreFields {
		if reason := clientNameRefusal(frame.Opcode, frame.Payload); reason != nil {
			return errorFrame(frame, withOutcome(outcomeNotApplied, reason)), nil
		}
	}
	if frame.Opcode == transport.OpCreateBucket {
		var req CreateBucketRequest
		if err := bson.Unmarshal(frame.Payload, &req); err == nil {
			if reason := s.createShapeRefusal(req); reason != nil {
				return respond(frame, transport.OpCreateBucket, &CreateBucketResponse{Success: false, Message: reason.Error()})
			}
		}
	}
	if frame.Opcode == transport.OpAlterBucket {
		if req, err := decodeAlter(frame.Payload); err == nil {
			if reason := s.alterShapeRefusal(req); reason != "" {
				return respond(frame, transport.OpAlterBucket, &AlterBucketResponse{Success: false, ReplicationFactor: req.ReplicationFactor, Message: reason})
			}
		}
	}
	// The door is where now() is fixed on a node that logs nothing, as
	// resolveDocumentIDs fixes it before the log on a node that does; the
	// handlers read no clock.
	stamped, err := resolveNowAtTheDoor(frame.Opcode, frame.Payload)
	if err != nil {
		return errorFrame(frame, withOutcome(outcomeNotApplied, err)), nil
	}
	if stamped != nil {
		stampedFrame := *frame
		stampedFrame.Payload = stamped
		frame = &stampedFrame
	}
	// The one-member door's router (spread MVP note, section 2.1, B2): a
	// client's write of a spread template goes to its part, here, after the
	// door's admission and before any fence, which the router does not hold
	// (spread_gate.go). The parts go through fencedDispatch, each under its
	// own fence.
	if answer, routed := s.routeSpreadAtOneMember(ctx, frame, database, bucket); routed {
		return answer, nil
	}
	if hook := beforeOneMemberFenceHook.Load(); hook != nil {
		(*hook)(frame)
	}
	// A clustered node takes the fence as the bucket's owner when a removal
	// left it alone (Cluster.fenceAtOneMemberDoor, BUG-0630).
	var hold *fenceHold
	if s.cluster != nil {
		hold, err = s.cluster.fenceAtOneMemberDoor(ctx, frame.Opcode, database, bucket, frame.Payload)
		if err != nil {
			return errorFrame(frame, withOutcome(outcomeNotApplied, err)), nil
		}
	} else {
		hold = s.acquireFence(placementOf(database, bucket), fenceModeFor(frame.Opcode))
	}
	defer hold.release()
	// The router's check above ran before the fence; a drop and a spread
	// create of the name may have landed since (spreadTemplateUnderFence).
	if err := s.spreadTemplateUnderFence(ctx, frame.Opcode, database, bucket, hold.key); err != nil {
		return errorFrame(frame, err), nil
	}
	if hook := dispatchApplyHook.Load(); hook != nil {
		(*hook)(frame)
	}
	return s.dispatch(ctx, frame)
}
