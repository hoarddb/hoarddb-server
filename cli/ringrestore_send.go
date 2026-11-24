package cli

import (
	"context"
	"fmt"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/ringclient"
	"github.com/hoarddb/hoarddb-server/transport"
)

const (
	// restoreMaxFrameOperations is the most operations one RING_BATCH carries.
	restoreMaxFrameOperations = ringMaxBatchDocuments
	// restoreFrameByteCap is the soft byte cap of a frame: half the frame limit. The first
	// operation of a frame always goes in however large, because one element can be as large
	// as a document, and a rule without that clause would never make progress (section 3.1).
	restoreFrameByteCap = transport.MaxFrameLength / 2
	// restoreOperationOverhead is what an operation costs beyond its payload: the names it
	// carries and the framing, counted so a frame of tiny elements is still cut by bytes.
	restoreOperationOverhead = 64
)

// restoreOpKind says which write an operation is.
type restoreOpKind int

const (
	restoreInsert restoreOpKind = iota
	restorePush
	restorePut
	restorePushHeap
)

// word names the operation in a refusal.
func (k restoreOpKind) word() string {
	switch k {
	case restoreInsert:
		return "insert"
	case restorePush:
		return "push"
	case restorePut:
		return "put"
	case restorePushHeap:
		return "push to a heap"
	}
	return "write"
}

// restoreOp is one write of a restore: the insert of an entity's document, or one element
// of one of its stores. It owns its bytes, copied out of the reader's buffer, because a
// group of entities is held across reads.
type restoreOp struct {
	kind     restoreOpKind
	database string
	bucket   string
	id       string
	field    string
	// key is the element key of a hash or btree put, and the entry id of a heap push.
	key      string
	priority int64
	element  bson.Raw
	fields   ringclient.Doc
	// size is what the operation counts against a frame's byte cap.
	size int
}

// describe names the operation for a refusal: the kind, the entity and, for a store write,
// the field.
func (o restoreOp) describe() string {
	if o.kind == restoreInsert {
		return fmt.Sprintf("the insert of %s.%s id %q", o.database, o.bucket, terminalSafeText(o.id))
	}
	return fmt.Sprintf("a %s into field %q of %s.%s id %q", o.kind.word(), o.field, o.database, o.bucket, terminalSafeText(o.id))
}

// restoreTransport is what the sender needs of a connection. It is the seam a test
// replaces; the real one is clientRestoreTransport.
type restoreTransport interface {
	// NextRequestID returns a request id no other request of this connection uses.
	NextRequestID() [16]byte
	// Send sends ops as one RING_BATCH under id and returns one answer per operation, in
	// order. A refusal of the whole frame, or a lost connection, is the error.
	Send(ctx context.Context, ops []restoreOp, id [16]byte) ([]ringDocAnswer, error)
	close() error
}

// clientRestoreTransport is the restoreTransport of a live client.
type clientRestoreTransport struct{ client *ringclient.Client }

// NextRequestID implements restoreTransport.
func (t clientRestoreTransport) NextRequestID() [16]byte { return t.client.NextRequestID() }

func (t clientRestoreTransport) close() error { return t.client.Close() }

// Send implements restoreTransport. An operation the client cannot build (a field the bucket
// does not declare, a value it cannot encode, a header it could not learn) is that operation's
// refusal and the frame is not sent at all (queueRestoreOps): a part of a frame sent without it
// would push into a document whose insert never went out.
func (t clientRestoreTransport) Send(ctx context.Context, ops []restoreOp, id [16]byte) ([]ringDocAnswer, error) {
	batch := t.client.NewBatch()
	unsent := queueRestoreOps(ops, func(op restoreOp) error {
		bucket := t.client.Bucket(op.database, op.bucket).In(batch)
		var err error
		switch op.kind {
		case restoreInsert:
			_, err = bucket.Insert(ctx, op.id, op.fields)
		case restorePush:
			_, err = bucket.Push(ctx, op.id, op.field, op.element)
		case restorePut:
			_, err = bucket.Put(ctx, op.id, op.field, op.key, op.element)
		case restorePushHeap:
			_, err = bucket.PushHeap(ctx, op.id, op.field, op.key, op.priority, op.element)
		}
		return err
	})
	if unsent != nil {
		return unsent, nil
	}
	result, err := batch.Send(ctx, ringclient.WithRequestID(id))
	if err != nil {
		return nil, err
	}
	return restoreAnswersOf(result), nil
}

// queueRestoreOps queues each operation through queue, in order. It returns nil when every
// operation was queued, and otherwise the answers of a frame that is not sent: the refusal of
// the operation that could not be queued in its place, and for every other operation a
// restoreNotSentError that wraps it. No answer may be empty, because an empty answer reads as
// applied: the order guard would see a push applied behind the refusal, and the insert step
// would count the other entities as orphans, though nothing was sent.
func queueRestoreOps(ops []restoreOp, queue func(restoreOp) error) []ringDocAnswer {
	for index, op := range ops {
		err := queue(op)
		if err == nil {
			continue
		}
		answers := make([]ringDocAnswer, len(ops))
		for other := range answers {
			answers[other].Err = &restoreNotSentError{index: index, op: op, cause: err}
		}
		answers[index].Err = err
		return answers
	}
	return nil
}

// restoreNotSentError is the answer of an operation whose frame was not sent because another
// operation could not be queued. It unwraps to that refusal, so it is judged as that refusal is:
// a lapsed lease is waited out and the frame sent again, a field the bucket does not declare
// stops the restore.
type restoreNotSentError struct {
	index int
	op    restoreOp
	cause error
}

func (e *restoreNotSentError) Error() string {
	return fmt.Sprintf("not sent: operation %d of the frame, %s, could not be queued, so the frame did not go out: %v", e.index, e.op.describe(), e.cause)
}

func (e *restoreNotSentError) Unwrap() error { return e.cause }

// restoreAnswersOf turns a batch result into the answers the sender judges: a refusal stays
// a refusal, and a write the owner did not decide becomes one that clears by waiting. The
// transport and its test both call it, so the conversion cannot go untested.
func restoreAnswersOf(result ringclient.BatchResult) []ringDocAnswer {
	answers := make([]ringDocAnswer, len(result.Answers))
	for index, answer := range result.Answers {
		answers[index] = ringDocAnswer{Err: answer.Err, Deduped: answer.Write.Deduped}
		if answer.Err == nil {
			answers[index].Err = restoreUnconfirmed(answer.Write)
		}
	}
	return answers
}

// restoreUnconfirmed turns a write the owner did not decide into a refusal that clears by
// waiting. An insert whose outcome is unknown must not be taken as applied: the pushes behind
// it would go into a document this restore may not have created. The frame is then sent again
// under the same request id, which asks the owner to repeat its recorded decision, and if it
// is still undecided after the resends the restore stops with the outcome unknown.
func restoreUnconfirmed(write ringclient.WriteResult) error {
	if write.Outcome != "unknown" {
		return nil
	}
	return &ringclient.Error{Code: cluster.CodeUnavailable, Message: "the owner did not confirm this write, so its outcome is unknown"}
}

// restoreSender sends frames and keeps each frame's request id for its whole life: on a
// lost connection it dials again, builds the same frame from the same operations and sends
// it under that id, so the owner answers Deduped for what it had already applied
// (section 3.4 step 6).
type restoreSender struct {
	transport restoreTransport
	// redial returns a transport on a fresh connection.
	redial func(ctx context.Context) (restoreTransport, error)
	// pause waits before resend number attempt; a test replaces it.
	pause func(ctx context.Context, attempt int) error
	// redialed are the transports this sender opened, closed by close.
	redialed []restoreTransport
	// resends counts frames sent again, for the report.
	resends int
	// lastResent says the frame of the latest send call went out more than once.
	lastResent bool
}

// newRestoreSender sends through client and dials again through redial.
func newRestoreSender(client *ringclient.Client, redial ringRedial) *restoreSender {
	return &restoreSender{
		transport: clientRestoreTransport{client},
		redial: func(ctx context.Context) (restoreTransport, error) {
			fresh, err := redial(ctx)
			if err != nil {
				return nil, err
			}
			return clientRestoreTransport{fresh}, nil
		},
		pause: pauseBeforeResend,
	}
}

// close closes the connections the sender opened itself. The caller owns the first one.
func (s *restoreSender) close() {
	for _, transport := range s.redialed {
		_ = transport.close()
	}
	s.redialed = nil
}

// restoreUnknownError says a frame was sent and never answered, even after the resends: the
// operations it held may or may not have been written.
type restoreUnknownError struct {
	operations int
	first      restoreOp
	cause      error
}

func (e *restoreUnknownError) Error() string {
	return fmt.Sprintf("%d operations, the first being %s, were sent and the ring never confirmed them: %v", e.operations, e.first.describe(), e.cause)
}

func (e *restoreUnknownError) Unwrap() error { return e.cause }

// send sends ops as one frame, resending as the answers require, and returns one answer per
// operation. The returned error is a refusal of the whole frame, or *restoreUnknownError when
// no send was ever answered. A refusal of one operation is in its answer.
func (s *restoreSender) send(ctx context.Context, ops []restoreOp) ([]ringDocAnswer, error) {
	id := s.transport.NextRequestID()
	s.lastResent = false
	var lost error
	var quorumRefusals int
	var quorumWaited bool
	var firstQuorumRefusal time.Time
	for attempt := 0; attempt <= ringMaxResends; attempt++ {
		if attempt > 0 && !quorumWaited {
			s.lastResent = true
			s.resends++
			if err := s.pause(ctx, attempt); err != nil {
				return nil, err
			}
		}
		quorumWaited = false
		answers, err := s.transport.Send(ctx, ops, id)
		switch {
		case err == nil && anyQuorumRefusal(answers):
			// NO_QUORUM and LEASE_LAPSED mean the owner applied nothing and clear within moments,
			// so the frame is sent again under the same id for the quorum bound without spending
			// the few resends of the other transient answers. A frame answered both ways keeps
			// the order guard below.
			if ops[0].kind != restoreInsert && transientBeforeApplied(ops, answers) {
				return nil, &restoreOutOfOrderError{operations: len(ops), first: ops[0]}
			}
			if quorumRefusals == 0 {
				firstQuorumRefusal = time.Now()
			}
			quorumRefusals++
			if time.Since(firstQuorumRefusal) >= ringDumpQuorumBound {
				return answers, nil
			}
			if err := waitForQuorumRetry(ctx, quorumRefusals); err != nil {
				return nil, err
			}
			s.lastResent = true
			s.resends++
			// The wait is not one of the few resends of other transient answers, and it has
			// already paused.
			quorumWaited = true
			attempt--
		case err == nil:
			// An answer that clears by waiting (a slot that is moving, an owner that is down,
			// an operation the frame's deadline never reached) is not a refusal: the frame is
			// sent again whole under the same id, and what was applied answers Deduped.
			if attempt < ringMaxResends && anyTransient(answers) {
				// A whole-frame resend runs the operation that was not applied after the
				// ones that were, so a transient answer in front of an applied operation
				// on the same store could invert a queue's order. The server refuses every
				// later operation on a store behind a refused or unconfirmed one, so this
				// server never answers that way; should a frame come back so anyway, it is
				// not resent: the restore stops, outcome unknown. A transient answer with
				// nothing applied behind it on its store is safe to resend.
				if ops[0].kind != restoreInsert && transientBeforeApplied(ops, answers) {
					return nil, &restoreOutOfOrderError{operations: len(ops), first: ops[0]}
				}
				continue
			}
			return answers, nil
		case isLostConnection(err):
			lost = err
			if attempt < ringMaxResends {
				if redialErr := s.reconnect(ctx); redialErr != nil {
					lost = redialErr
				}
			}
		default:
			return nil, err
		}
	}
	return nil, &restoreUnknownError{operations: len(ops), first: ops[0], cause: lost}
}

// anyQuorumRefusal reports whether some answer is a refusal for a lost majority or a lapsed lease.
func anyQuorumRefusal(answers []ringDocAnswer) bool {
	for _, answer := range answers {
		if answer.Err != nil && ringDumpQuorumRefusal(answer.Err) {
			return true
		}
	}
	return false
}

// reconnect replaces the dead connection with a fresh one.
func (s *restoreSender) reconnect(ctx context.Context) error {
	fresh, err := s.redial(ctx)
	if err != nil {
		return err
	}
	s.transport = fresh
	s.redialed = append(s.redialed, fresh)
	return nil
}

// transientBeforeApplied reports whether some operation answered with a refusal that clears
// by waiting while a LATER operation on the same store of the same entity was applied. Only
// that order can be seen: a frame holds the operations of many entities, and the server runs
// the operations of different slots in parallel and goes on after a refused one on another
// store, so a slot that is moving makes its operations come back transient while others in the
// frame apply, which is normal and is resent. On one store the server refuses every operation
// behind a refused one or one whose outcome is unknown (restoreUnconfirmed), so this guard is a
// backstop that a correct server never trips. For one store, a whole-frame resend runs the
// not-applied push after the applied one, which reorders a fifo or lifo (arrival order) and a heap (the
// order of equal priorities); a put of distinct keys into a hash or btree commutes, and only
// a put of the same key twice would not.
func transientBeforeApplied(ops []restoreOp, answers []ringDocAnswer) bool {
	for first, answer := range answers {
		if answer.Err == nil || !anyTransient([]ringDocAnswer{answer}) {
			continue
		}
		for later := first + 1; later < len(answers); later++ {
			if answers[later].Err == nil && restoreOrderMatters(ops[first], ops[later]) {
				return true
			}
		}
	}
	return false
}

// restoreOrderMatters reports whether two operations of a frame, the first earlier, would
// give a different store if run the other way round.
func restoreOrderMatters(earlier, later restoreOp) bool {
	if earlier.kind == restoreInsert || later.kind == restoreInsert {
		return false
	}
	if earlier.database != later.database || earlier.bucket != later.bucket || earlier.id != later.id || earlier.field != later.field {
		return false
	}
	switch earlier.kind {
	case restorePush, restorePushHeap:
		return later.kind == earlier.kind
	case restorePut:
		return later.kind == restorePut && later.key == earlier.key
	}
	return false
}

// restoreOutOfOrderError says a store frame came back with an operation not applied in front
// of one that was, and resending it whole could reorder a queue, so the restore stopped.
type restoreOutOfOrderError struct {
	operations int
	first      restoreOp
}

func (e *restoreOutOfOrderError) Error() string {
	return fmt.Sprintf("a frame of %d store operations, the first being %s, came back with one operation not applied and a later one applied; sending it again could put a queue's elements out of order, so the restore stopped and its outcome is unknown", e.operations, e.first.describe())
}
