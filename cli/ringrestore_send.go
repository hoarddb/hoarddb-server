package cli

import (
	"context"
	"fmt"

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
// does not declare, a value it cannot encode) is that operation's refusal and the frame is
// not sent at all: every refused operation stops a restore, and a part of a frame sent
// without it would push into a document whose insert never went out.
func (t clientRestoreTransport) Send(ctx context.Context, ops []restoreOp, id [16]byte) ([]ringDocAnswer, error) {
	batch := t.client.NewBatch()
	answers := make([]ringDocAnswer, len(ops))
	for index, op := range ops {
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
		if err != nil {
			answers[index].Err = err
			return answers, nil
		}
	}
	result, err := batch.Send(ctx, ringclient.WithRequestID(id))
	if err != nil {
		return nil, err
	}
	return restoreAnswersOf(result), nil
}

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
	for attempt := 0; attempt <= ringMaxResends; attempt++ {
		if attempt > 0 {
			s.lastResent = true
			s.resends++
			if err := s.pause(ctx, attempt); err != nil {
				return nil, err
			}
		}
		answers, err := s.transport.Send(ctx, ops, id)
		switch {
		case err == nil:
			// An answer that clears by waiting (a slot that is moving, an owner that is down,
			// an operation the frame's deadline never reached) is not a refusal: the frame is
			// sent again whole under the same id, and what was applied answers Deduped.
			if attempt < ringMaxResends && anyTransient(answers) {
				// A whole-frame resend runs the operation that was not applied after the
				// ones that were, so a transient answer in front of an applied operation
				// of a store frame could invert a queue's order (a batch goes on after a
				// refused operation). That is not resent: the restore stops, outcome
				// unknown. A transient answer with nothing applied behind it (the frame's
				// deadline reached only its tail) is safe to resend.
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
// the operations of different slots in parallel and goes on after a refused one, so a slot
// that is moving makes its operations come back transient while others in the frame apply,
// which is normal and is resent. For one store, a whole-frame resend runs the not-applied
// push after the applied one, which reorders a fifo or lifo (arrival order) and a heap (the
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
