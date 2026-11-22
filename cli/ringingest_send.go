package cli

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/ringclient"
)

const (
	// ringMaxResends is how many times a frame is sent again after the first send, on a lost
	// connection or on answers that clear by waiting, before the outcome is called unknown.
	ringMaxResends = 3
	// ringFrameByteBudget is the most document bytes one frame is cut to. The frame limit is
	// 32 MiB; a small budget keeps a frame's apply time short and its resend cheap.
	ringFrameByteBudget = 2 << 20
	// ringMaxBatchDocuments is the most operations one RING_BATCH carries.
	ringMaxBatchDocuments = 255
)

// ringIngestDoc is one document ready to send: its rendered id, its scalar fields, and the
// position and bytes needed to report or reject it.
type ringIngestDoc struct {
	// Ordinal is the document's position in the file, counted from 1.
	Ordinal int64
	// ID is the rendered _id (ringid.go).
	ID string
	// Fields are the document's top-level members except _id.
	Fields ringclient.Doc
	// Source is the document's input bytes, for the rejects file.
	Source []byte
	// Bytes is the size that counts against a frame's byte budget.
	Bytes int
}

// ringDocAnswer is what the ring said about one document of a frame.
type ringDocAnswer struct {
	// Err is the refusal of this document, or nil when it was answered.
	Err error
	// Deduped says an earlier send of the frame under the same request id had applied it.
	Deduped bool
}

// ringDocFailure is a document the ring refused or the client could not build.
type ringDocFailure struct {
	Doc ringIngestDoc
	Err error
}

// text is the sentence for the report: the document by position, then the ring's own words.
func (f ringDocFailure) text(file string) string {
	var refusal *ringclient.Error
	if errors.As(f.Err, &refusal) {
		return fmt.Sprintf("Document %s of %s was refused: %s: %s", formatCount(f.Doc.Ordinal), file, refusal.Code, refusal.Message)
	}
	return fmt.Sprintf("Document %s of %s was refused: %s", formatCount(f.Doc.Ordinal), file, strings.TrimPrefix(f.Err.Error(), "ringclient: "))
}

// ringSendStats counts what the sender did, for the report.
type ringSendStats struct {
	Applied, Deduped int64
	Resends          int
	Halvings         int
}

// ringOutcomeUnknownError says a frame was sent and never answered, even after the resends:
// the documents it held may or may not have been written. The report names the span so the
// user can run the same command again, which is safe because every document is an upsert.
type ringOutcomeUnknownError struct {
	Documents                 int
	FirstOrdinal, LastOrdinal int64
	Cause                     error
}

// Error states the facts.
func (e *ringOutcomeUnknownError) Error() string {
	return fmt.Sprintf("documents %s to %s (%s of them) were sent and the ring never confirmed them: %v",
		formatCount(e.FirstOrdinal), formatCount(e.LastOrdinal), formatCount(int64(e.Documents)), e.Cause)
}

func (e *ringOutcomeUnknownError) Unwrap() error { return e.Cause }

// ringBatchTransport is what the sender needs of a connection. It is the seam a test
// replaces; the real one is clientBatchTransport.
type ringBatchTransport interface {
	// NextRequestID returns a request id no other request of this connection uses.
	NextRequestID() [16]byte
	// Upserts sends the documents as one RING_BATCH under id and returns one answer per
	// document, in order. A refusal of the whole frame, or a lost connection, is the error.
	Upserts(ctx context.Context, database, bucket string, docs []ringIngestDoc, id [16]byte) ([]ringDocAnswer, error)
	close() error
}

// clientBatchTransport is the ringBatchTransport of a live client.
type clientBatchTransport struct{ client *ringclient.Client }

// NextRequestID implements ringBatchTransport.
func (t clientBatchTransport) NextRequestID() [16]byte { return t.client.NextRequestID() }

func (t clientBatchTransport) close() error { return t.client.Close() }

// Upserts implements ringBatchTransport. A document the client refuses while queueing it (a
// field the bucket does not declare, a value it cannot encode) is that document's failure and
// the rest of the frame is sent without it.
func (t clientBatchTransport) Upserts(ctx context.Context, database, bucket string, docs []ringIngestDoc, id [16]byte) ([]ringDocAnswer, error) {
	batch := t.client.NewBatch()
	queueing := t.client.Bucket(database, bucket).In(batch)
	answers := make([]ringDocAnswer, len(docs))
	queued := make([]int, 0, len(docs))
	for index, doc := range docs {
		if _, err := queueing.Upsert(ctx, doc.ID, doc.Fields); err != nil {
			answers[index].Err = err
			continue
		}
		queued = append(queued, index)
	}
	if len(queued) == 0 {
		return answers, nil
	}
	result, err := batch.Send(ctx, ringclient.WithRequestID(id))
	if err != nil {
		return nil, err
	}
	for position, answer := range result.Answers {
		answers[queued[position]] = ringDocAnswer{Err: answer.Err, Deduped: answer.Write.Deduped}
	}
	return answers, nil
}

// ringBatchSender writes documents to one bucket as upserts in frames of at most maxDocuments
// documents and maxBytes bytes. A frame keeps one request id for its whole life: on a lost
// connection the sender dials again, builds the same frame from the same documents and sends
// it under that id, so the owner answers Deduped for what it had already applied.
type ringBatchSender struct {
	database, bucket string
	transport        ringBatchTransport
	// redial returns a transport on a fresh connection, for a frame whose connection died.
	redial func(ctx context.Context) (ringBatchTransport, error)
	// pause waits before resend number attempt; a test replaces it.
	pause func(ctx context.Context, attempt int) error

	maxDocuments, maxBytes int
	stats                  ringSendStats

	pending      []ringIngestDoc
	pendingBytes int
	// redialed are the transports this sender opened, closed by close.
	redialed []ringBatchTransport
}

// newRingBatchSender sends to bucket through client, and dials again through redial.
func newRingBatchSender(client *ringclient.Client, redial ringRedial, database, bucket string, batch int) *ringBatchSender {
	sender := &ringBatchSender{
		database: database, bucket: bucket, transport: clientBatchTransport{client},
		maxDocuments: min(batch, ringMaxBatchDocuments), maxBytes: ringFrameByteBudget,
		pause: pauseBeforeResend,
	}
	sender.redial = func(ctx context.Context) (ringBatchTransport, error) {
		fresh, err := redial(ctx)
		if err != nil {
			return nil, err
		}
		return clientBatchTransport{fresh}, nil
	}
	return sender
}

// pauseBeforeResend waits a little longer before each resend, so a slot that is moving has
// time to finish, and stops at once when the run is cancelled.
func pauseBeforeResend(ctx context.Context, attempt int) error {
	timer := time.NewTimer(time.Duration(attempt) * 200 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return fmt.Errorf("stopped while waiting to resend: %w", ctx.Err())
	case <-timer.C:
		return nil
	}
}

// close closes the connections the sender opened itself. The caller owns the first one.
func (s *ringBatchSender) close() {
	for _, transport := range s.redialed {
		_ = transport.close()
	}
	s.redialed = nil
}

// add queues a document and sends the frame when it is full, returning the documents of that
// frame the ring refused.
func (s *ringBatchSender) add(ctx context.Context, doc ringIngestDoc) ([]ringDocFailure, error) {
	var failures []ringDocFailure
	if len(s.pending) > 0 && (len(s.pending) >= s.maxDocuments || s.pendingBytes+doc.Bytes > s.maxBytes) {
		flushed, err := s.flush(ctx)
		if err != nil {
			return flushed, err
		}
		failures = flushed
	}
	s.pending = append(s.pending, doc)
	s.pendingBytes += doc.Bytes
	return failures, nil
}

// flush sends what is queued.
func (s *ringBatchSender) flush(ctx context.Context) ([]ringDocFailure, error) {
	if len(s.pending) == 0 {
		return nil, nil
	}
	docs := s.pending
	s.pending, s.pendingBytes = nil, 0
	return s.sendFrame(ctx, docs)
}

// sendFrame sends docs as one frame, resending and halving as the answers require. The
// returned failures are documents refused one by one; the error is a refusal of the whole
// frame, or ringOutcomeUnknownError when no send was ever answered.
func (s *ringBatchSender) sendFrame(ctx context.Context, docs []ringIngestDoc) ([]ringDocFailure, error) {
	id := s.transport.NextRequestID()
	var lost error
	for attempt := 0; attempt <= ringMaxResends; attempt++ {
		if attempt > 0 {
			s.stats.Resends++
			if err := s.pause(ctx, attempt); err != nil {
				return nil, err
			}
		}
		answers, err := s.transport.Upserts(ctx, s.database, s.bucket, docs, id)
		switch {
		case err == nil:
			if attempt < ringMaxResends && anyTransient(answers) {
				continue
			}
			return s.settle(docs, answers), nil
		case isFrameTooLarge(err):
			return s.halve(ctx, docs, err)
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
	return nil, &ringOutcomeUnknownError{Documents: len(docs), FirstOrdinal: docs[0].Ordinal, LastOrdinal: docs[len(docs)-1].Ordinal, Cause: lost}
}

// reconnect replaces the dead connection with a fresh one.
func (s *ringBatchSender) reconnect(ctx context.Context) error {
	fresh, err := s.redial(ctx)
	if err != nil {
		return err
	}
	s.transport = fresh
	s.redialed = append(s.redialed, fresh)
	return nil
}

// settle counts the answers and returns the documents that were refused.
func (s *ringBatchSender) settle(docs []ringIngestDoc, answers []ringDocAnswer) []ringDocFailure {
	var failures []ringDocFailure
	for index, answer := range answers {
		switch {
		case answer.Err != nil:
			failures = append(failures, ringDocFailure{Doc: docs[index], Err: answer.Err})
		case answer.Deduped:
			s.stats.Deduped++
		default:
			s.stats.Applied++
		}
	}
	return failures
}

// halve splits a frame the ring refused for its size, remembers the smaller size for the
// frames that follow, and sends the halves. One document that is too large alone is that
// document's failure.
func (s *ringBatchSender) halve(ctx context.Context, docs []ringIngestDoc, refusal error) ([]ringDocFailure, error) {
	if len(docs) == 1 {
		return []ringDocFailure{{Doc: docs[0], Err: refusal}}, nil
	}
	s.stats.Halvings++
	half := len(docs) / 2
	s.maxDocuments = min(s.maxDocuments, half)
	var bytes int
	for _, doc := range docs {
		bytes += doc.Bytes
	}
	s.maxBytes = min(s.maxBytes, max(bytes/2, 1))
	first, err := s.sendFrame(ctx, docs[:half])
	if err != nil {
		return first, err
	}
	second, err := s.sendFrame(ctx, docs[half:])
	return append(first, second...), err
}

// anyTransient reports whether an answer is a refusal that clears by waiting (a slot that is
// moving, an owner that is down): the document is not at fault, so the frame is sent again.
func anyTransient(answers []ringDocAnswer) bool {
	for _, answer := range answers {
		var refusal *ringclient.Error
		if errors.As(answer.Err, &refusal) && refusal.Code.Exit() == ExitUnavailable {
			return true
		}
	}
	return false
}

// isFrameTooLarge recognises a frame over the size limit, on either side of the wire. The ring
// client refuses to encode one before sending it ("a frame holds at most N bytes", a
// cluster.Error that INVALID_ARGUMENT names), and a server that is sent one anyway answers
// INVALID_ARGUMENT naming "the frame limit". The ring has no BATCH_TOO_LARGE code of its own
// (build plan 7.4 item 11).
func isFrameTooLarge(err error) bool {
	var refusal *ringclient.Error
	if errors.As(err, &refusal) {
		return refusal.Code == cluster.CodeInvalidArgument && strings.Contains(refusal.Message, "frame limit")
	}
	var encoding *cluster.Error
	return errors.As(err, &encoding) && encoding.Code == cluster.CodeInvalidArgument && strings.Contains(encoding.Message, "a frame holds at most")
}

// isLostConnection reports whether err is the connection ending or a request going
// unanswered, so that the frame may or may not have been applied. A refusal from the ring is
// an answer, not a lost connection.
func isLostConnection(err error) bool {
	var refusal *ringclient.Error
	if errors.As(err, &refusal) {
		return false
	}
	return ringFailure(err).Exit == ExitUnavailable
}
