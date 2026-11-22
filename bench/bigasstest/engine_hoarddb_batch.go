package main

import (
	"context"
	"fmt"

	"github.com/hoarddb/hoarddb-server/bench/scenarios/model"
	"github.com/hoarddb/hoarddb-server/ringclient"
)

// ringBatchMax is the most operations one batch frame carries (the wire numbers them in one byte).
const ringBatchMax = 255

// ringWrite is one store verb of a write: a document upsert, or an element push or put.
type ringWrite struct {
	document *model.Document
	element  *model.Element
	keyed    bool
}

// describe names the write for an error message.
func (w ringWrite) describe(database string) string {
	if w.document != nil {
		return fmt.Sprintf("storing %s %s in %s", w.document.Bucket, w.document.ID, database)
	}
	return fmt.Sprintf("writing %s of %s %s in %s", w.element.Store, w.element.Bucket, w.element.Owner, database)
}

// ringBatchMode is the word the settings and the record carry for the batch flag.
func ringBatchMode(batch bool) string {
	if batch {
		return "on"
	}
	return "off"
}

// ringMultiGetMode is the word the settings and the record carry for the multiget flag.
func ringMultiGetMode(multiget bool) string {
	if multiget {
		return "on"
	}
	return "off"
}

// ringWrites lists the store verbs of a write in the order they are applied: the documents
// first (upserted, so repeating a write is harmless), then the elements.
func (o *ringOps) ringWrites(op WriteOp) ([]ringWrite, error) {
	writes := make([]ringWrite, 0, len(op.Documents)+len(op.Elements))
	for i := range op.Documents {
		writes = append(writes, ringWrite{document: &op.Documents[i]})
	}
	for i := range op.Elements {
		element := &op.Elements[i]
		kind, err := KindOf(o.layout, element.Bucket, element.Store)
		if err != nil {
			return nil, fmt.Errorf("ring: %w", err)
		}
		if o.native {
			if element, err = withCopy(op, element); err != nil {
				return nil, err
			}
		}
		writes = append(writes, ringWrite{element: element, keyed: kind.Keyed()})
	}
	return writes, nil
}

// ringFrames groups the writes into the requests that carry them. Unbatched, every write is
// its own request. Batched, they share frames of at most ringBatchMax, in order.
func ringFrames(writes []ringWrite, batch bool) [][]ringWrite {
	if len(writes) == 0 {
		return nil
	}
	size := 1
	if batch {
		size = ringBatchMax
	}
	frames := make([][]ringWrite, 0, (len(writes)+size-1)/size)
	for len(writes) > 0 {
		n := min(size, len(writes))
		frames = append(frames, writes[:n])
		writes = writes[n:]
	}
	return frames
}

// sendWrite sends one write as its own request.
func (o *ringOps) sendWrite(ctx context.Context, write ringWrite) error {
	var (
		outcome ringclient.WriteResult
		err     error
	)
	switch {
	case write.document != nil:
		doc := write.document
		outcome, err = o.bucket(doc.Bucket).Upsert(ctx, doc.ID, ringclient.Doc(ringDoc(doc.Fields)))
	case write.keyed:
		element := write.element
		outcome, err = o.bucket(element.Bucket).Put(ctx, element.Owner, element.Store, element.Key, ringBody(element.Body))
	default:
		element := write.element
		outcome, err = o.bucket(element.Bucket).Push(ctx, element.Owner, element.Store, ringBody(element.Body))
	}
	return acknowledged(write.describe(o.database), outcome, err)
}

// sendFrame sends several writes as one batch frame. A batch is not a transaction: a refused
// operation does not stop the others, so one refusal fails the write but leaves the rest
// applied, which the audit's ceiling allows for an unacknowledged write.
func (o *ringOps) sendFrame(ctx context.Context, frame []ringWrite) error {
	batch := o.client.NewBatch()
	for _, write := range frame {
		var err error
		switch {
		case write.document != nil:
			doc := write.document
			_, err = o.bucket(doc.Bucket).In(batch).Upsert(ctx, doc.ID, ringclient.Doc(ringDoc(doc.Fields)))
		case write.keyed:
			element := write.element
			_, err = o.bucket(element.Bucket).In(batch).Put(ctx, element.Owner, element.Store, element.Key, ringBody(element.Body))
		default:
			element := write.element
			_, err = o.bucket(element.Bucket).In(batch).Push(ctx, element.Owner, element.Store, ringBody(element.Body))
		}
		if err != nil {
			return fmt.Errorf("queuing %s: %w", write.describe(o.database), err)
		}
	}
	result, err := batch.Send(ctx)
	if err != nil {
		return fmt.Errorf("sending a batch of %d writes to %s: %w", len(frame), o.database, err)
	}
	if len(result.Answers) != len(frame) {
		return fmt.Errorf("a batch of %d writes to %s came back with %d answers", len(frame), o.database, len(result.Answers))
	}
	for i, answer := range result.Answers {
		if err := acknowledged(fmt.Sprintf("operation %d of a batch of %d: %s", i+1, len(frame), frame[i].describe(o.database)), answer.Write, answer.Err); err != nil {
			return err
		}
	}
	return nil
}

// applyWrites sends the writes and returns how many requests it took. A frame of one write
// goes as a plain request: a batch of one saves nothing and costs a frame header.
func (o *ringOps) applyWrites(ctx context.Context, writes []ringWrite) (int, error) {
	trips := 0
	for _, frame := range ringFrames(writes, o.batch) {
		trips++
		var err error
		if len(frame) == 1 {
			err = o.sendWrite(ctx, frame[0])
		} else {
			err = o.sendFrame(ctx, frame)
		}
		if err != nil {
			return trips, err
		}
	}
	return trips, nil
}
