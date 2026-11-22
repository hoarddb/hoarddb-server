package server

import (
	"context"
	"errors"
	"fmt"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// routeBulk splits a BATCH_PUT or BATCH_UPSERT by part (section 2.1 step 3):
// one write per part, sent to the part's owner, each atomic in its part (N3),
// the parts in order of first appearance. The answer gives every position in
// request order; when any part was refused it is an error answer whose
// write_outcome is not_applied, with each position's own outcome in results.
// A BATCH_PUT that repeats an _id_ is refused whole, before anything is
// marked, created or sent: a repeat is in one part, and the part alone would
// refuse only its own sub-batch.
func (r *spreadRouter) routeBulk(ctx context.Context, entry ReplEntry, wc WriteConcernLevel, born bornSet) (ReplResponse, error) {
	op := transport.Opcode(entry.Op)
	if op == transport.OpBatchPut {
		if _, err := resolveWritePayload(op, entry.Payload, false); err != nil {
			var repeated *repeatedIDError
			if errors.As(err, &repeated) {
				return ReplResponse{}, withOutcome(outcomeNotApplied, err)
			}
		}
	}
	documents, ids, err := bulkDocuments(entry.Payload)
	if err != nil {
		return ReplResponse{}, withOutcome(outcomeNotApplied, err)
	}
	var order []int
	positions := make(map[int][]int)
	for position, id := range ids {
		if id == "" {
			return ReplResponse{}, withOutcome(outcomeNotApplied, &sentencedError{cause: ErrMalformedPayload, message: fmt.Sprintf(
				"document %d of a bulk write to spread bucket %q (database %q) has no _id_ text", position+1, r.t.name, r.t.database)})
		}
		p, err := r.t.partOfKey(id)
		if err != nil {
			return ReplResponse{}, withOutcome(outcomeNotApplied, err)
		}
		if _, seen := positions[p]; !seen {
			order = append(order, p)
		}
		positions[p] = append(positions[p], position)
	}
	answers := make([]bulkPartAnswer, 0, len(order))
	for _, p := range order {
		answers = append(answers, r.writeBulkPart(ctx, entry, wc, born, p, positions[p], documents))
	}
	return assembleBulk(op, ids, answers)
}

// bulkPartAnswer is one part's answer to its share of a bulk write.
type bulkPartAnswer struct {
	positions []int
	resp      ReplResponse
	err       error
}

// writeBulkPart creates part p if it is not created yet and sends it its
// documents.
func (r *spreadRouter) writeBulkPart(ctx context.Context, entry ReplEntry, wc WriteConcernLevel, born bornSet, p int, positions []int, documents []bson.Raw) bulkPartAnswer {
	answer := bulkPartAnswer{positions: positions}
	if born.state(p) != partCreated {
		if answer.err = r.ensureCreated(ctx, p); answer.err != nil {
			return answer
		}
	}
	share := make(bson.A, 0, len(positions))
	for _, position := range positions {
		share = append(share, documents[position])
	}
	payload, err := stampPayload(entry.Payload, bson.D{{Key: "data", Value: share}})
	if err != nil {
		answer.err = withOutcome(outcomeNotApplied, err)
		return answer
	}
	partEntry := entry
	partEntry.Payload = payload
	answer.resp, answer.err = r.writeToPart(ctx, partEntry, p, wc)
	return answer
}

// bulkDocuments reads a bulk write's documents, undecoded, and each one's
// _id_. The door minted every missing id before the router ran.
func bulkDocuments(payload []byte) ([]bson.Raw, []string, error) {
	data, err := bson.Raw(payload).LookupErr("data")
	if err != nil {
		return nil, nil, &sentencedError{cause: ErrMalformedPayload, message: "a bulk write carries its documents in data: send data as a list of documents"}
	}
	array, ok := data.ArrayOK()
	if !ok {
		return nil, nil, &sentencedError{cause: ErrMalformedPayload, message: "a bulk write's data must be a list of documents"}
	}
	values, err := bson.Raw(array).Values()
	if err != nil {
		return nil, nil, fmt.Errorf("reading the documents of a bulk write: %w", err)
	}
	documents := make([]bson.Raw, len(values))
	ids := make([]string, len(values))
	for position, value := range values {
		document, ok := value.DocumentOK()
		if !ok {
			return nil, nil, &sentencedError{cause: ErrMalformedPayload, message: fmt.Sprintf("document %d of a bulk write is not a document", position+1)}
		}
		documents[position] = bson.Raw(document)
		if id, err := bson.Raw(document).LookupErr("_id_"); err == nil {
			ids[position], _ = id.StringValueOK()
		}
	}
	return documents, ids, nil
}

// assembleBulk puts the parts' answers back in request order.
func assembleBulk(op transport.Opcode, ids []string, answers []bulkPartAnswer) (ReplResponse, error) {
	results := make([]WriteResult, len(ids))
	okOpcode := transport.OpBatchPutOK
	if op == transport.OpBatchUpsert {
		okOpcode = transport.OpBatchUpsertOK
	}
	var (
		firstRefusal  *ErrorResponse
		count         int32
		confirmed     = -1
		factor        int
		appliedAnyway bool
	)
	replaced := appliedResults(ids)
	for _, answer := range answers {
		refusal, applied := bulkPartOutcome(answer)
		for index, position := range answer.positions {
			result := WriteResult{ID: ids[position], WriteOutcome: outcomeApplied}
			switch {
			case refusal != nil && index < len(refusal.Results):
				result = refusal.Results[index]
			case refusal != nil:
				result = WriteResult{ID: ids[position], WriteOutcome: outcomeNotApplied, Code: int32(refusal.Code), Message: refusal.Message}
			case applied != nil && index < len(applied.Results):
				// A repeat is in one part, but its "replaced by" names a
				// position in the part's share: the request's is named.
				result = applied.Results[index]
				result.Message = replaced[position].Message
			}
			results[position] = result
		}
		if refusal != nil {
			if firstRefusal == nil {
				firstRefusal = refusal
			}
			continue
		}
		appliedAnyway = true
		count += applied.Count
		if confirmed < 0 || applied.CopiesConfirmed < confirmed {
			confirmed = applied.CopiesConfirmed
		}
		factor = max(factor, applied.ReplicationFactor)
	}
	if firstRefusal != nil {
		message := firstRefusal.Message
		if appliedAnyway {
			message += " Documents of other parts of the bucket were stored: results gives each document's outcome."
		}
		payload, err := bson.Marshal(ErrorResponse{Code: firstRefusal.Code, Message: message, WriteOutcome: outcomeNotApplied, Results: results})
		if err != nil {
			return ReplResponse{}, fmt.Errorf("encoding the answer of a bulk write: %w", err)
		}
		return ReplResponse{Opcode: int32(transport.OpError), Payload: payload}, nil
	}
	answer := BatchInsertResponse{IDs: ids, Count: count, Success: true, Results: results, ReplicationFactor: factor}
	if confirmed > 0 {
		answer.CopiesConfirmed = confirmed
	}
	payload, err := bson.Marshal(answer)
	if err != nil {
		return ReplResponse{}, fmt.Errorf("encoding the answer of a bulk write: %w", err)
	}
	return ReplResponse{Opcode: int32(okOpcode), Payload: payload}, nil
}

// bulkPartOutcome reads one part's answer: its refusal, or what it applied.
func bulkPartOutcome(answer bulkPartAnswer) (refusal *ErrorResponse, applied *BatchInsertResponse) {
	if answer.err != nil {
		return &ErrorResponse{Code: int(codeFor(answer.err)), Message: explainError(answer.err)}, nil
	}
	if transport.Opcode(answer.resp.Opcode) == transport.OpError {
		var refused ErrorResponse
		if err := bson.Unmarshal(answer.resp.Payload, &refused); err != nil {
			refused = ErrorResponse{Code: int(transport.ErrInternal), Message: fmt.Sprintf("decoding a part's refusal: %v", err)}
		}
		return &refused, nil
	}
	var done BatchInsertResponse
	if err := bson.Unmarshal(answer.resp.Payload, &done); err != nil {
		return &ErrorResponse{Code: int(transport.ErrInternal), Message: fmt.Sprintf("decoding a part's answer: %v", err)}, nil
	}
	if !done.Success {
		return &ErrorResponse{Code: int(transport.ErrInternal), Message: done.Message}, nil
	}
	return nil, &done
}
