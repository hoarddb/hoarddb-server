package wire

import (
	"context"
	"errors"
	"fmt"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

// NearestAnswer is what Reader.Nearest returns: the kept elements best first with their keys (hash, btree, heap),
// their scores, and the walk's counts.
type NearestAnswer struct {
	FieldElements
	Scores  []float64
	Scanned int
	Skipped int
}

// Nearest scores a whole store field against query under one snapshot, through the gate a document read
// goes through (routeField), and returns the best query.K. A zero query.MaxElements takes the reader's
// configured ceiling. It takes no position and writes nothing.
func (r *Reader) Nearest(ctx context.Context, field FieldRead, query store.NearestRequest) (NearestAnswer, error) {
	if query.MaxElements == 0 {
		query.MaxElements = uint64(r.cfg.NearestMaxElements)
	}
	query.MaxReplyBytes, query.ReplyOverhead = r.replyBytes(field.Request), peekElementFraming
	return routeField(ctx, r, fieldTarget{Request: field.Request, FieldID: field.FieldID, Engine: field.Engine},
		func(snapshot storage.KVSnapshot, ref store.FieldRef, admitted ReadVerdict) (NearestAnswer, error) {
			return r.nearestElements(ctx, snapshot, ref, field, query)
		}, nil, func(ctx context.Context, owner string, hopped ReadRequest) (NearestAnswer, error) {
			answer, err := r.cfg.Forward.ForwardFieldRead(ctx, owner, FieldForward{Req: hopped, Op: FieldOpNearest, FieldID: field.FieldID, Engine: field.Engine,
				Metric: query.Metric, K: query.K, VectorField: query.Field, Vector: query.Vector})
			return answer.Nearest, err
		})
}

// nearestElements is the body of a nearest: it returns its answer and keeps no state, so a
// confirm pass that re-reads discards the first answer whole.
func (r *Reader) nearestElements(ctx context.Context, snapshot storage.KVSnapshot, ref store.FieldRef, field FieldRead, query store.NearestRequest) (NearestAnswer, error) {
	// The store charges the kept elements by size before it copies one, so a refusal holds no values.
	result, err := store.FieldNearest(ctx, snapshot, ref, query)
	var tooBig *store.NearestReplyTooLargeError
	if errors.As(err, &tooBig) {
		return NearestAnswer{}, &cluster.Error{Code: cluster.CodeInvalidArgument, Message: fmt.Sprintf(
			"the nearest %d elements of field %d of %s hold more than the %d bytes one reply carries. Nothing was returned. Ask for a smaller k",
			tooBig.Hits, field.FieldID, field.Request.idText(), query.MaxReplyBytes)}
	}
	if err != nil {
		if field.Request.FromNode {
			// The sender's node maps these typed errors with names it resolves itself; across a
			// peer link they would reach the client as an internal error, so the owner codes them.
			if refusal := forwardedNearestRefusal(err, field); refusal != nil {
				return NearestAnswer{}, refusal
			}
		}
		return NearestAnswer{}, fmt.Errorf("nearest in field %d of %s: %w", field.FieldID, field.Request.idText(), err)
	}
	var answer NearestAnswer
	for _, hit := range result.Hits {
		answer.add(field.Engine, hit.Entry)
		answer.Scores = append(answer.Scores, hit.Score)
	}
	answer.Scanned, answer.Skipped = result.Scanned, result.Skipped
	return answer, nil
}

// forwardedNearestRefusal is the coded refusal of a nearest the store refused because of its query
// or its elements, for the owner to send a peer; nil for any other error.
func forwardedNearestRefusal(err error, field FieldRead) *cluster.Error {
	var tooLarge *store.NearestTooLargeError
	var element *store.NearestElementError
	var badVector *store.NearestVectorError
	where := fmt.Sprintf("field %d of %s", field.FieldID, field.Request.idText())
	switch {
	case errors.As(err, &badVector):
		return &cluster.Error{Code: cluster.CodeInvalidArgument, Message: badVector.Error()}
	case errors.As(err, &tooLarge):
		return &cluster.Error{Code: cluster.CodeInvalidArgument, Message: fmt.Sprintf(
			"%s holds %d elements and a nearest scans at most %d in one call (query.nearest_max_elements, HOARDDB_QUERY_NEAREST_MAX_ELEMENTS on the owner). "+
				"Nothing was read. Split the entity across documents, keep fewer elements in this field, or raise query.nearest_max_elements",
			where, tooLarge.Count, tooLarge.Max)}
	case errors.As(err, &element):
		return &cluster.Error{Code: cluster.CodeInvalidArgument, Message: fmt.Sprintf("nearest on %s: %s", where, element.Error())}
	}
	return nil
}
