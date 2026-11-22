package wire

import (
	"context"
	"errors"
	"fmt"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/store"
)

// NearestRequest asks for the elements of a store field closest to Vector.
type NearestRequest struct {
	StoreTarget        // names the store field (its Field member is the store field, not the vector field)
	Metric      byte   // RingMetricCosine, RingMetricDot or RingMetricL2
	K           int    // 1 to 100
	VectorField string // the element field that holds the vector (not StoreTarget.Field)
	Vector      []byte // 4 to 16384 bytes of little-endian float32
}

// NearestResult is a nearest's answer. Keys is empty for a fifoStore or lifoStore.
type NearestResult struct {
	Elements [][]byte
	Keys     []string
	Scores   []float64
	Scanned  int
	Skipped  int
}

// Nearest returns the elements of a fifoStore, lifoStore, heapStore, hashStore or btreeStore field closest to
// req.Vector, best first. It is a read: no position, no log record, nothing removed.
func (n *Node) Nearest(ctx context.Context, req NearestRequest) (NearestResult, error) {
	call, err := n.fieldCall(ctx, req.StoreTarget, "nearest")
	if err != nil {
		return NearestResult{}, err
	}
	return n.nearestFieldOf(ctx, call, req.ID, req)
}

// nearestFieldOf is Nearest on a resolved field; id is how the caller spelled the document, for refusal texts, and may be empty.
func (n *Node) nearestFieldOf(ctx context.Context, call fieldCall, id string, req NearestRequest) (NearestResult, error) {
	if req.Metric < RingMetricCosine || req.Metric > RingMetricL2 {
		return NearestResult{}, ringRefusal("a nearest metric is 0x%02x; send 0x%02x (cosine), 0x%02x (dot) or 0x%02x (l2)",
			req.Metric, RingMetricCosine, RingMetricDot, RingMetricL2)
	}
	slot := call.ref.slot(call.encID)
	n.own.Acquire(slot, call.ref.dbID)
	answer, err := n.reader.Nearest(ctx, FieldRead{
		Request: ReadRequest{Slot: slot, DBID: call.ref.dbID, BucketID: call.ref.entry.Decl.BucketID, EncID: call.encID, ID: id},
		FieldID: call.fieldID, Engine: call.engine,
	}, store.NearestRequest{Metric: store.NearestMetric(req.Metric), K: req.K, Field: req.VectorField, Vector: req.Vector})
	if err != nil {
		var tooLarge *store.NearestTooLargeError
		var element *store.NearestElementError
		var badVector *store.NearestVectorError
		switch {
		case errors.As(err, &badVector):
			return NearestResult{}, &cluster.Error{Code: cluster.CodeInvalidArgument, Message: badVector.Error()}
		case errors.As(err, &tooLarge):
			return NearestResult{}, &cluster.Error{Code: cluster.CodeInvalidArgument, Message: fmt.Sprintf(
				"%s.%s.%s holds %d elements and a nearest scans at most %d in one call (query.nearest_max_elements, HOARDDB_QUERY_NEAREST_MAX_ELEMENTS). "+
					"Nothing was read. Split the entity across documents, keep fewer elements in this field, or raise query.nearest_max_elements",
				call.ref.db, call.ref.name, call.fieldName(), tooLarge.Count, tooLarge.Max)}
		case errors.As(err, &element):
			return NearestResult{}, &cluster.Error{Code: cluster.CodeInvalidArgument, Message: fmt.Sprintf(
				"nearest on %s.%s.%s: %s", call.ref.db, call.ref.name, call.fieldName(), element.Error())}
		}
		return NearestResult{}, fmt.Errorf("nearest %s.%s of %q: %w", call.ref.name, call.fieldName(), id, err)
	}
	return NearestResult{Elements: answer.Elements, Keys: answer.Keys, Scores: answer.Scores, Scanned: answer.Scanned, Skipped: answer.Skipped}, nil
}
