package store

import (
	"bytes"
	"container/heap"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
)

// NearestMetric is how a nearest query orders elements. The values are the wire bytes of
// cluster/wire (0x01 cosine, 0x02 dot, 0x03 l2); the zero value is invalid.
type NearestMetric uint8

const (
	// NearestCosine orders by cosine similarity, higher is closer, 1 is identical.
	NearestCosine NearestMetric = 1
	// NearestDot orders by inner product, higher is closer.
	NearestDot NearestMetric = 2
	// NearestL2 orders by Euclidean distance, lower is closer.
	NearestL2 NearestMetric = 3
)

// nearestQuery is a decoded query vector with its norm computed once, so a scan of a
// field does not recompute it per element.
type nearestQuery struct {
	values []float32
	norm   float64
}

// newNearestQuery decodes raw, little-endian float32 values. The caller has already checked
// the length; a trailing partial value is ignored rather than read.
func newNearestQuery(raw []byte) nearestQuery {
	values := make([]float32, len(raw)/4)
	var sum float64
	for i := range values {
		values[i] = math.Float32frombits(binary.LittleEndian.Uint32(raw[4*i:]))
		sum += float64(values[i]) * float64(values[i])
	}
	return nearestQuery{values: values, norm: math.Sqrt(sum)}
}

// score scores vector, the raw bytes of one stored element, against the query. Accumulation
// is in float64 in index order, so the same inputs always give the same score. Cosine is 0
// when either norm is 0. An element of another length, a NaN or infinite element value, or
// a non-finite result scores nearestWorst(metric).
func (q nearestQuery) score(metric NearestMetric, vector []byte) float64 {
	if len(vector) != 4*len(q.values) {
		return nearestWorst(metric)
	}
	var dot, sumSquares float64
	for i, qv := range q.values {
		v := float64(math.Float32frombits(binary.LittleEndian.Uint32(vector[4*i:])))
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return nearestWorst(metric)
		}
		qf := float64(qv)
		dot += qf * v
		if metric == NearestL2 {
			d := qf - v
			sumSquares += d * d
		} else {
			sumSquares += v * v
		}
	}

	var result float64
	switch metric {
	case NearestDot:
		result = dot
	case NearestL2:
		result = math.Sqrt(sumSquares)
	default:
		if q.norm == 0 || sumSquares == 0 {
			return 0
		}
		result = dot / (q.norm * math.Sqrt(sumSquares))
	}
	if math.IsNaN(result) || math.IsInf(result, 0) {
		return nearestWorst(metric)
	}
	return result
}

// nearestWorst is the value an unscorable element gets. The values are finite because a
// JSON driver cannot carry an infinity. Cosine is the default because the metric is
// validated before a query reaches this file.
func nearestWorst(metric NearestMetric) float64 {
	switch metric {
	case NearestDot:
		return -math.MaxFloat64
	case NearestL2:
		return math.MaxFloat64
	default:
		return -1
	}
}

// nearestBetter reports whether score a is strictly closer than score b under metric.
func nearestBetter(metric NearestMetric, a, b float64) bool {
	if metric == NearestL2 {
		return a < b
	}
	return a > b
}

// NearestTooLargeError says a field holds more elements than one nearest may scan. It is
// returned before any element is read.
type NearestTooLargeError struct {
	Count uint64
	Max   uint64
}

// Error names both counts and the setting that bounds them.
func (e *NearestTooLargeError) Error() string {
	return fmt.Sprintf("the field holds %d elements and a nearest scans at most %d (setting query.nearest_max_elements, env HOARDDB_QUERY_NEAREST_MAX_ELEMENTS). Raise that setting, or search a smaller field", e.Count, e.Max)
}

// NearestElementError says one element cannot be compared with the query. Key is the element
// key (hash, btree), the entry id (heap) or "position <n>" (fifo, lifo).
type NearestElementError struct {
	Key       string
	Field     string
	NotBinary bool
	Length    int
	Want      int
}

// Error names the element and the field, then the fix. NotBinary is set when the field holds
// a value that is not a BSON binary of subtype 0x00; otherwise Length and Want are the two
// vector lengths.
func (e *NearestElementError) Error() string {
	if e.NotBinary {
		return fmt.Sprintf("element %q holds field %q as a value that is not a BSON binary of subtype 0x00. Nothing was returned. Store the vector as BSON binary subtype 0x00 holding little-endian float32 values", e.Key, e.Field)
	}
	return fmt.Sprintf("element %q holds a vector of %d bytes in field %q and the query vector is %d bytes. Nothing was returned. Every element of a field is embedded by one model; re-embed the field or query with the model that wrote it", e.Key, e.Length, e.Field, e.Want)
}

// NearestVectorError says the query vector holds a NaN or infinite value at Index (an element
// of the float32 vector, not a byte offset). It is the caller's input, never a retryable fault.
type NearestVectorError struct {
	Index int
}

// Error names the position and the fix.
func (e *NearestVectorError) Error() string {
	return fmt.Sprintf("a nearest vector holds a NaN or infinite value at element %d; send finite float32 values", e.Index)
}

// NearestRequest asks FieldNearest for the K elements of one store closest to Vector.
type NearestRequest struct {
	Metric NearestMetric
	// K is how many to return, at least 1 (the wire bound of 100 is the wire layer's).
	K int
	// Field is the element field that holds the vector, one top-level key, not empty (the wire bounds it at 255 bytes).
	Field string
	// Vector is the query: 4*dim bytes of little-endian float32, 4 to 16384 bytes, finite.
	Vector []byte
	// MaxElements refuses a field holding more elements before any is read; it must be at least 1.
	MaxElements uint64
	// MaxReplyBytes, when above 0, refuses an answer whose kept elements would hold more than that many
	// bytes (each element's remainder, value and ReplyOverhead) with *NearestReplyTooLargeError, before
	// any kept value is copied out. 0 does not bound the answer.
	MaxReplyBytes int
	// ReplyOverhead is the framing bytes the caller's reply adds to each element, counted in MaxReplyBytes.
	ReplyOverhead int
}

// NearestReplyTooLargeError says the K closest elements hold more bytes than the caller's reply may carry.
// It is returned before any kept value is copied.
type NearestReplyTooLargeError struct {
	Hits  int
	Bytes int
	Max   int
}

// Error names the sizes and the fix.
func (e *NearestReplyTooLargeError) Error() string {
	return fmt.Sprintf("the nearest %d elements hold %d bytes and one reply carries at most %d. Nothing was returned. Ask for a smaller k", e.Hits, e.Bytes, e.Max)
}

// NearestHit is one kept element and its score. Entry.Value is a copy owned by the caller, read from
// the same snapshot as the scores.
type NearestHit struct {
	Entry FieldEntry
	Score float64
}

// NearestResult is the answer: Hits best first; Scanned counts elements scored, Skipped
// elements without the field.
type NearestResult struct {
	Hits    []NearestHit
	Scanned int
	Skipped int
}

// nearestPageEntries and nearestPageBytes bound one FieldList page of the walk; variables so a
// test can lower them.
var (
	nearestPageEntries = 256
	nearestPageBytes   = 4 << 20
)

// nearestMaxVectorBytes is the longest query vector: 4096 dimensions of float32.
const nearestMaxVectorBytes = 16384

// nearestCandidate is a kept element and its place in store order, which breaks score ties. Its
// hit holds the decoded key and remainder but no value: the heap keeps only what ranks and
// finds an element, so memory does not grow with the element size. valueLen is the value's size,
// known from the walk, so the reply can be charged before the value is fetched.
type nearestCandidate struct {
	hit      NearestHit
	order    int
	valueLen int
}

// nearestHeap keeps the K best candidates with the worst at the root, so a better
// element replaces the root in O(log K).
type nearestHeap struct {
	metric     NearestMetric
	candidates []nearestCandidate
}

func (h *nearestHeap) Len() int { return len(h.candidates) }

func (h *nearestHeap) Less(i, j int) bool {
	return h.worse(h.candidates[i], h.candidates[j])
}

func (h *nearestHeap) Swap(i, j int) {
	h.candidates[i], h.candidates[j] = h.candidates[j], h.candidates[i]
}

func (h *nearestHeap) Push(candidate any) {
	h.candidates = append(h.candidates, candidate.(nearestCandidate))
}

func (h *nearestHeap) Pop() any {
	last := len(h.candidates) - 1
	candidate := h.candidates[last]
	h.candidates = h.candidates[:last]
	return candidate
}

// worse reports whether a ranks below b: a lower score, or an equal score that came later
// in store order.
func (h *nearestHeap) worse(a, b nearestCandidate) bool {
	if nearestBetter(h.metric, b.hit.Score, a.hit.Score) {
		return true
	}
	return a.hit.Score == b.hit.Score && a.order > b.order
}

// validate refuses a request the walk cannot answer, before any read: K and MaxElements below
// 1, an empty Field, and a vector that is empty, over 16384 bytes, not a multiple of 4 or
// non-finite. It does not check the Metric (the wire decoder refuses a metric other than 1 to 3
// before a request reaches this file) or the length of Field (the wire's one-byte name length
// bounds it at 255).
func (req NearestRequest) validate() error {
	switch {
	case req.K < 1:
		return fmt.Errorf("store: nearest k must be at least 1, got %d", req.K)
	case req.MaxElements < 1:
		return errors.New("store: nearest max elements must be at least 1")
	case req.Field == "":
		return errors.New("store: nearest field name is empty")
	case len(req.Vector) == 0 || len(req.Vector) > nearestMaxVectorBytes || len(req.Vector)%4 != 0:
		return fmt.Errorf("store: nearest vector is %d bytes, want a multiple of 4 from 4 to %d", len(req.Vector), nearestMaxVectorBytes)
	}
	for i := 0; i < len(req.Vector); i += 4 {
		value := math.Float32frombits(binary.LittleEndian.Uint32(req.Vector[i:]))
		if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
			return &NearestVectorError{Index: i / 4}
		}
	}
	return nil
}

// nearestElementName is the key an error names for entry: the element key (hash, btree), the
// entry id (heap) or the position (fifo, lifo).
func nearestElementName(engine FieldEngine, entry FieldEntry) string {
	switch engine {
	case FieldFIFO, FieldLIFO:
		return fmt.Sprintf("position %d", entry.Position)
	case FieldHeap:
		return entry.EntryID
	default:
		return entry.ElementKey
	}
}

// FieldNearest scores every element of the store ref names against req.Vector under snap and
// returns the best req.K. It reads the count with FieldLen first and returns *NearestTooLargeError
// before any element is read; it walks with FieldList, carrying After from page to page; an
// element whose Field is absent is counted in Skipped; an element whose Field is not a BSON
// binary of subtype 0x00 or whose length differs from the query's returns *NearestElementError
// and nothing else (a partial top-k is a wrong answer). Ties keep the element that came first in
// store order. It writes nothing.
//
// The walk keeps only each candidate's score, place and key, never its bytes; once it ends, the
// K winners' sizes are charged against req.MaxReplyBytes (*NearestReplyTooLargeError) and only
// then are their values read, by key, from snap. ctx is checked before every page, so a cancelled
// or expired request stops at the next page with the context's error, wrapped.
func FieldNearest(ctx context.Context, snap storage.KVSnapshot, ref FieldRef, req NearestRequest) (NearestResult, error) {
	if err := req.validate(); err != nil {
		return NearestResult{}, err
	}
	count, err := FieldLen(snap, ref)
	if err != nil {
		return NearestResult{}, fmt.Errorf("counting the elements to scan: %w", err)
	}
	if count > req.MaxElements {
		return NearestResult{}, &NearestTooLargeError{Count: count, Max: req.MaxElements}
	}
	query := newNearestQuery(req.Vector)
	kept := &nearestHeap{metric: req.Metric}
	var result NearestResult
	var after []byte
	for {
		if err := ctx.Err(); err != nil {
			return NearestResult{}, fmt.Errorf("nearest stopped after %d elements: %w", result.Scanned+result.Skipped, err)
		}
		page, hasMore, err := FieldList(snap, ref, FieldListRequest{After: after, Limit: nearestPageEntries, MaxBytes: nearestPageBytes})
		if err != nil {
			return NearestResult{}, fmt.Errorf("walking the elements to scan: %w", err)
		}
		for _, entry := range page {
			value := bson.Raw(entry.Value).Lookup(req.Field)
			if value.IsZero() {
				result.Skipped++
				continue
			}
			subtype, data, ok := value.BinaryOK()
			name := nearestElementName(ref.Engine, entry)
			if !ok || subtype != 0x00 {
				return NearestResult{}, &NearestElementError{Key: name, Field: req.Field, NotBinary: true}
			}
			if len(data) != len(req.Vector) {
				return NearestResult{}, &NearestElementError{Key: name, Field: req.Field, Length: len(data), Want: len(req.Vector)}
			}
			candidate := nearestCandidate{hit: NearestHit{Entry: entry, Score: query.score(req.Metric, data)}, order: result.Scanned, valueLen: len(entry.Value)}
			candidate.hit.Entry.Value = nil
			result.Scanned++
			switch {
			case kept.Len() < req.K:
				heap.Push(kept, candidate)
			case kept.worse(kept.candidates[0], candidate):
				kept.candidates[0] = candidate
				heap.Fix(kept, 0)
			}
		}
		if !hasMore || len(page) == 0 {
			break
		}
		after = page[len(page)-1].Remainder
	}
	// The order is total (ties break on store order, which is unique), so an unstable sort is deterministic.
	sort.Slice(kept.candidates, func(i, j int) bool { return kept.worse(kept.candidates[j], kept.candidates[i]) })
	if req.MaxReplyBytes > 0 {
		var held int
		for _, candidate := range kept.candidates {
			held += candidate.valueLen + len(candidate.hit.Entry.Remainder) + req.ReplyOverhead
		}
		if held > req.MaxReplyBytes {
			return NearestResult{}, &NearestReplyTooLargeError{Hits: len(kept.candidates), Bytes: held, Max: req.MaxReplyBytes}
		}
	}
	lower := ref.elementBounds().Lower
	result.Hits = make([]NearestHit, len(kept.candidates))
	for i, candidate := range kept.candidates {
		hit := candidate.hit
		value, err := snap.Get(append(bytes.Clone(lower), hit.Entry.Remainder...))
		if err != nil {
			return NearestResult{}, fmt.Errorf("reading the kept element %q: %w", nearestElementName(ref.Engine, hit.Entry), err)
		}
		hit.Entry.Value = value
		result.Hits[i] = hit
	}
	return result, nil
}
