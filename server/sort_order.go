package server

import (
	"container/heap"
	"strings"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/bsontype"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"
)

// The sort order of a sorted find(), query-pipelining spec §3.2 "The sort
// order" (OC-5, decided 2026-09-24; declared as a result change under X-1).
//
// A bounded sort keeps only skip+limit candidates, so it needs a strict total
// order: with anything weaker, a candidate evicted on one page is kept on the
// next, and pages repeat and drop documents at their boundaries (T-5). The
// order is: a fixed rank per type class, then a comparison within the class,
// then position — the entry's index in the store's walk — always ascending.
// compareValues is not that order and is left alone: filters keep its
// "different types never match a range" semantics.

// The type ranks of §3.2's table. A key's class is its rank shifted left by
// eight bits, with rank 7's BSON type byte in the low bits, so one integer
// comparison orders both the rank and rank 7's sub-rank.
const (
	sortRankNumber   = 1
	sortRankString   = 2
	sortRankDate     = 3
	sortRankBoolean  = 4
	sortRankDocument = 5
	sortRankArray    = 6
	sortRankOther    = 7
	sortRankMissing  = 8
)

// sortKey is a sort field's value reduced to what the order compares. Only the
// field of its class is set.
type sortKey struct {
	class uint16
	// number keeps its kind so integers above 2^53 compare exactly, as the
	// filters do (BUG-0133); a float64 image tied distinct integers.
	number number
	text   string
	millis int64
	flag   bool
}

// sortKeyOf classifies a value as bson.Unmarshal decodes it into
// map[string]interface{}. Numbers and dates are classified by exactly
// toFloat64's and dateMillis' type sets, so a key compares within its class
// the way a filter compares it. A missing field and an explicit null both
// arrive as nil.
func sortKeyOf(value interface{}) sortKey {
	if value == nil {
		return sortKey{class: sortRankMissing << 8}
	}
	if millis, ok := dateMillis(value); ok {
		return sortKey{class: sortRankDate << 8, millis: millis}
	}
	if sortNumber, ok := numberOf(value); ok {
		return sortKey{class: sortRankNumber << 8, number: sortNumber}
	}
	switch typed := value.(type) {
	case string:
		return sortKey{class: sortRankString << 8, text: typed}
	case bool:
		return sortKey{class: sortRankBoolean << 8, flag: typed}
	case primitive.D, primitive.M, map[string]interface{}:
		return sortKey{class: sortRankDocument << 8}
	case primitive.A, []interface{}:
		return sortKey{class: sortRankArray << 8}
	}
	return sortKey{class: sortRankOther<<8 | uint16(otherBSONType(value))}
}

// otherBSONType is the BSON type byte of a rank-7 value: the Go types the
// driver's default registry decodes those BSON types into
// (bsoncodec.DefaultValueDecoders' type map). Anything else cannot come out of
// bson.Unmarshal; it gets type byte 0, below every real one, which keeps the
// order total over every Go value rather than only over decoded ones.
func otherBSONType(value interface{}) bsontype.Type {
	switch value.(type) {
	case primitive.Binary:
		return bson.TypeBinary
	case primitive.Undefined:
		return bson.TypeUndefined
	case primitive.ObjectID:
		return bson.TypeObjectID
	case primitive.Regex:
		return bson.TypeRegex
	case primitive.DBPointer:
		return bson.TypeDBPointer
	case primitive.JavaScript:
		return bson.TypeJavaScript
	case primitive.Symbol:
		return bson.TypeSymbol
	case primitive.CodeWithScope:
		return bson.TypeCodeWithScope
	case primitive.Timestamp:
		return bson.TypeTimestamp
	case primitive.Decimal128:
		return bson.TypeDecimal128
	case primitive.MinKey:
		return bson.TypeMinKey
	case primitive.MaxKey:
		return bson.TypeMaxKey
	}
	return 0
}

// compareSortKeys is steps 1 and 2 of the order, ascending: negative, zero or
// positive. Zero is a tie, which position breaks (step 3).
func compareSortKeys(left, right sortKey) int {
	if left.class != right.class {
		if left.class < right.class {
			return -1
		}
		return 1
	}
	switch left.class >> 8 {
	case sortRankNumber:
		// NaN sits below every other number and equals every other NaN, so
		// the comparison is total; float64's own < and > are both false for
		// NaN, which made it "equal" to 1 and 2 while 1 < 2 (RC-1).
		switch {
		case left.number.isNaN() && right.number.isNaN():
			return 0
		case left.number.isNaN():
			return -1
		case right.number.isNaN():
			return 1
		}
		return compareNumbers(left.number, right.number)
	case sortRankString:
		return strings.Compare(left.text, right.text)
	case sortRankDate:
		switch {
		case left.millis < right.millis:
			return -1
		case left.millis > right.millis:
			return 1
		}
		return 0
	case sortRankBoolean:
		switch {
		case left.flag == right.flag:
			return 0
		case !left.flag:
			return -1
		}
		return 1
	}
	// Documents, arrays, each rank-7 type, and missing or null are equal
	// within their class.
	return 0
}

// sortCandidate is one matched document as the bounded sort holds it: its key
// and its position in the walk.
type sortCandidate struct {
	key      sortKey
	position int64
	// keyBytes is what the result budget charged for holding this
	// candidate, given back if it is evicted.
	keyBytes int64
}

// sortsBefore reports whether a comes before b. Descending reverses the class
// and value comparison and never position, so ties come back in natural order
// in both directions and consecutive pages concatenate.
func sortsBefore(a, b sortCandidate, ascending bool) bool {
	comparison := compareSortKeys(a.key, b.key)
	if !ascending {
		comparison = -comparison
	}
	if comparison != 0 {
		return comparison < 0
	}
	return a.position < b.position
}

// sortCandidateHeap is a max-heap under sortsBefore: its root is the candidate
// that sorts last, the one a better candidate evicts.
type sortCandidateHeap struct {
	candidates []sortCandidate
	ascending  bool
}

func (h *sortCandidateHeap) Len() int { return len(h.candidates) }
func (h *sortCandidateHeap) Less(i, j int) bool {
	return sortsBefore(h.candidates[j], h.candidates[i], h.ascending)
}
func (h *sortCandidateHeap) Swap(i, j int) {
	h.candidates[i], h.candidates[j] = h.candidates[j], h.candidates[i]
}
func (h *sortCandidateHeap) Push(x any) { h.candidates = append(h.candidates, x.(sortCandidate)) }
func (h *sortCandidateHeap) Pop() any {
	last := h.candidates[len(h.candidates)-1]
	h.candidates = h.candidates[:len(h.candidates)-1]
	return last
}

// offer keeps candidate if it is among the capacity first seen so far under
// the order, charging budget for its key and releasing the key it evicts.
func (h *sortCandidateHeap) offer(candidate sortCandidate, capacity int64, budget *resultBudget) error {
	if int64(len(h.candidates)) < capacity {
		if err := budget.charge(candidate.keyBytes); err != nil {
			return err
		}
		heap.Push(h, candidate)
		return nil
	}
	if !sortsBefore(candidate, h.candidates[0], h.ascending) {
		return nil
	}
	budget.release(h.candidates[0].keyBytes)
	if err := budget.charge(candidate.keyBytes); err != nil {
		return err
	}
	h.candidates[0] = candidate
	heap.Fix(h, 0)
	return nil
}
