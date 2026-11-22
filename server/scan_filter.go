package server

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"
)

// substituteNowSentinels replaces every now() marker (nowMarker) in value
// with one reading of the server's clock, and returns value untouched when it
// holds none. It is for a read's filter, which is evaluated where it runs and
// never logged. A write's now() is fixed before it is logged
// (resolveDocumentIDs, resolveNowAtTheDoor) and never here.
func substituteNowSentinels(value interface{}) interface{} {
	substituted, _ := replaceNowSentinels(value, nowInstant())
	return substituted
}

// compiledFilter is a filter whose field keys are parsed into paths once per
// query, so each document examined pays the walk and never the parse (spec
// §4.9.5).
//
// The grammar is the v1 set of the scan-find-all-spec record 4.1: a field
// maps to a value (equality) or to an operator document ($gt, $gte, $lt,
// $lte, $in, $regex), and the reserved top-level keys $and and $or hold lists
// of filters. A field key is a path (typed-fields spec §4.9.6): a missing
// member, a position past the end or a non-document on the way is absent, and
// a name segment never reaches into an array. Comparison is type-aware —
// numbers compare numerically, strings lexicographically, dates compare by
// instant, and a type mismatch matches nothing rather than erroring — and an
// absent field matches nothing, comparisons included. A missing field is not
// zero.
//
// The document is read as decoded BSON or JSON: numbers may be int64, int32
// or float64, and arrays are []interface{} in process but bson.A once decoded
// from the wire, so both are accepted.
type compiledFilter struct {
	clauses []filterClause
	// refuses is whether any clause can return an error, so a document that
	// fails one clause must still be taken through the rest.
	refuses bool
}

// filterClause is one key of a filter: a field path and its condition, or a
// $and/$or with its compiled clauses. A malformed clause keeps its error and
// reports it when a document reaches it, as an uncompiled filter always did:
// a filter over an empty bucket is not refused for a clause it never
// evaluated. A limit refusal is not malformed and does not wait for a
// document: compiledFilter.refusal answers it before the scan (BUG-0690).
type filterClause struct {
	logical   string
	path      fieldPath
	condition interface{}
	nested    []*compiledFilter
	err       error
	// lookup and test are the path and the condition compiled for stored
	// bytes (scan_filter_document.go).
	lookup bson.Path
	test   fieldTest
	// refuses is whether evaluating the clause can return an error.
	refuses bool
}

// compileFilter parses filter once for a query.
// The filter's cost is bounded here, before any document is read (see
// filter_limits.go).
func compileFilter(filter map[string]interface{}) *compiledFilter {
	return compileFilterAt(filter, 1, &filterBudget{})
}

func compileFilterAt(filter map[string]interface{}, depth int, budget *filterBudget) *compiledFilter {
	compiled := &compiledFilter{clauses: make([]filterClause, 0, len(filter))}
	for key, condition := range filter {
		clause := compileClause(key, condition, depth, budget)
		compiled.refuses = compiled.refuses || clause.refuses
		compiled.clauses = append(compiled.clauses, clause)
	}
	return compiled
}

// refusal returns the first clause error that refuses the filter for its cost
// or its form, so a query answers it before reading any document: an empty
// bucket has no document to carry the error (BUG-0690), and the user limits page
// promises every filter-size limit is raised before any read. A malformed
// clause is not a refusal and keeps its error for the first document.
func (f *compiledFilter) refusal() error {
	for _, clause := range f.clauses {
		if clause.err != nil && errors.Is(clause.err, ErrFilterRefused) {
			return clause.err
		}
		for _, nested := range clause.nested {
			if err := nested.refusal(); err != nil {
				return err
			}
		}
	}
	return nil
}

func compileClause(key string, condition interface{}, depth int, budget *filterBudget) filterClause {
	clause := compileClauseParts(key, condition, depth, budget)
	switch {
	case clause.err != nil:
		clause.refuses = true
	case clause.logical != "":
		for _, nested := range clause.nested {
			clause.refuses = clause.refuses || nested.refuses
		}
	default:
		clause.refuses = clause.test.refuses
	}
	return clause
}

func compileClauseParts(key string, condition interface{}, depth int, budget *filterBudget) filterClause {
	if err := budget.chargeClause(); err != nil {
		return filterClause{err: err}
	}
	if strings.HasPrefix(key, "$") {
		if key != "$and" && key != "$or" {
			return filterClause{err: fmt.Errorf("unsupported filter operator %q: this version supports $and, $or, $ne, $exists, $nin at the top level", key)}
		}
		clauses, ok := asList(condition)
		if !ok {
			return filterClause{err: fmt.Errorf("%s expects an array of filter documents, got %T", key, condition)}
		}
		if depth >= maxFilterDepth {
			return filterClause{err: refuseFilter("%s is nested %d levels deep; this server's limit is %d levels of $and/$or. Flatten the filter, or run two queries", key, depth+1, maxFilterDepth)}
		}
		if len(clauses) > maxFilterLogicalClauses {
			return filterClause{err: refuseFilter("%s holds %d filters; this server's limit is %d per %s. Split the query, or use $in for several values of one field", key, len(clauses), maxFilterLogicalClauses, key)}
		}
		nested := make([]*compiledFilter, 0, len(clauses))
		for _, clause := range clauses {
			clauseFilter, ok := clause.(map[string]interface{})
			if !ok {
				return filterClause{err: fmt.Errorf("%s clauses must be filter documents, got %T", key, clause)}
			}
			nested = append(nested, compileFilterAt(clauseFilter, depth+1, budget))
		}
		return filterClause{logical: key, nested: nested}
	}
	path, err := parseFieldPath(key)
	if err != nil {
		return filterClause{err: fmt.Errorf("the filter's key %w. Write a path as names joined by \".\", with 0-based positions in brackets, for example Address.City or Lines[3].Qty", err)}
	}
	prepared, err := prepareCondition(key, condition, budget)
	if err != nil {
		return filterClause{err: err}
	}
	return filterClause{path: path, condition: prepared, lookup: libraryPath(path), test: newFieldTest(prepared)}
}

// operatorDocument reports whether a field's condition is an operator
// document, which it is when it is a document carrying at least one
// $-prefixed key.
func operatorDocument(condition interface{}) (map[string]interface{}, bool) {
	document, ok := condition.(map[string]interface{})
	if !ok || len(document) == 0 {
		return nil, false
	}
	for key := range document {
		if strings.HasPrefix(key, "$") {
			return document, true
		}
	}
	return nil, false
}

// asList returns a filter array's elements. In-process filters hold
// []interface{}; the same filter decoded from BSON holds bson.A, which is a
// distinct named type and not caught by the first case.
func asList(value interface{}) ([]interface{}, bool) {
	switch list := value.(type) {
	case []interface{}:
		return list, true
	case bson.A:
		return []interface{}(list), true
	default:
		return nil, false
	}
}

// dateMillis extracts the milliseconds since epoch from a date value.
// It accepts primitive.DateTime and time.Time, and returns the millisecond
// count and true, or 0 and false for non-date types.
func dateMillis(value interface{}) (int64, bool) {
	switch v := value.(type) {
	case primitive.DateTime:
		return int64(v), true
	case time.Time:
		return v.UnixMilli(), true
	default:
		return 0, false
	}
}

// valuesEqual reports whether two scalars are equal under the type-aware
// rule: numbers compare numerically across numeric kinds, strings compare as
// strings, dates compare by instant, and a type mismatch is not equal.
func valuesEqual(left, right interface{}) bool {
	// Date branch: both must be dates (or convertible to dates)
	if leftMs, ok := dateMillis(left); ok {
		rightMs, ok := dateMillis(right)
		return ok && leftMs == rightMs
	}
	if leftNumber, ok := numberOf(left); ok {
		rightNumber, ok := numberOf(right)
		if leftNumber.isNaN() || rightNumber.isNaN() {
			return ok && leftNumber.isNaN() && rightNumber.isNaN()
		}
		return ok && compareNumbers(leftNumber, rightNumber) == 0
	}
	if leftString, ok := left.(string); ok {
		rightString, ok := right.(string)
		return ok && leftString == rightString
	}
	return reflect.DeepEqual(left, right)
}

// compareValues orders two scalars like valuesEqual orders equality: numbers
// numerically, strings lexicographically, and dates by instant. The second
// result is false when the two are not comparable — a number against a string,
// a date against a non-date, or either against a non-scalar — which the caller
// treats as "matches nothing".
func compareValues(left, right interface{}) (int, bool) {
	// Date branch: both must be dates (or convertible to dates), compared first
	if leftMs, ok := dateMillis(left); ok {
		rightMs, ok := dateMillis(right)
		if !ok {
			return 0, false
		}
		switch {
		case leftMs < rightMs:
			return -1, true
		case leftMs > rightMs:
			return 1, true
		default:
			return 0, true
		}
	}

	if leftNumber, ok := numberOf(left); ok {
		rightNumber, ok := numberOf(right)
		// NaN is neither above, below nor at any number, so it is not
		// comparable and no ordered operator matches it. Sorting orders NaN
		// itself, below every number (server/sort_order.go).
		if !ok || leftNumber.isNaN() || rightNumber.isNaN() {
			return 0, false
		}
		return compareNumbers(leftNumber, rightNumber), true
	}

	leftString, ok := left.(string)
	if !ok {
		return 0, false
	}
	rightString, ok := right.(string)
	if !ok {
		return 0, false
	}
	return strings.Compare(leftString, rightString), true
}

// toFloat64 converts any numeric kind to float64. A decoded document's
// numbers may be int32, int64 or float64 depending on how the value was
// stored, and a filter parsed from JSON is always float64. It explicitly
// rejects primitive.DateTime to enforce D-3's type bracketing (no coercion
// between dates and numbers).
func toFloat64(value interface{}) (float64, bool) {
	switch number := value.(type) {
	case float64:
		return number, true
	case float32:
		return float64(number), true
	case primitive.DateTime:
		// Reject dates explicitly: D-3 forbids date-to-number coercion
		return 0, false
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(reflected.Int()), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return float64(reflected.Uint()), true
	default:
		return 0, false
	}
}

// number is a numeric value with its kind kept: a signed integer, an unsigned
// integer above int64's range, or a float. Converting every kind to float64
// loses the low bits of an integer above 2^53, so two different integers
// compared equal.
type number struct {
	kind     numberKind
	signed   int64
	unsigned uint64
	float    float64
}

type numberKind uint8

const (
	numberSigned numberKind = iota
	numberUnsigned
	numberFloat
)

// numberOf classifies any numeric kind toFloat64 accepts, rejecting the
// same values (dates first: primitive.DateTime is an int64 underneath, and D-3
// forbids date-to-number coercion). An unsigned value that fits int64 is
// held as signed, so each integer has one representation.
func numberOf(value interface{}) (number, bool) {
	switch typed := value.(type) {
	case float64:
		return number{kind: numberFloat, float: typed}, true
	case float32:
		return number{kind: numberFloat, float: float64(typed)}, true
	case primitive.DateTime:
		return number{}, false
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return number{kind: numberSigned, signed: reflected.Int()}, true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		unsigned := reflected.Uint()
		if unsigned <= math.MaxInt64 {
			return number{kind: numberSigned, signed: int64(unsigned)}, true
		}
		return number{kind: numberUnsigned, unsigned: unsigned}, true
	default:
		return number{}, false
	}
}

// compareNumbers orders two numbers by their mathematical value, the order
// the secondary index's key encoding gives them (store/indexkey.go): integers
// against integers as integers, and an integer against a float by exact
// value, so 2^53+1 is above the float 2^53. Neither number is NaN, which has
// no place in the order; the callers handle it first.
func compareNumbers(left, right number) int {
	switch {
	case left.kind == numberFloat && right.kind == numberFloat:
		return compareFloats(left.float, right.float)
	case left.kind == numberFloat:
		return -compareIntegerToFloat(right, left.float)
	case right.kind == numberFloat:
		return compareIntegerToFloat(left, right.float)
	case left.kind == numberUnsigned && right.kind == numberUnsigned:
		return compareUnsigned(left.unsigned, right.unsigned)
	case left.kind == numberUnsigned:
		return 1 // above int64's range, so above every signed integer
	case right.kind == numberUnsigned:
		return -1
	case left.signed < right.signed:
		return -1
	case left.signed > right.signed:
		return 1
	}
	return 0
}

func (n number) isNaN() bool {
	return n.kind == numberFloat && math.IsNaN(n.float)
}

func compareFloats(left, right float64) int {
	switch {
	case left < right:
		return -1
	case left > right:
		return 1
	}
	return 0
}

func compareUnsigned(left, right uint64) int {
	switch {
	case left < right:
		return -1
	case left > right:
		return 1
	}
	return 0
}

// compareIntegerToFloat orders an integer against a float by exact value: the
// float is cut into its whole part, which converts to an integer without
// loss once it is inside the integer's range, and its fraction, which breaks
// a tie on the whole part.
func compareIntegerToFloat(integer number, float float64) int {
	const (
		twoTo63 = float64(1 << 63)
		twoTo64 = twoTo63 * 2
	)
	whole := math.Trunc(float)
	if integer.kind == numberUnsigned {
		switch {
		case float < twoTo63: // below every unsigned-kind integer, NaN aside
			return 1
		case float >= twoTo64:
			return -1
		}
		return compareUnsignedFraction(integer.unsigned, uint64(whole), float-whole)
	}
	switch {
	case float >= twoTo63:
		return -1
	case float < -twoTo63:
		return 1
	}
	wholeInteger := int64(whole)
	switch {
	case integer.signed < wholeInteger:
		return -1
	case integer.signed > wholeInteger:
		return 1
	}
	return -compareFractionToZero(float - whole)
}

func compareUnsignedFraction(integer, whole uint64, fraction float64) int {
	if order := compareUnsigned(integer, whole); order != 0 {
		return order
	}
	return -compareFractionToZero(fraction)
}

// compareFractionToZero is the sign of a float's fractional part.
func compareFractionToZero(fraction float64) int {
	switch {
	case fraction > 0:
		return 1
	case fraction < 0:
		return -1
	}
	return 0
}

// distinctKey spells a number so that equal values spell alike: a float that
// holds a whole number in integer range is spelled as that integer.
func (n number) distinctKey() string {
	switch n.kind {
	case numberSigned:
		return strconv.FormatInt(n.signed, 10)
	case numberUnsigned:
		return strconv.FormatUint(n.unsigned, 10)
	}
	const (
		twoTo63 = float64(1 << 63)
		twoTo64 = twoTo63 * 2
	)
	switch {
	case n.float != math.Trunc(n.float):
		return strconv.FormatFloat(n.float, 'g', -1, 64)
	case n.float >= -twoTo63 && n.float < twoTo63:
		return strconv.FormatInt(int64(n.float), 10)
	case n.float >= twoTo63 && n.float < twoTo64:
		return strconv.FormatUint(uint64(n.float), 10)
	}
	// Beyond uint64, or NaN or infinite: the float is the only spelling.
	return strconv.FormatFloat(n.float, 'g', -1, 64)
}
