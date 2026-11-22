package server

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"reflect"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/bsontype"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"

	"github.com/hoarddb/hoarddb-server/store"
)

// A compiled filter matches a stored document on its bytes (bson-codec-spec
// 7.2). Each field clause resolves its path with the library's resolver and
// compares the value it finds, in place, against operands read once per
// query. The result is the one the map path gave, a decode of the whole
// document into map[string]interface{} followed by the map matcher, for
// every document the server stores: the comparison follows valuesEqual's and
// compareValues' branches on the value's type, and where those fall back to
// reflect.DeepEqual (a document, an array, or a type with no order) the one
// value is decoded as the whole-document decode would have decoded it.
//
// A filter stops at the first clause that fails, unless a later clause can
// refuse: the map path evaluated every clause, so a malformed clause, an
// operator the server does not have, or a $regex past its work budget refused
// the document even beside a clause that failed, and the same document is
// refused here.

// literalClass is the branch valuesEqual and compareValues take for an
// operand, read once when the query compiles.
type literalClass uint8

const (
	// literalOpaque is an operand no stored value equals or orders against:
	// a Go type the decode never produces, such as []interface{} or bson.M.
	literalOpaque literalClass = iota
	literalDate
	literalNumber
	literalString
	literalBoolean
	literalNull
	// literalDecoded is an operand of a type the decode produces for a
	// document, an array or a type with no order; equality is DeepEqual.
	literalDecoded
)

// filterLiteral is an operand classified once per query.
type filterLiteral struct {
	value  interface{}
	class  literalClass
	millis int64
	number number
	text   []byte
	flag   bool
}

func newFilterLiteral(value interface{}) filterLiteral {
	literal := filterLiteral{value: value}
	if millis, isDate := dateMillis(value); isDate {
		literal.class, literal.millis = literalDate, millis
		return literal
	}
	if parsed, isNumber := numberOf(value); isNumber {
		literal.class, literal.number = literalNumber, parsed
		return literal
	}
	switch typed := value.(type) {
	case nil:
		literal.class = literalNull
	case string:
		literal.class, literal.text = literalString, []byte(typed)
	case bool:
		literal.class, literal.flag = literalBoolean, typed
	case map[string]interface{}, primitive.A, primitive.Binary, primitive.Undefined, primitive.ObjectID,
		primitive.Regex, primitive.DBPointer, primitive.JavaScript, primitive.Symbol, primitive.CodeWithScope,
		primitive.Timestamp, primitive.Decimal128, primitive.MinKey, primitive.MaxKey:
		literal.class = literalDecoded
	}
	return literal
}

// operatorTest is one operator of a field's operator document.
type operatorTest struct {
	operator   string
	operand    filterLiteral
	candidates []filterLiteral
	exists     bool
	regex      *boundedRegex
	// err is the operator's refusal, returned whenever a document reaches it.
	err error
}

// mayRefuse reports whether evaluating the operator can return an error.
func (test *operatorTest) mayRefuse() bool { return test.err != nil || test.regex != nil }

// fieldTest is a field clause's condition, compiled for stored bytes: an
// equality, or the operators of an operator document.
type fieldTest struct {
	isOperators bool
	equals      filterLiteral
	operators   []operatorTest
	refuses     bool
}

func newFieldTest(prepared interface{}) fieldTest {
	operators, isOperators := operatorDocument(prepared)
	if !isOperators {
		return fieldTest{equals: newFilterLiteral(prepared)}
	}
	test := fieldTest{isOperators: true, operators: make([]operatorTest, 0, len(operators))}
	for operator, operand := range operators {
		compiled := newOperatorTest(operator, operand)
		test.refuses = test.refuses || compiled.mayRefuse()
		test.operators = append(test.operators, compiled)
	}
	return test
}

// newOperatorTest reads one operator's operand, with evalOperator's
// refusals of a malformed one.
func newOperatorTest(operator string, operand interface{}) operatorTest {
	test := operatorTest{operator: operator}
	switch operator {
	case "$gt", "$gte", "$lt", "$lte", "$ne":
		test.operand = newFilterLiteral(operand)
	case "$in", "$nin":
		candidates, ok := asList(operand)
		if !ok {
			test.err = fmt.Errorf("%s expects an array of values, got %T", operator, operand)
			return test
		}
		test.candidates = make([]filterLiteral, len(candidates))
		for position, candidate := range candidates {
			test.candidates[position] = newFilterLiteral(candidate)
		}
	case "$exists":
		wanted, ok := operand.(bool)
		if !ok {
			test.err = fmt.Errorf("$exists expects a boolean operand, got %T", operand)
		}
		test.exists = wanted
	case "$regex":
		pattern, ok := operand.(*boundedRegex)
		if !ok {
			test.err = fmt.Errorf("$regex operand is %T; the filter was not prepared for matching", operand)
		}
		test.regex = pattern
	default:
		test.err = fmt.Errorf("unsupported filter operator %q: this version supports $gt, $gte, $lt, $lte, $in, $nin, $ne, $exists and $regex on a field", operator)
	}
	return test
}

// libraryPath is a parsed path as the library's resolver reads it.
func libraryPath(path fieldPath) bson.Path {
	if path.simple() {
		return bson.Path{bson.Name(path.text)}
	}
	segments := make(bson.Path, len(path.segs))
	for position, seg := range path.segs {
		if seg.isIndex {
			segments[position] = bson.Position(seg.index)
		} else {
			segments[position] = bson.Name(seg.name)
		}
	}
	return segments
}

// matchesDocument reports whether a stored document satisfies the filter.
func (f *compiledFilter) matchesDocument(document bson.Document) (bool, error) {
	budget := matchBudget{remaining: maxMatchWork}
	return f.documentMatchesWithin(document, &budget)
}

// documentMatchesWithin is matchesDocument charging its $regex work to
// budget, which every nested $and/$or of one document shares.
func (f *compiledFilter) documentMatchesWithin(document bson.Document, budget *matchBudget) (bool, error) {
	allMatch := true
	for position := range f.clauses {
		clause := &f.clauses[position]
		if !allMatch && !clause.refuses {
			continue
		}
		matched, err := clause.documentMatches(document, budget)
		if err != nil {
			return false, err
		}
		if !matched {
			if !f.refuses {
				return false, nil
			}
			allMatch = false
		}
	}
	return allMatch, nil
}

func (clause *filterClause) documentMatches(document bson.Document, budget *matchBudget) (bool, error) {
	switch {
	case clause.err != nil:
		return false, clause.err
	case clause.logical == "$and":
		allMatch := true
		for _, nested := range clause.nested {
			if !allMatch && !nested.refuses {
				continue
			}
			matched, err := nested.documentMatchesWithin(document, budget)
			if err != nil {
				return false, err
			}
			if !matched {
				allMatch = false
			}
		}
		return allMatch, nil
	case clause.logical == "$or":
		for _, nested := range clause.nested {
			matched, err := nested.documentMatchesWithin(document, budget)
			if err != nil || matched {
				return matched, err
			}
		}
		return false, nil
	}
	view, present, err := document.Lookup(clause.lookup)
	if err != nil {
		return false, fmt.Errorf("resolving the filter's key %q: %w", clause.path.text, err)
	}
	return clause.test.matches(view, present, budget)
}

func (test *fieldTest) matches(view bson.View, present bool, budget *matchBudget) (bool, error) {
	if !test.isOperators {
		if !present {
			return false, nil
		}
		return viewEquals(view, test.equals)
	}
	allMatch := true
	for position := range test.operators {
		operator := &test.operators[position]
		if !allMatch && !operator.mayRefuse() {
			continue
		}
		matched, err := operator.matches(view, present, budget)
		if err != nil {
			return false, err
		}
		if !matched {
			if !test.refuses {
				return false, nil
			}
			allMatch = false
		}
	}
	return allMatch, nil
}

// matches is evalOperator on a stored value.
func (test *operatorTest) matches(view bson.View, present bool, budget *matchBudget) (bool, error) {
	if test.err != nil {
		return false, test.err
	}
	switch test.operator {
	case "$exists":
		return present == test.exists, nil
	case "$gt", "$gte", "$lt", "$lte":
		if !present {
			return false, nil
		}
		ordering, comparable := viewCompare(view, test.operand)
		if !comparable {
			return false, nil
		}
		switch test.operator {
		case "$gt":
			return ordering > 0, nil
		case "$gte":
			return ordering >= 0, nil
		case "$lt":
			return ordering < 0, nil
		}
		return ordering <= 0, nil
	case "$ne":
		if !present {
			return false, nil
		}
		equal, err := viewEquals(view, test.operand)
		return !equal, err
	case "$in", "$nin":
		if !present {
			return false, nil
		}
		for position := range test.candidates {
			equal, err := viewEquals(view, test.candidates[position])
			if err != nil {
				return false, err
			}
			if equal {
				return test.operator == "$in", nil
			}
		}
		return test.operator == "$nin", nil
	}
	// $regex: newOperatorTest refused every other operator.
	if !present || view.Type() != bsontype.String {
		return false, nil
	}
	return test.regex.matchBytes(stringBytes(view), budget)
}

// viewEquals is valuesEqual(decoded value, literal), on the value's bytes.
func viewEquals(view bson.View, literal filterLiteral) (bool, error) {
	switch view.Type() {
	case bsontype.DateTime:
		return literal.class == literalDate && dateTimeMillis(view) == literal.millis, nil
	case bsontype.Double, bsontype.Int32, bsontype.Int64:
		if literal.class != literalNumber {
			return false, nil
		}
		stored := viewNumber(view)
		if stored.isNaN() || literal.number.isNaN() {
			return stored.isNaN() && literal.number.isNaN(), nil
		}
		return compareNumbers(stored, literal.number) == 0, nil
	case bsontype.String:
		return literal.class == literalString && bytes.Equal(stringBytes(view), literal.text), nil
	case bsontype.Boolean:
		return literal.class == literalBoolean && booleanOf(view) == literal.flag, nil
	case bsontype.Null:
		return literal.class == literalNull, nil
	}
	if literal.class != literalDecoded {
		return false, nil
	}
	decoded, err := store.DecodeStoredValue(view.Type(), view.Bytes())
	if err != nil {
		return false, err
	}
	return reflect.DeepEqual(decoded, literal.value), nil
}

// viewCompare is compareValues(decoded value, literal), on the value's bytes.
func viewCompare(view bson.View, literal filterLiteral) (int, bool) {
	switch view.Type() {
	case bsontype.DateTime:
		if literal.class != literalDate {
			return 0, false
		}
		stored := dateTimeMillis(view)
		switch {
		case stored < literal.millis:
			return -1, true
		case stored > literal.millis:
			return 1, true
		}
		return 0, true
	case bsontype.Double, bsontype.Int32, bsontype.Int64:
		stored := viewNumber(view)
		if literal.class != literalNumber || stored.isNaN() || literal.number.isNaN() {
			return 0, false
		}
		return compareNumbers(stored, literal.number), true
	case bsontype.String:
		if literal.class != literalString {
			return 0, false
		}
		return bytes.Compare(stringBytes(view), literal.text), true
	}
	return 0, false
}

// viewNumber is numberOf of a stored int32, int64 or double.
func viewNumber(view bson.View) number {
	valueBytes := view.Bytes()
	switch view.Type() {
	case bsontype.Int32:
		return number{kind: numberSigned, signed: int64(int32(binary.LittleEndian.Uint32(valueBytes)))}
	case bsontype.Int64:
		return number{kind: numberSigned, signed: int64(binary.LittleEndian.Uint64(valueBytes))}
	}
	return number{kind: numberFloat, float: math.Float64frombits(binary.LittleEndian.Uint64(valueBytes))}
}

func dateTimeMillis(view bson.View) int64 {
	return int64(binary.LittleEndian.Uint64(view.Bytes()))
}

// booleanOf reads a stored boolean as the decoder does: 0x01 is true.
func booleanOf(view bson.View) bool { return view.Bytes()[0] == 1 }

// stringBytes is a stored string's text, without its length prefix and
// terminator.
func stringBytes(view bson.View) []byte {
	valueBytes := view.Bytes()
	return valueBytes[4 : len(valueBytes)-1]
}
