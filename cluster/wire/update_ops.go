package wire

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strconv"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/bsontype"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// The operators of conventions section 9.1 "UPDATE operators" beyond set, unset and inc.
// Each takes the stored typed value of one scalar field (nil when the field is absent) and
// the operand, and returns the typed value to write, or nil when the stored value stays.
// The owner's check and the apply both call updateResult, so they cannot disagree.

// mulNumbers is MongoDB's $mul arithmetic, typed as inc: int * int stays an int unless the
// product leaves its range, when it becomes a long; a long and an int are a long; a double
// makes a double. A long overflow and a double past the finite numbers are refused.
func mulNumbers(have, factor scalarNumber) (scalarNumber, error) {
	if have.kind == bsontype.Double || factor.kind == bsontype.Double {
		product := have.float() * factor.float()
		if math.IsInf(product, 0) {
			return scalarNumber{}, errors.New("the product is past the largest double, so the multiplication would overflow")
		}
		return scalarNumber{kind: bsontype.Double, real: product}, nil
	}
	product := have.whole * factor.whole
	if have.whole != 0 && (product/have.whole != factor.whole || (have.whole == -1 && factor.whole == math.MinInt64)) {
		return scalarNumber{}, errors.New("the product is outside a long, so the multiplication would overflow")
	}
	if have.kind == bsontype.Int32 && factor.kind == bsontype.Int32 && product >= math.MinInt32 && product <= math.MaxInt32 {
		return scalarNumber{kind: bsontype.Int32, whole: product}, nil
	}
	return scalarNumber{kind: bsontype.Int64, whole: product}, nil
}

// mulResult is the value a mul leaves: zero of the factor's type when the field holds none
// (MongoDB), otherwise the product.
func mulResult(stored, factor []byte) ([]byte, error) {
	multiplier, err := parseNumber(factor, "mul")
	if err != nil {
		return nil, fmt.Errorf("the factor is not a number: %w", err)
	}
	if stored == nil {
		return scalarNumber{kind: multiplier.kind}.typed(), nil
	}
	have, err := parseNumber(stored, "mul")
	if err != nil {
		return nil, fmt.Errorf("the stored value is not a number: %w", err)
	}
	product, err := mulNumbers(have, multiplier)
	if err != nil {
		return nil, err
	}
	return product.typed(), nil
}

// MongoDB's comparison order puts values in brackets, lowest first. The brackets here are
// the ones whose order is built exactly.
const (
	bracketNull = iota + 1
	bracketNumber
	bracketString
	bracketBinary
	bracketObjectID
	bracketBool
	bracketDate
	bracketTimestamp
)

// orderKey is a value read for comparison.
type orderKey struct {
	bracket  int
	number   scalarNumber
	text     []byte
	subtype  byte
	signed   int64
	unsigned uint64
}

// orderOf reads a typed value for min and max. It refuses a value whose order is not built.
func orderOf(typed []byte, operator string) (orderKey, error) {
	if len(typed) == 0 {
		return orderKey{}, errors.New("it has no type byte")
	}
	if err := ringCheckTypedValue(typed); err != nil {
		return orderKey{}, fmt.Errorf("it is not a BSON value: %w", err)
	}
	value := bson.RawValue{Type: bsontype.Type(typed[0]), Value: typed[1:]}
	switch value.Type {
	case bsontype.Null:
		return orderKey{bracket: bracketNull}, nil
	case bsontype.Int32, bsontype.Int64, bsontype.Double:
		number, err := parseNumber(typed, operator)
		if err != nil {
			return orderKey{}, err
		}
		return orderKey{bracket: bracketNumber, number: number}, nil
	case bsontype.String:
		return orderKey{bracket: bracketString, text: []byte(value.StringValue())}, nil
	case bsontype.Binary:
		subtype, payload := value.Binary()
		return orderKey{bracket: bracketBinary, subtype: subtype, text: payload}, nil
	case bsontype.ObjectID:
		id := value.ObjectID()
		return orderKey{bracket: bracketObjectID, text: id[:]}, nil
	case bsontype.Boolean:
		flag := int64(0)
		if value.Boolean() {
			flag = 1
		}
		return orderKey{bracket: bracketBool, signed: flag}, nil
	case bsontype.DateTime:
		return orderKey{bracket: bracketDate, signed: value.DateTime()}, nil
	case bsontype.Timestamp:
		return orderKey{bracket: bracketTimestamp, unsigned: binary.LittleEndian.Uint64(typed[1:])}, nil
	}
	return orderKey{}, fmt.Errorf("it is a %s, and %s orders null, numbers, strings, binary data, ObjectIds, booleans, dates and timestamps", value.Type, operator)
}

// compareNumbers orders two finite numbers by exact value: an int64 past 2^53 is never rounded.
func compareNumbers(left, right scalarNumber) int {
	switch {
	case left.kind != bsontype.Double && right.kind != bsontype.Double:
		return cmp.Compare(left.whole, right.whole)
	case left.kind == bsontype.Double && right.kind == bsontype.Double:
		return cmp.Compare(left.real, right.real)
	}
	return exactFloat(left).Cmp(exactFloat(right))
}

func exactFloat(number scalarNumber) *big.Float {
	if number.kind == bsontype.Double {
		return new(big.Float).SetFloat64(number.real)
	}
	return new(big.Float).SetInt64(number.whole)
}

// compareOrder is MongoDB's comparison of two keys: by bracket, then inside the bracket.
func compareOrder(left, right orderKey) int {
	if left.bracket != right.bracket {
		return cmp.Compare(left.bracket, right.bracket)
	}
	switch left.bracket {
	case bracketNumber:
		return compareNumbers(left.number, right.number)
	case bracketString, bracketObjectID:
		return bytes.Compare(left.text, right.text)
	case bracketBinary:
		if len(left.text) != len(right.text) {
			return cmp.Compare(len(left.text), len(right.text))
		}
		if left.subtype != right.subtype {
			return cmp.Compare(left.subtype, right.subtype)
		}
		return bytes.Compare(left.text, right.text)
	case bracketBool, bracketDate:
		return cmp.Compare(left.signed, right.signed)
	case bracketTimestamp:
		return cmp.Compare(left.unsigned, right.unsigned)
	}
	return 0
}

// minMaxResult is the value min or max leaves: the operand when the field holds none or the
// operand wins, nil when the stored value stays. A tie keeps the stored value and its type.
func minMaxResult(code byte, stored, operand []byte) ([]byte, error) {
	operator := updateCodeName(code)
	wanted, err := orderOf(operand, operator)
	if err != nil {
		return nil, fmt.Errorf("the operand cannot be compared: %w", err)
	}
	if stored == nil {
		return operand, nil
	}
	have, err := orderOf(stored, operator)
	if err != nil {
		return nil, fmt.Errorf("the stored value cannot be compared: %w", err)
	}
	order := compareOrder(wanted, have)
	if (code == cluster.UpdateMin && order < 0) || (code == cluster.UpdateMax && order > 0) {
		return operand, nil
	}
	return nil, nil
}

// arrayElements reads the stored typed value of an array field: its elements, in order.
func arrayElements(stored []byte, operator string) ([]bson.RawElement, error) {
	if len(stored) == 0 || bsontype.Type(stored[0]) != bsontype.Array {
		kind := "empty"
		if len(stored) > 0 {
			kind = bsontype.Type(stored[0]).String()
		}
		return nil, fmt.Errorf("the stored value is a %s, and %s needs an array", kind, operator)
	}
	if err := ringCheckTypedValue(stored); err != nil {
		return nil, fmt.Errorf("the stored array is not valid BSON: %w", err)
	}
	elements, err := bson.Raw(stored[1:]).Elements()
	if err != nil {
		return nil, fmt.Errorf("reading the stored array: %w", err)
	}
	for position, element := range elements {
		if element.Key() != strconv.Itoa(position) {
			return nil, fmt.Errorf("the stored array has key %q at position %d, so it is not a plain array and %s will not extend it", element.Key(), position, operator)
		}
	}
	return elements, nil
}

// appendedArray is the typed array holding elements and then operand as the last element.
func appendedArray(elements []bson.RawElement, operand []byte) []byte {
	body := make([]byte, 4, 64)
	for _, element := range elements {
		body = append(body, element...)
	}
	body = append(body, operand[0])
	body = strconv.AppendInt(body, int64(len(elements)), 10)
	body = append(body, 0)
	body = append(body, operand[1:]...)
	body = append(body, 0)
	binary.LittleEndian.PutUint32(body, uint32(len(body)))
	return append([]byte{byte(bsontype.Array)}, body...)
}

// pushResult is the array a push leaves: one value appended, or [value] when the field
// holds none.
func pushResult(stored, operand []byte) ([]byte, error) {
	if err := ringCheckTypedValue(operand); err != nil {
		return nil, fmt.Errorf("the operand is not a BSON value: %w", err)
	}
	var elements []bson.RawElement
	if stored != nil {
		var err error
		if elements, err = arrayElements(stored, "push"); err != nil {
			return nil, err
		}
	}
	return appendedArray(elements, operand), nil
}

// setEqual is MongoDB's equality of two values for addToSet: a number equals the same number
// of another numeric type (1 and 1.0), NaN equals NaN, anything else needs the same type and
// bytes. Documents and arrays never reach it.
func setEqual(left, right bson.RawValue) bool {
	first, firstIsNumber := readNumber(left.Type, left.Value)
	second, secondIsNumber := readNumber(right.Type, right.Value)
	switch {
	case firstIsNumber && secondIsNumber:
		if first.kind == bsontype.Double && second.kind == bsontype.Double && (!isFinite(first.real) || !isFinite(second.real)) {
			return first.real == second.real || (math.IsNaN(first.real) && math.IsNaN(second.real))
		}
		if !isFinite(first.real) || !isFinite(second.real) {
			return false
		}
		return compareNumbers(first, second) == 0
	case firstIsNumber != secondIsNumber:
		return false
	}
	return left.Type == right.Type && bytes.Equal(left.Value, right.Value)
}

func isFinite(real float64) bool { return !math.IsNaN(real) && !math.IsInf(real, 0) }

// addToSetResult is the array an addToSet leaves: the value appended unless an equal one is
// there, nil when it is; [value] when the field holds none.
func addToSetResult(stored, operand []byte) ([]byte, error) {
	if err := ringCheckTypedValue(operand); err != nil {
		return nil, fmt.Errorf("the operand is not a BSON value: %w", err)
	}
	if kind := bsontype.Type(operand[0]); kind == bsontype.EmbeddedDocument || kind == bsontype.Array {
		return nil, fmt.Errorf("the operand is a %s, and addToSet takes scalar values", kind)
	}
	var elements []bson.RawElement
	if stored != nil {
		var err error
		if elements, err = arrayElements(stored, "addToSet"); err != nil {
			return nil, err
		}
	}
	wanted := bson.RawValue{Type: bsontype.Type(operand[0]), Value: operand[1:]}
	for _, element := range elements {
		if setEqual(element.Value(), wanted) {
			return nil, nil
		}
	}
	return appendedArray(elements, operand), nil
}

// updateResult is what an operator leaves in its field: the typed value to write, or nil
// when the stored value stays as it is.
func updateResult(code byte, stored, operand []byte) ([]byte, error) {
	switch code {
	case cluster.UpdateInc:
		return incResult(stored, operand)
	case cluster.UpdateMul:
		return mulResult(stored, operand)
	case cluster.UpdateMin, cluster.UpdateMax:
		return minMaxResult(code, stored, operand)
	case cluster.UpdatePush:
		return pushResult(stored, operand)
	case cluster.UpdateAddToSet:
		return addToSetResult(stored, operand)
	}
	return nil, fmt.Errorf("update code %s has no value to compute", updateCodeName(code))
}

// updateComputes says an operator reads the stored value and writes a computed one.
func updateComputes(code byte) bool {
	switch code {
	case cluster.UpdateInc, cluster.UpdateMul, cluster.UpdateMin, cluster.UpdateMax, cluster.UpdatePush, cluster.UpdateAddToSet:
		return true
	}
	return false
}

// updateFix words what a developer does about a refused computation.
func updateFix(code byte) string {
	switch code {
	case cluster.UpdateInc, cluster.UpdateMul:
		return "Set the field to a number first, or use a smaller amount"
	case cluster.UpdateMin, cluster.UpdateMax:
		return "Compare values of one supported type, or set the field to the value you want"
	}
	return "Set the field to an array first, or unset it so the operator starts a new one"
}
