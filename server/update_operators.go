package server

import (
	"errors"
	"fmt"
	"math"
)

// ErrUpdateOperand is the sentinel under every update refused because an
// operator cannot be applied to the field it names. It reaches the wire as
// INVALID_ARGUMENT.
var ErrUpdateOperand = errors.New("update operator cannot be applied")

// operandRefusal is an update refused for what a field holds, with the text a
// developer reads. The refusal depends only on the stored document and the
// operators, so the owner, every replica and a replay of the log reach the same
// answer on the same document.
type operandRefusal struct{ message string }

func (e *operandRefusal) Error() string { return e.message }

func (e *operandRefusal) Unwrap() error { return ErrUpdateOperand }

// operandProblem is why a numeric operator could not be applied to a value.
type operandProblem int

const (
	operandFits operandProblem = iota
	operandNotANumber
	operandOverflows
)

// updateSite names the document an update is being applied to, for refusals.
type updateSite struct {
	database, bucket, key string
}

// int64Bound is 2^63, the first float64 above int64's range.
const int64Bound = float64(1 << 63)

// applyUpdateOperators applies all update operators to a document in memory. It
// returns the first operator refused, and the caller then discards doc without
// storing it, so an update is all or nothing. Each operator's keys are paths
// (spec §4.9), visited in name order, so the refusal a developer reads is the
// same on every node. A caller applying one update to many documents parses it
// once with parseUpdate instead.
func applyUpdateOperators(doc map[string]interface{}, key string, req UpdateRequest) error {
	parsed, err := parseUpdate(req)
	if err != nil {
		return err
	}
	return parsed.apply(doc, key)
}

// incrementField adds delta to a numeric value, or creates it holding delta
// when it is absent.
//
// The result type follows the stored value, because the wire carries the delta
// as an int64 and cannot say "2.5 + 0.5": an int32, an int64 and a whole double
// within int64's range are read as integers and the result is an int64 (a whole
// double has always been rewritten as an int64 here; typed-fields spec F-6
// owns that), a fractional double, or a double beyond int64's range, stays a
// double.
func incrementField(target pathSlot, site updateSite, field string, delta int64) error {
	current, exists := target.get()
	if !exists {
		target.set(delta)
		return nil
	}
	incremented, problem := incrementedValue(current, delta)
	switch problem {
	case operandNotANumber:
		return notANumberRefusal("$inc", site, field, current)
	case operandOverflows:
		return overflowRefusal("$inc", site, field, current, "the result")
	}
	target.set(incremented)
	return nil
}
func incrementedValue(current interface{}, delta int64) (interface{}, operandProblem) {
	if whole, isInteger := integerOf(current); isInteger {
		sum := whole + delta
		// The sum wrapped when it moved against the delta's sign.
		if (delta > 0 && sum < whole) || (delta < 0 && sum > whole) {
			return nil, operandOverflows
		}
		return sum, operandFits
	}
	if double, isDouble := current.(float64); isDouble {
		return double + float64(delta), operandFits
	}
	return nil, operandNotANumber
}

// multiplyField multiplies a numeric field by factor. A field that is absent is
// left absent: there is nothing to scale.
//
// An int32 or int64 times a whole factor is an int64, and an overflow is
// refused; an integer times a fractional factor is a double; a double stays a
// double.
func multiplyField(target pathSlot, site updateSite, field string, factor float64) error {
	current, exists := target.get()
	if !exists {
		return nil
	}
	multiplied, problem := multipliedValue(current, factor)
	switch problem {
	case operandNotANumber:
		return notANumberRefusal("$mul", site, field, current)
	case operandOverflows:
		return overflowRefusal("$mul", site, field, current, "the product")
	}
	target.set(multiplied)
	return nil
}

func multipliedValue(current interface{}, factor float64) (interface{}, operandProblem) {
	switch stored := current.(type) {
	case int32, int64:
		whole, _ := integerOf(stored)
		if !wholeInt64Factor(factor) {
			return float64(whole) * factor, operandFits
		}
		product, fits := multiplyInt64(whole, int64(factor))
		if !fits {
			return nil, operandOverflows
		}
		return product, operandFits
	case float64:
		return stored * factor, operandFits
	}
	return nil, operandNotANumber
}

// wholeInt64Factor reports whether a $mul factor is a whole number inside
// int64's range, so an integer times it stays an integer. A fractional, NaN
// or infinite factor, or one outside the range, makes the product a double.
func wholeInt64Factor(factor float64) bool {
	return factor == math.Trunc(factor) && factor >= -int64Bound && factor < int64Bound
}

// integerOf reads an int32, an int64, or a double that is a whole number inside
// int64's range as an int64.
func integerOf(value interface{}) (int64, bool) {
	switch stored := value.(type) {
	case int32:
		return int64(stored), true
	case int64:
		return stored, true
	case float64:
		if stored == math.Trunc(stored) && stored >= -int64Bound && stored < int64Bound {
			return int64(stored), true
		}
	}
	return 0, false
}

// multiplyInt64 reports whether left x right fits in an int64, and the product
// when it does.
func multiplyInt64(left, right int64) (int64, bool) {
	if left == 0 || right == 0 {
		return 0, true
	}
	product := left * right
	wrapped := product/right != left ||
		(left == -1 && right == math.MinInt64) ||
		(right == -1 && left == math.MinInt64)
	return product, !wrapped
}

func notANumberRefusal(operator string, site updateSite, field string, found interface{}) error {
	return &operandRefusal{message: fmt.Sprintf(
		"%s needs a number: field %q of document %q in %s.%s holds %s, so the update was refused and nothing was changed. "+
			"%s works on int32, int64 and double fields; use $set to replace the value, or $unset it first.",
		operator, field, site.key, site.database, site.bucket, receivedWord(found), operator)}
}

// overflowRefusal names the operator, the field, the document and the stored
// type, never the stored number or the operand: the message is logged on replay
// and a log names keys, not values.
func overflowRefusal(operator string, site updateSite, field string, found interface{}, outcome string) error {
	return &operandRefusal{message: fmt.Sprintf(
		"%s on field %q of document %q in %s.%s would overflow int64: the field holds %s and %s is outside int64's range, "+
			"so the update was refused and nothing was changed. Use $set to store a double, or a smaller operand.",
		operator, field, site.key, site.database, site.bucket, receivedWord(found), outcome)}
}
