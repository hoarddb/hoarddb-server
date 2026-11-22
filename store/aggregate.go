package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"github.com/hoarddb/hoarddb-server/storage"
)

// ErrAggregateOverflow is returned when a sum would leave its range: an int64
// sum past either end, or a double sum that is no longer finite. The write is
// refused rather than wrapped, so a sum is always exact or absent.
var ErrAggregateOverflow = errors.New("the aggregate would leave its range")

// Value tags: the first byte of every sum value says which number follows.
const (
	aggTagInt   byte = 1
	aggTagFloat byte = 2
)

// AggNumber is a sum: an exact int64 until a double arrives, and a float64 from
// then on, for good. Whole is read when Float is false, Real when it is true.
type AggNumber struct {
	Float bool
	Whole int64
	Real  float64
}

// Add returns n + o. Two integers add in int64 and refuse overflow. Any double
// makes the result a double, and a result that is not finite is refused.
func (n AggNumber) Add(o AggNumber) (AggNumber, error) {
	if !n.Float && !o.Float {
		sum := n.Whole + o.Whole
		if (o.Whole > 0 && sum < n.Whole) || (o.Whole < 0 && sum > n.Whole) {
			return AggNumber{}, fmt.Errorf("adding %d to %d: %w", o.Whole, n.Whole, ErrAggregateOverflow)
		}
		return AggNumber{Whole: sum}, nil
	}
	return finiteDouble(n.asDouble() + o.asDouble())
}

// Sub returns n - o, with the same integer and double rules as Add.
func (n AggNumber) Sub(o AggNumber) (AggNumber, error) {
	if !n.Float && !o.Float {
		diff := n.Whole - o.Whole
		if (o.Whole < 0 && diff < n.Whole) || (o.Whole > 0 && diff > n.Whole) {
			return AggNumber{}, fmt.Errorf("subtracting %d from %d: %w", o.Whole, n.Whole, ErrAggregateOverflow)
		}
		return AggNumber{Whole: diff}, nil
	}
	return finiteDouble(n.asDouble() - o.asDouble())
}

// asDouble is the number as a float64. An int64 past 2^53 rounds here; the
// spec accepts that drift once a double is in the sum.
func (n AggNumber) asDouble() float64 {
	if n.Float {
		return n.Real
	}
	return float64(n.Whole)
}

func finiteDouble(result float64) (AggNumber, error) {
	if math.IsInf(result, 0) || math.IsNaN(result) {
		return AggNumber{}, fmt.Errorf("a double sum became %v: %w", result, ErrAggregateOverflow)
	}
	return AggNumber{Float: true, Real: result}, nil
}

// EncodeSumValue writes the plain sum: [tag:1][sum:8][abs:8], both big-endian. abs is the sum of
// the absolute values of the elements the sum covers, which the apply keeps within AggregateBound so
// that no removal can leave the range (see AggregateBatch).
func EncodeSumValue(sum, abs AggNumber) []byte {
	return appendSum(make([]byte, 0, 17), sum, abs)
}

// EncodeSumByValue writes one by value's sum, absolute sum and element count:
// [tag:1][sum:8][abs:8][n:8].
func EncodeSumByValue(sum, abs AggNumber, count uint64) []byte {
	encoded := appendSum(make([]byte, 0, 25), sum, abs)
	return binary.BigEndian.AppendUint64(encoded, count)
}

// EncodeCountByValue writes one by value's element count: [n:8].
func EncodeCountByValue(count uint64) []byte {
	return binary.BigEndian.AppendUint64(make([]byte, 0, 8), count)
}

// DecodeSumValue reads a value EncodeSumValue wrote. A value of any other
// length, or an unknown tag, is an error naming key.
func DecodeSumValue(key, encoded []byte) (sum, abs AggNumber, err error) {
	if len(encoded) != 17 {
		return AggNumber{}, AggNumber{}, aggregateValueError(key, fmt.Sprintf("a sum is 17 bytes, got %d", len(encoded)))
	}
	return decodeSum(key, encoded[:17])
}

// DecodeSumByValue reads a value EncodeSumByValue wrote.
func DecodeSumByValue(key, encoded []byte) (sum, abs AggNumber, count uint64, err error) {
	if len(encoded) != 25 {
		return AggNumber{}, AggNumber{}, 0, aggregateValueError(key, fmt.Sprintf("a by value's sum is 25 bytes, got %d", len(encoded)))
	}
	sum, abs, err = decodeSum(key, encoded[:17])
	if err != nil {
		return AggNumber{}, AggNumber{}, 0, err
	}
	return sum, abs, binary.BigEndian.Uint64(encoded[17:25]), nil
}

// DecodeCountByValue reads a value EncodeCountByValue wrote.
func DecodeCountByValue(key, encoded []byte) (uint64, error) {
	if len(encoded) != 8 {
		return 0, aggregateValueError(key, fmt.Sprintf("a count is 8 bytes, got %d", len(encoded)))
	}
	return binary.BigEndian.Uint64(encoded), nil
}

func appendSum(dst []byte, sum, abs AggNumber) []byte {
	if sum.Float || abs.Float {
		dst = append(dst, aggTagFloat)
		dst = binary.BigEndian.AppendUint64(dst, math.Float64bits(sum.asDouble()))
		return binary.BigEndian.AppendUint64(dst, math.Float64bits(abs.asDouble()))
	}
	dst = append(dst, aggTagInt)
	dst = binary.BigEndian.AppendUint64(dst, uint64(sum.Whole))
	return binary.BigEndian.AppendUint64(dst, uint64(abs.Whole))
}

func decodeSum(key, encoded []byte) (sum, abs AggNumber, err error) {
	first, second := binary.BigEndian.Uint64(encoded[1:9]), binary.BigEndian.Uint64(encoded[9:17])
	switch encoded[0] {
	case aggTagInt:
		return AggNumber{Whole: int64(first)}, AggNumber{Whole: int64(second)}, nil
	case aggTagFloat:
		return AggNumber{Float: true, Real: math.Float64frombits(first)}, AggNumber{Float: true, Real: math.Float64frombits(second)}, nil
	}
	return AggNumber{}, AggNumber{}, aggregateValueError(key, fmt.Sprintf("the sum tag is %d, want 1 (int64) or 2 (float64)", encoded[0]))
}

// Absolute is |n| as a number of the same kind.
func (n AggNumber) Absolute() AggNumber {
	if n.Float {
		return AggNumber{Float: true, Real: math.Abs(n.Real)}
	}
	if n.Whole < 0 {
		return AggNumber{Whole: -n.Whole}
	}
	return n
}

func aggregateValueError(key []byte, reason string) error {
	return &storage.KeyError{Operation: "parsing an aggregate value", Key: bytes.Clone(key), Cause: errors.New(reason)}
}
