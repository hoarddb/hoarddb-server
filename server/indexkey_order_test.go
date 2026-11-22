package server

import (
	"bytes"
	"math"
	"math/big"
	"reflect"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson/primitive"

	"github.com/hoarddb/hoarddb-server/store"
)

// TestIndexOrderMatchesCompareValues crosses every pair of values the scan
// path can compare with the key encoding (index-parity-spec §3.4.1; T-23's
// encoder half). An index whose order disagrees with compareValues answers a
// query differently depending on whether the index or a scan served it.
//
// It lives in package server, not beside store/indexkey.go, because
// compareValues and valuesEqual are unexported here and store cannot import
// server.
//
// The one allowed disagreement is the documented one: above 2^53 the key is
// more exact than the scan, which compares through float64. There
// compareValues says "equal" while the keys differ, and the keys must then
// follow the exact mathematical order.
func TestIndexOrderMatchesCompareValues(t *testing.T) {
	negativeZero := math.Copysign(0, -1)
	values := []interface{}{
		// numbers, every spelling and the boundaries §3.4.1 names
		negativeZero, float64(0), int64(0), uint64(0), int32(0),
		float64(-0.5), float32(-0.5), float64(0.5),
		int(30), int8(30), int16(30), int32(30), int64(30), uint(30), uint8(30), uint16(30), uint32(30), uint64(30), float32(30), float64(30),
		int64(-7), float64(-7.25),
		int64(1 << 53), uint64(1 << 53), float64(1 << 53), int64(1<<53 + 1), uint64(1<<53 + 2),
		int64(math.MaxInt64), uint64(math.MaxInt64), uint64(1 << 63), float64(1 << 63),
		uint64(math.MaxUint64), float64(1 << 64),
		int64(math.MinInt64), float64(math.MinInt64),
		float64(1e300), float64(-1e300),
		// dates, both forms
		primitive.DateTime(-1), primitive.DateTime(0), primitive.DateTime(1700000000000),
		// strings, including the escape cases
		"", "a", "a\x00", "a\x00b", "a\x00\xffb", "a\x01", "ab", "b",
	}

	for _, left := range values {
		for _, right := range values {
			scanOrder, comparable := compareValues(left, right)
			if !comparable {
				continue
			}
			leftKey, err := store.EncodeComponent(left)
			if err != nil {
				t.Fatalf("EncodeComponent(%T %v): %v", left, left, err)
			}
			rightKey, err := store.EncodeComponent(right)
			if err != nil {
				t.Fatalf("EncodeComponent(%T %v): %v", right, right, err)
			}
			keyOrder := bytes.Compare(leftKey, rightKey)

			if scanOrder == 0 && keyOrder != 0 {
				exactOrder, isNumber := exactNumericOrder(left, right)
				if !isNumber || exactOrder == 0 {
					t.Fatalf("%T %v and %T %v are equal to the scan (valuesEqual=%v) but have different keys %x, %x",
						left, left, right, right, valuesEqual(left, right), leftKey, rightKey)
				}
				if keyOrder != exactOrder {
					t.Fatalf("%T %v and %T %v: the scan's float64 comparison cannot tell them apart, and the keys must follow their exact order %d, but compare %d",
						left, left, right, right, exactOrder, keyOrder)
				}
				continue
			}
			if keyOrder != scanOrder {
				t.Fatalf("%T %v vs %T %v: compareValues says %d, the keys compare %d (%x vs %x)",
					left, left, right, right, scanOrder, keyOrder, leftKey, rightKey)
			}
		}
	}
}

// exactNumericOrder compares two numbers by their exact values, not through
// float64. isNumber is false when either is not a number.
func exactNumericOrder(left, right interface{}) (order int, isNumber bool) {
	leftExact, ok := exactValue(left)
	if !ok {
		return 0, false
	}
	rightExact, ok := exactValue(right)
	if !ok {
		return 0, false
	}
	return leftExact.Cmp(rightExact), true
}

func exactValue(v interface{}) (*big.Float, bool) {
	reflected := reflect.ValueOf(v)
	switch reflected.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return new(big.Float).SetInt64(reflected.Int()), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return new(big.Float).SetUint64(reflected.Uint()), true
	case reflect.Float32, reflect.Float64:
		return new(big.Float).SetFloat64(reflected.Float()), true
	default:
		return nil, false
	}
}
