package store

import (
	"errors"
	"math"
	"testing"
)

// TestAggNumberIsExactOrRefused pins that integer sums are exact or refused:
// a wrapped sum would be a wrong number that looks right. A double from either
// side makes the sum a double for good, and a non-finite double is refused.
func TestAggNumberIsExactOrRefused(t *testing.T) {
	if _, err := (AggNumber{Whole: math.MaxInt64}).Add(AggNumber{Whole: 1}); !errors.Is(err, ErrAggregateOverflow) {
		t.Fatalf("MaxInt64 + 1: err %v, want ErrAggregateOverflow", err)
	}
	if _, err := (AggNumber{Whole: math.MinInt64}).Sub(AggNumber{Whole: 1}); !errors.Is(err, ErrAggregateOverflow) {
		t.Fatalf("MinInt64 - 1: err %v, want ErrAggregateOverflow", err)
	}
	if _, err := (AggNumber{Whole: math.MaxInt64}).Sub(AggNumber{Whole: -1}); !errors.Is(err, ErrAggregateOverflow) {
		t.Fatalf("MaxInt64 - -1: err %v, want ErrAggregateOverflow", err)
	}
	if got, err := (AggNumber{Whole: 2}).Add(AggNumber{Whole: 3}); err != nil || got != (AggNumber{Whole: 5}) {
		t.Fatalf("2 + 3 = %+v, %v; want {Whole:5}", got, err)
	}
	if got, err := (AggNumber{Whole: 2}).Add(AggNumber{Float: true, Real: 0.5}); err != nil || got != (AggNumber{Float: true, Real: 2.5}) {
		t.Fatalf("2 + 0.5 = %+v, %v; want a double 2.5", got, err)
	}
	if got, err := (AggNumber{Float: true, Real: 1}).Sub(AggNumber{Whole: 1}); err != nil || got != (AggNumber{Float: true, Real: 0}) {
		t.Fatalf("1.0 - 1 = %+v, %v; want a double 0 that stays a double", got, err)
	}
	if _, err := (AggNumber{Float: true, Real: math.MaxFloat64}).Add(AggNumber{Float: true, Real: math.MaxFloat64}); !errors.Is(err, ErrAggregateOverflow) {
		t.Fatalf("MaxFloat64 + MaxFloat64: err %v, want ErrAggregateOverflow (+Inf is refused)", err)
	}
	if _, err := (AggNumber{Float: true, Real: math.NaN()}).Add(AggNumber{Whole: 1}); !errors.Is(err, ErrAggregateOverflow) {
		t.Fatalf("NaN + 1: err %v, want ErrAggregateOverflow", err)
	}
}

// TestAggregateValueCodecsRoundTrip pins the three value shapes and that each
// decoder refuses any other length, so a truncated value is an error, not zero.
func TestAggregateValueCodecsRoundTrip(t *testing.T) {
	key := []byte("key")
	for _, sum := range []AggNumber{{Whole: -7}, {Whole: math.MaxInt64}, {Float: true, Real: 2.5}} {
		got, gotAbs, err := DecodeSumValue(key, EncodeSumValue(sum, sum.Absolute()))
		if err != nil || got != sum || gotAbs != sum.Absolute() {
			t.Fatalf("DecodeSumValue(EncodeSumValue(%+v)) = %+v, %v", sum, got, err)
		}
		gotSum, _, gotCount, err := DecodeSumByValue(key, EncodeSumByValue(sum, sum.Absolute(), 3))
		if err != nil || gotSum != sum || gotCount != 3 {
			t.Fatalf("DecodeSumByValue(EncodeSumByValue(%+v, 3)) = %+v, %d, %v", sum, gotSum, gotCount, err)
		}
	}
	count, err := DecodeCountByValue(key, EncodeCountByValue(9))
	if err != nil || count != 9 {
		t.Fatalf("DecodeCountByValue(EncodeCountByValue(9)) = %d, %v", count, err)
	}
	if _, _, err := DecodeSumValue(key, make([]byte, 16)); err == nil {
		t.Fatal("DecodeSumValue accepted an 8-byte value")
	}
	if _, _, _, err := DecodeSumByValue(key, make([]byte, 17)); err == nil {
		t.Fatal("DecodeSumByValue accepted a 9-byte value")
	}
	if _, err := DecodeCountByValue(key, make([]byte, 9)); err == nil {
		t.Fatal("DecodeCountByValue accepted a 9-byte value")
	}
}
