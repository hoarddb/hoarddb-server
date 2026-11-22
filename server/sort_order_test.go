package server

import (
	"math"
	"testing"
)

// TestSortKeysCompareIntegersAboveTwoToThe53 is BUG-0178: the sort key held a
// float64, so integers that differ only below float64's spacing tied and
// position ordered them instead of their value.
func TestSortKeysCompareIntegersAboveTwoToThe53(t *testing.T) {
	cases := []struct {
		name        string
		lower       interface{}
		higher      interface{}
		wantOrdered bool
	}{
		{"2^53 and 2^53+1", int64(1 << 53), int64(1<<53 + 1), true},
		{"2^62 and 2^62+1", int64(1 << 62), int64(1<<62 + 1), true},
		{"negative 2^62-1 and 2^62", int64(-(1 << 62)), int64(-(1 << 62) + 1), true},
		{"above int64 range", uint64(math.MaxUint64 - 1), uint64(math.MaxUint64), true},
		{"int64 max below uint64 above it", int64(math.MaxInt64), uint64(math.MaxInt64) + 1, true},
		{"integer below the float that rounds to it", int64(1<<53 + 1), float64(1<<53 + 2), true},
		{"NaN below an integer", math.NaN(), int64(math.MinInt64), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lowerKey, higherKey := sortKeyOf(tc.lower), sortKeyOf(tc.higher)
			if got := compareSortKeys(lowerKey, higherKey); got >= 0 {
				t.Fatalf("compareSortKeys(%v, %v) = %d, want negative", tc.lower, tc.higher, got)
			}
			if got := compareSortKeys(higherKey, lowerKey); got <= 0 {
				t.Fatalf("compareSortKeys(%v, %v) = %d, want positive", tc.higher, tc.lower, got)
			}
		})
	}
	if got := compareSortKeys(sortKeyOf(int64(1<<62)), sortKeyOf(int32(7))); got <= 0 {
		t.Fatalf("2^62 against 7 = %d, want positive", got)
	}
	if got := compareSortKeys(sortKeyOf(int64(5)), sortKeyOf(float64(5))); got != 0 {
		t.Fatalf("int64 5 against float64 5 = %d, want a tie", got)
	}
	if got := compareSortKeys(sortKeyOf(math.NaN()), sortKeyOf(math.NaN())); got != 0 {
		t.Fatalf("NaN against NaN = %d, want a tie", got)
	}
}
