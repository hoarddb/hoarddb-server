package main

import "testing"

func TestRangeKeySpansExactlyNKeys(t *testing.T) {
	cases := []struct {
		n                 int
		wantLow, wantHigh string
	}{
		{1, "k0000000", "k0000000"},
		{1000, "k0000000", "k0000999"},
		{10000, "k0000000", "k0009999"},
		{20000, "k0000000", "k0019999"},
	}
	for _, tc := range cases {
		if low, high := rangeKey(0), rangeKey(tc.n-1); low != tc.wantLow || high != tc.wantHigh {
			t.Errorf("n=%d: range %s..%s, want %s..%s", tc.n, low, high, tc.wantLow, tc.wantHigh)
		}
	}
}
