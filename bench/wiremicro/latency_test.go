package main

import (
	"testing"
	"time"
)

func TestPercentileUsesNearestRank(t *testing.T) {
	sorted := make([]time.Duration, 1000)
	for i := range sorted {
		sorted[i] = time.Duration(i+1) * time.Microsecond
	}
	cases := []struct {
		pct  float64
		want time.Duration
	}{
		{50, 500 * time.Microsecond},
		{99, 990 * time.Microsecond},
		{99.9, 999 * time.Microsecond},
		{100, 1000 * time.Microsecond},
	}
	for _, c := range cases {
		if got := percentile(sorted, c.pct); got != c.want {
			t.Errorf("p%v = %v, want %v", c.pct, got, c.want)
		}
	}
}

func TestPercentileOfOneSampleIsThatSample(t *testing.T) {
	if got := percentile([]time.Duration{7 * time.Millisecond}, 99.9); got != 7*time.Millisecond {
		t.Fatalf("got %v", got)
	}
	if got := percentile(nil, 50); got != 0 {
		t.Fatalf("empty input gave %v", got)
	}
}

func TestSummarizeSortsAndReportsRateAndMax(t *testing.T) {
	samples := []time.Duration{5 * time.Millisecond, 1 * time.Millisecond, 9 * time.Millisecond, 3 * time.Millisecond}
	got := summarize(samples, 2*time.Second)
	if got.Ops != 4 || got.OpsPerSec != 2 {
		t.Fatalf("ops %d rate %v, want 4 and 2", got.Ops, got.OpsPerSec)
	}
	if got.MaxUs != 9000 || got.P50Us != 3000 {
		t.Fatalf("max %v p50 %v, want 9000 and 3000", got.MaxUs, got.P50Us)
	}
	if samples[0] != 5*time.Millisecond {
		t.Fatal("summarize reordered the caller's slice")
	}
}

func TestSummarizeOfNothingIsZeroNotNaN(t *testing.T) {
	got := summarize(nil, time.Second)
	if got.Ops != 0 || got.OpsPerSec != 0 || got.MaxUs != 0 {
		t.Fatalf("got %+v", got)
	}
}
