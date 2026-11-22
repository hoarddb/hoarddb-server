package main

import (
	"encoding/json"
	"math"
	"testing"
	"time"
)

func TestHistogramQuantilesAndNotAvailable(t *testing.T) {
	hist := newHistogram()
	for i := 1; i <= 2000; i++ {
		if err := hist.record(time.Duration(i) * time.Millisecond); err != nil {
			t.Fatal(err)
		}
	}
	got := hist.quantile(0.5)
	if math.Abs(got.Seconds()-1.0) > 0.01 {
		t.Errorf("median %v, want about 1s", got)
	}
	if err := hist.record(2 * time.Minute); err != nil {
		t.Errorf("a latency above the ceiling is clamped and counted, not an error: %v", err)
	}
	if hist.clamped != 1 || hist.recorded.Max() < histMaxMicros || hist.recorded.Max() > histMaxMicros+histMaxMicros/500 {
		t.Errorf("clamped %d max %d: want 1 and the ceiling", hist.clamped, hist.recorded.Max())
	}
	other := newHistogram()
	other.merge(hist)
	if other.clamped != 1 {
		t.Errorf("merge lost the overflow counter: %d", other.clamped)
	}
	few := newHistogram()
	_ = few.record(time.Millisecond)
	encoded, err := json.Marshal(few.summary())
	if err != nil {
		t.Fatal(err)
	}
	var back map[string]any
	if err := json.Unmarshal(encoded, &back); err != nil {
		t.Fatal(err)
	}
	if back["p99"] != nil {
		t.Errorf("p99 of one sample is %v, want null", back["p99"])
	}
}

func TestPoissonArrivalsMeanRate(t *testing.T) {
	const arrivals = 50_000
	const rate = 200.0
	start := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	schedule := newArrivalSchedule(start, rate, 9)
	var last time.Time
	for range arrivals {
		last = schedule.next()
	}
	if achieved := arrivals / last.Sub(start).Seconds(); math.Abs(achieved-rate)/rate > 0.02 {
		t.Errorf("mean arrival rate %.1f/s, want %v/s within 2%%", achieved, rate)
	}
}

func TestProcProbeParsesStatWithParenthesesInName(t *testing.T) {
	dir := t.TempDir()
	stat := "42 (a (weird) name) S 1 2 3 4 5 6 7 8 9 10 300 200 0 0 20 0 1"
	writeFile(t, dir+"/stat", stat)
	got, err := procProbe{dir: dir}.cpuTime()
	if err != nil {
		t.Fatal(err)
	}
	if want := 5 * time.Second; got != want {
		t.Errorf("cpuTime = %v, want %v", got, want)
	}
	if !clientBoundLabel(0.9) || clientBoundLabel(0.5) {
		t.Error("client-bound label threshold is 85%")
	}
}

func TestHistogramLatencyBudgetShares(t *testing.T) {
	hist := newHistogram()
	// 4 under 10 ms, 3 more under 50 ms, 3 above; exactly 10 ms is not under 10 ms.
	for _, ms := range []float64{1, 5, 9.999, 9, 10, 20, 49.9, 50, 120, 3000} {
		if err := hist.record(time.Duration(ms * float64(time.Millisecond))); err != nil {
			t.Fatal(err)
		}
	}
	summary := hist.summary()
	if got := float64(summary.Under10Pct); math.Abs(got-40) > 1e-9 {
		t.Errorf("under 10 ms %.2f%%, want 40%%", got)
	}
	if got := float64(summary.Under50Pct); math.Abs(got-70) > 1e-9 {
		t.Errorf("under 50 ms %.2f%%, want 70%%", got)
	}
	other := newHistogram()
	for _, ms := range []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 100} {
		_ = other.record(time.Duration(ms * float64(time.Millisecond)))
	}
	hist.merge(other)
	merged := hist.summary()
	if math.Abs(float64(merged.Under10Pct)-65) > 1e-9 || math.Abs(float64(merged.Under50Pct)-80) > 1e-9 {
		t.Errorf("merged shares %.2f%% and %.2f%%, want 65%% and 85%%", float64(merged.Under10Pct), float64(merged.Under50Pct))
	}
	empty := newHistogram().summary()
	encoded, err := json.Marshal(empty)
	if err != nil {
		t.Fatal(err)
	}
	var back map[string]any
	if err := json.Unmarshal(encoded, &back); err != nil {
		t.Fatal(err)
	}
	if v, ok := back["under_10ms_pct"]; !ok || v != nil {
		t.Errorf("a share of no samples must be null, got %v", v)
	}
	var round latencySummary
	if err := json.Unmarshal(encoded, &round); err != nil || !math.IsNaN(float64(round.Under50Pct)) {
		t.Errorf("null must read back as unavailable: %v %v", err, round.Under50Pct)
	}
}
