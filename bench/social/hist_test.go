package main

import (
	"encoding/json"
	"math"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
	"time"
)

// lognormalSample draws a latency whose median is about one millisecond, the
// long-tailed shape a database's response times have.
func lognormalSample(rng *rand.Rand) time.Duration {
	micros := math.Exp(math.Log(1000) + rng.NormFloat64())
	return time.Duration(micros * float64(time.Microsecond))
}

func exactQuantile(sorted []time.Duration, quantile float64) time.Duration {
	return sorted[min(len(sorted)-1, int(math.Ceil(quantile*float64(len(sorted))))-1)]
}

func TestHistogramQuantilesWithinOnePercentOfExact(t *testing.T) {
	rng := rand.New(rand.NewPCG(7, 7))
	hist := newHistogram()
	samples := make([]time.Duration, 1_000_000)
	for i := range samples {
		samples[i] = lognormalSample(rng)
		mustRecord(t, hist, samples[i])
	}
	slices.Sort(samples)
	for _, quantile := range []float64{0.50, 0.95, 0.99, 0.999} {
		exact := exactQuantile(samples, quantile)
		got := hist.quantile(quantile)
		if relative := math.Abs(float64(got-exact)) / float64(exact); relative > 0.01 {
			t.Errorf("q%v: histogram %v, exact %v, off by %.3f%% (limit 1%%)", quantile, got, exact, relative*100)
		}
	}
}

func TestHistogramMerge(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 3))
	first, second, together := newHistogram(), newHistogram(), newHistogram()
	for i := range 50_000 {
		sample := lognormalSample(rng)
		mustRecord(t, together, sample)
		if i%2 == 0 {
			mustRecord(t, first, sample)
		} else {
			mustRecord(t, second, sample)
		}
	}
	first.merge(second)
	if first.count() != together.count() {
		t.Fatalf("merged count %d, want %d", first.count(), together.count())
	}
	for _, quantile := range []float64{0.5, 0.9, 0.99, 0.999, 1} {
		if got, want := first.quantile(quantile), together.quantile(quantile); got != want {
			t.Errorf("q%v after merge: %v, recorded together: %v", quantile, got, want)
		}
	}
}

func TestHistogramRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewPCG(5, 5))
	original := newHistogram()
	for range 20_000 {
		mustRecord(t, original, lognormalSample(rng))
	}
	encoded, err := original.encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	decoded, err := decodeHistogram(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if decoded.count() != original.count() {
		t.Fatalf("decoded count %d, want %d", decoded.count(), original.count())
	}
	for _, quantile := range []float64{0.5, 0.95, 0.99, 0.999, 1} {
		if got, want := decoded.quantile(quantile), original.quantile(quantile); got != want {
			t.Errorf("q%v after round trip: %v, before: %v", quantile, got, want)
		}
	}
}

func TestSamplesBelowThousandPrintNA(t *testing.T) {
	for _, tc := range []struct {
		samples int
		wantNA  bool
	}{{999, true}, {1000, false}} {
		hist := newHistogram()
		for range tc.samples {
			mustRecord(t, hist, 2*time.Millisecond)
		}
		summary := hist.summary()
		for name, got := range map[string]latencyMs{"p50": summary.P50, "p95": summary.P95, "p99": summary.P99, "p999": summary.P999} {
			if isNA := got.String() == "n.a."; isNA != tc.wantNA {
				t.Errorf("%d samples: %s printed %q, n.a. expected=%v", tc.samples, name, got.String(), tc.wantNA)
			}
		}
	}
}

func TestHistFileNameIsNotHdr(t *testing.T) {
	name := histFileName("results/leg-RH-closed-64")
	if !strings.HasSuffix(name, ".hist") || strings.HasSuffix(name, ".hdr") {
		t.Fatalf("histogram file name %q must end .hist and not .hdr", name)
	}
}

func TestReportCarriesP999(t *testing.T) {
	hist := newHistogram()
	for range 2000 {
		mustRecord(t, hist, 3*time.Millisecond)
	}
	encoded, err := json.Marshal(hist.summary())
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var fields map[string]any
	if err := json.Unmarshal(encoded, &fields); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, ok := fields["p999"]; !ok {
		t.Fatalf("latency summary JSON %s has no p999 field", encoded)
	}
	for _, kept := range []string{"count", "p50", "p95", "p99", "max"} {
		if _, ok := fields[kept]; !ok {
			t.Errorf("latency summary JSON %s lost the existing %q field", encoded, kept)
		}
	}
}
