package main

import (
	"math"
	"slices"
	"time"
)

// latencySummary is the measured shape of one phase. Latencies are in
// microseconds so the JSON reads the same for a 40 us get and a 4 ms insert.
type latencySummary struct {
	Ops       int     `json:"ops"`
	OpsPerSec float64 `json:"ops_per_sec"`
	P50Us     float64 `json:"p50_us"`
	P99Us     float64 `json:"p99_us"`
	P999Us    float64 `json:"p999_us"`
	MaxUs     float64 `json:"max_us"`
}

// percentile is the nearest-rank percentile of an ascending slice: the
// smallest sample that at least pct percent of the samples do not exceed. It
// never interpolates, so every reported figure is a latency that happened.
func percentile(sorted []time.Duration, pct float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	// The epsilon keeps 99.9% of 1000 at rank 999: the product is 999.0000000000001.
	rank := int(math.Ceil(pct/100*float64(len(sorted)) - 1e-9))
	return sorted[min(max(rank, 1), len(sorted))-1]
}

// summarize turns the per-operation latencies of one phase and its wall time
// into a latencySummary. It sorts a copy, not the caller's slice.
func summarize(samples []time.Duration, elapsed time.Duration) latencySummary {
	if len(samples) == 0 {
		return latencySummary{}
	}
	sorted := slices.Clone(samples)
	slices.Sort(sorted)
	summary := latencySummary{
		Ops:    len(sorted),
		P50Us:  micros(percentile(sorted, 50)),
		P99Us:  micros(percentile(sorted, 99)),
		P999Us: micros(percentile(sorted, 99.9)),
		MaxUs:  micros(sorted[len(sorted)-1]),
	}
	if elapsed > 0 {
		summary.OpsPerSec = float64(len(sorted)) / elapsed.Seconds()
	}
	return summary
}

func micros(d time.Duration) float64 { return float64(d) / float64(time.Microsecond) }
