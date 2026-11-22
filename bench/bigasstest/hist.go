package main

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
)

const (
	// histMinMicros and histMaxMicros bound what the histogram can hold: one
	// microsecond to sixty seconds, the longest a request is allowed to take.
	histMinMicros = 1
	histMaxMicros = 60_000_000
	// histSignificantFigures of 3 keeps every quantile within 0.1% of exact.
	histSignificantFigures = 3
	// budget10 and budget50 are the latency budgets of the share columns: a request is "under
	// 10 ms" or "under 50 ms" when its latency is strictly below them.
	budget10 = 10 * time.Millisecond
	budget50 = 50 * time.Millisecond
	// minSamplesForPercentile is the fewest samples from which a percentile is
	// printed: with fewer, p99.9 is the maximum in disguise.
	minSamplesForPercentile = 1000
)

// latencyMs is a latency in milliseconds that prints and encodes as "n.a."
// (JSON null) when too few samples back it, so a table never shows a
// percentile the data cannot support.
type latencyMs float64

var latencyNotAvailable = latencyMs(math.NaN())

// String formats a latency for the human summary and the tables.
func (latency latencyMs) String() string {
	if math.IsNaN(float64(latency)) {
		return "n.a."
	}
	return strconv.FormatFloat(float64(latency), 'f', 3, 64)
}

// MarshalJSON encodes an unavailable latency as null, because JSON has no NaN.
func (latency latencyMs) MarshalJSON() ([]byte, error) {
	if math.IsNaN(float64(latency)) {
		return []byte("null"), nil
	}
	encoded, err := json.Marshal(float64(latency))
	if err != nil {
		return nil, fmt.Errorf("encoding latency %v: %w", float64(latency), err)
	}
	return encoded, nil
}

// UnmarshalJSON reads null back as unavailable.
func (latency *latencyMs) UnmarshalJSON(encoded []byte) error {
	if string(encoded) == "null" {
		*latency = latencyNotAvailable
		return nil
	}
	var millis float64
	if err := json.Unmarshal(encoded, &millis); err != nil {
		return fmt.Errorf("decoding latency %s: %w", encoded, err)
	}
	*latency = latencyMs(millis)
	return nil
}

// sharePct is a share of requests in percent that encodes as null when there are no requests.
type sharePct float64

var shareNotAvailable = sharePct(math.NaN())

// String formats a share for the human summary.
func (share sharePct) String() string {
	if math.IsNaN(float64(share)) {
		return "n.a."
	}
	return strconv.FormatFloat(float64(share), 'f', 1, 64)
}

// MarshalJSON encodes a share of no requests as null, because JSON has no NaN.
func (share sharePct) MarshalJSON() ([]byte, error) {
	if math.IsNaN(float64(share)) {
		return []byte("null"), nil
	}
	encoded, err := json.Marshal(float64(share))
	if err != nil {
		return nil, fmt.Errorf("encoding share %v: %w", float64(share), err)
	}
	return encoded, nil
}

// UnmarshalJSON reads null back as unavailable.
func (share *sharePct) UnmarshalJSON(encoded []byte) error {
	if string(encoded) == "null" {
		*share = shareNotAvailable
		return nil
	}
	var percent float64
	if err := json.Unmarshal(encoded, &percent); err != nil {
		return fmt.Errorf("decoding share %s: %w", encoded, err)
	}
	*share = sharePct(percent)
	return nil
}

// histogram records latencies with bounded memory, however many samples a leg
// takes. It is not safe for concurrent use: every worker owns one and the
// leg merges them after the workers stop.
type histogram struct {
	recorded *hdrhistogram.Histogram
	// clamped counts the samples above the ceiling that were stored as the ceiling.
	clamped int64
	// under10 and under50 count the samples below the two latency budgets, from the exact
	// duration (the buckets of the histogram are not fine enough to cut at 10 and 50 ms).
	under10, under50 int64
}

func newHistogram() *histogram {
	return &histogram{recorded: hdrhistogram.New(histMinMicros, histMaxMicros, histSignificantFigures)}
}

// record adds one latency. A latency below 1 us is stored as 1 us (a fast cache hit is not an
// error). One above 60 s is stored as the ceiling and counted in clamped: an overloaded open
// loop must still produce a record, and the counter says the tail was cut.
func (h *histogram) record(latency time.Duration) error {
	if latency < budget10 {
		h.under10++
	}
	if latency < budget50 {
		h.under50++
	}
	micros := max(latency.Microseconds(), histMinMicros)
	if micros > histMaxMicros {
		micros = histMaxMicros
		h.clamped++
	}
	if err := h.recorded.RecordValue(micros); err != nil {
		return fmt.Errorf("recording latency %v: %w", latency, err)
	}
	return nil
}

// merge adds every sample of other to h.
func (h *histogram) merge(other *histogram) {
	h.recorded.Merge(other.recorded)
	h.clamped += other.clamped
	h.under10 += other.under10
	h.under50 += other.under50
}

// quantile returns the latency at fraction q of the samples, q in [0, 1].
func (h *histogram) quantile(q float64) time.Duration {
	return time.Duration(h.recorded.ValueAtQuantile(q*100)) * time.Microsecond
}

func (h *histogram) count() int64 { return h.recorded.TotalCount() }

// summary reports the quantiles in milliseconds. Percentiles are unavailable
// below minSamplesForPercentile samples; the maximum is reported whenever there is a sample.
func (h *histogram) summary() latencySummary {
	total := h.count()
	if total == 0 {
		return latencySummary{P50: latencyNotAvailable, P95: latencyNotAvailable, P99: latencyNotAvailable, P999: latencyNotAvailable, Max: latencyNotAvailable,
			Under10Pct: shareNotAvailable, Under50Pct: shareNotAvailable}
	}
	millis := func(latency time.Duration) latencyMs { return latencyMs(float64(latency) / float64(time.Millisecond)) }
	summary := latencySummary{Count: total, Max: millis(time.Duration(h.recorded.Max()) * time.Microsecond),
		Under10Pct: sharePct(100 * float64(h.under10) / float64(total)), Under50Pct: sharePct(100 * float64(h.under50) / float64(total))}
	if total < minSamplesForPercentile {
		summary.P50, summary.P95, summary.P99, summary.P999 = latencyNotAvailable, latencyNotAvailable, latencyNotAvailable, latencyNotAvailable
		return summary
	}
	summary.P50, summary.P95 = millis(h.quantile(0.50)), millis(h.quantile(0.95))
	summary.P99, summary.P999 = millis(h.quantile(0.99)), millis(h.quantile(0.999))
	return summary
}

// latencySummary is the quantiles of one histogram in milliseconds, as a record prints them.
type latencySummary struct {
	Count int64     `json:"count"`
	P50   latencyMs `json:"p50"`
	P95   latencyMs `json:"p95"`
	P99   latencyMs `json:"p99"`
	P999  latencyMs `json:"p999"`
	Max   latencyMs `json:"max"`
	// Under10Pct and Under50Pct are the share of requests, in percent, that took under 10 ms
	// and under 50 ms, measured over the measured window only. They are shares of the samples
	// that were timed, so a request that failed is not in them (its operation's error count is).
	Under10Pct sharePct `json:"under_10ms_pct"`
	Under50Pct sharePct `json:"under_50ms_pct"`
}
