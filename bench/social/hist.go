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

// histogram records latencies with bounded memory, however many samples a leg
// takes. It is not safe for concurrent use: every worker owns one and the
// leg merges them after the workers stop.
type histogram struct {
	recorded *hdrhistogram.Histogram
}

func newHistogram() *histogram {
	return &histogram{recorded: hdrhistogram.New(histMinMicros, histMaxMicros, histSignificantFigures)}
}

// record adds one latency. A latency below 1 us is stored as 1 us (a fast
// cache hit is not an error). One above 60 s is returned as an error and not
// clamped, because a clamped sample would understate exactly the stalls the
// tail percentiles exist to show.
func (h *histogram) record(latency time.Duration) error {
	micros := max(latency.Microseconds(), histMinMicros)
	// The library rounds its range up to a whole bucket and accepts values a
	// little above the 60 s it was built for, so the bound is enforced here.
	if micros > histMaxMicros {
		return fmt.Errorf("recording latency %v: above the %v the histogram holds", latency, histMaxMicros*time.Microsecond)
	}
	if err := h.recorded.RecordValue(micros); err != nil {
		return fmt.Errorf("recording latency %v: %w", latency, err)
	}
	return nil
}

// merge adds every sample of other to h.
func (h *histogram) merge(other *histogram) {
	h.recorded.Merge(other.recorded)
}

// quantile returns the latency at fraction q of the samples, q in [0, 1].
func (h *histogram) quantile(q float64) time.Duration {
	return time.Duration(h.recorded.ValueAtQuantile(q*100)) * time.Microsecond
}

func (h *histogram) count() int64 { return h.recorded.TotalCount() }

// encode returns the library's compressed encoding, the content of a .hist file.
func (h *histogram) encode() ([]byte, error) {
	encoded, err := h.recorded.Encode(hdrhistogram.V2CompressedEncodingCookieBase)
	if err != nil {
		return nil, fmt.Errorf("encoding histogram: %w", err)
	}
	return encoded, nil
}

// decodeHistogram reads what encode wrote.
func decodeHistogram(encoded []byte) (*histogram, error) {
	decoded, err := hdrhistogram.Decode(encoded)
	if err != nil {
		return nil, fmt.Errorf("decoding histogram: %w", err)
	}
	return &histogram{recorded: decoded}, nil
}

// histFileName names a leg's histogram file. The extension is .hist, not .hdr:
// .hdr is another tool's file type and its viewers would misread this encoding.
func histFileName(legBase string) string { return legBase + ".hist" }

// summary reports the quantiles in milliseconds. Percentiles are unavailable
// below minSamplesForPercentile samples; the maximum is reported whenever there is a sample.
func (h *histogram) summary() latencySummary {
	total := h.count()
	if total == 0 {
		return latencySummary{P50: latencyNotAvailable, P95: latencyNotAvailable, P99: latencyNotAvailable, P999: latencyNotAvailable, Max: latencyNotAvailable}
	}
	millis := func(latency time.Duration) latencyMs { return latencyMs(float64(latency) / float64(time.Millisecond)) }
	summary := latencySummary{Count: total, Max: millis(time.Duration(h.recorded.Max()) * time.Microsecond)}
	if total < minSamplesForPercentile {
		summary.P50, summary.P95, summary.P99, summary.P999 = latencyNotAvailable, latencyNotAvailable, latencyNotAvailable, latencyNotAvailable
		return summary
	}
	summary.P50, summary.P95 = millis(h.quantile(0.50)), millis(h.quantile(0.95))
	summary.P99, summary.P999 = millis(h.quantile(0.99)), millis(h.quantile(0.999))
	return summary
}
