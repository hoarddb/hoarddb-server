package ringprobe

import (
	"math"
	"sort"
)

// sampleStats is the median, p90 and maximum of a set of proportions in
// [0, 1] — the three statistics both the review and the accepted proposal
// report for these measurements.
type sampleStats struct {
	count  int
	median float64
	p90    float64
	max    float64
}

// summarize computes sampleStats over samples. It does not mutate samples.
func summarize(samples []float64) sampleStats {
	if len(samples) == 0 {
		return sampleStats{}
	}
	sorted := append([]float64(nil), samples...)
	sort.Float64s(sorted)
	return sampleStats{
		count:  len(sorted),
		median: percentile(sorted, 0.50),
		p90:    percentile(sorted, 0.90),
		max:    sorted[len(sorted)-1],
	}
}

// percentile returns the nearest-rank percentile p (in [0, 1]) of an
// already-sorted, non-empty slice.
func percentile(sorted []float64, p float64) float64 {
	idx := int(math.Ceil(p*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// minMax returns the smallest and largest value in samples. It panics on an
// empty slice because every caller in this package builds samples from a
// fixed, non-empty loop of ring parameters — an empty slice here is a
// programmer error in the probe, not a runtime condition callers should
// handle.
func minMax(samples []float64) (min, max float64) {
	min, max = samples[0], samples[0]
	for _, s := range samples[1:] {
		if s < min {
			min = s
		}
		if s > max {
			max = s
		}
	}
	return min, max
}
