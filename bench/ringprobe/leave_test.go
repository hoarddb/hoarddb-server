package ringprobe

import "testing"

// Recorded figures this test compares its own output against. These are not
// assertions the test enforces (see doc.go: this package reports divergence
// rather than tuning to a target). They come from
// the entity-placement-rev3-gemini record finding 2.1:
// "a median of 5.8%-7.0%, a p90 of ~9%, and a maximum of 16.6%", and
// the entity-placement-proposal record §B.4: "median ~6-7%, p90 ~9%, up to
// ~16.6%".
const (
	recordedLeaveMedianLow  = 0.058
	recordedLeaveMedianHigh = 0.070
	recordedLeaveP90        = 0.09
	recordedLeaveMax        = 0.166
	// approxTolerance covers the review's "~" figures, which it did not carry
	// to more than two significant digits.
	approxTolerance = 0.01
)

// TestLeaveEveryMember reproduces the entity-placement-rev3-dispositions record
// G-05's remedy: commit the probe named in the Gemini review's finding 2.1
// (ringprobe-rev2/rev2_test.go:TestLeaveEveryMember, run outside the
// repository) so its figures are reproducible.
//
// Method: for N in [4, 25], P in {256, 271, 4096}, and both node id styles,
// remove every member of an N-member ring in turn and measure the proportion
// of P partitions whose new owner held none of the replicationFactor copies
// the partition held immediately before. That yields N samples per (N, P,
// style); this test pools the two id styles into one set of samples per
// (N, P) — matching the dispatch's requested "median, p90 and maximum per
// (N, P)" — and also reports one statistic across every (N, P) combination,
// which is what the review's and the proposal's headline figures summarize.
func TestLeaveEveryMember(t *testing.T) {
	styles := []nodeIDStyle{hostnameIDs, hexIDs}
	partitionCounts := []int{256, 271, 4096}

	type cell struct {
		n, p  int
		stats sampleStats
	}
	var cells []cell
	var overall []float64

	for _, p := range partitionCounts {
		for n := 4; n <= 25; n++ {
			var pooled []float64
			for _, style := range styles {
				for _, s := range runLeaveEveryMember(t, n, p, style) {
					if s.proportion < 0 || s.proportion > 1 {
						t.Fatalf("N=%d P=%d style=%s leaver=%s: proportion %v out of [0,1]",
							n, p, style, s.leaverID, s.proportion)
					}
					pooled = append(pooled, s.proportion)
					overall = append(overall, s.proportion)
				}
			}
			cells = append(cells, cell{n: n, p: p, stats: summarize(pooled)})
		}
	}

	t.Log("per (N, P) — pooled across hostname and hex node ids, N samples x 2 styles each:")
	for _, c := range cells {
		t.Logf("  N=%2d P=%4d  median=%5.2f%%  p90=%5.2f%%  max=%5.2f%%  (%d samples)",
			c.n, c.p, c.stats.median*100, c.stats.p90*100, c.stats.max*100, c.stats.count)
	}

	overallStats := summarize(overall)
	t.Logf("overall across N=4..25, P={256,271,4096}, both id styles: median=%.4f (%.2f%%) p90=%.4f (%.2f%%) max=%.4f (%.2f%%), n=%d",
		overallStats.median, overallStats.median*100,
		overallStats.p90, overallStats.p90*100,
		overallStats.max, overallStats.max*100,
		overallStats.count)

	t.Logf("recorded (review finding 2.1 / proposal SS.4): median 5.8%%-7.0%%, p90 ~9%%, max 16.6%%")
	if overallStats.median < recordedLeaveMedianLow-approxTolerance || overallStats.median > recordedLeaveMedianHigh+approxTolerance {
		t.Logf("DIVERGENCE: measured median %.2f%% falls outside the recorded 5.8%%-7.0%% band (+/- %.0fpp tolerance)",
			overallStats.median*100, approxTolerance*100)
	} else {
		t.Logf("measured median %.2f%% is within the recorded 5.8%%-7.0%% band", overallStats.median*100)
	}
	if diff := overallStats.p90 - recordedLeaveP90; diff > approxTolerance || diff < -approxTolerance {
		t.Logf("DIVERGENCE: measured p90 %.2f%% is more than %.0fpp from the recorded ~9%%",
			overallStats.p90*100, approxTolerance*100)
	} else {
		t.Logf("measured p90 %.2f%% is within %.0fpp of the recorded ~9%%", overallStats.p90*100, approxTolerance*100)
	}
	if diff := overallStats.max - recordedLeaveMax; diff > approxTolerance || diff < -approxTolerance {
		t.Logf("DIVERGENCE: measured max %.2f%% is more than %.0fpp from the recorded 16.6%%",
			overallStats.max*100, approxTolerance*100)
	} else {
		t.Logf("measured max %.2f%% is within %.0fpp of the recorded 16.6%%", overallStats.max*100, approxTolerance*100)
	}
}
