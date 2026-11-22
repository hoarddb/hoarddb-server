package ringprobe

import "testing"

// Recorded figures this test compares its own output against (not enforced —
// see leave_test.go's comment on the same point). From
// the entity-placement-rev3-gemini record finding 2.2: "At N=5,
// slot copy movement is ~39.4%-40.2%; at N=25, slot copy movement reaches
// 87.0%-88.1%." Also the entity-placement-proposal record §B.4/Q7:
// "40-88% of stored copies" (N = 5-25).
const (
	recordedPChangeN5Low   = 0.394
	recordedPChangeN5High  = 0.402
	recordedPChangeN25Low  = 0.870
	recordedPChangeN25High = 0.881
)

// TestChangingP reproduces the entity-placement-rev3-dispositions record
// G-06's remedy: commit the probe named in the Gemini review's finding 2.2
// (ringprobe-rev2/pchange_test.go:TestChangingP, run outside the repository)
// so its figures are reproducible.
//
// Method: for N in [4, 25] and both node id styles, step the ring's partition
// count through 256 -> 512 -> 1024 -> 4096 and measure, at each step, the
// proportion of the totalSlots stored copies that land on a different node
// than they did at the previous P. It reports the range across the three
// steps and both id styles for every N, and calls out N=5 and N=25 — the two
// points the review cited — against the recorded ranges.
func TestChangingP(t *testing.T) {
	styles := []nodeIDStyle{hostnameIDs, hexIDs}

	type result struct {
		n     int
		props []float64 // one per (step, style) combination
	}
	var results []result

	for n := 4; n <= 25; n++ {
		r := result{n: n}
		for _, style := range styles {
			for i := 0; i+1 < len(pChangeSteps); i++ {
				from, to := pChangeSteps[i], pChangeSteps[i+1]
				prop := runPChangeStep(t, n, from, to, style)
				if prop < 0 || prop > 1 {
					t.Fatalf("N=%d style=%s P %d->%d: proportion %v out of [0,1]", n, style, from, to, prop)
				}
				t.Logf("N=%2d style=%-8s P %4d->%4d: %6.2f%% of stored copies move", n, style, from, to, prop*100)
				r.props = append(r.props, prop)
			}
		}
		results = append(results, r)
	}

	t.Log("range across the three P steps and both id styles, per N:")
	for _, r := range results {
		lo, hi := minMax(r.props)
		t.Logf("  N=%2d: %.2f%%-%.2f%%", r.n, lo*100, hi*100)
		switch r.n {
		case 5:
			logPChangeComparison(t, r.n, lo, hi, recordedPChangeN5Low, recordedPChangeN5High)
		case 25:
			logPChangeComparison(t, r.n, lo, hi, recordedPChangeN25Low, recordedPChangeN25High)
		}
	}
}

// logPChangeComparison logs whether the measured [lo, hi] range at n overlaps
// the review's recorded [recordedLo, recordedHi] range for that n. It never
// fails the test — see leave_test.go's comment on why divergence is reported,
// not asserted.
func logPChangeComparison(t *testing.T, n int, lo, hi, recordedLo, recordedHi float64) {
	t.Helper()
	overlaps := lo <= recordedHi+approxTolerance && hi >= recordedLo-approxTolerance
	if overlaps {
		t.Logf("    N=%d measured range %.2f%%-%.2f%% overlaps the recorded %.1f%%-%.1f%%",
			n, lo*100, hi*100, recordedLo*100, recordedHi*100)
	} else {
		t.Logf("    DIVERGENCE: N=%d measured range %.2f%%-%.2f%% does not overlap the recorded %.1f%%-%.1f%%",
			n, lo*100, hi*100, recordedLo*100, recordedHi*100)
	}
}
