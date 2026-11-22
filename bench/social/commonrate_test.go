package main

import "testing"

func TestCommonRateIsFractionOfSlowestEngine(t *testing.T) {
	closedMedians := map[string]float64{"hoarddb": 100, "mongo": 200, "mysql": 400}
	result, err := commonRate(closedMedians, 0.5)
	if err != nil {
		t.Fatalf("commonRate: %v", err)
	}
	if result.OfferedRate != 50 {
		t.Errorf("offered rate %v, want 50 (half of the slowest engine's 100 ops/s) for all three engines", result.OfferedRate)
	}
	wantUtilisation := map[string]float64{"hoarddb": 0.5, "mongo": 0.25, "mysql": 0.125}
	for engineName, want := range wantUtilisation {
		if got := result.Utilisation[engineName]; got != want {
			t.Errorf("%s utilisation %v, want %v (offered rate over its own closed median)", engineName, got, want)
		}
	}
	if result.SlowestEngine != "hoarddb" {
		t.Errorf("slowest engine %q, want hoarddb", result.SlowestEngine)
	}
	for _, bad := range []map[string]float64{{}, {"hoarddb": 0}, {"hoarddb": -3}} {
		if _, err := commonRate(bad, 0.5); err == nil {
			t.Errorf("commonRate(%v) accepted medians that cannot fix a rate", bad)
		}
	}
	if _, err := commonRate(closedMedians, 0); err == nil {
		t.Error("commonRate accepted a fraction of 0")
	}
}
