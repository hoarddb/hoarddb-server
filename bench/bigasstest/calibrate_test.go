package main

import (
	"bytes"
	"context"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"
)

// testGrowthModel builds the model the way `bigasstest calibrate` does, at a small probe scale.
func testGrowthModel(t *testing.T, access accessConfig) *growthModel {
	t.Helper()
	model, err := newGrowthModel(growthInput{Profile: profileR, Access: access, Seed: 7, ProbeScale: 4, Requests: 200_000})
	if err != nil {
		t.Fatal(err)
	}
	return model
}

// TestGrowthModelMatchesAFakeRun is the short local check of the model: a closed-loop run on the
// in-memory fake issues a number of requests, and the growth_pct the runner records must equal
// what the model predicts for that many requests, within sampling noise.
func TestGrowthModelMatchesAFakeRun(t *testing.T) {
	for _, access := range []accessConfig{
		{Mode: accessUniform, ZipfS: 1, PageDepth: 0},
		{Mode: accessZipf, ZipfS: 1.0, PageDepth: 0.3},
	} {
		p, _ := fakeRun(t, nil)
		if err := p.World.configureAccess(access); err != nil {
			t.Fatal(err)
		}
		p.Window = 400 * time.Millisecond
		rec, err := runRepetition(context.Background(), p)
		if err != nil {
			t.Fatal(err)
		}
		if rec.Failed {
			t.Fatalf("fake run failed: %v %v", rec.AuditError, rec.ErrorSamples)
		}
		model, err := newGrowthModel(growthInput{Profile: profileR, Access: access, Seed: 7, ProbeScale: 1, Requests: 300_000})
		if err != nil {
			t.Fatal(err)
		}
		// The fake run is at scale 1 and has no warm-up, so every request is in the record's
		// growth: predicted growth for rate x seconds requests is the whole run's.
		predicted := model.growthPct(float64(rec.Overall.Requests), 1, 1)
		if math.Abs(predicted-rec.GrowthPct)/rec.GrowthPct > 0.08 {
			t.Errorf("%s: the model predicts %.2f%% growth for %d requests, the run recorded %.2f%%", access.Mode, predicted, rec.Overall.Requests, rec.GrowthPct)
		}
	}
}

func TestScaleForStaysUnderTheDriftLine(t *testing.T) {
	model := testGrowthModel(t, accessConfig{Mode: accessUniform, ZipfS: 1})
	for _, rate := range []float64{100, 1000, 5000} {
		for _, seconds := range []float64{600, 1200} {
			scale := model.scaleFor(rate, seconds, 8)
			if got := model.growthPct(rate, seconds, scale); got > 8 {
				t.Errorf("rate %v for %vs at scale %d grows %.2f%%, over the 8%% target", rate, seconds, scale, got)
			}
			if scale > scaleStep {
				if got := model.growthPct(rate, seconds, scale-scaleStep); got <= 8 {
					t.Errorf("rate %v for %vs: scale %d already holds (%.2f%%), %d is not the smallest step", rate, seconds, scale-scaleStep, got, scale)
				}
			}
			if scale%scaleStep != 0 {
				t.Errorf("scale %d is not a multiple of %d", scale, scaleStep)
			}
		}
	}
	if model.scaleFor(0.001, 1, 8) != scaleStep {
		t.Errorf("a trivial load still needs one step of scale: %d", model.scaleFor(0.001, 1, 8))
	}
}

// Seeded elements grow in proportion to the scale, which the model relies on to avoid generating
// a large data set: the error between a small and a larger probe stays within 2%.
func TestSeededElementsGrowLinearlyWithScale(t *testing.T) {
	small := testGrowthModel(t, accessConfig{Mode: accessUniform, ZipfS: 1})
	large, err := newGrowthModel(growthInput{Profile: profileR, Access: accessConfig{Mode: accessUniform, ZipfS: 1}, Seed: 7, ProbeScale: 16, Requests: 10_000})
	if err != nil {
		t.Fatal(err)
	}
	if rel := math.Abs(small.SeededPerScale-large.SeededPerScale) / large.SeededPerScale; rel > 0.02 {
		t.Errorf("seeded elements per scale: %.1f at probe 4, %.1f at probe 16", small.SeededPerScale, large.SeededPerScale)
	}
}

func TestCalibrateReportsAndPrintsOnlyTheScale(t *testing.T) {
	cfg, err := parseCalibrateFlags([]string{"-rate", "500", "-probe-scale", "2", "-requests", "50000", "-print-scale"})
	if err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	scale, err := calibrate(cfg, &stdout, &stderr)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := strconv.Atoi(strings.TrimSpace(stdout.String())); err != nil || got != scale || scale < 1 {
		t.Errorf("-print-scale must print just the scale %d on stdout, got %q (%v)", scale, stdout.String(), err)
	}
	for _, want := range []string{"RECOMMENDED", "profile R", "acked writes per request", "growth_pct"} {
		if !strings.Contains(stderr.String(), want) {
			t.Errorf("the report lacks %q:\n%s", want, stderr.String())
		}
	}
}

func TestCalibrateSaysWhenTheTargetNeedsMoreThanMaxScale(t *testing.T) {
	cfg, err := parseCalibrateFlags([]string{"-rate", "100000", "-max-scale", "50", "-probe-scale", "2", "-requests", "50000"})
	if err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	scale, err := calibrate(cfg, &stdout, &stderr)
	if err != nil {
		t.Fatal(err)
	}
	if scale != 50 {
		t.Errorf("the recommendation is capped at -max-scale, got %d", scale)
	}
	if !strings.Contains(stderr.String(), "drift") || !strings.Contains(stderr.String(), "-max-scale") {
		t.Errorf("a capped recommendation must say the run will carry the drift label and why:\n%s", stderr.String())
	}
}

func TestCalibrateRefusesBadFlags(t *testing.T) {
	for _, args := range [][]string{{"-rate", "0"}, {"-target-pct", "0"}, {"-target-pct", "10"}, {"-profile", "Q"}, {"-max-scale", "0"}, {"-seconds", "0s"}} {
		if _, err := parseCalibrateFlags(args); err == nil {
			t.Errorf("%v must be refused", args)
		}
	}
	cfg, err := parseCalibrateFlags(nil)
	if err != nil || cfg.Rate != planningRate || cfg.TargetPct != 8 || cfg.Profile != "R" {
		t.Errorf("defaults %+v %v", cfg, err)
	}
}

// The growth of a repetition is set by the fastest engine, so a list of measured rates is reduced to
// its largest.
func TestCalibrateTakesTheFastestRateOfAList(t *testing.T) {
	cfg, err := parseCalibrateFlags([]string{"-rate", "2000,6000,1100"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Rate != 6000 {
		t.Errorf("-rate 2000,6000,1100 gave %v, want the fastest, 6000", cfg.Rate)
	}
	for _, bad := range []string{"", "x", "2000,,6000", "2000,-1", "0"} {
		if _, err := parseCalibrateFlags([]string{"-rate", bad}); err == nil {
			t.Errorf("-rate %q must be refused", bad)
		}
	}
}

func TestClientMemoryModelMatchesTheMeasuredRuns(t *testing.T) {
	// Measured: 7.2 to 7.35 GB at scale 100, 10 GB at scale 150.
	for scale, wantGB := range map[int]float64{100: 7.25, 150: 10.0} {
		if got := clientRSSBytes(scale) / 1e9; math.Abs(got-wantGB) > 0.5 {
			t.Errorf("scale %d: modelled client RSS %.2f GB, measured about %.2f GB", scale, got, wantGB)
		}
	}
}

func TestCalibrateRefusesAScaleTheClientCannotHold(t *testing.T) {
	// A 16 GB client: 60% is 9.6 GB, which scale 140 reaches (about 9.4 GB), 150 does not.
	cfg, err := parseCalibrateFlags([]string{"-scale", "150", "-client-ram-gb", "16", "-probe-scale", "2", "-requests", "20000"})
	if err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if _, err := calibrate(cfg, &stdout, &stderr); err == nil || !strings.Contains(err.Error(), "60%") || !strings.Contains(err.Error(), "-scale 140") {
		t.Errorf("scale 150 on a 16 GB client must be refused naming 60%% and the largest scale that fits, got %v", err)
	}
	cfg.Scale = 140
	if scale, err := calibrate(cfg, &stdout, &stderr); err != nil || scale != 140 {
		t.Errorf("scale 140 on a 16 GB client must pass, got %d, %v", scale, err)
	}
	cfg.ClientRAMGB = 0
	cfg.Scale = 300
	if _, err := calibrate(cfg, &stdout, &stderr); err != nil {
		t.Errorf("with the client's memory unknown nothing is refused, got %v", err)
	}
}

func TestCalibrateRefusesARecommendationThatDoesNotFit(t *testing.T) {
	cfg, err := parseCalibrateFlags([]string{"-rate", "8500", "-client-ram-gb", "8", "-probe-scale", "2", "-requests", "20000"})
	if err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if _, err := calibrate(cfg, &stdout, &stderr); err == nil || !strings.Contains(err.Error(), "60%") {
		t.Errorf("a recommendation over 60%% of an 8 GB client must be refused, got %v", err)
	}
}
