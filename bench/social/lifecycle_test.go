package main

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestSettledNeedsTwoLevelWindows(t *testing.T) {
	cases := []struct {
		name    string
		windows []float64
		want    bool
	}{
		{"no window", nil, false},
		{"one window", []float64{1000}, false},
		{"within 5%", []float64{1000, 1040}, true},
		{"exactly 5% apart", []float64{1000, 1052.6316}, false},
		{"still climbing", []float64{800, 1000}, false},
		{"settled after climbing", []float64{800, 1000, 1010}, true},
		{"an idle server never settles", []float64{0, 0}, false},
	}
	for _, tc := range cases {
		if got := settled(tc.windows); got != tc.want {
			t.Errorf("%s: settled(%v) = %v; want %v", tc.name, tc.windows, got, tc.want)
		}
	}
}

func TestWarmShortIsFiveMinutes(t *testing.T) {
	short := &config{WarmShort: true, WarmMin: time.Second, WarmMax: time.Hour}
	if minimum, maximum := short.warmBounds(); minimum != 5*time.Minute || maximum != 5*time.Minute {
		t.Errorf("short warm-up bounds %v..%v; want exactly 5m", minimum, maximum)
	}
	long := &config{WarmMin: 5 * time.Minute, WarmMax: 15 * time.Minute}
	if minimum, maximum := long.warmBounds(); minimum != 5*time.Minute || maximum != 15*time.Minute {
		t.Errorf("warm-up bounds %v..%v; want 5m..15m", minimum, maximum)
	}
}

// TestSeedModeSeedsAndTimes: -mode seed writes the dataset and prints its
// times; the dataset then hashes as the generator says.
func TestSeedModeSeedsAndTimes(t *testing.T) {
	engine := fakeHoardEngine(mixTestConfig("rh", "social_seed"))
	var out bytes.Buffer
	if err := seedOn(context.Background(), engine.cfg, engine, newSocialGraph(tinySpec, 7), &out); err != nil {
		t.Fatalf("seed: %v", err)
	}
	var seeded seedResult
	if err := json.Unmarshal(out.Bytes(), &seeded); err != nil || seeded.Mode != modeSeed || seeded.Users != tinySpec.Users || seeded.SeedSeconds <= 0 || seeded.PostsPerSecond <= 0 {
		t.Errorf("seed line %q (%v); want the mode, the users, a seed time and a rate", out.String(), err)
	}
	out.Reset()
	cfg := &config{Engine: engineHoardDB, SampleUsers: tinySpec.Users, Conc: 2}
	if err := parityOn(context.Background(), cfg, engine, newSocialGraph(tinySpec, 7), &out); err != nil {
		t.Fatalf("parity after seed: %v", err)
	}
	var parity parityResult
	if err := json.Unmarshal(out.Bytes(), &parity); err != nil || !parity.MatchesGenerator {
		t.Errorf("a freshly seeded dataset must match the generator: %q (%v)", out.String(), err)
	}
}

// TestWarmModeRunsWindowsAndStops: the warm-up runs whole windows of the mix on
// the attached data and stops by -warm-max at the latest.
func TestWarmModeRunsWindowsAndStops(t *testing.T) {
	world := newFakeWorld(t)
	cfg := mixTestConfig("rh", "social_warm")
	cfg.WarmWindow, cfg.WarmMin, cfg.WarmMax = 80*time.Millisecond, 160*time.Millisecond, 400*time.Millisecond
	var out bytes.Buffer
	if err := warmOn(context.Background(), cfg, world.engine, world.graph, &out); err != nil {
		t.Fatalf("warm: %v", err)
	}
	var warmed warmResult
	if err := json.Unmarshal(out.Bytes(), &warmed); err != nil {
		t.Fatalf("decoding %q: %v", out.String(), err)
	}
	if len(warmed.WindowOpsPerSec) < 2 || len(warmed.WindowOpsPerSec) > 5 || warmed.Mix != "RH" || warmed.Seconds > 1.5 {
		t.Errorf("warm line %+v; want between 2 and 5 windows of RH inside the bound", warmed)
	}
	for _, rate := range warmed.WindowOpsPerSec {
		if rate <= 0 {
			t.Errorf("a window completed no operations: %v", warmed.WindowOpsPerSec)
		}
	}
	if warmed.Warm != settled(warmed.WindowOpsPerSec) {
		t.Errorf("warm=%v does not follow the windows %v", warmed.Warm, warmed.WindowOpsPerSec)
	}
}

// TestRunRecordsWhetherTheWarmUpSettled: warm=false is the only channel between the
// separate warm and run processes.
func TestRunRecordsWhetherTheWarmUpSettled(t *testing.T) {
	cfg := mixTestConfig("rh", "social_warmflag")
	cfg.WarmConverged = true
	if record := completedMixRecord(t, fakeHoardEngine(cfg), cfg); !record.Warm {
		t.Error("a settled warm-up recorded warm=false")
	}
	cfg = mixTestConfig("rh", "social_warmflag2")
	cfg.WarmConverged = false
	if record := completedMixRecord(t, fakeHoardEngine(cfg), cfg); record.Warm {
		t.Error("an unsettled warm-up recorded warm=true")
	}
}
