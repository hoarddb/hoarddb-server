package main

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestParseVUsers(t *testing.T) {
	got, err := parseVUsers("1,4,16,64,128")
	if err != nil || !reflect.DeepEqual(got, []int{1, 4, 16, 64, 128}) {
		t.Errorf("got %v, %v", got, err)
	}
	for _, bad := range []string{"", "0", "4,x", "-1", "4,,8"} {
		if _, err := parseVUsers(bad); err == nil {
			t.Errorf("-vusers %q must be refused", bad)
		}
	}
}

func TestParseRunFlagsDefaultsAndRefusals(t *testing.T) {
	cfg, err := parseRunFlags([]string{"-engine", "ring", "-facts", "f.json", "-out", "o", "-reseed-cmd", "true", "-data-dir", "/d"})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(cfg.VUsers, []int{1, 4, 16, 64, 128}) || cfg.Repetitions != 3 || cfg.Loop != "closed" || cfg.Window != 300*time.Second {
		t.Errorf("defaults %+v", cfg)
	}
	if cfg.MinFreeGB != 3 || cfg.WarmMin != 5*time.Minute || cfg.WarmMax != 15*time.Minute || cfg.WarmWindow != 60*time.Second || cfg.WarmTol != 0.05 {
		t.Errorf("guard and warm-up defaults %+v", cfg)
	}
	cases := map[string][]string{
		"-workers is replaced": {"-engine", "ring", "-workers", "64"},
		"-engine":              {"-facts", "f"},
		"-rate":                {"-engine", "ring", "-loop", "open", "-facts", "f", "-out", "o", "-data-dir", "/d"},
		"-loop":                {"-engine", "ring", "-loop", "sideways", "-facts", "f", "-out", "o", "-data-dir", "/d"},
		"-param":               {"-engine", "ring", "-param", "novalue", "-facts", "f", "-out", "o", "-data-dir", "/d"},
	}
	for want, args := range cases {
		if _, err := parseRunFlags(args); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("args %v: err %v, want it to mention %q", args, err, want)
		}
	}
}

func TestProfileFlag(t *testing.T) {
	base := []string{"-engine", "ring", "-facts", "f", "-out", "o", "-data-dir", "/d"}
	cfg, err := parseRunFlags(base)
	if err != nil || cfg.Profile != "R" {
		t.Fatalf("the default profile must be R, got %q %v", cfg.Profile, err)
	}
	cfg, err = parseRunFlags(append(append([]string{}, base...), "-profile", "S"))
	if err != nil || cfg.Profile != "S" {
		t.Fatalf("-profile S: %q %v", cfg.Profile, err)
	}
	if _, err := parseRunFlags(append(append([]string{}, base...), "-profile", "X")); err == nil || !strings.Contains(err.Error(), "-profile") {
		t.Errorf("an unknown profile must be refused and name the flag, got %v", err)
	}
}

func TestAccessFlags(t *testing.T) {
	base := []string{"-engine", "ring", "-facts", "f", "-out", "o", "-data-dir", "/d"}
	cfg, err := parseRunFlags(base)
	if err != nil || cfg.Access != "uniform" || cfg.ZipfS != 1.0 || cfg.PageDepth != 0.30 {
		t.Fatalf("defaults: %+v %v", cfg, err)
	}
	cfg, err = parseRunFlags(append(append([]string{}, base...), "-access", "zipf", "-zipf-s", "1.3", "-page-depth", "0"))
	if err != nil || cfg.Access != "zipf" || cfg.ZipfS != 1.3 || cfg.PageDepth != 0 {
		t.Fatalf("zipf flags: %+v %v", cfg, err)
	}
	for _, bad := range [][]string{{"-access", "hot"}, {"-access", "zipf", "-zipf-s", "0"}, {"-zipf-s", "-2"}, {"-page-depth", "1.5"}, {"-page-depth", "-0.1"}} {
		if _, err := parseRunFlags(append(append([]string{}, base...), bad...)); err == nil {
			t.Errorf("%v must be refused", bad)
		}
	}
}

func TestParseParams(t *testing.T) {
	cfg, err := parseRunFlags([]string{"-engine", "ring", "-facts", "f", "-out", "o", "-data-dir", "/d", "-vusers", "2", "-reps", "1", "-param", "a=1", "-param", "b=x=y"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Params["a"] != "1" || cfg.Params["b"] != "x=y" {
		t.Errorf("params %v", cfg.Params)
	}
}

func TestParseSeedFlagsPassesTheRestThrough(t *testing.T) {
	cfg, err := parseSeedFlags([]string{"-engine", "mongo", "-scenarios-bin", "/x/scenarios", "-scale", "5", "--", "-mongo-uri", "mongodb://h"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Scale != 5 || !reflect.DeepEqual(cfg.Passthrough, []string{"-mongo-uri", "mongodb://h"}) {
		t.Errorf("%+v", cfg)
	}
}

func TestRunNeedsADiskGuardSource(t *testing.T) {
	_, err := parseRunFlags([]string{"-engine", "ring", "-facts", "f", "-out", "o", "-vusers", "2", "-reps", "1"})
	if err == nil || !strings.Contains(err.Error(), "-data-dir") {
		t.Errorf("a run with no disk to watch must be refused and say how, got %v", err)
	}
	if _, err := parseRunFlags([]string{"-engine", "ring", "-facts", "f", "-out", "o", "-vusers", "2", "-reps", "1", "-disk-guard=false"}); err != nil {
		t.Errorf("-disk-guard=false is the explicit opt-out: %v", err)
	}
}

func TestRequestTimeoutDefaultsAndStaysUnderTheHistogramCeiling(t *testing.T) {
	base := []string{"-engine", "ring", "-facts", "f.json", "-out", "o", "-reseed-cmd", "true", "-data-dir", "/d"}
	cfg, err := parseRunFlags(base)
	if err != nil || cfg.RequestTimeout != 30*time.Second {
		t.Fatalf("default %v err %v", cfg.RequestTimeout, err)
	}
	for _, bad := range []string{"0s", "60s", "2m"} {
		if _, err := parseRunFlags(append(append([]string{}, base...), "-request-timeout", bad)); err == nil {
			t.Errorf("-request-timeout %s must be refused", bad)
		}
	}
}

func TestDurationFlagErrorNamesTheUnit(t *testing.T) {
	_, err := parseRunFlags([]string{"-engine", "ring", "-seconds", "60"})
	if err == nil || !strings.Contains(err.Error(), "60s") || !strings.Contains(err.Error(), "unit") {
		t.Errorf("-seconds 60 must be refused with the fix (60s), got %v", err)
	}
	if _, err := parseCalibrateFlags([]string{"-warm-min", "5"}); err == nil || !strings.Contains(err.Error(), "unit") {
		t.Errorf("calibrate -warm-min 5 must name the unit, got %v", err)
	}
}
