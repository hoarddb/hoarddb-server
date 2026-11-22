package main

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/bench/scenarios/model"
)

func seedLine(t *testing.T, engine string, layout model.Layout, mutate func(model.Counts)) []byte {
	t.Helper()
	gen, err := model.New(layout, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	counts := gen.Counts()
	if mutate != nil {
		mutate(counts)
	}
	line, err := json.Marshal(map[string]any{"engine": engine, "layout": string(layout), "database": layoutDatabase("bat", layout), "scale": 2, "seed": 1, "counts": counts, "writes": 5, "seed_s": 0.1})
	if err != nil {
		t.Fatal(err)
	}
	return append([]byte("progress noise\n"), append(line, '\n')...)
}

func TestSeedRefusesCountMismatch(t *testing.T) {
	var gotArgs []string
	exec := func(_ context.Context, name string, args ...string) ([]byte, error) {
		gotArgs = args
		return seedLine(t, "hoarddb", model.Social, func(c model.Counts) { c["User.Feed"]-- }), nil
	}
	req := seedRequest{Bin: "scenarios", Engine: "ring", Scale: 2, Seed: 1, Database: "bat", Passthrough: []string{"-addr", "x:1"}}
	_, err := runSeed(context.Background(), exec, req, model.Social)
	if err == nil || !strings.Contains(err.Error(), "User.Feed") {
		t.Fatalf("a count mismatch must be refused and name the store, got %v", err)
	}
	for _, want := range []string{"-engine", "hoarddb", "-layout", "social", "-scale", "2", "-seed", "1", "-database", "bat", "-addr", "x:1"} {
		if !slices.Contains(gotArgs, want) {
			t.Errorf("scenarios was not given %q: %v", want, gotArgs)
		}
	}
}

func TestSeedAcceptsMatchingCounts(t *testing.T) {
	exec := func(_ context.Context, _ string, args ...string) ([]byte, error) {
		return seedLine(t, "mongo", model.Chat, nil), nil
	}
	rep, err := runSeed(context.Background(), exec, seedRequest{Bin: "s", Engine: "mongo", Scale: 2, Seed: 1, Database: "bat"}, model.Chat)
	if err != nil || rep.Database != "bat_chat" {
		t.Fatalf("rep=%+v err=%v", rep, err)
	}
}

func TestSeedRefusesOutputWithoutAReport(t *testing.T) {
	exec := func(context.Context, string, ...string) ([]byte, error) { return []byte("nothing useful\n"), nil }
	if _, err := runSeed(context.Background(), exec, seedRequest{Bin: "s", Engine: "mongo", Scale: 2, Seed: 1, Database: "bat"}, model.Chat); err == nil {
		t.Error("output with no JSON report line must be an error")
	}
}

// seedLineVariant is a scenarios report line that names a layout variant.
func seedLineVariant(t *testing.T, layout model.Layout, variant, fingerprint string) []byte {
	t.Helper()
	gen, err := model.New(layout, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	report := map[string]any{"engine": "hoarddb", "layout": string(layout), "database": layoutDatabase("bat", layout), "scale": 2, "seed": 1,
		"counts": gen.Counts(), "writes": 5, "seed_s": 0.1}
	if variant != "" {
		report["variant"] = variant
	}
	if fingerprint != "" {
		report["native_fingerprint"] = fingerprint
	}
	line, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	return append(line, '\n')
}

func TestSeedRefusesVariantMismatch(t *testing.T) {
	gen, err := model.New(model.Social, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	fingerprint := gen.Native().Fingerprint()
	for name, tc := range map[string]struct {
		asked, reported, fingerprint string
		wantErr                      string
	}{
		"native asked, common reported":   {variantNative, variantCommon, "", "common"},
		"native asked, a stale binary":    {variantNative, "", "", "scenarios binary"},
		"native asked, wrong fingerprint": {variantNative, variantNative, "native-social-0000000000000000", "fingerprint"},
		"native asked, no fingerprint":    {variantNative, variantNative, "", "fingerprint"},
		"common asked, native reported":   {variantCommon, variantNative, fingerprint, "native"},
		"native asked and reported":       {variantNative, variantNative, fingerprint, ""},
		"common asked and reported":       {variantCommon, variantCommon, "", ""},
		"common asked, a stale binary":    {variantCommon, "", "", ""},
		"nothing asked, nothing reported": {"", "", "", ""},
		"nothing asked, native reported":  {"", variantNative, fingerprint, "native"},
		"agg asked and reported":          {"common+agg", "common+agg", "", ""},
		"agg asked, common reported":      {"common+agg", variantCommon, "", "common"},
		"common asked, agg reported":      {variantCommon, "common+agg", "", "common+agg"},
		"agg asked, a stale binary":       {"common+agg", "", "", "scenarios binary"},
		"native+agg asked and reported":   {"native+agg", "native+agg", fingerprint, ""},
		"native+agg asked, native only":   {"native+agg", variantNative, fingerprint, "native"},
		"native+agg, no fingerprint":      {"native+agg", "native+agg", "", "fingerprint"},
		"deep asked and reported":         {"common+deep500", "common+deep500", "", ""},
		"deep asked, common reported":     {"common+deep500", variantCommon, "", "common+deep500"},
		"deep asked, other depth":         {"common+deep500", "common+deep900", "", "common+deep500"},
		"common asked, deep reported":     {variantCommon, "common+deep500", "", "common+deep500"},
	} {
		var gotArgs []string
		exec := func(_ context.Context, _ string, args ...string) ([]byte, error) {
			gotArgs = args
			return seedLineVariant(t, model.Social, tc.reported, tc.fingerprint), nil
		}
		_, err := runSeed(context.Background(), exec, seedRequest{Bin: "s", Engine: "ring", Scale: 2, Seed: 1, Database: "bat", Variant: tc.asked}, model.Social)
		switch {
		case tc.wantErr == "" && err != nil:
			t.Errorf("%s: %v", name, err)
		case tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr) || !strings.Contains(err.Error(), "variant")):
			t.Errorf("%s: error = %v, want one naming the variant and %q", name, err, tc.wantErr)
		}
		if tc.asked != "" && tc.asked != variantCommon && !slices.Contains(gotArgs, "-layout-variant") {
			t.Errorf("%s: scenarios was not given -layout-variant: %v", name, gotArgs)
		}
	}
}

func TestSeedFlagsReadTheLayoutVariant(t *testing.T) {
	cfg, err := parseSeedFlags([]string{"-engine", "ring", "-layout-variant", "native"})
	if err != nil || cfg.Variant != variantNative {
		t.Fatalf("variant = %q, err = %v", cfg.Variant, err)
	}
	if cfg, err = parseSeedFlags([]string{"-engine", "ring"}); err != nil || cfg.Variant != variantCommon {
		t.Fatalf("default variant = %q, err = %v, want common", cfg.Variant, err)
	}
	if _, err = parseSeedFlags([]string{"-engine", "ring", "-layout-variant", "fancy"}); err == nil || !strings.Contains(err.Error(), "-layout-variant") {
		t.Fatalf("an unknown variant must be refused naming the flag: %v", err)
	}
}

func TestSeedFlagsReadTheAggregatesVariant(t *testing.T) {
	cfg, err := parseSeedFlags([]string{"-engine", "ring", "-layout-variant", "common+agg"})
	if err != nil || cfg.Variant != "common+agg" {
		t.Fatalf("variant = %q, err = %v", cfg.Variant, err)
	}
	if _, err = parseSeedFlags([]string{"-engine", "ring", "-layout-variant", "agg"}); err == nil || !strings.Contains(err.Error(), "+agg") {
		t.Fatalf("a bare agg must be refused naming +agg: %v", err)
	}
}
