package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"sort"
	"strconv"
	"strings"

	"github.com/hoarddb/hoarddb-server/bench/scenarios/model"
)

// execFunc runs a program and returns its standard output. It is a variable of the seeder so
// tests can stand in for the scenarios binary.
type execFunc func(ctx context.Context, name string, args ...string) ([]byte, error)

// osExec is the real execFunc. A failure carries the program's standard error.
func osExec(ctx context.Context, name string, args ...string) ([]byte, error) {
	command := exec.CommandContext(ctx, name, args...)
	var stderr bytes.Buffer
	command.Stderr = &stderr
	out, err := command.Output()
	if err != nil {
		return out, fmt.Errorf("running %s: %w: %s", name, err, strings.TrimSpace(stderr.String()))
	}
	return out, nil
}

// The layout variants of the scenarios binary's -layout-variant flag.
const (
	variantCommon = "common"
	variantNative = "native"
)

// layoutDatabase is the database the scenarios binary seeds a layout into.
func layoutDatabase(base string, layout model.Layout) string { return base + "_" + string(layout) }

// scenariosEngine maps a bigasstest engine name to the -engine value of the scenarios binary.
func scenariosEngine(engine string) string {
	if engine == "ring" || engine == "h1" {
		return "hoarddb"
	}
	return engine
}

// seedRequest is one seeding of the scenarios binary.
type seedRequest struct {
	Bin      string
	Engine   string
	Scale    int
	Seed     uint64
	Database string
	// Variant is the layout variant to seed: variantCommon (also "") or variantNative.
	Variant string
	// Passthrough is the engine's connection flags, given to scenarios unchanged.
	Passthrough []string
}

// seedReport is the JSON line scenarios prints after loading one layout.
type seedReport struct {
	Engine   string       `json:"engine"`
	Layout   string       `json:"layout"`
	Database string       `json:"database"`
	Scale    int          `json:"scale"`
	Seed     uint64       `json:"seed"`
	Counts   model.Counts `json:"counts"`
	Writes   int64        `json:"writes"`
	Seconds  float64      `json:"seed_s"`
	// Variant and NativeFingerprint are what scenarios says it loaded; a binary from before the
	// variant existed reports neither and can only have loaded the common layout.
	Variant           string `json:"variant"`
	NativeFingerprint string `json:"native_fingerprint,omitempty"`
}

// runSeed seeds one layout through the scenarios binary and refuses a report whose counts
// differ from what the model says the layout holds. The runner never seeds by itself.
func runSeed(ctx context.Context, run execFunc, req seedRequest, layout model.Layout) (seedReport, error) {
	args := []string{"-engine", scenariosEngine(req.Engine), "-layout", string(layout), "-scale", strconv.Itoa(req.Scale),
		"-seed", strconv.FormatUint(req.Seed, 10), "-database", req.Database}
	if req.Variant != "" {
		args = append(args, "-layout-variant", req.Variant)
	}
	args = append(args, req.Passthrough...)
	out, err := run(ctx, req.Bin, args...)
	if err != nil {
		return seedReport{}, fmt.Errorf("seeding %s: %w", layout, err)
	}
	var report seedReport
	found := false
	for _, line := range strings.Split(string(out), "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "{") {
			continue
		}
		if err := json.Unmarshal([]byte(line), &report); err != nil {
			return seedReport{}, fmt.Errorf("seeding %s: reading the report line %q: %w", layout, line, err)
		}
		found = true
	}
	if !found {
		return seedReport{}, fmt.Errorf("seeding %s: scenarios printed no JSON report line, so nothing shows the data was loaded. Run it by hand with the same flags to see why", layout)
	}
	if want := layoutDatabase(req.Database, layout); report.Database != want {
		return seedReport{}, fmt.Errorf("seeding %s: scenarios loaded database %q, the run reads %q", layout, report.Database, want)
	}
	variant, err := model.ParseVariant(req.Variant)
	if err != nil {
		return seedReport{}, fmt.Errorf("seeding %s: %w", layout, err)
	}
	gen, err := model.NewWith(layout, req.Seed, req.Scale, variant.Options())
	if err != nil {
		return seedReport{}, fmt.Errorf("seeding %s: %w", layout, err)
	}
	if err := checkVariant(layout, req.Variant, report, gen); err != nil {
		return seedReport{}, err
	}
	var diffs []string
	for name, want := range gen.Counts() {
		if got := report.Counts[name]; got != want {
			diffs = append(diffs, fmt.Sprintf("%s has %d, the model counts %d", name, got, want))
		}
	}
	if len(diffs) > 0 {
		sort.Strings(diffs)
		return seedReport{}, fmt.Errorf("seeding %s: the loaded counts differ from the model (%s). The engines would not hold the same data, so the run is refused", layout, strings.Join(diffs, "; "))
	}
	return report, nil
}

// seedAll seeds every layout in turn and returns the reports.
func seedAll(ctx context.Context, run execFunc, req seedRequest, layouts []model.Layout) ([]seedReport, error) {
	var reports []seedReport
	for _, layout := range layouts {
		report, err := runSeed(ctx, run, req, layout)
		if err != nil {
			return nil, err
		}
		reports = append(reports, report)
	}
	return reports, nil
}

// checkVariant refuses a report that does not say the layout variant that was asked for. An empty
// report variant is a scenarios binary from before the variant existed: it loaded the common
// layout, so it satisfies a request for common and refuses one for native or +agg.
func checkVariant(layout model.Layout, asked string, report seedReport, gen *model.Generator) error {
	want, err := model.ParseVariant(asked)
	if err != nil {
		return fmt.Errorf("seeding %s: %w", layout, err)
	}
	loaded, err := model.ParseVariant(report.Variant)
	if err != nil {
		return fmt.Errorf("seeding %s: the layout variant scenarios reports: %w", layout, err)
	}
	if loaded != want {
		hint := ""
		if report.Variant == "" {
			hint = ". The scenarios binary reports no variant, so it is older than the native layout: rebuild it with go build ./bench/scenarios"
		}
		return fmt.Errorf("seeding %s: the layout variant asked for is %s and scenarios loaded %s%s. The run would read different data than it is labelled with, so it is refused", layout, want, loaded, hint)
	}
	if want.Native {
		if fingerprint := gen.Native().Fingerprint(); report.NativeFingerprint != fingerprint {
			return fmt.Errorf("seeding %s: the native layout variant fingerprint scenarios reports is %q, the model's is %q. The seeder and this run disagree on which fields are copied; rebuild both from one checkout", layout, report.NativeFingerprint, fingerprint)
		}
	}
	return nil
}
