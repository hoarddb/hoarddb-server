package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"time"
)

// The three steps of a leg, as separate processes the harness can run, time and
// resume on their own (spec 11 rule 2: a seed or a warm-up over ten minutes runs
// under nohup and is polled): -mode seed writes the dataset, -mode warm runs the
// mix unmeasured until throughput settles, -mode run measures one leg on data
// that is already there.

// seedResult is the one JSON line -mode seed prints.
type seedResult struct {
	Mode         string  `json:"mode"`
	Engine       string  `json:"engine"`
	Dataset      string  `json:"dataset"`
	Users        int     `json:"users"`
	Posts        int     `json:"posts"`
	WriteConcern string  `json:"write_concern,omitempty"`
	SetupSeconds float64 `json:"setup_s"`
	SeedSeconds  float64 `json:"seed_s"`
	// PostsPerSecond is the bulk-load rate the harness records.
	PostsPerSecond float64 `json:"bulk_load_posts_per_s"`
}

// runSeed creates the schema and seeds the dataset. It refuses a database that
// already holds one, as the engines' setup does.
func runSeed(ctx context.Context, cfg *config, out io.Writer) error {
	spec, err := lookupDataset(cfg.Dataset)
	if err != nil {
		return err
	}
	eng, err := newEngine(ctx, cfg)
	if err != nil {
		return err
	}
	defer eng.close()
	social, ok := eng.(socialEngine)
	if !ok {
		return fmt.Errorf("-engine %s does not implement the social operations", cfg.Engine)
	}
	return seedOn(ctx, cfg, social, newSocialGraph(spec, datasetSeed), out)
}

// seedOn is runSeed for an engine that is already open, seeding it from graph.
func seedOn(ctx context.Context, cfg *config, social socialEngine, graph *socialGraph, out io.Writer) error {
	spec := graph.spec
	setupStart := time.Now()
	if _, err := social.setupSocial(ctx, spec); err != nil {
		return fmt.Errorf("setting up the %s schema: %w", spec.Name, err)
	}
	seedStart := time.Now()
	if err := social.seedSocial(ctx, graph, cfg.Conc); err != nil {
		return fmt.Errorf("seeding %s: %w", spec.Name, err)
	}
	result := seedResult{
		Mode: modeSeed, Engine: cfg.Engine, Dataset: spec.Name, Users: spec.Users, Posts: graph.postsPerUser * spec.Users,
		WriteConcern: cfg.WriteConcern, SetupSeconds: seedStart.Sub(setupStart).Seconds(), SeedSeconds: time.Since(seedStart).Seconds(),
	}
	result.PostsPerSecond = float64(result.Posts) / result.SeedSeconds
	return printJSON(out, "seed result", result)
}

// printJSON writes v as one line.
func printJSON(out io.Writer, what string, v any) error {
	line, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("encoding the %s: %w", what, err)
	}
	if _, err := fmt.Fprintln(out, string(line)); err != nil {
		return fmt.Errorf("writing the %s: %w", what, err)
	}
	return nil
}

// warmResult is the one JSON line -mode warm prints.
type warmResult struct {
	Mode    string `json:"mode"`
	Engine  string `json:"engine"`
	Dataset string `json:"dataset"`
	Mix     string `json:"mix"`
	// Warm is true when two consecutive windows differed by under 5%.
	Warm            bool      `json:"warm"`
	Seconds         float64   `json:"seconds"`
	WindowSeconds   float64   `json:"window_s"`
	WindowOpsPerSec []float64 `json:"window_ops_per_s"`
}

// warmConvergence is how close two consecutive windows' throughput must be.
const warmConvergence = 0.05

// settled says whether the last two windows differ by under 5% of the larger.
func settled(windows []float64) bool {
	if len(windows) < 2 {
		return false
	}
	previous, latest := windows[len(windows)-2], windows[len(windows)-1]
	return math.Max(previous, latest) > 0 && math.Abs(latest-previous)/math.Max(previous, latest) < warmConvergence
}

// warmShort is the length of a short warm-up (spec 11, K-warm-short).
const warmShort = 5 * time.Minute

// warmBounds are the shortest and longest warm-up: -warm-min and -warm-max, or
// a fixed five minutes for a short one.
func (c *config) warmBounds() (minimum, maximum time.Duration) {
	if c.WarmShort {
		return warmShort, warmShort
	}
	return c.WarmMin, c.WarmMax
}

// warmWindows runs the mix in windows of cfg.WarmWindow until throughput has
// settled after at least cfg.WarmMin, or cfg.WarmMax passes, and returns each
// window's operations per second. Nothing here is measured for a record: the
// point is that the caches and the engine are in the state a leg will find.
func warmWindows(ctx context.Context, cfg *config, graph *socialGraph, eng socialEngine) ([]float64, error) {
	book := newSocialBook(graph, true)
	minimum, maximum := cfg.warmBounds()
	var windows []float64
	started := time.Now()
	for {
		windowStart := time.Now()
		results, err := runMixWorkers(ctx, cfg, eng, graph, book, windowStart, windowStart.Add(cfg.WarmWindow))
		if failure := firstFailure(results); failure != nil {
			return windows, fmt.Errorf("warming up: verification failed: %w", failure)
		}
		if err != nil {
			return windows, fmt.Errorf("warming up: %w", err)
		}
		completed := int64(0)
		for _, result := range results {
			completed += result.reads.count() + result.writes.count()
		}
		windows = append(windows, float64(completed)/time.Since(windowStart).Seconds())
		elapsed := time.Since(started)
		if (elapsed >= minimum && settled(windows)) || elapsed+cfg.WarmWindow > maximum {
			return windows, nil
		}
	}
}

// runWarm is -mode warm: it attaches to the seeded dataset and warms it with
// the configured mix. A warm-up that has not settled by the end records
// warm=false and the harness goes on (spec 6.6).
func runWarm(ctx context.Context, cfg *config, out io.Writer) error {
	spec, err := lookupDataset(cfg.Dataset)
	if err != nil {
		return err
	}
	eng, err := newEngine(ctx, cfg)
	if err != nil {
		return err
	}
	defer eng.close()
	social, ok := eng.(socialEngine)
	if !ok {
		return fmt.Errorf("-engine %s does not implement the social operations", cfg.Engine)
	}
	return warmOn(ctx, cfg, social, newSocialGraph(spec, datasetSeed), out)
}

// warmOn is runWarm for an engine that is already open, whose dataset was seeded from graph.
func warmOn(ctx context.Context, cfg *config, social socialEngine, graph *socialGraph, out io.Writer) error {
	spec := graph.spec
	started := time.Now()
	windows, err := warmWindows(ctx, cfg, graph, social)
	if err != nil {
		return err
	}
	return printJSON(out, "warm result", warmResult{
		Mode: modeWarm, Engine: cfg.Engine, Dataset: spec.Name, Mix: mixLabel(cfg.Mix), Warm: settled(windows),
		Seconds: time.Since(started).Seconds(), WindowSeconds: cfg.WarmWindow.Seconds(), WindowOpsPerSec: windows,
	})
}
