package main

import (
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	// planningRate is the offered request rate (requests per second, all four layouts together)
	// `calibrate` assumes when -rate is not given: the fastest engine's measured rate at the highest
	// virtual-user level. The growth of a repetition is set by the fastest engine, not the slowest,
	// because it acknowledges the most writes against the same seeded data. It is a planning input:
	// replace it with the measured rates, -rate takes a list and uses the largest.
	planningRate = 8500.0
	// clientRSSPerScaleBytes and clientRSSBaseBytes model the load generator's resident memory: the model
	// of the seeded data plus the runner, measured at 7.2 to 7.35 GB at scale 100 and 10 GB at scale 150.
	clientRSSPerScaleBytes = 56e6
	clientRSSBaseBytes     = 1.6e9
	// clientRAMShare is the share of the client VM's memory the run may take; over it the run is refused.
	clientRAMShare = 0.60
	// scaleStep is the granularity of a recommended scale.
	scaleStep = 10
	// defaultTargetPct is the growth_pct a recommendation aims at, a margin under the 10% drift line.
	defaultTargetPct = 8.0
	// defaultMaxScale caps a recommendation by what the load generator can hold: the model of the
	// seeded data lives in the client's memory (README: about 56 MB per step of scale plus 1.6 GB); the
	// -client-ram-gb check refuses a scale the client cannot hold.
	defaultMaxScale = 300
)

// growthInput is what the growth model is built from.
type growthInput struct {
	Profile profile
	Access  accessConfig
	Seed    uint64
	// ProbeScale is the scale the seeded elements and the writes per request are measured at;
	// the seeded count is then taken as proportional to the scale.
	ProbeScale int
	// Requests is how many requests of the planner's stream are sampled for the writes per request.
	Requests int
}

// growthModel predicts growth_pct, the elements and documents the engine acknowledged during a
// repetition (warm-up included) as a share of what was seeded, from the request rate and the
// duration alone: growth_pct = 100 * rate * seconds * UnitsPerRequest / (SeededPerScale * scale).
type growthModel struct {
	// UnitsPerRequest is the acknowledged elements plus documents per request, averaged over the
	// mix: reads add none, a write adds one or more (a post adds one per follower inbox).
	UnitsPerRequest float64
	// WritesPerRequest is the share of requests that write.
	WritesPerRequest float64
	// SeededPerScale is the seeded elements and documents per unit of scale.
	SeededPerScale float64
}

// newGrowthModel measures the model: the seeded count from the generated layouts at the probe
// scale, and the acknowledged units per request by drawing requests from the planner with the
// run's profile and access, as the run itself would.
func newGrowthModel(in growthInput) (*growthModel, error) {
	w, err := newWorld(in.Seed, in.ProbeScale)
	if err != nil {
		return nil, err
	}
	if err := w.configureAccess(in.Access); err != nil {
		return nil, err
	}
	mix, err := newMixProfile(benchLayouts, "", in.Profile)
	if err != nil {
		return nil, err
	}
	var seeded int64
	for _, ds := range w.sets {
		for _, count := range ds.counts {
			seeded += count
		}
	}
	p := newPlanner(w, mix, 1, 0, 1)
	var units, writes int
	for range in.Requests {
		req := p.next()
		if req.Write != nil {
			writes++
			units += len(req.Write.Elements) + len(req.Write.Documents)
		}
	}
	if seeded == 0 || in.Requests == 0 {
		return nil, errors.New("the growth model has nothing to measure: no seeded elements or no sampled requests")
	}
	return &growthModel{
		UnitsPerRequest:  float64(units) / float64(in.Requests),
		WritesPerRequest: float64(writes) / float64(in.Requests),
		SeededPerScale:   float64(seeded) / float64(in.ProbeScale),
	}, nil
}

// growthPct is the predicted growth_pct of a repetition that offers rate requests per second for
// seconds (warm-up plus measured window) against data seeded at scale.
func (g *growthModel) growthPct(rate, seconds float64, scale int) float64 {
	return 100 * rate * seconds * g.UnitsPerRequest / (g.SeededPerScale * float64(scale))
}

// scaleFor is the smallest scale, in steps of scaleStep, at which growthPct is at most targetPct.
func (g *growthModel) scaleFor(rate, seconds, targetPct float64) int {
	exact := 100 * rate * seconds * g.UnitsPerRequest / (g.SeededPerScale * targetPct)
	steps := max(1, int(math.Ceil(exact/scaleStep)))
	scale := steps * scaleStep
	// Guard against float rounding at an exact multiple.
	for g.growthPct(rate, seconds, scale) > targetPct {
		scale += scaleStep
	}
	return scale
}

// calibrateConfig is what `bigasstest calibrate` is told.
type calibrateConfig struct {
	Profile   string
	Access    string
	ZipfS     float64
	PageDepth float64
	Seed      uint64
	Window    time.Duration
	WarmMin   time.Duration
	WarmMax   time.Duration
	Rate      float64
	// ClientRAMGB is the load generator's memory; 0 skips the memory check.
	ClientRAMGB float64
	// Scale, when above 0, is checked instead of recommended.
	Scale      int
	TargetPct  float64
	MaxScale   int
	ProbeScale int
	Requests   int
	PrintScale bool
}

// rateList is the -rate flag: one rate or a comma list, reduced to the fastest.
type rateList struct {
	set     bool
	fastest float64
}

func (r *rateList) String() string { return strconv.FormatFloat(r.fastest, 'g', -1, 64) }

func (r *rateList) Set(text string) error {
	r.fastest = 0
	for _, part := range strings.Split(text, ",") {
		rate, err := strconv.ParseFloat(strings.TrimSpace(part), 64)
		if err != nil || !(rate > 0) {
			return fmt.Errorf("%q is not a rate. Pass requests per second above 0, one number or a comma list such as 2000,6000", part)
		}
		r.fastest = max(r.fastest, rate)
	}
	r.set = true
	return nil
}

// clientRSSBytes is the modelled resident memory of the load generator at a scale.
func clientRSSBytes(scale int) float64 {
	return clientRSSBaseBytes + clientRSSPerScaleBytes*float64(scale)
}

// largestScaleForClient is the largest scale, in steps of scaleStep, whose modelled client memory stays
// within clientRAMShare of ramGB; 0 when not even one step fits.
func largestScaleForClient(ramGB float64) int {
	fit := (clientRAMShare*ramGB*1e9 - clientRSSBaseBytes) / clientRSSPerScaleBytes
	return max(0, int(math.Floor(fit/scaleStep))) * scaleStep
}

// parseCalibrateFlags parses the calibrate subcommand and refuses values that cannot give a
// recommendation.
func parseCalibrateFlags(args []string) (calibrateConfig, error) {
	var cfg calibrateConfig
	fs, out := newFlagSet("calibrate")
	fs.SetOutput(out)
	fs.StringVar(&cfg.Profile, "profile", "R", "read-share profile of the run: R or S (writes per request depend on it)")
	fs.StringVar(&cfg.Access, "access", "uniform", "entity selection of the run: uniform or zipf")
	fs.Float64Var(&cfg.ZipfS, "zipf-s", 1.0, "Zipf exponent of the run")
	fs.Float64Var(&cfg.PageDepth, "page-depth", 0.30, "page-depth of the run")
	fs.Uint64Var(&cfg.Seed, "seed", 1, "seed the data is seeded with")
	fs.DurationVar(&cfg.Window, "seconds", 300*time.Second, "measured window of each repetition (the run's -seconds)")
	fs.DurationVar(&cfg.WarmMin, "warm-min", 5*time.Minute, "shortest warm-up (the run's -warm-min); the recommendation assumes the warm-up settles at this length")
	fs.DurationVar(&cfg.WarmMax, "warm-max", 15*time.Minute, "longest warm-up (the run's -warm-max); shown as the worst case")
	var rates rateList
	fs.Var(&rates, "rate", "offered requests per second, all layouts together; a comma list of measured rates uses the largest, because the FASTEST engine grows the data most (default: the fastest engine's measured rate)")
	fs.Float64Var(&cfg.ClientRAMGB, "client-ram-gb", 0, "memory of the load-generator VM in GB; refuses a scale whose modelled client memory (56 MB per step of scale plus 1.6 GB) is over 60% of it (0: not checked)")
	fs.IntVar(&cfg.Scale, "scale", 0, "check this scale against the client memory and print its predicted growth instead of recommending one")
	fs.Float64Var(&cfg.TargetPct, "target-pct", defaultTargetPct, "growth_pct to stay under, a margin below the 10% drift line")
	fs.IntVar(&cfg.MaxScale, "max-scale", defaultMaxScale, "largest scale the load generator's memory and the data disk allow")
	fs.IntVar(&cfg.ProbeScale, "probe-scale", 16, "scale the model measures the seeded elements at")
	fs.IntVar(&cfg.Requests, "requests", 400_000, "requests sampled to measure the writes per request")
	fs.BoolVar(&cfg.PrintScale, "print-scale", false, "print only the recommended scale on stdout (the report goes to stderr), for scripts")
	if err := fs.Parse(args); err != nil {
		return calibrateConfig{}, durationHint(err)
	}
	if _, err := profileByName(cfg.Profile); err != nil {
		return calibrateConfig{}, err
	}
	cfg.Rate = planningRate
	if rates.set {
		cfg.Rate = rates.fastest
	}
	switch {
	case cfg.ClientRAMGB < 0 || cfg.Scale < 0:
		return calibrateConfig{}, errors.New("-client-ram-gb and -scale must be 0 or more")
	case cfg.Access != accessUniform && cfg.Access != accessZipf:
		return calibrateConfig{}, fmt.Errorf("-access %q is not a mode. Use uniform or zipf", cfg.Access)
	case !(cfg.Rate > 0):
		return calibrateConfig{}, errors.New("-rate must be above 0: the offered requests per second")
	case !(cfg.TargetPct > 0) || cfg.TargetPct >= driftPct:
		return calibrateConfig{}, fmt.Errorf("-target-pct %v must be above 0 and under the %v%% drift line", cfg.TargetPct, driftPct)
	case cfg.MaxScale < 1 || cfg.ProbeScale < 1 || cfg.Requests < 1:
		return calibrateConfig{}, errors.New("-max-scale, -probe-scale and -requests must be 1 or more")
	case cfg.Window <= 0 || cfg.WarmMin < 0 || cfg.WarmMax < cfg.WarmMin:
		return calibrateConfig{}, errors.New("-seconds must be above 0 and -warm-max at least -warm-min")
	case cfg.PageDepth < 0 || cfg.PageDepth > 1 || !(cfg.ZipfS > 0):
		return calibrateConfig{}, errors.New("-page-depth must be 0 to 1 and -zipf-s above 0")
	}
	return cfg, nil
}

// calibrateMain runs the calibrate subcommand.
func calibrateMain(args []string) error {
	cfg, err := parseCalibrateFlags(args)
	if err != nil {
		return err
	}
	_, err = calibrate(cfg, os.Stdout, os.Stderr)
	return err
}

// calibrate derives the recommended scale and writes the report to report; with PrintScale the
// scale alone goes to stdout. It returns the recommended scale.
func calibrate(cfg calibrateConfig, stdout, report io.Writer) (int, error) {
	prof, err := profileByName(cfg.Profile)
	if err != nil {
		return 0, err
	}
	model, err := newGrowthModel(growthInput{Profile: prof, Access: accessConfig{Mode: cfg.Access, ZipfS: cfg.ZipfS, PageDepth: cfg.PageDepth},
		Seed: cfg.Seed, ProbeScale: cfg.ProbeScale, Requests: cfg.Requests})
	if err != nil {
		return 0, fmt.Errorf("measuring the growth model: %w", err)
	}
	typical := (cfg.WarmMin + cfg.Window).Seconds()
	worst := (cfg.WarmMax + cfg.Window).Seconds()
	// Built in memory so a failed write is reported once instead of after every line.
	var text strings.Builder
	fmt.Fprintf(&text, "growth model, profile %s, access %s (zipf-s %g, page-depth %g), seed %d, probed at scale %d:\n", cfg.Profile, cfg.Access, cfg.ZipfS, cfg.PageDepth, cfg.Seed, cfg.ProbeScale)
	fmt.Fprintf(&text, "  acked writes per request   %.4f units (%.1f%% of requests write)\n", model.UnitsPerRequest, 100*model.WritesPerRequest)
	fmt.Fprintf(&text, "  seeded per unit of scale   %.1f elements and documents\n", model.SeededPerScale)
	fmt.Fprintf(&text, "  growth_pct = 100 x rate x seconds x %.4f / (%.1f x scale), seconds = warm-up + measured window\n", model.UnitsPerRequest, model.SeededPerScale)
	fmt.Fprintf(&text, "  duration: %.0f s typical (warm-min %v + %v measured), %.0f s worst case (warm-max %v)\n\n", typical, cfg.WarmMin, cfg.Window, worst, cfg.WarmMax)
	rates := map[float64]bool{250: true, 500: true, 1000: true, 2000: true, 5000: true, cfg.Rate: true}
	var ordered []float64
	for rate := range rates {
		ordered = append(ordered, rate)
	}
	sort.Float64s(ordered)
	fmt.Fprintf(&text, "scale that keeps growth_pct at or under %g%% (the drift line is %g%%), by offered rate:\n", cfg.TargetPct, driftPct)
	fmt.Fprintf(&text, "  %12s %14s %14s\n", "req/s", "typical", "worst case")
	for _, rate := range ordered {
		fmt.Fprintf(&text, "  %12.0f %14d %14d%s\n", rate, model.scaleFor(rate, typical, cfg.TargetPct), model.scaleFor(rate, worst, cfg.TargetPct), mark(rate == cfg.Rate))
	}
	wanted := model.scaleFor(cfg.Rate, typical, cfg.TargetPct)
	scale := min(wanted, cfg.MaxScale)
	heading := "RECOMMENDED"
	if cfg.Scale > 0 {
		wanted, scale, heading = cfg.Scale, cfg.Scale, "CHECKED"
	}
	fmt.Fprintf(&text, "\n%s scale %d for %.0f req/s (profile %s, %.0f s): predicted growth_pct %.1f%% typical, %.1f%% worst case\n",
		heading, scale, cfg.Rate, cfg.Profile, typical, model.growthPct(cfg.Rate, typical, scale), model.growthPct(cfg.Rate, worst, scale))
	fmt.Fprintf(&text, "  modelled load-generator memory at scale %d: %.1f GB (56 MB per step of scale plus 1.6 GB, measured)\n", scale, clientRSSBytes(scale)/1e9)
	if wanted > cfg.MaxScale && cfg.Scale == 0 {
		fmt.Fprintf(&text, "  the target needs scale %d, over -max-scale %d: at %d the run is predicted at %.1f%% growth and will carry the drift label. Lower the duration, or accept the label and read drift records as a worse-than-seeded data set\n",
			wanted, cfg.MaxScale, cfg.MaxScale, model.growthPct(cfg.Rate, typical, cfg.MaxScale))
	}
	if _, err := io.WriteString(report, text.String()); err != nil {
		return 0, fmt.Errorf("writing the calibration report: %w", err)
	}
	if cfg.ClientRAMGB > 0 && clientRSSBytes(scale) > clientRAMShare*cfg.ClientRAMGB*1e9 {
		return 0, fmt.Errorf("scale %d needs about %.1f GB of memory on the load generator, over 60%% of its %.1f GB. Use -scale %d or lower, a shorter run (-seconds, -warm-min), or a client VM with more memory",
			scale, clientRSSBytes(scale)/1e9, cfg.ClientRAMGB, largestScaleForClient(cfg.ClientRAMGB))
	}
	if cfg.PrintScale {
		if _, err := fmt.Fprintln(stdout, scale); err != nil {
			return 0, fmt.Errorf("writing the recommended scale: %w", err)
		}
	}
	return scale, nil
}

func mark(selected bool) string {
	if selected {
		return "  <- -rate"
	}
	return ""
}
