package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/hoarddb/hoarddb-server/bench/scenarios/model"
)

// listFlag collects a repeatable flag.
type listFlag []string

func (l *listFlag) String() string     { return strings.Join(*l, ",") }
func (l *listFlag) Set(v string) error { *l = append(*l, v); return nil }

// parseVUsers reads the -vusers list, in the order given.
func parseVUsers(text string) ([]int, error) {
	if text == "" {
		return nil, errors.New("-vusers is empty. Pass the levels to run in order, for example -vusers 1,4,16,64,128")
	}
	var levels []int
	for _, part := range strings.Split(text, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(part))
		if err != nil || n < 1 {
			return nil, fmt.Errorf("-vusers has %q, which is not a whole number of 1 or more", part)
		}
		levels = append(levels, n)
	}
	return levels, nil
}

// runConfig is what `bigasstest run` is told.
type runConfig struct {
	Engine, URI string
	Params      map[string]string
	Database    string
	Scale       int
	Seed        uint64
	RandSeed    uint64
	VUsers      []int
	Repetitions int
	Loop        string
	Rate        float64
	Window      time.Duration
	WarmWindow  time.Duration
	WarmMin     time.Duration
	WarmMax     time.Duration
	WarmTol     float64
	Op          string
	// LedgerExpenses is the expenses per ledger account the data was seeded with (the +deep<N>
	// variant word); 0 is the default ledger.
	LedgerExpenses int
	// Profile is the read-share profile of the mix: "R" or "S".
	Profile string
	// Access is the entity selection, "uniform" or "zipf", with exponent ZipfS; PageDepth is
	// the share of paged reads that start at a random offset.
	Access     string
	ZipfS      float64
	PageDepth  float64
	Level      string
	Facts, Out string
	DataDir    string
	FreeCmd    string
	MinFreeGB  int64
	DiskGuard  bool
	ReseedCmd  string
	// ServerCPUCmd prints the engine server process's cumulative CPU seconds.
	ServerCPUCmd string
	// RequestTimeout bounds one request.
	RequestTimeout time.Duration
	// AllowCold permits -warm-max 0.
	AllowCold bool
	// Profiled labels the repetitions as run with profile samplers attached.
	Profiled bool
	// MarkDir receives an empty file at the start and at the end of every measured window.
	MarkDir string
}

// durationFlagError matches the flag package's refusal of a value for one of the duration flags.
var durationFlagError = regexp.MustCompile(`for flag -(seconds|warm-window|warm-min|warm-max|request-timeout)\b`)

// durationHint adds the fix to a flag error about a duration: the runner's duration flags take Go
// duration strings with a unit, so -seconds 60 is an error and -seconds 60s is right.
func durationHint(err error) error {
	if durationFlagError.MatchString(err.Error()) {
		return fmt.Errorf("%w. Durations need a unit: write 60s, 5m or 1h30m, never a bare 60 (for example -seconds 60s -warm-min 5m)", err)
	}
	return err
}

func newFlagSet(name string) (*flag.FlagSet, io.Writer) {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	return fs, os.Stderr
}

// parseRunFlags parses the flags of the run subcommand and refuses a configuration that
// cannot produce a comparable record, naming the fix.
func parseRunFlags(args []string) (runConfig, error) {
	var cfg runConfig
	fs, out := newFlagSet("run")
	fs.SetOutput(out)
	var vusers, workers string
	var params listFlag
	fs.StringVar(&cfg.Engine, "engine", "", "engine adapter to run: ring, mongo, mysql, postgres, ...")
	fs.StringVar(&cfg.URI, "uri", "", "connection string or address of the engine (adapter specific)")
	fs.Var(&params, "param", "adapter setting key=value (repeatable; adapter specific)")
	fs.StringVar(&cfg.Database, "database", "bat", "base database name; layout L is <database>_L, as `bigasstest seed` created it")
	fs.IntVar(&cfg.Scale, "scale", 300, "scale the data was seeded at")
	fs.Uint64Var(&cfg.Seed, "seed", 1, "seed the data was seeded with")
	fs.Uint64Var(&cfg.RandSeed, "rand-seed", 1, "seed of every virtual user's request stream (with the user's index); identical on every engine")
	fs.StringVar(&vusers, "vusers", "1,4,16,64,128", "virtual-user levels to run, in this order; each level gets -reps repetitions")
	fs.StringVar(&workers, "workers", "", "removed: use -vusers")
	fs.IntVar(&cfg.Repetitions, "reps", 3, "repetitions per level, each from freshly seeded data")
	fs.StringVar(&cfg.Loop, "loop", "closed", "closed (one request in flight per virtual user) or open (Poisson arrivals at -rate)")
	fs.Float64Var(&cfg.Rate, "rate", 0, "open loop: offered requests per second, the same for every engine")
	fs.DurationVar(&cfg.Window, "seconds", 300*time.Second, "measured window of each repetition")
	fs.DurationVar(&cfg.WarmWindow, "warm-window", 60*time.Second, "warm-up window length")
	fs.DurationVar(&cfg.WarmMin, "warm-min", 5*time.Minute, "shortest warm-up")
	fs.DurationVar(&cfg.WarmMax, "warm-max", 15*time.Minute, "longest warm-up; the record says warm=false if it has not settled by then (0 skips the warm-up)")
	fs.Float64Var(&cfg.WarmTol, "warm-tol", 0.05, "two successive warm-up windows within this fraction of each other count as settled")
	fs.IntVar(&cfg.LedgerExpenses, "ledger-expenses", 0, "expenses per ledger account the data was seeded with (scenarios -layout-variant ...+deep<N>); 0 is the default of 100")
	fs.StringVar(&cfg.Op, "op", "", "run only this operation, for a record-only leg, for example social.follow_scan")
	fs.StringVar(&cfg.Profile, "profile", "R", "read-share profile of the mix: R (per-layout read shares social 90%, chat 60%, ledger 70%, agent 50%; the headline) or S (the layouts spec's 20/30/30/10)")
	fs.StringVar(&cfg.Access, "access", "uniform", "how a request picks its entity among the seeded ones: uniform, or zipf (a seeded Zipf draw over each bucket's seeding order, rank 0 hottest, the same on every engine)")
	fs.Float64Var(&cfg.ZipfS, "zipf-s", 1.0, "exponent of the Zipf law for -access zipf (1.0: rank k is drawn with probability proportional to 1/k)")
	fs.Float64Var(&cfg.PageDepth, "page-depth", 0.30, "share of paged reads on an ordered store that start at a random offset inside the seeded elements instead of the first one (0 reads every page from the start)")
	fs.StringVar(&cfg.Level, "level", "L-A", "engine configuration level label recorded in every record")
	fs.StringVar(&cfg.Facts, "facts", "", "facts.json with the host-side items of comparison-plan 3.4 (the wrapper collects it)")
	fs.StringVar(&cfg.Out, "out", "", "directory the records are written to, one JSON file per repetition")
	fs.StringVar(&cfg.DataDir, "data-dir", "", "local data directory to watch for free space")
	fs.StringVar(&cfg.FreeCmd, "free-cmd", "", "command that prints the free bytes of a remote data disk (an ssh df line)")
	fs.Int64Var(&cfg.MinFreeGB, "min-free-gb", 3, "abort when less than this many GB are free")
	fs.BoolVar(&cfg.DiskGuard, "disk-guard", true, "watch free disk space (turn off only for an engine on a disk that cannot fill)")
	fs.DurationVar(&cfg.RequestTimeout, "request-timeout", 30*time.Second, "longest one request may take; a request past it counts as an error with its elapsed time in the latency, so a hung request cannot stall a level (must be under the 60 s histogram ceiling)")
	fs.BoolVar(&cfg.AllowCold, "allow-cold", false, "permit -warm-max 0 (no warm-up). Lazy connection setup then lands in the measured latency on some engines, so use it only for a record-only check")
	fs.BoolVar(&cfg.Profiled, "profiled", false, "label every record as run with profile samplers attached: such a record is listed apart in the summary and never enters a median")
	fs.StringVar(&cfg.MarkDir, "mark-dir", "", "directory that gets an empty file measure-start-v<N>-r<R> when each measured window opens and measure-end-v<N>-r<R> when it closes, so a wrapper can capture profiles after the warm-up")
	fs.StringVar(&cfg.ServerCPUCmd, "server-cpu-cmd", "", "command that prints the engine server process's cumulative CPU seconds (an ssh cat of its /proc stat, converted); read before and after the measured window to fill server_cpu_seconds")
	fs.StringVar(&cfg.ReseedCmd, "reseed-cmd", "", "command that stops, cleans, starts and seeds the engine again; run before every repetition but the first")
	if err := fs.Parse(args); err != nil {
		return runConfig{}, durationHint(err)
	}
	if workers != "" {
		return runConfig{}, errors.New("-workers is replaced by -vusers: every virtual user is an independent client. Pass -vusers 1,4,16,64,128")
	}
	if cfg.Engine == "" {
		return runConfig{}, errors.New("-engine is empty. Pass the adapter to run, for example -engine ring")
	}
	if _, err := profileByName(cfg.Profile); err != nil {
		return runConfig{}, err
	}
	if cfg.Access != accessUniform && cfg.Access != accessZipf {
		return runConfig{}, fmt.Errorf("-access %q is not a mode. Use uniform or zipf", cfg.Access)
	}
	if !(cfg.ZipfS > 0) {
		return runConfig{}, fmt.Errorf("-zipf-s %v is not an exponent. Use a number above 0, 1.0 is the usual choice", cfg.ZipfS)
	}
	if cfg.PageDepth < 0 || cfg.PageDepth > 1 {
		return runConfig{}, fmt.Errorf("-page-depth %v is not a share. Use a number from 0 to 1", cfg.PageDepth)
	}
	if cfg.LedgerExpenses < 0 {
		return runConfig{}, fmt.Errorf("-ledger-expenses %d is not a depth. Use the N of the seed's +deep<N>, or 0 for the default ledger", cfg.LedgerExpenses)
	}
	if cfg.Loop != "closed" && cfg.Loop != "open" {
		return runConfig{}, fmt.Errorf("-loop %q: use closed or open", cfg.Loop)
	}
	if cfg.Loop == "open" && cfg.Rate <= 0 {
		return runConfig{}, errors.New("-rate is missing. An open loop needs the offered requests per second, one rate for every engine")
	}
	levels, err := parseVUsers(vusers)
	if err != nil {
		return runConfig{}, err
	}
	cfg.VUsers = levels
	cfg.Params = map[string]string{}
	for _, entry := range params {
		key, value, ok := strings.Cut(entry, "=")
		if !ok || key == "" {
			return runConfig{}, fmt.Errorf("-param %q is not key=value", entry)
		}
		cfg.Params[key] = value
	}
	if cfg.Facts == "" {
		return runConfig{}, errors.New("-facts is empty. Pass the facts.json the wrapper collected (scripts/certification/bigasstest.sh writes it)")
	}
	if cfg.Out == "" {
		return runConfig{}, errors.New("-out is empty. Pass the directory the records are written to")
	}
	if cfg.WarmMax == 0 && !cfg.AllowCold {
		return runConfig{}, errors.New("-warm-max 0 skips the warm-up, so connections that some engines open lazily (Mongo, PostgreSQL) are opened inside the measured window and the comparison is not fair. Keep the default, or pass -allow-cold for a record-only check")
	}
	if cfg.RequestTimeout <= 0 || cfg.RequestTimeout >= histMaxMicros*time.Microsecond {
		return runConfig{}, fmt.Errorf("-request-timeout %v: use more than 0 and less than %v, the histogram ceiling", cfg.RequestTimeout, histMaxMicros*time.Microsecond)
	}
	if cfg.Repetitions < 1 || cfg.Scale < 1 {
		return runConfig{}, errors.New("-reps and -scale must be 1 or more")
	}
	if cfg.DiskGuard && cfg.DataDir == "" && cfg.FreeCmd == "" {
		return runConfig{}, errors.New("nothing to guard. Pass -data-dir for a local data disk or -free-cmd for a remote one (a full disk crashes hoardDB with SIGBUS), or -disk-guard=false to run without the guard")
	}
	return cfg, nil
}

// seedConfig is what `bigasstest seed` is told.
type seedConfig struct {
	Engine, Database, ScenariosBin string
	Scale                          int
	Seed                           uint64
	Passthrough                    []string
	// Variant is the layout variant to seed, variantCommon or variantNative.
	Variant string
}

// parseSeedFlags parses the seed subcommand; everything after -- goes to scenarios unchanged.
func parseSeedFlags(args []string) (seedConfig, error) {
	var cfg seedConfig
	fs, out := newFlagSet("seed")
	fs.SetOutput(out)
	fs.StringVar(&cfg.Engine, "engine", "", "engine to seed: ring, mongo, mysql, postgres, ...")
	fs.StringVar(&cfg.Database, "database", "bat", "base database name; layout L is seeded into <database>_L")
	fs.StringVar(&cfg.ScenariosBin, "scenarios-bin", "scenarios", "the scenarios binary (go build ./bench/scenarios)")
	fs.StringVar(&cfg.Variant, "layout-variant", variantCommon, "common (every engine), or native (ring only): User.Feed elements also carry the post's author and text, Room.Members elements the member's name; run with -param native=on. Add +agg (ring only) to declare the ledger's aggregates, for example common+agg; run with -param aggregates=on")
	fs.IntVar(&cfg.Scale, "scale", 300, "scale of every layout")
	fs.Uint64Var(&cfg.Seed, "seed", 1, "seed of the generator")
	if err := fs.Parse(args); err != nil {
		return seedConfig{}, err
	}
	if cfg.Engine == "" {
		return seedConfig{}, errors.New("-engine is empty. Pass the engine to seed, for example -engine ring, then -- and the connection flags of scenarios")
	}
	if cfg.Scale < 1 {
		return seedConfig{}, errors.New("-scale must be 1 or more")
	}
	if _, err := model.ParseVariant(cfg.Variant); err != nil {
		return seedConfig{}, fmt.Errorf("-layout-variant: %w", err)
	}
	cfg.Passthrough = fs.Args()
	return cfg, nil
}

// secretKey matches parameter names whose values must not reach a record.
var secretKey = regexp.MustCompile(`(?i)pass|secret|token|key|dsn|uri|auth|cred`)

// describeRunFlags is the run_flags object of the record.
func describeRunFlags(cfg runConfig) map[string]string {
	flags := map[string]string{
		"database": cfg.Database, "disk_guard": strconv.FormatBool(cfg.DiskGuard), "allow_cold": strconv.FormatBool(cfg.AllowCold),
		"warm_window": cfg.WarmWindow.String(), "warm_min": cfg.WarmMin.String(), "warm_max": cfg.WarmMax.String(),
		"warm_tol": strconv.FormatFloat(cfg.WarmTol, 'g', -1, 64), "request_timeout": cfg.RequestTimeout.String(),
	}
	for key, value := range cfg.Params {
		if secretKey.MatchString(key) {
			value = "<redacted>"
		}
		flags["param."+key] = value
	}
	return flags
}
