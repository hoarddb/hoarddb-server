package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/hoarddb/hoarddb-server/ringclient"
	"github.com/hoarddb/hoarddb-server/seed"
	"github.com/hoarddb/hoarddb-server/seed/model"
)

const seedUsageText = `Usage: hoarddb seed <layout> [flags]

Loads one of the example data sets the how-to pages use into a running server, so you
can follow a page with its sample data. <layout> is social, chat, ledger, agent or video,
or all for the five of them.

Each layout goes into its own database, named scenarios_<layout>. The data is generated:
the same on every run, so what a page shows is what you get. The command creates the database and its
buckets, and refuses a database that already holds the layout's buckets.

Connection: -address, -user and -password say where and who; pass -fingerprint SHA256:<hex>
(the server prints it when it starts) or, for a node you started yourself, -insecure.

Examples:
  hoarddb seed agent --insecure
  hoarddb seed all --database demo --insecure
  hoarddb seed social --address db.example.com:7433 --fingerprint SHA256:<hex>

Flags:
`

// seedBase is the default base name of the databases a seed creates, kept from the how-to pages
// that read scenarios_<layout>.
const seedBase = "scenarios"

// seedLayoutAll is the layout word that seeds every layout.
const seedLayoutAll = "all"

// seedDataSeed and the scale of one are fixed on purpose: the how-to pages show output that is
// true only for this data.
const seedDataSeed = 1

// seedEnv is what the verb touches outside its arguments, so a test can run it without a process.
type seedEnv struct{ stdout, stderr io.Writer }

// seedFlags are the parsed flags and the one positional argument.
type seedFlags struct {
	connection *connectionFlags
	flagSet    *flag.FlagSet
	layout     string
	database   string
	jsonOut    bool
}

// seedReport is the JSON object --json prints for each layout.
type seedReport struct {
	Layout   string       `json:"layout"`
	Database string       `json:"database"`
	Entities int64        `json:"entities"`
	Elements int64        `json:"elements"`
	Counts   model.Counts `json:"counts"`
}

// RunSeedCommand implements 'hoarddb seed'. Progress and every message go to stderr; with --json
// the report objects go to stdout. The exit code is the shared table of cli/exitcodes.go.
func RunSeedCommand(args []string) int {
	installClientLogger(false)
	ctx, stop := verbStopContext()
	defer stop()
	return runSeed(ctx, args, seedEnv{stdout: os.Stdout, stderr: os.Stderr})
}

// parseSeedArgs reads the flags, which may stand before or after the layout (Go's flag package
// stops at the first positional, so it parses again after taking one).
func parseSeedArgs(args []string, env seedEnv) (*seedFlags, error) {
	var usage strings.Builder
	fs := flag.NewFlagSet("hoarddb seed", flag.ContinueOnError)
	fs.SetOutput(&usage)
	parsed := &seedFlags{connection: registerConnectionFlags(fs), flagSet: fs}
	fs.StringVar(&parsed.database, "database", seedBase, "base name of the databases to create; a layout goes into <database>_<layout>")
	fs.BoolVar(&parsed.jsonOut, "json", false, "print one JSON result object per layout to stdout as well as the text summary on stderr")

	var positionals []string
	remaining := args
	for {
		if err := fs.Parse(remaining); err != nil {
			if errors.Is(err, flag.ErrHelp) {
				_, _ = io.WriteString(env.stdout, seedUsageText)
				fs.SetOutput(env.stdout)
				fs.PrintDefaults()
				return nil, flag.ErrHelp
			}
			return nil, &ingestUsageError{Message: fmt.Sprintf("%s. Run 'hoarddb seed --help' for the flags and examples.", firstLine(usage.String(), err))}
		}
		remaining = fs.Args()
		if len(remaining) == 0 {
			break
		}
		positionals = append(positionals, remaining[0])
		remaining = remaining[1:]
	}
	if len(positionals) != 1 {
		return nil, &ingestUsageError{Message: fmt.Sprintf("seed takes exactly one layout (social, chat, ledger, agent, video or all), and got %d. Example: hoarddb seed agent --insecure", len(positionals))}
	}
	parsed.layout = positionals[0]
	return parsed, nil
}

// layouts resolves the layout word to the layouts to seed.
func (f *seedFlags) layouts() ([]model.Layout, error) {
	if f.layout == seedLayoutAll {
		return model.Layouts, nil
	}
	layout, err := model.ParseLayout(f.layout)
	if err != nil {
		return nil, &ingestUsageError{Message: err.Error()}
	}
	return []model.Layout{layout}, nil
}

// databaseOf is the database a layout is seeded into.
func (f *seedFlags) databaseOf(layout model.Layout) string { return f.database + "_" + string(layout) }

// refuseSeed prints a failure and returns its exit code.
func refuseSeed(env seedEnv, err error) int {
	_, _ = fmt.Fprintf(env.stderr, "seed: %s\n", terminalSafeText(err.Error()))
	var usage *ingestUsageError
	var refusal *ingestRefusal
	switch {
	case errors.As(err, &usage):
		return ExitUsage
	case errors.As(err, &refusal):
		return ExitRefused
	}
	return ringFailure(err).Exit
}

// existsText says what to do about a database that already holds the layout: seed under another
// name, or drop the buckets one by one (there is no drop database statement yet).
func existsText(layout model.Layout, base string, exists *seed.DatabaseExistsError) string {
	var text strings.Builder
	fmt.Fprintf(&text, "Database %q already holds %d bucket(s) of the %s layout (%s), and seeding it again would collide with them. Nothing was written.\n",
		exists.Database, len(exists.Buckets), layout, strings.Join(exists.Buckets, ", "))
	fmt.Fprintf(&text, "Seed under another name:  hoarddb seed %s --database %s2   (creates %s2_%s)\n", layout, base, base, layout)
	fmt.Fprintf(&text, "Or empty %q first, then wait until 'show buckets in %s' lists nothing:", exists.Database, exists.Database)
	for _, bucket := range exists.Buckets {
		fmt.Fprintf(&text, "\n  drop bucket %s in %s", bucket, exists.Database)
	}
	text.WriteString("\n(There is no drop database statement yet, so the buckets are dropped one by one.)")
	return text.String()
}

// runSeed is the verb: usage checks first (exit 1, no dial), then connect (6, 7), then a check of
// every target so a refusal writes nothing, then the loads.
func runSeed(ctx context.Context, args []string, env seedEnv) int {
	flags, err := parseSeedArgs(args, env)
	if errors.Is(err, flag.ErrHelp) {
		return ExitOK
	}
	if err != nil {
		return refuseSeed(env, err)
	}
	layouts, err := flags.layouts()
	if err != nil {
		return refuseSeed(env, err)
	}
	client, _, exit := dialRingForVerb(ctx, env.stderr, flags.flagSet, flags.connection)
	if client == nil {
		return exit
	}
	defer func() { _ = client.Close() }()
	return flags.load(ctx, client, layouts, env)
}

// load seeds each layout into its database.
func (f *seedFlags) load(ctx context.Context, client *ringclient.Client, layouts []model.Layout, env seedEnv) int {
	seeder := seed.New([]*ringclient.Client{client}, seed.Options{Workers: seedWorkers})
	generators := make([]*model.Generator, len(layouts))
	for i, layout := range layouts {
		gen, err := model.New(layout, seedDataSeed, 1)
		if err != nil {
			return refuseSeed(env, fmt.Errorf("generating the %s layout: %w", layout, err))
		}
		generators[i] = gen
		var exists *seed.DatabaseExistsError
		if err := seeder.Check(ctx, gen, f.databaseOf(layout)); errors.As(err, &exists) {
			return refuseSeed(env, &ingestRefusal{Message: existsText(layout, f.database, exists)})
		} else if err != nil {
			return refuseSeed(env, err)
		}
	}
	for i, layout := range layouts {
		database := f.databaseOf(layout)
		_, _ = fmt.Fprintf(env.stderr, "Seeding the %s layout into %s\n", layout, database)
		if _, err := seeder.Seed(ctx, generators[i], database, seed.NewProgress(env.stderr)); err != nil {
			if ctx.Err() != nil {
				_, _ = fmt.Fprintf(env.stderr, "seed: interrupted while loading %s; it may hold part of the layout. Seed under another name with --database, or drop its buckets and run again.\n", database)
				return ExitInterrupted
			}
			var exists *seed.DatabaseExistsError
			if errors.As(err, &exists) {
				return refuseSeed(env, &ingestRefusal{Message: existsText(layout, f.database, exists)})
			}
			return refuseSeed(env, fmt.Errorf("loading %s: %w", database, err))
		}
		f.report(env, layout, generators[i].Counts())
	}
	return ExitOK
}

// seedWorkers is how many documents a seed writes at once.
const seedWorkers = 16

// report prints the summary of one loaded layout, and the JSON object with --json.
func (f *seedFlags) report(env seedEnv, layout model.Layout, counts model.Counts) {
	report := seedReport{Layout: string(layout), Database: f.databaseOf(layout), Counts: counts}
	for name, count := range counts {
		if strings.Contains(name, ".") {
			report.Elements += count
		} else {
			report.Entities += count
		}
	}
	_, _ = fmt.Fprintf(env.stderr, "Seeded %s: %s entities, %s elements. Try: use %s\n",
		report.Database, formatCount(report.Entities), formatCount(report.Elements), report.Database)
	if !f.jsonOut {
		return
	}
	line, err := json.Marshal(report)
	if err != nil {
		_, _ = fmt.Fprintf(env.stderr, "seed: encoding the report: %v\n", err)
		return
	}
	_, _ = fmt.Fprintf(env.stdout, "%s\n", line)
}
