// Command bigasstest is the internal four-layout benchmark: social, chat, ledger and agent are
// seeded on one node of one engine and served at the same time to a sweep of virtual users,
// at per-layout read shares (profile R), with every read verified and every write audited. See README.md.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"

	"github.com/hoarddb/hoarddb-server/bench/scenarios/model"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: bigasstest seed|run|summary|calibrate|plan-check [flags]. Run 'bigasstest run -h' for the flags of one.")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	var err error
	switch os.Args[1] {
	case "seed":
		err = seedMain(ctx, os.Args[2:])
	case "run":
		err = runMain(ctx, os.Args[2:])
	case "summary":
		err = summaryMain(os.Args[2:])
	case "calibrate":
		err = calibrateMain(os.Args[2:])
	case "plan-check":
		err = planCheckMain(os.Stdout, 1)
	default:
		err = fmt.Errorf("unknown command %q. Use seed, run, summary, calibrate or plan-check", os.Args[1])
	}
	stop()
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		fmt.Fprintln(os.Stderr, "bigasstest:", err)
		os.Exit(1)
	}
}

func seedMain(ctx context.Context, args []string) error {
	cfg, err := parseSeedFlags(args)
	if err != nil {
		return err
	}
	reports, err := seedAll(ctx, osExec, seedRequest{Bin: cfg.ScenariosBin, Engine: cfg.Engine, Scale: cfg.Scale, Seed: cfg.Seed,
		Database: cfg.Database, Variant: cfg.Variant, Passthrough: cfg.Passthrough}, benchLayouts)
	if err != nil {
		return err
	}
	for _, report := range reports {
		fmt.Printf("seeded %s into %s: %d writes in %.1f s, counts match the model\n", report.Layout, report.Database, report.Writes, report.Seconds)
	}
	return nil
}

func runMain(ctx context.Context, args []string) error {
	cfg, err := parseRunFlags(args)
	if err != nil {
		return err
	}
	facts, err := loadFacts(cfg.Facts)
	if err != nil {
		return err
	}
	w, err := newWorldWith(cfg.Seed, cfg.Scale, model.Options{LedgerExpenses: cfg.LedgerExpenses})
	if err != nil {
		return err
	}
	prof, err := profileByName(cfg.Profile)
	if err != nil {
		return err
	}
	if err := w.configureAccess(accessConfig{Mode: cfg.Access, ZipfS: cfg.ZipfS, PageDepth: cfg.PageDepth}); err != nil {
		return err
	}
	mix, err := newMixProfile(benchLayouts, cfg.Op, prof)
	if err != nil {
		return err
	}
	base := runParams{EngineName: cfg.Engine, World: w, Mix: mix, Base: cfg.Database, Loop: cfg.Loop, Rate: cfg.Rate,
		Window: cfg.Window, WarmWindow: cfg.WarmWindow, WarmMin: cfg.WarmMin, WarmMax: cfg.WarmMax, WarmTol: cfg.WarmTol,
		RandSeed: cfg.RandSeed, Seed: cfg.Seed, Scale: cfg.Scale, Level: cfg.Level, Op: cfg.Op, LedgerExpenses: cfg.LedgerExpenses, Facts: facts, RequestTimeout: cfg.RequestTimeout, RunFlags: describeRunFlags(cfg), Profiled: cfg.Profiled}
	if cfg.ServerCPUCmd != "" {
		base.ServerCPU = cmdCPU(cfg.ServerCPUCmd)
	}
	if cfg.MarkDir != "" {
		base.Mark = markFile(cfg.MarkDir)
	}
	if cfg.DiskGuard {
		free := cmdFree(cfg.FreeCmd)
		if cfg.FreeCmd == "" {
			free = statfsFree(cfg.DataDir)
		}
		base.Guard = &diskGuard{free: free, minFree: cfg.MinFreeGB << 30, interval: 5 * time.Second}
	}
	sweep := sweepConfig{Base: base, VUsers: cfg.VUsers, Repetitions: cfg.Repetitions,
		Open: func(ctx context.Context, vusers int) (Engine, error) {
			return openEngine(ctx, Config{Engine: cfg.Engine, URI: cfg.URI, Params: cfg.Params, VUsers: vusers})
		},
		Emit: func(rec *Record) error {
			path, err := writeRecord(cfg.Out, rec)
			if err != nil {
				return err
			}
			fmt.Printf("%s: %d vusers, repetition %d: %.1f req/s, p99 %s ms, failed=%v labels=%v\n", path, rec.VUsers, rec.Repetition,
				rec.Overall.Throughput, rec.Overall.P99, rec.Failed, rec.Labels)
			return nil
		}}
	if cfg.ReseedCmd != "" {
		sweep.Reseed = func(ctx context.Context) error {
			command := exec.CommandContext(ctx, "sh", "-c", cfg.ReseedCmd)
			command.Stdout, command.Stderr = os.Stderr, os.Stderr
			if err := command.Run(); err != nil {
				return fmt.Errorf("-reseed-cmd failed: %w", err)
			}
			return nil
		}
	}
	runErr := runSweep(ctx, sweep)
	if records, skipped, err := loadRecords(cfg.Out); err == nil {
		fmt.Print(summaryTable(records))
		reportSkipped(skipped)
	}
	return runErr
}

func summaryMain(args []string) error {
	if len(args) != 1 {
		return errors.New("usage: bigasstest summary RECORD_DIR")
	}
	records, skipped, err := loadRecords(args[0])
	if err != nil {
		return err
	}
	fmt.Print(summaryTable(records))
	reportSkipped(skipped)
	return nil
}

// reportSkipped says which files of the record directory the summary did not read.
func reportSkipped(skipped []string) {
	if len(skipped) > 0 {
		fmt.Fprintf(os.Stderr, "bigasstest: the summary skipped %d files that are not records of this schema: %v\n", len(skipped), skipped)
	}
}
