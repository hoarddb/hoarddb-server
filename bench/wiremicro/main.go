package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"runtime"
	"time"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	err := run(ctx, os.Args[1:], os.Getenv, os.Stdout, os.Stderr)
	stop()
	if errors.Is(err, flag.ErrHelp) {
		return
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "wiremicro: %v\n", err)
		os.Exit(1)
	}
}

// run is main without the process: it parses, checks durability, runs the
// phases, and writes the table to stdout and the JSON to -out.
func run(ctx context.Context, args []string, getenv func(string) string, stdout, stderr io.Writer) error {
	cfg, err := parseConfigTo(args, getenv, stderr)
	if err != nil {
		return err
	}
	logf := log.New(stderr, "", 0)
	logf.Printf("wiremicro %s on %s/%s", cfg.describe(), runtime.GOOS, runtime.GOARCH)
	eng, err := newEngine(ctx, cfg)
	if err != nil {
		return fmt.Errorf("opening %s: %w", cfg.Engine, err)
	}
	defer eng.close()
	if cfg.VerifyLedger != "" {
		return verifyLedger(ctx, cfg, eng, stdout)
	}
	return execute(ctx, cfg, eng, stdout, stderr)
}

// execute is the run after the engine exists: durability check first, then
// setup, workers and phases. The tests drive it with a fake engine.
func execute(ctx context.Context, cfg *config, eng engine, stdout, stderr io.Writer) error {
	observed, err := checkDurability(ctx, cfg.Engine, eng)
	if err != nil {
		return err
	}
	logf := log.New(stderr, "", 0)
	logf.Printf("durability observed: %v", observed)
	if err := eng.setup(ctx, cfg.Payload); err != nil {
		return fmt.Errorf("setting up %s: %w", cfg.Engine, err)
	}
	if cfg.Ledger != "" {
		header, ok := eng.(interface{ ledgerHeader() string })
		if !ok {
			return fmt.Errorf("-ledger is not supported by engine %s", cfg.Engine)
		}
		if cfg.ledger, err = openAckLedger(cfg.Ledger, header.ledgerHeader()); err != nil {
			return err
		}
		defer func() {
			if cerr := cfg.ledger.close(); cerr != nil {
				logf.Printf("%v", cerr)
			}
		}()
	}
	workers, err := openWorkers(ctx, eng, cfg.Workers)
	if err != nil {
		return err
	}
	defer func() {
		for _, w := range workers {
			w.close()
		}
	}()
	started := time.Now().UTC()
	phases, err := runBenchmark(ctx, cfg, workers, func(line string) { logf.Println(line) })
	if err != nil {
		return err
	}
	result := runResult{
		Tool: "wiremicro", Engine: cfg.Engine, StartedAt: started,
		Config:     resultConfig{Ops: cfg.Ops, Workers: cfg.Workers, PayloadBytes: cfg.Payload, Preload: cfg.Preload, Smoke: cfg.Smoke, Target: recordedTarget(eng.target())},
		Durability: observed, DatadirFSNote: cfg.DatadirFSNote, Phases: phases,
	}
	if err := result.writeTable(stdout); err != nil {
		return err
	}
	return writeResultFile(cfg.Out, result)
}

// openWorkers opens n connections one after another: a login burst of 16 is
// well under a node's limit, and a failure names the connection that failed.
func openWorkers(ctx context.Context, eng engine, n int) ([]worker, error) {
	workers := make([]worker, 0, n)
	for i := 0; i < n; i++ {
		w, err := eng.newWorker(ctx)
		if err != nil {
			for _, opened := range workers {
				opened.close()
			}
			return nil, fmt.Errorf("opening connection %d of %d: %w", i+1, n, err)
		}
		workers = append(workers, w)
	}
	return workers, nil
}

func writeResultFile(path string, result runResult) error {
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("creating the result file %s (set -out): %w", path, err)
	}
	if err := result.writeJSON(file); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("closing the result file %s: %w", path, err)
	}
	return nil
}

// verifyLedger reads every id of the ledger back from the server and fails when
// any is missing: an acknowledged insert the server no longer has.
func verifyLedger(ctx context.Context, cfg *config, eng engine, stdout io.Writer) error {
	adopter, ok := eng.(interface{ adoptLedgerHeader(string) error })
	if !ok {
		return fmt.Errorf("-verify-ledger is not supported by engine %s", cfg.Engine)
	}
	header, ids, err := readLedger(cfg.VerifyLedger)
	if err != nil {
		return err
	}
	if err := adopter.adoptLedgerHeader(header); err != nil {
		return err
	}
	w, err := eng.newWorker(ctx)
	if err != nil {
		return fmt.Errorf("opening the verify connection: %w", err)
	}
	defer w.close()
	missing, err := verifyIDs(ctx, w, ids)
	if err != nil {
		return fmt.Errorf("verifying the ledger: %w", err)
	}
	if _, err := fmt.Fprintf(stdout, "ledger %d acknowledged ids, %d missing\n", len(ids), len(missing)); err != nil {
		return fmt.Errorf("writing the ledger report: %w", err)
	}
	if len(missing) > 0 {
		for _, id := range missing[:min(len(missing), 20)] {
			if _, err := fmt.Fprintf(stdout, "missing %s\n", id); err != nil {
				return fmt.Errorf("writing the ledger report: %w", err)
			}
		}
		return fmt.Errorf("%d of %d acknowledged inserts are missing from the server: acknowledged means durable was broken", len(missing), len(ids))
	}
	return nil
}
