package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"slices"
	"strings"
	"syscall"
)

// The -mode values the shell harness calls (HARNESS.md). Each prints one JSON
// line to stdout, or appends it to -out, and exits non-zero when the check it
// stands for fails.
const (
	modeSeed       = "seed"
	modeWarm       = "warm"
	modeRun        = "run"
	modeParityHash = "parity-hash"
	modeConverge   = "converge"
	modeWitness    = "witness"
	modeTLSCheck   = "tlscheck"
	modeSyncProbe  = "syncprobe"
	// modeLedgerCheck reads back the posts a leg wrote with -ledger-out.
	modeLedgerCheck = "ledgercheck"
	// modeWALCheck looks for the same posts in a copy of a node's write-ahead log.
	modeWALCheck = "walcheck"
	// modeMergeHist merges the histograms of a leg's client VMs.
	modeMergeHist = "merge-hist"
)

// harnessModes are the modes runHarnessMode runs, in the order HARNESS.md lists them.
var harnessModes = []string{modeSeed, modeWarm, modeRun, modeParityHash, modeConverge, modeWitness, modeTLSCheck, modeSyncProbe, modeLedgerCheck, modeWALCheck}

// knownModes is every -mode value; the empty one runs a leg.
func knownModes() []string {
	return append(slices.Clone(harnessModes), modeClientCPU, modeSoak, modeDriftReport, modeMergeHist)
}

func isHarnessMode(mode string) bool { return slices.Contains(harnessModes, mode) }

// modeError is the refusal for an unknown -mode.
func modeError(mode string) error {
	return fmt.Errorf("-mode %q is not one of %s (leave it empty to run a leg)", mode, strings.Join(knownModes(), ", "))
}

// runHarnessMode runs one of harnessModes, writing its JSON line to out. The
// modes that work on a dataset that is already seeded attach to it: a seed, a
// warm-up and a leg are separate processes, and only the first creates anything.
func runHarnessMode(ctx context.Context, cfg *config, out io.Writer) error {
	switch cfg.Mode {
	case modeSeed:
		return runSeed(ctx, cfg, out)
	case modeWarm:
		return runWarm(ctx, cfg, out)
	case modeRun:
		cfg.Attach = true
		return executeMix(ctx, cfg, out)
	case modeParityHash:
		return runParityHash(ctx, cfg, out)
	case modeConverge:
		return runConverge(ctx, cfg, out)
	case modeWitness:
		return runWitness(ctx, cfg, out)
	case modeTLSCheck:
		return runTLSCheck(ctx, cfg, out)
	case modeSyncProbe:
		return runSyncProbe(cfg, out)
	case modeLedgerCheck:
		return runLedgerCheck(ctx, cfg, out)
	case modeWALCheck:
		return runWALCheck(ctx, cfg, out)
	default:
		return modeError(cfg.Mode)
	}
}

// withOutput runs body with the writer the result goes to: -out, appended to,
// or stdout.
func withOutput(cfg *config, body func(io.Writer) error) error {
	if cfg.Out == "" {
		return body(os.Stdout)
	}
	file, err := os.OpenFile(cfg.Out, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("opening -out %s: %w", cfg.Out, err)
	}
	err = body(file)
	if closeErr := file.Close(); err == nil && closeErr != nil {
		err = fmt.Errorf("closing %s: %w", cfg.Out, closeErr)
	}
	return err
}

// runHarnessCommand runs a harness mode and exits on failure. SIGINT and SIGTERM
// cancel it, so a stopped harness does not leave a scratch namespace behind.
func runHarnessCommand(cfg *config) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	err := withOutput(cfg, func(out io.Writer) error { return runHarnessMode(ctx, cfg, out) })
	if err != nil {
		fmt.Fprintln(os.Stderr, "social: FAILED:", err)
		stop()
		os.Exit(1)
	}
}
