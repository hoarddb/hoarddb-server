package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"time"
)

// The durability witness (spec 3.3). Every engine is given the same
// behavioural test of the one setting the comparison rests on: at depth 1 a
// sequential write cannot be acknowledged much faster than the cheapest flush
// the server's disk offers. The harness measures that flush with -mode
// syncprobe and passes it as -sync-ms; the witness times depth-1 writes in a
// scratch namespace and compares their median with it. A median far below the
// reference means the engine answered before the data was on stable storage,
// and the comparison is invalid (spec 2, integrity failure 3). The O_DSYNC
// figures are printed beside the reference so a reviewer sees them, but they
// do not decide.

// witnessMinRatio is the fraction of the reference a durable depth-1 write must
// reach. It is below 1 because the reference is the cheapest flush the disk
// offers (a data-only fdatasync of a preallocated block), and a durable engine
// also pays for its own bookkeeping around its flush while another may flush
// less than a whole block. 0.3 sits in the measured gap on a quiet VM: durable
// hoardDB at L-A ran at 0.61 to 0.82 of the reference, the unsafe-ack path at
// 0.04 to 0.09.
const witnessMinRatio = 0.3

// syncFigures are the three sync figures of the server's disk, in milliseconds.
// DatasyncMs is the reference; OverwriteMs and AppendMs are the O_DSYNC
// overwrite and append, recorded beside it.
type syncFigures struct {
	DatasyncMs  float64
	OverwriteMs float64
	AppendMs    float64
}

// witnessWarmup writes are made and thrown away before the timed ones, so a
// first-write cost (a file extension, a prepared statement) is not the median.
const witnessWarmup = 10

// witnessDefaultWrites is how many depth-1 writes are timed.
const witnessDefaultWrites = 200

// witnessWriter is an engine that can time depth-1 writes in a namespace of
// its own, which it removes afterwards so the dataset is untouched.
type witnessWriter interface {
	witnessWrites(ctx context.Context, timed int) ([]time.Duration, error)
}

// witnessResult is the one JSON line -mode witness prints. The three figures
// named in spec 9.2's "witness" object keep their names.
type witnessResult struct {
	Mode               string   `json:"mode"`
	Engine             string   `json:"engine"`
	Level              string   `json:"level"`
	Depth              int      `json:"depth"`
	Writes             int      `json:"writes"`
	SingleSyncMsP50    float64  `json:"single_sync_ms_p50"`
	SyncOverwriteMsP50 float64  `json:"sync_overwrite_ms_p50,omitempty"`
	SingleSyncAppendMs float64  `json:"single_sync_append_ms_p50,omitempty"`
	MinRatio           float64  `json:"witness_min_ratio"`
	Depth1WriteP50Ms   float64  `json:"depth1_write_p50_ms"`
	Depth1WriteP99Ms   float64  `json:"depth1_write_p99_ms"`
	P50OverSingleSync  float64  `json:"p50_over_single_sync"`
	P50OverAppendSync  float64  `json:"p50_over_append_sync,omitempty"`
	Pass               bool     `json:"pass"`
	ExpectedRelaxed    bool     `json:"expected_relaxed,omitempty"`
	IntegrityFailures  []string `json:"integrity_failures"`
	DurabilityFindings []string `json:"durability_findings,omitempty"`
}

// milliseconds is d as a fractional number of milliseconds.
func milliseconds(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

// quantileOf is the q-quantile (0 to 1) of durations by the nearest-rank rule,
// from the sorted values themselves: a witness has hundreds of samples, not the
// millions a histogram is for.
func quantileOf(durations []time.Duration, q float64) time.Duration {
	sorted := slices.Clone(durations)
	slices.Sort(sorted)
	rank := int(q*float64(len(sorted))+0.999999) - 1
	return sorted[min(max(rank, 0), len(sorted)-1)]
}

// judgeWitness compares the median depth-1 write with the reference, the
// cheapest data-only flush on the disk (sync.DatasyncMs). It passes at
// witnessMinRatio of the reference or above. The O_DSYNC overwrite and append
// are larger on most file systems because they also commit metadata; they are
// recorded and their ratios printed, and they do not decide. At L-B the engine
// is meant to answer early, so the figures are recorded and nothing fails.
func judgeWitness(engine, level string, durations []time.Duration, sync syncFigures) witnessResult {
	result := witnessResult{
		Mode: modeWitness, Engine: engine, Level: level, Depth: 1, Writes: len(durations),
		SingleSyncMsP50: sync.DatasyncMs, MinRatio: witnessMinRatio, IntegrityFailures: []string{},
		Depth1WriteP50Ms: milliseconds(quantileOf(durations, 0.5)), Depth1WriteP99Ms: milliseconds(quantileOf(durations, 0.99)),
	}
	result.P50OverSingleSync = result.Depth1WriteP50Ms / sync.DatasyncMs
	if sync.OverwriteMs > 0 {
		result.SyncOverwriteMsP50 = sync.OverwriteMs
	}
	if sync.AppendMs > 0 {
		result.SingleSyncAppendMs, result.P50OverAppendSync = sync.AppendMs, result.Depth1WriteP50Ms/sync.AppendMs
	}
	result.Pass = result.Depth1WriteP50Ms >= witnessMinRatio*sync.DatasyncMs
	if level == "L-B" {
		result.ExpectedRelaxed, result.Pass = true, true
		return result
	}
	if !result.Pass {
		result.IntegrityFailures = append(result.IntegrityFailures, fmt.Sprintf(
			"%s acknowledged a depth-1 write in a median of %.3f ms, under %.0f%% of the %.3f ms the cheapest data-only flush takes on this disk (ratio %.2f): it answers before the data is on stable storage, so its figures cannot be compared with an engine that waits. Check its durability settings (hoardDB: unset %s; MongoDB: w:1 with j:true; MySQL: innodb_flush_log_at_trx_commit=1 and sync_binlog=1)",
			engine, result.Depth1WriteP50Ms, witnessMinRatio*100, sync.DatasyncMs, result.P50OverSingleSync, envUnsafeAck))
	}
	return result
}

// runWitness runs -mode witness: it opens the engine and judges it.
func runWitness(ctx context.Context, cfg *config, out io.Writer) error {
	if cfg.SyncMS <= 0 {
		return fmt.Errorf("-mode witness needs -sync-ms, the latency in milliseconds of the cheapest data-only flush (pwrite plus fdatasync to a preallocated block) on the server's disk; measure it with 'social -mode syncprobe -sync-dir <the server's data directory>' and pass sync_datasync_ms_p50, not an O_DSYNC figure; without it there is nothing to compare the writes with")
	}
	eng, err := newEngine(ctx, cfg)
	if err != nil {
		return err
	}
	defer eng.close()
	return judgeEngineWitness(ctx, cfg, eng, out)
}

// judgeEngineWitness times eng's depth-1 writes, applies the launch-file check
// for hoardDB, prints the result and fails on an integrity failure.
func judgeEngineWitness(ctx context.Context, cfg *config, eng engine, out io.Writer) error {
	writer, ok := eng.(witnessWriter)
	if !ok {
		return fmt.Errorf("-engine %s cannot run the durability witness", cfg.Engine)
	}
	durations, err := writer.witnessWrites(ctx, witnessDefaultWrites)
	if err != nil {
		return fmt.Errorf("timing depth-1 writes on %s: %w", cfg.Engine, err)
	}
	result := judgeWitness(cfg.Engine, cfg.Level, durations, syncFigures{DatasyncMs: cfg.SyncMS, OverwriteMs: cfg.SyncOverwriteMS, AppendMs: cfg.SyncAppendMS})
	if failure := eng.durability()[durabilityFailureKey]; failure != "" {
		result.Pass = false
		result.DurabilityFindings = append(result.DurabilityFindings, failure)
		result.IntegrityFailures = append(result.IntegrityFailures, failure)
	}
	line, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("encoding the witness result: %w", err)
	}
	if _, err := fmt.Fprintln(out, string(line)); err != nil {
		return fmt.Errorf("writing the witness result: %w", err)
	}
	if !result.Pass {
		return fmt.Errorf("durability witness failed: %v", result.IntegrityFailures)
	}
	return nil
}
