// bench/social — a load generator for hoardDB's target workload: a social
// network whose users own their stores (AGENTS.md rule 11), run identically
// against hoardDB, MongoDB and MySQL so the three can be compared.
//
// Two workloads live here. The certification workload (-mix rh, wh, sk or
// op:<ID>) is the one the performance certification measures: users with
// posts, a follow graph, feeds, likes, comments, notifications, avatars and a
// job queue, sixteen operations (R1-R7, N1, W1-W6, M1, Q1) in exact ratios,
// on a seeded dataset (S-10k, S-100k, S-1M), verified against the writers'
// bookkeeping and hashed in a canonical form so the three engines can be shown
// to hold the same data. hoardDB keeps each user's posts, feed, edges, inbox
// and avatar as store fields of the user's own document; MongoDB and MySQL use
// the collections and tables a competent developer would. One JSON leg record
// (perf-record/2, REPORT.md) comes out per measured leg.
//
// The older timeline workload (no -mix) is kept as the density probe it was:
// database "social" with one hash bucket "users" (a ~300 B profile per user)
// and one LIFO bucket per user, tl_<userid>, holding that user's posts
// (0.4-2 KB each). A read fetches the newest 20 posts of one timeline plus the
// profile; a write pushes one post. It models no fan-out, follows, deletes or
// more than one node; a LIFO bucket has no keys, so a post's key (author:seq)
// lives only in its document, and the scan that fetches the newest N also
// counts the whole bucket (ScanResponse.Total), so read cost grows with
// timeline length.
//
// Correctness comes first: at least 1% of reads are checked against the
// writers' own bookkeeping (right author, newest first, a count consistent
// with what was written) and one wrong read fails the run with exit status 1.
// Checking happens after the operation's clock stops.
//
// Run:
//
//	HOARDDB_TCP_LISTEN=127.0.0.1:17433 HOARDDB_DATA_DIR=/tmp/x ./server &
//	go run ./bench/social -addr 127.0.0.1:17433 -users 10000 -depth 64
//	go run ./bench/social -addr 127.0.0.1:17433 -attach -depth 128   # a ramp step
//
// stdout carries one JSON line per run; the human summary goes to stderr.
// Like bench/smokeperf it is a measuring instrument, not product code, and it
// adds only the HdrHistogram module (MIT) for latency quantiles.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// report is the JSON line a run prints.
type report struct {
	Config        *config           `json:"config"`
	Engine        string            `json:"engine"`
	Target        string            `json:"target"`
	Schema        string            `json:"schema"`
	SchemaDetail  []string          `json:"schema_detail"`
	Durability    map[string]string `json:"durability,omitempty"`
	DurationS     float64           `json:"duration_s"`
	WarmupS       float64           `json:"warmup_s"`
	OpsPerSec     float64           `json:"ops_per_sec"`
	ReadsPerSec   float64           `json:"reads_per_sec"`
	WritesPerSec  float64           `json:"writes_per_sec"`
	ReadLatency   latencySummary    `json:"read_latency_ms"`
	WriteLatency  latencySummary    `json:"write_latency_ms"`
	Errors        map[string]int64  `json:"errors_by_type"`
	BucketCount   int               `json:"bucket_count"`
	SetupSeconds  float64           `json:"setup_s"`
	BucketsPerSec float64           `json:"buckets_created_per_sec"`
	SeedSeconds   float64           `json:"seed_s"`
	Attached      bool              `json:"attached"`
	VerifiedReads int64             `json:"verified_reads"`
	VerifyFailure string            `json:"verify_failure,omitempty"`
}

type latencySummary struct {
	Count int64     `json:"count"`
	P50   latencyMs `json:"p50"`
	P95   latencyMs `json:"p95"`
	P99   latencyMs `json:"p99"`
	P999  latencyMs `json:"p999"`
	Max   latencyMs `json:"max"`
}

func main() {
	cfg, err := parseFlags(os.Args[1:])
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			os.Exit(0)
		}
		fmt.Fprintln(os.Stderr, "social:", err)
		os.Exit(2)
	}
	if cfg.Summarize {
		if err := runSummarize(cfg.Files, cfg.CSV, os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, "social:", err)
			os.Exit(1)
		}
		return
	}
	if cfg.Mode == modeMergeHist {
		if err := withOutput(cfg, func(out io.Writer) error { return runMergeHist(cfg.Files, out) }); err != nil {
			fmt.Fprintln(os.Stderr, "social: FAILED:", err)
			os.Exit(1)
		}
		return
	}
	logins = newLoginGate(cfg.LoginConcurrency, loginAttempts, loginFirstBackoff)
	if isHarnessMode(cfg.Mode) {
		runHarnessCommand(cfg)
		return
	}
	// A soak is told -mix like a leg is, so it is dispatched before the leg runner.
	if cfg.Mode == modeSoak || cfg.Mode == modeDriftReport {
		runSoakCommand(cfg)
		return
	}
	if cfg.Mix != "" {
		runMixCommand(cfg)
		return
	}
	if cfg.Mode == modeClientCPU {
		if err := runClientCPU(context.Background(), cfg, os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, "social: FAILED:", err)
			os.Exit(1)
		}
		return
	}
	out, err := execute(context.Background(), cfg)
	if out != nil {
		line, marshalErr := json.Marshal(out)
		if marshalErr != nil {
			fmt.Fprintln(os.Stderr, "social: encoding report:", marshalErr)
			os.Exit(1)
		}
		fmt.Println(string(line))
		fmt.Fprintln(os.Stderr, humanSummary(out))
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "social: FAILED:", err)
		os.Exit(1)
	}
}

// execute performs setup (or attach), seeding, the warmup and the measured
// run. A non-nil report comes back with a non-nil error when verification
// failed, so the evidence is printed either way.
func execute(ctx context.Context, cfg *config) (*report, error) {
	out := &report{Config: cfg, Engine: cfg.Engine, Errors: map[string]int64{}, Attached: cfg.Attach, WarmupS: cfg.Warmup.Seconds()}
	eng, err := newEngine(ctx, cfg)
	if err != nil {
		return nil, err
	}
	defer eng.close()
	out.Target = eng.target()
	out.Durability = eng.durability()
	states := make([]userState, cfg.Users)
	var schema schemaInfo
	if cfg.Attach {
		if schema, err = eng.attach(ctx); err != nil {
			return nil, err
		}
		if err := attachData(ctx, cfg, eng, states); err != nil {
			return nil, err
		}
	} else {
		setupStart := time.Now()
		if schema, err = eng.setup(ctx); err != nil {
			return nil, err
		}
		setup := time.Since(setupStart)
		out.SetupSeconds = setup.Seconds()
		if schema.PerBucketCreation {
			out.BucketsPerSec = float64(schema.Buckets) / setup.Seconds()
		}
		seedStart := time.Now()
		if err := seedData(ctx, cfg, eng, states); err != nil {
			return nil, err
		}
		out.SeedSeconds = time.Since(seedStart).Seconds()
	}
	out.Schema, out.SchemaDetail, out.BucketCount = schema.Layout, schema.Detail, schema.Buckets

	begin := time.Now()
	measureFrom := begin.Add(cfg.Warmup)
	stopAt := measureFrom.Add(cfg.Duration)
	results := runWorkers(ctx, cfg, eng, states, measureFrom, stopAt)
	measured := min(max(time.Since(measureFrom), 0), cfg.Duration)

	readLatencies, writeLatencies := newHistogram(), newHistogram()
	var failure, recordFailure error
	for _, result := range results {
		readLatencies.merge(result.readLatencies)
		writeLatencies.merge(result.writeLatencies)
		out.VerifiedReads += result.verified
		for kind, count := range result.errors {
			out.Errors[kind] += count
		}
		if failure == nil {
			failure = result.failure
		}
		if recordFailure == nil {
			recordFailure = result.recordFailure
		}
	}
	out.DurationS = measured.Seconds()
	if measured > 0 { // a run that failed during warmup has no measured window
		out.ReadsPerSec = float64(readLatencies.count()) / measured.Seconds()
		out.WritesPerSec = float64(writeLatencies.count()) / measured.Seconds()
		out.OpsPerSec = out.ReadsPerSec + out.WritesPerSec
	}
	out.ReadLatency = readLatencies.summary()
	out.WriteLatency = writeLatencies.summary()
	if err := writeHistFiles(cfg.HistPrefix, readLatencies, writeLatencies); err != nil {
		return out, err
	}
	if recordFailure != nil {
		return out, histogramRangeError(recordFailure)
	}
	if failure != nil {
		out.VerifyFailure = failure.Error()
		return out, fmt.Errorf("verification failed: %w", failure)
	}
	if out.VerifiedReads == 0 {
		return out, errors.New("no read was verified: the run was too short or read-pct too low to prove anything")
	}
	return out, nil
}

func humanSummary(r *report) string {
	var errorTotal int64
	for _, count := range r.Errors {
		errorTotal += count
	}
	return fmt.Sprintf("social: engine=%s users=%d conc=%d read-pct=%d | %.0f ops/s (read %.0f, write %.0f) | read p50/p99/p99.9 %s/%s/%s ms | write p50/p99/p99.9 %s/%s/%s ms | errors=%d verified=%d | setup %.1fs (%.0f buckets/s) seed %.1fs",
		r.Engine, r.Config.Users, r.Config.Conc, r.Config.ReadPct, r.OpsPerSec, r.ReadsPerSec, r.WritesPerSec,
		r.ReadLatency.P50, r.ReadLatency.P99, r.ReadLatency.P999, r.WriteLatency.P50, r.WriteLatency.P99, r.WriteLatency.P999,
		errorTotal, r.VerifiedReads, r.SetupSeconds, r.BucketsPerSec, r.SeedSeconds)
}

// writeHistFiles saves both histograms next to the report so a later run can
// merge or re-quantile them. An empty prefix writes nothing.
func writeHistFiles(prefix string, reads, writes *histogram) error {
	if prefix == "" {
		return nil
	}
	for suffix, hist := range map[string]*histogram{"-read": reads, "-write": writes} {
		encoded, err := hist.encode()
		if err != nil {
			return fmt.Errorf("saving %s histogram: %w", suffix[1:], err)
		}
		name := histFileName(prefix + suffix)
		if err := os.WriteFile(name, encoded, 0o644); err != nil {
			return fmt.Errorf("writing %s: %w", name, err)
		}
	}
	return nil
}

// runSoakCommand runs -mode soak or -mode driftreport and exits on failure.
// SIGINT and SIGTERM stop a soak cleanly between windows' files, so a restart
// carries on from them.
func runSoakCommand(cfg *config) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var err error
	if cfg.Mode == modeDriftReport {
		err = runDriftReport(cfg.Soak.Dir, os.Stdout)
	} else {
		err = runSoakMode(ctx, cfg, os.Stdout)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "social: FAILED:", err)
		stop()
		os.Exit(1)
	}
}

// histogramRangeError explains a latency the histogram could not hold. There are two
// ways to get one and the fix differs: in an open loop the latency includes the time
// an arrival waited for a free worker, so a server that stalls for a minute pushes
// queued operations over the limit even when no single request lasted that long.
func histogramRangeError(recordFailure error) error {
	return fmt.Errorf("a latency passed the 60 s the latency histogram holds: either one request lasted that long (lower -op-timeout below 60s), or, with -rate, arrivals queued behind a stalled server for that long and queueing delay counts toward latency (lower -rate or find the stall): %w", recordFailure)
}

// runMixCommand runs a -mix leg and exits on failure. The record goes to -out
// when given, else to stdout.
func runMixCommand(cfg *config) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	err := withOutput(cfg, func(out io.Writer) error { return executeMix(ctx, cfg, out) })
	if err != nil {
		fmt.Fprintln(os.Stderr, "social: FAILED:", err)
		stop()
		os.Exit(1)
	}
}
