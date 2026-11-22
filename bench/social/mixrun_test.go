package main

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

// fakeHoardEngine is a hoardDB engine whose connections reach a fresh in-memory server.
func fakeHoardEngine(cfg *config) *hoardEngine { return fakeHoardEngineOn(cfg, newFakeServer()) }

// fakeHoardEngineOn is fakeHoardEngine on a server the test already holds.
func fakeHoardEngineOn(cfg *config, fake *fakeServer) *hoardEngine {
	return &hoardEngine{cfg: cfg, dial: func(string) (wireCaller, func(), error) { return fake.dial(nil, nil), func() {}, nil }}
}

func mixTestConfig(mix string, database string) *config {
	return &config{
		Engine: engineHoardDB, Database: database, Users: tinySpec.Users, Mix: mix, Dataset: "tiny", Conc: 4,
		Duration: 400 * time.Millisecond, Warmup: 50 * time.Millisecond, OpTimeout: 5 * time.Second,
		ZipfS: 1.1, RandSeed: 3, VerifyPct: 100, ClientVMs: 1, Repeat: 2, WarmConverged: true,
	}
}

// completedMixRecord runs a short leg of mix against the engine and returns the
// record read back from the file it was written to.
func completedMixRecord(t *testing.T, eng socialEngine, cfg *config) *legRecord {
	t.Helper()
	record, err := runMixLeg(context.Background(), cfg, tinySpec, eng)
	if err != nil {
		t.Fatalf("running the %s leg: %v", cfg.Mix, err)
	}
	path := filepath.Join(t.TempDir(), "leg.json")
	file, err := os.Create(path)
	if err != nil {
		t.Fatalf("creating the record file: %v", err)
	}
	if err := writeLegRecord(file, record); err != nil {
		t.Fatalf("writing the leg record: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("closing the record file: %v", err)
	}
	records, err := readLegRecords(path)
	if err != nil || len(records) != 1 {
		t.Fatalf("readLegRecords = %d records, %v; want the one record written", len(records), err)
	}
	return records[0]
}

// TestMixRunWritesLegRecord: a completed -mix run emits a perf-record/2 record
// the summariser's reader accepts, and the record says what ran.
func TestMixRunWritesLegRecord(t *testing.T) {
	engine := fakeHoardEngine(mixTestConfig("rh", "social_mix"))
	record := completedMixRecord(t, engine, engine.cfg)
	if record.Schema != recordSchema || record.Engine != engineHoardDB || record.Mix != "RH" || record.Dataset != "tiny" {
		t.Errorf("record identifies itself as schema %q engine %q mix %q dataset %q", record.Schema, record.Engine, record.Mix, record.Dataset)
	}
	if record.Loop != "closed" || record.Depth != 4 || record.Placement != "single" || record.Repeat != 2 {
		t.Errorf("record loop=%q depth=%d placement=%q repeat=%d; want closed, 4, single, 2", record.Loop, record.Depth, record.Placement, record.Repeat)
	}
	if record.OpsPerSec <= 0 || record.ReadsPerSec <= 0 || record.Latency.All.Count == 0 {
		t.Errorf("record carries no throughput: ops/s %v reads/s %v samples %d", record.OpsPerSec, record.ReadsPerSec, record.Latency.All.Count)
	}
	if len(record.Ops) == 0 {
		t.Fatal("record lists no per-operation figures")
	}
	for operation, figures := range record.Ops {
		if figures.Count == 0 || figures.ReqPerOp < 1 {
			t.Errorf("operation %s: count %d, %v requests per operation; want a count and at least one request", operation, figures.Count, figures.ReqPerOp)
		}
	}
	if record.Ops["R1"].ReqPerOp != 1 {
		t.Errorf("R1 took %v requests per operation, want exactly 1", record.Ops["R1"].ReqPerOp)
	}
	if len(record.ErrorsByType) != 0 {
		t.Errorf("a clean run recorded errors: %v", record.ErrorsByType)
	}
}

// TestMixRunPinnedOperation: -mix op:<ID> runs that one operation.
func TestMixRunPinnedOperation(t *testing.T) {
	engine := fakeHoardEngine(mixTestConfig("op:R3", "social_op"))
	record := completedMixRecord(t, engine, engine.cfg)
	if len(record.Ops) != 1 || record.Ops["R3"].Count == 0 || record.Mix != "op:R3" {
		t.Errorf("a pinned op:R3 run recorded mix %q and operations %v; want only R3", record.Mix, record.Ops)
	}
}

// TestMixRunOpenLoopRecordsOfferedRate: an open-loop leg says so and records the rate.
func TestMixRunOpenLoopRecordsOfferedRate(t *testing.T) {
	cfg := mixTestConfig("wh", "social_open")
	cfg.Rate = 500
	engine := fakeHoardEngine(cfg)
	record := completedMixRecord(t, engine, cfg)
	if record.Loop != "open" || record.OfferedRate == nil || *record.OfferedRate != 500 {
		t.Errorf("record loop=%q offered=%v; want open at 500", record.Loop, record.OfferedRate)
	}
}

// TestMixFlagIsAccepted: -mix is accepted for every engine that implements the
// social operations.
func TestMixFlagIsAccepted(t *testing.T) {
	cfg, err := parseFlags([]string{"-mix", "rh"})
	if err != nil || cfg.Dataset != "S-10k" {
		t.Fatalf("-mix rh: dataset %q, %v; want it accepted with the default S-10k", cfg.Dataset, err)
	}
	if _, err := parseFlags([]string{"-mix", "wh", "-dataset", "S-2"}); err == nil || !strings.Contains(err.Error(), "S-10k") {
		t.Errorf("-dataset S-2: %v; want a refusal naming the datasets", err)
	}
	for _, engineName := range []string{"mongo", "mysql"} {
		if _, err := parseFlags([]string{"-mix", "rh", "-engine", engineName}); err != nil {
			t.Errorf("-mix with -engine %s: %v; want it accepted", engineName, err)
		}
	}
}

// TestMixRunLive runs a short leg against a local server and checks that the
// record it writes is accepted.
func TestMixRunLive(t *testing.T) {
	world := newLiveWorldUnseeded(t)
	cfg := mixTestConfig("sk", world.engine.cfg.Database)
	cfg.Addr, cfg.Password, cfg.OpTimeout = world.engine.cfg.Addr, world.engine.cfg.Password, 30*time.Second
	record := completedMixRecord(t, newHoardEngine(cfg), cfg)
	if record.OpsPerSec <= 0 || len(record.ErrorsByType) != 0 {
		t.Errorf("live SK leg: %v ops/s, errors %v", record.OpsPerSec, record.ErrorsByType)
	}
}

// TestMixRunVerifiesReads: sampled reads are checked against the bookkeeping,
// every one of them here, and a clean run finds nothing wrong.
func TestMixRunVerifiesReads(t *testing.T) {
	record := completedMixRecord(t, fakeHoardEngine(mixTestConfig("rh", "social_verify")), mixTestConfig("rh", "social_verify"))
	if record.Verified == 0 || record.Misses != 0 {
		t.Errorf("verified %d reads with %d misses; want some verified and no misses", record.Verified, record.Misses)
	}
}

// TestMixRunVerifiesUnderConcurrency is the check against false alarms: many
// workers posting, following, liking and reading the same small dataset, with
// every read verified, must not fail on anything the bounds allow.
func TestMixRunVerifiesUnderConcurrency(t *testing.T) {
	cfg := mixTestConfig("wh", "social_busy")
	cfg.Conc, cfg.Duration = 8, 800*time.Millisecond
	record := completedMixRecord(t, fakeHoardEngine(cfg), cfg)
	if record.Verified < 50 || record.Ops["W1"].Count == 0 {
		t.Errorf("only %d reads verified and %d posts written; the run did not exercise the checks", record.Verified, record.Ops["W1"].Count)
	}
}

// TestMixRunFailsOnWrongRead: a server that answers a feed oldest first fails
// the run, and the record carries the evidence.
func TestMixRunFailsOnWrongRead(t *testing.T) {
	fake := newFakeServer()
	fake.reverseFeedPeeks = true
	cfg := mixTestConfig("op:R1", "social_wrong")
	record, err := runMixLeg(context.Background(), cfg, tinySpec, fakeHoardEngineOn(cfg, fake))
	if err == nil || !strings.Contains(err.Error(), "verification failed") || !strings.Contains(err.Error(), "not newest-first") {
		t.Fatalf("a run against a server that reverses feeds: error = %v, want a verification failure naming the order", err)
	}
	if record == nil || !slices.ContainsFunc(record.Notes, func(note string) bool { return strings.HasPrefix(note, "verification failed") }) {
		t.Errorf("the record did not keep the failure: %+v", record)
	}
}

// TestMixRunOnAttachedDataset: a second leg on the dataset the first left
// behind (more posts, follows, likes than the generator says) still verifies.
func TestMixRunOnAttachedDataset(t *testing.T) {
	fake := newFakeServer()
	first := mixTestConfig("wh", "social_attach")
	completedMixRecord(t, fakeHoardEngineOn(first, fake), first)
	second := mixTestConfig("wh", "social_attach")
	second.Attach = true
	record := completedMixRecord(t, fakeHoardEngineOn(second, fake), second)
	if record.Verified == 0 || record.Ops["W1"].Count == 0 {
		t.Errorf("attached leg verified %d reads and wrote %d posts", record.Verified, record.Ops["W1"].Count)
	}
}

func TestMixVerifiesOnlyWhatItChecks(t *testing.T) {
	for mix, want := range map[string]bool{"rh": true, "wh": true, "sk": true, "op:R1": true, "op:R5": true, "op:R2": false, "op:W1": false, "op:Q1": false} {
		if got := mixVerifies(mix); got != want {
			t.Errorf("mixVerifies(%q) = %v, want %v", mix, got, want)
		}
	}
}

// TestMixRunRecordsPublishFanout: W1 sends two requests, plus one per follower,
// plus one to resume the author's post numbering the first time the run
// publishes as that author. The record carries the last two as means
// (ops.W1.fanout_mean, ops.W1.resume_requests_mean) so a gate can judge the
// part that is neither; no other operation has the fields.
func TestMixRunRecordsPublishFanout(t *testing.T) {
	engine := fakeHoardEngine(mixTestConfig("op:W1", "social_fanout"))
	record := completedMixRecord(t, engine, engine.cfg)
	w1 := record.Ops["W1"]
	if w1.Count == 0 {
		t.Fatal("the pinned W1 leg ran no publishes")
	}
	if w1.FanoutMean == nil || w1.ResumeRequestsMean == nil {
		t.Fatalf("ops.W1 fanout_mean %v, resume_requests_mean %v; want both", w1.FanoutMean, w1.ResumeRequestsMean)
	}
	if *w1.ResumeRequestsMean < 0 || *w1.ResumeRequestsMean > 1 {
		t.Errorf("resume_requests_mean %v, want a mean of 0 or 1 request per publish", *w1.ResumeRequestsMean)
	}
	if got, want := w1.ReqPerOp-*w1.FanoutMean-*w1.ResumeRequestsMean, 2.0; got < want-1e-9 || got > want+1e-9 {
		t.Errorf("W1 req_per_op %v minus fanout_mean %v minus resume_requests_mean %v = %v, want exactly %v (the post and the follower page)", w1.ReqPerOp, *w1.FanoutMean, *w1.ResumeRequestsMean, got, want)
	}

	mixed := fakeHoardEngine(mixTestConfig("wh", "social_fanout_wh"))
	for operation, figures := range completedMixRecord(t, mixed, mixed.cfg).Ops {
		if operation != "W1" && (figures.FanoutMean != nil || figures.ResumeRequestsMean != nil) {
			t.Errorf("operation %s carries publish figures; only W1 publishes", operation)
		}
	}
}
