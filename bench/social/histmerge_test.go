package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// writeHist records the latencies (microseconds) in a histogram and saves it.
func writeHist(t *testing.T, dir, name string, micros []int64) {
	t.Helper()
	hist := newHistogram()
	for _, sample := range micros {
		if err := hist.record(time.Duration(sample) * time.Microsecond); err != nil {
			t.Fatal(err)
		}
	}
	encoded, err := hist.encode()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, name), encoded, 0o644); err != nil {
		t.Fatal(err)
	}
}

// spread returns count samples growing from start by step microseconds.
func spread(count int, start, step int64) []int64 {
	samples := make([]int64, count)
	for i := range samples {
		samples[i] = start + int64(i)*step
	}
	return samples
}

// TestMergeHistGivesThePercentilesOfTheUnion: two clients with different
// latency profiles merge to exactly what one histogram fed both sample sets
// reports, and that differs from either client alone.
func TestMergeHistGivesThePercentilesOfTheUnion(t *testing.T) {
	dir := t.TempDir()
	fast, slow := spread(3000, 200, 3), spread(2000, 5000, 40)
	writeHist(t, dir, "c1-read.hist", fast)
	writeHist(t, dir, "c2-read.hist", slow)

	var output bytes.Buffer
	if err := runMergeHist([]string{filepath.Join(dir, "c1-read.hist"), filepath.Join(dir, "c2-read.hist")}, &output); err != nil {
		t.Fatal(err)
	}
	var got mergedHistRecord
	if err := json.Unmarshal(output.Bytes(), &got); err != nil {
		t.Fatalf("output %q is not one JSON record: %v", output.String(), err)
	}

	union := newHistogram()
	for _, sample := range append(append([]int64{}, fast...), slow...) {
		if err := union.record(time.Duration(sample) * time.Microsecond); err != nil {
			t.Fatal(err)
		}
	}
	want := union.summary()
	if got.Count != 5000 || got.Count != want.Count {
		t.Errorf("count %d, want 5000", got.Count)
	}
	for name, pair := range map[string][2]latencyMs{"p50": {got.P50, want.P50}, "p95": {got.P95, want.P95}, "p99": {got.P99, want.P99}, "p99.9": {got.P999, want.P999}, "max": {got.Max, want.Max}} {
		if pair[0] != pair[1] {
			t.Errorf("%s = %v, want %v of the union", name, pair[0], pair[1])
		}
	}
	firstAlone := newHistogram()
	for _, sample := range fast {
		_ = firstAlone.record(time.Duration(sample) * time.Microsecond)
	}
	if got.P99 == firstAlone.summary().P99 {
		t.Error("the merged p99 equals the first client's alone; the test data cannot tell a merge from client 1's percentiles")
	}
}

// TestMergeHistUnderThresholdIsNotAvailable: under 1,000 samples in all, the percentiles are n.a.
func TestMergeHistUnderThresholdIsNotAvailable(t *testing.T) {
	dir := t.TempDir()
	writeHist(t, dir, "a.hist", spread(400, 100, 5))
	writeHist(t, dir, "b.hist", spread(500, 100, 5))
	var output bytes.Buffer
	if err := runMergeHist([]string{filepath.Join(dir, "a.hist"), filepath.Join(dir, "b.hist")}, &output); err != nil {
		t.Fatal(err)
	}
	var got mergedHistRecord
	if err := json.Unmarshal(output.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.Count != 900 || got.P99.String() != "n.a." {
		t.Errorf("count %d, p99 %s; want 900 samples and n.a.", got.Count, got.P99)
	}
}

func TestMergeHistRefusesAMissingOrForeignFile(t *testing.T) {
	dir := t.TempDir()
	if _, err := mergeHistFiles([]string{filepath.Join(dir, "absent.hist")}); err == nil {
		t.Error("a missing file was accepted")
	}
	foreign := filepath.Join(dir, "foreign.hist")
	if err := os.WriteFile(foreign, []byte("not a histogram"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := mergeHistFiles([]string{foreign}); err == nil {
		t.Error("a file that is not a histogram was accepted")
	}
}

func TestMergeHistModeNeedsFiles(t *testing.T) {
	if _, err := parseFlags([]string{"-mode", "merge-hist"}); err == nil {
		t.Error("-mode merge-hist with no files was accepted")
	}
	cfg, err := parseFlags([]string{"-mode", "merge-hist", "a.hist", "b.hist"})
	if err != nil || len(cfg.Files) != 2 {
		t.Errorf("files %v, err %v; want both files", cfg, err)
	}
}
