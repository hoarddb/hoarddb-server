package main

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

func TestMarkFileWritesNamedEmptyFiles(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "marks")
	mark := markFile(dir)
	if err := mark(64, 2, markMeasureStart); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "measure-start-v64-r2")); err != nil {
		t.Fatalf("the mark is not there: %v", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 1 {
		t.Fatalf("the directory holds %v, %v: only the published name may remain", entries, err)
	}
}

func TestRunnerMarksTheMeasuredWindowNotTheWarmUp(t *testing.T) {
	p, _ := fakeRun(t, nil)
	p.WarmWindow, p.WarmMin, p.WarmMax, p.WarmTol = 50*time.Millisecond, 100*time.Millisecond, 400*time.Millisecond, 0.5
	var events []string
	var warmWindowsAtStart int
	p.Mark = func(vusers, repetition int, event string) error {
		events = append(events, event)
		return nil
	}
	rec, err := runRepetition(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}
	warmWindowsAtStart = len(rec.WarmWindows)
	if !slices.Equal(events, []string{markMeasureStart, markMeasureEnd}) {
		t.Fatalf("marks %v, want the start then the end of the measured window", events)
	}
	if warmWindowsAtStart < 2 {
		t.Errorf("the warm-up ran %d windows; the mark must come after them", warmWindowsAtStart)
	}
}

func TestParseRunFlagsReadsMarkDir(t *testing.T) {
	cfg, err := parseRunFlags([]string{"-engine", "ring", "-facts", "f.json", "-out", "o", "-reseed-cmd", "true", "-data-dir", "/d", "-mark-dir", "marks"})
	if err != nil || cfg.MarkDir != "marks" {
		t.Fatalf("got %q, %v", cfg.MarkDir, err)
	}
}
