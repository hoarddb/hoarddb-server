package main

import (
	"os"
	"strings"
	"testing"
)

// TestBenchmarkStoresOpenAtTheMeasuredMemtableSize guards the measurement pin
// this bench depends on: VER-17 and VER-19 recorded their per-bucket address
// space with a 32 MiB memtable, and a re-run only reproduces those figures if
// every store here still opens there. The helper must report 32 MiB, and no
// source file may open a store with an empty tuning, which would let storage's
// refusal (or a future default) pick the size silently.
func TestBenchmarkStoresOpenAtTheMeasuredMemtableSize(t *testing.T) {
	if got := benchTuning().MemTableMB; got != 32 {
		t.Errorf("benchTuning().MemTableMB = %d, want 32: the benches must open at the memtable size VER-17 and VER-19 were measured at", got)
	}

	// The needle is assembled at run time so this file cannot match itself.
	emptyTuning := "BadgerTuning" + "{}"
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("reading the package directory: %v", err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") {
			continue
		}
		source, err := os.ReadFile(entry.Name())
		if err != nil {
			t.Fatalf("reading %s: %v", entry.Name(), err)
		}
		if strings.Contains(string(source), emptyTuning) {
			t.Errorf("%s opens a store with an empty tuning; every OpenBadger/OpenBadgerKV call must pass benchTuning()", entry.Name())
		}
	}
}
