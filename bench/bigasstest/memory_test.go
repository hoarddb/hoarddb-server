package main

import (
	"os"
	"runtime"
	"testing"
	"time"
)

// TestWorldMemoryAtScale300 measures what the load client holds: the model of all four layouts
// at the starting scale. It is off by default because it takes seconds and about 6 GB;
// BIGASSTEST_MEASURE=1 turns it on and the figures are logged.
func TestWorldMemoryAtScale300(t *testing.T) {
	if os.Getenv("BIGASSTEST_MEASURE") == "" {
		t.Skip("set BIGASSTEST_MEASURE=1 to measure the client's memory at scale 300")
	}
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	started := time.Now()
	w, err := newWorld(1, 300)
	if err != nil {
		t.Fatal(err)
	}
	took := time.Since(started)
	runtime.GC()
	runtime.ReadMemStats(&after)
	var seeded int64
	for _, ds := range w.sets {
		for _, count := range ds.counts {
			seeded += count
		}
	}
	t.Logf("scale 300: %d seeded documents and elements, built in %v, %d MB of live heap", seeded, took.Round(time.Millisecond), (after.HeapAlloc-before.HeapAlloc)>>20)
	runtime.KeepAlive(w)
}
