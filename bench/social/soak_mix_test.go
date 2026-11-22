package main

import (
	"context"
	"testing"
	"time"
)

// TestSoakWindowRunsTheSocialMix: a soak window offers the social mix (the dataset the harness seeds),
// not the older timeline workload, and reports the verified reads and latencies of its window.
func TestSoakWindowRunsTheSocialMix(t *testing.T) {
	cfg := mixTestConfig("rh", "social_soak")
	engine := fakeHoardEngine(cfg)
	if _, err := runMixLeg(context.Background(), cfg, tinySpec, engine); err != nil {
		t.Fatalf("seeding the fake server with a first leg: %v", err)
	}
	attached := *cfg
	attached.Attach = true
	graph := newSocialGraph(tinySpec, datasetSeed)
	runner := mixWindowRunner(&attached, engine, graph, newSocialBook(graph, true))
	capture, err := runner(context.Background(), 0, 400*time.Millisecond)
	if err != nil {
		t.Fatalf("a window of the rh mix on a seeded dataset: %v", err)
	}
	if capture.Verified == 0 {
		t.Errorf("window verified %d reads, want some: the social reads are checked against the bookkeeping", capture.Verified)
	}
	if capture.Read.count() == 0 {
		t.Errorf("window read histogram is empty, want the latencies of the window's reads")
	}
}
