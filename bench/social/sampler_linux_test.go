//go:build linux

package main

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func samplerFor(t *testing.T, metricsAddr string) procServerSampler {
	t.Helper()
	pidFile := filepath.Join(t.TempDir(), "server.pid")
	if err := os.WriteFile(pidFile, []byte(strconv.Itoa(os.Getpid())), 0o600); err != nil {
		t.Fatal(err)
	}
	return procServerSampler{pidFile: pidFile, dataDir: t.TempDir(), metricsAddr: metricsAddr}
}

func TestSamplerGoroutineDriftFromRisingSeries(t *testing.T) {
	sampler := samplerFor(t, serverAddr(metricsServer(t, 30, 5, true)))
	var samples []soakSample
	for range 3 {
		reading, err := sampler.sample(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		reading.Window = len(samples)
		samples = append(samples, reading)
	}
	if samples[0].Goroutines != 30 || samples[2].Goroutines != 40 {
		t.Fatalf("goroutines read as %d, %d, %d; want 30, 35, 40", samples[0].Goroutines, samples[1].Goroutines, samples[2].Goroutines)
	}
	if drift := computeDrift(nil, samples, soakWindowDefault); !nearly(drift.GoroutineDrift, 10, 0.001) || !nearly(drift.GoroutineDriftPct, 33.33, 0.01) {
		t.Errorf("goroutine drift = %v (%v%%), want 10 (33.33%%)", deref(drift.GoroutineDrift), deref(drift.GoroutineDriftPct))
	}
}

func TestSamplerGoroutinesNotAvailableSaysWhy(t *testing.T) {
	closed := metricsServer(t, 1, 0, true)
	closedAddr := serverAddr(closed)
	closed.Close()
	cases := map[string]struct {
		addr     string
		wantNote string
	}{
		"no address":     {"", "-metrics-addr"},
		"scrape fails":   {closedAddr, "scraping"},
		"metric missing": {serverAddr(metricsServer(t, 0, 0, false)), "go_goroutines"},
	}
	for name, tc := range cases {
		reading, err := samplerFor(t, tc.addr).sample(context.Background())
		if err != nil {
			t.Fatalf("%s: a goroutine count that is not available must not stop the soak: %v", name, err)
		}
		if reading.Goroutines != -1 || !strings.Contains(reading.GoroutinesNote, tc.wantNote) {
			t.Errorf("%s: goroutines=%d note=%q; want -1 and a note containing %q", name, reading.Goroutines, reading.GoroutinesNote, tc.wantNote)
		}
		if drift := computeDrift(nil, []soakSample{reading, reading}, soakWindowDefault); drift.GoroutineDrift != nil {
			t.Errorf("%s: a drift was computed from an unavailable count", name)
		}
	}
}

func TestSamplerScrapesWithACancelledWindowContext(t *testing.T) {
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	reading, err := samplerFor(t, serverAddr(metricsServer(t, 30, 0, true))).sample(cancelled)
	if err != nil || reading.Goroutines != 30 {
		t.Errorf("sample with a cancelled context = %d goroutines, note %q, err %v; the boundary sample at the end of a window must still count", reading.Goroutines, reading.GoroutinesNote, err)
	}
}
