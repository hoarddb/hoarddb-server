//go:build linux

package server

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// platformProbeFailureRows are T-9's rows for the /proc/self/status parser,
// which exists only on Linux: a missing field is a failure, never a zero.
func platformProbeFailureRows() []probeFailureRow {
	statusRow := func(name, status string) probeFailureRow {
		return probeFailureRow{name: name, resource: resourceMemory, inject: func(t *testing.T, p *capacityProbes) {
			statusPath := filepath.Join(t.TempDir(), "status")
			mustWrite(t, statusPath, status)
			p.anonMemory = func() (int64, error) { return readAnonMemory(statusPath) }
		}}
	}
	return []probeFailureRow{
		statusRow("status has no VmSwap line", "RssAnon:\t     300 kB\n"),
		statusRow("status has no RssAnon line", "VmSwap:\t     300 kB\n"),
	}
}

// TestMemoryRatioRisesAsTheNodeSwaps (spec T-14) makes the node
// look full by the route a resident-only numerator cannot see. The limit is
// fixed at 1000 MiB and /proc/self/status is read through the real parser
// from a sequence of fixtures: the ratio must not fall on a pure swap-out,
// and it is not clamped at 1.0.
func TestMemoryRatioRisesAsTheNodeSwaps(t *testing.T) {
	s := newTestServer(t)
	cs, logs := newLoggedSampler(t, s)
	statusPath := filepath.Join(t.TempDir(), "status")
	var ratios []float64
	for _, tt := range []struct {
		name                string
		rssAnonMiB, swapMiB int64
		wantRatio           float64
		wantState           capacityState
	}{
		{name: "(a) resident only", rssAnonMiB: 600, swapMiB: 0, wantRatio: 0.60, wantState: stateOK},
		{name: "(b) pure swap-out", rssAnonMiB: 300, swapMiB: 300, wantRatio: 0.60, wantState: stateOK},
		{name: "(c) growth under pressure", rssAnonMiB: 350, swapMiB: 550, wantRatio: 0.90, wantState: stateRed},
		{name: "(d) past the limit", rssAnonMiB: 400, swapMiB: 800, wantRatio: 1.20, wantState: stateRed},
	} {
		mustWrite(t, statusPath, fmt.Sprintf("Name:\thoarddb\nRssAnon:\t%8d kB\nRssFile:\t  900000 kB\nVmSwap:\t%8d kB\n",
			tt.rssAnonMiB*1024, tt.swapMiB*1024))
		setCapacityProbes(t, s, func(p *capacityProbes) {
			greenProbes(p)
			p.anonMemory = func() (int64, error) { return readAnonMemory(statusPath) }
			p.memoryLimit = func() (memoryLimit, error) {
				return memoryLimit{bytes: 1000 * mib, source: limitCgroupMax, cgroup: "/fixture.slice", hardBytes: 1000 * mib}, nil
			}
		})
		cs.sampleNow()
		row := rowFor(t, cs, resourceMemory)
		if !closeTo(row.ratio, tt.wantRatio) || row.state != tt.wantState {
			t.Fatalf("%s: ratio %v state %v, want %v %v", tt.name, row.ratio, row.state, tt.wantRatio, tt.wantState)
		}
		ratios = append(ratios, row.ratio)
		// The WARN half: (c) is the one crossing, so exactly one memory WARN
		// from (c) on; (d) is the same red episode, within the hour.
		wantWarns := 0
		if tt.wantState == stateRed {
			wantWarns = 1
		}
		if got := logs.count(slog.LevelWarn, "memory on this node is at"); got != wantWarns {
			t.Fatalf("%s: %d memory WARNs so far, want %d", tt.name, got, wantWarns)
		}
	}
	if ratios[1] < ratios[0] || ratios[2] <= ratios[0] || ratios[3] <= 1 {
		t.Fatalf("ratios %v: swapping must not lower the ratio, and past the limit it must exceed 1", ratios)
	}
}

// BenchmarkCapacitySample (spec B-1) records one sample's wall and CPU time.
// It asserts nothing. The fixture run reads 10^4 descriptors (a directory of
// 10^4 entries) and 10^4 mappings (a maps file of 10^4 lines) through the
// real counting functions; the second run reads this process's real /proc.
// CPU is the process's, so it includes whatever else the test binary does.
func BenchmarkCapacitySample(b *testing.B) {
	qs := newCapacityService(b, nil, nil)
	b.Cleanup(func() { _ = qs.Stop() })
	s := qs.core

	fdDir := b.TempDir()
	for i := range 10_000 {
		f, err := os.Create(filepath.Join(fdDir, fmt.Sprintf("fd%05d", i)))
		if err != nil {
			b.Fatalf("create descriptor fixture: %v", err)
		}
		if err := f.Close(); err != nil {
			b.Fatalf("close descriptor fixture: %v", err)
		}
	}
	mapsPath := filepath.Join(b.TempDir(), "maps")
	mapsLine := "7f0000000000-7f0000001000 r--p 00000000 08:01 1234567    /var/lib/hoarddb/000001.sst\n"
	if err := os.WriteFile(mapsPath, []byte(strings.Repeat(mapsLine, 10_000)), 0o600); err != nil {
		b.Fatalf("write maps fixture: %v", err)
	}

	b.Run("fixtures 1e4 descriptors 1e4 mappings", func(b *testing.B) {
		setCapacityProbes(b, s, func(p *capacityProbes) {
			greenProbes(p)
			p.openFDs = func() (int64, int64, error) {
				open, err := countDirEntries(fdDir)
				return open, 65536, err
			}
			p.mappings = func() (int64, int64, error) {
				count, err := countMapsLines(mapsPath)
				return count, 65530, err
			}
		})
		benchmarkSamples(b, newIdleSampler(b, s))
	})
	b.Run("real proc", func(b *testing.B) {
		setCapacityProbes(b, s, func(p *capacityProbes) { *p = *defaultCapacityProbes(s) })
		benchmarkSamples(b, newIdleSampler(b, s))
	})
}

func benchmarkSamples(b *testing.B, cs *capacitySampler) {
	b.Helper()
	cpuBefore := processCPU(b)
	b.ResetTimer()
	for range b.N {
		cs.sampleNow()
	}
	b.StopTimer()
	cpu := processCPU(b) - cpuBefore
	b.ReportMetric(float64(cpu.Nanoseconds())/float64(b.N), "cpu-ns/op")
}

func processCPU(b *testing.B) time.Duration {
	b.Helper()
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		b.Fatalf("getrusage: %v", err)
	}
	return time.Duration(usage.Utime.Nano() + usage.Stime.Nano())
}

// TestAnonMemoryIsRssAnonPlusVmSwap (build doc BT-3) proves the memory
// numerator is resident plus swapped anonymous memory, and that a missing
// field is an error rather than a zero that reads healthy.
func TestAnonMemoryIsRssAnonPlusVmSwap(t *testing.T) {
	const statusHead = "Name:\thoarddb\nVmRSS:\t  900 kB\n"
	for _, tt := range []struct {
		name    string
		status  string
		want    int64
		wantErr string
	}{
		{name: "resident plus swapped", status: statusHead + "RssAnon:\t     300 kB\nRssFile:\t  600 kB\nVmSwap:\t     300 kB\n", want: 600 * 1024},
		{name: "no VmSwap line", status: statusHead + "RssAnon:\t     300 kB\n", wantErr: "no VmSwap line"},
		{name: "no RssAnon line", status: statusHead + "VmSwap:\t     300 kB\n", wantErr: "no RssAnon line"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			statusPath := filepath.Join(t.TempDir(), "status")
			mustWrite(t, statusPath, tt.status)
			got, err := readAnonMemory(statusPath)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("readAnonMemory = %d, %v; want an error containing %q", got, err, tt.wantErr)
				}
				return
			}
			if err != nil || got != tt.want {
				t.Fatalf("readAnonMemory = %d, %v; want %d", got, err, tt.want)
			}
		})
	}
}

// TestHostProbesReadThisProcess is a smoke check that each real Linux probe
// reads a plausible value from this process: nothing here is injected.
func TestHostProbesReadThisProcess(t *testing.T) {
	open, fdLimit, err := readOpenFDs()
	if err != nil || open <= 0 || fdLimit <= 0 {
		t.Fatalf("readOpenFDs = %d, %d, %v", open, fdLimit, err)
	}
	maps, mapLimit, err := readMappings()
	if err != nil || maps <= 0 || mapLimit <= 0 {
		t.Fatalf("readMappings = %d, %d, %v", maps, mapLimit, err)
	}
	anon, err := hostAnonMemory()
	if err != nil || anon <= 0 {
		t.Fatalf("hostAnonMemory = %d, %v", anon, err)
	}
	// The walk must resolve on the real kernel, not only on fixtures: a
	// failure here on an ordinary host would make memory unavailable for
	// every operator on it (node-capacity-build.md §8 S-5).
	limit, err := hostMemoryLimit()
	if err != nil || limit.bytes <= 0 {
		t.Fatalf("hostMemoryLimit = %+v, %v", limit, err)
	}
	t.Logf("this host's memory limit: %d bytes, source %d, cgroup %q, hard %d", limit.bytes, limit.source, limit.cgroup, limit.hardBytes)
}
