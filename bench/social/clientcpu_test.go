package main

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// costEngine is a stub whose every operation costs a known amount of client
// CPU and whose every open session costs a known amount of memory, so the
// calibration's arithmetic can be checked against exact answers.
type costEngine struct {
	operations   atomic.Int64
	openSessions atomic.Int64
}

func (e *costEngine) setup(context.Context) (schemaInfo, error)  { return schemaInfo{}, nil }
func (e *costEngine) attach(context.Context) (schemaInfo, error) { return schemaInfo{}, nil }
func (e *costEngine) newSession() (engineSession, error) {
	e.openSessions.Add(1)
	return &costSession{engine: e}, nil
}
func (e *costEngine) desynced(error) bool           { return false }
func (e *costEngine) durability() map[string]string { return nil }
func (e *costEngine) target() string                { return "cost-engine" }
func (e *costEngine) close()                        {}

type costSession struct{ engine *costEngine }

func (s *costSession) readTimeline(context.Context, readRequest) (timelinePage, error) {
	s.engine.operations.Add(1)
	time.Sleep(200 * time.Microsecond)
	return timelinePage{}, nil
}
func (s *costSession) writePost(context.Context, string, map[string]interface{}) error { return nil }
func (s *costSession) putProfile(context.Context, string, map[string]interface{}) error {
	return nil
}
func (s *costSession) attachState(context.Context, string) (int64, int64, error) { return 0, 0, nil }
func (s *costSession) close()                                                    { s.engine.openSessions.Add(-1) }

const (
	costPerOperation = 100 * time.Microsecond
	costPerSession   = 1 << 20
	baseResident     = 50 << 20
)

// costProbe charges costPerOperation of CPU for each operation the engine has
// served and costPerSession of memory for each open session.
type costProbe struct{ engine *costEngine }

func (p costProbe) cpuTime() (time.Duration, error) {
	return time.Duration(p.engine.operations.Load()) * costPerOperation, nil
}
func (p costProbe) residentBytes() (int64, error) {
	return baseResident + p.engine.openSessions.Load()*costPerSession, nil
}

func costConfig(workers int) *config {
	return &config{Users: 100, ZipfS: 1.1, Conc: workers, ReadPct: 100, ReadPosts: 20, RandSeed: 1, ClientVMs: 1, Duration: 400 * time.Millisecond}
}

func TestClientCPUBoundIsComputed(t *testing.T) {
	engine := &costEngine{}
	result, err := measureClientCPU(context.Background(), costConfig(8), engine, make([]userState, 100), costProbe{engine}, 4)
	if err != nil {
		t.Fatal(err)
	}
	if result.Operations == 0 {
		t.Fatal("no operation completed")
	}
	if got := result.CPUPerOp; got < 99*time.Microsecond || got > 101*time.Microsecond {
		t.Errorf("CPU per operation = %v, want the stub's 100us", got)
	}
	// 60% of 4 cores is 2.4 CPU-seconds a second; at 100 us an operation that is 24,000 operations a second.
	if want := 24000.0; math.Abs(result.MaxRate-want) > want/100 {
		t.Errorf("max offered rate at 60%% CPU = %.0f, want %.0f", result.MaxRate, want)
	}
	if got := maxOfferedRate(250*time.Microsecond, 2); math.Abs(got-4800) > 0.01 {
		t.Errorf("maxOfferedRate(250us, 2 cores) = %v, want 4800", got)
	}
}

func TestClientMemoryIsReported(t *testing.T) {
	var peaks []int64
	for _, workers := range []int{64, 128} {
		engine := &costEngine{}
		result, err := measureClientCPU(context.Background(), costConfig(workers), engine, make([]userState, 100), costProbe{engine}, 4)
		if err != nil {
			t.Fatal(err)
		}
		if want := int64(baseResident + workers*costPerSession); result.ResidentBytes != want {
			t.Errorf("%d workers: resident %d bytes, want %d", workers, result.ResidentBytes, want)
		}
		peaks = append(peaks, result.ResidentBytes)
		printed := strings.Join(result.lines(), "\n")
		wantMB := (baseResident + int64(workers)*costPerSession) >> 20
		if !strings.Contains(printed, "RECORD\tclient_rss_mb_w"+strconv.Itoa(workers)+"\t"+strconv.Itoa(int(wantMB))) || !strings.Contains(printed, "CHECK PASS client resident memory under 4 GB") {
			t.Errorf("%d workers: output does not report %d MB and the memory check:\n%s", workers, wantMB, printed)
		}
	}
	if peaks[1] <= peaks[0] {
		t.Errorf("128 workers hold %d bytes, 64 hold %d: the memory must grow with the workers", peaks[1], peaks[0])
	}
	over := clientCPUResult{Workers: 64, ResidentBytes: clientMemoryLimit + 1}
	printed := strings.Join(over.lines(), "\n")
	if strings.Contains(printed, "CHECK PASS") || !strings.Contains(printed, "client-memory") {
		t.Errorf("a client over 4 GB must not pass and must carry the client-memory label:\n%s", printed)
	}
}

func TestClientBoundLabelAt85Percent(t *testing.T) {
	cases := []struct {
		fraction float64
		want     bool
	}{{0.60, false}, {0.85, false}, {0.851, true}, {1.0, true}}
	for _, tc := range cases {
		if got := clientBoundLabel(tc.fraction); got != tc.want {
			t.Errorf("clientBoundLabel(%v) = %v, want %v", tc.fraction, got, tc.want)
		}
	}
	bound := clientCPUResult{Workers: 64, CPUFraction: 0.9}
	if printed := strings.Join(bound.lines(), "\n"); !strings.Contains(printed, "RECORD\tclient_label_w64\tclient-bound") {
		t.Errorf("a result at 90%% CPU is not labelled client-bound:\n%s", printed)
	}
	free := clientCPUResult{Workers: 64, CPUFraction: 0.5}
	if printed := strings.Join(free.lines(), "\n"); strings.Contains(printed, "client-bound") {
		t.Errorf("a result at 50%% CPU is labelled client-bound:\n%s", printed)
	}
}

func TestProcProbeReadsStatAndStatus(t *testing.T) {
	dir := t.TempDir()
	// The process name may hold spaces and parentheses; utime and stime are fields 14 and 15 after it.
	stat := "4242 (social (test)) S 1 4242 4242 0 -1 4194304 100 0 0 0 250 50 0 0 20 0 8 0 100 1000000 500 18446744073709551615\n"
	status := "Name:\tsocial\nVmPeak:\t  900000 kB\nVmRSS:\t   2048 kB\nThreads:\t8\n"
	for name, content := range map[string]string{"stat": stat, "status": status} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	probe := procProbe{dir: dir}
	cpu, err := probe.cpuTime()
	if err != nil || cpu != 3*time.Second {
		t.Errorf("cpuTime = %v, %v; want 3s (250 + 50 ticks at 100 a second)", cpu, err)
	}
	rss, err := probe.residentBytes()
	if err != nil || rss != 2048*1024 {
		t.Errorf("residentBytes = %v, %v; want 2 MiB", rss, err)
	}
	if _, err := (procProbe{dir: filepath.Join(dir, "missing")}).cpuTime(); err == nil {
		t.Error("an unreadable stat file was not an error")
	}
}

func TestModeFlagAcceptsOnlyKnownModes(t *testing.T) {
	if cfg, err := parseFlags([]string{"-mode", "clientcpu"}); err != nil || cfg.Mode != modeClientCPU {
		t.Errorf("-mode clientcpu = %+v, %v", cfg, err)
	}
	if _, err := parseFlags([]string{"-mode", "profile"}); err == nil || !strings.Contains(err.Error(), "clientcpu") {
		t.Errorf("-mode profile: %v; want an error that names clientcpu", err)
	}
}

func TestProcProbeReadsThisProcess(t *testing.T) {
	if _, err := os.Stat("/proc/self/stat"); err != nil {
		t.Skip("no /proc on this host")
	}
	probe := procProbe{dir: "/proc/self"}
	if rss, err := probe.residentBytes(); err != nil || rss <= 0 {
		t.Errorf("resident size of this process = %d, %v; want a positive size", rss, err)
	}
	if _, err := probe.cpuTime(); err != nil {
		t.Errorf("CPU time of this process: %v", err)
	}
}
