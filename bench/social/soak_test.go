package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func nearly(got *float64, want, tolerance float64) bool {
	return got != nil && math.Abs(*got-want) <= tolerance
}

func TestDriftComputation(t *testing.T) {
	const window = 5 * time.Minute
	// Six hours of 5-minute windows: 1000 ops/s and a 10 ms p99 in the first hour, 900 ops/s and 12 ms in the last.
	windows := make([]windowSummary, 72)
	for index := range windows {
		windows[index] = windowSummary{Index: index, LengthS: 300, OpsPerSec: 950, ReadP99: 11, WriteP99: 20}
		switch {
		case index < 12:
			windows[index].OpsPerSec, windows[index].ReadP99 = 1000, 10
		case index >= 60:
			windows[index].OpsPerSec, windows[index].ReadP99 = 900, 12
		}
	}
	// A sample a minute. The resident size spikes in the first hour (warm-up) and then grows 10 MB an hour,
	// so a slope over the whole run would be wrong and one over the last five hours is exactly 10.
	var samples []soakSample
	for minute := 0; minute <= 360; minute++ {
		rssMB := 5000.0
		if minute >= 60 {
			rssMB = 2000 + 10*float64(minute)/60
		}
		samples = append(samples, soakSample{UnixSec: int64(minute) * 60, Window: minute / 5, ServerPID: 7, RSSBytes: int64(rssMB * (1 << 20)), Goroutines: 50, FDs: 100 + minute/36, Threads: 20})
	}
	drift := computeDrift(windows, samples, window)
	checks := []struct {
		name string
		got  *float64
		want float64
	}{
		{"first hour ops/s", drift.FirstHourOpsPerSec, 1000},
		{"last hour ops/s", drift.LastHourOpsPerSec, 900},
		{"throughput drift %", drift.ThroughputDriftPct, -10},
		{"read p99 drift %", drift.ReadP99DriftPct, 20},
		{"write p99 drift %", drift.WriteP99DriftPct, 0},
		{"RSS slope MB/h", drift.RSSSlopeMBPerHour, 10},
		{"goroutine drift", drift.GoroutineDrift, 0},
		{"descriptor drift", drift.DescriptorDrift, 10},
		{"descriptor drift %", drift.DescriptorDriftPct, 10},
	}
	for _, check := range checks {
		if !nearly(check.got, check.want, 0.01) {
			t.Errorf("%s = %v, want %v", check.name, deref(check.got), check.want)
		}
	}
	if drift.Windows != 72 {
		t.Errorf("windows = %d, want 72", drift.Windows)
	}
	if short := computeDrift(windows[:1], samples[:1], window); short.ThroughputDriftPct != nil || short.RSSSlopeMBPerHour != nil {
		t.Errorf("one window and one sample must give no drift, got %+v", short)
	}
}

func deref(value *float64) interface{} {
	if value == nil {
		return "n.a."
	}
	return *value
}

// scriptedRunner records which windows it ran and returns 1,000 reads and 200 writes for each.
type scriptedRunner struct {
	started atomic.Int64
	ran     []int
	failAt  int
}

func (r *scriptedRunner) run(_ context.Context, index int, _ time.Duration) (windowCapture, error) {
	r.started.Add(1)
	if index == r.failAt {
		return windowCapture{}, errors.New("the load client crashed")
	}
	r.ran = append(r.ran, index)
	reads, writes := newHistogram(), newHistogram()
	for sample := 0; sample < 1000; sample++ {
		if err := reads.record(time.Duration(1+index) * time.Millisecond); err != nil {
			return windowCapture{}, err
		}
	}
	for sample := 0; sample < 200; sample++ {
		if err := writes.record(5 * time.Millisecond); err != nil {
			return windowCapture{}, err
		}
	}
	return windowCapture{Read: reads, Write: writes, Verified: 10}, nil
}

// scriptedSampler reports the pid and free disk that the test wants once the given number of windows has started.
type scriptedSampler struct {
	runner       *scriptedRunner
	pidAfter     int64 // windows started before the pid changes; 0 never
	diskLowAfter int64 // windows started before free disk drops under the guard; 0 never
	restartedPID int
	basePID      int // the pid before any restart; 0 means 100
}

func (s *scriptedSampler) sample(context.Context) (soakSample, error) {
	started := s.runner.started.Load()
	pid := 100
	if s.basePID != 0 {
		pid = s.basePID
	}
	reading := soakSample{UnixSec: time.Now().Unix(), ServerPID: pid, RSSBytes: 1 << 30, Goroutines: 50, FDs: 80, Threads: 12, FreeDiskBytes: 50 << 30}
	if s.pidAfter > 0 && started >= s.pidAfter {
		reading.ServerPID = s.restartedPID
	}
	if s.diskLowAfter > 0 && started >= s.diskLowAfter {
		reading.FreeDiskBytes = 2 << 30
	}
	return reading, nil
}

func testSoakConfig(t *testing.T, windows int) soakConfig {
	return soakConfig{Dir: t.TempDir(), Windows: windows, Window: 5 * time.Minute, SampleInterval: time.Hour, MinFreeBytes: soakDiskGuardBytes}
}

func TestDiskGuardStopsAndRecords(t *testing.T) {
	cfg := testSoakConfig(t, 10)
	runner := &scriptedRunner{failAt: -1}
	sampler := &scriptedSampler{runner: runner, diskLowAfter: 3}
	outcome, err := runSoak(context.Background(), cfg, runner.run, sampler)
	if err != nil {
		t.Fatalf("a disk-guard stop is a recorded result, not an error: %v", err)
	}
	if outcome.StoppedReason != stoppedByDiskGuard || outcome.Completed != 3 {
		t.Errorf("outcome = %+v; want 3 windows done and %q", outcome, stoppedByDiskGuard)
	}
	if want := []int{0, 1, 2}; !reflect.DeepEqual(runner.ran, want) {
		t.Errorf("windows run = %v, want %v: the guard must stop before the next window starts", runner.ran, want)
	}
	state, err := os.ReadFile(filepath.Join(cfg.Dir, soakStateFile))
	if err != nil || !strings.Contains(string(state), stoppedByDiskGuard) {
		t.Errorf("the state file does not record %q: %v\n%s", stoppedByDiskGuard, err, state)
	}
	if outcome.Drift.StoppedReason != stoppedByDiskGuard {
		t.Errorf("the drift report does not say the soak was stopped: %+v", outcome.Drift)
	}
}

func TestSoakResumesFromWindowFiles(t *testing.T) {
	cfg := testSoakConfig(t, 6)
	crashing := &scriptedRunner{failAt: 3}
	if _, err := runSoak(context.Background(), cfg, crashing.run, &scriptedSampler{runner: crashing}); err == nil {
		t.Fatal("a crashed load client was not an error")
	}
	// A window that was cut short left histogram files but no summary: it must be run again, not trusted.
	debris := filepath.Join(cfg.Dir, "window-00003-read.hist")
	if err := os.WriteFile(debris, []byte("half a histogram"), 0o600); err != nil {
		t.Fatal(err)
	}
	resumed := &scriptedRunner{failAt: -1}
	outcome, err := runSoak(context.Background(), cfg, resumed.run, &scriptedSampler{runner: resumed})
	if err != nil {
		t.Fatal(err)
	}
	if want := []int{3, 4, 5}; !reflect.DeepEqual(resumed.ran, want) {
		t.Errorf("the restart ran windows %v, want %v: finished windows must not be run again", resumed.ran, want)
	}
	if outcome.Completed != 6 || outcome.Drift.Windows != 6 {
		t.Errorf("outcome = %+v; want all 6 windows counted across both runs", outcome)
	}
	again := &scriptedRunner{failAt: -1}
	if _, err := runSoak(context.Background(), cfg, again.run, &scriptedSampler{runner: again}); err != nil || len(again.ran) != 0 {
		t.Errorf("a finished soak ran windows %v (err %v); a third start must do nothing", again.ran, err)
	}
}

func TestServerRestartIsDetectedByPIDChange(t *testing.T) {
	cfg := testSoakConfig(t, 8)
	runner := &scriptedRunner{failAt: -1}
	sampler := &scriptedSampler{runner: runner, pidAfter: 4, restartedPID: 200}
	outcome, err := runSoak(context.Background(), cfg, runner.run, sampler)
	if !errors.Is(err, errServerRestarted) {
		t.Fatalf("err = %v; a server restart mid-soak is a failure (P3) and must be errServerRestarted", err)
	}
	if outcome.Completed != 4 || outcome.Drift.ServerRestartedAtWindow == nil || *outcome.Drift.ServerRestartedAtWindow != 4 {
		t.Errorf("outcome = %+v; want 4 windows done and the restart placed at window 4", outcome)
	}
	if !strings.Contains(err.Error(), "100") || !strings.Contains(err.Error(), "200") {
		t.Errorf("the error does not name both pids: %v", err)
	}
	// The same must be noticed when the soak driver itself is restarted over a server that restarted meanwhile.
	resumeCfg := testSoakConfig(t, 6)
	crashing := &scriptedRunner{failAt: 2}
	if _, err := runSoak(context.Background(), resumeCfg, crashing.run, &scriptedSampler{runner: crashing}); err == nil {
		t.Fatal("a crashed load client was not an error")
	}
	resumed := &scriptedRunner{failAt: -1}
	_, err = runSoak(context.Background(), resumeCfg, resumed.run, &scriptedSampler{runner: resumed, basePID: 300})
	if !errors.Is(err, errServerRestarted) || len(resumed.ran) != 0 {
		t.Errorf("a resume over a different server pid: err=%v, windows run=%v; want errServerRestarted and no window", err, resumed.ran)
	}
	// A soak that failed this way is invalid and is not resumed even if the pid is back.
	again := &scriptedRunner{failAt: -1}
	if _, err := runSoak(context.Background(), resumeCfg, again.run, &scriptedSampler{runner: again}); !errors.Is(err, errServerRestarted) || len(again.ran) != 0 {
		t.Errorf("restarting a failed soak: err=%v, windows run=%v; want the failure again and no window", err, again.ran)
	}
}

func TestWindowHistogramsStreamedToDisk(t *testing.T) {
	cfg := testSoakConfig(t, 150)
	runner := &scriptedRunner{failAt: -1}
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	outcome, err := runSoak(context.Background(), cfg, runner.run, &scriptedSampler{runner: runner})
	if err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	runtime.ReadMemStats(&after)
	// Each histogram is about 150 KB; keeping 300 of them would hold 40 MB.
	if growth := int64(after.HeapAlloc) - int64(before.HeapAlloc); growth > 15<<20 {
		t.Errorf("the heap grew by %d MB over %d windows: window histograms are being kept in memory", growth>>20, outcome.Completed)
	}
	summaryType := reflect.TypeOf(windowSummary{})
	for field := 0; field < summaryType.NumField(); field++ {
		switch summaryType.Field(field).Type.Kind() {
		case reflect.Pointer, reflect.Slice, reflect.Map, reflect.Interface, reflect.Chan:
			t.Errorf("windowSummary.%s is a %s: what the driver keeps per window must be fixed-size scalars", summaryType.Field(field).Name, summaryType.Field(field).Type.Kind())
		}
	}
	for _, name := range []string{"window-00000.json", "window-00007-read.hist", "window-00007-write.hist", "window-00149.json"} {
		if _, err := os.Stat(filepath.Join(cfg.Dir, name)); err != nil {
			t.Errorf("window file %s: %v", name, err)
		}
	}
	encoded, err := os.ReadFile(filepath.Join(cfg.Dir, "window-00007-read.hist"))
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeHistogram(encoded)
	if err != nil || decoded.count() != 1000 || decoded.quantile(0.5) < 7900*time.Microsecond || decoded.quantile(0.5) > 8100*time.Microsecond {
		t.Errorf("window 7's read histogram on disk: %v samples, median %v, err %v; want 1000 samples around 8 ms", decoded.count(), decoded.quantile(0.5), err)
	}
}

func TestTornLastSampleLineIsDropped(t *testing.T) {
	path := filepath.Join(t.TempDir(), soakSamplesFile)
	good := `{"unix_s":1,"server_pid":5,"rss_bytes":10,"goroutines":-1,"fds":3,"threads":4,"free_disk_bytes":9}`
	if err := os.WriteFile(path, []byte(good+"\n"+good+"\n"+`{"unix_s":3,"serv`), 0o600); err != nil {
		t.Fatal(err)
	}
	samples, err := loadSamples(path)
	if err != nil || len(samples) != 2 {
		t.Fatalf("loadSamples = %d samples, %v; want the 2 whole lines and no error for the torn last one", len(samples), err)
	}
	if err := os.WriteFile(path, []byte(`{"unix_s":1,"serv`+"\n"+good+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadSamples(path); err == nil {
		t.Error("a bad line before the last was accepted: that is corruption, not a torn write")
	}
}

func TestDriftReportRebuildsFromFiles(t *testing.T) {
	cfg := testSoakConfig(t, 4)
	runner := &scriptedRunner{failAt: -1}
	if _, err := runSoak(context.Background(), cfg, runner.run, &scriptedSampler{runner: runner}); err != nil {
		t.Fatal(err)
	}
	var out strings.Builder
	if err := runDriftReport(cfg.Dir, &out); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"RECORD\tsoak_windows\t4", "RECORD\tsoak_goroutine_drift\tn.a.", "CHECK PASS server pid unchanged for the whole soak"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("drift report lacks %q:\n%s", want, out.String())
		}
	}
	if _, err := os.Stat(filepath.Join(cfg.Dir, soakDriftFile)); err != nil {
		t.Errorf("the run did not leave %s: %v", soakDriftFile, err)
	}
	if err := runDriftReport(t.TempDir(), &out); err == nil {
		t.Error("an empty directory has no drift report")
	}
}

func TestProcServerSamplerReadsThisProcess(t *testing.T) {
	if _, err := os.Stat("/proc/self/fd"); err != nil {
		t.Skip("no /proc on this host")
	}
	pidFile := filepath.Join(t.TempDir(), "server.pid")
	if err := os.WriteFile(pidFile, []byte(strconv.Itoa(os.Getpid())+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	reading, err := procServerSampler{pidFile: pidFile, dataDir: t.TempDir()}.sample(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if reading.ServerPID != os.Getpid() || reading.RSSBytes <= 0 || reading.FDs <= 0 || reading.Threads <= 0 || reading.FreeDiskBytes <= 0 || reading.Goroutines != -1 {
		t.Errorf("sample of this process = %+v", reading)
	}
	if err := os.WriteFile(pidFile, []byte("not a pid"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := (procServerSampler{pidFile: pidFile, dataDir: t.TempDir()}).sample(context.Background()); err == nil {
		t.Error("a pid file holding text was accepted")
	}
}

func TestSoakModeNeedsItsFlags(t *testing.T) {
	cfg := &config{Attach: true, Rate: 500, Mix: "sk"}
	options := soakOptions{Dir: t.TempDir(), Hours: 6, Window: 5 * time.Minute, SampleEvery: time.Minute, ServerPIDFile: "pid", DataDir: "data", DiskGuardGB: 3}
	if err := options.validate(cfg); err != nil {
		t.Fatalf("complete options rejected: %v", err)
	}
	if got := options.windows(); got != 72 {
		t.Errorf("6 hours of 5-minute windows = %d windows, want 72", got)
	}
	for name, mutate := range map[string]func(*soakOptions, *config){
		"no directory":  func(o *soakOptions, _ *config) { o.Dir = "" },
		"no pid file":   func(o *soakOptions, _ *config) { o.ServerPIDFile = "" },
		"not attached":  func(_ *soakOptions, c *config) { c.Attach = false },
		"closed loop":   func(_ *soakOptions, c *config) { c.Rate = 0 },
		"no mix":        func(_ *soakOptions, c *config) { c.Mix = "" },
		"zero length":   func(o *soakOptions, _ *config) { o.Hours = 0 },
		"zero interval": func(o *soakOptions, _ *config) { o.SampleEvery = 0 },
	} {
		broken, brokenCfg := options, *cfg
		mutate(&broken, &brokenCfg)
		if err := broken.validate(&brokenCfg); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if _, err := parseFlags([]string{"-mode", "soak", "-soak-dir", "x"}); err != nil {
		t.Errorf("-mode soak rejected by parseFlags: %v", err)
	}
}

const sampleMetrics = `# HELP go_goroutines Number of goroutines that currently exist.
# TYPE go_goroutines gauge
go_goroutines_other 99
go_goroutines{job="x"} 7
go_goroutines %d
process_open_fds 16
`

// metricsServer serves go_goroutines with a value that rises by step on every scrape.
func metricsServer(t *testing.T, first, step int, withMetric bool) *httptest.Server {
	t.Helper()
	var scrapes atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/metrics" {
			http.NotFound(w, r)
			return
		}
		if !withMetric {
			_, _ = io.WriteString(w, "process_open_fds 16\n")
			return
		}
		_, _ = fmt.Fprintf(w, sampleMetrics, first+step*int(scrapes.Add(1)-1))
	}))
	t.Cleanup(server.Close)
	return server
}

func serverAddr(server *httptest.Server) string { return strings.TrimPrefix(server.URL, "http://") }

func TestScrapeGoroutinesParsesGauge(t *testing.T) {
	got, err := scrapeGoroutines(context.Background(), serverAddr(metricsServer(t, 31, 0, true)))
	if err != nil || got != 31 {
		t.Errorf("scrapeGoroutines = %d, %v; want 31 (not the labelled 7 or go_goroutines_other 99)", got, err)
	}
	if _, err := scrapeGoroutines(context.Background(), serverAddr(metricsServer(t, 0, 0, false))); err == nil || !strings.Contains(err.Error(), "go_goroutines") {
		t.Errorf("an endpoint without the metric: %v; want an error naming go_goroutines", err)
	}
}

func TestMetricsAddrFlagNamesServerSettings(t *testing.T) {
	cfg, err := parseFlags([]string{"-mode", "soak", "-metrics-addr", "127.0.0.1:9100"})
	if err != nil || cfg.Soak.MetricsAddr != "127.0.0.1:9100" {
		t.Fatalf("-metrics-addr = %+v, %v", cfg.Soak, err)
	}
	flags := flag.NewFlagSet("x", flag.ContinueOnError)
	var out bytes.Buffer
	flags.SetOutput(&out)
	registerSoakFlags(flags, &soakOptions{})
	flags.PrintDefaults()
	for _, want := range []string{"HOARDDB_METRICS_ENABLED=true", "HOARDDB_METRICS_LISTEN"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("the -metrics-addr help does not name %s:\n%s", want, out.String())
		}
	}
}

func TestDriftUsesValidSamplesOnly(t *testing.T) {
	series := func(goroutines ...int) []soakSample {
		samples := make([]soakSample, len(goroutines))
		for index, count := range goroutines {
			samples[index] = soakSample{UnixSec: int64(index) * 60, Window: index, Goroutines: count, FDs: 100 + index}
		}
		return samples
	}
	drift := computeDrift(nil, series(30, 35, -1), soakWindowDefault)
	if !nearly(drift.GoroutineDrift, 5, 0.001) {
		t.Errorf("a failed last scrape erased the drift: %v, want 5 (35 against 30)", deref(drift.GoroutineDrift))
	}
	if drift := computeDrift(nil, series(-1, 30, 36, -1), soakWindowDefault); !nearly(drift.GoroutineDrift, 6, 0.001) {
		t.Errorf("failed scrapes at both ends: %v, want 6", deref(drift.GoroutineDrift))
	}
	if drift := computeDrift(nil, series(-1, 30, -1), soakWindowDefault); drift.GoroutineDrift != nil {
		t.Errorf("one valid sample is no drift, got %v", deref(drift.GoroutineDrift))
	}
}

func TestIdleSampleIsNoBaseline(t *testing.T) {
	// 31 goroutines idle before the load, 63 to 65 under it: the drift under load is 2, not 34.
	samples := []soakSample{
		{UnixSec: 0, Goroutines: 31, FDs: 16, RSSBytes: 10 << 20, Idle: true, Boundary: true},
		{UnixSec: 60, Window: 0, Goroutines: 63, FDs: 60, RSSBytes: 100 << 20},
		{UnixSec: 120, Window: 1, Goroutines: 64, FDs: 61, RSSBytes: 100 << 20},
		{UnixSec: 180, Window: 2, Goroutines: 65, FDs: 62, RSSBytes: 100 << 20},
	}
	drift := computeDrift(nil, samples, soakWindowDefault)
	if !nearly(drift.GoroutineDrift, 2, 0.001) || !nearly(drift.DescriptorDrift, 2, 0.001) {
		t.Errorf("goroutine drift %v, descriptor drift %v; want 2 and 2 with the idle sample left out", deref(drift.GoroutineDrift), deref(drift.DescriptorDrift))
	}
	// Over two hours the idle 10 MB sample would tilt the slope; left out, the flat 100 MB reads 0.
	long := append([]soakSample{samples[0]}, samples[1])
	long = append(long, soakSample{UnixSec: 7200, Window: 24, RSSBytes: 100 << 20})
	if drift := computeDrift(nil, long, soakWindowDefault); !nearly(drift.RSSSlopeMBPerHour, 0, 0.001) {
		t.Errorf("RSS slope %v includes the idle sample; want 0", deref(drift.RSSSlopeMBPerHour))
	}
}

func TestFirstSampleOfASoakIsIdle(t *testing.T) {
	cfg := testSoakConfig(t, 3)
	runner := &scriptedRunner{failAt: -1}
	if _, err := runSoak(context.Background(), cfg, runner.run, &scriptedSampler{runner: runner}); err != nil {
		t.Fatal(err)
	}
	files, err := loadSoak(cfg.Dir)
	if err != nil || len(files.Samples) < 3 {
		t.Fatalf("samples = %d, %v", len(files.Samples), err)
	}
	if !files.Samples[0].Idle || files.Samples[1].Idle || files.Samples[2].Idle {
		t.Errorf("idle flags = %v %v %v; only the sample before window 0 is idle", files.Samples[0].Idle, files.Samples[1].Idle, files.Samples[2].Idle)
	}
	for index := range 3 {
		if sample := files.Samples[index]; !sample.Boundary || sample.Window != index {
			t.Errorf("sample %d: boundary=%v window=%d; a sample taken before window %d is a boundary sample of that window", index, sample.Boundary, sample.Window, index)
		}
	}
}

func TestBoundarySamplesAreNotAnEnd(t *testing.T) {
	// Four windows. In-window samples read 47 goroutines; the sample before each window, with the load
	// clients closed, reads 31, and the series ends on one of those. Last against first must be 0, not -16.
	var samples []soakSample
	for window := 0; window < 4; window++ {
		samples = append(samples, soakSample{UnixSec: int64(window) * 100, Window: window, Boundary: true, Idle: window == 0, Goroutines: 31, FDs: 16})
		for tick := 1; tick <= 3; tick++ {
			samples = append(samples, soakSample{UnixSec: int64(window)*100 + int64(tick)*20, Window: window, Goroutines: 47, FDs: 24})
		}
	}
	samples = append(samples, soakSample{UnixSec: 400, Window: 4, Boundary: true, Goroutines: 31, FDs: 16})
	drift := computeDrift(nil, samples, soakWindowDefault)
	if !nearly(drift.GoroutineDrift, 0, 0.001) || !nearly(drift.DescriptorDrift, 0, 0.001) {
		t.Errorf("goroutine drift %v, descriptor drift %v; want 0 and 0: boundary samples must not enter the drift", deref(drift.GoroutineDrift), deref(drift.DescriptorDrift))
	}
	// One low reading inside a window (the scrape that lands as the clients close) does not move the median.
	samples[len(samples)-3].Goroutines = 31
	if drift := computeDrift(nil, samples, soakWindowDefault); !nearly(drift.GoroutineDrift, 0, 0.001) {
		t.Errorf("one low in-window sample moved the drift to %v; each end is the median of its window", deref(drift.GoroutineDrift))
	}
	// A real rise is still seen.
	for index := range samples {
		if samples[index].Window == 3 && !samples[index].Boundary {
			samples[index].Goroutines = 60
		}
	}
	if drift := computeDrift(nil, samples, soakWindowDefault); !nearly(drift.GoroutineDrift, 13, 0.001) {
		t.Errorf("a rise from 47 to 60 in the last window gave %v, want 13", deref(drift.GoroutineDrift))
	}
}

func TestRSSSlopeIsNotAvailableUnderAnHour(t *testing.T) {
	short := make([]soakSample, 0, 10)
	for minute := range 10 {
		short = append(short, soakSample{UnixSec: int64(minute) * 60, Window: minute, RSSBytes: int64(1000+minute*100) << 20})
	}
	drift := computeDrift(nil, short, soakWindowDefault)
	if drift.RSSSlopeMBPerHour != nil || !strings.Contains(drift.RSSSlopeNote, "1 h") {
		t.Errorf("a 10-minute soak gave slope %v, note %q; want n.a. with a note that names the 1 h minimum", deref(drift.RSSSlopeMBPerHour), drift.RSSSlopeNote)
	}
	long := make([]soakSample, 0, 130)
	for minute := range 130 {
		long = append(long, soakSample{UnixSec: int64(minute) * 60, Window: minute / 5, RSSBytes: int64(1000+minute) << 20})
	}
	if drift := computeDrift(nil, long, soakWindowDefault); !nearly(drift.RSSSlopeMBPerHour, 60, 0.01) || drift.RSSSlopeNote != "" {
		t.Errorf("a 130-minute soak gave slope %v note %q; want 60 MB an hour and no note", deref(drift.RSSSlopeMBPerHour), drift.RSSSlopeNote)
	}
	lines := strings.Join(driftLines(soakOutcome{Drift: drift}, nil), "\n")
	if !strings.Contains(lines, "soak_rss_slope_mb_per_hour") {
		t.Errorf("the slope record is missing:\n%s", lines)
	}
	shortLines := strings.Join(driftLines(soakOutcome{Drift: computeDrift(nil, short, soakWindowDefault)}, nil), "\n")
	if !strings.Contains(shortLines, "soak_rss_slope_mb_per_hour\tn.a. (") {
		t.Errorf("a short soak's slope line has no reason:\n%s", shortLines)
	}
}
