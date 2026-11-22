package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// The soak (spec 7.3, build step S7) runs a leg in 5-minute windows. Every
// finished window is on disk (its two histograms and a summary), so a restart of
// this process carries on from the files, memory does not grow with the length
// of the soak, and the drift report can be rebuilt from the directory alone.
const (
	soakWindowDefault  = 5 * time.Minute
	soakSampleDefault  = 60 * time.Second
	soakDiskGuardBytes = int64(3) << 30
	// soakRSSSlopeSpan is how far back the resident-size slope looks: warm-up
	// and cache fill are over by then.
	soakRSSSlopeSpan   = 5 * time.Hour
	stoppedByDiskGuard = "stopped by the disk guard"
	soakStateFile      = "soak.json"
	soakSamplesFile    = "samples.jsonl"
	soakDriftFile      = "drift.json"
)

// errServerRestarted is the failure of a soak whose server changed pid: the
// windows after the change measure a different process (P3).
var errServerRestarted = errors.New("the server restarted during the soak")

// soakSample is one reading of the server, taken every sample interval.
type soakSample struct {
	UnixSec       int64 `json:"unix_s"`
	ServerPID     int   `json:"server_pid"`
	RSSBytes      int64 `json:"rss_bytes"`
	Goroutines    int   `json:"goroutines"` // -1 when the sampler cannot read it; GoroutinesNote says why
	FDs           int   `json:"fds"`
	Threads       int   `json:"threads"`
	FreeDiskBytes int64 `json:"free_disk_bytes"`
	// Window is the window the sample was taken in, or is about to start.
	Window int `json:"window"`
	// Boundary marks a sample taken between windows, when the load clients
	// are closed: their goroutines and descriptors are gone, so it says nothing
	// about the server under load.
	Boundary bool `json:"boundary,omitempty"`
	// Idle marks the sample taken before the first window's load starts: the
	// warm-up reading, which is no baseline for drift.
	Idle bool `json:"idle,omitempty"`
	// GoroutinesNote records why Goroutines is -1.
	GoroutinesNote string `json:"goroutines_note,omitempty"`
}

// serverSampler reads the server under soak.
type serverSampler interface {
	sample(ctx context.Context) (soakSample, error)
}

// windowCapture is what one window's load produced.
type windowCapture struct {
	Read, Write *histogram
	Errors      int64
	Verified    int64
}

// windowRunner offers load for one window.
type windowRunner func(ctx context.Context, index int, length time.Duration) (windowCapture, error)

// windowSummary is everything the driver keeps in memory about a finished
// window: scalars only, so memory does not grow with the window count. The
// histograms live in the window's files.
type windowSummary struct {
	Index     int       `json:"index"`
	StartUnix int64     `json:"start_unix_s"`
	LengthS   float64   `json:"length_s"`
	OpsPerSec float64   `json:"ops_per_s"`
	Reads     int64     `json:"reads"`
	Writes    int64     `json:"writes"`
	Errors    int64     `json:"errors"`
	Verified  int64     `json:"verified_reads"`
	ReadP99   latencyMs `json:"read_p99_ms"`
	WriteP99  latencyMs `json:"write_p99_ms"`
}

// soakState is the soak's own bookkeeping file.
type soakState struct {
	WindowsPlanned int     `json:"windows_planned"`
	WindowS        float64 `json:"window_s"`
	ServerPID      int     `json:"server_pid"`
	StoppedReason  string  `json:"stopped_reason,omitempty"`
	// Failure is set when the soak is invalid (a server restart); a failed soak is not resumed.
	Failure          string `json:"failure,omitempty"`
	RestartedAtIndex *int   `json:"restarted_at_window,omitempty"`
	Finished         bool   `json:"finished"`
}

// soakConfig is what a soak run is told.
type soakConfig struct {
	Dir            string
	Windows        int
	Window         time.Duration
	SampleInterval time.Duration
	MinFreeBytes   int64
}

// soakDrift is the report of spec 7.3. A nil number is "not computable from
// this run" (too few windows or samples). The p99 drifts compare the mean of
// the windows' p99 values in the first hour with the last hour.
type soakDrift struct {
	Windows                 int      `json:"windows"`
	FirstHourOpsPerSec      *float64 `json:"first_hour_ops_per_s"`
	LastHourOpsPerSec       *float64 `json:"last_hour_ops_per_s"`
	ThroughputDriftPct      *float64 `json:"throughput_drift_pct"`
	ReadP99DriftPct         *float64 `json:"read_p99_drift_pct"`
	WriteP99DriftPct        *float64 `json:"write_p99_drift_pct"`
	RSSSlopeMBPerHour       *float64 `json:"rss_slope_mb_per_hour"`
	RSSSlopeNote            string   `json:"rss_slope_note,omitempty"`
	GoroutineDrift          *float64 `json:"goroutine_drift"`
	GoroutineDriftPct       *float64 `json:"goroutine_drift_pct"`
	DescriptorDrift         *float64 `json:"descriptor_drift"`
	DescriptorDriftPct      *float64 `json:"descriptor_drift_pct"`
	StoppedReason           string   `json:"stopped_reason,omitempty"`
	ServerRestartedAtWindow *int     `json:"server_restarted_at_window,omitempty"`
}

// soakOutcome is how a run ended.
type soakOutcome struct {
	Completed     int
	StoppedReason string
	Drift         soakDrift
}

func percentChange(first, last float64) *float64 {
	if first == 0 {
		return nil
	}
	change := (last - first) / first * 100
	return &change
}

func mean(values []float64) *float64 {
	if len(values) == 0 {
		return nil
	}
	total := 0.0
	for _, value := range values {
		total += value
	}
	average := total / float64(len(values))
	return &average
}

// p99Values are the usable window p99s: a window with too few samples has none.
func p99Values(windows []windowSummary, pick func(windowSummary) latencyMs) []float64 {
	var values []float64
	for _, window := range windows {
		if p99 := pick(window); !math.IsNaN(float64(p99)) {
			values = append(values, float64(p99))
		}
	}
	return values
}

func p99Drift(first, last []windowSummary, pick func(windowSummary) latencyMs) *float64 {
	before, after := mean(p99Values(first, pick)), mean(p99Values(last, pick))
	if before == nil || after == nil {
		return nil
	}
	return percentChange(*before, *after)
}

// inWindow are the samples taken while a window's load was running: not the
// idle one before the soak, and not those between windows.
func inWindow(samples []soakSample) []soakSample {
	var kept []soakSample
	for _, sample := range samples {
		if !sample.Idle && !sample.Boundary {
			kept = append(kept, sample)
		}
	}
	return kept
}

// soakMinSlopeSpan is the shortest soak whose resident-size slope is reported:
// MB per hour read off a few minutes is noise scaled by twelve or more.
const soakMinSlopeSpan = time.Hour

// rssSlope is the least-squares slope of resident size in MB per hour over the
// in-window samples of the last soakRSSSlopeSpan. It is nil, with the reason,
// when the soak is shorter than soakMinSlopeSpan.
func rssSlope(samples []soakSample) (*float64, string) {
	samples = inWindow(samples)
	if len(samples) < 2 {
		return nil, "fewer than two samples were taken under load"
	}
	span := time.Duration(samples[len(samples)-1].UnixSec-samples[0].UnixSec) * time.Second
	if span < soakMinSlopeSpan {
		return nil, fmt.Sprintf("the soak sampled %v under load, under the 1 h needed for a meaningful MB per hour", span.Round(time.Second))
	}
	cutoff := samples[len(samples)-1].UnixSec - int64(soakRSSSlopeSpan/time.Second)
	var hours, megabytes []float64
	for _, sample := range samples {
		if sample.UnixSec >= cutoff {
			hours = append(hours, float64(sample.UnixSec)/3600)
			megabytes = append(megabytes, float64(sample.RSSBytes)/(1<<20))
		}
	}
	meanHours, meanMB := *mean(hours), *mean(megabytes)
	var covariance, variance float64
	for index := range hours {
		covariance += (hours[index] - meanHours) * (megabytes[index] - meanMB)
		variance += (hours[index] - meanHours) * (hours[index] - meanHours)
	}
	if variance == 0 {
		return nil, "the samples under load share one instant"
	}
	slope := covariance / variance
	return &slope, ""
}

// countDrift is the last window's count against the first window's after
// warm-up (spec 7.3). Each end is the median of that window's in-window
// samples, so one scrape that lands as the clients close does not decide it;
// idle and between-window samples (load clients closed) never count, and a
// sample whose count could not be read (-1) is skipped. A soak with fewer than
// two windows of usable samples has no drift.
func countDrift(samples []soakSample, pick func(soakSample) int) (drift, percent *float64) {
	counts := map[int][]float64{}
	var windows []int
	for _, sample := range inWindow(samples) {
		count := pick(sample)
		if count < 0 {
			continue
		}
		if _, seen := counts[sample.Window]; !seen {
			windows = append(windows, sample.Window)
		}
		counts[sample.Window] = append(counts[sample.Window], float64(count))
	}
	if len(windows) < 2 {
		return nil, nil
	}
	slices.Sort(windows)
	first, last := median(counts[windows[0]]), median(counts[windows[len(windows)-1]])
	change := last - first
	return &change, percentChange(first, last)
}

// computeDrift is the drift report of spec 7.3. "First sample after warm-up" is
// taken under load: the harness warms the server before the soak starts, but
// the sample that opens window 0 is still idle (marked Idle), and the samples
// between windows are taken with the load clients closed (marked Boundary).
func computeDrift(windows []windowSummary, samples []soakSample, window time.Duration) soakDrift {
	drift := soakDrift{Windows: len(windows)}
	drift.RSSSlopeMBPerHour, drift.RSSSlopeNote = rssSlope(samples)
	drift.GoroutineDrift, drift.GoroutineDriftPct = countDrift(samples, func(s soakSample) int { return s.Goroutines })
	drift.DescriptorDrift, drift.DescriptorDriftPct = countDrift(samples, func(s soakSample) int { return s.FDs })
	hourly := min(int(time.Hour/window), len(windows)/2)
	if hourly < 1 {
		return drift
	}
	first, last := windows[:hourly], windows[len(windows)-hourly:]
	opsOf := func(slice []windowSummary) *float64 {
		values := make([]float64, len(slice))
		for index, w := range slice {
			values[index] = w.OpsPerSec
		}
		return mean(values)
	}
	drift.FirstHourOpsPerSec, drift.LastHourOpsPerSec = opsOf(first), opsOf(last)
	drift.ThroughputDriftPct = percentChange(*drift.FirstHourOpsPerSec, *drift.LastHourOpsPerSec)
	drift.ReadP99DriftPct = p99Drift(first, last, func(w windowSummary) latencyMs { return w.ReadP99 })
	drift.WriteP99DriftPct = p99Drift(first, last, func(w windowSummary) latencyMs { return w.WriteP99 })
	return drift
}

// soakFiles is what a soak directory holds.
type soakFiles struct {
	State   *soakState
	Windows []windowSummary
	Samples []soakSample
}

func windowBase(index int) string { return fmt.Sprintf("window-%05d", index) }

// atomicWrite writes through a temporary file so a crash leaves the old
// content or the new, never half of it.
func atomicWrite(path string, content []byte) error {
	temporary := path + ".tmp"
	if err := os.WriteFile(temporary, content, 0o644); err != nil {
		return fmt.Errorf("writing %s: %w", temporary, err)
	}
	if err := os.Rename(temporary, path); err != nil {
		return fmt.Errorf("replacing %s: %w", path, err)
	}
	return nil
}

func writeJSONFile(path string, value any) error {
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("encoding %s: %w", filepath.Base(path), err)
	}
	return atomicWrite(path, append(encoded, '\n'))
}

// loadSoak reads a soak directory. A window counts only if its summary file
// exists: that file is written last, so a window cut short is simply absent.
func loadSoak(dir string) (soakFiles, error) {
	var files soakFiles
	stateBytes, err := os.ReadFile(filepath.Join(dir, soakStateFile))
	switch {
	case err == nil:
		files.State = new(soakState)
		if err := json.Unmarshal(stateBytes, files.State); err != nil {
			return files, fmt.Errorf("reading %s: %w", soakStateFile, err)
		}
	case !errors.Is(err, os.ErrNotExist):
		return files, fmt.Errorf("reading %s: %w", soakStateFile, err)
	}
	names, err := filepath.Glob(filepath.Join(dir, "window-?????.json"))
	if err != nil {
		return files, fmt.Errorf("listing windows in %s: %w", dir, err)
	}
	slices.Sort(names)
	for position, name := range names {
		content, err := os.ReadFile(name)
		if err != nil {
			return files, fmt.Errorf("reading window summary: %w", err)
		}
		var summary windowSummary
		if err := json.Unmarshal(content, &summary); err != nil {
			return files, fmt.Errorf("reading %s: %w", filepath.Base(name), err)
		}
		if summary.Index != position {
			return files, fmt.Errorf("%s holds window %d but windows 0..%d come before it: the window files are not contiguous", filepath.Base(name), summary.Index, position-1)
		}
		files.Windows = append(files.Windows, summary)
	}
	files.Samples, err = loadSamples(filepath.Join(dir, soakSamplesFile))
	return files, err
}

// loadSamples reads the sample lines. A last line that does not decode is the
// write a crash cut short and is dropped; an earlier bad line is corruption.
func loadSamples(path string) ([]soakSample, error) {
	content, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", soakSamplesFile, err)
	}
	lines := strings.Split(strings.TrimSpace(string(content)), "\n")
	samples := make([]soakSample, 0, len(lines))
	for number, line := range lines {
		var sample soakSample
		if err := json.Unmarshal([]byte(line), &sample); err != nil {
			if number == len(lines)-1 {
				break
			}
			return nil, fmt.Errorf("reading %s line %d: %w", soakSamplesFile, number+1, err)
		}
		samples = append(samples, sample)
	}
	return samples, nil
}

// soak is one run's state.
type soak struct {
	cfg     soakConfig
	runner  windowRunner
	sampler serverSampler
	state   soakState
	windows []windowSummary
	samples []soakSample
}

// halt says why a window must not go on.
type halt struct {
	reason  string
	restart bool
}

// runSoak runs the windows still missing from cfg.Dir and returns how the soak
// ended. A disk-guard stop is a recorded result (nil error); a server restart
// returns errServerRestarted after the outcome is on disk.
func runSoak(ctx context.Context, cfg soakConfig, runner windowRunner, sampler serverSampler) (soakOutcome, error) {
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return soakOutcome{}, fmt.Errorf("creating soak directory: %w", err)
	}
	files, err := loadSoak(cfg.Dir)
	if err != nil {
		return soakOutcome{}, err
	}
	run := &soak{cfg: cfg, runner: runner, sampler: sampler, windows: files.Windows, samples: files.Samples,
		state: soakState{WindowsPlanned: cfg.Windows, WindowS: cfg.Window.Seconds()}}
	if files.State != nil {
		run.state = *files.State
		if run.state.WindowsPlanned != cfg.Windows || run.state.WindowS != cfg.Window.Seconds() {
			return soakOutcome{}, fmt.Errorf("%s was started for %d windows of %vs, not %d of %vs: use a new directory or the same length", cfg.Dir, run.state.WindowsPlanned, run.state.WindowS, cfg.Windows, cfg.Window.Seconds())
		}
		if run.state.Failure != "" {
			return run.outcome(), fmt.Errorf("this soak already failed (%s) and is not resumed: %w", run.state.Failure, errServerRestarted)
		}
		if run.state.Finished {
			return run.outcome(), nil
		}
	}
	return run.loop(ctx)
}

func (s *soak) outcome() soakOutcome {
	drift := computeDrift(s.windows, s.samples, s.cfg.Window)
	drift.StoppedReason = s.state.StoppedReason
	drift.ServerRestartedAtWindow = s.state.RestartedAtIndex
	return soakOutcome{Completed: len(s.windows), StoppedReason: s.state.StoppedReason, Drift: drift}
}

func (s *soak) loop(ctx context.Context) (soakOutcome, error) {
	for len(s.windows) < s.cfg.Windows {
		index := len(s.windows)
		stopped, err := s.beforeWindow(ctx)
		if err != nil {
			return s.outcome(), err
		}
		if stopped != nil {
			return s.finish(stopped)
		}
		stopped, err = s.runWindow(ctx, index)
		if err != nil {
			return s.outcome(), err
		}
		if stopped != nil {
			return s.finish(stopped)
		}
	}
	return s.finish(nil)
}

// finish records how the soak ended and writes the drift report.
func (s *soak) finish(stopped *halt) (soakOutcome, error) {
	var failure error
	if stopped != nil {
		index := len(s.windows)
		if stopped.restart {
			s.state.Failure, s.state.RestartedAtIndex = stopped.reason, &index
			failure = fmt.Errorf("%s: %w", stopped.reason, errServerRestarted)
		} else {
			s.state.StoppedReason = stopped.reason
		}
	}
	s.state.Finished = true
	outcome := s.outcome()
	if err := writeJSONFile(filepath.Join(s.cfg.Dir, soakStateFile), s.state); err != nil {
		return outcome, err
	}
	if err := writeJSONFile(filepath.Join(s.cfg.Dir, soakDriftFile), outcome.Drift); err != nil {
		return outcome, err
	}
	return outcome, failure
}

// beforeWindow takes the sample that opens a window, so the guards are
// checked before any load is offered.
func (s *soak) beforeWindow(ctx context.Context) (*halt, error) {
	stopped, err := s.takeSample(ctx, sampleTag{Window: len(s.windows), Boundary: true, Idle: len(s.windows) == 0})
	if err != nil {
		return nil, err
	}
	if stopped == nil {
		// The state records the baseline pid the first time it is known.
		if err := writeJSONFile(filepath.Join(s.cfg.Dir, soakStateFile), s.state); err != nil {
			return nil, err
		}
	}
	return stopped, nil
}

// takeSample reads the server, appends the reading to the samples file, and
// says whether it breaks a guard.
// sampleTag says where in the soak a sample was taken.
type sampleTag struct {
	Window         int
	Boundary, Idle bool
}

func (s *soak) takeSample(ctx context.Context, tag sampleTag) (*halt, error) {
	sample, err := s.sampler.sample(ctx)
	if err != nil {
		return nil, fmt.Errorf("sampling the server: %w", err)
	}
	sample.Window, sample.Boundary, sample.Idle = tag.Window, tag.Boundary, tag.Idle
	if err := s.appendSample(sample); err != nil {
		return nil, err
	}
	switch {
	case s.state.ServerPID == 0:
		s.state.ServerPID = sample.ServerPID
	case sample.ServerPID != s.state.ServerPID:
		return &halt{restart: true, reason: fmt.Sprintf("server pid changed from %d to %d at window %d", s.state.ServerPID, sample.ServerPID, len(s.windows))}, nil
	}
	if sample.FreeDiskBytes < s.cfg.MinFreeBytes {
		return &halt{reason: stoppedByDiskGuard}, nil
	}
	return nil, nil
}

func (s *soak) appendSample(sample soakSample) error {
	encoded, err := json.Marshal(sample)
	if err != nil {
		return fmt.Errorf("encoding a sample: %w", err)
	}
	file, err := os.OpenFile(filepath.Join(s.cfg.Dir, soakSamplesFile), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("opening %s: %w", soakSamplesFile, err)
	}
	if _, err := file.Write(append(encoded, '\n')); err != nil {
		return errors.Join(fmt.Errorf("appending to %s: %w", soakSamplesFile, err), file.Close())
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("closing %s: %w", soakSamplesFile, err)
	}
	s.samples = append(s.samples, sample)
	return nil
}

// runWindow offers one window of load while sampling the server every
// SampleInterval. A guard that trips mid-window cancels the window and
// discards it: a window cut short is not a measurement.
func (s *soak) runWindow(ctx context.Context, index int) (*halt, error) {
	windowCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var sampling sync.WaitGroup
	var stopped *halt
	var samplingErr error
	sampling.Add(1)
	go func() {
		defer sampling.Done()
		ticker := time.NewTicker(s.cfg.SampleInterval)
		defer ticker.Stop()
		for {
			select {
			case <-windowCtx.Done():
				return
			case <-ticker.C:
				if stopped, samplingErr = s.takeSample(windowCtx, sampleTag{Window: index}); stopped != nil || samplingErr != nil {
					cancel()
					return
				}
			}
		}
	}()
	startedAt := time.Now()
	capture, runErr := s.runner(windowCtx, index, s.cfg.Window)
	cancel()
	sampling.Wait()
	switch {
	case samplingErr != nil:
		return nil, samplingErr
	case stopped != nil:
		return stopped, nil
	case runErr != nil:
		return nil, fmt.Errorf("window %d: %w", index, runErr)
	}
	return nil, s.saveWindow(index, startedAt, capture)
}

// saveWindow streams the histograms to their files and then writes the
// summary, the file that makes the window count.
func (s *soak) saveWindow(index int, startedAt time.Time, capture windowCapture) error {
	base := windowBase(index)
	if err := writeHistFiles(filepath.Join(s.cfg.Dir, base), capture.Read, capture.Write); err != nil {
		return fmt.Errorf("saving window %d: %w", index, err)
	}
	reads, writes := capture.Read.count(), capture.Write.count()
	summary := windowSummary{
		Index: index, StartUnix: startedAt.Unix(), LengthS: s.cfg.Window.Seconds(),
		OpsPerSec: float64(reads+writes) / s.cfg.Window.Seconds(), Reads: reads, Writes: writes,
		Errors: capture.Errors, Verified: capture.Verified,
		ReadP99: capture.Read.summary().P99, WriteP99: capture.Write.summary().P99,
	}
	if err := writeJSONFile(filepath.Join(s.cfg.Dir, base+".json"), summary); err != nil {
		return err
	}
	s.windows = append(s.windows, summary)
	return nil
}

// procServerSampler reads the server through /proc, the pid from the pid file
// the harness keeps (the pid changes when the server restarts, which is the point).
type procServerSampler struct {
	pidFile string
	dataDir string
	// metricsAddr is the server's Prometheus listener (host:port); empty means none.
	metricsAddr string
}

// metricsScrapeTimeout bounds one scrape so a hung metrics listener cannot stall a window's sampling.
const metricsScrapeTimeout = 5 * time.Second

// scrapeGoroutines reads go_goroutines from the server's Prometheus endpoint
// (http://<metricsAddr>/metrics). The server serves it only with
// HOARDDB_METRICS_ENABLED=true and HOARDDB_METRICS_LISTEN set.
func scrapeGoroutines(ctx context.Context, metricsAddr string) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, metricsScrapeTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+metricsAddr+"/metrics", nil)
	if err != nil {
		return 0, fmt.Errorf("building the metrics request for %q: %w", metricsAddr, err)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return 0, fmt.Errorf("scraping %s/metrics: %w", metricsAddr, err)
	}
	body, readErr := io.ReadAll(response.Body)
	closeErr := response.Body.Close()
	if readErr != nil || closeErr != nil {
		return 0, fmt.Errorf("scraping %s/metrics: reading the reply: %w", metricsAddr, errors.Join(readErr, closeErr))
	}
	if response.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("scraping %s/metrics: status %s", metricsAddr, response.Status)
	}
	return parseGoroutines(string(body))
}

// parseGoroutines finds the unlabelled go_goroutines sample in the Prometheus
// text format: "go_goroutines 31", optionally followed by a timestamp.
func parseGoroutines(text string) (int, error) {
	for _, line := range strings.Split(text, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 || fields[0] != "go_goroutines" {
			continue
		}
		value, err := strconv.ParseFloat(fields[1], 64)
		if err != nil {
			return 0, fmt.Errorf("go_goroutines has the value %q: %w", fields[1], err)
		}
		return int(value), nil
	}
	return 0, errors.New("the metrics reply has no go_goroutines sample (is this the hoardDB metrics listener?)")
}

func (p procServerSampler) sample(ctx context.Context) (soakSample, error) {
	pidText, err := os.ReadFile(p.pidFile)
	if err != nil {
		return soakSample{}, fmt.Errorf("reading the server pid file: %w", err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(pidText)))
	if err != nil {
		return soakSample{}, fmt.Errorf("the server pid file holds %q, not a pid: %w", strings.TrimSpace(string(pidText)), err)
	}
	dir := filepath.Join("/proc", strconv.Itoa(pid))
	rss, err := procProbe{dir: dir}.residentBytes()
	if err != nil {
		return soakSample{}, fmt.Errorf("server pid %d is not running: %w", pid, err)
	}
	threads, err := procThreads(dir)
	if err != nil {
		return soakSample{}, err
	}
	descriptors, err := os.ReadDir(filepath.Join(dir, "fd"))
	if err != nil {
		return soakSample{}, fmt.Errorf("counting the server's descriptors: %w", err)
	}
	var disk syscall.Statfs_t
	if err := syscall.Statfs(p.dataDir, &disk); err != nil {
		return soakSample{}, fmt.Errorf("reading free space of %s: %w", p.dataDir, err)
	}
	reading := soakSample{UnixSec: time.Now().Unix(), ServerPID: pid, RSSBytes: rss, FDs: len(descriptors),
		Threads: threads, FreeDiskBytes: int64(disk.Bavail) * int64(disk.Bsize)}
	reading.Goroutines, reading.GoroutinesNote = p.goroutines(ctx)
	return reading, nil
}

// goroutines reads the count from the metrics endpoint. A count that cannot be
// read is -1 with the reason, and the soak goes on: the drift report then
// prints n.a. for goroutines instead of inventing a figure. Descriptors come
// from counting /proc/<pid>/fd, which is exact for the process and needs no
// listener.
func (p procServerSampler) goroutines(ctx context.Context) (int, string) {
	if p.metricsAddr == "" {
		return -1, "no -metrics-addr given; start the server with HOARDDB_METRICS_ENABLED=true and HOARDDB_METRICS_LISTEN=<addr> and pass that address"
	}
	// A sample is often taken as its window ends and the window's context is
	// cancelled then; the scrape has its own timeout, so it must not inherit that.
	count, err := scrapeGoroutines(context.WithoutCancel(ctx), p.metricsAddr)
	if err != nil {
		return -1, err.Error()
	}
	return count, ""
}

func procThreads(dir string) (int, error) {
	content, err := os.ReadFile(filepath.Join(dir, "status"))
	if err != nil {
		return 0, fmt.Errorf("reading the server's thread count: %w", err)
	}
	for _, line := range strings.Split(string(content), "\n") {
		if rest, found := strings.CutPrefix(line, "Threads:"); found {
			threads, err := strconv.Atoi(strings.TrimSpace(rest))
			if err != nil {
				return 0, fmt.Errorf("reading Threads %q: %w", rest, err)
			}
			return threads, nil
		}
	}
	return 0, errors.New("process status has no Threads line")
}

// mixWindowRunner offers one window of the configured social mix (the dataset the harness seeds) at the
// configured open-loop rate. A wrong read, a read that found nothing where something must be, or a latency
// the histogram cannot hold ends the soak: those are correctness failures, not drift.
func mixWindowRunner(cfg *config, eng socialEngine, graph *socialGraph, book *socialBook) windowRunner {
	return func(ctx context.Context, _ int, length time.Duration) (windowCapture, error) {
		begin := time.Now()
		results, runErr := runMixWorkers(ctx, cfg, eng, graph, book, begin, begin.Add(length))
		capture := windowCapture{Read: newHistogram(), Write: newHistogram()}
		for _, result := range results {
			capture.Read.merge(result.reads)
			capture.Write.merge(result.writes)
			capture.Verified += result.verified
			for _, count := range result.errors {
				capture.Errors += count
			}
			switch {
			case result.failure != nil:
				return capture, fmt.Errorf("verification failed: %w", result.failure)
			case result.misses > 0:
				return capture, fmt.Errorf("%d reads found nothing where the dataset has something", result.misses)
			case result.recordFailure != nil:
				return capture, fmt.Errorf("an operation outlasted the latency histogram: %w", result.recordFailure)
			}
		}
		return capture, runErr
	}
}

// soakOptions are the flags of -mode soak and -mode driftreport.
type soakOptions struct {
	MetricsAddr   string
	Dir           string
	Hours         float64
	Window        time.Duration
	SampleEvery   time.Duration
	ServerPIDFile string
	DataDir       string
	DiskGuardGB   float64
}

func (o soakOptions) windows() int {
	return int(math.Round(o.Hours * float64(time.Hour) / float64(o.Window)))
}

func (o soakOptions) validate(cfg *config) error {
	switch {
	case o.Dir == "":
		return errors.New("-mode soak needs -soak-dir, the directory for window files and the drift report")
	case o.Window <= 0 || o.SampleEvery <= 0 || o.windows() < 1:
		return errors.New("-soak-hours, -soak-window and -sample-interval must be positive, and the soak at least one window long")
	case o.ServerPIDFile == "" || o.DataDir == "":
		return errors.New("-mode soak needs -server-pidfile (the harness's pid file) and -data-dir (to watch free disk)")
	case cfg.Mix == "":
		return errors.New("-mode soak needs -mix, the social mix to offer (sk for the soak of spec 7.3)")
	case !cfg.Attach:
		return errors.New("-mode soak runs against a dataset a seed run left behind: pass -attach")
	case cfg.Rate <= 0:
		return errors.New("-mode soak is open loop: pass -rate, the offered operations per second")
	}
	return nil
}

// runSoakMode is `-mode soak`.
func runSoakMode(ctx context.Context, cfg *config, out io.Writer) error {
	options := cfg.Soak
	if err := options.validate(cfg); err != nil {
		return err
	}
	spec, err := lookupDataset(cfg.Dataset)
	if err != nil {
		return err
	}
	eng, err := newEngine(ctx, cfg)
	if err != nil {
		return err
	}
	defer eng.close()
	social, ok := eng.(socialEngine)
	if !ok {
		return fmt.Errorf("-engine %s does not implement the social operations", cfg.Engine)
	}
	graph := newSocialGraph(spec, datasetSeed)
	book := newSocialBook(graph, true)
	soakCfg := soakConfig{Dir: options.Dir, Windows: options.windows(), Window: options.Window, SampleInterval: options.SampleEvery, MinFreeBytes: int64(options.DiskGuardGB * (1 << 30))}
	outcome, runErr := runSoak(ctx, soakCfg, mixWindowRunner(cfg, social, graph, book), procServerSampler{pidFile: options.ServerPIDFile, dataDir: options.DataDir, metricsAddr: options.MetricsAddr})
	if _, err := io.WriteString(out, strings.Join(driftLines(outcome, runErr), "\n")+"\n"); err != nil {
		return errors.Join(runErr, fmt.Errorf("writing the drift report: %w", err))
	}
	return runErr
}

// runDriftReport is `-mode driftreport`: the report rebuilt from the files.
func runDriftReport(dir string, out io.Writer) error {
	files, err := loadSoak(dir)
	if err != nil {
		return err
	}
	if files.State == nil {
		return fmt.Errorf("%s holds no %s: nothing was run there", dir, soakStateFile)
	}
	run := &soak{cfg: soakConfig{Window: time.Duration(files.State.WindowS * float64(time.Second))}, state: *files.State, windows: files.Windows, samples: files.Samples}
	var failure error
	if files.State.Failure != "" {
		failure = fmt.Errorf("%s: %w", files.State.Failure, errServerRestarted)
	}
	if _, err := io.WriteString(out, strings.Join(driftLines(run.outcome(), failure), "\n")+"\n"); err != nil {
		return fmt.Errorf("writing the drift report: %w", err)
	}
	return nil
}

func figure(value *float64) string {
	if value == nil {
		return notAvailable
	}
	return strconv.FormatFloat(*value, 'f', 2, 64)
}

// driftLines is what the harness reads.
func driftLines(outcome soakOutcome, runErr error) []string {
	drift := outcome.Drift
	lines := []string{
		fmt.Sprintf("RECORD\tsoak_windows\t%d", outcome.Completed),
		"RECORD\tsoak_throughput_drift_pct\t" + figure(drift.ThroughputDriftPct),
		"RECORD\tsoak_read_p99_drift_pct\t" + figure(drift.ReadP99DriftPct),
		"RECORD\tsoak_write_p99_drift_pct\t" + figure(drift.WriteP99DriftPct),
		"RECORD\tsoak_rss_slope_mb_per_hour\t" + slopeFigure(drift),
		"RECORD\tsoak_goroutine_drift\t" + figure(drift.GoroutineDrift),
		"RECORD\tsoak_descriptor_drift\t" + figure(drift.DescriptorDrift),
	}
	if outcome.StoppedReason != "" {
		lines = append(lines, "RECORD\tsoak_stopped\t"+outcome.StoppedReason)
	}
	if errors.Is(runErr, errServerRestarted) {
		return append(lines, "CHECK FAIL server pid unchanged for the whole soak: "+runErr.Error())
	}
	return append(lines, "CHECK PASS server pid unchanged for the whole soak")
}

// registerSoakFlags adds the -mode soak flags.
func registerSoakFlags(flags *flag.FlagSet, options *soakOptions) {
	flags.StringVar(&options.Dir, "soak-dir", "", "-mode soak and driftreport: the directory for window files, samples and the drift report")
	flags.Float64Var(&options.Hours, "soak-hours", 6, "-mode soak: length of the soak in hours")
	flags.DurationVar(&options.Window, "soak-window", soakWindowDefault, "-mode soak: length of one window; each window's histograms are written to their own files")
	flags.DurationVar(&options.SampleEvery, "sample-interval", soakSampleDefault, "-mode soak: how often the server is sampled")
	flags.StringVar(&options.ServerPIDFile, "server-pidfile", "", "-mode soak: file holding the server's pid; a changed pid means the server restarted")
	flags.StringVar(&options.DataDir, "data-dir", "", "-mode soak: the server's data directory, whose free space the disk guard watches")
	flags.Float64Var(&options.DiskGuardGB, "disk-guard-gb", 3, "-mode soak: stop when free space falls under this many GB")
	flags.StringVar(&options.MetricsAddr, "metrics-addr", "", "-mode soak: host:port of the server's Prometheus listener, read for the goroutine count; the server serves it only when started with HOARDDB_METRICS_ENABLED=true and HOARDDB_METRICS_LISTEN=<host:port>. Without it goroutine drift is n.a.")
}

// slopeFigure prints the slope, or n.a. with the reason it is not reported.
func slopeFigure(drift soakDrift) string {
	if drift.RSSSlopeMBPerHour == nil {
		return notAvailable + " (" + drift.RSSSlopeNote + ")"
	}
	return figure(drift.RSSSlopeMBPerHour)
}
