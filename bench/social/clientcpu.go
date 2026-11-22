package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

const (
	// clientCPUTarget is the share of a client VM's CPU a certification leg may
	// use: above it the client, not the database, starts to set the numbers.
	clientCPUTarget = 0.60
	// clientBoundFraction is the CPU share above which a leg is labelled client-bound.
	clientBoundFraction = 0.85
	// clientMemoryLimit is the resident size above which a leg is labelled
	// client-memory: the client VMs have 7 GB.
	clientMemoryLimit int64 = 4 << 30
	// userHZ is the unit of /proc/<pid>/stat CPU times. It is 100 on every
	// Linux architecture, whatever the kernel's own tick rate.
	userHZ = 100
	// residentSampleInterval is how often the resident size is read during a run.
	residentSampleInterval = 20 * time.Millisecond

	// modeClientCPU is the -mode value that runs the calibration.
	modeClientCPU = "clientcpu"
	// modeSoak and modeDriftReport are the -mode values of the soak driver (soak.go).
	modeSoak        = "soak"
	modeDriftReport = "driftreport"

	labelClientBoundResult  = "client-bound"
	labelClientMemoryResult = "client-memory"
)

// calibrationWorkers are the depths the calibration runs at.
var calibrationWorkers = []int{64, 128}

// processProbe reads what the calibration needs to know about this process.
type processProbe interface {
	cpuTime() (time.Duration, error)
	residentBytes() (int64, error)
}

// procProbe reads the stat and status files of a /proc/<pid> directory.
type procProbe struct{ dir string }

// cpuTime is the user plus system CPU time the process has used so far.
func (p procProbe) cpuTime() (time.Duration, error) {
	content, err := os.ReadFile(filepath.Join(p.dir, "stat"))
	if err != nil {
		return 0, fmt.Errorf("reading the process CPU time: %w", err)
	}
	// The command name is in parentheses and may itself contain spaces and
	// parentheses, so fields are counted from the last closing one. After it,
	// field 3 (the state) is index 0, so utime (14) and stime (15) are 11 and 12.
	text := string(content)
	closing := strings.LastIndexByte(text, ')')
	if closing < 0 {
		return 0, fmt.Errorf("process stat %q has no command name in parentheses", strings.TrimSpace(text))
	}
	fields := strings.Fields(text[closing+1:])
	if len(fields) < 13 {
		return 0, fmt.Errorf("process stat has %d fields after the command name, need at least 13", len(fields))
	}
	ticks := int64(0)
	for _, field := range fields[11:13] {
		count, err := strconv.ParseInt(field, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("reading CPU ticks %q from the process stat: %w", field, err)
		}
		ticks += count
	}
	return time.Duration(ticks) * time.Second / userHZ, nil
}

// residentBytes is the process's resident set size now.
func (p procProbe) residentBytes() (int64, error) {
	content, err := os.ReadFile(filepath.Join(p.dir, "status"))
	if err != nil {
		return 0, fmt.Errorf("reading the process resident size: %w", err)
	}
	for _, line := range strings.Split(string(content), "\n") {
		rest, found := strings.CutPrefix(line, "VmRSS:")
		if !found {
			continue
		}
		fields := strings.Fields(rest)
		if len(fields) != 2 || fields[1] != "kB" {
			return 0, fmt.Errorf("unexpected VmRSS line %q", line)
		}
		kilobytes, err := strconv.ParseInt(fields[0], 10, 64)
		if err != nil {
			return 0, fmt.Errorf("reading VmRSS %q: %w", fields[0], err)
		}
		return kilobytes << 10, nil
	}
	return 0, errors.New("process status has no VmRSS line")
}

// clientCPUResult is one calibration run at one depth.
type clientCPUResult struct {
	Workers    int
	Operations int64
	CPUPerOp   time.Duration
	// CPUFraction is the share of all the client's cores the run kept busy.
	CPUFraction float64
	// ResidentBytes is the peak resident size sampled during the run.
	ResidentBytes int64
	// MaxRate is the offered rate at which the client would be at clientCPUTarget.
	MaxRate float64
}

// maxOfferedRate is the operations per second a client with cores CPUs can
// offer while staying at clientCPUTarget of them, at cpuPerOp each.
func maxOfferedRate(cpuPerOp time.Duration, cores int) float64 {
	return clientCPUTarget * float64(cores) / cpuPerOp.Seconds()
}

// clientBoundLabel is true when a run used more than clientBoundFraction of the client's CPU.
func clientBoundLabel(cpuFraction float64) bool { return cpuFraction > clientBoundFraction }

// measureClientCPU drives eng at cfg.Conc workers for cfg.Duration (after an
// unmeasured cfg.Warmup) and reads what the client spent doing it. The CPU is
// the process's own, so the run measures the load client, never the server.
func measureClientCPU(ctx context.Context, cfg *config, eng engine, states []userState, probe processProbe, cores int) (clientCPUResult, error) {
	if cfg.Warmup > 0 {
		warmEnd := time.Now().Add(cfg.Warmup)
		for _, warm := range runWorkers(ctx, cfg, eng, states, warmEnd, warmEnd) {
			if warm.failure != nil {
				return clientCPUResult{}, fmt.Errorf("warming up at %d workers: %w", cfg.Conc, warm.failure)
			}
		}
	}
	samplerCtx, stopSampler := context.WithCancel(ctx)
	sampled := sampleResident(samplerCtx, probe)
	cpuBefore, err := probe.cpuTime()
	if err != nil {
		stopSampler()
		<-sampled
		return clientCPUResult{}, err
	}
	begin := time.Now()
	results := runWorkers(ctx, cfg, eng, states, begin, begin.Add(cfg.Duration))
	wall := time.Since(begin)
	cpuAfter, cpuErr := probe.cpuTime()
	stopSampler()
	peak := <-sampled
	if cpuErr != nil {
		return clientCPUResult{}, cpuErr
	}
	if peak.err != nil {
		return clientCPUResult{}, peak.err
	}
	return summarizeCalibration(cfg.Conc, results, cpuAfter-cpuBefore, wall, peak.bytes, cores)
}

func summarizeCalibration(workers int, results []*workerResult, cpuUsed, wall time.Duration, peakBytes int64, cores int) (clientCPUResult, error) {
	result := clientCPUResult{Workers: workers, ResidentBytes: peakBytes}
	var failed int64
	for _, worker := range results {
		result.Operations += worker.readLatencies.count() + worker.writeLatencies.count()
		for _, count := range worker.errors {
			failed += count
		}
		if worker.failure != nil {
			return result, fmt.Errorf("verification failed at %d workers: %w", workers, worker.failure)
		}
		if worker.recordFailure != nil {
			return result, fmt.Errorf("an operation outlasted the latency histogram at %d workers: %w", workers, worker.recordFailure)
		}
	}
	if failed > 0 {
		return result, fmt.Errorf("%d operations failed at %d workers: CPU per operation would count work that did not complete; fix the engine connection first", failed, workers)
	}
	if result.Operations == 0 {
		return result, fmt.Errorf("no operation completed at %d workers in %v", workers, wall)
	}
	result.CPUPerOp = cpuUsed / time.Duration(result.Operations)
	result.CPUFraction = cpuUsed.Seconds() / (wall.Seconds() * float64(cores))
	result.MaxRate = maxOfferedRate(result.CPUPerOp, cores)
	return result, nil
}

// residentPeak is the largest resident size sampleResident saw, or why it could not read it.
type residentPeak struct {
	bytes int64
	err   error
}

// sampleResident reads the resident size every residentSampleInterval until
// ctx ends, then delivers the peak. The workers' sockets and buffers come and
// go during a run, so one reading at the end would understate the memory.
func sampleResident(ctx context.Context, probe processProbe) <-chan residentPeak {
	delivered := make(chan residentPeak, 1)
	go func() {
		var peak residentPeak
		defer func() { delivered <- peak }()
		ticker := time.NewTicker(residentSampleInterval)
		defer ticker.Stop()
		for {
			current, err := probe.residentBytes()
			if err != nil {
				peak.err = err
				return
			}
			peak.bytes = max(peak.bytes, current)
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	return delivered
}

// lines is what the harness reads: RECORD lines (tab separated, as lib.sh
// writes them) and the memory check of T21 step 10, one set per depth.
func (r clientCPUResult) lines() []string {
	suffix := "_w" + strconv.Itoa(r.Workers)
	out := []string{
		fmt.Sprintf("RECORD\tclient_cpu_us_per_op%s\t%.2f", suffix, float64(r.CPUPerOp)/float64(time.Microsecond)),
		fmt.Sprintf("RECORD\tmax_offered_rate_at_60pct_cpu%s\t%.0f", suffix, r.MaxRate),
		fmt.Sprintf("RECORD\tclient_cpu_pct%s\t%.1f", suffix, r.CPUFraction*100),
		fmt.Sprintf("RECORD\tclient_rss_mb%s\t%d", suffix, r.ResidentBytes>>20),
	}
	if clientBoundLabel(r.CPUFraction) {
		out = append(out, "RECORD\tclient_label"+suffix+"\t"+labelClientBoundResult)
	}
	if r.ResidentBytes > clientMemoryLimit {
		out = append(out,
			"RECORD\tclient_label"+suffix+"\t"+labelClientMemoryResult,
			fmt.Sprintf("CHECK FAIL client resident memory %d MB is over 4 GB at %d workers (label %s)", r.ResidentBytes>>20, r.Workers, labelClientMemoryResult))
	} else {
		out = append(out, "CHECK PASS client resident memory under 4 GB")
	}
	return out
}

// runClientCPU is `-mode clientcpu`: it prepares a dataset (or attaches to
// one), measures the client at each calibration depth and prints the lines.
// The unsuffixed RECORD lines are the conservative reading across depths: the
// highest CPU per operation, the lowest rate, the largest resident size.
func runClientCPU(ctx context.Context, cfg *config, out io.Writer) error {
	eng, err := newEngine(ctx, cfg)
	if err != nil {
		return err
	}
	defer eng.close()
	states := make([]userState, cfg.Users)
	if cfg.Attach {
		if _, err := eng.attach(ctx); err != nil {
			return err
		}
		err = attachData(ctx, cfg, eng, states)
	} else {
		if _, err = eng.setup(ctx); err != nil {
			return err
		}
		err = seedData(ctx, cfg, eng, states)
	}
	if err != nil {
		return err
	}
	var worst clientCPUResult
	var printed []string
	for _, workers := range calibrationWorkers {
		depthCfg := *cfg
		depthCfg.Conc = workers
		result, err := measureClientCPU(ctx, &depthCfg, eng, states, procProbe{dir: "/proc/self"}, runtime.NumCPU())
		if err != nil {
			return err
		}
		printed = append(printed, result.lines()...)
		worst.CPUPerOp = max(worst.CPUPerOp, result.CPUPerOp)
		worst.ResidentBytes = max(worst.ResidentBytes, result.ResidentBytes)
		worst.CPUFraction = max(worst.CPUFraction, result.CPUFraction)
		if worst.MaxRate == 0 || result.MaxRate < worst.MaxRate {
			worst.MaxRate = result.MaxRate
		}
	}
	printed = append(printed,
		fmt.Sprintf("RECORD\tclient_cpu_us_per_op\t%.2f", float64(worst.CPUPerOp)/float64(time.Microsecond)),
		fmt.Sprintf("RECORD\tmax_offered_rate_at_60pct_cpu\t%.0f", worst.MaxRate),
		fmt.Sprintf("RECORD\tclient_rss_mb\t%d", worst.ResidentBytes>>20))
	if _, err := io.WriteString(out, strings.Join(printed, "\n")+"\n"); err != nil {
		return fmt.Errorf("writing the calibration: %w", err)
	}
	return nil
}
