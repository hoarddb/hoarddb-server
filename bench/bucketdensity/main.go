// bench/bucketdensity measures what one empty bucket costs a running hoardDB
// server: resident memory, file descriptors, disk (apparent and allocated),
// and the time a restart pays to reopen them. It closes VER-17 in
// docs/publish-checklist.md, the measurement the small-bucket principle
// (the small-bucket-principle record) is waiting on.
//
// It is deliberately NOT a Go test and NOT wired into CI: a single run takes
// minutes and can exhaust the host. Run it by hand:
//
//	go build -o /tmp/hoarddb-server ./cmd/server
//	go run ./bench/bucketdensity \
//	    -server-bin=/tmp/hoarddb-server \
//	    -data-dir=$HOME/hoarddb-density \
//	    -counts=10,100,1000 \
//	    -out=/tmp/density.json
//
// Three properties make the numbers mean what they claim:
//
//  1. Every resource figure is read from /proc/<server pid>, never from this
//     process. The harness's own allocations answer a different question.
//  2. The data directory must not be on tmpfs. tmpfs is RAM, so measuring disk
//     there would corrupt the memory figure; -data-dir is checked against
//     /proc/mounts and the run is refused if it lands on one.
//  3. The run stops itself before the host does. Free disk and MemAvailable are
//     sampled continuously; crossing either floor ends the scale step and the
//     count it reached is reported as the result, not as a crash.
//
// The failure mode is the point of the exercise, so a step that trips a guard
// or loses the server is recorded with the bucket count it reached and the
// reason, and the run continues to the report.
package main

import (
	"bufio"
	"crypto/tls"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

const (
	bytesPerBlock  = 512 // st_blocks is always in 512-byte units, whatever the block size
	bytesPerMB     = 1 << 20
	bytesPerGB     = 1 << 30
	readyPollEvery = 50 * time.Millisecond
	readyTimeout   = 120 * time.Second
	// readyDialTimeout bounds one readiness probe, so a half-open connection
	// cannot outlive readyTimeout.
	readyDialTimeout = 5 * time.Second
	shutdownTimeout  = 120 * time.Second
	// callTimeout bounds one request. Creating a bucket is tens of
	// milliseconds on a healthy server even at 8000 buckets; a minute is
	// generous and still finite.
	callTimeout = 60 * time.Second
)

// guardFloors are the host limits the run refuses to cross. They exist because
// this harness is run on a workstation, not a lab box: filling the root
// filesystem is a worse outcome than any measurement is worth, and swapping
// makes every RSS figure after it a fiction.
type guardFloors struct {
	FreeDiskBytes    int64
	AvailMemoryBytes int64
	// HardMemoryBytes is the last resort, below AvailMemoryBytes: crossing it
	// kills the server outright rather than ending the step politely.
	HardMemoryBytes int64
}

// hostFacts is recorded with every measurement. These are numbers from one
// machine and the report is required to label them as such.
type hostFacts struct {
	Kernel          string `json:"kernel"`
	CPUModel        string `json:"cpu_model"`
	CPUCount        int    `json:"cpu_count"`
	MemTotalBytes   int64  `json:"mem_total_bytes"`
	MemAvailBytes   int64  `json:"mem_avail_bytes_at_start"`
	DataDirFS       string `json:"data_dir_filesystem"`
	DataDirFreeByte int64  `json:"data_dir_free_bytes_at_start"`
	OpenFileLimit   uint64 `json:"open_file_limit"`
	MaxMapCount     int64  `json:"vm_max_map_count"`
}

// diskUsage separates apparent size from allocated blocks. They differ here by
// design: Badger preallocates its value log, so a directory whose files sum to
// a few kilobytes can hold 128 MB of allocated blocks — or the reverse, if the
// file is sparse. Reporting only one of them is how the 192-versus-43 MB
// confusion happened once already.
type diskUsage struct {
	ApparentBytes  int64 `json:"apparent_bytes"`
	AllocatedBytes int64 `json:"allocated_bytes"`
	FileCount      int64 `json:"file_count"`
}

func (d *diskUsage) add(other diskUsage) {
	d.ApparentBytes += other.ApparentBytes
	d.AllocatedBytes += other.AllocatedBytes
	d.FileCount += other.FileCount
}

// diskBreakdown splits a data directory by the components the dispatch names.
// "other" catches the catalog, the cluster token and anything a future change
// adds, so the parts always sum to the total.
type diskBreakdown struct {
	Badger  diskUsage `json:"badger"`
	Indexes diskUsage `json:"indexes"`
	Blobs   diskUsage `json:"blobs"`
	KeyFile diskUsage `json:"key_wrapped"`
	Other   diskUsage `json:"other"`
	Total   diskUsage `json:"total"`
}

// sample is one observation of the server process during bucket creation.
type sample struct {
	Buckets          int            `json:"buckets"`
	Process          processReading `json:"process"`
	HostAvailBytes   int64          `json:"host_mem_avail_bytes"`
	HostFreeDiskByte int64          `json:"host_free_disk_bytes"`
	ElapsedMillis    int64          `json:"elapsed_ms"`
}

// scaleResult is one N. Reached may be below Requested when a guard fired or
// the server died; that is a result, not an error.
type scaleResult struct {
	Requested int    `json:"requested_buckets"`
	Reached   int    `json:"reached_buckets"`
	Outcome   string `json:"outcome"`
	Detail    string `json:"detail,omitempty"`

	Baseline     processReading `json:"baseline"`
	Final        processReading `json:"final"`
	PeakRSSBytes int64          `json:"peak_rss_bytes"`
	PeakFDs      int            `json:"peak_file_descriptors"`

	Disk         diskBreakdown `json:"disk"`
	PeakDiskUsed int64         `json:"peak_data_dir_allocated_bytes"`

	ColdStartMillis      int64          `json:"cold_start_ms"`
	CreateAllMillis      int64          `json:"create_all_buckets_ms"`
	OneMoreCreateMicros  int64          `json:"one_more_create_us"`
	RestartReadyMillis   int64          `json:"restart_to_ready_ms"`
	CatalogLoadMillis    int64          `json:"catalog_load_ms"`
	CatalogBucketsLoaded int            `json:"catalog_buckets_loaded"`
	Restart              processReading `json:"restart"`

	Samples []sample `json:"samples"`
}

// runConfig is every knob the run takes. It is a struct because the parameter
// list crossed six the moment the guards were added.
type runConfig struct {
	ServerBin     string
	DataDir       string
	Addr          string
	InternodeAddr string
	Username      string
	Password      string
	Counts        []int
	SampleEvery   int
	Floors        guardFloors
	FDLimit       uint64
	LogDir        string
}

func main() {
	serverBin := flag.String("server-bin", "", "path to a built hoarddb-server binary (required)")
	dataDir := flag.String("data-dir", "", "data directory for the server under test; must not be on tmpfs (required)")
	addr := flag.String("addr", "127.0.0.1:7599", "client-facing driver address the server under test listens on (HOARDDB_TCP_LISTEN)")
	internodeAddr := flag.String("internode-addr", "127.0.0.1:7601", "cluster address the server under test listens on (HOARDDB_LISTEN); it also binds this port plus one")
	username := flag.String("user", "admin", "root username the server bootstraps with")
	password := flag.String("pass", "density-harness-pw1", "root password the server bootstraps with")
	countsRaw := flag.String("counts", "10,100,1000,10000", "comma-separated bucket counts to measure, ascending; in -mode=queueopen these are entry counts per queue and 0 is allowed")
	sampleEvery := flag.Int("sample-every", 0, "sample /proc every N buckets (0 = automatic)")
	minFreeDiskGB := flag.Float64("min-free-disk-gb", 10, "abort a scale step if free disk would fall below this")
	minAvailMemGB := flag.Float64("min-avail-mem-gb", 6, "abort a scale step if MemAvailable falls below this")
	hardFloorGB := flag.Float64("hard-floor-gb", 2.5, "kill the server outright if MemAvailable falls below this at any point, shutdown included; the last resort that protects the host")
	fdLimit := flag.Uint64("fd-limit", 0, "lower RLIMIT_NOFILE to this before spawning the server (0 = inherit); use it to reach the descriptor failure mode deliberately")
	outPath := flag.String("out", "", "write the full result as JSON to this path")
	mode := flag.String("mode", "density", "density (what an open bucket costs, VER-17), opencost (what opening and closing one costs, VER-19), queueopen (how a fifo/lifo open scales with queue length), or queueforest (the directory-per-item queue layout spike)")
	openCostFlags := registerOpenCostFlags()
	queueOpenFlags := registerQueueOpenFlags()
	queueForestFlags := registerQueueForestFlags()
	flag.Parse()

	// In the queue-open mode -counts is a curve of entry counts rather than a
	// set of bucket counts, and 0 is a real point on it: the empty open is the
	// baseline the slope is measured against. queueforest does not use
	// -counts at all -- it has its own -items/-min-entries/-max-entries.
	var counts []int
	var err error
	switch *mode {
	case "queueopen":
		counts, err = parseCountsAllowingZero(*countsRaw)
	case "queueforest":
		// no-op: queueforest has its own sizing flags.
	default:
		counts, err = parseCounts(*countsRaw)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "bucketdensity: -counts: %v\n", err)
		os.Exit(2)
	}
	// queueopen and queueforest need no server: a SegmentStore is
	// filesystem-resident, so the harness creates and opens the queues
	// itself, and queueforest's badger/mongo/mysql targets reach their
	// engine directly rather than through a hoarddb-server subprocess.
	if *serverBin == "" && *mode != "queueopen" && *mode != "queueforest" {
		fmt.Fprintln(os.Stderr, "bucketdensity: -server-bin is required for -mode=density and -mode=opencost")
		os.Exit(2)
	}
	if *dataDir == "" {
		fmt.Fprintln(os.Stderr, "bucketdensity: -data-dir is required")
		os.Exit(2)
	}
	absDataDir, err := filepath.Abs(*dataDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "bucketdensity: resolving -data-dir %q: %v\n", *dataDir, err)
		os.Exit(2)
	}

	cfg := runConfig{
		ServerBin:     *serverBin,
		DataDir:       absDataDir,
		Addr:          *addr,
		InternodeAddr: *internodeAddr,
		Username:      *username,
		Password:      *password,
		Counts:        counts,
		SampleEvery:   *sampleEvery,
		Floors: guardFloors{
			FreeDiskBytes:    int64(*minFreeDiskGB * bytesPerGB),
			AvailMemoryBytes: int64(*minAvailMemGB * bytesPerGB),
			HardMemoryBytes:  int64(*hardFloorGB * bytesPerGB),
		},
		FDLimit: *fdLimit,
		LogDir:  filepath.Join(absDataDir, "..", "bucketdensity-logs"),
	}

	switch *mode {
	case "density":
		if err := run(cfg, *outPath); err != nil {
			fmt.Fprintf(os.Stderr, "bucketdensity: %v\n", err)
			os.Exit(1)
		}
	case "opencost":
		openCost, err := openCostFlags.config()
		if err != nil {
			fmt.Fprintf(os.Stderr, "bucketdensity: %v\n", err)
			os.Exit(2)
		}
		if err := runOpenCost(cfg, openCost, *outPath); err != nil {
			fmt.Fprintf(os.Stderr, "bucketdensity: %v\n", err)
			os.Exit(1)
		}
	case "queueopen":
		queueOpen, err := queueOpenFlags.config(counts)
		if err != nil {
			fmt.Fprintf(os.Stderr, "bucketdensity: %v\n", err)
			os.Exit(2)
		}
		if err := runQueueOpen(cfg, queueOpen, *outPath); err != nil {
			fmt.Fprintf(os.Stderr, "bucketdensity: %v\n", err)
			os.Exit(1)
		}
	case "queueforest":
		queueForest, err := queueForestFlags.config()
		if err != nil {
			fmt.Fprintf(os.Stderr, "bucketdensity: %v\n", err)
			os.Exit(2)
		}
		if err := runQueueForest(cfg, queueForest, *outPath); err != nil {
			fmt.Fprintf(os.Stderr, "bucketdensity: %v\n", err)
			os.Exit(1)
		}
	default:
		fmt.Fprintf(os.Stderr, "bucketdensity: -mode=%q is none of \"density\", \"opencost\", \"queueopen\" or \"queueforest\"\n", *mode)
		os.Exit(2)
	}
}

func run(cfg runConfig, outPath string) error {
	if err := refuseTmpfs(cfg.DataDir); err != nil {
		return err
	}
	if err := os.MkdirAll(cfg.LogDir, 0o755); err != nil {
		return fmt.Errorf("creating log directory %q: %w", cfg.LogDir, err)
	}

	facts, err := collectHostFacts(cfg.DataDir)
	if err != nil {
		return err
	}
	printHostFacts(facts)

	results := make([]scaleResult, 0, len(cfg.Counts))
	for _, wanted := range cfg.Counts {
		freeNow, err := freeDiskBytes(filepath.Dir(cfg.DataDir))
		if err != nil {
			return err
		}
		if freeNow < cfg.Floors.FreeDiskBytes {
			fmt.Printf("\nREFUSING N=%d: free disk is %s, floor is %s. Stopping before the host is harmed.\n",
				wanted, humanBytes(freeNow), humanBytes(cfg.Floors.FreeDiskBytes))
			break
		}

		fmt.Printf("\n=== N=%d (free disk %s, MemAvailable %s) ===\n",
			wanted, humanBytes(freeNow), humanBytes(mustMemAvailable()))

		measured, err := measureScale(cfg, wanted)
		if err != nil {
			return err
		}
		results = append(results, measured)
		printScaleResult(measured)

		if err := os.RemoveAll(cfg.DataDir); err != nil {
			return fmt.Errorf("clearing data directory %q after N=%d: %w", cfg.DataDir, wanted, err)
		}
		if measured.Reached < measured.Requested {
			fmt.Printf("\nN=%d did not complete (%s). Larger counts cannot, so the run stops here.\n",
				wanted, measured.Outcome)
			break
		}
	}

	printMarginals(results)

	if outPath != "" {
		report := struct {
			Host    hostFacts     `json:"host"`
			Floors  guardFloors   `json:"guard_floors"`
			Results []scaleResult `json:"results"`
		}{Host: facts, Floors: cfg.Floors, Results: results}
		encoded, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			return fmt.Errorf("encoding report: %w", err)
		}
		if err := os.WriteFile(outPath, encoded, 0o644); err != nil {
			return fmt.Errorf("writing report to %q: %w", outPath, err)
		}
		fmt.Printf("\nJSON report: %s\n", outPath)
	}
	return nil
}

// measureScale runs one N end to end: cold start on an empty directory, create
// N buckets while sampling, measure disk, restart on the populated directory to
// price the reopen, then stop.
func measureScale(cfg runConfig, wanted int) (scaleResult, error) {
	measured := scaleResult{Requested: wanted, Outcome: "completed"}

	if err := os.RemoveAll(cfg.DataDir); err != nil {
		return measured, fmt.Errorf("clearing data directory %q: %w", cfg.DataDir, err)
	}
	noteWatchdog := func(proc *serverProcess) {
		select {
		case reason := <-proc.killed:
			measured.Outcome = "host-watchdog"
			measured.Detail = strings.TrimSpace(measured.Detail + " " + reason)
		default:
		}
	}
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return measured, fmt.Errorf("creating data directory %q: %w", cfg.DataDir, err)
	}

	coldLog := filepath.Join(cfg.LogDir, fmt.Sprintf("n%d-cold.log", wanted))
	proc, coldStart, err := startServer(cfg, coldLog)
	if err != nil {
		return measured, err
	}
	measured.ColdStartMillis = coldStart.Milliseconds()

	conn, err := dialAndAuth(cfg)
	if err != nil {
		_ = proc.stop()
		return measured, err
	}

	measured.Baseline, err = readProcess(proc.pid())
	if err != nil {
		_ = conn.close()
		_ = proc.stop()
		return measured, err
	}

	const databaseName = "Density"
	if err := conn.call(transport.OpCreateDB, &server.CreateDatabaseRequest{Name: databaseName}); err != nil {
		_ = conn.close()
		_ = proc.stop()
		return measured, err
	}

	createOutcome := createBuckets(&measured, cfg, conn, proc, databaseName, wanted)
	measured.Reached = createOutcome.reached
	measured.Outcome = createOutcome.outcome
	measured.Detail = createOutcome.detail

	// Everything below reads the process, so it only applies while it lives.
	if createOutcome.serverAlive {
		finalReading, readErr := readProcess(proc.pid())
		if readErr != nil {
			measured.Outcome = "process-gone"
			measured.Detail = strings.TrimSpace(measured.Detail + " final reading failed: " + readErr.Error())
		}
		measured.Final = finalReading
		if measured.Final.RSSBytes > measured.PeakRSSBytes {
			measured.PeakRSSBytes = measured.Final.RSSBytes
		}
		if measured.Final.FileDescriptors > measured.PeakFDs {
			measured.PeakFDs = measured.Final.FileDescriptors
		}

		// One more bucket on a warm server with N already open: the marginal
		// create cost as the Nth+1 caller experiences it. Only meaningful when
		// the step actually completed — after a refused or unanswered create,
		// the connection is not usable and the timing would be of the failure.
		if createOutcome.outcome == "completed" {
			singleStart := time.Now()
			if err := conn.call(transport.OpCreateBucket, &server.CreateBucketRequest{
				Database: databaseName,
				Name:     fmt.Sprintf("b%08d", wanted),
				Type:     server.BucketTypeHash,
			}); err == nil {
				measured.OneMoreCreateMicros = time.Since(singleStart).Microseconds()
			}
		}
		_ = conn.close()
		if err := proc.stop(); err != nil {
			return measured, err
		}
		noteWatchdog(proc)
	} else {
		_ = conn.close()
		_ = proc.stop()
	}

	breakdown, err := measureDisk(cfg.DataDir)
	if err != nil {
		return measured, err
	}
	measured.Disk = breakdown
	if breakdown.Total.AllocatedBytes > measured.PeakDiskUsed {
		measured.PeakDiskUsed = breakdown.Total.AllocatedBytes
	}

	// The restart is the measurement the catalog costs: LoadCatalog reopens
	// every bucket's Badger instance before the server answers anything.
	if measured.Reached > 0 {
		warmLog := filepath.Join(cfg.LogDir, fmt.Sprintf("n%d-restart.log", wanted))
		restartProc, restartReady, err := startServer(cfg, warmLog)
		if err != nil {
			measured.Detail = strings.TrimSpace(measured.Detail + " restart failed: " + err.Error())
			if measured.Outcome == "completed" {
				measured.Outcome = "restart-failed"
			}
			return measured, nil
		}
		measured.RestartReadyMillis = restartReady.Milliseconds()
		restartReading, readErr := readProcess(restartProc.pid())
		if readErr != nil {
			measured.Detail = strings.TrimSpace(measured.Detail + " restart reading failed: " + readErr.Error())
		}
		measured.Restart = restartReading
		loadMillis, loaded, parseErr := catalogLoadFromLog(warmLog)
		if parseErr == nil {
			measured.CatalogLoadMillis = loadMillis
			measured.CatalogBucketsLoaded = loaded
		}
		if err := restartProc.stop(); err != nil {
			return measured, err
		}
		noteWatchdog(restartProc)
	}

	return measured, nil
}

type createOutcome struct {
	reached     int
	outcome     string
	detail      string
	serverAlive bool
}

// createBuckets creates buckets one at a time, sampling the server process as
// it goes and stopping the moment a host guard trips or the server stops
// answering. The count it stopped at is the answer this harness exists to find.
func createBuckets(measured *scaleResult, cfg runConfig, conn *serverConn, proc *serverProcess, databaseName string, wanted int) createOutcome {
	sampleEvery := cfg.SampleEvery
	if sampleEvery <= 0 {
		sampleEvery = wanted / 40
		if sampleEvery < 1 {
			sampleEvery = 1
		}
	}

	started := time.Now()
	takeSample := func(created int) (sample, error) {
		reading, err := readProcess(proc.pid())
		if err != nil {
			return sample{}, err
		}
		avail, err := memAvailableBytes()
		if err != nil {
			return sample{}, err
		}
		free, err := freeDiskBytes(cfg.DataDir)
		if err != nil {
			return sample{}, err
		}
		return sample{
			Buckets:          created,
			Process:          reading,
			HostAvailBytes:   avail,
			HostFreeDiskByte: free,
			ElapsedMillis:    time.Since(started).Milliseconds(),
		}, nil
	}

	record := func(observed sample) {
		measured.Samples = append(measured.Samples, observed)
		if observed.Process.RSSBytes > measured.PeakRSSBytes {
			measured.PeakRSSBytes = observed.Process.RSSBytes
		}
		if observed.Process.FileDescriptors > measured.PeakFDs {
			measured.PeakFDs = observed.Process.FileDescriptors
		}
	}

	if zero, err := takeSample(0); err == nil {
		record(zero)
	}

	for created := 1; created <= wanted; created++ {
		err := conn.call(transport.OpCreateBucket, &server.CreateBucketRequest{
			Database: databaseName,
			Name:     fmt.Sprintf("b%08d", created-1),
			Type:     server.BucketTypeHash,
		})
		if err != nil {
			measured.CreateAllMillis = time.Since(started).Milliseconds()
			return createOutcome{
				reached:     created - 1,
				outcome:     "server-error",
				detail:      fmt.Sprintf("creating bucket %d: %v", created, err),
				serverAlive: proc.alive(),
			}
		}

		if created%sampleEvery != 0 && created != wanted {
			continue
		}
		observed, err := takeSample(created)
		if err != nil {
			measured.CreateAllMillis = time.Since(started).Milliseconds()
			return createOutcome{
				reached:     created,
				outcome:     "process-gone",
				detail:      fmt.Sprintf("reading /proc for the server after %d buckets: %v", created, err),
				serverAlive: false,
			}
		}
		record(observed)
		fmt.Printf("  %6d buckets | rss %9s | virt %9s | pte %8s | swap %8s | fds %6d | maps %6d | thr %3d | avail %8s | free %8s\n",
			created, humanBytes(observed.Process.RSSBytes), humanBytes(observed.Process.VirtualBytes),
			humanBytes(observed.Process.PageTableBytes), humanBytes(observed.Process.SwapBytes),
			observed.Process.FileDescriptors, observed.Process.Mappings, observed.Process.Threads,
			humanBytes(observed.HostAvailBytes), humanBytes(observed.HostFreeDiskByte))

		if observed.HostAvailBytes < cfg.Floors.AvailMemoryBytes {
			measured.CreateAllMillis = time.Since(started).Milliseconds()
			return createOutcome{
				reached: created,
				outcome: "memory-guard",
				detail: fmt.Sprintf("MemAvailable fell to %s at %d buckets, below the %s floor; stopped before the host swapped",
					humanBytes(observed.HostAvailBytes), created, humanBytes(cfg.Floors.AvailMemoryBytes)),
				serverAlive: true,
			}
		}
		if observed.HostFreeDiskByte < cfg.Floors.FreeDiskBytes {
			measured.CreateAllMillis = time.Since(started).Milliseconds()
			return createOutcome{
				reached: created,
				outcome: "disk-guard",
				detail: fmt.Sprintf("free disk fell to %s at %d buckets, below the %s floor",
					humanBytes(observed.HostFreeDiskByte), created, humanBytes(cfg.Floors.FreeDiskBytes)),
				serverAlive: true,
			}
		}
	}

	measured.CreateAllMillis = time.Since(started).Milliseconds()
	return createOutcome{reached: wanted, outcome: "completed", serverAlive: true}
}

// --- server process ---------------------------------------------------------

type serverProcess struct {
	cmd    *exec.Cmd
	logFD  *os.File
	exited chan struct{}
	// killed records that the watchdog, not the caller, ended this process.
	killed chan string
}

func (p *serverProcess) pid() int { return p.cmd.Process.Pid }

// watchHost kills the server if the host runs out of memory, for the whole
// life of the process rather than only while buckets are being created.
//
// This exists because the create loop's guard was not enough. Stopping a
// server that holds thousands of open instances makes Badger flush every
// memtable, and RSS *grows* during that shutdown — on the first run it went
// from 17.3 GB to 21.6 GB after the create-phase guard had already fired,
// taking the host to 1.9 GB of MemAvailable and 45 MB of free swap. A guard
// that stops creating buckets but then waits politely for a graceful shutdown
// is not a guard.
//
// The floor here is deliberately below the create-phase floor: this is the
// last resort that protects the machine, not the measurement.
func (p *serverProcess) watchHost(floorBytes int64, done <-chan struct{}) {
	const pollEvery = 500 * time.Millisecond
	for {
		select {
		case <-done:
			return
		case <-p.exited:
			return
		case <-time.After(pollEvery):
		}
		avail, err := memAvailableBytes()
		if err != nil || avail >= floorBytes {
			continue
		}
		reason := fmt.Sprintf("host watchdog: MemAvailable fell to %s, below the %s hard floor; the server was killed to protect the host",
			humanBytes(avail), humanBytes(floorBytes))
		fmt.Printf("\n!! %s\n", reason)
		if err := p.cmd.Process.Kill(); err != nil {
			fmt.Printf("!! could not kill the server: %v\n", err)
		}
		select {
		case p.killed <- reason:
		default:
		}
		return
	}
}

func (p *serverProcess) alive() bool {
	select {
	case <-p.exited:
		return false
	default:
		return true
	}
}

func (p *serverProcess) stop() error {
	defer func() { _ = p.logFD.Close() }()
	if !p.alive() {
		return nil
	}
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		return fmt.Errorf("signalling server pid %d: %w", p.pid(), err)
	}
	select {
	case <-p.exited:
		return nil
	case <-time.After(shutdownTimeout):
		if err := p.cmd.Process.Kill(); err != nil {
			return fmt.Errorf("killing unresponsive server pid %d: %w", p.pid(), err)
		}
		<-p.exited
		return nil
	}
}

// startServer spawns the server against cfg.DataDir and returns once a TLS
// handshake succeeds, which is the earliest point a client could use it. The
// duration is therefore start-to-usable, catalog load included.
func startServer(cfg runConfig, logPath string) (*serverProcess, time.Duration, error) {
	logFD, err := os.Create(logPath)
	if err != nil {
		return nil, 0, fmt.Errorf("creating server log %q: %w", logPath, err)
	}

	cmd := serverCommand(cfg)
	cmd.Env = append(os.Environ(),
		"HOARDDB_DATA_DIR="+cfg.DataDir,
		// HOARDDB_TCP_LISTEN is the client-facing driver port; HOARDDB_LISTEN is
		// the cluster/internode base and binds base+1 as well. Both are pinned
		// so a run cannot collide with a server already on the default 7433.
		"HOARDDB_TCP_LISTEN="+cfg.Addr,
		"HOARDDB_LISTEN="+cfg.InternodeAddr,
		"HOARDDB_ROOT_USER="+cfg.Username,
		"HOARDDB_ROOT_PASSWORD="+cfg.Password,
		"HOARDDB_LOG_FORMAT=json",
		"HOARDDB_LOG_LEVEL=info",
	)
	cmd.Stdout = logFD
	cmd.Stderr = logFD

	begin := time.Now()
	if err := cmd.Start(); err != nil {
		_ = logFD.Close()
		return nil, 0, fmt.Errorf("starting %q: %w", cfg.ServerBin, err)
	}

	proc := &serverProcess{
		cmd:    cmd,
		logFD:  logFD,
		exited: make(chan struct{}),
		killed: make(chan string, 1),
	}
	go func() {
		// The exit status is not the measurement: a killed server and a clean
		// one both end the step, and which it was is recorded by the watchdog.
		_ = cmd.Wait()
		close(proc.exited)
	}()
	go proc.watchHost(cfg.Floors.HardMemoryBytes, proc.exited)

	deadline := time.Now().Add(readyTimeout)
	for {
		if !proc.alive() {
			_ = logFD.Close()
			return nil, 0, fmt.Errorf("server exited before becoming ready; log: %s", logPath)
		}
		tlsCfg, tlsErr := insecureClientTLS(cfg.Addr)
		if tlsErr != nil {
			_ = proc.stop()
			return nil, 0, tlsErr
		}
		// The readiness deadline is only consulted after a dial returns, so the
		// dial itself has to be bounded: a server that accepts the socket but
		// never finishes the handshake would otherwise hold this loop open
		// past readyTimeout forever.
		probe, dialErr := tls.DialWithDialer(&net.Dialer{Timeout: readyDialTimeout}, "tcp", cfg.Addr, tlsCfg)
		if dialErr == nil {
			_ = probe.Close()
			if cfg.FDLimit > 0 {
				actual, limitErr := verifyOpenFileLimit(proc.pid())
				if limitErr != nil {
					_ = proc.stop()
					return nil, 0, limitErr
				}
				if actual != cfg.FDLimit {
					_ = proc.stop()
					return nil, 0, fmt.Errorf("-fd-limit=%d did not reach the server: it is running with a soft limit of %d. The descriptor experiment would have measured nothing", cfg.FDLimit, actual)
				}
				fmt.Printf("server running with RLIMIT_NOFILE soft limit %d (verified from /proc)\n", actual)
			}
			return proc, time.Since(begin), nil
		}
		if time.Now().After(deadline) {
			_ = proc.stop()
			return nil, 0, fmt.Errorf("server did not accept a connection on %s within %s; log: %s", cfg.Addr, readyTimeout, logPath)
		}
		time.Sleep(readyPollEvery)
	}
}

// catalogLoadFromLog reads the restart log for the interval between the server
// announcing itself and LoadCatalog reporting done. That interval is what N
// open Badger instances cost on every start.
func catalogLoadFromLog(logPath string) (int64, int, error) {
	raw, err := os.ReadFile(logPath)
	if err != nil {
		return 0, 0, fmt.Errorf("reading server log %q: %w", logPath, err)
	}
	var startedAt, loadedAt time.Time
	bucketsLoaded := 0
	for _, line := range strings.Split(string(raw), "\n") {
		if !strings.HasPrefix(strings.TrimSpace(line), "{") {
			continue
		}
		var entry struct {
			Time    time.Time `json:"time"`
			Msg     string    `json:"msg"`
			Buckets int       `json:"buckets"`
		}
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			continue
		}
		switch entry.Msg {
		case "hoardDB server starting":
			startedAt = entry.Time
		case "catalog loaded":
			loadedAt = entry.Time
			bucketsLoaded = entry.Buckets
		}
	}
	if startedAt.IsZero() || loadedAt.IsZero() {
		return 0, 0, errors.New("server log has no start/catalog-loaded pair")
	}
	return loadedAt.Sub(startedAt).Milliseconds(), bucketsLoaded, nil
}

// --- client -----------------------------------------------------------------

type serverConn struct {
	conn   *tls.Conn
	reader *bufio.Reader
}

// clientTLSOnce builds the client TLS config a single time. Building it per
// dial logs the insecure-trust warning on every readiness poll, which buries
// the measurements in a wall of identical lines.
var clientTLSOnce struct {
	sync.Once
	cfg *tls.Config
	err error
}

func insecureClientTLS(addr string) (*tls.Config, error) {
	clientTLSOnce.Do(func() {
		clientTLSOnce.cfg, clientTLSOnce.err = transport.MakeClientTLSConfig("", "",
			transport.TrustPolicy{Insecure: &transport.InsecureTrust{Reason: "bench/bucketdensity harness against a local throwaway server"}}, addr)
	})
	if clientTLSOnce.err != nil {
		return nil, fmt.Errorf("building client TLS config for %s: %w", addr, clientTLSOnce.err)
	}
	return clientTLSOnce.cfg, nil
}

func dialAndAuth(cfg runConfig) (*serverConn, error) {
	tlsCfg, err := insecureClientTLS(cfg.Addr)
	if err != nil {
		return nil, err
	}
	conn, err := tls.Dial("tcp", cfg.Addr, tlsCfg)
	if err != nil {
		return nil, fmt.Errorf("dialling %s: %w", cfg.Addr, err)
	}
	sc := &serverConn{conn: conn, reader: bufio.NewReader(conn)}
	if err := sc.call(transport.OpAuth, &server.LoginRequest{Username: cfg.Username, Password: cfg.Password}); err != nil {
		_ = sc.close()
		return nil, fmt.Errorf("authenticating as %q: %w", cfg.Username, err)
	}
	return sc, nil
}

func (s *serverConn) close() error { return s.conn.Close() }

// call sends one request and waits for its response. It deliberately does not
// pipeline: the harness is measuring the server's resting cost per bucket, and
// a pipelined burst would measure its queueing behaviour instead.
func (s *serverConn) call(op transport.Opcode, request interface{}) error {
	payload, err := bson.Marshal(request)
	if err != nil {
		return fmt.Errorf("encoding %s request: %w", op, err)
	}
	// The deadline is per call, not per connection.
	//
	// A single long deadline on the connection is the same unbounded wait with
	// a timestamp on it, and it cost a run: at the descriptor limit the server
	// stayed up but stopped answering, and the next call blocked in
	// DecodeFrame until a six-hour connection deadline. A request that a
	// healthy server answers in tens of milliseconds has no business waiting
	// longer than this, and a server that has stopped answering is the result,
	// not an error to wait out.
	if err := s.conn.SetDeadline(time.Now().Add(callTimeout)); err != nil {
		return fmt.Errorf("setting the deadline for %s: %w", op, err)
	}
	if _, err := s.conn.Write(transport.NewFrame(op, payload).Encode()); err != nil {
		return fmt.Errorf("sending %s: %w", op, err)
	}
	frame, err := transport.DecodeFrame(s.reader)
	if err != nil {
		return fmt.Errorf("reading the response to %s: %w", op, err)
	}
	if frame.Opcode == transport.OpError {
		return fmt.Errorf("%s refused: %s", op, frame.Payload)
	}
	var reply struct {
		Success bool   `bson:"success"`
		Message string `bson:"message"`
	}
	if len(frame.Payload) > 0 && bson.Unmarshal(frame.Payload, &reply) == nil && !reply.Success && reply.Message != "" {
		return fmt.Errorf("%s failed: %s", op, reply.Message)
	}
	return nil
}

// --- /proc and filesystem readings ------------------------------------------

// procStatusField reads one numeric field out of /proc/<pid>/status. Both
// callers want a plain number and neither wants to own the parsing.
func procStatusField(pid int, field string) (int64, error) {
	statusPath := fmt.Sprintf("/proc/%d/status", pid)
	raw, err := os.ReadFile(statusPath)
	if err != nil {
		return 0, fmt.Errorf("reading %s: %w", statusPath, err)
	}
	prefix := field + ":"
	for _, line := range strings.Split(string(raw), "\n") {
		if !strings.HasPrefix(line, prefix) {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			return 0, fmt.Errorf("%s has an unparseable %s line: %q", statusPath, field, line)
		}
		parsed, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			return 0, fmt.Errorf("parsing %s from %s: %w", field, statusPath, err)
		}
		return parsed, nil
	}
	return 0, fmt.Errorf("%s has no %s line", statusPath, field)
}

func readRSS(pid int) (int64, error) {
	kilobytes, err := procStatusField(pid, "VmRSS")
	if err != nil {
		return 0, err
	}
	return kilobytes * 1024, nil
}

// readProcess collects the whole per-observation picture in one place.
//
// RSS alone understates a bucket badly. Badger maps its value log and memtable
// as sparse files, so a bucket's cost shows up in four separate accounts: the
// resident pages (RSS), the address space the mappings consume (VmSize, which
// is finite and far smaller than it looks on x86-64), the page tables the
// kernel builds to describe them (VmPTE, charged to the kernel and absent from
// RSS), and the number of distinct mappings (against vm.max_map_count). Any
// one of them can be the wall, so all four are recorded.
func readProcess(pid int) (processReading, error) {
	var reading processReading
	var err error
	if reading.RSSBytes, err = readRSS(pid); err != nil {
		return reading, err
	}
	virtualKB, err := procStatusField(pid, "VmSize")
	if err != nil {
		return reading, err
	}
	reading.VirtualBytes = virtualKB * 1024
	pageTableKB, err := procStatusField(pid, "VmPTE")
	if err != nil {
		return reading, err
	}
	reading.PageTableBytes = pageTableKB * 1024
	// VmSwap is absent on a kernel built without swap; zero is the honest
	// reading there, so its absence is not an error.
	if swapKB, swapErr := procStatusField(pid, "VmSwap"); swapErr == nil {
		reading.SwapBytes = swapKB * 1024
	}
	threads, err := procStatusField(pid, "Threads")
	if err != nil {
		return reading, err
	}
	reading.Threads = int(threads)
	if reading.FileDescriptors, err = countOpenFDs(pid); err != nil {
		return reading, err
	}
	if reading.Mappings, err = countMappings(pid); err != nil {
		return reading, err
	}
	return reading, nil
}

// processReading is one look at the server process.
type processReading struct {
	RSSBytes        int64 `json:"rss_bytes"`
	VirtualBytes    int64 `json:"virtual_bytes"`
	PageTableBytes  int64 `json:"page_table_bytes"`
	SwapBytes       int64 `json:"swap_bytes"`
	Threads         int   `json:"os_threads"`
	FileDescriptors int   `json:"file_descriptors"`
	Mappings        int   `json:"mappings"`
}

// countMappings counts the entries in /proc/<pid>/maps, which is what
// vm.max_map_count bounds. Badger's two sparse files per instance make this a
// per-bucket cost, and the default ceiling of 65530 is low enough to matter.
func countMappings(pid int) (int, error) {
	mapsPath := fmt.Sprintf("/proc/%d/maps", pid)
	raw, err := os.ReadFile(mapsPath)
	if err != nil {
		return 0, fmt.Errorf("reading %s: %w", mapsPath, err)
	}
	return strings.Count(strings.TrimRight(string(raw), "\n"), "\n") + 1, nil
}

// readSysctlInt reads a single-integer sysctl such as vm/max_map_count.
func readSysctlInt(name string) (int64, error) {
	path := filepath.Join("/proc/sys", name)
	raw, err := os.ReadFile(path)
	if err != nil {
		return 0, fmt.Errorf("reading %s: %w", path, err)
	}
	parsed, err := strconv.ParseInt(strings.TrimSpace(string(raw)), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parsing %s: %w", path, err)
	}
	return parsed, nil
}

func countOpenFDs(pid int) (int, error) {
	fdDir := fmt.Sprintf("/proc/%d/fd", pid)
	entries, err := os.ReadDir(fdDir)
	if err != nil {
		return 0, fmt.Errorf("listing %s: %w", fdDir, err)
	}
	return len(entries), nil
}

func memAvailableBytes() (int64, error) {
	raw, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0, fmt.Errorf("reading /proc/meminfo: %w", err)
	}
	for _, line := range strings.Split(string(raw), "\n") {
		if !strings.HasPrefix(line, "MemAvailable:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			return 0, fmt.Errorf("/proc/meminfo has an unparseable MemAvailable line: %q", line)
		}
		kilobytes, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			return 0, fmt.Errorf("parsing MemAvailable: %w", err)
		}
		return kilobytes * 1024, nil
	}
	return 0, errors.New("/proc/meminfo has no MemAvailable line")
}

func mustMemAvailable() int64 {
	avail, err := memAvailableBytes()
	if err != nil {
		return 0
	}
	return avail
}

func memTotalBytes() (int64, error) {
	raw, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0, fmt.Errorf("reading /proc/meminfo: %w", err)
	}
	for _, line := range strings.Split(string(raw), "\n") {
		if !strings.HasPrefix(line, "MemTotal:") {
			continue
		}
		fields := strings.Fields(line)
		kilobytes, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			return 0, fmt.Errorf("parsing MemTotal: %w", err)
		}
		return kilobytes * 1024, nil
	}
	return 0, errors.New("/proc/meminfo has no MemTotal line")
}

func freeDiskBytes(path string) (int64, error) {
	probe := path
	for {
		if _, err := os.Stat(probe); err == nil {
			break
		}
		parent := filepath.Dir(probe)
		if parent == probe {
			return 0, fmt.Errorf("no existing ancestor of %q to stat for free space", path)
		}
		probe = parent
	}
	var stat syscall.Statfs_t
	if err := syscall.Statfs(probe, &stat); err != nil {
		return 0, fmt.Errorf("statfs %q: %w", probe, err)
	}
	return int64(stat.Bavail) * int64(stat.Bsize), nil
}

// refuseTmpfs is a guard, not defensive code: tmpfs is RAM, so a data directory
// on one would make every disk byte also a memory byte and silently invalidate
// the whole measurement.
func refuseTmpfs(dataDir string) error {
	fsType, err := filesystemFor(dataDir)
	if err != nil {
		return err
	}
	if fsType == "tmpfs" || fsType == "ramfs" {
		return fmt.Errorf("-data-dir %q is on %s, which is RAM: the disk figures would be memory figures and the memory figures would be wrong. Point -data-dir at a real disk (for example %s/hoarddb-density)",
			dataDir, fsType, os.Getenv("HOME"))
	}
	return nil
}

// filesystemFor finds the mount whose point is the longest prefix of path.
func filesystemFor(path string) (string, error) {
	raw, err := os.ReadFile("/proc/mounts")
	if err != nil {
		return "", fmt.Errorf("reading /proc/mounts: %w", err)
	}
	bestPoint, bestType := "", ""
	for _, line := range strings.Split(string(raw), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 3 {
			continue
		}
		point, fsType := fields[1], fields[2]
		if point != "/" && !strings.HasPrefix(path, point+"/") && path != point {
			continue
		}
		if len(point) >= len(bestPoint) {
			bestPoint, bestType = point, fsType
		}
	}
	if bestType == "" {
		return "", fmt.Errorf("no mount in /proc/mounts covers %q", path)
	}
	return bestType, nil
}

// lowerOpenFileLimit is deliberately NOT syscall.Setrlimit.
//
// Setting RLIMIT_NOFILE in this process does not reach the server, and the
// failure is silent: since Go 1.19 the runtime raises the soft limit to the
// hard limit at startup and records the original, and os/exec then restores
// that recorded original in every child it spawns — so a Setrlimit here is
// overwritten on exec. The first attempt at the descriptor experiment reached
// 12009 descriptors under a nominal limit of 4096 and reported success.
//
// The limit therefore has to be applied in the child, between fork and exec,
// which os/exec offers no hook for. A shell does: ulimit runs in the child,
// and exec replaces it so no extra process survives to distort the
// measurement.
func serverCommand(cfg runConfig) *exec.Cmd {
	if cfg.FDLimit == 0 {
		return exec.Command(cfg.ServerBin)
	}
	return exec.Command("/bin/sh", "-c",
		fmt.Sprintf("ulimit -n %d && exec \"$0\"", cfg.FDLimit), cfg.ServerBin)
}

// verifyOpenFileLimit reads the limit the server actually got, so a run that
// silently failed to lower it cannot be mistaken for one that did.
func verifyOpenFileLimit(pid int) (uint64, error) {
	limitsPath := fmt.Sprintf("/proc/%d/limits", pid)
	raw, err := os.ReadFile(limitsPath)
	if err != nil {
		return 0, fmt.Errorf("reading %s: %w", limitsPath, err)
	}
	for _, line := range strings.Split(string(raw), "\n") {
		if !strings.HasPrefix(line, "Max open files") {
			continue
		}
		fields := strings.Fields(strings.TrimPrefix(line, "Max open files"))
		if len(fields) == 0 {
			return 0, fmt.Errorf("%s has an unparseable open-files line: %q", limitsPath, line)
		}
		soft, err := strconv.ParseUint(fields[0], 10, 64)
		if err != nil {
			return 0, fmt.Errorf("parsing the soft open-file limit from %s: %w", limitsPath, err)
		}
		return soft, nil
	}
	return 0, fmt.Errorf("%s has no open-files line", limitsPath)
}

// measureDisk walks the data directory once and attributes every file to the
// component that owns it. Both apparent size and allocated blocks are kept
// because Badger's preallocated value log makes them disagree by two orders of
// magnitude.
func measureDisk(dataDir string) (diskBreakdown, error) {
	var breakdown diskBreakdown
	err := filepath.WalkDir(dataDir, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return fmt.Errorf("walking %q: %w", path, walkErr)
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil // Badger rotated a file out from under the walk
			}
			return fmt.Errorf("stat %q: %w", path, err)
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok {
			return fmt.Errorf("stat %q returned no syscall.Stat_t; allocated blocks are unreadable", path)
		}
		usage := diskUsage{
			ApparentBytes:  info.Size(),
			AllocatedBytes: stat.Blocks * bytesPerBlock,
			FileCount:      1,
		}
		breakdown.Total.add(usage)

		relative, err := filepath.Rel(dataDir, path)
		if err != nil {
			return fmt.Errorf("relativising %q: %w", path, err)
		}
		switch {
		case filepath.Base(relative) == "key.wrapped":
			breakdown.KeyFile.add(usage)
		case pathHasSegment(relative, "badger"):
			breakdown.Badger.add(usage)
		case pathHasSegment(relative, "indexes"):
			breakdown.Indexes.add(usage)
		case pathHasSegment(relative, "blobs"):
			breakdown.Blobs.add(usage)
		default:
			breakdown.Other.add(usage)
		}
		return nil
	})
	if err != nil {
		return breakdown, err
	}
	return breakdown, nil
}

func pathHasSegment(relative, segment string) bool {
	for _, part := range strings.Split(relative, string(filepath.Separator)) {
		if part == segment {
			return true
		}
	}
	return false
}

func collectHostFacts(dataDir string) (hostFacts, error) {
	facts := hostFacts{CPUCount: runtime.NumCPU()}

	if kernel, err := os.ReadFile("/proc/sys/kernel/osrelease"); err == nil {
		facts.Kernel = strings.TrimSpace(string(kernel))
	}
	if cpuinfo, err := os.ReadFile("/proc/cpuinfo"); err == nil {
		for _, line := range strings.Split(string(cpuinfo), "\n") {
			if strings.HasPrefix(line, "model name") {
				if _, value, found := strings.Cut(line, ":"); found {
					facts.CPUModel = strings.TrimSpace(value)
				}
				break
			}
		}
	}
	total, err := memTotalBytes()
	if err != nil {
		return facts, err
	}
	facts.MemTotalBytes = total
	avail, err := memAvailableBytes()
	if err != nil {
		return facts, err
	}
	facts.MemAvailBytes = avail
	fsType, err := filesystemFor(dataDir)
	if err != nil {
		return facts, err
	}
	facts.DataDirFS = fsType
	free, err := freeDiskBytes(dataDir)
	if err != nil {
		return facts, err
	}
	facts.DataDirFreeByte = free
	var limit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &limit); err != nil {
		return facts, fmt.Errorf("reading RLIMIT_NOFILE: %w", err)
	}
	// Rlimit.Cur is int64 on freebsd, where infinity is -1; uint64(-1) is MaxUint64, the value linux reports for the same infinity.
	facts.OpenFileLimit = uint64(limit.Cur)
	maxMaps, err := readSysctlInt("vm/max_map_count")
	if err != nil {
		return facts, err
	}
	facts.MaxMapCount = maxMaps
	return facts, nil
}

// --- reporting --------------------------------------------------------------

func printHostFacts(facts hostFacts) {
	fmt.Println("host")
	fmt.Printf("  cpu            %s (%d logical)\n", facts.CPUModel, facts.CPUCount)
	fmt.Printf("  kernel         %s\n", facts.Kernel)
	fmt.Printf("  memory         %s total, %s available at start\n", humanBytes(facts.MemTotalBytes), humanBytes(facts.MemAvailBytes))
	fmt.Printf("  data dir fs    %s, %s free at start\n", facts.DataDirFS, humanBytes(facts.DataDirFreeByte))
	fmt.Printf("  RLIMIT_NOFILE  %d\n", facts.OpenFileLimit)
	fmt.Printf("  max_map_count  %d (the Linux default is 65530; a raised value moves the mapping wall)\n", facts.MaxMapCount)
}

func printScaleResult(measured scaleResult) {
	fmt.Printf("\nN=%d -> %s (%d buckets)\n", measured.Requested, measured.Outcome, measured.Reached)
	if measured.Detail != "" {
		fmt.Printf("  detail            %s\n", measured.Detail)
	}
	fmt.Printf("  rss               %s baseline -> %s final (peak %s)\n",
		humanBytes(measured.Baseline.RSSBytes), humanBytes(measured.Final.RSSBytes), humanBytes(measured.PeakRSSBytes))
	fmt.Printf("  virtual / pte     %s -> %s virtual | %s -> %s page tables | %s swapped\n",
		humanBytes(measured.Baseline.VirtualBytes), humanBytes(measured.Final.VirtualBytes),
		humanBytes(measured.Baseline.PageTableBytes), humanBytes(measured.Final.PageTableBytes),
		humanBytes(measured.Final.SwapBytes))
	fmt.Printf("  file descriptors  %d baseline -> %d final (peak %d)\n",
		measured.Baseline.FileDescriptors, measured.Final.FileDescriptors, measured.PeakFDs)
	fmt.Printf("  mappings          %d baseline -> %d final\n", measured.Baseline.Mappings, measured.Final.Mappings)
	fmt.Printf("  os threads        %d baseline -> %d final\n", measured.Baseline.Threads, measured.Final.Threads)
	fmt.Printf("  disk apparent     total %s | badger %s | indexes %s | blobs %s | key.wrapped %s | other %s\n",
		humanBytes(measured.Disk.Total.ApparentBytes), humanBytes(measured.Disk.Badger.ApparentBytes),
		humanBytes(measured.Disk.Indexes.ApparentBytes), humanBytes(measured.Disk.Blobs.ApparentBytes),
		humanBytes(measured.Disk.KeyFile.ApparentBytes), humanBytes(measured.Disk.Other.ApparentBytes))
	fmt.Printf("  disk allocated    total %s | badger %s | indexes %s | blobs %s | key.wrapped %s | other %s\n",
		humanBytes(measured.Disk.Total.AllocatedBytes), humanBytes(measured.Disk.Badger.AllocatedBytes),
		humanBytes(measured.Disk.Indexes.AllocatedBytes), humanBytes(measured.Disk.Blobs.AllocatedBytes),
		humanBytes(measured.Disk.KeyFile.AllocatedBytes), humanBytes(measured.Disk.Other.AllocatedBytes))
	fmt.Printf("  files on disk     %d\n", measured.Disk.Total.FileCount)
	fmt.Printf("  timings           cold start %dms | create all %dms | one more create %dus | restart ready %dms | catalog load %dms (%d buckets)\n",
		measured.ColdStartMillis, measured.CreateAllMillis, measured.OneMoreCreateMicros,
		measured.RestartReadyMillis, measured.CatalogLoadMillis, measured.CatalogBucketsLoaded)
}

// printMarginals reports the slope between consecutive completed steps. The
// marginal cost is the honest per-bucket number: the absolute total at any N
// still carries the server's own fixed cost.
func printMarginals(results []scaleResult) {
	fmt.Println("\nmarginal cost per bucket (slope between consecutive steps)")
	fmt.Println("  from ->    to |    rss/bucket |   virt/bucket |    pte/bucket | fds/bkt | maps/bkt | alloc disk/bkt")
	previous := scaleResult{Reached: 0}
	haveBaseline := false
	for _, current := range results {
		// A step whose server was killed has a zero final reading, and a slope
		// computed against zero prints as a negative per-bucket cost — a number
		// that looks like a measurement and is not one.
		if current.Reached == 0 || current.Final.RSSBytes == 0 {
			continue
		}
		if !haveBaseline {
			fmt.Printf("  %5d -> %5d | %13s | %13s | %13s | %7.2f | %8.2f | %14s\n",
				0, current.Reached,
				perBucket(current.Final.RSSBytes-current.Baseline.RSSBytes, current.Reached),
				perBucket(current.Final.VirtualBytes-current.Baseline.VirtualBytes, current.Reached),
				perBucket(current.Final.PageTableBytes-current.Baseline.PageTableBytes, current.Reached),
				float64(current.Final.FileDescriptors-current.Baseline.FileDescriptors)/float64(current.Reached),
				float64(current.Final.Mappings-current.Baseline.Mappings)/float64(current.Reached),
				perBucket(current.Disk.Total.AllocatedBytes, current.Reached))
			previous, haveBaseline = current, true
			continue
		}
		delta := current.Reached - previous.Reached
		if delta <= 0 {
			continue
		}
		fmt.Printf("  %5d -> %5d | %13s | %13s | %13s | %7.2f | %8.2f | %14s\n",
			previous.Reached, current.Reached,
			perBucket(current.Final.RSSBytes-previous.Final.RSSBytes, delta),
			perBucket(current.Final.VirtualBytes-previous.Final.VirtualBytes, delta),
			perBucket(current.Final.PageTableBytes-previous.Final.PageTableBytes, delta),
			float64(current.Final.FileDescriptors-previous.Final.FileDescriptors)/float64(delta),
			float64(current.Final.Mappings-previous.Final.Mappings)/float64(delta),
			perBucket(current.Disk.Total.AllocatedBytes-previous.Disk.Total.AllocatedBytes, delta))
		previous = current
	}
}

func perBucket(total int64, buckets int) string {
	if buckets == 0 {
		return "n/a"
	}
	return humanBytes(total / int64(buckets))
}

func humanBytes(n int64) string {
	sign := ""
	if n < 0 {
		sign, n = "-", -n
	}
	switch {
	case n >= bytesPerGB:
		return fmt.Sprintf("%s%.2f GB", sign, float64(n)/bytesPerGB)
	case n >= bytesPerMB:
		return fmt.Sprintf("%s%.2f MB", sign, float64(n)/bytesPerMB)
	case n >= 1024:
		return fmt.Sprintf("%s%.2f KB", sign, float64(n)/1024)
	default:
		return fmt.Sprintf("%s%d B", sign, n)
	}
}

func parseCounts(raw string) ([]int, error) {
	parts := strings.Split(raw, ",")
	counts := make([]int, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			continue
		}
		n, err := strconv.Atoi(trimmed)
		if err != nil {
			return nil, fmt.Errorf("%q is not a number", trimmed)
		}
		if n <= 0 {
			return nil, fmt.Errorf("%d is not a usable bucket count", n)
		}
		counts = append(counts, n)
	}
	if len(counts) == 0 {
		return nil, errors.New("no counts given")
	}
	return counts, nil
}
