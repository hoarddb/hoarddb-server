package main

// The open-cost mode answers a different question from the density mode above:
// not "what does a bucket cost while it is open", but "what does it cost to
// open one that is not". The density measurement priced a node that opens its
// whole catalog eagerly (server/catalog.go:LoadCatalog -> server/service.go:
// Server.CreateBucket). If opening is cheap and O(1), a node could hold a
// catalog it never opens and pay only on a miss; if opening is expensive or
// O(data), it could not. That is VER-19.
//
// Two things about this mode differ from the density mode and are load-bearing:
//
//  1. The measured process is THIS one. The density mode reads /proc/<server
//     pid> precisely so the harness's own allocations never count; here the
//     harness is the thing doing the opening, so it reads /proc/self. Every
//     figure is a delta around a single open, not an absolute.
//  2. The server must be stopped. Badger holds a LOCK per directory, so a
//     bucket cannot be opened here while the server that created it holds it.
//     Seeding and measuring are therefore separate phases, and the seed phase
//     writes a manifest so the measure phase can be re-run on its own.

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"

	"github.com/hoarddb/hoarddb-server/bench/bucketdensity/legacyblob"
	"github.com/hoarddb/hoarddb-server/bench/bucketdensity/legacyqueue"
	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

const (
	// openCostDatabase is the single database every seeded bucket lives in, so
	// the catalog this measures has the shape a real node's would.
	openCostDatabase = "OpenCost"
	// seedBatchSize bounds one OpBatchUpsert. Seeding 100k entries one round trip
	// at a time is the slowest part of the run by an order of magnitude.
	seedBatchSize = 500
	// manifestName lets -measure run against a corpus an earlier -seed left.
	manifestName = "opencost-manifest.json"
	// openCostShuffleSeed is fixed so two runs over the same corpus open the
	// same buckets in the same order and can be compared.
	openCostShuffleSeed = 20260922
	// indexedFieldCount is how many secondary indexes the indexed comparison
	// bucket declares. Each one is its own Badger instance (store/index.go:
	// NewSecondaryIndex), which is the multiplier being priced.
	indexedFieldCount = 2
)

// openCostConfig is the knob set for this mode. It is a struct for the same
// reason runConfig is: the parameter list crossed six immediately.
type openCostConfig struct {
	Population     int
	SizeCurve      []int
	SizeReplicates int
	EngineEntries  int
	ConcurrentOpen int
	IdleHold       time.Duration
	DirtyWrites    int
	LeverInstances int
	DoSeed         bool
	DoMeasure      bool
}

// seededBucket is one bucket the seed phase created, named well enough for the
// measure phase to open its engine directly.
type seededBucket struct {
	Name    string `json:"name"`
	Type    string `json:"type"`
	Entries int    `json:"entries"`
	Indexes int    `json:"indexes"`
	Dir     string `json:"dir"`
}

// seedManifest is the handover between the two phases, and the record of what
// the eager start on this corpus actually cost.
type seedManifest struct {
	DataDir     string         `json:"data_dir"`
	SeededAt    time.Time      `json:"seeded_at"`
	Population  []seededBucket `json:"population"`
	SizeCurve   []seededBucket `json:"size_curve"`
	Engines     []seededBucket `json:"engines"`
	SeedSeconds float64        `json:"seed_seconds"`

	CatalogPath  string `json:"catalog_path"`
	CatalogBytes int64  `json:"catalog_bytes"`

	// The eager-start measurement, from a restart on the seeded directory: what
	// LoadCatalog pays to open every bucket before the node serves anything.
	EagerStartSeconds   float64        `json:"eager_start_seconds"`
	EagerStartBuckets   int            `json:"eager_start_buckets_loaded"`
	EagerStartReading   processReading `json:"eager_start_process"`
	EagerStartIO        processIO      `json:"eager_start_io"`
	EagerStopSeconds    float64        `json:"eager_stop_seconds"`
	CatalogLoadLogMilli int64          `json:"catalog_load_ms_from_log"`
}

// selfReading is one look at this process across every account an open charges.
//
// CPUNanos is the one that decides the miss arithmetic: if an open is CPU time
// rather than I/O wait, a node cannot hide it behind concurrency, and the
// ceiling on opens per second is the core count divided by the CPU each open
// costs. It is user+system time from getrusage, not a clock-tick reading from
// /proc/<pid>/stat, so it needs no assumption about _SC_CLK_TCK.
type selfReading struct {
	Process    processReading `json:"process"`
	IO         processIO      `json:"io"`
	Goroutines int            `json:"goroutines"`
	CPUNanos   int64          `json:"cpu_ns"`
}

func readSelf() (selfReading, error) {
	pid := os.Getpid()
	process, err := readProcess(pid)
	if err != nil {
		return selfReading{}, err
	}
	counters, err := readProcessIO(pid)
	if err != nil {
		return selfReading{}, err
	}
	cpu, err := selfCPUNanos()
	if err != nil {
		return selfReading{}, err
	}
	return selfReading{
		Process:    process,
		IO:         counters,
		Goroutines: runtime.NumGoroutine(),
		CPUNanos:   cpu,
	}, nil
}

// selfCPUNanos is user plus system CPU time for this process and every thread
// in it, which is what a Badger open actually spends.
func selfCPUNanos() (int64, error) {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return 0, fmt.Errorf("reading this process's CPU time: %w", err)
	}
	return usage.Utime.Nano() + usage.Stime.Nano(), nil
}

// openObservation is one open/close cycle, split into the three steps a server
// actually performs so the report can say which one the time is in.
type openObservation struct {
	Bucket  string `json:"bucket"`
	Entries int    `json:"entries"`
	Cold    bool   `json:"cold"`

	// EngineMillis is storage.OpenBadger (or NewSegmentStore / NewBlobStore).
	EngineMillis float64 `json:"engine_open_ms"`
	// StoreMillis is what the server builds on top of the engine before the
	// bucket is usable: HashStore.Recount, the blobmap scan, the index manager.
	StoreMillis float64 `json:"store_open_ms"`
	CloseMillis float64 `json:"close_ms"`

	DeviceReadBytes  int64 `json:"device_read_bytes"`
	DeviceWriteBytes int64 `json:"device_write_bytes"`
	// OpenCPUNanos is CPU time charged between the reading before the open and
	// the reading while it is open.
	OpenCPUNanos int64 `json:"open_cpu_ns"`

	FDDelta        int   `json:"fd_delta"`
	MappingDelta   int   `json:"mapping_delta"`
	RSSDelta       int64 `json:"rss_delta_bytes"`
	VirtualDelta   int64 `json:"virtual_delta_bytes"`
	GoroutineDelta int   `json:"goroutine_delta"`
}

func (o openObservation) totalOpen() time.Duration {
	return time.Duration((o.EngineMillis + o.StoreMillis) * float64(time.Millisecond))
}

// hostGuard stops a phase when the host is running out of memory. The density
// mode's watchdog kills a child process; there is no child here, so this one
// raises a flag the phase loops check and the run ends with what it has.
type hostGuard struct {
	tripped atomic.Bool
	reason  atomic.Value
	done    chan struct{}
}

func startHostGuard(floorBytes int64) *hostGuard {
	guard := &hostGuard{done: make(chan struct{})}
	go func() {
		const pollEvery = 500 * time.Millisecond
		for {
			select {
			case <-guard.done:
				return
			case <-time.After(pollEvery):
			}
			avail, err := memAvailableBytes()
			if err != nil || avail >= floorBytes {
				continue
			}
			reason := fmt.Sprintf("host guard: MemAvailable fell to %s, below the %s floor; the phase was cut short and reports what it reached",
				humanBytes(avail), humanBytes(floorBytes))
			guard.reason.Store(reason)
			guard.tripped.Store(true)
			fmt.Printf("\n!! %s\n", reason)
			return
		}
	}()
	return guard
}

func (g *hostGuard) stop() {
	select {
	case <-g.done:
	default:
		close(g.done)
	}
}

func (g *hostGuard) trippedReason() string {
	if !g.tripped.Load() {
		return ""
	}
	stored, ok := g.reason.Load().(string)
	if !ok {
		return "host guard tripped"
	}
	return stored
}

// --- the report -------------------------------------------------------------

type openCostReport struct {
	Host     hostFacts      `json:"host"`
	Floors   guardFloors    `json:"guard_floors"`
	Manifest seedManifest   `json:"manifest"`
	Config   openCostConfig `json:"config"`
	// MemTableMB is the memtable every engine open in this report ran at. The
	// lever phase below records its per-arm sizes too; this is the one every
	// other phase shares, so no figure here can be read at the wrong size.
	MemTableMB int `json:"memtable_mb"`

	ZeroOpen  zeroOpenBaseline `json:"zero_open_baseline"`
	ColdOpens phaseSummary     `json:"cold_opens"`
	// OpenDescriptors names the descriptors one open bucket holds. VER-17
	// counted six on a bucket that had just been created; this counts what a
	// bucket holds after it has been closed and reopened, which is the state a
	// lazy-open server would always be in.
	OpenDescriptors namedDescriptors  `json:"open_descriptors"`
	WarmReopens     phaseSummary      `json:"warm_reopens"`
	SizeCurve       []sizeCurvePoint  `json:"size_curve"`
	Concurrent      concurrentSummary `json:"concurrent_open"`
	CloseCost       closeCostSummary  `json:"close_cost"`
	Engines         []enginePoint     `json:"engines"`
	Lever           []leverPoint      `json:"constant_lever"`

	CutShort string `json:"cut_short,omitempty"`
}

// zeroOpenBaseline is what a start that opens nothing would still pay: read and
// parse the catalog, then stat each bucket directory to confirm it is there.
type namedDescriptors struct {
	Bucket string   `json:"bucket"`
	Count  int      `json:"count"`
	Paths  []string `json:"paths"`
}

type zeroOpenBaseline struct {
	CatalogBytes        int64   `json:"catalog_bytes"`
	CatalogBuckets      int     `json:"catalog_buckets"`
	ColdReadMillis      float64 `json:"cold_read_ms"`
	ColdReadDeviceBytes int64   `json:"cold_read_device_bytes"`
	ParseMillis         float64 `json:"parse_ms"`
	StatAllMillis       float64 `json:"stat_all_buckets_ms"`
	StatPerBucketMicros float64 `json:"per_bucket_stat_us"`
}

type phaseSummary struct {
	Buckets          int         `json:"buckets"`
	Engine           statSummary `json:"engine_open_ms"`
	Store            statSummary `json:"store_open_ms"`
	Total            statSummary `json:"total_open_ms"`
	Close            statSummary `json:"close_ms"`
	MedianReadBytes  int64       `json:"median_device_read_bytes"`
	MedianWriteBytes int64       `json:"median_device_write_bytes"`
	MedianFDDelta    int         `json:"median_fd_delta"`
	MedianMapDelta   int         `json:"median_mapping_delta"`
	MedianRSSDelta   int64       `json:"median_rss_delta_bytes"`
	MedianVirtDelta  int64       `json:"median_virtual_delta_bytes"`
	MedianGoroutines int         `json:"median_goroutine_delta"`
	MedianOpenCPUNs  int64       `json:"median_open_cpu_ns"`
}

type sizeCurvePoint struct {
	Entries    int          `json:"entries"`
	Cold       phaseSummary `json:"cold"`
	Warm       phaseSummary `json:"warm"`
	DiskBytes  int64        `json:"bucket_allocated_bytes"`
	Replicates int          `json:"replicates"`
}

type concurrentSummary struct {
	Opened      int         `json:"opened"`
	Before      selfReading `json:"before"`
	AfterOpen   selfReading `json:"after_open"`
	AfterIdle   selfReading `json:"after_idle"`
	AfterClose  selfReading `json:"after_close"`
	IdleSeconds float64     `json:"idle_seconds"`
	// Open is the per-open duration while instances ACCUMULATE rather than
	// being opened and closed one at a time. It is the number a lazy-open
	// server's miss would actually pay: a process already holding a working
	// set, opening one more.
	Open        statSummary    `json:"open_ms_while_accumulating"`
	OpenCPUNs   int64          `json:"median_open_cpu_ns"`
	ColdOpened  bool           `json:"opened_cold"`
	IdleSamples []idleSample   `json:"idle_samples"`
	Steps       []openStepMark `json:"open_steps"`
	CloseAllMS  float64        `json:"close_all_ms"`
}

type idleSample struct {
	ElapsedSeconds float64     `json:"elapsed_s"`
	Reading        selfReading `json:"reading"`
}

type openStepMark struct {
	Opened  int         `json:"opened"`
	Reading selfReading `json:"reading"`
}

type closeCostSummary struct {
	CleanBuckets     int         `json:"clean_buckets"`
	CleanClose       statSummary `json:"clean_close_ms"`
	CleanWriteBytes  int64       `json:"clean_median_device_write_bytes"`
	DirtyBuckets     int         `json:"dirty_buckets"`
	DirtyWrites      int         `json:"writes_per_dirty_bucket"`
	DirtyClose       statSummary `json:"dirty_close_ms"`
	DirtyWriteBytes  int64       `json:"dirty_median_device_write_bytes"`
	DirtySwapAtClose int64       `json:"process_swap_bytes_after_dirty_closes"`
}

type enginePoint struct {
	Engine      string       `json:"engine"`
	Entries     int          `json:"entries"`
	Indexes     int          `json:"indexes"`
	Cold        phaseSummary `json:"cold"`
	Note        string       `json:"note,omitempty"`
	Descriptors int          `json:"descriptors_while_open"`
}

type leverPoint struct {
	Label            string       `json:"label"`
	ValueLogBytes    int64        `json:"value_log_file_size_bytes"`
	MemTableBytes    int64        `json:"memtable_size_bytes"`
	Cold             phaseSummary `json:"cold"`
	ApparentDiskEach int64        `json:"apparent_bytes_per_instance"`
	// Refused records an arm Badger would not open at all. That is a result:
	// it says the two constants are not independently tunable, which is part
	// of the answer to whether they could be exposed through BadgerTuning.
	Refused string `json:"refused,omitempty"`
}

// --- flags ------------------------------------------------------------------

// openCostFlagSet holds the -mode=opencost knobs. They are registered even when
// the density mode runs, which costs nothing and keeps -help honest about what
// the binary can do.
type openCostFlagSet struct {
	population     *int
	sizeCurve      *string
	sizeReplicates *int
	engineEntries  *int
	concurrentOpen *int
	idleHold       *time.Duration
	dirtyWrites    *int
	leverInstances *int
	skipSeed       *bool
	seedOnly       *bool
}

func registerOpenCostFlags() *openCostFlagSet {
	return &openCostFlagSet{
		population:     flag.Int("population", 1000, "opencost: empty hash buckets to seed and open one at a time for the duration distribution"),
		sizeCurve:      flag.String("entry-sizes", "0,100,1000,10000,100000", "opencost: entry counts for the size curve, ascending"),
		sizeReplicates: flag.Int("size-replicates", 3, "opencost: buckets per size-curve point"),
		engineEntries:  flag.Int("engine-entries", 1000, "opencost: entries in the non-empty fifo/lifo/blob/indexed comparison buckets"),
		concurrentOpen: flag.Int("concurrent-open", 500, "opencost: buckets to hold open at once to measure the working-set footprint"),
		idleHold:       flag.Duration("idle-hold", 60*time.Second, "opencost: how long to hold them open and idle, to catch background compaction"),
		dirtyWrites:    flag.Int("dirty-writes", 5000, "opencost: entries written into a bucket before it is closed, for the dirty-close cost"),
		leverInstances: flag.Int("lever-instances", 50, "opencost: Badger instances per arm of the altered-constants comparison"),
		skipSeed:       flag.Bool("skip-seed", false, "opencost: reuse the corpus and manifest an earlier -seed run left on disk"),
		seedOnly:       flag.Bool("seed-only", false, "opencost: build the corpus and stop, so the measurement can be run separately"),
	}
}

func (f *openCostFlagSet) config() (openCostConfig, error) {
	sizes, err := parseCountsAllowingZero(*f.sizeCurve)
	if err != nil {
		return openCostConfig{}, fmt.Errorf("-entry-sizes: %w", err)
	}
	if *f.skipSeed && *f.seedOnly {
		return openCostConfig{}, errors.New("-skip-seed and -seed-only ask for opposite things; pass at most one")
	}
	return openCostConfig{
		Population:     *f.population,
		SizeCurve:      sizes,
		SizeReplicates: *f.sizeReplicates,
		EngineEntries:  *f.engineEntries,
		ConcurrentOpen: *f.concurrentOpen,
		IdleHold:       *f.idleHold,
		DirtyWrites:    *f.dirtyWrites,
		LeverInstances: *f.leverInstances,
		DoSeed:         !*f.skipSeed,
		DoMeasure:      !*f.seedOnly,
	}, nil
}

// parseCountsAllowingZero is parseCounts with zero permitted: an empty bucket is
// the first and most important point on the size curve, and parseCounts refuses
// zero because a zero-bucket density step is meaningless.
func parseCountsAllowingZero(raw string) ([]int, error) {
	counts := make([]int, 0, 8)
	for _, part := range strings.Split(raw, ",") {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			continue
		}
		parsed, err := strconv.Atoi(trimmed)
		if err != nil {
			return nil, fmt.Errorf("%q is not a number", trimmed)
		}
		if parsed < 0 {
			return nil, fmt.Errorf("%d is not a usable entry count", parsed)
		}
		counts = append(counts, parsed)
	}
	if len(counts) == 0 {
		return nil, errors.New("no entry counts given")
	}
	return counts, nil
}

// --- entry point ------------------------------------------------------------

func runOpenCost(cfg runConfig, oc openCostConfig, outPath string) error {
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

	manifestPath := filepath.Join(cfg.LogDir, manifestName)
	var manifest seedManifest
	if oc.DoSeed {
		manifest, err = seedCorpus(cfg, oc, manifestPath)
		if err != nil {
			return err
		}
	} else {
		manifest, err = loadManifest(manifestPath)
		if err != nil {
			return err
		}
		fmt.Printf("\nreusing the corpus seeded at %s (%d population, %d size-curve, %d engine buckets)\n",
			manifest.SeededAt.Format(time.RFC3339), len(manifest.Population), len(manifest.SizeCurve), len(manifest.Engines))
	}
	if !oc.DoMeasure {
		fmt.Println("\n-seed-only: the corpus is on disk and the manifest is written; stopping before the measurement.")
		return nil
	}

	report := openCostReport{Host: facts, Floors: cfg.Floors, Manifest: manifest, Config: oc, MemTableMB: benchMemTableMB}
	guard := startHostGuard(cfg.Floors.AvailMemoryBytes)
	defer guard.stop()

	if err := measureOpenCost(&report, cfg, oc, guard); err != nil {
		return err
	}
	report.CutShort = guard.trippedReason()

	printOpenCostReport(report)

	if outPath != "" {
		encoded, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			return fmt.Errorf("encoding the open-cost report: %w", err)
		}
		if err := os.WriteFile(outPath, encoded, 0o644); err != nil {
			return fmt.Errorf("writing the open-cost report to %q: %w", outPath, err)
		}
		fmt.Printf("\nJSON report: %s\n", outPath)
	}
	return nil
}

// measureOpenCost runs every phase in turn. Each phase is skipped rather than
// failed once the host guard trips, so a run that runs out of memory still
// reports the phases it completed.
func measureOpenCost(report *openCostReport, cfg runConfig, oc openCostConfig, guard *hostGuard) error {
	manifest := report.Manifest

	fmt.Println("\n--- phase 1: the zero-open baseline (catalog parse + per-bucket stat) ---")
	baseline, err := measureZeroOpen(manifest)
	if err != nil {
		return err
	}
	report.ZeroOpen = baseline
	fmt.Printf("  catalog %s, %d buckets | cold read %.1f ms (%s from the device) | parse %.1f ms | stat all %.1f ms (%.1f us/bucket)\n",
		humanBytes(baseline.CatalogBytes), baseline.CatalogBuckets, baseline.ColdReadMillis,
		humanBytes(baseline.ColdReadDeviceBytes), baseline.ParseMillis, baseline.StatAllMillis, baseline.StatPerBucketMicros)

	// The dirty-close phase writes to the buckets it uses, so it gets its own
	// slice of the population and no other phase touches those. The order is
	// shuffled first so neither slice is a contiguous run of creation order,
	// which is also the order the directories sit in on disk.
	population := append([]seededBucket(nil), manifest.Population...)
	shuffleSeeded(population, rand.New(rand.NewSource(openCostShuffleSeed)))
	dirtyCount := min(32, len(population)/8)
	closeSet := population[len(population)-dirtyCount:]
	openSet := population[:len(population)-dirtyCount]

	named, err := describeOpenDescriptors(openSet[0])
	if err != nil {
		return err
	}
	report.OpenDescriptors = named
	fmt.Printf("\n  one open bucket holds %d descriptors: %s\n", named.Count, strings.Join(named.Paths, ", "))

	fmt.Printf("\n--- phase 2: %d cold opens, one at a time, each followed by a warm reopen ---\n", len(openSet))
	cold, warm, err := measurePopulationOpens(openSet, guard)
	if err != nil {
		return err
	}
	report.ColdOpens = summarise(cold)
	report.WarmReopens = summarise(warm)

	fmt.Println("\n--- phase 3: the size curve — is open O(1) or O(data)? ---")
	curve, err := measureSizeCurve(manifest.SizeCurve, guard)
	if err != nil {
		return err
	}
	report.SizeCurve = curve

	if !guard.tripped.Load() {
		fmt.Printf("\n--- phase 4: %d buckets held open together, then idle for %s ---\n", oc.ConcurrentOpen, oc.IdleHold)
		concurrent, err := measureConcurrentOpen(openSet, oc.ConcurrentOpen, oc.IdleHold, guard)
		if err != nil {
			return err
		}
		report.Concurrent = concurrent
	}

	fmt.Println("\n--- phase 5: close cost, clean and dirty ---")
	closeCost, err := measureCloseCost(closeSet, oc.DirtyWrites)
	if err != nil {
		return err
	}
	report.CloseCost = closeCost

	fmt.Println("\n--- phase 6: the comparison engines ---")
	engines, err := measureEngines(manifest.Engines)
	if err != nil {
		return err
	}
	report.Engines = engines

	fmt.Println("\n--- phase 7: the constant lever (Badger opened with ALTERED options) ---")
	lever, err := measureConstantLever(filepath.Join(cfg.DataDir, "..", "opencost-lever"), oc.LeverInstances)
	if err != nil {
		return err
	}
	report.Lever = lever

	return nil
}

// --- phase 1: the zero-open baseline ----------------------------------------

func measureZeroOpen(manifest seedManifest) (zeroOpenBaseline, error) {
	baseline := zeroOpenBaseline{CatalogBytes: manifest.CatalogBytes}

	if err := evictFileFromPageCache(manifest.CatalogPath); err != nil {
		return baseline, err
	}
	before, err := readSelf()
	if err != nil {
		return baseline, err
	}
	readStart := time.Now()
	raw, err := os.ReadFile(manifest.CatalogPath)
	if err != nil {
		return baseline, fmt.Errorf("reading the catalog %q: %w", manifest.CatalogPath, err)
	}
	baseline.ColdReadMillis = millis(time.Since(readStart))
	afterRead, err := readSelf()
	if err != nil {
		return baseline, err
	}
	baseline.ColdReadDeviceBytes = afterRead.IO.ReadBytes - before.IO.ReadBytes
	baseline.CatalogBytes = int64(len(raw))

	// The catalog is parsed into the shape the server parses it into: a
	// per-database map of per-bucket entries. json.Unmarshal into an
	// interface{} tree is the same work.
	var parsed struct {
		Version   int `json:"version"`
		Databases map[string]struct {
			Buckets map[string]json.RawMessage `json:"buckets"`
		} `json:"databases"`
	}
	parseStart := time.Now()
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return baseline, fmt.Errorf("parsing the catalog %q: %w", manifest.CatalogPath, err)
	}
	baseline.ParseMillis = millis(time.Since(parseStart))
	for _, database := range parsed.Databases {
		baseline.CatalogBuckets += len(database.Buckets)
	}

	everyBucket := allSeeded(manifest)
	statStart := time.Now()
	for _, bucket := range everyBucket {
		if _, err := os.Stat(bucket.Dir); err != nil {
			return baseline, fmt.Errorf("stat %q, which the catalog lists: %w", bucket.Dir, err)
		}
	}
	baseline.StatAllMillis = millis(time.Since(statStart))
	if len(everyBucket) > 0 {
		baseline.StatPerBucketMicros = baseline.StatAllMillis * 1000 / float64(len(everyBucket))
	}
	return baseline, nil
}

// --- phase 2 and 3: opening one bucket --------------------------------------

// openHashBucket runs one open/close cycle on a hash bucket, timing the engine
// open and the store build separately because the server pays both and only one
// of them is expected to be O(1).
func openHashBucket(bucket seededBucket, cold bool) (openObservation, error) {
	observed := openObservation{Bucket: bucket.Name, Entries: bucket.Entries, Cold: cold}
	badgerDir := filepath.Join(bucket.Dir, "badger")

	if cold {
		if _, err := evictTreeFromPageCache(bucket.Dir); err != nil {
			return observed, err
		}
	}
	before, err := readSelf()
	if err != nil {
		return observed, err
	}

	engineStart := time.Now()
	instance, err := storage.OpenBadger(badgerDir, benchTuning(), nil)
	if err != nil {
		return observed, fmt.Errorf("opening bucket %q: %w", bucket.Name, err)
	}
	observed.EngineMillis = millis(time.Since(engineStart))

	storeStart := time.Now()
	counted, err := countLegacyHashEntries(instance, bucket.Name)
	if err != nil {
		_ = instance.Close()
		return observed, fmt.Errorf("recounting bucket %q after open: %w", bucket.Name, err)
	}
	observed.StoreMillis = millis(time.Since(storeStart))
	if int(counted) != bucket.Entries {
		_ = instance.Close()
		return observed, fmt.Errorf("bucket %q holds %d entries, the manifest says %d: the corpus and the manifest disagree",
			bucket.Name, counted, bucket.Entries)
	}

	whileOpen, err := readSelf()
	if err != nil {
		_ = instance.Close()
		return observed, err
	}

	closeStart := time.Now()
	if err := instance.Close(); err != nil {
		return observed, fmt.Errorf("closing bucket %q: %w", bucket.Name, err)
	}
	observed.CloseMillis = millis(time.Since(closeStart))

	afterClose, err := readSelf()
	if err != nil {
		return observed, err
	}

	observed.DeviceReadBytes = whileOpen.IO.ReadBytes - before.IO.ReadBytes
	observed.DeviceWriteBytes = afterClose.IO.WriteBytes - whileOpen.IO.WriteBytes
	observed.FDDelta = whileOpen.Process.FileDescriptors - before.Process.FileDescriptors
	observed.MappingDelta = whileOpen.Process.Mappings - before.Process.Mappings
	observed.RSSDelta = whileOpen.Process.RSSBytes - before.Process.RSSBytes
	observed.VirtualDelta = whileOpen.Process.VirtualBytes - before.Process.VirtualBytes
	observed.GoroutineDelta = whileOpen.Goroutines - before.Goroutines
	observed.OpenCPUNanos = whileOpen.CPUNanos - before.CPUNanos
	return observed, nil
}

// describeOpenDescriptors opens one bucket and names every descriptor that
// appeared, so the per-bucket descriptor count is a list of files rather than
// an unexplained integer.
func describeOpenDescriptors(bucket seededBucket) (namedDescriptors, error) {
	named := namedDescriptors{Bucket: bucket.Name}
	before, err := openDescriptorTargets()
	if err != nil {
		return named, err
	}
	instance, err := storage.OpenBadger(filepath.Join(bucket.Dir, "badger"), benchTuning(), nil)
	if err != nil {
		return named, fmt.Errorf("opening bucket %q to name its descriptors: %w", bucket.Name, err)
	}
	during, err := openDescriptorTargets()
	if err != nil {
		_ = instance.Close()
		return named, err
	}
	if err := instance.Close(); err != nil {
		return named, fmt.Errorf("closing bucket %q after naming its descriptors: %w", bucket.Name, err)
	}
	for target, count := range during {
		if count > before[target] {
			named.Paths = append(named.Paths, fmt.Sprintf("%s x%d", target, count-before[target]))
			named.Count += count - before[target]
		}
	}
	sort.Strings(named.Paths)
	return named, nil
}

// openDescriptorTargets counts this process's open descriptors by the file each
// points at, with the bucket directory stripped so the names are comparable.
func openDescriptorTargets() (map[string]int, error) {
	fdDir := fmt.Sprintf("/proc/%d/fd", os.Getpid())
	entries, err := os.ReadDir(fdDir)
	if err != nil {
		return nil, fmt.Errorf("listing %s: %w", fdDir, err)
	}
	targets := map[string]int{}
	for _, entry := range entries {
		target, err := os.Readlink(filepath.Join(fdDir, entry.Name()))
		if err != nil {
			// The descriptor closed between the listing and the readlink; it
			// is not one of ours either way.
			continue
		}
		targets[filepath.Base(target)]++
	}
	return targets, nil
}

func measurePopulationOpens(population []seededBucket, guard *hostGuard) ([]openObservation, []openObservation, error) {
	cold := make([]openObservation, 0, len(population))
	warm := make([]openObservation, 0, len(population))
	for index, bucket := range population {
		if guard.tripped.Load() {
			break
		}
		coldOpen, err := openHashBucket(bucket, true)
		if err != nil {
			return cold, warm, err
		}
		cold = append(cold, coldOpen)

		// The same bucket again with the page cache left as the cold open
		// warmed it: this is the "second open" question — cheaper because the
		// tables are cached, or dearer because of what the first close wrote.
		warmOpen, err := openHashBucket(bucket, false)
		if err != nil {
			return cold, warm, err
		}
		warm = append(warm, warmOpen)

		if (index+1)%100 == 0 || index+1 == len(population) {
			fmt.Printf("  %5d/%d opened | cold p50 %.2f ms | warm p50 %.2f ms\n",
				index+1, len(population),
				durationStats(totalOpens(cold)).P50Millis,
				durationStats(totalOpens(warm)).P50Millis)
		}
	}
	return cold, warm, nil
}

func measureSizeCurve(curve []seededBucket, guard *hostGuard) ([]sizeCurvePoint, error) {
	bySize := map[int][]seededBucket{}
	sizes := make([]int, 0, len(curve))
	for _, bucket := range curve {
		if _, seen := bySize[bucket.Entries]; !seen {
			sizes = append(sizes, bucket.Entries)
		}
		bySize[bucket.Entries] = append(bySize[bucket.Entries], bucket)
	}
	sort.Ints(sizes)

	points := make([]sizeCurvePoint, 0, len(sizes))
	for _, size := range sizes {
		if guard.tripped.Load() {
			break
		}
		buckets := bySize[size]
		coldObservations := make([]openObservation, 0, len(buckets))
		warmObservations := make([]openObservation, 0, len(buckets))
		var allocated int64
		for _, bucket := range buckets {
			coldOpen, err := openHashBucket(bucket, true)
			if err != nil {
				return points, err
			}
			coldObservations = append(coldObservations, coldOpen)
			warmOpen, err := openHashBucket(bucket, false)
			if err != nil {
				return points, err
			}
			warmObservations = append(warmObservations, warmOpen)
			usage, err := measureDisk(bucket.Dir)
			if err != nil {
				return points, err
			}
			allocated += usage.Total.AllocatedBytes
		}
		point := sizeCurvePoint{
			Entries:    size,
			Cold:       summarise(coldObservations),
			Warm:       summarise(warmObservations),
			DiskBytes:  allocated / int64(len(buckets)),
			Replicates: len(buckets),
		}
		points = append(points, point)
		fmt.Printf("  %7d entries | cold engine %.2f ms + store %.2f ms = %.2f ms | warm %.2f ms | close %.2f ms | %s on disk | %s read from device\n",
			size, point.Cold.Engine.P50Millis, point.Cold.Store.P50Millis, point.Cold.Total.P50Millis,
			point.Warm.Total.P50Millis, point.Cold.Close.P50Millis,
			humanBytes(point.DiskBytes), humanBytes(point.Cold.MedianReadBytes))
	}
	return points, nil
}

// --- phase 4: many open at once, then idle ----------------------------------

func measureConcurrentOpen(population []seededBucket, wanted int, idleHold time.Duration, guard *hostGuard) (concurrentSummary, error) {
	summary := concurrentSummary{}
	if wanted > len(population) {
		wanted = len(population)
	}

	before, err := readSelf()
	if err != nil {
		return summary, err
	}
	summary.Before = before

	open := make([]*storage.BadgerInstance, 0, wanted)
	closeAll := func() {
		for _, instance := range open {
			_ = instance.Close()
		}
	}

	step := wanted / 8
	if step < 1 {
		step = 1
	}
	openDurations := make([]time.Duration, 0, wanted)
	openCPU := make([]int64, 0, wanted)
	summary.ColdOpened = true
	for index := 0; index < wanted; index++ {
		if guard.tripped.Load() {
			break
		}
		// Evicted first, so this is a cold open like the one-at-a-time phase
		// and the only variable between them is whether instances accumulate.
		if _, err := evictTreeFromPageCache(population[index].Dir); err != nil {
			closeAll()
			return summary, err
		}
		beforeOne, err := readSelf()
		if err != nil {
			closeAll()
			return summary, err
		}
		openStart := time.Now()
		instance, err := storage.OpenBadger(filepath.Join(population[index].Dir, "badger"), benchTuning(), nil)
		if err != nil {
			closeAll()
			return summary, fmt.Errorf("opening bucket %q as one of %d held open: %w", population[index].Name, wanted, err)
		}
		openDurations = append(openDurations, time.Since(openStart))
		afterOne, err := readSelf()
		if err != nil {
			closeAll()
			return summary, err
		}
		openCPU = append(openCPU, afterOne.CPUNanos-beforeOne.CPUNanos)
		open = append(open, instance)
		if (index+1)%step != 0 && index+1 != wanted {
			continue
		}
		reading, err := readSelf()
		if err != nil {
			closeAll()
			return summary, err
		}
		summary.Steps = append(summary.Steps, openStepMark{Opened: index + 1, Reading: reading})
		fmt.Printf("  %5d open | rss %9s | virt %9s | fds %6d | maps %6d | goroutines %6d | threads %3d\n",
			index+1, humanBytes(reading.Process.RSSBytes), humanBytes(reading.Process.VirtualBytes),
			reading.Process.FileDescriptors, reading.Process.Mappings, reading.Goroutines, reading.Process.Threads)
	}
	summary.Opened = len(open)
	summary.Open = durationStats(openDurations)
	summary.OpenCPUNs = medianInt64(openCPU)

	afterOpen, err := readSelf()
	if err != nil {
		closeAll()
		return summary, err
	}
	summary.AfterOpen = afterOpen

	// Idle: nothing is read or written, so anything that moves here is
	// background work — Badger's compactors and value-log GC.
	fmt.Printf("  holding %d open and idle for %s\n", summary.Opened, idleHold)
	idleStart := time.Now()
	for time.Since(idleStart) < idleHold {
		time.Sleep(5 * time.Second)
		reading, err := readSelf()
		if err != nil {
			closeAll()
			return summary, err
		}
		summary.IdleSamples = append(summary.IdleSamples, idleSample{
			ElapsedSeconds: time.Since(idleStart).Seconds(),
			Reading:        reading,
		})
		if guard.tripped.Load() {
			break
		}
	}
	summary.IdleSeconds = time.Since(idleStart).Seconds()
	afterIdle, err := readSelf()
	if err != nil {
		closeAll()
		return summary, err
	}
	summary.AfterIdle = afterIdle

	closeStart := time.Now()
	closeAll()
	summary.CloseAllMS = millis(time.Since(closeStart))

	afterClose, err := readSelf()
	if err != nil {
		return summary, err
	}
	summary.AfterClose = afterClose
	return summary, nil
}

// --- phase 5: close cost ----------------------------------------------------

func measureCloseCost(buckets []seededBucket, dirtyWrites int) (closeCostSummary, error) {
	summary := closeCostSummary{DirtyWrites: dirtyWrites}
	half := len(buckets) / 2
	clean, dirty := buckets[:half], buckets[half:]

	cleanObservations := make([]openObservation, 0, len(clean))
	for _, bucket := range clean {
		observed, err := openHashBucket(bucket, true)
		if err != nil {
			return summary, err
		}
		cleanObservations = append(cleanObservations, observed)
	}
	summary.CleanBuckets = len(cleanObservations)
	summary.CleanClose = durationStats(closeDurations(cleanObservations))
	summary.CleanWriteBytes = medianInt64(writeBytes(cleanObservations))

	dirtyObservations := make([]openObservation, 0, len(dirty))
	payload := make([]byte, 256)
	for index := range payload {
		payload[index] = byte('a' + index%26)
	}
	for _, bucket := range dirty {
		observed := openObservation{Bucket: bucket.Name, Entries: bucket.Entries, Cold: true}
		if _, err := evictTreeFromPageCache(bucket.Dir); err != nil {
			return summary, err
		}
		before, err := readSelf()
		if err != nil {
			return summary, err
		}
		engineStart := time.Now()
		instance, err := storage.OpenBadger(filepath.Join(bucket.Dir, "badger"), benchTuning(), nil)
		if err != nil {
			return summary, fmt.Errorf("opening bucket %q for the dirty-close measurement: %w", bucket.Name, err)
		}
		observed.EngineMillis = millis(time.Since(engineStart))

		for write := 0; write < dirtyWrites; write++ {
			key := fmt.Sprintf("dirty:%08d", write)
			if err := instance.Put([]byte(key), payload); err != nil {
				_ = instance.Close()
				return summary, fmt.Errorf("writing entry %d into bucket %q: %w", write, bucket.Name, err)
			}
		}
		beforeClose, err := readSelf()
		if err != nil {
			_ = instance.Close()
			return summary, err
		}
		closeStart := time.Now()
		if err := instance.Close(); err != nil {
			return summary, fmt.Errorf("closing dirty bucket %q: %w", bucket.Name, err)
		}
		observed.CloseMillis = millis(time.Since(closeStart))
		afterClose, err := readSelf()
		if err != nil {
			return summary, err
		}
		observed.DeviceWriteBytes = afterClose.IO.WriteBytes - beforeClose.IO.WriteBytes
		observed.DeviceReadBytes = beforeClose.IO.ReadBytes - before.IO.ReadBytes
		observed.RSSDelta = afterClose.Process.RSSBytes - before.Process.RSSBytes
		dirtyObservations = append(dirtyObservations, observed)
		summary.DirtySwapAtClose = afterClose.Process.SwapBytes
	}
	summary.DirtyBuckets = len(dirtyObservations)
	summary.DirtyClose = durationStats(closeDurations(dirtyObservations))
	summary.DirtyWriteBytes = medianInt64(writeBytes(dirtyObservations))

	fmt.Printf("  clean: %d buckets, close p50 %.2f ms, %s written per close\n",
		summary.CleanBuckets, summary.CleanClose.P50Millis, humanBytes(summary.CleanWriteBytes))
	fmt.Printf("  dirty: %d buckets, %d writes each, close p50 %.2f ms, %s written per close, process swap %s\n",
		summary.DirtyBuckets, summary.DirtyWrites, summary.DirtyClose.P50Millis,
		humanBytes(summary.DirtyWriteBytes), humanBytes(summary.DirtySwapAtClose))
	return summary, nil
}

// --- phase 6: the comparison engines ----------------------------------------

func measureEngines(engines []seededBucket) ([]enginePoint, error) {
	points := make([]enginePoint, 0, len(engines))
	for _, bucket := range engines {
		observed, note, err := openComparisonEngine(bucket)
		if err != nil {
			return points, err
		}
		point := enginePoint{
			Engine:      bucket.Type,
			Entries:     bucket.Entries,
			Indexes:     bucket.Indexes,
			Cold:        summarise([]openObservation{observed}),
			Note:        note,
			Descriptors: observed.FDDelta,
		}
		points = append(points, point)
		fmt.Printf("  %-14s entries %6d indexes %d | engine %.2f ms + store %.2f ms | close %.2f ms | fds +%d | maps +%d | goroutines +%d\n",
			bucket.Type, bucket.Entries, bucket.Indexes,
			observed.EngineMillis, observed.StoreMillis, observed.CloseMillis,
			observed.FDDelta, observed.MappingDelta, observed.GoroutineDelta)
	}
	return points, nil
}

// openComparisonEngine opens one non-plain-hash bucket the way the server opens
// it (server/service.go:Server.CreateBucket), so the numbers are comparable
// with the hash figures rather than with an idealised open.
func openComparisonEngine(bucket seededBucket) (openObservation, string, error) {
	observed := openObservation{Bucket: bucket.Name, Entries: bucket.Entries, Cold: true}
	if _, err := evictTreeFromPageCache(bucket.Dir); err != nil {
		return observed, "", err
	}
	before, err := readSelf()
	if err != nil {
		return observed, "", err
	}

	var closeEngine func() error
	var note string
	engineStart := time.Now()
	switch bucket.Type {
	case string(server.BucketTypeFIFO), string(server.BucketTypeLIFO):
		// 64 MB is the segment size server/service.go passes; it is a literal
		// there, not a tunable.
		segments, err := legacyqueue.NewSegmentStore(filepath.Join(bucket.Dir, "segments"), 64<<20, bucket.Type)
		if err != nil {
			return observed, "", fmt.Errorf("opening the %s segment store for %q: %w", bucket.Type, bucket.Name, err)
		}
		observed.EngineMillis = millis(time.Since(engineStart))
		observed.StoreMillis = 0
		note = "SegmentStore recovers its tail at open; there is no separate store-build step"
		closeEngine = segments.Close

	case string(server.BucketTypeBlob):
		instance, err := storage.OpenBadger(filepath.Join(bucket.Dir, "badger"), benchTuning(), nil)
		if err != nil {
			return observed, "", fmt.Errorf("opening the blobmap Badger for %q: %w", bucket.Name, err)
		}
		observed.EngineMillis = millis(time.Since(engineStart))
		storeStart := time.Now()
		if _, err := legacyblob.NewBlobStore(filepath.Join(bucket.Dir, "blobs")); err != nil {
			_ = instance.Close()
			return observed, "", fmt.Errorf("opening the blob store for %q: %w", bucket.Name, err)
		}
		// The server counts the blobmap prefix at open so Count() is O(1)
		// afterwards; that scan is part of what opening a blob bucket costs.
		blobPrefix := []byte("blobmap:")
		blobmapEntries := 0
		iterator, txn := instance.PrefixIterator(blobPrefix)
		for ; iterator.ValidForPrefix(blobPrefix); iterator.Next() {
			blobmapEntries++
		}
		iterator.Close()
		txn.Discard()
		observed.StoreMillis = millis(time.Since(storeStart))
		if blobmapEntries != bucket.Entries {
			_ = instance.Close()
			return observed, "", fmt.Errorf("blob bucket %q holds %d blobmap entries, the manifest says %d",
				bucket.Name, blobmapEntries, bucket.Entries)
		}
		note = "legacyblob.BlobStore has no Close method, so the in-memory blob index is released only when the process exits"
		closeEngine = instance.Close

	case string(server.BucketTypeHash):
		instance, err := storage.OpenBadger(filepath.Join(bucket.Dir, "badger"), benchTuning(), nil)
		if err != nil {
			return observed, "", fmt.Errorf("opening bucket %q: %w", bucket.Name, err)
		}
		observed.EngineMillis = millis(time.Since(engineStart))
		storeStart := time.Now()
		if _, err := countLegacyHashEntries(instance, bucket.Name); err != nil {
			_ = instance.Close()
			return observed, "", fmt.Errorf("recounting bucket %q: %w", bucket.Name, err)
		}
		var indexInstances []*storage.BadgerInstance
		if bucket.Indexes > 0 {
			indexInstances, err = openLegacyIndexes(filepath.Join(bucket.Dir, "indexes"), indexFieldNames(bucket.Indexes))
			if err != nil {
				_ = instance.Close()
				return observed, "", fmt.Errorf("opening the indexes of %q: %w", bucket.Name, err)
			}
			note = fmt.Sprintf("%d declared indexes, each its own Badger instance (the per-bucket layout's idx_<field>/badger)", bucket.Indexes)
		}
		observed.StoreMillis = millis(time.Since(storeStart))
		closeEngine = func() error {
			var closeErr error
			for _, index := range indexInstances {
				closeErr = errors.Join(closeErr, index.Close())
			}
			closeErr = errors.Join(closeErr, instance.Close())
			if closeErr != nil {
				return fmt.Errorf("closing bucket %q and its indexes: %w", bucket.Name, closeErr)
			}
			return nil
		}

	default:
		return observed, "", fmt.Errorf("bucket %q has type %q, which the open-cost mode does not know how to open", bucket.Name, bucket.Type)
	}

	whileOpen, err := readSelf()
	if err != nil {
		_ = closeEngine()
		return observed, note, err
	}
	closeStart := time.Now()
	if err := closeEngine(); err != nil {
		return observed, note, fmt.Errorf("closing %s bucket %q: %w", bucket.Type, bucket.Name, err)
	}
	observed.CloseMillis = millis(time.Since(closeStart))
	afterClose, err := readSelf()
	if err != nil {
		return observed, note, err
	}

	observed.DeviceReadBytes = whileOpen.IO.ReadBytes - before.IO.ReadBytes
	observed.DeviceWriteBytes = afterClose.IO.WriteBytes - whileOpen.IO.WriteBytes
	observed.FDDelta = whileOpen.Process.FileDescriptors - before.Process.FileDescriptors
	observed.MappingDelta = whileOpen.Process.Mappings - before.Process.Mappings
	observed.RSSDelta = whileOpen.Process.RSSBytes - before.Process.RSSBytes
	observed.VirtualDelta = whileOpen.Process.VirtualBytes - before.Process.VirtualBytes
	observed.GoroutineDelta = whileOpen.Goroutines - before.Goroutines
	observed.OpenCPUNanos = whileOpen.CPUNanos - before.CPUNanos
	return observed, note, nil
}

// --- phase 7: the constant lever --------------------------------------------

// productionBadgerOptions mirrors storage.OpenBadger's option set as it stood
// when VER-19 was measured, with the 32 MiB memtable the node then used: the
// evidence is for the constants it names. It is
// duplicated here rather than called because the lever needs to vary two
// constants that BadgerTuning does not expose — a finding in its own right, not
// a licence to edit production defaults.
func productionBadgerOptions(path string) badger.Options {
	return badger.DefaultOptions(path).
		WithCompression(options.Snappy).
		WithValueLogFileSize(128 << 20).
		WithMemTableSize(32 << 20).
		WithNumMemtables(3).
		WithSyncWrites(false).
		WithNumCompactors(4).
		WithBlockCacheSize(128 << 20).
		WithIndexCacheSize(64 << 20).
		WithBloomFalsePositive(0.01)
}

func measureConstantLever(scratchDir string, instances int) ([]leverPoint, error) {
	// The 4 MB memtable is the size VER-19's dispatch asked for. Badger refuses
	// it outright: maxBatchSize is derived from MemTableSize, and the default
	// ValueThreshold of 1 MiB does not fit inside 15% of 4 MiB. The arm is kept
	// so the refusal is recorded rather than quietly replaced, and 8 MB — the
	// smallest memtable that does fit the default threshold — is measured next
	// to it as the nearest point the lever actually reaches.
	arms := []struct {
		label         string
		valueLogBytes int64
		memTableBytes int64
	}{
		{"production constants (128 MB vlog / 32 MB memtable)", 128 << 20, 32 << 20},
		{"ALTERED constants (16 MB vlog / 4 MB memtable)", 16 << 20, 4 << 20},
		{"ALTERED constants (16 MB vlog / 8 MB memtable)", 16 << 20, 8 << 20},
	}

	// Wipe first: instances left by an earlier run would be reopened rather
	// than created, and would be counted by the disk measurement below.
	if err := os.RemoveAll(scratchDir); err != nil {
		return nil, fmt.Errorf("clearing the lever scratch directory %q: %w", scratchDir, err)
	}

	points := make([]leverPoint, 0, len(arms))
	for armIndex, arm := range arms {
		armDir := filepath.Join(scratchDir, fmt.Sprintf("arm%d", armIndex))
		if err := os.MkdirAll(armDir, 0o750); err != nil {
			return points, fmt.Errorf("creating the lever directory %q: %w", armDir, err)
		}
		paths := make([]string, 0, instances)
		refused := ""
		for index := 0; index < instances; index++ {
			path := filepath.Join(armDir, fmt.Sprintf("i%05d", index))
			opts := productionBadgerOptions(path).
				WithValueLogFileSize(arm.valueLogBytes).
				WithMemTableSize(arm.memTableBytes)
			db, err := badger.Open(opts)
			if err != nil {
				refused = err.Error()
				break
			}
			if err := db.Close(); err != nil {
				return points, fmt.Errorf("closing lever instance %q after creating it: %w", path, err)
			}
			paths = append(paths, path)
		}
		if refused != "" {
			points = append(points, leverPoint{
				Label:         arm.label,
				ValueLogBytes: arm.valueLogBytes,
				MemTableBytes: arm.memTableBytes,
				Refused:       refused,
			})
			fmt.Printf("  %-52s | REFUSED BY BADGER: %s\n", arm.label, refused)
			continue
		}
		if err := syncTree(armDir); err != nil {
			return points, err
		}

		observations := make([]openObservation, 0, len(paths))
		for _, path := range paths {
			observed, err := openLeverInstance(path, arm.valueLogBytes, arm.memTableBytes)
			if err != nil {
				return points, err
			}
			observations = append(observations, observed)
		}
		usage, err := measureDisk(armDir)
		if err != nil {
			return points, err
		}
		point := leverPoint{
			Label:            arm.label,
			ValueLogBytes:    arm.valueLogBytes,
			MemTableBytes:    arm.memTableBytes,
			Cold:             summarise(observations),
			ApparentDiskEach: usage.Total.ApparentBytes / int64(max(1, len(paths))),
		}
		points = append(points, point)
		fmt.Printf("  %-52s | open p50 %.2f ms | close p50 %.2f ms | fds +%d | maps +%d | virt +%s | apparent %s each\n",
			arm.label, point.Cold.Total.P50Millis, point.Cold.Close.P50Millis,
			point.Cold.MedianFDDelta, point.Cold.MedianMapDelta,
			humanBytes(point.Cold.MedianVirtDelta), humanBytes(point.ApparentDiskEach))
	}
	return points, nil
}

func openLeverInstance(path string, valueLogBytes, memTableBytes int64) (openObservation, error) {
	observed := openObservation{Bucket: filepath.Base(path), Cold: true}
	if _, err := evictTreeFromPageCache(path); err != nil {
		return observed, err
	}
	before, err := readSelf()
	if err != nil {
		return observed, err
	}
	opts := productionBadgerOptions(path).
		WithValueLogFileSize(valueLogBytes).
		WithMemTableSize(memTableBytes)
	openStart := time.Now()
	db, err := badger.Open(opts)
	if err != nil {
		return observed, fmt.Errorf("opening lever instance %q: %w", path, err)
	}
	observed.EngineMillis = millis(time.Since(openStart))

	whileOpen, err := readSelf()
	if err != nil {
		_ = db.Close()
		return observed, err
	}
	closeStart := time.Now()
	if err := db.Close(); err != nil {
		return observed, fmt.Errorf("closing lever instance %q: %w", path, err)
	}
	observed.CloseMillis = millis(time.Since(closeStart))
	afterClose, err := readSelf()
	if err != nil {
		return observed, err
	}

	observed.DeviceReadBytes = whileOpen.IO.ReadBytes - before.IO.ReadBytes
	observed.DeviceWriteBytes = afterClose.IO.WriteBytes - whileOpen.IO.WriteBytes
	observed.FDDelta = whileOpen.Process.FileDescriptors - before.Process.FileDescriptors
	observed.MappingDelta = whileOpen.Process.Mappings - before.Process.Mappings
	observed.RSSDelta = whileOpen.Process.RSSBytes - before.Process.RSSBytes
	observed.VirtualDelta = whileOpen.Process.VirtualBytes - before.Process.VirtualBytes
	observed.GoroutineDelta = whileOpen.Goroutines - before.Goroutines
	observed.OpenCPUNanos = whileOpen.CPUNanos - before.CPUNanos
	return observed, nil
}

// --- seeding ----------------------------------------------------------------

// seedCorpus builds the corpus with a real server — the buckets have to be the
// ones the product creates, not ones this harness invented — then stops it,
// restarts it once to price the eager start, and stops it again so the measure
// phase can take the Badger LOCKs.
func seedCorpus(cfg runConfig, oc openCostConfig, manifestPath string) (seedManifest, error) {
	manifest := seedManifest{DataDir: cfg.DataDir, SeededAt: time.Now()}

	if err := os.RemoveAll(cfg.DataDir); err != nil {
		return manifest, fmt.Errorf("clearing the data directory %q before seeding: %w", cfg.DataDir, err)
	}
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return manifest, fmt.Errorf("creating the data directory %q: %w", cfg.DataDir, err)
	}

	seedLog := filepath.Join(cfg.LogDir, "opencost-seed.log")
	proc, _, err := startServer(cfg, seedLog)
	if err != nil {
		return manifest, err
	}
	conn, err := dialAndAuth(cfg)
	if err != nil {
		_ = proc.stop()
		return manifest, err
	}
	if err := conn.call(transport.OpCreateDB, &server.CreateDatabaseRequest{Name: openCostDatabase}); err != nil {
		_ = conn.close()
		_ = proc.stop()
		return manifest, err
	}

	seedStart := time.Now()
	dbDir := filepath.Join(cfg.DataDir, openCostDatabase)

	fmt.Printf("\nseeding %d empty hash buckets for the open distribution\n", oc.Population)
	for index := 0; index < oc.Population; index++ {
		name := fmt.Sprintf("pop%06d", index)
		if err := createSeedBucket(conn, name, string(server.BucketTypeHash), 0); err != nil {
			_ = conn.close()
			_ = proc.stop()
			return manifest, err
		}
		manifest.Population = append(manifest.Population, seededBucket{
			Name: name, Type: string(server.BucketTypeHash), Dir: filepath.Join(dbDir, name),
		})
		if (index+1)%200 == 0 {
			fmt.Printf("  %5d/%d\n", index+1, oc.Population)
		}
	}

	fmt.Printf("\nseeding the size curve: %v entries, %d replicates each\n", oc.SizeCurve, oc.SizeReplicates)
	for _, size := range oc.SizeCurve {
		for replicate := 0; replicate < oc.SizeReplicates; replicate++ {
			name := fmt.Sprintf("size%07d_%d", size, replicate)
			if err := createSeedBucket(conn, name, string(server.BucketTypeHash), 0); err != nil {
				_ = conn.close()
				_ = proc.stop()
				return manifest, err
			}
			if err := fillHashBucket(conn, name, size); err != nil {
				_ = conn.close()
				_ = proc.stop()
				return manifest, err
			}
			manifest.SizeCurve = append(manifest.SizeCurve, seededBucket{
				Name: name, Type: string(server.BucketTypeHash), Entries: size, Dir: filepath.Join(dbDir, name),
			})
		}
		fmt.Printf("  %7d entries x%d done\n", size, oc.SizeReplicates)
	}

	fmt.Printf("\nseeding the comparison engines with %d entries each\n", oc.EngineEntries)
	engines, err := seedEngines(conn, dbDir, oc.EngineEntries)
	if err != nil {
		_ = conn.close()
		_ = proc.stop()
		return manifest, err
	}
	manifest.Engines = engines
	manifest.SeedSeconds = time.Since(seedStart).Seconds()

	_ = conn.close()
	if err := proc.stop(); err != nil {
		return manifest, err
	}

	manifest.CatalogPath = filepath.Join(cfg.DataDir, "catalog.json")
	if info, err := os.Stat(manifest.CatalogPath); err == nil {
		manifest.CatalogBytes = info.Size()
	}

	if err := priceEagerStart(cfg, &manifest); err != nil {
		return manifest, err
	}

	// Every cold open evicts its bucket, and fadvise keeps dirty pages: without
	// this the first phase would quietly measure warm opens.
	fmt.Println("\nsyncing the data directory so the page-cache evictions can take effect")
	if err := syncTree(cfg.DataDir); err != nil {
		return manifest, err
	}

	encoded, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return manifest, fmt.Errorf("encoding the seed manifest: %w", err)
	}
	if err := os.WriteFile(manifestPath, encoded, 0o644); err != nil {
		return manifest, fmt.Errorf("writing the seed manifest to %q: %w", manifestPath, err)
	}
	fmt.Printf("seed manifest: %s\n", manifestPath)
	return manifest, nil
}

// priceEagerStart restarts the server on the seeded directory and records what
// the eager LoadCatalog cost — the number the lazy design would be replacing.
func priceEagerStart(cfg runConfig, manifest *seedManifest) error {
	fmt.Println("\npricing the eager start: restarting the server on the seeded directory")
	restartLog := filepath.Join(cfg.LogDir, "opencost-eager-start.log")
	proc, ready, err := startServer(cfg, restartLog)
	if err != nil {
		return err
	}
	manifest.EagerStartSeconds = ready.Seconds()
	reading, err := readProcess(proc.pid())
	if err != nil {
		_ = proc.stop()
		return err
	}
	manifest.EagerStartReading = reading
	counters, err := readProcessIO(proc.pid())
	if err != nil {
		_ = proc.stop()
		return err
	}
	manifest.EagerStartIO = counters

	loadMillis, loaded, err := catalogLoadFromLog(restartLog)
	if err != nil {
		return fmt.Errorf("reading the eager-start log %q: %w", restartLog, err)
	}
	manifest.CatalogLoadLogMilli = loadMillis
	manifest.EagerStartBuckets = loaded

	stopStart := time.Now()
	if err := proc.stop(); err != nil {
		return err
	}
	manifest.EagerStopSeconds = time.Since(stopStart).Seconds()

	fmt.Printf("  eager start: %.1f s to ready, catalog load %d ms, %d buckets, rss %s, %s written and %s read from the device, stop %.1f s\n",
		manifest.EagerStartSeconds, loadMillis, loaded, humanBytes(reading.RSSBytes),
		humanBytes(counters.WriteBytes), humanBytes(counters.ReadBytes), manifest.EagerStopSeconds)
	return nil
}

func seedEngines(conn *serverConn, dbDir string, entries int) ([]seededBucket, error) {
	seeded := make([]seededBucket, 0, 8)

	queues := []struct {
		bucketType server.BucketType
		op         transport.Opcode
	}{
		{server.BucketTypeFIFO, transport.OpFIFOPush},
		{server.BucketTypeLIFO, transport.OpLIFOPush},
	}
	for _, queue := range queues {
		for _, count := range []int{0, entries} {
			name := fmt.Sprintf("eng_%s_%d", queue.bucketType, count)
			if err := createSeedBucket(conn, name, string(queue.bucketType), 0); err != nil {
				return seeded, err
			}
			for index := 0; index < count; index++ {
				if err := conn.call(queue.op, &server.PushRequest{
					Database: openCostDatabase,
					Bucket:   name,
					Data:     seedDocument(index),
				}); err != nil {
					return seeded, fmt.Errorf("pushing entry %d into %s bucket %q: %w", index, queue.bucketType, name, err)
				}
			}
			seeded = append(seeded, seededBucket{
				Name: name, Type: string(queue.bucketType), Entries: count, Dir: filepath.Join(dbDir, name),
			})
		}
	}

	for _, count := range []int{0, entries} {
		name := fmt.Sprintf("eng_blob_%d", count)
		if err := createSeedBucket(conn, name, string(server.BucketTypeBlob), 0); err != nil {
			return seeded, err
		}
		blobPayload := make([]byte, 1024)
		for index := range blobPayload {
			blobPayload[index] = byte(index)
		}
		for index := 0; index < count; index++ {
			if err := conn.call(transport.OpBlobWrite, &server.BlobWriteRequest{
				Database: openCostDatabase,
				Bucket:   name,
				Key:      fmt.Sprintf("blob%06d", index),
				Data:     blobPayload,
			}); err != nil {
				return seeded, fmt.Errorf("writing blob %d into bucket %q: %w", index, name, err)
			}
		}
		seeded = append(seeded, seededBucket{
			Name: name, Type: string(server.BucketTypeBlob), Entries: count, Dir: filepath.Join(dbDir, name),
		})
	}

	for _, count := range []int{0, entries} {
		name := fmt.Sprintf("eng_hash_idx%d_%d", indexedFieldCount, count)
		if err := createSeedBucket(conn, name, string(server.BucketTypeHash), indexedFieldCount); err != nil {
			return seeded, err
		}
		if err := fillHashBucket(conn, name, count); err != nil {
			return seeded, err
		}
		seeded = append(seeded, seededBucket{
			Name: name, Type: string(server.BucketTypeHash), Entries: count,
			Indexes: indexedFieldCount, Dir: filepath.Join(dbDir, name),
		})
	}
	return seeded, nil
}

func createSeedBucket(conn *serverConn, name, bucketType string, indexes int) error {
	request := &server.CreateBucketRequest{
		Database: openCostDatabase,
		Name:     name,
		Type:     server.BucketType(bucketType),
	}
	if indexes > 0 {
		declared := make([]interface{}, 0, indexes)
		for _, field := range indexFieldNames(indexes) {
			declared = append(declared, field)
		}
		request.Options = map[string]interface{}{"indices": declared}
	}
	if err := conn.call(transport.OpCreateBucket, request); err != nil {
		return fmt.Errorf("creating %s bucket %q: %w", bucketType, name, err)
	}
	return nil
}

// fillHashBucket writes count entries in batches. A batch is used because
// seeding 100k entries one round trip at a time dominates the whole run, and
// the entries themselves are not what is being timed.
func fillHashBucket(conn *serverConn, name string, count int) error {
	for written := 0; written < count; {
		batchSize := min(seedBatchSize, count-written)
		documents := make([]map[string]interface{}, 0, batchSize)
		for index := 0; index < batchSize; index++ {
			documents = append(documents, seedDocument(written+index))
		}
		if err := conn.call(transport.OpBatchUpsert, &server.BatchInsertRequest{
			Database: openCostDatabase,
			Bucket:   name,
			Data:     documents,
		}); err != nil {
			return fmt.Errorf("batch-inserting entries %d..%d into bucket %q: %w", written, written+batchSize, name, err)
		}
		written += batchSize
	}
	return nil
}

// seedDocument is a small document of the shape the small-bucket principle
// describes — one entity, a handful of fields — not a synthetic blob.
func seedDocument(index int) map[string]interface{} {
	return map[string]interface{}{
		"_id":   fmt.Sprintf("k%09d", index),
		"f1":    fmt.Sprintf("value-%d", index%1000),
		"f2":    int64(index),
		"body":  strings.Repeat("x", 64),
		"epoch": time.Now().UnixNano(),
	}
}

func indexFieldNames(count int) []string {
	fields := make([]string, 0, count)
	for index := 0; index < count; index++ {
		fields = append(fields, fmt.Sprintf("f%d", index+1))
	}
	return fields
}

func loadManifest(path string) (seedManifest, error) {
	var manifest seedManifest
	raw, err := os.ReadFile(path)
	if err != nil {
		return manifest, fmt.Errorf("reading the seed manifest %q (run once with -seed to create it): %w", path, err)
	}
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return manifest, fmt.Errorf("parsing the seed manifest %q: %w", path, err)
	}
	if len(manifest.Population) == 0 {
		return manifest, fmt.Errorf("the seed manifest %q lists no population buckets; re-seed", path)
	}
	return manifest, nil
}

// --- summarising ------------------------------------------------------------

func summarise(observations []openObservation) phaseSummary {
	engine := make([]time.Duration, 0, len(observations))
	storeBuild := make([]time.Duration, 0, len(observations))
	total := make([]time.Duration, 0, len(observations))
	closes := make([]time.Duration, 0, len(observations))
	reads := make([]int64, 0, len(observations))
	writes := make([]int64, 0, len(observations))
	fds := make([]int64, 0, len(observations))
	maps := make([]int64, 0, len(observations))
	rss := make([]int64, 0, len(observations))
	virt := make([]int64, 0, len(observations))
	goroutines := make([]int64, 0, len(observations))
	cpu := make([]int64, 0, len(observations))

	for _, observed := range observations {
		engine = append(engine, time.Duration(observed.EngineMillis*float64(time.Millisecond)))
		storeBuild = append(storeBuild, time.Duration(observed.StoreMillis*float64(time.Millisecond)))
		total = append(total, observed.totalOpen())
		closes = append(closes, time.Duration(observed.CloseMillis*float64(time.Millisecond)))
		reads = append(reads, observed.DeviceReadBytes)
		writes = append(writes, observed.DeviceWriteBytes)
		fds = append(fds, int64(observed.FDDelta))
		maps = append(maps, int64(observed.MappingDelta))
		rss = append(rss, observed.RSSDelta)
		virt = append(virt, observed.VirtualDelta)
		goroutines = append(goroutines, int64(observed.GoroutineDelta))
		cpu = append(cpu, observed.OpenCPUNanos)
	}

	return phaseSummary{
		Buckets:          len(observations),
		Engine:           durationStats(engine),
		Store:            durationStats(storeBuild),
		Total:            durationStats(total),
		Close:            durationStats(closes),
		MedianReadBytes:  medianInt64(reads),
		MedianWriteBytes: medianInt64(writes),
		MedianFDDelta:    int(medianInt64(fds)),
		MedianMapDelta:   int(medianInt64(maps)),
		MedianRSSDelta:   medianInt64(rss),
		MedianVirtDelta:  medianInt64(virt),
		MedianGoroutines: int(medianInt64(goroutines)),
		MedianOpenCPUNs:  medianInt64(cpu),
	}
}

func totalOpens(observations []openObservation) []time.Duration {
	durations := make([]time.Duration, 0, len(observations))
	for _, observed := range observations {
		durations = append(durations, observed.totalOpen())
	}
	return durations
}

func closeDurations(observations []openObservation) []time.Duration {
	durations := make([]time.Duration, 0, len(observations))
	for _, observed := range observations {
		durations = append(durations, time.Duration(observed.CloseMillis*float64(time.Millisecond)))
	}
	return durations
}

func writeBytes(observations []openObservation) []int64 {
	counts := make([]int64, 0, len(observations))
	for _, observed := range observations {
		counts = append(counts, observed.DeviceWriteBytes)
	}
	return counts
}

func medianInt64(values []int64) int64 {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]int64(nil), values...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	return sorted[len(sorted)/2]
}

func allSeeded(manifest seedManifest) []seededBucket {
	every := make([]seededBucket, 0, len(manifest.Population)+len(manifest.SizeCurve)+len(manifest.Engines))
	every = append(every, manifest.Population...)
	every = append(every, manifest.SizeCurve...)
	every = append(every, manifest.Engines...)
	return every
}

// --- reporting --------------------------------------------------------------

func printOpenCostReport(report openCostReport) {
	fmt.Println("\n================ open cost ================")
	fmt.Printf("corpus: %d population + %d size-curve + %d engine buckets, catalog %s\n",
		len(report.Manifest.Population), len(report.Manifest.SizeCurve), len(report.Manifest.Engines),
		humanBytes(report.Manifest.CatalogBytes))
	fmt.Printf("eager start on this corpus: %.1f s to ready, catalog load %d ms, %s written to the device\n",
		report.Manifest.EagerStartSeconds, report.Manifest.CatalogLoadLogMilli,
		humanBytes(report.Manifest.EagerStartIO.WriteBytes))

	fmt.Println("\nzero-open baseline (what a start that opens nothing still pays)")
	fmt.Printf("  catalog read (cold) %.1f ms | parse %.1f ms | stat %d bucket dirs %.1f ms (%.1f us each)\n",
		report.ZeroOpen.ColdReadMillis, report.ZeroOpen.ParseMillis,
		report.ZeroOpen.CatalogBuckets, report.ZeroOpen.StatAllMillis, report.ZeroOpen.StatPerBucketMicros)

	fmt.Println("\nopening one empty bucket (ms)")
	fmt.Println("             |  n  |   min |   p50 |   p95 |   max")
	printStatRow("cold engine", report.ColdOpens.Engine)
	printStatRow("cold store ", report.ColdOpens.Store)
	printStatRow("cold total ", report.ColdOpens.Total)
	printStatRow("cold close ", report.ColdOpens.Close)
	printStatRow("warm total ", report.WarmReopens.Total)
	fmt.Printf("  per open: fds +%d | mappings +%d | rss +%s | virtual +%s | goroutines +%d | %s read from the device | %.2f ms of CPU\n",
		report.ColdOpens.MedianFDDelta, report.ColdOpens.MedianMapDelta,
		humanBytes(report.ColdOpens.MedianRSSDelta), humanBytes(report.ColdOpens.MedianVirtDelta),
		report.ColdOpens.MedianGoroutines, humanBytes(report.ColdOpens.MedianReadBytes),
		float64(report.ColdOpens.MedianOpenCPUNs)/1e6)
	fmt.Printf("  warm reopen: %.2f ms wall, %.2f ms of CPU, %s read from the device\n",
		report.WarmReopens.Total.P50Millis, float64(report.WarmReopens.MedianOpenCPUNs)/1e6,
		humanBytes(report.WarmReopens.MedianReadBytes))

	fmt.Println("\nsize curve (p50 ms)")
	fmt.Println("   entries | cold engine | cold store | cold total | warm total |  close | device read | cold cpu ms")
	for _, point := range report.SizeCurve {
		fmt.Printf("  %8d | %11.2f | %10.2f | %10.2f | %10.2f | %6.2f | %11s | %11.2f\n",
			point.Entries, point.Cold.Engine.P50Millis, point.Cold.Store.P50Millis,
			point.Cold.Total.P50Millis, point.Warm.Total.P50Millis, point.Cold.Close.P50Millis,
			humanBytes(point.Cold.MedianReadBytes), float64(point.Cold.MedianOpenCPUNs)/1e6)
	}

	if report.Concurrent.Opened > 0 {
		opened := int64(report.Concurrent.Opened)
		fmt.Printf("\n%d buckets held open together\n", report.Concurrent.Opened)
		fmt.Printf("  rss %s -> %s (%s each) | virtual %s -> %s | fds %d -> %d | maps %d -> %d | goroutines %d -> %d\n",
			humanBytes(report.Concurrent.Before.Process.RSSBytes), humanBytes(report.Concurrent.AfterOpen.Process.RSSBytes),
			humanBytes((report.Concurrent.AfterOpen.Process.RSSBytes-report.Concurrent.Before.Process.RSSBytes)/opened),
			humanBytes(report.Concurrent.Before.Process.VirtualBytes), humanBytes(report.Concurrent.AfterOpen.Process.VirtualBytes),
			report.Concurrent.Before.Process.FileDescriptors, report.Concurrent.AfterOpen.Process.FileDescriptors,
			report.Concurrent.Before.Process.Mappings, report.Concurrent.AfterOpen.Process.Mappings,
			report.Concurrent.Before.Goroutines, report.Concurrent.AfterOpen.Goroutines)
		fmt.Printf("  one more open while %d are already held: p50 %.2f ms, p95 %.2f ms, max %.2f ms, %.2f ms of CPU\n",
			report.Concurrent.Opened, report.Concurrent.Open.P50Millis, report.Concurrent.Open.P95Millis,
			report.Concurrent.Open.MaxMillis, float64(report.Concurrent.OpenCPUNs)/1e6)
		fmt.Printf("  after %.0f s idle: rss %s | %s written to the device while idle | goroutines %d\n",
			report.Concurrent.IdleSeconds, humanBytes(report.Concurrent.AfterIdle.Process.RSSBytes),
			humanBytes(report.Concurrent.AfterIdle.IO.WriteBytes-report.Concurrent.AfterOpen.IO.WriteBytes),
			report.Concurrent.AfterIdle.Goroutines)
		fmt.Printf("  closing all %d took %.0f ms; rss fell to %s, fds to %d, goroutines to %d\n",
			report.Concurrent.Opened, report.Concurrent.CloseAllMS,
			humanBytes(report.Concurrent.AfterClose.Process.RSSBytes),
			report.Concurrent.AfterClose.Process.FileDescriptors, report.Concurrent.AfterClose.Goroutines)
	}

	fmt.Println("\nclose cost")
	fmt.Printf("  clean (never written): p50 %.2f ms, %s to the device\n",
		report.CloseCost.CleanClose.P50Millis, humanBytes(report.CloseCost.CleanWriteBytes))
	fmt.Printf("  dirty (%d writes):     p50 %.2f ms, %s to the device\n",
		report.CloseCost.DirtyWrites, report.CloseCost.DirtyClose.P50Millis, humanBytes(report.CloseCost.DirtyWriteBytes))

	fmt.Println("\ncomparison engines (cold, ms)")
	for _, point := range report.Engines {
		fmt.Printf("  %-6s entries %6d indexes %d | engine %6.2f | store %6.2f | close %6.2f | fds +%d | maps +%d\n",
			point.Engine, point.Entries, point.Indexes,
			point.Cold.Engine.P50Millis, point.Cold.Store.P50Millis, point.Cold.Close.P50Millis,
			point.Cold.MedianFDDelta, point.Cold.MedianMapDelta)
		if point.Note != "" {
			fmt.Printf("           note: %s\n", point.Note)
		}
	}

	fmt.Println("\nthe constant lever — Badger opened with ALTERED options, not the production configuration")
	for _, point := range report.Lever {
		if point.Refused != "" {
			fmt.Printf("  %-52s | REFUSED BY BADGER: %s\n", point.Label, point.Refused)
			continue
		}
		fmt.Printf("  %-52s | open p50 %6.2f ms | close p50 %6.2f ms | fds +%d | maps +%d | virtual +%s | apparent %s\n",
			point.Label, point.Cold.Total.P50Millis, point.Cold.Close.P50Millis,
			point.Cold.MedianFDDelta, point.Cold.MedianMapDelta,
			humanBytes(point.Cold.MedianVirtDelta), humanBytes(point.ApparentDiskEach))
	}

	if report.CutShort != "" {
		fmt.Printf("\n!! the run was cut short: %s\n", report.CutShort)
	}
}

func printStatRow(label string, summary statSummary) {
	fmt.Printf("  %s | %3d | %5.2f | %5.2f | %5.2f | %6.2f\n",
		label, summary.Count, summary.MinMillis, summary.P50Millis, summary.P95Millis, summary.MaxMillis)
}

// shuffleSeeded randomises open order so the measurement is not an artefact of
// the order the buckets were created in, which is also the order they sit in on
// disk.
func shuffleSeeded(buckets []seededBucket, source *rand.Rand) {
	source.Shuffle(len(buckets), func(i, j int) {
		buckets[i], buckets[j] = buckets[j], buckets[i]
	})
}

// countLegacyHashEntries is the walk the per-bucket layout's
// store.HashStore.Recount did on open: every key under
// "hash:<database>:<bucket>:" in the bucket's own Badger instance. The hash
// engine has moved onto the node's shared store (shared-storage build Step 8)
// and opens nothing, so this harness, which measures the per-bucket layout's
// open cost on a corpus written in it, keeps the walk it measured.
func countLegacyHashEntries(instance *storage.BadgerInstance, bucket string) (int64, error) {
	prefix := []byte(fmt.Sprintf("hash:%s:%s:", openCostDatabase, bucket))
	iterator, txn := instance.PrefixIterator(prefix)
	defer txn.Discard()
	defer iterator.Close()
	var entries int64
	for ; iterator.ValidForPrefix(prefix); iterator.Next() {
		entries++
	}
	return entries, nil
}

// openLegacyIndexes opens each declared index's own Badger instance where the
// per-bucket layout kept it, <bucket>/indexes/idx_<field>/badger: the
// multiplier this harness prices. Indexes have moved into the node's shared
// store (shared-storage build Step 9) and open nothing there, so the harness
// opens the per-bucket layout's instances itself.
func openLegacyIndexes(indexDir string, fields []string) ([]*storage.BadgerInstance, error) {
	instances := make([]*storage.BadgerInstance, 0, len(fields))
	for _, field := range fields {
		index, err := storage.OpenBadger(filepath.Join(indexDir, "idx_"+field, "badger"), benchTuning(), nil)
		if err != nil {
			var closeErr error
			for _, opened := range instances {
				closeErr = errors.Join(closeErr, opened.Close())
			}
			return nil, errors.Join(fmt.Errorf("opening the index on %s: %w", field, err), closeErr)
		}
		instances = append(instances, index)
	}
	return instances, nil
}
