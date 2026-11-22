package main

// The queue-open mode answers the one question the open-cost mode left with a
// two-point line: how does the cost of opening a fifo/lifo bucket scale with
// the number of entries in it. VER-19 measured 0 and 1,000 entries as part of
// its engine comparison (0.19 ms and 6.23 ms for fifo) and named the linear
// term -- storage/segments.go:NewSegmentStore ending in countTombstonesLocked,
// a head-to-tail walk with a disk read per live entry. This mode runs that
// curve out to 100,000 entries so the slope can be seen rather than inferred,
// and so the same command run on the pre-fix and post-fix trees produces the
// two columns the open-cost fix is measured against.
//
// It differs from -mode=opencost in one way that is worth stating, because it
// is what makes the mode cheap enough to run twice: there is no server and no
// seed phase. A SegmentStore is filesystem-resident -- no Badger instance, no
// LOCK file, no catalog -- so this harness creates the queues itself with the
// same legacyqueue.NewSegmentStore the server would call, and measures the opens
// in the same process. Everything else is shared with the open-cost mode: the
// tmpfs refusal, the host guard, the page-cache eviction, and the percentile
// definitions.

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/bench/bucketdensity/legacyqueue"
)

const (
	// queueOpenSegmentSize is the segment size server/service.go passes when it
	// creates a fifo or lifo bucket. It is a literal there, not a tunable, so
	// it is a literal here.
	queueOpenSegmentSize = 64 << 20
	// queueOpenSeedBatch bounds one PushBatch during seeding. SegmentStore.Push
	// fsyncs per entry; PushBatch amortises the sync across the batch, and the
	// seeding is not what is being timed.
	queueOpenSeedBatch = 1000
)

// queueOpenConfig is this mode's knob set.
type queueOpenConfig struct {
	StoreTypes  []string `json:"store_types"`
	EntryCounts []int    `json:"entry_counts"`
	Opens       int      `json:"opens_per_point"`
	SegmentSize int64    `json:"segment_size_bytes"`
}

// queueOpenPoint is one (store type, entry count) pair: what opening a queue of
// that length costs, over Opens cold opens.
type queueOpenPoint struct {
	StoreType string `json:"store_type"`
	Entries   int    `json:"entries"`

	OpenMillis     statSummary `json:"open_ms"`
	P99Millis      float64     `json:"p99_ms"`
	OpensPerSecond float64     `json:"opens_per_second"`

	// MedianFDDelta is descriptors held by one open store: SegmentStore keeps
	// currentSeg and, for a store whose head is in an older segment, headSeg.
	MedianFDDelta      int   `json:"median_fd_delta"`
	MedianMappingDelta int   `json:"median_mapping_delta"`
	MedianRSSDelta     int64 `json:"median_rss_delta_bytes"`
	MedianReadBytes    int64 `json:"median_device_read_bytes"`
	MedianOpenCPUNs    int64 `json:"median_open_cpu_ns"`

	SegmentFiles  int     `json:"segment_files"`
	DirectoryByte int64   `json:"directory_bytes"`
	SeedSeconds   float64 `json:"seed_seconds"`
}

type queueOpenReport struct {
	Host   hostFacts        `json:"host"`
	Floors guardFloors      `json:"guard_floors"`
	Config queueOpenConfig  `json:"config"`
	Points []queueOpenPoint `json:"points"`

	CutShort string `json:"cut_short,omitempty"`
}

type queueOpenFlagSet struct {
	storeTypes *string
	opens      *int
}

func registerQueueOpenFlags() *queueOpenFlagSet {
	return &queueOpenFlagSet{
		storeTypes: flag.String("queue-types", "fifo,lifo", "queueopen: comma-separated segment store types to measure (fifo, lifo)"),
		opens:      flag.Int("queue-opens", 25, "queueopen: cold opens to time at each entry count; the p99 is a nearest-rank percentile over these"),
	}
}

func (f *queueOpenFlagSet) config(entryCounts []int) (queueOpenConfig, error) {
	types, err := parseQueueTypes(*f.storeTypes)
	if err != nil {
		return queueOpenConfig{}, err
	}
	if *f.opens < 1 {
		return queueOpenConfig{}, fmt.Errorf("-queue-opens must be at least 1, got %d", *f.opens)
	}
	return queueOpenConfig{
		StoreTypes:  types,
		EntryCounts: entryCounts,
		Opens:       *f.opens,
		SegmentSize: queueOpenSegmentSize,
	}, nil
}

func parseQueueTypes(raw string) ([]string, error) {
	var types []string
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if part != "fifo" && part != "lifo" {
			return nil, fmt.Errorf("-queue-types: %q is neither \"fifo\" nor \"lifo\"", part)
		}
		types = append(types, part)
	}
	if len(types) == 0 {
		return nil, fmt.Errorf("-queue-types: no store types given")
	}
	return types, nil
}

func runQueueOpen(cfg runConfig, qc queueOpenConfig, outPath string) error {
	if err := refuseTmpfs(cfg.DataDir); err != nil {
		return err
	}
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return fmt.Errorf("creating the queue-open data directory %q: %w", cfg.DataDir, err)
	}
	facts, err := collectHostFacts(cfg.DataDir)
	if err != nil {
		return err
	}
	printHostFacts(facts)

	report := queueOpenReport{Host: facts, Floors: cfg.Floors, Config: qc}
	guard := startHostGuard(cfg.Floors.AvailMemoryBytes)
	defer guard.stop()

	fmt.Printf("\n=== queue open cost: %d entry counts x %d store types, %d cold opens each ===\n",
		len(qc.EntryCounts), len(qc.StoreTypes), qc.Opens)

	for _, storeType := range qc.StoreTypes {
		for _, entries := range qc.EntryCounts {
			if guard.tripped.Load() {
				break
			}
			if err := checkQueueOpenFloors(cfg); err != nil {
				report.CutShort = err.Error()
				fmt.Printf("\n!! %v\n", err)
				break
			}
			point, err := measureQueueOpen(cfg.DataDir, storeType, entries, qc)
			if err != nil {
				return err
			}
			report.Points = append(report.Points, point)
			printQueueOpenPoint(point)
		}
	}
	if report.CutShort == "" {
		report.CutShort = guard.trippedReason()
	}

	printQueueOpenTable(report)

	if outPath != "" {
		encoded, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			return fmt.Errorf("encoding the queue-open report: %w", err)
		}
		if err := os.WriteFile(outPath, encoded, 0o644); err != nil {
			return fmt.Errorf("writing the queue-open report to %q: %w", outPath, err)
		}
		fmt.Printf("\nJSON report: %s\n", outPath)
	}
	return nil
}

// checkQueueOpenFloors refuses to start another entry count if the host is
// already below a floor. The hard floor has no child process to kill in this
// mode -- the harness is the only process -- so it ends the run instead.
func checkQueueOpenFloors(cfg runConfig) error {
	freeNow, err := freeDiskBytes(cfg.DataDir)
	if err != nil {
		return err
	}
	if freeNow < cfg.Floors.FreeDiskBytes {
		return fmt.Errorf("free disk is %s, below the %s floor; stopping with the points already measured",
			humanBytes(freeNow), humanBytes(cfg.Floors.FreeDiskBytes))
	}
	availNow, err := memAvailableBytes()
	if err != nil {
		return err
	}
	if availNow < cfg.Floors.HardMemoryBytes {
		return fmt.Errorf("MemAvailable is %s, below the %s hard floor; stopping outright",
			humanBytes(availNow), humanBytes(cfg.Floors.HardMemoryBytes))
	}
	return nil
}

// measureQueueOpen seeds one queue of the given length, then opens and closes
// it repeatedly, evicting its files from the page cache before each open so
// every open is cold. The queue directory is removed afterwards, so a run over
// the whole curve never holds more than one queue's bytes at a time.
func measureQueueOpen(dataDir, storeType string, entries int, qc queueOpenConfig) (queueOpenPoint, error) {
	point := queueOpenPoint{StoreType: storeType, Entries: entries}
	dir := filepath.Join(dataDir, fmt.Sprintf("queueopen_%s_%d", storeType, entries))
	if err := os.RemoveAll(dir); err != nil {
		return point, fmt.Errorf("clearing the previous %s queue at %q: %w", storeType, dir, err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	seedStart := time.Now()
	if err := seedQueue(dir, storeType, entries, qc.SegmentSize); err != nil {
		return point, err
	}
	point.SeedSeconds = time.Since(seedStart).Seconds()

	if err := syncTree(dir); err != nil {
		return point, err
	}
	segments, bytesOnDisk, err := describeQueueDirectory(dir)
	if err != nil {
		return point, err
	}
	point.SegmentFiles = segments
	point.DirectoryByte = bytesOnDisk

	durations := make([]time.Duration, 0, qc.Opens)
	fdDeltas := make([]int64, 0, qc.Opens)
	mapDeltas := make([]int64, 0, qc.Opens)
	rssDeltas := make([]int64, 0, qc.Opens)
	readBytes := make([]int64, 0, qc.Opens)
	cpuNanos := make([]int64, 0, qc.Opens)

	for open := 0; open < qc.Opens; open++ {
		observed, err := openQueueOnce(dir, storeType, qc.SegmentSize)
		if err != nil {
			return point, err
		}
		durations = append(durations, observed.totalOpen())
		fdDeltas = append(fdDeltas, int64(observed.FDDelta))
		mapDeltas = append(mapDeltas, int64(observed.MappingDelta))
		rssDeltas = append(rssDeltas, observed.RSSDelta)
		readBytes = append(readBytes, observed.DeviceReadBytes)
		cpuNanos = append(cpuNanos, observed.OpenCPUNanos)
	}

	point.OpenMillis = durationStats(durations)
	point.P99Millis = queueOpenP99(durations)
	if point.OpenMillis.MeanMillis > 0 {
		point.OpensPerSecond = 1000 / point.OpenMillis.MeanMillis
	}
	point.MedianFDDelta = int(medianInt64(fdDeltas))
	point.MedianMappingDelta = int(medianInt64(mapDeltas))
	point.MedianRSSDelta = medianInt64(rssDeltas)
	point.MedianReadBytes = medianInt64(readBytes)
	point.MedianOpenCPUNs = medianInt64(cpuNanos)
	return point, nil
}

// seedQueue writes entries into a fresh store with the same document shape the
// open-cost mode seeds its comparison engines with, so the two measurements can
// be read against each other.
func seedQueue(dir, storeType string, entries int, segmentSize int64) error {
	queue, err := legacyqueue.NewSegmentStore(dir, segmentSize, storeType)
	if err != nil {
		return fmt.Errorf("creating the %s queue at %q: %w", storeType, dir, err)
	}
	for written := 0; written < entries; {
		size := min(queueOpenSeedBatch, entries-written)
		items := make([][]byte, 0, size)
		for index := 0; index < size; index++ {
			encoded, marshalErr := bson.Marshal(seedDocument(written + index))
			if marshalErr != nil {
				_ = queue.Close()
				return fmt.Errorf("encoding seed document %d: %w", written+index, marshalErr)
			}
			items = append(items, encoded)
		}
		if _, pushErr := queue.PushBatch(items); pushErr != nil {
			_ = queue.Close()
			return fmt.Errorf("pushing entries %d..%d into the %s queue: %w", written, written+size, storeType, pushErr)
		}
		written += size
	}
	if err := queue.Close(); err != nil {
		return fmt.Errorf("closing the seeded %s queue at %q: %w", storeType, dir, err)
	}
	return nil
}

// openQueueOnce is one cold open/close cycle around legacyqueue.NewSegmentStore,
// measured as a delta on this process the way the open-cost mode measures its
// engines. SegmentStore recovers its tail at open and has no separate
// store-build step, so StoreMillis stays zero.
func openQueueOnce(dir, storeType string, segmentSize int64) (openObservation, error) {
	observed := openObservation{Bucket: filepath.Base(dir), Cold: true}
	if _, err := evictTreeFromPageCache(dir); err != nil {
		return observed, err
	}
	before, err := readSelf()
	if err != nil {
		return observed, err
	}

	openStart := time.Now()
	queue, err := legacyqueue.NewSegmentStore(dir, segmentSize, storeType)
	if err != nil {
		return observed, fmt.Errorf("opening the %s queue at %q: %w", storeType, dir, err)
	}
	observed.EngineMillis = millis(time.Since(openStart))

	whileOpen, err := readSelf()
	if err != nil {
		_ = queue.Close()
		return observed, err
	}
	closeStart := time.Now()
	if err := queue.Close(); err != nil {
		return observed, fmt.Errorf("closing the %s queue at %q: %w", storeType, dir, err)
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

func describeQueueDirectory(dir string) (int, int64, error) {
	listed, err := os.ReadDir(dir)
	if err != nil {
		return 0, 0, fmt.Errorf("reading the queue directory %q: %w", dir, err)
	}
	segments := 0
	var total int64
	for _, item := range listed {
		info, statErr := item.Info()
		if statErr != nil {
			return 0, 0, fmt.Errorf("stat %q in the queue directory: %w", item.Name(), statErr)
		}
		total += info.Size()
		if filepath.Ext(item.Name()) == ".fifo" || filepath.Ext(item.Name()) == ".lifo" {
			segments++
		}
	}
	return segments, total, nil
}

// queueOpenP99 is the nearest-rank 99th percentile. statSummary reports p95,
// which is the tail the open-cost mode chose; this mode is asked for p99, and
// adding a field to the shared summary would change the shape of VER-19's
// published JSON for no reason.
func queueOpenP99(observed []time.Duration) float64 {
	if len(observed) == 0 {
		return 0
	}
	sorted := append([]time.Duration(nil), observed...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	return millis(nearestRank(sorted, 0.99))
}

func printQueueOpenPoint(point queueOpenPoint) {
	fmt.Printf("  %-4s %7d entries: p50 %8.3f ms  p99 %8.3f ms  %9.1f opens/s  %d fds  %d segments  %s on disk (seeded in %.1fs)\n",
		point.StoreType, point.Entries, point.OpenMillis.P50Millis, point.P99Millis,
		point.OpensPerSecond, point.MedianFDDelta, point.SegmentFiles,
		humanBytes(point.DirectoryByte), point.SeedSeconds)
}

func printQueueOpenTable(report queueOpenReport) {
	fmt.Printf("\n=== queue open cost by entry count ===\n")
	fmt.Printf("%-6s %9s %10s %10s %10s %12s %6s %10s\n",
		"type", "entries", "min ms", "p50 ms", "p99 ms", "opens/s", "fds", "read B")
	for _, point := range report.Points {
		fmt.Printf("%-6s %9d %10.3f %10.3f %10.3f %12.1f %6d %10d\n",
			point.StoreType, point.Entries, point.OpenMillis.MinMillis, point.OpenMillis.P50Millis,
			point.P99Millis, point.OpensPerSecond, point.MedianFDDelta, point.MedianReadBytes)
	}
	if report.CutShort != "" {
		fmt.Printf("\ncut short: %s\n", report.CutShort)
	}
}
