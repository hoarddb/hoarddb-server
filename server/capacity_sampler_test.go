package server

import (
	"errors"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/hoarddb/hoarddb-server/config"
)

const (
	familyRatio         = "hoarddb_node_capacity_ratio"
	familyThreshold     = "hoarddb_node_capacity_threshold_ratio"
	familySampleErrors  = "hoarddb_node_capacity_sample_errors_total"
	familyLastSample    = "hoarddb_node_capacity_last_sample_timestamp_seconds"
	familyMappings      = "hoarddb_node_memory_mappings"
	familyMappingsLimit = "hoarddb_node_memory_mappings_limit"
	familyAnonBytes     = "hoarddb_node_memory_anon_bytes"
	familyMemLimitBytes = "hoarddb_node_memory_limit_bytes"
	familyDiskUsed      = "hoarddb_node_disk_used_bytes"
	familyDiskSize      = "hoarddb_node_disk_size_bytes"
	familyDataBytes     = "hoarddb_node_data_bytes"
)

// resourceFamilies is each resource's reading and limit series; fds has none
// of its own (process_open_fds and process_max_fds publish them).
var resourceFamilies = map[string][]string{
	resourceFDs:      nil,
	resourceMappings: {familyMappings, familyMappingsLimit},
	resourceMemory:   {familyAnonBytes, familyMemLimitBytes},
	resourceDisk:     {familyDiskUsed, familyDiskSize},
}

// TestCapacityGaugesRegistered (spec T-1) proves every
// probe-backed series is absent until the first sample and present after it.
// Before it, only the thresholds, the error counters at 0, and the one plain
// gauge (the last-sample timestamp at 0, the Unix epoch, which is maximally
// stale, not healthy) are gathered, with the memtable size and the two
// encryption series, which no probe reads: they are fixed at start.
func TestCapacityGaugesRegistered(t *testing.T) {
	s := newTestServer(t)
	cs := newIdleSampler(t, s)

	before := gatherSampler(t, cs)
	wantBefore := map[string]int{familyThreshold: 4, familySampleErrors: 4, familyLastSample: 1, "hoarddb_node_memtable_bytes": 1,
		"hoarddb_node_encryption_info": 1, "hoarddb_node_encryption_sealed": 1}
	if len(before) != len(wantBefore) {
		t.Fatalf("before the first sample the registry holds %v, want exactly %v", familyNames(before), wantBefore)
	}
	for family, count := range wantBefore {
		if got := len(before[family]); got != count {
			t.Fatalf("before the first sample %s has %d series, want %d", family, got, count)
		}
	}
	for _, m := range append(before[familySampleErrors], before[familyLastSample]...) {
		if v := metricValue(m); v != 0 {
			t.Fatalf("before the first sample a counter or the timestamp reads %v, want 0", v)
		}
	}

	setCapacityProbes(t, s, greenProbes)
	cs.sampleNow()
	after := gatherSampler(t, cs)
	if got := len(after[familyRatio]); got != len(capacityResources) {
		t.Fatalf("after a sample %s has %d series, want %d", familyRatio, got, len(capacityResources))
	}
	for _, family := range []string{familyMappings, familyMappingsLimit, familyAnonBytes, familyMemLimitBytes, familyDiskUsed, familyDiskSize} {
		if len(after[family]) != 1 {
			t.Fatalf("after a sample %s has %d series, want 1", family, len(after[family]))
		}
	}
	if got := len(after[familyDataBytes]); got != len(capacityDataParts) {
		t.Fatalf("after a sample %s has %d series, want %d", familyDataBytes, got, len(capacityDataParts))
	}
	if ts, _ := seriesValue(after, familyLastSample, "", ""); ts <= 0 {
		t.Fatalf("after a sample the last-sample timestamp reads %v, want the sample's time", ts)
	}
	// hoarddb_node_buckets has no failure mode, but it too is absent until the
	// first sample rather than exported as 0.
	if len(before[familyBuckets]) != 0 || len(after[familyBuckets]) != 1 {
		t.Fatalf("hoarddb_node_buckets: %d series before the first sample, %d after; want 0 and 1",
			len(before[familyBuckets]), len(after[familyBuckets]))
	}

	// The mux form (acceptance criterion 5): /metrics serves the running sampler's
	// registry beside the default one, with the same set of families.
	t.Run("served on /metrics", func(t *testing.T) {
		waitFirstSample(t, s.capacity)
		s.capacity.sampleNow()
		body := scrapeMetrics(t, s)
		registered := gatherCapacity(t, s)
		for _, family := range capacityFamilies {
			if len(registered[family]) == 0 {
				t.Fatalf("precondition: the registry holds no %s after a green sample", family)
			}
			if !served(body, family) {
				t.Fatalf("/metrics does not serve %s", family)
			}
		}
		for _, resource := range capacityResources {
			if !strings.Contains(body, familyRatio+`{resource="`+resource+`"}`) {
				t.Fatalf("/metrics does not serve the %s ratio", resource)
			}
		}
	})
}

// familyNames lists a gathered registry's family names, sorted.
func familyNames(gathered map[string][]*dto.Metric) []string {
	names := make([]string, 0, len(gathered))
	for name := range gathered {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// TestCapacityRatioIsAlwaysLabelled (spec T-4) proves no hoarddb_node_capacity
// series lacks the resource label, and the label's values are exactly the
// four resources. The last-sample timestamp is exempt by exact name: the
// spec's own metric table defines it as the one unlabelled series.
func TestCapacityRatioIsAlwaysLabelled(t *testing.T) {
	s := newTestServer(t)
	setCapacityProbes(t, s, greenProbes)
	cs := newIdleSampler(t, s)
	cs.sampleNow()

	want := strings.Join(capacityResources, ",")
	for family, series := range gatherSampler(t, cs) {
		if !strings.HasPrefix(family, "hoarddb_node_capacity") || family == familyLastSample {
			continue
		}
		var values []string
		for _, m := range series {
			value := labelValue(m, "resource")
			if value == "" {
				t.Fatalf("%s has a series with no resource label", family)
			}
			values = append(values, value)
		}
		sort.Strings(values)
		wantSorted := append([]string(nil), capacityResources...)
		sort.Strings(wantSorted)
		if strings.Join(values, ",") != strings.Join(wantSorted, ",") {
			t.Fatalf("%s carries resources %v, want exactly %s", family, values, want)
		}
	}
}

// TestCapacityRatioUsesTheLiveLimit (spec T-5) proves the fds ratio divides by
// the limit read at sample time, never a compiled one.
func TestCapacityRatioUsesTheLiveLimit(t *testing.T) {
	s := newTestServer(t)
	cs := newIdleSampler(t, s)
	maxFDsBefore := defaultRegistryValue(t, "process_max_fds")
	for _, tt := range []struct {
		limit int64
		want  float64
	}{
		{limit: 1000, want: 0.80},
		{limit: 4000, want: 0.20},
	} {
		setCapacityProbes(t, s, func(p *capacityProbes) {
			greenProbes(p)
			p.openFDs = func() (int64, int64, error) { return 800, tt.limit, nil }
		})
		cs.sampleNow()
		got, ok := seriesValue(gatherSampler(t, cs), familyRatio, "resource", resourceFDs)
		if !ok || got != tt.want {
			t.Fatalf("fds ratio at 800 of %d = %v (present %v), want %v", tt.limit, got, ok, tt.want)
		}
	}
	if after := defaultRegistryValue(t, "process_max_fds"); after != maxFDsBefore {
		t.Fatalf("process_max_fds moved from %v to %v; the injected limit must not reach it", maxFDsBefore, after)
	}
}

// defaultRegistryValue reads one unlabelled series from the default registry.
func defaultRegistryValue(t *testing.T, name string) float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gathering the default registry: %v", err)
	}
	for _, family := range families {
		if family.GetName() == name && len(family.GetMetric()) == 1 {
			return metricValue(family.GetMetric()[0])
		}
	}
	t.Fatalf("the default registry has no single %s series", name)
	return 0
}

// TestDiskRatioAgreesWithTheJoinGuard (spec T-6) proves the disk ratio, both
// byte gauges and the join guard come from one probe member at one instant.
// The build doc orders the readings 90% then 80%; they run 80% then 90% here,
// because from red the hysteresis (step 4) holds a node at 0.80 red until it
// falls below threshold − 0.05, and that is not what this test is about.
func TestDiskRatioAgreesWithTheJoinGuard(t *testing.T) {
	tc := newTestCluster(t, 1, 1)
	defer tc.stop()
	node := tc.nodes[0].server.core
	waitFirstSample(t, node.capacity)

	for _, tt := range []struct {
		bavail       uint64
		wantRatio    float64
		wantState    capacityState
		wantHeadroom bool
	}{
		{bavail: 200, wantRatio: 0.80, wantState: stateApproaching, wantHeadroom: true},
		{bavail: 100, wantRatio: 0.90, wantState: stateRed, wantHeadroom: false},
	} {
		st := diskStat{Blocks: 1000, Bavail: tt.bavail, Frsize: 4096}
		setCapacityProbes(t, node, func(p *capacityProbes) {
			greenProbes(p)
			p.diskStatfs = fixedDisk(st)
		})
		node.capacity.sampleNow()
		gathered := gatherCapacity(t, node)
		ratio, _ := seriesValue(gathered, familyRatio, "resource", resourceDisk)
		used, _ := seriesValue(gathered, familyDiskUsed, "", "")
		size, _ := seriesValue(gathered, familyDiskSize, "", "")
		threshold, _ := seriesValue(gathered, familyThreshold, "resource", resourceDisk)
		if ratio != tt.wantRatio || used != float64((1000-tt.bavail)*4096) || size != 1000*4096 {
			t.Fatalf("Bavail %d: ratio %v used %v size %v, want %v, %d, %d", tt.bavail, ratio, used, size, tt.wantRatio, (1000-tt.bavail)*4096, 1000*4096)
		}
		if ratio != used/size {
			t.Fatalf("Bavail %d: ratio %v is not used/size %v: two statfs calls", tt.bavail, ratio, used/size)
		}
		if threshold != 0.85 {
			t.Fatalf("disk threshold = %v, want the default mark 0.85", threshold)
		}
		if got := rowFor(t, node.capacity, resourceDisk).state; got != tt.wantState {
			t.Fatalf("Bavail %d: disk state %v, want %v", tt.bavail, got, tt.wantState)
		}
		if got := node.cluster.diskHeadroomOK(); got != tt.wantHeadroom {
			t.Fatalf("Bavail %d: diskHeadroomOK = %v, want %v", tt.bavail, got, tt.wantHeadroom)
		}
	}
}

// TestDiskMarkIsOneComparison (build doc BT-2) proves the gauge's red line and
// the guard agree at the float boundary T-6's round numbers cannot reach: 43
// blocks free of 100 is 56.99999999999999% used, under a mark of 57, while
// the ratio 0.57 is not under the threshold 0.57.
func TestDiskMarkIsOneComparison(t *testing.T) {
	qs := newCapacityService(t, func(cfg *config.Config) { cfg.Server.DiskHighWaterPct = 57 }, nil)
	t.Cleanup(func() { _ = qs.Stop() })
	s := qs.core
	setCapacityProbes(t, s, func(p *capacityProbes) {
		greenProbes(p)
		p.diskStatfs = fixedDisk(diskStat{Blocks: 100, Bavail: 43, Frsize: 4096})
	})
	cs := newIdleSampler(t, s)
	cs.sampleNow()

	row := rowFor(t, cs, resourceDisk)
	if row.ratio < row.threshold {
		t.Fatalf("fixture no longer sits on the boundary: ratio %v < threshold %v", row.ratio, row.threshold)
	}
	if !(&Cluster{s: s}).diskHeadroomOK() {
		t.Fatal("the guard refused at 56.99999999999999% against a mark of 57")
	}
	if row.state == stateRed {
		t.Fatal("the gauge reads red where the guard still accepts a join")
	}
}

// probeFailureRow is one probe × one failure condition (spec §6.3).
type probeFailureRow struct {
	name string
	// resource is the resource whose probe fails, or "" for a data-bytes row.
	resource string
	// parts are the data_bytes parts a data-bytes row removes.
	parts  []string
	inject func(t *testing.T, p *capacityProbes)
}

// probeFailureRows is every probe × every failure condition, including the
// cgroup walk's (T-16's cases) and, on Linux, the /proc/self/status parser's.
func probeFailureRows() []probeFailureRow {
	failing := errors.New("injected probe failure")
	rows := []probeFailureRow{
		{name: "fds probe error", resource: resourceFDs, inject: func(_ *testing.T, p *capacityProbes) {
			p.openFDs = func() (int64, int64, error) { return 0, 0, failing }
		}},
		{name: "fds limit 0", resource: resourceFDs, inject: func(_ *testing.T, p *capacityProbes) {
			p.openFDs = func() (int64, int64, error) { return 10, 0, nil }
		}},
		{name: "fds limit negative", resource: resourceFDs, inject: func(_ *testing.T, p *capacityProbes) {
			p.openFDs = func() (int64, int64, error) { return 10, -1, nil }
		}},
		{name: "mappings probe error", resource: resourceMappings, inject: func(_ *testing.T, p *capacityProbes) {
			p.mappings = func() (int64, int64, error) { return 0, 0, failing }
		}},
		{name: "vm.max_map_count 0", resource: resourceMappings, inject: func(_ *testing.T, p *capacityProbes) {
			p.mappings = func() (int64, int64, error) { return 10, 0, nil }
		}},
		{name: "anonymous memory probe error", resource: resourceMemory, inject: func(_ *testing.T, p *capacityProbes) {
			p.anonMemory = func() (int64, error) { return 0, failing }
		}},
		{name: "memory limit probe error", resource: resourceMemory, inject: func(_ *testing.T, p *capacityProbes) {
			p.memoryLimit = func() (memoryLimit, error) { return memoryLimit{}, failing }
		}},
		{name: "memory limit 0", resource: resourceMemory, inject: func(_ *testing.T, p *capacityProbes) {
			p.memoryLimit = func() (memoryLimit, error) { return memoryLimit{source: limitCgroupMax}, nil }
		}},
		{name: "F-1 hybrid host whose MemTotal cannot be read", resource: resourceMemory, inject: func(t *testing.T, p *capacityProbes) {
			f := cgFixture{procCgroup: "0::/u.scope\n7:memory:/u.scope\n", mountRoot: "/", mountPoint: "/sys/fs/unified",
				levels: []cgLevel{{path: "", trueRoot: true}, {path: "u.scope", noMemory: true}}, meminfo: "MemFree: 1 kB\n"}
			paths := f.build(t)
			p.memoryLimit = func() (memoryLimit, error) { return readMemoryLimit(paths) }
		}},
		{name: "disk statfs error", resource: resourceDisk, inject: func(_ *testing.T, p *capacityProbes) {
			p.diskStatfs = func(string) (diskStat, error) { return diskStat{}, failing }
		}},
		{name: "disk zero blocks", resource: resourceDisk, inject: func(_ *testing.T, p *capacityProbes) {
			p.diskStatfs = fixedDisk(diskStat{Blocks: 0, Frsize: 4096})
		}},
		{name: "disk zero fragment size", resource: resourceDisk, inject: func(_ *testing.T, p *capacityProbes) {
			p.diskStatfs = fixedDisk(diskStat{Blocks: 100, Bavail: 50})
		}},
		{name: "store size error", parts: []string{dataPartStoreLSM, dataPartStoreVlog}, inject: func(_ *testing.T, p *capacityProbes) {
			p.storeSize = func() (int64, int64, error) { return 0, 0, errStoreSizeUnknown }
		}},
		{name: "WAL listing error", parts: []string{dataPartWAL}, inject: func(_ *testing.T, p *capacityProbes) {
			p.walBytes = func() (int64, bool, error) { return 0, true, failing }
		}},
	}
	for _, walk := range memoryWalkFailureCases() {
		rows = append(rows, probeFailureRow{name: "memory walk " + walk.name, resource: resourceMemory,
			inject: func(t *testing.T, p *capacityProbes) {
				paths := walk.fixture(t)
				p.memoryLimit = func() (memoryLimit, error) { return readMemoryLimit(paths) }
			}})
	}
	return append(rows, platformProbeFailureRows()...)
}

// TestCapacityProbeFailureRemovesTheSeries (spec T-9, parts 1-4) proves every
// probe failure takes the single failure path: from a successful sample, the
// resource's ratio, reading and limit series are deleted, its threshold stays,
// its error counter rises by exactly one, and status reads unavailable with
// no ratio. A data-bytes failure deletes its part and counts against no
// resource.
func TestCapacityProbeFailureRemovesTheSeries(t *testing.T) {
	rows := probeFailureRows()
	covered := map[string]bool{}
	for _, row := range rows {
		covered[row.resource] = true
		for _, part := range row.parts {
			covered["part:"+part] = true
		}
	}
	for _, resource := range capacityResources {
		if !covered[resource] {
			t.Fatalf("completeness: resource %q has no failure row", resource)
		}
	}
	for _, part := range capacityDataParts {
		if !covered["part:"+part] {
			t.Fatalf("completeness: data part %q has no failure row", part)
		}
	}

	s := newTestServer(t)
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			setCapacityProbes(t, s, greenProbes)
			cs := newIdleSampler(t, s)
			cs.sampleNow()
			beforeErrors := gatherSampler(t, cs)

			setCapacityProbes(t, s, func(p *capacityProbes) {
				greenProbes(p)
				row.inject(t, p)
			})
			cs.sampleNow()
			after := gatherSampler(t, cs)

			if row.resource == "" {
				for _, part := range row.parts {
					if _, ok := seriesValue(after, familyDataBytes, "part", part); ok {
						t.Fatalf("data_bytes{part=%q} survived its probe's failure", part)
					}
				}
				for _, resource := range capacityResources {
					if _, ok := seriesValue(after, familyRatio, "resource", resource); !ok {
						t.Fatalf("a data-bytes failure removed the %s ratio", resource)
					}
					assertErrorDelta(t, beforeErrors, after, resource, 0)
				}
				return
			}
			// (1) the ratio and every reading and limit series are gone.
			if _, ok := seriesValue(after, familyRatio, "resource", row.resource); ok {
				t.Fatalf("(1) the %s ratio survived its probe's failure", row.resource)
			}
			for _, family := range resourceFamilies[row.resource] {
				if len(after[family]) != 0 {
					t.Fatalf("(1) %s survived the %s probe's failure", family, row.resource)
				}
			}
			// (2) the threshold stays, so "threshold without ratio" alerts.
			if _, ok := seriesValue(after, familyThreshold, "resource", row.resource); !ok {
				t.Fatalf("(2) the %s threshold was removed", row.resource)
			}
			// (3) exactly one error, on this resource only.
			for _, resource := range capacityResources {
				want := 0.0
				if resource == row.resource {
					want = 1
				}
				assertErrorDelta(t, beforeErrors, after, resource, want)
			}
			// (4) status reads unavailable, with no ratio to mistake for 0.
			if got := rowFor(t, cs, row.resource); got.state != stateUnavailable || got.hasRatio {
				t.Fatalf("(4) status row %+v, want unavailable with no ratio", got)
			}
		})
	}
}

// assertErrorDelta checks how far one resource's error counter moved.
func assertErrorDelta(t *testing.T, before, after map[string][]*dto.Metric, resource string, want float64) {
	t.Helper()
	beforeCount, _ := seriesValue(before, familySampleErrors, "resource", resource)
	afterCount, _ := seriesValue(after, familySampleErrors, "resource", resource)
	if afterCount-beforeCount != want {
		t.Fatalf("(3) %s errors moved by %v, want %v", resource, afterCount-beforeCount, want)
	}
}

// TestCapacitySamplerStopsWithTheServer (spec T-12) proves both stop paths
// wait for the sampler before they close the store it reads.
//
// An idle sampler exits on cancel long before Stop finishes closing things,
// so "exited after Stop" alone cannot tell a Stop that waited from one that
// was merely slow. The sampler is therefore caught mid-sample: a second
// sampler goroutine is started on the server (through the real
// startCapacitySampler, so it joins the real WaitGroup) with a disk probe
// that blocks until released, and Stop must not return while it is blocked.
// Server.Stop runs on its own server: calling Service.Stop after it would
// close the store a second time.
func TestCapacitySamplerStopsWithTheServer(t *testing.T) {
	for _, stopPath := range []struct {
		name string
		stop func(qs *Service) error
	}{
		{name: "Service.Stop", stop: func(qs *Service) error { return qs.Stop() }},
		{name: "Server.Stop", stop: func(qs *Service) error { return qs.core.Stop() }},
	} {
		t.Run(stopPath.name, func(t *testing.T) {
			qs := newCapacityService(t, nil, nil)
			started := qs.core.capacity
			waitFirstSample(t, started)

			entered, release := make(chan struct{}), make(chan struct{})
			var enteredOnce sync.Once
			setCapacityProbes(t, qs.core, func(p *capacityProbes) {
				greenProbes(p)
				p.diskStatfs = func(string) (diskStat, error) {
					enteredOnce.Do(func() { close(entered) })
					<-release
					return diskFixture(10), nil
				}
			})
			blocked := newIdleSampler(t, qs.core)
			qs.core.startCapacitySampler(blocked)
			<-entered

			stopped := make(chan error, 1)
			go func() { stopped <- stopPath.stop(qs) }()
			select {
			case err := <-stopped:
				close(release)
				t.Fatalf("%s returned (%v) while a sample was still reading the node", stopPath.name, err)
			case <-time.After(300 * time.Millisecond):
			}
			close(release)
			if err := <-stopped; err != nil {
				t.Fatalf("%s: %v", stopPath.name, err)
			}
			assertExited(t, blocked, stopPath.name)
			assertExited(t, started, stopPath.name)
		})
	}
}

func assertExited(t *testing.T, cs *capacitySampler, stopPath string) {
	t.Helper()
	select {
	case <-cs.exited:
	default:
		t.Fatalf("%s returned while the capacity sampler was still running", stopPath)
	}
}

func closeTo(got, want float64) bool {
	diff := got - want
	return diff < 1e-9 && diff > -1e-9
}

// TestDiskGuardOffUsesThePolicyThreshold (spec T-19, threshold and state
// half) proves a guard of 0 still warns at the policy line with no amber, a
// guard above 100 turns off both, and the default guard has its amber line.
func TestDiskGuardOffUsesThePolicyThreshold(t *testing.T) {
	for _, tt := range []struct {
		name          string
		guardPct      int
		wantThreshold float64
		usage         map[int]capacityState
	}{
		{name: "guard off (0)", guardPct: 0, wantThreshold: 0.80,
			usage: map[int]capacityState{50: stateOK, 78: stateOK, 90: stateRed}},
		{name: "guard above 100 (150)", guardPct: 150, wantThreshold: 1.50,
			usage: map[int]capacityState{99: stateOK}},
		{name: "default guard (85)", guardPct: 85, wantThreshold: 0.85,
			usage: map[int]capacityState{74: stateOK, 76: stateApproaching}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			qs := newCapacityService(t, func(cfg *config.Config) { cfg.Server.DiskHighWaterPct = tt.guardPct }, nil)
			t.Cleanup(func() { _ = qs.Stop() })
			s := qs.core
			cs := newIdleSampler(t, s)
			if got, _ := seriesValue(gatherSampler(t, cs), familyThreshold, "resource", resourceDisk); got != tt.wantThreshold {
				t.Fatalf("published disk threshold %v, want %v", got, tt.wantThreshold)
			}
			for _, pct := range sortedKeys(tt.usage) {
				setCapacityProbes(t, s, func(p *capacityProbes) {
					greenProbes(p)
					p.diskStatfs = fixedDisk(diskFixture(pct))
				})
				cs.sampleNow()
				if got := rowFor(t, cs, resourceDisk).state; got != tt.usage[pct] {
					t.Fatalf("usage %d%%: state %v, want %v", pct, got, tt.usage[pct])
				}
				if !(&Cluster{s: s}).diskHeadroomOK() {
					t.Fatalf("usage %d%%: the guard refused with DiskHighWaterPct %d", pct, tt.guardPct)
				}
			}
		})
	}
}

func sortedKeys(m map[int]capacityState) []int {
	keys := make([]int, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	return keys
}

// TestFirstSampleIsImmediate (build doc BT-7) proves a started node samples at
// once, not on its first tick 15s later.
func TestFirstSampleIsImmediate(t *testing.T) {
	qs := newCapacityService(t, nil, nil)
	t.Cleanup(func() { _ = qs.Stop() })
	start := time.Now()
	waitFirstSample(t, qs.core.capacity)
	if waited := time.Since(start); waited >= capacitySampleInterval {
		t.Fatalf("the first sample took %v, a whole interval", waited)
	}
}

// TestTwoServersKeepSeparateCapacitySeries (build doc BT-14) is R-8's proof:
// two in-process servers keep separate series, so a failure on one is not
// hidden, or shown, by the other.
func TestTwoServersKeepSeparateCapacitySeries(t *testing.T) {
	failing, healthy := newTestServer(t), newTestServer(t)
	for _, s := range []*Server{failing, healthy} {
		waitFirstSample(t, s.capacity)
		setCapacityProbes(t, s, greenProbes)
	}
	setCapacityProbes(t, failing, func(p *capacityProbes) {
		greenProbes(p)
		p.mappings = func() (int64, int64, error) { return 0, 0, errors.New("injected mappings failure") }
	})
	failing.capacity.sampleNow()
	healthy.capacity.sampleNow()

	failingSeries, healthySeries := gatherCapacity(t, failing), gatherCapacity(t, healthy)
	if _, ok := seriesValue(failingSeries, familyRatio, "resource", resourceMappings); ok {
		t.Fatal("the failing server still publishes a mappings ratio")
	}
	if _, ok := seriesValue(healthySeries, familyRatio, "resource", resourceMappings); !ok {
		t.Fatal("the healthy server lost its mappings ratio to the other server's failure")
	}
	if got, _ := seriesValue(failingSeries, familySampleErrors, "resource", resourceMappings); got != 1 {
		t.Fatalf("failing server's mappings errors = %v, want 1", got)
	}
	if got, _ := seriesValue(healthySeries, familySampleErrors, "resource", resourceMappings); got != 0 {
		t.Fatalf("healthy server's mappings errors = %v, want 0", got)
	}
}
