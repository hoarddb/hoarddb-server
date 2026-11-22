package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

var updateGolden = flag.Bool("update", false, "rewrite testdata/summary.golden")

// stats builds a latency_ms object backed by count samples.
func stats(count int64, p50, p99 float64) latencyStats {
	return latencyStats{
		latencySummary: latencySummary{Count: count, P50: latencyMs(p50), P95: latencyMs(p50 * 2), P99: latencyMs(p99), P999: latencyMs(p99 * 2), Max: latencyMs(p99 * 4)},
		Mean:           latencyMs(p50 * 1.1),
	}
}

// testLeg is a closed-loop RH leg of hoardDB on one VM; mutators adjust it.
func testLeg(mutators ...func(*legRecord)) *legRecord {
	record := newLegRecord()
	record.Release, record.Commit, record.Date = "v1.0.0-test", "abc1234", "2026-10-24"
	record.Engine, record.EngineVersion = "hoarddb", "1.0.0"
	record.Config, record.Nodes, record.Level, record.Routing = "C1", 1, "L-A", "RP-A"
	record.WriteConcern, record.Copies = "majority", 1
	record.Placement, record.Shards = "single", 1
	record.Dataset, record.Mix, record.Loop, record.Depth = "S-100k", "RH", "closed", 64
	record.RateBasis, record.Warm, record.MeasuredS, record.Seed, record.Repeat = "self-relative", true, 300, 1, 1
	record.OpsPerSec, record.ReadsPerSec, record.WritesPerSec = 1000, 800, 200
	record.Latency = legLatency{Read: stats(240000, 1.5, 9), Write: stats(60000, 4, 20), All: stats(300000, 2, 12)}
	record.Ops["R1"] = opRecord{Count: 100, ReqPerOp: 2, Latency: stats(5000, 1, 5)}
	record.Resources.Server = []serverResources{{VM: "VM1", CPUPctP95: 55}}
	record.Resources.Clients = []clientResources{{VM: "VM7", CPUPctP95: 40, CPUUsPerOp: 80}}
	record.VM.FsyncP99DuringLeg = 4.4
	record.Verified = 5000
	for _, mutate := range mutators {
		mutate(record)
	}
	return record
}

func onRepeat(repeat int, serverVM string, opsPerSec float64) func(*legRecord) {
	return func(r *legRecord) {
		r.Repeat, r.OpsPerSec = repeat, opsPerSec
		r.Resources.Server = []serverResources{{VM: serverVM, CPUPctP95: 50 + float64(repeat)}}
	}
}

func withEngine(name string) func(*legRecord) {
	return func(r *legRecord) { r.Engine = name }
}

func TestMedianBasisSameVM(t *testing.T) {
	groups := groupLegs([]*legRecord{
		testLeg(onRepeat(1, "VM1", 900)), testLeg(onRepeat(2, "VM1", 1100)), testLeg(onRepeat(3, "VM1", 1000)),
	})
	if len(groups) != 1 {
		t.Fatalf("three repeats of one leg made %d groups, want 1", len(groups))
	}
	group := groups[0]
	if group.Basis != basisSameVM || group.Median != 1000 || group.Min != 900 || group.Max != 1100 {
		t.Errorf("basis=%q median=%v range=%v..%v; want same-vm, median 1000, range 900..1100", group.Basis, group.Median, group.Min, group.Max)
	}
	table := renderSummary(groupedRecords(groups))
	if !strings.Contains(table, labelSameVM) {
		t.Errorf("a same-vm group's row does not carry the %q label:\n%s", labelSameVM, table)
	}
}

func groupedRecords(groups []*legGroup) []*legRecord {
	var records []*legRecord
	for _, group := range groups {
		records = append(records, group.Records...)
	}
	return records
}

func TestMedianBasisRotated(t *testing.T) {
	rotated := []*legRecord{testLeg(onRepeat(1, "VM1", 900)), testLeg(onRepeat(2, "VM2", 1100)), testLeg(onRepeat(3, "VM3", 1000))}
	group := groupLegs(rotated)[0]
	if group.Basis != basisRotated || group.Median != 1000 {
		t.Errorf("basis=%q median=%v; want rotated, 1000", group.Basis, group.Median)
	}
	if table := renderSummary(rotated); strings.Contains(table, labelSameVM) {
		t.Errorf("a rotated group carries the %q label:\n%s", labelSameVM, table)
	}

	single := groupLegs([]*legRecord{testLeg()})[0]
	if single.Basis != basisSingle {
		t.Errorf("one repeat has basis %q, want single", single.Basis)
	}

	// Phase 2: three back-to-back repeats on VM1, then one repeat each on VM2 and VM3.
	mixed := groupLegs([]*legRecord{
		testLeg(onRepeat(1, "VM1", 500)), testLeg(onRepeat(2, "VM1", 500)), testLeg(onRepeat(3, "VM1", 500)),
		testLeg(onRepeat(4, "VM2", 900)), testLeg(onRepeat(5, "VM3", 1100)),
	})[0]
	if mixed.Basis != basisRotated || !mixed.HasBoth {
		t.Fatalf("same-vm repeats plus rotated ones: basis=%q hasBoth=%v; want rotated with both medians", mixed.Basis, mixed.HasBoth)
	}
	if mixed.Median != 900 || mixed.AllMedian != 500 {
		t.Errorf("rotated median %v, all-repeat median %v; want 900 and 500", mixed.Median, mixed.AllMedian)
	}
}

const wantLimitation = "LIMITATION same-vm-repeats: the median of three is of back-to-back repeats on one VM (no engine-to-VM rotation); a slow guest is not separated from the engine. Applies to: C1/hoarddb, C2/mongo."

func sameVMRepeats(mutators ...func(*legRecord)) []*legRecord {
	var records []*legRecord
	for repeat := 1; repeat <= 3; repeat++ {
		records = append(records, testLeg(append([]func(*legRecord){onRepeat(repeat, "VM1", 1000)}, mutators...)...))
	}
	return records
}

func TestHeaderCarriesSameVMLimitation(t *testing.T) {
	records := append(sameVMRepeats(), sameVMRepeats(withEngine("mongo"), func(r *legRecord) { r.Config = "C2" })...)
	header := summaryHeader(groupLegs(records))
	if !strings.Contains(header, wantLimitation+"\n") {
		t.Errorf("header lacks the computed limitation line.\nwant: %s\ngot:\n%s", wantLimitation, header)
	}
	if !strings.Contains(header, "C2 to C5 never rotate") {
		t.Errorf("header does not say that C2 to C5 never rotate:\n%s", header)
	}
}

func TestHeaderOmitsLimitationWhenAllRotated(t *testing.T) {
	records := []*legRecord{testLeg(onRepeat(1, "VM1", 900)), testLeg(onRepeat(2, "VM2", 1100)), testLeg(onRepeat(3, "VM3", 1000))}
	header := summaryHeader(groupLegs(records))
	if strings.Contains(header, "LIMITATION") {
		t.Errorf("every group is rotated, yet the header prints a limitation:\n%s", header)
	}
	if !strings.Contains(header, "CAVEAT (6)") {
		t.Errorf("header has no caveat (6) when all groups are rotated:\n%s", header)
	}
}

func TestHeaderHasTheFiveFixedCaveats(t *testing.T) {
	header := summaryHeader(groupLegs([]*legRecord{testLeg()}))
	fixed := []string{
		"(1) These guests complete a synced 4 KiB write in 3.6 to 4.3 ms, about 250 syncs a second; replicated hoardDB and MySQL pay two sequential syncs before the acknowledgement (owner, then replica), MongoDB journals on the members in parallel; none of it transfers to local NVMe, where a sync costs tens of microseconds.",
		"(2) The virtual disk's write-back caching behaviour was not verified.",
		"(3) The host has a hybrid-core CPU; its effect is visible only through the canary.",
		"(4) The fsync sidecar adds one synced write per second per server VM.",
		"(5) The development database that calibrates 16.4 is a development database.",
	}
	for _, caveat := range fixed {
		if !strings.Contains(header, "CAVEAT "+caveat) {
			t.Errorf("header lacks %q", caveat)
		}
	}
}

func TestReportSchemaHasRepeatField(t *testing.T) {
	encoded, err := json.Marshal(testLeg(onRepeat(2, "VM1", 1000)))
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil {
		t.Fatal(err)
	}
	if string(fields["repeat"]) != "2" {
		t.Errorf("repeat = %s, want 2", fields["repeat"])
	}
	// The field is optional on input: a record written before it existed reads as repeat 0.
	path := filepath.Join(t.TempDir(), "old.json")
	old := strings.Replace(string(encoded), `"repeat":2,`, "", 1)
	if old == string(encoded) {
		t.Fatal("test bug: repeat key not found to strip")
	}
	if err := os.WriteFile(path, []byte(old), 0o600); err != nil {
		t.Fatal(err)
	}
	records, err := readLegRecords(path)
	if err != nil || len(records) != 1 || records[0].Repeat != 0 {
		t.Errorf("record without repeat: %v, %v; want one record with repeat 0", records, err)
	}
	if doc, err := os.ReadFile("REPORT.md"); err != nil || !strings.Contains(string(doc), "`repeat`") || !strings.Contains(string(doc), labelSameVM) {
		t.Errorf("REPORT.md must document repeat and %s: %v", labelSameVM, err)
	}
}

// specKeys are every key of the leg record of spec 9.2, as dotted paths; "[]"
// is a list element and "{}" a map entry.
var specKeys = []string{
	"schema", "release", "commit", "date", "engine", "engine_version", "engine_settings",
	"config", "nodes", "level", "routing", "write_concern", "copies",
	"placement", "shards", "owners_per_node", "holders_of_primary_bucket", "copies_below_factor",
	"dataset", "mix", "loop", "depth", "offered_rate", "rate_basis", "utilisation",
	"warm", "warmup_s", "measured_s", "seed", "repeat", "ops_per_s", "reads_per_s", "writes_per_s",
	"ops", "ops.{}.count", "ops.{}.req_per_op",
	"ops.{}.latency_ms.p50", "ops.{}.latency_ms.p95", "ops.{}.latency_ms.p99", "ops.{}.latency_ms.p999", "ops.{}.latency_ms.max", "ops.{}.latency_ms.mean",
	"latency_ms.read.p50", "latency_ms.read.p95", "latency_ms.read.p99", "latency_ms.read.p999", "latency_ms.read.max", "latency_ms.read.mean",
	"latency_ms.write.p999", "latency_ms.all.p999",
	"histogram", "errors_by_type", "verified_reads", "misses",
	"resources.server.[].vm", "resources.server.[].cpu_pct_p95", "resources.server.[].rss_mb_max", "resources.server.[].disk_write_mb_s_p95", "resources.server.[].steal_pct",
	"resources.clients.[].vm", "resources.clients.[].cpu_pct_p95", "resources.clients.[].cpu_us_per_op", "resources.shard_top5_op_share",
	"vm.fsync_ms", "vm.steal_pct", "vm.nproc", "vm.mem_mb", "vm.fsync_ms_p50_during_leg", "vm.fsync_ms_p99_during_leg", "vm.fsync_ms_max_during_leg", "vm.concurrent_fsync_vms",
	"canary.before.fsync_ms", "canary.before.cpu_loop_ms", "canary.after.fsync_ms", "canary.after.cpu_loop_ms", "canary.drift_pct",
	"witness.single_sync_ms_p50", "witness.depth1_write_p50_ms", "witness.fsyncs_per_ack",
	"labels", "integrity_failures", "notes",
}

func hasKeyPath(node interface{}, path []string) bool {
	if len(path) == 0 {
		return true
	}
	switch typed := node.(type) {
	case map[string]interface{}:
		if path[0] == "{}" {
			for _, child := range typed {
				if hasKeyPath(child, path[1:]) {
					return true
				}
			}
			return false
		}
		child, present := typed[path[0]]
		return present && hasKeyPath(child, path[1:])
	case []interface{}:
		return path[0] == "[]" && len(typed) > 0 && hasKeyPath(typed[0], path[1:])
	default:
		return false
	}
}

func TestReportSchemaHasEveryField(t *testing.T) {
	var out bytes.Buffer
	// Repeat 0 and every other zero value must still be written: a missing key
	// is not the same as zero when two releases are diffed.
	if err := writeLegRecord(&out, testLeg(func(r *legRecord) { r.Repeat = 0 })); err != nil {
		t.Fatal(err)
	}
	if strings.Count(out.String(), "\n") != 1 {
		t.Errorf("a record must be one JSON line, got %q", out.String())
	}
	var decoded map[string]interface{}
	if err := json.Unmarshal(out.Bytes(), &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded["schema"] != "perf-record/2" {
		t.Errorf("schema = %v", decoded["schema"])
	}
	for _, key := range specKeys {
		if !hasKeyPath(decoded, strings.Split(key, ".")) {
			t.Errorf("record lacks %s", key)
		}
	}
}

func TestRecordCarriesPlacementAndShards(t *testing.T) {
	sharded := testLeg(func(r *legRecord) {
		r.Placement, r.Shards = "shard-64", 64
		r.OwnersPerNode = []int{21, 22, 21}
		r.HoldersOfPrimaryBucket = []string{"VM1", "VM2"}
	})
	var out bytes.Buffer
	if err := writeLegRecord(&out, sharded); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "leg.json")
	if err := os.WriteFile(path, out.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	records, err := readLegRecords(path)
	if err != nil {
		t.Fatal(err)
	}
	got := records[0]
	if got.Placement != "shard-64" || got.Shards != 64 || len(got.OwnersPerNode) != 3 || got.HoldersOfPrimaryBucket[1] != "VM2" {
		t.Errorf("placement fields did not survive a round trip: %+v", got)
	}
	// Two releases must not be diffed across placement forms: the same leg in two forms is two groups.
	groups := groupLegs([]*legRecord{testLeg(), sharded})
	if len(groups) != 2 {
		t.Errorf("single and shard-64 legs were merged into %d group(s); placement must separate them", len(groups))
	}
	if table := renderSummary([]*legRecord{sharded}); !strings.Contains(table, "shard-64") {
		t.Errorf("the table does not name the placement form:\n%s", table)
	}
}

func scalingRecords(mutators ...func(*legRecord)) []*legRecord {
	var records []*legRecord
	for nodes, rate := range map[int]float64{1: 1000, 3: 2400} {
		records = append(records, testLeg(append([]func(*legRecord){func(r *legRecord) {
			r.Nodes, r.OpsPerSec, r.Copies, r.Config = nodes, rate, min(nodes, 3), map[int]string{1: "C1", 3: "C3"}[nodes]
		}}, mutators...)...))
	}
	return records
}

func TestEfficiencyColumnOmittedWhenClientBound(t *testing.T) {
	// 2400 ops/s on 3 nodes against 1000 on one: efficiency 2400 / (3 x 1000) = 0.80.
	free := renderSummary(scalingRecords())
	if !strings.Contains(free, "efficiency") || !strings.Contains(free, "0.800") || !strings.Contains(free, "copies/nodes") {
		t.Errorf("a plain scaling table must show efficiency 0.800 with copies/nodes beside it:\n%s", free)
	}
	bound := renderSummary(scalingRecords(func(r *legRecord) {
		if r.Nodes == 3 {
			r.Labels = append(r.Labels, "client-bound")
		}
	}))
	if strings.Contains(bound, "efficiency") {
		t.Errorf("a client-bound configuration still prints an efficiency column:\n%s", bound)
	}
	oneOwner := renderSummary(scalingRecords(func(r *legRecord) { r.Mix = "WH" }))
	if strings.Contains(oneOwner, "efficiency") {
		t.Errorf("hoardDB writes in the H-1 form still print an efficiency column:\n%s", oneOwner)
	}
	if !strings.Contains(oneOwner, "one owner") {
		t.Errorf("an H-1 hoardDB table must say \"one owner\":\n%s", oneOwner)
	}
	mongoWrites := renderSummary(scalingRecords(withEngine("mongo"), func(r *legRecord) { r.Mix = "WH" }))
	if !strings.Contains(mongoWrites, "efficiency") {
		t.Errorf("only hoardDB writes lose the efficiency column; MongoDB writes must keep it:\n%s", mongoWrites)
	}
}

func TestPercentileBelowThousandSamplesPrintsNA(t *testing.T) {
	few := testLeg(func(r *legRecord) { r.Latency.Read = stats(999, 1.234, 5.678) })
	table := renderSummary([]*legRecord{few})
	if !strings.Contains(table, "n.a.") || strings.Contains(table, "1.23") || strings.Contains(table, "5.68") {
		t.Errorf("999 read samples must print n.a., never a percentile:\n%s", table)
	}
	csv, err := renderCSV([]*legRecord{few})
	if err != nil || !strings.Contains(csv, "n.a.") || strings.Contains(csv, "1.23") {
		t.Errorf("the CSV must print n.a. below 1,000 samples: %v\n%s", err, csv)
	}
	enough := testLeg(func(r *legRecord) { r.Latency.Read = stats(1000, 1.234, 5.678) })
	if table := renderSummary([]*legRecord{enough}); !strings.Contains(table, "1.23") || !strings.Contains(table, "5.68") {
		t.Errorf("1,000 read samples must print numbers rounded to three figures:\n%s", table)
	}
}

func TestClosedLoopP99LabelledCoordinatedOmission(t *testing.T) {
	const label = "closed-loop p99, affected by coordinated omission"
	if table := renderSummary([]*legRecord{testLeg()}); !strings.Contains(table, label) {
		t.Errorf("a closed-loop table lacks %q:\n%s", label, table)
	}
	open := testLeg(func(r *legRecord) {
		rate := 500.0
		r.Loop, r.OfferedRate, r.RateBasis = "open", &rate, "common"
		r.Utilisation = new(float64)
	})
	if table := renderSummary([]*legRecord{open}); strings.Contains(table, label) {
		t.Errorf("an open-loop table carries the closed-loop label:\n%s", table)
	}
}

func goldenRecords() []*legRecord {
	records := sameVMRepeats()
	records = append(records, sameVMRepeats(withEngine("mongo"), func(r *legRecord) {
		r.Latency.Read = stats(240000, 2.2, 11)
	})...)
	records = append(records, scalingRecords(func(r *legRecord) { r.Mix = "WH"; r.Level = "L-R"; r.Routing = "RP-B" })...)
	rate := 500.0
	utilisation := 0.4
	records = append(records, testLeg(func(r *legRecord) {
		r.Loop, r.OfferedRate, r.Utilisation, r.RateBasis = "open", &rate, &utilisation, "common"
		r.Labels = []string{"steal"}
		r.Integrity = []string{"tls check failed"}
		r.Witness.FsyncsPerAck = 1.01
	}))
	return records
}

func TestSummaryGolden(t *testing.T) {
	got := summaryHeader(groupLegs(goldenRecords())) + renderSummary(goldenRecords())
	golden := filepath.Join("testdata", "summary.golden")
	if *updateGolden {
		if err := os.MkdirAll("testdata", 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(golden, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(golden)
	if err != nil {
		t.Fatalf("reading the golden file: %v (rerun with -update to create it, then read it)", err)
	}
	if string(want) != got {
		t.Errorf("summary differs from %s (rerun with -update to accept a deliberate change):\n--- got\n%s", golden, got)
	}
}

func TestSummarizeFlagReadsFiles(t *testing.T) {
	cfg, err := parseFlags([]string{"-summarize", "-csv", "a.json", "b.json"})
	if err != nil || !cfg.Summarize || !cfg.CSV || len(cfg.Files) != 2 {
		t.Fatalf("parseFlags(-summarize -csv a b) = %+v, %v", cfg, err)
	}
	if _, err := parseFlags([]string{"-summarize"}); err == nil {
		t.Error("-summarize with no files was accepted")
	}
	path := filepath.Join(t.TempDir(), "leg.json")
	var line bytes.Buffer
	if err := writeLegRecord(&line, testLeg()); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, line.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := runSummarize([]string{path}, false, &out); err != nil || !strings.Contains(out.String(), "CAVEAT (1)") {
		t.Errorf("runSummarize = %v, output:\n%s", err, out.String())
	}
	out.Reset()
	if err := runSummarize([]string{path}, true, &out); err != nil || !strings.HasPrefix(out.String(), "config,engine,") {
		t.Errorf("runSummarize -csv = %v, output:\n%s", err, out.String())
	}
	if err := runSummarize([]string{filepath.Join(t.TempDir(), "missing.json")}, false, &out); err == nil {
		t.Error("a missing file was not an error")
	}
}

func TestNewLatencyStatsCountsAndAveragesSamples(t *testing.T) {
	hist := newHistogram()
	for sample := 0; sample < minSamplesForPercentile; sample++ {
		mustRecord(t, hist, 2*time.Millisecond)
	}
	stats := newLatencyStats(hist)
	if stats.Count != minSamplesForPercentile || float64(stats.Mean) < 1.9 || float64(stats.Mean) > 2.1 {
		t.Errorf("count=%d mean=%v ms; want 1000 samples with a mean of about 2 ms", stats.Count, stats.Mean)
	}
	if fewer := newLatencyStats(newHistogram()); !math.IsNaN(float64(fewer.Mean)) || !math.IsNaN(float64(fewer.P99)) {
		t.Errorf("an empty histogram has mean %v and p99 %v; both must be n.a.", fewer.Mean, fewer.P99)
	}
}
