package main

import (
	"cmp"
	"encoding/csv"
	"fmt"
	"io"
	"math"
	"slices"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"
)

// Median bases of a group of repeats (BD-4).
const (
	basisSingle  = "single"
	basisSameVM  = "same-vm"
	basisRotated = "rotated"
	labelSameVM  = "same-vm-repeats"
	// labelClientBound drops the efficiency column of every scaling table that
	// holds a leg carrying it (spec 9.4 item 3).
	labelClientBound = "client-bound"
	notAvailable     = "n.a."
	closedLoopNote   = "closed-loop p99, affected by coordinated omission"
	semiSyncNote     = "MySQL semi-sync: replica has received and flushed; hoardDB: replica has applied; MongoDB: majority commit point"
)

// fixedCaveats are caveats (1) to (5) of spec 9.5, printed above every
// summary, with the spec's markdown emphasis removed. Caveat (6) is computed.
var fixedCaveats = []string{
	"(1) These guests complete a synced 4 KiB write in 3.6 to 4.3 ms, about 250 syncs a second; replicated hoardDB and MySQL pay two sequential syncs before the acknowledgement (owner, then replica), MongoDB journals on the members in parallel; none of it transfers to local NVMe, where a sync costs tens of microseconds.",
	"(2) The virtual disk's write-back caching behaviour was not verified.",
	"(3) The host has a hybrid-core CPU; its effect is visible only through the canary.",
	"(4) The fsync sidecar adds one synced write per second per server VM.",
	"(5) The development database that calibrates 16.4 is a development database.",
}

// groupKey names what one median is of: every field that makes two legs
// incomparable, placement included so two releases cannot be diffed across
// placement forms.
type groupKey struct {
	Config, Engine, Placement, Mix, Loop string
	Depth                                int
	Level, Routing                       string
	// Variant and RateBasis keep a cold, cache-raised or memory-limited leg, and an open leg at the
	// common rate against one at 50% of the engine's own median, out of the primary leg's repeats.
	Variant, RateBasis string
	// Fanout keeps a leg whose publish wrote one feed entry per request apart
	// from one that wrote them in bulk: their request counts are not comparable.
	Fanout string
}

// legGroup is the repeats of one leg and the median read from them.
type legGroup struct {
	Key     groupKey
	Records []*legRecord
	Basis   string
	// Median, Min and Max are of ops/s over the repeats the basis selects.
	Median, Min, Max float64
	// AllMedian is the median over every repeat. HasBoth is set when rotated
	// repeats were added to same-VM ones, so that both medians are printed.
	AllMedian float64
	HasBoth   bool
	// Typical is the repeat whose ops/s is nearest the median; its latencies
	// and resources stand for the group.
	Typical *legRecord
}

func (key groupKey) compare(other groupKey) int {
	return cmp.Or(
		cmp.Compare(key.Config, other.Config), cmp.Compare(key.Mix, other.Mix), cmp.Compare(key.Loop, other.Loop),
		cmp.Compare(key.Depth, other.Depth), cmp.Compare(key.Level, other.Level), cmp.Compare(key.Routing, other.Routing),
		cmp.Compare(key.Engine, other.Engine), cmp.Compare(key.Placement, other.Placement),
		cmp.Compare(key.Variant, other.Variant), cmp.Compare(key.RateBasis, other.RateBasis), cmp.Compare(key.Fanout, other.Fanout))
}

// serverVMs names the VMs a leg's servers ran on, sorted, as one comparable string.
func serverVMs(record *legRecord) string {
	names := make([]string, 0, len(record.Resources.Server))
	for _, server := range record.Resources.Server {
		names = append(names, server.VM)
	}
	slices.Sort(names)
	return strings.Join(names, ",")
}

func median(values []float64) float64 {
	sorted := slices.Sorted(slices.Values(values))
	middle := len(sorted) / 2
	if len(sorted)%2 == 1 {
		return sorted[middle]
	}
	return (sorted[middle-1] + sorted[middle]) / 2
}

// groupLegs gathers records into groups of repeats and reads each group's
// median and basis (BD-4): single for one repeat, same-vm when every repeat
// ran on one set of server VMs, rotated otherwise. Once rotated repeats join
// same-VM ones, the median is of the per-VM-set medians and the median of all
// repeats is kept beside it.
func groupLegs(records []*legRecord) []*legGroup {
	byKey := map[groupKey]*legGroup{}
	for _, record := range records {
		key := groupKey{record.Config, record.Engine, record.Placement, record.Mix, record.Loop, record.Depth, record.Level, record.Routing,
			record.EngineSettings["variant"], record.RateBasis, record.Fanout}
		if byKey[key] == nil {
			byKey[key] = &legGroup{Key: key}
		}
		byKey[key].Records = append(byKey[key].Records, record)
	}
	groups := make([]*legGroup, 0, len(byKey))
	for _, group := range byKey {
		group.settle()
		groups = append(groups, group)
	}
	slices.SortFunc(groups, func(a, b *legGroup) int { return a.Key.compare(b.Key) })
	return groups
}

func (group *legGroup) settle() {
	slices.SortStableFunc(group.Records, func(a, b *legRecord) int { return cmp.Compare(a.Repeat, b.Repeat) })
	perSet := map[string][]float64{}
	var every []float64
	for _, record := range group.Records {
		perSet[serverVMs(record)] = append(perSet[serverVMs(record)], record.OpsPerSec)
		every = append(every, record.OpsPerSec)
	}
	group.AllMedian = median(every)
	used := every
	switch {
	case len(every) == 1:
		group.Basis = basisSingle
	case len(perSet) == 1:
		group.Basis = basisSameVM
	default:
		group.Basis = basisRotated
		if len(perSet) < len(every) {
			group.HasBoth = true
			used = used[:0:0]
			for _, values := range perSet {
				used = append(used, median(values))
			}
		}
	}
	group.Median = median(used)
	group.Min, group.Max = slices.Min(used), slices.Max(used)
	group.Typical = group.Records[0]
	for _, record := range group.Records {
		if math.Abs(record.OpsPerSec-group.Median) < math.Abs(group.Typical.OpsPerSec-group.Median) {
			group.Typical = record
		}
	}
}

// labels is every label any repeat carries, plus the computed same-vm-repeats.
func (group *legGroup) labels() []string {
	set := map[string]bool{}
	for _, record := range group.Records {
		for _, label := range record.Labels {
			set[label] = true
		}
	}
	if group.Basis == basisSameVM {
		set[labelSameVM] = true
	}
	return slices.Sorted(func(yield func(string) bool) {
		for label := range set {
			if !yield(label) {
				return
			}
		}
	})
}

func (group *legGroup) integrityFailures() []string {
	set := map[string]bool{}
	for _, record := range group.Records {
		for _, failure := range record.Integrity {
			set[failure] = true
		}
	}
	failures := make([]string, 0, len(set))
	for failure := range set {
		failures = append(failures, failure)
	}
	sort.Strings(failures)
	return failures
}

func (group *legGroup) label() string {
	key := group.Key
	return fmt.Sprintf("%s/%s", key.Config, key.Engine)
}

// summaryHeader is printed above every summary: the five fixed caveats of spec
// 9.5 and the computed caveat (6), which names the groups whose median rests
// on back-to-back repeats of one VM.
func summaryHeader(groups []*legGroup) string {
	var header strings.Builder
	header.WriteString("hoardDB certification summary (perf-record/2)\n")
	for _, caveat := range fixedCaveats {
		header.WriteString("CAVEAT " + caveat + "\n")
	}
	var applies []string
	var neverRotates bool
	for _, group := range groups {
		if group.Basis != basisSameVM {
			continue
		}
		if !slices.Contains(applies, group.label()) {
			applies = append(applies, group.label())
		}
		neverRotates = neverRotates || isMultiNodeConfig(group.Key.Config)
	}
	if len(applies) == 0 {
		header.WriteString("CAVEAT (6) No median below rests on back-to-back repeats of one VM.\n")
		return header.String()
	}
	header.WriteString("LIMITATION " + labelSameVM + ": the median of three is of back-to-back repeats on one VM (no engine-to-VM rotation); a slow guest is not separated from the engine. Applies to: " + strings.Join(applies, ", ") + ".\n")
	if neverRotates {
		header.WriteString("NOTE C2 to C5 never rotate in either phase (spec 3.1): their groups carry " + labelSameVM + " in both phases.\n")
	}
	return header.String()
}

// isMultiNodeConfig is true for C2 to C5 (including C5a and C5b), which the
// engine-to-VM rotation never covers.
func isMultiNodeConfig(config string) bool {
	return len(config) >= 2 && config[0] == 'C' && config[1] >= '2' && config[1] <= '5'
}

// sig3 rounds to three significant figures and prints without an exponent.
func sig3(value float64) string {
	if value == 0 || math.IsNaN(value) || math.IsInf(value, 0) {
		return strconv.FormatFloat(value, 'f', -1, 64)
	}
	magnitude := int(math.Floor(math.Log10(math.Abs(value))))
	scale := math.Pow(10, float64(magnitude-2))
	return strconv.FormatFloat(math.Round(value/scale)*scale, 'f', max(0, 2-magnitude), 64)
}

// percentileCell prints a percentile, or n.a. when fewer than
// minSamplesForPercentile samples back it, whatever the file claims.
func percentileCell(samples int64, value latencyMs) string {
	if samples < minSamplesForPercentile || math.IsNaN(float64(value)) {
		return notAvailable
	}
	return sig3(float64(value))
}

func optional(value *float64) string {
	if value == nil {
		return "-"
	}
	return sig3(*value)
}

// copiesCell prints the count of writes acknowledged below the factor, "-" when
// it was not measured.
func copiesCell(count *int) string {
	if count == nil {
		return "-"
	}
	return strconv.Itoa(*count)
}

func joinVMs[T any](items []T, describe func(T) string) string {
	if len(items) == 0 {
		return "-"
	}
	parts := make([]string, len(items))
	for index, item := range items {
		parts[index] = describe(item)
	}
	return strings.Join(parts, " ")
}

func serverCPU(record *legRecord) string {
	return joinVMs(record.Resources.Server, func(s serverResources) string { return s.VM + "=" + sig3(s.CPUPctP95) })
}

func clientCPU(record *legRecord) string {
	return joinVMs(record.Resources.Clients, func(c clientResources) string { return c.VM + "=" + sig3(c.CPUPctP95) })
}

// requestsPerOperation is the operation-weighted mean of req_per_op.
func requestsPerOperation(record *legRecord) string {
	var requests float64
	var operations int64
	for _, op := range record.Ops {
		requests += op.ReqPerOp * float64(op.Count)
		operations += op.Count
	}
	if operations == 0 {
		return "-"
	}
	return sig3(requests / float64(operations))
}

func latencyCells(stats latencyStats) []string {
	return []string{
		percentileCell(stats.Count, stats.P50), percentileCell(stats.Count, stats.P95),
		percentileCell(stats.Count, stats.P99), percentileCell(stats.Count, stats.P999),
	}
}

// tableWriter collects tables; each is a heading line and aligned rows.
type tableWriter struct{ out strings.Builder }

func (w *tableWriter) table(heading string, columns []string, rows [][]string) {
	w.out.WriteString("\n" + heading + "\n")
	widths := make([]int, len(columns))
	for _, row := range append([][]string{columns}, rows...) {
		for index, cell := range row {
			widths[index] = max(widths[index], utf8.RuneCountInString(cell))
		}
	}
	for _, row := range append([][]string{columns}, rows...) {
		var line strings.Builder
		for index, cell := range row {
			line.WriteString(cell)
			line.WriteString(strings.Repeat(" ", widths[index]-utf8.RuneCountInString(cell)+2))
		}
		w.out.WriteString(strings.TrimRight(line.String(), " ") + "\n")
	}
}

// placementNote adds the words the spec requires for hoardDB in the H-1 form.
func placementNote(group *legGroup) string {
	placement := group.Key.Placement
	if group.Key.Engine == "hoarddb" && group.Key.Placement == "single" {
		placement += " (one owner)"
	}
	if group.Key.Variant != "" {
		placement += " [" + group.Key.Variant + "]"
	}
	if group.Key.Fanout != "" && group.Key.Engine != "hoarddb" {
		placement += " [fanout " + group.Key.Fanout + "]"
	}
	if group.Key.Loop == "open" && group.Key.RateBasis != "" {
		placement += " [rate " + group.Key.RateBasis + "]"
	}
	return placement
}

// headingFacts is the depth, level, routing and loop every table prints in its
// heading, because earlier benchmarks were misread by quoting different depths
// side by side.
func headingFacts(key groupKey) string {
	facts := fmt.Sprintf("depth %d, level %s, routing %s, loop %s", key.Depth, key.Level, key.Routing, key.Loop)
	if key.Loop == "closed" {
		facts += " (" + closedLoopNote + ")"
	}
	if key.Level == "L-R" {
		facts += "; " + semiSyncNote
	}
	return facts
}

type tableKey struct {
	Config, Mix, Loop string
	Depth             int
	Level, Routing    string
}

func (group *legGroup) tableKey() tableKey {
	key := group.Key
	return tableKey{key.Config, key.Mix, key.Loop, key.Depth, key.Level, key.Routing}
}

// renderSummary prints the reading tables of spec 9.4 for the records.
func renderSummary(records []*legRecord) string {
	groups := groupLegs(records)
	var tables tableWriter
	writeLegTables(&tables, groups)
	writeOperationTables(&tables, groups)
	writeScalingTables(&tables, groups)
	writeReplicationCost(&tables, groups)
	writeFsyncTable(&tables, groups)
	writeIntegrity(&tables, groups)
	return tables.out.String()
}

func writeLegTables(tables *tableWriter, groups []*legGroup) {
	columns := []string{"engine", "placement", "median-basis", "repeats", "ops/s", "range", "ops/s all repeats", "reads/s", "writes/s",
		"read p50", "read p95", "read p99", "read p99.9", "write p50", "write p95", "write p99", "write p99.9",
		"fsync p99 ms", "server CPU p95", "top5 shard share", "client CPU p95", "req/op", "copies below factor", "offered", "utilisation", "labels"}
	var heading string
	var rows [][]string
	flush := func() {
		if len(rows) > 0 {
			tables.table(heading, columns, rows)
		}
		rows = nil
	}
	var current tableKey
	for index, group := range groups {
		if index == 0 || group.tableKey() != current {
			flush()
			current = group.tableKey()
			heading = fmt.Sprintf("TABLE 1 per configuration: config %s, mix %s, dataset %s, %s", group.Key.Config, group.Key.Mix, group.Typical.Dataset, headingFacts(group.Key))
		}
		typical := group.Typical
		allMedian := "-"
		if group.HasBoth {
			allMedian = sig3(group.AllMedian)
		}
		offered, utilisation := optional(typical.OfferedRate), optional(typical.Utilisation)
		rows = append(rows, append(append([]string{group.Key.Engine, placementNote(group), group.Basis, strconv.Itoa(len(group.Records)),
			sig3(group.Median), sig3(group.Min) + ".." + sig3(group.Max), allMedian, sig3(typical.ReadsPerSec), sig3(typical.WritesPerSec)},
			append(latencyCells(typical.Latency.Read), latencyCells(typical.Latency.Write)...)...),
			sig3(typical.VM.FsyncP99DuringLeg), serverCPU(typical), optional(typical.Resources.ShardTop5Share), clientCPU(typical),
			requestsPerOperation(typical), copiesCell(typical.CopiesBelowFactor), offered, utilisation, strings.Join(group.labels(), ",")))
	}
	flush()
}

func writeOperationTables(tables *tableWriter, groups []*legGroup) {
	columns := []string{"operation", "engine", "placement", "requests/op", "p50", "p99", "p99.9"}
	var heading string
	var rows [][]string
	flush := func() {
		if len(rows) > 0 {
			tables.table(heading, columns, rows)
		}
		rows = nil
	}
	var current tableKey
	started := false
	for _, group := range groups {
		if len(group.Typical.Ops) == 0 {
			continue
		}
		if !started || group.tableKey() != current {
			flush()
			started, current = true, group.tableKey()
			heading = fmt.Sprintf("TABLE 2 per operation: config %s, mix %s, %s", group.Key.Config, group.Key.Mix, headingFacts(group.Key))
		}
		for _, name := range slices.Sorted(func(yield func(string) bool) {
			for name := range group.Typical.Ops {
				if !yield(name) {
					return
				}
			}
		}) {
			op := group.Typical.Ops[name]
			cells := latencyCells(op.Latency)
			rows = append(rows, []string{name, group.Key.Engine, placementNote(group), sig3(op.ReqPerOp), cells[0], cells[2], cells[3]})
		}
	}
	flush()
}

type scalingKey struct {
	Engine, Mix, Loop, Placement string
	Depth                        int
}

func writeScalingTables(tables *tableWriter, groups []*legGroup) {
	byKey := map[scalingKey][]*legGroup{}
	for _, group := range groups {
		key := scalingKey{group.Key.Engine, group.Key.Mix, group.Key.Loop, group.Key.Placement, group.Key.Depth}
		byKey[key] = append(byKey[key], group)
	}
	keys := slices.SortedFunc(func(yield func(scalingKey) bool) {
		for key := range byKey {
			if !yield(key) {
				return
			}
		}
	}, func(a, b scalingKey) int {
		return cmp.Or(cmp.Compare(a.Engine, b.Engine), cmp.Compare(a.Mix, b.Mix), cmp.Compare(a.Loop, b.Loop), cmp.Compare(a.Placement, b.Placement), cmp.Compare(a.Depth, b.Depth))
	})
	for _, key := range keys {
		members := byKey[key]
		slices.SortStableFunc(members, func(a, b *legGroup) int { return cmp.Compare(a.Typical.Nodes, b.Typical.Nodes) })
		if members[0].Typical.Nodes == members[len(members)-1].Typical.Nodes {
			continue
		}
		writeScalingTable(tables, key, members)
	}
}

func writeScalingTable(tables *tableWriter, key scalingKey, members []*legGroup) {
	// One owner by construction: adding nodes cannot add write throughput, so an
	// efficiency figure there would only mislead (spec 9.4 item 3).
	showEfficiency := key.Engine != "hoarddb" || key.Placement != "single" || key.Mix != "WH"
	var singleNode float64
	for _, group := range members {
		if slices.Contains(group.labels(), labelClientBound) {
			showEfficiency = false
		}
		if group.Typical.Nodes == 1 && singleNode == 0 {
			singleNode = group.Median
		}
	}
	columns := []string{"nodes", "config", "level", "routing", "ops/s", "copies/nodes"}
	if showEfficiency {
		columns = append(columns, "efficiency")
	}
	rows := make([][]string, 0, len(members))
	for _, group := range members {
		nodes := group.Typical.Nodes
		row := []string{strconv.Itoa(nodes), group.Key.Config, group.Key.Level, group.Key.Routing, sig3(group.Median), fmt.Sprintf("%d/%d", group.Typical.Copies, nodes)}
		if showEfficiency {
			efficiency := notAvailable
			if singleNode > 0 {
				efficiency = strconv.FormatFloat(group.Median/(float64(nodes)*singleNode), 'f', 3, 64)
			}
			row = append(row, efficiency)
		}
		rows = append(rows, row)
	}
	placement := key.Placement
	if key.Engine == "hoarddb" && key.Placement == "single" {
		placement += " (one owner)"
	}
	tables.table(fmt.Sprintf("TABLE 3 scaling: engine %s, mix %s, depth %d, loop %s, placement %s", key.Engine, key.Mix, key.Depth, key.Loop, placement), columns, rows)
}

func writeReplicationCost(tables *tableWriter, groups []*legGroup) {
	type costKey struct {
		Engine, Mix, Loop, Placement string
		Depth, Nodes                 int
	}
	byKey := map[costKey][]*legGroup{}
	for _, group := range groups {
		key := costKey{group.Key.Engine, group.Key.Mix, group.Key.Loop, group.Key.Placement, group.Key.Depth, group.Typical.Nodes}
		byKey[key] = append(byKey[key], group)
	}
	var rows [][]string
	for _, key := range slices.SortedFunc(func(yield func(costKey) bool) {
		for key := range byKey {
			if !yield(key) {
				return
			}
		}
	}, func(a, b costKey) int {
		return cmp.Or(cmp.Compare(a.Engine, b.Engine), cmp.Compare(a.Nodes, b.Nodes), cmp.Compare(a.Mix, b.Mix), cmp.Compare(a.Placement, b.Placement), cmp.Compare(a.Depth, b.Depth), cmp.Compare(a.Loop, b.Loop))
	}) {
		members := byKey[key]
		levels := map[string]bool{}
		for _, group := range members {
			levels[group.Key.Level] = true
		}
		if !levels["L-A"] || !levels["L-R"] {
			continue
		}
		for _, group := range members {
			if group.Key.Level != "L-A" && group.Key.Level != "L-R" {
				continue
			}
			write := group.Typical.Latency.Write
			rows = append(rows, []string{key.Engine, strconv.Itoa(key.Nodes), key.Mix, group.Key.Level, group.Key.Routing, sig3(group.Typical.WritesPerSec), percentileCell(write.Count, write.P99)})
		}
	}
	if len(rows) > 0 {
		tables.table("TABLE 4 replication cost: writes on L-A against L-R at the same node count", []string{"engine", "nodes", "mix", "level", "routing", "writes/s", "write p99"}, rows)
	}
}

func writeFsyncTable(tables *tableWriter, groups []*legGroup) {
	var rows [][]string
	for _, group := range groups {
		if fsyncs := group.Typical.Witness.FsyncsPerAck; fsyncs != 0 {
			rows = append(rows, []string{group.Key.Engine, group.Key.Level, group.Key.Config, sig3(fsyncs)})
		}
	}
	if len(rows) > 0 {
		tables.table("TABLE 6 fsyncs per acknowledged write (from /proc/diskstats)", []string{"engine", "level", "config", "fsyncs/ack"}, rows)
	}
}

// writeIntegrity lists the legs that failed an integrity check; the numbers in
// the tables above are not valid for those.
func writeIntegrity(tables *tableWriter, groups []*legGroup) {
	var rows [][]string
	for _, group := range groups {
		if failures := group.integrityFailures(); len(failures) > 0 {
			key := group.Key
			rows = append(rows, []string{fmt.Sprintf("%s %s %s %s depth %d %s %s", key.Config, key.Engine, key.Placement, key.Mix, key.Depth, key.Level, key.Loop), strings.Join(failures, "; ")})
		}
	}
	if len(rows) > 0 {
		tables.table("INTEGRITY FAILURES (the comparison is invalid for these legs)", []string{"leg", "failures"}, rows)
	}
}

// renderCSV prints one row per group: the median of the repeats with the range.
func renderCSV(records []*legRecord) (string, error) {
	var out strings.Builder
	writer := csv.NewWriter(&out)
	header := []string{"config", "engine", "placement", "mix", "loop", "depth", "level", "routing", "nodes", "median_basis", "repeats",
		"ops_per_s_median", "ops_per_s_min", "ops_per_s_max", "ops_per_s_all_repeats_median", "reads_per_s", "writes_per_s",
		"read_p50_ms", "read_p95_ms", "read_p99_ms", "read_p999_ms", "write_p50_ms", "write_p95_ms", "write_p99_ms", "write_p999_ms",
		"fsync_p99_during_leg_ms", "labels", "integrity_failures"}
	if err := writer.Write(header); err != nil {
		return "", fmt.Errorf("writing the CSV header: %w", err)
	}
	for _, group := range groupLegs(records) {
		key, typical := group.Key, group.Typical
		row := append(append([]string{key.Config, key.Engine, key.Placement, key.Mix, key.Loop, strconv.Itoa(key.Depth), key.Level, key.Routing,
			strconv.Itoa(typical.Nodes), group.Basis, strconv.Itoa(len(group.Records)),
			sig3(group.Median), sig3(group.Min), sig3(group.Max), sig3(group.AllMedian), sig3(typical.ReadsPerSec), sig3(typical.WritesPerSec)},
			append(latencyCells(typical.Latency.Read), latencyCells(typical.Latency.Write)...)...),
			sig3(typical.VM.FsyncP99DuringLeg), strings.Join(group.labels(), ";"), strings.Join(group.integrityFailures(), ";"))
		if err := writer.Write(row); err != nil {
			return "", fmt.Errorf("writing the CSV row for %s: %w", group.label(), err)
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return "", fmt.Errorf("flushing the CSV: %w", err)
	}
	return out.String(), nil
}

// runSummarize reads leg-record files and prints the caveat header and the
// reading tables, or the CSV.
func runSummarize(paths []string, asCSV bool, out io.Writer) error {
	var records []*legRecord
	for _, path := range paths {
		fileRecords, err := readLegRecords(path)
		if err != nil {
			return fmt.Errorf("summarizing: %w", err)
		}
		records = append(records, fileRecords...)
	}
	text := summaryHeader(groupLegs(records)) + renderSummary(records)
	if asCSV {
		var err error
		if text, err = renderCSV(records); err != nil {
			return err
		}
	}
	if _, err := io.WriteString(out, text); err != nil {
		return fmt.Errorf("writing the summary: %w", err)
	}
	return nil
}
