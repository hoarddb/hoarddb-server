package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// recordSchema names the layout of the JSON record.
const recordSchema = "bigasstest/1"

// driftPct is the growth above which a repetition is labelled drift: the data set grew so
// much during the run that the cost of a read is no longer the cost at the start.
const driftPct = 10.0

// requiredFacts are the host-side items of comparison-plan section 3.4 that the wrapper
// collects into facts.json. A value of "n/a" says an item does not apply; a missing key
// refuses the record. The engine read-back items come from Engine.Settings instead.
var requiredFacts = []string{
	"level_settings", "git_next", "git_bench", "filesystem", "mount_options", "syncprobe_before", "syncprobe_after",
	"canary_drift", "kernel", "nproc", "memory", "steal", "cgroup_limit", "date", "ran_by",
}

// requiredSettings are the keys every adapter's Settings must return.
var requiredSettings = []string{"engine_version", "tls"}

// Summary is throughput and latency of a group of requests.
type Summary struct {
	Requests   int64   `json:"requests"`
	Throughput float64 `json:"throughput_per_s"`
	latencySummary
}

// thinOpSamples is the fewest measured samples an operation needs to carry weight: under it the
// operation is marked thin in the record and the summary.
const thinOpSamples = 1000

// OpStats is one operation's counts, latency and cost.
type OpStats struct {
	Count   int64 `json:"count"`
	Errors  int64 `json:"errors"`
	Retries int64 `json:"retries"`
	// OffsetReads counts the reads of this operation that started inside their store: the
	// realised depth share is OffsetReads over Count.
	OffsetReads int64 `json:"offset_reads,omitempty"`
	// Thin marks an operation with under thinOpSamples measured samples: its percentiles and
	// shares rest on too few requests to compare.
	Thin            bool    `json:"thin,omitempty"`
	RoundTripsPerOp float64 `json:"round_trips_per_op"`
	CommitsPerOp    float64 `json:"commits_per_op"`
	// LatencySumMs is the summed latency of the measured requests of this operation, in
	// milliseconds, so a time share is a sum over sums and not an estimate from percentiles.
	LatencySumMs float64 `json:"latency_sum_ms"`
	// DocumentsPerOp and ElementsPerOp are the documents and elements a successful request of
	// this operation returned, on average (a room page's members, a feed's posts).
	DocumentsPerOp float64 `json:"documents_per_op"`
	ElementsPerOp  float64 `json:"elements_per_op"`
	latencySummary
}

// Record is the result of one repetition at one virtual-user level: one JSON file.
type Record struct {
	Schema   string            `json:"schema"`
	Engine   string            `json:"engine"`
	Level    string            `json:"level"`
	Settings map[string]string `json:"settings"`
	// SettingsAfter is Engine.Settings read a second time after the measured window, so
	// end-of-run counters (deadlocks, retries, cache state) reach the record.
	SettingsAfter map[string]string `json:"settings_after,omitempty"`
	Facts         map[string]any    `json:"facts"`
	VUsers        int               `json:"vusers"`
	Repetition    int               `json:"repetition"`
	Loop          string            `json:"loop"`
	Rate          float64           `json:"rate_per_s,omitempty"`
	Op            string            `json:"op,omitempty"`
	// Profile names the read-share profile of the mix ("R" or "S") and ReadShares are its
	// per-layout shares of requests that read, so a result states the mix it was run with.
	Profile    string             `json:"profile"`
	ReadShares map[string]float64 `json:"read_shares"`
	// Access is the entity selection ("uniform" or "zipf"), ZipfS its exponent (0 for uniform)
	// and PageDepth the share of paged reads that started at a random offset.
	Access    string  `json:"access"`
	ZipfS     float64 `json:"zipf_s"`
	PageDepth float64 `json:"page_depth"`
	// Batch says whether the hoardDB adapter sent a write's several elements as one batch frame
	// ("on" or "off"); empty for an engine that has no such switch.
	Batch string `json:"batch,omitempty"`
	// MultiGet says whether the hoardDB adapter read a page's documents with get-many frames
	// ("on" or "off"); empty for an engine that has no such switch.
	MultiGet string `json:"multiget,omitempty"`
	// Native says whether the hoardDB adapter read the native layout variant, whose User.Feed and
	// Room.Members elements carry copies ("on" or "off"); empty for an engine that has no such
	// switch. NativeFingerprint is then the model's hash of the embedded fields per layout that
	// has any, so the run can be shown to agree with the seed.
	Native            string            `json:"native,omitempty"`
	NativeFingerprint map[string]string `json:"native_fingerprint,omitempty"`
	// Aggregates says whether the ledger declared its sum and count aggregates and the audit read
	// the maintained sums back ("on" or "off"); empty for an engine that has no such switch.
	Aggregates string `json:"aggregates,omitempty"`
	// LedgerExpenses is the expenses per ledger account when the ledger was seeded deeper than the
	// default of 100 (scenarios +deep<N>); 0 for the default ledger.
	LedgerExpenses int `json:"ledger_expenses,omitempty"`
	// MaintainedSumsAudited is how many maintained sums (one per ledger store written to) the audit
	// read back and compared with the elements; 0 for a run without aggregates.
	MaintainedSumsAudited int `json:"maintained_sums_audited,omitempty"`
	// Capabilities is the hoardDB abilities this leg exercised (capabilities.go); an engine with
	// none writes an empty list, and a record without the list is refused.
	Capabilities []string `json:"capabilities"`
	// Profiled marks a repetition run with profile samplers attached. It is never part of a
	// measured median: the samplers cost throughput.
	Profiled bool   `json:"profiled,omitempty"`
	Scale    int    `json:"scale"`
	Seed     uint64 `json:"seed"`
	RandSeed uint64 `json:"rand_seed"`
	// Fingerprint is the model's content hash per layout, the proof every engine was seeded alike.
	Fingerprint map[string]string `json:"model_fingerprint"`

	Warm        bool      `json:"warm"`
	WarmWindows []float64 `json:"warm_window_req_per_s"`
	WarmSeconds float64   `json:"warm_seconds"`
	Seconds     float64   `json:"seconds"`

	Overall Summary            `json:"overall"`
	Layouts map[string]Summary `json:"layouts"`
	Ops     map[string]OpStats `json:"ops"`
	// ReadElements is the page size of every capped read (elements per read, by kind).
	ReadElements map[string]int `json:"elements_per_read"`
	// ReadPages is, per paged read, the spec limit, the limit in use, how many owners were
	// eligible (seeded with at least that many elements) and whether the limit fell back.
	ReadPages map[string]PageInfo `json:"read_pages"`
	Retries   int64               `json:"retries"`
	// LatencyClamped counts latencies above the 60 s histogram ceiling that were stored as the
	// ceiling, so the tail percentiles are understated by that many samples.
	LatencyClamped int64 `json:"latency_clamped,omitempty"`
	Reads          int64 `json:"reads"`
	Writes         int64 `json:"writes"`

	// Hits, Misses, VerifyFailures and Errors count every request of the repetition,
	// warm-up included. A miss is a read that returned fewer elements than the data requires
	// and is also a verify failure.
	Hits           int64    `json:"hits"`
	Misses         int64    `json:"misses"`
	VerifyFailures int64    `json:"verify_failures"`
	Errors         int64    `json:"errors"`
	ErrorSamples   []string `json:"error_samples,omitempty"`

	// CloseErrors are the errors closing the virtual users' sessions after the run.
	CloseErrors []string `json:"close_errors,omitempty"`
	// RunFlags are the settings of this run that no other field carries (warm-up flags, the
	// disk guard, the request timeout, the adapter parameters with secrets redacted).
	RunFlags map[string]string `json:"run_flags,omitempty"`

	ClientCPUPct float64 `json:"client_cpu_pct"`
	// ServerCPUSeconds is the CPU time the engine's server process used during the measured
	// window and ServerCPUCores that over the window's length (cores kept busy); both are 0
	// when no -server-cpu-cmd was given.
	ServerCPUSeconds float64  `json:"server_cpu_seconds,omitempty"`
	ServerCPUCores   float64  `json:"server_cpu_cores,omitempty"`
	GrowthPct        float64  `json:"growth_pct"`
	Labels           []string `json:"labels,omitempty"`
	AuditOK          bool     `json:"audit_ok"`
	AuditError       string   `json:"audit_error,omitempty"`
	Aborted          string   `json:"aborted,omitempty"`
	Failed           bool     `json:"failed"`
	Started          string   `json:"started"`
}

// labelIdiomatic marks the one hoardDB leg a developer following the how-to would write: references
// with get-many reads and one batch frame per write, on the common layout. It equals batch=on and
// multiget=on with native and aggregates off, so the leg name stays -batched-multiget; the label only names it.
const labelIdiomatic = "idiomatic"

// idiomatic reports whether the record is the idiomatic hoardDB baseline leg.
func (r *Record) idiomatic() bool {
	return r.Batch == "on" && r.MultiGet == "on" && r.Native != "on" && r.Aggregates != "on"
}

// label sets the labels the figures earn: drift above 10% growth, client-bound above 85%
// client CPU, warm=false when the warm-up did not settle.
func (r *Record) label() {
	r.Labels = nil
	if r.GrowthPct > driftPct {
		r.Labels = append(r.Labels, "drift")
	}
	if clientBoundLabel(r.ClientCPUPct / 100) {
		r.Labels = append(r.Labels, "client-bound")
	}
	for _, stats := range r.Ops {
		if stats.Thin {
			r.Labels = append(r.Labels, "thin-ops")
			break
		}
	}
	if r.LatencyClamped > 0 {
		r.Labels = append(r.Labels, "latency-clamped")
	}
	if r.Retries > 0 {
		r.Labels = append(r.Labels, "retries")
	}
	if !r.Warm {
		r.Labels = append(r.Labels, "warm=false")
	}
	if r.idiomatic() {
		r.Labels = append(r.Labels, labelIdiomatic)
	}
}

// validate refuses a record that lacks a fact the comparison needs.
func (r *Record) validate() error {
	var missing []string
	for _, key := range requiredSettings {
		if value := r.Settings[key]; value == "" || value == "unknown" {
			missing = append(missing, "settings."+key)
		}
	}
	for _, key := range requiredFacts {
		if value, ok := r.Facts[key]; !ok || value == nil || value == "" {
			missing = append(missing, "facts."+key)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return fmt.Errorf("the record lacks %s. The wrapper scripts/certification/bigasstest.sh collects the host facts into -facts, and the engine adapter's Settings supplies the others; write \"n/a\" for an item that does not apply", strings.Join(missing, ", "))
	}
	if r.VUsers < 1 {
		return errors.New("the record has no virtual-user count")
	}
	if r.Capabilities == nil {
		return errors.New("the record has no capabilities list: it must say which hoardDB abilities its leg exercised (an empty list for an engine with none). The runner fills it from the engine's settings")
	}
	for _, name := range r.Capabilities {
		if _, known := capabilityDescriptions[name]; !known {
			return fmt.Errorf("the record declares the capability %q, which is not one of the vocabulary of capabilities.go", name)
		}
	}
	if r.Native == "on" && len(r.NativeFingerprint) == 0 {
		return errors.New("the record says native: on but has no native_fingerprint: a native result must carry the model's hash of the embedded copies, so it can be shown to match the seed")
	}
	if r.Profile == "" {
		return errors.New("the record has no profile name: it must say which mix it measured (R or S)")
	}
	if r.Access != accessUniform && r.Access != accessZipf {
		return fmt.Errorf("the record's access mode is %q: it must say whether entities were picked uniform or zipf", r.Access)
	}
	if len(r.ReadShares) == 0 {
		return errors.New("the record has no read_shares: it must carry the per-layout read shares of its profile")
	}
	return nil
}

// legName names what a record measured besides the engine and level: the operation ("mix" for
// the whole mix), the loop with its offered rate, and the scale. Two records with different
// legs never share a file name or a summary group.
func (r *Record) legName() string {
	op := r.Op
	if op == "" {
		op = "mix"
	}
	loop := r.Loop
	if r.Loop == "open" {
		loop = fmt.Sprintf("open%grps", r.Rate)
	}
	access := r.Access
	if r.Access == accessZipf {
		access = fmt.Sprintf("zipf%g", r.ZipfS)
	}
	leg := fmt.Sprintf("%s-p%s-%s-pd%d-%s-s%d", op, r.Profile, access, int(math.Round(100*r.PageDepth)), loop, r.Scale)
	if r.Batch == "on" {
		leg += "-batched"
	}
	if r.MultiGet == "on" {
		leg += "-multiget"
	}
	if r.Native == "on" {
		leg += "-native"
	}
	if r.Aggregates == "on" {
		leg += "-agg"
	}
	if r.LedgerExpenses > 0 {
		leg += fmt.Sprintf("-deep%d", r.LedgerExpenses)
	}
	if r.Profiled {
		leg += "-profiled"
	}
	return leg
}

// recordPath names a record file by engine, level, leg, virtual users and repetition.
func recordPath(dir string, r *Record) string {
	return filepath.Join(dir, fmt.Sprintf("%s-%s-%s-v%d-r%d.json", r.Engine, r.Level, r.legName(), r.VUsers, r.Repetition))
}

// writeRecord validates and writes one record, returning its path.
func writeRecord(dir string, r *Record) (string, error) {
	if err := r.validate(); err != nil {
		return "", err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("making the output directory: %w", err)
	}
	encoded, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return "", fmt.Errorf("encoding the record: %w", err)
	}
	path := recordPath(dir, r)
	if err := os.WriteFile(path, append(encoded, '\n'), 0o644); err != nil {
		return "", fmt.Errorf("writing %s: %w", path, err)
	}
	return path, nil
}

// loadFacts reads the host facts the wrapper wrote.
func loadFacts(path string) (map[string]any, error) {
	if path == "" {
		return nil, errors.New("-facts is empty. Pass the facts.json the wrapper collected (scripts/certification/bigasstest.sh writes it)")
	}
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading -facts: %w", err)
	}
	var facts map[string]any
	if err := json.Unmarshal(content, &facts); err != nil {
		return nil, fmt.Errorf("-facts %s is not a JSON object: %w", path, err)
	}
	return facts, nil
}

var addressPatterns = []*regexp.Regexp{
	regexp.MustCompile(`\[[0-9a-fA-F:.]+\](?::\d+)?`),
	regexp.MustCompile(`\b\d{1,3}(?:\.\d{1,3}){3}(?::\d+)?\b`),
	regexp.MustCompile(`\b[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)+:\d+\b`),
	regexp.MustCompile(`\b(?:[0-9a-fA-F]{1,4}:){2,7}[0-9a-fA-F]{1,4}\b`),
}

// redactAddresses replaces IP addresses and dotted host:port pairs with <addr>, so a driver's
// error message in a record names no machine (no fixed addresses in results).
func redactAddresses(message string) string {
	for _, pattern := range addressPatterns {
		message = pattern.ReplaceAllString(message, "<addr>")
	}
	return message
}
