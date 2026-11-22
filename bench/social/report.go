package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"
)

// recordSchema is the version stamp of the leg record. The summariser refuses
// any other, because a table built from two shapes would compare unlike things.
const recordSchema = "perf-record/2"

// legRecord is the one JSON document a certification leg leaves behind
// (spec 9.2, documented field by field in REPORT.md). Two releases are
// compared by diffing these, so a field is never renamed and never dropped.
type legRecord struct {
	Schema         string            `json:"schema"`
	Release        string            `json:"release"`
	Commit         string            `json:"commit"`
	Date           string            `json:"date"`
	Engine         string            `json:"engine"`
	EngineVersion  string            `json:"engine_version"`
	EngineSettings map[string]string `json:"engine_settings"`

	Config       string `json:"config"`
	Nodes        int    `json:"nodes"`
	Level        string `json:"level"`
	Routing      string `json:"routing"`
	WriteConcern string `json:"write_concern"`
	Copies       int    `json:"copies"`
	// Fanout is single or bulk: how a competitor engine wrote the feed entries
	// of a publish. Records from before the flag have none, and were bulk.
	Fanout string `json:"fanout"`

	Placement              string   `json:"placement"`
	Shards                 int      `json:"shards"`
	OwnersPerNode          []int    `json:"owners_per_node"`
	HoldersOfPrimaryBucket []string `json:"holders_of_primary_bucket"`
	// OwnerNodeIDs names the nodes OwnersPerNode counts, in the same order
	// (the server's node ids, sorted); only a sharded hoardDB leg has them.
	OwnerNodeIDs []string `json:"owner_node_ids,omitempty"`
	// CopiesBelowFactor is the count of writes acknowledged with fewer copies
	// than the factor. Nothing measures it yet, so it is null: a 0 would read
	// as "none were".
	CopiesBelowFactor *int `json:"copies_below_factor"`

	Dataset     string   `json:"dataset"`
	Mix         string   `json:"mix"`
	Loop        string   `json:"loop"`
	Depth       int      `json:"depth"`
	OfferedRate *float64 `json:"offered_rate"`
	RateBasis   string   `json:"rate_basis"`
	Utilisation *float64 `json:"utilisation"`
	Warm        bool     `json:"warm"`
	WarmupS     float64  `json:"warmup_s"`
	MeasuredS   float64  `json:"measured_s"`
	Seed        uint64   `json:"seed"`
	// Repeat is the N of `social.sh run ... <N>`: which of the back-to-back
	// repeats of one leg this is, so the median of three can be read from the
	// records (BD-4). Optional in a file; 0 means a leg that was not repeated.
	Repeat int `json:"repeat"`

	OpsPerSec    float64             `json:"ops_per_s"`
	ReadsPerSec  float64             `json:"reads_per_s"`
	WritesPerSec float64             `json:"writes_per_s"`
	Ops          map[string]opRecord `json:"ops"`
	Latency      legLatency          `json:"latency_ms"`
	Histogram    string              `json:"histogram"`
	ErrorsByType map[string]int64    `json:"errors_by_type"`
	Verified     int64               `json:"verified_reads"`
	Misses       int64               `json:"misses"`
	Resources    legResources        `json:"resources"`
	VM           vmFacts             `json:"vm"`
	Canary       canaryFacts         `json:"canary"`
	Witness      witnessFacts        `json:"witness"`
	Labels       []string            `json:"labels"`
	Integrity    []string            `json:"integrity_failures"`
	Notes        []string            `json:"notes"`
}

// latencyStats is a latencySummary plus the mean, the shape of every latency_ms
// object. Count is an addition to spec 9.2: it lets the summariser apply the
// fewer-than-1,000-samples rule to a file it did not write.
type latencyStats struct {
	latencySummary
	Mean latencyMs `json:"mean"`
}

// opRecord is one operation's share of a leg.
type opRecord struct {
	Count    int64   `json:"count"`
	ReqPerOp float64 `json:"req_per_op"`
	// FanoutMean is W1 only: the mean followers a publish copied to. W1 sends
	// two requests plus one per follower, so req_per_op minus this is the part
	// a code change can move.
	FanoutMean *float64 `json:"fanout_mean,omitempty"`
	// ResumeRequestsMean is W1 only: the mean requests a publish spent resuming
	// the author's post numbering (one the first time the run publishes as that
	// author, none after), so it grows with the number of distinct authors.
	ResumeRequestsMean *float64     `json:"resume_requests_mean,omitempty"`
	Latency            latencyStats `json:"latency_ms"`
}

type legLatency struct {
	Read  latencyStats `json:"read"`
	Write latencyStats `json:"write"`
	All   latencyStats `json:"all"`
}

type legResources struct {
	Server         []serverResources `json:"server"`
	Clients        []clientResources `json:"clients"`
	ShardTop5Share *float64          `json:"shard_top5_op_share"`
}

type serverResources struct {
	VM              string  `json:"vm"`
	CPUPctP95       float64 `json:"cpu_pct_p95"`
	RSSMBMax        float64 `json:"rss_mb_max"`
	DiskWriteMBsP95 float64 `json:"disk_write_mb_s_p95"`
	StealPct        float64 `json:"steal_pct"`
}

type clientResources struct {
	VM         string  `json:"vm"`
	CPUPctP95  float64 `json:"cpu_pct_p95"`
	CPUUsPerOp float64 `json:"cpu_us_per_op"`
}

type vmFacts struct {
	FsyncMs            float64 `json:"fsync_ms"`
	StealPct           float64 `json:"steal_pct"`
	NProc              int     `json:"nproc"`
	MemMB              float64 `json:"mem_mb"`
	FsyncP50DuringLeg  float64 `json:"fsync_ms_p50_during_leg"`
	FsyncP99DuringLeg  float64 `json:"fsync_ms_p99_during_leg"`
	FsyncMaxDuringLeg  float64 `json:"fsync_ms_max_during_leg"`
	ConcurrentFsyncVMs int     `json:"concurrent_fsync_vms"`
}

type canaryFacts struct {
	Before   canaryReading `json:"before"`
	After    canaryReading `json:"after"`
	DriftPct float64       `json:"drift_pct"`
}

type canaryReading struct {
	FsyncMs   float64 `json:"fsync_ms"`
	CPULoopMs float64 `json:"cpu_loop_ms"`
}

type witnessFacts struct {
	// SingleSyncMsP50 is the witness reference: the cheapest data-only flush
	// (pwrite plus fdatasync to a preallocated block).
	SingleSyncMsP50 float64 `json:"single_sync_ms_p50"`
	// SyncOverwriteMsP50 and SingleSyncAppendMsP50 are optional: the O_DSYNC
	// overwrite and append figures, recorded beside the reference.
	SyncOverwriteMsP50    float64 `json:"sync_overwrite_ms_p50,omitempty"`
	SingleSyncAppendMsP50 float64 `json:"single_sync_append_ms_p50,omitempty"`
	Depth1WriteP50Ms      float64 `json:"depth1_write_p50_ms"`
	// P50OverSingleSync is Depth1WriteP50Ms over SingleSyncMsP50; the witness
	// passes at or above witnessMinRatio.
	P50OverSingleSync float64 `json:"p50_over_single_sync,omitempty"`
	FsyncsPerAck      float64 `json:"fsyncs_per_ack"`
}

// newLegRecord returns a record with its schema stamp set and every map and
// list empty rather than nil, so a record encodes `{}` and `[]`, never null,
// and a reader can tell "none" from "missing".
func newLegRecord() *legRecord {
	return &legRecord{
		Schema:                 recordSchema,
		EngineSettings:         map[string]string{},
		OwnersPerNode:          []int{},
		HoldersOfPrimaryBucket: []string{},
		Ops:                    map[string]opRecord{},
		ErrorsByType:           map[string]int64{},
		Resources:              legResources{Server: []serverResources{}, Clients: []clientResources{}},
		Labels:                 []string{},
		Integrity:              []string{},
		Notes:                  []string{},
	}
}

// newLatencyStats summarises a histogram in milliseconds; the percentile rule
// of histogram.summary (n.a. below 1,000 samples) applies.
func newLatencyStats(hist *histogram) latencyStats {
	stats := latencyStats{latencySummary: hist.summary(), Mean: latencyNotAvailable}
	if hist.count() > 0 {
		stats.Mean = latencyMs(hist.recorded.Mean() * float64(time.Microsecond) / float64(time.Millisecond))
	}
	return stats
}

// writeLegRecord writes record as one JSON line.
func writeLegRecord(out io.Writer, record *legRecord) error {
	encoded, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("encoding the %s leg record: %w", record.Engine, err)
	}
	if _, err := out.Write(append(encoded, '\n')); err != nil {
		return fmt.Errorf("writing the %s leg record: %w", record.Engine, err)
	}
	return nil
}

// readLegRecords reads every leg record in a file (one JSON document per leg,
// one line each or pretty-printed).
func readLegRecords(path string) ([]*legRecord, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading leg record file: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(content))
	var records []*legRecord
	for decoder.More() {
		record := newLegRecord()
		if err := decoder.Decode(record); err != nil {
			return nil, fmt.Errorf("reading %s: record %d is not valid JSON: %w", path, len(records)+1, err)
		}
		if record.Schema != recordSchema {
			return nil, fmt.Errorf("%s: record %d has schema %q; this summariser reads %q", path, len(records)+1, record.Schema, recordSchema)
		}
		records = append(records, record)
	}
	if len(records) == 0 {
		return nil, fmt.Errorf("%s holds no leg record", path)
	}
	return records, nil
}
