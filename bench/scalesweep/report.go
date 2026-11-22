package main

import (
	"encoding/json"
	"fmt"
	"time"
)

// latencyRecord is a kind's latency in milliseconds; fields are null under
// minSamples so a percentile never rests on a handful of operations.
type latencyRecord struct {
	Count int64    `json:"count"`
	P50   *float64 `json:"p50"`
	P99   *float64 `json:"p99"`
}

const minSamples = 1000

func latencyOf(counters *legCounters, kinds ...opKind) latencyRecord {
	merged := newLegCounters().hists[opPointRead]
	for _, kind := range kinds {
		merged.Merge(counters.hists[kind])
	}
	record := latencyRecord{Count: merged.TotalCount()}
	if record.Count < minSamples {
		return record
	}
	p50 := float64(merged.ValueAtQuantile(50)) / 1000
	p99 := float64(merged.ValueAtQuantile(99)) / 1000
	record.P50, record.P99 = &p50, &p99
	return record
}

// legRecord is the line appended to -out for one measured leg.
type legRecord struct {
	Schema        string                   `json:"schema"`
	Engine        string                   `json:"engine"`
	Durability    string                   `json:"durability"`
	Layout        string                   `json:"layout"`
	OwnerFanout   int64                    `json:"owner_fanout"`
	Records       int64                    `json:"records_at_start"`
	Inserted      int64                    `json:"inserted_during_leg"`
	Workers       int                      `json:"workers"`
	BodyBytes     int                      `json:"body_bytes"`
	WarmupS       float64                  `json:"warmup_s"`
	MeasuredS     float64                  `json:"measured_s"`
	Ops           int64                    `json:"ops"`
	OpsPerS       float64                  `json:"ops_per_s"`
	Read          latencyRecord            `json:"read_ms"`
	Write         latencyRecord            `json:"write_ms"`
	All           latencyRecord            `json:"all_ms"`
	ByKind        map[string]latencyRecord `json:"by_kind_ms"`
	Errors        map[string]int64         `json:"errors"`
	Mix           string                   `json:"mix"`
	StartedAtUnix int64                    `json:"started_at_unix"`
}

// buildRecord assembles the leg record from a finished leg.
func buildRecord(db store, cfg legConfig, counters *legCounters, inserted int64, measured time.Duration) legRecord {
	total := int64(0)
	byKind := map[string]latencyRecord{}
	for kind := opKind(0); kind < opKinds; kind++ {
		total += counters.hists[kind].TotalCount()
		byKind[opNames[kind]] = latencyOf(counters, kind)
	}
	seconds := measured.Seconds()
	return legRecord{
		Schema:      "scale-sweep-leg/1",
		Engine:      db.name(),
		Durability:  db.durability(),
		Layout:      layoutOf(db),
		OwnerFanout: max(cfg.Fanout, 1),
		Records:     cfg.Records,
		Inserted:    inserted,
		Workers:     cfg.Workers,
		BodyBytes:   cfg.BodyBytes,
		WarmupS:     cfg.Warmup.Seconds(),
		MeasuredS:   seconds,
		Ops:         total,
		OpsPerS:     float64(total) / max(seconds, 1e-9),
		Read:        latencyOf(counters, opPointRead, opOwnerRead),
		Write:       latencyOf(counters, opUpdate, opInsert),
		All:         latencyOf(counters, opPointRead, opOwnerRead, opUpdate, opInsert),
		ByKind:      byKind,
		Errors:      counters.errors,
		Mix: fmt.Sprintf("point_read %d, owner_read %d, update %d, insert %d per mille",
			mixPointRead, mixOwnerRead, mixUpdate, mixTotal-mixPointRead-mixOwnerRead-mixUpdate),
		StartedAtUnix: time.Now().Add(-measured).Unix(),
	}
}

func (record legRecord) line() ([]byte, error) {
	encoded, err := json.Marshal(record)
	if err != nil {
		return nil, fmt.Errorf("encoding the leg record: %w", err)
	}
	return encoded, nil
}

// layoutOf is the layout a store reports, h1 (one table or collection) for the
// competitors that have no other.
func layoutOf(db store) string {
	if laid, ok := db.(interface{ layout() string }); ok {
		return laid.layout()
	}
	return "h1"
}
