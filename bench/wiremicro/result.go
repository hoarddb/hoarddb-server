package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sort"
	"strings"
	"time"
)

// resultConfig is the part of the command line that decides what was measured.
// It holds no credential and no connection string.
type resultConfig struct {
	Ops          int    `json:"ops"`
	Workers      int    `json:"workers"`
	PayloadBytes int    `json:"payload_bytes"`
	Preload      int    `json:"preload"`
	Smoke        bool   `json:"smoke"`
	Target       string `json:"target"`
}

// phaseResult is one phase: its name, its connection count and its latencies.
type phaseResult struct {
	Name    string `json:"name"`
	Workers int    `json:"workers"`
	latencySummary
}

// runResult is the JSON file. InternalOnly is always true: competitor figures
// are never published.
type runResult struct {
	Tool          string            `json:"tool"`
	Engine        string            `json:"engine"`
	StartedAt     time.Time         `json:"started_at"`
	Config        resultConfig      `json:"config"`
	Durability    map[string]string `json:"durability"`
	DatadirFSNote string            `json:"datadir_fs_note"`
	Phases        []phaseResult     `json:"phases"`
	InternalOnly  bool              `json:"internal_only"`
}

// writeJSON writes the result, indented, with internal_only forced on.
func (r runResult) writeJSON(w io.Writer) error {
	r.InternalOnly = true
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(r); err != nil {
		return fmt.Errorf("encoding the result: %w", err)
	}
	return nil
}

// writeTable prints the human table: the durability settings the run observed,
// then one row per phase.
func (r runResult) writeTable(w io.Writer) error {
	var w2 strings.Builder
	fmt.Fprintf(&w2, "wiremicro %s  target %s  INTERNAL: do not publish\n", r.Engine, r.Config.Target)
	fmt.Fprintf(&w2, "ops/phase %d  workers %d  payload %dB  preload %d\n", r.Config.Ops, r.Config.Workers, r.Config.PayloadBytes, r.Config.Preload)
	keys := make([]string, 0, len(r.Durability))
	for key := range r.Durability {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	pairs := make([]string, 0, len(keys))
	for _, key := range keys {
		pairs = append(pairs, key+"="+r.Durability[key])
	}
	fmt.Fprintf(&w2, "durability observed: %s\n", strings.Join(pairs, " "))
	if r.DatadirFSNote != "" {
		fmt.Fprintf(&w2, "data dir: %s\n", r.DatadirFSNote)
	}
	fmt.Fprintf(&w2, "\n%-16s %7s %9s %11s %10s %10s %10s %10s\n", "phase", "workers", "ops", "ops/s", "p50 us", "p99 us", "p99.9 us", "max us")
	for _, phase := range r.Phases {
		fmt.Fprintf(&w2, "%-16s %7d %9d %11.0f %10.0f %10.0f %10.0f %10.0f\n",
			phase.Name, phase.Workers, phase.Ops, phase.OpsPerSec, phase.P50Us, phase.P99Us, phase.P999Us, phase.MaxUs)
	}
	if _, err := io.WriteString(w, w2.String()); err != nil {
		return fmt.Errorf("writing the table: %w", err)
	}
	return nil
}

// recordedTarget is what a result file and table say about the server: the role and the port. The host is
// never written, because an address in a result goes stale and breaks the no-fixed-addresses rule.
func recordedTarget(addr string) string {
	_, port, err := net.SplitHostPort(addr)
	if err != nil || port == "" {
		return "server"
	}
	return "server:" + port
}
