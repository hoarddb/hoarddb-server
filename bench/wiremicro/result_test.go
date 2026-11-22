package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func sampleResult() runResult {
	return runResult{
		Tool: "wiremicro", Engine: "mysql", StartedAt: time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC),
		Config:     resultConfig{Ops: 20000, Workers: 16, PayloadBytes: 256, Preload: 1000},
		Durability: map[string]string{"sync_binlog": "1"}, DatadirFSNote: "ext4 on nvme",
		Phases: []phaseResult{{Name: "insert_serial", Workers: 1, latencySummary: latencySummary{Ops: 20000, OpsPerSec: 1234.5, P50Us: 800, P99Us: 2000, P999Us: 5000, MaxUs: 9000}}},
	}
}

func TestResultJSONHasTheDocumentedKeys(t *testing.T) {
	var buf bytes.Buffer
	if err := sampleResult().writeJSON(&buf); err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(buf.Bytes(), &decoded); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"tool", "engine", "started_at", "config", "durability", "datadir_fs_note", "phases", "internal_only"} {
		if _, ok := decoded[key]; !ok {
			t.Errorf("missing key %q in %s", key, buf.String())
		}
	}
	phase := decoded["phases"].([]any)[0].(map[string]any)
	for _, key := range []string{"name", "workers", "ops", "ops_per_sec", "p50_us", "p99_us", "p999_us", "max_us"} {
		if _, ok := phase[key]; !ok {
			t.Errorf("phase is missing %q in %s", key, buf.String())
		}
	}
	if decoded["internal_only"] != true {
		t.Fatalf("internal_only = %v, want true", decoded["internal_only"])
	}
}

func TestResultJSONNeverCarriesCredentials(t *testing.T) {
	var buf bytes.Buffer
	result := sampleResult()
	result.Config.Target = "host:3306"
	if err := result.writeJSON(&buf); err != nil {
		t.Fatal(err)
	}
	for _, banned := range []string{"password", "dsn", "secret"} {
		if strings.Contains(strings.ToLower(buf.String()), banned) {
			t.Errorf("result mentions %q: %s", banned, buf.String())
		}
	}
}

func TestTableListsEveryPhaseAndTheDurabilityLines(t *testing.T) {
	var buf bytes.Buffer
	if err := sampleResult().writeTable(&buf); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	for _, want := range []string{"insert_serial", "1234", "p99.9", "sync_binlog=1", "ext4 on nvme", "INTERNAL"} {
		if !strings.Contains(out, want) {
			t.Errorf("table lacks %q:\n%s", want, out)
		}
	}
}

func TestRecordedTargetCarriesNoHostOrAddress(t *testing.T) {
	for addr, want := range map[string]string{
		"192.0.2.7:7433": "server:7433", "db-host.example:3306": "server:3306", "[2001:db8::1]:27017": "server:27017", "nohost": "server",
	} {
		if got := recordedTarget(addr); got != want {
			t.Errorf("recordedTarget(%q) = %q, want %q", addr, got, want)
		}
	}
}
