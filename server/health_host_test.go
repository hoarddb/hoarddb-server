package server

import (
	"runtime"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// TestHealthCarriesHostFacts: the health reply names the node's usable CPUs,
// and on Linux its memory and load, in one nested document.
func TestHealthCarriesHostFacts(t *testing.T) {
	s := newTestServer(t)
	health, err := s.HealthCheck()
	if err != nil {
		t.Fatalf("HealthCheck: %v", err)
	}
	if health.Host == nil {
		t.Fatal("HealthCheck.Host is nil: the reply carries no host facts")
	}
	if health.Host.CPUs < 1 || health.Host.CPUs > runtime.NumCPU() {
		t.Errorf("Host.CPUs = %d, want between 1 and runtime.NumCPU() = %d", health.Host.CPUs, runtime.NumCPU())
	}
	if runtime.GOOS == "linux" {
		if health.Host.MemoryTotalBytes <= 0 || health.Host.MemoryUsedBytes <= 0 || health.Host.MemoryUsedBytes > health.Host.MemoryTotalBytes {
			t.Errorf("Host memory = used %d of %d, want 0 < used <= total on Linux", health.Host.MemoryUsedBytes, health.Host.MemoryTotalBytes)
		}
		if health.Host.MemorySource != MemorySourceHost && health.Host.MemorySource != MemorySourceCgroup {
			t.Errorf("Host.MemorySource = %q, want %q or %q", health.Host.MemorySource, MemorySourceHost, MemorySourceCgroup)
		}
		if health.Host.Load == nil {
			t.Error("Host.Load is nil on Linux")
		}
	}
}

// TestHealthHostWireNames pins the names the host facts travel under: an
// untagged field would go on the wire lower-cased without underscores.
func TestHealthHostWireNames(t *testing.T) {
	encoded, err := bson.Marshal(HealthResponse{Host: &HealthHost{
		CPUs: 2, HostCPUs: 8, MemoryTotalBytes: 3, MemoryUsedBytes: 2, MemorySource: MemorySourceCgroup,
		Load: &HealthLoad{OneMinute: 1, FiveMinutes: 2, FifteenMinutes: 3},
	}})
	if err != nil {
		t.Fatalf("encoding: %v", err)
	}
	host, ok := bson.Raw(encoded).Lookup("host").DocumentOK()
	if !ok {
		t.Fatalf("no host document in %s", bson.Raw(encoded))
	}
	for _, name := range []string{"cpus", "host_cpus", "memory_total_bytes", "memory_used_bytes", "memory_source", "load"} {
		if _, err := host.LookupErr(name); err != nil {
			t.Errorf("host has no %q member: %s", name, host)
		}
	}
	load, _ := host.Lookup("load").DocumentOK()
	for _, name := range []string{"one_minute", "five_minutes", "fifteen_minutes"} {
		if _, err := load.LookupErr(name); err != nil {
			t.Errorf("load has no %q member: %s", name, load)
		}
	}
}

// TestHealthWithoutHostFactsIsUnchanged: an older reply and a node with no
// host facts encode with no host member, so no driver sees a new key it did
// not ask for.
func TestHealthWithoutHostFactsIsUnchanged(t *testing.T) {
	encoded, err := bson.Marshal(HealthResponse{NodeID: "n1", Healthy: true})
	if err != nil {
		t.Fatalf("encoding: %v", err)
	}
	if _, err := bson.Raw(encoded).LookupErr("host"); err == nil {
		t.Error("an empty HealthResponse encodes a host member")
	}
}

// TestUnauthenticatedProbesCarryNoHostFacts: CPU count and RAM are shown to an
// authenticated caller of HEALTH_CHECK only. The HTTP probes answer without
// authentication and must not carry them (owner decision, 2026-10-11).
func TestUnauthenticatedProbesCarryNoHostFacts(t *testing.T) {
	s := newTestServer(t)
	for _, path := range []string{"/health", "/ready", "/live"} {
		body := strings.ToLower(probe(t, s, path).Body.String())
		for _, word := range []string{"cpu", "memory", "load", "host"} {
			if strings.Contains(body, word) {
				t.Errorf("%s answers %q, which mentions %q: host facts are for authenticated callers", path, body, word)
			}
		}
	}
}
