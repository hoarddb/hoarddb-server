package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/hoarddb/hoarddb-server/transport"
)

// capacityFamilies is every series family the capacity feature publishes.
var capacityFamilies = []string{
	familyRatio, familyThreshold, familySampleErrors, familyLastSample, familyBuckets,
	familyMappings, familyMappingsLimit, familyAnonBytes, familyMemLimitBytes,
	familyDiskUsed, familyDiskSize, familyDataBytes,
}

// scrapeMetrics fetches /metrics from the mux the metrics server serves.
func scrapeMetrics(t *testing.T, s *Server) string {
	t.Helper()
	rec := httptest.NewRecorder()
	newMetricsMux(s).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/metrics answered %d: %s", rec.Code, rec.Body.String())
	}
	return rec.Body.String()
}

// served reports whether a scrape carries a family.
func served(body, family string) bool {
	return strings.Contains(body, "\n# TYPE "+family+" ") || strings.HasPrefix(body, "# TYPE "+family+" ")
}

// countingProbes wraps every member of a probe set so a test can count calls.
func countingProbes(calls *atomic.Int64) func(p *capacityProbes) {
	return func(p *capacityProbes) {
		greenProbes(p)
		diskStatfs, openFDs, mappings := p.diskStatfs, p.openFDs, p.mappings
		anonMemory, memLimit, storeSize, walBytes := p.anonMemory, p.memoryLimit, p.storeSize, p.walBytes
		p.diskStatfs = func(dir string) (diskStat, error) { calls.Add(1); return diskStatfs(dir) }
		p.openFDs = func() (int64, int64, error) { calls.Add(1); return openFDs() }
		p.mappings = func() (int64, int64, error) { calls.Add(1); return mappings() }
		p.anonMemory = func() (int64, error) { calls.Add(1); return anonMemory() }
		p.memoryLimit = func() (memoryLimit, error) { calls.Add(1); return memLimit() }
		p.storeSize = func() (int64, int64, error) { calls.Add(1); return storeSize() }
		p.walBytes = func() (int64, bool, error) { calls.Add(1); return walBytes() }
	}
}

// TestScrapeDoesNoCapacityIO (spec T-10) proves a scrape reads series that
// are already set: 100 scrapes call no probe, so the scrape rate can never
// drive a /proc/self/maps read.
func TestScrapeDoesNoCapacityIO(t *testing.T) {
	s := newTestServer(t)
	waitFirstSample(t, s.capacity)
	var calls atomic.Int64
	setCapacityProbes(t, s, countingProbes(&calls))
	s.capacity.sampleNow()
	if calls.Load() == 0 {
		t.Fatal("precondition: a sample through the counting probes called none of them")
	}
	calls.Store(0)
	for range 100 {
		if body := scrapeMetrics(t, s); !served(body, familyRatio) {
			t.Fatalf("/metrics does not serve %s, so this test would prove nothing", familyRatio)
		}
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("100 scrapes made %d probe calls, want 0", got)
	}
}

// TestMetricsEndpointKeepsDefaultSeries (build doc BT-13) pins what /metrics
// served before the capacity registry joined it: the process and Go
// collectors, promhttp's own handler series, and the default registry's
// hoardDB series. It passes on the parent by design; its proof is its breaks.
func TestMetricsEndpointKeepsDefaultSeries(t *testing.T) {
	// hoarddb_requests_total is a vector, served once it has a child: one
	// instrumented request gives it one.
	wrapped := InstrumentHandler(func(context.Context, *transport.Frame) (*transport.Frame, error) {
		return &transport.Frame{Opcode: transport.OpPong}, nil
	})
	if _, err := wrapped(context.Background(), &transport.Frame{Opcode: transport.OpPing}); err != nil {
		t.Fatalf("instrumented request: %v", err)
	}
	s := newTestServer(t)
	waitFirstSample(t, s.capacity)
	body := scrapeMetrics(t, s)
	for _, family := range []string{"process_open_fds", "go_goroutines", "promhttp_metric_handler_requests_total", "hoarddb_requests_total"} {
		if !served(body, family) {
			t.Fatalf("/metrics no longer serves %s", family)
		}
	}
	// The nil servers the probe tests build still get the default registry.
	for _, server := range []*Server{nil, {auth: &AuthManager{}}} {
		if body := scrapeMetrics(t, server); !served(body, "go_goroutines") {
			t.Fatalf("/metrics for server %v does not serve the default registry", server)
		}
	}
}
