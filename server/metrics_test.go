// Package server — metrics tests.
package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/hoarddb/hoarddb-server/transport"
)

// TestBucketOpsTotal asserts that bucketOpsTotal increments after real traffic.
// Break test: comment out bucketOpsTotal.WithLabelValues(op).Inc() in InstrumentHandler.
func TestBucketOpsTotal(t *testing.T) {
	handler := func(ctx context.Context, frame *transport.Frame) (*transport.Frame, error) {
		return &transport.Frame{Opcode: transport.OpPut}, nil
	}

	wrapped := InstrumentHandler(handler)
	ctx := context.Background()

	// Read metric value before
	before := readMetric("hoarddb_bucket_operations_total")

	// Generate traffic
	for i := 0; i < 5; i++ {
		if _, err := wrapped(ctx, &transport.Frame{Opcode: transport.OpPut}); err != nil {
			t.Fatalf("wrapped(ctx, frame): %v", err)
		}
	}

	// Read metric value after
	after := readMetric("hoarddb_bucket_operations_total")

	// Assert counter increased
	if after <= before {
		t.Errorf("bucketOpsTotal did not increment: before=%v, after=%v", before, after)
	}
	if after-before < 5 {
		t.Errorf("bucketOpsTotal should increment by 5, got %v", after-before)
	}
}

// TestHealthEndpointReturnsOK verifies /health answers 200 with a JSON verdict
// for a healthy node. TestHealthProbeFollowsHealthCheck covers the 503.
// Break test: in metrics.go's /health handler, answer 503 for a healthy node.
func TestHealthEndpointReturnsOK(t *testing.T) {
	mux := newMetricsMux(newTestServer(t))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/health", nil)
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("/health: expected 200, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), `"healthy":true`) {
		t.Errorf("/health: expected healthy true in body, got %s", rec.Body.String())
	}
}

// TestReadyEndpointReturnsOKWhenReady verifies /ready returns 200 when the server is initialized and healthy.
// Break test: in metrics.go's /ready handler, change the final success response
// (after the nil checks and the health verdict pass) away from 200/"ready".
func TestReadyEndpointReturnsOKWhenReady(t *testing.T) {
	mux := newMetricsMux(newTestServer(t))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/ready", nil)
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("/ready (ready): expected 200, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "ready") {
		t.Errorf("/ready (ready): expected 'ready' in body, got %s", rec.Body.String())
	}
}

// TestReadyEndpointReturnsUnavailableWhenServerNil verifies /ready returns 503 when server not initialized.
// Break test: in metrics.go's /ready handler, comment out the server == nil check.
func TestReadyEndpointReturnsUnavailableWhenServerNil(t *testing.T) {
	mux := newMetricsMux(nil) // explicitly nil

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/ready", nil)
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("/ready (server nil): expected 503, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "server not initialized") {
		t.Errorf("/ready (server nil): expected 'server not initialized' in body, got %s", rec.Body.String())
	}
}

// TestReadyEndpointReturnsUnavailableWhenAuthNil verifies /ready returns 503 when auth not initialized.
// Break test: in metrics.go's /ready handler, comment out the server.auth == nil check.
func TestReadyEndpointReturnsUnavailableWhenAuthNil(t *testing.T) {
	mux := newMetricsMux(&Server{
		auth: nil, // explicitly nil
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/ready", nil)
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("/ready (auth nil): expected 503, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "auth not initialized") {
		t.Errorf("/ready (auth nil): expected 'auth not initialized' in body, got %s", rec.Body.String())
	}
}

// readMetric scrapes the Prometheus registry and sums every label series of
// the named counter family. A full-package test run leaves other op-labeled
// series in bucketOpsTotal from earlier tests, so reading only the first
// series Gather returns can pick a series this test never touches.
func readMetric(name string) float64 {
	gather, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		return 0
	}

	var sum float64
	for _, mf := range gather {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			if m.Counter != nil && m.Counter.Value != nil {
				sum += *m.Counter.Value
			}
		}
	}
	return sum
}
