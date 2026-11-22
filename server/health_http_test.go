package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// probe sends one GET to the metrics mux of s and returns the recorded answer.
func probe(t *testing.T, s *Server, path string) *httptest.ResponseRecorder {
	t.Helper()
	recorder := httptest.NewRecorder()
	newMetricsMux(s).ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
	return recorder
}

// TestHealthProbeFollowsHealthCheck: /health answers from Server.HealthCheck,
// the same verdict the HEALTH_CHECK opcode and 'status' give. A healthy node
// answers 200 with a JSON body; a node whose check reports a problem answers
// 503 and names it, so an orchestrator's probe can see it.
func TestHealthProbeFollowsHealthCheck(t *testing.T) {
	s := newTestServer(t)

	healthy := probe(t, s, "/health")
	if healthy.Code != http.StatusOK {
		t.Fatalf("/health on a healthy node answered %d, want 200: %s", healthy.Code, healthy.Body)
	}
	var healthyBody struct {
		Healthy bool `json:"healthy"`
	}
	if err := json.Unmarshal(healthy.Body.Bytes(), &healthyBody); err != nil || !healthyBody.Healthy {
		t.Errorf("/health on a healthy node answered %q, want JSON with healthy true (%v)", healthy.Body, err)
	}

	const problem = "the store holds a file that is not sealed"
	s.storeSeal = storeSealCheck{problem: problem}
	unhealthy := probe(t, s, "/health")
	if unhealthy.Code != http.StatusServiceUnavailable {
		t.Fatalf("/health on a node with a problem answered %d, want 503: %s", unhealthy.Code, unhealthy.Body)
	}
	var unhealthyBody struct {
		Healthy bool     `json:"healthy"`
		Reasons []string `json:"reasons"`
	}
	if err := json.Unmarshal(unhealthy.Body.Bytes(), &unhealthyBody); err != nil {
		t.Fatalf("/health 503 body is not JSON: %q (%v)", unhealthy.Body, err)
	}
	if unhealthyBody.Healthy || len(unhealthyBody.Reasons) == 0 || !strings.Contains(strings.Join(unhealthyBody.Reasons, "; "), problem) {
		t.Errorf("/health 503 body = %q, want healthy false and a reason naming %q", unhealthy.Body, problem)
	}

	check, err := s.HealthCheck()
	if err != nil {
		t.Fatalf("HealthCheck: %v", err)
	}
	if check.Healthy {
		t.Errorf("HealthCheck.Healthy is true for a node with a problem; the probe and status must agree")
	}
}

// TestReadyProbeRefusesAnUnhealthyNode: /ready agrees with /health, so a
// probe that only watches readiness stops sending traffic to a damaged node.
func TestReadyProbeRefusesAnUnhealthyNode(t *testing.T) {
	s := newTestServer(t)
	if got := probe(t, s, "/ready"); got.Code != http.StatusOK {
		t.Fatalf("/ready on a healthy node answered %d, want 200: %s", got.Code, got.Body)
	}
	s.storeSeal = storeSealCheck{problem: "the store holds a file that is not sealed"}
	got := probe(t, s, "/ready")
	if got.Code != http.StatusServiceUnavailable {
		t.Errorf("/ready on a node with a problem answered %d, want 503: %s", got.Code, got.Body)
	}
}

// TestLiveProbeAnswersWhileUnhealthy: /live says only that the process is up.
// A node short of disk or with a damaged store is not fixed by a restart, so a
// liveness probe must not follow the health verdict.
func TestLiveProbeAnswersWhileUnhealthy(t *testing.T) {
	s := newTestServer(t)
	s.storeSeal = storeSealCheck{problem: "the store holds a file that is not sealed"}
	got := probe(t, s, "/live")
	if got.Code != http.StatusOK {
		t.Errorf("/live answered %d, want 200 while the process is up: %s", got.Code, got.Body)
	}
}
