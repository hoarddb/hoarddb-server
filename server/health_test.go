package server

import (
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/version"
)

// TestHealthReportsTheBuildVersion: status prints the version the health
// answer carries, so it must come from the version package every other report
// reads and not from a literal (BUG-0273).
func TestHealthReportsTheBuildVersion(t *testing.T) {
	previous := version.Version
	version.Version = "v9.9.9"
	t.Cleanup(func() { version.Version = previous })

	health, err := newTestServer(t).HealthCheck()
	if err != nil {
		t.Fatalf("HealthCheck: %v", err)
	}
	if health.Version != "v9.9.9" {
		t.Errorf("health version = %q, want the build's version %q", health.Version, "v9.9.9")
	}
}

// TestHealthCheckSaysWhyAReplayingOrDivergedNodeIsNotHealthy (review n2): HEALTH_CHECK passes the
// replay gate, so a node that serves nothing yet must not answer healthy: a readiness probe
// would send traffic to it. The reason says which. Break: drop the ringLoading read.
func TestHealthCheckSaysWhyAReplayingOrDivergedNodeIsNotHealthy(t *testing.T) {
	for _, tc := range []struct {
		name             string
		diverged, loaded bool
		want             string
	}{
		{"replaying", false, false, "replaying its log"},
		{"diverged", true, false, "diverged"},
		{"diverged beats replaying", true, true, "diverged"},
		{"serving", false, true, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestServer(t)
			loading := func() string { return ringLoadingReason(tc.diverged, tc.loaded) }
			s.ringLoading.Store(&loading)
			health, err := s.HealthCheck()
			if err != nil {
				t.Fatal(err)
			}
			if tc.want == "" {
				if !health.Healthy || len(health.Reasons) != 0 {
					t.Fatalf("a serving node: healthy %v, reasons %v", health.Healthy, health.Reasons)
				}
				return
			}
			if health.Healthy || len(health.Reasons) == 0 || !strings.HasPrefix(health.Reasons[0], tc.want) {
				t.Fatalf("healthy %v, reasons %v, want a first reason starting %q", health.Healthy, health.Reasons, tc.want)
			}
		})
	}
}
