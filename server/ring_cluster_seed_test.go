package server

import (
	"fmt"
	"testing"

	"github.com/hoarddb/hoarddb-server/config"
)

// TestTheServerCarriesTheConfiguredSeedRate: replication.seed_bytes_per_second reaches the seed
// options the cluster opens with, and 0 stays 0, which is unlimited. [BREAK: drop the
// BytesPerSecond line of ringPeerPlan.clusterOptions]
func TestTheServerCarriesTheConfiguredSeedRate(t *testing.T) {
	for _, rate := range []int{12345, 0} {
		t.Run(fmt.Sprint(rate), func(t *testing.T) {
			cfg, identity, fingerprint := ringPlanFixture(t, func(cfg *config.Config) {
				cfg.Server.Listen = ringClusterPorts(t)
				cfg.Replication.SeedBytesPerSecond = rate
			})
			plan, err := planRingPeers(cfg, fingerprint, identity)
			if err != nil {
				t.Fatal(err)
			}
			options, listener, err := plan.clusterOptions(cfg, nil)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = listener.Close() })
			if got := options.Seed.BytesPerSecond; got != int64(rate) {
				t.Errorf("seed byte rate %d, want the configured %d", got, rate)
			}
		})
	}
}
