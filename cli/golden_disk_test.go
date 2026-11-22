package cli

import (
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/config"
)

// TestStatusDiskRowIgnoresTheHost is BUG-0483: the disk row of the status
// golden came from the test host's filesystem, so a nearly-full host failed a
// test with no code change. The server's high-water mark here is below this
// host's real usage, so an unpinned disk probe reads red; the golden fixture
// pins the probe, and the row must still read ok whatever the host holds.
func TestStatusDiskRowIgnoresTheHost(t *testing.T) {
	s := newGoldenSessionWith(t, func(cfg *config.Config) {
		cfg.Server.DiskHighWaterPct = 15
	})
	transcript := runStatements(t, s.cli, s.out, "status")

	if !strings.Contains(transcript, "Healthy:      true") {
		t.Errorf("a pinned disk left the node unhealthy:\n%s", transcript)
	}
	if !strings.Contains(transcript, "threshold 0.15  ok") {
		t.Errorf("the disk row did not read ok under the pinned probe:\n%s", transcript)
	}
}
