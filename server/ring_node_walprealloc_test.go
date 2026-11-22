package server

import (
	"testing"

	"github.com/hoarddb/hoarddb-server/config"
)

// TestRingNodeTakesThePreallocationTheServerResolved: the entry-ring node's log is
// preallocated exactly when storage.wal_preallocate resolves to on (or to auto on ext2/3/4).
func TestRingNodeTakesThePreallocationTheServerResolved(t *testing.T) {
	original := walPreallocateResolver
	t.Cleanup(func() { walPreallocateResolver = original })
	for _, resolved := range []bool{false, true} {
		walPreallocateResolver = func(*config.Config, string) walModeResolution { return walModeResolution{On: resolved} }
		cfg := config.DefaultConfig()
		cfg.Storage.DataDir = "/data"
		if got := ringNodeConfig(cfg).WALPreallocate; got != resolved {
			t.Fatalf("resolver said %v, the ring node was configured with %v", resolved, got)
		}
	}
}
