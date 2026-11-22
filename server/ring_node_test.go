package server

import (
	"testing"

	"github.com/hoarddb/hoarddb-server/config"
)

// TestRingNodeConfigCarriesNearestMaxElements guards the carry from the file
// setting to the node: a server that drops the value would leave every node at
// the 100000 default whatever the operator set.
func TestRingNodeConfigCarriesNearestMaxElements(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Query.NearestMaxElements = 7
	if got := ringNodeConfig(cfg).NearestMaxElements; got != 7 {
		t.Errorf("ringNodeConfig.NearestMaxElements = %d, want 7", got)
	}
}
