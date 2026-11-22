package config

import (
	"strings"
	"testing"
)

// TestLoopbackRefusalDoesNotSuggestABrokenRing is SEC-1 F2/F5. HOARDDB_LISTEN=
// 0.0.0.0 makes the node advertise 127.0.0.1 to its peers unless it also has
// a node ID and id@addr seeds, so the message must lead with the node's real
// address and must not offer the bare wildcard as the fix. It says "the
// default" only when the address is the default.
func TestLoopbackRefusalDoesNotSuggestABrokenRing(t *testing.T) {
	defaultErr := clusterConfig(t, DefaultConfig().Server.Listen, "192.0.2.2:4433").Validate()
	if defaultErr == nil {
		t.Fatal("default loopback node with a remote seed was accepted")
	}
	if strings.Contains(defaultErr.Error(), "HOARDDB_LISTEN=0.0.0.0") {
		t.Errorf("message offers HOARDDB_LISTEN=0.0.0.0 as a fix: %v", defaultErr)
	}
	for _, want := range []string{"HOARDDB_LISTEN=192.0.2.1:4433", "HOARDDB_NODE_ID", "the default"} {
		if !strings.Contains(defaultErr.Error(), want) {
			t.Errorf("message %q does not mention %q", defaultErr, want)
		}
	}

	explicitErr := clusterConfig(t, "[::1]:4433", "192.0.2.2:4433").Validate()
	if explicitErr == nil {
		t.Fatal("explicit [::1] node with a remote seed was accepted")
	}
	if strings.Contains(explicitErr.Error(), "the default") {
		t.Errorf("message calls an explicit [::1] the default: %v", explicitErr)
	}
}
