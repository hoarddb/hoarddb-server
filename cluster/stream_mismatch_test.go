package cluster

import (
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/transport"
)

// TestClusterMismatchNamesThisNodesAddress: [GUARD the refusal prints the address the other node
// is told to be joined through, not a placeholder].
func TestClusterMismatchNamesThisNodesAddress(t *testing.T) {
	layer := &streamLayer{hooks: streamHooks{local: func() localIdentity {
		return localIdentity{NodeID: "a", ClusterID: [16]byte{1}, DataAddr: "node-a.example.test:4433"}
	}}}
	err := layer.checkIdentity(transport.Handshake{NodeID: "b", ClusterID: [16]byte{2}})
	if err == nil {
		t.Fatal("two cluster ids were accepted")
	}
	if !strings.Contains(err.Error(), "node-a.example.test:4433") || strings.Contains(err.Error(), "<host:port>") {
		t.Fatalf("refusal %q does not name this node's address", err)
	}
}
