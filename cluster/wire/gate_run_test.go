package wire

import "testing"

// TestTheNodeWiresTheGate: a networked node owns one gate driver, so a retired copy is
// deleted only by a pass that asked its copies. [BREAK: leave c.gate unset in startCluster]
func TestTheNodeWiresTheGate(t *testing.T) {
	node := s0OpenClusterNode(t, t.TempDir(), nil)
	if node.cl.gate == nil {
		t.Fatal("the node has no gate driver, so a retired copy would never be deleted")
	}
}
