package server

import (
	"net"
	"testing"
	"time"
)

// TestRebindOutlastsAnotherProcessHoldingThePort: A restarted node
// must come back on its old internode address, and between Stop closing it and
// the rebind the port is free for any other process on the host: a test suite
// running beside this one, or an outbound connection whose source port is in
// the ephemeral range. The helper has to wait out a holder that keeps the port
// for longer than a socket takes to close, not just for the close itself.
func TestRebindOutlastsAnotherProcessHoldingThePort(t *testing.T) {
	const holdFor = 4 * time.Second // longer than the helper's original 3 s retry window

	ports := reserveNodePorts(t)
	_ = ports.internode.Close()
	_ = ports.driver.Close()

	holder, err := net.Listen("tcp", ports.internodeAddr)
	if err != nil {
		t.Fatalf("test setup: taking the released port %s: %v", ports.internodeAddr, err)
	}
	released := time.AfterFunc(holdFor, func() { _ = holder.Close() })
	defer released.Stop()
	defer func() { _ = holder.Close() }()

	started := time.Now()
	fresh := ports.rebind(t)
	if waited := time.Since(started); waited < holdFor-time.Second {
		t.Errorf("rebind returned after %s while another listener still held %s", waited, ports.internodeAddr)
	}
	if fresh.internode.Addr().String() != ports.internodeAddr {
		t.Errorf("rebound internode on %s; want the old address %s", fresh.internode.Addr(), ports.internodeAddr)
	}
}
