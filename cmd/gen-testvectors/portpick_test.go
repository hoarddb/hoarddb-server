package main

import (
	"net"
	"strconv"
	"testing"
)

// TestPickedPortsAvoidTheEphemeralRange fails on a picker that reserves a port
// with Listen on port 0 and releases it: those ports come from the range the
// kernel gives outgoing connections, so one can be taken before the server
// binds it (BUG-0487).
func TestPickedPortsAvoidTheEphemeralRange(t *testing.T) {
	ephemeralStart := ephemeralPortStart()
	seen := make(map[int]bool)
	for pick := 0; pick < 100; pick++ {
		ringPort, err := freeRingPort()
		if err != nil {
			t.Fatalf("freeRingPort: %v", err)
		}
		plainPort, err := freePort()
		if err != nil {
			t.Fatalf("freePort: %v", err)
		}
		for _, port := range []int{ringPort, ringPort + 1, plainPort} {
			if port >= ephemeralStart {
				t.Fatalf("port %d is in the ephemeral range starting at %d", port, ephemeralStart)
			}
			if seen[port] {
				t.Fatalf("port %d was handed out twice", port)
			}
			seen[port] = true
		}
	}
}

// TestPickedRingPortLeavesItsSuccessorFree checks the internode listener's
// port, ring+1, can be bound right after the pick.
func TestPickedRingPortLeavesItsSuccessorFree(t *testing.T) {
	ringPort, err := freeRingPort()
	if err != nil {
		t.Fatalf("freeRingPort: %v", err)
	}
	for _, port := range []int{ringPort, ringPort + 1} {
		listener, err := net.Listen("tcp", "127.0.0.1:"+strconv.Itoa(port))
		if err != nil {
			t.Fatalf("binding picked port %d: %v", port, err)
		}
		if err := listener.Close(); err != nil {
			t.Fatalf("closing port %d: %v", port, err)
		}
	}
}
