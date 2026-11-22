package server

import (
	"testing"
	"time"
)

// TestAGossipWakeInsideTheRateLimitIsDeferredNotDropped: wakeGossip is rate
// limited to one wake per minGossipWakeInterval. A second wake inside the
// limit — a node settling just after it confirmed its view (owner ruling 52),
// or after it adopted the change it settles — must still wake the gossip loops
// once the limit passes; dropped, the news waits for the next twenty-second
// exchange. Guard: drop a wake inside the limit.
func TestAGossipWakeInsideTheRateLimitIsDeferredNotDropped(t *testing.T) {
	c := &Cluster{gossipWake: make(chan struct{})}
	c.wakeGossip()
	second := c.gossipWakeSignal()
	c.wakeGossip()
	select {
	case <-second:
	case <-time.After(10 * minGossipWakeInterval):
		t.Fatalf("a wake %v inside the rate limit never woke the gossip loops", minGossipWakeInterval)
	}
	third := c.gossipWakeSignal()
	c.wakeGossip()
	c.wakeGossip()
	select {
	case <-third:
	case <-time.After(10 * minGossipWakeInterval):
		t.Fatal("two wakes inside the rate limit never woke the gossip loops")
	}
	if fourth := c.gossipWakeSignal(); fourth == third {
		t.Fatal("the deferred wake did not replace the signal it closed")
	}
}
