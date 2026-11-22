package server

import (
	"testing"
	"time"
)

// The drain series are registered once, node-wide, and move by one per wait and by the wait's seconds.
func TestDrainMetricCountsWaitsAndSeconds(t *testing.T) {
	const drainsName, secondsName = "hoarddb_write_pipeline_drains_total", "hoarddb_write_pipeline_drain_wait_seconds_total"
	drains, seconds := counterValue(t, drainsName), counterValue(t, secondsName)
	observeDrain(250 * time.Millisecond)
	observeDrain(750 * time.Millisecond)
	if got := counterValue(t, drainsName) - drains; got != 2 {
		t.Errorf("drains rose by %v, want 2", got)
	}
	if got := counterValue(t, secondsName) - seconds; got < 0.999 || got > 1.001 {
		t.Errorf("drain wait seconds rose by %v, want 1", got)
	}
}
