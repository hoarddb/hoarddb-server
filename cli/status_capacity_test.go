package cli

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/server"
)

// TestStatusPrintsCapacityRows (node-capacity spec T-13, CLI half) proves
// status prints one capacity line per resource from a live server.
func TestStatusPrintsCapacityRows(t *testing.T) {
	c := newDumpTestServer(t)
	var terminal bytes.Buffer
	c.handler.terminal = &terminal
	if err := c.handler.handleStatus(); err != nil {
		t.Fatalf("handleStatus: %v", err)
	}
	out := terminal.String()
	if !strings.Contains(out, "Capacity:\n") {
		t.Fatalf("status printed no capacity block:\n%s", out)
	}
	for _, resource := range []string{"fds", "mappings", "memory", "disk"} {
		if !strings.Contains(out, "\n  "+resource+" ") {
			t.Fatalf("status printed no %s row:\n%s", resource, out)
		}
	}
}

// TestPrintCapacityShowsWhatIsNotFine (build doc BT-11 and spec T-20, CLI
// halves) proves an unavailable row prints "-" rather than a ratio of 0, a
// stale sample prints its age, a red row points to the log, and an older
// server's missing capacity prints nothing.
func TestPrintCapacityShowsWhatIsNotFine(t *testing.T) {
	ratio := func(v float64) *float64 { return &v }
	sampledAt := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	now := sampledAt.Add(3*time.Minute + 12*time.Second)

	for _, tt := range []struct {
		name     string
		capacity *server.HealthCapacity
		want     []string
		banned   []string
	}{
		{
			name: "an unavailable row prints no ratio",
			capacity: &server.HealthCapacity{SampledAt: sampledAt.Unix(), Rows: []server.CapacityRow{
				{Resource: "disk", Threshold: 0.85, State: "unavailable"},
			}},
			want:   []string{"  disk           -  threshold 0.85  unavailable\n"},
			banned: []string{"0.00"},
		},
		{
			name: "a stale sample prints its age",
			capacity: &server.HealthCapacity{SampledAt: sampledAt.Unix(), Rows: []server.CapacityRow{
				{Resource: "fds", Ratio: ratio(0.12), Threshold: 0.80, State: "stale"},
			}},
			want: []string{"capacity last sampled 3m12s ago; the capacity sampler may be stuck\n"},
		},
		{
			name: "a red row points to the log",
			capacity: &server.HealthCapacity{SampledAt: sampledAt.Unix(), Rows: []server.CapacityRow{
				{Resource: "memory", Ratio: ratio(0.91), Threshold: 0.80, State: "red"},
			}},
			want: []string{"  memory      0.91  threshold 0.80  red\n", "A red row's action is in this node's log (the capacity WARN line).\n"},
		},
		{
			name:   "an older server sends no capacity",
			banned: []string{"Capacity", "capacity"},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var terminal bytes.Buffer
			h := NewCommandHandler(&terminal, false)
			h.printCapacity(tt.capacity, now)
			out := terminal.String()
			for _, want := range tt.want {
				if !strings.Contains(out, want) {
					t.Fatalf("output lacks %q:\n%s", want, out)
				}
			}
			for _, banned := range tt.banned {
				if strings.Contains(out, banned) {
					t.Fatalf("output contains %q:\n%s", banned, out)
				}
			}
		})
	}
}
