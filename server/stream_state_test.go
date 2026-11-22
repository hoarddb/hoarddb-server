package server

import (
	"testing"
	"time"
)

// TestReplayFrameTimeoutDoesNotShrinkWithTheWriteConcernTimeout is BUG-0245
// part 2: a write-concern timeout bounds a client's wait, not a peer's apply
// of a replay frame, so a short one must not shrink the frame's timeout below
// the floor, and a long one still halves as before.
func TestReplayFrameTimeoutDoesNotShrinkWithTheWriteConcernTimeout(t *testing.T) {
	cases := []struct {
		name      string
		wcTimeout time.Duration
		want      time.Duration
	}{
		{"short write-concern timeout hits the floor", 50 * time.Millisecond, 2 * time.Second},
		{"just under twice the floor hits the floor", 3 * time.Second, 2 * time.Second},
		{"default five seconds is half", 5 * time.Second, 2500 * time.Millisecond},
		{"long write-concern timeout is half", 10 * time.Second, 5 * time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cluster := &Cluster{wcTimeout: tc.wcTimeout}
			if got := cluster.replShipTimeout(); got != tc.want {
				t.Errorf("replShipTimeout() with wcTimeout %s = %s, want %s", tc.wcTimeout, got, tc.want)
			}
		})
	}
}
