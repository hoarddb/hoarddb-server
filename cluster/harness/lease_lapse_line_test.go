package harness_test

import (
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster/harness"
	"github.com/hoarddb/hoarddb-server/transport"
)

// findLapseLine returns a lapse line about node that holds all of want, or "".
func findLapseLine(c *harness.Cluster, node string, want ...string) string {
	for _, line := range c.LogLines() {
		if !strings.Contains(line, "lease lapse node="+node+" ") {
			continue
		}
		matches := true
		for _, part := range want {
			matches = matches && strings.Contains(line, part)
		}
		if matches {
			return line
		}
	}
	return ""
}

// lapseLineFor waits for a lapse line about node that holds all of want, and returns it.
func lapseLineFor(t *testing.T, c *harness.Cluster, node string, within time.Duration, want ...string) string {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		if line := findLapseLine(c, node, want...); line != "" {
			return line
		}
		if time.Now().After(deadline) {
			t.Fatalf("no lapse line for %s holding %q within %v; lines: %q", node, want, within, c.LogLines())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// lapseLineRaftTimeout makes Raft's leader lease (half of it, 500 ms) twice the renewer's heartbeat
// of 250 ms. A cut leader keeps its seat for at least that long after its last contact, so a
// renewer round always starts inside the cut while the node still leads, and its VerifyLeader
// stays in flight until Raft steps the node down. At the harness default (a 75 ms leader lease)
// only a tick that happened to fall in those 75 ms failed a round at all, and one that fell in
// their last millisecond failed it in under a millisecond, which the line reports as grantor_ms=0.
const lapseLineRaftTimeout = time.Second

// TestALapseLineNamesTheSlowPhase is spec lease-renewer-cadence O6, rewritten for S2 (O6'). A slow
// supervisor step no longer reaches the renewer, so the lapse is provoked by the round itself: at
// heartbeat_ms 250 every LEASE_REQ the follower writes is delayed 800 ms, the next tick comes over
// three heartbeats (750 ms) after the last, and the line written when the lapse ends names the
// round as the longest phase. A leader cut off from both voters and then healed writes a
// quorum_failed line that carries the error its VerifyLeader returned.
func TestALapseLineNamesTheSlowPhase(t *testing.T) {
	c := renewalClusterWith(t, harness.Options{NoDetection: true, NoProbes: true, CaptureLog: true, RaftTimeout: lapseLineRaftTimeout})
	leader := c.AwaitLeader(10 * time.Second)
	follower := otherThan(c, leader)

	c.DelayChannel(follower, leader, transport.ChannelControl, 800*time.Millisecond)
	line := lapseLineFor(t, c, follower, 30*time.Second, "cause=tick_gap", "phase_max=round:")
	c.DelayChannel(follower, leader, transport.ChannelControl, 0)
	if match := regexp.MustCompile(`gap_ms=(\d+)`).FindStringSubmatch(line); match == nil {
		t.Errorf("the tick_gap line has no gap_ms: %s", line)
	} else if gap, err := strconv.Atoi(match[1]); err != nil || gap <= 750 {
		t.Errorf("the tick_gap line's gap is not over three heartbeats (750 ms): %s", line)
	}
	for _, field := range []string{"role=follower", " began=", " lasted_ms=", " gap_ms=", " tick_late=true", " round_ms=", " turn_wait_ms=", " pre_tick_ms=", " wake_cut=", " unaccounted_ms=", " freeze_ms="} {
		if !strings.Contains(line, field) {
			t.Errorf("the lapse line has no %q: %s", field, line)
		}
	}
	c.AwaitLeases(30 * time.Second)

	// The cut lasts until the leader's lease is gone, not for a fixed time: the failed round ends
	// it when Raft steps the node down, at most two leader leases after the cut. Only a renewer
	// that a loaded host kept from ticking for a whole leader lease misses the round; its lease
	// then expires (step_down_ms, 2 s) with another cause, and the cut is repeated.
	for attempt := 0; attempt < 5; attempt++ {
		leader = c.AwaitLeader(10 * time.Second)
		cutLeader := c.Node(leader)
		c.Pause(leader)
		c.Await("the cut leader "+leader+" to lose its lease", 10*time.Second, func() bool { return !cutLeader.LeaseValid() })
		c.Heal()
		c.AwaitLeases(30 * time.Second)
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			if quorum := findLapseLine(c, leader, "cause=quorum_failed", `verify_err="`); quorum != "" {
				t.Logf("attempt %d: %s", attempt, quorum)
				// The text must be what Raft's VerifyLeader returned, not a later failed ask of the
				// node once it is a follower with no leader to ask.
				if match := regexp.MustCompile(`grantor_ms=(\d+)`).FindStringSubmatch(quorum); match == nil || match[1] == "0" {
					t.Errorf("the quorum_failed line does not carry the failing round's time as grantor_ms: %s", quorum)
				}
				_, errText, _ := strings.Cut(quorum, "verify_err=")
				if !strings.Contains(errText, "leader") || strings.Contains(errText, "no leader is known") || strings.Contains(errText, "control call") {
					t.Errorf("the quorum_failed line does not carry the VerifyLeader error: %s", quorum)
				}
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	t.Fatalf("no cut leader wrote a quorum_failed line in 5 attempts; lines: %q", c.LogLines())
}
