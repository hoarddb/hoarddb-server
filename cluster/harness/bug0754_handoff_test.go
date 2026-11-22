package harness_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/harness"
)

// bug0754HandoffOptions lets the returning node ask for itself back, at the profile whose stop
// bound is short enough for the ask to meet the handoff that moved its slots.
func bug0754HandoffOptions() harness.Options {
	return harness.Options{JoinerActivation: true}
}

// bug0754AwaitActiveEverywhere waits until every running node commits member as active.
func bug0754AwaitActiveEverywhere(t *testing.T, c *harness.Cluster, member string) {
	t.Helper()
	for _, watcher := range c.Running() {
		h3AwaitMember(t, c, watcher, member, cluster.MemberActive, quorumFailoverBudget)
	}
}

// TestADownMemberThatReturnsDuringTheHandoffLosesNoWrite: n2 is restarted the moment it is
// committed down, so its ask can land while the barrier that moved its slots to the survivors
// is still open. Every write acknowledged through n1 meanwhile is readable through all three
// nodes afterwards, and the ledger names no violated invariant (BUG-0754, invariant I4). A run
// in which the ask lands after the barrier is also valid: the ledger judges the outcome, no
// sleep forces the overlap.
func TestADownMemberThatReturnsDuringTheHandoffLosesNoWrite(t *testing.T) {
	h3Gate(t)
	finds := newH3Findings(t, "H3-0754")
	defer finds.conclude()
	c := h3Setup(t, harness.P1, bug0754HandoffOptions())
	ids := h3Owned(t, c, "n2", 40, "d0754h", "")
	l := h3Ledger(t, ids[0])

	l.Fault("kill n2")
	c.Kill("n2")
	h3AwaitMember(t, c, "n1", "n2", cluster.MemberDown, 30*time.Second)

	l.Fault("restart n2")
	restarted := make(chan struct{})
	go func() {
		defer close(restarted)
		c.Restart("n2")
	}()
	for _, id := range ids {
		h3WriteAcked(t, l, c, "n1", "w-handoff", id, quorumFailoverBudget)
	}
	<-restarted
	bug0754AwaitActiveEverywhere(t, c, "n2")
	for _, via := range []string{"n1", "n2", "n3"} {
		for _, id := range ids {
			h3ReadServed(t, l, c, via, "r-"+via, id, quorumFailoverBudget)
		}
	}
	h3Check(t, c, l, nil, finds, map[string]h3Known{harness.InvRefusalsAreNamed: h3UncodedForwardKnown})
}

// TestADownMemberThatReturnsTwiceIsActivatedBothTimes: n2 is killed and restarted twice, the
// second kill after the first activation is seen everywhere. The restart follows the down
// commit at once: at this profile a member down for dead_ms (3 s) is committed dead, which only a
// re-registration reverses and which this bug does not cover. Each cycle commits down and then
// active, so the epoch rises by two per cycle and the second return needs the same ask as the
// first (BUG-0754).
func TestADownMemberThatReturnsTwiceIsActivatedBothTimes(t *testing.T) {
	h3Gate(t)
	finds := newH3Findings(t, "H3-0754")
	defer finds.conclude()
	c := h3Setup(t, harness.P1, bug0754HandoffOptions())
	ids := h3Owned(t, c, "n2", 6, "t0754h", "")
	l := h3Ledger(t, ids[0])
	c.AwaitConverged(10 * time.Second)
	epoch := h3Epoch(c, "n1")

	for cycle := 1; cycle <= 2; cycle++ {
		l.Fault(fmt.Sprintf("kill n2 (cycle %d)", cycle))
		c.Kill("n2")
		h3AwaitMember(t, c, "n1", "n2", cluster.MemberDown, 30*time.Second)
		l.Fault(fmt.Sprintf("restart n2 (cycle %d)", cycle))
		c.Restart("n2")
		bug0754AwaitActiveEverywhere(t, c, "n2")
		c.AwaitConverged(quorumFailoverBudget)
		if got, want := h3Epoch(c, "n1"), epoch+2*uint64(cycle); got != want {
			t.Fatalf("after cycle %d the epoch is %d, want %d (down and active are one commit each)", cycle, got, want)
		}
		for _, id := range ids[(cycle-1)*3 : cycle*3] {
			h3WriteAcked(t, l, c, "n1", fmt.Sprintf("w-cycle-%d", cycle), id, quorumFailoverBudget)
		}
	}
	for _, via := range []string{"n1", "n2", "n3"} {
		for _, id := range ids {
			h3ReadServed(t, l, c, via, "r-"+via, id, quorumFailoverBudget)
		}
	}
	h3Check(t, c, l, nil, finds, map[string]h3Known{harness.InvRefusalsAreNamed: h3UncodedForwardKnown})
}
