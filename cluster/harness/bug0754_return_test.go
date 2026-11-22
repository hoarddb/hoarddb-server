package harness_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/harness"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/transport"
)

// bug0754ReturnOptions lets the returning node ask for itself back, which a harness cluster
// does not do unless a test says so.
func bug0754ReturnOptions() harness.Options {
	return harness.Options{FastDetection: true, JoinerActivation: true, WCTimeout: 5 * time.Second}
}

// bug0754IDWithN2 returns a document id whose copy set holds n2, or that n2 owns when owns is set.
func bug0754IDWithN2(t *testing.T, c *harness.Cluster, seed string, owns bool) string {
	t.Helper()
	probe := c.Node("n1")
	for i := 0; i < 2000; i++ {
		id := fmt.Sprintf("%s-%d", seed, i)
		owner, copies, err := probe.Owner(proofCtx(t), proofDB, proofBucket, id)
		if err != nil {
			t.Fatalf("placing %s: %v", id, err)
		}
		if owns && owner == "n2" {
			return id
		}
		if !owns {
			for _, copyNode := range copies {
				if copyNode == "n2" {
					return id
				}
			}
		}
	}
	t.Fatalf("no id with n2 in its copy set (owns %v) in 2000 tries", owns)
	return ""
}

// bug0754InsertThrough inserts id through via, retrying while the owner's pair is held by an
// open handoff: every epoch change opens one and a write sent inside it is refused with
// "nothing was written", which is safe to repeat.
func bug0754InsertThrough(t *testing.T, c *harness.Cluster, via, id string) wire.Inserted {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		got, err := c.Node(via).Insert(proofCtx(t), proofDB, proofBucket, bson.D{{Key: "_id_", Value: id}, {Key: "name", Value: "Ada"}})
		if err == nil {
			return got
		}
		if time.Now().After(deadline) {
			t.Fatalf("insert of %s through %s was not accepted within 30s: %v (background: %v)", id, via, err, c.BackgroundErrors())
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// bug0754WriteWhileN2IsDown writes id through n1 and requires an acknowledged outcome that a
// surviving copy confirmed, so the document is one n2 must catch up on when it returns.
func bug0754WriteWhileN2IsDown(t *testing.T, c *harness.Cluster, id string) {
	t.Helper()
	// The survivors' leases lapse around the commit that marks n2 down; a write sent before
	// they are renewed is refused for a reason that is not under test.
	// n2 is still a running process when it is only cut off, so the Await helpers that cover
	// every running node cannot be used here: they would wait for n2 as well.
	c.Await("n1 and n3 to agree on the state with n2 down and hold a lease", 30*time.Second, func() bool {
		first, second := c.Node("n1").State(), c.Node("n3").State()
		return first.StateSeq == second.StateSeq && first.Epoch == second.Epoch &&
			c.Node("n1").LeaseValid() && c.Node("n3").LeaseValid()
	})
	got := bug0754InsertThrough(t, c, "n1", id)
	if got.Outcome != machine.OutcomeApplied && got.Outcome != machine.OutcomeAppliedDegraded {
		t.Fatalf("insert of %s while n2 is down answered outcome %v, want applied or applied_degraded", id, got.Outcome)
	}
	if got.CopiesConfirmed < 1 {
		t.Fatalf("insert of %s while n2 is down was confirmed by %d copies, want at least 1", id, got.CopiesConfirmed)
	}
}

// bug0754AwaitReturned checks what a returned member must show without an operator: n2
// committed active at every running node within the failover budget, holding the document it
// missed, owning its slots again, and staying active.
func bug0754AwaitReturned(t *testing.T, c *harness.Cluster, missed string) {
	t.Helper()
	c.Await("n2 committed active at every node", quorumFailoverBudget, func() bool {
		for _, running := range c.Running() {
			active := false
			for _, member := range c.Node(running).State().Members {
				if member.ID == "n2" && member.State == cluster.MemberActive {
					active = true
				}
			}
			if !active {
				return false
			}
		}
		return true
	})
	c.Await("n2 to hold "+missed, 30*time.Second, func() bool {
		_, found, err := c.Node("n2").LocalDocument(proofCtx(t), proofDB, proofBucket, missed)
		return err == nil && found
	})
	owned := bug0754IDWithN2(t, c, "o0754", true)
	got := bug0754InsertThrough(t, c, "n1", owned)
	if got.Outcome != machine.OutcomeApplied || got.CopiesConfirmed != 2 {
		t.Fatalf("insert of %s, a slot n2 owns again, answered %v with %d copies confirmed, want applied and 2", owned, got.Outcome, got.CopiesConfirmed)
	}
	// The detector must not put the returned member down again behind the activation.
	holdUntil := time.Now().Add(3 * time.Second)
	for time.Now().Before(holdUntil) {
		for _, running := range c.Running() {
			for _, member := range c.Node(running).State().Members {
				if member.ID == "n2" && member.State != cluster.MemberActive {
					t.Fatalf("%s shows n2 as %v after it was committed active", running, member.State)
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TestADownMemberThatRestartsIsActivatedOnceCaughtUp: a member committed down whose process
// comes back at the incarnation it was registered with is committed active by its own ask, with
// no operator (BUG-0754).
func TestADownMemberThatRestartsIsActivatedOnceCaughtUp(t *testing.T) {
	c := proofSetup(t, bug0754ReturnOptions())
	id := bug0754IDWithN2(t, c, "d0754", false)
	c.Kill("n2")
	h3AwaitMember(t, c, "n1", "n2", cluster.MemberDown, 30*time.Second)
	bug0754WriteWhileN2IsDown(t, c, id)
	c.Restart("n2")
	bug0754AwaitReturned(t, c, id)
}

// TestADownMemberThatHealsIsActivatedOnceCaughtUp: the same return without a restart, a member
// cut off long enough to be committed down whose partition then heals (BUG-0754).
func TestADownMemberThatHealsIsActivatedOnceCaughtUp(t *testing.T) {
	c := proofSetup(t, bug0754ReturnOptions())
	id := bug0754IDWithN2(t, c, "d0754", false)
	c.Pause("n2")
	h3AwaitMember(t, c, "n1", "n2", cluster.MemberDown, 30*time.Second)
	bug0754WriteWhileN2IsDown(t, c, id)
	c.Heal()
	c.AwaitConverged(30 * time.Second)
	bug0754AwaitReturned(t, c, id)
}

// TestAHandAskedActivationOfADownMemberIsCommitted: the leader's side alone. With the ask loop
// held n2 stays down after its restart, and the activation request n2 sends by hand is
// committed once; asking again commits nothing (BUG-0754).
func TestAHandAskedActivationOfADownMemberIsCommitted(t *testing.T) {
	c := proofSetup(t, harness.Options{FastDetection: true, HoldSeedReports: true, WCTimeout: 5 * time.Second})
	c.Kill("n2")
	h3AwaitMember(t, c, "n1", "n2", cluster.MemberDown, 30*time.Second)
	c.Restart("n2")
	for _, member := range c.Node("n1").State().Members {
		if member.ID == "n2" && member.State != cluster.MemberDown {
			t.Fatalf("n2 is %v after its restart with the ask loop held; the test needs it down", member.State)
		}
	}
	leader := c.AwaitLeader(10 * time.Second)
	if leader == "n2" {
		t.Fatal("n2 won the election after its restart; the test needs another node to lead")
	}

	msg, err := c.CallAs("n2", leader, transport.ChannelControl, membershipMessage(t, 21, transport.MembershipActivate, "n2", "", false))
	first := membershipReplyOf(t, msg, err)
	if first.Status != transport.MembershipDone || !strings.Contains(first.Result, "Activated n2") {
		t.Fatalf("the leader %s answered n2's activation ask with status %v result %q (%s), want done and %q: a down member is never activated", leader, first.Status, first.Result, first.Message, "Activated n2")
	}
	c.Await("n2 committed active at every node", quorumFailoverBudget, func() bool {
		for _, running := range c.Running() {
			for _, member := range c.Node(running).State().Members {
				if member.ID == "n2" && member.State != cluster.MemberActive {
					return false
				}
			}
		}
		return true
	})
	stateSeq := c.AwaitConverged(10 * time.Second).StateSeq

	msg, err = c.CallAs("n2", leader, transport.ChannelControl, membershipMessage(t, 22, transport.MembershipActivate, "n2", "", false))
	second := membershipReplyOf(t, msg, err)
	if second.Status != transport.MembershipDone || !strings.Contains(second.Result, "already active") {
		t.Fatalf("the second ask answered status %v result %q (%s), want done and %q", second.Status, second.Result, second.Message, "already active")
	}
	if after := c.Node(leader).State().StateSeq; after != stateSeq {
		t.Fatalf("state_seq moved from %d to %d on an ask for a member that is already active", stateSeq, after)
	}
}
