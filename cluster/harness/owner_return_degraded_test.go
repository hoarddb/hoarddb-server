package harness_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/harness"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/transport"
)

// bug0926SameSlot returns count ids of the ledger bucket that share the pair of first.
func bug0926SameSlot(t *testing.T, first string, count int) []string {
	t.Helper()
	slot, err := wire.PlacementOf(proofDB, ledgerBucket, first)
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for i := 0; len(ids) < count; i++ {
		if i > 400000 {
			t.Fatalf("no %d more ids on slot %d", count, slot)
		}
		id := fmt.Sprintf("%s-same-%d", first, i)
		got, err := wire.PlacementOf(proofDB, ledgerBucket, id)
		if err != nil {
			t.Fatal(err)
		}
		if got == slot {
			ids = append(ids, id)
		}
	}
	return ids
}

// bug0926WriteUntil retries one write through via until it is acknowledged or budget passes.
func bug0926WriteUntil(l *harness.Ledger, c *harness.Cluster, via, id string, budget time.Duration) (wire.Inserted, error) {
	deadline := time.Now().Add(budget)
	for {
		got, err := h3Write(l, c, via, "bug0926", id, 3*time.Second)
		if err == nil || time.Now().After(deadline) {
			return got, err
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// bug0926OwnerReturns is the scenario of BUG-0926 without a join. Three voters, RF 2. A follower
// owns a pair whose other copy is the other follower. The owner is stopped and committed down,
// and a write to the pair is ordered by the other copy with one copy (applied_degraded; v1.1.0
// answers applied, BUG-0919). The owner is started again; with loseAnswers its data-channel
// answers from the other copy are lost while it takes the pair back (the other copy still
// receives its requests, and every other path between them works). A write to the same pair is
// tried during that window, the answers flow again, and one more write is made. The write the
// other copy acknowledged must then be on both copies.
func bug0926OwnerReturns(t *testing.T, loseAnswers bool) {
	h3Gate(t)
	c := h3Setup(t, harness.P1, harness.Options{JoinerActivation: true})
	leader := c.AwaitLeader(10 * time.Second)
	var followers []string
	for _, id := range c.IDs() {
		if id != leader {
			followers = append(followers, id)
		}
	}
	owner, other := followers[0], followers[1]
	first := h3Owned(t, c, owner, 1, "bug0926", leader)[0]
	later := bug0926SameSlot(t, first, 2)
	l := h3Ledger(t, "bug0926")

	l.Fault("stop the owner " + owner)
	c.Kill(owner)
	if !h3MemberIs(c, leader, owner, cluster.MemberDown, 30*time.Second) {
		t.Fatalf("%s was stopped and is not committed down 30 s later", owner)
	}
	degraded, err := bug0926WriteUntil(l, c, other, first, 20*time.Second)
	if err != nil {
		t.Fatalf("writing %s through %s while %s is down: %v", first, other, owner, err)
	}
	if degraded.CopiesConfirmed != 1 {
		t.Fatalf("the write while %s is down was acknowledged with %d copies, want 1", owner, degraded.CopiesConfirmed)
	}
	t.Logf("%s acknowledged %s, %d copy, at %s by %s", first, degraded.Outcome, degraded.CopiesConfirmed, degraded.Pos, degraded.ServedBy)

	if loseAnswers {
		c.DropReplies(owner, other, transport.ChannelData, true)
		l.Fault("start " + owner + " with its data-channel answers from " + other + " lost")
	} else {
		l.Fault("start " + owner)
	}
	c.Restart(owner)
	if !h3MemberIs(c, leader, owner, cluster.MemberActive, 30*time.Second) {
		t.Fatalf("%s was started and is not active 30 s later", owner)
	}
	// Held, or ordered after the other copy's tail; on the broken build the owner opens on its
	// own record a few seconds after its return and orders this write at the new epoch.
	if got, err := bug0926WriteUntil(l, c, owner, later[0], 8*time.Second); err == nil {
		t.Logf("%s acknowledged %s at %s by %s", later[0], got.Outcome, got.Pos, got.ServedBy)
	}
	if loseAnswers {
		c.DropReplies(owner, other, transport.ChannelData, false)
		l.Fault("answers from " + other + " to " + owner + " flow again")
	}
	if _, err := bug0926WriteUntil(l, c, owner, later[1], 30*time.Second); err != nil {
		t.Fatalf("writing %s through %s: %v", later[1], owner, err)
	}

	var missing []string
	deadline := time.Now().Add(15 * time.Second)
	for {
		missing = missing[:0]
		for _, id := range []string{owner, other} {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			_, err := c.Node(id).GetLocal(ctx, proofDB, ledgerBucket, first)
			cancel()
			if err != nil {
				missing = append(missing, fmt.Sprintf("%s: %v", id, err))
			}
		}
		if len(missing) == 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if len(missing) > 0 {
		t.Fatalf("%s was acknowledged %s by %s with one copy while %s was down, and after %s returned it is gone from the pair's copies: %v",
			first, degraded.Outcome, degraded.ServedBy, owner, owner, missing)
	}
}

// TestAWriteOrderedWhileTheOwnerWasDownSurvivesItsReturn is BUG-0926 without a join: the returning
// owner cannot hear the copy that ordered the pair while it was down. It must hold the pair until
// it does, not open on its own older record: once it orders, its first record names an earlier
// prevEnd than that copy's tail, the copy is reset to the owner's, and the write is gone from both.
func TestAWriteOrderedWhileTheOwnerWasDownSurvivesItsReturn(t *testing.T) {
	bug0926OwnerReturns(t, true)
}

// TestAWriteOrderedWhileTheOwnerWasDownSurvivesItsReturnWhenItHearsTheCopy is the same return with
// every answer flowing. [GUARD: the scenario itself keeps the write when nothing is lost]
func TestAWriteOrderedWhileTheOwnerWasDownSurvivesItsReturnWhenItHearsTheCopy(t *testing.T) {
	bug0926OwnerReturns(t, false)
}
