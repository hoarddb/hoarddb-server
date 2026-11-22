package harness_test

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// TestADatabaseCreatedAfterTheLastEpochStillGetsItsBarrierWhenTheOwnerDies (BUG-0747):
// the database is created after the last membership change, so the Handoff's rf map has
// never heard of it. The owner and one copy acknowledge a write that the other copy never
// received (the owner and that copy are partitioned from each other only). The owner dies and
// that copy becomes the owner: it must catch up from the copy that holds the write before
// it answers, and must never answer "not found" for an acknowledged write.
func TestADatabaseCreatedAfterTheLastEpochStillGetsItsBarrierWhenTheOwnerDies(t *testing.T) {
	t.Run("database created after the last epoch", func(t *testing.T) { newDBBarrier(t, false, false) })
	// The control: a membership change after the creation puts the database in the
	// handoff's rf map, and the same scenario then passes.
	t.Run("database known at an earlier epoch", func(t *testing.T) { newDBBarrier(t, true, false) })
	// BUG-0758: an ordinary read at the future owner, while it is only a copy, marks the pair
	// on its first touch; the barrier must still hold it after the owner dies.
	t.Run("future owner read the pair as a copy", func(t *testing.T) { newDBBarrier(t, true, true) })
}

func newDBBarrier(t *testing.T, epochAfterCreate, readAsCopy bool) {
	c := quorumSetup(t)
	if epochAfterCreate {
		_, first := quorumIDs(t, c, 1)
		for _, id := range c.IDs() {
			if id != "n1" && !slices.Contains(first, id) {
				epoch := c.Node("n1").State().Epoch
				c.Kill(id)
				c.Await("an unrelated member to be committed down", 30*time.Second, func() bool {
					return c.Node("n1").State().Epoch > epoch
				})
				c.AwaitConverged(10 * time.Second)
				c.AwaitLeader(10 * time.Second)
				c.AwaitLeases(20 * time.Second)
				break
			}
		}
	}
	ids, copies := quorumIDs(t, c, 4)
	owner, newOwner, holder := copies[0], copies[1], copies[2]
	// A long lease bound makes the stop proof observable: the old owner is gone, so only the
	// bound proves it stopped, and a new owner that serves earlier skipped the proof.
	const bound = 6100 * time.Millisecond // step_down_ms 6000 + heartbeat_ms 100
	if err := c.Node(c.AwaitLeader(10*time.Second)).SetSetting(proofCtx(t), "step_down_ms", 6000); err != nil {
		t.Fatalf("setting step_down_ms: %v", err)
	}
	// A first write caches the bucket declaration on the owner while every copy is reachable.
	warm := ids[3]
	ids = ids[:3]
	proofInsert(t, c, owner, warm, "warm")
	c.Await("the future owner to hold the warm-up write", 20*time.Second, func() bool {
		_, found, err := c.Node(newOwner).LocalDocument(proofCtx(t), proofDB, proofBucket, warm)
		return err == nil && found
	})
	if readAsCopy {
		// Retried: right after the lease setting changes a node may briefly refuse reads.
		c.Await("an ordinary read through the future owner to succeed", 20*time.Second, func() bool {
			_, err := c.Node(newOwner).Get(proofCtx(t), proofDB, proofBucket, warm)
			return err == nil
		})
	}
	c.Partition([]string{owner}, []string{newOwner})
	for _, id := range ids {
		proofInsert(t, c, owner, id, "acknowledged")
		c.Await("the holder to hold "+id, 20*time.Second, func() bool {
			_, found, err := c.Node(holder).LocalDocument(proofCtx(t), proofDB, proofBucket, id)
			return err == nil && found
		})
	}
	if _, found, err := c.Node(newOwner).LocalDocument(proofCtx(t), proofDB, proofBucket, ids[0]); err != nil || found {
		t.Fatalf("setup: the future owner %s already holds %s (found %v, err %v); the cut did not hold", newOwner, ids[0], found, err)
	}
	epoch := c.Node(holder).State().Epoch
	c.Kill(owner)
	c.Await("the owner to be committed down", 30*time.Second, func() bool {
		return c.Node(holder).State().Epoch > epoch && c.Node(newOwner).State().Epoch > epoch
	})
	committed := time.Now()
	deadline := committed.Add(quorumFailoverBudget)
	for _, id := range ids {
		for {
			_, err := c.Node(newOwner).Get(proofCtx(t), proofDB, proofBucket, id)
			if err == nil {
				break
			}
			var refusal *cluster.Error
			if errors.As(err, &refusal) && refusal.Code == cluster.CodeNotFound {
				t.Fatalf("the new owner %s answered not-found for the acknowledged write %s: it served before catching up from %s", newOwner, id, holder)
			}
			if time.Now().After(deadline) {
				t.Fatalf("acknowledged write %s never became readable through %s: %v", id, newOwner, err)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	if took := time.Since(committed); took < bound-2*time.Second {
		t.Fatalf("the new owner %s served %s after the owner-down commit, before the %s stop bound: it skipped the stop proof", newOwner, took, bound)
	}
}
