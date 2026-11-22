package harness_test

import (
	"context"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster/harness"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
)

// restartHoldFast is far below the 2.1 s stop bound at the defaults: a pair served inside it
// was not held by it.
const restartHoldFast = 1500 * time.Millisecond

// TestAFreshClusterServesItsFirstWriteWithoutAHold guards BUG-0743's fix against
// over-reach: a first boot is not a restart, so its first write and read are not held for
// the stop bound. This must pass before the fix too.
func TestAFreshClusterServesItsFirstWriteWithoutAHold(t *testing.T) {
	for _, nodes := range []int{1, 3} {
		t.Run(map[int]string{1: "one node", 3: "three nodes"}[nodes], func(t *testing.T) {
			c := harness.NewCluster(t, nodes, harness.Options{})
			leader := c.Node(c.AwaitLeader(10 * time.Second))
			if _, err := leader.CreateDatabaseRF(proofCtx(t), proofDB, min(nodes, 2)); err != nil {
				t.Fatalf("creating the database: %v", err)
			}
			c.AwaitConverged(10 * time.Second)
			if _, err := leader.CreateBucket(proofCtx(t), proofDB, proofBucket, wire.Field{Name: "name", Type: "string"}); err != nil {
				t.Fatalf("creating the bucket: %v", err)
			}
			began := time.Now()
			proofInsert(t, c, c.IDs()[0], "fresh-1", "first")
			if _, err := c.Node(c.IDs()[0]).Get(proofCtx(t), proofDB, proofBucket, "fresh-1"); err != nil {
				t.Fatalf("the first read of a fresh cluster: %v", err)
			}
			if took := time.Since(began); took > restartHoldFast {
				t.Fatalf("the first write and read of a fresh cluster took %s; a first boot must not be held", took)
			}
		})
	}
}

// TestASingleNodeRestartServesWithoutTheBound: one member has no other owner, so a restart
// waits for no stop bound (it still takes one catch-up step).
func TestASingleNodeRestartServesWithoutTheBound(t *testing.T) {
	c := harness.NewCluster(t, 1, harness.Options{})
	node := c.IDs()[0]
	leader := c.Node(c.AwaitLeader(10 * time.Second))
	if _, err := leader.CreateDatabaseRF(proofCtx(t), proofDB, 1); err != nil {
		t.Fatalf("creating the database: %v", err)
	}
	c.AwaitConverged(10 * time.Second)
	if _, err := leader.CreateBucket(proofCtx(t), proofDB, proofBucket, wire.Field{Name: "name", Type: "string"}); err != nil {
		t.Fatalf("creating the bucket: %v", err)
	}
	proofInsert(t, c, node, "solo-1", "kept")
	began := time.Now()
	c.Kill(node)
	c.Restart(node)
	for {
		_, err := c.Node(node).Get(proofCtx(t), proofDB, proofBucket, "solo-1")
		// A held read parks until the pair opens, so the answer's time is the test.
		if took := time.Since(began); took > restartHoldFast {
			t.Fatalf("a restarted single node served its document only after %s (err %v): it waited for a stop bound nobody can owe", took, err)
		}
		if err == nil {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestARestartedOwnerHoldsAReadForwardedByAnotherNodeThenAnswersIt: the first touch of a pair
// after a restart can be a read another node forwarded. It must open the restart hold, so the
// forwarded read is not answered inside the hold (it parks at the owner until the caller's
// deadline) and is answered once the hold ends. Without the Acquire in the forwarded-read
// handler the pair stays unmarked and the read is refused "database not in this node's
// committed state" for as long as nothing touches the pair locally. step_down_ms is widened so the hold outlasts the restart.
func TestARestartedOwnerHoldsAReadForwardedByAnotherNodeThenAnswersIt(t *testing.T) {
	c := proofSetup(t, harness.Options{NoDetection: true})
	if err := c.Node(c.AwaitLeader(10*time.Second)).SetSetting(proofCtx(t), "step_down_ms", 6000); err != nil {
		t.Fatalf("setting step_down_ms: %v", err)
	}
	id, owner, _, outsider := proofID(t, c, "fwd")
	proofInsert(t, c, owner, id, "kept")
	c.Kill(owner)
	booted := time.Now()
	c.Restart(owner)
	if time.Since(booted) > 3*time.Second {
		t.Skipf("the restart took %s, too close to the 6.1 s hold (step_down_ms 6000 + heartbeat_ms 100) to see it", time.Since(booted))
	}
	short, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	_, err := c.Node(outsider).Get(short, proofDB, proofBucket, id)
	if err == nil {
		t.Fatalf("a read forwarded to the restarted owner %s was answered %s after boot, inside its hold", owner, time.Since(booted))
	}
	deadline := time.Now().Add(quorumFailoverBudget)
	for {
		_, err := c.Node(outsider).Get(proofCtx(t), proofDB, proofBucket, id)
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("a read through %s was still refused %s after %s restarted: %v", outsider, quorumFailoverBudget, owner, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TestARestartedNodeOpensEveryPairItOwnsInBoundedTime: a list touches every pair the node owns
// (about a third of the 8192 slots at three members), so it is served only when every restart
// hold has ended. The time from the restart to that list is logged: it is the stop bound plus
// the catch-up of thousands of pairs, spread over several supervisor ticks.
func TestARestartedNodeOpensEveryPairItOwnsInBoundedTime(t *testing.T) {
	c := proofSetup(t, harness.Options{NoDetection: true})
	id, owner, _, _ := proofID(t, c, "all")
	proofInsert(t, c, owner, id, "kept")
	c.Kill(owner)
	booted := time.Now()
	c.Restart(owner)
	restarted := time.Since(booted)
	for {
		_, err := c.Node(owner).List(proofCtx(t), proofDB, proofBucket, nil, 1)
		if err == nil {
			break
		}
		if time.Since(booted) > quorumFailoverBudget {
			t.Fatalf("%s still could not list %s after its restart: %v", owner, quorumFailoverBudget, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Logf("restart returned after %s; every owned pair was open %s after boot", restarted, time.Since(booted))
}

// TestARestartHoldFollowsAStepDownCommittedAfterTheLastEpoch is BUG-0813's failing test: the
// restart hold is step_down_ms + heartbeat_ms counted from boot, using the step_down_ms that
// was committed. SetSetting commits no epoch, so the hold kept the 2000 ms default (2.1 s)
// although 12000 was committed on every node before the kill.
func TestARestartHoldFollowsAStepDownCommittedAfterTheLastEpoch(t *testing.T) {
	const stepDown = 12 * time.Second
	c := proofSetup(t, harness.Options{NoDetection: true})
	if err := c.Node(c.AwaitLeader(10*time.Second)).SetSetting(proofCtx(t), "step_down_ms", stepDown.Milliseconds()); err != nil {
		t.Fatalf("setting step_down_ms: %v", err)
	}
	time.Sleep(time.Second)
	id, owner, _, _ := proofID(t, c, "bug0813")
	// Raising step_down_ms makes every node's lease start over under the new value, so the
	// first inserts can still be refused 'lease lapsed' or 'no majority': wait, then retry.
	c.AwaitLeases(stepDown + 10*time.Second)
	if _, acked := proofInsertRetrying(t, c, owner, id); !acked {
		t.Fatalf("the insert before the kill was never acknowledged (background: %v)", c.BackgroundErrors())
	}
	c.Kill(owner)
	booted := time.Now()
	c.Restart(owner)
	restarted := time.Since(booted)
	// A lone read at the owner parks until the pair opens, so its answer's time is the hold. A
	// read that reaches the pair before the node decided its hold (its log or the latest state
	// not yet fed to the handoff) is refused with a retry instead; ask again until one is served.
	deadline := time.Now().Add(30 * time.Second)
	for {
		_, err := c.Node(owner).Get(proofCtx(t), proofDB, proofBucket, id)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the read at the restarted owner was still refused 30s after the restart: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	held := time.Since(booted)
	if held < stepDown {
		t.Fatalf("the restarted owner served %s after boot, inside the committed step_down_ms of %s", held, stepDown)
	}
	// The hold ends at step_down_ms + heartbeat_ms (the harness runs 100 ms) after the process
	// start, which is inside the restart; add the restart itself and 5 s for the catch-up to the
	// copies and the 50 ms retry step. A hold that never ends, or that is a second long, fails.
	if limit := stepDown + 100*time.Millisecond + restarted + 5*time.Second; held > limit {
		t.Fatalf("the restarted owner served %s after boot, past step_down_ms 12s + heartbeat_ms 100ms + the %s restart + 5s: the hold lasts longer than the committed value", held, restarted)
	}
}
