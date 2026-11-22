package harness_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/harness"
)

func assertLocked(t *testing.T, err error, node string) {
	t.Helper()
	var refusal *cluster.Error
	if !errors.As(err, &refusal) || refusal.Code != cluster.CodeInvalidArgument {
		t.Fatalf("a locked statement answered %v, want INVALID_ARGUMENT", err)
	}
	for _, text := range []string{"cluster.allow_membership is false", "Nothing was changed", "HOARDDB_CLUSTER_ALLOW_MEMBERSHIP=1) on " + node} {
		if !strings.Contains(refusal.Message, text) {
			t.Errorf("M1 text lacks %q:\n%s", text, refusal.Message)
		}
	}
}

// TestAllowMembershipFalseRefusesEveryStatement (M1): a locked node refuses all five statements,
// first, with the key and the fix, and nothing changes.
func TestAllowMembershipFalseRefusesEveryStatement(t *testing.T) {
	c := joinCluster(t, 1, 1, func(o *harness.Options) { o.LockMembership = []string{"n1"} })
	joiner := c.OpenJoiner()
	before := c.Node("n1").State().StateSeq
	ctx := askCtx(t)
	ops := c.Membership("n1")
	_, err := ops.Join(ctx, joiner, c.Addr(joiner), false)
	assertLocked(t, err, "n1")
	_, err = ops.Promote(ctx, joiner)
	assertLocked(t, err, "n1")
	_, err = ops.Leave(ctx, joiner)
	assertLocked(t, err, "n1")
	_, err = ops.RemoveNode(ctx, joiner)
	assertLocked(t, err, "n1")
	_, err = ops.ReplaceNode(ctx, joiner, c.Addr(joiner), false)
	assertLocked(t, err, "n1")
	if after := c.Node("n1").State().StateSeq; after != before {
		t.Fatalf("a locked node changed the state: state_seq %d -> %d", before, after)
	}
}

// lockedLeaderCluster is a two-member cluster in which n1, the locked member, leads. Leadership
// after the first election is the library's to decide, so a cluster whose leader is n2 is dropped
// and built again; a test that skipped instead would let the named break survive its run.
func lockedLeaderCluster(t *testing.T) *harness.Cluster {
	t.Helper()
	const attempts = 8
	for range attempts {
		c := joinCluster(t, 2, 1, func(o *harness.Options) { o.LockMembership = []string{"n1"} })
		if c.AwaitLeader(10*time.Second) == "n1" {
			return c
		}
	}
	t.Fatalf("n1 did not lead in %d fresh clusters, so the leader's own lock cannot be tested", attempts)
	return nil
}

// TestAllowMembershipIsCheckedOnTheLeaderToo (M1): a member whose key is true forwards to a leader
// whose key is false, and the leader refuses, naming itself. A forward cannot bypass the lock.
// [BREAK: check the lock at the entry node only].
func TestAllowMembershipIsCheckedOnTheLeaderToo(t *testing.T) {
	c := lockedLeaderCluster(t)
	joiner := c.OpenJoiner()
	_, err := c.Membership("n2").Join(askCtx(t), joiner, c.Addr(joiner), false)
	assertLocked(t, err, "n1")
	if _, ok := memberOf(c.Node("n1").State(), joiner); ok {
		t.Fatal("the locked leader registered the node")
	}
}
