package harness_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/harness"
)

func TestAJoinerBelowTheClusterVersionIsRefusedWithTheExactTextAndNothingIsCommitted(t *testing.T) {
	// The levels are stubbed one above the build's, so the test holds at the first release, where
	// StateLevel is 1 and no level lies below it: the cluster runs StateLevel+1 and the joiner
	// declares StateLevel.
	level := cluster.StateLevel + 1
	below := cluster.StateLevel
	c := joinCluster(t, 1, 1, func(o *harness.Options) {
		o.StateRanges = map[string]cluster.StateRange{
			"n1": {Min: level, Max: level, Writes: level},
			"n2": {Min: below, Max: below, Writes: below},
		}
	})
	joiner := c.OpenJoiner()
	before := c.Node("n1").State()
	_, err := joinAt(t, c, "n1", joiner, false)
	var refusal *cluster.Error
	if !errors.As(err, &refusal) {
		t.Fatalf("a joiner below the cluster version: %v", err)
	}
	want := fmt.Sprintf("cannot join %s@%s: node %s runs build devel, which supports cluster versions %d to %d, and this cluster runs version %d. "+
		"This build records no release for version %d: run hoarddb-server -v on a cluster member and start %s on that build or later, then run 'join %s@%s' again.",
		joiner, c.Addr(joiner), joiner, below, below, level, level, joiner, joiner, c.Addr(joiner))
	if refusal.Message != want {
		t.Fatalf("the text is\n%s\nwant\n%s", refusal.Message, want)
	}
	after := c.Node("n1").State()
	if _, ok := memberOf(after, joiner); ok || after.StateSeq != before.StateSeq || len(after.Members) != len(before.Members) {
		t.Fatalf("a refused join changed the cluster: seq %d to %d, members %d to %d", before.StateSeq, after.StateSeq, len(before.Members), len(after.Members))
	}
}

func TestAJoinerAboveTheClusterVersionIsRefusedInPhaseTwo(t *testing.T) {
	// The cluster runs this build's level (n1 declares the build range); the joiner declares the one above.
	level := cluster.StateLevel
	above := level + 1
	c := joinCluster(t, 1, 1, func(o *harness.Options) {
		o.StateRanges = map[string]cluster.StateRange{"n2": {Min: above, Max: above, Writes: above}}
	})
	joiner := c.OpenJoiner()
	_, err := joinAt(t, c, "n1", joiner, false)
	// The build's release table records level 1 as first released in 1.1, so the text names it.
	release := cluster.ReleaseOf(level)
	if release == "" {
		t.Fatalf("level %d records no release: the first release's row names it", level)
	}
	want := fmt.Sprintf("cannot join %s@%s: node %s runs build devel, which supports cluster versions %d to %d and is newer than this cluster, and this cluster runs version %d (first released in %s). "+
		"A newer build cannot join an older cluster until the cluster is upgraded. Start %s on %s, the release that carries version %d, or upgrade every member to %s's build first, then run 'join %s@%s' again.",
		joiner, c.Addr(joiner), joiner, above, above, level, release, joiner, release, level, joiner, joiner, c.Addr(joiner))
	if err == nil || err.Error() != want {
		t.Fatalf("a newer joiner before the cluster version gate exists:\n%v\nwant\n%s", err, want)
	}
}

func TestALevelZeroJoinerIsRefusedByName(t *testing.T) {
	c := joinCluster(t, 1, 1, func(o *harness.Options) {
		o.StateRanges = map[string]cluster.StateRange{"n2": {}}
	})
	joiner := c.OpenJoiner()
	_, err := joinAt(t, c, "n1", joiner, false)
	want := fmt.Sprintf("which sends no state version, and this cluster runs version %d", cluster.StateLevel)
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("a joiner that declares no range: %v", err)
	}
	if _, ok := memberOf(c.Node("n1").State(), joiner); ok {
		t.Fatal("the refused join registered the node")
	}
}

func TestAJoinOnAKnownJoiningRowAlsoRunsTheRangeCheck(t *testing.T) {
	c := joinCluster(t, 1, 1, nil)
	joiner := c.OpenJoiner()
	if err := c.Node("n1").JoinRegisterOnly(askCtx(t), joiner, c.Addr(joiner)); err != nil {
		t.Fatal(err)
	}
	row, ok := memberOf(c.Node("n1").State(), joiner)
	if !ok || row.State != cluster.MemberJoining {
		t.Fatalf("the registration left %+v", row)
	}
	c.Kill(joiner)
	c.SetStateRange(joiner, cluster.StateRange{Min: 0, Max: 0, Writes: 0})
	c.RestartUnsynced(joiner)
	_, err := joinAt(t, c, "n1", joiner, false)
	if err == nil || !strings.Contains(err.Error(), "which sends no state version") {
		t.Fatalf("a joining row whose node is now below the cluster version: %v", err)
	}
	if got, _ := memberOf(c.Node("n1").State(), joiner); got.State != cluster.MemberJoining {
		t.Fatalf("the refused retry finished the join: %+v", got)
	}
}

// TestAReturningMemberBelowTheClusterVersionIsRefusedAtConnection: the ACCEPTOR refuses a
// returning member that writes below the cluster version. The returning node must get past its
// own open check, which judges the directory's version V against Min..Max (4.4), so its range
// still contains V (Max is the cluster's level) while what it writes is the level below: no
// range that passes the open check can lie wholly below V, and Writes is the part the acceptors
// judge (their Min exceeds it).
func TestAReturningMemberBelowTheClusterVersionIsRefusedAtConnection(t *testing.T) {
	// The members' levels are stubbed one above the build's (as the joiner test above), so a level
	// below the cluster's exists at the first release too.
	level := cluster.StateLevel + 1
	below := cluster.StateLevel
	members := cluster.StateRange{Min: level, Max: level, Writes: level}
	c := harness.NewCluster(t, 3, harness.Options{Voters: 3, NoDetection: true,
		StateRanges: map[string]cluster.StateRange{"n1": members, "n2": members, "n3": members}})
	c.AwaitConverged(15 * time.Second)
	c.Kill("n3")
	stub := cluster.StateRange{Min: below, Max: level, Writes: below}
	c.SetStateRange("n3", stub)
	c.RestartUnsynced("n3")
	deadline := time.Now().Add(20 * time.Second)
	for {
		for _, id := range []string{"n1", "n2"} {
			for _, row := range c.Node(id).RefusedPeers() {
				if row.NodeID == "n3" && row.Range == stub {
					// Spec 9.4 text 1, the range form. The acceptor's WARN carries the same text
					// (cluster TestTheRaftAcceptorLogsARangeRefusalOncePerMinute).
					want := fmt.Sprintf(`refused %s connection from node "n3": it speaks state versions %d-%d (writes %d) and this cluster runs version %d`,
						row.Channel, below, level, below, level)
					if text := cluster.StateRangeRefusal(row.Channel, row.NodeID, members, row.Range); !strings.Contains(text, want) || !strings.Contains(text, "which it cannot act at") {
						t.Fatalf("the refusal of n3 is\n%s\nwant it to say\n%s ... which it cannot act at", text, want)
					}
					return
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("no member refused the returning node n3 for its state range")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestARetriedJoinOnAKnownJoiningRowAfterASameBuildRestartFinishes: a registered joiner that
// restarted holds Raft state and has not yet applied the bootstrap entry, so it names no cluster;
// the retry must finish the join, not tell the operator it bootstrapped a cluster of its own.
// Scratch proof: put the HoldsRaftState case back on the joining-row path and this fails with
// "it has bootstrapped a cluster of its own".
func TestARetriedJoinOnAKnownJoiningRowAfterASameBuildRestartFinishes(t *testing.T) {
	c := joinCluster(t, 1, 1, nil)
	joiner := c.OpenJoiner()
	if err := c.Node("n1").JoinRegisterOnly(askCtx(t), joiner, c.Addr(joiner)); err != nil {
		t.Fatal(err)
	}
	c.Kill(joiner)
	c.RestartUnsynced(joiner)
	if _, err := joinAt(t, c, "n1", joiner, false); err != nil {
		t.Fatalf("the retried join: %v", err)
	}
	row, _ := memberOf(c.Node("n1").State(), joiner)
	if row.State != cluster.MemberActive {
		t.Fatalf("after the retry the row is %+v, want active", row)
	}
}

// TestAJoinCarriesTheJoinersRangeIntoTheRegistration (cluster-version spec 4.2, finding M3): a
// leader that runs versions 1 to 2 bootstraps its cluster at 2, and a joiner that runs version 1
// only passes the Phase 2 overlap check (1 to 2 against 1 to 1) but not the registration, which
// carries the joiner's own range: text F8, and no member row. Against: REGISTER_MEMBER filled
// from the leader's range (Raft.RegisterJoiner's zero fallback), which registers a joining
// member that can never run the cluster's version. Proved red by removing
// `Range: identity.Range` from wire.Node.runJoin.
func TestAJoinCarriesTheJoinersRangeIntoTheRegistration(t *testing.T) {
	c := joinCluster(t, 1, 1, func(o *harness.Options) {
		o.StateRanges = map[string]cluster.StateRange{"n1": {Min: 1, Max: 2, Writes: 1}, "n2": {Min: 1, Max: 1, Writes: 1}}
	})
	if got := c.Node("n1").State().ClusterVersion; got != 2 {
		t.Fatalf("the leader bootstrapped at version %d, want 2 (its Max)", got)
	}
	joiner := c.OpenJoiner()
	_, err := joinAt(t, c, "n1", joiner, false)
	if err == nil || !strings.Contains(err.Error(), "the cluster moved to version 2 while the join was checked") {
		t.Fatalf("a joiner that runs version 1 only, at a cluster at 2: %v", err)
	}
	if _, ok := memberOf(c.Node("n1").State(), joiner); ok {
		t.Fatal("the refused join registered the node")
	}
}
