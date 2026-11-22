package server

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/transport"
)

// Join-freeze build step 9: the refusal's text, 'nodes' and the gauge
// (join-freeze spec §6.2, §7.1, §7.2, review F-5, F-7).

// downMemberDuringJoin is a join in which node 3 settles and then stops while
// nodes 1 and 2 are still handing off: their hand-offs wait on node 3.
type downMemberDuringJoin struct {
	*settlingCluster
	joinEpoch uint64
	restart   func() *testNode
}

func setUpDownMemberDuringJoin(t *testing.T) *downMemberDuringJoin {
	t.Helper()
	s := &downMemberDuringJoin{settlingCluster: startSettlingCluster(t)}
	held := []string{s.tc.addrs[0], s.tc.addrs[1]}
	gate := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(gate) }) }
	setAskNodeToMigrateHook(t, func(from, _, _, _ string) error {
		if slices.Contains(held, from) {
			<-gate
		}
		return nil
	})
	t.Cleanup(release)
	s.joinEpoch = s.join(t, 0)
	waitSettled(t, clusterOf(s.tc, 2), s.joinEpoch)
	s.restart = stopNode(t, s.tc, 2)
	release()

	first := clusterOf(s.tc, 0)
	// Nodes 1 and 2 must each fail a hand-off to node 3, node 1 must find node 3
	// down and hear node 2's progress: a few gossip rounds, longer than
	// pollUntil's bound under -race on four cores.
	waitsOnDown := func() bool {
		wakeAll(s.members())
		report := first.joinFreeze.Load()
		if !report.Paused || !slices.Equal(report.Stalled, held) {
			return false
		}
		for _, member := range report.WaitingOn {
			if member.Node == s.tc.addrs[2] && member.Down {
				return true
			}
		}
		return false
	}
	for deadline := time.Now().Add(time.Minute); !waitsOnDown(); time.Sleep(20 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("node 1's report never waited on the stopped node 3 behind nodes 1 and 2: %+v", *first.joinFreeze.Load())
		}
	}
	return s
}

// TestTheRefusalNamesTheMemberTheHandOffsWaitOn is review F-5: node 3 settled
// the join and stopped; nodes 1 and 2 cannot finish their hand-offs without
// its answer. A create through node 1 is refused naming the joiner, the
// progress, node 3 as unreachable, nodes 1 and 2 as waiting on it, and the ways
// out with their cost; never a create. Guard: name only members whose own
// Settled is low — node 3 had settled, and is not named.
func TestTheRefusalNamesTheMemberTheHandOffsWaitOn(t *testing.T) {
	s := setUpDownMemberDuringJoin(t)
	defer s.tc.stop()
	joiner, down := s.ports.ringAddr, s.tc.addrs[2]
	refused := refusalOf(t, s.tc.frame(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: "while_down", Type: BucketTypeHash}))
	if refused.Code != int(transport.ErrUnavailable) || refused.WriteOutcome != outcomeNotApplied {
		t.Errorf("code %d outcome %q, want UNAVAILABLE, not_applied", refused.Code, refused.WriteOutcome)
	}
	message := refused.Message
	t.Logf("the refusal:\n%s", message)
	for _, want := range []string{
		"while " + joiner + " joins",
		" of ",
		down + ", unreachable for",
		s.tc.addrs[0] + " and " + s.tc.addrs[1] + " cannot finish handing off until " + down,
		"start " + down + " again",
		"'remove-node " + joiner + "'",
		"'remove-node " + down + "'",
		"only copy is on " + down,
		"new, empty bucket",
		"'nodes'",
	} {
		if !strings.Contains(message, want) {
			t.Errorf("the refusal does not say %q:\n%s", want, message)
		}
	}
	if strings.Contains(message, "'create") {
		t.Errorf("the refusal advises a create, but the data exists:\n%s", message)
	}
}

// TestAPausedJoinLiftsWhenTheDownMemberReturns: once node 3 answers again the
// holders finish their hand-offs and every node lifts the pause.
func TestAPausedJoinLiftsWhenTheDownMemberReturns(t *testing.T) {
	s := setUpDownMemberDuringJoin(t)
	defer s.tc.stop()
	s.restart()
	pollUntil(t, "every node lifts the pause once node 3 is back", func() bool {
		wakeAll(s.members())
		for _, member := range s.members() {
			if report := member.joinFreeze.Load(); report.Paused {
				return false
			}
		}
		return true
	})
}

// TestRemovingTheJoinerAfterABucketWasReleasedKeepsEveryDocument: four members
// at factor 3 and a joiner; one bucket's hand-off completes, so a holder the
// join moved out of its set releases it, while every other hand-off to the
// joiner is held. Undoing the join with 'remove-node' keeps every document of
// every bucket readable through every member, and the pause lifts once the
// members have handed back what the join moved (spec §6.3, way out 3).
// Guard: release on a partial confirmation.
func TestRemovingTheJoinerAfterABucketWasReleasedKeepsEveryDocument(t *testing.T) {
	tc := newTestCluster(t, 4, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	s := &settlingCluster{tc: tc, ports: reserveNodePorts(t)}
	joined := s.joinedRing(t)
	joiner := s.ports.ringAddr
	released := bucketNamed(t, "released", func(name string) bool {
		set, err := joined.Replicas(placementOf(testDatabase, name).encoded, 3)
		return err == nil && containsID(nodeIDs(set), joiner)
	})
	names := []string{released}
	for index := 0; index < 8; index++ {
		names = append(names, fmt.Sprintf("kept%03d", index))
	}
	for _, name := range names {
		tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: name, Type: BucketTypeHash})
		tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: name, Key: "doc", Data: map[string]interface{}{"name": name}})
		waitForServing(t, tc, testDatabase, name, 3)
	}
	// Every other hand-off to the joiner fails, rather than waits: a sweep that
	// waited on one would never reach the released bucket.
	var holding atomic.Bool
	holding.Store(true)
	release := func() { holding.Store(false) }
	setAskNodeToMigrateHook(t, func(_, memberID, _, bucket string) error {
		if holding.Load() && memberID == joiner && bucket != released {
			return errSeamFailsHandOff
		}
		return nil
	})
	joinEpoch := s.join(t, 0)
	pollUntil(t, "a holder outside the released bucket's new set releases it", func() bool {
		holders := 0
		for _, node := range tc.nodes {
			if node.server.core.bucketStoreOpen(testDatabase, released) {
				holders++
			}
		}
		return holders == 2
	})

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := clusterOf(tc, 0).proposeRemoval(ctx, joiner); err != nil {
		t.Fatalf("proposeRemoval of the joiner: %v", err)
	}
	release()
	members := make([]*Cluster, 0, len(tc.nodes))
	for index := range tc.nodes {
		members = append(members, clusterOf(tc, index))
	}
	pollUntil(t, "every member lifts the pause once the removal is handed back", func() bool {
		wakeAll(members)
		for _, member := range members {
			if report := member.joinFreeze.Load(); report.Paused || report.Epoch != joinEpoch+1 {
				return false
			}
		}
		return true
	})
	for _, name := range names {
		for index := range tc.nodes {
			frame := tc.frame(index, transport.OpGet, FindRequest{Database: testDatabase, Bucket: name, Key: "doc"})
			if frame.Opcode == transport.OpError {
				t.Errorf("reading %s through node %d after the joiner was removed: %s", name, index+1, string(frame.Payload))
				continue
			}
			if !strings.Contains(string(frame.Payload), name) {
				t.Errorf("reading %s through node %d after the joiner was removed found no document", name, index+1)
			}
		}
	}
}

// TestARemovedJoinersPauseSaysTheRemovalIsSettling is the verifier's probe: a
// joiner removed before its join settled keeps the pause until the members
// have handed back what the join moved, and the refusal said it was paused
// "while <J> joins" and offered 'remove-node <J>' for a node that is gone. It
// now says the members are settling the removal of J.
func TestARemovedJoinersPauseSaysTheRemovalIsSettling(t *testing.T) {
	s := startSettlingCluster(t)
	defer s.tc.stop()
	var holding atomic.Bool
	holding.Store(true)
	setAskNodeToMigrateHook(t, func(_, _, _, _ string) error {
		if holding.Load() {
			return errSeamFailsHandOff
		}
		return nil
	})
	defer holding.Store(false)
	s.join(t, 0)
	joiner := s.ports.ringAddr
	proposeRemovalVia(t, clusterOf(s.tc, 0), joiner)
	member := clusterOf(s.tc, 1)
	pollUntil(t, "node 2 adopts the removal and stays paused", func() bool {
		report := member.joinFreeze.Load()
		return report.Epoch == member.Epoch() && report.Paused && !nodeListed(member.Members(), joiner)
	})
	refused := refusalOf(t, s.tc.frame(1, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: "after_removal", Type: BucketTypeHash}))
	t.Logf("the refusal:\n%s", refused.Message)
	if strings.Contains(refused.Message, "while "+joiner+" joins") {
		t.Errorf("the refusal says the removed node %s is joining:\n%s", joiner, refused.Message)
	}
	if strings.Contains(refused.Message, "'remove-node "+joiner+"'") {
		t.Errorf("the refusal offers to remove the node %s that was removed:\n%s", joiner, refused.Message)
	}
	if !strings.Contains(refused.Message, "removal of "+joiner) {
		t.Errorf("the refusal does not say the members are settling the removal of %s:\n%s", joiner, refused.Message)
	}
}

// TestTheRefusalSaysWhenTheMembershipChangedMidRequest: a schema change judged
// at an epoch the report is not for is refused UNAVAILABLE with its own text.
// Guard: answer the freeze's text.
func TestTheRefusalSaysWhenTheMembershipChangedMidRequest(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	c := clusterOf(tc, 0)
	err := c.refuseSchemaChange(transport.OpCreateBucket, c.Epoch()+1)
	if err == nil {
		t.Fatal("a schema change judged at another epoch than the report's was allowed")
	}
	refused := refusalOf(t, errorFrame(transport.NewFrame(transport.OpCreateBucket, nil), err))
	if refused.Code != int(transport.ErrUnavailable) {
		t.Errorf("code %d, want UNAVAILABLE", refused.Code)
	}
	if !strings.Contains(refused.Message, "membership changed while this request was in flight") || !strings.Contains(refused.Message, "Retry") {
		t.Errorf("the refusal does not say the membership changed mid-request: %q", refused.Message)
	}
}

// TestTheJoiningGaugeCountsUnsettledJoiners: hoarddb_cluster_members_joining is
// 1 while a join is unsettled and 0 once it has settled.
func TestTheJoiningGaugeCountsUnsettledJoiners(t *testing.T) {
	s := setUpSettlingJoin(t)
	defer s.tc.stop()
	member := clusterOf(s.tc, 1)
	if joining := joiningGauge(t, member); joining != 1 {
		t.Fatalf("hoarddb_cluster_members_joining reads %v during the join, want 1", joining)
	}
	s.release()
	pollUntil(t, "the gauge reads 0 once the join settles", func() bool {
		wakeAll(s.members())
		return joiningGauge(t, member) == 0
	})
}

// joiningGauge reads hoarddb_cluster_members_joining from c's registry.
func joiningGauge(t *testing.T, c *Cluster) float64 {
	t.Helper()
	families, err := c.metrics.Gather()
	if err != nil {
		t.Fatalf("gathering the replication metrics: %v", err)
	}
	for _, family := range families {
		if family.GetName() == "hoarddb_cluster_members_joining" {
			return family.GetMetric()[0].GetGauge().GetValue()
		}
	}
	t.Fatal("no hoarddb_cluster_members_joining gauge is registered")
	return 0
}

// TestTheProgressFigureIsAFloor: the refusal prints how many buckets have been
// copied as "at least n", never "n of m" — a holder's total is known only once
// its pass has walked its catalog, so a sum of totals can fall below the
// copies made — and prints no figure while nothing has been. Guard: print
// "%d of %d".
func TestTheProgressFigureIsAFloor(t *testing.T) {
	cases := []struct {
		copied, total int
		want          string
	}{
		{copied: 1, total: 1, want: ": at least 1 bucket of this membership change has been copied to their new places."},
		{copied: 4, total: 3, want: ": at least 4 buckets of this membership change have been copied to their new places."},
		{copied: 0, total: 9, want: "Schema changes are paused while n4 joins the cluster. Nothing was created"},
	}
	for _, tc := range cases {
		report := &joinFreezeReport{Paused: true, Joiners: []string{"n4"}, Joining: []string{"n4"}, Copied: tc.copied, Total: tc.total}
		refused := report.refusal()
		if !strings.Contains(refused, tc.want) {
			t.Errorf("copied %d, total %d: the refusal does not say %q:\n%s", tc.copied, tc.total, tc.want, refused)
		}
		if strings.Contains(refused, fmt.Sprintf(" of %d", tc.total)) {
			t.Errorf("copied %d, total %d: the refusal prints a total:\n%s", tc.copied, tc.total, refused)
		}
	}
}
