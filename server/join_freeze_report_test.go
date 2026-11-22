package server

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/ring"
)

// Join-freeze build step 7: joinedAt, and the published report (join-freeze
// spec §3.2, §8.3, §8.4, review F-1, F-15).

// holdEveryHandOff holds every hand-off a sweep asks for until the returned
// release is called; it is also called when the test ends.
func holdEveryHandOff(t *testing.T) (release func()) {
	t.Helper()
	gate := make(chan struct{})
	var once sync.Once
	release = func() { once.Do(func() { close(gate) }) }
	setAskNodeToMigrateHook(t, func(_, _, _, _ string) error {
		<-gate
		return nil
	})
	t.Cleanup(release)
	return release
}

// pausedReport returns c's published report, failing unless it is paused at
// epoch.
func pausedReport(t *testing.T, c *Cluster, epoch uint64, who string) *joinFreezeReport {
	t.Helper()
	report := c.joinFreeze.Load()
	if report == nil {
		t.Fatalf("%s publishes no report", who)
	}
	if report.Epoch != epoch || !report.Paused {
		t.Fatalf("%s's report is %+v; want it paused at epoch %d", who, *report, epoch)
	}
	return report
}

// TestAJoinerBootedFromSeedsIsPausedAfterItsJoin is review F-1: a joiner booted
// the documented way already lists every member at its first epoch, so a diff
// of member sets finds nobody new; at the adoption that confirms it, it takes
// every member as joined at the join epoch, and its own report is paused.
// Guard: derive joinedAt by the local diff only.
func TestAJoinerBootedFromSeedsIsPausedAfterItsJoin(t *testing.T) {
	s := startSettlingCluster(t)
	defer s.tc.stop()
	release := holdEveryHandOff(t)
	defer release()
	joinEpoch := s.join(t, 0)
	joiner := s.joiner.server.core.cluster

	joinedAt := joiner.joinedAtSnapshot()
	for _, member := range joiner.Members() {
		if joinedAt[member.ID] != joinEpoch {
			t.Errorf("the joiner records %s as joined at %d, want the join epoch %d", member.ID, joinedAt[member.ID], joinEpoch)
		}
	}
	pausedReport(t, joiner, joinEpoch, "the joiner")
}

// TestAReenteringNodeIsPausedOnItsOwnView: a node that catches up through an
// epoch that did not list it and ends a member again is joining again; it
// takes every member as joined at the epoch it ends on. Guard: drop the
// re-entering case.
func TestAReenteringNodeIsPausedOnItsOwnView(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	reentering := clusterOf(tc, 0)
	founders := reentering.Members()
	var without []ring.Node
	for _, member := range founders {
		if member.ID != reentering.self.ID {
			without = append(without, member)
		}
	}
	// A member's answer: epoch 2 removed node 1, epoch 3 added it back.
	answer := peerGossip{records: []MembershipRecord{{Epoch: InitialEpoch + 1, Members: nodesToPeers(without)}}, answered: true}
	final := InitialEpoch + 2
	if err := reentering.reconcileMembershipWith(tc.addrs[1], nodesToPeers(founders), memberIDsOf(founders), final, answer); err != nil {
		t.Fatalf("catching node 1 up through its removal and return: %v", err)
	}
	if epoch := reentering.Epoch(); epoch != final {
		t.Fatalf("test setup: node 1 is at epoch %d, want %d", epoch, final)
	}
	joinedAt := reentering.joinedAtSnapshot()
	for _, member := range founders {
		if joinedAt[member.ID] != final {
			t.Errorf("node 1 records %s as joined at %d, want %d, the epoch it re-entered at", member.ID, joinedAt[member.ID], final)
		}
	}
	pausedReport(t, reentering, final, "node 1")
}

// TestAJoinPausesSchemaChangesAtAdoptionWithoutGossip: the report a member
// publishes the moment it adopts a join is already paused, from its own state
// alone (spec §4.1). Guard: recompute only from gossip.
func TestAJoinPausesSchemaChangesAtAdoptionWithoutGossip(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	member := clusterOf(tc, 0)
	for index := 1; index < len(tc.nodes); index++ {
		defer freezePeer(peerOf(t, tc, 0, index))()
	}
	newcomer := ring.Node{ID: fabricatedMemberAddr(t)}
	newcomer.Addr = newcomer.ID
	joinEpoch := member.Epoch() + 1
	if err := member.adoptMembership(joinEpoch, append(member.Members(), newcomer)); err != nil {
		t.Fatalf("adopting the join: %v", err)
	}
	report := pausedReport(t, member, joinEpoch, "node 1")
	if len(report.Joiners) != 1 || report.Joiners[0] != newcomer.ID {
		t.Errorf("the report names joiners %v, want [%s]", report.Joiners, newcomer.ID)
	}
}

// TestABootingClusterNodeHasAReportBeforeStart is review F-15: the listeners
// open before Cluster.Start, so the first report is made in NewCluster, for
// the node's epoch. Guard: compute the first report in Start.
func TestABootingClusterNodeHasAReportBeforeStart(t *testing.T) {
	ports := reserveNodePorts(t)
	dir := t.TempDir()
	if err := SaveClusterToken(dir, ClusterToken{Token: "booting-token", CreatedAt: time.Now(), ExpiresAt: time.Now().Add(time.Hour)}); err != nil {
		t.Fatalf("save cluster token: %v", err)
	}
	cfg := config.DefaultConfig()
	cfg.Storage.DataDir = dir
	cfg.Server.Listen = ports.ringAddr
	cfg.Server.NodeID = ports.ringAddr
	cfg.HashRing.SeedNodes = []string{ports.ringAddr, fabricatedMemberAddr(t)}
	cfg.HashRing.SeedFingerprints = preGeneratedNodeFingerprints(t, []string{dir})
	cfg.Metrics.Enabled = false
	cfg.Audit.Enabled = false
	cfg.TCP.Listen = ports.driverAddr
	srv, err := NewService(cfg, quietLogger())
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	defer func() { _ = srv.Stop() }()
	booting := srv.core.cluster
	report := booting.joinFreeze.Load()
	if report == nil {
		t.Fatal("a cluster node that has not started has no report: it would refuse every schema change")
	}
	if report.Epoch != booting.Epoch() {
		t.Fatalf("the first report is for epoch %d, the node is at %d", report.Epoch, booting.Epoch())
	}
}

// TestJoinedAtSurvivesARestartAndIsPrunedOnRemoval: a member's record of a
// join is read back at start, and dropped once the joiner is removed after the
// join settled. Guard: do not persist it.
func TestJoinedAtSurvivesARestartAndIsPrunedOnRemoval(t *testing.T) {
	s := startSettlingCluster(t)
	defer s.tc.stop()
	joinEpoch := s.join(t, 0)
	restarted := s.tc.restartNode(1).server.core.cluster
	if joinedAt := restarted.joinedAtSnapshot()[s.ports.ringAddr]; joinedAt != joinEpoch {
		t.Fatalf("after a restart node 2 records the joiner as joined at %d, want %d", joinedAt, joinEpoch)
	}

	members := s.members()
	for _, member := range members {
		waitSettled(t, member, joinEpoch)
	}
	pollUntil(t, "node 1 hears every member settled", func() bool {
		wakeAll(members)
		for _, member := range members {
			if heardSettled(clusterOf(s.tc, 0), member.self.ID) < joinEpoch {
				return false
			}
		}
		return true
	})
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := clusterOf(s.tc, 0).proposeRemoval(ctx, s.ports.ringAddr); err != nil {
		t.Fatalf("proposeRemoval: %v", err)
	}
	if _, held := clusterOf(s.tc, 0).joinedAtSnapshot()[s.ports.ringAddr]; held {
		t.Fatal("node 1 still records the removed joiner's join after every member settled it")
	}
	record, found, err := readMembershipRecord(clusterOf(s.tc, 0).membershipPath())
	if err != nil || !found {
		t.Fatalf("reading node 1's membership record: found=%v err=%v", found, err)
	}
	if _, held := record.JoinedAt[s.ports.ringAddr]; held {
		t.Fatal("node 1's membership.json still records the removed joiner's join")
	}
}
