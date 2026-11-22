package server

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Join-freeze build step 11: with auto_rebalance off, a confirm-only pass
// (join-freeze spec §8.6, Q2 option A, review F-14).

// manualJoin is three members at factor 3 with auto-rebalance off, buckets of
// one document each, and a joiner started the documented way, also with
// auto-rebalance off, joined through node 1.
type manualJoin struct {
	*settlingCluster
	joinEpoch uint64
}

// setUpManualJoin builds a manualJoin whose joiner takes ports; pick chooses
// the buckets from each name's replica set after the join.
func setUpManualJoin(t *testing.T, ports *testNodePorts, pick func(setOf func(name string) []string) []string) *manualJoin {
	t.Helper()
	lowerSweepBounds(t, time.Minute)
	tc := newTestClusterWith(t, 3, 3, func(_ int, cfg *config.Config) { cfg.Server.AutoRebalance = false })
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	s := &manualJoin{settlingCluster: &settlingCluster{tc: tc, ports: ports}}
	joined := s.joinedRing(t)
	setOf := func(name string) []string {
		set, err := joined.Replicas(placementOf(testDatabase, name).encoded, 3)
		if err != nil {
			t.Fatalf("placing %s after the join: %v", name, err)
		}
		return nodeIDs(set)
	}
	for _, name := range pick(setOf) {
		tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: name, Type: BucketTypeHash})
		tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: name, Key: "doc", Data: map[string]interface{}{"n": 1}})
		waitForServing(t, tc, testDatabase, name, 3)
		s.buckets = append(s.buckets, name)
	}
	s.joiner = startJoinTarget(t, tc, s.ports, tc.token, func(cfg *config.Config) { cfg.Server.AutoRebalance = false })
	s.joinEpoch = s.join(t, 0)
	return s
}

// gainingJoiner picks up to n bucket names whose set gains joiner and for
// which want holds of the set (owner first).
func gainingJoiner(prefix, joiner string, n int, want func(set []string) bool) func(func(string) []string) []string {
	return func(setOf func(string) []string) []string {
		var names []string
		for index := 0; len(names) < n && index < 2000; index++ {
			name := fmt.Sprintf("%s%03d", prefix, index)
			if set := setOf(name); slices.Contains(set, joiner) && want(set) {
				names = append(names, name)
			}
		}
		return names
	}
}

// TestWithAutoRebalanceOffAJoinPausesUntilEveryMovedPlacementIsMigrated is
// review F-14: with auto-rebalance off a join pauses schema changes; the
// refusal lists the 'migrate' commands that copy each moved bucket to the node
// that lacks it — where the joiner is the owner and where it is a replica —
// and once the operator has run them all, the pause lifts. Guard: mark the
// node settled at once when auto-rebalance is off.
func TestWithAutoRebalanceOffAJoinPausesUntilEveryMovedPlacementIsMigrated(t *testing.T) {
	ports := reserveNodePorts(t)
	joiner := ports.ringAddr
	s := setUpManualJoin(t, ports, func(setOf func(string) []string) []string {
		owned := gainingJoiner("owned", joiner, 2, func(set []string) bool { return set[0] == joiner })(setOf)
		replicated := gainingJoiner("replica", joiner, 2, func(set []string) bool { return set[0] != joiner })(setOf)
		return append(owned, replicated...)
	})
	defer s.tc.stop()

	pollUntil(t, "a create through a member is refused with the migrate commands", func() bool {
		wakeAll(s.members())
		frame := s.tc.frame(1, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: "manual_new", Type: BucketTypeHash})
		return frame.Opcode == transport.OpError && strings.Contains(refusalOf(t, frame).Message, "'migrate "+testDatabase+" ")
	})
	for _, name := range s.buckets {
		frame := s.tc.frameOn(s.joiner, transport.OpMigrate, MigrateRequest{Database: testDatabase, Bucket: name})
		if frame.Opcode == transport.OpError {
			t.Fatalf("migrate %s on the joiner: %s", name, string(frame.Payload))
		}
	}
	pollUntil(t, "every node lifts the pause once every moved bucket is migrated", func() bool {
		wakeAll(s.members())
		for _, member := range s.members() {
			if member.joinFreeze.Load().Paused {
				return false
			}
		}
		return true
	})
	s.tc.ok(1, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: "manual_new", Type: BucketTypeHash})
}

// TestTheConfirmPassListsAReplicaPlacement: a bucket on which the joiner is a
// replica, not the owner, is listed as a copy to make on the joiner. Guard:
// confirm only the buckets whose owner moved.
func TestTheConfirmPassListsAReplicaPlacement(t *testing.T) {
	ports := reserveNodePorts(t)
	joiner := ports.ringAddr
	// The owner's stream to its new replica would replay the bucket's create
	// from its log, and the copy could be there before the first question:
	// the streams to the joiner are held, so the copy is made by 'migrate' or
	// not at all.
	releaseStreams := holdStreamsTo(t, joiner)
	s := setUpManualJoin(t, ports, gainingJoiner("replica", joiner, 1, func(set []string) bool { return set[0] != joiner }))
	defer s.tc.stop()
	defer releaseStreams()
	want := PlacementToCopy{Database: testDatabase, Bucket: s.buckets[0], Node: joiner}
	pollUntil(t, "a holder lists the replica placement on the joiner as a copy to make", func() bool {
		for index := range s.tc.nodes {
			if progress := clusterOf(s.tc, index).ownHandOff(); progress != nil && slices.Contains(progress.Unconfirmed, want) {
				return true
			}
		}
		return false
	})
}

// autoOffOrphan is a cluster of three with auto-rebalance off, joined by a
// fourth, and one orphan create: a create the join took from its owner while
// it was in flight.
type autoOffOrphan struct {
	*settlingCluster
	owner  *Cluster
	orphan string
}

// setUpAutoOffOrphan builds an autoOffOrphan and returns once the orphan's
// create has been answered. The caller stops s.tc.
func setUpAutoOffOrphan(t *testing.T) *autoOffOrphan {
	t.Helper()
	lowerSweepBounds(t, time.Minute)
	tc := newTestClusterWith(t, 3, 3, func(_ int, cfg *config.Config) { cfg.Server.AutoRebalance = false })
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	joiner := reserveJoinerPlaced(t, func(joiner string) bool { return joinerFrom(t, "orphan", tc.addrs, joiner, 0) })
	s := &autoOffOrphan{settlingCluster: &settlingCluster{tc: tc, ports: joiner}, owner: clusterOf(tc, 0)}
	joined := s.joinedRing(t)
	s.orphan = bucketNamed(t, "orphan", func(name string) bool {
		return s.owner.ownsBucket(placementOf(testDatabase, name)) && ownerOn(joined, name) == s.ports.ringAddr
	})
	create := holdCreateAtOwnerView(t, tc, 0, s.owner, s.orphan)
	defer create.release()
	s.joiner = startJoinTarget(t, tc, s.ports, tc.token, func(cfg *config.Config) { cfg.Server.AutoRebalance = false })
	s.join(t, 1)
	create.release()
	create.awaitAnswer(t)
	return s
}

// listedCopies is the owner's copies of the orphan to make, as its hand-off
// lists them, once the joiner's is among them.
func (s *autoOffOrphan) listedCopies(t *testing.T) []PlacementToCopy {
	t.Helper()
	var copies []PlacementToCopy
	pollUntil(t, "the owner lists the orphan's copies to make", func() bool {
		copies = nil
		if progress := s.owner.ownHandOff(); progress != nil {
			for _, copyToMake := range progress.Unconfirmed {
				if copyToMake.Bucket == s.orphan {
					copies = append(copies, copyToMake)
				}
			}
		}
		return slices.ContainsFunc(copies, func(c PlacementToCopy) bool { return c.Node == s.ports.ringAddr })
	})
	return copies
}

// TestAnOrphanCreateWithAutoRebalanceOffSettlesOnceMigrated is the verifier's
// probe: with auto-rebalance off, a create the join took from its owner while
// it was in flight is an orphan, and nothing ever confirmed it, so the owner
// never settled and the pause never lifted, even after the operator ran
// 'migrate' for it. The orphan is now confirmed like any moved bucket.
func TestAnOrphanCreateWithAutoRebalanceOffSettlesOnceMigrated(t *testing.T) {
	s := setUpAutoOffOrphan(t)
	defer s.tc.stop()
	// The orphan was logged under the epoch before the join and shipped to
	// nobody: every member of its new set lacks it, and the owner lists each
	// copy to make, as the refusal prints them.
	s.migrateListedCopies(t)
}

// TestAnOrphanReplayedToItsNewSetSettlesOnceEveryListedCopyIsMigrated is
// BUG-0694: a replay of the owner's log can deliver the orphan's create to the
// members of its new set after the join (BUG-0545). Holding a copy, they are
// not listed while the joiner, its new owner, has none: the first list names
// the joiner alone. Once the joiner's copy is opened their copies are in an
// older term than the owner's and they are listed in turn (join-freeze spec
// §8.6); until they are migrated the owner stays unsettled with nothing of its
// own left to hand off, and schema changes stay paused. The replay is forced
// here with a reset of the owner's streams, which derives every entry from the
// log again.
func TestAnOrphanReplayedToItsNewSetSettlesOnceEveryListedCopyIsMigrated(t *testing.T) {
	s := setUpAutoOffOrphan(t)
	defer s.tc.stop()
	set, err := s.joinedRing(t).Replicas(placementOf(testDatabase, s.orphan).encoded, 3)
	if err != nil {
		t.Fatalf("placing %s after the join: %v", s.orphan, err)
	}
	clusters := make(map[string]*Cluster)
	for _, member := range s.members() {
		clusters[member.self.ID] = member
	}
	s.owner.resetStreams("test: replay the log")
	pollUntil(t, "the replay delivers the orphan to every member of its new set but the joiner", func() bool {
		for _, member := range set[1:] {
			if own, err := clusters[member.ID].answerCopy(ReplBucketRef{Database: testDatabase, Bucket: s.orphan}); err != nil || !own.Holds {
				return false
			}
		}
		return true
	})
	pollUntil(t, "the owner lists the joiner's copy of the orphan alone", func() bool {
		progress := s.owner.ownHandOff()
		return progress != nil && len(progress.Unconfirmed) == 1 &&
			progress.Unconfirmed[0] == PlacementToCopy{Database: testDatabase, Bucket: s.orphan, Node: s.ports.ringAddr}
	})
	if migrated := s.migrateListedCopies(t); len(migrated) < 2 {
		t.Errorf("the pause lifted with only %+v migrated; the members the replay reached hold the orphan in a term older than its owner's, "+
			"and nothing but a migrate brings them to the owner's", migrated)
	}
}

// migrateListedCopies runs 'migrate' for the orphan on every node the owner
// lists as lacking a copy, as an operator does from the refusal, and again
// for each copy listed afterwards, until every member lifts the pause; it
// returns the copies it migrated. One snapshot of the list is not enough: a
// member holding the orphan in a term older than its owner's is listed only
// once the owner has opened its copy (join-freeze spec §8.6, BUG-0545), and a
// replay of the owner's log can have given the members such a copy before the
// first list (BUG-0694). A migrate refused as retryable is run again.
func (s *autoOffOrphan) migrateListedCopies(t *testing.T) []PlacementToCopy {
	t.Helper()
	s.listedCopies(t)
	nodes := map[string]*testNode{s.ports.ringAddr: s.joiner}
	for index, addr := range s.tc.addrs {
		nodes[addr] = s.tc.nodes[index]
	}
	var migrated []PlacementToCopy
	members := s.members()
	for deadline := time.Now().Add(30 * time.Second); !allLifted(members); time.Sleep(50 * time.Millisecond) {
		if time.Now().After(deadline) {
			for _, member := range members {
				t.Logf("%s: settled %d, report %+v", member.self.ID, member.settled.Load(), *member.joinFreeze.Load())
			}
			t.Fatalf("schema changes still paused 30s after the first 'migrate' of the orphan; migrated %+v", migrated)
		}
		wakeAll(members)
		progress := s.owner.ownHandOff()
		if progress == nil {
			continue
		}
		for _, copyToMake := range progress.Unconfirmed {
			if copyToMake.Bucket != s.orphan || slices.Contains(migrated, copyToMake) {
				continue
			}
			frame := s.tc.frameOn(nodes[copyToMake.Node], transport.OpMigrate, MigrateRequest{Database: testDatabase, Bucket: s.orphan})
			if frame.Opcode == transport.OpError {
				t.Logf("migrate %s on %s, run again: %s", s.orphan, copyToMake.Node, refusalOf(t, frame).Message)
				continue
			}
			migrated = append(migrated, copyToMake)
		}
	}
	return migrated
}

// TestARefusalThroughANonHolderListsTheMigrateCommands is the verifier's
// probe: with auto-rebalance off a node recomputed its report only when a
// member's settled epoch rose, so the owner's list of copies to make never
// reached another node's refusal, which said to wait — wrong advice, since
// nothing moves until the operator runs 'migrate'. A hand-off heard on gossip
// that changes now recomputes the report, at most once a second.
func TestARefusalThroughANonHolderListsTheMigrateCommands(t *testing.T) {
	s := setUpAutoOffOrphan(t)
	defer s.tc.stop()
	s.listedCopies(t)
	const nonHolder = 1
	want := "'migrate " + testDatabase + " " + s.orphan + "'"
	var refused string
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		wakeAll(s.members())
		frame := s.tc.frame(nonHolder, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: "through_non_holder", Type: BucketTypeHash})
		if frame.Opcode != transport.OpError {
			t.Fatalf("a create through node %d was accepted while the orphan's copies are unmade", nonHolder+1)
		}
		if refused = refusalOf(t, frame).Message; strings.Contains(refused, want) {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("15 s after the owner listed the orphan's copies, the refusal through node %d does not list %s:\n%s", nonHolder+1, want, refused)
}

// TestTheConfirmPassMovesNothing: with auto-rebalance off the confirm pass
// asks; it hands nothing off, so no holder releases a copy. (The joiner may
// still receive a bucket by its owner's stream replaying the create from the
// log — that is replication, not the pass.) Guard: hand off in the pass.
func TestTheConfirmPassMovesNothing(t *testing.T) {
	ports := reserveNodePorts(t)
	s := setUpManualJoin(t, ports, gainingJoiner("moved", ports.ringAddr, 4, func([]string) bool { return true }))
	defer s.tc.stop()
	pollUntil(t, "every holder's confirm pass has run", func() bool {
		for index := range s.tc.nodes {
			if progress := clusterOf(s.tc, index).ownHandOff(); progress == nil || progress.Epoch != s.joinEpoch {
				return false
			}
		}
		return true
	})
	time.Sleep(3 * sweepRetryFloor.get())
	for _, name := range s.buckets {
		for index, node := range s.tc.nodes {
			if !node.server.core.bucketStoreOpen(testDatabase, name) {
				t.Errorf("node %d no longer holds %s; with auto-rebalance off nothing is released", index+1, name)
			}
		}
	}
}
