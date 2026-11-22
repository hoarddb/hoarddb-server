package server

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/transport"
)

// Join-freeze build step 6: gossip carries each node's Settled epoch, the
// best it has heard for every other member, and its hand-off progress
// (join-freeze spec §3.4, §8.2, §8.5).

// heardSettled is node c's value for member id: its own for itself, the best
// it has heard for any other.
func heardSettled(c *Cluster, id string) uint64 {
	if id == c.self.ID {
		return c.settled.Load()
	}
	c.settleMu.Lock()
	defer c.settleMu.Unlock()
	return c.settledOf[id]
}

// wakeAll makes every node gossip now.
func wakeAll(clusters []*Cluster) {
	for _, c := range clusters {
		c.wakeGossip()
	}
}

// TestGossipCarriesEachMembersSettledEpoch: once every member has settled a
// join, each holds every other's value within a woken round.
func TestGossipCarriesEachMembersSettledEpoch(t *testing.T) {
	s := startSettlingCluster(t)
	defer s.tc.stop()
	joinEpoch := s.join(t, 0)
	members := s.members()
	for _, member := range members {
		waitSettled(t, member, joinEpoch)
	}
	pollUntil(t, "every member hears every other member's settled epoch", func() bool {
		wakeAll(members)
		for _, listener := range members {
			for _, member := range members {
				if heardSettled(listener, member.self.ID) < joinEpoch {
					return false
				}
			}
		}
		return true
	})
}

// TestASettledEpochHeardSecondHandIsKept: a member that stopped after settling
// is still known settled — a node that lost its value learns it from a member
// that heard it. Guard: ignore SettledOf.
func TestASettledEpochHeardSecondHandIsKept(t *testing.T) {
	s := startSettlingCluster(t)
	defer s.tc.stop()
	joinEpoch := s.join(t, 0)
	members := s.members()
	for _, member := range members {
		waitSettled(t, member, joinEpoch)
	}
	first, second, stopped := clusterOf(s.tc, 0), clusterOf(s.tc, 1), s.tc.addrs[2]
	pollUntil(t, "node 2 hears node 3 settled", func() bool {
		wakeAll(members)
		return heardSettled(second, stopped) >= joinEpoch
	})
	stopNode(t, s.tc, 2)
	first.settleMu.Lock()
	delete(first.settledOf, stopped)
	first.settleMu.Unlock()

	pollUntil(t, "node 1 learns node 3's settled epoch from node 2", func() bool {
		first.wakeGossip()
		second.wakeGossip()
		return heardSettled(first, stopped) >= joinEpoch
	})
}

// TestANodeNeverTakesAHeardValueForItself: a member relaying a higher value for
// this node changes nothing here; a node's own value is its own. Guard: merge
// a relayed value for this node.
func TestANodeNeverTakesAHeardValueForItself(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	first, second := clusterOf(tc, 0), clusterOf(tc, 1)
	own := first.settled.Load()
	first.mergeSettled(second.self.ID, 7, []MemberSettled{{ID: first.self.ID, Settled: 99}}, nil)
	if settled := first.settled.Load(); settled != own {
		t.Errorf("node 1's own settled epoch moved from %d to %d on a value relayed for it", own, settled)
	}
	first.settleMu.Lock()
	relayedForSelf, held := first.settledOf[first.self.ID]
	heardSecond := first.settledOf[second.self.ID]
	first.settleMu.Unlock()
	if held {
		t.Errorf("node 1 keeps a heard value %d for itself", relayedForSelf)
	}
	if heardSecond != 7 {
		t.Errorf("node 1 holds %d for node 2, which said 7", heardSecond)
	}
}

// TestSettledOfIsPrunedToTheMembers: a member removed is forgotten, in memory
// and in membership.json. Guard: no prune.
func TestSettledOfIsPrunedToTheMembers(t *testing.T) {
	s := startSettlingCluster(t)
	defer s.tc.stop()
	joinEpoch := s.join(t, 0)
	members := s.members()
	for _, member := range members {
		waitSettled(t, member, joinEpoch)
	}
	first := clusterOf(s.tc, 0)
	pollUntil(t, "node 1 hears the joiner settled", func() bool {
		wakeAll(members)
		return heardSettled(first, s.ports.ringAddr) >= joinEpoch
	})
	proposeRemovalVia(t, first, s.ports.ringAddr)
	for index := range s.tc.nodes {
		member := clusterOf(s.tc, index)
		pollUntil(t, fmt.Sprintf("node %d adopts the removal", index+1), func() bool { return member.Epoch() == joinEpoch+1 })
		member.settleMu.Lock()
		_, held := member.settledOf[s.ports.ringAddr]
		member.settleMu.Unlock()
		if held {
			t.Errorf("node %d still holds a settled epoch for the removed joiner", index+1)
		}
		record, found, err := readMembershipRecord(member.membershipPath())
		if err != nil || !found {
			t.Fatalf("reading node %d's membership record: found=%v err=%v", index+1, found, err)
		}
		if _, held := record.SettledOf[s.ports.ringAddr]; held {
			t.Errorf("node %d's membership.json still records a settled epoch for the removed joiner", index+1)
		}
	}
}

// TestHandOffCountsEachMovedBucketOnce: with three members at factor 3 and a
// joiner, the members' totals add up to the buckets whose set the join
// changed — each counted by its previous owner alone — and their done counts
// reach it. A join changes more sets than those it adds the joiner to: the
// ring can reorder a set of the same members, which moves the owner (seen
// for about one bucket in thirty here), and that bucket is moved too. Guard:
// count on every holder.
func TestHandOffCountsEachMovedBucketOnce(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	s := &settlingCluster{tc: tc, ports: reserveNodePorts(t)}
	before := ringOf(t, clusterOf(tc, 0).Members(), 3)
	joined := s.joinedRing(t)
	var moved uint64
	for index := 0; index < 30; index++ {
		name := fmt.Sprintf("count%03d", index)
		tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: name, Type: BucketTypeHash})
		tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: name, Key: "doc", Data: map[string]interface{}{"n": index}})
		oldSet, err := before.Replicas(placementOf(testDatabase, name).encoded, 3)
		if err != nil {
			t.Fatalf("placing %s before the join: %v", name, err)
		}
		newSet, err := joined.Replicas(placementOf(testDatabase, name).encoded, 3)
		if err != nil {
			t.Fatalf("placing %s after the join: %v", name, err)
		}
		if !slices.Equal(nodeIDs(oldSet), nodeIDs(newSet)) {
			moved++
		}
	}
	joinEpoch := s.join(t, 0)
	sums := func() (total, done uint64) {
		for index := range tc.nodes {
			if progress := clusterOf(tc, index).ownHandOff(); progress != nil && progress.Epoch == joinEpoch {
				total += progress.Total
				done += progress.Done
			}
		}
		return total, done
	}
	// A previous owner whose copy a puller released (handleMigrateDone) before
	// its own sweep walked its catalog no longer lists that bucket; it counts
	// the release instead, so each moved bucket is counted, once.
	for _, member := range s.members() {
		waitSettled(t, member, joinEpoch)
	}
	total, done := sums()
	if total != moved || done != total {
		t.Fatalf("the members count %d moved buckets, %d done; the rings say %d moved", total, done, moved)
	}
}

// TestHandOffNamesTheMemberItWaitsOn is review F-5 with C-3: the members'
// hand-offs are held from before the join commits; node 3 is stopped; once
// released, node 1's progress names node 3 as the member it waits on — after
// showing it has buckets left. Guard: discard the ids.
func TestHandOffNamesTheMemberItWaitsOn(t *testing.T) {
	lowerSweepBounds(t, time.Second)
	s := startSettlingCluster(t)
	defer s.tc.stop()
	held := []string{s.tc.addrs[0], s.tc.addrs[1]}
	release := make(chan struct{})
	var releaseOnce sync.Once
	setAskNodeToMigrateHook(t, func(from, _, _, _ string) error {
		if slices.Contains(held, from) {
			<-release
		}
		return nil
	})
	defer releaseOnce.Do(func() { close(release) })
	holder := clusterOf(s.tc, 0)
	// A re-armed sweep (every 100 ms here) goes on to rewrite the residue the
	// first sweep recorded, so on a starved host the test could look after it
	// and find nothing (BUG-0457, BUG-0534). Hold it at its start, until the
	// test has looked. Registered after s.tc.stop's defer so it runs first:
	// stopping the cluster waits on the goroutine this holds.
	stages := holdSweepStages(t, holder.self.ID)
	joinEpoch := holder.Epoch() + 1
	stages.hold(joinEpoch, rearmPassing)
	defer stages.release(joinEpoch, rearmPassing)
	if got := s.join(t, 0); got != joinEpoch {
		t.Fatalf("test setup: the join is epoch %d, want %d", got, joinEpoch)
	}
	stopped := s.tc.addrs[2]
	stopNode(t, s.tc, 2)
	releaseOnce.Do(func() { close(release) })

	pollUntil(t, "node 1's sweep gives up and records what it left", func() bool {
		residue := holder.residueSnapshot()
		return residue.epoch == joinEpoch && residue.generation != 0
	})
	if residue := holder.residueSnapshot(); len(residue.list) == 0 {
		t.Fatal("test setup: node 1's sweep left nothing, so there is nothing to wait on")
	}
	progress := holder.ownHandOff()
	if progress == nil || progress.Epoch != joinEpoch {
		t.Fatalf("node 1 publishes no hand-off of epoch %d: %+v", joinEpoch, progress)
	}
	// The joiner may be named too: as a bucket's owner it pulls from the
	// members in turn and stops at one that does not answer, so it refuses
	// node 1 for node 3's absence (ensureMigrated). Node 3 must be named, and
	// no member that answers and could pull.
	requireWaitsOnStopped := func(what string, waitingOn []string) {
		t.Helper()
		if !slices.Contains(waitingOn, stopped) {
			t.Fatalf("%s waits on %v, which does not name the stopped node 3 (%s)", what, waitingOn, stopped)
		}
		for _, member := range waitingOn {
			if member != stopped && member != s.ports.ringAddr {
				t.Fatalf("%s waits on %v, naming %s, which answers and holds its copies", what, waitingOn, member)
			}
		}
	}
	requireWaitsOnStopped("node 1's hand-off", progress.WaitingOn)
	requireWaitsOnStopped("node 1's residue", holder.residueSnapshot().waitingOn)
}

// proposeRemovalVia removes node id through proposer.
func proposeRemovalVia(t *testing.T, proposer *Cluster, id string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := proposer.proposeRemoval(ctx, id); err != nil {
		t.Fatalf("proposeRemoval of %s: %v", id, err)
	}
}

// TestABucketReleasedBeforeTheWalkIsCountedOnce: a previous owner that released
// a copy (releaseOutsideSet) before its own sweep of the join walked its
// catalog no longer lists that bucket; it counts the release, in Total and in
// Done, so its figure is every bucket it owned whose ordered set changed.
// The previous owner's sweep is held before its walk while the joiner takes
// the bucket over and the previous owner releases it.
// A join only inserts a node into a set, so a previous owner leaves the set
// only at a factor of 1: the released bucket is created at 1. Guard: count no
// release.
func TestABucketReleasedBeforeTheWalkIsCountedOnce(t *testing.T) {
	const previous = 0
	s := startSettlingClusterPlaced(t, func(founders []string, joiner string) bool {
		return joinerFrom(t, "released", founders, joiner, previous)
	})
	defer s.tc.stop()
	holder := clusterOf(s.tc, previous)
	before := ringOf(t, holder.Members(), 3)
	joined := s.joinedRing(t)
	joiner := s.ports.ringAddr
	released := bucketNamed(t, "released", func(name string) bool {
		return ownerOn(before, name) == holder.self.ID && ownerOn(joined, name) == joiner
	})
	s.tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: released, Type: BucketTypeHash, ReplicationFactor: 1})
	s.tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: released, Key: "doc", Data: map[string]interface{}{"n": 1}})
	waitForServing(t, s.tc, testDatabase, released, 1)
	var owned uint64
	for _, name := range append(slices.Clone(s.buckets), released) {
		factor := 3
		if name == released {
			factor = 1
		}
		oldSet, err := before.Replicas(placementOf(testDatabase, name).encoded, factor)
		if err != nil {
			t.Fatalf("placing %s before the join: %v", name, err)
		}
		newSet, err := joined.Replicas(placementOf(testDatabase, name).encoded, factor)
		if err != nil {
			t.Fatalf("placing %s after the join: %v", name, err)
		}
		if oldSet[0].ID == holder.self.ID && !slices.Equal(nodeIDs(oldSet), nodeIDs(newSet)) {
			owned++
		}
	}

	stages := holdSweepStages(t, holder.self.ID)
	joinEpoch := holder.Epoch() + 1
	stages.hold(joinEpoch, sweepStarting)
	if s.join(t, 1) != joinEpoch {
		t.Fatalf("test setup: the join is not epoch %d", joinEpoch)
	}
	pollUntil(t, "the previous owner has adopted the join", func() bool { return holder.Epoch() >= joinEpoch })
	select {
	case <-stages.reachedChan(joinEpoch, sweepStarting):
	case <-time.After(30 * time.Second):
		t.Fatal("test setup: the previous owner's sweep of the join never started")
	}
	pullCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// The joiner takes the bucket over (step 10b.5), and the previous owner,
	// outside the set, releases its copy once the set holds what it holds —
	// by a hand-off made before its sweep walks, as its start's relocation
	// makes one (re-aimed by replication dispatch 10b, step 10b.7: the source of
	// a pull no longer deletes its copy at MIGRATE_DONE).
	puller := s.joiner.server.core.cluster
	if err := puller.ensureMigrated(pullCtx, testDatabase, released); err != nil {
		t.Fatalf("the joiner taking %s over from its previous owner: %v", released, err)
	}
	replicas, err := holder.Replicas(placementOf(testDatabase, released))
	if err != nil {
		t.Fatalf("placing %s: %v", released, err)
	}
	pollUntil(t, "the previous owner releases the bucket the joiner took over", func() bool {
		if err := holder.handOffBucket(pullCtx, testDatabase, released, replicas); err != nil {
			t.Logf("the previous owner's hand-off of %s: %v", released, err)
		}
		return !holder.s.bucketStoreOpen(testDatabase, released)
	})
	stages.release(joinEpoch, sweepStarting)
	waitSettled(t, holder, joinEpoch)
	progress := holder.ownHandOff()
	if progress == nil || progress.Epoch != joinEpoch {
		t.Fatalf("the previous owner reports no hand-off of epoch %d: %+v", joinEpoch, progress)
	}
	if progress.Total != owned || progress.Done != owned {
		t.Fatalf("the previous owner counts %d moved buckets, %d done; it owned %d whose set changed, %s among them, released before its walk",
			progress.Total, progress.Done, owned, released)
	}
}
