package server

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/transport"
)

// The owner's other clearings of its awaiting records (bucket-replication-rev8-
// spec §3.3, §7.3; G3 build doc step 3): the bucket's drop and its release, a
// database drop of the record's generation, live and replayed, the copy
// leaving the term the record names, and the placement pass once the member
// has left the bucket's set. Each deletes the record and the map entry.

// requireNoAwaiting fails if node s still holds an awaiting record of the
// bucket placed by key, on disk or in its map.
func requireNoAwaiting(t *testing.T, s *Server, key placementKey, when string) {
	t.Helper()
	if onDisk := awaitingOnDisk(t, s, key); len(onDisk) != 0 {
		t.Errorf("%s the owner's store still holds awaiting records for %v", when, onDisk)
	}
	if inMemory := awaitingInMap(s.cluster, key); len(inMemory) != 0 {
		t.Errorf("%s the owner's awaiting map still holds %v", when, inMemory)
	}
}

// requireFoundLocally fails unless node s's own copy of bucket holds every key
// in keys.
func requireFoundLocally(t *testing.T, s *Server, bucket string, keys []string) {
	t.Helper()
	for _, key := range keys {
		found, err := s.Find(FindRequest{Database: testDatabase, Bucket: bucket, Key: key})
		if err != nil {
			t.Fatalf("reading %q from %s's own copy of %q: %v", key, s.cluster.self.ID, bucket, err)
		}
		if !found.Found {
			t.Fatalf("%s's own copy of %q lacks the acknowledged write %q", s.cluster.self.ID, bucket, key)
		}
	}
}

// TestADatabaseDropDeletesTheAwaitingRecordsOfItsGeneration is T-190 (RF-3).
// An increase is pending on member Y; the database is dropped, and the same
// bucket name is created again in the same epoch, with no membership change
// between, so the new bucket's term equals the old record's. The drop deletes
// the owner's records of the dropped generation in its own batch, and Y
// receives the new bucket's create and every later write, read from Y's own
// storage. In the second case the owner stops after the drop is logged and
// before its batch commits: the drop replayed at start deletes the records
// before the start-up load reads them (the drop's batch never committed, so
// its replay learns it).
func TestADatabaseDropDeletesTheAwaitingRecordsOfItsGeneration(t *testing.T) {
	t.Run("live", func(t *testing.T) {
		tc, bucket, member := pendingOnOneMember(t)
		defer tc.stop()
		owner := tc.nodes[0].server.core
		key := placementOf(testDatabase, bucket)
		epoch := owner.cluster.Epoch()

		tc.ok(0, transport.OpDeleteDB, DeleteDatabaseRequest{Name: testDatabase})
		requireNoAwaiting(t, owner, key, "after the database drop")
		tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
		createBucketAt(tc, 0, testDatabase, bucket, 3)
		if now := owner.cluster.Epoch(); now != epoch {
			t.Fatalf("setup: the epoch moved from %d to %d; the recreated bucket must be in the dropped one's epoch", epoch, now)
		}
		keys := []string{"first", "second", "third"}
		for _, key := range keys {
			putDocument(tc, 0, testDatabase, bucket, key)
		}
		waitForBarrier(t, tc, 0, "barrier", member)
		if !tc.nodes[member].server.core.bucketStoreOpen(testDatabase, bucket) {
			t.Fatalf("node %d did not create the recreated bucket %q", member+1, bucket)
		}
		requireHeldLocally(t, tc.nodes[member].server.core, bucket, keys)
	})

	t.Run("replayed at start", func(t *testing.T) {
		// A member taught the drop would log it again and ship it back (G-12),
		// and the owner would apply it live before it restarts.
		generationSeamsHook.Store(&generationSeams{noRelog: true})
		t.Cleanup(func() { generationSeamsHook.Store(nil) })
		tc, bucket, _ := pendingOnOneMember(t)
		defer tc.stop()
		key := placementOf(testDatabase, bucket)

		var failed atomic.Bool
		hook := func(name string) error {
			if name == testDatabase && failed.CompareAndSwap(false, true) {
				return errors.New("injected: the drop's batch did not commit, as if the owner stopped after the append")
			}
			return nil
		}
		generationDropCommitHook.Store(&hook)
		t.Cleanup(func() { generationDropCommitHook.Store(nil) })
		tc.frame(0, transport.OpDeleteDB, DeleteDatabaseRequest{Name: testDatabase})
		generationDropCommitHook.Store(nil)
		if !failed.Load() || !tc.nodes[0].server.core.bucketStoreOpen(testDatabase, bucket) {
			t.Fatal("setup: the drop's commit did not fail on the owner, which logged it")
		}

		restarted := tc.restartNode(0).server.core
		if restarted.bucketStoreOpen(testDatabase, bucket) {
			t.Fatalf("after the replay the owner still holds %q of the dropped database", bucket)
		}
		requireNoAwaiting(t, restarted, key, "after the drop was replayed at start")
	})
}

// TestABucketDropDeletesItsAwaitingRecords: an increase is pending on a
// member; the bucket is dropped and its name created again in the same epoch
// and database, so the new bucket's term and generation equal the old
// record's. The drop deleted the record in its batch, so nothing is left to
// match the new bucket, and an alter that places the member is not refused R8.
func TestABucketDropDeletesItsAwaitingRecords(t *testing.T) {
	tc, bucket, member := pendingOnOneMember(t)
	defer tc.stop()
	owner := tc.nodes[0].server.core
	key := placementOf(testDatabase, bucket)

	tc.ok(0, transport.OpDeleteBucket, DeleteBucketRequest{Database: testDatabase, Name: bucket})
	requireNoAwaiting(t, owner, key, "after the bucket's drop")
	createBucketAt(tc, 0, testDatabase, bucket, 2)
	waitForServing(t, tc, testDatabase, bucket, 2)
	answer := alterAccepted(t, alterFrame(t, tc, 0, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 3}))
	if len(answer.Pending) != 1 || answer.Pending[0] != tc.addrs[member] {
		t.Fatalf("the recreated bucket's increase names %v as pending, want only node %d", answer.Pending, member+1)
	}
}

// TestThePlacementPassClearsTheRecordOfAMemberThatLeftTheSet: an increase is
// pending on a member, which is then removed, so it is in no set of the
// bucket's. Once the owner's placement pass for the new epoch has finished,
// its record and its map entry are gone (spec §7.3; OQ-G3-9: the pass clears
// them before it records itself finished).
func TestThePlacementPassClearsTheRecordOfAMemberThatLeftTheSet(t *testing.T) {
	tc, bucket, member := pendingOnOneMember(t)
	defer tc.stop()
	owner := tc.nodes[0].server.core
	key := placementOf(testDatabase, bucket)
	epoch := owner.cluster.Epoch()

	tc.stopNodeKeepingData(member)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := owner.cluster.proposeRemoval(ctx, tc.addrs[member]); err != nil {
		t.Fatalf("removing node %d: %v", member+1, err)
	}
	pollUntil(t, "the owner's placement pass for the removal finishes", func() bool {
		return owner.cluster.Epoch() > epoch && owner.cluster.placementPassFinished()
	})
	requireNoAwaiting(t, owner, key, fmt.Sprintf("after node %d left the set and the pass finished", member+1))
}

// findOwnedByJoinerWithThirdAwaiting names a bucket of testDatabase that a fourth
// ring member, joining three, owns, and returns the node that owned it before
// and the node that node's walk puts third: created at 2 and raised to 3, it
// is pending on that third node. Both stay in the bucket's set at 3 after the
// join, so the old owner follows the joiner's term and the awaiting member is
// still placed: neither a release nor the placement pass deletes the record,
// and only the term change does. A ring walks the members after an owner in
// one fixed order, and the joiner's place in it is fixed by its identity, so
// the old owner is searched for as well as the name. It reports false when no
// name qualifies: whether one does depends on where the joiner's drawn
// identity falls in the ring, so the caller draws again.
func findOwnedByJoinerWithThirdAwaiting(t *testing.T, tc *testCluster, joiner string) (bucket string, owner, member int, found bool) {
	t.Helper()
	before := ringWith(t, 3, tc.addrs...)
	after := ringWith(t, 3, append(append([]string(nil), tc.addrs...), joiner)...)
	for attempt := 0; attempt < 8000; attempt++ {
		name := fmt.Sprintf("away%04d", attempt)
		key := placementOf(testDatabase, name).encoded
		oldSet, err := before.Replicas(key, 3)
		if err != nil {
			t.Fatalf("placing %s before the join: %v", name, err)
		}
		newSet, err := after.Replicas(key, 3)
		if err != nil {
			t.Fatalf("placing %s after the join: %v", name, err)
		}
		if newSet[0].ID == joiner && nodeListed(newSet, oldSet[0].ID) && nodeListed(newSet, oldSet[2].ID) {
			return name, nodeIndexByAddr(t, tc.addrs, oldSet[0].ID), nodeIndexByAddr(t, tc.addrs, oldSet[2].ID), true
		}
	}
	return "", -1, -1, false
}

// TestAnAwaitingRecordOfAnEarlierTermFiltersNothing is T-173 (rev 4 F-6's
// first case: away and back). An increase is pending on a member when a
// joining node takes the bucket over: the old owner's copy, still in the set,
// follows the joiner's term and so leaves the term its record names, and the
// record is deleted. The joiner is removed, and the old
// owner regains the bucket in a new term; the member copies it, and every
// write of the new term reaches the member's own storage. Kept across the
// term, the record would be on disk still, and R8 would read it.
func TestAnAwaitingRecordOfAnEarlierTermFiltersNothing(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()
	// The joiner's ports fix its place in the ring; some draws leave no name
	// that fits, so draw again rather than fail the setup (BUG-0487 family).
	var (
		ports              *testNodePorts
		bucket             string
		ownerIndex, member int
		found              bool
	)
	for draw := 0; draw < 20 && !found; draw++ {
		ports = reserveNodePorts(t)
		bucket, ownerIndex, member, found = findOwnedByJoinerWithThirdAwaiting(t, tc, ports.ringNode().ID)
	}
	if !found {
		t.Fatalf("setup: no port draw in 20 gave a joiner that owns a bucket with its old owner and third node staying in its set")
	}
	tc.ok(ownerIndex, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitForAlterFeature(t, tc)
	createBucketAt(tc, ownerIndex, testDatabase, bucket, 2)
	waitForServing(t, tc, testDatabase, bucket, 2)
	answer := alterAccepted(t, alterFrame(t, tc, ownerIndex, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 3}))
	if len(answer.Pending) != 1 || answer.Pending[0] != tc.addrs[member] {
		t.Fatalf("the increase names %v as pending, want only node %d", answer.Pending, member+1)
	}
	putDocument(tc, ownerIndex, testDatabase, bucket, "before-the-join")
	owner := tc.nodes[ownerIndex].server.core
	key := placementOf(testDatabase, bucket)
	firstTerm, _, err := owner.bucketLineage(testDatabase, bucket)
	if err != nil {
		t.Fatalf("reading the owner's term: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := owner.cluster.proposeJoin(ctx, ports.ringNode()); err != nil {
		t.Fatalf("proposing the join: %v", err)
	}
	joiner := startJoinTarget(t, tc, ports, tc.token)
	defer func() { _ = joiner.client.Close() }()
	bringNodesTo(t, append(append([]*testNode(nil), tc.nodes...), joiner), owner.cluster)
	waitTakenOver(t, joiner.server.core, testDatabase, bucket)
	pollUntil(t, "the old owner's copy leaves the term its awaiting record names", func() bool {
		term, _, err := owner.bucketLineage(testDatabase, bucket)
		return err == nil && term != firstTerm
	})
	requireNoAwaiting(t, owner, key, "once the old owner's copy left its term")

	if err := owner.cluster.proposeRemoval(ctx, joiner.server.core.cluster.self.ID); err != nil {
		t.Fatalf("removing the joiner: %v", err)
	}
	bringNodesTo(t, append(append([]*testNode(nil), tc.nodes...), joiner), owner.cluster)
	waitTakenOver(t, owner, testDatabase, bucket)
	requireNoAwaiting(t, owner, key, "once the old owner regained the bucket in a new term")
	if err := owner.cluster.refusePendingCopy(AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 3}, 3); err != nil {
		t.Errorf("R8 refuses an alter of the regained bucket: %v", err)
	}

	awaitMigrated(t, migrateInBackground(clusterOf(tc, member), bucket), fmt.Sprintf("node %d's migrate of %q", member+1, bucket))
	keys := []string{"new-term-1", "new-term-2", "new-term-3"}
	for _, key := range keys {
		putDocument(tc, ownerIndex, testDatabase, bucket, key)
	}
	// A database create would be the barrier, but schema changes stay paused
	// while the members settle the joiner's removal: the member's own copy is
	// read until it holds the writes.
	seeded := tc.nodes[member].server.core
	holdsWithin(20*time.Second, func() bool {
		for _, key := range keys {
			if found, err := seeded.Find(FindRequest{Database: testDatabase, Bucket: bucket, Key: key}); err != nil || !found.Found {
				return false
			}
		}
		return true
	})
	requireFoundLocally(t, seeded, bucket, keys)
}
