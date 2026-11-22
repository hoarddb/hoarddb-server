package server

import (
	"fmt"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The pending-copy worker's terminal answer and the queue on every delete of a
// needs-a-copy record (bucket-replication-rev8-spec §3.3, §7.5; the G4 to G7
// build doc step 2). NotPlaced ends a pending copy only once the bucket's owner
// confirms it over askCopies; not found never does; and each of the six
// deletes of a record takes the bucket out of the worker's queue.

// TestNotPlacedFromASettledOwnerEndsThePendingCopy is T-177 (owner R9-Q6). A
// NotPlaced the owner confirms over askCopies, its copy held and opened at a
// factor that does not place this node, ends the pending copy: the record and
// the queue entry go and nothing lands. A NotPlaced the owner does not
// confirm, its factor still placing this node, keeps both and is tried again.
func TestNotPlacedFromASettledOwnerEndsThePendingCopy(t *testing.T) {
	t.Run("confirmed by the owner", func(t *testing.T) {
		tc, buckets, member := rebalancingPendingOn(t, 1, "unplaced")
		defer tc.stop()
		bucket := buckets[0]
		key := placementOf(testDatabase, bucket)
		core := tc.nodes[member].server.core
		held, release := holdBucketAt(t, &pullStartedHook, bucket)
		raiseToThree(t, tc, bucket, member)
		awaitHeld(t, held, fmt.Sprintf("node %d's worker pull of %q", member+1, bucket))
		// The decrease is kept from the member, so only the owner's answer can
		// end its record: §5.3 row 5 never runs there.
		owner := clusterOf(tc, 0)
		unfreeze := freezePeer(owner.peerFor(mustNode(t, owner, tc.addrs[member])))
		defer unfreeze()
		alterAccepted(t, alterFrame(t, tc, 0, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 2}))
		release()
		pollUntil(t, fmt.Sprintf("node %d ends its pending copy of %q on the owner's confirmation", member+1, bucket), func() bool {
			_, pending := core.cluster.pendingCopyOf(key)
			return !pending
		})
		requireNoNeedsCopy(t, core, bucket, "once the owner confirmed it does not place the member")
		if core.bucketStoreOpen(testDatabase, bucket) {
			t.Errorf("node %d landed %q, which its owner no longer places on it", member+1, bucket)
		}
		if walOpCount(t, core.wal, transport.OpAlterBucket, bucket) != 1 {
			t.Errorf("setup: node %d logged the decrease of %q, so row 5 and not the owner's answer may have ended the record", member+1, bucket)
		}
	})

	t.Run("not confirmed", func(t *testing.T) {
		attempts := injectPendingCopyPulls(t, func(_, bucket string) error {
			if strings.HasPrefix(bucket, "unconfirmed") {
				return fmt.Errorf("%w: injected for this test", errNotPlacedHere)
			}
			return nil
		})
		tc, buckets, member := rebalancingPendingOn(t, 1, "unconfirmed")
		defer tc.stop()
		bucket := buckets[0]
		core := tc.nodes[member].server.core
		raiseToThree(t, tc, bucket, member)
		pollUntil(t, fmt.Sprintf("node %d's worker is answered NotPlaced for %q twice", member+1, bucket), func() bool {
			return attempts.of(core.cluster.self.ID, bucket) >= 2
		})
		if _, recorded := needsCopyOnDisk(t, core, bucket); !recorded {
			t.Fatalf("node %d deleted its needs-a-copy record of %q on a NotPlaced its owner, at factor 3, does not confirm", member+1, bucket)
		}
		outcome, pending := core.cluster.pendingCopyOf(placementOf(testDatabase, bucket))
		if !pending || !strings.Contains(outcome.reason, "does not confirm") {
			t.Errorf("node %d's pending copy of %q after an unconfirmed NotPlaced is %+v (queued %v), want it kept with the reason", member+1, bucket, outcome, pending)
		}
	})
}

// TestAnOwnerWithNoCopyYetKeepsThePendingCopy is T-54's not-found case (spec
// §7.5: not found is never terminal on its own). An owner that has not landed
// the bucket answers not found; the member keeps its record and its queue
// entry, says the owner has no copy yet, and tries again.
func TestAnOwnerWithNoCopyYetKeepsThePendingCopy(t *testing.T) {
	attempts := injectPendingCopyPulls(t, func(_, bucket string) error {
		if strings.HasPrefix(bucket, "notfound") {
			return fmt.Errorf("%w (from the owner): injected for this test", ErrMigrateBucketNotFound)
		}
		return nil
	})
	tc, buckets, member := rebalancingPendingOn(t, 1, "notfound")
	defer tc.stop()
	bucket := buckets[0]
	core := tc.nodes[member].server.core
	raiseToThree(t, tc, bucket, member)
	pollUntil(t, fmt.Sprintf("node %d's worker is answered not found for %q twice", member+1, bucket), func() bool {
		return attempts.of(core.cluster.self.ID, bucket) >= 2
	})
	if _, recorded := needsCopyOnDisk(t, core, bucket); !recorded {
		t.Fatalf("node %d deleted its needs-a-copy record of %q on a not-found answer", member+1, bucket)
	}
	outcome, pending := core.cluster.pendingCopyOf(placementOf(testDatabase, bucket))
	if !pending || !strings.Contains(outcome.reason, "the owner has no copy yet") {
		t.Errorf("node %d's pending copy of %q after a not-found answer is %+v (queued %v), want it kept, saying the owner has no copy yet",
			member+1, bucket, outcome, pending)
	}
}

// requireNotQueued fails if node s's pending-copy worker still has bucket
// queued.
func requireNotQueued(t *testing.T, s *Server, bucket, when string) {
	t.Helper()
	if outcome, pending := s.cluster.pendingCopyOf(placementOf(testDatabase, bucket)); pending {
		t.Errorf("%s, %s's pending-copy worker still has %q queued: %+v", when, s.cluster.self.ID, bucket, outcome)
	}
}

// requireQueued fails unless node s's pending-copy worker has bucket queued.
func requireQueued(t *testing.T, s *Server, bucket, when string) {
	t.Helper()
	if _, pending := s.cluster.pendingCopyOf(placementOf(testDatabase, bucket)); !pending {
		t.Errorf("%s, %s's pending-copy worker has nothing queued for %q", when, s.cluster.self.ID, bucket)
	}
}

// TestEveryNeedsCopyDeleteEmptiesTheQueue is the build doc's fail-silently 1:
// each of the six deletes of a needs-a-copy record takes the bucket out of the
// worker's queue once its batch has committed. Auto-rebalance is off, so the
// worker pulls nothing and only the delete under test can empty the queue; the
// worker's own terminal answer is T-177's first case.
func TestEveryNeedsCopyDeleteEmptiesTheQueue(t *testing.T) {
	t.Run("the landing", func(t *testing.T) {
		tc, bucket, _, _, _ := increasedOnEmptyMembers(t, BucketTypeHash)
		defer tc.stop()
		core := tc.nodes[1].server.core
		requireQueued(t, core, bucket, "before the migrate")
		awaitMigrated(t, migrateInBackground(clusterOf(tc, 1), bucket), fmt.Sprintf("node 2's migrate of %q", bucket))
		requireNotQueued(t, core, bucket, "once the migrate landed the copy")
	})

	t.Run("the bucket's drop", func(t *testing.T) {
		tc, bucket, _, _, _ := increasedOnEmptyMembers(t, BucketTypeHash)
		defer tc.stop()
		tc.ok(0, transport.OpDeleteBucket, DeleteBucketRequest{Database: testDatabase, Name: bucket})
		waitForBarrier(t, tc, 0, "after-the-drop", 1, 2)
		requireNotQueued(t, tc.nodes[1].server.core, bucket, "after the bucket's drop")
	})

	t.Run("a database drop of its generation", func(t *testing.T) {
		tc, bucket, _, _, generation := increasedOnEmptyMembers(t, BucketTypeHash)
		defer tc.stop()
		core := tc.nodes[1].server.core
		if _, err := core.commitGenerationDrop(testDatabase, []int64{generation}, 0, true); err != nil {
			t.Fatalf("committing the drop of generation %d on node 2: %v", generation, err)
		}
		requireNoNeedsCopy(t, core, bucket, "after the database drop of its generation")
		requireNotQueued(t, core, bucket, "after the database drop of its generation")
	})

	t.Run("a database drop logged before generations", func(t *testing.T) {
		tc, bucket, _, _, _ := increasedOnEmptyMembers(t, BucketTypeHash)
		defer tc.stop()
		core := tc.nodes[1].server.core
		if err := core.commitDatabaseDrop(testDatabase, 0); err != nil {
			t.Fatalf("committing the drop of database %q on node 2: %v", testDatabase, err)
		}
		requireNoNeedsCopy(t, core, bucket, "after the database drop")
		requireNotQueued(t, core, bucket, "after the database drop")
	})

	t.Run("a later alter that does not place the member", func(t *testing.T) {
		tc := newTestClusterWith(t, 3, 1, func(_ int, cfg *config.Config) { cfg.Server.AutoRebalance = false })
		defer tc.stop()
		tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
		waitForAlterFeature(t, tc)
		buckets, member := pendingOn(t, tc, 1, "unplaced")
		bucket := buckets[0]
		createBucketAt(tc, 0, testDatabase, bucket, 2)
		waitForServing(t, tc, testDatabase, bucket, 2)
		raiseToThree(t, tc, bucket, member)
		waitForBarrier(t, tc, 0, "after-the-increase", member)
		core := tc.nodes[member].server.core
		requireQueued(t, core, bucket, "after the increase")
		alterAccepted(t, alterFrame(t, tc, 0, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 2}))
		waitForBarrier(t, tc, 0, "after-the-decrease", member)
		requireNoNeedsCopy(t, core, bucket, "after the decrease reached the member")
		requireNotQueued(t, core, bucket, "after the decrease reached the member")
	})
}

// TestAQueueEntryOutlivesNoDropOfItsDatabase is T-54's database-drop half for
// the queue (spec §3.3, R9.1-14.8): a database drop of the record's
// generation empties the queue live and when the member replays the drop at
// start, before its start-up scan; a drop of another generation leaves the
// recreated bucket's entry.
func TestAQueueEntryOutlivesNoDropOfItsDatabase(t *testing.T) {
	t.Run("live", func(t *testing.T) {
		tc, bucket, _, _, _ := increasedOnEmptyMembers(t, BucketTypeHash)
		defer tc.stop()
		tc.ok(0, transport.OpDeleteDB, DeleteDatabaseRequest{Name: testDatabase})
		waitForBarrier(t, tc, 0, "after-the-drop", 1, 2)
		for member := 1; member <= 2; member++ {
			requireNotQueued(t, tc.nodes[member].server.core, bucket, "after the database drop")
		}
	})

	t.Run("replayed at start", func(t *testing.T) {
		tc, bucket, _, _, _ := increasedOnEmptyMembers(t, BucketTypeHash)
		defer tc.stop()
		restarted := replayDatabaseDropOn(t, tc, 1)
		requireNoNeedsCopy(t, restarted.server.core, bucket, "once node 2 replayed the database drop at start")
		requireNotQueued(t, restarted.server.core, bucket, "once node 2 replayed the database drop at start")
	})

	t.Run("a drop of another generation", func(t *testing.T) {
		tc, bucket, _, _, generation := increasedOnEmptyMembers(t, BucketTypeHash)
		defer tc.stop()
		member := tc.nodes[1].server.core
		other := unknownGenerationOtherThan(t, member, generation)
		if _, err := member.commitGenerationDrop(testDatabase, []int64{other}, 0, true); err != nil {
			t.Fatalf("committing a drop of generation %d on node 2: %v", other, err)
		}
		requireQueued(t, member, bucket, fmt.Sprintf("after a drop of generation %d, not the bucket's %d", other, generation))
	})
}
