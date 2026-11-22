package server

import (
	"errors"
	"testing"

	"github.com/hoarddb/hoarddb-server/transport"
)

// Step 11.2 (dispatch 11): replay stamps the version its copies carry.

// TestAReplayedWriteCarriesTheVersionItsCopiesCarry: a put whose apply a node
// makes only on replay carries the version the other copies carry. On the
// owner, the put's store apply fails (owed) and the owner is restarted; on a
// replica, the put's store apply fails after its append, and the replica is
// restarted with its stream from the owner frozen, so the only apply it can
// make is replay's. Guard: drop the replay's withWriteVersion (the replayed
// copy has no version).
func TestAReplayedWriteCarriesTheVersionItsCopiesCarry(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, testBucket, 3)
	owner, replicas := ownerAndReplicas(t, tc, testDatabase, testBucket)
	requireEveryCopy(t, putValue(t, tc, owner, "warm", "w", "all"), "the warm-up put")

	t.Run("owner", func(t *testing.T) {
		failed := failNextApplyOn(t, tc.nodes[owner].server.core.kv, anySeq)
		putValue(t, tc, owner, "owed", "o", "all")
		if failed.Load() == 0 {
			t.Fatal("setup: the owner's apply of the put did not fail")
		}
		for _, index := range replicas {
			pollUntil(t, "the replicas apply the put the owner owes", func() bool {
				found, _, _, err := tc.nodes[index].server.core.versionedFind(testDatabase, testBucket, "owed")
				return err == nil && found
			})
		}
		tc.restartNode(owner)
		replayed := versionOn(t, tc.nodes[owner].server.core, testDatabase, testBucket, "owed")
		for _, index := range replicas {
			if version := versionOn(t, tc.nodes[index].server.core, testDatabase, testBucket, "owed"); version != replayed {
				t.Errorf("the owner replayed the put at version %d and node %d holds it at %d", replayed, index+1, version)
			}
		}
	})

	t.Run("replica", func(t *testing.T) {
		// A replica stopped while a hold in beforeLocalApplyHook is waiting
		// does not stop (C-3, run: Service.Stop waits for the held apply), so
		// the replica's commit is failed instead: the put is in its log and
		// not in its store, and its stream from the owner is frozen before
		// the owner can re-send it, so the only apply it can make is replay's.
		// A replica voids an entry its store failed (BUG-0570), so that replay
		// leaves it to the owner's re-send; the void is failed here, as a crash
		// between the failure and the void leaves it, and replay applies the put.
		replica := replicas[0]
		replicaID := tc.addrs[replica]
		voidHook := func(c *Cluster, _ string) error {
			if c.self.ID != replicaID {
				return nil
			}
			return errors.New("injected crash before the void of the failed put commits")
		}
		orphanVoidFaultHook.Store(&voidHook)
		defer orphanVoidFaultHook.Store(nil)
		failed := failNextApplyOn(t, tc.nodes[replica].server.core.kv, anySeq)
		putValue(t, tc, owner, "held", "h", "majority")
		pollUntil(t, "the replica's apply of the put fails", func() bool { return failed.Load() != 0 })
		ownerCluster := clusterOf(tc, owner)
		unfreeze := freezePeer(ownerCluster.peerFor(mustNode(t, ownerCluster, tc.addrs[replica])))
		defer unfreeze()
		if found, _, _, err := tc.nodes[replica].server.core.versionedFind(testDatabase, testBucket, "held"); err != nil || found {
			t.Fatalf("setup: the replica holds the put before its restart (found=%v, %v): a re-send reached it", found, err)
		}
		tc.restartNode(replica)
		replayed := versionOn(t, tc.nodes[replica].server.core, testDatabase, testBucket, "held")
		if version := versionOn(t, tc.nodes[owner].server.core, testDatabase, testBucket, "held"); version != replayed {
			t.Errorf("the replica replayed the put at version %d and the owner holds it at %d", replayed, version)
		}
	})
}
