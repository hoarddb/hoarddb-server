package server

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/transport"
)

// TestMigrateDoneAtALeavingOwnerKeepsItsCopy is step 10b.7's "handleMigrateDone
// deletes nothing" on a running node: a join moves a bucket's owner slot to the
// newcomer and the old owner O out of the set. O alone holds five writes at
// one (its streams to both replicas held). The newcomer's take-over copies from
// O. O keeps its copy after the pull's MIGRATE_DONE while the set members lack
// the writes: it is released by the cover test only. Guard: delete the
// source's copy in handleMigrateDone — the only copies of the writes outside
// the newcomer go with it.
func TestMigrateDoneAtALeavingOwnerKeepsItsCopy(t *testing.T) {
	tc := newTestClusterWith(t, 4, 3, func(_ int, cfg *config.Config) { cfg.Server.WALRetentionSeconds = 0 })
	defer tc.stop()
	ports := reserveNodePorts(t)
	newcomer := ports.ringNode()
	before := ringWith(t, 3, tc.addrs...)
	after := ringWith(t, 3, append(append([]string(nil), tc.addrs...), newcomer.ID)...)
	var owner, a, b int
	bucket := bucketNamed(t, "leave", func(name string) bool {
		old, next := replicaIDs(t, before, 3, name), replicaIDs(t, after, 3, name)
		if next[0] != newcomer.ID || slices.Contains(next, old[0]) {
			return false
		}
		owner = nodeIndexByAddr(t, tc.addrs, old[0])
		a = nodeIndexByAddr(t, tc.addrs, old[1])
		b = nodeIndexByAddr(t, tc.addrs, old[2])
		return true
	})
	tc.ok(owner, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(owner, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, bucket, 3)
	releaseA := holdStreamPushesOnly(peerOf(t, tc, owner, a))
	defer releaseA()
	releaseB := holdStreamPushesOnly(peerOf(t, tc, owner, b))
	defer releaseB()
	for i := 0; i < 5; i++ {
		tc.ok(owner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: fmt.Sprintf("k%d", i),
			Data: map[string]interface{}{"n": i}, WriteConcern: "one"})
	}
	joinFreezeOffHook.Store(true)
	defer joinFreezeOffHook.Store(false)
	joiner := joinNewcomer(t, tc, owner, ports)
	opened := holdsWithin(takeOverWait, func() bool {
		core := joiner.node.server.core
		joiner.cluster().wakeTakeOver(testDatabase, bucket)
		if !core.bucketStoreOpen(testDatabase, bucket) {
			return false
		}
		_, op, err := core.bucketLineage(testDatabase, bucket)
		return err == nil && op && holdsKeyLocally(joiner.node, bucket, "k4")
	})
	if !opened {
		key := placementOf(testDatabase, bucket)
		t.Fatalf("setup: the newcomer never took %s over: status=%+v", bucket, joiner.cluster().takeOverStatusOf(key))
	}
	end := time.Now().Add(3 * time.Second)
	for time.Now().Before(end) {
		lackA, lackB := !holdsKeyLocally(tc.nodes[a], bucket, "k4"), !holdsKeyLocally(tc.nodes[b], bucket, "k4")
		if (lackA || lackB) && !tc.nodes[owner].server.core.bucketStoreOpen(testDatabase, bucket) {
			t.Fatalf("the old owner, node %d, outside the set, released %s while set members lack its writes (node %d lacks: %v, node %d lacks: %v): only the newcomer holds them",
				owner+1, bucket, a+1, lackA, b+1, lackB)
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Logf("the old owner kept its copy; node %d lacks=%v node %d lacks=%v", a+1, !holdsKeyLocally(tc.nodes[a], bucket, "k4"), b+1, !holdsKeyLocally(tc.nodes[b], bucket, "k4"))
}
