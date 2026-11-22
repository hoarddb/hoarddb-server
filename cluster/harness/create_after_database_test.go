package harness_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster/harness"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

// TestCreateBucketRightAfterCreateDatabaseWhenAFollowerOwnsTheDeclaration reproduces the
// failure of the first three-node RF bench rig (2026-11-16): a client creates a database
// and then, at once, a bucket in it, through the same node, as every seeder and every
// developer script does. CreateDatabase returns once the node it ran on has applied the
// command (the leader, or awaitApplied on a follower); the bucket create then writes its
// two _buckets documents at the owners of their slots, and an owner that has not yet
// applied CREATE_DATABASE refuses the forwarded write NOT_FOUND with the database id and
// the _buckets id (max uint64) in place of the names. Every in-process test so far
// called AwaitConverged between the two calls, which no client can do.
//
// The bucket name is chosen so that the declaration's slot is owned by a follower, which
// makes the forwarded write certain; the follower's lag is Raft's own commit propagation
// (no fault is injected). The right behaviour is that the create succeeds. It is run at RF 1
// and RF 2, the bug's acceptance criterion being RF 2.
func TestCreateBucketRightAfterCreateDatabaseWhenAFollowerOwnsTheDeclaration(t *testing.T) {
	for _, copies := range []int{1, 2} {
		t.Run(fmt.Sprintf("RF%d", copies), func(t *testing.T) {
			createBucketRightAfterCreateDatabase(t, copies)
		})
	}
}

func createBucketRightAfterCreateDatabase(t *testing.T, copies int) {
	t.Helper()
	c := harness.NewCluster(t, 3, harness.Options{NoDetection: true})
	leaderID := c.AwaitLeader(10 * time.Second)
	c.AwaitConverged(10 * time.Second)
	c.AwaitLeases(30 * time.Second)
	leader := c.Node(leaderID)

	for round := 0; round < 5; round++ {
		db := fmt.Sprintf("fresh%d", round)
		dbID, err := leader.CreateDatabaseRF(proofCtx(t), db, copies)
		if err != nil {
			t.Fatalf("creating database %s: %v", db, err)
		}
		// No AwaitConverged here: that is the point of the test.
		bucket, owner := bucketOwnedByAFollower(t, leader, leaderID, db, dbID)
		err = proofRetryTransient(c, func() error {
			_, createErr := leader.CreateBucket(proofCtx(t), db, bucket, wire.Field{Name: "name", Type: "string"})
			return createErr
		})
		if err != nil {
			t.Fatalf("round %d: create bucket %s in %s right after create database, declaration owned by follower %s: %v",
				round, bucket, db, owner, err)
		}
	}
}

// bucketOwnedByAFollower returns a bucket name whose declaration slot in db is owned by a
// node other than the leader, and that owner. The leader has applied the database, so it
// knows the owners.
func bucketOwnedByAFollower(t *testing.T, leader *wire.Node, leaderID, db string, dbID uint64) (string, string) {
	t.Helper()
	for i := 0; i < 256; i++ {
		name := fmt.Sprintf("B%d", i)
		encID, err := store.EncodeComponent(name)
		if err != nil {
			t.Fatal(err)
		}
		slot := storage.SlotOf(storage.PlacementKey(db, "_buckets", encID))
		if owner, ok := leader.OwnerOfSlot(slot, dbID); ok && owner != leaderID {
			return name, owner
		}
	}
	t.Fatalf("no bucket name among 256 has a declaration slot owned by a follower in %s", db)
	return "", ""
}
