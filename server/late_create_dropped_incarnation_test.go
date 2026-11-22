package server

import (
	"context"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// TestACreateOfADroppedIncarnationIsRefusedAtANodeWithNoCopy is BUG-0466. A
// drop of incarnation I reaches a replica, which deletes its copy and records
// the drop; the owner's older create(I) is delivered to it afterwards. The
// replica holds nothing, so only its drop record can tell it that I is gone:
// the create must not bring the dropped bucket back.
func TestACreateOfADroppedIncarnationIsRefusedAtANodeWithNoCopy(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	const bucket = "dropped-then-late"
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, bucket, 3)
	owner := ownerIndexOf(t, tc, bucket)
	replica := (owner + 1) % 3
	node := tc.nodes[replica].server.core
	generation, incarnation, err := node.openBucketIdentity(testDatabase, bucket)
	if err != nil || incarnation == 0 {
		t.Fatalf("setup: the replica's copy identity: %d %d %v", generation, incarnation, err)
	}

	tc.ok(owner, transport.OpDeleteBucket, DeleteBucketRequest{Database: testDatabase, Name: bucket})
	if !eventually(func() bool {
		_, held := node.peekBucket(testDatabase, bucket)
		dropped, err := node.dropRecorded(testDatabase, bucket, incarnation)
		return !held && err == nil && dropped
	}) {
		t.Fatalf("setup: node %d never dropped its copy of %q and recorded incarnation %d dropped", replica+1, bucket, incarnation)
	}

	late, err := bson.Marshal(bson.D{
		{Key: "database", Value: testDatabase}, {Key: "name", Value: bucket}, {Key: "type", Value: string(BucketTypeHash)},
		{Key: "db_gen", Value: generation}, {Key: "dropped_generations", Value: []int64{}},
		{Key: "term", Value: int64(InitialEpoch)}, {Key: "incarnation", Value: int64(incarnation)},
	})
	if err != nil {
		t.Fatal(err)
	}
	const seq = 1 << 40
	c := clusterOf(tc, replica)
	// The outcome is not asserted: a refusal may come back as an error or as an
	// unsuccessful answer. What matters is that no bucket exists afterwards.
	_, _ = c.applyAsReplica(context.Background(), ReplEntry{Seq: seq, Op: int32(transport.OpCreateBucket), Bucket: bucket, Payload: late}, true, tc.addrs[owner], true)
	if _, held := node.peekBucket(testDatabase, bucket); held {
		t.Errorf("node %d holds %q again after a late create of incarnation %d, which it recorded dropped", replica+1, bucket, incarnation)
	}
}
