package server

import (
	"context"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/transport"
)

// TestTwoReplicasPullingOneBucketFromItsOwnerEachGetTheirSnapshot is the
// product half of the T-152 flake (BUG-0487). A bucket raised from one copy to
// three has two members awaiting it, and each pulls it from the owner. The
// owner keeps one pending snapshot per bucket, so the second MIGRATE_START
// replaces the first puller's snapshot and the first puller's chunk request is
// refused ("a later pull of the bucket replaced"); the first puller's migrate
// fails, though both pulls were placed and asked in order. Guard: a pull that
// the source accepted is served its chunks whatever another member's pull of
// the same bucket did since.
func TestTwoReplicasPullingOneBucketFromItsOwnerEachGetTheirSnapshot(t *testing.T) {
	// Auto-rebalance off: the members stay awaiting, so the test is the
	// only puller.
	tc := newTestClusterWith(t, 3, 1, func(_ int, cfg *config.Config) { cfg.Server.AutoRebalance = false })
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitForAlterFeature(t, tc)
	bucket := bucketOwnedBy(t, tc, 0, "sessions")
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	putDocument(tc, 0, testDatabase, bucket, "doc")
	alterAccepted(t, alterFrame(t, tc, 0, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 3}))

	owner := tc.nodes[0].server.core
	incarnation := owner.wal.Incarnation()
	start := func(member int) MigrateStartResponse {
		t.Helper()
		payload, err := bson.Marshal(MigrateStartRequest{Token: owner.cluster.localToken(), Database: testDatabase, Bucket: bucket,
			FromNode: tc.addrs[member], Epoch: owner.cluster.Epoch(), Store: tc.nodes[member].server.core.storeClaim(),
			Incarnation: incarnation[:]})
		if err != nil {
			t.Fatalf("encoding node %d's MIGRATE_START: %v", member+1, err)
		}
		frame, err := handleMigrateStart(context.Background(), transport.NewFrame(transport.OpMigrateStart, payload), owner)
		if err != nil {
			t.Fatalf("the owner's handler failed node %d's MIGRATE_START: %v", member+1, err)
		}
		var started MigrateStartResponse
		if err := bson.Unmarshal(frame.Payload, &started); err != nil || !started.Ok {
			t.Fatalf("the owner refused node %d's MIGRATE_START: %+v, %v", member+1, started, err)
		}
		return started
	}
	chunk := func(member int, started MigrateStartResponse) MigrateDataResponse {
		t.Helper()
		payload, err := bson.Marshal(MigrateDataRequest{Token: owner.cluster.localToken(), Database: testDatabase, Bucket: bucket,
			Index: 0, Transfer: started.Transfer})
		if err != nil {
			t.Fatalf("encoding node %d's MIGRATE_DATA: %v", member+1, err)
		}
		frame, err := handleMigrateData(context.Background(), transport.NewFrame(transport.OpMigrateData, payload), owner)
		if err != nil {
			t.Fatalf("the owner's handler failed node %d's MIGRATE_DATA: %v", member+1, err)
		}
		var served MigrateDataResponse
		if err := bson.Unmarshal(frame.Payload, &served); err != nil {
			t.Fatalf("decoding node %d's MIGRATE_DATA answer: %v", member+1, err)
		}
		return served
	}

	first := start(1)
	second := start(2)
	if served := chunk(1, first); !served.Ok {
		t.Errorf("node 2's pull was accepted, then node 3's pull of the same bucket began; node 2's chunk was refused: %s", served.Error)
	}
	if served := chunk(2, second); !served.Ok {
		t.Errorf("node 3's chunk was refused: %s", served.Error)
	}
}

// clearAllPending drops every snapshot a test left held for the bucket.
func clearAllPending(c *Cluster, key string) {
	c.migrateMu.Lock()
	defer c.migrateMu.Unlock()
	c.forgetPendingLocked(key)
}
