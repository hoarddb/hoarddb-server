package server

import (
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// TestAdminRunsMigrateAndRemoveNodeOverTheListener pins the half of the
// authorization-gate fix an operator feels: the membership and rebalancing verbs now pass the
// authorization gate like any other client request, and the administrator's
// session must still be able to run them. `join` has the same check in
// TestClusterAddResolvesBareAddress.
func TestAdminRunsMigrateAndRemoveNodeOverTheListener(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})

	// With three nodes and three copies every node is in the bucket's replica
	// set, so `migrate` is valid on any of them. The create reaches a replica
	// after it is acknowledged, and a pull from an owner that is still
	// applying it is refused for that reason, so the verb is retried for a
	// short while; an authorization refusal is an error frame and fails at once.
	deadline := time.Now().Add(30 * time.Second)
	for {
		answer := tc.frame(1, transport.OpMigrate, MigrateRequest{Database: testDatabase, Bucket: testBucket})
		if answer.Opcode == transport.OpError {
			t.Fatalf("the administrator's migrate was refused: %s", answerText(answer))
		}
		var migrated MigrateResponse
		if err := bson.Unmarshal(answer.Payload, &migrated); err != nil {
			t.Fatalf("decoding the migrate answer: %v", err)
		}
		if migrated.Success {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("migrate did not succeed within the retry window: %s", migrated.Error)
		}
		time.Sleep(200 * time.Millisecond)
	}

	epochBefore := tc.nodes[0].server.core.cluster.Epoch()
	answer := tc.frame(0, transport.OpClusterRemove, ClusterRemoveRequest{NodeID: tc.addrs[2]})
	if answer.Opcode == transport.OpError {
		t.Fatalf("the administrator's remove-node was refused: %s", answerText(answer))
	}
	var removed ClusterRemoveResponse
	if err := bson.Unmarshal(answer.Payload, &removed); err != nil {
		t.Fatalf("decoding the remove-node answer: %v", err)
	}
	if !removed.Success {
		t.Fatalf("remove-node did not succeed: %s", removed.Error)
	}
	if removed.Epoch != epochBefore+1 {
		t.Errorf("epoch after the removal = %d, want %d", removed.Epoch, epochBefore+1)
	}
}
