package server

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// TestRemovingTwoLostNodesRestoresRFOnTheSurvivors is the operator's path after
// two of five nodes at RF=3 are lost for good: remove each lost node by majority
// from a survivor, and the three survivors, which equal RF, take the buckets
// over and hold a full copy again. Every document is written at w=all before
// the losses, so every copy holds it and the check isolates the rebalance from
// the acknowledged-write loss a majority write can suffer when both of its
// copies are on the lost nodes.
func TestRemovingTwoLostNodesRestoresRFOnTheSurvivors(t *testing.T) {
	tc := newTestCluster(t, 5, 3)
	defer tc.stop()

	const documents = 20
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	for i := 0; i < documents; i++ {
		tc.frame(0, transport.OpPut, InsertRequest{
			Database: testDatabase, Bucket: testBucket, Key: fmt.Sprintf("k%02d", i),
			Data: map[string]interface{}{"n": i}, WriteConcern: "all",
		})
	}

	// Nodes 4 and 5 are lost for good: their data is not kept.
	tc.stopNodeKeepingData(3)
	tc.stopNodeKeepingData(4)

	survivor := tc.nodes[0].server.core.cluster
	for _, lost := range []int{3, 4} {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		err := survivor.proposeRemoval(ctx, tc.addrs[lost])
		cancel()
		if err != nil {
			t.Fatalf("removing the lost node %s by majority of the survivors: %v", tc.addrs[lost], err)
		}
	}
	if got := survivor.Epoch(); got != InitialEpoch+2 {
		t.Fatalf("epoch after two removals = %d, want %d", got, InitialEpoch+2)
	}
	if got := len(survivor.Members()); got != 3 {
		t.Fatalf("members after two removals = %d, want 3 (the survivors)", got)
	}

	owner, err := survivor.Owner(placementOf(testDatabase, testBucket))
	if err != nil {
		t.Fatalf("the owner after the removals: %v", err)
	}
	waitTakenOver(t, tc.nodes[nodeIndexByAddr(t, tc.addrs, owner.ID)].server.core, testDatabase, testBucket)

	// The rebalance is done when every survivor holds every document.
	for _, i := range []int{0, 1, 2} {
		deadline := time.Now().Add(120 * time.Second)
		for {
			frame := tc.frame(i, transport.OpCount, CountRequest{Database: testDatabase, Bucket: testBucket})
			var resp CountResponse
			if frame.Opcode != transport.OpError && bson.Unmarshal(frame.Payload, &resp) == nil && resp.Count == documents {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("survivor %s did not reach %d documents within 120s (last reply: opcode %v)",
					tc.addrs[i], documents, frame.Opcode)
			}
			time.Sleep(250 * time.Millisecond)
		}
	}

	// Majority writes are accepted again on the three survivors, and read back.
	if frame := tc.writeConcerned(0, "after-rebalance", "majority"); frame.Opcode == transport.OpError {
		t.Fatalf("majority write after the rebalance was rejected: %s", string(frame.Payload))
	}
	// A majority write is acknowledged by two copies, so the third survivor can
	// lag behind it briefly; the read is polled, as a read on a lagging replica
	// must be (see the replication page on consistent get).
	for _, i := range []int{0, 1, 2} {
		deadline := time.Now().Add(30 * time.Second)
		for {
			if found, _ := tc.readFromNode(i, "after-rebalance"); found {
				break
			}
			if time.Now().After(deadline) {
				t.Errorf("survivor %s cannot read the write made after the rebalance within 30s", tc.addrs[i])
				break
			}
			time.Sleep(250 * time.Millisecond)
		}
	}
}
