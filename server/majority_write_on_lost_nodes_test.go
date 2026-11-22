package server

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// TestMajorityWriteWhoseCopiesAreAllOnLostNodesIsLostAfterRemoval pins the
// limit the replication page states: a write acknowledged at majority is on two
// of a bucket's three copies, so if both copies are on the nodes that are lost,
// the write is gone once those nodes are removed; a write at all is not. The
// third holder is down during the majority write, so the two copies are exactly
// the two that are then lost. The documents written at all survive.
func TestMajorityWriteWhoseCopiesAreAllOnLostNodesIsLostAfterRemoval(t *testing.T) {
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

	holders, err := tc.nodes[0].server.core.cluster.Replicas(placementOf(testDatabase, testBucket))
	if err != nil || len(holders) != 3 {
		t.Fatalf("the bucket's replicas: %d, %v; want 3", len(holders), err)
	}
	h := make([]int, 3)
	for i := range holders {
		h[i] = nodeIndexByAddr(t, tc.addrs, holders[i].ID)
	}
	// h[2] is down for the majority write, so only h[0] and h[1] hold it.
	down := h[2]
	lost := []int{h[0], h[1]}
	base := tc.stopNodeKeepingData(down)

	coord := 0
	for coord == down {
		coord++
	}
	if frame := tc.writeConcerned(coord, "lost-key", "majority"); frame.Opcode == transport.OpError {
		t.Fatalf("the majority write was refused with two of three copies live: %s", string(frame.Payload))
	}
	for _, i := range lost {
		if found, _ := tc.readFromNode(i, "lost-key"); !found {
			t.Fatalf("holder %s does not have the acknowledged majority write before the loss", tc.addrs[i])
		}
	}

	// The two holders that acknowledged the write are lost for good. The third
	// holder comes back, having never seen the write.
	for _, i := range lost {
		tc.stopNodeKeepingData(i)
	}
	tc.nodes[down] = tc.startNode(down, base, tc.token)

	var survivors []int
	for i := 0; i < 5; i++ {
		if i != lost[0] && i != lost[1] {
			survivors = append(survivors, i)
		}
	}
	remover := tc.nodes[survivors[0]].server.core.cluster
	for _, gone := range lost {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		err := remover.proposeRemoval(ctx, tc.addrs[gone])
		cancel()
		if err != nil {
			t.Fatalf("removing the lost node %s by majority of the survivors: %v", tc.addrs[gone], err)
		}
	}

	owner, err := remover.Owner(placementOf(testDatabase, testBucket))
	if err != nil {
		t.Fatalf("the owner after the removals: %v", err)
	}
	waitTakenOver(t, tc.nodes[nodeIndexByAddr(t, tc.addrs, owner.ID)].server.core, testDatabase, testBucket)

	// Every survivor settles on the documents written at all, and no more.
	for _, i := range survivors {
		deadline := time.Now().Add(120 * time.Second)
		for {
			frame := tc.frame(i, transport.OpCount, CountRequest{Database: testDatabase, Bucket: testBucket})
			var resp CountResponse
			if frame.Opcode != transport.OpError && bson.Unmarshal(frame.Payload, &resp) == nil && resp.Count == documents {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("survivor %s did not settle on %d documents within 120s", tc.addrs[i], documents)
			}
			time.Sleep(250 * time.Millisecond)
		}
		if found, _ := tc.readFromNode(i, "lost-key"); found {
			t.Errorf("survivor %s serves the majority write whose copies were all on lost nodes", tc.addrs[i])
		}
		if found, _ := tc.readFromNode(i, "k00"); !found {
			t.Errorf("survivor %s lost a document written at all", tc.addrs[i])
		}
	}
}
