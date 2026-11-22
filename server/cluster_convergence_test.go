package server

import (
	"fmt"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/transport"
)

// This file answers a question the existing cluster test could not: when a write
// is acknowledged at w=majority, the replica that has not applied it yet is
// *allowed* to be behind. Does it catch up on its own, and how long does that
// take?
//
// TestClusterPropagatesWritesToEveryReplica reads every node the instant the last
// write returns and requires all of them to hold everything. That is a stronger
// promise than the write concern makes: NewCluster defaults to WcMajority for
// RF>1 (cluster_state.go), so two of three nodes have applied the write when the
// client is answered, and the third is legitimately in flight. Measured failures
// have always been the third node and always a small count (1 and 6 of 30), never
// one of the two the write concern named.
//
// The test below asserts the property that *is* owed — every replica converges to
// the full set with no further traffic — and reports how long it took, so a
// convergence failure is distinguishable from the test simply being impatient.

// convergenceDeadline is deliberately just longer than one catch-up round. With
// no further writes, the only healing path is the background peer loop:
// verifyPeer gossips and then calls catchUp every 10*peerRetryInterval, with
// peerRetryInterval = 2s (cluster_state.go). So a replica that missed a ship is
// expected to be repaired within about 20 seconds, and a test that gave up sooner
// would report a hole that is really a schedule.
const convergenceDeadline = 30 * time.Second

// missingFromNode returns how many of the keys a node does not hold, and the
// first one it is missing for the failure message.
func missingFromNode(tc *testCluster, index int, keys []string) (int, string) {
	missing, first := 0, ""
	for _, key := range keys {
		if found, _ := tc.readFromNode(index, key); !found {
			missing++
			if first == "" {
				first = key
			}
		}
	}
	return missing, first
}

// TestReplicasConvergeAfterMajorityWrites is the convergence contract: a write
// acknowledged at w=majority is eventually present on every replica, without any
// further writes to nudge it along.
func TestReplicasConvergeAfterMajorityWrites(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{
		Database: testDatabase,
		Name:     testBucket,
		Type:     BucketTypeHash,
	})

	const docs = 30
	keys := make([]string, 0, docs)
	for i := 0; i < docs; i++ {
		key := fmt.Sprintf("k%02d", i)
		keys = append(keys, key)
		// w=majority on purpose: that is the default a client gets, and it is the
		// write concern whose lag we are measuring.
		if frame := tc.frame(0, transport.OpPut, InsertRequest{
			Database:     testDatabase,
			Bucket:       testBucket,
			Key:          key,
			Data:         map[string]interface{}{"n": i},
			WriteConcern: "majority",
		}); frame.Opcode == transport.OpError {
			t.Fatalf("put %s rejected: %s", key, string(frame.Payload))
		}
	}

	// What the default write concern actually promised, recorded before anything
	// waits: the nodes that are behind at this instant are not a bug.
	start := time.Now()
	initial := make([]int, len(tc.nodes))
	for index := range tc.nodes {
		initial[index], _ = missingFromNode(tc, index, keys)
	}
	t.Logf("immediately after %d majority writes, missing per node: %v", docs, initial)

	// Wait for every node to hold everything, and record when each one got there.
	converged := make([]time.Duration, len(tc.nodes))
	for index := range tc.nodes {
		converged[index] = -1
	}
	deadline := start.Add(convergenceDeadline)
	for time.Now().Before(deadline) {
		pending := 0
		for index := range tc.nodes {
			if converged[index] >= 0 {
				continue
			}
			missing, _ := missingFromNode(tc, index, keys)
			if missing == 0 {
				converged[index] = time.Since(start).Round(10 * time.Millisecond)
				continue
			}
			pending++
		}
		if pending == 0 {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}

	for index := range tc.nodes {
		if converged[index] < 0 {
			missing, first := missingFromNode(tc, index, keys)
			t.Errorf("node %d (%s) never converged: still missing %d of %d documents (first %s) after %s. "+
				"A replica that allowed a majority write to be acknowledged has not been repaired by the background catch-up loop",
				index+1, tc.addrs[index], missing, docs, first, convergenceDeadline)
			continue
		}
		t.Logf("node %d (%s): missing %d at check time, converged after %s",
			index+1, tc.addrs[index], initial[index], converged[index])
	}
}
