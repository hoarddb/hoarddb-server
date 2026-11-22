package server

import (
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// holdersAnsweringStats polls every node's BucketStats for one bucket until
// want of them answer, and returns those answers by node index. A node outside
// the bucket's replica set answers with the not-holder error and is skipped.
func holdersAnsweringStats(t *testing.T, tc *testCluster, database, bucket string, want int) map[int]*BucketStatsResponse {
	t.Helper()

	deadline := time.Now().Add(15 * time.Second)
	for {
		answers := map[int]*BucketStatsResponse{}
		for index, node := range tc.nodes {
			answer, err := node.server.core.BucketStats(BucketStatsRequest{Database: database, Bucket: bucket})
			if err == nil && answer.Found {
				answers[index] = answer
			}
		}
		if len(answers) >= want {
			return answers
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d nodes answer BucketStats for %s.%s, want %d", len(answers), database, bucket, want)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestDescribeBucketPrintsTheStoredFactorFromANodeThatDoesNotHoldIt is step
// 4.2a. Five nodes run at the default factor of 1 and one bucket declares 3.
// A client connected to each node asks it to describe the bucket. The nodes
// that hold the bucket answer from the catalog record their read view already
// resolved; the nodes that do not hold it forward the read to the owner
// (Cluster.serveRead), so every node answers with the bucket's own stored
// factor. An answering node that read its own configured default would say 1
// for "wide"; one that did not forward would answer the not-holder error. The
// factor comes from the resolved catalog record: O(1), no ring lookup beyond
// the routing every read does.
func TestDescribeBucketPrintsTheStoredFactorFromANodeThatDoesNotHoldIt(t *testing.T) {
	tc := newTestCluster(t, 5, 1)
	defer tc.stop()

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	createBucketAt(tc, 0, testDatabase, "wide", 3)
	createBucketAt(tc, 0, testDatabase, "narrow", 0)

	for _, want := range []struct {
		bucket    string
		factor    int
		inherited bool
		holders   int
	}{
		{"wide", 3, false, 3},
		{"narrow", 1, true, 1},
	} {
		// Wait for the bucket's copies to land, and confirm at least one node
		// does not hold it, or the forward path this test names goes untested.
		holders := holdersAnsweringStats(t, tc, testDatabase, want.bucket, want.holders)
		if len(holders) >= len(tc.nodes) {
			t.Fatalf("every node holds %s.%s, so no non-holder answers it", testDatabase, want.bucket)
		}
		for index := range tc.nodes {
			frame := tc.frame(index, transport.OpBucketStats, BucketStatsRequest{Database: testDatabase, Bucket: want.bucket})
			if frame.Opcode == transport.OpError {
				t.Fatalf("node %d answered describe of %s.%s with an error: %s", index, testDatabase, want.bucket, string(frame.Payload))
			}
			var answer BucketStatsResponse
			if err := bson.Unmarshal(frame.Payload, &answer); err != nil {
				t.Fatalf("decoding the describe answer from node %d: %v", index, err)
			}
			if !answer.Found || answer.ReplicationFactor != want.factor || answer.Inherited != want.inherited {
				t.Errorf("node %d describes %s as found=%v factor=%d (inherited %v), want found=true factor=%d (inherited %v)",
					index, want.bucket, answer.Found, answer.ReplicationFactor, answer.Inherited, want.factor, want.inherited)
			}
		}
	}
}
