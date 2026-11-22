package server

import (
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/transport"
)

// ownerSmallerCluster is three nodes at replication factor 3, node 1 (the
// owner of the buckets these tests use) at the 32 MiB floor and the other two
// at the test default of 64 MiB. It is the mirror of memtableMixedCluster: here
// the owner has the smaller memtable, so it refuses a batch its members commit.
func ownerSmallerCluster(t *testing.T) *testCluster {
	t.Helper()
	return newTestClusterWith(t, 3, 3, func(index int, cfg *config.Config) {
		if index == 0 {
			cfg.Storage.MemtableMB = config.MinMemtableMB
		}
	})
}

// TestAnOwnerRefusalOfAQueueBatchLeavesItOnLargerMembersSoHalvesDouble is the
// premise of the ingest unknown-outcome bug. The 32 MiB owner refuses a 7.2 MB
// push batch with BATCH_TOO_LARGE and write_outcome unknown, and its 64 MiB
// members apply that same logged entry. A client that then halves the batch, as
// 'hoarddb ingest' did on any BATCH_TOO_LARGE, pushes the entries again: each
// member holds them twice, because a queue appends and never overwrites.
func TestAnOwnerRefusalOfAQueueBatchLeavesItOnLargerMembersSoHalvesDouble(t *testing.T) {
	tc := ownerSmallerCluster(t)
	defer tc.stop()
	owner := tc.nodes[0].server.core.cluster
	bucket := bucketNameOwnedBy(t, owner, "app", tc.addrs[0])
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: bucket, Type: BucketTypeFIFO})

	const pushed = 60
	whole := largeBatch("app", bucket, pushed, 120_000)
	refusal := errorAnswerOf(t, tc.frame(0, transport.OpBatchPut, whole), "the over-bound batch")
	requireOutcome(t, refusal, "the owner's refusal of an over-bound queue batch", outcomeUnknown, transport.ErrBatchTooLarge)

	// The members apply what the owner refused.
	tc.requireMemberCounts(t, bucket, pushed, []int{1, 2}, "after the owner's refusal, on the members")
	if held := tc.bucketCountOn(0, "app", bucket); held != 0 {
		t.Fatalf("the owner refused the batch but holds %d of its %d entries", held, pushed)
	}

	for _, half := range [][]map[string]interface{}{whole.Data[:pushed/2], whole.Data[pushed/2:]} {
		tc.ok(0, transport.OpBatchPut, BatchInsertRequest{Database: "app", Bucket: bucket, Data: half, WriteConcern: "one"})
	}
	tc.requireMemberCounts(t, bucket, 2*pushed, []int{1, 2}, "after the halves landed, on the members")
}

// requireMemberCounts requires the listed nodes' own copies of bucket to hold
// want, waiting for replicas still applying.
func (tc *testCluster) requireMemberCounts(t *testing.T, bucket string, want int64, indexes []int, when string) {
	t.Helper()
	counts := make([]int64, len(indexes))
	deadline := time.Now().Add(60 * time.Second)
	for {
		wrong := false
		for position, index := range indexes {
			counts[position] = tc.bucketCountOn(index, "app", bucket)
			wrong = wrong || counts[position] != want
		}
		if !wrong {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: nodes %v hold %v entries of %q, want %d each", when, indexes, counts, bucket, want)
		}
		time.Sleep(200 * time.Millisecond)
	}
}
