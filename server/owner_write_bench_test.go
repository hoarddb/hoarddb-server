package server

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// The cost of an owner's write with its addressees computed inside the
// bucket's fence (bucket-replication-rev8-spec §6.2 rule F1, §14.1). The two
// benchmarks are run on the same runner before and after F1 moves the reads,
// and the build doc carries the numbers (OQ-8: a 5 % rise in the median ns/op
// of the first, or any growth in allocs/op, is a stop-and-ask).

// ownerWriteBench is a three-node cluster at factor 3 and one bucket on it,
// with the node that owns the bucket.
type ownerWriteBench struct {
	tc     *testCluster
	owner  *Cluster
	bucket string
}

// newOwnerWriteBench starts the cluster and creates the bucket on its owner.
func newOwnerWriteBench(b *testing.B) *ownerWriteBench {
	b.Helper()
	tc := newTestCluster(b, 3, 3)
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	owner := tc.nodes[0].server.core.cluster
	bucket := benchBucketOwnedBy(b, owner, testDatabase, owner.self.ID)
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	return &ownerWriteBench{tc: tc, owner: owner, bucket: bucket}
}

// run times one owner write per iteration through Cluster.applyAsOwner, at
// write concern one, so what is timed is the owner's own path: the fence, the
// append, the apply and the routing, not a replica's answer.
func (bench *ownerWriteBench) run(b *testing.B) {
	b.Helper()
	payload, err := bson.Marshal(InsertRequest{Database: testDatabase, Bucket: bench.bucket, Key: "k", Data: map[string]interface{}{"n": 1}})
	if err != nil {
		b.Fatalf("encoding the write: %v", err)
	}
	key := placementOf(testDatabase, bench.bucket)
	entry := ReplEntry{Op: int32(transport.OpPut), Bucket: bench.bucket, Key: []byte("k"), Payload: payload}
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		if _, err := bench.owner.applyAsOwner(ctx, entry, key, WcOne); err != nil {
			b.Fatalf("the owner's write failed: %v", err)
		}
	}
}

// BenchmarkOwnerWriteNoAlterPending is one owner write while no alter of any
// bucket awaits a copy: the awaiting map's count is zero.
func BenchmarkOwnerWriteNoAlterPending(b *testing.B) {
	bench := newOwnerWriteBench(b)
	bench.run(b)
}

// BenchmarkOwnerWriteWithAlterPending is BenchmarkOwnerWriteNoAlterPending
// with an increase of another bucket pending on one member, so the awaiting
// map's count is not zero.
func BenchmarkOwnerWriteWithAlterPending(b *testing.B) {
	bench := newOwnerWriteBench(b)
	other := ""
	for attempt := 0; other == "" && attempt < 512; attempt++ {
		name := fmt.Sprintf("pending-%d", attempt)
		if owner, err := bench.owner.Owner(placementOf(testDatabase, name)); err == nil && owner.ID == bench.owner.self.ID {
			other = name
		}
	}
	if other == "" {
		b.Fatal("no bucket name in 512 tries is placed on the owner")
	}
	// Declared at 1 on nodes at default 3, so the alter to 2 is an increase.
	bench.tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: other, Type: BucketTypeHash, ReplicationFactor: 1})
	payload, err := bson.Marshal(AlterBucketRequest{Database: testDatabase, Name: other, ReplicationFactor: 2})
	if err != nil {
		b.Fatalf("encoding the alter: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if frame, err := bench.owner.handleClientFrame(ctx, transport.NewFrame(transport.OpAlterBucket, payload)); err != nil || frame.Opcode == transport.OpError {
		b.Fatalf("the increase of the other bucket failed: %v", err)
	}
	if bench.owner.awaitingCount.Load() == 0 {
		b.Fatal("no alter is pending after the increase")
	}
	bench.run(b)
}
