package server

import (
	"context"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// The per-request cost of the one-member door (Server.fencedDispatch) on a
// node alone on its ring, for BUG-0630: a survivor that a removal left alone
// asks the routed owner's take-over questions of every read and write, and a
// node that was always alone asks nothing. Each pair is run interleaved on
// one host; on the survivor the bucket is its own, taken over, so what is
// timed is the steady state, not a take-over.

// loneDoorBench is a node alone on its ring with one bucket it serves.
type loneDoorBench struct {
	core   *Server
	bucket string
}

// newLoneDoorBench builds a lone node: always alone, or the survivor of a
// two-node cluster after the removal of node 2, whose bucket it owned.
func newLoneDoorBench(b *testing.B, afterARemoval bool) *loneDoorBench {
	b.Helper()
	nodes := 1
	if afterARemoval {
		nodes = 2
	}
	tc := newTestCluster(b, nodes, 1)
	b.Cleanup(tc.stop)
	lone := clusterOf(tc, 0)
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	bucket := benchBucketOwnedBy(b, lone, testDatabase, lone.self.ID)
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "k", Data: map[string]interface{}{"n": 1}, WriteConcern: "all"})
	if afterARemoval {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if err := lone.proposeRemoval(ctx, tc.addrs[1]); err != nil {
			b.Fatalf("removing node 2: %v", err)
		}
		if !holdsWithin(10*time.Second, lone.aloneAfterARemoval) {
			b.Fatal("setup: the survivor is not alone after the removal")
		}
	} else if lone.aloneAfterARemoval() {
		b.Fatal("setup: a node that was always alone answers alone after a removal")
	}
	key := placementOf(testDatabase, bucket)
	_, epoch, err := lone.ownerView(key)
	if err != nil {
		b.Fatalf("setup: reading the owner: %v", err)
	}
	if awaits, err := lone.ownerAwaitsTakeOver(key, epoch, testDatabase, bucket); err != nil || awaits {
		b.Fatalf("setup: the lone node has not taken its bucket over (awaits %v, %v)", awaits, err)
	}
	return &loneDoorBench{core: tc.nodes[0].server.core, bucket: bucket}
}

// run times one request per iteration through fencedDispatch.
func (bench *loneDoorBench) run(b *testing.B, op transport.Opcode, request any) {
	b.Helper()
	payload, err := bson.Marshal(request)
	if err != nil {
		b.Fatalf("encoding the request: %v", err)
	}
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		reply, err := bench.core.fencedDispatch(ctx, transport.NewFrameWithID(op, [transport.RequestIDLen]byte{}, payload))
		if err != nil || reply.Opcode == transport.OpError {
			b.Fatalf("the request failed: %v %q", err, reply.Payload)
		}
	}
}

func benchmarkLoneDoorGet(b *testing.B, afterARemoval bool) {
	bench := newLoneDoorBench(b, afterARemoval)
	bench.run(b, transport.OpGet, FindRequest{Database: testDatabase, Bucket: bench.bucket, Key: "k"})
}

func benchmarkLoneDoorPut(b *testing.B, afterARemoval bool) {
	bench := newLoneDoorBench(b, afterARemoval)
	bench.run(b, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bench.bucket, Key: "k", Data: map[string]interface{}{"n": 2}})
}

// BenchmarkLoneDoorGetAfterARemoval is a get at a survivor a removal left alone.
func BenchmarkLoneDoorGetAfterARemoval(b *testing.B) { benchmarkLoneDoorGet(b, true) }

// BenchmarkLoneDoorGetNeverRemoved is a get at a node that was always alone.
func BenchmarkLoneDoorGetNeverRemoved(b *testing.B) { benchmarkLoneDoorGet(b, false) }

// BenchmarkLoneDoorPutAfterARemoval is a put at a survivor a removal left alone.
func BenchmarkLoneDoorPutAfterARemoval(b *testing.B) { benchmarkLoneDoorPut(b, true) }

// BenchmarkLoneDoorPutNeverRemoved is a put at a node that was always alone.
func BenchmarkLoneDoorPutNeverRemoved(b *testing.B) { benchmarkLoneDoorPut(b, false) }
