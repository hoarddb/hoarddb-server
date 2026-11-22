package server

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// sendOn sends one request to a node and returns its answer, refusals
// included. It takes no *testing.T: a test runs it on a goroutine of its own.
func sendOn(node *testNode, op transport.Opcode, req any) (*transport.Frame, error) {
	payload, err := bson.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("encoding %s: %w", op, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	frame, err := node.client.SendRequest(ctx, op, payload)
	if err != nil {
		return nil, fmt.Errorf("sending %s to %s: %w", op, node.addr, err)
	}
	if frame.Opcode == transport.OpError {
		var refusal ErrorResponse
		_ = bson.Unmarshal(frame.Payload, &refusal)
		return nil, fmt.Errorf("%s was refused, code %d: %s", op, refusal.Code, refusal.Message)
	}
	return frame, nil
}

// TestOwnerServedReadsTakeNoBucketLock is T-1 (query-pipelining spec §3.5,
// I-1) on the cluster path: a read the bucket's owner serves for a client goes
// through Cluster.serveRead before any handler, and must not queue behind a
// writer that holds the bucket's lock for its apply either. T-1 itself calls
// the Server's read methods directly and cannot see that path. On the join-fence
// fix as first written, the join fence's store check took the bucket's read
// lock on every owner-served read, and a get and a peek through the owner
// waited for the writer.
func TestOwnerServedReadsTakeNoBucketLock(t *testing.T) {
	tc := newTestCluster(t, 2, 2)
	t.Cleanup(tc.stop)
	const hashBucket, fifoBucket = "lockfreeread", "lockfreepeek"
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: hashBucket, Type: BucketTypeHash})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: fifoBucket, Type: BucketTypeFIFO})
	waitForServing(t, tc, testDatabase, hashBucket, 2)
	waitForServing(t, tc, testDatabase, fifoBucket, 2)
	tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: hashBucket, Key: "k",
		Data: map[string]interface{}{"n": 1}, WriteConcern: "all"})
	tc.ok(0, transport.OpFIFOPush, PushRequest{Database: testDatabase, Bucket: fifoBucket,
		Data: map[string]interface{}{"n": 1}, WriteConcern: "all"})

	ownerOf := func(bucket string) int {
		return nodeIndexByAddr(t, tc.addrs, mustReplicaIDs(t, tc.nodes[0].server.core.cluster, testDatabase, bucket)[0])
	}
	hashOwner, fifoOwner := tc.nodes[ownerOf(hashBucket)], tc.nodes[ownerOf(fifoBucket)]

	for _, held := range []struct {
		bucket string
		path   readPath
	}{
		{hashBucket, readPath{"a get through the owner", func() error {
			frame, err := sendOn(hashOwner, transport.OpGet, FindRequest{Database: testDatabase, Bucket: hashBucket, Key: "k"})
			if err != nil {
				return err
			}
			var found FindResponse
			if err := bson.Unmarshal(frame.Payload, &found); err != nil || !found.Found {
				return fmt.Errorf("the get did not find the document (err %v)", err)
			}
			return nil
		}}},
		{fifoBucket, readPath{"a peek through the owner", func() error {
			frame, err := sendOn(fifoOwner, transport.OpFIFOPeek, PeekRequest{Database: testDatabase, Bucket: fifoBucket})
			if err != nil {
				return err
			}
			var peeked PeekResponse
			if err := bson.Unmarshal(frame.Payload, &peeked); err != nil || !peeked.Found {
				return fmt.Errorf("the peek found no element (err %v)", err)
			}
			return nil
		}}},
	} {
		served, err := tc.nodes[ownerOf(held.bucket)].server.core.resolveBucket(testDatabase, held.bucket)
		if err != nil {
			t.Fatalf("resolving %s on its owner: %v", held.bucket, err)
		}
		served.rec.mu.Lock()
		runReadPathsUnderLock(t, []readPath{held.path}, served.rec.mu.Unlock)
	}
}
