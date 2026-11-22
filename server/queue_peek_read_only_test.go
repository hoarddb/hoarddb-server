package server

import (
	"fmt"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// TestQueuePeekIsAnsweredByTheOwner is the second half of the read-only peek fix (verifier's
// rejection). A peek promises the element the next pop takes, and a pop is
// applied on the owner, so a peek must be answered from the owner's copy too.
// Served from a replica's open store, a peek through a lagging replica showed
// an element the owner had already popped. With the owner's stream to one
// replica frozen, a pop on the owner is followed by a peek through that
// replica, for a FIFO bucket, a LIFO bucket and a fifoStore field.
func TestQueuePeekIsAnsweredByTheOwner(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	ownerIndex := ownerIndexOf(t, tc, testBucket)
	replicaIndex := (ownerIndex + 1) % 3

	fifo := bucketOwnedBy(t, tc, ownerIndex, "fifo")
	lifo := bucketOwnedBy(t, tc, ownerIndex, "lifo")
	posts := bucketOwnedBy(t, tc, ownerIndex, "posts")
	tc.ok(ownerIndex, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: fifo, Type: BucketTypeFIFO})
	tc.ok(ownerIndex, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: lifo, Type: BucketTypeLIFO})
	createFieldBucket(t, tc, ownerIndex, posts)
	for _, bucket := range []string{fifo, lifo, posts} {
		waitForServing(t, tc, testDatabase, bucket, 3)
	}
	comments := FieldRequest{Database: testDatabase, Bucket: posts, Key: "p1", Field: "Comments", WriteConcern: "all"}
	for _, name := range []string{"first", "second"} {
		for _, bucket := range []string{fifo, lifo} {
			push := transport.OpFIFOPush
			if bucket == lifo {
				push = transport.OpLIFOPush
			}
			tc.ok(ownerIndex, push, PushRequest{Database: testDatabase, Bucket: bucket, Data: map[string]interface{}{"name": name}, WriteConcern: "all"})
		}
		element := comments
		element.Data = map[string]interface{}{"name": name}
		tc.ok(ownerIndex, transport.OpFieldPush, element)
	}

	ownerCluster := tc.nodes[ownerIndex].server.core.cluster
	unfreeze := freezePeer(ownerCluster.peerFor(mustNode(t, ownerCluster, tc.addrs[replicaIndex])))
	defer unfreeze()

	nameOf := func(document bson.Raw) string {
		name, _ := document.Lookup("name").StringValueOK()
		return name
	}
	for _, queue := range []struct {
		what      string
		pop, peek transport.Opcode
		bucket    string
		left      string
	}{
		{"a FIFO bucket", transport.OpFIFOPop, transport.OpFIFOPeek, fifo, "second"},
		{"a LIFO bucket", transport.OpLIFOPop, transport.OpLIFOPeek, lifo, "first"},
	} {
		tc.ok(ownerIndex, queue.pop, bson.M{"database": testDatabase, "bucket": queue.bucket, "write_concern": "one"})
		frame := tc.ok(replicaIndex, queue.peek, PeekRequest{Database: testDatabase, Bucket: queue.bucket})
		var peeked PeekResponse
		if err := bson.Unmarshal(frame.Payload, &peeked); err != nil {
			t.Fatalf("decoding the peek of %s: %v", queue.what, err)
		}
		if got := nameOf(peeked.Items[0]); got != queue.left {
			t.Errorf("a peek of %s through a lagging replica returned %q after the owner's pop; want the owner's head %q", queue.what, got, queue.left)
		}
	}

	pop := comments
	pop.WriteConcern = "one"
	tc.ok(ownerIndex, transport.OpFieldPop, pop)
	peek := comments
	peek.WriteConcern = ""
	peeked := fieldCall(t, tc, replicaIndex, transport.OpFieldPeek, peek)
	if len(peeked.Items) != 1 {
		t.Fatalf("a peek of a fifoStore field answered %d items, want 1", len(peeked.Items))
	}
	if got := nameOf(peeked.Items[0].Element); got != "second" {
		t.Errorf("a peek of a fifoStore field through a lagging replica returned %q after the owner's pop; want the owner's head \"second\"", got)
	}
}

// TestQueuePeekAppendsNothingToAnyLog is the acceptance test for read-only peeks. A FIFO or
// LIFO peek changes nothing, so on a cluster it must be served as a read: five
// peeks, through the owner and through a node that is not the owner, leave
// every node's write-ahead log where it was. Before the fix the two queue
// peeks were missing from transport.Opcode.IsReadOnly, so replicationRequired
// sent each one down the replicated write path and every copy logged it.
func TestQueuePeekAppendsNothingToAnyLog(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})

	for _, queue := range []struct {
		bucketType BucketType
		push, peek transport.Opcode
	}{
		{BucketTypeFIFO, transport.OpFIFOPush, transport.OpFIFOPeek},
		{BucketTypeLIFO, transport.OpLIFOPush, transport.OpLIFOPeek},
	} {
		bucket := string(queue.bucketType) + "-peeks"
		tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: queue.bucketType})
		waitForServing(t, tc, testDatabase, bucket, 3)
		ownerIndex := ownerIndexOf(t, tc, bucket)
		tc.ok(ownerIndex, queue.push, PushRequest{Database: testDatabase, Bucket: bucket,
			Data: map[string]interface{}{"n": 1}, WriteConcern: "all"})

		for _, via := range []int{ownerIndex, (ownerIndex + 1) % 3} {
			before := make([]uint64, len(tc.nodes))
			for index, node := range tc.nodes {
				before[index] = node.server.core.wal.LastSequence()
			}
			for range 5 {
				tc.ok(via, queue.peek, PeekRequest{Database: testDatabase, Bucket: bucket})
			}
			for index, node := range tc.nodes {
				if after := node.server.core.wal.LastSequence(); after != before[index] {
					what := fmt.Sprintf("five %s peeks through node %d", queue.bucketType, via+1)
					t.Errorf("%s moved node %d's log from sequence %d to %d; a peek changes nothing and must log nothing",
						what, index+1, before[index], after)
				}
			}
		}
	}
}
