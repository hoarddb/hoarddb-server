package server

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// TestARemovedNodeOfATwoNodeClusterRefusesWrites is BUG-0626: a node removed
// from a two-node cluster builds its ring from the members without it, a ring
// of one, so it is not Enabled and its client frames take the one-member door
// (Server.fencedDispatch). It must refuse every mutation there as the routed
// door does (Cluster.refuseWhenRemoved): with ErrRemovedNode's sentence, the
// member to connect to, and write_outcome not_applied; and its copies stay as
// they were, since it may hold the only complete one. Guard: drop the refusal
// at the one-member door and the put is acknowledged, the create makes a
// bucket and the drop deletes the copy the removed node is documented to keep.
func TestARemovedNodeOfATwoNodeClusterRefusesWrites(t *testing.T) {
	tc := newTestCluster(t, 2, 1)
	defer tc.stop()
	const removedIndex = 1
	survivor := clusterOf(tc, 0)
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	bucket := bucketNameOwnedBy(t, survivor, testDatabase, tc.addrs[removedIndex])
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "kept",
		Data: map[string]interface{}{"n": 1}, WriteConcern: "all"})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := survivor.proposeRemoval(ctx, tc.addrs[removedIndex]); err != nil {
		t.Fatalf("removing node %d: %v", removedIndex+1, err)
	}
	removed := clusterOf(tc, removedIndex)
	if !holdsWithin(10*time.Second, removed.removed) {
		t.Fatal("setup: node 2 never learned it was removed")
	}
	if removed.Enabled() {
		t.Fatal("setup: the removed node's ring has more than one member, so it takes the routed door")
	}
	const twin = "twin-of-nothing"

	cases := []struct {
		name string
		op   transport.Opcode
		req  any
	}{
		{"put", transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "stray",
			Data: map[string]interface{}{"n": 2}, WriteConcern: "one"}},
		{"create bucket", transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: twin, Type: BucketTypeHash}},
		{"alter bucket", transport.OpAlterBucket, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 2}},
		{"drop bucket", transport.OpDeleteBucket, DeleteBucketRequest{Database: testDatabase, Name: bucket}},
		{"create database", transport.OpCreateDB, CreateDatabaseRequest{Name: "removed-made"}},
		{"drop database", transport.OpDeleteDB, DeleteDatabaseRequest{Name: testDatabase}},
	}
	for _, tt := range cases {
		frame := tc.frame(removedIndex, tt.op, tt.req)
		if frame.Opcode != transport.OpError {
			t.Errorf("%s on the removed node was answered %s: %q", tt.name, frame.Opcode, string(frame.Payload))
			continue
		}
		text := string(frame.Payload)
		if !strings.Contains(text, ErrRemovedNode.Error()) || !strings.Contains(text, "connect to "+tc.addrs[0]) {
			t.Errorf("%s on the removed node is refused without saying it was removed and which member to connect to: %q", tt.name, text)
		}
		if isDataWrite(tt.op) {
			if outcome := writeOutcomeOf(t, frame); outcome != outcomeNotApplied {
				t.Errorf("%s on the removed node is refused with write_outcome %q, want %q", tt.name, outcome, outcomeNotApplied)
			}
		}
	}

	core := tc.nodes[removedIndex].server.core
	if !core.bucketStoreOpen(testDatabase, bucket) || !holdsKeyIn(tc.nodes[removedIndex], testDatabase, bucket, "kept") {
		t.Errorf("the removed node no longer holds bucket %s and its acknowledged write: a drop reached the copy it must keep", bucket)
	}
	if holdsKeyIn(tc.nodes[removedIndex], testDatabase, bucket, "stray") {
		t.Error("the removed node applied a put after its removal")
	}
	if core.bucketStoreOpen(testDatabase, twin) {
		t.Errorf("the removed node created bucket %s after its removal", twin)
	}
}

// TestARemovedNodeOfATwoNodeClusterDoesNotAnswerReadsFromItsOwnCopy is
// BUG-0628, the read half of BUG-0626: removed_node.go promises a removed node
// "forwards every read", a three-node cluster's removed node does, and a
// two-node cluster's takes the one-member door and answered from its own copy,
// which stopped changing at the removal. After the survivor deletes a key, adds
// a key and creates a bucket, every read verb on the removed node must answer
// what the survivor answers. Guard: drop the removed-node routing of a read at
// the one-member door (Server.dispatchInFence) and each of them answers from
// the removed node's own copies.
func TestARemovedNodeOfATwoNodeClusterDoesNotAnswerReadsFromItsOwnCopy(t *testing.T) {
	tc := newTestCluster(t, 2, 1)
	defer tc.stop()
	const removedIndex = 1
	survivor := clusterOf(tc, 0)
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	bucket := bucketNameOwnedBy(t, survivor, testDatabase, tc.addrs[removedIndex])
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	for _, key := range []string{"a", "b"} {
		tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: key,
			Data: map[string]interface{}{"n": 1}, WriteConcern: "all"})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := survivor.proposeRemoval(ctx, tc.addrs[removedIndex]); err != nil {
		t.Fatalf("removing node %d: %v", removedIndex+1, err)
	}
	removed := clusterOf(tc, removedIndex)
	if !holdsWithin(10*time.Second, removed.removed) {
		t.Fatal("setup: node 2 never learned it was removed")
	}
	if removed.Enabled() {
		t.Fatal("setup: the removed node's ring has more than one member, so it takes the routed door")
	}
	if !holdsWithin(20*time.Second, func() bool { return tc.nodes[0].server.core.bucketStoreOpen(testDatabase, bucket) }) {
		t.Fatal("setup: the survivor never took the bucket over")
	}
	tc.ok(0, transport.OpDelete, DeleteRequest{Database: testDatabase, Bucket: bucket, Key: "a"})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: "made-after", Type: BucketTypeHash})

	cases := []struct {
		name string
		op   transport.Opcode
		req  any
	}{
		{"get", transport.OpGet, FindRequest{Database: testDatabase, Bucket: bucket, Key: "a"}},
		{"exists", transport.OpExists, FindRequest{Database: testDatabase, Bucket: bucket, Key: "a"}},
		{"count", transport.OpCount, CountRequest{Database: testDatabase, Bucket: bucket}},
	}
	for _, tt := range cases {
		want := tc.frame(0, tt.op, tt.req)
		got := tc.frame(removedIndex, tt.op, tt.req)
		if got.Opcode != want.Opcode || string(got.Payload) != string(want.Payload) {
			t.Errorf("%s on the removed node answered %s %q from its own copies; the survivor answers %s %q",
				tt.name, got.Opcode, string(got.Payload), want.Opcode, string(want.Payload))
		}
	}
}

// TestARemovedNodeOfATwoNodeClusterListsTheClustersBuckets is BUG-0628 for the
// listing verbs: the removed node's listing names the buckets the survivor
// made after the removal. A bucket's own row stays this node's copy's, as a
// listing says of any copy a node keeps outside the bucket's set (user docs,
// limitations), so the rows are compared by name only.
func TestARemovedNodeOfATwoNodeClusterListsTheClustersBuckets(t *testing.T) {
	tc := newTestCluster(t, 2, 1)
	defer tc.stop()
	const removedIndex = 1
	survivor := clusterOf(tc, 0)
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := survivor.proposeRemoval(ctx, tc.addrs[removedIndex]); err != nil {
		t.Fatalf("removing node %d: %v", removedIndex+1, err)
	}
	removed := clusterOf(tc, removedIndex)
	if !holdsWithin(10*time.Second, removed.removed) || removed.Enabled() {
		t.Fatal("setup: node 2 is not a removed node with a ring of one")
	}
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: "made-after", Type: BucketTypeHash})

	var listing ListBucketsResponse
	frame := tc.ok(removedIndex, transport.OpListBuckets, ListBucketsRequest{Database: testDatabase})
	if err := bson.Unmarshal(frame.Payload, &listing); err != nil {
		t.Fatalf("decoding the removed node's listing: %v", err)
	}
	for _, row := range listing.Buckets {
		if row.Name == "made-after" {
			return
		}
	}
	t.Errorf("the removed node's listing lacks bucket made-after, which the survivor made after the removal: %+v", listing.Buckets)
}
