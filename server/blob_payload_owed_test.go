package server

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// blockPayloadWrites makes every payload-file write under the node's blob root
// fail, by putting a plain file where the root directory is: creating a leaf
// directory under it fails with "not a directory", whoever runs the test. The
// returned function puts the root back.
func blockPayloadWrites(t *testing.T, s *Server) (unblock func()) {
	t.Helper()
	root := s.blobRoot()
	aside := root + ".aside"
	_, statErr := os.Stat(root)
	hadRoot := statErr == nil
	if statErr != nil && !errors.Is(statErr, fs.ErrNotExist) {
		t.Fatalf("test setup: reading the blob root %s: %v", root, statErr)
	}
	if hadRoot {
		if err := os.Rename(root, aside); err != nil {
			t.Fatalf("test setup: moving the blob root aside: %v", err)
		}
	}
	if err := os.WriteFile(root, nil, 0o600); err != nil {
		t.Fatalf("test setup: putting a file where the blob root is: %v", err)
	}
	return func() {
		t.Helper()
		if err := os.Remove(root); err != nil {
			t.Fatalf("removing the file put where the blob root is: %v", err)
		}
		if hadRoot {
			if err := os.Rename(aside, root); err != nil {
				t.Fatalf("putting the blob root back: %v", err)
			}
		}
	}
}

// TestABlobPayloadWriteFailureIsOwedAndReplayed is the acceptance test for an owed payload.
// A blob of at least store.InlineThreshold has its payload file written before
// the batch that names it. When that write fails on the owner, the entry is
// already logged and shipped, and the replicas write their own files and hold
// the blob. The owner must owe the entry — answer unknown, and hold the blob
// once a restart replays it — rather than leave its copy without the blob for
// good. A blob bucket's put and a blobStore field's put share the payload
// write, so both are checked.
func TestABlobPayloadWriteFailureIsOwedAndReplayed(t *testing.T) {
	content := bytes.Repeat([]byte("payload "), store.InlineThreshold/8+1024)

	for _, write := range []struct {
		name string
		// create makes the bucket on its owner and returns the request that
		// writes the blob, a later write to the same bucket that does not
		// touch the blob's key, and a read of one node's own copy of the blob.
		create func(tc *testCluster, ownerIndex int, bucket string) (transport.Opcode, any, func(), func(s *Server) bool)
	}{
		{"a blob bucket's put", func(tc *testCluster, ownerIndex int, bucket string) (transport.Opcode, any, func(), func(s *Server) bool) {
			tc.ok(ownerIndex, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeBlob})
			return transport.OpBlobWrite, BlobWriteRequest{Database: testDatabase, Bucket: bucket, Key: "big", Data: content},
				func() {
					tc.ok(ownerIndex, transport.OpBlobWrite, BlobWriteRequest{Database: testDatabase, Bucket: bucket, Key: "small", Data: []byte("small")})
				},
				func(s *Server) bool {
					read, err := s.BlobRead(BlobReadRequest{Database: testDatabase, Bucket: bucket, Key: "big"})
					return err == nil && read.Found && bytes.Equal(read.Data, content)
				}
		}},
		{"a blobStore field's put", func(tc *testCluster, ownerIndex int, bucket string) (transport.Opcode, any, func(), func(s *Server) bool) {
			tc.ok(ownerIndex, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash,
				Options: storesOption(StoreDeclaration{Name: "Files", Engine: "blobStore"})})
			tc.ok(ownerIndex, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "p1",
				Data: map[string]interface{}{"title": "p1"}, WriteConcern: "all"})
			address := FieldRequest{Database: testDatabase, Bucket: bucket, Key: "p1", Field: "Files", Element: "big"}
			put := address
			put.Content = content
			put.WriteConcern = "all"
			// Another document: a later write to p1 does not commute with the
			// failed one, and replay would rightly refuse to apply it beneath.
			later := func() {
				tc.ok(ownerIndex, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "p2",
					Data: map[string]interface{}{"title": "p2"}, WriteConcern: "all"})
			}
			return transport.OpFieldPut, put, later, func(s *Server) bool {
				read, err := s.FieldGet(address)
				return err == nil && read.Found && bytes.Equal(read.Content, content)
			}
		}},
	} {
		t.Run(write.name, func(t *testing.T) {
			tc := newTestCluster(t, 3, 3)
			defer tc.stop()
			ownerIndex, _ := ownerAndReplica(t, tc)
			bucket := bucketOwnedBy(t, tc, ownerIndex, "files")
			op, request, later, holds := write.create(tc, ownerIndex, bucket)
			waitForServing(t, tc, testDatabase, bucket, 3)

			unblock := blockPayloadWrites(t, tc.nodes[ownerIndex].server.core)
			frame := tc.frame(ownerIndex, op, request)
			unblock()
			outcome, _ := bson.Raw(frame.Payload).Lookup("write_outcome").StringValueOK()
			if outcome != outcomeUnknown {
				t.Errorf("%s whose payload file failed to write on the owner was answered %s, write_outcome %q; want %q: "+
					"the entry is logged and shipped, and the replicas hold it: %s", write.name, frame.Opcode, outcome, outcomeUnknown, bson.Raw(frame.Payload))
			}
			for index := range tc.nodes {
				if index == ownerIndex {
					continue
				}
				pollUntil(t, fmt.Sprintf("node %d holds the blob", index+1), func() bool {
					return holds(tc.nodes[index].server.core)
				})
			}
			if holds(tc.nodes[ownerIndex].server.core) {
				t.Fatal("test setup: the owner holds the blob although its payload write was blocked")
			}
			// A later write moves the bucket's applied mark past the failed
			// entry, so a restart's replay retries it only if it was recorded.
			later()

			restarted := tc.restartNode(ownerIndex)
			if !holds(restarted.server.core) {
				t.Errorf("after a restart the owner does not hold the blob its replicas hold: the failed payload write was not owed, so replay never retried it")
			}
		})
	}
}
