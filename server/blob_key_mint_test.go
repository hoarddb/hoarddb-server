package server

import (
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// A keyless blob write minted its key inside BlobWriteCtx, once
// per applier. resolveDocumentIDs (server/write_path.go) resolves every
// other opcode's key before the entry is logged, so the owner, every
// replica, and every replay see the same payload — but it had no
// transport.OpBlobWrite case, so a blob write with Key: "" reached
// BlobWriteCtx on each node still empty, and each node minted its own key.
// The client got back the owner's key; the replica stored the blob under a
// different one, so a read by the returned key found it on one node and
// missed on the other, with no error anywhere.

// blobKeyMintCluster starts a two-node cluster at RF=2 (both nodes hold
// every write) with a blob bucket, so a keyless write's divergence between
// the owner and the replica is directly observable by reading the same key
// from both.
func blobKeyMintCluster(t *testing.T) *testCluster {
	t.Helper()
	tc := newTestCluster(t, 2, 2)
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeBlob})
	return tc
}

// blobReadFromNode reads a blob by key directly from one node, the way
// readFromNode does for the hash store: it asks that node's own copy rather
// than letting a coordinator fetch on its behalf.
func (tc *testCluster) blobReadFromNode(index int, key string) BlobReadResponse {
	tc.t.Helper()
	frame := tc.frame(index, transport.OpBlobRead, BlobReadRequest{
		Database: testDatabase,
		Bucket:   testBucket,
		Key:      key,
	})
	if frame.Opcode == transport.OpError {
		return BlobReadResponse{Found: false, Message: string(frame.Payload)}
	}
	var resp BlobReadResponse
	if len(frame.Payload) > 0 {
		if err := bson.Unmarshal(frame.Payload, &resp); err != nil {
			tc.t.Fatalf("decode blob read response from node %d: %v", index+1, err)
		}
	}
	return resp
}

// TestKeylessBlobWriteResolvesSameKeyOnEveryApplier is the acceptance
// test. It fails on the pre-fix tree because the owner and the replica each
// mint their own key for the same logical write: the client is handed the
// owner's key, and reading that key back from the replica finds nothing.
func TestKeylessBlobWriteResolvesSameKeyOnEveryApplier(t *testing.T) {
	tc := blobKeyMintCluster(t)
	defer tc.stop()

	content := []byte("keyless blob payload")
	writeFrame := tc.ok(0, transport.OpBlobWrite, BlobWriteRequest{
		Database: testDatabase,
		Bucket:   testBucket,
		Data:     content,
	})
	var writeResp BlobWriteResponse
	if err := bson.Unmarshal(writeFrame.Payload, &writeResp); err != nil {
		t.Fatalf("decode blob write response: %v", err)
	}
	if !writeResp.Success || writeResp.Key == "" {
		t.Fatalf("blob write did not return a minted key: %+v", writeResp)
	}

	for index := 0; index < 2; index++ {
		read := tc.blobReadFromNode(index, writeResp.Key)
		if !read.Found {
			t.Fatalf("node %d: blob %q not found — the owner and the replica minted different keys for the same write (%s)",
				index+1, writeResp.Key, read.Message)
		}
		if string(read.Data) != string(content) {
			t.Fatalf("node %d: blob %q content mismatch: got %q, want %q", index+1, writeResp.Key, read.Data, content)
		}
	}
}

// TestKeylessBlobWriteKeySurvivesRestart is a bonus arm on the same fix: the
// minted key is logged in the WAL entry itself (resolveDocumentIDs rewrites
// the payload before append), so replaying that entry on restart must
// reproduce the same key rather than minting yet another one.
//
// Both nodes are restarted, not just one: restarting only the non-owner
// replica leaves the owner's own copy — already self-consistent by
// construction, since it stores whatever key it minted and returns exactly
// that key to the client — as a silent pass that says nothing about the
// bug. Pre-fix, the owner's own WAL entry still carries an empty key too
// (resolveDocumentIDs never touched it), so replaying it after restart
// mints the owner a *new* key that also does not match what the client was
// handed, and both nodes fail this assertion the same way.
func TestKeylessBlobWriteKeySurvivesRestart(t *testing.T) {
	tc := blobKeyMintCluster(t)
	defer tc.stop()

	content := []byte("keyless blob restart payload")
	writeFrame := tc.ok(0, transport.OpBlobWrite, BlobWriteRequest{
		Database: testDatabase,
		Bucket:   testBucket,
		Data:     content,
	})
	var writeResp BlobWriteResponse
	if err := bson.Unmarshal(writeFrame.Payload, &writeResp); err != nil {
		t.Fatalf("decode blob write response: %v", err)
	}
	if !writeResp.Success || writeResp.Key == "" {
		t.Fatalf("blob write did not return a minted key: %+v", writeResp)
	}

	for index := 0; index < 2; index++ {
		tc.restartNode(index)
	}

	for index := 0; index < 2; index++ {
		// Restart replays the WAL before the node accepts a client; give
		// the replacement client a moment to finish auth before the read.
		deadline := time.Now().Add(10 * time.Second)
		var read BlobReadResponse
		for time.Now().Before(deadline) {
			read = tc.blobReadFromNode(index, writeResp.Key)
			if read.Found {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if !read.Found {
			t.Fatalf("node %d after restart: blob %q not found — replay minted a different key than the live apply (%s)",
				index+1, writeResp.Key, read.Message)
		}
		if string(read.Data) != string(content) {
			t.Fatalf("node %d after restart: blob %q content mismatch: got %q, want %q", index+1, writeResp.Key, read.Data, content)
		}
	}
}
