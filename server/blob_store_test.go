package server

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// writeBlobThroughDispatch writes through the opcode handler, the path a live
// client, a replicated apply and WAL replay all take.
func writeBlobThroughDispatch(t *testing.T, s *Server, ctx context.Context, database, bucket, key string, content []byte) {
	t.Helper()
	payload, err := bson.Marshal(BlobWriteRequest{Database: database, Bucket: bucket, Key: key, Data: content})
	if err != nil {
		t.Fatalf("encoding blob write: %v", err)
	}
	frame, err := s.dispatch(ctx, &transport.Frame{Opcode: transport.OpBlobWrite, Payload: payload})
	if err != nil {
		t.Fatalf("dispatching blob write of %s: %v", key, err)
	}
	var resp BlobWriteResponse
	if err := bson.Unmarshal(frame.Payload, &resp); err != nil || !resp.Success {
		t.Fatalf("blob write of %s: %+v (decode: %v, frame opcode %v)", key, resp, err, frame.Opcode)
	}
}

// readBlob reads key's content back, failing the test if it is not there.
func readBlob(t *testing.T, s *Server, database, bucket, key string) []byte {
	t.Helper()
	resp, err := s.BlobRead(BlobReadRequest{Database: database, Bucket: bucket, Key: key})
	if err != nil || !resp.Found {
		t.Fatalf("blob read of %s: %+v, %v", key, resp, err)
	}
	return resp.Data
}

// TestBlobBucketLivesOnTheNodeStore is build doc Step 13 at the server: a blob
// bucket opens no Badger instance and makes no directory of its own, on create
// and — because Server.LoadCatalog attaches every catalogued bucket through the
// same path — on reopen. Its large payloads are in the node-wide tree under a
// name that parses to the bucket's recorded slot and id (Part A).
func TestBlobBucketLivesOnTheNodeStore(t *testing.T) {
	const database, bucket = "app", "files"
	dataDir := t.TempDir()
	first := startCatalogServerAt(t, dataDir)
	createTestBucket(t, first.core, database, bucket, BucketTypeBlob)
	small := bytes.Repeat([]byte{'s'}, 100)
	large := bytes.Repeat([]byte{'l'}, store.InlineThreshold+1)
	writeBlobThroughDispatch(t, first.core, context.Background(), database, bucket, "small", small)
	writeBlobThroughDispatch(t, first.core, context.Background(), database, bucket, "large", large)

	ownDir := filepath.Join(dataDir, database, bucket)
	if _, err := os.Stat(ownDir); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("a blob bucket made a directory of its own, %s (stat: %v); it must live on the node's store", ownDir, err)
	}

	second := restartCatalog(t, first, dataDir)
	if _, err := os.Stat(ownDir); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("reopening a blob bucket made a directory of its own, %s (stat: %v)", ownDir, err)
	}
	if got := readBlob(t, second.core, database, bucket, "small"); !bytes.Equal(got, small) {
		t.Fatalf("after a restart the small blob reads %d bytes, want %d", len(got), len(small))
	}
	if got := readBlob(t, second.core, database, bucket, "large"); !bytes.Equal(got, large) {
		t.Fatalf("after a restart the large blob reads %d bytes, want %d", len(got), len(large))
	}
	counted, err := second.core.Count(CountRequest{Database: database, Bucket: bucket})
	if err != nil || counted.Count != 2 {
		t.Fatalf("count after a restart: %+v, %v; want 2", counted, err)
	}

	served := lookupBucket(second.core, database, bucket)
	var names []store.PayloadName
	err = filepath.WalkDir(filepath.Join(dataDir, blobRootDirName), func(path string, entry fs.DirEntry, err error) error {
		if err != nil || !entry.Type().IsRegular() {
			return err
		}
		name, ok := store.ParsePayloadFileName(entry.Name())
		if !ok {
			t.Errorf("payload file %s does not parse", path)
		}
		names = append(names, name)
		return nil
	})
	if err != nil {
		t.Fatalf("walking the blob tree: %v", err)
	}
	if len(names) != 1 || names[0].Slot != served.slot || names[0].BucketID != served.id.Load() {
		t.Fatalf("the blob tree holds %+v; want one payload of slot %d, bucket %d", names, served.slot, served.id.Load())
	}
}

// TestReplayedBlobWriteIsGated is spec §4.4 rule 2 for a blob: a replayed
// write at or below the bucket's applied_seq changes nothing. The blob write
// handler carried no context before blob moved onto the node store, so a
// replayed write put its old content back over a newer one.
func TestReplayedBlobWriteIsGated(t *testing.T) {
	const database, bucket = "app", "gated-files"
	s := newTestServer(t)
	createTestBucket(t, s, database, bucket, BucketTypeBlob)
	writeBlobThroughDispatch(t, s, withApplySeq(context.Background(), 1), database, bucket, "k", []byte("first"))
	writeBlobThroughDispatch(t, s, withApplySeq(context.Background(), 2), database, bucket, "k", []byte("second"))

	writeBlobThroughDispatch(t, s, withReplay(withApplySeq(context.Background(), 1)), database, bucket, "k", []byte("first"))
	if got := readBlob(t, s, database, bucket, "k"); string(got) != "second" {
		t.Fatalf("after replaying the write at sequence 1, below applied_seq 2, key k reads %q, want %q", got, "second")
	}
	if st := bucketStateOf(t, s, database, bucket); st.AppliedSeq != 2 || st.Entries != 1 {
		t.Fatalf("state after a gated replay: applied_seq %d, entries %d; want 2 and 1", st.AppliedSeq, st.Entries)
	}
}
