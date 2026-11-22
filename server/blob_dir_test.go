package server

import (
	"bytes"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/store"
)

// newBlobDirService starts a single node whose blob payloads live in blobDir,
// and stops it when the test ends.
func newBlobDirService(t *testing.T, dataDir, blobDir string) *Server {
	t.Helper()
	qs := newCapacityService(t, func(cfg *config.Config) {
		cfg.Storage.DataDir = dataDir
		cfg.Storage.BlobDir = blobDir
	}, nil)
	t.Cleanup(func() {
		if err := qs.Stop(); err != nil {
			t.Logf("stopping server: %v", err)
		}
	})
	return qs.core
}

func TestBlobRootDefaultsUnderDataDir(t *testing.T) {
	s := newTestServer(t)
	if want := filepath.Join(s.config.Storage.DataDir, "blobs"); s.blobRoot() != want {
		t.Fatalf("blobRoot() = %q with blob_dir unset, want %q (zero-config keeps the old path)", s.blobRoot(), want)
	}
}

// TestBlobDirHoldsPayloads writes a file-backed blob on a node with blob_dir
// set and finds its payload there, and nothing under <data dir>/blobs.
func TestBlobDirHoldsPayloads(t *testing.T) {
	dataDir, blobDir := t.TempDir(), filepath.Join(t.TempDir(), "big-blobs")
	s := newBlobDirService(t, dataDir, blobDir)
	createBlobBucket(t, s, "photos")
	content := bytes.Repeat([]byte{0x42}, store.InlineThreshold+100)
	if resp, err := s.BlobWrite(BlobWriteRequest{Database: "app", Bucket: "photos", Key: "cat", Data: content}); err != nil || !resp.Success {
		t.Fatalf("blob write: %+v, %v", resp, err)
	}
	if files := payloadFilesOf(t, blobDir, lookupBucket(s, "app", "photos").id.Load()); len(files) != 1 {
		t.Fatalf("found %d payload files under the configured blob_dir %s, want 1", len(files), blobDir)
	}
	if _, err := os.Stat(filepath.Join(dataDir, "blobs")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("<data dir>/blobs exists (stat: %v); with blob_dir set, nothing belongs there", err)
	}
	read, err := s.BlobRead(BlobReadRequest{Database: "app", Bucket: "photos", Key: "cat"})
	if err != nil || !read.Found || !bytes.Equal(read.Data, content) {
		t.Fatalf("blob read after the write: found=%v, %d bytes, err %v; want the %d bytes written", read.Found, len(read.Data), err, len(content))
	}
}

// TestBlobDirReservesItsOwnName is §4(a): the reserved name follows the
// configured directory, at both places a database name is accepted, and the
// refusal names the path the operator configured.
func TestBlobDirReservesItsOwnName(t *testing.T) {
	blobDir := filepath.Join(t.TempDir(), "big-blobs")
	s := newBlobDirService(t, t.TempDir(), blobDir)

	created, err := s.CreateDatabase("big-blobs")
	if err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}
	if created.Success || !strings.Contains(created.Message, blobDir) {
		t.Errorf("create database big-blobs = %+v; want a refusal naming the configured blob_dir %s", created, blobDir)
	}
	bucket, err := s.CreateBucket(CreateBucketRequest{Database: "big-blobs", Name: "b", Type: BucketTypeBlob})
	if err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	if bucket.Success || bucket.Message != created.Message {
		t.Errorf("create bucket big-blobs.b = %+v; want the same refusal as create database: %q", bucket, created.Message)
	}

	if freed, err := s.CreateDatabase("blobs"); err != nil || !freed.Success {
		t.Errorf("create database blobs = %+v, %v; with blob_dir elsewhere, \"blobs\" names nothing of this node", freed, err)
	}
	if store, err := s.CreateDatabase("store"); err != nil || store.Success {
		t.Errorf("create database store = %+v, %v; the node store's name stays reserved", store, err)
	}
}

func TestDefaultBlobRootNameStaysReserved(t *testing.T) {
	s := newTestServer(t)
	created, err := s.CreateDatabase("blobs")
	if err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}
	if created.Success || !strings.Contains(created.Message, s.blobRoot()) {
		t.Errorf("create database blobs = %+v; want a refusal naming %s", created, s.blobRoot())
	}
}

// TestBlobDirInsideTheNodesOwnDirectoryIsRefused: those directories belong to
// the store, the keys, the WAL and the cluster state, and a payload tree
// inside one of them is a tree something else owns.
func TestBlobDirInsideTheNodesOwnDirectoryIsRefused(t *testing.T) {
	for _, inside := range []string{"", "store", "keys", filepath.Join("wal", "blobs"), "cluster"} {
		t.Run("data/"+inside, func(t *testing.T) {
			cfg := config.DefaultConfig()
			cfg.Storage.DataDir = t.TempDir()
			cfg.Storage.BlobDir = filepath.Join(cfg.Storage.DataDir, inside)
			cfg.Metrics.Enabled, cfg.Audit.Enabled = false, false
			qs, err := NewService(cfg, discardLogger())
			if err == nil {
				_ = qs.Stop()
				t.Fatalf("NewService accepted blob_dir %s inside the data directory's own %q", cfg.Storage.BlobDir, inside)
			}
			if !strings.Contains(err.Error(), "blob_dir") {
				t.Fatalf("the refusal must name the setting to change, got: %v", err)
			}
		})
	}
}

// TestBlobDirSharedByTwoNodesIsRefused: the reclaimer unlinks any payload in
// the blob tree its own store does not own, so a second node on the same
// tree would delete the first one's blobs. The second start is refused while
// the first runs, and allowed once it has stopped.
func TestBlobDirSharedByTwoNodesIsRefused(t *testing.T) {
	blobDir := t.TempDir()
	first := newCapacityService(t, func(cfg *config.Config) { cfg.Storage.BlobDir = blobDir }, nil)

	cfg := config.DefaultConfig()
	cfg.Storage.DataDir = t.TempDir()
	cfg.Storage.BlobDir = blobDir
	cfg.Metrics.Enabled, cfg.Audit.Enabled = false, false
	if qs, err := NewService(cfg, discardLogger()); err == nil {
		_ = qs.Stop()
		_ = first.Stop()
		t.Fatalf("a second node started on blob_dir %s while the first was using it", blobDir)
	} else if !strings.Contains(err.Error(), blobDir) {
		t.Errorf("the refusal must name the shared directory %s, got: %v", blobDir, err)
	}

	if err := first.Stop(); err != nil {
		t.Fatalf("stopping the first node: %v", err)
	}
	second, err := NewService(cfg, discardLogger())
	if err != nil {
		t.Fatalf("NewService after the first node stopped: %v", err)
	}
	if err := second.Stop(); err != nil {
		t.Fatalf("stopping the second node: %v", err)
	}
}

// TestMigrationBetweenDifferentBlobDirs is §4(c): the stream carries blob ids
// and bytes, never a path, so a target with its own blob_dir writes every
// payload under its own root and serves it.
func TestMigrationBetweenDifferentBlobDirs(t *testing.T) {
	sourceBlobs, targetBlobs := filepath.Join(t.TempDir(), "src-blobs"), filepath.Join(t.TempDir(), "dst-blobs")
	source := newBlobDirService(t, t.TempDir(), sourceBlobs)
	target := newBlobDirService(t, t.TempDir(), targetBlobs)

	migrated := newMigrationSource(t, source, "photos")
	if err := target.materializeRange(migratedBucket{database: "app", name: "photos", storeType: BucketTypeBlob}, migrated.stream, 0); err != nil {
		t.Fatalf("materializing on the target: %v", err)
	}
	if files := payloadFilesOf(t, targetBlobs, lookupBucket(target, "app", "photos").id.Load()); len(files) != 1 {
		t.Fatalf("the target holds %d payload files under its own blob_dir %s, want 1", len(files), targetBlobs)
	}
	read, err := target.BlobRead(BlobReadRequest{Database: "app", Bucket: "photos", Key: "big"})
	if err != nil || !read.Found || !bytes.Equal(read.Data, migrated.content) {
		t.Fatalf("reading the migrated blob on the target: found=%v, %d bytes, err %v", read.Found, len(read.Data), err)
	}
}
