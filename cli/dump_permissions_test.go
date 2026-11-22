package cli

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// seedDumpBucket creates app/users with one document on a test server.
func seedDumpBucket(t *testing.T, c *CLI) {
	t.Helper()
	if err := c.handler.send(transport.OpCreateDB, &server.CreateDatabaseRequest{Name: "app"}, nil); err != nil {
		t.Fatalf("create database: %v", err)
	}
	if err := c.handler.send(transport.OpCreateBucket, &server.CreateBucketRequest{
		Database: "app", Name: "users", Type: server.BucketTypeHash,
	}, nil); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	if err := c.handler.send(transport.OpPut, &server.InsertRequest{
		Database: "app", Bucket: "users", Key: "alice", Data: map[string]interface{}{"tag": "alice-tag"},
	}, nil); err != nil {
		t.Fatalf("insert: %v", err)
	}
}

func assertMode(t *testing.T, path string, want os.FileMode) {
	t.Helper()
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	if got := info.Mode().Perm(); got != want {
		t.Errorf("%s mode = %o, want %o", path, got, want)
	}
}

// TestDumpCreatesPrivateFilesAndDirectories pins dump output to the owner:
// every file 0600 and every directory the dump creates 0700, whatever the
// process umask.
func TestDumpCreatesPrivateFilesAndDirectories(t *testing.T) {
	previousMask := setUmaskForTest(0)
	t.Cleanup(func() { setUmaskForTest(previousMask) })

	c := newDumpTestServer(t)
	seedDumpBucket(t, c)

	outDir := filepath.Join(t.TempDir(), "dump")
	if _, err := runDumpBucketsForTest(t, c.handler, outDir, []dumpBucketTarget{{Database: "app", Bucket: "users"}}); err != nil {
		t.Fatalf("dump: %v", err)
	}
	assertMode(t, outDir, 0o700)
	assertMode(t, filepath.Join(outDir, "app"), 0o700)
	assertMode(t, filepath.Join(outDir, "manifest.json"), 0o600)
	assertMode(t, filepath.Join(outDir, "app", "users.bson"), 0o600)
	assertMode(t, filepath.Join(outDir, "app", "users.metadata.json"), 0o600)
}

// TestSectionFileWriterCreatesPrivateFile covers the stores and dead section
// files, which only exist for some buckets.
func TestSectionFileWriterCreatesPrivateFile(t *testing.T) {
	previousMask := setUmaskForTest(0)
	t.Cleanup(func() { setUmaskForTest(previousMask) })

	sectionPath := filepath.Join(t.TempDir(), "users.stores.bson")
	section := sectionFileWriter{path: sectionPath, hasher: newTestHasher()}
	if err := section.write([]byte("x")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := section.close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	assertMode(t, sectionPath, 0o600)
}

// TestDumpNarrowsAnExistingFileAndRefusesALink fixes what a re-dump into an
// existing dump does: a regular file is rewritten and narrowed to 0600, and a
// symbolic link in its place is refused and never written through.
func TestDumpNarrowsAnExistingFileAndRefusesALink(t *testing.T) {
	previousMask := setUmaskForTest(0)
	t.Cleanup(func() { setUmaskForTest(previousMask) })

	c := newDumpTestServer(t)
	seedDumpBucket(t, c)

	outDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(outDir, "manifest.json"), []byte(`{"format_version":1}`), 0o644); err != nil {
		t.Fatalf("seeding manifest: %v", err)
	}
	dbDir := filepath.Join(outDir, "app")
	if err := os.Mkdir(dbDir, 0o755); err != nil {
		t.Fatalf("seeding database directory: %v", err)
	}
	bsonPath := filepath.Join(dbDir, "users.bson")
	staleTail := "stale" + strings.Repeat("STALE-TAIL", 1000)
	if err := os.WriteFile(bsonPath, []byte(staleTail), 0o644); err != nil {
		t.Fatalf("seeding bucket file: %v", err)
	}

	summary, err := runDumpBucketsForTest(t, c.handler, outDir, []dumpBucketTarget{{Database: "app", Bucket: "users"}})
	if err != nil || summary.incomplete {
		t.Fatalf("re-dump: %v, incomplete=%v", err, summary != nil && summary.incomplete)
	}
	assertMode(t, bsonPath, 0o600)
	// The new dump is shorter than the file it replaces, so a missing O_TRUNC
	// leaves the old tail after the new bytes.
	if content, err := os.ReadFile(bsonPath); err != nil || bytes.Contains(content, []byte("STALE-TAIL")) || len(content) >= len(staleTail) {
		t.Fatalf("existing bucket file was not rewritten whole: %d bytes, %v", len(content), err)
	}

	victim := filepath.Join(t.TempDir(), "victim")
	if err := os.WriteFile(victim, []byte("keep"), 0o644); err != nil {
		t.Fatalf("seeding link target: %v", err)
	}
	if err := os.Remove(bsonPath); err != nil {
		t.Fatalf("removing bucket file: %v", err)
	}
	if err := os.Symlink(victim, bsonPath); err != nil {
		t.Fatalf("creating link: %v", err)
	}
	summary, err = runDumpBucketsForTest(t, c.handler, outDir, []dumpBucketTarget{{Database: "app", Bucket: "users"}})
	if err != nil {
		t.Fatalf("second re-dump: %v", err)
	}
	if !summary.incomplete {
		t.Error("a link in place of the bucket file was written through, want it refused")
	}
	if content, err := os.ReadFile(victim); err != nil || string(content) != "keep" {
		t.Errorf("link target = %q, %v, want it untouched", content, err)
	}
}
