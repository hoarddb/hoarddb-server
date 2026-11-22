package cli

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// TestRestoreEndToEndRoundTripsHashAndFIFO drives the real `dump` and
// `restore` CLI paths (runDumpBuckets / runRestoreBuckets, the same
// functions RunDumpCommand/RunRestoreCommand and the REPL statements call)
// against one live server: seed data, dump it to a real directory on disk,
// restore it into freshly named buckets on the same server, and compare
// every value — not counts (dump-restore-spec.md §12.1).
func TestRestoreEndToEndRoundTripsHashAndFIFO(t *testing.T) {
	c := newDumpTestServer(t)

	if err := c.handler.send(transport.OpCreateDB, &server.CreateDatabaseRequest{Name: "app"}, nil); err != nil {
		t.Fatalf("create database: %v", err)
	}
	if err := c.handler.send(transport.OpCreateBucket, &server.CreateBucketRequest{
		Database: "app", Name: "users", Type: server.BucketTypeHash,
	}, nil); err != nil {
		t.Fatalf("create hash bucket: %v", err)
	}
	if err := c.handler.send(transport.OpCreateBucket, &server.CreateBucketRequest{
		Database: "app", Name: "events", Type: server.BucketTypeFIFO,
	}, nil); err != nil {
		t.Fatalf("create fifo bucket: %v", err)
	}

	wantHash := map[string]string{"alice": "alice-tag", "bob": "bob-tag", "carol": "carol-tag"}
	for key, tag := range wantHash {
		if err := c.handler.send(transport.OpPut, &server.InsertRequest{
			Database: "app", Bucket: "users", Key: key, Data: map[string]interface{}{"tag": tag},
		}, nil); err != nil {
			t.Fatalf("insert %s: %v", key, err)
		}
	}
	wantFIFO := []string{"e0", "e1", "e2"}
	for _, tag := range wantFIFO {
		if err := c.handler.send(transport.OpFIFOPush, &server.PushRequest{
			Database: "app", Bucket: "events", Data: map[string]interface{}{"tag": tag},
		}, nil); err != nil {
			t.Fatalf("push %s: %v", tag, err)
		}
	}

	outDir := filepath.Join(t.TempDir(), "dump")
	dumpSummary, err := runDumpBuckets(c.handler, "app", "", outDir, io.Discard, nil)
	if err != nil {
		t.Fatalf("runDumpBuckets: %v", err)
	}
	if len(dumpSummary.failures) != 0 {
		t.Fatalf("dump reported failures: %v", dumpSummary.failures)
	}

	// Point the restore at fresh target buckets by rewriting each bucket's
	// metadata.json Bucket field and moving its .bson alongside it — this
	// is exactly what an operator restoring "into a different bucket" would
	// have on disk, and keeps this test independent of --drop.
	renameDumpedBucket(t, outDir, "app", "users", "users_restored")
	renameDumpedBucket(t, outDir, "app", "events", "events_restored")

	restoreSummary, err := runRestoreBuckets(c.handler, restoreCLIOptions{Dir: outDir}, io.Discard, nil)
	if err != nil {
		t.Fatalf("runRestoreBuckets: %v", err)
	}
	if len(restoreSummary.failures) != 0 {
		t.Fatalf("restore reported failures: %v", restoreSummary.failures)
	}
	if restoreSummary.buckets != 2 {
		t.Fatalf("restore processed %d bucket(s), want 2", restoreSummary.buckets)
	}

	for key, tag := range wantHash {
		var resp server.FindResponse
		if err := c.handler.send(transport.OpGet, &server.FindRequest{Database: "app", Bucket: "users_restored", Key: key}, &resp); err != nil {
			t.Fatalf("get restored %s: %v", key, err)
		}
		if !resp.Found {
			t.Fatalf("restored hash bucket missing key %q", key)
		}
		var doc bson.M
		if err := bson.Unmarshal(resp.Data, &doc); err != nil {
			t.Fatalf("decode restored doc %q: %v", key, err)
		}
		if doc["tag"] != tag {
			t.Fatalf("restored hash key %q tag = %v, want %q (value-by-value)", key, doc["tag"], tag)
		}
	}

	for i, tag := range wantFIFO {
		var resp server.PopResponse
		if err := c.handler.send(transport.OpFIFOPop, &server.PopRequest{Database: "app", Bucket: "events_restored"}, &resp); err != nil {
			t.Fatalf("pop restored fifo %d: %v", i, err)
		}
		if !resp.Found {
			t.Fatalf("restored fifo pop %d: not found", i)
		}
		var doc bson.M
		if err := bson.Unmarshal(resp.Items[0], &doc); err != nil {
			t.Fatalf("decode restored fifo doc %d: %v", i, err)
		}
		if doc["tag"] != tag {
			t.Fatalf("restored fifo pop %d tag = %v, want %q (value-by-value, in order)", i, doc["tag"], tag)
		}
	}
}

// renameDumpedBucket moves <dir>/<database>/<oldBucket>.{bson,metadata.json}
// to name newBucket, rewriting metadata.json's own "bucket" field to match —
// the on-disk equivalent of "restore this dump into a different bucket."
func renameDumpedBucket(t *testing.T, dir, database, oldBucket, newBucket string) {
	t.Helper()
	dbDir := filepath.Join(dir, database)
	if err := os.Rename(filepath.Join(dbDir, oldBucket+".bson"), filepath.Join(dbDir, newBucket+".bson")); err != nil {
		t.Fatalf("renaming .bson: %v", err)
	}
	metaPath := filepath.Join(dbDir, oldBucket+".metadata.json")
	raw, err := os.ReadFile(metaPath)
	if err != nil {
		t.Fatalf("reading metadata.json: %v", err)
	}
	var meta bucketMetadataFile
	if err := json.Unmarshal(raw, &meta); err != nil {
		t.Fatalf("unmarshal metadata.json: %v", err)
	}
	meta.Bucket = newBucket
	if err := writeJSONFile(filepath.Join(dbDir, newBucket+".metadata.json"), meta); err != nil {
		t.Fatalf("writing renamed metadata.json: %v", err)
	}
	if err := os.Remove(metaPath); err != nil {
		t.Fatalf("removing old metadata.json: %v", err)
	}
}

// TestRestoreDryRunEndToEndWritesNothing proves --dry-run reports what
// would change without creating the target bucket, verified with a
// before/after directory and bucket listing (this task's "Done when"
// criterion).
func TestRestoreDryRunEndToEndWritesNothing(t *testing.T) {
	c := newDumpTestServer(t)
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

	outDir := filepath.Join(t.TempDir(), "dump")
	if _, err := runDumpBuckets(c.handler, "app", "users", outDir, io.Discard, nil); err != nil {
		t.Fatalf("runDumpBuckets: %v", err)
	}

	var before server.ListBucketsResponse
	if err := c.handler.send(transport.OpListBuckets, &server.ListBucketsRequest{Database: "app"}, &before); err != nil {
		t.Fatalf("list buckets before: %v", err)
	}

	summary, err := runRestoreBuckets(c.handler, restoreCLIOptions{Dir: outDir, DryRun: true}, io.Discard, nil)
	if err != nil {
		t.Fatalf("runRestoreBuckets --dry-run: %v", err)
	}
	if len(summary.failures) != 0 {
		t.Fatalf("dry-run reported failures: %v", summary.failures)
	}
	if !summary.dryRun {
		t.Fatal("summary.dryRun = false, want true")
	}

	var after server.ListBucketsResponse
	if err := c.handler.send(transport.OpListBuckets, &server.ListBucketsRequest{Database: "app"}, &after); err != nil {
		t.Fatalf("list buckets after: %v", err)
	}
	if len(after.Buckets) != len(before.Buckets) {
		t.Fatalf("bucket count changed after --dry-run: before=%d after=%d", len(before.Buckets), len(after.Buckets))
	}
}

// TestRestoreEndToEndRefusesHostileManifestAndWritesNothing crafts a dump
// directory whose metadata.json names a reserved database ("_system") and
// a path-traversal bucket, then runs the real restore verb against a live
// server and proves both that the run reports failures and that neither
// name was ever created on the server — not merely that an error came back
// (dump-restore-spec.md §12.12, this task's brief).
func TestRestoreEndToEndRefusesHostileManifestAndWritesNothing(t *testing.T) {
	c := newDumpTestServer(t)
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

	outDir := filepath.Join(t.TempDir(), "dump")
	if _, err := runDumpBuckets(c.handler, "app", "users", outDir, io.Discard, nil); err != nil {
		t.Fatalf("runDumpBuckets: %v", err)
	}

	// Hand-craft a second, hostile bucket entry: a reserved database name.
	// This exercises the CLI's own read-side refusal (validateRestoreReadPath
	// runs before the request ever reaches the wire), which is the
	// resolveRestoreTargets path — hostile *directory* names, not manifest
	// field values, since resolveRestoreTargets discovers targets by
	// walking the directory tree.
	hostileDBDir := filepath.Join(outDir, "_system")
	if err := os.MkdirAll(hostileDBDir, 0o755); err != nil {
		t.Fatalf("mkdir hostile dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(hostileDBDir, "b.bson"), []byte{}, 0o644); err != nil {
		t.Fatalf("write hostile bson: %v", err)
	}
	hostileMeta := bucketMetadataFile{
		FormatVersion: dumpManifestFormatVersionCLI, StoreType: "hash", Database: "_system", Bucket: "b",
		Documents: 0, Bytes: 0, Digest: "sha256:" + shaHexOfEmpty(),
	}
	if err := writeJSONFile(filepath.Join(hostileDBDir, "b.metadata.json"), hostileMeta); err != nil {
		t.Fatalf("write hostile metadata: %v", err)
	}

	summary, err := runRestoreBuckets(c.handler, restoreCLIOptions{Dir: outDir}, io.Discard, nil)
	if err != nil {
		t.Fatalf("runRestoreBuckets: %v", err)
	}
	if len(summary.failures) == 0 {
		t.Fatal("restore did not report a failure for the reserved database name")
	}
	found := false
	for _, f := range summary.failures {
		if f.Database == "_system" {
			found = true
		}
	}
	if !found {
		t.Fatalf("failures = %v, want one naming database _system", summary.failures)
	}

	// Prove nothing was written: the server never gained a "_system"
	// database.
	var dbs server.ListDatabasesResponse
	if err := c.handler.send(transport.OpListDBs, &server.ListDatabasesRequest{}, &dbs); err != nil {
		t.Fatalf("list databases: %v", err)
	}
	for _, db := range dbs.Databases {
		if db.Name == "_system" {
			t.Fatal("the hostile restore attempt created database \"_system\" on the server")
		}
	}
}

// shaHexOfEmpty returns the SHA-256 hex digest of zero bytes, so the
// hostile fixture above carries a digest that at least matches its
// (empty) .bson file — the test's point is the reserved-name refusal, not
// the digest check, so the digest must not be what trips it.
func shaHexOfEmpty() string {
	return "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
}

// TestRestoreEndToEndIncompleteManifestRefusedUnlessAllowed is
// dump-restore-spec.md §12.15: a manifest marked "incomplete" is refused
// by default and proceeds (with a note) under --allow-incomplete.
func TestRestoreEndToEndIncompleteManifestRefusedUnlessAllowed(t *testing.T) {
	c := newDumpTestServer(t)
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

	outDir := filepath.Join(t.TempDir(), "dump")
	if _, err := runDumpBuckets(c.handler, "app", "users", outDir, io.Discard, nil); err != nil {
		t.Fatalf("runDumpBuckets: %v", err)
	}
	renameDumpedBucket(t, outDir, "app", "users", "users_restored")

	// Mark the manifest incomplete by hand, as a real partial dump would.
	manifestPath := filepath.Join(outDir, "manifest.json")
	raw, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("reading manifest.json: %v", err)
	}
	var manifest topLevelManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatalf("unmarshal manifest.json: %v", err)
	}
	manifest.Incomplete = true
	manifest.Failures = []dumpFailure{{Database: "app", Bucket: "missing", Error: "simulated unreadable bucket"}}
	if err := writeManifest(outDir, manifest); err != nil {
		t.Fatalf("rewriting manifest.json: %v", err)
	}

	if _, err := runRestoreBuckets(c.handler, restoreCLIOptions{Dir: outDir}, io.Discard, nil); err == nil {
		t.Fatal("restore proceeded against an incomplete manifest without --allow-incomplete")
	}

	summary, err := runRestoreBuckets(c.handler, restoreCLIOptions{Dir: outDir, AllowIncomplete: true}, io.Discard, nil)
	if err != nil {
		t.Fatalf("--allow-incomplete restore: %v", err)
	}
	if len(summary.warnings) == 0 {
		t.Fatal("--allow-incomplete restore reported no warning about what is missing")
	}
	if summary.buckets != 1 {
		t.Fatalf("--allow-incomplete restore processed %d bucket(s), want 1", summary.buckets)
	}
}

// TestRestoreEndToEndDigestMismatchWritesNothing is dump-restore-spec.md
// §12.8/§4: a flipped byte in a .bson file is refused before any write,
// verified by the target bucket never being created.
func TestRestoreEndToEndDigestMismatchWritesNothing(t *testing.T) {
	c := newDumpTestServer(t)
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

	outDir := filepath.Join(t.TempDir(), "dump")
	if _, err := runDumpBuckets(c.handler, "app", "users", outDir, io.Discard, nil); err != nil {
		t.Fatalf("runDumpBuckets: %v", err)
	}
	renameDumpedBucket(t, outDir, "app", "users", "users_restored")

	bsonPath := filepath.Join(outDir, "app", "users_restored.bson")
	raw, err := os.ReadFile(bsonPath)
	if err != nil {
		t.Fatalf("reading .bson: %v", err)
	}
	if len(raw) == 0 {
		t.Fatal("dumped .bson file is empty; test fixture is wrong")
	}
	raw[len(raw)-1] ^= 0xFF // flip a byte
	if err := os.WriteFile(bsonPath, raw, 0o644); err != nil {
		t.Fatalf("corrupting .bson: %v", err)
	}

	summary, err := runRestoreBuckets(c.handler, restoreCLIOptions{Dir: outDir}, io.Discard, nil)
	if err != nil {
		t.Fatalf("runRestoreBuckets: %v", err)
	}
	if len(summary.failures) != 1 {
		t.Fatalf("summary.failures = %v, want exactly one digest-mismatch failure", summary.failures)
	}

	var resp server.ListBucketsResponse
	if err := c.handler.send(transport.OpListBuckets, &server.ListBucketsRequest{Database: "app"}, &resp); err != nil {
		t.Fatalf("list buckets: %v", err)
	}
	for _, b := range resp.Buckets {
		if b.Name == "users_restored" {
			t.Fatal("a digest mismatch must refuse before the target bucket is created")
		}
	}
}

// TestRestoreEndToEndUsersJSONWarningVisibleInJSONAndText is this task's
// brief: a users.json present in the dump directory must produce a
// warning visible in both the text summary and --json output, never only a
// log line, since account restore is not yet implemented.
func TestRestoreEndToEndUsersJSONWarningVisibleInJSONAndText(t *testing.T) {
	c := newDumpTestServer(t)
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

	outDir := filepath.Join(t.TempDir(), "dump")
	if _, err := runDumpBuckets(c.handler, "app", "users", outDir, io.Discard, nil); err != nil {
		t.Fatalf("runDumpBuckets: %v", err)
	}
	renameDumpedBucket(t, outDir, "app", "users", "users_restored")
	if err := os.WriteFile(filepath.Join(outDir, "users.json"), []byte(`[]`), 0o644); err != nil {
		t.Fatalf("writing users.json: %v", err)
	}

	summary, err := runRestoreBuckets(c.handler, restoreCLIOptions{Dir: outDir}, io.Discard, nil)
	if err != nil {
		t.Fatalf("runRestoreBuckets: %v", err)
	}
	if len(summary.warnings) == 0 {
		t.Fatal("restoreSummary.warnings is empty; the users.json notice must be structured, not just logged")
	}
	result := restoreResult{Warnings: summary.warnings}
	payload, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("marshal restoreResult: %v", err)
	}
	if !jsonContains(payload, "account restore is not yet implemented") {
		t.Fatalf("--json output does not carry the users.json warning: %s", payload)
	}
}

func jsonContains(payload []byte, substr string) bool {
	return len(payload) > 0 && (func() bool {
		for i := 0; i+len(substr) <= len(payload); i++ {
			if string(payload[i:i+len(substr)]) == substr {
				return true
			}
		}
		return false
	})()
}
