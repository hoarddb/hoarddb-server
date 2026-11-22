package cli

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Dumps carry index descriptors (GAP-14 build doc Step 10, §4 F-10): a dump
// that recorded only sorted field names restored a unique index as a plain one
// and renumbered the ids, and reported success.

// rewriteJSONFile applies edit to a JSON file as a generic map, so every key
// the file holds survives the edit whether or not this build's structs know it.
func rewriteJSONFile(t *testing.T, path string, edit func(map[string]interface{})) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	var document map[string]interface{}
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatalf("parsing %s: %v", path, err)
	}
	edit(document)
	rewritten, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		t.Fatalf("encoding %s: %v", path, err)
	}
	if err := os.WriteFile(path, rewritten, 0o600); err != nil {
		t.Fatalf("writing %s: %v", path, err)
	}
}

// dumpUsersFor seeds app/users with indices, dumps it, and moves the dump to
// app/restored, keeping every key of its metadata.
func dumpUsersFor(t *testing.T, c *CLI, indices []interface{}) string {
	t.Helper()
	if err := c.handler.send(transport.OpCreateDB, &server.CreateDatabaseRequest{Name: "app"}, nil); err != nil {
		t.Fatalf("create database: %v", err)
	}
	var created server.CreateBucketResponse
	if err := c.handler.send(transport.OpCreateBucket, &server.CreateBucketRequest{Database: "app", Name: "users", Type: server.BucketTypeHash,
		Options: map[string]interface{}{"placement": "whole", "indices": indices}}, &created); err != nil || !created.Success {
		t.Fatalf("create bucket = %+v, %v", created, err)
	}
	if err := c.handler.send(transport.OpPut, &server.InsertRequest{Database: "app", Bucket: "users", Key: "u1",
		Data: map[string]interface{}{"Email": "held@example.com", "Zeta": "z", "Alpha": "a"}}, nil); err != nil {
		t.Fatalf("insert: %v", err)
	}
	outDir := filepath.Join(t.TempDir(), "dump")
	if summary, err := runDumpBuckets(c.handler, "app", "users", outDir, io.Discard, nil); err != nil || len(summary.failures) != 0 {
		t.Fatalf("dump = %+v, %v", summary, err)
	}
	dbDir := filepath.Join(outDir, "app")
	if err := os.Rename(filepath.Join(dbDir, "users.bson"), filepath.Join(dbDir, "restored.bson")); err != nil {
		t.Fatalf("renaming the .bson: %v", err)
	}
	if err := os.Rename(filepath.Join(dbDir, "users.metadata.json"), filepath.Join(dbDir, "restored.metadata.json")); err != nil {
		t.Fatalf("renaming the metadata: %v", err)
	}
	rewriteJSONFile(t, filepath.Join(dbDir, "restored.metadata.json"), func(meta map[string]interface{}) { meta["bucket"] = "restored" })
	return outDir
}

func restoreFrom(t *testing.T, c *CLI, dir string) *restoreSummary {
	t.Helper()
	summary, err := runRestoreBuckets(c.handler, restoreCLIOptions{Dir: dir}, io.Discard, nil)
	if err != nil {
		t.Fatalf("runRestoreBuckets: %v", err)
	}
	return summary
}

func restoredIndexes(t *testing.T, c *CLI) []string {
	t.Helper()
	var stats server.BucketStatsResponse
	if err := c.handler.send(transport.OpBucketStats, &server.BucketStatsRequest{Database: "app", Bucket: "restored"}, &stats); err != nil {
		t.Fatalf("stats: %v", err)
	}
	return stats.Indexes
}

// insertRestored reports whether an insert into app/restored was accepted.
func insertRestored(t *testing.T, c *CLI, key, email string) bool {
	t.Helper()
	var resp server.InsertResponse
	err := c.handler.send(transport.OpPut, &server.InsertRequest{Database: "app", Bucket: "restored", Key: key,
		Data: map[string]interface{}{"Email": email}}, &resp)
	return err == nil && resp.Success
}

// TestDumpRestoreRoundTripsAUniqueIndex: the restored bucket refuses a
// duplicate, as its source did.
func TestDumpRestoreRoundTripsAUniqueIndex(t *testing.T) {
	c := newDumpTestServer(t)
	dir := dumpUsersFor(t, c, []interface{}{map[string]interface{}{"field": "Email", "unique": true}})
	if summary := restoreFrom(t, c, dir); len(summary.failures) != 0 {
		t.Fatalf("restore reported failures: %v", summary.failures)
	}
	if got := restoredIndexes(t, c); len(got) != 1 || got[0] != "Email_1 (unique)" {
		t.Fatalf("the restored bucket declares %q, want [Email_1 (unique)]", got)
	}
	if insertRestored(t, c, "u2", "held@example.com") {
		t.Fatal("the restored unique index accepted a duplicate: the dump restored it as a plain index")
	}
}

// TestDumpPreservesIndexOrderAndIDs: the restored bucket declares its indexes
// in the source's order, so every id — which is what every entry carries —
// is the source's. The old manifest sorted the names.
func TestDumpPreservesIndexOrderAndIDs(t *testing.T) {
	c := newDumpTestServer(t)
	dir := dumpUsersFor(t, c, []interface{}{"Zeta", "Alpha"})
	if summary := restoreFrom(t, c, dir); len(summary.failures) != 0 {
		t.Fatalf("restore reported failures: %v", summary.failures)
	}
	if got := restoredIndexes(t, c); strings.Join(got, ",") != "Zeta_1,Alpha_1" {
		t.Fatalf("the restored bucket declares %q, want [Zeta_1 Alpha_1] in the source's order", got)
	}
}

// TestAFormatOneDumpIsRefused was
// TestAPreChangeDumpRestoresAsNonUniqueSingleFieldIndexes, which restored a
// format-1 dump — names only — as non-unique single-field indexes. The owner's
// ruling of 2026-09-30 removed format 1: no release ever wrote one, so there
// is no legacy dump to keep readable.
//
// The refusal is also a security guard, and the reason the accepted set
// cannot be widened back to include 1. A format-1 manifest carries no
// index_specs, so restoring one for a bucket with a unique index declares the
// index without its uniqueness and the restored bucket accepts duplicates its
// source refused. That manifest now does not restore at all, which is
// stronger than restoring it and warning.
func TestAFormatOneDumpIsRefused(t *testing.T) {
	for _, tc := range []struct {
		name  string
		files []string
	}{
		{name: "the whole dump says 1", files: []string{"manifest.json", filepath.Join("app", "restored.metadata.json")}},
		{name: "one bucket's metadata says 1", files: []string{filepath.Join("app", "restored.metadata.json")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newDumpTestServer(t)
			dir := dumpUsersFor(t, c, []interface{}{map[string]interface{}{"field": "Email", "unique": true}})
			for _, file := range tc.files {
				rewriteJSONFile(t, filepath.Join(dir, file), func(document map[string]interface{}) {
					document["format_version"] = 1
					delete(document, "index_specs")
				})
			}
			summary, err := runRestoreBuckets(c.handler, restoreCLIOptions{Dir: dir}, io.Discard, nil)
			refusal := ""
			switch {
			case err != nil:
				refusal = err.Error()
			case summary != nil && len(summary.failures) == 1:
				refusal = summary.failures[0].Error
			default:
				t.Fatalf("a format-1 dump was restored: %+v", summary)
			}
			if !strings.Contains(refusal, "format_version 1") || !strings.Contains(refusal, "format_versions 2 and 3") {
				t.Errorf("the refusal %q does not name the version found (1) and the ones supported (2 and 3)", refusal)
			}
			var stats server.BucketStatsResponse
			if err := c.handler.send(transport.OpBucketStats, &server.BucketStatsRequest{Database: "app", Bucket: "restored"}, &stats); err == nil && stats.Found {
				t.Fatalf("the refused format-1 dump created its bucket, declaring %q: a unique index restored without its uniqueness", stats.Indexes)
			}
		})
	}
}

// TestRestoreRefusesAFormatItDoesNotKnow: a dump from a later format is
// refused before anything is written.
func TestRestoreRefusesAFormatItDoesNotKnow(t *testing.T) {
	c := newDumpTestServer(t)
	dir := dumpUsersFor(t, c, []interface{}{"Email"})
	rewriteJSONFile(t, filepath.Join(dir, "manifest.json"), func(manifest map[string]interface{}) { manifest["format_version"] = 4 })
	rewriteJSONFile(t, filepath.Join(dir, "app", "restored.metadata.json"), func(meta map[string]interface{}) { meta["format_version"] = 4 })
	summary, err := runRestoreBuckets(c.handler, restoreCLIOptions{Dir: dir}, io.Discard, nil)
	if err == nil && (summary == nil || len(summary.failures) == 0) {
		t.Fatal("a format-4 dump was restored")
	}
	var stats server.BucketStatsResponse
	if err := c.handler.send(transport.OpBucketStats, &server.BucketStatsRequest{Database: "app", Bucket: "restored"}, &stats); err == nil && stats.Found {
		t.Fatal("the refused format-4 dump created its bucket")
	}
}
