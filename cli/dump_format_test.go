package cli

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Owner ruling 2026-09-30: every dump has one format_version (3 since the dead
// file, claim/ack build step 17), whatever its buckets declare. These tests are about the layout — which sections a bucket
// carries — because a bucket that differs only by version no longer exists.

// readJSONFile decodes a dump's JSON file into target.
func readJSONFile(t *testing.T, path string, target any) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	if err := json.Unmarshal(raw, target); err != nil {
		t.Fatalf("parsing %s: %v", path, err)
	}
}

// TestDumpRestoreRoundTripsEveryLayout: a plain bucket, one with only an
// index declaration, one with only store fields and one with both each dump
// as format 3 and restore into a fresh server with every section they had.
func TestDumpRestoreRoundTripsEveryLayout(t *testing.T) {
	uniqueEmail := []interface{}{map[string]interface{}{"field": "Email", "unique": true}}
	comments := []interface{}{map[string]interface{}{"name": "Comments", "engine": "fifoStore"}}
	cases := []struct {
		name    string
		options map[string]interface{}
		indexed bool
		nested  bool
	}{
		{name: "plain"},
		{name: "index only", options: map[string]interface{}{"placement": "whole", "indices": uniqueEmail}, indexed: true},
		{name: "stores only", options: map[string]interface{}{"stores": comments}, nested: true},
		{name: "both", options: map[string]interface{}{"placement": "whole", "indices": uniqueEmail, "stores": comments}, indexed: true, nested: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			source := newDumpTestServer(t)
			mustSend(t, source, transport.OpCreateDB, &server.CreateDatabaseRequest{Name: "app"}, nil)
			var created server.CreateBucketResponse
			mustSend(t, source, transport.OpCreateBucket, &server.CreateBucketRequest{Database: "app", Name: "Post", Type: server.BucketTypeHash, Options: tc.options}, &created)
			if !created.Success {
				t.Fatalf("creating Post: %s", created.Message)
			}
			mustSend(t, source, transport.OpPut, &server.InsertRequest{Database: "app", Bucket: "Post", Key: "p1",
				Data: map[string]interface{}{"Email": "held@example.com"}}, nil)
			if tc.nested {
				for n := range 3 {
					mustSend(t, source, transport.OpFieldPush, &server.FieldRequest{Database: "app", Bucket: "Post", Key: "p1", Field: "Comments",
						Data: map[string]interface{}{"n": n}}, nil)
				}
			}

			outDir := filepath.Join(t.TempDir(), "dump")
			if summary, err := runDumpBuckets(source.handler, "app", "Post", outDir, io.Discard, nil); err != nil || summary.incomplete {
				t.Fatalf("dumping Post: %+v, %v", summary, err)
			}
			var manifest topLevelManifest
			readJSONFile(t, filepath.Join(outDir, "manifest.json"), &manifest)
			var meta bucketMetadataFile
			readJSONFile(t, filepath.Join(outDir, "app", "Post.metadata.json"), &meta)
			if manifest.FormatVersion != 3 || meta.FormatVersion != 3 {
				t.Errorf("manifest.json is format_version %d and Post.metadata.json %d, want 3 and 3", manifest.FormatVersion, meta.FormatVersion)
			}
			if (len(meta.IndexSpecs) == 1) != tc.indexed || (len(meta.Stores) == 1) != tc.nested {
				t.Errorf("the metadata carries %d index specs and %d stores; want an index spec: %v, a store: %v",
					len(meta.IndexSpecs), len(meta.Stores), tc.indexed, tc.nested)
			}
			_, statErr := os.Stat(filepath.Join(outDir, "app", "Post.stores.bson"))
			if wrote := statErr == nil; wrote != tc.nested {
				t.Errorf("Post.stores.bson written: %v, want %v (stat: %v)", wrote, tc.nested, statErr)
			}
			if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("checking for Post.stores.bson: %v", statErr)
			}

			target := newDumpTestServer(t)
			restored, err := runRestoreBuckets(target.handler, restoreCLIOptions{Dir: outDir}, io.Discard, nil)
			if err != nil || len(restored.failures) != 0 {
				t.Fatalf("restoring Post: %+v, %v", restored, err)
			}
			var stats server.BucketStatsResponse
			mustSend(t, target, transport.OpBucketStats, &server.BucketStatsRequest{Database: "app", Bucket: "Post"}, &stats)
			if !stats.Found || stats.Entries != 1 {
				t.Fatalf("the restored bucket reads %+v, want it found with 1 document", stats)
			}
			if tc.indexed {
				if len(stats.Indexes) != 1 || stats.Indexes[0] != "Email_1 (unique)" {
					t.Errorf("the restored bucket declares %q, want [Email_1 (unique)]", stats.Indexes)
				}
				var duplicate server.InsertResponse
				err := target.handler.send(transport.OpPut, &server.InsertRequest{Database: "app", Bucket: "Post", Key: "p2",
					Data: map[string]interface{}{"Email": "held@example.com"}}, &duplicate)
				if err == nil && duplicate.Success {
					t.Error("the restored unique index accepted a duplicate")
				}
			} else if len(stats.Indexes) != 0 {
				t.Errorf("the restored bucket declares %q, want no index", stats.Indexes)
			}
			if tc.nested {
				if len(stats.Stores) != 1 || stats.Stores[0].Name != "Comments" || stats.Stores[0].Elements != 3 {
					t.Errorf("the restored bucket's stores read %+v, want Comments with 3 elements", stats.Stores)
				}
			} else if len(stats.Stores) != 0 {
				t.Errorf("the restored bucket's stores read %+v, want none", stats.Stores)
			}
		})
	}
}
