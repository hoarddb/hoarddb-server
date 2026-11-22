package cli

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// TestDumpAndRestoreKeepAHashBucketsPlacement is T-65's CLI half (build step
// 12b): the metadata.json of a dumped hash bucket records its placement, and
// restoring it creates the bucket with that placement, so a whole bucket
// keeps its unique index and a spread one stays spread. Read by wire names,
// so the test runs against the parent and fails on what the parent writes.
// A spread bucket's dump fans in over its parts (spread MVP step 3).
func TestDumpAndRestoreKeepAHashBucketsPlacement(t *testing.T) {
	c, connection := newFactorTestServer(t)
	if err := c.handler.send(transport.OpCreateDB, &server.CreateDatabaseRequest{Name: "app"}, nil); err != nil {
		t.Fatalf("create database: %v", err)
	}
	for name, options := range map[string]map[string]interface{}{
		"kept": {"placement": "whole", "indices": []interface{}{map[string]interface{}{"fields": []interface{}{"Email"}, "unique": true}}},
		"wide": {"placement": "spread"},
	} {
		var created server.CreateBucketResponse
		if err := c.handler.send(transport.OpCreateBucket, &server.CreateBucketRequest{
			Database: "app", Name: name, Type: server.BucketTypeHash, Options: options,
		}, &created); err != nil || !created.Success {
			t.Fatalf("create bucket %s: %v %s", name, err, created.Message)
		}
	}
	outDir := filepath.Join(t.TempDir(), "dump")
	if summary, err := runDumpBuckets(c.handler, "app", "", outDir, io.Discard, nil); err != nil || len(summary.failures) != 0 {
		t.Fatalf("dump: %v %v", err, summary)
	}
	for name, want := range map[string]string{"kept": "whole", "wide": "spread"} {
		if got := readMetadataAsWritten(t, outDir, "app", name)["placement"]; got != want {
			t.Errorf("%s.metadata.json records placement %v, want %q", name, got, want)
		}
	}

	renameDumpedBucket(t, outDir, "app", "kept", "kept2")
	renameDumpedBucket(t, outDir, "app", "wide", "wide2")
	if code, _, stderr := captureProcessStreams(t, func() int {
		return RunRestoreCommand(append(append([]string{}, connection...), "--dir", outDir))
	}); code != ExitOK {
		t.Fatalf("restore exited %d, want %d:\n%s", code, ExitOK, stderr)
	}
	for name, want := range map[string]string{"kept2": "whole", "wide2": "spread"} {
		var stats server.BucketStatsResponse
		if err := c.handler.send(transport.OpBucketStats, &server.BucketStatsRequest{Database: "app", Bucket: name}, &stats); err != nil || !stats.Found {
			t.Fatalf("describe %s: %v %+v", name, err, stats)
		}
		if stats.Placement != want {
			t.Errorf("restored %s describes placement %q, want %q", name, stats.Placement, want)
		}
	}
}

// TestDumpOfASpreadBucketRestoresIntoAWholeBucket is B3's round trip (spread
// MVP step 3): the dump of a spread bucket fans in over every part into one
// dump of the bucket, and restoring it into a whole bucket created first
// gives the same documents and store elements, byte for byte.
func TestDumpOfASpreadBucketRestoresIntoAWholeBucket(t *testing.T) {
	c, connection := newFactorTestServer(t)
	if err := c.handler.send(transport.OpCreateDB, &server.CreateDatabaseRequest{Name: "app"}, nil); err != nil {
		t.Fatalf("create database: %v", err)
	}
	stores := []interface{}{map[string]interface{}{"name": "Purchases", "engine": "fifoStore"}}
	for name, placement := range map[string]string{"wide": "spread", "flat": "whole"} {
		var created server.CreateBucketResponse
		if err := c.handler.send(transport.OpCreateBucket, &server.CreateBucketRequest{Database: "app", Name: name, Type: server.BucketTypeHash,
			Options: map[string]interface{}{"placement": placement, "stores": stores}}, &created); err != nil || !created.Success {
			t.Fatalf("create bucket %s: %v %s", name, err, created.Message)
		}
	}
	const documents = 300
	for i := 0; i < documents; i++ {
		key := fmt.Sprintf("doc-%03d", i)
		if err := c.handler.send(transport.OpPut, &server.InsertRequest{Database: "app", Bucket: "wide", Key: key,
			Data: map[string]interface{}{"N": int32(i), "Name": fmt.Sprintf("n%d", i*31%97)}}, nil); err != nil {
			t.Fatalf("insert %s: %v", key, err)
		}
		if i%10 == 0 {
			if err := c.handler.send(transport.OpFieldPush, &server.FieldRequest{Database: "app", Bucket: "wide", Key: key, Field: "Purchases",
				Data: map[string]interface{}{"sku": int32(i)}}, nil); err != nil {
				t.Fatalf("push to %s: %v", key, err)
			}
		}
	}
	outDir := filepath.Join(t.TempDir(), "dump")
	if summary, err := runDumpBuckets(c.handler, "app", "wide", outDir, io.Discard, nil); err != nil || len(summary.failures) != 0 {
		t.Fatalf("dump of the spread bucket: %v %+v", err, summary)
	}
	if got := readMetadataAsWritten(t, outDir, "app", "wide")["bucket"]; got != "wide" {
		t.Errorf("the dump's metadata names bucket %v, want wide", got)
	}
	renameDumpedBucket(t, outDir, "app", "wide", "flat")
	// renameDumpedBucket moves the documents and the metadata; the store
	// elements' file moves with them.
	if err := os.Rename(filepath.Join(outDir, "app", "wide.stores.bson"), filepath.Join(outDir, "app", "flat.stores.bson")); err != nil {
		t.Fatalf("renaming the stores file: %v", err)
	}
	if code, _, stderr := captureProcessStreams(t, func() int {
		return RunRestoreCommand(append(append([]string{}, connection...), "--dir", outDir))
	}); code != ExitOK {
		t.Fatalf("restore into the whole bucket exited %d:\n%s", code, stderr)
	}
	read := func(name string) []string {
		var page server.ScanResponse
		if err := c.handler.send(transport.OpScan, &server.ScanRequest{Database: "app", Bucket: name, Limit: 1000}, &page); err != nil {
			t.Fatalf("find on %s: %v", name, err)
		}
		docs := make([]string, 0, len(page.Documents))
		for _, document := range page.Documents {
			key, _ := document.Lookup("_id_").StringValueOK()
			var length server.FieldResponse
			if err := c.handler.send(transport.OpFieldLen, &server.FieldRequest{Database: "app", Bucket: name, Key: key, Field: "Purchases"}, &length); err != nil {
				t.Fatalf("len of %s.%s.Purchases: %v", name, key, err)
			}
			docs = append(docs, fmt.Sprintf("%x|%d", []byte(document), length.Len))
		}
		sort.Strings(docs)
		return docs
	}
	spread, whole := read("wide"), read("flat")
	if len(spread) != documents || len(whole) != documents {
		t.Fatalf("the spread bucket reads %d documents and the restored whole one %d, want %d each", len(spread), len(whole), documents)
	}
	for i := range spread {
		if spread[i] != whole[i] {
			t.Fatalf("document %d differs after the round trip:\nspread %s\nwhole  %s", i, spread[i], whole[i])
		}
	}
	var stats server.BucketStatsResponse
	if err := c.handler.send(transport.OpBucketStats, &server.BucketStatsRequest{Database: "app", Bucket: "flat"}, &stats); err != nil || stats.Placement != "whole" {
		t.Errorf("the restored bucket describes placement %q (%v), want whole", stats.Placement, err)
	}
}
