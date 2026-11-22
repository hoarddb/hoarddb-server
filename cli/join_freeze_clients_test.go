package cli

import (
	"bytes"
	"io"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Join-freeze build step 10: restore says why a schema change was
// refused while a node joins (join-freeze spec §7.3).

// TestARestoreThatMeetsAJoinSaysWhichBucketsLanded is review F-15: a restore
// whose second bucket's start is refused names, in its summary, the bucket it
// restored and the one refused with the server's text. On a standalone server
// the second start is refused by the store-type check, not by a join: the CLI
// cannot host a cluster, and what is under test is the summary, whatever the
// server's reason (the server refuses a restore start during a join in
// server/join_freeze_cluster_test.go). Guard: drop the refused bucket from the
// summary.
func TestARestoreThatMeetsAJoinSaysWhichBucketsLanded(t *testing.T) {
	c := newDumpTestServer(t)
	if err := c.handler.send(transport.OpCreateDB, &server.CreateDatabaseRequest{Name: "app"}, nil); err != nil {
		t.Fatalf("create database: %v", err)
	}
	for _, name := range []string{"first", "second"} {
		if err := c.handler.send(transport.OpCreateBucket, &server.CreateBucketRequest{Database: "app", Name: name, Type: server.BucketTypeHash}, nil); err != nil {
			t.Fatalf("create bucket %s: %v", name, err)
		}
		if err := c.handler.send(transport.OpPut, &server.InsertRequest{Database: "app", Bucket: name, Key: "k", Data: map[string]interface{}{"n": 1}}, nil); err != nil {
			t.Fatalf("insert into %s: %v", name, err)
		}
	}
	outDir := filepath.Join(t.TempDir(), "dump")
	if _, err := runDumpBuckets(c.handler, "app", "", outDir, io.Discard, nil); err != nil {
		t.Fatalf("runDumpBuckets: %v", err)
	}
	renameDumpedBucket(t, outDir, "app", "first", "first_restored")
	renameDumpedBucket(t, outDir, "app", "second", "second_restored")
	// The second restore's target exists with another store type: its start is
	// refused by the server, after the first bucket has landed.
	if err := c.handler.send(transport.OpCreateBucket, &server.CreateBucketRequest{Database: "app", Name: "second_restored", Type: server.BucketTypeFIFO}, nil); err != nil {
		t.Fatalf("create the conflicting target: %v", err)
	}

	summary, err := runRestoreBuckets(c.handler, restoreCLIOptions{Dir: outDir}, io.Discard, nil)
	if err != nil {
		t.Fatalf("runRestoreBuckets: %v", err)
	}
	if len(summary.failures) != 1 {
		t.Fatalf("the restore recorded %d failures, want the second bucket's: %v", len(summary.failures), summary.failures)
	}
	var printed bytes.Buffer
	printRestoreSummary(&printed, summary)
	text := printed.String()
	if !strings.Contains(text, "restored app/first_restored") {
		t.Errorf("the summary does not name the bucket that landed:\n%s", text)
	}
	if !strings.Contains(text, "FAILED app/second_restored: "+summary.failures[0].Error) {
		t.Errorf("the summary does not name the refused bucket with the server's text:\n%s", text)
	}
}
