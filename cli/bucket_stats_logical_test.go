package cli

import (
	"bytes"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// TestSizeReportingIsLogicalAndPathFree is T-13 at the CLI: the three
// consumers of BucketStatsResponse and DatabaseInfo — describe bucket,
// db.<bucket>.stats() and show databases — print the bucket's logical size and
// no path, because the node keeps one store and there is no per-bucket
// directory to name (shared-storage spec §5.4, Q16, Q22).
func TestSizeReportingIsLogicalAndPathFree(t *testing.T) {
	c := newDumpTestServer(t)
	if err := c.handler.send(transport.OpCreateDB, &server.CreateDatabaseRequest{Name: "app"}, nil); err != nil {
		t.Fatalf("create database: %v", err)
	}
	if err := c.handler.send(transport.OpCreateBucket, &server.CreateBucketRequest{Database: "app", Name: "files", Type: server.BucketTypeBlob}, nil); err != nil {
		t.Fatalf("create blob bucket: %v", err)
	}
	// The key is 6 bytes and the payload 1000: the logical size is 1006.
	if err := c.handler.send(transport.OpBlobWrite, &server.BlobWriteRequest{Database: "app", Bucket: "files", Key: "report", Data: bytes.Repeat([]byte{'x'}, 1000)}, nil); err != nil {
		t.Fatalf("write blob: %v", err)
	}
	var terminal bytes.Buffer
	c.handler.terminal = &terminal

	consumers := []struct {
		label string
		run   func() error
		sized bool
	}{
		{"describe bucket", func() error { return c.handler.handleDescribeBucket(&Cmd{Database: "app", Name: "files"}) }, true},
		{"db.files.stats()", func() error { return c.handler.handleBucketStats("app", "files") }, true},
		{"show databases", func() error { return c.handler.handleShowDatabases(&Cmd{}) }, false},
	}
	for _, consumer := range consumers {
		t.Run(consumer.label, func(t *testing.T) {
			terminal.Reset()
			if err := consumer.run(); err != nil {
				t.Fatalf("%s: %v", consumer.label, err)
			}
			output := terminal.String()
			if strings.Contains(output, "Path:") || strings.Contains(output, "PATH") || strings.Contains(output, "On disk") {
				t.Errorf("%s still reports where or how big on disk:\n%s", consumer.label, output)
			}
			if consumer.sized && (!strings.Contains(output, "Logical size:") || !strings.Contains(output, "1006 B (before compression and compaction)")) {
				t.Errorf("%s does not report the logical size of 1006 B:\n%s", consumer.label, output)
			}
		})
	}
}
