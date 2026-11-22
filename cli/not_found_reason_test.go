package cli

import (
	"bytes"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// TestMissesShowTheServersReason checks the server's reason from the CLI. Before the fix
// the server sent an empty frame for every miss and pop/peek printed "EMPTY"
// with a blank reason; now the reason arrives. An absence still prints the
// server's wording. A reply that is not an absence (a verb the bucket type does
// not have) is a refusal and carries the same wording as the error, since
// a server from before the reason was sent sends an empty frame, which decodes
// as Found:false with no message and is now refused too, not printed as
// "EMPTY ".
func TestMissesShowTheServersReason(t *testing.T) {
	c := newDumpTestServer(t)
	if err := c.handler.send(transport.OpCreateDB, &server.CreateDatabaseRequest{Name: "app"}, nil); err != nil {
		t.Fatalf("create database: %v", err)
	}
	for name, bucketType := range map[string]server.BucketType{"fifo": server.BucketTypeFIFO, "heap": server.BucketTypeHeap, "hashes": server.BucketTypeHash} {
		var created server.CreateBucketResponse
		if err := c.handler.send(transport.OpCreateBucket, &server.CreateBucketRequest{Database: "app", Name: name, Type: bucketType}, &created); err != nil || !created.Success {
			t.Fatalf("create %s bucket: %+v, %v", bucketType, created, err)
		}
	}
	var terminal bytes.Buffer
	c.handler.terminal = &terminal

	cases := []struct {
		name string
		run  func() error
		want string
	}{
		{"pop from an empty fifo", func() error { return c.handler.handlePop("app", "fifo", nil) }, "EMPTY  queue is empty"},
		{"peek at an empty fifo", func() error { return c.handler.handlePeek("app", "fifo", nil) }, "EMPTY  queue is empty"},
		{"pop from an empty heap", func() error { return c.handler.handlePop("app", "heap", nil) }, "EMPTY  heap is empty"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			terminal.Reset()
			if err := tc.run(); err != nil {
				t.Fatalf("%s: %v", tc.name, err)
			}
			if !strings.Contains(terminal.String(), tc.want) {
				t.Errorf("%s printed %q, want it to contain %q", tc.name, terminal.String(), tc.want)
			}
		})
	}

	t.Run("peek at a hash bucket is refused with the server's reason", func(t *testing.T) {
		terminal.Reset()
		err := c.handler.handlePeek("app", "hashes", nil)
		if err == nil || !strings.Contains(err.Error(), "peek is not defined for hash buckets") {
			t.Fatalf("peek at a hash bucket returned %v, want an error naming the bucket type", err)
		}
		if terminal.Len() != 0 {
			t.Errorf("peek at a hash bucket printed %q before failing", terminal.String())
		}
	})
}
