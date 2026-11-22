package cli

import (
	"bytes"
	"testing"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// TestRangeAndEdgeRenderRawEntries pins the CLI's range and edge output
// against a live server. A range entry's value reaches the CLI
// as the stored BSON, undecoded; jsonLine decodes it exactly as it does a
// point read's FindResponse.Data, so the printed lines stay what they were.
func TestRangeAndEdgeRenderRawEntries(t *testing.T) {
	c := newDumpTestServer(t)
	if err := c.handler.send(transport.OpCreateDB, &server.CreateDatabaseRequest{Name: "app"}, nil); err != nil {
		t.Fatalf("create database: %v", err)
	}
	if err := c.handler.send(transport.OpCreateBucket, &server.CreateBucketRequest{
		Database: "app", Name: "events", Type: server.BucketTypeBTree,
	}, nil); err != nil {
		t.Fatalf("create btree bucket: %v", err)
	}
	for _, key := range []string{"a", "b", "c"} {
		if err := c.handler.send(transport.OpPut, &server.InsertRequest{
			Database: "app", Bucket: "events", Key: key, Data: map[string]interface{}{"name": key + "-name", "n": int32(len(key))},
		}, nil); err != nil {
			t.Fatalf("insert %s: %v", key, err)
		}
	}

	var terminal bytes.Buffer
	c.handler.terminal = &terminal

	if err := c.handler.handleRange("app", "events", []string{`{$gte: "a", $lte: "b"}`}); err != nil {
		t.Fatalf("handleRange: %v", err)
	}
	wantRange := "2 entries in range:\n" +
		`  a                    {"_id_": "a", "n": 1, "name": "a-name"}` + "\n" +
		`  b                    {"_id_": "b", "n": 1, "name": "b-name"}` + "\n"
	if got := terminal.String(); got != wantRange {
		t.Errorf("range output:\n%s\nwant:\n%s", got, wantRange)
	}

	terminal.Reset()
	if err := c.handler.handleEdge("app", "events", transport.OpBTreeLast, "last"); err != nil {
		t.Fatalf("handleEdge: %v", err)
	}
	wantEdge := "last: c\n" + `{"_id_": "c", "n": 1, "name": "c-name"}` + "\n"
	if got := terminal.String(); got != wantEdge {
		t.Errorf("edge output:\n%s\nwant:\n%s", got, wantEdge)
	}
}

// TestRangeAndEdgeQuoteAKeyATerminalWouldActOn is BUG-0613's CLI half: the
// server now lists a key exactly as written, so a key holding an escape
// sequence or a NUL must be shown quoted, not sent to the terminal.
func TestRangeAndEdgeQuoteAKeyATerminalWouldActOn(t *testing.T) {
	c := newDumpTestServer(t)
	if err := c.handler.send(transport.OpCreateDB, &server.CreateDatabaseRequest{Name: "app"}, nil); err != nil {
		t.Fatalf("create database: %v", err)
	}
	if err := c.handler.send(transport.OpCreateBucket, &server.CreateBucketRequest{
		Database: "app", Name: "events", Type: server.BucketTypeBTree,
	}, nil); err != nil {
		t.Fatalf("create btree bucket: %v", err)
	}
	const key = "e\x1b[31mred\x00"
	if err := c.handler.send(transport.OpPut, &server.InsertRequest{
		Database: "app", Bucket: "events", Key: key, Data: map[string]interface{}{"n": int32(1)},
	}, nil); err != nil {
		t.Fatalf("insert: %v", err)
	}

	var terminal bytes.Buffer
	c.handler.terminal = &terminal
	if err := c.handler.handleRange("app", "events", []string{`{$gte: "e", $lte: "f"}`}); err != nil {
		t.Fatalf("handleRange: %v", err)
	}
	if err := c.handler.handleEdge("app", "events", transport.OpBTreeFirst, "first"); err != nil {
		t.Fatalf("handleEdge: %v", err)
	}
	output := terminal.String()
	if bytes.ContainsAny([]byte(output), "\x1b\x00") {
		t.Errorf("output holds a raw escape or NUL byte: %q", output)
	}
	if want := `"e\x1b[31mred\x00"`; !bytes.Contains([]byte(output), []byte(want)) {
		t.Errorf("output does not show the key quoted as %s: %q", want, output)
	}
}
