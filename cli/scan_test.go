package cli

import (
	"bytes"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// seedCommentsForPaging creates app.comments on the live server behind c with
// the given keys.
func seedCommentsForPaging(t *testing.T, c *CLI, keys []string) {
	t.Helper()
	if err := c.handler.send(transport.OpCreateBucket, &server.CreateBucketRequest{Database: "app", Name: "comments", Type: server.BucketTypeHash}, nil); err != nil {
		t.Fatalf("create app.comments: %v", err)
	}
	for _, key := range keys {
		if err := c.handler.send(transport.OpPut, &server.InsertRequest{
			Database: "app", Bucket: "comments", Key: key, Data: map[string]interface{}{"n": int32(1)},
		}, nil); err != nil {
			t.Fatalf("insert %s: %v", key, err)
		}
	}
}

// TestCLIPageAcrossDropAndRecreateFails is T-25(b): the CLI itself remembers
// the bucket id of the page it printed, so the second find of a session, with
// a non-zero skip, fails when the bucket was dropped and re-created in
// between, instead of printing the new bucket's rows as page two.
func TestCLIPageAcrossDropAndRecreateFails(t *testing.T) {
	c := newDumpTestServer(t)
	if err := c.handler.send(transport.OpCreateDB, &server.CreateDatabaseRequest{Name: "app"}, nil); err != nil {
		t.Fatalf("create database: %v", err)
	}
	seedCommentsForPaging(t, c, []string{"old-0", "old-1", "old-2", "old-3"})

	var terminal bytes.Buffer
	c.handler.terminal = &terminal
	if err := c.handler.handleFind("find", "app", "comments", []string{`{}`, `{limit: 2}`}); err != nil {
		t.Fatalf("page one: %v", err)
	}
	if err := c.handler.handleFind("find", "app", "comments", []string{`{}`, `{skip: 2, limit: 2}`}); err != nil {
		t.Fatalf("page two of the same bucket: %v", err)
	}

	if err := c.handler.send(transport.OpDeleteBucket, &server.DeleteBucketRequest{Database: "app", Name: "comments"}, nil); err != nil {
		t.Fatalf("drop app.comments: %v", err)
	}
	seedCommentsForPaging(t, c, []string{"new-0", "new-1", "new-2", "new-3"})

	terminal.Reset()
	err := c.handler.handleFind("find", "app", "comments", []string{`{}`, `{skip: 2, limit: 2}`})
	if err == nil {
		t.Fatalf("page two of a re-created bucket printed as a continuation:\n%s", terminal.String())
	}
	if !strings.Contains(err.Error(), "was dropped and re-created since the previous page") {
		t.Errorf("error = %v, want the dropped-and-re-created sentence", err)
	}
	if strings.Contains(terminal.String(), "new-") {
		t.Errorf("the new bucket's rows were printed before the refusal:\n%s", terminal.String())
	}

	// Starting again from the first page, as the message says, works and
	// teaches the session the new bucket's id.
	if err := c.handler.handleFind("find", "app", "comments", []string{`{}`, `{limit: 2}`}); err != nil {
		t.Fatalf("restarting from the first page: %v", err)
	}
	if err := c.handler.handleFind("find", "app", "comments", []string{`{}`, `{skip: 2, limit: 2}`}); err != nil {
		t.Fatalf("page two after restarting: %v", err)
	}
}

// TestFindWithoutArgumentsScans covers the CLI decision spec section 2 adds:
// find() with no argument is a scan, a filter document is a scan carrying that
// filter (with {skip, limit} in a second document), and a key argument stays
// the point read find("alice") has always been.
func TestFindWithoutArgumentsScans(t *testing.T) {
	cases := []struct {
		label      string
		args       []string
		opcode     transport.Opcode
		key        string
		filter     map[string]interface{}
		skip       int
		limit      int
		sort       map[string]int
		projection map[string]int
	}{
		{label: "no argument", opcode: transport.OpScan},
		{label: "bare key", args: []string{`"alice"`}, opcode: transport.OpGet, key: "alice"},
		{label: "document key", args: []string{`{_id_: "alice"}`}, opcode: transport.OpGet, key: "alice"},
		{label: "filter document", args: []string{`{Status: "active"}`}, opcode: transport.OpScan, filter: map[string]interface{}{"Status": "active"}},
		{label: "empty filter with page", args: []string{`{}`, `{skip: 100, limit: 20}`}, opcode: transport.OpScan, skip: 100, limit: 20},
		{label: "with sort ascending", args: []string{`{}`, `{sort: {age: 1}}`}, opcode: transport.OpScan, sort: map[string]int{"age": 1}},
		{label: "with sort descending", args: []string{`{}`, `{sort: {age: -1}}`}, opcode: transport.OpScan, sort: map[string]int{"age": -1}},
		{label: "with projection inclusion", args: []string{`{}`, `{projection: {name: 1, age: 1}}`}, opcode: transport.OpScan, projection: map[string]int{"name": 1, "age": 1}},
		{label: "with projection exclusion", args: []string{`{}`, `{projection: {email: 0, phone: 0}}`}, opcode: transport.OpScan, projection: map[string]int{"email": 0, "phone": 0}},
		{label: "with sort and projection", args: []string{`{}`, `{sort: {age: 1}, projection: {name: 1, age: 1}}`}, opcode: transport.OpScan, sort: map[string]int{"age": 1}, projection: map[string]int{"name": 1, "age": 1}},
	}

	for _, tc := range cases {
		t.Run(tc.label, func(t *testing.T) {
			plan, err := planFind(tc.args)
			if err != nil {
				t.Fatalf("planFind(%v): %v", tc.args, err)
			}
			if plan.opcode != tc.opcode {
				t.Fatalf("planFind(%v) chose %s, want %s", tc.args, plan.opcode, tc.opcode)
			}
			if plan.key != tc.key {
				t.Fatalf("key = %q, want %q", plan.key, tc.key)
			}
			if plan.skip != tc.skip || plan.limit != tc.limit {
				t.Fatalf("skip/limit = %d/%d, want %d/%d", plan.skip, plan.limit, tc.skip, tc.limit)
			}
			if len(plan.filter) != len(tc.filter) {
				t.Fatalf("filter = %v, want %v", plan.filter, tc.filter)
			}
			for field, want := range tc.filter {
				if plan.filter[field] != want {
					t.Fatalf("filter[%q] = %v, want %v", field, plan.filter[field], want)
				}
			}
			if len(plan.sort) != len(tc.sort) {
				t.Fatalf("sort = %v, want %v", plan.sort, tc.sort)
			}
			for field, want := range tc.sort {
				if plan.sort[field] != want {
					t.Fatalf("sort[%q] = %d, want %d", field, plan.sort[field], want)
				}
			}
			if len(plan.projection) != len(tc.projection) {
				t.Fatalf("projection = %v, want %v", plan.projection, tc.projection)
			}
			for field, want := range tc.projection {
				if plan.projection[field] != want {
					t.Fatalf("projection[%q] = %d, want %d", field, plan.projection[field], want)
				}
			}
		})
	}
}

// TestFindTruncationLine covers both halves of the spec 3.3 line: a page with
// more matches prints it with the exact bucket-wide count, and an exactly-full
// page with nothing beyond it prints nothing.
func TestFindTruncationLine(t *testing.T) {
	first := scanTruncationLine(100, 1234, 0, true)
	wantFirst := "showing 100 of 1,234 — next: find({}, {skip: 100})"
	if first != wantFirst {
		t.Fatalf("first page line = %q, want %q", first, wantFirst)
	}

	second := scanTruncationLine(100, 1234, 100, true)
	wantSecond := "showing 100 of 1,234 — next: find({}, {skip: 200})"
	if second != wantSecond {
		t.Fatalf("second page line = %q, want %q", second, wantSecond)
	}

	if line := scanTruncationLine(100, 100, 0, false); line != "" {
		t.Fatalf("an exactly-full final page printed %q, want no next line", line)
	}
	if line := scanTruncationLine(37, 37, 0, false); line != "" {
		t.Fatalf("a short final page printed %q, want no next line", line)
	}
}
