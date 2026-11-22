package cli

import (
	"bytes"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// TestScanAndSearchRenderRawDocuments pins the CLI's find() and search output
// against a live server. Scan and search results reach the CLI
// as the stored BSON, undecoded; printDocuments hands each to jsonLine, which
// decodes it exactly as it does a point read's FindResponse.Data, so the
// printed lines stay what they were.
func TestScanAndSearchRenderRawDocuments(t *testing.T) {
	c := newDumpTestServer(t)
	if err := c.handler.send(transport.OpCreateDB, &server.CreateDatabaseRequest{Name: "app"}, nil); err != nil {
		t.Fatalf("create database: %v", err)
	}
	if err := c.handler.send(transport.OpCreateBucket, &server.CreateBucketRequest{
		Database: "app", Name: "users", Type: server.BucketTypeHash,
		Options: map[string]interface{}{"indices": []interface{}{"team"}},
	}, nil); err != nil {
		t.Fatalf("create indexed hash bucket: %v", err)
	}
	for _, key := range []string{"a", "b", "c"} {
		team := "red"
		if key == "b" {
			team = "blue"
		}
		if err := c.handler.send(transport.OpPut, &server.InsertRequest{
			Database: "app", Bucket: "users", Key: key, Data: map[string]interface{}{"name": key + "-name", "team": team, "n": int32(len(key))},
		}, nil); err != nil {
			t.Fatalf("insert %s: %v", key, err)
		}
	}

	var terminal bytes.Buffer
	c.handler.terminal = &terminal

	if err := c.handler.handleFind("find", "app", "users", nil); err != nil {
		t.Fatalf("handleFind: %v", err)
	}
	wantScan := `{"_id_": "a", "n": 1, "name": "a-name", "team": "red"}` + "\n" +
		`{"_id_": "b", "n": 1, "name": "b-name", "team": "blue"}` + "\n" +
		`{"_id_": "c", "n": 1, "name": "c-name", "team": "red"}` + "\n"
	if got := terminal.String(); got != wantScan {
		t.Errorf("find() output:\n%s\nwant:\n%s", got, wantScan)
	}

	terminal.Reset()
	if err := c.handler.handleSearch("app", "users", []string{`{team: "blue"}`}); err != nil {
		t.Fatalf("handleSearch: %v", err)
	}
	wantSearch := "Found 1 results (via team_1):\n" + `{"_id_": "b", "n": 1, "name": "b-name", "team": "blue"}` + "\n"
	if got := terminal.String(); got != wantSearch {
		t.Errorf("search output:\n%s\nwant:\n%s", got, wantSearch)
	}
}

// TestSearchSaysWhetherAnIndexServedIt is the property behind that header: a
// search names the index it used, or says it scanned. Before this the server
// sent index_used on every response and the CLI dropped it, so a developer
// could not tell an index hit from a full scan — which is the only way to tell
// whether a declared index is earning its keep.
//
// Four paths, all against a live server, because which path runs is the
// server's planner's decision and not something the CLI can fake: an equality
// on an indexed field, an equality on an unindexed field, an operator on an
// indexed field (never an equality, so never an index), and an indexed search
// with no hits.
func TestSearchSaysWhetherAnIndexServedIt(t *testing.T) {
	c := newDumpTestServer(t)
	if err := c.handler.send(transport.OpCreateDB, &server.CreateDatabaseRequest{Name: "app"}, nil); err != nil {
		t.Fatalf("create database: %v", err)
	}
	if err := c.handler.send(transport.OpCreateBucket, &server.CreateBucketRequest{
		Database: "app", Name: "users", Type: server.BucketTypeHash,
		Options: map[string]interface{}{"indices": []interface{}{"team"}},
	}, nil); err != nil {
		t.Fatalf("create indexed hash bucket: %v", err)
	}
	for _, doc := range []map[string]interface{}{
		{"_id_": "a", "name": "ada", "team": "red", "n": int32(1)},
		{"_id_": "b", "name": "bob", "team": "blue", "n": int32(2)},
	} {
		if err := c.handler.send(transport.OpPut, &server.InsertRequest{
			Database: "app", Bucket: "users", Key: doc["_id_"].(string), Data: doc,
		}, nil); err != nil {
			t.Fatalf("insert %v: %v", doc["_id_"], err)
		}
	}

	cases := []struct {
		name   string
		filter string
		want   string
	}{
		{"equality on the indexed field", `{team: "blue"}`, "Found 1 results (via team_1):\n"},
		{"equality on an unindexed field", `{name: "ada"}`, "Found 1 results (full scan):\n"},
		{"operator on the indexed field", `{team: {$regex: "bl"}}`, "Found 1 results (full scan):\n"},
		{"indexed, no hits", `{team: "green"}`, "NO RESULTS (via team_1)\n"},
		{"unindexed, no hits", `{name: "nobody"}`, "NO RESULTS (full scan)\n"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var terminal bytes.Buffer
			c.handler.terminal = &terminal
			if err := c.handler.handleSearch("app", "users", []string{tc.filter}); err != nil {
				t.Fatalf("handleSearch(%s): %v", tc.filter, err)
			}
			got := terminal.String()
			if !strings.HasPrefix(got, tc.want) {
				t.Fatalf("search(%s) header:\n%s\nwant it to start with %q", tc.filter, got, tc.want)
			}
			// The index note belongs on the header, never interleaved into
			// the documents: a reader counting result lines must not find a
			// line that is not a document.
			if lines := strings.Split(strings.TrimRight(got, "\n"), "\n"); tc.want != "" && lines[0] != strings.TrimRight(tc.want, "\n") {
				t.Fatalf("the index note is not on the first line: %q", got)
			}
		})
	}
}

// TestFindDoesNotClaimToUseAnIndex: find has no index path, so printing the
// server's constant "none — full scan" under every scan would be noise on the
// verb a developer reaches for first. Its output must not change.
func TestFindDoesNotClaimToUseAnIndex(t *testing.T) {
	c := newDumpTestServer(t)
	if err := c.handler.send(transport.OpCreateDB, &server.CreateDatabaseRequest{Name: "app"}, nil); err != nil {
		t.Fatalf("create database: %v", err)
	}
	if err := c.handler.send(transport.OpCreateBucket, &server.CreateBucketRequest{
		Database: "app", Name: "users", Type: server.BucketTypeHash,
		Options: map[string]interface{}{"indices": []interface{}{"team"}},
	}, nil); err != nil {
		t.Fatalf("create indexed hash bucket: %v", err)
	}
	if err := c.handler.send(transport.OpPut, &server.InsertRequest{
		Database: "app", Bucket: "users", Key: "a", Data: map[string]interface{}{"name": "ada", "team": "red"},
	}, nil); err != nil {
		t.Fatalf("insert: %v", err)
	}

	var terminal bytes.Buffer
	c.handler.terminal = &terminal
	if err := c.handler.handleFind("find", "app", "users", []string{`{team: "red"}`}); err != nil {
		t.Fatalf("handleFind: %v", err)
	}
	got := terminal.String()
	if strings.Contains(got, "full scan") || strings.Contains(got, "team_1") {
		t.Fatalf("find output mentions an index (%q); find always scans and must not claim otherwise", got)
	}
}
