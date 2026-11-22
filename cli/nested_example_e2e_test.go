package cli

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestNestedSchemaExampleRuns is spec T-19 (RD-9): the checked-in example —
// spec §3.1's schema, two posts, their comments — runs through the CLI
// against a real server, and prints what a developer following it expects.
// Before the feature the create statement made a plain hash bucket and
// dropped every declaration, which the "hql-spec" user-docs page warns of.
func TestNestedSchemaExampleRuns(t *testing.T) {
	c := newDumpTestServer(t)
	var out bytes.Buffer
	c.handler.terminal = &out
	script, err := os.ReadFile(filepath.Join("testdata", "nested-example.hql"))
	if err != nil {
		t.Fatalf("reading the example: %v", err)
	}
	var outputs []string
	for _, line := range strings.Split(strings.TrimSpace(string(script)), "\n") {
		out.Reset()
		if err := c.handle(line); err != nil {
			t.Fatalf("%s\n  failed: %v", line, err)
		}
		outputs = append(outputs, out.String())
	}
	expect := func(statement int, parts ...string) {
		t.Helper()
		for _, part := range parts {
			if !strings.Contains(outputs[statement], part) {
				t.Errorf("statement %d printed\n%s\nwhich does not say %q", statement+1, outputs[statement], part)
			}
		}
	}
	expect(2, "created bucket PhotoJournal.Post", "2 store field(s) declared",
		"note: field types on scalar fields (_id_ uuid auto, Caption string) are not enforced in this release; only store fields are created.")
	expect(9, `"Author": "ada"`, `"Text": "first"`)
	expect(10, "2\n")
	expect(11, "1\n")
	expect(12, "Stores:", "Photos", "blobStore  0 element(s)", "Comments", "fifoStore  3 element(s)")
}
