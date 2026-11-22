package cli

import (
	"os"
	"strings"
	"testing"
)

// planTraceFixtureKinds are the collections the seven spec section 4.2 lines
// read. They are not all in blog.model.json, which has users, posts and
// comments only, so the fixture is read against this list.
var planTraceFixtureKinds = []string{"users", "posts", "comments", "scores", "jobs", "avatars"}

// TestLoadTraceBlogFixture guards the happy path: the seven operations of spec
// section 4.2 load in file order, each with its ordinal, op and collection.
func TestLoadTraceBlogFixture(t *testing.T) {
	file, err := os.Open("testdata/plan/blog.trace.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := file.Close(); err != nil {
			t.Errorf("closing the trace fixture: %v", err)
		}
	})
	lines, err := LoadTrace(file, planTraceFixtureKinds)
	if err != nil {
		t.Fatalf("LoadTrace: %v", err)
	}
	want := []struct{ op, coll string }{
		{"get", "users"}, {"list", "comments"}, {"insert", "comments"}, {"list", "comments"},
		{"take", "jobs"}, {"range", "scores"}, {"put", "avatars"},
	}
	if len(lines) != len(want) {
		t.Fatalf("got %d lines, want %d", len(lines), len(want))
	}
	for i, line := range lines {
		if line.Ordinal != i+1 {
			t.Fatalf("line %d has ordinal %d, want %d", i+1, line.Ordinal, i+1)
		}
		if line.Op != want[i].op || line.Coll != want[i].coll {
			t.Fatalf("line %d is %s %s, want %s %s", i+1, line.Op, line.Coll, want[i].op, want[i].coll)
		}
	}
}

// TestLoadTraceRefusalsNameLineAndAccepted guards the repair message: an
// unknown op must name its line, the value, and every accepted op.
func TestLoadTraceRefusalsNameLineAndAccepted(t *testing.T) {
	src := `{"op":"get","coll":"users","by":["_id"]}` + "\n" +
		`{"op":"insert","coll":"comments"}` + "\n" +
		`{"op":"fetch","coll":"users"}` + "\n"
	_, err := LoadTrace(strings.NewReader(src), planTraceFixtureKinds)
	const want = `trace line 3: op "fetch" is not one of get, list, range, insert, update, delete, take, put, join, aggregate, search`
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("error %v does not contain %q", err, want)
	}
}
