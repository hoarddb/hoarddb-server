package cli

import (
	"encoding/json"
	"fmt"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"
)

// planKindByName indexes a model's kinds by name for assertions.
func planKindByName(model *PlanModel) map[string]ModelKind {
	kinds := make(map[string]ModelKind, len(model.Kinds))
	for _, kind := range model.Kinds {
		kinds[kind.Name] = kind
	}
	return kinds
}

// planFieldNamed returns the named field of a kind, or nil.
func planFieldNamed(kind ModelKind, name string) *ModelField {
	for index := range kind.Fields {
		if kind.Fields[index].Name == name {
			return &kind.Fields[index]
		}
	}
	return nil
}

// TestPlanCollectBlogDump guards the counts and the reference resolution the
// blog example depends on. The spec also names comments.ts as the OrderField;
// OrderField is a fact of an array (ModelArray), and the blog's comments carry
// no array, so this asserts the datetime the model can hold.
func TestPlanCollectBlogDump(t *testing.T) {
	dir := t.TempDir()
	writeBlogDump(t, dir)

	model, _, err := planCollect([]string{dir}, 0, defaultMaxBuckets)
	if err != nil {
		t.Fatalf("planCollect: %v", err)
	}
	kinds := planKindByName(model)
	for name, want := range map[string]int64{"comments": 24000, "posts": 2000, "users": 200} {
		kind, ok := kinds[name]
		if !ok {
			t.Fatalf("kind %q missing; got %d kinds", name, len(model.Kinds))
		}
		if kind.Documents != want || kind.Sampled != want {
			t.Fatalf("kind %q: documents %d sampled %d, want %d", name, kind.Documents, kind.Sampled, want)
		}
	}
	postID := planFieldNamed(kinds["comments"], "post_id")
	if postID == nil || postID.Ref == nil {
		t.Fatalf("comments.post_id has no reference: %+v", postID)
	}
	if postID.Ref.To != "posts" || postID.Ref.ResolvePermille < 990 {
		t.Fatalf("comments.post_id ref = %+v, want posts at 990 per-mille or more", postID.Ref)
	}
	ts := planFieldNamed(kinds["comments"], "ts")
	if ts == nil || ts.Type != "datetime" || ts.PresentPermille != 1000 {
		t.Fatalf("comments.ts = %+v, want a datetime present in every document", ts)
	}
}

// TestBlogModelMatchesGeneratedDump pins the committed fixture to the dump the
// generator writes: a change to either side without the other is drift.
func TestBlogModelMatchesGeneratedDump(t *testing.T) {
	dir := t.TempDir()
	writeBlogDump(t, dir)
	generated, _, err := planCollect([]string{dir}, 0, defaultMaxBuckets)
	if err != nil {
		t.Fatalf("planCollect: %v", err)
	}
	file, err := os.Open(filepath.Join("testdata", "plan", "blog.model.json"))
	if err != nil {
		t.Fatalf("opening the committed model: %v", err)
	}
	defer func() { _ = file.Close() }()
	committed, err := LoadPlanModel(file)
	if err != nil {
		t.Fatalf("loading the committed model: %v", err)
	}
	if !reflect.DeepEqual(generated.Kinds, committed.Kinds) {
		got, _ := json.MarshalIndent(generated.Kinds, "", "  ")
		t.Fatalf("committed blog.model.json drifted from the generated dump; generated kinds:\n%s", got)
	}
}

// TestPlanCollectRefusesAsIngestDoes guards that a plan refuses an input the way
// ingest refuses it, with the same sentence, before reading any document.
func TestPlanCollectRefusesAsIngestDoes(t *testing.T) {
	gzipped := filepath.Join(t.TempDir(), "dump.bson.gz")
	if err := os.WriteFile(gzipped, []byte{0x1f, 0x8b, 8, 0}, 0o600); err != nil {
		t.Fatal(err)
	}
	pretty := filepath.Join(t.TempDir(), "one.json")
	if err := os.WriteFile(pretty, []byte("{\n  \"a\": 1\n}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	unnamed := filepath.Join(t.TempDir(), "notes.txt")
	if err := os.WriteFile(unnamed, []byte("hello\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	crowded := t.TempDir()
	for index := range defaultMaxBuckets + 1 {
		if err := os.WriteFile(filepath.Join(crowded, fmt.Sprintf("c%04d.bson", index)), []byte{5, 0, 0, 0, 0}, 0o600); err != nil {
			t.Fatal(err)
		}
	}

	cases := map[string]func() error{
		"gzip file": func() error {
			head, _ := readHead(gzipped)
			_, err := detectFormat(gzipped, head, "")
			return err
		},
		"pretty-printed JSON": func() error {
			head, _ := readHead(pretty)
			_, err := detectFormat(pretty, head, "")
			return err
		},
		"unnamed format": func() error {
			head, _ := readHead(unnamed)
			_, err := detectFormat(unnamed, head, "")
			return err
		},
		"directory over the bucket ceiling": func() error {
			_, err := resolveDirectory(directoryRequest{Input: crowded, MaxBuckets: defaultMaxBuckets})
			return err
		},
	}
	inputs := map[string]string{
		"gzip file": gzipped, "pretty-printed JSON": pretty, "unnamed format": unnamed, "directory over the bucket ceiling": crowded,
	}
	for name, want := range cases {
		wantErr := want()
		if wantErr == nil {
			t.Fatalf("%s: ingest does not refuse it; the fixture is wrong", name)
		}
		_, _, err := planCollect([]string{inputs[name]}, 0, defaultMaxBuckets)
		if err == nil {
			t.Fatalf("%s: planCollect accepted it", name)
		}
		if err.Error() != wantErr.Error() {
			t.Fatalf("%s: planCollect said %q, ingest says %q", name, err, wantErr)
		}
	}
}

// TestPlanNeverConnectsWithoutFrom guards spec T12: a run over files opens no
// socket. Go has no hook that sees every dial, so it checks two things: no plan
// file imports a package that opens sockets, and no descriptor opened during the
// run is still open afterwards. A socket that is kept fails the second check; a
// dial that is closed at once is caught by the first.
func TestPlanNeverConnectsWithoutFrom(t *testing.T) {
	files, err := filepath.Glob("plan_*.go")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(fset, name, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		for _, spec := range parsed.Imports {
			path, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				t.Fatalf("%s: import path %s: %v", name, spec.Path.Value, err)
			}
			if socketPackages[path] {
				t.Errorf("%s imports %s; a plan over files must open no socket", name, path)
			}
		}
	}

	dir := t.TempDir()
	writeBlogDump(t, dir)
	before := openDescriptors(t)
	if _, _, err := planCollect([]string{dir}, 100, defaultMaxBuckets); err != nil {
		t.Fatalf("planCollect: %v", err)
	}
	if after := openDescriptors(t); after > before {
		t.Fatalf("a plan over files left %d descriptors open; a socket is never meant to outlive a run", after-before)
	}
}

// socketPackages are the standard packages a plan file must not import to reach
// the network.
var socketPackages = map[string]bool{"net": true, "net/http": true, "syscall": true}

// openDescriptors counts this process's open file descriptors on Linux. Elsewhere
// the count is unavailable and the test relies on the import check alone.
func openDescriptors(t *testing.T) int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Skipf("no /proc/self/fd to count descriptors: %v", err)
	}
	return len(entries)
}

// writeSameShapedFiles writes files customer-NNN.bson into dir, each holding three
// customer documents of one shape: the same fields, a different customer per file.
func writeSameShapedFiles(t *testing.T, dir string, files int) {
	t.Helper()
	statuses := []string{"open", "closed", "held"}
	for file := range files {
		var content []byte
		for index := range 3 {
			content = appendBSON(t, content, bson.D{
				{Key: "_id", Value: primitive.NewObjectID()},
				{Key: "customer", Value: fmt.Sprintf("%03d", file)},
				{Key: "status", Value: statuses[index]},
				{Key: "amount", Value: int32(index * 10)},
			})
		}
		name := fmt.Sprintf("customer-%03d.bson", file)
		if err := os.WriteFile(filepath.Join(dir, name), content, 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

// TestPlanDirectoryOfSameShapedFilesIsOneKind guards spec T5: a directory of
// many files of one shape is one kind, and the plan says the field that tells
// the files apart.
func TestPlanDirectoryOfSameShapedFilesIsOneKind(t *testing.T) {
	dir := t.TempDir()
	writeSameShapedFiles(t, dir, 300)

	model, notes, err := planCollect([]string{dir}, 0, defaultMaxBuckets)
	if err != nil {
		t.Fatalf("planCollect: %v", err)
	}
	if len(model.Kinds) != 1 {
		t.Fatalf("300 same-shaped files made %d kinds, want 1", len(model.Kinds))
	}
	kind := model.Kinds[0]
	if kind.Name != "customer" || kind.Documents != 900 {
		t.Fatalf("kind %q with %d documents, want customer with 900", kind.Name, kind.Documents)
	}
	if planFieldNamed(kind, "customer") == nil {
		t.Fatalf("kind %q has no customer field", kind.Name)
	}
	joined := strings.Join(notes, "\n")
	if !strings.Contains(joined, "one kind with a discriminating field") || !strings.Contains(joined, `"customer"`) {
		t.Fatalf("notes do not say the discriminating field:\n%s", joined)
	}
}
