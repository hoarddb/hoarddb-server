package repofile

import (
	"os"
	"path/filepath"
	"testing"
)

// writeTree creates the named files (with placeholder content) under root.
func writeTree(t *testing.T, root string, files ...string) {
	t.Helper()
	for _, name := range files {
		path := filepath.Join(root, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("creating %s: %v", filepath.Dir(path), err)
		}
		if err := os.WriteFile(path, []byte("content of "+name), 0o644); err != nil {
			t.Fatalf("writing %s: %v", path, err)
		}
	}
}

// skippedBy reports whether fn, run as a subtest, called t.Skip. ReadPrivate
// skips by stopping its goroutine, so the only way to observe it is from a
// subtest's cleanup.
func skippedBy(t *testing.T, fn func(t *testing.T)) bool {
	t.Helper()
	skipped := false
	t.Run("probe", func(t *testing.T) {
		t.Cleanup(func() { skipped = t.Skipped() })
		fn(t)
	})
	return skipped
}

// TestReadPrivateFindsPrivateFilesInThePrivateLayout is the guard against a
// vacuous pass: ReadPrivate skips when a file is missing, so if the private
// layout were mis-detected every private-only test would skip and still show
// green. Here the file exists one level above the module, as in the repository,
// and the read must return it and must not skip.
func TestReadPrivateFindsPrivateFilesInThePrivateLayout(t *testing.T) {
	repo := t.TempDir()
	writeTree(t, repo, "github.com/hoarddb/hoarddb-server/go.mod", "github.com/hoarddb/hoarddb-server/config/x.go", "docs/specs/spec.md", "docs/user/page.md", "Dockerfile")
	t.Chdir(filepath.Join(repo, "hoardDB", "config"))

	var got []byte
	if skippedBy(t, func(t *testing.T) { got = ReadPrivate(t, "docs/specs/spec.md") }) {
		t.Fatal("ReadPrivate skipped for a file that exists in the private layout")
	}
	if string(got) != "content of docs/specs/spec.md" {
		t.Fatalf("ReadPrivate returned %q", got)
	}
	if page := Read(t, "docs/user/page.md"); string(page) != "content of docs/user/page.md" {
		t.Fatalf("Read of a user page returned %q", page)
	}
	if file := Read(t, "Dockerfile"); string(file) != "content of Dockerfile" {
		t.Fatalf("Read of the Dockerfile returned %q", file)
	}
}

// TestPublishedLayoutMapsUserDocsAndSkipsPrivateOnlyFiles covers the other
// tree: the module is the root, the user docs are in docs/, and a spec is absent.
func TestPublishedLayoutMapsUserDocsAndSkipsPrivateOnlyFiles(t *testing.T) {
	root := t.TempDir()
	writeTree(t, root, "go.mod", "config/x.go", "docs/page.md", "Dockerfile")
	t.Chdir(filepath.Join(root, "config"))

	if page := Read(t, "docs/user/page.md"); string(page) != "content of docs/page.md" {
		t.Fatalf("docs/user/page.md should map to docs/page.md, got %q", page)
	}
	if file := Read(t, "Dockerfile"); string(file) != "content of Dockerfile" {
		t.Fatalf("Read of the Dockerfile returned %q", file)
	}
	if !skippedBy(t, func(t *testing.T) { ReadPrivate(t, "docs/specs/spec.md") }) {
		t.Fatal("ReadPrivate must skip for a private-only file that the published tree lacks")
	}
}

// TestTheRealRepositoryIsSeenAsPrivateWhenItIs fails if, in the private
// repository, ReadPrivate would skip for a spec that is there. It does nothing
// in the published tree, which has no specs directory.
func TestTheRealRepositoryIsSeenAsPrivateWhenItIs(t *testing.T) {
	root, err := moduleRoot()
	if err != nil {
		t.Fatal(err)
	}
	const spec = "docs/specs/at-rest-encryption-spec.md"
	if _, err := os.Stat(filepath.Join(root, "..", filepath.FromSlash(spec))); err != nil {
		t.Skip("not the private repository layout")
	}
	if skippedBy(t, func(t *testing.T) { ReadPrivate(t, spec) }) {
		t.Fatalf("ReadPrivate skipped for %s, which exists above the module: private-layout tests would pass vacuously", spec)
	}
}
