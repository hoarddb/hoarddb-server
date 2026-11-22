// Package repofile locates files that tests read from outside their own package
// and that sit in a different place in the two trees this module is seen in.
//
// In the private repository the Go module is the hoardDB/ directory and the
// files live above it: the user docs, Dockerfile,
// docker-compose.yml. In the published tree (scripts/publish-tree.sh) the
// module is the tree root, the user docs are in docs/, and the
// container files sit beside go.mod. A test that built a path up out of the module
// passed in the first and failed in the second, which is how the published
// tree's own test run would have gone red for a reason no user could fix.
//
// Callers name the file by its PRIVATE-tree path, relative to the repository
// root (the user docs sit under a "user" directory of docs/ there); this
// package maps it.
package repofile

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// moduleRoot walks up from the test's working directory to the directory that
// holds go.mod.
func moduleRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("working directory: %w", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", errors.New("no go.mod in any parent of the working directory")
		}
		dir = parent
	}
}

// Find returns the path of the file named by its private-tree path
// (a user-docs page, "Dockerfile"), looking in the published
// layout first and the private layout second. The published layout is checked
// first because the private module directory has neither a docs/ nor a
// Dockerfile of its own, so the first match cannot be a lookalike.
func Find(privatePath string) (string, error) {
	root, err := moduleRoot()
	if err != nil {
		return "", err
	}
	published := privatePath
	if privatePath == "docs/user" {
		published = "docs"
	} else if rest, ok := strings.CutPrefix(privatePath, "docs/user/"); ok {
		published = filepath.Join("docs", rest)
	}
	candidates := []string{
		filepath.Join(root, filepath.FromSlash(published)),
		filepath.Join(root, "..", filepath.FromSlash(privatePath)),
	}
	for _, candidate := range candidates {
		if _, err := os.Stat(candidate); err == nil {
			return candidate, nil
		}
	}
	return "", fmt.Errorf("%s: %w (looked in %s)", privatePath, fs.ErrNotExist, strings.Join(candidates, ", "))
}

// Read returns the file's bytes, failing the test when it cannot be found. Use
// it for files both trees carry.
func Read(t testing.TB, privatePath string) []byte {
	t.Helper()
	path, err := Find(privatePath)
	if err != nil {
		t.Fatalf("locating %s: %v", privatePath, err)
	}
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	return content
}

// ReadPrivate returns the file's bytes, or skips the test when the file is not
// there. Use it for files only the private repository carries (specs, bug
// files): the published tree has no copy, and a test that checks a spec against
// code has nothing to check there.
func ReadPrivate(t testing.TB, privatePath string) []byte {
	t.Helper()
	path, err := Find(privatePath)
	if errors.Is(err, fs.ErrNotExist) {
		t.Skipf("%s is not part of the published tree", privatePath)
	}
	if err != nil {
		t.Fatalf("locating %s: %v", privatePath, err)
	}
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	return content
}
