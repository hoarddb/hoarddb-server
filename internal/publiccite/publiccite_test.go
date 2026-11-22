// Package publiccite guards the published tree against citations of paths that
// only the private repository carries.
//
// Only this module is published, and its user docs arrive one directory higher
// than in the private repository. A comment that cites a spec path sends a
// public reader to a file that does not exist, and a message that prints a
// user-docs path prints one that is wrong in the published layout. The guard reads every Go file with the Go
// scanner, so it judges comments and string literals separately.
package publiccite

import (
	"go/scanner"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

var (
	// privateDocsPath covers every directory the published tree lacks.
	privateDocsPath = regexp.MustCompile(`docs/(?:specs|internals|bugs|user)\b`)
)

// A string literal in a test file may name a private path: tests open those
// files through repofile, which maps the path to the layout the test runs in
// and skips by name where the file does not exist. The same goes for repofile
// itself, which holds the mapping.
const repofileSource = "repofile.go"

func moduleRoot(t *testing.T) string {
	t.Helper()
	// The test runs in internal/publiccite.
	return filepath.Join("..", "..")
}

func TestPublishedCodeNamesNoPrivateDocPaths(t *testing.T) {
	root := moduleRoot(t)
	selfName := "publiccite_test.go"
	var findings []string
	walkErr := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || entry.Name() == selfName {
			return nil
		}
		source, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		findings = append(findings, scanFile(path, source)...)
		return nil
	})
	if walkErr != nil {
		t.Fatalf("walking %s: %v", root, walkErr)
	}
	for _, finding := range findings {
		t.Error(finding)
	}
}

func scanFile(path string, source []byte) []string {
	fileSet := token.NewFileSet()
	file := fileSet.AddFile(path, fileSet.Base(), len(source))
	var tokenScanner scanner.Scanner
	tokenScanner.Init(file, source, nil, scanner.ScanComments)
	isTest := strings.HasSuffix(path, "_test.go")
	var findings []string
	for {
		position, kind, text := tokenScanner.Scan()
		if kind == token.EOF {
			return findings
		}
		isComment := kind == token.COMMENT
		isCheckedString := kind == token.STRING && !isTest && filepath.Base(path) != repofileSource
		if !isComment && !isCheckedString {
			continue
		}
		if match := privateDocsPath.FindString(text); match != "" {
			findings = append(findings, fileSet.Position(position).String()+": names "+match+", a path the published tree does not carry")
		}
	}
}
