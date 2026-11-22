// bench/nomix_test.go — keeps the MongoDB client and hoarddb-bson out of the
// same benchmark file.
package main

import (
	"fmt"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

const (
	mongoClientImport = "go.mongodb.org/mongo-driver/mongo"
	bsonLibraryImport = "github.com/hoarddb/hoarddb-bson"
)

// TestNoFileImportsTheMongoClientAndTheLibrary fails when one bench file mixes
// the two codecs. A file on the MongoDB client times the vendor and a file on
// hoarddb-bson times the library; a file importing both would leave a number's
// codec ambiguous, and it would also keep the vendor in the dependencies of a
// binary that must not link it (bson-codec-spec §7.5, O14).
func TestNoFileImportsTheMongoClientAndTheLibrary(t *testing.T) {
	fset := token.NewFileSet()
	visited := 0
	var mixed []string
	walkErr := filepath.WalkDir(".", func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || filepath.Ext(path) != ".go" {
			return nil
		}
		visited++
		file, parseErr := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if parseErr != nil {
			return fmt.Errorf("parse %s: %w", path, parseErr)
		}
		usesMongoClient, usesLibrary := false, false
		for _, spec := range file.Imports {
			imported, unquoteErr := strconv.Unquote(spec.Path.Value)
			if unquoteErr != nil {
				return fmt.Errorf("unquote the import in %s: %w", path, unquoteErr)
			}
			switch {
			case imported == mongoClientImport || strings.HasPrefix(imported, mongoClientImport+"/"):
				usesMongoClient = true
			case imported == bsonLibraryImport || strings.HasPrefix(imported, bsonLibraryImport+"/"):
				usesLibrary = true
			}
		}
		if usesMongoClient && usesLibrary {
			mixed = append(mixed, path)
		}
		return nil
	})
	if walkErr != nil {
		t.Fatalf("walk the bench tree: %v", walkErr)
	}
	if visited == 0 {
		t.Fatal("found no .go file under bench/: the test ran outside its package directory")
	}
	for _, path := range mixed {
		t.Errorf("%s imports both %s and %s: keep the vendor client in its own file, or move the shared wire helpers to a file that imports neither", path, mongoClientImport, bsonLibraryImport)
	}
}
