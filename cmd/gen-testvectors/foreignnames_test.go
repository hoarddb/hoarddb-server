package main

import (
	"os"
	"strings"
	"testing"
)

// foreignProductNames are other databases' names. The driver repositories sync
// the generated vector files and refuse any file that names one, and a driver
// cannot edit a synced file, so the generators must never write one. The
// wire-format library is described without its vendor or module path.
var foreignProductNames = []string{"mongo", "postgres", "mysql", "mariadb", "redis", "cassandra", "couchbase", "dynamodb", "sqlite", "rocksdb"}

// TestGeneratedVectorFilesNameNoForeignProduct pins that the generated vector
// files, committed and freshly generated, contain no other database's name.
func TestGeneratedVectorFilesNameNoForeignProduct(t *testing.T) {
	driverVectors, err := os.ReadFile(committedVectorsPath)
	if err != nil {
		t.Skipf("no committed driver vectors (published server tree): %v", err)
	}
	generatedFiles := map[string][]byte{
		"fresh bson-vectors":       freshBSONCorpus(t, "0000000000000000000000000000000000000000"),
		"committed bson-vectors":   committedBSONCorpus(t),
		"committed trust-vectors":  committedTrustCorpus(t),
		"committed driver-vectors": driverVectors,
	}

	for fileName, content := range generatedFiles {
		lowered := strings.ToLower(string(content))
		for _, productName := range foreignProductNames {
			index := strings.Index(lowered, productName)
			if index < 0 {
				continue
			}
			start, end := max(index-40, 0), min(index+60, len(lowered))
			t.Errorf("%s names %q near %q: describe the library without its vendor or module path", fileName, productName, lowered[start:end])
		}
	}
}
