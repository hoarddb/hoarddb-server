package storage_test

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"sort"
	"strings"
	"testing"
)

const (
	badgerImportPath  = "github.com/dgraph-io/badger/v4"
	kvfaultImportPath = "github.com/hoarddb/hoarddb-server/storage/kvfault"
)

// badgerImportExceptions are the packages other than hoardDB/storage that may
// import Badger directly, each with the reason it needs to. The list is
// exact: a package on it that stops importing Badger fails the test, so it
// only ever shrinks.
var badgerImportExceptions = map[string]string{
	"github.com/hoarddb/hoarddb-server/server": "server/badger_assertion.go parses Badger's MANIFEST and key registry to prove the " +
		"store is sealed; that is a statement about Badger's own files, which the KV seam does not expose",
	"github.com/hoarddb/hoarddb-server/bench/bucketdensity": "the density harness opens Badger instances itself to measure what a " +
		"per-bucket instance costs, which is the comparison the shared layout is justified by",
}

// listedPackage is the part of `go list -json` this test reads.
type listedPackage struct {
	ImportPath string
	Imports    []string
}

// TestOnlyTheSeamImportsBadger is the end of shared-storage build Step 23: the
// KV seam in this package is the only way the node reaches Badger, so nothing
// above it can bypass the seam's key layout, sealing or fault injection. It
// reads direct imports, not -deps: every package that imports storage has
// Badger among its transitive dependencies, which says nothing about
// bypassing. Test files are excluded by construction; a test may import what
// it likes. The fault-injecting KV is a test double and no production package
// may import it.
func TestOnlyTheSeamImportsBadger(t *testing.T) {
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("the go toolchain is not on PATH; cannot list the module's packages")
	}
	command := exec.Command("go", "list", "-json", "./...")
	command.Dir = ".."
	var stderr bytes.Buffer
	command.Stderr = &stderr
	output, err := command.Output()
	if err != nil {
		t.Fatalf("go list -json ./...: %v\n%s", err, stderr.String())
	}

	var importers []string
	decoder := json.NewDecoder(bytes.NewReader(output))
	for {
		var pkg listedPackage
		if err := decoder.Decode(&pkg); err == io.EOF {
			break
		} else if err != nil {
			t.Fatalf("decoding go list output: %v", err)
		}
		for _, imported := range pkg.Imports {
			switch {
			case imported == badgerImportPath || strings.HasPrefix(imported, badgerImportPath+"/"):
				importers = append(importers, pkg.ImportPath)
			case imported == kvfaultImportPath:
				t.Errorf("%s imports %s: the fault-injecting KV is for tests only", pkg.ImportPath, kvfaultImportPath)
			}
		}
	}
	sort.Strings(importers)

	seen := map[string]bool{}
	for _, importer := range importers {
		seen[importer] = true
		if importer == "github.com/hoarddb/hoarddb-server/storage" {
			continue
		}
		if _, allowed := badgerImportExceptions[importer]; !allowed {
			t.Errorf("%s imports Badger directly. Go through storage.KV, or add it to badgerImportExceptions with the reason it cannot", importer)
		}
	}
	if !seen["github.com/hoarddb/hoarddb-server/storage"] {
		t.Errorf("github.com/hoarddb/hoarddb-server/storage no longer imports Badger: the seam has moved, and this test's premise with it")
	}
	// The density harness lives in the private tree's bench/, which the public tree does not
	// carry; its entry can only be checked where the directory exists.
	_, benchErr := os.Stat("../bench")
	benchPresent := benchErr == nil
	for exception := range badgerImportExceptions {
		if strings.HasPrefix(exception, "github.com/hoarddb/hoarddb-server/bench/") && !benchPresent {
			continue
		}
		if !seen[exception] {
			t.Errorf("%s is listed in badgerImportExceptions but no longer imports Badger: remove it from the list", exception)
		}
	}
}
