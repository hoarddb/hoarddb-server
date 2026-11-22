package bsonoracle

import (
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// shimImportPath is this package's import path, and the prefix of any
// subpackage a later step adds under it.
const shimImportPath = "github.com/hoarddb/hoarddb-server/internal/bsonoracle"

// TestNoProductionPackageImportsTheShim is spec §9.3 gate 4: the shim links
// the vendor, so a non-test importer would put the oracle in a binary. go list
// without -test reports only the imports of non-test files.
func TestNoProductionPackageImportsTheShim(t *testing.T) {
	goModOutput, err := exec.Command("go", "env", "GOMOD").Output()
	if err != nil {
		t.Fatalf("locating go.mod with go env GOMOD: %v", err)
	}
	listCommand := exec.Command("go", "list", "-f", `{{.ImportPath}} {{join .Imports " "}}`, "./...")
	listCommand.Dir = filepath.Dir(strings.TrimSpace(string(goModOutput)))
	listOutput, err := listCommand.Output()
	if err != nil {
		t.Fatalf("listing the module's packages and imports in %s: %v", listCommand.Dir, err)
	}
	packagesListed := 0
	for _, line := range strings.Split(strings.TrimSpace(string(listOutput)), "\n") {
		fields := strings.Fields(line)
		packagesListed++
		importer := fields[0]
		if importer == shimImportPath || strings.HasPrefix(importer, shimImportPath+"/") {
			continue
		}
		for _, imported := range fields[1:] {
			if imported == shimImportPath || strings.HasPrefix(imported, shimImportPath+"/") {
				t.Errorf("non-test package %s imports %s. The shim links the vendor BSON codec and is for _test.go files only (bson-codec-spec §9.3); move the import into a _test.go file", importer, imported)
			}
		}
	}
	if packagesListed < 2 {
		t.Fatalf("go list ./... in %s listed %d packages; the check would pass over an empty list", listCommand.Dir, packagesListed)
	}
}
