package bsonoracle

import (
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// moduleDirectory is hoardDB/, where go list runs from.
func moduleDirectory(t *testing.T) string {
	t.Helper()
	goModOutput, err := exec.Command("go", "env", "GOMOD").Output()
	if err != nil {
		t.Fatalf("locating go.mod with go env GOMOD: %v", err)
	}
	return filepath.Dir(strings.TrimSpace(string(goModOutput)))
}

func goList(t *testing.T, arguments ...string) string {
	t.Helper()
	command := exec.Command("go", append([]string{"list"}, arguments...)...)
	command.Dir = moduleDirectory(t)
	output, err := command.Output()
	if err != nil {
		t.Fatalf("go list %s in %s: %v", strings.Join(arguments, " "), command.Dir, err)
	}
	return strings.TrimSpace(string(output))
}

// TestDefaultBuildsNeverLinkTheTap is the 4f deps proof that holds on both
// trees: no dependency list of the two binaries, with or without the
// bsonoracle tag, holds the oracle shim or the tap, so the tap that installs
// the library's hook never reaches a binary. The first version of this test
// also refused every hoarddb-bson package; that half cannot hold once the
// swap makes the library the server's codec, and it is superseded by step 6d's
// boundary gates (TestBinariesLinkNoVendor and its neighbours in
// boundary_test.go), which pin the other side of the boundary: no vendor in a
// binary.
func TestDefaultBuildsNeverLinkTheTap(t *testing.T) {
	for _, tags := range [][]string{nil, {"-tags", "bsonoracle"}} {
		arguments := append(append([]string{"-deps"}, tags...), "-f", "{{.ImportPath}}", "./cmd/server", "./cmd/cli")
		listed := strings.Fields(goList(t, arguments...))
		if len(listed) < 100 {
			t.Fatalf("go list %v listed %d packages; the check would pass over a truncated list", arguments, len(listed))
		}
		for _, importPath := range listed {
			if strings.Contains(importPath, "internal/bsonoracle") {
				t.Errorf("tags %v: cmd/server or cmd/cli depends on %s. The oracle shim and the tap are for test code; they must not reach a binary", tags, importPath)
			}
		}
	}
}

// TestTapPackageHasNoCodeWithoutTheTag: the tap's only untagged file is its
// package comment, so a default build of any package that imported it would
// still carry no tap.
func TestTapPackageHasNoCodeWithoutTheTag(t *testing.T) {
	untagged := strings.Fields(goList(t, "-f", "{{join .GoFiles \" \"}}", "./internal/bsonoracle/tap"))
	if len(untagged) != 1 || untagged[0] != "doc.go" {
		t.Errorf("untagged files of the tap package: %v, want only doc.go", untagged)
	}
	tagged := strings.Fields(goList(t, "-tags", "bsonoracle", "-f", "{{join .GoFiles \" \"}}", "./internal/bsonoracle/tap"))
	if len(tagged) != 2 {
		t.Errorf("files of the tap package with the tag: %v, want doc.go and tap.go", tagged)
	}
}
