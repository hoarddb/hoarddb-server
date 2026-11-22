package bsonoracle

import (
	"encoding/json"
	"go/parser"
	"go/token"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// The dependency boundary of bson-codec-spec 9.3 and build doc step 6d. The
// vendor (go.mongodb.org/mongo-driver) may be imported in exactly three places:
// bench/, _test.go files using the 9.1 oracle, and this package, which only
// _test.go files import. Gate 4 is TestNoProductionPackageImportsTheShim in
// importers_test.go. The gates are written to fail on the tree before the step
// 6 swap, where every server package imports the vendor.

const vendorModulePath = "go.mongodb.org/mongo-driver"

func isVendorPath(importPath string) bool {
	return importPath == vendorModulePath || strings.HasPrefix(importPath, vendorModulePath+"/")
}

func isShimPath(importPath string) bool {
	return importPath == shimImportPath || strings.HasPrefix(importPath, shimImportPath+"/")
}

// outsideTheBoundaryScope is the part of the module spec 9.3 lets reach the
// vendor: bench/... and the oracle shim with its subpackages.
func outsideTheBoundaryScope(importPath string) bool {
	return importPath == "github.com/hoarddb/hoarddb-server/bench" || strings.HasPrefix(importPath, "github.com/hoarddb/hoarddb-server/bench/") || isShimPath(importPath)
}

// listedPackage is the part of `go list -json` the gates read.
type listedPackage struct {
	ImportPath   string
	Dir          string
	Deps         []string
	TestGoFiles  []string
	XTestGoFiles []string
	TestImports  []string
	XTestImports []string
}

// shippedBinaries are the two packages the project ships.
var shippedBinaries = []string{"./cmd/server", "./cmd/cli"}

func listModulePackages(t *testing.T) []listedPackage {
	t.Helper()
	command := exec.Command("go", "list", "-json", "./...")
	command.Dir = moduleDirectory(t)
	output, err := command.Output()
	if err != nil {
		t.Fatalf("go list -json ./... in %s: %v", command.Dir, err)
	}
	var listed []listedPackage
	for decoder := json.NewDecoder(strings.NewReader(string(output))); decoder.More(); {
		var listedOne listedPackage
		if err := decoder.Decode(&listedOne); err != nil {
			t.Fatalf("decoding go list -json: %v", err)
		}
		listed = append(listed, listedOne)
	}
	if len(listed) < 20 {
		t.Fatalf("go list -json ./... listed %d packages; the gate would pass over a truncated list", len(listed))
	}
	return listed
}

func vendorPathsIn(importPaths []string) []string {
	var found []string
	for _, importPath := range importPaths {
		if isVendorPath(importPath) {
			found = append(found, importPath)
		}
	}
	return found
}

// TestBinariesLinkNoVendor is gate 1, the one that carries the weight: the
// vendor is in neither binary's dependency list, and `go version -m` of the two
// built binaries lists no vendor module. go.mod keeps the vendor in any case,
// for bench/.
func TestBinariesLinkNoVendor(t *testing.T) {
	dependencyList := strings.Fields(goList(t, append([]string{"-deps", "-f", "{{.ImportPath}}"}, shippedBinaries...)...))
	if len(dependencyList) < 100 {
		t.Fatalf("go list -deps listed %d packages; the check would pass over a truncated list", len(dependencyList))
	}
	for _, vendorPath := range vendorPathsIn(dependencyList) {
		t.Errorf("go list -deps %s reaches %s. A binary must link the hoarddb-bson library, not the MongoDB driver (bson-codec-spec 9.3 gate 1)", strings.Join(shippedBinaries, " "), vendorPath)
	}

	binaryDirectory := t.TempDir()
	for _, binary := range []string{"server", "cli"} {
		binaryPath := filepath.Join(binaryDirectory, binary)
		build := exec.Command("go", "build", "-o", binaryPath, "./cmd/"+binary)
		build.Dir = moduleDirectory(t)
		if output, err := build.CombinedOutput(); err != nil {
			t.Fatalf("building ./cmd/%s: %v\n%s", binary, err, output)
		}
		modules, err := exec.Command("go", "version", "-m", binaryPath).Output()
		if err != nil {
			t.Fatalf("go version -m %s: %v", binaryPath, err)
		}
		if !strings.Contains(string(modules), "\tdep\t") {
			t.Fatalf("go version -m %s listed no dependency modules; the check would pass over nothing:\n%s", binary, modules)
		}
		for _, line := range strings.Split(string(modules), "\n") {
			if strings.Contains(line, vendorModulePath) {
				t.Errorf("go version -m of the %s binary lists %s", binary, strings.TrimSpace(line))
			}
		}
	}
}

// TestProductionPackagesReachNoVendor is gate 2: go list -deps without -test,
// so only non-test files count, of every package except bench/... and the shim
// reaches no vendor path.
func TestProductionPackagesReachNoVendor(t *testing.T) {
	checked := 0
	for _, listedOne := range listModulePackages(t) {
		if outsideTheBoundaryScope(listedOne.ImportPath) {
			continue
		}
		checked++
		for _, vendorPath := range vendorPathsIn(listedOne.Deps) {
			t.Errorf("production package %s reaches %s. Import github.com/hoarddb/hoarddb-bson/bson instead (bson-codec-spec 9.3 gate 2)", listedOne.ImportPath, vendorPath)
		}
	}
	if checked < 10 {
		t.Fatalf("only %d packages were checked; the gate would pass over a truncated list", checked)
	}
}

// unaliasedVendorImportsInTestFiles reports, for a package, each vendor import path its
// _test.go files import without a vendor* alias. The alias is what marks an
// import as the 9.1 oracle's, and it is what the swap script skips.
func unaliasedVendorImportsInTestFiles(t *testing.T, listedOne listedPackage) []string {
	t.Helper()
	var unaliased []string
	fileSet := token.NewFileSet()
	for _, testFile := range append(append([]string{}, listedOne.TestGoFiles...), listedOne.XTestGoFiles...) {
		parsed, err := parser.ParseFile(fileSet, filepath.Join(listedOne.Dir, testFile), nil, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("parsing the imports of %s: %v", filepath.Join(listedOne.Dir, testFile), err)
		}
		for _, spec := range parsed.Imports {
			importPath := strings.Trim(spec.Path.Value, `"`)
			if isVendorPath(importPath) && (spec.Name == nil || !strings.HasPrefix(spec.Name.Name, "vendor")) {
				unaliased = append(unaliased, importPath+" in "+testFile)
			}
		}
	}
	return unaliased
}

// TestTestImportsReachVendorOnlyThroughTheOracle is gate 3. A test package of
// the module (bench/... and the shim aside) may reach the vendor in two ways:
// through internal/bsonoracle, or through an import in its own _test.go files
// that carries a vendor* alias. An unaliased vendor import in a test file, or a
// test import of any other package that reaches the vendor, fails.
func TestTestImportsReachVendorOnlyThroughTheOracle(t *testing.T) {
	allPackages := listModulePackages(t)
	depsOf := make(map[string][]string, len(allPackages))
	for _, listedOne := range allPackages {
		depsOf[listedOne.ImportPath] = listedOne.Deps
	}
	checked := 0
	for _, listedOne := range allPackages {
		if outsideTheBoundaryScope(listedOne.ImportPath) {
			continue
		}
		checked++
		for _, unaliased := range unaliasedVendorImportsInTestFiles(t, listedOne) {
			t.Errorf("test package %s imports %s without a vendor* alias. Use hoarddb-bson, or alias it vendorbson and compare through internal/bsonoracle (bson-codec-spec 9.3 gate 3)", listedOne.ImportPath, unaliased)
		}
		for _, testImport := range append(append([]string{}, listedOne.TestImports...), listedOne.XTestImports...) {
			if isShimPath(testImport) || isVendorPath(testImport) {
				continue
			}
			if reached := vendorPathsIn(append([]string{testImport}, depsOf[testImport]...)); len(reached) > 0 {
				t.Errorf("test package %s imports %s, which reaches %s. A test may reach the vendor only through internal/bsonoracle or its own vendor*-aliased import (bson-codec-spec 9.3 gate 3)", listedOne.ImportPath, testImport, reached[0])
			}
		}
	}
	if checked < 10 {
		t.Fatalf("only %d packages were checked; the gate would pass over a truncated list", checked)
	}
}
