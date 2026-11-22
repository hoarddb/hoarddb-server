package server

import (
	"bufio"
	"fmt"
	"go/ast"
	"go/types"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/tools/go/packages"
)

// libraryBSONPath is the package that defines TrustStored.
const libraryBSONPath = "github.com/hoarddb/hoarddb-bson/bson"

// trustStoredAllowlist is the file of the functions that may call
// TrustStored, one "path/to/file.go:Function" per line, the path relative to
// the module root and a method written "(*Type).Method".
const trustStoredAllowlist = "trust_stored_allowlist.txt"

// TestTrustStoredIsCalledOnlyFromStorageReads: TrustStored checks only a
// document's framing, so it is for the bytes this node read back from its own
// store, and nothing else (spec 5.2; build trap 4). Bytes from a client, a
// peer, a restore file, a migration stream or a WAL record go through
// Validate. Every use of TrustStored in non-test code of the module must sit
// in a function the allowlist names, and every function the allowlist names
// must still use it. A dot-import of the library is refused, since it would
// hide a call from a reader. Adding a function to the allowlist is a reviewed
// change.
func TestTrustStoredIsCalledOnlyFromStorageReads(t *testing.T) {
	moduleRoot, err := filepath.Abs("..")
	if err != nil {
		t.Fatalf("finding the module root: %v", err)
	}
	loaded, err := packages.Load(&packages.Config{
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo,
		Dir:  moduleRoot,
	}, "./...")
	if err != nil {
		t.Fatalf("loading the module's packages: %v", err)
	}
	if packages.PrintErrors(loaded) > 0 {
		t.Fatal("the module's packages do not load; see the errors above")
	}
	found := map[string]bool{}
	for _, pkg := range loaded {
		for _, file := range pkg.Syntax {
			fileName, err := filepath.Rel(moduleRoot, pkg.Fset.Position(file.Pos()).Filename)
			if err != nil {
				t.Fatalf("placing %s in the module: %v", pkg.Fset.Position(file.Pos()).Filename, err)
			}
			fileName = filepath.ToSlash(fileName)
			for _, spec := range file.Imports {
				if spec.Name != nil && spec.Name.Name == "." && strings.Trim(spec.Path.Value, `"`) == libraryBSONPath {
					t.Errorf("%s dot-imports %s, which hides its TrustStored calls; import it by name", fileName, libraryBSONPath)
				}
			}
			for _, user := range trustStoredUsers(pkg.TypesInfo, file) {
				found[fileName+":"+user] = true
			}
		}
	}
	allowed := readTrustStoredAllowlist(t)
	for use := range found {
		if !allowed[use] {
			t.Errorf("%s uses bson.TrustStored, and the allowlist (%s) does not name it. TrustStored is for bytes this node "+
				"read back from its own store; bytes from anywhere else go through bson.Validate at the site's limit", use, trustStoredAllowlist)
		}
	}
	for entry := range allowed {
		if !found[entry] {
			t.Errorf("the allowlist (%s) names %s, which no longer uses bson.TrustStored; remove the line", trustStoredAllowlist, entry)
		}
	}
}

// trustStoredUsers names the enclosing function of every use of the library's
// TrustStored in file: a call, or the function taken as a value.
func trustStoredUsers(info *types.Info, file *ast.File) []string {
	var users []string
	for _, declaration := range file.Decls {
		enclosing := "(package level)"
		if function, ok := declaration.(*ast.FuncDecl); ok {
			enclosing = functionName(function)
		}
		ast.Inspect(declaration, func(node ast.Node) bool {
			identifier, ok := node.(*ast.Ident)
			if !ok || identifier.Name != "TrustStored" {
				return true
			}
			if function, ok := info.Uses[identifier].(*types.Func); ok && function.Pkg() != nil && function.Pkg().Path() == libraryBSONPath {
				users = append(users, enclosing)
			}
			return true
		})
	}
	return users
}

// functionName is a declared function's name, a method's as "(*Type).Method"
// or "(Type).Method".
func functionName(function *ast.FuncDecl) string {
	if function.Recv == nil || len(function.Recv.List) == 0 {
		return function.Name.Name
	}
	receiver := function.Recv.List[0].Type
	pointer := ""
	if star, ok := receiver.(*ast.StarExpr); ok {
		pointer, receiver = "*", star.X
	}
	if generic, ok := receiver.(*ast.IndexExpr); ok {
		receiver = generic.X
	}
	if generic, ok := receiver.(*ast.IndexListExpr); ok {
		receiver = generic.X
	}
	return fmt.Sprintf("(%s%s).%s", pointer, types.ExprString(receiver), function.Name.Name)
}

// readTrustStoredAllowlist reads the allowlist: one entry per line, blank lines
// and lines starting with # ignored.
func readTrustStoredAllowlist(t *testing.T) map[string]bool {
	t.Helper()
	file, err := os.Open(trustStoredAllowlist)
	if err != nil {
		t.Fatalf("opening the allowlist: %v", err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			t.Errorf("closing the allowlist: %v", err)
		}
	}()
	allowed := map[string]bool{}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if allowed[line] {
			t.Errorf("the allowlist names %s twice", line)
		}
		allowed[line] = true
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("reading the allowlist: %v", err)
	}
	return allowed
}

// TestTrustStoredAllowlistNamesTheStorageReads pins the allowlist's contents
// to the storage reads step 7a moved onto TrustStored, so that adding or
// dropping a line is a change to this list as well, and so a reviewer sees it.
func TestTrustStoredAllowlistNamesTheStorageReads(t *testing.T) {
	want := map[string]bool{
		"server/scan.go:scanFilteredPage":                       true,
		"server/scan_sorted.go:scanSortedPage":                  true,
		"server/search_plan.go:residualMatches":                 true,
		"server/manage.go:countFilteredEntries":                 true,
		"server/manage.go:distinctValues":                       true,
		"server/manage.go:(*Server).updateMultiple":             true,
		"store/hash.go:(*HashStore).indexDocument":              true,
		"store/hash.go:(*HashStore).removeStored":               true,
		"store/hash.go:(*HashStore).deleteCascading":            true,
		"store/unique.go:(*IndexManager).conflictedIndexError":  true,
		"server/index_rebuild.go:(*Server).writeRebuiltEntries": true,
	}
	got := readTrustStoredAllowlist(t)
	for entry := range want {
		if !got[entry] {
			t.Errorf("the allowlist lacks %s, a storage read of step 7a", entry)
		}
	}
	for entry := range got {
		if !want[entry] {
			t.Errorf("the allowlist names %s, which is not a storage read this test knows; add it here in the same reviewed change", entry)
		}
	}
}
