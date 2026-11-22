package storage_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
)

// TestNoOldKeyAPIRemains walks the whole module and fails on any use of the
// storage key API that entry-ring unit U1 replaced: BucketPrefix, the
// three-argument Key(slot, id, ns) and the two-argument SlotOf(database,
// bucket). The compiler catches most of these; the walk also covers files a
// build tag hides (bsoncorpus) and keeps the old names from coming back.
func TestNoOldKeyAPIRemains(t *testing.T) {
	fset := token.NewFileSet()
	walkErr := filepath.WalkDir("..", func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			switch entry.Name() {
			case "vendor", "testdata", ".git":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "no_old_key_api_test.go") {
			return nil
		}
		file, parseErr := parser.ParseFile(fset, path, nil, 0)
		if parseErr != nil {
			t.Errorf("parse %s: %v", path, parseErr)
			return nil
		}
		ast.Inspect(file, func(node ast.Node) bool {
			switch typed := node.(type) {
			case *ast.Ident:
				if typed.Name == "BucketPrefix" {
					t.Errorf("%s: BucketPrefix was deleted; use storage.Scope or storage.PairPrefix", fset.Position(typed.Pos()))
				}
			case *ast.CallExpr:
				name := calleeName(typed.Fun)
				if name == "Key" && len(typed.Args) < 4 && typed.Ellipsis == token.NoPos {
					t.Errorf("%s: storage.Key takes (slot, databaseID, bucketID, ns, rest...)", fset.Position(typed.Pos()))
				}
				if name == "SlotOf" && len(typed.Args) != 1 {
					t.Errorf("%s: storage.SlotOf takes one placement key", fset.Position(typed.Pos()))
				}
			}
			return true
		})
		return nil
	})
	if walkErr != nil {
		t.Fatalf("walk module: %v", walkErr)
	}
}

// calleeName returns the called function's bare name for storage.Key,
// storage.SlotOf and the unqualified forms used inside package storage; any
// other selector (a method named Key on some type) returns "".
func calleeName(fun ast.Expr) string {
	switch typed := fun.(type) {
	case *ast.Ident:
		return typed.Name
	case *ast.SelectorExpr:
		if pkg, ok := typed.X.(*ast.Ident); ok && pkg.Name == "storage" {
			return typed.Sel.Name
		}
	}
	return ""
}
