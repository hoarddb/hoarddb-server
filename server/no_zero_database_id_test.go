package server

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// zeroDatabaseIDAllowlist names the non-test composite literals that may omit
// DatabaseID: none today. An entry is "file.go:StructName" and needs a comment
// saying why zero is right there.
var zeroDatabaseIDAllowlist = map[string]string{}

// TestNoZeroDatabaseIDOutsideTests closes the hole the compiler leaves open: a
// keyed struct literal that omits DatabaseID compiles and then reads and
// writes keys under database id 0. It fails on a non-test literal of
// ApplyRequest, reclaimTarget or any struct in the module's server and store
// sources that declares a DatabaseID/databaseID field, when the literal omits
// the field or sets it to 0.
func TestNoZeroDatabaseIDOutsideTests(t *testing.T) {
	roots := []string{".", filepath.Join("..", "store")}
	fset := token.NewFileSet()
	var files []*ast.File
	var names []string
	for _, root := range roots {
		entries, err := os.ReadDir(root)
		if err != nil {
			t.Fatalf("reading %s: %v", root, err)
		}
		for _, entry := range entries {
			name := entry.Name()
			if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
				continue
			}
			file, err := parser.ParseFile(fset, filepath.Join(root, name), nil, 0)
			if err != nil {
				t.Fatalf("parsing %s: %v", name, err)
			}
			files = append(files, file)
			names = append(names, name)
		}
	}
	carriers := map[string]bool{"ApplyRequest": true, "reclaimTarget": true}
	for _, file := range files {
		ast.Inspect(file, func(node ast.Node) bool {
			spec, ok := node.(*ast.TypeSpec)
			if !ok {
				return true
			}
			if structType, ok := spec.Type.(*ast.StructType); ok {
				for _, field := range structType.Fields.List {
					for _, name := range field.Names {
						if name.Name == "DatabaseID" || name.Name == "databaseID" {
							carriers[spec.Name.Name] = true
						}
					}
				}
			}
			return true
		})
	}
	for index, file := range files {
		ast.Inspect(file, func(node ast.Node) bool {
			lit, ok := node.(*ast.CompositeLit)
			if !ok {
				return true
			}
			var typeName string
			switch typed := lit.Type.(type) {
			case *ast.Ident:
				typeName = typed.Name
			case *ast.SelectorExpr:
				typeName = typed.Sel.Name
			}
			if !carriers[typeName] || len(lit.Elts) == 0 {
				return true
			}
			keyed, found, zero := false, false, false
			for _, elt := range lit.Elts {
				pair, ok := elt.(*ast.KeyValueExpr)
				if !ok {
					continue
				}
				keyed = true
				if key, ok := pair.Key.(*ast.Ident); ok && (key.Name == "DatabaseID" || key.Name == "databaseID") {
					found = true
					if basic, ok := pair.Value.(*ast.BasicLit); ok && basic.Value == "0" {
						zero = true
					}
				}
			}
			if !keyed || (found && !zero) {
				return true
			}
			if _, allowed := zeroDatabaseIDAllowlist[names[index]+":"+typeName]; allowed {
				return true
			}
			t.Errorf("%s: %s literal omits DatabaseID or sets it to 0; keys would be built under database id 0", fset.Position(lit.Pos()), typeName)
			return true
		})
	}
}
