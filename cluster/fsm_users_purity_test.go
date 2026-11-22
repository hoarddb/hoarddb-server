package cluster

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// authrulesImports is every package hoardDB/authrules may import: standard packages that read no
// clock, file, randomness or configuration.
var authrulesImports = map[string]bool{
	"cmp": true, "fmt": true, "slices": true, "strings": true, "strconv": true, "sort": true,
	"unicode": true, "unicode/utf8": true, "errors": true, "math": true, "bytes": true,
}

// userApplierImports is every package the user appliers and the table codec may import. No clock,
// no file, no randomness, no configuration, no keyring and no hash: a pepper never reaches them.
var userApplierImports = map[string]bool{
	"bytes": true, "cmp": true, "encoding/binary": true, "encoding/hex": true, "fmt": true, "math": true,
	"slices": true, "strconv": true, "unicode": true,
	"github.com/hoarddb/hoarddb-bson/bson":             true,
	"github.com/hoarddb/hoarddb-bson/bson/bsontype":    true,
	"github.com/hoarddb/hoarddb-bson/x/bsonx/bsoncore": true,
	"github.com/hoarddb/hoarddb-server/authrules":                                true,
	"github.com/hoarddb/hoarddb-server/namepolicy":                               true,
}

// forbiddenCalls are package selectors an applier must never call, whatever it imports.
var forbiddenCalls = []string{"time.", "os.", "rand.", "config.", "hmac.", "sha256.", "hkdf."}

func importsOf(t *testing.T, path string) []string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
	if err != nil {
		t.Fatalf("parsing %s: %v", path, err)
	}
	var paths []string
	for _, spec := range file.Imports {
		p, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			t.Fatalf("%s: import %s: %v", path, spec.Path.Value, err)
		}
		paths = append(paths, p)
	}
	return paths
}

// TestUserApplyAndAuthrulesReadNoClockNoConfigNoKeyringAndNoPepper (B10): the user appliers are a
// pure function of the state, the command and authrules, so the leader's dry run and every
// follower's apply agree. An import check of hoardDB/authrules (only an allowed list of standard
// packages), the same of the applier files, and an AST scan of the applier files for a clock,
// file, randomness, configuration or hash call. Break: call time.Now() from an authrules function
// the applier uses; red.
func TestUserApplyAndAuthrulesReadNoClockNoConfigNoKeyringAndNoPepper(t *testing.T) {
	sources, err := filepath.Glob(filepath.Join("..", "authrules", "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	checked := 0
	for _, path := range sources {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		checked++
		for _, imported := range importsOf(t, path) {
			if !authrulesImports[imported] {
				t.Errorf("%s imports %q: authrules may import only standard packages that read no clock, file, randomness or configuration", path, imported)
			}
		}
	}
	if checked == 0 {
		t.Fatal("found no authrules source")
	}
	// namepolicy is the one other hoardDB package the appliers call; filepath.IsAbs reads only the
	// build's path separator, a constant of the build.
	namepolicyImports := map[string]bool{"errors": true, "fmt": true, "path/filepath": true, "strings": true}
	for _, imported := range importsOf(t, filepath.Join("..", "namepolicy", "namepolicy.go")) {
		if !namepolicyImports[imported] {
			t.Errorf("namepolicy imports %q, which the user appliers must not reach", imported)
		}
	}
	for _, path := range []string{"fsm_users.go", "users.go"} {
		for _, imported := range importsOf(t, path) {
			if !userApplierImports[imported] {
				t.Errorf("%s imports %q, which the user appliers must not read", path, imported)
			}
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(file, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			pkg, ok := sel.X.(*ast.Ident)
			if !ok {
				return true
			}
			for _, forbidden := range forbiddenCalls {
				if pkg.Name+"." == forbidden {
					t.Errorf("%s calls %s.%s, which an applier must not", path, pkg.Name, sel.Sel.Name)
				}
			}
			return true
		})
	}
}
