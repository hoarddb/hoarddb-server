package server

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/authrules"
)

// roleWordUsers are the only functions of package server that may name a role: they map an action
// to its privilege name and back, which is the server's authorization table and not a role rule.
var roleWordUsers = map[string]bool{"String": true, "roleSatisfies": true}

// retiredRoleRules are functions whose bodies used to be role rules here; they now live in
// authrules and must not be declared again.
var retiredRoleRules = []string{"roleIncludes", "isAdminRole", "validRole"}

// TestServerKeepsNoRoleTableOfItsOwn checks, from the source, that package server keeps no role
// table and no role rule (ring user store spec 4.2, F10 and B10): no role name written as a
// literal, no list built from the role constants, no rule body (roleIncludes, the admin predicate)
// and no role constant used outside the action table. The cluster's appliers and the server must
// apply one set of rules, and two copies would drift. Break: paste a role list back into
// server/roles.go, or restore isAdminRole's switch; red.
func TestServerKeepsNoRoleTableOfItsOwn(t *testing.T) {
	if len(roleConstants) < 2*len(authrules.RoleNames()) {
		t.Fatalf("read %d role constant names from the authrules source, want %d: the scan is broken", len(roleConstants), 2*len(authrules.RoleNames()))
	}
	words := map[string]bool{}
	for _, name := range authrules.RoleNames() {
		words[name] = true
	}
	for _, retired := range []string{"read", "readWrite", "dbAdmin", "userAdmin", "clusterAdmin", "backup", "restore"} {
		if _, renamed := authrules.RenamedRole(retired); !renamed {
			t.Fatalf("authrules no longer knows the retired role %q; update this test's list", retired)
		}
	}
	paths, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("listing the server sources: %v", err)
	}
	fset := token.NewFileSet()
	checked := 0
	for _, path := range paths {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Fatalf("parsing %s: %v", path, err)
		}
		checked++
		for _, decl := range file.Decls {
			switch decl := decl.(type) {
			case *ast.GenDecl:
				checkRoleDecl(t, fset, decl, words)
			case *ast.FuncDecl:
				for _, retired := range retiredRoleRules {
					if decl.Name.Name == retired {
						t.Errorf("%s declares %s: the rule lives in authrules; call it there", fset.Position(decl.Pos()), retired)
					}
				}
				if !roleWordUsers[decl.Name.Name] {
					checkNoRoleWords(t, fset, decl, words)
				}
			}
		}
	}
	if checked == 0 {
		t.Fatal("found no server source to check")
	}
}

// roleConstants are the names of authrules's role constants (RoleRoot, ...) and of their server
// aliases (roleRoot, ...), read from the authrules source so a new role is covered.
var roleConstants = func() map[string]bool {
	names := map[string]bool{}
	file, err := parser.ParseFile(token.NewFileSet(), filepath.Join("..", "authrules", "authrules.go"), nil, 0)
	if err != nil {
		return names
	}
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.CONST {
			continue
		}
		for _, spec := range gen.Specs {
			for _, name := range spec.(*ast.ValueSpec).Names {
				if strings.HasPrefix(name.Name, "Role") {
					names[name.Name] = true
					names["role"+strings.TrimPrefix(name.Name, "Role")] = true
				}
			}
		}
	}
	return names
}()

// isRoleIdent reports whether expr names a role constant: one of server's role aliases or an
// authrules.Role* constant.
func isRoleIdent(expr ast.Expr) bool {
	switch e := expr.(type) {
	case *ast.Ident:
		return strings.HasPrefix(e.Name, "role") && roleConstants[e.Name]
	case *ast.SelectorExpr:
		pkg, ok := e.X.(*ast.Ident)
		return ok && pkg.Name == "authrules" && roleConstants[e.Sel.Name]
	}
	return false
}

// checkRoleDecl allows a role constant only as an alias of authrules's, and refuses any other
// declaration that writes a role word or lists role constants.
func checkRoleDecl(t *testing.T, fset *token.FileSet, decl *ast.GenDecl, words map[string]bool) {
	t.Helper()
	for _, spec := range decl.Specs {
		value, ok := spec.(*ast.ValueSpec)
		if !ok {
			continue
		}
		for i, name := range value.Names {
			if decl.Tok == token.CONST && isRoleIdent(name) {
				if i >= len(value.Values) {
					t.Errorf("%s: role constant %s has no value", fset.Position(name.Pos()), name.Name)
					continue
				}
				if sel, ok := value.Values[i].(*ast.SelectorExpr); !ok || !isRoleIdent(sel) {
					t.Errorf("%s: role constant %s is not an alias of an authrules role", fset.Position(name.Pos()), name.Name)
				}
			}
		}
		for _, expr := range value.Values {
			if _, alias := expr.(*ast.SelectorExpr); alias && isRoleIdent(expr) {
				continue
			}
			ast.Inspect(expr, func(n ast.Node) bool { return reportRoleWord(t, fset, n, words) })
		}
	}
}

func checkNoRoleWords(t *testing.T, fset *token.FileSet, decl *ast.FuncDecl, words map[string]bool) {
	t.Helper()
	if decl.Body == nil {
		return
	}
	// A field value such as Grant{Role: authrules.RoleRoot, DB: "*"} is data (the root account's
	// seed), not a rule, and may name the authrules constant; a comparison or a case may not.
	data := map[ast.Node]bool{}
	ast.Inspect(decl.Body, func(n ast.Node) bool {
		if kv, ok := n.(*ast.KeyValueExpr); ok {
			if sel, ok := kv.Value.(*ast.SelectorExpr); ok && isRoleIdent(sel) {
				data[sel] = true
			}
		}
		return true
	})
	ast.Inspect(decl.Body, func(n ast.Node) bool {
		if data[n] {
			return false
		}
		if expr, ok := n.(ast.Expr); ok && isRoleIdent(expr) {
			t.Errorf("%s: %s names a role constant; a role rule lives in authrules, call it there", fset.Position(n.Pos()), decl.Name.Name)
			return false
		}
		return reportRoleWord(t, fset, n, words)
	})
}

// reportRoleWord refuses a role name written as a string literal and a composite literal that
// lists role constants.
func reportRoleWord(t *testing.T, fset *token.FileSet, n ast.Node, words map[string]bool) bool {
	t.Helper()
	switch n := n.(type) {
	case *ast.BasicLit:
		if n.Kind == token.STRING {
			if text, err := strconv.Unquote(n.Value); err == nil && words[text] {
				t.Errorf("%s: the role name %q is written as a literal; use the authrules constant", fset.Position(n.Pos()), text)
			}
		}
	case *ast.CompositeLit:
		roles := 0
		for _, elt := range n.Elts {
			if kv, ok := elt.(*ast.KeyValueExpr); ok {
				elt = kv.Value
			}
			if isRoleIdent(elt) {
				roles++
			}
		}
		if roles >= 2 {
			t.Errorf("%s: a list of %d role constants; the role table lives in authrules", fset.Position(n.Pos()), roles)
			return false
		}
	}
	return true
}
