package storage

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// replicationKind is one record kind of the NSReplication namespace: the
// first byte of the key after the namespace, and the constant that names it.
type replicationKind struct {
	name string
	kind rune
	file string
}

// replicationKindsIn returns every constant of the Go files in dir (tests
// excluded) whose name ends in RecordKind and whose value is a rune literal:
// how every NSReplication kind is named, here and in the server package.
func replicationKindsIn(t *testing.T, dir string) []replicationKind {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(dir, "*.go"))
	if err != nil {
		t.Fatalf("listing %s: %v", dir, err)
	}
	var kinds []replicationKind
	fset := token.NewFileSet()
	for _, path := range paths {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Fatalf("parsing %s: %v", path, err)
		}
		ast.Inspect(file, func(node ast.Node) bool {
			spec, ok := node.(*ast.ValueSpec)
			if !ok {
				return true
			}
			for i, name := range spec.Names {
				if !strings.HasSuffix(name.Name, "RecordKind") || i >= len(spec.Values) {
					continue
				}
				literal, ok := spec.Values[i].(*ast.BasicLit)
				if !ok || literal.Kind != token.CHAR {
					continue
				}
				kind, _, _, err := strconv.UnquoteChar(strings.Trim(literal.Value, "'"), '\'')
				if err != nil {
					t.Fatalf("reading the kind of %s in %s: %v", name.Name, path, err)
				}
				kinds = append(kinds, replicationKind{name: name.Name, kind: kind, file: filepath.Base(path)})
			}
			return true
		})
	}
	return kinds
}

// nsReplicationComment is the doc comment of the NSReplication constant in
// keys.go: the one list of the namespace's kinds a reader is sent to.
func nsReplicationComment(t *testing.T) string {
	t.Helper()
	source, err := os.ReadFile("keys.go")
	if err != nil {
		t.Fatalf("reading keys.go: %v", err)
	}
	file, err := parser.ParseFile(token.NewFileSet(), "keys.go", source, parser.ParseComments)
	if err != nil {
		t.Fatalf("parsing keys.go: %v", err)
	}
	var comment string
	ast.Inspect(file, func(node ast.Node) bool {
		spec, ok := node.(*ast.ValueSpec)
		if !ok {
			return true
		}
		for _, name := range spec.Names {
			if name.Name == "NSReplication" && spec.Doc != nil {
				comment = spec.Doc.Text()
			}
		}
		return true
	})
	if comment == "" {
		t.Fatal("NSReplication has no doc comment in keys.go")
	}
	return comment
}

// TestEveryReplicationRecordKindIsUniqueAndListed is T-170
// (bucket-replication-rev8-spec §14.2): a freeze. Every record of the node's
// replication namespace is told apart by one byte, so two kinds that share a
// byte read each other's records with no error anywhere, and a kind left out
// of the NSReplication comment is the next one somebody takes twice. It fails
// when a kind is added twice or left out of the comment.
func TestEveryReplicationRecordKindIsUniqueAndListed(t *testing.T) {
	kinds := append(replicationKindsIn(t, "."), replicationKindsIn(t, "../server")...)
	// p f l m h x g d v s on main when this test was written: fewer means the
	// parser or the naming changed and the test checks nothing.
	if len(kinds) < 10 {
		t.Fatalf("found only %d record kinds named *RecordKind; the naming or the parser changed: %v", len(kinds), kinds)
	}
	comment := nsReplicationComment(t)
	byKind := make(map[rune]replicationKind, len(kinds))
	for _, kind := range kinds {
		if taken, twice := byKind[kind.kind]; twice {
			t.Errorf("record kind %q is taken twice: %s (%s) and %s (%s)", kind.kind, taken.name, taken.file, kind.name, kind.file)
		}
		byKind[kind.kind] = kind
		if !strings.Contains(comment, "'"+string(kind.kind)+"'") {
			t.Errorf("record kind %q (%s, %s) is not listed in NSReplication's comment in keys.go", kind.kind, kind.name, kind.file)
		}
	}
}
