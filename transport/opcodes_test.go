package transport

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"testing"
)

// TestOpScanIsClassifiedReadOnly pins the classification the replication path
// reads: server/write_path.go replicationRequired routes anything not
// classified read-only onto the replica set, so a scan misclassified as a
// write would be replicated and WAL-logged. The neighbouring read verbs are
// asserted with it so the list stays a list.
func TestOpScanIsClassifiedReadOnly(t *testing.T) {
	for _, op := range []Opcode{OpScan, OpGet, OpSearch, OpCount} {
		if !op.IsReadOnly() {
			t.Fatalf("%s.IsReadOnly() = false; a read must not enter the replication write path", op)
		}
	}
	if name := OpScan.String(); name != "SCAN" {
		t.Fatalf(`OpScan.String() = %q, want "SCAN": a log line must not carry a bare integer`, name)
	}
}

// TestOpcodeStringNamesEveryOpcode: Opcode.String had a case for
// every constant but OpSearch, which logged and metered as "UNKNOWN". The
// constants are read from opcodes.go's source rather than from a list here, so
// an opcode added without a name fails this test instead of passing it.
func TestOpcodeStringNamesEveryOpcode(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "opcodes.go", nil, 0)
	if err != nil {
		t.Fatalf("parsing opcodes.go: %v", err)
	}
	checked := 0
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.CONST {
			continue
		}
		for _, spec := range gen.Specs {
			valueSpec := spec.(*ast.ValueSpec)
			typeName, ok := valueSpec.Type.(*ast.Ident)
			if !ok || typeName.Name != "Opcode" || len(valueSpec.Values) != len(valueSpec.Names) {
				continue
			}
			for i, name := range valueSpec.Names {
				literal, ok := valueSpec.Values[i].(*ast.BasicLit)
				if !ok {
					t.Fatalf("%s is not a literal; this test cannot read its value", name.Name)
				}
				value, err := strconv.ParseUint(literal.Value, 0, 8)
				if err != nil {
					t.Fatalf("%s = %s: %v", name.Name, literal.Value, err)
				}
				checked++
				if got := Opcode(value).String(); got == "UNKNOWN" {
					t.Errorf("%s (0x%02X).String() = %q; every opcode needs a name for logs and metrics", name.Name, value, got)
				}
			}
		}
	}
	if checked == 0 {
		t.Fatal("found no Opcode constants in opcodes.go; the test is reading the wrong shape")
	}
}
