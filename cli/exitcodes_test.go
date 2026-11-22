package cli

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// TestExitTableHasOneRowPerNumber checks the table's own integrity: nine codes,
// 0 to 8, each defined once and in order, every named class unique, and every
// exported constant equal to the row that carries its meaning.
func TestExitTableHasOneRowPerNumber(t *testing.T) {
	wantCodes := []int{ExitOK, ExitUsage, ExitRefused, ExitPartial, ExitInterrupted, ExitAccountsAwaiting, ExitConnect, ExitAuth, ExitUnavailable}
	if len(exitTable) != len(wantCodes) {
		t.Fatalf("exitTable has %d rows, want %d (one per code 0 to 8)", len(exitTable), len(wantCodes))
	}
	seenClass := map[string]int{}
	for i, row := range exitTable {
		if row.Code != wantCodes[i] || row.Code != i {
			t.Errorf("row %d is code %d, want %d: the table is 0 to 8 in order", i, row.Code, wantCodes[i])
		}
		if strings.TrimSpace(row.Meaning) == "" {
			t.Errorf("code %d has no meaning", row.Code)
		}
		if row.Class == "" {
			continue
		}
		if other, dup := seenClass[row.Class]; dup {
			t.Errorf("class %q names both %d and %d", row.Class, other, row.Code)
		}
		seenClass[row.Class] = row.Code
	}

	wantClasses := map[int]string{
		ExitUsage: "usage", ExitRefused: "refused", ExitPartial: "partial", ExitInterrupted: "interrupted",
		ExitConnect: "connect", ExitAuth: "auth", ExitUnavailable: "unavailable",
	}
	for code, want := range wantClasses {
		if got, defined := ExitClass(code); !defined || got != want {
			t.Errorf("ExitClass(%d) = %q, %v; want %q, true", code, got, defined, want)
		}
	}
	for _, code := range []int{ExitOK, ExitAccountsAwaiting} {
		if got, defined := ExitClass(code); !defined || got != "" {
			t.Errorf("ExitClass(%d) = %q, %v; want an empty class that is still defined", code, got, defined)
		}
	}
	for _, undefined := range []int{-1, 9, 255} {
		if _, defined := ExitClass(undefined); defined {
			t.Errorf("ExitClass(%d) reports a code the table does not define", undefined)
		}
	}
}

// verbReturnSets is the subset of the exit table each entry point returns
// (build doc Part 2 step 4). Parts 8b and 10 add the check that a run of the
// entry point really returns only numbers from its subset; this test pins the
// subsets themselves, and that they come from the one table.
var verbReturnSets = map[string][]int{
	"dump":     {ExitOK, ExitUsage, ExitRefused, ExitPartial, ExitInterrupted, ExitConnect, ExitAuth, ExitUnavailable},
	"restore":  {ExitOK, ExitUsage, ExitRefused, ExitPartial, ExitInterrupted, ExitAccountsAwaiting, ExitConnect, ExitAuth, ExitUnavailable},
	"ingest":   {ExitOK, ExitUsage, ExitRefused, ExitPartial, ExitInterrupted, ExitConnect, ExitAuth, ExitUnavailable},
	"one-shot": {ExitOK, ExitUsage, ExitRefused, ExitInterrupted, ExitConnect, ExitAuth, ExitUnavailable},
}

// TestOneShotAndVerbsShareOneExitTable (T26, part 1) checks that every entry
// point's return set is drawn from the one table, and that no file in this
// package still declares a private exit constant beside it.
func TestOneShotAndVerbsShareOneExitTable(t *testing.T) {
	for entryPoint, codes := range verbReturnSets {
		seen := map[int]bool{}
		for _, code := range codes {
			if _, defined := ExitClass(code); !defined {
				t.Errorf("%s returns %d, which the exit table does not define", entryPoint, code)
			}
			if seen[code] {
				t.Errorf("%s lists %d twice", entryPoint, code)
			}
			seen[code] = true
		}
		if entryPoint == "one-shot" {
			for _, never := range []int{ExitPartial, ExitAccountsAwaiting} {
				if seen[never] {
					t.Errorf("a one-shot run returns %d, which spec §6.2 reserves for the verbs", never)
				}
			}
		}
		if entryPoint != "restore" && seen[ExitAccountsAwaiting] {
			t.Errorf("%s returns %d, which only restore may", entryPoint, ExitAccountsAwaiting)
		}
	}

	privateExitNames := privateExitConstants(t)
	if len(privateExitNames) > 0 {
		t.Errorf("exit numbers are defined in exitcodes.go only, but these package-level names look like private exit constants: %v", privateExitNames)
	}
}

// privateExitConstants parses the package's non-test files and returns
// package-level constants or variables that are initialised from an integer
// literal, whose name contains "exit" (any case), and which are not in
// exitcodes.go. Sentinel errors such as ErrExit are not numbers and are ignored. It is how "one file, one set of names" is
// enforced instead of promised.
func privateExitConstants(t *testing.T) []string {
	t.Helper()
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("listing the package's files: %v", err)
	}
	var offenders []string
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") || file == "exitcodes.go" {
			continue
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, 0)
		if err != nil {
			t.Fatalf("parsing %s: %v", file, err)
		}
		for _, decl := range parsed.Decls {
			general, ok := decl.(*ast.GenDecl)
			if !ok || (general.Tok != token.CONST && general.Tok != token.VAR) {
				continue
			}
			for _, spec := range general.Specs {
				valueSpec := spec.(*ast.ValueSpec)
				if !hasIntegerLiteral(valueSpec.Values) {
					continue
				}
				for _, name := range valueSpec.Names {
					if strings.Contains(strings.ToLower(name.Name), "exit") {
						offenders = append(offenders, file+":"+name.Name)
					}
				}
			}
		}
	}
	sort.Strings(offenders)
	return offenders
}

func hasIntegerLiteral(values []ast.Expr) bool {
	for _, value := range values {
		if literal, ok := value.(*ast.BasicLit); ok && literal.Kind == token.INT {
			return true
		}
	}
	return false
}
