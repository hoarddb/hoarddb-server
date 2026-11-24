package cli

import (
	"github.com/hoarddb/hoarddb-bson/bson"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// connectCallSites are the only functions that may build a CLI or run the
// two-phase dial. GAP-24 Part 8a collapsed four copies of the connect sequence
// (the REPL, dump, restore, ingest) into connectSession; a fifth copy is what
// this list refuses. The REPL entry builds its own CLI because it adds the
// session-only options (theme, debug, password source) to the shared ones.
var connectCallSites = map[string][]string{
	// ringdial.go:dialRing builds a CLI only to reuse its AUTH and TLS options for the
	// ringclient connection; ringdial.go:dialRingVerb does the same for the verbs (dialRingForVerb,
	// dialRingForVerbUser and dialRingForMembers wrap it; dump and restore need the dial options to
	// reach every member);
	// U54 deletes the legacy engine and with it this entry.
	"NewCLI":              {"connect.go:connectSession", "run.go:runSession", "ringdial.go:dialRing", "ringdial.go:dialRingVerb"},
	"dialAndAuthenticate": {"connect.go:connectSession", "cli.go:Connect"},
}

// TestConnectPathIsShared is T27. It scans the package's non-test sources for
// calls to NewCLI and dialAndAuthenticate and fails on any call outside
// connectCallSites; then it runs each entry point against a dead listener and
// a wrong password and checks they report the same failure.
func TestConnectPathIsShared(t *testing.T) {
	t.Run("only the shared path builds and dials", func(t *testing.T) {
		files, err := filepath.Glob("*.go")
		if err != nil {
			t.Fatal(err)
		}
		for _, file := range files {
			if strings.HasSuffix(file, "_test.go") {
				continue
			}
			parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, 0)
			if err != nil {
				t.Fatalf("parsing %s: %v", file, err)
			}
			for _, declaration := range parsed.Decls {
				function, isFunction := declaration.(*ast.FuncDecl)
				if !isFunction || function.Body == nil {
					continue
				}
				site := file + ":" + function.Name.Name
				ast.Inspect(function.Body, func(node ast.Node) bool {
					call, isCall := node.(*ast.CallExpr)
					if !isCall {
						return true
					}
					name := calledName(call)
					if allowed, guarded := connectCallSites[name]; guarded && !slices.Contains(allowed, site) {
						t.Errorf("%s is called from %s; connect through connectSession in connect.go (allowed: %v)", name, site, allowed)
					}
					return true
				})
			}
		}
	})

	t.Run("every entry point reports a dead listener and a wrong password alike", func(t *testing.T) {
		session := newGoldenSession(t)
		deadAddress := closedTCPAddr(t)
		dumpDir := t.TempDir()
		input := writeBSONFile(t, "users.bson", bson.D{{Key: "_id", Value: "a"}})

		entries := []struct {
			name string
			run  func(connection []string) (code int, stderr string)
			// wrongPassword is the text a rejected login prints; the ring client words it
			// as its own refusal.
			wrongPassword string
		}{
			{"repl", func(connection []string) (int, string) {
				code, _, stderr := runMainCapturing(t, connection...)
				return code, stderr
			}, `rejected the login of user "admin"`},
			{"dump", func(connection []string) (int, string) {
				run := runProcessVerb(t, "dump", RunDumpCommand, connection, []string{"--out", dumpDir}, []string{"app"})
				return run.code, run.stderr
			}, ""},
			{"restore", func(connection []string) (int, string) {
				run := runProcessVerb(t, "restore", RunRestoreCommand, connection, []string{"--dir", dumpDir}, nil)
				return run.code, run.stderr
			}, ""},
			{"ingest", func(connection []string) (int, string) {
				code, _, stderr := runRingIngestForTest(t, append([]string{input, "--into", "app.x"}, connection...)...)
				return code, stderr
			}, `rejected the login of user "admin"`},
		}
		for _, entry := range entries {
			t.Run(entry.name, func(t *testing.T) {
				_, deadStderr := entry.run(closedAddressFlags(deadAddress))
				if want := "connect to " + deadAddress + ": "; !strings.Contains(deadStderr, want) {
					t.Errorf("a dead listener did not print %q:\n%s", want, deadStderr)
				}
				_, wrongStderr := entry.run(wrongPasswordFlags(session))
				want := "authentication failed (CREDENTIALS)"
				if entry.wrongPassword != "" {
					want = entry.wrongPassword
				}
				if !strings.Contains(wrongStderr, want) {
					t.Errorf("a wrong password did not print %q:\n%s", want, wrongStderr)
				}
			})
		}
	})
}

// calledName is the identifier a call expression names, whether it is called
// as f(...) or x.f(...).
func calledName(call *ast.CallExpr) string {
	switch function := call.Fun.(type) {
	case *ast.Ident:
		return function.Name
	case *ast.SelectorExpr:
		return function.Sel.Name
	}
	return ""
}
