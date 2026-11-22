package cli

import (
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// untypedAllowed is the only way past the static half of
// TestEveryHandlerErrorIsTyped: a function or package variable, by name, that
// may build a plain error, with the one-line reason. The goal is an empty map.
// Every entry is a review flag: it is a path a script cannot classify.
var untypedAllowed = map[string]string{
	"ErrNotConnected": "a sentinel, matched by errors.Is in classify (connect, exit 6)",
	"ErrExit":         "control flow for the exit statement; CLI.handle and RunMain consume it and it never reaches classify",
}

// handlerErrorFiles are the files whose functions turn a statement into an
// error a script will see.
var handlerErrorFiles = []string{
	"handlers.go", "declare.go", "user_commands.go", "documents.go",
	"date_functions.go", "store_types.go", "unsupported_features.go",
}

// TestEveryHandlerErrorIsTyped (T24). The static half fails on any function in
// the handler files that returns a plain fmt.Errorf or errors.New, or a package
// variable initialised with one, unless it wraps an error with %w or is in
// untypedAllowed. The runtime half provokes the error paths of the build doc's
// Appendix B, and then every statement of every REPL golden, against a real
// server, and fails on any that reaches classify's UNCLASSIFIED fallback.
func TestEveryHandlerErrorIsTyped(t *testing.T) {
	t.Run("static: no function returns an untyped error", staticUntypedErrorScan)
	t.Run("runtime: Appendix B paths classify as documented", runtimeAppendixB)
	t.Run("runtime: no golden statement reaches UNCLASSIFIED", runtimeNothingUnclassified)
}

// plainErrorCall reports whether expression is a call to fmt.Errorf or
// errors.New that does not wrap another error with %w.
func plainErrorCall(expression ast.Expr) (bool, string) {
	call, isCall := expression.(*ast.CallExpr)
	if !isCall {
		return false, ""
	}
	selector, isSelector := call.Fun.(*ast.SelectorExpr)
	if !isSelector {
		return false, ""
	}
	pkg, isIdent := selector.X.(*ast.Ident)
	if !isIdent {
		return false, ""
	}
	name := pkg.Name + "." + selector.Sel.Name
	if name != "fmt.Errorf" && name != "errors.New" {
		return false, ""
	}
	if name == "fmt.Errorf" && len(call.Args) > 0 {
		if format, isLiteral := call.Args[0].(*ast.BasicLit); isLiteral && strings.Contains(format.Value, "%w") {
			return false, ""
		}
		// A format built by concatenation still carries %w in one of its parts.
		if binary, isBinary := call.Args[0].(*ast.BinaryExpr); isBinary && strings.Contains(exprText(binary), "%w") {
			return false, ""
		}
	}
	return true, name
}

// exprText concatenates the string literals inside a + expression.
func exprText(expression ast.Expr) string {
	switch node := expression.(type) {
	case *ast.BasicLit:
		return node.Value
	case *ast.BinaryExpr:
		return exprText(node.X) + exprText(node.Y)
	}
	return ""
}

func staticUntypedErrorScan(t *testing.T) {
	fileSet := token.NewFileSet()
	var offenders []string
	for _, name := range handlerErrorFiles {
		file, err := parser.ParseFile(fileSet, name, nil, 0)
		if err != nil {
			t.Fatalf("parsing %s: %v", name, err)
		}
		for _, declaration := range file.Decls {
			switch node := declaration.(type) {
			case *ast.FuncDecl:
				if node.Body == nil {
					continue
				}
				if _, allowed := untypedAllowed[node.Name.Name]; allowed {
					continue
				}
				ast.Inspect(node.Body, func(inner ast.Node) bool {
					returnStmt, isReturn := inner.(*ast.ReturnStmt)
					if !isReturn {
						return true
					}
					for _, result := range returnStmt.Results {
						if plain, call := plainErrorCall(result); plain {
							offenders = append(offenders, fileSet.Position(result.Pos()).String()+": "+node.Name.Name+" returns "+call+" with no %w")
						}
					}
					return true
				})
			case *ast.GenDecl:
				for _, spec := range node.Specs {
					valueSpec, isValue := spec.(*ast.ValueSpec)
					if !isValue {
						continue
					}
					for index, value := range valueSpec.Values {
						if plain, call := plainErrorCall(value); plain {
							variable := valueSpec.Names[index].Name
							if _, allowed := untypedAllowed[variable]; !allowed {
								offenders = append(offenders, fileSet.Position(value.Pos()).String()+": var "+variable+" is "+call+" with no %w")
							}
						}
					}
				}
			}
		}
	}
	sort.Strings(offenders)
	if len(offenders) > 0 {
		t.Errorf("%d untyped error sites (type each as *UsageError, *RefusedError or *NotFoundError, or add the function to untypedAllowed with a reason):\n%s",
			len(offenders), strings.Join(offenders, "\n"))
	}
	for name, reason := range untypedAllowed {
		if strings.TrimSpace(reason) == "" {
			t.Errorf("untypedAllowed[%q] has no reason", name)
		}
	}
}

// errorPathRow is one handler error path: a statement that provokes it, the
// session it runs on, and what classify must say.
type errorPathRow struct {
	name      string
	statement string
	// session prepares the state the statement needs and returns the CLI to
	// run it on; nil means the fixture session's own CLI.
	session func(t *testing.T, fixture *goldenSession) *CLI
	class   string
	code    string
}

// readOnlyUserSession returns a connection authenticated as a user with the
// read role only, who is refused every cluster verb.
func readOnlyUserSession(t *testing.T, fixture *goldenSession) *CLI {
	t.Helper()
	runQuiet(t, fixture, "create user bob roles [reader@app]")
	bob := NewCLI(WithAddress(fixture.addr), WithInsecureTLS(true), WithCredentials("bob", goldenUserPassword), WithTerminal(io.Discard))
	if err := bob.Connect(); err != nil {
		t.Fatalf("connecting as bob: %v", err)
	}
	t.Cleanup(bob.Close)
	return bob
}

// userBobExistsSession returns the fixture CLI after user bob was created.
func userBobExistsSession(t *testing.T, fixture *goldenSession) *CLI {
	t.Helper()
	runQuiet(t, fixture, "create user bob roles [reader@app]")
	return fixture.cli
}

// appDroppedSession returns the fixture CLI after another connection dropped
// the database it is using.
func appDroppedSession(t *testing.T, fixture *goldenSession) *CLI {
	t.Helper()
	dropAppFromAnotherConnection(t, fixture)
	return fixture.cli
}

// appendixBRows are the rows of build doc Appendix B, then the paths the
// sweep found that the appendix does not list.
var appendixBRows = []errorPathRow{
	{name: "missing bucket, database present", statement: `db.Nope.count()`, class: "refused", code: "NOT_FOUND"},
	{name: "missing bucket via the handler's own round trip", statement: `db.Nope.get("k")`, class: "refused", code: "NOT_FOUND"},
	{name: "missing database", statement: `use nope`, class: "refused", code: "NOT_FOUND"},
	{name: "missing database on a read", statement: `db.Users.count()`, session: appDroppedSession, class: "refused", code: "NOT_FOUND"},
	{name: "get on a queue", statement: `db.Jobs.get("k")`, class: "usage", code: "USAGE"},
	{name: "get on a heap", statement: `db.Tasks.get("k")`, class: "usage", code: "USAGE"},
	{name: "peek(N): a positional count belongs in {limit: N}", statement: `db.Jobs.peek(2)`, class: "usage", code: "USAGE"},
	{name: "wrong argument count", statement: `db.Users.update("u1")`, class: "usage", code: "USAGE"},
	{name: "parse error", statement: `db.Users.`, class: "usage", code: "PARSE_ERROR"},
	// Appendix B names an insert into a fifo bucket, which the server accepts.
	// A create of an existing database was the one in-band refusal a single
	// node produced; it is now an error frame with its own code.
	{name: "create database on an existing name", statement: `create database app`, class: "refused", code: "ALREADY_EXISTS"},
	{name: "duplicate on a unique index", statement: `db.Emails.insert({"_id_": "e2", "Email": "a@example.com"})`, class: "refused", code: "DUPLICATE_KEY"},
	{name: "unknown verb", statement: `count Users`, class: "usage", code: "PARSE_ERROR"},
	{name: "field store, missing parent", statement: `db.Post["nope"].Comments.peek()`, class: "refused", code: "NOT_FOUND"},
	{name: "field store, wrong verb", statement: `db.Post["p1"].Comments.put("k", {})`, class: "refused", code: "INVALID_ARGUMENT"},
	{name: "alter bucket unique", statement: `alter bucket Post { Email string unique }`, class: "usage", code: "USAGE"},
	{name: "non-admin on a cluster verb", statement: `remove-node n2`, session: readOnlyUserSession, class: "refused", code: "PERMISSION_DENIED"},

	// Paths the sweep found that Appendix B does not list.
	{name: "user that does not exist", statement: `user describe nobody`, class: "refused", code: "REFUSED"}, // USER_GET answers found:false (BUG-0622), so the CLI refuses with its own sentence
	{name: "a document that is not JSON", statement: `db.Users.insert({not json})`, class: "usage", code: "USAGE"},
	{name: "use with no database", statement: `use`, class: "usage", code: "PARSE_ERROR"},
	{name: "describe database that is absent", statement: `describe database nope`, class: "refused", code: "NOT_FOUND"},
	{name: "describe bucket that is absent", statement: `describe bucket Nope`, class: "refused", code: "NOT_FOUND"},
	{name: "unknown write concern", statement: `write-concern bogus`, class: "usage", code: "USAGE"},
	{name: "roles that are not a list", statement: `alter user bob roles reader@app`, session: userBobExistsSession, class: "usage", code: "USAGE"},
	{name: "create bucket with an unknown type", statement: `create bucket X { type: nope }`, class: "usage", code: "USAGE"},
	{name: "update with an operator that is not supported", statement: `db.Users.update("u1", {$pull: {"a": "b"}})`, class: "usage", code: "USAGE"},
	{name: "stream", statement: `db.Files.stream()`, class: "usage", code: "USAGE"},
}

func runtimeAppendixB(t *testing.T) {
	for _, row := range appendixBRows {
		t.Run(row.name, func(t *testing.T) {
			fixture := newFixtureSession(t)
			runner := fixture.cli
			if row.session != nil {
				runner = row.session(t, fixture)
			}
			err := runner.handle(row.statement)
			if err == nil {
				t.Fatalf("%s returned nil, want an error", row.statement)
			}
			info := classify(err)
			if info.Class != row.class || info.Code != row.code {
				t.Errorf("%s: classify(%v) = %s/%s, want %s/%s", row.statement, err, info.Class, info.Code, row.class, row.code)
			}
		})
	}
}

// deferredToALaterPart names the part of the build that types the errors of
// statements the handler files do not build. The connect statement's errors are
// CLI.Connect's dial and TLS failures, which become *ConnectError in Part 8a;
// dump and restore statements return the verbs' errors from dump.go, typed with
// the verbs in Part 8b. Each entry here is an error a script cannot classify
// yet, and the report lists them.
func deferredToALaterPart(statement string) (string, bool) {
	switch {
	case strings.HasPrefix(statement, "connect"):
		return "Part 8a", true
	case strings.HasPrefix(statement, "dump"), strings.HasPrefix(statement, "restore"):
		return "Part 8b", true
	}
	return "", false
}

func runtimeNothingUnclassified(t *testing.T) {
	for _, row := range goldenRows {
		t.Run(row.slug, func(t *testing.T) {
			session := newFixtureSession(t)
			expand := func(lines []string) []string {
				expanded := make([]string, len(lines))
				for i, line := range lines {
					expanded[i] = strings.ReplaceAll(line, "{OUT}", session.outDir)
				}
				return expanded
			}
			session.cli.handler.SetExtendedJSON(row.extendedJSON)
			if row.prepare != nil {
				row.prepare(t, session)
			}
			runQuiet(t, session, expand(row.setup)...)
			var held heldHandles
			for index, statement := range expand(row.statements) {
				session.out.Reset()
				err := session.cli.handle(held.expand(statement))
				held.observe(session.out.String())
				if err == nil || errors.Is(err, ErrExit) {
					continue
				}
				if _, deferred := deferredToALaterPart(row.statements[index]); deferred {
					continue
				}
				if info := classify(err); info.Code == "UNCLASSIFIED" {
					t.Errorf("%s: %v classifies as UNCLASSIFIED: type it", strconv.Quote(row.statements[index]), err)
				}
			}
		})
	}
}
