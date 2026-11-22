package server

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// preAuthorizeGates is the recorded decision for every opcode NewHandler
// dispatches before s.authorize runs. Such an opcode never meets opRules, so
// it needs two other gates, and the guard below checks both rather than
// taking this comment's word for them:
//
//   - the driver listener refuses it by name (transport/tcp.go,
//     isNodeToNodeOpcode), so a user session cannot send it at all, and
//   - its handler checks the cluster token the frame carries
//     (Cluster.ValidateToken), so a connection that does reach it must hold
//     the node-to-node credential.
//
// The value says why the opcode cannot go behind the gate instead. An opcode
// an operator sends from a user session (cluster add, cluster remove, migrate)
// does not belong here: it is authorized by its opRules entry like any other
// client request.
var preAuthorizeGates = map[string]string{
	"OpGossip":           "membership announcement between nodes; carries no user identity to check a role against",
	"OpReplPush":         "replication and forwarded client frames between nodes; the coordinator authorized the user before forwarding",
	"OpClusterPropose":   "membership vote between nodes; the proposer's operator was authorized on the node that received the request",
	"OpMigrateStart":     "a node pulling a bucket snapshot from its source; carries no user identity",
	"OpMigrateData":      "a node fetching one snapshot chunk; carries no user identity",
	"OpMigrateDone":      "a node confirming a pulled snapshot; carries no user identity",
	"OpVersionedGet":     "a node's raw read for a quorum read; the coordinator authorized the user before asking",
	"OpMembershipChange": "a phase of a membership change between members; the operator who started it was authorized on the node that received the request",
	"OpAuthConfigPush":   "the auth master replicating the user store; carries no user identity",
}

// TestNoOpcodeIsDispatchedBeforeAuthorizeWithoutARecordedGate guards the cause
// of the authorization-gate defect: NewHandler dispatched the operator-facing cluster opcodes in the
// node-to-node switch above s.authorize, so their cluster-manager rules in opRules
// never ran. Any opcode dispatched above the gate must be in preAuthorizeGates,
// and the two gates that list promises must exist in the source.
func TestNoOpcodeIsDispatchedBeforeAuthorizeWithoutARecordedGate(t *testing.T) {
	fileSet := token.NewFileSet()
	dispatched := opcodesDispatchedBeforeAuthorize(t, fileSet)
	if len(dispatched) == 0 {
		t.Fatal("found no opcode dispatched before s.authorize in NewHandler; the guard no longer reads the dispatch order and proves nothing")
	}
	refusedOnDriver := driverRefusedOpcodes(t, fileSet)
	handlers := serverFuncDecls(t, fileSet)

	names := make([]string, 0, len(dispatched))
	for name := range dispatched {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		if _, recorded := preAuthorizeGates[name]; !recorded {
			t.Errorf("%s is dispatched before s.authorize in NewHandler with no recorded gate: its opRules entry never runs. "+
				"Dispatch it below s.authorize, or, if it is node-to-node traffic, record it in preAuthorizeGates", name)
			continue
		}
		if !refusedOnDriver[name] {
			t.Errorf("%s is dispatched before s.authorize but transport's isNodeToNodeOpcode does not refuse it on the driver listener", name)
		}
		handler, found := handlers[dispatched[name]]
		if !found {
			t.Errorf("%s is dispatched to %s, which is not a function in this package", name, dispatched[name])
			continue
		}
		if !checksClusterToken(handler) {
			t.Errorf("%s is dispatched before s.authorize to %s, which does not check the cluster token before anything else "+
				"(no ValidateToken call, one skipped when the node has no cluster, one nested in a branch, "+
				"or one after a statement other than the shared preamble; see checksClusterToken)", name, dispatched[name])
		}
	}
	for name := range preAuthorizeGates {
		if _, stillDispatched := dispatched[name]; !stillDispatched {
			t.Errorf("preAuthorizeGates records %s, which NewHandler no longer dispatches before s.authorize; delete the entry", name)
		}
	}
}

// opcodesDispatchedBeforeAuthorize maps each opcode NewHandler dispatches
// above its s.authorize call to the handler it is dispatched to.
func opcodesDispatchedBeforeAuthorize(t *testing.T, fileSet *token.FileSet) map[string]string {
	t.Helper()
	parsed, err := parser.ParseFile(fileSet, "dispatch.go", nil, 0)
	if err != nil {
		t.Fatalf("parsing dispatch.go: %v", err)
	}
	var newHandler *ast.FuncDecl
	for _, decl := range parsed.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && fn.Name.Name == "NewHandler" {
			newHandler = fn
		}
	}
	if newHandler == nil {
		t.Fatal("dispatch.go has no NewHandler; point the guard at the function that holds the authorization gate")
	}

	gate := token.NoPos
	ast.Inspect(newHandler, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		if callee, ok := call.Fun.(*ast.SelectorExpr); ok && callee.Sel.Name == "authorize" && gate == token.NoPos {
			gate = call.Pos()
		}
		return true
	})
	if gate == token.NoPos {
		t.Fatal("NewHandler never calls authorize; the authorization gate is gone")
	}

	dispatched := make(map[string]string)
	ast.Inspect(newHandler, func(node ast.Node) bool {
		clause, ok := node.(*ast.CaseClause)
		if !ok || clause.Pos() > gate {
			return true
		}
		handlerName := ""
		ast.Inspect(clause, func(inner ast.Node) bool {
			if call, ok := inner.(*ast.CallExpr); ok && handlerName == "" {
				if callee, ok := call.Fun.(*ast.Ident); ok {
					handlerName = callee.Name
				}
			}
			return true
		})
		for _, expr := range clause.List {
			if opcode, ok := expr.(*ast.SelectorExpr); ok && strings.HasPrefix(opcode.Sel.Name, "Op") {
				dispatched[opcode.Sel.Name] = handlerName
			}
		}
		return true
	})
	return dispatched
}

// driverRefusedOpcodes is the set of opcodes transport's isNodeToNodeOpcode
// names, which the driver listener refuses before any handler runs.
func driverRefusedOpcodes(t *testing.T, fileSet *token.FileSet) map[string]bool {
	t.Helper()
	parsed, err := parser.ParseFile(fileSet, filepath.Join("..", "transport", "tcp.go"), nil, 0)
	if err != nil {
		t.Fatalf("parsing transport/tcp.go: %v", err)
	}
	refused := make(map[string]bool)
	for _, decl := range parsed.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name.Name != "isNodeToNodeOpcode" {
			continue
		}
		ast.Inspect(fn, func(node ast.Node) bool {
			clause, ok := node.(*ast.CaseClause)
			if !ok {
				return true
			}
			for _, expr := range clause.List {
				if opcode, ok := expr.(*ast.Ident); ok {
					refused[opcode.Name] = true
				}
			}
			return true
		})
	}
	if len(refused) == 0 {
		t.Fatal("transport/tcp.go has no isNodeToNodeOpcode cases; the driver listener's refusal by name is gone or has moved")
	}
	return refused
}

// serverFuncDecls indexes this package's non-test functions by name.
func serverFuncDecls(t *testing.T, fileSet *token.FileSet) map[string]*ast.FuncDecl {
	t.Helper()
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("listing server sources: %v", err)
	}
	funcs := make(map[string]*ast.FuncDecl)
	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(fileSet, path, nil, 0)
		if err != nil {
			t.Fatalf("parsing %s: %v", path, err)
		}
		for _, decl := range parsed.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok && fn.Recv == nil {
				funcs[fn.Name.Name] = fn
			}
		}
	}
	return funcs
}

// checksClusterToken reports whether a failed cluster-token check leaves the
// handler before it does any work. The check must be a direct statement of the
// handler body, not nested in a branch, loop or closure (tokenGuard), and only
// the preamble the handlers share may come before it (preambleStatement).
// handleClusterPropose refuses every request and checks the token in a case of
// a tagless switch instead (tokenCheckedSwitch).
//
// It rejects `s.cluster != nil && !s.cluster.ValidateToken(...)`, which is
// false on a node with no cluster and so skips the check, without reading every
// unrelated `err != nil && ...` in the handler as that form (BUG-0574). "Work"
// is judged by syntax: a call rooted at the handler's *Server parameter or a
// cluster alias, a write to anything but a local, or any call where only a
// call-free expression may stand.
func checksClusterToken(handler *ast.FuncDecl) bool {
	reader := tokenCheckReader{server: serverParam(handler), clusterAliases: make(map[string]bool)}
	if reader.server == "" {
		return false
	}
	for i, stmt := range handler.Body.List {
		if reader.tokenGuard(stmt) || reader.tokenCheckedSwitch(stmt, handler.Body.List[i+1:]) {
			return true
		}
		if !reader.preambleStatement(stmt) {
			return false
		}
	}
	return false
}

// tokenCheckReader reads one handler, knowing which names reach node state:
// its *Server parameter and the locals that alias s.cluster.
type tokenCheckReader struct {
	server         string
	clusterAliases map[string]bool
}

// serverParam is the name of the handler's *Server parameter, or "".
func serverParam(handler *ast.FuncDecl) string {
	for _, param := range handler.Type.Params.List {
		pointer, ok := param.Type.(*ast.StarExpr)
		if !ok || len(param.Names) != 1 {
			continue
		}
		if typeName, ok := pointer.X.(*ast.Ident); ok && typeName.Name == "Server" {
			return param.Names[0].Name
		}
	}
	return ""
}

// tokenGuard reports whether stmt is `if <refuses on a bad token> { log...; return }`
// with no work in the condition, the logging or the return.
func (r tokenCheckReader) tokenGuard(stmt ast.Stmt) bool {
	guard, ok := stmt.(*ast.IfStmt)
	if !ok || guard.Init != nil || guard.Else != nil || !r.refusesOnBadToken(guard.Cond, false) {
		return false
	}
	last := len(guard.Body.List) - 1
	if last < 0 {
		return false
	}
	for _, bodyStmt := range guard.Body.List[:last] {
		if !r.logs(bodyStmt) {
			return false
		}
	}
	refusal, ok := guard.Body.List[last].(*ast.ReturnStmt)
	return ok && r.returnsWithoutWork(refusal, true)
}

// tokenCheckedSwitch reports whether stmt is a tagless switch whose cases up
// to and including the one that refuses on a bad token do no work, followed
// by a single return that does none either: every refused request then
// reaches that return having changed nothing but locals.
func (r tokenCheckReader) tokenCheckedSwitch(stmt ast.Stmt, followingStmts []ast.Stmt) bool {
	choice, ok := stmt.(*ast.SwitchStmt)
	if !ok || choice.Tag != nil || (choice.Init != nil && !r.aliasesCluster(choice.Init)) {
		return false
	}
	if len(followingStmts) != 1 {
		return false
	}
	if refusal, ok := followingStmts[0].(*ast.ReturnStmt); !ok || !r.returnsWithoutWork(refusal, false) {
		return false
	}
	for _, clauseStmt := range choice.Body.List {
		clause := clauseStmt.(*ast.CaseClause)
		tokenCase := false
		for _, caseExpr := range clause.List {
			switch {
			case r.refusesOnBadToken(caseExpr, true):
				tokenCase = true
			case containsCall(caseExpr):
				return false
			}
		}
		if !r.assignsLocalsOnly(clause.Body) {
			return false
		}
		if tokenCase {
			return true
		}
	}
	return false
}

// refusesOnBadToken reports whether cond is true whenever the cluster's
// ValidateToken fails: `!<cluster>.ValidateToken(...)` alone or as an operand
// of an `||` chain whose other operands make no call. An `&&` never qualifies,
// because its left side can make it false without the token ever being
// checked. allowDecode also admits `bson.Unmarshal(..., &req) != nil` as an
// operand, the decode handleClusterPropose does inside its token case.
func (r tokenCheckReader) refusesOnBadToken(cond ast.Expr, allowDecode bool) bool {
	checksToken := false
	for _, operand := range orOperands(cond) {
		switch {
		case r.failedTokenCheck(operand):
			checksToken = true
		case allowDecode && failedDecode(operand):
		case containsCall(operand):
			return false
		}
	}
	return checksToken
}

func orOperands(cond ast.Expr) []ast.Expr {
	cond = ast.Unparen(cond)
	if chain, ok := cond.(*ast.BinaryExpr); ok && chain.Op == token.LOR {
		return append(orOperands(chain.X), orOperands(chain.Y)...)
	}
	return []ast.Expr{cond}
}

// failedTokenCheck reports whether operand is `!<cluster>.ValidateToken(<call-free>)`.
func (r tokenCheckReader) failedTokenCheck(operand ast.Expr) bool {
	negation, ok := operand.(*ast.UnaryExpr)
	if !ok || negation.Op != token.NOT {
		return false
	}
	call, ok := ast.Unparen(negation.X).(*ast.CallExpr)
	if !ok {
		return false
	}
	callee, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || callee.Sel.Name != "ValidateToken" || !r.isCluster(callee.X) {
		return false
	}
	for _, arg := range call.Args {
		if containsCall(arg) {
			return false
		}
	}
	return true
}

// preambleStatement reports whether stmt is one of the few things a handler
// may do before its token check, none of which touches node state:
//   - `var req T`, with no initial value;
//   - `c := s.cluster`, which makes c a cluster alias, or
//     `refusal := errOldMembershipProtocol.Error()` on a package-level error;
//   - noteRequestDecode(...) with call-free arguments (a test-hook observation);
//   - `if <cluster> == nil { return ... }`, or
//     `if err := bson.Unmarshal(..., &req); err != nil { return ... }`: a body
//     of one return that does no work.
func (r tokenCheckReader) preambleStatement(stmt ast.Stmt) bool {
	switch preamble := stmt.(type) {
	case *ast.DeclStmt:
		return declaresOnly(preamble)
	case *ast.AssignStmt:
		return r.aliasesCluster(preamble) || r.definesErrorText(preamble)
	case *ast.ExprStmt:
		call, ok := preamble.X.(*ast.CallExpr)
		if !ok {
			return false
		}
		callee, ok := call.Fun.(*ast.Ident)
		return ok && callee.Name == "noteRequestDecode" && !containsCall(&ast.CompositeLit{Elts: call.Args})
	case *ast.IfStmt:
		if preamble.Else != nil || len(preamble.Body.List) != 1 {
			return false
		}
		refusal, ok := preamble.Body.List[0].(*ast.ReturnStmt)
		if !ok || !r.returnsWithoutWork(refusal, true) {
			return false
		}
		if preamble.Init == nil {
			return r.nilCluster(preamble.Cond)
		}
		return decodeFailedOnInit(preamble.Init, preamble.Cond)
	}
	return false
}

func declaresOnly(decl *ast.DeclStmt) bool {
	general, ok := decl.Decl.(*ast.GenDecl)
	if !ok || general.Tok != token.VAR {
		return false
	}
	for _, spec := range general.Specs {
		if variable, ok := spec.(*ast.ValueSpec); !ok || len(variable.Values) != 0 {
			return false
		}
	}
	return true
}

// aliasesCluster reports whether stmt is `<name> := s.cluster`, and records
// the alias when it is.
func (r tokenCheckReader) aliasesCluster(stmt ast.Stmt) bool {
	assign, ok := stmt.(*ast.AssignStmt)
	if !ok || assign.Tok != token.DEFINE || len(assign.Lhs) != 1 || len(assign.Rhs) != 1 {
		return false
	}
	alias, ok := assign.Lhs[0].(*ast.Ident)
	if !ok || !r.isServerCluster(assign.Rhs[0]) {
		return false
	}
	r.clusterAliases[alias.Name] = true
	return true
}

// definesErrorText reports whether stmt is `<name> := <pkgErr>.Error()`, where
// pkgErr is a plain identifier that does not reach node state.
func (r tokenCheckReader) definesErrorText(assign *ast.AssignStmt) bool {
	if assign.Tok != token.DEFINE || len(assign.Lhs) != 1 || len(assign.Rhs) != 1 {
		return false
	}
	call, ok := assign.Rhs[0].(*ast.CallExpr)
	if !ok || len(call.Args) != 0 {
		return false
	}
	callee, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || callee.Sel.Name != "Error" {
		return false
	}
	errValue, ok := callee.X.(*ast.Ident)
	return ok && !r.reachesNode(errValue.Name)
}

// nilCluster reports whether cond is exactly `<cluster> == nil`.
func (r tokenCheckReader) nilCluster(cond ast.Expr) bool {
	comparison, ok := ast.Unparen(cond).(*ast.BinaryExpr)
	return ok && comparison.Op == token.EQL && r.isCluster(comparison.X) && isNil(comparison.Y)
}

// decodeFailedOnInit reports whether init is `err := bson.Unmarshal(..., &req)`
// and cond is exactly `err != nil` on that err.
func decodeFailedOnInit(init ast.Stmt, cond ast.Expr) bool {
	assign, ok := init.(*ast.AssignStmt)
	if !ok || assign.Tok != token.DEFINE || len(assign.Lhs) != 1 || len(assign.Rhs) != 1 || !decodesRequest(assign.Rhs[0]) {
		return false
	}
	errName, ok := assign.Lhs[0].(*ast.Ident)
	if !ok {
		return false
	}
	comparison, ok := ast.Unparen(cond).(*ast.BinaryExpr)
	if !ok || comparison.Op != token.NEQ || !isNil(comparison.Y) {
		return false
	}
	checked, ok := comparison.X.(*ast.Ident)
	return ok && checked.Name == errName.Name
}

// failedDecode reports whether operand is `bson.Unmarshal(..., &req) != nil`.
func failedDecode(operand ast.Expr) bool {
	comparison, ok := operand.(*ast.BinaryExpr)
	return ok && comparison.Op == token.NEQ && decodesRequest(comparison.X) && isNil(comparison.Y)
}

// decodesRequest reports whether expr is `bson.Unmarshal(<call-free>, &<name>)`.
func decodesRequest(expr ast.Expr) bool {
	call, ok := expr.(*ast.CallExpr)
	if !ok || len(call.Args) != 2 || containsCall(call.Args[0]) {
		return false
	}
	callee, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || callee.Sel.Name != "Unmarshal" {
		return false
	}
	if pkg, ok := callee.X.(*ast.Ident); !ok || pkg.Name != "bson" {
		return false
	}
	target, ok := call.Args[1].(*ast.UnaryExpr)
	if !ok || target.Op != token.AND {
		return false
	}
	_, ok = target.X.(*ast.Ident)
	return ok
}

// logs reports whether stmt is `<server or cluster>.logger.<Level>(...)` whose
// arguments make no call but len and pass nothing that reaches node state.
// Logging is the one thing a refusal may do before it returns (handleGossip,
// handleReplPush and others warn on a bad token).
func (r tokenCheckReader) logs(stmt ast.Stmt) bool {
	expr, ok := stmt.(*ast.ExprStmt)
	if !ok {
		return false
	}
	call, ok := expr.X.(*ast.CallExpr)
	if !ok {
		return false
	}
	level, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || !logLevels[level.Sel.Name] {
		return false
	}
	logger, ok := level.X.(*ast.SelectorExpr)
	if !ok || logger.Sel.Name != "logger" {
		return false
	}
	nodeLogger := r.isCluster(logger.X)
	if owner, ok := logger.X.(*ast.Ident); ok && owner.Name == r.server {
		nodeLogger = true
	}
	if !nodeLogger {
		return false
	}
	for _, arg := range call.Args {
		if r.reachesNode(chainRoot(arg)) || callsOtherThanLen(arg) {
			return false
		}
	}
	return true
}

var logLevels = map[string]bool{"Debug": true, "Info": true, "Warn": true, "Error": true}

func callsOtherThanLen(expr ast.Expr) bool {
	other := false
	ast.Inspect(expr, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return !other
		}
		if builtin, ok := call.Fun.(*ast.Ident); !ok || builtin.Name != "len" {
			other = true
		}
		return !other
	})
	return other
}

// returnsWithoutWork reports whether a refusal's return makes no call rooted
// at the server or a cluster alias and passes neither to any call. allowSelfRead
// admits `<cluster>.selfPeer()`, which reads this node's own identity for
// handleGossip's refusal; the switch form's return admits nothing.
func (r tokenCheckReader) returnsWithoutWork(refusal *ast.ReturnStmt, allowSelfRead bool) bool {
	clean := true
	for _, result := range refusal.Results {
		ast.Inspect(result, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok || !clean {
				return clean
			}
			permittedRead := allowSelfRead && r.readsSelfPeer(call)
			if r.reachesNode(chainRoot(call.Fun)) && !permittedRead {
				clean = false
			}
			for _, arg := range call.Args {
				if r.reachesNode(chainRoot(arg)) {
					clean = false
				}
			}
			return clean
		})
	}
	return clean
}

func (r tokenCheckReader) readsSelfPeer(call *ast.CallExpr) bool {
	callee, ok := call.Fun.(*ast.SelectorExpr)
	return ok && callee.Sel.Name == "selfPeer" && len(call.Args) == 0 && r.isCluster(callee.X)
}

// assignsLocalsOnly reports whether every statement is an assignment to plain
// local identifiers from call-free expressions.
func (r tokenCheckReader) assignsLocalsOnly(body []ast.Stmt) bool {
	for _, stmt := range body {
		assign, ok := stmt.(*ast.AssignStmt)
		if !ok || (assign.Tok != token.ASSIGN && assign.Tok != token.DEFINE) {
			return false
		}
		for _, target := range assign.Lhs {
			local, ok := target.(*ast.Ident)
			if !ok || r.reachesNode(local.Name) {
				return false
			}
		}
		for _, source := range assign.Rhs {
			if containsCall(source) {
				return false
			}
		}
	}
	return true
}

// isCluster reports whether expr is s.cluster or a local alias of it.
func (r tokenCheckReader) isCluster(expr ast.Expr) bool {
	if alias, ok := expr.(*ast.Ident); ok {
		return r.clusterAliases[alias.Name]
	}
	return r.isServerCluster(expr)
}

func (r tokenCheckReader) isServerCluster(expr ast.Expr) bool {
	field, ok := expr.(*ast.SelectorExpr)
	if !ok || field.Sel.Name != "cluster" {
		return false
	}
	owner, ok := field.X.(*ast.Ident)
	return ok && owner.Name == r.server
}

func (r tokenCheckReader) reachesNode(name string) bool {
	return name != "" && (name == r.server || r.clusterAliases[name])
}

// chainRoot is the identifier a selector, index, call or address chain starts
// from (`s` in `s.cluster.bootstrapped.Swap(false)`), or "" when there is none.
func chainRoot(expr ast.Expr) string {
	for {
		switch step := expr.(type) {
		case *ast.Ident:
			return step.Name
		case *ast.SelectorExpr:
			expr = step.X
		case *ast.CallExpr:
			expr = step.Fun
		case *ast.IndexExpr:
			expr = step.X
		case *ast.StarExpr:
			expr = step.X
		case *ast.ParenExpr:
			expr = step.X
		case *ast.UnaryExpr:
			expr = step.X
		default:
			return ""
		}
	}
}

func isNil(expr ast.Expr) bool {
	nilIdent, ok := expr.(*ast.Ident)
	return ok && nilIdent.Name == "nil"
}

func containsCall(node ast.Node) bool {
	found := false
	ast.Inspect(node, func(inner ast.Node) bool {
		if _, ok := inner.(*ast.CallExpr); ok {
			found = true
		}
		return !found
	})
	return found
}

// parsedHandler parses one handler written as a test fixture, so the guard's
// helper can be shown to accept and reject specific shapes.
func parsedHandler(t *testing.T, handlerSource string) *ast.FuncDecl {
	t.Helper()
	parsed, err := parser.ParseFile(token.NewFileSet(), "handler.go", "package server\n"+handlerSource, 0)
	if err != nil {
		t.Fatalf("parsing fixture handler: %v", err)
	}
	for _, decl := range parsed.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok {
			return fn
		}
	}
	t.Fatal("fixture has no function")
	return nil
}

// The preamble every real pre-authorize handler opens with: a cluster alias, a
// nil-cluster refusal, a decode refusal.
const fixturePreamble = `
	c := s.cluster
	if c == nil {
		return refuse(frame, "replication is not enabled on this node")
	}
	var req ReplPushRequest
	noteRequestDecode("handleFixture", frame.Opcode, frame.Payload)
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return refuse(frame, "decode")
	}
`

// BUG-0574: an `err != nil && ...` that has nothing to do with the cluster,
// after an unconditional token check, is not a skipped check.
func TestChecksClusterTokenAcceptsAnUnrelatedNilCheckAfterAnUnconditionalToken(t *testing.T) {
	handler := parsedHandler(t, `func handleFixture(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {`+fixturePreamble+`
	if !c.ValidateToken(req.Token) {
		return refuse(frame, "invalid cluster token")
	}
	for _, e := range req.Entries {
		if err := c.apply(e); err != nil && len(req.Entries) == 0 {
			return refuse(frame, err.Error())
		}
	}
	return accept(frame)
}`)
	if !checksClusterToken(handler) {
		t.Error("checksClusterToken rejected a handler whose token check is unconditional and leaves the handler; " +
			"an unrelated `err != nil && ...` after it does not skip the check")
	}
}

func TestChecksClusterTokenRejectsATokenCheckSkippedWithoutACluster(t *testing.T) {
	handler := parsedHandler(t, `func handleFixture(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	var req ReplPushRequest
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return refuse(frame, "decode")
	}
	if s.cluster != nil && !s.cluster.ValidateToken(req.Token) {
		return refuse(frame, "invalid cluster token")
	}
	s.apply(req.Entries)
	return accept(frame)
}`)
	if checksClusterToken(handler) {
		t.Error("checksClusterToken accepted `s.cluster != nil && !s.cluster.ValidateToken(...)`, which applies the request unchecked on a node with no cluster")
	}
}

func TestChecksClusterTokenRejectsATokenCheckNestedInABranch(t *testing.T) {
	handler := parsedHandler(t, `func handleFixture(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	var req ReplPushRequest
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return refuse(frame, "decode")
	}
	if s.cluster != nil && len(req.Entries) > 0 {
		if !s.cluster.ValidateToken(req.Token) {
			return refuse(frame, "invalid cluster token")
		}
	}
	s.apply(req.Entries)
	return accept(frame)
}`)
	if checksClusterToken(handler) {
		t.Error("checksClusterToken accepted a token check nested inside a branch, which every path that skips the branch bypasses")
	}
}

// A nested check with no nil comparison anywhere: the old helper's nil scan
// could not see it, the dominance rule must.
func TestChecksClusterTokenRejectsATokenCheckNestedWithoutANilComparison(t *testing.T) {
	handler := parsedHandler(t, `func handleFixture(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {`+fixturePreamble+`
	if len(req.Entries) > 0 {
		if !c.ValidateToken(req.Token) {
			return refuse(frame, "invalid cluster token")
		}
	}
	c.apply(req.Entries)
	return accept(frame)
}`)
	if checksClusterToken(handler) {
		t.Error("checksClusterToken accepted a token check that only runs when the request has entries")
	}
}

func TestChecksClusterTokenRejectsATokenCheckAfterAStateChange(t *testing.T) {
	handler := parsedHandler(t, `func handleFixture(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {`+fixturePreamble+`
	c.apply(req.Entries)
	if !c.ValidateToken(req.Token) {
		return refuse(frame, "invalid cluster token")
	}
	return accept(frame)
}`)
	if checksClusterToken(handler) {
		t.Error("checksClusterToken accepted a handler that applies the request before it checks the cluster token")
	}
}

// fixtureIfHandler is a handler of the `if` form: the shared preamble, then
// beforeToken, then tokenCheck, then the work the check protects.
func fixtureIfHandler(beforeToken, tokenCheck string) string {
	return `func handleFixture(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {` +
		fixturePreamble + beforeToken + tokenCheck + `
	c.apply(req.Entries)
	return accept(frame)
}`
}

const fixtureTokenCheck = `
	if !c.ValidateToken(req.Token) {
		return refuse(frame, "invalid cluster token")
	}
`

// fixtureSwitchHandler is handleClusterPropose's shape: it refuses every
// request, checking the token in a case of a tagless switch.
const fixtureSwitchHandler = `func handleFixture(_ context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	refusal := errOldMembershipProtocol.Error()
	var req ClusterProposeRequest
	noteRequestDecode("handleFixture", frame.Opcode, frame.Payload)
	switch c := s.cluster; {
	case c == nil:
		refusal = "replication is not enabled on this node"
	case bson.Unmarshal(frame.Payload, &req) != nil || !c.ValidateToken(req.Token):
		refusal = "invalid cluster token"
	}
	return nodeResponse(transport.OpClusterPropose, frame, ClusterProposeResponse{Success: false, Error: refusal}, nil)
}`

// switchFixture rewrites one fragment of fixtureSwitchHandler, failing the
// test if the fragment is not there to rewrite.
func switchFixture(t *testing.T, original, replacement string) string {
	t.Helper()
	if !strings.Contains(fixtureSwitchHandler, original) {
		t.Fatalf("fixtureSwitchHandler has no %q to rewrite", original)
	}
	return strings.Replace(fixtureSwitchHandler, original, replacement, 1)
}

// The shapes the real handlers use must stay accepted; each is a control that
// passes before and after the tightening.
func TestChecksClusterTokenAcceptsTheShapesTheHandlersUse(t *testing.T) {
	shapes := map[string]string{
		"if form (handleReplPush)": fixtureIfHandler("", fixtureTokenCheck),
		"logging and a read of this node in the refusal (handleGossip)": fixtureIfHandler("", `
	if !c.ValidateToken(req.Token) {
		c.logger.Warn("gossip rejected: invalid cluster token", "node_id", req.NodeID, "entries", len(req.Entries))
		return nodeResponse(transport.OpGossip, frame, GossipResponse{Error: "invalid cluster token", Self: c.selfPeer(), Features: clusterFeatures()}, nil)
	}
`),
		"nil cluster in the token chain (handleMigrateStart)": `func handleFixture(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	var req MigrateStartRequest
	noteRequestDecode("handleFixture", frame.Opcode, frame.Payload)
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return nodeResponse(transport.OpMigrateStart, frame, MigrateStartResponse{Error: fmt.Sprintf("decode: %v", err)}, nil)
	}
	c := s.cluster
	if c == nil || !c.ValidateToken(req.Token) {
		return nodeResponse(transport.OpMigrateStart, frame, MigrateStartResponse{Error: "invalid cluster token"}, nil)
	}
	c.apply(req)
	return accept(frame)
}`,
		"no alias (handleVersionedGet)": `func handleFixture(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	var req VersionedGetRequest
	noteRequestDecode("handleFixture", frame.Opcode, frame.Payload)
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return errorFrame(frame, err), nil
	}
	if s.cluster == nil {
		return nodeResponse(transport.OpVersionedGet, frame, VersionedGetResponse{Error: "replication is not enabled on this node"}, nil)
	}
	if !s.cluster.ValidateToken(req.Token) {
		return nodeResponse(transport.OpVersionedGet, frame, VersionedGetResponse{Error: "invalid cluster token"}, nil)
	}
	s.read(req)
	return accept(frame)
}`,
		"switch form (handleClusterPropose)": fixtureSwitchHandler,
	}
	for shape, handlerSource := range shapes {
		t.Run(shape, func(t *testing.T) {
			if !checksClusterToken(parsedHandler(t, handlerSource)) {
				t.Errorf("checksClusterToken rejected the %s shape, which a real handler uses", shape)
			}
		})
	}
}

// F1 of the BUG-0574 rework: a preamble `if` refuses only on a nil cluster or
// a failed decode. A later answer moved above the token check, or work hidden
// in a guard's condition, is not a preamble.
func TestChecksClusterTokenRejectsWorkInThePreamble(t *testing.T) {
	moved := map[string]string{
		"catalog answer moved above the token check": `
	if req.Catalog != nil {
		return nodeResponse(transport.OpReplAck, frame, c.answerCatalog(req), nil)
	}
`,
		"handoff answer moved above the token check": `
	if req.Handoff != nil {
		return nodeResponse(transport.OpReplAck, frame, c.answerQuestion(req), nil)
	}
`,
		"mismatch refusal moved above the token check": `
	if c.mismatch.Load() {
		return nodeResponse(transport.OpReplAck, frame, ReplPushResponse{Error: ErrMemberMismatch.Error()}, nil)
	}
`,
		"state-changing call in a guard condition": `
	if s.cluster.bootstrapped.Swap(false) && len(frame.Payload) == 0 {
		return refuse(frame, "empty")
	}
`,
	}
	for hole, beforeToken := range moved {
		t.Run(hole, func(t *testing.T) {
			if checksClusterToken(parsedHandler(t, fixtureIfHandler(beforeToken, fixtureTokenCheck))) {
				t.Errorf("checksClusterToken accepted a handler with a %s", hole)
			}
		})
	}
}

// F2 of the BUG-0574 rework: in the switch form, nothing up to the token case
// may do work, and the return every refused request reaches may not either.
func TestChecksClusterTokenRejectsWorkInTheSwitchForm(t *testing.T) {
	holes := map[string]string{
		"call in an earlier case expression": switchFixture(t,
			"case c == nil:", "case c == nil || s.cluster.bootstrapped.Swap(false):"),
		"field assignment in the nil case body": switchFixture(t,
			`refusal = "replication is not enabled on this node"`,
			"refusal = \"replication is not enabled on this node\"\n\t\ts.memtable.appliedMB = 0"),
		"work in the final return": switchFixture(t,
			"ClusterProposeResponse{Success: false,", "ClusterProposeResponse{Success: s.cluster.bootstrapped.Swap(false),"),
		"another call in the token case": switchFixture(t,
			"case bson.Unmarshal(frame.Payload, &req) != nil ||", "case bson.Unmarshal(frame.Payload, &req) != nil || s.cluster.bootstrapped.Swap(false) ||"),
	}
	for hole, handlerSource := range holes {
		t.Run(hole, func(t *testing.T) {
			if checksClusterToken(parsedHandler(t, handlerSource)) {
				t.Errorf("checksClusterToken accepted a switch-form handler with %s", hole)
			}
		})
	}
}

// F3 of the BUG-0574 rework: the token check's own condition and refusal body
// do no work; the refusal may log and read this node's identity.
func TestChecksClusterTokenRejectsWorkInTheTokenCheck(t *testing.T) {
	checks := map[string]string{
		"state-changing call beside ValidateToken in the || chain": `
	if s.cluster.bootstrapped.Swap(false) || !c.ValidateToken(req.Token) {
		return refuse(frame, "invalid cluster token")
	}
`,
		"state-changing statement in the refusal body": `
	if !c.ValidateToken(req.Token) {
		s.cluster.removedAt.Store(0)
		return refuse(frame, "invalid cluster token")
	}
`,
		"state-changing call in the refusal's return": `
	if !c.ValidateToken(req.Token) {
		return nodeResponse(transport.OpReplAck, frame, ReplPushResponse{Success: c.bootstrapped.Swap(false)}, nil)
	}
`,
	}
	for hole, tokenCheck := range checks {
		t.Run(hole, func(t *testing.T) {
			if checksClusterToken(parsedHandler(t, fixtureIfHandler("", tokenCheck))) {
				t.Errorf("checksClusterToken accepted a token check with a %s", hole)
			}
		})
	}
}
