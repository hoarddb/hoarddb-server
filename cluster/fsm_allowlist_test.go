package cluster

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// applierPackageNames is every package-level variable, constant and function the apply*
// functions of fsm.go may reference. ApplyCommand is documented as a pure function of the
// state and the command, and the dry run of premise P1 (section 3 of the mixed-build spec) is
// sound only if two nodes of one build compute the same answer from the same bytes. A new name
// here needs a sentence in review saying why it is a constant of the build and not state that
// can differ between nodes or over time: a package variable that something else writes, a
// clock, a random source, a map iterated in range order.
var applierPackageNames = allowlisted(map[string][]string{
	"a command kind, a state word or a numeric limit: a constant of the build, which moves only with a version bump (the Phase 2 vocabulary golden)": {
		"CommandAlterDatabase", "CommandBootstrap", "CommandBumpSchema", "CommandCreateDatabase", "CommandDropDatabase",
		"CommandRegisterMember", "CommandRemoveMember", "CommandRetire", "CommandSeeded", "CommandSetClusterVersion", "CommandSetMemberState", "CommandSetSetting", "CommandSetVoters", "CommandVersion", "MaxBumpRetired", "MemberLeaving", "StorageWordDisk", "StorageWordMemory",
		"MemberActive", "MemberDead", "MemberDown", "MemberJoining", "StorageDisk", "StorageMemory", "seedBitmapBytes",
	},
	"a refusal code, condition or text: constants and pure formatters of the build": {
		"CodeClusterMismatch", "CodeConflict", "CodeInvalidArgument", "CodeNotFound", "CodeReservedName", "CodeSchemaNotCurrent", "CodeStateMoved", "CodeRFUnsupported", "CondConflict", "CondStateMoved",
		"CodeUserExists", "CondUserExists", "clipName",
		"CondRFUnsupportedCreateData", "CondRFUnsupportedCreateVoters", "MemoryRFRefusal",
		"commandRefusal", "conflict", "schemaNotCurrent", "stateMovedRefusal", "unknownDatabaseID", "unknownMember", "echoClipped",
		"joinVersionRefused", "membersChanged",
	},
	"the release table, read only for the words of a refusal (F8 names the release to upgrade to): rows only grow and a released row is never edited (TestEveryStateLevelHasARowAndRowsOnlyGrow), and whether a command is refused never depends on it": {
		"ReleaseOf", "stateVersions",
	},
	"a helper of the apply path that takes only its arguments (its body is followed by this test, and so are the helpers it calls; cross-build purity is the golden's and the digest's)": {
		"ParseStorageWord", "storageWordTable", "checkCommandShape", "compareBitmaps", "databaseNamePolicy", "decodeStateWord", "expectSetting",
		"newCommandFieldsObserving", "readBumpNote", "ringProjection", "checkCommandHeader", "differs", "knownCommandKind", "rankedState", "stateRefusal", "unpackRetired",
	},
	"the one package variable an applier reads: the bootstrap's settings, never written after init": {
		"defaultSettings",
	},
	"the user commands (ring user store spec 4.2): their kinds, the one algorithm word and the refusal code a grantor's refusal carries, constants of the build recorded in the vocabulary golden": {
		"CommandUserAlter", "CommandUserBootstrap", "CommandUserCreate", "CommandUserDrop", "CommandUserGrant", "CommandUserRevoke",
		"UserAlgo", "CodePermissionDenied",
	},
	"a helper of the user appliers that takes only its arguments (fsm_users.go; TestUserApplyAndAuthrulesReadNoClockNoConfigNoKeyringAndNoPepper scans the whole file for a clock, file, randomness, configuration or hash call)": {
		"UserCommandKind", "bumped", "checkCeiling", "refuseNegativeTime", "copyRow", "credentialPolicy", "grantPolicy", "replaceRow", "userIndex", "usernamePolicy",
	},
})

// applierImportedPackages are the imported packages an apply* function of fsm.go may call into:
// codecs, formatters, ordering and the name policy, which compute from their arguments alone.
// A package that reads the clock, the environment, the filesystem or a random source (time, os,
// math/rand, crypto/rand, runtime, net, sync/atomic over shared state) makes ApplyCommand depend
// on something other than the state and the command, so it is not here and cannot be added
// without a sentence in review saying why a call into it is deterministic.
var applierImportedPackages = map[string]bool{
	"bson": true, "bsoncore": true, "bsontype": true, "binary": true, "bytes": true, "cmp": true,
	"errors": true, "fmt": true, "namepolicy": true, "slices": true, "sort": true, "strconv": true, "strings": true,
	// authrules is the leaf of role rules and literal limits the user appliers share with the
	// server; TestUserApplyAndAuthrulesReadNoClockNoConfigNoKeyringAndNoPepper holds its imports to
	// standard packages that read no clock, file, randomness or configuration. math gives only the
	// uint32 bound and unicode only the fixed control-character table.
	"authrules": true, "math": true, "unicode": true,
}

// chainRoot is the identifier a selector, call, index or parenthesised chain starts from:
// time.Now().Unix() and pkg.Fn(x).Field[0] both resolve to the package name, so a call chained
// off an imported package is checked against applierImportedPackages like a plain pkg.Fn.
// It is nil when the chain starts from something that is not an identifier (a literal).
func chainRoot(expr ast.Expr) *ast.Ident {
	for {
		switch node := expr.(type) {
		case *ast.Ident:
			return node
		case *ast.SelectorExpr:
			expr = node.X
		case *ast.CallExpr:
			expr = node.Fun
		case *ast.IndexExpr:
			expr = node.X
		case *ast.ParenExpr:
			expr = node.X
		case *ast.StarExpr:
			expr = node.X
		default:
			return nil
		}
	}
}

// allowlisted flattens groups of names, each with its reason, into one lookup.
func allowlisted(groups map[string][]string) map[string]string {
	flat := map[string]string{}
	for reason, names := range groups {
		for _, name := range names {
			flat[name] = reason
		}
	}
	return flat
}

// TestApplyCommandReadsOnlyAnAllowlistOfPackageState is the static half of the purity premise
// (round-5 M4): it parses the package, finds every package-level var, const and func the
// apply* methods of fsm.go reference, and fails on one outside applierPackageNames. It cannot
// prove that two builds apply alike (the Phase 2 state golden and the Phase 3 digest do that);
// it stops a build from making an applier read a new global without somebody saying why.
// It follows the receiver-less functions of the package that the appliers reach, transitively,
// and checks their bodies against the same two lists. Its limit: a METHOD an applier calls on the
// state or the command (st.memberIndex, cmd.field) is not followed, so a clock read in such a
// method (the commandFields method case) is not seen; the Phase 2 state golden and the Phase 3 digest are the net for those.
// Break: make an applier read a new package variable, or make an allowlisted helper call time.Now.
func TestApplyCommandReadsOnlyAnAllowlistOfPackageState(t *testing.T) {
	fset := token.NewFileSet()
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	var parsed []*ast.File
	declared := map[string]string{}    // package-level var, const and func name -> kind
	helpers := map[string]helperDecl{} // receiver-less functions, to follow from the appliers
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		src, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		file, err := parser.ParseFile(fset, name, src, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		parsed = append(parsed, file)
		for _, decl := range file.Decls {
			switch d := decl.(type) {
			case *ast.FuncDecl:
				if d.Recv == nil {
					declared[d.Name.Name] = "func"
					helpers[d.Name.Name] = helperDecl{file: file, fn: d}
				}
			case *ast.GenDecl:
				for _, spec := range d.Specs {
					if v, ok := spec.(*ast.ValueSpec); ok {
						for _, n := range v.Names {
							declared[n.Name] = strings.ToLower(d.Tok.String())
						}
					}
				}
			}
		}
	}

	referenced := map[string]string{}    // name -> the applier that reads it
	importedCalls := map[string]string{} // imported package name -> the applier that selects from it
	scan := func(file *ast.File, fn *ast.FuncDecl, reader string) {
		local := localNames(fn)
		imported := importNames(file)
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			switch node := n.(type) {
			case *ast.SelectorExpr:
				if base := chainRoot(node.X); base != nil && imported[base.Name] && !local[base.Name] {
					if _, seen := importedCalls[base.Name]; !seen {
						importedCalls[base.Name] = reader
					}
				}
				ast.Inspect(node.X, func(inner ast.Node) bool { return visitApplierIdent(inner, declared, local, referenced, reader) })
				return false
			case *ast.KeyValueExpr:
				// A struct literal's key is a field name, not a package-level name.
				if _, plain := node.Key.(*ast.Ident); plain {
					ast.Inspect(node.Value, func(inner ast.Node) bool { return visitApplierIdent(inner, declared, local, referenced, reader) })
					return false
				}
			}
			return visitApplierIdent(n, declared, local, referenced, reader)
		})
	}
	appliers := 0
	for _, file := range parsed {
		// fsm_users.go holds the user commands' appliers (ring user store spec 4.2): the same
		// purity rule, so the same scan.
		if name := fset.Position(file.Pos()).Filename; name != "fsm.go" && name != "fsm_users.go" {
			continue
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv == nil || !strings.HasPrefix(fn.Name.Name, "apply") || fn.Body == nil {
				continue
			}
			appliers++
			scan(file, fn, fn.Name.Name)
		}
	}
	// Follow the same-package functions the appliers reach, however deep: a helper that reads
	// the clock or a global is as much a part of ApplyCommand as the applier that calls it.
	followed := map[string]bool{}
	for again := true; again; {
		again = false
		for name, reader := range referenced {
			helper, isHelper := helpers[name]
			if !isHelper || followed[name] || helper.fn.Body == nil {
				continue
			}
			followed[name] = true
			again = true
			scan(helper.file, helper.fn, name+" (called by "+reader+")")
		}
	}
	if appliers < 10 {
		t.Fatalf("found %d apply* methods in fsm.go, want at least 10: the scan no longer finds the appliers", appliers)
	}

	var unlisted []string
	for name, reader := range referenced {
		if _, ok := applierPackageNames[name]; !ok {
			unlisted = append(unlisted, name+" ("+declared[name]+", read by "+reader+")")
		}
	}
	sort.Strings(unlisted)
	if len(unlisted) > 0 {
		t.Errorf("the apply* methods reference package-level names outside the allowlist:\n  %s\nIf a name is a constant of the build, add it to applierPackageNames with the reason; if it is state that can differ between nodes or over time, the applier must not read it", strings.Join(unlisted, "\n  "))
	}
	var forbidden []string
	for pkg, reader := range importedCalls {
		if !applierImportedPackages[pkg] {
			forbidden = append(forbidden, pkg+" (selected from by "+reader+")")
		}
	}
	sort.Strings(forbidden)
	if len(forbidden) > 0 {
		t.Errorf("the apply* methods call into imported packages outside applierImportedPackages:\n  %s\nA clock, the environment, the filesystem or a random source makes ApplyCommand depend on more than the state and the command; if the package is deterministic, add it with a sentence in review", strings.Join(forbidden, "\n  "))
	}
	for name := range applierPackageNames {
		if _, ok := referenced[name]; !ok {
			t.Errorf("the allowlist holds %q, which no applier references: remove it so the list stays the truth", name)
		}
	}
}

// helperDecl is a receiver-less function and the file whose imports its body is read against.
type helperDecl struct {
	file *ast.File
	fn   *ast.FuncDecl
}

// visitApplierIdent records n when it is a reference to a declared package-level name.
func visitApplierIdent(n ast.Node, declared map[string]string, local map[string]bool, referenced map[string]string, reader string) bool {
	ident, ok := n.(*ast.Ident)
	if !ok || local[ident.Name] {
		return true
	}
	if _, isPackageLevel := declared[ident.Name]; isPackageLevel {
		if _, seen := referenced[ident.Name]; !seen {
			referenced[ident.Name] = reader
		}
	}
	return true
}

// importNames is the name each import of file is referred to by in its code: the explicit name,
// else the last element of the path (the packages this scan sees keep their last element as
// their name; a version suffix would need the explicit name).
func importNames(file *ast.File) map[string]bool {
	names := map[string]bool{}
	for _, spec := range file.Imports {
		if spec.Name != nil {
			names[spec.Name.Name] = true
			continue
		}
		path := strings.Trim(spec.Path.Value, `"`)
		names[path[strings.LastIndex(path, "/")+1:]] = true
	}
	return names
}

// localNames is every name fn declares for itself: receiver, parameters, results, := and var
// declarations, range variables and closure parameters. A local that shadows a package-level
// name hides it from the scan, which errs toward silence only for a function that shadows.
func localNames(fn *ast.FuncDecl) map[string]bool {
	local := map[string]bool{}
	add := func(list *ast.FieldList) {
		if list == nil {
			return
		}
		for _, field := range list.List {
			for _, name := range field.Names {
				local[name.Name] = true
			}
		}
	}
	add(fn.Recv)
	add(fn.Type.Params)
	add(fn.Type.Results)
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		switch node := n.(type) {
		case *ast.AssignStmt:
			if node.Tok == token.DEFINE {
				for _, lhs := range node.Lhs {
					if ident, ok := lhs.(*ast.Ident); ok {
						local[ident.Name] = true
					}
				}
			}
		case *ast.RangeStmt:
			if node.Tok == token.DEFINE {
				for _, e := range []ast.Expr{node.Key, node.Value} {
					if ident, ok := e.(*ast.Ident); ok {
						local[ident.Name] = true
					}
				}
			}
		case *ast.ValueSpec:
			for _, name := range node.Names {
				local[name.Name] = true
			}
		case *ast.FuncLit:
			add(node.Type.Params)
			add(node.Type.Results)
		}
		return true
	})
	return local
}
