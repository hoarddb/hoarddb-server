package storage_test

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// versionChoosingCalls are the Badger calls that write at versions the caller or the call itself
// chooses, so the order of commit versions, which the vouched cut's open step relies on
// (third_party/badger/flushed_log.go: DB.removeLogsOfNamedTables removes every replayed log whose
// highest version is at or below the tables'), no longer holds. Under a vouched cut Badger refuses
// them (TestTheCutIsOptIn); the legacy store may call none of them either, except DropPrefix,
// which keeps the versions it finds. A call listed here appears in no production file.
var versionChoosingCalls = map[string]bool{
	"RunValueLogGC":        true,
	"NewStreamWriter":      true,
	"BanNamespace":         true,
	"DropAll":              true,
	"OpenManaged":          true,
	"NewWriteBatchAt":      true,
	"NewManagedWriteBatch": true,
	"NewTransactionAt":     true,
	"CommitAt":             true,
	"SetDiscardTs":         true,
	// NewKVLoader writes at the versions its caller hands it (KVLoader.Set) and is not refused
	// under the cut (review R2 F-12).
	"NewKVLoader": true,
}

// testOnlyFlushSeams are the exported seams of the patched Badger's flush that only tests may set
// (third_party/badger: SetFlushStepHook, SetFlushTestHook); production code names neither, and the
// scan below gates that rather than a comment. In production each costs one atomic load per flush
// step.
var testOnlyFlushSeams = map[string]bool{
	"SetFlushStepHook": true,
	"SetFlushTestHook": true,
}

// versionChoosingCallsIn returns where source names a call of versionChoosingCalls or a seam of
// testOnlyFlushSeams, as "file:line name". Every selector with a listed name is reported, called or
// not: a method value (db.RunValueLogGC) or a method expression ((*badger.DB).DropAll) is a call
// whose later invocation names nothing (review R-6). A bare identifier is reported when called.
// DB.Load takes a reader and a count; a call named Load with two arguments is reported (an atomic's
// Load takes none), and a method value of Load is not, since it cannot be told from an atomic's.
func versionChoosingCallsIn(fileName string, source any) ([]string, error) {
	fileSet := token.NewFileSet()
	parsed, err := parser.ParseFile(fileSet, fileName, source, 0)
	if err != nil {
		return nil, fmt.Errorf("parsing %s: %w", fileName, err)
	}
	listed := func(name string) bool { return versionChoosingCalls[name] || testOnlyFlushSeams[name] }
	var found []string
	report := func(position token.Pos, name string) {
		found = append(found, fmt.Sprintf("%s:%d %s", fileName, fileSet.Position(position).Line, name))
	}
	ast.Inspect(parsed, func(node ast.Node) bool {
		switch expression := node.(type) {
		case *ast.SelectorExpr:
			if listed(expression.Sel.Name) {
				report(expression.Pos(), expression.Sel.Name)
			}
		case *ast.CallExpr:
			switch function := expression.Fun.(type) {
			case *ast.Ident:
				if listed(function.Name) {
					report(expression.Pos(), function.Name)
				}
			case *ast.SelectorExpr:
				if function.Sel.Name == "Load" && len(expression.Args) == 2 {
					report(expression.Pos(), "Load")
				}
			}
		}
		return true
	})
	return found, nil
}

// TestNoProductionCodeCallsAVersionChoosingBadgerCall is the gate of the version-order reliance
// (design note 2026-11-18, section 6): it fails when production code outside third_party calls
// a call that breaks the order the open step relies on. The first part shows the checker sees a
// planted call and passes DropPrefix, an atomic's Load and a comment; the second reads every
// non-test Go file of the module.
func TestNoProductionCodeCallsAVersionChoosingBadgerCall(t *testing.T) {
	t.Run("the checker sees a planted call", func(t *testing.T) {
		planted := `package planted

import badger "github.com/dgraph-io/badger/v4"

// db.RunValueLogGC(0.5) in a comment is not a call.
func plant(db *badger.DB, counter interface{ Load() int }) error {
	_ = counter.Load()
	_ = db.DropPrefix([]byte("legacy"))
	if err := db.Load(nil, 16); err != nil {
		return err
	}
	gc := db.RunValueLogGC
	_ = gc(0.5)
	dropAll := (*badger.DB).DropAll
	_ = dropAll(db)
	defer badger.SetFlushStepHook(nil)()
	defer badger.SetFlushTestHook(nil)()
	_ = db.NewKVLoader(16)
	return db.RunValueLogGC(0.5)
}
`
		found, err := versionChoosingCallsIn("planted.go", planted)
		if err != nil {
			t.Fatal(err)
		}
		// A method value and a method expression are reported where they are named, since the
		// call through them names nothing; so are the flush seams, which no production code sets.
		want := []string{"planted.go:9 Load", "planted.go:12 RunValueLogGC", "planted.go:14 DropAll",
			"planted.go:16 SetFlushStepHook", "planted.go:17 SetFlushTestHook", "planted.go:18 NewKVLoader",
			"planted.go:19 RunValueLogGC"}
		if strings.Join(found, ",") != strings.Join(want, ",") {
			t.Fatalf("the checker found %v in the planted file, want %v", found, want)
		}
	})

	t.Run("production code makes none", func(t *testing.T) {
		moduleRoot := ".."
		var offenders []string
		walkErr := filepath.WalkDir(moduleRoot, func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() {
				name := entry.Name()
				if name == "third_party" || name == "vendor" || strings.HasPrefix(name, ".") && path != moduleRoot {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			found, err := versionChoosingCallsIn(path, nil)
			if err != nil {
				return err
			}
			offenders = append(offenders, found...)
			return nil
		})
		if walkErr != nil {
			t.Fatalf("reading the module's Go files: %v", walkErr)
		}
		sort.Strings(offenders)
		if len(offenders) > 0 {
			t.Fatalf("production code names what writes at versions the open step's premise does not cover "+
				"(Badger's own DropPrefix is the only one the legacy store may call), or a flush seam only tests may set: %s. "+
				"The vouched cut's open step (third_party/badger/flushed_log.go) would remove the log of a commit below "+
				"the tables' highest version",
				strings.Join(offenders, "; "))
		}
	})
}
