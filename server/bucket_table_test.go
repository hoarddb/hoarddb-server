package server

import (
	"fmt"
	"go/ast"
	"go/build"
	"go/parser"
	"go/token"
	"go/types"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
)

// stubImporter stands in for the package's dependencies while the guard
// type-checks it. The guard reads only the types this package declares, and
// resolving the real dependency graph would make the test slower than what it
// guards.
type stubImporter struct{}

func (stubImporter) Import(path string) (*types.Package, error) {
	name := path
	if slash := strings.LastIndex(path, "/"); slash >= 0 {
		name = path[slash+1:]
	}
	return types.NewPackage(path, name), nil
}

// enclosingFunctionName returns "Recv.Name" or "Name" for the declaration
// that holds position, or "" for a package-level declaration.
func enclosingFunctionName(file *ast.File, position token.Pos) string {
	for _, decl := range file.Decls {
		fn, isFunc := decl.(*ast.FuncDecl)
		if !isFunc || position < fn.Pos() || position > fn.End() {
			continue
		}
		if fn.Recv == nil || len(fn.Recv.List) == 0 {
			return fn.Name.Name
		}
		receiver := fn.Recv.List[0].Type
		if star, isStar := receiver.(*ast.StarExpr); isStar {
			receiver = star.X
		}
		if receiverName, isIdent := receiver.(*ast.Ident); isIdent {
			return receiverName.Name + "." + fn.Name.Name
		}
		return fn.Name.Name
	}
	return ""
}

// TestNoCodeReachesIntoTheBucketMap fails for any field read of a catalogTable
// outside catalog_table.go, and for any read of bucketTable.records outside
// bucket_table.go: between them they are the registry and the only values that
// carry it. It type-checks the package, so it matches the type rather than the
// field's name: the four files whose own struct has a field called buckets
// (ttl_sweeper.go, stranded.go, capacity_metrics.go, capacity_sampler.go) need
// no exemption, and neither does any file with an unrelated records field.
func TestNoCodeReachesIntoTheBucketMap(t *testing.T) {
	paths, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("listing the package's files: %v", err)
	}
	fileSet := token.NewFileSet()
	var files []*ast.File
	parsed := map[string]*ast.File{}
	for _, path := range paths {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		// MatchFile drops the other platforms' files, whose declarations
		// collide with this one's.
		matched, err := build.Default.MatchFile(".", filepath.Base(path))
		if err != nil {
			t.Fatalf("reading the build constraints of %s: %v", path, err)
		}
		if !matched {
			continue
		}
		file, err := parser.ParseFile(fileSet, path, nil, 0)
		if err != nil {
			t.Fatalf("parsing %s: %v", path, err)
		}
		files = append(files, file)
		parsed[path] = file
	}

	info := &types.Info{Selections: map[*ast.SelectorExpr]*types.Selection{}}
	config := types.Config{
		Importer: stubImporter{},
		// The stub imports leave much of the package uncheckable, so Check's
		// error is expected and says nothing about this package's own types;
		// what the guard reads is asserted below, from the resolved selections.
		Error: func(error) {},
	}
	pkg, _ := config.Check("github.com/hoarddb/hoarddb-server/server", fileSet, files, info)
	if pkg == nil {
		t.Fatal("type-checking the package produced no package; the guard cannot find the catalog table's type")
	}
	catalogType := pkg.Scope().Lookup("catalogTable")
	tableType := pkg.Scope().Lookup("bucketTable")
	if catalogType == nil || tableType == nil {
		t.Fatal("the package declares no catalogTable or bucketTable type; the guard cannot see what to guard")
	}

	var offenders []string
	ownFileReads, doorReads := 0, 0
	for selector, selection := range info.Selections {
		if selection.Kind() != types.FieldVal {
			continue
		}
		receiver := selection.Recv()
		if pointer, isPointer := receiver.(*types.Pointer); isPointer {
			receiver = pointer.Elem()
		}
		named, isNamed := receiver.(*types.Named)
		if !isNamed {
			continue
		}
		path := fileSet.Position(selector.Pos()).Filename
		site := filepath.Base(path) + ":" + enclosingFunctionName(parsed[path], selector.Pos())
		switch {
		case named.Obj() == catalogType:
			if filepath.Base(path) == "catalog_table.go" {
				ownFileReads++
				continue
			}
			offenders = append(offenders, site+" reads "+selector.Sel.Name+" of a catalogTable ("+fileSet.Position(selector.Pos()).String()+")")
		case named.Obj() == tableType && selector.Sel.Name == "records":
			if filepath.Base(path) == "bucket_table.go" {
				doorReads++
				continue
			}
			offenders = append(offenders, site+" takes the registry out of its bucketTable ("+fileSet.Position(selector.Pos()).String()+")")
		}
	}
	// A guard that resolved nothing passes for the wrong reason; these counts
	// are what it has to see in the two files that are allowed to reach in.
	if ownFileReads == 0 || doorReads == 0 {
		t.Fatalf("the guard resolved %d catalogTable field reads in catalog_table.go and %d bucketTable.records reads in bucket_table.go; it is not reading the types any more", ownFileReads, doorReads)
	}
	sort.Strings(offenders)
	for _, offender := range offenders {
		t.Errorf("reaches into the bucket map instead of going through bucketTable: %s", offender)
	}
}

func TestBucketTableClaimGetUnlinkCount(t *testing.T) {
	var guard sync.RWMutex
	table := newBucketTable(&guard)
	claim := func(name string, btype BucketType, generation int64, creating bool) heldRecord {
		t.Helper()
		held, err := table.claim(name, btype, generation, creating)
		if err != nil {
			t.Fatalf("claiming %q: %v", name, err)
		}
		return held
	}

	if _, found := table.get("orders"); found {
		t.Fatal("an empty table found a bucket")
	}
	first := claim("orders", BucketTypeHash, 7, true)
	if first.rec.generation.Load() != 7 || !first.rec.isCreating() {
		t.Fatalf("a fresh claim holds generation %d creating %v, want 7 true", first.rec.generation.Load(), first.rec.isCreating())
	}
	if got, _ := table.get("orders"); got != first {
		t.Fatalf("get returned %+v, want the claimed %+v", got, first)
	}

	again := claim("orders", BucketTypeLIFO, 9, false)
	if again != first {
		t.Fatal("a second claim of the same name made a second record")
	}
	if again.rec.generation.Load() != 9 || again.rec.isCreating() {
		t.Fatalf("a re-claim of an uncommitted record holds generation %d creating %v, want 9 false", again.rec.generation.Load(), again.rec.isCreating())
	}
	if got := bucketTypeOfCode(again.rec.bucketType); got != BucketTypeHash {
		t.Fatalf("a re-claim changed the type to %v", got)
	}

	first.rec.id.Store(5)
	claim("orders", BucketTypeHash, 11, true)
	if first.rec.generation.Load() != 9 || first.rec.isCreating() {
		t.Fatal("a claim of a committed record overwrote its generation or creating flag")
	}

	claim("users", BucketTypeFIFO, 1, false)
	if table.count() != 2 {
		t.Fatalf("count = %d, want 2", table.count())
	}
	if unlinked, err := table.unlink("orders", first); err != nil || !unlinked {
		t.Fatalf("unlinking the record the name finds: unlinked %v, err %v", unlinked, err)
	}
	if _, found := table.get("orders"); found || table.count() != 1 {
		t.Fatalf("after unlink: found %v, count %d; want not found, 1", found, table.count())
	}
	if unlinked, err := table.unlink("orders", first); err != nil || unlinked {
		t.Fatalf("a second unlink of the same record: unlinked %v, err %v; only the first drop owns the record", unlinked, err)
	}
}

// TestBucketTableUnlinkRefusesAnotherEpoch is the guard a drop's first phase
// relies on: a drop that found a record, and comes to unlink it after another
// drop freed it and a create of the same name took the slot again, unlinks
// nothing, so it cannot close the new bucket.
func TestBucketTableUnlinkRefusesAnotherEpoch(t *testing.T) {
	var guard sync.RWMutex
	table := newBucketTable(&guard)
	stale, err := table.claim("orders", BucketTypeHash, 1, false)
	if err != nil {
		t.Fatalf("claiming: %v", err)
	}
	if unlinked, err := table.unlink("orders", stale); err != nil || !unlinked {
		t.Fatalf("the first unlink: unlinked %v, err %v", unlinked, err)
	}
	stale.rec.mu.Lock()
	stale.rec.closed = true
	stale.rec.mu.Unlock()
	if err := table.free(stale.rec); err != nil {
		t.Fatalf("freeing: %v", err)
	}
	fresh, err := table.claim("orders", BucketTypeHash, 2, false)
	if err != nil {
		t.Fatalf("claiming again: %v", err)
	}
	if fresh.rec != stale.rec {
		t.Fatal("the freed slot was not reused; the test needs it reused")
	}
	if unlinked, err := table.unlink("orders", stale); err != nil || unlinked {
		t.Fatalf("a drop holding the previous epoch unlinked the bucket that reused its slot: unlinked %v, err %v", unlinked, err)
	}
	if _, found := table.get("orders"); !found {
		t.Fatal("the new bucket is gone")
	}
}

func TestBucketTableScanVisitsEachNameOnceInSteps(t *testing.T) {
	var guard sync.RWMutex
	table := newBucketTable(&guard)
	for i := range 3*firstRecordChunk + 1 {
		if _, err := table.claim(fmt.Sprintf("b%03d", i), BucketTypeHash, 1, false); err != nil {
			t.Fatalf("claiming: %v", err)
		}
	}

	visited := map[string]int{}
	table.scan(func(names []string) {
		if len(names) > maxRecordChunk {
			t.Errorf("a step carried %d names, more than one record chunk", len(names))
		}
		// The visitor runs outside the guard, so a writer can take it here.
		if guard.TryLock() {
			guard.Unlock()
		} else {
			t.Error("the guard was held while the visitor ran")
		}
		for _, name := range names {
			visited[name]++
		}
	})
	if len(visited) != 3*firstRecordChunk+1 {
		t.Fatalf("visited %d names, want %d", len(visited), 3*firstRecordChunk+1)
	}
	for name, times := range visited {
		if times != 1 {
			t.Errorf("%q visited %d times, want once", name, times)
		}
	}
}
