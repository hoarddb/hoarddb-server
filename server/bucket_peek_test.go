package server

import (
	"go/ast"
	"go/parser"
	"go/token"
	"sort"
	"testing"
	"time"
)

// peekSites are the lookup sites of spec §5.4 classes P, PL and PW outside the
// repl-path files (catalog-gc build doc CG-10): rows 1, 16, 17, 19, 20, 21, 23,
// 24 and 25, and openBucketIdentity, which the create-beside-copy fix added (as
// openBucketGeneration) after the spec's table was drawn, and the four
// repl-path rows of CG-11 (13, 14, 15, 18). Each reads copies a peek took; none
// holds the bucket's struct.
var peekSites = map[string]bool{
	"blob_limit.go:Server.refuseBlobBucketPut":      true,
	"owed.go:Server.bucketOwesApplies":              true,
	"lineage.go:Cluster.shippedTerm":                true,
	"lineage.go:Server.bucketLineage":               true,
	"ttl_sweeper.go:Server.logTTLIndexes":           true,
	"ttl_sweeper.go:Server.sweepBucket":             true,
	"repl_recover.go:Server.bucketStateNamed":       true,
	"manage.go:Server.ListBuckets":                  true,
	"unique.go:Cluster.uniquePrecheck":              true,
	"unique.go:Server.bucketDeclaresUnique":         true,
	"catalog_question.go:Server.openBucketIdentity": true,
	"cluster_join.go:Server.bucketStoreOpen":        true,
	"cluster_join.go:Server.bucketServed":           true,
	"generation.go:Server.decideGenerationOfCreate": true,
	"migrate.go:Cluster.snapshotBucket":             true,
}

// holdsTheStruct names what, inside a function, means it holds a bucket's
// resident struct: the type itself, or a call that hands one out. A read view
// from resolveForRead is a per-request copy and is not on the list.
func holdsTheStruct(node ast.Node) (string, bool) {
	switch expr := node.(type) {
	case *ast.Ident:
		if expr.Name == "inMemoryBucket" {
			return "names *inMemoryBucket", true
		}
	case *ast.CallExpr:
		selector, isSelector := expr.Fun.(*ast.SelectorExpr)
		if !isSelector {
			return "", false
		}
		switch selector.Sel.Name {
		case "getBucket", "resolveBucket", "lockBucket":
			return "calls " + selector.Sel.Name, true
		case "get":
			if table, isTable := selector.X.(*ast.SelectorExpr); isTable && table.Sel.Name == "table" {
				return "calls table.get", true
			}
		}
	}
	return "", false
}

// TestPeekSitesDoNotHoldTheResidentStruct is CG-10's structural proof: every
// peek site reads a bucketFacts copy, so none of them reaches the struct the
// swap (CG-13) replaces with a record that can be recycled. A site that is
// renamed or moved fails here too, rather than leaving the list silently.
func TestPeekSitesDoNotHoldTheResidentStruct(t *testing.T) {
	fileSet := token.NewFileSet()
	found := make(map[string]bool, len(peekSites))
	var offenders []string
	files := make(map[string]bool)
	for site := range peekSites {
		for i := range site {
			if site[i] == ':' {
				files[site[:i]] = true
				break
			}
		}
	}
	for path := range files {
		parsed, err := parser.ParseFile(fileSet, path, nil, 0)
		if err != nil {
			t.Fatalf("parsing %s: %v", path, err)
		}
		for _, decl := range parsed.Decls {
			fn, isFunc := decl.(*ast.FuncDecl)
			if !isFunc {
				continue
			}
			site := path + ":" + enclosingFunctionName(parsed, fn.Pos())
			if !peekSites[site] {
				continue
			}
			found[site] = true
			ast.Inspect(fn, func(node ast.Node) bool {
				if why, holds := holdsTheStruct(node); holds {
					offenders = append(offenders, site+" "+why+" ("+fileSet.Position(node.Pos()).String()+")")
				}
				return true
			})
		}
	}
	for site := range peekSites {
		if !found[site] {
			offenders = append(offenders, site+" was not found: update peekSites with its new name")
		}
	}
	sort.Strings(offenders)
	for _, offender := range offenders {
		t.Errorf("a peek site holds the bucket's resident struct instead of a copy: %s", offender)
	}
}

// TestPeekSettledWaitsOutAnAttach pins the two peek classes apart. A plain
// peek (P) answers at once for a bucket whose create holds its lock, with an
// id of 0 and none of the fields that id publishes; a settled peek (PL) waits
// for the create to finish and then copies what it published, and reports a
// bucket a drop closed as not held.
func TestPeekSettledWaitsOutAnAttach(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "existing", BucketTypeHash)
	db := s.serverDataAccessor().getDB("app")
	pending := claimForTest(t, db, "pending", BucketTypeFIFO)

	pending.mu.Lock()
	facts, held := s.peekBucket("app", "pending")
	if !held || facts.id != 0 || facts.slot != 0 || facts.bucketType != "" {
		pending.mu.Unlock()
		t.Fatalf("a peek of a bucket whose create is in flight = %+v, held %v; want held, with no id, slot or type", facts, held)
	}
	settled := make(chan bucketFacts, 1)
	go func() {
		facts, _ := s.peekSettled("app", "pending")
		settled <- facts
	}()
	select {
	case early := <-settled:
		pending.mu.Unlock()
		t.Fatalf("a settled peek answered %+v while the create still held the bucket's lock", early)
	case <-time.After(50 * time.Millisecond):
	}
	pending.slot = 7
	pending.id.Store(99)
	pending.mu.Unlock()
	if after := <-settled; after.id != 99 || after.slot != 7 || after.bucketType != BucketTypeFIFO {
		t.Fatalf("a settled peek after the attach = %+v, want id 99, slot 7, type fifo", after)
	}

	closeRecordForTest(s, pending)
	if facts, held := s.peekSettled("app", "pending"); held {
		t.Fatalf("a settled peek of a closed bucket = %+v, held; want not held", facts)
	}
}

// TestJoinFenceReadsSettledPeeks pins the repl-path peeks (CG-11).
// bucketStoreOpen waits out an attach and then answers on the id the attach
// stored; bucketServed answers at once on the same id; neither reports a
// bucket a drop closed; and the placement pass calls the first once per held
// bucket, so it allocates nothing.
func TestJoinFenceReadsSettledPeeks(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "attached", BucketTypeHash)
	if !s.bucketStoreOpen("app", "attached") || !s.bucketServed("app", "attached") {
		t.Fatalf("an attached bucket is not reported open (%v) or served (%v)",
			s.bucketStoreOpen("app", "attached"), s.bucketServed("app", "attached"))
	}
	if s.bucketStoreOpen("app", "absent") || s.bucketServed("app", "absent") || s.bucketStoreOpen("nodb", "attached") {
		t.Fatal("a bucket or database that is not held is reported open or served")
	}
	if allocs := minAllocs(7, 100, func() { s.bucketStoreOpen("app", "attached") }); allocs != 0 {
		t.Fatalf("bucketStoreOpen allocates %v times per call, want 0: the placement pass calls it once per bucket", allocs)
	}

	db := s.serverDataAccessor().getDB("app")
	pending := claimForTest(t, db, "pending", BucketTypeFIFO)
	pending.mu.Lock()
	if s.bucketServed("app", "pending") {
		pending.mu.Unlock()
		t.Fatal("a bucket whose create is in flight is served")
	}
	opened := make(chan bool, 1)
	go func() { opened <- s.bucketStoreOpen("app", "pending") }()
	select {
	case early := <-opened:
		pending.mu.Unlock()
		t.Fatalf("bucketStoreOpen answered %v while the create still held the bucket's lock", early)
	case <-time.After(50 * time.Millisecond):
	}
	pending.id.Store(99)
	pending.mu.Unlock()
	if !<-opened {
		t.Fatal("bucketStoreOpen answered false after the create stored its id")
	}

	closeRecordForTest(s, pending)
	if s.bucketStoreOpen("app", "pending") {
		t.Fatal("bucketStoreOpen reports a bucket a drop closed as open")
	}
}
