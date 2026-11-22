package server

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"
)

// createPlacementBucket creates a bucket on s with an optional placement
// option and returns the answer.
func createPlacementBucket(t *testing.T, s *Server, name string, bucketType BucketType, placement string) *CreateBucketResponse {
	t.Helper()
	req := CreateBucketRequest{Database: "app", Name: name, Type: bucketType}
	if placement != "" {
		req.Options = map[string]interface{}{"placement": placement}
	}
	resp, err := s.CreateBucket(req)
	if err != nil {
		t.Fatalf("CreateBucket(%q, %s, placement %q): %v", name, bucketType, placement, err)
	}
	return resp
}

// describedPlacement is what describe bucket reports for a bucket.
func describedPlacement(t *testing.T, s *Server, name string) string {
	t.Helper()
	stats, err := s.BucketStats(BucketStatsRequest{Database: "app", Bucket: name})
	if err != nil || !stats.Found {
		t.Fatalf("BucketStats(%q) = %+v, %v", name, stats, err)
	}
	return stats.Placement
}

// TestPlacementRecordedAndDefaultWhole is T-37 (build step 12a, default moved
// by MVP step 1b): a hash bucket created with no placement is recorded whole
// (defaultHashPlacement, spread again at MVP step 7), one created with
// placement spread is recorded spread, both survive a restart and describe
// bucket says which, and spread on any other type is refused naming the type.
func TestPlacementRecordedAndDefaultWhole(t *testing.T) {
	dataDir := t.TempDir()
	first := startCatalogServerAt(t, dataDir)
	s := first.core
	if resp, err := s.CreateDatabase("app"); err != nil || !resp.Success {
		t.Fatalf("CreateDatabase = %+v, %v", resp, err)
	}

	if resp := createPlacementBucket(t, s, "Defaulted", BucketTypeHash, ""); !resp.Success || resp.Placement != "whole" {
		t.Fatalf("a hash create with no placement answered %+v, want success and placement whole", resp)
	}
	if resp := createPlacementBucket(t, s, "Kept", BucketTypeHash, "whole"); !resp.Success || resp.Placement != "whole" {
		t.Fatalf("a hash create with placement whole answered %+v, want success and placement whole", resp)
	}
	if resp := createPlacementBucket(t, s, "Explicit", BucketTypeHash, "spread"); !resp.Success || resp.Placement != "spread" {
		t.Fatalf("a hash create with placement spread answered %+v, want success and placement spread", resp)
	}
	for _, bucketType := range []BucketType{BucketTypeFIFO, BucketTypeLIFO, BucketTypeHeap, BucketTypeBTree, BucketTypeBlob} {
		name := "Plain" + string(bucketType)
		resp := createPlacementBucket(t, s, name, bucketType, "spread")
		if resp.Success {
			t.Errorf("placement spread on a %s bucket was accepted", bucketType)
		}
		for _, want := range []string{string(bucketType), "only hash buckets spread"} {
			if !strings.Contains(resp.Message, want) {
				t.Errorf("the refusal of spread on a %s bucket is %q, want it to contain %q", bucketType, resp.Message, want)
			}
		}
		if resp := createPlacementBucket(t, s, name, bucketType, ""); !resp.Success || resp.Placement != "" {
			t.Errorf("a %s create with no placement answered %+v, want success and no placement", bucketType, resp)
		}
		if resp := createPlacementBucket(t, s, name+"W", bucketType, "whole"); !resp.Success || resp.Placement != "" {
			t.Errorf("a %s create with placement whole answered %+v, want success and no placement (it is always whole)", bucketType, resp)
		}
	}
	if resp := createPlacementBucket(t, s, "Odd", BucketTypeHash, "everywhere"); resp.Success || !strings.Contains(resp.Message, "spread") || !strings.Contains(resp.Message, "whole") {
		t.Errorf("placement everywhere answered %+v, want a refusal naming spread and whole", resp)
	}

	check := func(s *Server, when string) {
		t.Helper()
		for name, want := range map[string]string{"Defaulted": "whole", "Kept": "whole", "Explicit": "spread", "PlainfifoW": ""} {
			if name == "PlainfifoW" {
				name = "Plain" + string(BucketTypeFIFO)
			}
			if got := describedPlacement(t, s, name); got != want {
				t.Errorf("%s: describe bucket %s says placement %q, want %q", when, name, got, want)
			}
		}
		entry, found, err := s.readCatalogEntry("app", "Defaulted")
		if err != nil || !found || entry.Placement != "whole" {
			t.Errorf("%s: the catalog entry of Defaulted = %+v, %v, %v; want placement whole", when, entry, found, err)
		}
		entry, found, err = s.readCatalogEntry("app", "Plain"+string(BucketTypeFIFO))
		if err != nil || !found || entry.Placement != "" {
			t.Errorf("%s: the catalog entry of a fifo bucket = %+v, %v, %v; want no placement", when, entry, found, err)
		}
	}
	check(s, "before a restart")

	second := restartCatalog(t, first, dataDir)
	check(second.core, "after a restart")
}

// TestSpreadBucketNameCappedAt250Bytes is T-41: a 251-byte name is refused
// for a spread hash bucket and accepted for a whole one, up to maxNameBytes.
func TestSpreadBucketNameCappedAt250Bytes(t *testing.T) {
	s := newTestServer(t)
	if resp, err := s.CreateDatabase("app"); err != nil || !resp.Success {
		t.Fatalf("CreateDatabase = %+v, %v", resp, err)
	}
	long := nameOfBytes("post-", 251)
	resp := createPlacementBucket(t, s, long, BucketTypeHash, "spread")
	if resp.Success {
		t.Fatalf("a 251-byte name was accepted for a spread hash bucket")
	}
	for _, want := range []string{"250", "251 bytes", "placement: whole"} {
		if !strings.Contains(resp.Message, want) {
			t.Errorf("the refusal is %q, want it to contain %q", resp.Message, want)
		}
	}
	if resp := createPlacementBucket(t, s, nameOfBytes("post-", 250), BucketTypeHash, "spread"); !resp.Success {
		t.Errorf("a 250-byte spread name was refused: %s", resp.Message)
	}
	if resp := createPlacementBucket(t, s, nameOfBytes("dflt-", 251), BucketTypeHash, ""); !resp.Success {
		t.Errorf("a 251-byte name with no placement was refused (the default is whole): %s", resp.Message)
	}
	if resp := createPlacementBucket(t, s, long, BucketTypeHash, "whole"); !resp.Success {
		t.Errorf("a 251-byte whole name was refused: %s", resp.Message)
	}
	if resp := createPlacementBucket(t, s, nameOfBytes("pre-", maxNameBytes), BucketTypeHash, "whole"); !resp.Success {
		t.Errorf("a %d-byte whole name was refused: %s", maxNameBytes, resp.Message)
	}
	if resp := createPlacementBucket(t, s, nameOfBytes("queue-", 251), BucketTypeFIFO, ""); !resp.Success {
		t.Errorf("a 251-byte fifo name was refused (only a spread hash bucket is capped): %s", resp.Message)
	}
}

// catalogEntryLiteralsExemptFromPlacement lists, by file, function and the
// literal's position in that function, each catalogEntry literal in non-test
// code that need not set Placement, with the
// reason. A literal for a hash entry that is written to the catalog must set
// it; a drop of this field from one of those is the silent failure T-63 is
// for (the entry would read as whole after the next restart).
var catalogEntryLiteralsExemptFromPlacement = map[string]string{
	"catalog.go:readCatalogEntry:0":            "the zero value returned beside a miss; nothing is written",
	"catalog.go:readCatalogEntry:1":            "the zero value returned beside a read error; nothing is written",
	"catalog.go:readCatalogEntry:2":            "the zero value returned beside a decode error; nothing is written",
	"fieldtypes_schema.go:prepareFieldTypes:0": "a carrier handing the recorded typed fields to adoptFieldTypes; never written",
	"migrate.go:materializeRange:0":            "only DatabaseGeneration is read from it, by entryGeneration; never written",
}

// TestEveryCatalogEntryWriterCopiesPlacement is T-63: every catalogEntry
// literal in the server package's non-test files sets Placement, or is listed
// above with the reason it need not.
func TestEveryCatalogEntryWriterCopiesPlacement(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("listing the package: %v", err)
	}
	fset := token.NewFileSet()
	seenExempt := map[string]bool{}
	literals := 0
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(fset, file, nil, 0)
		if err != nil {
			t.Fatalf("parsing %s: %v", file, err)
		}
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			position := -1
			ast.Inspect(fn, func(node ast.Node) bool {
				lit, ok := node.(*ast.CompositeLit)
				if !ok {
					return true
				}
				if ident, ok := lit.Type.(*ast.Ident); !ok || ident.Name != "catalogEntry" {
					return true
				}
				literals++
				position++
				key := fmt.Sprintf("%s:%s:%d", file, fn.Name.Name, position)
				if _, exempt := catalogEntryLiteralsExemptFromPlacement[key]; exempt {
					seenExempt[key] = true
					return true
				}
				for _, element := range lit.Elts {
					if pair, ok := element.(*ast.KeyValueExpr); ok {
						if name, ok := pair.Key.(*ast.Ident); ok && name.Name == "Placement" {
							return true
						}
					}
				}
				t.Errorf("%s: the catalogEntry literal at %s does not set Placement, and %q is not listed as exempt",
					file, fset.Position(lit.Pos()), key)
				return true
			})
		}
	}
	if literals == 0 {
		t.Fatal("found no catalogEntry literal; the scan is broken")
	}
	for key := range catalogEntryLiteralsExemptFromPlacement {
		if !seenExempt[key] {
			t.Errorf("%q is listed as exempt but no catalogEntry literal is there; remove it", key)
		}
	}
}
