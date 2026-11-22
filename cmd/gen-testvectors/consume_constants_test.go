package main

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"testing"

	"github.com/hoarddb/hoarddb-server/store"
)

// TestConstantsFileComesFromTheServersConstants (claim-ack-spec 13.4, build doc
// B-9): the server-side numbers in consume-constants.json equal the store's
// exported constants, and the generator names each of those constants where it
// builds the file, so a number typed in by hand would be noticed.
func TestConstantsFileComesFromTheServersConstants(t *testing.T) {
	encoded, err := encodeConsumeConstants()
	if err != nil {
		t.Fatal(err)
	}
	var generated consumeConstants
	if err := json.Unmarshal(encoded, &generated); err != nil {
		t.Fatal(err)
	}
	want := map[string][2]int64{
		"claim_max":        {int64(generated.ClaimMax), store.MaxClaimLimit},
		"lease_ms_min":     {generated.LeaseMsMin, store.MinLeaseMs},
		"lease_ms_default": {generated.LeaseMsDefault, store.DefaultLeaseMs},
		"lease_ms_max":     {generated.LeaseMsMax, store.MaxLeaseMs},
		"max_deliveries":   {int64(generated.MaxDeliveries), store.MaxDeliveries},
		"item_ids_max":     {int64(generated.ItemIDsMax), store.MaxItemIDsPerAck},
	}
	for name, pair := range want {
		if pair[0] != pair[1] {
			t.Errorf("%s is %d in the file and %d in the store", name, pair[0], pair[1])
		}
	}

	parsed, err := parser.ParseFile(token.NewFileSet(), "consume_constants.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	named := map[string]bool{}
	ast.Inspect(parsed, func(node ast.Node) bool {
		if selector, ok := node.(*ast.SelectorExpr); ok {
			if pkg, ok := selector.X.(*ast.Ident); ok && pkg.Name == "store" {
				named[selector.Sel.Name] = true
			}
		}
		return true
	})
	for _, constant := range []string{"MaxClaimLimit", "MinLeaseMs", "DefaultLeaseMs", "MaxLeaseMs", "MaxDeliveries", "MaxItemIDsPerAck"} {
		if !named[constant] {
			t.Errorf("consume_constants.go does not read store.%s: a hand-typed number would drift", constant)
		}
	}
	if generated.ConsumeBackoffFloorMs != 100 || generated.ConsumeBackoffCeilingMs != 5000 || generated.ConsumeBackoff != "full_jitter_doubling" ||
		generated.ConsumeMarginMinMs != 250 || generated.ConsumeMarginFraction != 0.1 {
		t.Errorf("the consume loop's constants are %+v", generated)
	}
}

// TestConstantsFileIsWrittenAndCheckedWithTheVectors: the file is written beside
// v1.json by the same run and compared by the check, and a drifted copy is
// refused with the fix.
func TestConstantsFileIsWrittenAndCheckedWithTheVectors(t *testing.T) {
	dir := t.TempDir()
	if _, err := writeConsumeConstants(dir); err != nil {
		t.Fatal(err)
	}
	vectorPath := filepath.Join(dir, vectorFileName)
	if err := checkConsumeConstants(vectorPath); err != nil {
		t.Fatalf("a file just written does not check: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, constantsFileName), []byte("{}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := checkConsumeConstants(vectorPath); err == nil {
		t.Fatal("a drifted constants file passed the check")
	}
	if err := checkConsumeConstants(filepath.Join(t.TempDir(), vectorFileName)); err == nil {
		t.Fatal("a missing constants file passed the check")
	}
}
