package server

import (
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// uniqueCreate creates app/name as a hash bucket declaring a unique Email,
// with the body text a shell sent (SchemaJSON) and the placement asked for
// ("" leaves the option out, which is defaultHashPlacement).
func uniqueCreate(t *testing.T, s *Server, name, body, placement string) *CreateBucketResponse {
	t.Helper()
	options := map[string]interface{}{"indices": []interface{}{
		map[string]interface{}{"fields": []interface{}{"Email"}, "unique": true},
	}}
	if placement != "" {
		options["placement"] = placement
	}
	resp, err := s.CreateBucket(CreateBucketRequest{Database: "app", Name: name, Type: BucketTypeHash, SchemaJSON: body, Options: options})
	if err != nil {
		t.Fatalf("CreateBucket(%s): %v", name, err)
	}
	return resp
}

// TestUniqueRefusedOnSpreadNamesTheWholeCreate is T-24 (build step 12b): a
// unique field other than _id_ cannot be checked across the parts of a spread
// bucket, so the create is refused with the create line that works, built from
// the declaration the caller wrote; placement whole accepts it. Spec 8.3 also
// names a "later index declaration"; this tree has none (an index is declared
// at create only), so the create is the one door.
func TestUniqueRefusedOnSpreadNamesTheWholeCreate(t *testing.T) {
	s := newTestServer(t)
	if resp, err := s.CreateDatabase("app"); err != nil || !resp.Success {
		t.Fatalf("CreateDatabase = %+v, %v", resp, err)
	}
	const body = "{ Name string, Email string unique }"

	// MVP step 1b: a create naming no placement is whole, so a unique field
	// is accepted there; only an explicit placement: spread is refused. The
	// "spread by default" wording comes back when step 7 flips the default.
	if resp := uniqueCreate(t, s, "Defaulted", body, ""); !resp.Success || resp.Placement != "whole" {
		t.Fatalf("a unique Email with no placement answered %+v, want success and placement whole", resp)
	}

	for _, asked := range []string{"spread"} {
		resp := uniqueCreate(t, s, "User", body, asked)
		if resp.Success {
			t.Fatalf("a unique Email on a spread bucket (placement %q) was accepted: %+v", asked, resp)
		}
		for _, want := range []string{
			`Cannot declare Email unique on bucket "User" (database "app")`,
			map[string]string{"spread": "declared spread (placement: spread)"}[asked],
			"create bucket User in app { placement: whole, Name string, Email string unique }",
		} {
			if !strings.Contains(resp.Message, want) {
				t.Errorf("placement %q: the refusal lacks %q:\n%s", asked, want, resp.Message)
			}
		}
	}
	if stats, err := s.BucketStats(BucketStatsRequest{Database: "app", Bucket: "User"}); err == nil && stats.Found {
		t.Fatalf("a refused create left the bucket behind: %+v", stats)
	}

	// A body that already says placement: spread is turned, not doubled.
	resp := uniqueCreate(t, s, "User", "{ placement: spread, Email string unique }", "spread")
	if resp.Success || !strings.Contains(resp.Message, "create bucket User in app { placement: whole, Email string unique }") {
		t.Errorf("a body naming spread answered %+v, want the same body with placement: whole", resp)
	}

	// With no body to quote, the create line declares the index in the option form.
	resp = uniqueCreate(t, s, "User", "", "spread")
	if resp.Success || !strings.Contains(resp.Message, `create bucket User in app { placement: whole, indices: [{ field: "Email", unique: true }] }`) {
		t.Errorf("a create with no body answered %+v, want a create line with the indices option", resp)
	}

	if resp := uniqueCreate(t, s, "User", body, "whole"); !resp.Success || resp.Placement != "whole" {
		t.Fatalf("a unique Email on a whole bucket answered %+v, want success", resp)
	}

	// Not a unique field on a part: a plain index and a unique _id_ stay allowed on spread.
	plain, err := s.CreateBucket(CreateBucketRequest{Database: "app", Name: "Plain", Type: BucketTypeHash,
		Options: map[string]interface{}{"placement": "spread", "indices": []interface{}{"Email"}}})
	if err != nil || !plain.Success || plain.Placement != "spread" {
		t.Fatalf("a plain index on a spread bucket answered %+v, %v, want success", plain, err)
	}
}

// TestRestoreKeepsTheManifestPlacement is T-65 (build step 12b): the dump
// manifest records placement and a restore creates the bucket with it, so a
// whole bucket restores whole with its unique index, and a spread one spread.
// The manifest and the request are read and written by wire names, so this
// test compiles against the parent and fails on what the parent does.
func TestRestoreKeepsTheManifestPlacement(t *testing.T) {
	s := newTestServer(t)
	ctx := rootContext()
	if resp, err := s.CreateDatabase("app"); err != nil || !resp.Success {
		t.Fatalf("CreateDatabase = %+v, %v", resp, err)
	}
	if resp := uniqueCreate(t, s, "Whole", "", "whole"); !resp.Success {
		t.Fatalf("creating the whole bucket: %+v", resp)
	}
	if resp := createPlacementBucket(t, s, "Spread", BucketTypeHash, "spread"); !resp.Success {
		t.Fatalf("creating the spread bucket: %+v", resp)
	}
	for _, name := range []string{"Whole", "Spread"} {
		mustInsert(t, s, "app", name, "k1", map[string]interface{}{"Email": "a@example.com"})
	}

	for _, want := range []string{"whole", "spread"} {
		name := strings.ToUpper(want[:1]) + want[1:]
		manifest, raw := dumpBucketRaw(t, s, ctx, "app", name, 1)
		if got := asWire(t, manifest)["placement"]; got != want {
			t.Fatalf("the dump manifest of %s records placement %v, want %q", name, got, want)
		}
		if resp, err := s.DeleteBucket(DeleteBucketRequest{Database: "app", Name: name}); err != nil || !resp.Success {
			t.Fatalf("dropping %s: %+v, %v", name, resp, err)
		}
		start := restoreStartFromManifest(t, manifest, name, 0, raw, false, false)
		start["placement"] = asWire(t, manifest)["placement"]
		payload, err := bson.Marshal(start)
		if err != nil {
			t.Fatalf("encoding the restore start: %v", err)
		}
		var req RestoreStartRequest
		if err := bson.Unmarshal(payload, &req); err != nil {
			t.Fatalf("decoding the restore start: %v", err)
		}
		if _, _, msg := restoreBucketRaw(t, s, ctx, req, raw, 1); msg != "" {
			t.Fatalf("restoring %s refused: %s", name, msg)
		}
		if got := describedPlacement(t, s, name); got != want {
			t.Errorf("restored %s describes placement %q, want %q", name, got, want)
		}
	}
}

// TestRestoreDropRefusedByTheCreateLeavesTheExistingBucket is the 12b
// verifier's hazard: restore --drop deleted the live bucket before its create
// ran, so a create the unique-on-spread rule refuses left no bucket at all.
// The refusal is now decided before the delete, so the bucket and a sentinel
// document survive and the answer says nothing was dropped.
func TestRestoreDropRefusedByTheCreateLeavesTheExistingBucket(t *testing.T) {
	s := newTestServer(t)
	ctx := rootContext()
	if resp, err := s.CreateDatabase("app"); err != nil || !resp.Success {
		t.Fatalf("CreateDatabase = %+v, %v", resp, err)
	}
	if resp := createPlacementBucket(t, s, "Live", BucketTypeHash, "whole"); !resp.Success {
		t.Fatalf("creating the live bucket: %+v", resp)
	}
	mustInsert(t, s, "app", "Live", "sentinel", map[string]interface{}{"n": int32(1)})

	// A dump of a spread bucket carrying a unique index: the shape a dump made
	// before the refusal existed has, built by hand.
	req := RestoreStartRequest{Database: "app", Bucket: "Live", FormatVersion: 3, StoreType: BucketTypeHash,
		Indexes:    []string{"Email"},
		IndexSpecs: []DumpIndexSpec{{ID: 0, Name: "Email_1", Fields: []string{"Email"}, Unique: true}},
		Placement:  "spread", Drop: true}
	_, _, msg := restoreBucketRaw(t, s, ctx, req, nil, 0)
	for _, want := range []string{"placement: whole", "Email", "not dropped"} {
		if !strings.Contains(msg, want) {
			t.Errorf("the refusal lacks %q: %s", want, msg)
		}
	}
	if doc := documentOf(t, s, "app", "Live", "sentinel"); doc == nil {
		t.Fatal("the sentinel document is gone: the live bucket was dropped before the refused create")
	}
	if got := describedPlacement(t, s, "Live"); got != "whole" {
		t.Errorf("the live bucket describes placement %q, want whole", got)
	}
}

// TestUniqueOnIDAloneIsAllowedOnSpread: _id_ is unique by construction, so
// declaring it unique is not a cross-part check and stays accepted.
func TestUniqueOnIDAloneIsAllowedOnSpread(t *testing.T) {
	s := newTestServer(t)
	if resp, err := s.CreateDatabase("app"); err != nil || !resp.Success {
		t.Fatalf("CreateDatabase = %+v, %v", resp, err)
	}
	resp, err := s.CreateBucket(CreateBucketRequest{Database: "app", Name: "Ids", Type: BucketTypeHash,
		Options: map[string]interface{}{"placement": "spread",
			"indices": []interface{}{map[string]interface{}{"field": "_id_", "unique": true}}}})
	if err != nil || !resp.Success || resp.Placement != "spread" {
		t.Fatalf("unique _id_ on a spread bucket answered %+v, %v, want success", resp, err)
	}
}

// TestRestoreIntoAnExistingBucketKeepsItsPlacement: a restore without --drop
// merges documents and leaves the bucket's own placement, whatever the dump
// recorded.
func TestRestoreIntoAnExistingBucketKeepsItsPlacement(t *testing.T) {
	s := newTestServer(t)
	ctx := rootContext()
	if resp, err := s.CreateDatabase("app"); err != nil || !resp.Success {
		t.Fatalf("CreateDatabase = %+v, %v", resp, err)
	}
	for _, kept := range []string{"whole", "spread"} {
		other := map[string]string{"whole": "spread", "spread": "whole"}[kept]
		name := "Into" + kept
		if resp := createPlacementBucket(t, s, name, BucketTypeHash, kept); !resp.Success {
			t.Fatalf("creating %s: %+v", name, resp)
		}
		mustInsert(t, s, "app", name, "k1", map[string]interface{}{"n": int32(1)})
		manifest, raw := dumpBucketRaw(t, s, ctx, "app", name, 1)
		req := restoreRequestFromManifest(manifest, int64(len(raw)), false)
		req.Placement = other
		if _, _, msg := restoreBucketRaw(t, s, ctx, req, raw, 1); msg != "" {
			t.Fatalf("restore into %s refused: %s", name, msg)
		}
		if got := describedPlacement(t, s, name); got != kept {
			t.Errorf("%s restored with a dump saying %s describes placement %q, want %q", name, other, got, kept)
		}
	}
}
