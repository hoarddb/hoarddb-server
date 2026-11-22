package server

import (
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// Create, attach and interning of a declaration (typed-fields spec §8.3,
// §8.4; build doc TF-5 step 5.3).

// userFieldsOption is a create's Options["fields"] as a wire client sends
// it: synonyms and mixed case included, which the server canonicalises.
func userFieldsOption() bson.A {
	return bson.A{
		bson.M{"name": "Name", "type": "String", "required": true},
		bson.M{"name": "Age", "type": "integer"},
		bson.M{"name": "Active", "type": "boolean"},
		bson.M{"name": "Address", "type": "object", "members": bson.A{
			bson.M{"name": "City", "type": "string", "required": true},
			bson.M{"name": "Geo", "type": "object", "members": bson.A{bson.M{"name": "Lat", "type": "float"}}},
		}},
		bson.M{"name": "Tags", "type": "array", "elem": bson.M{"type": "string"}},
		bson.M{"name": "Any", "type": "array", "elem": bson.M{"type": "any"}},
		bson.M{"name": "Empty", "type": "object", "members": bson.A{}},
	}
}

// userFieldsCanonical is what the catalog records for userFieldsOption.
func userFieldsCanonical() []catalogField {
	return []catalogField{
		{Name: "Name", catalogType: catalogType{Type: "string"}, Required: true},
		{Name: "Age", catalogType: catalogType{Type: "int"}},
		{Name: "Active", catalogType: catalogType{Type: "bool"}},
		{Name: "Address", catalogType: catalogType{Type: "object", Members: []catalogField{
			{Name: "City", catalogType: catalogType{Type: "string"}, Required: true},
			{Name: "Geo", catalogType: catalogType{Type: "object", Members: []catalogField{{Name: "Lat", catalogType: catalogType{Type: "double"}}}}},
		}}},
		{Name: "Tags", catalogType: catalogType{Type: "array", Elem: &catalogType{Type: "string"}}},
		{Name: "Any", catalogType: catalogType{Type: "array"}},
		{Name: "Empty", catalogType: catalogType{Type: "object"}},
	}
}

// createTypedBucket creates bucket in database "app" with the given fields
// option and mode, and fails the test on a refusal.
func createTypedBucket(t *testing.T, s *Server, bucket string, fields bson.A, mode string) {
	t.Helper()
	if _, err := s.CreateDatabase("app"); err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}
	options := map[string]interface{}{"fields": fields}
	if mode != "" {
		options["field_types"] = mode
	}
	resp, err := s.CreateBucket(CreateBucketRequest{Database: "app", Name: bucket, Type: BucketTypeHash, Options: options})
	if err != nil || !resp.Success {
		t.Fatalf("creating %s with typed fields: %+v, %v", bucket, resp, err)
	}
}

func TestCreateRecordsDeclaredFieldsInTheCatalog(t *testing.T) {
	s := newTestServer(t)
	createTypedBucket(t, s, "User", userFieldsOption(), "WARN")
	entry, _, err := s.readCatalogEntry("app", "User")
	if err != nil {
		t.Fatalf("reading the entry: %v", err)
	}
	if !reflect.DeepEqual(entry.Fields, userFieldsCanonical()) {
		t.Errorf("the catalog records fields\n  %+v\nwant\n  %+v", entry.Fields, userFieldsCanonical())
	}
	if entry.FieldTypes != "warn" {
		t.Errorf("the catalog records field_types %q, want warn", entry.FieldTypes)
	}

	// strict is the default and is not recorded (spec §8.1).
	createTypedBucket(t, s, "Strict", bson.A{bson.M{"name": "Age", "type": "int"}}, "strict")
	if entry, _, _ := s.readCatalogEntry("app", "Strict"); entry.FieldTypes != "" || len(entry.Fields) != 1 {
		t.Errorf("a strict bucket records field_types %q and %d fields; want none and 1", entry.FieldTypes, len(entry.Fields))
	}
	// A mode may be set before any field is declared (spec §4.8).
	createTypedBucket(t, s, "ModeOnly", nil, "off")
	if entry, _, _ := s.readCatalogEntry("app", "ModeOnly"); entry.FieldTypes != "off" || entry.Fields != nil {
		t.Errorf("a mode-only create records field_types %q and fields %+v; want off and none", entry.FieldTypes, entry.Fields)
	}
}

func TestCreateRefusesAnInvalidDeclaration(t *testing.T) {
	s := newTestServer(t)
	if _, err := s.CreateDatabase("app"); err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}
	for _, refused := range []struct {
		name    string
		options map[string]interface{}
		says    string
	}{
		{"unknown word", map[string]interface{}{"fields": bson.A{bson.M{"name": "Age", "type": "integr"}}}, `"integr" is not a field type`},
		{"unknown key", map[string]interface{}{"fields": bson.A{bson.M{"name": "Age", "type": "int", "default": 3}}}, `"default"`},
		{"not a list", map[string]interface{}{"fields": "Age int"}, `fields on bucket "Refused" must be a list`},
		{"bad mode", map[string]interface{}{"field_types": "lenient"}, `field_types "lenient" is not a mode`},
		{"store and field", map[string]interface{}{"fields": bson.A{bson.M{"name": "Comments", "type": "string"}},
			"stores": bson.A{bson.M{"name": "Comments", "engine": "fifoStore"}}}, "both as a typed field and as a store field"},
		{"unique on an objectid", map[string]interface{}{"fields": bson.A{bson.M{"name": "Ref", "type": "objectid"}},
			"placement": "whole", "indices": bson.A{bson.M{"fields": bson.A{"Ref"}, "unique": true}}}, "an index cannot hold objectid values"},
	} {
		resp, err := s.CreateBucket(CreateBucketRequest{Database: "app", Name: "Refused", Type: BucketTypeHash, Options: refused.options})
		if err != nil || resp.Success || !strings.Contains(resp.Message, refused.says) {
			t.Errorf("%s: the create answered %+v, %v; want refused saying %q", refused.name, resp, err, refused.says)
		}
		if entry, catalogued, _ := s.readCatalogEntry("app", "Refused"); catalogued {
			t.Fatalf("%s: the refused create recorded %+v", refused.name, entry)
		}
	}
	resp, err := s.CreateBucket(CreateBucketRequest{Database: "app", Name: "Photos", Type: BucketTypeBlob,
		Options: map[string]interface{}{"fields": bson.A{bson.M{"name": "Size", "type": "int"}}}})
	if err != nil || resp.Success || !strings.Contains(resp.Message, "is a blob bucket") {
		t.Errorf("a typed blob create answered %+v, %v; want refused", resp, err)
	}
}

func TestDeclarationSurvivesRestart(t *testing.T) {
	dataDir := t.TempDir()
	cs := startCatalogServerAt(t, dataDir)
	createTypedBucket(t, cs.core, "User", userFieldsOption(), "warn")
	before := rawCatalogEntry(t, cs.core, "app", "User")

	cs = restartCatalog(t, cs, dataDir)
	holder := servedBucket(t, cs.core, "app", "User").declaredFieldTypes()
	if holder == nil {
		t.Fatal("after a restart the bucket declares no typed fields")
	}
	fields, mode := holder.catalogForm()
	if !reflect.DeepEqual(fields, userFieldsCanonical()) || mode != "warn" {
		t.Errorf("after a restart the bucket declares %+v in mode %q; want %+v in warn", fields, mode, userFieldsCanonical())
	}
	if after := rawCatalogEntry(t, cs.core, "app", "User"); !reflect.DeepEqual(before, after) {
		t.Errorf("the start rewrote the declared bucket's entry")
	}
}

func TestIdenticalDeclarationsShareOneSet(t *testing.T) {
	s := newTestServer(t)
	createTypedBucket(t, s, "Alice", userFieldsOption(), "")
	createTypedBucket(t, s, "Bob", userFieldsOption(), "")
	createTypedBucket(t, s, "Carol", userFieldsOption(), "warn")
	alice, bob, carol := lookupBucket(s, "app", "Alice").declaredFieldTypes(), lookupBucket(s, "app", "Bob").declaredFieldTypes(),
		lookupBucket(s, "app", "Carol").declaredFieldTypes()
	if alice == nil || bob == nil || carol == nil {
		t.Fatalf("a typed bucket has no holder: Alice %v, Bob %v, Carol %v", alice, bob, carol)
	}
	if alice.schema != bob.schema {
		t.Errorf("two buckets that declare the same fields hold two schemas (%p, %p); want one shared", alice.schema, bob.schema)
	}
	if alice.schema == carol.schema {
		t.Error("a bucket in warn mode shares the strict buckets' schema: the mode is part of what is interned")
	}
}

func TestInternTableReleasesAnOrphanedSchema(t *testing.T) {
	s := newTestServer(t)
	createTypedBucket(t, s, "Kept", bson.A{bson.M{"name": "Kept", "type": "int"}}, "")
	createTypedBucket(t, s, "Dropped", bson.A{bson.M{"name": "Orphan", "type": "string"}}, "")
	if held := s.schemas.size(); held != 2 {
		t.Fatalf("the intern table holds %d schemas for two distinct declarations, want 2", held)
	}
	if resp, err := s.DeleteBucket(DeleteBucketRequest{Database: "app", Name: "Dropped"}); err != nil || !resp.Success {
		t.Fatalf("dropping Dropped: %+v, %v", resp, err)
	}
	runtime.GC()
	runtime.GC()
	createTypedBucket(t, s, "Later", bson.A{bson.M{"name": "Later", "type": "bool"}}, "")
	if held := s.schemas.size(); held != 2 {
		t.Errorf("after the drop, a collection and a new create, the intern table holds %d schemas; want 2 (Kept and Later)", held)
	}
}
