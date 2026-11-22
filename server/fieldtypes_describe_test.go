package server

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// describe shows a declaration (typed-fields spec §8.3 "describe bucket's
// shape", D-2, D-3; build doc TF-5 step 5.5).

func TestFieldTypeTextPrintsTheCanonicalForm(t *testing.T) {
	for _, printed := range []struct {
		declared FieldDeclaration
		want     string
	}{
		{FieldDeclaration{Type: "integer"}, "int"},
		{FieldDeclaration{Type: "Float"}, "double"},
		{FieldDeclaration{Type: "array", Elem: &FieldDeclaration{Type: "string"}}, "array<string>"},
		{FieldDeclaration{Type: "array", Elem: &FieldDeclaration{Type: "any"}}, "array"},
		{FieldDeclaration{Type: "object"}, "object"},
		{FieldDeclaration{Type: "object", Members: []FieldDeclaration{{Name: "City", Type: "string", Required: true}}}, "object { City string required }"},
		{FieldDeclaration{Type: "array", Elem: &FieldDeclaration{Type: "object", Members: []FieldDeclaration{
			{Name: "Sku", Type: "string", Required: true}, {Name: "Qty", Type: "int"},
		}}}, "array<object { Sku string required, Qty int }>"},
		{FieldDeclaration{Type: "array", Elem: &FieldDeclaration{Type: "array", Elem: &FieldDeclaration{Type: "int"}}}, "array<array<int>>"},
	} {
		if got := FieldTypeText(printed.declared); got != printed.want {
			t.Errorf("FieldTypeText(%+v) = %q, want %q", printed.declared, got, printed.want)
		}
	}
}

// userFieldsDescribed is how describe shows userFieldsOption's declaration.
func userFieldsDescribed() []FieldInfo {
	return []FieldInfo{
		{Name: "Name", Type: "string", Required: true},
		{Name: "Age", Type: "int"},
		{Name: "Active", Type: "bool"},
		{Name: "Address", Type: "object { City string required, Geo object { Lat double } }"},
		{Name: "Tags", Type: "array<string>"},
		{Name: "Any", Type: "array"},
		{Name: "Empty", Type: "object"},
	}
}

func TestDescribeBucketListsDeclaredFieldsAndMode(t *testing.T) {
	s := newTestServer(t)
	createTypedBucket(t, s, "User", userFieldsOption(), "warn")
	createTypedBucket(t, s, "Strict", bson.A{bson.M{"name": "Age", "type": "int"}}, "")
	createTestBucket(t, s, "app", "Plain", BucketTypeHash)

	described, err := s.BucketStats(BucketStatsRequest{Database: "app", Bucket: "User"})
	if err != nil {
		t.Fatalf("describing User: %v", err)
	}
	if !reflect.DeepEqual(described.Fields, userFieldsDescribed()) || described.FieldTypes != "warn" {
		t.Errorf("describe shows %+v in mode %q; want %+v in warn", described.Fields, described.FieldTypes, userFieldsDescribed())
	}
	strict, err := s.BucketStats(BucketStatsRequest{Database: "app", Bucket: "Strict"})
	if err != nil || strict.FieldTypes != "strict" || len(strict.Fields) != 1 {
		t.Errorf("describing a strict bucket: %+v, %v; want one field in strict", strict, err)
	}
	plain, err := s.BucketStats(BucketStatsRequest{Database: "app", Bucket: "Plain"})
	if err != nil || plain.Fields != nil || plain.FieldTypes != "" {
		t.Errorf("describing an undeclared bucket: fields %+v, mode %q (%v); want neither", plain.Fields, plain.FieldTypes, err)
	}
}

// TestWireTreeRoundTripsThroughCreateAlterAndDescribe: the create and describe
// clauses (spec §8.3). The tree a client builds with FieldDeclaration crosses
// the wire under its tags, and describe answers under FieldInfo's and the
// response's explicit tags, in BSON and in JSON. TF-8 adds the alter clause,
// TF-10 a store entry's types.
func TestWireTreeRoundTripsThroughCreateAlterAndDescribe(t *testing.T) {
	tc := newTestCluster(t, 1, 1)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	declared := []FieldDeclaration{
		{Name: "Name", Type: "string", Required: true},
		{Name: "Lines", Type: "array", Elem: &FieldDeclaration{Type: "object", Members: []FieldDeclaration{{Name: "Qty", Type: "int32"}}}},
	}
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: "orders", Type: BucketTypeHash,
		Options: map[string]interface{}{"fields": declared, "field_types": "off"}})

	answer := tc.frame(0, transport.OpBucketStats, BucketStatsRequest{Database: testDatabase, Bucket: "orders"})
	if answer.Opcode == transport.OpError {
		t.Fatalf("describe answered an error: %s", answer.Payload)
	}
	raw := bson.Raw(answer.Payload)
	if mode, ok := raw.Lookup("field_types").StringValueOK(); !ok || mode != "off" {
		t.Errorf("the describe answer's field_types is %v; want \"off\" under that key", raw.Lookup("field_types"))
	}
	fields, ok := raw.Lookup("fields").ArrayOK()
	if !ok {
		t.Fatalf("the describe answer has no fields array: %s", raw)
	}
	first, _ := fields.Index(0).Value().DocumentOK()
	second, _ := fields.Index(1).Value().DocumentOK()
	if name, _ := first.Lookup("name").StringValueOK(); name != "Name" {
		t.Errorf("the first field's name key reads %q", name)
	}
	if required, _ := first.Lookup("required").BooleanOK(); !required {
		t.Error("the first field's required key is not true")
	}
	if typ, _ := second.Lookup("type").StringValueOK(); typ != "array<object { Qty int32 }>" {
		t.Errorf("the second field's type key reads %q", typ)
	}

	var described BucketStatsResponse
	if err := bson.Unmarshal(answer.Payload, &described); err != nil {
		t.Fatalf("decoding the describe answer: %v", err)
	}
	encoded, err := json.Marshal(described)
	if err != nil {
		t.Fatalf("encoding the answer as JSON: %v", err)
	}
	var asJSON map[string]interface{}
	if err := json.Unmarshal(encoded, &asJSON); err != nil {
		t.Fatalf("decoding the JSON: %v", err)
	}
	if asJSON["field_types"] != "off" {
		t.Errorf("as JSON, describe's field_types is %v: %s", asJSON["field_types"], encoded)
	}
	list, _ := asJSON["fields"].([]interface{})
	if len(list) != 2 {
		t.Fatalf("as JSON, describe's fields are %v", asJSON["fields"])
	}
	if lines, _ := list[1].(map[string]interface{}); lines["name"] != "Lines" || lines["type"] != "array<object { Qty int32 }>" {
		t.Errorf("as JSON, the second field reads %v", list[1])
	}
}
