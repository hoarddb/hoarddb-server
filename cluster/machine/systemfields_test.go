package machine

import "testing"

// TestSystemWritableAdmitsEveryOptionalFieldOfTheBucketsTableAndNothingElse: [GUARD the set is
// the whole mechanism. When two features land, a bucket that carries both optional fields (the
// aggregates, 12, and the references, 13) must pass: the second to merge adds its id to the
// set and extends this test.]
func TestSystemWritableAdmitsEveryOptionalFieldOfTheBucketsTableAndNothingElse(t *testing.T) {
	for id := range systemOptionalFields {
		if !systemWritable(SystemBucketID, id) {
			t.Errorf("optional field %d is not admitted on the _buckets bucket", id)
		}
		if systemWritable(SystemBucketID-1, id) {
			t.Errorf("optional field %d is admitted on another bucket", id)
		}
	}
	if !systemWritable(SystemBucketID, SystemOptionalReferences) {
		t.Error("the references field is not admitted")
	}
	for _, id := range []uint16{0, 1, 3, 9, 14, 99} {
		if systemWritable(SystemBucketID, id) {
			t.Errorf("field %d is admitted without being listed", id)
		}
	}
}

// TestFoldPrunesTheReferencesOfDroppedFields: [GUARD the fold that makes a drop effective
// deletes the dropped field's reference row; none left means none].
func TestFoldPrunesTheReferencesOfDroppedFields(t *testing.T) {
	table := FieldTable{NextFieldID: 4, Entries: []FieldEntry{{ID: 1, Type: "any", Name: "_id_"}, {ID: 2, Type: "string", Name: "Author"}, {ID: 3, Type: "string", Name: "Editor"}}}
	pending := FieldTable{NextFieldID: 4, Entries: []FieldEntry{{ID: 1, Type: "any", Name: "_id_"}, {ID: 3, Type: "string", Name: "Editor"}}}
	decl := Declaration{Fields: table, Version: 1, Pending: &Pending{Version: 2, Table: pending},
		References: []Reference{{FieldID: 2, BucketID: 7}, {FieldID: 3, BucketID: 8}}}
	folded := Fold(decl)
	if len(folded.References) != 1 || folded.References[0].FieldID != 3 {
		t.Fatalf("references after the fold = %+v, want only field 3", folded.References)
	}
	last := Fold(Declaration{Fields: table, Pending: &Pending{Version: 2, Table: FieldTable{NextFieldID: 4, Entries: pending.Entries[:1]}}, References: decl.References})
	if last.References != nil {
		t.Fatalf("all references pruned left %+v, want none", last.References)
	}
	if kept := PruneReferences(decl.References, table); len(kept) != 2 {
		t.Fatalf("nothing to prune changed the list: %+v", kept)
	}
}
