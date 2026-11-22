package cli

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"

	"github.com/hoarddb/hoarddb-server/ringclient"
)

// statsOfValues folds real BSON values into one field's counters the way pass 1 does: each
// value is marshalled into a document and read back as a RawValue, so the word planTypeWord
// gives it is the word a file would give it.
func statsOfValues(t *testing.T, values ...any) *planFieldStats {
	t.Helper()
	stats := newPlanFieldStats()
	for _, value := range values {
		raw, err := bson.Marshal(bson.D{{Key: "f", Value: value}})
		if err != nil {
			t.Fatalf("marshal %T: %v", value, err)
		}
		if err := stats.observe(bson.Raw(raw).Lookup("f")); err != nil {
			t.Fatalf("observe %T: %v", value, err)
		}
	}
	return stats
}

func declare(t *testing.T, stats ...*planFieldStats) []ringclient.FieldDecl {
	t.Helper()
	byName := map[string]*planFieldStats{}
	var order []string
	for i, one := range stats {
		name := fmt.Sprintf("f%d", i)
		byName[name] = one
		order = append(order, name)
	}
	fields, _, err := inferDeclaration(byName, order)
	if err != nil {
		t.Fatalf("inferDeclaration: %v", err)
	}
	return fields
}

// TestInferDeclarationMapsEveryTypeRow: one subtest per row of the table in build plan 4.3,
// each driven by real BSON values.
func TestInferDeclarationMapsEveryTypeRow(t *testing.T) {
	objectID := primitive.NewObjectID()
	decimal, err := primitive.ParseDecimal128("1.5")
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		values []any
		want   string
	}{
		{"only string", []any{"a", "b"}, "string"},
		{"only int32", []any{int32(1)}, "int"},
		{"int32 and int64 together", []any{int32(1), int64(2)}, "int"},
		{"only double", []any{1.5}, "double"},
		{"int and double", []any{int32(1), 2.5}, "number"},
		{"int and decimal128", []any{int64(1), decimal}, "number"},
		{"double and decimal128", []any{2.5, decimal}, "number"},
		{"only decimal128", []any{decimal}, "decimal"},
		{"only bool", []any{true}, "bool"},
		{"only datetime", []any{primitive.DateTime(1700000000000)}, "datetime"},
		{"only objectid", []any{objectID}, "objectid"},
		{"only binary", []any{primitive.Binary{Data: []byte{1}}}, "binary"},
		{"only object", []any{bson.D{{Key: "a", Value: 1}}}, "object"},
		{"only array", []any{bson.A{1, 2}}, "array"},
		{"only regex", []any{primitive.Regex{Pattern: "a"}}, "regex"},
		{"only bson timestamp", []any{primitive.Timestamp{T: 1, I: 1}}, "bsontimestamp"},
		{"string and int", []any{"a", int32(1)}, "any"},
		{"string and bool", []any{"a", true}, "any"},
		{"only undefined", []any{primitive.Undefined{}}, "any"},
		{"only symbol", []any{primitive.Symbol("s")}, "any"},
		{"only javascript", []any{primitive.JavaScript("1")}, "any"},
		{"only min key", []any{primitive.MinKey{}}, "any"},
		{"only max key", []any{primitive.MaxKey{}}, "any"},
		{"only null", []any{primitive.Null{}}, "any"},
		{"datetime and bool", []any{primitive.DateTime(1), false}, "any"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fields := declare(t, statsOfValues(t, tc.values...))
			if len(fields) != 1 || fields[0].Type != tc.want {
				t.Errorf("values %v declare %+v, want one field of type %q", tc.values, fields, tc.want)
			}
		})
	}
}

// TestInferDeclarationNullNeverChangesTheWord: a nullable string is a string, and an int that
// is null in some documents is an int.
func TestInferDeclarationNullNeverChangesTheWord(t *testing.T) {
	fields := declare(t,
		statsOfValues(t, "a", primitive.Null{}, "b"),
		statsOfValues(t, primitive.Null{}, int64(3)),
		statsOfValues(t, 1.5, primitive.Null{}, int32(2)),
	)
	want := []string{"string", "int", "number"}
	for i, field := range fields {
		if field.Type != want[i] {
			t.Errorf("field %d (%s) is %q, want %q: null must not change the word", i, field.Name, field.Type, want[i])
		}
	}
}

// TestInferDeclarationMixedStringAndIntIsAny: two words that no row joins fall to any, and the
// notes name the field so the user knows a type was given up.
func TestInferDeclarationMixedStringAndIntIsAny(t *testing.T) {
	stats := map[string]*planFieldStats{"code": statsOfValues(t, "a", int32(1))}
	fields, notes, err := inferDeclaration(stats, []string{"code"})
	if err != nil {
		t.Fatal(err)
	}
	if len(fields) != 1 || fields[0].Name != "code" || fields[0].Type != "any" {
		t.Fatalf("declared %+v, want code: any", fields)
	}
	if len(notes) != 1 || !strings.Contains(notes[0], `"code"`) || !strings.Contains(notes[0], "int") || !strings.Contains(notes[0], "string") {
		t.Errorf("notes %q should say field code holds both string and int values and is declared any", notes)
	}
}

// TestInferDeclarationRefusesMoreThanAThousandFields: a bucket holds at most 999 fields. The
// refusal is raised before anything is created and names the file and the fix.
func TestInferDeclarationRefusesMoreThanAThousandFields(t *testing.T) {
	build := func(count int) (map[string]*planFieldStats, []string) {
		stats := map[string]*planFieldStats{}
		var order []string
		for i := 0; i < count; i++ {
			name := fmt.Sprintf("field%04d", i)
			stats[name] = statsOfValues(t, "x")
			order = append(order, name)
		}
		return stats, order
	}
	stats, order := build(999)
	fields, _, err := inferDeclaration(stats, order)
	if err != nil || len(fields) != 999 {
		t.Fatalf("999 fields: %d declared, error %v; want 999 and no error", len(fields), err)
	}
	stats, order = build(1000)
	_, _, err = inferDeclaration(stats, order)
	var tooMany *tooManyFieldsError
	if !errors.As(err, &tooMany) {
		t.Fatalf("1,000 fields: error %v, want *tooManyFieldsError", err)
	}
	want := "users.bson has 1,000 distinct field names and a bucket holds at most 999. Nothing was created. Drop the fields you do not need from the file, or split it."
	if got := tooMany.fileText("users.bson"); got != want {
		t.Errorf("refusal text:\n got %q\nwant %q", got, want)
	}
}

// TestInferDeclarationNeverProducesAnOfField: a link is something the developer declares by
// hand, so inference never sets Of or an aggregate, and never names a store type.
func TestInferDeclarationNeverProducesAnOfField(t *testing.T) {
	objectID := primitive.NewObjectID()
	fields := declare(t,
		statsOfValues(t, objectID),
		statsOfValues(t, "507f1f77bcf86cd799439011"),
		statsOfValues(t, bson.A{"a"}),
		statsOfValues(t, bson.D{{Key: "k", Value: "v"}}),
	)
	for _, field := range fields {
		if field.Of != "" || len(field.Aggregates) != 0 || field.ExpirePath != "" || strings.HasSuffix(field.Type, "Store") {
			t.Errorf("field %+v carries a link, an aggregate, an expiry or a store type, and inference declares plain words only", field)
		}
	}
}

// TestInferDeclarationKeepsFirstSeenOrder: the field order is the order the file showed them,
// so a declaration is the same on every run.
func TestInferDeclarationKeepsFirstSeenOrder(t *testing.T) {
	stats := map[string]*planFieldStats{"b": statsOfValues(t, "x"), "a": statsOfValues(t, "x"), "c": statsOfValues(t, "x")}
	fields, _, err := inferDeclaration(stats, []string{"c", "a", "b"})
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, field := range fields {
		got = append(got, field.Name)
	}
	if strings.Join(got, ",") != "c,a,b" {
		t.Errorf("declared order %v, want c,a,b", got)
	}
}
