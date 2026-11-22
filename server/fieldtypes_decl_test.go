package server

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

// The declaration validator (typed-fields spec §4.7, E-9; build doc TF-5 step
// 5.1). One function decides what a declaration may say, for the create, the
// attach, the alter and the CLI alike.

// typedValidation is a validation of fields on a hash bucket "User" in "app".
func typedValidation(fields ...FieldDeclaration) FieldValidation {
	return FieldValidation{Database: "app", Bucket: "User", BucketType: BucketTypeHash, Fields: fields}
}

// refusedWith asserts that v is refused, as a declaration refusal, with a
// message that contains every part.
func refusedWith(t *testing.T, v FieldValidation, parts ...string) {
	t.Helper()
	err := ValidateFieldDeclarations(v)
	if err == nil {
		t.Fatalf("a declaration of %d top-level fields was accepted; want it refused saying %q", len(v.Fields), parts)
	}
	if !errors.Is(err, ErrFieldDeclarationInvalid) {
		t.Errorf("the refusal %q is not a declaration refusal (ErrFieldDeclarationInvalid)", err)
	}
	for _, part := range parts {
		if !strings.Contains(err.Error(), part) {
			t.Errorf("the refusal %q does not say %q", err, part)
		}
	}
}

// accepted asserts that v is accepted.
func accepted(t *testing.T, v FieldValidation) {
	t.Helper()
	if err := ValidateFieldDeclarations(v); err != nil {
		t.Fatalf("a declaration of %d top-level fields was refused: %v", len(v.Fields), err)
	}
}

func TestUnknownTypeWordIsRefusedListingTheWords(t *testing.T) {
	refusedWith(t, typedValidation(FieldDeclaration{Name: "Age", Type: "integr"}),
		`"integr" is not a field type`, `field "Age"`,
		"string, int32, int (or integer, int64), double (or float), decimal, number, bool (or boolean), "+
			"datetime (or date, timestamp), bsontimestamp, binary, uuid, objectid, object, array, regex, null, any.")
	// A deprecated BSON type is not declarable either (spec §3.1).
	refusedWith(t, typedValidation(FieldDeclaration{Name: "Code", Type: "javascript"}), `"javascript" is not a field type`)
	// Every word and synonym of spec §3.1 is accepted, in any case.
	for word := range wordKinds {
		accepted(t, typedValidation(FieldDeclaration{Name: "F", Type: strings.ToUpper(word)}))
	}
}

func TestIDIsNotDeclarable(t *testing.T) {
	refusedWith(t, typedValidation(FieldDeclaration{Name: "Name", Type: "string"}, FieldDeclaration{Name: "_id_", Type: "uuid"}),
		"_id_ is the document key", `Remove this entry ("_id_ uuid")`, "nothing else changes")
	// Inside an object, the reserved words are ordinary member names.
	accepted(t, typedValidation(FieldDeclaration{Name: "Ref", Type: "object", Members: []FieldDeclaration{
		{Name: "_id_", Type: "string"}, {Name: "type", Type: "string"}, {Name: "field_types", Type: "string"},
	}}))
	for _, reserved := range []string{"type", "indices", "acknowledge", "replication_factor", "field_types"} {
		refusedWith(t, typedValidation(FieldDeclaration{Name: reserved, Type: "string"}), fmt.Sprintf("%q", reserved), "top-level field")
	}
}

func TestBlobBucketCannotDeclareTypedFields(t *testing.T) {
	v := typedValidation(FieldDeclaration{Name: "Size", Type: "int"})
	v.BucketType = BucketTypeBlob
	refusedWith(t, v, `Bucket "User" is a blob bucket`, "cannot declare typed fields")
	for _, documents := range []BucketType{BucketTypeHash, BucketTypeBTree, BucketTypeFIFO, BucketTypeLIFO, BucketTypeHeap} {
		v.BucketType = documents
		accepted(t, v)
	}
}

func TestDottedFieldNameIsRefused(t *testing.T) {
	refusedWith(t, typedValidation(FieldDeclaration{Name: "Address.City", Type: "string"}),
		`"Address.City" cannot name a field`, "for example AddressCity")
	refusedWith(t, typedValidation(FieldDeclaration{Name: "Address", Type: "object", Members: []FieldDeclaration{{Name: "Geo.Lat", Type: "double"}}}),
		`"Geo.Lat" cannot name a field`)
	for _, name := range []string{"", "$set", "nul\x00here"} {
		refusedWith(t, typedValidation(FieldDeclaration{Name: name, Type: "string"}), "cannot name a field")
	}
}

func TestBracketInADeclaredNameIsRefused(t *testing.T) {
	for _, name := range []string{"Lines[3]", "a]b", "[x"} {
		refusedWith(t, typedValidation(FieldDeclaration{Name: name, Type: "int"}), fmt.Sprintf("%q cannot name a field", name), `"[", "]"`)
	}
}

// fieldsNamed returns count fields F0, F1, … of type string.
func fieldsNamed(count int) []FieldDeclaration {
	fields := make([]FieldDeclaration, 0, count)
	for i := range count {
		fields = append(fields, FieldDeclaration{Name: fmt.Sprintf("F%d", i), Type: "string"})
	}
	return fields
}

func TestMoreThan256TypedFieldsIsRefused(t *testing.T) {
	accepted(t, typedValidation(fieldsNamed(256)...))
	refusedWith(t, typedValidation(fieldsNamed(257)...), "declares 257 typed fields on one level; the limit is 256")
}

func TestMoreThan256MembersIsRefused(t *testing.T) {
	accepted(t, typedValidation(FieldDeclaration{Name: "Wide", Type: "object", Members: fieldsNamed(256)}))
	refusedWith(t, typedValidation(FieldDeclaration{Name: "Wide", Type: "object", Members: fieldsNamed(257)}),
		"declares 257 typed fields on one level; the limit is 256")
}

// nestedArrays is a field whose type is levels array<…> around an int.
func nestedArrays(levels int) FieldDeclaration {
	elem := FieldDeclaration{Type: "int"}
	for range levels - 1 {
		inner := elem
		elem = FieldDeclaration{Type: "array", Elem: &inner}
	}
	return FieldDeclaration{Name: "Deep", Type: "array", Elem: &elem}
}

func TestNestingDeeperThanEightIsRefused(t *testing.T) {
	accepted(t, typedValidation(nestedArrays(8)))
	refusedWith(t, typedValidation(nestedArrays(9)), "levels deep inside \"Deep\"", "nests at most 8 levels")
	// Objects count as arrays do: eight object { … } levels are accepted.
	leaf := FieldDeclaration{Name: "L", Type: "int"}
	for level := 8; level >= 1; level-- {
		leaf = FieldDeclaration{Name: fmt.Sprintf("O%d", level), Type: "object", Members: []FieldDeclaration{leaf}}
	}
	accepted(t, typedValidation(leaf))
	refusedWith(t, typedValidation(FieldDeclaration{Name: "O0", Type: "object", Members: []FieldDeclaration{leaf}}),
		"9 levels deep inside \"O0\"")
}

func TestMoreThan1024DeclaredNodesIsRefused(t *testing.T) {
	// Four objects of 255 members are 4 + 1,020 nodes.
	var fields []FieldDeclaration
	for i := range 4 {
		fields = append(fields, FieldDeclaration{Name: fmt.Sprintf("O%d", i), Type: "object", Members: fieldsNamed(255)})
	}
	accepted(t, typedValidation(fields...))
	fields = append(fields, FieldDeclaration{Name: "One", Type: "int"})
	refusedWith(t, typedValidation(fields...), "declares 1025 typed nodes", "the limit is 1,024")
}

func TestStoreKindInsideAnObjectIsRefused(t *testing.T) {
	refusedWith(t, typedValidation(FieldDeclaration{Name: "Address", Type: "object", Members: []FieldDeclaration{{Name: "Visits", Type: "fifoStore"}}}),
		`"Visits" inside "Address" is declared fifoStore`, "a store belongs to a document")
	refusedWith(t, typedValidation(FieldDeclaration{Name: "Lines", Type: "array", Elem: &FieldDeclaration{Type: "HASHSTORE"}}),
		"is declared hashStore")
}

func TestUniqueOnAMemberOrArrayIsRefused(t *testing.T) {
	address := FieldDeclaration{Name: "Address", Type: "object", Members: []FieldDeclaration{{Name: "City", Type: "string"}}}
	tags := FieldDeclaration{Name: "Tags", Type: "array", Elem: &FieldDeclaration{Type: "string"}}
	for _, unique := range []struct{ name, on string }{
		{"Address.City", "an object member"},
		{"Tags", "an array"},
		{"Tags[0]", "an array"},
	} {
		v := typedValidation(address, tags)
		v.Unique = []string{unique.name}
		refusedWith(t, v, fmt.Sprintf("%q declares unique on %s", unique.name, unique.on), "only to a top-level field")
	}
}

func TestUniqueOnAnUnencodableTypeIsRefused(t *testing.T) {
	for _, word := range []string{"objectid", "decimal", "regex", "bsontimestamp", "number"} {
		v := typedValidation(FieldDeclaration{Name: "Ref", Type: word})
		v.Unique = []string{"Ref"}
		refusedWith(t, v, fmt.Sprintf(`"Ref" is declared %s unique, and an index cannot hold %s values`, word, word))
	}
	for _, word := range []string{"string", "int32", "int", "integer", "double", "bool", "datetime", "binary", "uuid"} {
		v := typedValidation(FieldDeclaration{Name: "Key", Type: word})
		v.Unique = []string{"Key", "Undeclared"}
		accepted(t, v)
	}
}

func TestDuplicateFieldNameIsRefused(t *testing.T) {
	refusedWith(t, typedValidation(FieldDeclaration{Name: "Age", Type: "int"}, FieldDeclaration{Name: "Age", Type: "string"}),
		`Field "Age" is declared twice`)
	v := typedValidation(FieldDeclaration{Name: "Comments", Type: "string"})
	v.Stores = []string{"Comments"}
	refusedWith(t, v, `"Comments" is declared on bucket "User" both as a typed field and as a store field`)
}

func TestFieldTypesModeIsStrictWarnOrOff(t *testing.T) {
	for _, mode := range []string{"", "strict", "warn", "off", "WARN"} {
		v := typedValidation(FieldDeclaration{Name: "Age", Type: "int"})
		v.Mode = mode
		accepted(t, v)
	}
	// A mode may be set before any field is declared (spec §4.8).
	accepted(t, FieldValidation{Database: "app", Bucket: "User", BucketType: BucketTypeHash, Mode: "warn"})
	v := typedValidation(FieldDeclaration{Name: "Age", Type: "int"})
	v.Mode = "lenient"
	refusedWith(t, v, `field_types "lenient" is not a mode`, "strict", "warn", "off")
}

func TestMembersOnlyOnObjectAndElementOnlyOnArray(t *testing.T) {
	refusedWith(t, typedValidation(FieldDeclaration{Name: "Age", Type: "int", Members: []FieldDeclaration{{Name: "X", Type: "int"}}}),
		`Field "Age" on bucket "User" is declared int with members`)
	refusedWith(t, typedValidation(FieldDeclaration{Name: "Age", Type: "object", Elem: &FieldDeclaration{Type: "int"}}),
		`is declared object with an element type`)
}

// SA-13: field_types is reserved for a store field too, as every other option
// word of the declaration body is.
func TestFieldTypesCannotNameAStoreField(t *testing.T) {
	err := ValidateStoreDeclarations(StoreValidation{Database: "app", Bucket: "User", BucketType: BucketTypeHash,
		Added: []StoreDeclaration{{Name: "field_types", Engine: "fifoStore"}}})
	if err == nil || !strings.Contains(err.Error(), `"field_types" is reserved`) {
		t.Fatalf("a store field named field_types: %v; want it refused as reserved", err)
	}
}

// TestDeclarationRefusalTextsAreExact pins E-9's texts word for word (spec
// §6.2), each with its placeholders filled.
func TestDeclarationRefusalTextsAreExact(t *testing.T) {
	address := FieldDeclaration{Name: "Address", Type: "object", Members: []FieldDeclaration{{Name: "City", Type: "string"}}}
	cases := []struct {
		name string
		v    FieldValidation
		want string
	}{
		{"unknown word", typedValidation(FieldDeclaration{Name: "Age", Type: "integr"}),
			`"integr" is not a field type, so field "Age" on bucket "User" cannot be declared with it. The field types are: string, int32, int (or integer, int64), double (or float), decimal, number, bool (or boolean), datetime (or date, timestamp), bsontimestamp, binary, uuid, objectid, object, array, regex, null, any.`},
		{"_id_", typedValidation(FieldDeclaration{Name: "_id_", Type: "uuid", Required: true}),
			`_id_ is the document key; hoardDB always stores it as a string and creates it when a document has none, so it is never declared. Remove this entry ("_id_ uuid required") from the declaration; nothing else changes.`},
		{"name", typedValidation(FieldDeclaration{Name: "Lines[3]", Type: "int"}),
			`"Lines[3]" cannot name a field: a field name is non-empty, contains no ".", "[", "]" or NUL, and does not start with "$" ("." and brackets are how a path names a value inside a value). Name it like a document field, for example Lines3.`},
		{"blob", FieldValidation{Database: "app", Bucket: "Photos", BucketType: BucketTypeBlob, Fields: []FieldDeclaration{{Name: "Size", Type: "int"}}},
			`Bucket "Photos" is a blob bucket, and a blob's entries are content, not documents with fields, so it cannot declare typed fields. Declare them on a hash bucket that references the blob by key.`},
		{"depth", typedValidation(nestedArrays(9)),
			`"Deep[][][][][][][][][]" is declared 9 levels deep inside "Deep"; a field's type nests at most 8 levels (each object { … } and array<…> is one). Give the inner part its own top-level field, or its own bucket referenced by key.`},
		{"nodes", typedValidation(append(fieldsNamed(1), []FieldDeclaration{
			{Name: "A", Type: "object", Members: fieldsNamed(255)}, {Name: "B", Type: "object", Members: fieldsNamed(255)},
			{Name: "C", Type: "object", Members: fieldsNamed(255)}, {Name: "D", Type: "object", Members: fieldsNamed(255)},
		}...)...),
			`Bucket "User" declares 1025 typed nodes (fields, members and element types at every level); the limit is 1,024. Declare the rarely checked parts as object or array without braces.`},
		{"store kind", typedValidation(FieldDeclaration{Name: "Address", Type: "object", Members: []FieldDeclaration{{Name: "Visits", Type: "fifoStore"}}}),
			`"Visits" inside "Address" is declared fifoStore; a store belongs to a document, and an object inside a document is not one. Declare Visits as a store field of the bucket, or as array<object { … }> inside Address.`},
		{"unique on a member", FieldValidation{Database: "app", Bucket: "User", BucketType: BucketTypeHash, Fields: []FieldDeclaration{address}, Unique: []string{"Address.City"}},
			`"Address.City" declares unique on an object member; unique applies only to a top-level field in this release.`},
		{"unique unencodable", FieldValidation{Database: "app", Bucket: "User", BucketType: BucketTypeHash, Fields: []FieldDeclaration{{Name: "Ref", Type: "objectid"}}, Unique: []string{"Ref"}},
			`"Ref" is declared objectid unique, and an index cannot hold objectid values, so every write carrying one would be refused. unique applies to string, int32, int, double, bool, datetime, binary and uuid fields. Drop unique, or declare the field one of those types.`},
		{"256", typedValidation(fieldsNamed(257)...),
			`Bucket "User" declares 257 typed fields on one level; the limit is 256. Group related values in an object field.`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateFieldDeclarations(tc.v)
			if err == nil {
				t.Fatalf("accepted; want %q", tc.want)
			}
			if err.Error() != tc.want {
				t.Errorf("the refusal reads\n  %q\nwant\n  %q", err.Error(), tc.want)
			}
		})
	}
}

// TestFieldNameLongerThan255BytesIsRefused is the coordinator's decision of
// 2026-10-05 (build doc TF-6): a field name, at every level, is at most 255
// bytes of UTF-8, as a bucket or database name is (catalog GC CG-2), so spec
// §9.3's bound on a declaration's size holds by construction. The refusal
// repeats at most 64 bytes of the name, so it does not grow with it.
func TestFieldNameLongerThan255BytesIsRefused(t *testing.T) {
	atLimit := strings.Repeat("a", 255)
	accepted(t, typedValidation(FieldDeclaration{Name: atLimit, Type: "string"}))
	accepted(t, typedValidation(FieldDeclaration{Name: "Address", Type: "object", Members: []FieldDeclaration{{Name: atLimit, Type: "string"}}}))
	// The limit counts bytes, not characters: 127 two-byte runes are 254 bytes.
	accepted(t, typedValidation(FieldDeclaration{Name: strings.Repeat("é", 127), Type: "string"}))

	overLimit := strings.Repeat("a", 256)
	for _, v := range []FieldValidation{
		typedValidation(FieldDeclaration{Name: overLimit, Type: "string"}),
		typedValidation(FieldDeclaration{Name: "Address", Type: "object", Members: []FieldDeclaration{{Name: overLimit, Type: "string"}}}),
	} {
		refusedWith(t, v, "is too long: it is 256 bytes and the limit is 255", `bucket "User"`, "Use a shorter field name")
		if err := ValidateFieldDeclarations(v); err != nil && strings.Contains(err.Error(), overLimit[:65]) {
			t.Errorf("the refusal repeats more than 64 bytes of the name: %q", err)
		}
	}
	refusedWith(t, typedValidation(FieldDeclaration{Name: strings.Repeat("é", 128), Type: "string"}), "it is 256 bytes and the limit is 255")
}
