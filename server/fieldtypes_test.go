package server

import (
	"math"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"
)

// declarableWords is typed-fields spec §3.1's table: every canonical word and
// every synonym the CLI accepts today, with the kind each means.
var declarableWords = []struct {
	word      string
	kind      fieldKind
	canonical string
}{
	{"string", kindString, "string"},
	{"int32", kindInt32, "int32"},
	{"int", kindInt, "int"},
	{"integer", kindInt, "int"},
	{"int64", kindInt, "int"},
	{"double", kindDouble, "double"},
	{"float", kindDouble, "double"},
	{"decimal", kindDecimal, "decimal"},
	{"number", kindNumber, "number"},
	{"bool", kindBool, "bool"},
	{"boolean", kindBool, "bool"},
	{"datetime", kindDatetime, "datetime"},
	{"date", kindDatetime, "datetime"},
	{"timestamp", kindDatetime, "datetime"},
	{"bsontimestamp", kindBSONTimestamp, "bsontimestamp"},
	{"binary", kindBinary, "binary"},
	{"uuid", kindUUID, "uuid"},
	{"objectid", kindObjectID, "objectid"},
	{"object", kindObject, "object"},
	{"array", kindArray, "array"},
	{"regex", kindRegex, "regex"},
	{"null", kindNull, "null"},
	{"any", kindAny, "any"},
}

// canonicalWords are the words the catalog records, one per kind.
var canonicalWords = []string{
	"string", "int32", "int", "double", "decimal", "number", "bool", "datetime", "bsontimestamp", "binary",
	"uuid", "objectid", "object", "array", "regex", "null", "any",
}

// TestEveryDeclarableWordMapsToItsBSONTypes: the §3.1 table, word by word,
// synonyms and capitals included. A word outside the table is not a type.
func TestEveryDeclarableWordMapsToItsBSONTypes(t *testing.T) {
	for _, row := range declarableWords {
		for _, spelling := range []string{row.word, upperFirst(row.word), toUpper(row.word)} {
			kind, ok := canonicalWord(spelling)
			if !ok || kind != row.kind {
				t.Errorf("canonicalWord(%q) = %v, %v; want %v, true", spelling, kind, ok, row.kind)
			}
		}
		if got := row.kind.String(); got != row.canonical {
			t.Errorf("%q: kind prints %q, want the canonical word %q", row.word, got, row.canonical)
		}
	}
	for _, notAWord := range []string{"", "text", "int128", "bson", "double ", "undefined", "javascript", "symbol", "minkey", "object {}", "array<string>"} {
		if kind, ok := canonicalWord(notAWord); ok {
			t.Errorf("canonicalWord(%q) = %v, true; want it refused", notAWord, kind)
		}
	}
	if kindAny != 0 {
		t.Errorf("kindAny = %d; it must be the zero value so an unset kind accepts everything", kindAny)
	}
}

func upperFirst(word string) string {
	return string(word[0]-'a'+'A') + word[1:]
}

func toUpper(word string) string {
	upper := []byte(word)
	for index, letter := range upper {
		if letter >= 'a' && letter <= 'z' {
			upper[index] = letter - 'a' + 'A'
		}
	}
	return string(upper)
}

// everyBSONType holds one value per BSON type as a wire decode yields it
// (and the deprecated ones as the codec builds them), with the canonical
// words that accept it. This is typed-fields spec §3.1 read down the other
// column; it is written out, not derived from accepts, so the two can disagree.
var everyBSONType = []struct {
	name     string
	value    interface{}
	acceptBy []string
}{
	{"string", "text", []string{"string"}},
	{"int32", int32(7), []string{"int32", "int", "double", "number"}},
	{"int64 that fits int32", int64(7), []string{"int32", "int", "double", "number"}},
	{"int64 beyond int32", int64(1) << 40, []string{"int", "double", "number"}},
	{"double", 9.5, []string{"double", "number"}},
	{"decimal128", primitive.NewDecimal128(0, 7), []string{"decimal", "number"}},
	{"bool", true, []string{"bool"}},
	{"datetime", primitive.DateTime(1759492800000), []string{"datetime"}},
	{"bson timestamp", primitive.Timestamp{T: 1759492800, I: 1}, []string{"bsontimestamp"}},
	{"binary generic", primitive.Binary{Subtype: 0x00, Data: []byte{1, 2, 3}}, []string{"binary"}},
	{"binary uuid", primitive.Binary{Subtype: 0x04, Data: make([]byte, 16)}, []string{"binary", "uuid"}},
	{"objectid", primitive.NewObjectID(), []string{"objectid"}},
	{"document", map[string]interface{}{"City": "Tirana"}, []string{"object"}},
	{"array", primitive.A{"a", "b"}, []string{"array"}},
	{"regex", primitive.Regex{Pattern: "^a", Options: "i"}, []string{"regex"}},
	{"null", nil, []string{"null"}},
	{"undefined", primitive.Undefined{}, nil},
	{"dbpointer", primitive.DBPointer{DB: "d", Pointer: primitive.NewObjectID()}, nil},
	{"javascript", primitive.JavaScript("1"), nil},
	{"symbol", primitive.Symbol("s"), nil},
	{"javascript with scope", primitive.CodeWithScope{Code: "1", Scope: bson.D{}}, nil},
	{"minkey", primitive.MinKey{}, nil},
	{"maxkey", primitive.MaxKey{}, nil},
}

// TestEveryBSONTypeAgainstEveryDeclaredWord is the full matrix. `any` accepts
// every type, the deprecated ones included, and nothing else accepts them.
func TestEveryBSONTypeAgainstEveryDeclaredWord(t *testing.T) {
	for _, row := range everyBSONType {
		accepting := map[string]bool{"any": true}
		for _, word := range row.acceptBy {
			accepting[word] = true
		}
		for _, word := range canonicalWords {
			kind, ok := canonicalWord(word)
			if !ok {
				t.Fatalf("canonicalWord(%q) is not a word", word)
			}
			reason := accepts(kind, row.value)
			if accepting[word] && reason != mismatchNone {
				t.Errorf("a %s into %s: refused (reason %d); the table says it is accepted", row.name, word, reason)
			}
			if !accepting[word] && reason == mismatchNone {
				t.Errorf("a %s into %s: accepted; the table says it is refused", row.name, word)
			}
		}
	}
}

func TestInt32AcceptsAnInt64ThatFits(t *testing.T) {
	for _, fits := range []int64{0, 1, -1, math.MaxInt32, math.MinInt32} {
		if reason := accepts(kindInt32, fits); reason != mismatchNone {
			t.Errorf("int64 %d into int32: refused (reason %d); an int64 that fits is accepted", fits, reason)
		}
	}
}

func TestInt32RefusesAnInt64ThatDoesNot(t *testing.T) {
	for _, outside := range []int64{math.MaxInt32 + 1, math.MinInt32 - 1, math.MaxInt64, math.MinInt64} {
		if reason := accepts(kindInt32, outside); reason != mismatchInt32Range {
			t.Errorf("int64 %d into int32: reason %d, want mismatchInt32Range (E-2)", outside, reason)
		}
	}
}

func TestIntAcceptsInt32(t *testing.T) {
	for _, small := range []int32{0, -1, math.MaxInt32, math.MinInt32} {
		if reason := accepts(kindInt, small); reason != mismatchNone {
			t.Errorf("int32 %d into int: refused (reason %d)", small, reason)
		}
	}
	if reason := accepts(kindInt, int64(math.MinInt64)); reason != mismatchNone {
		t.Errorf("int64 min into int: refused (reason %d)", reason)
	}
}

// TestDoubleAcceptsIntegersUpTo2Pow53: every integer up to 2^53 in magnitude
// is a double exactly; one past it is not (E-3), for both signs.
func TestDoubleAcceptsIntegersUpTo2Pow53(t *testing.T) {
	const limit = int64(1) << 53
	for _, held := range []int64{0, 10, limit, -limit} {
		if reason := accepts(kindDouble, held); reason != mismatchNone {
			t.Errorf("int64 %d into double: refused (reason %d); every integer up to 2^53 is held exactly", held, reason)
		}
	}
	for _, rounded := range []int64{limit + 1, -limit - 1, math.MaxInt64, math.MinInt64} {
		if reason := accepts(kindDouble, rounded); reason != mismatchDoubleMagnitude {
			t.Errorf("int64 %d into double: reason %d, want mismatchDoubleMagnitude (E-3)", rounded, reason)
		}
	}
	if reason := accepts(kindDouble, int32(math.MinInt32)); reason != mismatchNone {
		t.Errorf("int32 min into double: refused (reason %d)", reason)
	}
}

// TestDoubleNeverSatisfiesAnIntegerType: a whole double is still a double
// (E-4), and the reason says so, for the sender to be told to drop the point.
func TestDoubleNeverSatisfiesAnIntegerType(t *testing.T) {
	for _, word := range []string{"int32", "int"} {
		kind, _ := canonicalWord(word)
		for _, whole := range []float64{36.0, 0, -4, 1 << 40} {
			if reason := accepts(kind, whole); reason != mismatchDoubleForInteger {
				t.Errorf("double %v into %s: reason %d, want mismatchDoubleForInteger (E-4)", whole, word, reason)
			}
		}
	}
}

// TestNaNAndInfinitySatisfyDoubleAndNumber: they are doubles (spec §3.3).
func TestNaNAndInfinitySatisfyDoubleAndNumber(t *testing.T) {
	for _, odd := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		for _, kind := range []fieldKind{kindDouble, kindNumber} {
			if reason := accepts(kind, odd); reason != mismatchNone {
				t.Errorf("%v into %s: refused (reason %d)", odd, kind, reason)
			}
		}
	}
}

// TestDecimalAcceptsOnlyDecimal128 is OC-5: an int32, an int64 and a double
// are refused, and a double says why it is not a decimal (E-4's variant).
func TestDecimalAcceptsOnlyDecimal128(t *testing.T) {
	if reason := accepts(kindDecimal, primitive.NewDecimal128(0, 10)); reason != mismatchNone {
		t.Errorf("a Decimal128 into decimal: refused (reason %d)", reason)
	}
	for _, integer := range []interface{}{int32(10), int64(10)} {
		if reason := accepts(kindDecimal, integer); reason != mismatchWrongType {
			t.Errorf("%T into decimal: reason %d, want mismatchWrongType (E-1)", integer, reason)
		}
	}
	if reason := accepts(kindDecimal, 0.1); reason != mismatchDoubleForDecimal {
		t.Errorf("a double into decimal: reason %d, want mismatchDoubleForDecimal", reason)
	}
}

func TestUUIDRequiresSubtype4And16Bytes(t *testing.T) {
	if reason := accepts(kindUUID, primitive.Binary{Subtype: 0x04, Data: make([]byte, 16)}); reason != mismatchNone {
		t.Errorf("subtype 4 with 16 bytes into uuid: refused (reason %d)", reason)
	}
	refused := map[string]interface{}{
		"subtype 3 (the old UUID), 16 bytes": primitive.Binary{Subtype: 0x03, Data: make([]byte, 16)},
		"subtype 4, 15 bytes":                primitive.Binary{Subtype: 0x04, Data: make([]byte, 15)},
		"subtype 4, 17 bytes":                primitive.Binary{Subtype: 0x04, Data: make([]byte, 17)},
		"generic binary, 16 bytes":           primitive.Binary{Subtype: 0x00, Data: make([]byte, 16)},
	}
	for name, value := range refused {
		if reason := accepts(kindUUID, value); reason != mismatchWrongType {
			t.Errorf("%s into uuid: reason %d, want mismatchWrongType", name, reason)
		}
	}
	if reason := accepts(kindUUID, "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b"); reason != mismatchUUIDText {
		t.Errorf("UUID text into uuid: reason %d, want mismatchUUIDText (E-8)", reason)
	}
}

// TestNullAndMissingSatisfyOptionalAndFailRequired, the accepts half: null is
// a value of its own type, so only `null` and `any` accept it. That a null
// satisfies an optional field is the document check's rule (§4.2), proved in
// TestNullAndMissingSatisfyOptionalAndFailRequiredInADocument.
func TestNullAndMissingSatisfyOptionalAndFailRequired(t *testing.T) {
	if reason := accepts(kindNull, nil); reason != mismatchNone {
		t.Errorf("null into null: refused (reason %d)", reason)
	}
	if reason := accepts(kindString, nil); reason == mismatchNone {
		t.Error("null into string was accepted by the type rule; the optional-field rule belongs to the document check")
	}
	if reason := accepts(kindNull, "x"); reason == mismatchNone {
		t.Error("a string into null was accepted")
	}
}

// TestInProcessGoTypesAreClassifiedAsTheyEncode: a value only an in-process
// caller can pass is classified by the BSON type it would encode as (spec
// §4.5.4's slow path), so the answer is the same as for the wire form.
func TestInProcessGoTypesAreClassifiedAsTheyEncode(t *testing.T) {
	type address struct{ City string }
	rows := []struct {
		name     string
		value    interface{}
		accepted []fieldKind
		received string
	}{
		{"int that fits int32", int(5), []fieldKind{kindInt32, kindInt, kindDouble, kindNumber}, "a 32-bit integer"},
		{"int beyond int32", int(1) << 40, []fieldKind{kindInt, kindDouble, kindNumber}, "a 64-bit integer"},
		{"uint32 (encodes as int64)", uint32(4000000000), []fieldKind{kindInt, kindDouble, kindNumber}, "a 64-bit integer"},
		{"time.Time", time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC), []fieldKind{kindDatetime}, "a datetime"},
		{"[]string", []string{"a", "b"}, []fieldKind{kindArray}, "an array"},
		{"a struct", address{City: "Tirana"}, []fieldKind{kindObject}, "a document"},
		{"bson.M", bson.M{"City": "Tirana"}, []fieldKind{kindObject}, "a document"},
		{"bson.D", bson.D{{Key: "City", Value: "Tirana"}}, []fieldKind{kindObject}, "a document"},
		{"bson.A", bson.A{"a"}, []fieldKind{kindArray}, "an array"},
		{"[]byte", []byte{1, 2}, []fieldKind{kindBinary}, "binary data (subtype 0x00)"},
	}
	for _, row := range rows {
		accepted := map[fieldKind]bool{}
		for _, kind := range row.accepted {
			accepted[kind] = true
		}
		for _, word := range canonicalWords[:len(canonicalWords)-1] { // every word but any
			kind, _ := canonicalWord(word)
			reason := accepts(kind, row.value)
			if accepted[kind] != (reason == mismatchNone) {
				t.Errorf("%s into %s: reason %d, accepted-by-table %v", row.name, word, reason, accepted[kind])
			}
		}
		if got := receivedWord(row.value); got != row.received {
			t.Errorf("receivedWord(%s) = %q, want %q", row.name, got, row.received)
		}
	}
}

// TestReceivedWordsAreTheSpecsWords: spec §6.2's <received> list, exactly.
func TestReceivedWordsAreTheSpecsWords(t *testing.T) {
	want := map[string]string{
		"string":                "a string",
		"int32":                 "a 32-bit integer",
		"int64 that fits int32": "a 64-bit integer",
		"int64 beyond int32":    "a 64-bit integer",
		"double":                "a double",
		"decimal128":            "a 128-bit decimal",
		"bool":                  "a boolean",
		"datetime":              "a datetime",
		"bson timestamp":        "a BSON timestamp",
		"binary generic":        "binary data (subtype 0x00)",
		"binary uuid":           "a UUID",
		"objectid":              "an object id",
		"document":              "a document",
		"array":                 "an array",
		"regex":                 "a regular expression",
		"null":                  "null",
		"undefined":             "a deprecated BSON type (undefined)",
		"dbpointer":             "a deprecated BSON type (dbPointer)",
		"javascript":            "a deprecated BSON type (javascript)",
		"symbol":                "a deprecated BSON type (symbol)",
		"javascript with scope": "a deprecated BSON type (code with scope)",
		"minkey":                "a deprecated BSON type (min key)",
		"maxkey":                "a deprecated BSON type (max key)",
	}
	for _, row := range everyBSONType {
		if got := receivedWord(row.value); got != want[row.name] {
			t.Errorf("receivedWord(%s) = %q, want %q", row.name, got, want[row.name])
		}
	}
	if got := receivedWord([]interface{}{"a"}); got != "an array" {
		t.Errorf("receivedWord([]interface{}) = %q, want an array", got)
	}
	if got := receivedWord(primitive.Binary{Subtype: 0x80, Data: []byte{1}}); got != "binary data (subtype 0x80)" {
		t.Errorf("a user-defined subtype is %q", got)
	}
	if got := receivedWord(primitive.Binary{Subtype: 0x04, Data: make([]byte, 15)}); got != "binary data (subtype 0x04)" {
		t.Errorf("a short subtype-4 value is %q, not a UUID", got)
	}
}

// TestDecodedWireValuesAreClassifiedLikeLiterals: the check must see the
// types a handler's decode produces, not the ones a test would write by hand.
func TestDecodedWireValuesAreClassifiedLikeLiterals(t *testing.T) {
	decoded := wireDecoded(t, bson.D{
		{Key: "s", Value: "x"}, {Key: "i", Value: int32(1)}, {Key: "l", Value: int64(1)}, {Key: "f", Value: 1.5},
		{Key: "d", Value: primitive.NewDecimal128(0, 1)}, {Key: "b", Value: true}, {Key: "t", Value: primitive.DateTime(1)},
		{Key: "o", Value: bson.D{{Key: "x", Value: int32(1)}}}, {Key: "a", Value: bson.A{int32(1)}},
	})
	want := map[string]string{"s": "string", "i": "int32", "l": "int32", "f": "double", "d": "decimal", "b": "bool", "t": "datetime", "o": "object", "a": "array"}
	for field, word := range want {
		kind, _ := canonicalWord(word)
		if reason := accepts(kind, decoded[field]); reason != mismatchNone {
			t.Errorf("decoded %s (%T) into %s: refused (reason %d)", field, decoded[field], word, reason)
		}
	}
}

// wireDecoded encodes a document and decodes it into a map the way a handler
// does, so its values carry the Go types a network client's write has.
func wireDecoded(tb testing.TB, document bson.D) map[string]interface{} {
	tb.Helper()
	encoded, err := bson.Marshal(document)
	if err != nil {
		tb.Fatalf("encoding the document: %v", err)
	}
	var decoded map[string]interface{}
	if err := bson.Unmarshal(encoded, &decoded); err != nil {
		tb.Fatalf("decoding the document: %v", err)
	}
	return decoded
}
