package cli

import (
	"bytes"
	"encoding/hex"
	"reflect"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/bsontype"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// contains is a small helper so test failures read well.
func contains(haystack, needle string) bool {
	return strings.Contains(haystack, needle)
}

func TestParseDocumentAcceptsDocumentedSyntax(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want map[string]interface{}
	}{
		{
			name: "strict json",
			in:   `{"name": "ada", "age": 42}`,
			want: map[string]interface{}{"name": "ada", "age": int32(42)},
		},
		{
			name: "unquoted keys",
			in:   `{name: "ada", age: 42}`,
			want: map[string]interface{}{"name": "ada", "age": int32(42)},
		},
		{
			name: "single quotes",
			in:   `{'name': 'ada'}`,
			want: map[string]interface{}{"name": "ada"},
		},
		{
			name: "trailing comma",
			in:   `{name: "ada",}`,
			want: map[string]interface{}{"name": "ada"},
		},
		{
			name: "bare word value",
			in:   `{type: hash}`,
			want: map[string]interface{}{"type": "hash"},
		},
		{
			name: "colon separator",
			in:   `{_id_: u1}`,
			want: map[string]interface{}{"_id_": "u1"},
		},
		{
			name: "equals separator",
			in:   `{_id_ = u1}`,
			want: map[string]interface{}{"_id_": "u1"},
		},
		{
			name: "booleans stay booleans",
			in:   `{active: true, deleted: false, nothing: null}`,
			want: map[string]interface{}{"active": true, "deleted": false, "nothing": nil},
		},
		{
			name: "nested objects and lists",
			in:   `{a: {b: 'c'}, list: [1, 2, three]}`,
			want: map[string]interface{}{
				"a":    map[string]interface{}{"b": "c"},
				"list": primitive.A{int32(1), int32(2), "three"},
			},
		},
		{
			name: "empty document",
			in:   `{}`,
			want: map[string]interface{}{},
		},
		{
			name: "quoted key with space",
			in:   `{"full name": "Ada Lovelace"}`,
			want: map[string]interface{}{"full name": "Ada Lovelace"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseDocument(tc.in)
			if err != nil {
				t.Fatalf("parseDocument(%s) returned error: %v", tc.in, err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("parseDocument(%s) = %#v, want %#v", tc.in, got, tc.want)
			}
		})
	}
}

func TestParseDocumentErrorsExplainTheFix(t *testing.T) {
	cases := []struct {
		name string
		in   string
	}{
		{"empty", ""},
		{"unbalanced", `{name: "ada"`},
		{"unterminated string", `{name: "ada}`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseDocument(tc.in)
			if err == nil {
				t.Fatalf("parseDocument(%q) returned no error", tc.in)
			}
		})
	}

	_, err := parseDocument("")
	if err == nil || !contains(err.Error(), "{\"name\": \"ada\"}") {
		t.Errorf("empty document error %q does not show an example document", err)
	}
}

func TestKeyFromArgs(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want string
	}{
		{"bare key", []string{"u1"}, "u1"},
		{"quoted key", []string{`"u1"`}, "u1"},
		{"document key", []string{`{_id_: "u1"}`}, "u1"},
		{"document id", []string{`{id: "u1"}`}, "u1"},
		{"document key field", []string{`{key: "u1"}`}, "u1"},
		{"numeric key", []string{`{_id_: 7}`}, "7"},
		{"document without key", []string{`{Name: "ada"}`}, ""},
		{"no args", nil, ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := keyFromArgs(tc.args)
			if err != nil {
				t.Fatalf("keyFromArgs(%v) returned error: %v", tc.args, err)
			}
			if got != tc.want {
				t.Errorf("keyFromArgs(%v) = %q, want %q", tc.args, got, tc.want)
			}
		})
	}
}

func TestBlobPayloadSplitsDataAndMetadata(t *testing.T) {
	payload, metadata := blobPayload(map[string]interface{}{
		"_id_": "hello",
		"data": "hello world",
		"name": "hello.txt",
	})
	if string(payload) != "hello world" {
		t.Errorf("payload = %q, want %q", payload, "hello world")
	}
	if _, ok := metadata["data"]; ok {
		t.Error("data was left in the metadata")
	}
	if _, ok := metadata["_id_"]; ok {
		t.Error("key field was left in the metadata")
	}
	if metadata["name"] != "hello.txt" {
		t.Errorf("metadata = %+v, want name=hello.txt", metadata)
	}
}

func TestBlobPayloadFallsBackToWholeDocument(t *testing.T) {
	doc := map[string]interface{}{"_id_": "k", "name": "doc"}
	payload, _ := blobPayload(doc)
	if string(payload) != `{"_id_":"k","name":"doc"}` {
		t.Errorf("payload = %s, want the document as JSON", payload)
	}
}

func TestApplyUpdateOperators(t *testing.T) {
	req := &server.UpdateRequest{}
	if err := applyUpdateOperators(req, map[string]interface{}{
		"$set":   map[string]interface{}{"City": "London"},
		"$unset": []interface{}{"Name"},
		"$inc":   map[string]interface{}{"Age": float64(2), "Visits": float64(1)},
	}); err != nil {
		t.Fatalf("applyUpdateOperators: %v", err)
	}

	if req.Set["City"] != "London" {
		t.Errorf("$set = %+v, want City=London", req.Set)
	}
	if len(req.Unset) != 1 || req.Unset[0] != "Name" {
		t.Errorf("$unset = %v, want [Name]", req.Unset)
	}
	if req.Inc["Age"] != 2 || req.Inc["Visits"] != 1 {
		t.Errorf("$inc = %+v, want Age=2 Visits=1", req.Inc)
	}
}

func TestApplyUpdateOperatorsRejectsUnsupportedOperator(t *testing.T) {
	req := &server.UpdateRequest{}
	err := applyUpdateOperators(req, map[string]interface{}{
		"$pull": map[string]interface{}{"Tags": "new"},
	})
	if err == nil {
		t.Fatal("$pull was accepted, but the server does not implement it")
	}
	if !contains(err.Error(), "$pull") {
		t.Errorf("error %q does not name the offending operator", err)
	}
	for _, supported := range []string{"$set", "$unset", "$inc", "$push", "$addToSet", "$mul", "$min", "$max", "$rename"} {
		if !contains(err.Error(), supported) {
			t.Errorf("error %q does not list %s among the supported operators", err, supported)
		}
	}
}

func TestApplyUpdateOperatorsMergesRepeatedOperators(t *testing.T) {
	req := &server.UpdateRequest{}
	if err := applyUpdateOperators(req, map[string]interface{}{
		"$inc": map[string]interface{}{"Age": float64(2)},
	}); err != nil {
		t.Fatalf("applyUpdateOperators: %v", err)
	}
	if err := applyUpdateOperators(req, map[string]interface{}{
		"$inc": map[string]interface{}{"Age": float64(3)},
	}); err != nil {
		t.Fatalf("applyUpdateOperators: %v", err)
	}
	if got := req.Inc["Age"]; got != 5 {
		t.Errorf("$inc accumulated to %d, want 5", got)
	}
}

// TestParseBucketSchemaReadsType goes through ParseCommand, not a hand-built
// Cmd: the parser and the handler must agree on where the schema text lives
// (they did not once — every "create bucket ... { type: fifo }" silently
// created a hash bucket).
func TestParseBucketSchemaReadsType(t *testing.T) {
	cases := []struct {
		line string
		want string
	}{
		{"create bucket Users { type: hash }", "hash"},
		{`create bucket ByEmail { type: "btree", indices: ["Email"] }`, "btree"},
		{"create bucket Jobs type fifo", "fifo"},
		{"create bucket Cache", "hash"},
		{`create bucket Cache { indices: ["Email"] }`, "hash"},
	}

	for _, tc := range cases {
		cmd, err := ParseCommand(tc.line)
		if err != nil {
			t.Fatalf("ParseCommand(%q): %v", tc.line, err)
		}
		schema, err := parseBucketSchema(cmd, "app")
		if err != nil {
			t.Fatalf("parseBucketSchema(%q) returned error: %v", tc.line, err)
		}
		if string(schema.Type) != tc.want {
			t.Errorf("parseBucketSchema(%q) type = %q, want %q", tc.line, schema.Type, tc.want)
		}
	}
}

func TestParseBucketSchemaRejectsUnknownType(t *testing.T) {
	cmd, err := ParseCommand("create bucket Graph { type: graph }")
	if err != nil {
		t.Fatalf("ParseCommand: %v", err)
	}
	_, err = parseBucketSchema(cmd, "app")
	if err == nil {
		t.Fatal("unknown bucket type was accepted")
	}
	if !contains(err.Error(), "graph") {
		t.Errorf("error %q does not name the unknown type", err)
	}
	if !contains(err.Error(), "btree") {
		t.Errorf("error %q does not list the valid types", err)
	}
}

func TestParseBucketSchemaKeepsIndices(t *testing.T) {
	cmd, err := ParseCommand(`create bucket ByEmail { type: btree, indices: ["Email"] }`)
	if err != nil {
		t.Fatalf("ParseCommand: %v", err)
	}
	schema, err := parseBucketSchema(cmd, "app")
	if err != nil {
		t.Fatalf("parseBucketSchema: %v", err)
	}
	if schema.Type != server.BucketTypeBTree {
		t.Errorf("type = %q, want btree", schema.Type)
	}
	indices, ok := schema.Options["indices"].([]interface{})
	if !ok {
		t.Fatalf("indices = %#v, want a list", schema.Options["indices"])
	}
	if len(indices) != 1 || indices[0] != "Email" {
		t.Errorf("indices = %v, want [Email]", indices)
	}
	if _, present := schema.Options["type"]; present {
		t.Error("type was left in the bucket options")
	}
}

func TestParseDocumentReadsISODateLiteral(t *testing.T) {
	got, err := parseDocument(`{ts: ISODate("2026-09-19T16:47:43Z")}`)
	if err != nil {
		t.Fatalf("parseDocument failed: %v", err)
	}
	tsValue, ok := got["ts"]
	if !ok {
		t.Fatal("parsed document does not contain ts field")
	}
	dt, ok := tsValue.(primitive.DateTime)
	if !ok {
		t.Fatalf("ts is %T, want primitive.DateTime", tsValue)
	}
	// The reference time 2026-09-19T16:47:43Z should be 1789836463000 ms since epoch
	if dt != 1789836463000 {
		t.Errorf("ISODate value = %d, want 1789836463000", dt)
	}
}

func TestParseDocumentNewDateIsAnAliasForISODate(t *testing.T) {
	got1, err := parseDocument(`{ts: ISODate("2026-09-19T16:47:43Z")}`)
	if err != nil {
		t.Fatalf("parseDocument(ISODate) failed: %v", err)
	}
	got2, err := parseDocument(`{ts: new Date("2026-09-19T16:47:43Z")}`)
	if err != nil {
		t.Fatalf("parseDocument(new Date) failed: %v", err)
	}

	// Both should produce byte-identical BSON
	bytes1, err := bson.Marshal(got1)
	if err != nil {
		t.Fatalf("bson.Marshal(ISODate doc) failed: %v", err)
	}
	bytes2, err := bson.Marshal(got2)
	if err != nil {
		t.Fatalf("bson.Marshal(new Date doc) failed: %v", err)
	}
	if !reflect.DeepEqual(bytes1, bytes2) {
		t.Errorf("BSON bytes differ:\n  ISODate:  %s\n  new Date: %s", hex.EncodeToString(bytes1), hex.EncodeToString(bytes2))
	}
}

func TestParseDocumentReadsExtendedJSONBothSpellings(t *testing.T) {
	got1, err := parseDocument(`{ts: {"$date": "2026-09-19T16:47:43Z"}}`)
	if err != nil {
		t.Fatalf("parseDocument($date string) failed: %v", err)
	}
	got2, err := parseDocument(`{ts: {"$date": {"$numberLong": "1789836463000"}}}`)
	if err != nil {
		t.Fatalf("parseDocument($date $numberLong) failed: %v", err)
	}

	bytes1, err := bson.Marshal(got1)
	if err != nil {
		t.Fatalf("bson.Marshal($date string) failed: %v", err)
	}
	bytes2, err := bson.Marshal(got2)
	if err != nil {
		t.Fatalf("bson.Marshal($date $numberLong) failed: %v", err)
	}
	if !reflect.DeepEqual(bytes1, bytes2) {
		t.Errorf("BSON bytes differ:\n  $date string:     %s\n  $date $numberLong: %s", hex.EncodeToString(bytes1), hex.EncodeToString(bytes2))
	}
}

func TestParseDocumentLeavesBareISOStringAString(t *testing.T) {
	got, err := parseDocument(`{ts: "2026-09-19T16:47:43Z"}`)
	if err != nil {
		t.Fatalf("parseDocument failed: %v", err)
	}
	tsValue, ok := got["ts"]
	if !ok {
		t.Fatal("parsed document does not contain ts field")
	}
	_, isDateTime := tsValue.(primitive.DateTime)
	if isDateTime {
		t.Fatalf("ts should be a string, not primitive.DateTime")
	}
	strValue, ok := tsValue.(string)
	if !ok {
		t.Fatalf("ts is %T, want string", tsValue)
	}
	if strValue != "2026-09-19T16:47:43Z" {
		t.Errorf("ts = %q, want %q", strValue, "2026-09-19T16:47:43Z")
	}
}

func TestParseDocumentNormalisesOffsetToUTC(t *testing.T) {
	got1, err := parseDocument(`{ts: ISODate("2026-09-19T18:47:43+02:00")}`)
	if err != nil {
		t.Fatalf("parseDocument failed: %v", err)
	}
	got2, err := parseDocument(`{ts: ISODate("2026-09-19T16:47:43Z")}`)
	if err != nil {
		t.Fatalf("parseDocument failed: %v", err)
	}

	dt1 := got1["ts"].(primitive.DateTime)
	dt2 := got2["ts"].(primitive.DateTime)
	if dt1 != dt2 {
		t.Errorf("normalised offset = %d, want %d (both should be the same instant)", dt1, dt2)
	}
}

func TestParseDocumentRefusesNamedTimeZone(t *testing.T) {
	_, err := parseDocument(`{ts: ISODate("2026-09-19T16:47:43 Europe/Tirane")}`)
	if err == nil {
		t.Fatal("parseDocument should reject named time zones")
	}
	if !strings.Contains(err.Error(), "offset") {
		t.Errorf("error %q should mention 'offset' as the fix", err.Error())
	}
}

func TestJSONLineIsStableAndSorted(t *testing.T) {
	doc := map[string]interface{}{"b": float64(2), "a": "x"}
	if got := jsonLine(doc, false); got != `{"a": "x", "b": 2}` {
		t.Errorf("jsonLine = %s, want {\"a\": \"x\", \"b\": 2}", got)
	}
}

func TestJSONLineExtendedJSONKeepsDateType(t *testing.T) {
	doc := map[string]interface{}{"ts": primitive.DateTime(1789836463000), "name": "ada"}

	// Default mode (non-extended JSON) should output RFC 3339 string
	defaultOut := jsonLine(doc, false)
	if !strings.Contains(defaultOut, "2026-09-19T16:47:43") {
		t.Errorf("default jsonLine should contain RFC 3339 date, got: %s", defaultOut)
	}
	if strings.Contains(defaultOut, "$date") {
		t.Errorf("default jsonLine should not contain $date, got: %s", defaultOut)
	}

	// Extended JSON mode should output {"$date": "..."}
	extOut := jsonLineExtJSON(doc)
	if !strings.Contains(extOut, `"$date"`) {
		t.Errorf("extended JSON jsonLine should contain $date, got: %s", extOut)
	}
	if !strings.Contains(extOut, "2026-09-19T16:47:43") {
		t.Errorf("extended JSON jsonLine should contain the date string, got: %s", extOut)
	}

	// Verify that the default output is unchanged (byte-for-byte stable)
	// Re-parse and check the string form matches
	if !strings.Contains(defaultOut, `"ts": "2026-09-19T16:47:43`) {
		t.Errorf("default output format changed unexpectedly: %s", defaultOut)
	}
}

func TestHumanBytes(t *testing.T) {
	cases := map[int64]string{
		0:       "0 B",
		512:     "512 B",
		1024:    "1.0 KiB",
		1048576: "1.0 MiB",
	}
	for in, want := range cases {
		if got := humanBytes(in); got != want {
			t.Errorf("humanBytes(%d) = %q, want %q", in, got, want)
		}
	}
}

// TestCLIPassesTheIndexObjectFormThrough: the object form of an index
// declaration reaches the server as an object. The CLI coerced every element
// to a string, so {field: "Email", unique: true} arrived as the text of a map
// and only bare names were expressible (spec §7.3). The CLI does not
// validate: the server's refusal is the one message.
func TestCLIPassesTheIndexObjectFormThrough(t *testing.T) {
	cmd, err := ParseCommand(`create bucket U { type: hash, indices: [{field: "Email", unique: true}, "Login"] }`)
	if err != nil {
		t.Fatalf("ParseCommand: %v", err)
	}
	schema, err := parseBucketSchema(cmd, "app")
	if err != nil {
		t.Fatalf("parseBucketSchema: %v", err)
	}
	indices, ok := schema.Options["indices"].([]interface{})
	if !ok || len(indices) != 2 {
		t.Fatalf("indices = %#v, want a two-element list", schema.Options["indices"])
	}
	object, ok := indices[0].(map[string]interface{})
	if !ok {
		t.Fatalf("the first index reached the server as %#v, want the object", indices[0])
	}
	if object["field"] != "Email" || object["unique"] != true {
		t.Errorf("the index object reached the server as %v, want field Email and unique true", object)
	}
	if indices[1] != "Login" {
		t.Errorf("the bare name reached the server as %#v, want \"Login\"", indices[1])
	}
}

// TestBucketListShowsTheFactorAndWhereItCameFrom: `rf 3` for a factor the bucket
// declared, `rf 1 (inherited)` for the node's, and nothing invented for a server
// that sends neither.
func TestBucketListShowsTheFactorAndWhereItCameFrom(t *testing.T) {
	var out bytes.Buffer
	h := &CommandHandler{terminal: &out}
	h.printBucketList([]server.BucketInfo{
		{Name: "Sessions", Type: server.BucketTypeHash, Entries: 4, ReplicationFactor: 3},
		{Name: "Plain", Type: server.BucketTypeHash, Entries: 2, ReplicationFactor: 1, Inherited: true},
		{Name: "Old", Type: server.BucketTypeHash, Entries: 1},
	})
	printed := out.String()
	for _, want := range []string{"REPLICATION", "rf 3\n", "rf 1 (inherited)\n"} {
		if !strings.Contains(printed, want) {
			t.Errorf("the listing does not contain %q:\n%s", want, printed)
		}
	}
	if strings.Contains(printed, "rf 0") {
		t.Errorf("the listing invented a factor for a server that sent none:\n%s", printed)
	}
}

// wrapperCases is every Extended JSON wrapper the CLI revives (spec 7.2 item 2)
// and the Go type the parser must hand the client for it.
var wrapperCases = []struct {
	name     string
	document string
	want     interface{}
}{
	{"$numberInt", `{"$numberInt": "7"}`, int32(7)},
	{"$numberLong", `{"$numberLong": "7"}`, int64(7)},
	{"$numberDouble", `{"$numberDouble": "1.5"}`, float64(1.5)},
	{"$numberDecimal", `{"$numberDecimal": "1.5"}`, mustDecimal("1.5")},
	{"$binary", `{"$binary": {"base64": "aGk=", "subType": "00"}}`, primitive.Binary{Subtype: 0, Data: []byte("hi")}},
	{"$uuid", `{"$uuid": "d9c3b3a0-1c3a-4f5e-8a3a-0123456789ab"}`, primitive.Binary{Subtype: 4, Data: []byte{0xd9, 0xc3, 0xb3, 0xa0, 0x1c, 0x3a, 0x4f, 0x5e, 0x8a, 0x3a, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab}}},
	{"$oid", `{"$oid": "507f1f77bcf86cd799439011"}`, mustObjectID("507f1f77bcf86cd799439011")},
	{"$regularExpression", `{"$regularExpression": {"pattern": "^a", "options": "i"}}`, primitive.Regex{Pattern: "^a", Options: "i"}},
	{"$timestamp", `{"$timestamp": {"t": 1, "i": 2}}`, primitive.Timestamp{T: 1, I: 2}},
	{"$date", `{"$date": "2026-09-19T16:47:43Z"}`, primitive.DateTime(1789836463000)},
}

func mustDecimal(text string) primitive.Decimal128 {
	decimal, err := primitive.ParseDecimal128(text)
	if err != nil {
		panic(err)
	}
	return decimal
}

func mustObjectID(hex string) primitive.ObjectID {
	identifier, err := primitive.ObjectIDFromHex(hex)
	if err != nil {
		panic(err)
	}
	return identifier
}

// TestParseDocumentKeepsIntegersAsIntegers: an integer literal is
// int32 when it fits and int64 when it does not, a literal with a decimal point
// or exponent is a double, and a list keeps the BSON array type. This is
// ingest's rule (bson.UnmarshalExtJSON, relaxed), so the two text inputs agree.
func TestParseDocumentKeepsIntegersAsIntegers(t *testing.T) {
	for _, input := range []string{
		`{"Age": 36, "Big": 3000000000, "F": 36.0, "E": 1e3, "Neg": -5, "List": [1, 2.5]}`,
		`{Age: 36, Big: 3000000000, F: 36.0, E: 1e3, Neg: -5, List: [1, 2.5]}`,
	} {
		got, err := parseDocument(input)
		if err != nil {
			t.Fatalf("parseDocument(%s): %v", input, err)
		}
		want := map[string]interface{}{
			"Age": int32(36), "Big": int64(3000000000), "F": float64(36), "E": float64(1000), "Neg": int32(-5),
			"List": primitive.A{int32(1), float64(2.5)},
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("parseDocument(%s) =\n  %#v\nwant\n  %#v", input, got, want)
		}
	}
	got, err := parseDocument(`{max: 9223372036854775807, min: -9223372036854775808}`)
	if err != nil {
		t.Fatalf("the int64 limits are valid integers: %v", err)
	}
	if got["max"] != int64(9223372036854775807) || got["min"] != int64(-9223372036854775808) {
		t.Errorf("the int64 limits parsed as %#v, want int64 both", got)
	}
}

// TestCLIAndIngestStoreTheSameTypes checks that one
// document text, two inputs, one stored type per field.
func TestCLIAndIngestStoreTheSameTypes(t *testing.T) {
	text := `{"Age": 36, "Big": 3000000000, "F": 36.0, "Neg": -5, "Id": {"$uuid": "d9c3b3a0-1c3a-4f5e-8a3a-0123456789ab"}, "Price": {"$numberDecimal": "1.5"}, "When": {"$date": "2026-09-19T16:47:43Z"}}`
	ingested, reason := decodeJSONLine([]byte(text))
	if reason != "" {
		t.Fatalf("ingest refused the line: %s", reason)
	}
	parsed, err := parseDocument(text)
	if err != nil {
		t.Fatalf("parseDocument: %v", err)
	}
	statement, err := bson.Marshal(parsed)
	if err != nil {
		t.Fatalf("bson.Marshal: %v", err)
	}
	ingestElements, err := ingested.Elements()
	if err != nil {
		t.Fatalf("reading the ingested document: %v", err)
	}
	for _, element := range ingestElements {
		field := element.Key()
		if got, want := bson.Raw(statement).Lookup(field).Type, element.Value().Type; got != want {
			t.Errorf("%s: the CLI stores %s, ingest stores %s", field, got, want)
		}
	}
}

// TestParseDocumentRevivesEveryExtendedJSONWrapper: each supported
// wrapper becomes its BSON type, at the top level, nested in a document and
// inside an array. Before, each stayed a one-key map and was stored as an
// embedded document.
func TestParseDocumentRevivesEveryExtendedJSONWrapper(t *testing.T) {
	for _, tc := range wrapperCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseDocument(`{v: ` + tc.document + `, n: {v: ` + tc.document + `}, l: [` + tc.document + `]}`)
			if err != nil {
				t.Fatalf("parseDocument: %v", err)
			}
			if !reflect.DeepEqual(got["v"], tc.want) {
				t.Errorf("v = %#v (%T), want %#v (%T)", got["v"], got["v"], tc.want, tc.want)
			}
			if nested, ok := got["n"].(map[string]interface{}); !ok || !reflect.DeepEqual(nested["v"], tc.want) {
				t.Errorf("n = %#v, want a document holding %#v", got["n"], tc.want)
			}
			if list, ok := got["l"].(primitive.A); !ok || len(list) != 1 || !reflect.DeepEqual(list[0], tc.want) {
				t.Errorf("l = %#v, want an array holding %#v", got["l"], tc.want)
			}
		})
	}
}

// TestCLIRefusesAMalformedWrapperByName: a malformed
// wrapper is refused with a message naming the wrapper and the shape
// it expects, not the BSON library's bare sentence.
func TestCLIRefusesAMalformedWrapperByName(t *testing.T) {
	cases := []struct{ document, wrapper, shape string }{
		{`{v: {"$oid": "zz"}}`, "$oid", "24 hex"},
		{`{v: {"$numberDecimal": "abc"}}`, "$numberDecimal", "$numberDecimal"},
		{`{v: {"$uuid": "not-a-uuid"}}`, "$uuid", "8-4-4-4-12"},
		{`{v: {"$timestamp": {"t": "x"}}}`, "$timestamp", `"t"`},
	}
	for _, tc := range cases {
		_, err := parseDocument(tc.document)
		if err == nil {
			t.Errorf("parseDocument(%s) was accepted", tc.document)
			continue
		}
		if !strings.Contains(err.Error(), tc.wrapper) || !strings.Contains(err.Error(), tc.shape) {
			t.Errorf("parseDocument(%s) = %q, want it to name %s and show its shape (%s)", tc.document, err, tc.wrapper, tc.shape)
		}
	}
}

// TestCLIRefusesDeprecatedExtendedJSONWrappers is spec 7.2 item 3. The BSON
// parser revives these (primitive.Symbol, JavaScript, MinKey ...), so without
// a walk after the decode a typed document would carry a deprecated type.
func TestCLIRefusesDeprecatedExtendedJSONWrappers(t *testing.T) {
	deprecated := []struct{ wrapper, document string }{
		{"$symbol", `{"$symbol": "x"}`},
		{"$code", `{"$code": "return 1"}`},
		{"$code", `{"$code": "return 1", "$scope": {"a": 1}}`},
		{"$dbPointer", `{"$dbPointer": {"$ref": "c", "$id": {"$oid": "507f1f77bcf86cd799439011"}}}`},
		{"$undefined", `{"$undefined": true}`},
		{"$minKey", `{"$minKey": 1}`},
		{"$maxKey", `{"$maxKey": 1}`},
	}
	for _, tc := range deprecated {
		for _, place := range []string{`{v: %s}`, `{n: {v: %s}}`, `{l: [1, %s]}`} {
			document := strings.Replace(place, "%s", tc.document, 1)
			_, err := parseDocument(document)
			if err == nil {
				t.Errorf("parseDocument(%s) accepted a deprecated type", document)
				continue
			}
			for _, want := range []string{tc.wrapper, "$numberDecimal", "$oid"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("parseDocument(%s) = %q, want it to name %s and the supported wrappers", document, err, want)
				}
			}
		}
	}
}

// TestCLIRefusesAnIntegerAboveInt64 is spec 7.2 item 4. The relaxed parser
// turns 18446744073709551615 into a double, and the server would then say the
// value was "written with a decimal point".
func TestCLIRefusesAnIntegerAboveInt64(t *testing.T) {
	for _, document := range []string{
		`{n: 18446744073709551615}`,
		`{"n": 9223372036854775808}`,
		`{n: -9223372036854775809}`,
		`{l: [1, 99999999999999999999]}`,
		`{a: {b: 99999999999999999999}}`,
	} {
		_, err := parseDocument(document)
		if err == nil {
			t.Errorf("parseDocument(%s) accepted an integer outside int64", document)
			continue
		}
		if !strings.Contains(err.Error(), "int64") || !strings.Contains(err.Error(), "$numberDecimal") {
			t.Errorf("parseDocument(%s) = %q, want it to say int64 is the limit and name a wrapper that holds more", document, err)
		}
	}
	// A decimal point or an exponent is a double however large, and stays one.
	got, err := parseDocument(`{big: 18446744073709551615.0, e: 1e30}`)
	if err != nil {
		t.Fatalf("a double written with a point or exponent is not an integer: %v", err)
	}
	if _, isDouble := got["big"].(float64); !isDouble {
		t.Errorf("18446744073709551615.0 parsed as %T, want float64", got["big"])
	}
}

// TestParseDocumentRefusesTextAfterTheDocument: the BSON reader stops after the
// first value and reports success, so `{a: 1} junk` would have been sent as {a: 1}.
func TestParseDocumentRefusesTextAfterTheDocument(t *testing.T) {
	for _, document := range []string{`{"a": 1} junk`, `{"a": 1}{"b": 2}`} {
		if _, err := parseDocument(document); err == nil {
			t.Errorf("parseDocument(%s) was accepted", document)
		}
	}
}

// TestCLIOptionNumbersStillParse guards the split: options and DDL documents
// stay JSON, so their numbers are float64 and every reader of them keeps
// working. Swap parseOptionDocument for parseDocument and it fails, because a
// limit, a sort direction and a factor arrive as int32.
func TestCLIOptionNumbersStillParse(t *testing.T) {
	options, err := parseOptionDocument(`{limit: 5, skip: 2, sort: {a: -1}, projection: {a: 1}, replication_factor: 3}`)
	if err != nil {
		t.Fatalf("parseOptionDocument: %v", err)
	}
	if options["limit"] != float64(5) || options["replication_factor"] != float64(3) {
		t.Errorf("option numbers parsed as %#v, want float64", options)
	}

	plan, err := planFind([]string{`{a: 1}`, `{limit: 5, skip: 2, sort: {a: -1}, projection: {a: 1}}`})
	if err != nil {
		t.Fatalf("planFind: %v", err)
	}
	if plan.limit != 5 || plan.skip != 2 || plan.sort["a"] != -1 || plan.projection["a"] != 1 {
		t.Errorf("planFind read the options as limit %d skip %d sort %v projection %v, want 5 2 map[a:-1] map[a:1]", plan.limit, plan.skip, plan.sort, plan.projection)
	}

	command, err := ParseCommand(`create bucket Sessions { type: hash, replication_factor: 3 }`)
	if err != nil {
		t.Fatalf("ParseCommand: %v", err)
	}
	schema, err := parseBucketSchema(command, "app")
	if err != nil {
		t.Fatalf("parseBucketSchema: %v", err)
	}
	if schema.ReplicationFactor != 3 {
		t.Errorf("replication_factor: 3 read as %d, want 3", schema.ReplicationFactor)
	}
}

// TestCLIFieldListLimitStillParses: list({limit: 2}) on a field store reads
// its option as JSON; it must keep returning two entries of three.
func TestCLIFieldListLimitStillParses(t *testing.T) {
	c := newDumpTestServer(t)
	mustSend(t, c, transport.OpCreateDB, &server.CreateDatabaseRequest{Name: "app"}, nil)
	mustSend(t, c, transport.OpCreateBucket, &server.CreateBucketRequest{Database: "app", Name: "Post", Type: server.BucketTypeHash, Options: map[string]interface{}{
		"stores": []interface{}{map[string]interface{}{"name": "Tags", "engine": "hashStore"}},
	}}, nil)
	mustSend(t, c, transport.OpPut, &server.InsertRequest{Database: "app", Bucket: "Post", Key: "p1", Data: map[string]interface{}{"title": "p1"}}, nil)
	for _, element := range []string{"a", "b", "c"} {
		if _, err := runStatement(t, c, `db.Post["p1"].Tags.put("`+element+`", {n: 1})`); err != nil {
			t.Fatalf("put %s: %v", element, err)
		}
	}
	printed, err := runStatement(t, c, `db.Post["p1"].Tags.list({limit: 2})`)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(printed), "\n")
	if len(lines) != 3 || !strings.HasPrefix(lines[2], "(more:") {
		t.Errorf("list({limit: 2}) printed %q, want two entries and a (more:) hint", printed)
	}
}

// TestDefaultPrinterRoundTripsEveryType is spec 7.2 item 6: what the default
// printer shows for the types that have a wrapper is a wrapper the CLI reads
// back. Before, a UUID printed as {"Subtype":4,"Data":"..."}, a timestamp as
// {"T":1,"I":2} and a regular expression as {"Pattern":...}, none of them
// something a developer can type into a filter.
func TestDefaultPrinterRoundTripsEveryType(t *testing.T) {
	uuid := wrapperCases[5].want.(primitive.Binary)
	values := map[string]interface{}{
		"binary":    primitive.Binary{Subtype: 0, Data: []byte("hi")},
		"userBytes": primitive.Binary{Subtype: 0x80, Data: []byte{1, 2, 3}},
		"shortUUID": primitive.Binary{Subtype: 4, Data: []byte{1, 2, 3}},
		"uuid":      uuid,
		"oid":       mustObjectID("507f1f77bcf86cd799439011"),
		"decimal":   mustDecimal("12.34"),
		"regex":     primitive.Regex{Pattern: "^a", Options: "i"},
		"timestamp": primitive.Timestamp{T: 1700000000, I: 3},
		"int":       int32(36),
		"long":      int64(3000000000),
		"text":      "ada",
		"nested":    map[string]interface{}{"uuid": uuid, "oid": mustObjectID("507f1f77bcf86cd799439011")},
		"list":      primitive.A{uuid, mustDecimal("1.5"), int32(7)},
	}
	encoded, err := bson.Marshal(values)
	if err != nil {
		t.Fatalf("bson.Marshal: %v", err)
	}
	printed := jsonLine(bson.Raw(encoded), false)
	for _, shape := range []string{`"$uuid"`, `"$oid"`, `"$numberDecimal"`, `"$regularExpression"`, `"$timestamp"`, `"$binary"`} {
		if !strings.Contains(printed, shape) {
			t.Errorf("the default printer shows no %s wrapper:\n%s", shape, printed)
		}
	}
	reparsed, err := parseDocument(printed)
	if err != nil {
		t.Fatalf("the printed line does not parse back: %v\n%s", err, printed)
	}
	if !reflect.DeepEqual(reparsed, values) {
		for name, want := range values {
			if !reflect.DeepEqual(reparsed[name], want) {
				t.Errorf("%s: printed and parsed back as %#v (%T), want %#v (%T)", name, reparsed[name], reparsed[name], want, want)
			}
		}
	}
}

// storedElementTypes inserts one document through the CLI's own insert handler
// and returns the BSON element types the server stored for it.
func storedElementTypes(t *testing.T, statement string) bson.Raw {
	t.Helper()
	c := newDumpTestServer(t)
	mustSend(t, c, transport.OpCreateDB, &server.CreateDatabaseRequest{Name: "app"}, nil)
	mustSend(t, c, transport.OpCreateBucket, &server.CreateBucketRequest{Database: "app", Name: "Q", Type: server.BucketTypeHash}, nil)
	if _, err := runStatement(t, c, statement); err != nil {
		t.Fatalf("%s: %v", statement, err)
	}
	var found server.FindResponse
	mustSend(t, c, transport.OpGet, &server.FindRequest{Database: "app", Bucket: "Q", Key: "d"}, &found)
	if !found.Found {
		t.Fatalf("the document was not stored: %s", found.Message)
	}
	return found.Data
}

// TestCLIIntegerLiteralIsSentAsAnInteger checks end to end that what the
// server stores, not what the parser returned.
func TestCLIIntegerLiteralIsSentAsAnInteger(t *testing.T) {
	stored := storedElementTypes(t, `db.Q.insert({_id_: "d", Age: 36, Big: 3000000000, F: 36.0})`)
	for field, want := range map[string]bsontype.Type{"Age": bson.TypeInt32, "Big": bson.TypeInt64, "F": bson.TypeDouble} {
		if got := stored.Lookup(field).Type; got != want {
			t.Errorf("%s was stored as %s, want %s", field, got, want)
		}
	}
}

// TestCLIRevivesEveryExtendedJSONWrapper checks end to end that every wrapper is revived.
func TestCLIRevivesEveryExtendedJSONWrapper(t *testing.T) {
	var fields []string
	wantTypes := map[string]bsontype.Type{
		"$numberInt": bson.TypeInt32, "$numberLong": bson.TypeInt64, "$numberDouble": bson.TypeDouble,
		"$numberDecimal": bson.TypeDecimal128, "$binary": bson.TypeBinary, "$uuid": bson.TypeBinary,
		"$oid": bson.TypeObjectID, "$regularExpression": bson.TypeRegex, "$timestamp": bson.TypeTimestamp, "$date": bson.TypeDateTime,
	}
	for index, tc := range wrapperCases {
		fields = append(fields, `F`+string(rune('a'+index))+`: `+tc.document)
	}
	stored := storedElementTypes(t, `db.Q.insert({_id_: "d", `+strings.Join(fields, ", ")+`})`)
	for index, tc := range wrapperCases {
		field := `F` + string(rune('a'+index))
		if got := stored.Lookup(field).Type; got != wantTypes[tc.name] {
			t.Errorf("%s was stored as %s, want %s", tc.name, got, wantTypes[tc.name])
		}
	}
	if subtype, _ := stored.Lookup("Ff").Binary(); subtype != 4 {
		t.Errorf("$uuid was stored with subtype %d, want 4", subtype)
	}
}
