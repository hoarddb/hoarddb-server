package server

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"
)

// TestPathGrammar is spec §11.11's grammar list: three paths parse into the
// segments they name, and every malformed key is refused with E-14's grammar
// variant, wrapped in ErrPathRefused.
func TestPathGrammar(t *testing.T) {
	parsed := []struct {
		key  string
		want []pathSeg
	}{
		{"Address.City", []pathSeg{{name: "Address"}, {name: "City"}}},
		{"Lines[3].Qty", []pathSeg{{name: "Lines"}, {index: 3, isIndex: true}, {name: "Qty"}}},
		{"Scores[0][2]", []pathSeg{{name: "Scores"}, {index: 0, isIndex: true}, {index: 2, isIndex: true}}},
		{"Orders[0].Lines[10].Sku", []pathSeg{{name: "Orders"}, {index: 0, isIndex: true}, {name: "Lines"}, {index: 10, isIndex: true}, {name: "Sku"}}},
	}
	for _, accepted := range parsed {
		path, err := parseFieldPath(accepted.key)
		if err != nil {
			t.Errorf("parseFieldPath(%q) refused a path: %v", accepted.key, err)
			continue
		}
		if !reflect.DeepEqual(path.segs, accepted.want) {
			t.Errorf("parseFieldPath(%q) = %+v, want %+v", accepted.key, path.segs, accepted.want)
		}
		if path.text != accepted.key {
			t.Errorf("parseFieldPath(%q).text = %q", accepted.key, path.text)
		}
	}

	segments := make([]string, maxPathSegments+1)
	for position := range segments {
		segments[position] = "a"
	}
	refused := []struct {
		key, reason string
	}{
		{"Address..City", "empty name"},
		{"Lines[-1]", "sign"},
		{"Lines[01]", "leading zero"},
		{"Lines[x]", "whole number"},
		{"Lines[]", "empty position"},
		{"Lines[3", "no closing"},
		{"Lines]3", `"]"`},
		{".City", "empty name"},
		{"City.", "empty name"},
		{"$set.x", `starts with "$"`},
		{"Address.$x", `starts with "$"`},
		{"_id_.x", "_id_"},
		{"_id_[0]", "_id_"},
		{"a\x00b.c", "NUL"},
		{strings.Join(segments, "."), "at most 100"},
	}
	for _, malformed := range refused {
		_, err := parseFieldPath(malformed.key)
		if err == nil {
			t.Errorf("parseFieldPath(%q) accepted a malformed path", malformed.key)
			continue
		}
		if !errors.Is(err, ErrPathRefused) {
			t.Errorf("parseFieldPath(%q) refusal is not an ErrPathRefused: %v", malformed.key, err)
		}
		message := err.Error()
		if !strings.Contains(message, "is not a path: ") || !strings.Contains(message, malformed.reason) {
			t.Errorf("parseFieldPath(%q) = %q; want E-14's grammar variant naming %q", malformed.key, message, malformed.reason)
		}
	}

	exactly100, err := parseFieldPath(strings.Join(segments[:maxPathSegments], "."))
	if err != nil || len(exactly100.segs) != maxPathSegments {
		t.Errorf("a path of exactly %d segments was refused or mis-split: %v", maxPathSegments, err)
	}
}

// TestBracketsAndDotsCannotBeConfused: Address.3 names a member called "3";
// Lines[3] names position 3. The same document answers each by its own rule.
func TestBracketsAndDotsCannotBeConfused(t *testing.T) {
	member, err := parseFieldPath("Address.3")
	if err != nil {
		t.Fatalf("parseFieldPath(Address.3): %v", err)
	}
	if want := []pathSeg{{name: "Address"}, {name: "3"}}; !reflect.DeepEqual(member.segs, want) {
		t.Fatalf("Address.3 = %+v, want a member named 3: %+v", member.segs, want)
	}
	position, err := parseFieldPath("Lines[3]")
	if err != nil {
		t.Fatalf("parseFieldPath(Lines[3]): %v", err)
	}
	if want := []pathSeg{{name: "Lines"}, {index: 3, isIndex: true}}; !reflect.DeepEqual(position.segs, want) {
		t.Fatalf("Lines[3] = %+v, want a position: %+v", position.segs, want)
	}

	document := map[string]interface{}{
		"Address": map[string]interface{}{"3": "member three"},
		"Lines":   primitive.A{"zero", "one", "two", "three"},
	}
	if got, found := resolvePath(document, member); !found || got != "member three" {
		t.Errorf("Address.3 resolved to %v (found %v), want the member named 3", got, found)
	}
	if got, found := resolvePath(document, position); !found || got != "three" {
		t.Errorf("Lines[3] resolved to %v (found %v), want element 3", got, found)
	}
	// A name segment on an array is absent: Lines.3 is not Lines[3].
	if got, found := resolvePath(document, mustParsePath(t, "Lines.3")); found {
		t.Errorf("Lines.3 resolved to %v; a name segment on an array is absent", got)
	}
	// And a position on an object is absent: Address[3] is not Address.3.
	if got, found := resolvePath(document, mustParsePath(t, "Address[3]")); found {
		t.Errorf("Address[3] resolved to %v; a position on an object is absent", got)
	}
}

// TestUndottedKeyTakesTheOneLookupPath: a key without "." or "[" is parsed and
// resolved with one map lookup and no allocation (spec §4.9.5).
func TestUndottedKeyTakesTheOneLookupPath(t *testing.T) {
	document := map[string]interface{}{"Age": int32(36), "Name": "Ana"}
	var resolved interface{}
	allocations := minAllocs(7, 1000, func() {
		path, err := parseFieldPath("Age")
		if err != nil {
			t.Fatalf("parseFieldPath(Age): %v", err)
		}
		if !path.simple() {
			t.Fatal("an undotted key is not a simple path")
		}
		resolved, _ = resolvePath(document, path)
	})
	if allocations != 0 {
		t.Errorf("parsing and resolving an undotted key allocated %.1f times per run, want 0", allocations)
	}
	if resolved != int32(36) {
		t.Errorf("Age resolved to %v, want 36", resolved)
	}
	path := mustParsePath(t, "Age")
	if path.segs != nil {
		t.Errorf("an undotted key allocated a segment slice: %+v", path.segs)
	}
}

// resolverCase is one row of the table both resolvers run (T-12).
type resolverCase struct {
	path  string
	found bool
	// want is the decoded value at the path; nil with found means a value
	// whose shape is compared by the decoded resolver only (a document).
	want interface{}
}

// resolverDocument is the stored document TestResolversAgree reads.
var resolverDocument = map[string]interface{}{
	"Name": "Ana",
	"Address": map[string]interface{}{
		"City": "Tirana",
		"Geo":  map[string]interface{}{"Lat": 41.33},
	},
	"Lines": primitive.A{
		map[string]interface{}{"Sku": "A-1", "Qty": int32(2)},
		map[string]interface{}{"Sku": "B-2", "Qty": int64(5)},
	},
	"Scores":  primitive.A{primitive.A{int32(1), int32(2)}, primitive.A{int32(3)}},
	"Text":    "not a document",
	"Nothing": nil,
}

// resolverCases is the shared table. Every absent row is absent for a reason
// the spec names: a missing member, a name segment on an array, a position on
// an object or past the end, a path through a scalar or null.
var resolverCases = []resolverCase{
	{path: "Name", found: true, want: "Ana"},
	{path: "Address.City", found: true, want: "Tirana"},
	{path: "Address.Geo.Lat", found: true, want: 41.33},
	{path: "Lines[0].Sku", found: true, want: "A-1"},
	{path: "Lines[1].Qty", found: true, want: int64(5)},
	{path: "Scores[0][1]", found: true, want: int32(2)},
	{path: "Scores[1][0]", found: true, want: int32(3)},
	{path: "Nothing", found: true, want: nil},
	{path: "Address.Zip", found: false},
	{path: "Missing.City", found: false},
	{path: "Lines.0.Sku", found: false},
	{path: "Lines.Sku", found: false},
	{path: "Lines[2].Sku", found: false},
	{path: "Address[0]", found: false},
	{path: "Text.City", found: false},
	{path: "Text[0]", found: false},
	{path: "Nothing.City", found: false},
	{path: "Scores[1][1]", found: false},
}

// TestResolversAgree is the shared table the decoded resolver and the raw
// resolver run (spec §4.9.6, K4): every path resolves to the same value, or is
// absent, on both representations of one document. The raw resolver is the
// library's Lookup over the path as the server compiles it (libraryPath).
func TestResolversAgree(t *testing.T) {
	raw, err := bson.Marshal(resolverDocument)
	if err != nil {
		t.Fatalf("encoding the document: %v", err)
	}
	var decoded map[string]interface{}
	if err := bson.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("decoding the document: %v", err)
	}
	stored, err := bson.TrustStored(raw)
	if err != nil {
		t.Fatalf("framing the document: %v", err)
	}
	for _, row := range resolverCases {
		path := mustParsePath(t, row.path)
		decodedValue, decodedFound := resolvePath(decoded, path)
		view, rawFound, err := stored.Lookup(libraryPath(path))
		if err != nil {
			t.Fatalf("%s: Lookup: %v", row.path, err)
		}
		rawValue := bson.RawValue{Type: view.Type(), Value: view.Bytes()}
		if decodedFound != row.found || rawFound != row.found {
			t.Errorf("%s: decoded found=%v, raw found=%v, want %v", row.path, decodedFound, rawFound, row.found)
			continue
		}
		if !row.found {
			continue
		}
		var fromRaw interface{}
		if err := rawValue.Unmarshal(&fromRaw); err != nil {
			t.Errorf("%s: decoding the raw resolver's value: %v", row.path, err)
			continue
		}
		if !reflect.DeepEqual(decodedValue, row.want) || !reflect.DeepEqual(fromRaw, row.want) {
			t.Errorf("%s: decoded %#v, raw %#v, want %#v", row.path, decodedValue, fromRaw, row.want)
		}
	}
}

func mustParsePath(t testing.TB, key string) fieldPath {
	t.Helper()
	path, err := parseFieldPath(key)
	if err != nil {
		t.Fatalf("parseFieldPath(%q): %v", key, err)
	}
	return path
}

// BenchmarkPath measures a path's resolution per document. undotted must
// equal a bare map lookup within noise, with no allocation (spec §4.9.5).
func BenchmarkPath(b *testing.B) {
	document := map[string]interface{}{
		"Name": "Ana",
		"A":    map[string]interface{}{"B": map[string]interface{}{"C": int32(7)}},
		"Orders": primitive.A{map[string]interface{}{
			"Lines": primitive.A{map[string]interface{}{"Item": map[string]interface{}{"Sku": "A-1"}}},
		}},
	}
	b.Run("bare_map_lookup", func(b *testing.B) {
		b.ReportAllocs()
		var found bool
		for range b.N {
			_, found = document["Name"]
		}
		_ = found
	})
	for _, bench := range []struct{ name, key string }{
		{"undotted", "Name"},
		{"depth3_members", "A.B.C"},
		{"depth5_mixed", "Orders[0].Lines[0].Item.Sku"},
	} {
		path := mustParsePath(b, bench.key)
		b.Run(bench.name, func(b *testing.B) {
			b.ReportAllocs()
			var found bool
			for range b.N {
				_, found = resolvePath(document, path)
			}
			if !found {
				b.Fatalf("%s did not resolve", bench.key)
			}
		})
	}
}
