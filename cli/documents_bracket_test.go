package cli

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson/primitive"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// TestCLIReadsABracketedBareKey is typed-fields T-11: `[digits]` inside a bare
// key in key position belongs to the key, so a path with a position needs no
// quotes. A `[` anywhere else still opens an array.
func TestCLIReadsABracketedBareKey(t *testing.T) {
	tests := []struct {
		name     string
		document string
		want     map[string]interface{}
	}{
		{"a position and a member", `{Lines[3].Qty: 2}`, map[string]interface{}{"Lines[3].Qty": int32(2)}},
		{"two positions", `{Scores[0][2]: 7}`, map[string]interface{}{"Scores[0][2]": int32(7)}},
		{"a trailing position", `{Tags[1]: "x"}`, map[string]interface{}{"Tags[1]": "x"}},
		{"after a comma", `{a: 1, Lines[0].Sku: "A"}`, map[string]interface{}{"a": int32(1), "Lines[0].Sku": "A"}},
		{"in a nested document", `{$set: {Lines[0].Qty: 4}}`, map[string]interface{}{"$set": map[string]interface{}{"Lines[0].Qty": int32(4)}}},
		{"quoted still works", `{"Lines[0].Qty": 3}`, map[string]interface{}{"Lines[0].Qty": int32(3)}},
		{"an array value is still an array", `{a: [1, 2]}`, map[string]interface{}{"a": primitive.A{int32(1), int32(2)}}},
		{"a key and an array value", `{Lines[0].Tags: [x, y]}`, map[string]interface{}{"Lines[0].Tags": primitive.A{"x", "y"}}},
		{"a key after an array value", `{a: [1], b[0]: 3}`, map[string]interface{}{"a": primitive.A{int32(1)}, "b[0]": int32(3)}},
		{"an array of documents", `{a: [{Lines[1].Qty: 1}]}`, map[string]interface{}{"a": primitive.A{map[string]interface{}{"Lines[1].Qty": int32(1)}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := parseDocument(test.document)
			if err != nil {
				t.Fatalf("parseDocument(%s): %v", test.document, err)
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Errorf("parseDocument(%s) = %#v, want %#v", test.document, got, test.want)
			}
		})
	}

	// Only digits make a position: a word in brackets is still a syntax error,
	// not a key that quietly holds the brackets. A value is not a key either:
	// {a: b[1]} stays an error, so a bracket after a bare value is never swallowed.
	for _, document := range []string{`{Lines[x].Qty: 2}`, `{Lines[3.Qty: 2}`, `{Lines[].Qty: 2}`, `{a: b[1]}`, `{a: [{x: 1}, b[1]]}`} {
		if got, err := parseDocument(document); err == nil {
			t.Errorf("parseDocument(%s) = %#v, want an error", document, got)
		}
	}
}

// TestCLIUpdatesAnUnquotedBracketedPath is T-11 through the statement parser
// and the server: a path with a position, typed without quotes, writes the
// element it names.
func TestCLIUpdatesAnUnquotedBracketedPath(t *testing.T) {
	c := newBucketQ(t)
	mustSend(t, c, transport.OpPut, &server.InsertRequest{Database: "app", Bucket: "Q", Key: "d", Data: map[string]interface{}{
		"Lines": []interface{}{map[string]interface{}{"Sku": "A", "Qty": int32(1)}, map[string]interface{}{"Sku": "B", "Qty": int32(1)}},
	}}, nil)
	if _, err := runStatement(t, c, `db.Q.update("d", {$set: {Lines[1].Qty: 7}, $inc: {Lines[0].Qty: 2}})`); err != nil {
		t.Fatalf("update with unquoted bracketed paths: %v", err)
	}
	if got, want := fmt.Sprint(storedDocument(t, c, "d")["Lines"]), "[map[Qty:3 Sku:A] map[Qty:7 Sku:B]]"; got != want {
		t.Errorf("Lines = %s, want %s", got, want)
	}
}
