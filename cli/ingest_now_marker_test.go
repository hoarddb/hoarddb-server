package cli

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"
)

// TestOptionsDocumentRefusesNowCall: now() is a value in a stored or matched
// document; an options or DDL document has no such place, and the placeholder
// the reader writes for a call must never reach the server as a key.
func TestOptionsDocumentRefusesNowCall(t *testing.T) {
	document, err := parseOptionDocument(`{limit: now()}`)
	if err == nil {
		t.Fatalf("parseOptionDocument accepted now() and returned %#v", document)
	}
	if !strings.Contains(err.Error(), "now()") || strings.Contains(err.Error(), "$$now-call") {
		t.Errorf("error %q should name now() and not the internal placeholder", err)
	}
}

// TestIngestRefusesTheNowMarkerNamespace is BUG-0151 on the ingest path: a
// DBPointer in the namespace the server reserves for now() would be stored as
// the time of the write, so ingest refuses it, in both formats, at the top
// level and inside an array, and names the field. An ordinary DBPointer from a
// mongodump still loads.
func TestIngestRefusesTheNowMarkerNamespace(t *testing.T) {
	marker := primitive.DBPointer{DB: "$$hoardDB.now", Pointer: primitive.ObjectID{1}}
	ordinary := primitive.DBPointer{DB: "app.users", Pointer: primitive.ObjectID{2}}
	bsonOf := func(document bson.D) []byte {
		encoded, err := bson.Marshal(document)
		if err != nil {
			t.Fatal(err)
		}
		return encoded
	}
	const jsonMarker = `{"$dbPointer":{"$ref":"$$hoardDB.now","$id":{"$oid":"000000000000000000000000"}}}`
	const jsonOrdinary = `{"$dbPointer":{"$ref":"app.users","$id":{"$oid":"000000000000000000000002"}}}`

	cases := []struct {
		name      string
		read      func() documentReader
		wantField string
		wantLoads bool
	}{
		{"bson top level", func() documentReader {
			return newBSONReader(bytes.NewReader(bsonOf(bson.D{{Key: "n", Value: marker}})), readerStart{name: "d.bson", totalBytes: -1})
		}, `"n"`, false},
		{"bson nested in an array", func() documentReader {
			return newBSONReader(bytes.NewReader(bsonOf(bson.D{{Key: "list", Value: bson.A{int32(1), marker}}})), readerStart{name: "d.bson", totalBytes: -1})
		}, `"list.1"`, false},
		{"bson ordinary pointer", func() documentReader {
			return newBSONReader(bytes.NewReader(bsonOf(bson.D{{Key: "n", Value: ordinary}})), readerStart{name: "d.bson", totalBytes: -1})
		}, "", true},
		{"jsonl top level", func() documentReader {
			return newJSONLReader(strings.NewReader(`{"n":`+jsonMarker+"}\n"), readerStart{name: "d.jsonl", totalBytes: -1})
		}, `"n"`, false},
		{"jsonl nested in an array", func() documentReader {
			return newJSONLReader(strings.NewReader(`{"list":[1,`+jsonMarker+"]}\n"), readerStart{name: "d.jsonl", totalBytes: -1})
		}, `"list.1"`, false},
		{"jsonl ordinary pointer", func() documentReader {
			return newJSONLReader(strings.NewReader(`{"n":`+jsonOrdinary+"}\n"), readerStart{name: "d.jsonl", totalBytes: -1})
		}, "", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := c.read().Next()
			if c.wantLoads {
				if err != nil {
					t.Fatalf("an ordinary DBPointer was refused: %v", err)
				}
				return
			}
			var failure *documentFailure
			if !errors.As(err, &failure) {
				t.Fatalf("Next() = %v; want a per-document refusal of the reserved namespace", err)
			}
			for _, want := range []string{c.wantField, "$$hoardDB.now", "now()"} {
				if !strings.Contains(failure.Reason, want) {
					t.Errorf("reason %q does not mention %s", failure.Reason, want)
				}
			}
		})
	}
}
