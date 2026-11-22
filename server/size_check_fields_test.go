package server

import (
	"fmt"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"
)

// The size checks read a write without the codec (size_check_fields.go). This
// holds them to it: every payload below is decoded both ways, into the
// handler's own request type and by the check's reader, and wherever the
// handler's decode succeeds the two must agree on every field a check reads.

// fieldNameForms is a field's key as written, and forms the codec may or may
// not decode into it: other letter cases, and look-alike or case-folding
// characters outside ASCII.
func fieldNameForms(key string) []string {
	forms := []string{key, strings.ToUpper(key), strings.ToUpper(key[:1]) + key[1:], key + "_", " " + key}
	mixed := []byte(key)
	for position := range mixed {
		if position%2 == 1 {
			mixed[position] = byte(strings.ToUpper(string(mixed[position]))[0])
		}
	}
	forms = append(forms, string(mixed))
	for _, swap := range [][2]string{{"k", "K"}, {"s", "ſ"}, {"a", "а"}, {"e", "Е"}, {"t", "Ｔ"}} {
		if strings.Contains(key, swap[0]) {
			forms = append(forms, strings.Replace(key, swap[0], swap[1], 1))
		}
	}
	return forms
}

var (
	parityStrings   = []any{"text", primitive.Symbol("symbol"), primitive.NewObjectID(), primitive.Binary{Data: []byte("binary")}, primitive.Binary{Subtype: 2, Data: []byte("old")}, primitive.Binary{Subtype: 5, Data: []byte("other")}, nil, primitive.Undefined{}, int32(7), ""}
	parityDocuments = []any{bson.D{{Key: "a", Value: int32(1)}}, bson.D{{Key: "a", Value: "longer"}, {Key: "b", Value: bson.D{{Key: "c", Value: true}}}}, bson.D{}, nil, primitive.Undefined{}, int32(7), "text"}
	parityBytes     = []any{primitive.Binary{Data: make([]byte, 10)}, primitive.Binary{Subtype: 2, Data: make([]byte, 7)}, primitive.Binary{Subtype: 5, Data: make([]byte, 3)}, "text", primitive.Symbol("symbol"), nil, primitive.Undefined{}, int32(7)}
	parityArrays    = []any{bson.A{bson.D{{Key: "a", Value: int32(1)}}, bson.D{{Key: "b", Value: "two"}}}, bson.A{}, bson.A{bson.D{{Key: "a", Value: "x"}}, nil}, nil, bson.D{{Key: "a", Value: int32(1)}}, "text"}
)

// parityPayloads is every payload built from one key's forms and values: each
// form with each value, and each pair of them in order, so a repeated field is
// covered for every combination of the two forms and values.
func parityPayloads(key string, values []any) []bson.D {
	var single []bson.E
	for _, form := range fieldNameForms(key) {
		for _, value := range values {
			single = append(single, bson.E{Key: form, Value: value})
		}
	}
	var payloads []bson.D
	for _, first := range single {
		payloads = append(payloads, bson.D{first})
		for _, second := range single {
			if first.Key != second.Key || fmt.Sprint(first.Value) != fmt.Sprint(second.Value) {
				payloads = append(payloads, bson.D{first, second})
			}
		}
	}
	return payloads
}

func marshaledLength(t *testing.T, document map[string]interface{}) int {
	t.Helper()
	encoded, err := bson.Marshal(document)
	if err != nil {
		t.Fatalf("encoding a decoded document: %v", err)
	}
	return len(encoded)
}

// requireSameDocument fails unless the reader's document is the handler's map.
func requireSameDocument(t *testing.T, payload bson.D, handlers map[string]interface{}, reader measuredDocument) {
	t.Helper()
	document, isDocument := reader.document()
	if (handlers != nil) != isDocument {
		t.Fatalf("%v: the handler decodes data %v, the size check finds a document: %v", payload, handlers, isDocument)
	}
	if isDocument && marshaledLength(t, handlers) != len(document) {
		t.Fatalf("%v: the handler's document encodes to %d bytes, the size check measures %d", payload, marshaledLength(t, handlers), len(document))
	}
}

func requireSameString(t *testing.T, payload bson.D, field, handlers string, reader measuredString) {
	t.Helper()
	if handlers != reader.text {
		t.Fatalf("%v: the handler decodes %s %q, the size check reads %q", payload, field, handlers, reader.text)
	}
}

func requireSameBytes(t *testing.T, payload bson.D, field string, handlers []byte, reader measuredBytes) {
	t.Helper()
	if len(handlers) != reader.length {
		t.Fatalf("%v: the handler decodes %d bytes of %s, the size check measures %d", payload, len(handlers), field, reader.length)
	}
}

// decodeBoth decodes payload into the handler's request and reports whether
// the handler would accept it; read runs the size check's reader.
func decodeBoth(t *testing.T, payload bson.D, request any, read func([]byte)) bool {
	t.Helper()
	encoded := mustMarshalBSON(t, payload)
	read(encoded)
	return bson.Unmarshal(encoded, request) == nil
}

func TestSizeChecksReadFieldsAsTheHandlersDecodeThem(t *testing.T) {
	t.Run("insert", func(t *testing.T) {
		check := func(payload bson.D) {
			var request InsertRequest
			var reader insertFields
			if !decodeBoth(t, payload, &request, reader.read) {
				return
			}
			requireSameDocument(t, payload, request.Data, reader.data)
			requireSameString(t, payload, "key", request.Key, reader.key)
			requireSameString(t, payload, "bucket", request.Bucket, reader.bucket)
			requireSameString(t, payload, "database", request.Database, reader.database)
		}
		for _, payload := range parityPayloads("data", parityDocuments) {
			check(payload)
		}
		for _, key := range []string{"key", "bucket", "database"} {
			for _, payload := range parityPayloads(key, parityStrings) {
				check(payload)
			}
		}
	})
	t.Run("push", func(t *testing.T) {
		check := func(payload bson.D) {
			var request PushRequest
			var reader pushFields
			if !decodeBoth(t, payload, &request, reader.read) {
				return
			}
			requireSameDocument(t, payload, request.Data, reader.data)
			requireSameString(t, payload, "bucket", request.Bucket, reader.bucket)
		}
		for _, payload := range parityPayloads("data", parityDocuments) {
			check(payload)
		}
		for _, payload := range parityPayloads("bucket", parityStrings) {
			check(payload)
		}
	})
	t.Run("batch", func(t *testing.T) {
		check := func(payload bson.D) {
			var request BatchInsertRequest
			var reader batchFields
			if !decodeBoth(t, payload, &request, reader.read) {
				return
			}
			var handlers, measured []int
			for _, document := range request.Data {
				if document != nil {
					handlers = append(handlers, marshaledLength(t, document))
				}
			}
			for _, element := range readableElements(reader.data.array) {
				if document, isDocument := element.Value().DocumentOK(); isDocument {
					measured = append(measured, len(document))
				}
			}
			if fmt.Sprint(handlers) != fmt.Sprint(measured) {
				t.Fatalf("%v: the handler's documents encode to %v bytes, the size check measures %v", payload, handlers, measured)
			}
			requireSameString(t, payload, "bucket", request.Bucket, reader.bucket)
		}
		for _, payload := range parityPayloads("data", parityArrays) {
			check(payload)
		}
		for _, payload := range parityPayloads("bucket", parityStrings) {
			check(payload)
		}
	})
	t.Run("blob write", func(t *testing.T) {
		check := func(payload bson.D) {
			var request BlobWriteRequest
			var reader blobWriteFields
			if !decodeBoth(t, payload, &request, reader.read) {
				return
			}
			requireSameBytes(t, payload, "data", request.Data, reader.data)
			requireSameString(t, payload, "key", request.Key, reader.key)
			requireSameString(t, payload, "bucket", request.Bucket, reader.bucket)
		}
		for _, payload := range parityPayloads("data", parityBytes) {
			check(payload)
		}
		for _, key := range []string{"key", "bucket"} {
			for _, payload := range parityPayloads(key, parityStrings) {
				check(payload)
			}
		}
	})
	t.Run("field element", func(t *testing.T) {
		check := func(payload bson.D) {
			var request FieldRequest
			var reader fieldElementFields
			if !decodeBoth(t, payload, &request, reader.read) {
				return
			}
			requireSameDocument(t, payload, request.Data, reader.data)
			requireSameBytes(t, payload, "content", request.Content, reader.content)
			requireSameString(t, payload, "element", request.Element, reader.element)
			requireSameString(t, payload, "bucket", request.Bucket, reader.bucket)
		}
		for _, payload := range parityPayloads("data", parityDocuments) {
			check(payload)
		}
		for _, payload := range parityPayloads("content", parityBytes) {
			check(payload)
		}
		for _, key := range []string{"element", "bucket"} {
			for _, payload := range parityPayloads(key, parityStrings) {
				check(payload)
			}
		}
	})
}
