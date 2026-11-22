package server

import (
	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/x/bsonx/bsoncore"
)

// The raw resolver the server read stored bytes with before the library's
// Document.Lookup replaced it (bson-codec-spec 5.2, build doc step 7a). It
// has no caller in the server any more; it stays here as the oracle of the
// resolver differential (bson_resolver_differential_test.go) and of the
// step 7a differentials, which compare the new readers with the old ones.

// resolveRawPath is the raw resolver, for readers that hold only stored bytes
// (the search residual and the sort size estimate). It walks bsoncore itself:
// the library's own multi-key lookup walks an array by a name segment, so
// Lines.0.Sku would mean Lines[0].Sku, and it errors instead of answering
// "absent" when a non-document is on the way (spec §4.9.6, K4).
func resolveRawPath(doc bson.Raw, p fieldPath) (bson.RawValue, bool) {
	if p.segs == nil {
		value, found := rawMember(bsoncore.Document(doc), p.text)
		return bson.RawValue{Type: value.Type, Value: value.Data}, found
	}
	current := bsoncore.Value{Type: bson.TypeEmbeddedDocument, Data: doc}
	for _, seg := range p.segs {
		var found bool
		switch {
		case seg.isIndex && current.Type == bson.TypeArray:
			current, found = rawPosition(bsoncore.Document(current.Data), seg.index)
		case !seg.isIndex && current.Type == bson.TypeEmbeddedDocument:
			current, found = rawMember(bsoncore.Document(current.Data), seg.name)
		}
		if !found {
			return bson.RawValue{}, false
		}
	}
	return bson.RawValue{Type: current.Type, Value: current.Data}, true
}

// rawMember is the value of the element named name in doc. A malformed
// document has no members.
func rawMember(doc bsoncore.Document, name string) (bsoncore.Value, bool) {
	var member bsoncore.Value
	found := false
	walkRawElements(doc, func(position int, element bsoncore.Element) bool {
		if string(element.KeyBytes()) == name {
			member, found = element.Value(), true
			return false
		}
		return true
	})
	return member, found
}

// rawPosition is the element at a 0-based position of an array. An array's
// elements are counted, not looked up by key, so a key out of sequence cannot
// make a position mean another element.
func rawPosition(array bsoncore.Document, index int) (bsoncore.Value, bool) {
	var element bsoncore.Value
	found := false
	walkRawElements(array, func(position int, candidate bsoncore.Element) bool {
		if position == index {
			element, found = candidate.Value(), true
			return false
		}
		return true
	})
	return element, found
}

// walkRawElements visits the elements of a document in order until visit
// returns false. It stops quietly at the first malformed element.
func walkRawElements(doc bsoncore.Document, visit func(position int, element bsoncore.Element) bool) {
	length, _, ok := bsoncore.ReadLength(doc)
	if !ok || length < 5 || int(length) > len(doc) {
		return
	}
	remaining := doc[4 : length-1]
	for position := 0; len(remaining) > 0; position++ {
		element, rest, ok := bsoncore.ReadElement(remaining)
		if !ok || !visit(position, element) {
			return
		}
		remaining = rest
	}
}
