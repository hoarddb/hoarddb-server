package store

import (
	"fmt"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/bsontype"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"
)

// DecodeStoredValue decodes one stored value as a decode of the whole
// document into map[string]interface{} gives it: a document inside it
// decodes to map[string]interface{} and an array to primitive.A, at every
// level, where a decode of the value on its own would give a document as
// primitive.D. The paths that read single fields of stored bytes, instead of
// decoding the whole document, use it so that every value they hand on is
// the one the whole-document decode gave (bson-codec-spec 7.2).
func DecodeStoredValue(valueType bsontype.Type, valueBytes []byte) (interface{}, error) {
	switch valueType {
	case bsontype.EmbeddedDocument:
		var members map[string]interface{}
		if err := bson.Unmarshal(valueBytes, &members); err != nil {
			return nil, fmt.Errorf("decoding a stored document value: %w", err)
		}
		return members, nil
	case bsontype.Array:
		values, err := bson.Raw(valueBytes).Values()
		if err != nil {
			return nil, fmt.Errorf("reading a stored array value: %w", err)
		}
		elements := make(primitive.A, len(values))
		for position, element := range values {
			if elements[position], err = DecodeStoredValue(element.Type, element.Value); err != nil {
				return nil, err
			}
		}
		return elements, nil
	case bsontype.CodeWithScope:
		return decodeStoredCodeWithScope(valueBytes)
	}
	var decoded interface{}
	if err := (bson.RawValue{Type: valueType, Value: valueBytes}).Unmarshal(&decoded); err != nil {
		return nil, fmt.Errorf("decoding a stored %s value: %w", valueType, err)
	}
	return decoded, nil
}

// decodeStoredCodeWithScope decodes a stored code-with-scope as the
// whole-document decode gives it: the scope is a primitive.D, and the values
// inside it are decoded under the map's rules, so a document in the scope is a
// map[string]interface{}. A decode of the value on its own would make it a
// primitive.D.
func decodeStoredCodeWithScope(valueBytes []byte) (interface{}, error) {
	code, scope, ok := bson.RawValue{Type: bsontype.CodeWithScope, Value: valueBytes}.CodeWithScopeOK()
	if !ok {
		return nil, fmt.Errorf("reading a stored code-with-scope value: it is not one")
	}
	elements, err := scope.Elements()
	if err != nil {
		return nil, fmt.Errorf("reading the scope of a stored code-with-scope value: %w", err)
	}
	members := make(primitive.D, len(elements))
	for position, element := range elements {
		value := element.Value()
		decoded, err := DecodeStoredValue(value.Type, value.Value)
		if err != nil {
			return nil, err
		}
		members[position] = primitive.E{Key: element.Key(), Value: decoded}
	}
	return primitive.CodeWithScope{Code: primitive.JavaScript(code), Scope: members}, nil
}
