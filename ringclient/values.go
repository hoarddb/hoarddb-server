package ringclient

import (
	"fmt"
	"strings"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

// typedValue is a field value as the wire carries it: the BSON type byte, then the value's
// bytes.
func typedValue(value any) ([]byte, error) {
	raw, err := bson.Marshal(bson.D{{Key: "v", Value: value}})
	if err != nil {
		return nil, fmt.Errorf("encoding %T as a BSON value: %w", value, err)
	}
	member := bson.Raw(raw).Lookup("v")
	return append([]byte{byte(member.Type)}, member.Value...), nil
}

// elementBytes is the BSON document of a store element: bson.Raw and []byte pass through as
// they are, anything else is marshalled.
func elementBytes(element any) ([]byte, error) {
	switch typed := element.(type) {
	case bson.Raw:
		return typed, nil
	case []byte:
		return typed, nil
	}
	raw, err := bson.Marshal(element)
	if err != nil {
		return nil, fmt.Errorf("encoding the element (%T) as a BSON document: %w", element, err)
	}
	return raw, nil
}

// filterBytes is the BSON document of a list or count filter, or nil for none.
func filterBytes(filter any) ([]byte, error) {
	if filter == nil {
		return nil, nil
	}
	return elementBytes(filter)
}

// encodedID is the EncodeComponent encoding of a document id and its typed string value.
func encodedID(id string) (encoded, typed []byte, err error) {
	encoded, err = store.EncodeComponent(id)
	if err != nil {
		return nil, nil, fmt.Errorf("encoding document id %q: %w", id, err)
	}
	typed, err = typedValue(id)
	if err != nil {
		return nil, nil, err
	}
	return encoded, typed, nil
}

// decodeID inverts EncodeComponent for a string id; any other encoding is shown in hex.
func decodeID(encoded []byte) string {
	if len(encoded) >= 3 && encoded[0] == storage.TagString {
		body := encoded[1 : len(encoded)-2]
		return strings.ReplaceAll(string(body), "\x00\xff", "\x00")
	}
	return fmt.Sprintf("%x", encoded)
}

func isStoreType(word string) bool { return strings.HasSuffix(word, "Store") }
