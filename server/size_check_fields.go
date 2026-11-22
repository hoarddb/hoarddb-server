package server

import (
	"encoding/binary"
	"iter"
	"strings"
	"unicode/utf8"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/x/bsonx/bsoncore"
)

// The size checks read a write's fields the way its handler's bson.Unmarshal
// does, without decoding it: one walk over the payload's top-level elements,
// in which each element goes to the field the struct codec would decode it
// into (routedTo), and each field keeps what the handler's field of the same
// Go type would hold after the same elements, as lengths or as the encoded
// bytes inside the payload. Decoding with the codec itself would copy every
// measured value, the whole blob of a blob write included, a second time on
// every write. TestSizeChecksReadFieldsAsTheHandlersDecodeThem holds both
// halves to the codec: the routing for every guarded request type, and each
// field's value semantics.

// insertFields is what the checks read of an InsertRequest (OpPut, OpUpsert,
// OpBTreeInsert).
type insertFields struct {
	database measuredString
	bucket   measuredString
	key      measuredString
	data     measuredDocument
}

func (f *insertFields) read(payload []byte) {
	for _, element := range readableElements(payload) {
		switch name := element.KeyBytes(); {
		case routedTo(name, "data"):
			f.data.take(element.Value())
		case routedTo(name, "key"):
			f.key.take(element.Value())
		case routedTo(name, "bucket"):
			f.bucket.take(element.Value())
		case routedTo(name, "database"):
			f.database.take(element.Value())
		}
	}
}

// pushFields is what the checks read of a PushRequest, which has no key.
type pushFields struct {
	bucket measuredString
	data   measuredDocument
}

func (f *pushFields) read(payload []byte) {
	for _, element := range readableElements(payload) {
		switch name := element.KeyBytes(); {
		case routedTo(name, "data"):
			f.data.take(element.Value())
		case routedTo(name, "bucket"):
			f.bucket.take(element.Value())
		}
	}
}

// batchFields is what the checks read of a BatchInsertRequest.
type batchFields struct {
	bucket measuredString
	data   measuredDocuments
}

func (f *batchFields) read(payload []byte) {
	for _, element := range readableElements(payload) {
		switch name := element.KeyBytes(); {
		case routedTo(name, "data"):
			f.data.take(element.Value())
		case routedTo(name, "bucket"):
			f.bucket.take(element.Value())
		}
	}
}

// blobWriteFields is what the checks read of a BlobWriteRequest.
type blobWriteFields struct {
	bucket measuredString
	key    measuredString
	data   measuredBytes
}

func (f *blobWriteFields) read(payload []byte) {
	for _, element := range readableElements(payload) {
		switch name := element.KeyBytes(); {
		case routedTo(name, "data"):
			f.data.take(element.Value())
		case routedTo(name, "key"):
			f.key.take(element.Value())
		case routedTo(name, "bucket"):
			f.bucket.take(element.Value())
		}
	}
}

// fieldElementFields is what the checks read of a FieldRequest.
type fieldElementFields struct {
	bucket  measuredString
	element measuredString
	data    measuredDocument
	content measuredBytes
}

func (f *fieldElementFields) read(payload []byte) {
	for _, entry := range readableElements(payload) {
		switch name := entry.KeyBytes(); {
		case routedTo(name, "data"):
			f.data.take(entry.Value())
		case routedTo(name, "content"):
			f.content.take(entry.Value())
		case routedTo(name, "element"):
			f.element.take(entry.Value())
		case routedTo(name, "bucket"):
			f.bucket.take(entry.Value())
		}
	}
}

// routedTo reports whether the struct codec decodes an element named name into
// the field whose BSON key is field. The codec looks the name up as it is,
// and when no field has that key, lowercased with strings.ToLower (see
// StructCodec.DecodeValue in bsoncodec). Every guarded request's keys are
// lowercase, so no name the lowercasing changes is any field's own key, and
// the two lookups reduce to this one test.
func routedTo(name []byte, field string) bool {
	if string(name) == field {
		return true
	}
	if !needsLowering(name) {
		return false
	}
	return strings.ToLower(string(name)) == field
}

// needsLowering reports whether strings.ToLower could change name: it has an
// upper-case ASCII letter or a byte outside ASCII. A name it cannot change
// matched or missed on the exact test already.
func needsLowering(name []byte) bool {
	for _, character := range name {
		if character >= utf8.RuneSelf || ('A' <= character && character <= 'Z') {
			return true
		}
	}
	return false
}

// readableElements yields a document's top-level elements, with their
// positions, up to the first one that cannot be read. The handler's decode
// reads element by element too and fails at the same place, so nothing after
// an unreadable element is stored. Values are not validated: the handler's
// decode does not hold them to bsoncore's validation, so validating here could
// pass over an element the handler stores.
func readableElements(document []byte) iter.Seq2[int, bsoncore.Element] {
	return func(yield func(int, bsoncore.Element) bool) {
		length, rest, ok := bsoncore.ReadLength(document)
		if !ok || length < 5 || int(length) > len(document) {
			return
		}
		rest = rest[:int(length)-4]
		for position := 0; len(rest) > 1; position++ {
			element, remaining, readable := bsoncore.ReadElement(rest)
			if !readable || !yield(position, element) {
				return
			}
			rest = remaining
		}
	}
}

// measuredString is a string request field. The codec decodes a string from a
// string, a symbol, an ObjectID (as its hex) or a binary; the last value wins
// and a null empties it. Any other type, and a binary of a subtype other than
// 0 or 2, fails the handler's decode, which stores nothing.
type measuredString struct {
	text string
}

func (m *measuredString) take(value bsoncore.Value) {
	switch value.Type {
	case bson.TypeString:
		m.text, _ = value.StringValueOK()
	case bson.TypeSymbol:
		m.text, _ = value.SymbolOK()
	case bson.TypeObjectID:
		id, _ := value.ObjectIDOK()
		m.text = id.Hex()
	case bson.TypeBinary:
		_, content, _ := value.BinaryOK()
		m.text = string(content)
	case bson.TypeNull, bson.TypeUndefined:
		m.text = ""
	}
}

// measuredDocument is a map[string]interface{} request field as the encoded
// documents decoded into it. The codec decodes every document routed to a map
// field into the same map, so a later document adds to an earlier one rather
// than replacing it, and a null empties the map. Any other type fails the
// handler's decode, which stores nothing.
type measuredDocument struct {
	parts []bson.Raw
}

func (m *measuredDocument) take(value bsoncore.Value) {
	switch value.Type {
	case bson.TypeEmbeddedDocument:
		m.parts = append(m.parts, bson.Raw(value.Data))
	case bson.TypeNull, bson.TypeUndefined:
		m.parts = nil
	}
}

// document is the document the handler's map holds, encoded, and whether
// there is one.
func (m measuredDocument) document() (bson.Raw, bool) {
	switch len(m.parts) {
	case 0:
		return nil, false
	case 1:
		return m.parts[0], true
	}
	return mergedDocument(m.parts), true
}

// mergedDocument is the documents decoded into one map, encoded: each key once,
// with the value of its last appearance, as the map keeps it.
func mergedDocument(parts []bson.Raw) bson.Raw {
	latest := make(map[string]bsoncore.Element)
	var keys []string
	for _, part := range parts {
		for _, element := range readableElements(part) {
			key := element.Key()
			if _, seen := latest[key]; !seen {
				keys = append(keys, key)
			}
			latest[key] = element
		}
	}
	merged := make([]byte, 4, len(parts[len(parts)-1])+1)
	for _, key := range keys {
		merged = append(merged, latest[key]...)
	}
	merged = append(merged, 0)
	binary.LittleEndian.PutUint32(merged, uint32(len(merged)))
	return merged
}

// lastStringValue is the string under key in document as a map decode keeps
// it: the last appearance wins, and "" when that one is not a string.
func lastStringValue(document []byte, key string) string {
	var found string
	for _, element := range readableElements(document) {
		if string(element.KeyBytes()) == key {
			found, _ = element.Value().StringValueOK()
		}
	}
	return found
}

// measuredDocuments is a []map[string]interface{} request field as its
// encoded array. The codec replaces a slice field with each array routed to
// it, so the last one is what the handler holds, and a null empties it.
type measuredDocuments struct {
	array []byte
}

func (m *measuredDocuments) take(value bsoncore.Value) {
	switch value.Type {
	case bson.TypeArray:
		m.array = value.Data
	case bson.TypeNull, bson.TypeUndefined:
		m.array = nil
	}
}

// measuredBytes is a []byte request field by its length. The codec decodes a
// []byte from a binary of subtype 0 or 2, a string or a symbol; the last value
// wins and a null empties it.
type measuredBytes struct {
	length int
	isSet  bool
}

// take measures one value. A binary of another subtype fails the handler's
// decode, but is measured all the same: refusing it for its size refuses
// nothing the handler would store.
func (m *measuredBytes) take(value bsoncore.Value) {
	switch value.Type {
	case bson.TypeBinary:
		_, content, _ := value.BinaryOK()
		m.length, m.isSet = len(content), true
	case bson.TypeString, bson.TypeSymbol:
		m.length, m.isSet = textLength(value.Data), true
	case bson.TypeNull, bson.TypeUndefined:
		m.length, m.isSet = 0, false
	}
}

// textLength is the length of an encoded string or symbol, read off its
// length prefix rather than by copying it into a string: the prefix counts the
// terminating zero byte.
func textLength(encoded []byte) int {
	length, _, ok := bsoncore.ReadLength(encoded)
	if !ok || length < 1 {
		return 0
	}
	return int(length) - 1
}
