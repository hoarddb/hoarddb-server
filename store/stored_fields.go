package store

import (
	"fmt"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// StoredFields reads the top-level fields named in names from a document,
// each decoded as a decode of the whole document into map[string]interface{}
// decodes it (DecodeStoredValue), without decoding anything else
// (bson-codec-spec 7.2). A name the document repeats takes its last element,
// as the map decode does, so the map holds exactly the named members of the
// map decode's result. The returned map is never nil, even when it holds no
// field: a document was read.
//
// Every top-level element is walked, so a document whose top level is not
// BSON is refused, as the map decode refused it, and so is a named field whose
// own bytes do not decode. A malformed value inside a field that is not named
// is not read, and so not refused: the fields that are read are the ones the
// map decode would have given.
func StoredFields(document bson.Document, names []string) (map[string]interface{}, error) {
	return readFields(document, len(names), func(cursor *bson.Cursor) (string, bool) {
		for _, name := range names {
			if cursor.KeyEquals(name) {
				return name, true
			}
		}
		return "", false
	})
}

// IndexedFields is StoredFields over the top-level fields the manager's
// indexes name: everything EntryKey, CheckUnique and RenderIndexValue read of
// a document. The names are found from the specs on each call, so the manager
// keeps nothing more per bucket.
func (im *IndexManager) IndexedFields(document bson.Document) (map[string]interface{}, error) {
	return readFields(document, len(im.specs), func(cursor *bson.Cursor) (string, bool) {
		for _, spec := range im.specs {
			for _, part := range spec.Fields {
				if cursor.KeyEquals(part.Field) {
					return part.Field, true
				}
			}
		}
		return "", false
	})
}

// readFields walks document's top level and decodes the value of every
// element named reports a name for.
func readFields(document bson.Document, expected int, named func(*bson.Cursor) (string, bool)) (map[string]interface{}, error) {
	fields := make(map[string]interface{}, expected)
	cursor := document.Cursor()
	for cursor.Next() {
		name, wanted := named(&cursor)
		if !wanted {
			continue
		}
		view := cursor.Value()
		value, err := DecodeStoredValue(view.Type(), view.Bytes())
		if err != nil {
			return nil, fmt.Errorf("reading field %q: %w", name, err)
		}
		fields[name] = value
	}
	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("reading the document's fields: %w", err)
	}
	return fields, nil
}
