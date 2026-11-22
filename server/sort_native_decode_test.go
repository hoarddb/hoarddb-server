package server

import (
	"github.com/hoarddb/hoarddb-bson/bson"
)

// decodeNative decodes a document with the BSON package this tree's server
// code is built on: the vendor's before the step 6 swap, the library's after.
// sortKeyOf consumes that package's types (a date is its primitive.DateTime),
// so the comparator differential must decode with this import and no other.
// The file has this import alone, so the swap script rewrites it like any
// production import.
func decodeNative(raw []byte) (map[string]interface{}, error) {
	var decoded map[string]interface{}
	if err := bson.Unmarshal(raw, &decoded); err != nil {
		return nil, err
	}
	return decoded, nil
}
