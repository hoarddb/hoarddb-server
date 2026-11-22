package server

import (
	"fmt"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// The filter's unit tests state documents as Go maps. The server matches
// stored bytes (scan_filter_document.go), so these helpers store the map
// first, as a write does (bson.Marshal), and match the stored bytes. A value
// the encoder changes on the way, such as a uint64 or a time.Time, is
// matched as the server would hold it.

// matchesFilter reports whether doc, once stored, satisfies filter.
func matchesFilter(doc map[string]interface{}, filter map[string]interface{}) (bool, error) {
	return compileFilter(filter).matches(doc)
}

// matches reports whether doc, once stored, satisfies the compiled filter.
func (f *compiledFilter) matches(doc map[string]interface{}) (bool, error) {
	encoded, err := bson.Marshal(doc)
	if err != nil {
		return false, fmt.Errorf("storing the test document: %w", err)
	}
	document, err := bson.TrustStored(encoded)
	if err != nil {
		return false, fmt.Errorf("framing the test document: %w", err)
	}
	return f.matchesDocument(document)
}
