package server

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/bsontype"
	"github.com/hoarddb/hoarddb-bson/x/bsonx/bsoncore"
)

// The search residual differential (build doc step 7a, path "$and/$or
// fallback"). A search re-applies its filter to every candidate with
// residualMatches. A filter of field paths read each path's value from the
// stored bytes and decoded that one value inside a synthetic one-field
// document (decodeRawFieldValue), and a filter with a top-level $and or $or
// decoded the whole document into a map. The oracle is that function as it
// was, copied here with its decode; the server's residualMatches is run
// beside it over the scan differential's documents and filters.
//
// Both oracle forms already resolved a path through the stored bytes for
// field filters, first element first, and through the map, last element
// first, for logical ones; on a document that repeats a key, the second form
// and the resolver may answer apart. That is the ruled difference of the scan
// differential, counted the same way.
//
// One further difference is the residual's own: its one-value decode gave a
// code-with-scope's scope documents as primitive.D, where find's decode gives
// maps, so search and find disagreed on equality with such a value. The
// server's matcher is find's, so on those answers it is compared with find's
// map path instead, and counted apart.

func oracleResidualMatches(raw []byte, filter *compiledFilter) (bool, error) {
	if oracleHasLogical(filter) {
		var document map[string]interface{}
		if err := bson.Unmarshal(raw, &document); err != nil {
			return false, fmt.Errorf("decoding a search hit: %w", err)
		}
		return oracleMatches(filter, document)
	}
	allMatch := true
	budget := &matchBudget{remaining: maxMatchWork}
	for _, clause := range filter.clauses {
		value, present := resolveRawPath(bson.Raw(raw), clause.path)
		var docValue interface{}
		if present {
			decoded, err := oracleDecodeRawFieldValue(value)
			if err != nil {
				return false, fmt.Errorf("decoding field %q of a search hit: %w", clause.path.text, err)
			}
			docValue = decoded
		}
		matched, err := oracleMatchField(docValue, present, clause.condition, budget)
		if err != nil {
			return false, err
		}
		if !matched {
			allMatch = false
		}
	}
	return allMatch, nil
}

// oracleHasLogical is compiledFilter.hasLogical as it was: a $and, a $or or
// a malformed clause sent the residual to the whole-document decode.
func oracleHasLogical(filter *compiledFilter) bool {
	for _, clause := range filter.clauses {
		if clause.logical != "" || clause.err != nil {
			return true
		}
	}
	return false
}

// oracleDecodeRawFieldValue is decodeRawFieldValue as it was: a document or
// an array is decoded inside a one-field wrapper so its documents decode as
// maps, the way the whole-document decode gives them.
func oracleDecodeRawFieldValue(value bson.RawValue) (interface{}, error) {
	if value.Type != bson.TypeEmbeddedDocument && value.Type != bson.TypeArray {
		var decoded interface{}
		err := value.Unmarshal(&decoded)
		return decoded, err
	}
	element := append(bsoncore.AppendHeader(nil, value.Type, "v"), value.Value...)
	wrapped := bsoncore.BuildDocument(nil, element)
	var holder map[string]interface{}
	if err := bson.Unmarshal(wrapped, &holder); err != nil {
		return nil, err
	}
	return holder["v"], nil
}

// TestResidualMatchesMatchesTheMapPath: every document, every derived
// filter, field and logical, the same match and the same refusal as the
// residual matcher it replaces.
func TestResidualMatchesMatchesTheMapPath(t *testing.T) {
	random := rand.New(rand.NewSource(73))
	pathsPerDocument := 4
	if raceEnabled {
		pathsPerDocument = 2
	}
	var tally matchTally
	codeWithScopeAlignedWithFind := 0
	for _, document := range differentialDocuments(t) {
		var decoded map[string]interface{}
		if err := bson.Unmarshal(document.raw, &decoded); err != nil {
			continue
		}
		tally.documents++
		repeats := repeatsAKey(document.raw)
		withRefusals := tally.documents%8 == 1
		for _, filterSpec := range documentFilters(document.raw, decoded, random, pathsPerDocument, withRefusals) {
			filter := compileFilter(filterSpec)
			tally.evaluations++
			wantMatch, wantErr := oracleResidualMatches(document.raw, filter)
			gotMatch, gotErr := residualMatches(document.raw, filter)
			if wantMatch == gotMatch && sameRefusal(wantErr, gotErr) {
				if wantMatch {
					tally.matched++
				}
				if wantErr != nil {
					tally.refused++
				}
				continue
			}
			if repeats && wantErr == nil && gotErr == nil && filterMeetsRepeatedKey(filter, document.raw) &&
				!strings.HasPrefix(document.name, "v1 record") {
				tally.duplicateKeyRuled++
				continue
			}
			// The residual's own decode of a code-with-scope value gave its
			// scope's documents as primitive.D, where find's whole-document
			// decode gives maps, so a search and a find disagreed on
			// equality with one. The server now answers as find does.
			findMatch, findErr := oracleMatches(filter, decoded)
			if containsCodeWithScope(document.raw) && gotMatch == findMatch && sameRefusal(findErr, gotErr) {
				codeWithScopeAlignedWithFind++
				continue
			}
			tally.disagreements++
			t.Errorf("%s, filter %v: the residual matcher said (%v, %v), the server says (%v, %v)",
				document.name, filterSpec, wantMatch, wantErr, gotMatch, gotErr)
			if tally.disagreements >= maxReportedDisagreements {
				t.Fatalf("stopping after %d disagreements; %s", tally.disagreements, tally)
			}
		}
	}
	if tally.matched == 0 || tally.refused == 0 || tally.evaluations-tally.matched == 0 {
		t.Fatalf("the run compared nothing in some class: %s", tally)
	}
	t.Logf("%s; %d answers where the residual's code-with-scope decode disagreed with find's and the server now agrees with find", tally, codeWithScopeAlignedWithFind)
}

// containsCodeWithScope reports whether any document inside raw holds a
// code-with-scope value.
func containsCodeWithScope(raw []byte) bool {
	framed, err := bson.TrustStored(raw)
	if err != nil {
		return false
	}
	cursor := framed.Cursor()
	for cursor.Next() {
		value := cursor.Value()
		switch value.Type() {
		case bsontype.CodeWithScope:
			return true
		case bsontype.EmbeddedDocument, bsontype.Array:
			if containsCodeWithScope(value.Bytes()) {
				return true
			}
		}
	}
	return false
}
