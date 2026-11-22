package server

import (
	"context"
	"slices"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"
)

// pathDocuments are the documents the path reader tests store, by key.
var pathDocuments = map[string]map[string]interface{}{
	"a": {"Status": "open", "Address": map[string]interface{}{"City": "Tirana", "Zip": "1001"},
		"Lines": primitive.A{map[string]interface{}{"Sku": "A-1", "Qty": int32(2)}}},
	"b": {"Status": "open", "Address": map[string]interface{}{"City": "Paris"},
		"Lines": primitive.A{map[string]interface{}{"Sku": "B-2", "Qty": int32(1)}, map[string]interface{}{"Sku": "A-1", "Qty": int32(4)}}},
	"c": {"Status": "closed", "Name": "no address"},
	"d": {"Status": "open", "Address": "a string, not a document", "Text": "plain"},
}

// seedPathDocuments creates bucket as a hash bucket, with the given indices
// when there are any, and stores pathDocuments in it.
func seedPathDocuments(t *testing.T, s *Server, bucket string, indices ...interface{}) {
	t.Helper()
	if len(indices) > 0 {
		createUniqueBucket(t, s, "app", bucket, indices...)
	} else {
		createTestBucket(t, s, "app", bucket, BucketTypeHash)
	}
	for key, document := range pathDocuments {
		stored := make(map[string]interface{}, len(document)+1)
		for field, value := range document {
			stored[field] = value
		}
		if resp, err := s.Insert(InsertRequest{Database: "app", Bucket: bucket, Key: key, Data: stored}); err != nil || !resp.Success {
			t.Fatalf("inserting %s: %+v, %v", key, resp, err)
		}
	}
}

// documentKeys is the sorted _id_ of every document in documents.
func documentKeys(t *testing.T, documents []bson.Raw) []string {
	t.Helper()
	keys := make([]string, 0, len(documents))
	for _, document := range documents {
		key, ok := document.Lookup("_id_").StringValueOK()
		if !ok {
			t.Fatalf("a returned document has no _id_: %s", document)
		}
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}

// pathFindKeys is find(filter) on bucket: the keys of every match.
func pathFindKeys(t *testing.T, s *Server, bucket string, filter map[string]interface{}) []string {
	t.Helper()
	resp, err := s.ScanCtx(context.Background(), ScanRequest{Database: "app", Bucket: bucket, Filter: filter, Limit: 100})
	if err != nil {
		t.Fatalf("find(%v): %v", filter, err)
	}
	return documentKeys(t, resp.Documents)
}

// pathSearchKeys is search(filter) on bucket: the keys of every match and the
// index that served it.
func pathSearchKeys(t *testing.T, s *Server, bucket string, filter map[string]interface{}) ([]string, string) {
	t.Helper()
	resp, err := s.SearchCtx(context.Background(), SearchRequest{Database: "app", Bucket: bucket, Filter: filter, Limit: 100})
	if err != nil {
		t.Fatalf("search(%v): %v", filter, err)
	}
	if resp.Message != "" {
		t.Fatalf("search(%v) failed: %s", filter, resp.Message)
	}
	return documentKeys(t, resp.Results), resp.IndexUsed
}

// pathCountMatches is count(filter) on bucket.
func pathCountMatches(t *testing.T, s *Server, bucket string, filter map[string]interface{}) int64 {
	t.Helper()
	resp, err := s.CountCtx(context.Background(), CountRequest{Database: "app", Bucket: bucket, Filter: filter})
	if err != nil {
		t.Fatalf("count(%v): %v", filter, err)
	}
	return resp.Count
}

// TestFilterResolvesPaths is the filter half of the field-path rule: a filter key is a
// path, and a missing member, a missing intermediate document and a
// non-document on the way are all absent, so $exists: false matches them.
func TestFilterResolvesPaths(t *testing.T) {
	s := newTestServer(t)
	seedPathDocuments(t, s, "people")
	for _, row := range []struct {
		filter map[string]interface{}
		want   []string
	}{
		{map[string]interface{}{"Address.City": "Tirana"}, []string{"a"}},
		{map[string]interface{}{"Address.Zip": map[string]interface{}{"$gt": "1000"}}, []string{"a"}},
		{map[string]interface{}{"Address.City": map[string]interface{}{"$in": primitive.A{"Paris", "Rome"}}}, []string{"b"}},
		{map[string]interface{}{"Lines[1].Qty": int32(4)}, []string{"b"}},
		{map[string]interface{}{"Lines[0].Sku": "A-1"}, []string{"a"}},
		// c has no Address; d's Address is a string: both are absent.
		{map[string]interface{}{"Address.City": map[string]interface{}{"$exists": false}}, []string{"c", "d"}},
		{map[string]interface{}{"Address.City": map[string]interface{}{"$exists": true}}, []string{"a", "b"}},
		{map[string]interface{}{"Lines[1]": map[string]interface{}{"$exists": false}}, []string{"a", "c", "d"}},
	} {
		if got := pathFindKeys(t, s, "people", row.filter); !slices.Equal(got, row.want) {
			t.Errorf("find(%v) = %v, want %v", row.filter, got, row.want)
		}
		if got := pathCountMatches(t, s, "people", row.filter); got != int64(len(row.want)) {
			t.Errorf("count(%v) = %d, want %d", row.filter, got, len(row.want))
		}
	}
}

// TestFilterPathThroughAnArrayWithoutAPositionIsAbsent: there is no implicit
// "any element" match. Lines is an array, so Lines.Sku names nothing.
func TestFilterPathThroughAnArrayWithoutAPositionIsAbsent(t *testing.T) {
	s := newTestServer(t)
	seedPathDocuments(t, s, "people")
	if got := pathFindKeys(t, s, "people", map[string]interface{}{"Lines.Sku": "A-1"}); len(got) != 0 {
		t.Errorf(`find({"Lines.Sku": "A-1"}) = %v; a name segment on an array is absent, so nothing matches`, got)
	}
	want := []string{"a", "b", "c", "d"}
	if got := pathFindKeys(t, s, "people", map[string]interface{}{"Lines.Sku": map[string]interface{}{"$exists": false}}); !slices.Equal(got, want) {
		t.Errorf(`find({"Lines.Sku": {$exists: false}}) = %v, want every document %v`, got, want)
	}
}

// TestFilterPathInAndOrClauses: the clauses of $and and $or resolve paths as
// a top-level filter does.
func TestFilterPathInAndOrClauses(t *testing.T) {
	s := newTestServer(t)
	seedPathDocuments(t, s, "people")
	for _, row := range []struct {
		filter map[string]interface{}
		want   []string
	}{
		{map[string]interface{}{"$and": primitive.A{
			map[string]interface{}{"Address.City": "Tirana"},
			map[string]interface{}{"Address.Zip": "1001"},
		}}, []string{"a"}},
		{map[string]interface{}{"$or": primitive.A{
			map[string]interface{}{"Address.City": "Paris"},
			map[string]interface{}{"Lines[0].Sku": "A-1"},
		}}, []string{"a", "b"}},
		{map[string]interface{}{"$or": primitive.A{
			map[string]interface{}{"$and": primitive.A{map[string]interface{}{"Lines[1].Sku": "A-1"}}},
			map[string]interface{}{"Name": "no address"},
		}}, []string{"b", "c"}},
	} {
		if got := pathFindKeys(t, s, "people", row.filter); !slices.Equal(got, row.want) {
			t.Errorf("find(%v) = %v, want %v", row.filter, got, row.want)
		}
	}
}

// dottedResidualRows are filters a search answers through the residual, each
// with the documents a scan finds. Every one names Status, which the bucket
// indexes, so search serves it from that index and re-applies the rest.
var dottedResidualRows = []struct {
	filter map[string]interface{}
	want   []string
}{
	{map[string]interface{}{"Status": "open", "Address.City": "Tirana"}, []string{"a"}},
	{map[string]interface{}{"Status": "open", "Lines[0].Sku": "A-1"}, []string{"a"}},
	{map[string]interface{}{"Status": "open", "Lines[1].Sku": "A-1"}, []string{"b"}},
	{map[string]interface{}{"Status": "open", "Address.City": map[string]interface{}{"$exists": false}}, []string{"d"}},
}

// TestDottedResidualOnAnIndexedSearchEqualsTheScan: index-served search
// re-applies a dotted predicate through the raw resolver and answers what a
// scan answers (spec §4.9.6, T-12).
func TestDottedResidualOnAnIndexedSearchEqualsTheScan(t *testing.T) {
	s := newTestServer(t)
	seedPathDocuments(t, s, "orders", "Status")
	for _, row := range dottedResidualRows {
		searched, used := pathSearchKeys(t, s, "orders", row.filter)
		if used == indexUsedFullScan {
			t.Fatalf("search(%v) was not served by the Status index", row.filter)
		}
		found := pathFindKeys(t, s, "orders", row.filter)
		if !slices.Equal(searched, row.want) || !slices.Equal(found, row.want) {
			t.Errorf("filter %v: search (index %s) = %v, find = %v, want %v", row.filter, used, searched, found, row.want)
		}
	}
}

// TestNameSegmentOnAnArrayIsAbsentOnEveryPlan: a name segment on an array, a
// path through a string and a missing member are absent — never an error — on
// index-served search, scanned search and find alike (K4).
func TestNameSegmentOnAnArrayIsAbsentOnEveryPlan(t *testing.T) {
	s := newTestServer(t)
	seedPathDocuments(t, s, "orders", "Status")
	for _, row := range []struct {
		path string
		want []string // the open documents where the path is absent
	}{
		{"Lines.0.Sku", []string{"a", "b", "d"}},
		{"Lines.Sku", []string{"a", "b", "d"}},
		{"Text.City", []string{"a", "b", "d"}},
		{"Address.Geo", []string{"a", "b", "d"}},
	} {
		absent := map[string]interface{}{row.path: map[string]interface{}{"$exists": false}}
		indexed := map[string]interface{}{"Status": "open", row.path: map[string]interface{}{"$exists": false}}
		scanned := map[string]interface{}{"Status": map[string]interface{}{"$in": primitive.A{"open"}}, row.path: map[string]interface{}{"$exists": false}}

		viaIndex, used := pathSearchKeys(t, s, "orders", indexed)
		if used == indexUsedFullScan {
			t.Fatalf("search(%v) was not served by the Status index", indexed)
		}
		viaScan, scanUsed := pathSearchKeys(t, s, "orders", scanned)
		if scanUsed != indexUsedFullScan {
			t.Fatalf("search(%v) was served by index %s, want a scan", scanned, scanUsed)
		}
		viaFind := pathFindKeys(t, s, "orders", indexed)
		if !slices.Equal(viaIndex, row.want) || !slices.Equal(viaScan, row.want) || !slices.Equal(viaFind, row.want) {
			t.Errorf("%s absent: index search %v, scanned search %v, find %v, want %v", row.path, viaIndex, viaScan, viaFind, row.want)
		}
		if got := pathFindKeys(t, s, "orders", absent); !slices.Equal(got, []string{"a", "b", "c", "d"}) {
			t.Errorf("find(%v) = %v, want every document", absent, got)
		}
	}
	present := map[string]interface{}{"Status": "open", "Lines[0].Sku": map[string]interface{}{"$exists": true}}
	if got, _ := pathSearchKeys(t, s, "orders", present); !slices.Equal(got, []string{"a", "b"}) {
		t.Errorf("search(%v) = %v, want [a b]", present, got)
	}
}

// TestNoPathEntersAStoreField: a store field's elements live outside the
// document, so Comments[0] names position 0 of a document field the document
// does not have (spec §4.9.1, review A N-4). It is absent, and not an error.
func TestNoPathEntersAStoreField(t *testing.T) {
	s := newPostServer(t)
	fieldOK(t)(s.FieldPushCtx(context.Background(), FieldRequest{Database: "app", Bucket: "Post", Key: "p1", Field: "Comments",
		Data: map[string]interface{}{"text": "first"}}))
	if got := pathFindKeys(t, s, "Post", map[string]interface{}{"Comments[0]": map[string]interface{}{"$exists": true}}); len(got) != 0 {
		t.Errorf("find({Comments[0]: {$exists: true}}) = %v; a path never enters a store field", got)
	}
	if got := pathFindKeys(t, s, "Post", map[string]interface{}{"Comments[0].text": "first"}); len(got) != 0 {
		t.Errorf("find({Comments[0].text: first}) = %v; a path never enters a store field", got)
	}
	if got := pathFindKeys(t, s, "Post", map[string]interface{}{"Comments[0]": map[string]interface{}{"$exists": false}}); !slices.Equal(got, []string{"p1", "p2"}) {
		t.Errorf("find({Comments[0]: {$exists: false}}) = %v, want [p1 p2]", got)
	}
}
