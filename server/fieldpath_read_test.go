package server

import (
	"context"
	"reflect"
	"slices"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"
)

// scanDocuments runs find with the given sort or projection and decodes every
// returned document.
func scanDocuments(t *testing.T, s *Server, bucket string, req ScanRequest) []map[string]interface{} {
	t.Helper()
	req.Database, req.Bucket = "app", bucket
	if req.Limit == 0 {
		req.Limit = 100
	}
	resp, err := s.ScanCtx(context.Background(), req)
	if err != nil {
		t.Fatalf("find(%+v): %v", req, err)
	}
	documents := make([]map[string]interface{}, 0, len(resp.Documents))
	for _, raw := range resp.Documents {
		var document map[string]interface{}
		if err := bson.Unmarshal(raw, &document); err != nil {
			t.Fatalf("decoding a returned document: %v", err)
		}
		documents = append(documents, document)
	}
	return documents
}

// TestSortByAPath: a sort key is a path, and a document where it is absent
// sorts as a missing field always has. The sort's size estimate reads the
// same path from the raw bytes.
func TestSortByAPath(t *testing.T) {
	s := newTestServer(t)
	seedPathDocuments(t, s, "people")

	var order []string
	for _, document := range scanDocuments(t, s, "people", ScanRequest{Sort: map[string]int{"Address.City": 1}}) {
		order = append(order, document["_id_"].(string))
	}
	// c and d have no Address.City: absent ranks after every value, as a
	// missing top-level field does (sortRankMissing); Paris before Tirana.
	if len(order) != 4 || !slices.Equal(order[:2], []string{"b", "a"}) {
		t.Errorf("sorted by Address.City = %v, want b (Paris), a (Tirana), then the two absent documents", order)
	}
	order = order[:0]
	for _, document := range scanDocuments(t, s, "people", ScanRequest{Sort: map[string]int{"Lines[0].Qty": -1}}) {
		order = append(order, document["_id_"].(string))
	}
	if len(order) != 4 || !slices.Equal(order[2:], []string{"a", "b"}) {
		t.Errorf("sorted by Lines[0].Qty descending = %v, want the two absent documents, then a (2), then b (1)", order)
	}

	raw, err := bson.Marshal(map[string]interface{}{"Address": map[string]interface{}{"City": "Tirana"}})
	if err != nil {
		t.Fatalf("encoding: %v", err)
	}
	stored, err := bson.TrustStored(raw)
	if err != nil {
		t.Fatalf("framing: %v", err)
	}
	sortCharge := func(key string) int64 {
		t.Helper()
		_, charge, err := documentSortKey(stored, libraryPath(mustParsePath(t, key)))
		if err != nil {
			t.Fatalf("the sort key %q: %v", key, err)
		}
		return charge
	}
	// A string's value is a 4-byte length, its bytes and a NUL: 4+6+1 = 11.
	if got, want := sortCharge("Address.City"), int64(11+16); got != want {
		t.Errorf("the sort estimate of Address.City = %d bytes, want %d (the nested value read from the raw bytes, plus 16)", got, want)
	}
	if got := sortCharge("Address.Zip"); got != 16 {
		t.Errorf("the sort estimate of an absent path = %d, want 16", got)
	}
}

// TestProjectionOfAPathKeepsItsParents: an included path is copied with its
// parents, and an excluded path removes that member only.
func TestProjectionOfAPathKeepsItsParents(t *testing.T) {
	s := newTestServer(t)
	seedPathDocuments(t, s, "people")
	filter := map[string]interface{}{"_id_": "a"}

	included := scanDocuments(t, s, "people", ScanRequest{Filter: filter, Projection: map[string]int{"Address.City": 1, "Status": 1}})
	want := map[string]interface{}{"Status": "open", "Address": map[string]interface{}{"City": "Tirana"}}
	if len(included) != 1 || !reflect.DeepEqual(included[0], want) {
		t.Errorf("projection {Address.City: 1, Status: 1} = %v, want %v", included, want)
	}

	excluded := scanDocuments(t, s, "people", ScanRequest{Filter: filter, Projection: map[string]int{"Address.Zip": 0, "Lines": 0}})
	want = map[string]interface{}{"_id_": "a", "Status": "open", "Address": map[string]interface{}{"City": "Tirana"}}
	if len(excluded) != 1 || !reflect.DeepEqual(excluded[0], want) {
		t.Errorf("projection {Address.Zip: 0, Lines: 0} = %v, want %v", excluded, want)
	}

	// The excluded member is gone from the answer only: the stored document
	// still has it.
	stored := scanDocuments(t, s, "people", ScanRequest{Filter: filter})
	if _, hasZip := stored[0]["Address"].(map[string]interface{})["Zip"]; !hasZip {
		t.Error("projecting Address.Zip away removed it from the stored document")
	}

	// A path absent from a document projects nothing for it.
	absent := scanDocuments(t, s, "people", ScanRequest{Filter: map[string]interface{}{"_id_": "d"}, Projection: map[string]int{"Address.City": 1}})
	if len(absent) != 1 || len(absent[0]) != 0 {
		t.Errorf("projecting Address.City of a document whose Address is a string = %v, want {}", absent)
	}
}

// TestProjectionMergesSeveralPositionsOfOneArray: several positions of one
// array merge into one array of those elements in ascending position order,
// each holding only its projected members; positions are not kept (review A
// N-6).
func TestProjectionMergesSeveralPositionsOfOneArray(t *testing.T) {
	s := newTestServer(t)
	seedPathDocuments(t, s, "people")
	filter := map[string]interface{}{"_id_": "b"}

	merged := scanDocuments(t, s, "people", ScanRequest{Filter: filter,
		Projection: map[string]int{"Lines[1].Qty": 1, "Lines[0].Sku": 1, "Lines[1].Sku": 1}})
	want := map[string]interface{}{"Lines": primitive.A{
		map[string]interface{}{"Sku": "B-2"},
		map[string]interface{}{"Sku": "A-1", "Qty": int32(4)},
	}}
	if len(merged) != 1 || !reflect.DeepEqual(merged[0], want) {
		t.Errorf("projection of Lines[0].Sku, Lines[1].Sku and Lines[1].Qty = %v, want %v", merged, want)
	}

	// One position alone: the stored Lines[1] is the answer's Lines[0].
	single := scanDocuments(t, s, "people", ScanRequest{Filter: filter, Projection: map[string]int{"Lines[1].Qty": 1}})
	want = map[string]interface{}{"Lines": primitive.A{map[string]interface{}{"Qty": int32(4)}}}
	if len(single) != 1 || !reflect.DeepEqual(single[0], want) {
		t.Errorf("projection of Lines[1].Qty = %v, want %v (positions are not kept)", single, want)
	}

	// Sorted scans project through the same code.
	sorted := scanDocuments(t, s, "people", ScanRequest{Filter: filter, Sort: map[string]int{"Status": 1},
		Projection: map[string]int{"Lines[1].Qty": 1}})
	if len(sorted) != 1 || !reflect.DeepEqual(sorted[0], want) {
		t.Errorf("a sorted scan's projection of Lines[1].Qty = %v, want %v", sorted, want)
	}
}

// TestDistinctOfAPath: distinct reads the path.
func TestDistinctOfAPath(t *testing.T) {
	s := newTestServer(t)
	seedPathDocuments(t, s, "people")
	resp, err := s.DistinctCtx(context.Background(), DistinctRequest{Database: "app", Bucket: "people", Field: "Address.City"})
	if err != nil {
		t.Fatalf("distinct(Address.City): %v", err)
	}
	var cities []string
	for _, value := range resp.Values {
		cities = append(cities, value.(string))
	}
	slices.Sort(cities)
	if !slices.Equal(cities, []string{"Paris", "Tirana"}) {
		t.Errorf("distinct(Address.City) = %v, want [Paris Tirana]", resp.Values)
	}
	resp, err = s.DistinctCtx(context.Background(), DistinctRequest{Database: "app", Bucket: "people", Field: "Lines[0].Sku"})
	if err != nil {
		t.Fatalf("distinct(Lines[0].Sku): %v", err)
	}
	if len(resp.Values) != 2 {
		t.Errorf("distinct(Lines[0].Sku) = %v, want A-1 and B-2", resp.Values)
	}
}
