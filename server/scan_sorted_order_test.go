package server

import (
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"
)

// This file is T-5 of the query-pipelining spec (§8): the property test for
// the sort order of §3.2 and the bounded two-pass sort that relies on it.
//
// The reference order below is written from the spec's table, independently
// of server/sort_order.go, so a fault in the production comparator cannot hide
// in the reference too. Assertions (a) and (b) are put to the production
// comparator itself; (c) and (d) compare what Scan returns with the reference;
// (e) compares the two passes' walks through sortedScanPassHook.

// referenceClass is the spec's type rank (§3.2, "The sort order", step 1),
// with rank 7 sub-ranked by the BSON type byte of the value.
func referenceClass(value interface{}) (rank, subRank int) {
	switch value.(type) {
	case nil:
		return 8, 0
	case float64, float32, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return 1, 0
	case string:
		return 2, 0
	case primitive.DateTime, time.Time:
		return 3, 0
	case bool:
		return 4, 0
	case primitive.D, primitive.M, map[string]interface{}:
		return 5, 0
	case primitive.A, []interface{}:
		return 6, 0
	}
	bsonType, _, err := bson.MarshalValue(value)
	if err != nil {
		panic(fmt.Sprintf("the generator produced %T, which BSON cannot encode: %v", value, err))
	}
	return 7, int(bsonType)
}

// referenceNumber maps a number to a float64 with NaN below every other value,
// which is the spec's "NaN is below every other number and equal to every
// other NaN" as one totally ordered image.
func referenceNumber(value interface{}) (float64, bool) {
	var number float64
	switch typed := value.(type) {
	case float64:
		number = typed
	case float32:
		number = float64(typed)
	case int32:
		number = float64(typed)
	case int64:
		number = float64(typed)
	case int:
		number = float64(typed)
	default:
		panic(fmt.Sprintf("referenceNumber: the generator produced %T", value))
	}
	return number, math.IsNaN(number)
}

// referenceCompare is the ascending comparison of steps 1 and 2: negative,
// zero or positive. Ties (zero) are left to position, step 3.
func referenceCompare(left, right interface{}) int {
	leftRank, leftSub := referenceClass(left)
	rightRank, rightSub := referenceClass(right)
	if leftRank != rightRank {
		return leftRank - rightRank
	}
	if leftSub != rightSub {
		return leftSub - rightSub
	}
	switch leftRank {
	case 1:
		leftNumber, leftNaN := referenceNumber(left)
		rightNumber, rightNaN := referenceNumber(right)
		switch {
		case leftNaN && rightNaN:
			return 0
		case leftNaN:
			return -1
		case rightNaN:
			return 1
		case leftNumber < rightNumber:
			return -1
		case leftNumber > rightNumber:
			return 1
		}
		return 0
	case 2:
		return strings.Compare(left.(string), right.(string))
	case 3:
		leftMillis, _ := dateMillis(left)
		rightMillis, _ := dateMillis(right)
		switch {
		case leftMillis < rightMillis:
			return -1
		case leftMillis > rightMillis:
			return 1
		}
		return 0
	case 4:
		leftBool, rightBool := left.(bool), right.(bool)
		switch {
		case leftBool == rightBool:
			return 0
		case !leftBool:
			return -1
		}
		return 1
	}
	return 0
}

// referenceSort returns the ids of documents, given in natural walk order,
// ordered by the spec's order. The stable sort over natural order is what
// makes ties come back by position ascending, in both directions (step 3).
func referenceSort(documents []map[string]interface{}, field string, ascending bool) []string {
	ordered := make([]map[string]interface{}, len(documents))
	copy(ordered, documents)
	sort.SliceStable(ordered, func(i, j int) bool {
		comparison := referenceCompare(ordered[i][field], ordered[j][field])
		if !ascending {
			comparison = -comparison
		}
		return comparison < 0
	})
	ids := make([]string, len(ordered))
	for i, document := range ordered {
		ids[i] = document["_id_"].(string)
	}
	return ids
}

// sortValuePool is what the generator draws a sort field's value from. The
// pools are small on purpose, so that ties are heavy.
func sortValuePool() []interface{} {
	objectID, err := primitive.ObjectIDFromHex("65a1b2c3d4e5f60718293a4b")
	if err != nil {
		panic(err)
	}
	decimal, err := primitive.ParseDecimal128("1.5")
	if err != nil {
		panic(err)
	}
	return []interface{}{
		// numbers, including NaN, both infinities, and int64 values above
		// 2^53 that one float64 cannot tell apart
		int32(-3), int32(0), int32(2), int64(2), int64(1<<53 + 1), int64(1<<53 + 2),
		float64(2), -1.5, 2.5, math.Inf(1), math.Inf(-1), math.NaN(), math.NaN(),
		// strings
		"", "a", "ab", "b", "B",
		// dates
		primitive.DateTime(-5), primitive.DateTime(0), primitive.DateTime(1000),
		// booleans
		false, true,
		// documents and arrays
		map[string]interface{}{"x": int32(1)}, map[string]interface{}{"y": "z"},
		[]interface{}{int32(1), int32(2)}, []interface{}{},
		// every other BSON type
		objectID, primitive.Binary{Subtype: 0, Data: []byte{1}}, decimal,
		primitive.Timestamp{T: 1, I: 1}, primitive.Regex{Pattern: "a"},
		primitive.MinKey{}, primitive.MaxKey{},
		// explicit null
		nil,
	}
}

// missingField marks a document the generator leaves without the sort field.
type missingField struct{}

// seedSortOrderBucket fills one bucket of storeType with count documents whose
// "v" field is drawn from the pool, or left out, and whose "grp" is 0..2 so a
// filter can keep about a third of them.
func seedSortOrderBucket(t *testing.T, s *Server, storeType BucketType, bucket string, count int, rng *rand.Rand) {
	t.Helper()
	createTestBucket(t, s, "app", bucket, storeType)
	pool := sortValuePool()
	for i := 0; i < count; i++ {
		id := fmt.Sprintf("d%03d", i)
		document := map[string]interface{}{"_id_": id, "grp": int32(rng.Intn(3))}
		var value interface{} = missingField{}
		if rng.Intn(len(pool)+2) < len(pool) {
			value = pool[rng.Intn(len(pool))]
		}
		if _, missing := value.(missingField); !missing {
			document["v"] = value
		}
		if storeType == BucketTypeHeap {
			pushed, err := s.Push(PushRequest{Database: "app", Bucket: bucket, Priority: int64(rng.Intn(4)), Data: document})
			if err != nil || !pushed.Success {
				t.Fatalf("pushing %s into %s: %+v, %v", id, bucket, pushed, err)
			}
			continue
		}
		inserted, err := s.Insert(InsertRequest{Database: "app", Bucket: bucket, Key: id, Data: document})
		if err != nil || !inserted.Success {
			t.Fatalf("inserting %s into %s: %+v, %v", id, bucket, inserted, err)
		}
	}
}

// naturalOrder returns a bucket's documents in the store's walk order, which
// is the order position counts.
func naturalOrder(t *testing.T, s *Server, bucket string, count int) []map[string]interface{} {
	t.Helper()
	resp, err := s.Scan(ScanRequest{Database: "app", Bucket: bucket, Limit: count + 1})
	if err != nil || !resp.Found || int(resp.Total) != count {
		t.Fatalf("natural-order scan of %s: %+v, %v", bucket, resp, err)
	}
	return decodedDocuments(t, resp.Documents)
}

// sortedIDs runs one sorted page through Scan and returns its ids.
func sortedIDs(t *testing.T, s *Server, request ScanRequest) ([]string, *ScanResponse) {
	t.Helper()
	resp, err := s.Scan(request)
	if err != nil || !resp.Found {
		t.Fatalf("sorted scan %+v: %+v, %v", request, resp, err)
	}
	ids := make([]string, 0, len(resp.Documents))
	for _, document := range decodedDocuments(t, resp.Documents) {
		ids = append(ids, document["_id_"].(string))
	}
	return ids, resp
}

// TestSortedScanBoundedEqualsFullSort is T-5.
func TestSortedScanBoundedEqualsFullSort(t *testing.T) {
	const count = 90
	storeTypes := []BucketType{BucketTypeHash, BucketTypeBTree, BucketTypeFIFO, BucketTypeLIFO, BucketTypeHeap}

	for _, seed := range []int64{1, 2, 3} {
		rng := rand.New(rand.NewSource(seed))
		s := newTestServer(t)
		for _, storeType := range storeTypes {
			bucket := fmt.Sprintf("%s_%d", storeType, seed)
			seedSortOrderBucket(t, s, storeType, bucket, count, rng)
			natural := naturalOrder(t, s, bucket, count)
			assertNaNStored(t, natural, bucket)

			t.Run(fmt.Sprintf("seed=%d/%s", seed, storeType), func(t *testing.T) {
				for _, ascending := range []bool{true, false} {
					direction := 1
					if !ascending {
						direction = -1
					}
					checkComparatorIsTotal(t, natural, ascending, rng)
					checkComparatorIgnoresInputOrder(t, natural, ascending, rng)
					checkPagesMatchReference(t, s, bucket, natural, direction, rng)
					checkPassesAgree(t, s, bucket, direction)
				}
			})
		}
	}
}

// assertNaNStored proves the generator's NaN reached the store as a NaN, so
// the test exercises the rule it claims to.
func assertNaNStored(t *testing.T, natural []map[string]interface{}, bucket string) {
	t.Helper()
	for _, document := range natural {
		if number, ok := document["v"].(float64); ok && math.IsNaN(number) {
			return
		}
	}
	t.Fatalf("%s holds no NaN after seeding: the generator no longer exercises the NaN rule", bucket)
}

// candidatesOf builds the production comparator's inputs from documents in
// natural order: the key of each "v", and its position.
func candidatesOf(natural []map[string]interface{}) []sortCandidate {
	candidates := make([]sortCandidate, len(natural))
	for position, document := range natural {
		candidates[position] = sortCandidate{key: sortKeyOf(document["v"]), position: int64(position)}
	}
	return candidates
}

// checkComparatorIsTotal is assertion (a): over random triples of the
// generated keys, the production order is irreflexive, antisymmetric, total
// and transitive.
func checkComparatorIsTotal(t *testing.T, natural []map[string]interface{}, ascending bool, rng *rand.Rand) {
	t.Helper()
	candidates := candidatesOf(natural)
	for i := 0; i < 20000; i++ {
		x := candidates[rng.Intn(len(candidates))]
		y := candidates[rng.Intn(len(candidates))]
		z := candidates[rng.Intn(len(candidates))]
		if sortsBefore(x, x, ascending) {
			t.Fatalf("(a) ascending=%v: position %d sorts before itself", ascending, x.position)
		}
		xy, yx := sortsBefore(x, y, ascending), sortsBefore(y, x, ascending)
		if xy && yx {
			t.Fatalf("(a) ascending=%v: positions %d and %d each sort before the other", ascending, x.position, y.position)
		}
		if x.position != y.position && !xy && !yx {
			t.Fatalf("(a) ascending=%v: positions %d (%v) and %d (%v) are unordered, so the order is not total",
				ascending, x.position, natural[x.position]["v"], y.position, natural[y.position]["v"])
		}
		if xy && sortsBefore(y, z, ascending) && !sortsBefore(x, z, ascending) {
			t.Fatalf("(a) ascending=%v: not transitive: %v@%d < %v@%d < %v@%d, but not %v@%d < %v@%d", ascending,
				natural[x.position]["v"], x.position, natural[y.position]["v"], y.position, natural[z.position]["v"], z.position,
				natural[x.position]["v"], x.position, natural[z.position]["v"], z.position)
		}
	}
}

// checkComparatorIgnoresInputOrder is assertion (b): shuffled inputs sort to
// the same order under the production comparator.
func checkComparatorIgnoresInputOrder(t *testing.T, natural []map[string]interface{}, ascending bool, rng *rand.Rand) {
	t.Helper()
	var want []int64
	for trial := 0; trial < 8; trial++ {
		candidates := candidatesOf(natural)
		rng.Shuffle(len(candidates), func(i, j int) { candidates[i], candidates[j] = candidates[j], candidates[i] })
		sort.Slice(candidates, func(i, j int) bool { return sortsBefore(candidates[i], candidates[j], ascending) })
		got := make([]int64, len(candidates))
		for i, candidate := range candidates {
			got[i] = candidate.position
		}
		if want == nil {
			want = got
			continue
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("(b) ascending=%v: a shuffled input sorted differently at index %d (position %d, want %d): the order depends on input order",
					ascending, i, got[i], want[i])
			}
		}
	}
}

// checkPagesMatchReference is assertions (c) and (d), unfiltered, filtered
// and projected: every page equals its slice of the reference, and pages of
// every size concatenate to the whole reference with nothing repeated or
// dropped.
func checkPagesMatchReference(t *testing.T, s *Server, bucket string, natural []map[string]interface{}, direction int, rng *rand.Rand) {
	t.Helper()
	filters := []struct {
		name   string
		filter map[string]interface{}
		keep   func(map[string]interface{}) bool
	}{
		{"unfiltered", nil, func(map[string]interface{}) bool { return true }},
		{"grp<1", map[string]interface{}{"grp": map[string]interface{}{"$lt": int32(1)}}, func(d map[string]interface{}) bool { return d["grp"].(int32) < 1 }},
	}
	for _, variant := range filters {
		var matched []map[string]interface{}
		for _, document := range natural {
			if variant.keep(document) {
				matched = append(matched, document)
			}
		}
		reference := referenceSort(matched, "v", direction > 0)
		base := ScanRequest{Database: "app", Bucket: bucket, Filter: variant.filter, Sort: map[string]int{"v": direction}}

		// (d): pages of each size, concatenated.
		for _, pageSize := range []int{1, 4, 7, len(reference) + 3} {
			var concatenated []string
			for skip := 0; skip < len(reference)+pageSize; skip += pageSize {
				request := base
				request.Skip, request.Limit = skip, pageSize
				ids, resp := sortedIDs(t, s, request)
				if resp.Total != int64(len(reference)) {
					t.Fatalf("(d) %s %s dir=%d skip=%d: Total = %d, want %d", bucket, variant.name, direction, skip, resp.Total, len(reference))
				}
				if wantMore := skip+pageSize < len(reference); resp.HasMore != wantMore {
					t.Fatalf("(d) %s %s dir=%d skip=%d limit=%d: HasMore = %v, want %v", bucket, variant.name, direction, skip, pageSize, resp.HasMore, wantMore)
				}
				concatenated = append(concatenated, ids...)
			}
			assertSameIDs(t, fmt.Sprintf("(d) %s %s dir=%d pages of %d", bucket, variant.name, direction, pageSize), concatenated, reference)
		}

		// (c): random pages.
		for trial := 0; trial < 25; trial++ {
			skip, limit := rng.Intn(len(reference)+5), 1+rng.Intn(20)
			request := base
			request.Skip, request.Limit = skip, limit
			ids, _ := sortedIDs(t, s, request)
			assertSameIDs(t, fmt.Sprintf("(c) %s %s dir=%d skip=%d limit=%d", bucket, variant.name, direction, skip, limit), ids, pageOf(reference, skip, limit))
		}

		// A projected page is the same page, holding only the projected field.
		request := base
		request.Skip, request.Limit, request.Projection = 3, 11, map[string]int{"_id_": 1}
		ids, resp := sortedIDs(t, s, request)
		assertSameIDs(t, fmt.Sprintf("(c) %s %s dir=%d projected", bucket, variant.name, direction), ids, pageOf(reference, 3, 11))
		for _, document := range decodedDocuments(t, resp.Documents) {
			if len(document) != 1 {
				t.Fatalf("(c) %s projected page kept %v, want only _id_", bucket, document)
			}
		}
	}
}

// pageOf is reference[skip : skip+limit], clamped.
func pageOf(reference []string, skip, limit int) []string {
	if skip > len(reference) {
		skip = len(reference)
	}
	end := skip + limit
	if end > len(reference) {
		end = len(reference)
	}
	return reference[skip:end]
}

func assertSameIDs(t *testing.T, label string, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: %d documents, want %d\n got %v\nwant %v", label, len(got), len(want), got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%s: document %d is %s, want %s\n got %v\nwant %v", label, i, got[i], want[i], got, want)
		}
	}
}

// checkPassesAgree is assertion (e): pass 1 visits every position once, in
// order, and every position pass 2 visits holds the same entry pass 1 saw
// there — the two walks read the one snapshot.
func checkPassesAgree(t *testing.T, s *Server, bucket string, direction int) {
	t.Helper()
	seen := map[int]map[int64][]byte{1: {}, 2: {}}
	var outOfOrder []string
	last := map[int]int64{1: -1, 2: -1}
	hook := func(hookBucket string, pass int, position int64, rawEntry []byte) {
		if hookBucket != bucket {
			return
		}
		if position <= last[pass] {
			outOfOrder = append(outOfOrder, fmt.Sprintf("pass %d visited position %d after %d", pass, position, last[pass]))
		}
		last[pass] = position
		seen[pass][position] = append([]byte(nil), rawEntry...)
	}
	sortedScanPassHook.Store(&hook)
	defer sortedScanPassHook.Store(nil)

	if _, err := s.Scan(ScanRequest{Database: "app", Bucket: bucket, Sort: map[string]int{"v": direction}, Skip: 10, Limit: 15}); err != nil {
		t.Fatalf("(e) scan: %v", err)
	}
	if len(outOfOrder) > 0 {
		t.Fatalf("(e) %s: %v", bucket, outOfOrder)
	}
	if len(seen[1]) == 0 || len(seen[2]) == 0 {
		t.Fatalf("(e) %s: pass 1 visited %d positions and pass 2 %d; both must walk", bucket, len(seen[1]), len(seen[2]))
	}
	for position := int64(0); position < int64(len(seen[1])); position++ {
		if _, ok := seen[1][position]; !ok {
			t.Fatalf("(e) %s: pass 1 skipped position %d", bucket, position)
		}
	}
	for position, entry := range seen[2] {
		if !bytes.Equal(entry, seen[1][position]) {
			t.Fatalf("(e) %s: position %d holds different entries in pass 1 and pass 2", bucket, position)
		}
	}
}
