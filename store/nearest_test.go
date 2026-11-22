package store

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"
)

// float32Bytes packs values as little-endian float32, the vector layout a nearest query
// and a stored vector both use.
func float32Bytes(values ...float32) []byte {
	out := make([]byte, 4*len(values))
	for i, v := range values {
		binary.LittleEndian.PutUint32(out[4*i:], math.Float32bits(v))
	}
	return out
}

func TestNearestScoreMetrics(t *testing.T) {
	ones := make([]float32, 1536)
	for i := range ones {
		ones[i] = 0.1
	}
	cases := []struct {
		name   string
		metric NearestMetric
		query  []float32
		vector []float32
		want   float64
		exact  bool
	}{
		{"cosine identical", NearestCosine, []float32{1, 0}, []float32{1, 0}, 1, true},
		{"cosine orthogonal", NearestCosine, []float32{1, 0}, []float32{0, 1}, 0, true},
		{"cosine opposite", NearestCosine, []float32{1, 0}, []float32{-1, 0}, -1, true},
		{"cosine zero vector norm", NearestCosine, []float32{0, 0}, []float32{1, 0}, 0, true},
		{"cosine half angle", NearestCosine, []float32{1, 0}, []float32{0.5, 0.5}, 0.7071067811865475, false},
		{"cosine 1536 dimensions", NearestCosine, ones, ones, 1, false},
		{"dot", NearestDot, []float32{1, 2}, []float32{3, 4}, 11, true},
		{"l2", NearestL2, []float32{1, 2}, []float32{4, 6}, 5, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := newNearestQuery(float32Bytes(tc.query...)).score(tc.metric, float32Bytes(tc.vector...))
			if tc.exact && got != tc.want {
				t.Fatalf("score = %v, want exactly %v", got, tc.want)
			}
			if !tc.exact && math.Abs(got-tc.want) > 1e-9 {
				t.Fatalf("score = %v, want %v within 1e-9", got, tc.want)
			}
		})
	}
}

func TestNearestZeroAndNonFiniteScoreWorst(t *testing.T) {
	nan := float32(math.NaN())
	inf := float32(math.Inf(1))
	cases := []struct {
		metric NearestMetric
		want   float64
	}{
		{NearestCosine, -1},
		{NearestDot, -math.MaxFloat64},
		{NearestL2, math.MaxFloat64},
	}
	for _, tc := range cases {
		q := newNearestQuery(float32Bytes(1, 0))
		if got := q.score(tc.metric, float32Bytes(nan, 1)); got != tc.want {
			t.Errorf("metric %d with NaN: score = %v, want %v", tc.metric, got, tc.want)
		}
		if got := q.score(tc.metric, float32Bytes(inf, 1)); got != tc.want {
			t.Errorf("metric %d with +Inf: score = %v, want %v", tc.metric, got, tc.want)
		}
		if got := nearestWorst(tc.metric); got != tc.want {
			t.Errorf("nearestWorst(%d) = %v, want %v", tc.metric, got, tc.want)
		}
	}
}

func TestNearestBetterPerMetric(t *testing.T) {
	cases := []struct {
		metric  NearestMetric
		closer  float64
		farther float64
	}{
		{NearestCosine, 2, 1},
		{NearestDot, 2, 1},
		{NearestL2, 1, 2},
	}
	for _, tc := range cases {
		if !nearestBetter(tc.metric, tc.closer, tc.farther) {
			t.Errorf("metric %d: nearestBetter(%v, %v) = false, want true", tc.metric, tc.closer, tc.farther)
		}
		if nearestBetter(tc.metric, tc.farther, tc.closer) {
			t.Errorf("metric %d: nearestBetter(%v, %v) = true, want false", tc.metric, tc.farther, tc.closer)
		}
		if nearestBetter(tc.metric, tc.closer, tc.closer) {
			t.Errorf("metric %d: equal scores reported as better", tc.metric)
		}
	}
}

func TestNearestErrorsNameTheKeyAndBothLengths(t *testing.T) {
	mismatch := &NearestElementError{Key: "m-1", Field: "Vector", Length: 12, Want: 8}
	wantMismatch := `element "m-1" holds a vector of 12 bytes in field "Vector" and the query vector is 8 bytes. Nothing was returned. Every element of a field is embedded by one model; re-embed the field or query with the model that wrote it`
	if got := mismatch.Error(); got != wantMismatch {
		t.Fatalf("length mismatch text:\n got %q\nwant %q", got, wantMismatch)
	}

	notBinary := &NearestElementError{Key: "m-1", Field: "Vector", NotBinary: true}
	wantNotBinary := `element "m-1" holds field "Vector" as a value that is not a BSON binary of subtype 0x00. Nothing was returned. Store the vector as BSON binary subtype 0x00 holding little-endian float32 values`
	if got := notBinary.Error(); got != wantNotBinary {
		t.Fatalf("not-binary text:\n got %q\nwant %q", got, wantNotBinary)
	}
}

// vectorElement is a store element document holding values as a BSON binary of subtype 0x00 under field.
func vectorElement(t *testing.T, field string, values ...float32) []byte {
	t.Helper()
	out, err := bson.Marshal(bson.D{{Key: field, Value: primitive.Binary{Subtype: 0x00, Data: float32Bytes(values...)}}})
	if err != nil {
		t.Fatalf("marshalling a vector element: %v", err)
	}
	return out
}

func nearestPutElement(t *testing.T, p *fieldParent, ref FieldRef, key string, element []byte) {
	t.Helper()
	if _, err := p.apply(ApplyInfo{}, true, func(w FieldWrite) error { return FieldPut(w, ref, key, element) }); err != nil {
		t.Fatalf("putting %q: %v", key, err)
	}
}

func nearestPushElement(t *testing.T, p *fieldParent, ref FieldRef, element []byte) {
	t.Helper()
	if _, err := p.apply(ApplyInfo{}, false, func(w FieldWrite) error {
		_, err := FieldPush(w, ref, element)
		return err
	}); err != nil {
		t.Fatalf("pushing: %v", err)
	}
}

func nearestRun(t *testing.T, p *fieldParent, ref FieldRef, req NearestRequest) (NearestResult, error) {
	t.Helper()
	snap := p.kv.NewSnapshot()
	t.Cleanup(snap.Discard)
	return FieldNearest(context.Background(), snap, ref, req)
}

func nearestKeys(result NearestResult) []string {
	keys := make([]string, len(result.Hits))
	for i, hit := range result.Hits {
		keys[i] = hit.Entry.ElementKey
	}
	return keys
}

func nearestFixture(t *testing.T) (*fieldParent, FieldRef) {
	p := newFieldParent(t)
	ref := p.ref("u1", 1, FieldHash)
	for key, v := range map[string][]float32{"a": {1, 0}, "b": {0, 1}, "c": {-1, 0}, "d": {0.5, 0.5}, "e": {2, 0}} {
		nearestPutElement(t, p, ref, key, vectorElement(t, "Vector", v...))
	}
	return p, ref
}

func nearestReq(metric NearestMetric, k int, query ...float32) NearestRequest {
	return NearestRequest{Metric: metric, K: k, Field: "Vector", Vector: float32Bytes(query...), MaxElements: 100000}
}

func TestNearestReturnsBestKInOrder(t *testing.T) {
	p, ref := nearestFixture(t)
	cases := []struct {
		metric NearestMetric
		keys   []string
		scores []float64
	}{
		{NearestCosine, []string{"a", "e", "d"}, []float64{1, 1, 0.7071067811865475}},
		{NearestDot, []string{"e", "a", "d"}, []float64{2, 1, 0.5}},
		{NearestL2, []string{"a", "d", "e"}, []float64{0, 0.7071067811865476, 1}},
	}
	for _, tc := range cases {
		result, err := nearestRun(t, p, ref, nearestReq(tc.metric, 3, 1, 0))
		if err != nil {
			t.Fatalf("metric %d: %v", tc.metric, err)
		}
		if fmt.Sprint(nearestKeys(result)) != fmt.Sprint(tc.keys) {
			t.Fatalf("metric %d keys = %v, want %v", tc.metric, nearestKeys(result), tc.keys)
		}
		for i, hit := range result.Hits {
			if math.Abs(hit.Score-tc.scores[i]) > 1e-9 {
				t.Fatalf("metric %d hit %d score = %v, want %v", tc.metric, i, hit.Score, tc.scores[i])
			}
		}
		if result.Scanned != 5 || result.Skipped != 0 {
			t.Fatalf("metric %d scanned %d skipped %d, want 5 and 0", tc.metric, result.Scanned, result.Skipped)
		}
	}
}

func TestNearestKeepsTheEarlierElementOnATie(t *testing.T) {
	p := newFieldParent(t)
	ref := p.ref("u1", 1, FieldHash)
	for i := 0; i < 10; i++ {
		nearestPutElement(t, p, ref, fmt.Sprintf("k%d", i), vectorElement(t, "Vector", 1, 0))
	}
	for _, page := range []int{256, 3} {
		old := nearestPageEntries
		nearestPageEntries = page
		t.Cleanup(func() { nearestPageEntries = old })
		result, err := nearestRun(t, p, ref, nearestReq(NearestCosine, 2, 1, 0))
		if err != nil {
			t.Fatalf("page %d: %v", page, err)
		}
		if got := fmt.Sprint(nearestKeys(result)); got != "[k0 k1]" {
			t.Fatalf("page %d keys = %s, want [k0 k1]", page, got)
		}
	}
}

func TestNearestSkipsElementsWithoutTheField(t *testing.T) {
	p := newFieldParent(t)
	ref := p.ref("u1", 1, FieldHash)
	for i := 0; i < 3; i++ {
		nearestPutElement(t, p, ref, fmt.Sprintf("v%d", i), vectorElement(t, "Vector", 1, float32(i)))
	}
	plain, err := bson.Marshal(bson.D{{Key: "Text", Value: "x"}})
	if err != nil {
		t.Fatal(err)
	}
	nearestPutElement(t, p, ref, "t0", plain)
	nearestPutElement(t, p, ref, "t1", plain)
	result, err := nearestRun(t, p, ref, nearestReq(NearestCosine, 5, 1, 0))
	if err != nil {
		t.Fatal(err)
	}
	if result.Scanned != 3 || result.Skipped != 2 || len(result.Hits) != 3 {
		t.Fatalf("scanned %d skipped %d hits %d, want 3, 2, 3", result.Scanned, result.Skipped, len(result.Hits))
	}
}

func TestNearestRefusesAWrongLength(t *testing.T) {
	p := newFieldParent(t)
	ref := p.ref("u1", 1, FieldHash)
	nearestPutElement(t, p, ref, "good", vectorElement(t, "Vector", 1, 0))
	nearestPutElement(t, p, ref, "bad", vectorElement(t, "Vector", 1, 0, 0))
	result, err := nearestRun(t, p, ref, nearestReq(NearestCosine, 2, 1, 0))
	var elementErr *NearestElementError
	if !errors.As(err, &elementErr) {
		t.Fatalf("err = %v, want *NearestElementError", err)
	}
	if elementErr.Key != "bad" || elementErr.Length != 12 || elementErr.Want != 8 || elementErr.NotBinary {
		t.Fatalf("error = %+v", elementErr)
	}
	want := (&NearestElementError{Key: "bad", Field: "Vector", Length: 12, Want: 8}).Error()
	if err.Error() != want {
		t.Fatalf("message = %q, want %q", err.Error(), want)
	}
	if len(result.Hits) != 0 || result.Scanned != 0 {
		t.Fatalf("partial result returned with an error: %+v", result)
	}
}

func TestNearestRefusesANonBinaryField(t *testing.T) {
	p := newFieldParent(t)
	ref := p.ref("u1", 1, FieldHash)
	text, err := bson.Marshal(bson.D{{Key: "Vector", Value: "abc"}})
	if err != nil {
		t.Fatal(err)
	}
	nearestPutElement(t, p, ref, "x", text)
	result, err := nearestRun(t, p, ref, nearestReq(NearestCosine, 1, 1, 0))
	var elementErr *NearestElementError
	if !errors.As(err, &elementErr) || !elementErr.NotBinary || elementErr.Key != "x" || elementErr.Field != "Vector" {
		t.Fatalf("err = %v, want a NotBinary error for x", err)
	}
	if len(result.Hits) != 0 {
		t.Fatalf("partial result returned with an error: %+v", result)
	}
}

func TestNearestRefusesAboveTheCeiling(t *testing.T) {
	p, kv := newCountedFieldParent(t)
	ref := p.ref("u1", 1, FieldHash)
	for i := 0; i < 5; i++ {
		nearestPutElement(t, p, ref, fmt.Sprintf("k%d", i), vectorElement(t, "Vector", 1, 0))
	}
	kv.valueReads.Store(0)
	req := nearestReq(NearestCosine, 1, 1, 0)
	req.MaxElements = 4
	result, err := nearestRun(t, p, ref, req)
	var tooLarge *NearestTooLargeError
	if !errors.As(err, &tooLarge) || tooLarge.Count != 5 || tooLarge.Max != 4 {
		t.Fatalf("err = %v, want NearestTooLargeError{5, 4}", err)
	}
	for _, want := range []string{"5", "4", "query.nearest_max_elements", "HOARDDB_QUERY_NEAREST_MAX_ELEMENTS", "smaller field"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("message %q does not contain %q", err.Error(), want)
		}
	}
	if got := kv.valueReads.Load(); got != 0 {
		t.Fatalf("%d element values were read before the refusal, want 0", got)
	}
	if len(result.Hits) != 0 {
		t.Fatalf("hits returned with a refusal: %+v", result)
	}
	req.MaxElements = 5
	if _, err := nearestRun(t, p, ref, req); err != nil {
		t.Fatalf("count equal to the ceiling must pass: %v", err)
	}
}

func TestNearestPagesAcrossTheWholeStore(t *testing.T) {
	old := nearestPageEntries
	nearestPageEntries = 2
	t.Cleanup(func() { nearestPageEntries = old })
	p := newFieldParent(t)
	ref := p.ref("u1", 1, FieldFIFO)
	for i := 0; i < 7; i++ {
		nearestPushElement(t, p, ref, vectorElement(t, "Vector", 1, float32(7-i)))
	}
	nearestPushElement(t, p, ref, vectorElement(t, "Vector", 1, 0))
	result, err := nearestRun(t, p, ref, nearestReq(NearestL2, 1, 1, 0))
	if err != nil {
		t.Fatal(err)
	}
	if result.Scanned != 8 || len(result.Hits) != 1 || result.Hits[0].Entry.Position != 7 {
		t.Fatalf("scanned %d, hits %+v, want 8 scanned and position 7 best", result.Scanned, result.Hits)
	}
}

func TestNearestIgnoresKeysOutsideTheElementNamespace(t *testing.T) {
	p, ref := nearestFixture(t)
	batch := p.kv.NewBatch()
	if err := batch.Set(FieldElementKey(ref.StorePrefix(), 0x06, []byte("x")), vectorElement(t, "Vector", 1, 0)); err != nil {
		t.Fatal(err)
	}
	if err := batch.Commit(false); err != nil {
		t.Fatal(err)
	}
	result, err := nearestRun(t, p, ref, nearestReq(NearestDot, 1, 1, 0))
	if err != nil {
		t.Fatal(err)
	}
	if result.Scanned != 5 || nearestKeys(result)[0] != "e" {
		t.Fatalf("scanned %d, best %v, want 5 and e", result.Scanned, nearestKeys(result))
	}
}

func TestNearestWorksOnEveryEngine(t *testing.T) {
	cases := []struct {
		name    string
		engine  FieldEngine
		wantKey string
	}{
		{"fifo", FieldFIFO, "position 0"},
		{"lifo", FieldLIFO, "position 0"},
		{"heap", FieldHeap, "job-1"},
		{"btree", FieldBTree, "job-1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := newFieldParent(t)
			ref := p.ref("u1", 1, tc.engine)
			bad := vectorElement(t, "Vector", 1, 0, 0)
			var err error
			switch tc.engine {
			case FieldFIFO, FieldLIFO:
				nearestPushElement(t, p, ref, bad)
			case FieldHeap:
				_, err = p.apply(ApplyInfo{}, false, func(w FieldWrite) error { return FieldHeapPush(w, ref, 5, "job-1", bad) })
			default:
				nearestPutElement(t, p, ref, "job-1", bad)
			}
			if err != nil {
				t.Fatal(err)
			}
			_, err = nearestRun(t, p, ref, nearestReq(NearestCosine, 1, 1, 0))
			var elementErr *NearestElementError
			if !errors.As(err, &elementErr) || elementErr.Key != tc.wantKey {
				t.Fatalf("err = %v, want an element error keyed %q", err, tc.wantKey)
			}
		})
	}
}

func TestNearestRefusesAnInvalidRequest(t *testing.T) {
	p, ref := nearestFixture(t)
	nan := float32Bytes(float32(math.NaN()), 0)
	cases := map[string]func(*NearestRequest){
		"k zero":       func(r *NearestRequest) { r.K = 0 },
		"ceiling zero": func(r *NearestRequest) { r.MaxElements = 0 },
		"empty field":  func(r *NearestRequest) { r.Field = "" },
		"empty vector": func(r *NearestRequest) { r.Vector = nil },
		"five bytes":   func(r *NearestRequest) { r.Vector = make([]byte, 5) },
		"too long":     func(r *NearestRequest) { r.Vector = make([]byte, 16388) },
		"nan":          func(r *NearestRequest) { r.Vector = nan },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			req := nearestReq(NearestCosine, 1, 1, 0)
			mutate(&req)
			if _, err := nearestRun(t, p, ref, req); err == nil {
				t.Fatal("an invalid request was accepted")
			}
		})
	}
}

// TestNearestReturnsNothingPartialWhenABadElementIsMet keeps good elements before a bad one
// (and bad before good) and requires the error with an empty result either way.
func TestNearestReturnsNothingPartialWhenABadElementIsMet(t *testing.T) {
	text, err := bson.Marshal(bson.D{{Key: "Vector", Value: "abc"}})
	if err != nil {
		t.Fatal(err)
	}
	subtype4, err := bson.Marshal(bson.D{{Key: "Vector", Value: primitive.Binary{Subtype: 0x04, Data: float32Bytes(1, 0)}}})
	if err != nil {
		t.Fatal(err)
	}
	bads := map[string][]byte{
		"wrong length":     vectorElement(t, "Vector", 1, 0, 0),
		"not binary":       text,
		"binary subtype 4": subtype4,
	}
	for name, bad := range bads {
		for _, position := range []string{"first", "later"} {
			t.Run(name+" "+position, func(t *testing.T) {
				p := newFieldParent(t)
				ref := p.ref("u1", 1, FieldHash)
				badKey := "aaa"
				if position == "later" {
					badKey = "zzz"
				}
				nearestPutElement(t, p, ref, badKey, bad)
				for _, key := range []string{"g1", "g2", "g3"} {
					nearestPutElement(t, p, ref, key, vectorElement(t, "Vector", 1, 0))
				}
				result, err := nearestRun(t, p, ref, nearestReq(NearestCosine, 2, 1, 0))
				var elementErr *NearestElementError
				if !errors.As(err, &elementErr) || elementErr.Key != badKey {
					t.Fatalf("err = %v, want an element error keyed %q", err, badKey)
				}
				if elementErr.NotBinary != (name != "wrong length") {
					t.Fatalf("NotBinary = %v for %s", elementErr.NotBinary, name)
				}
				if len(result.Hits) != 0 || result.Scanned != 0 || result.Skipped != 0 {
					t.Fatalf("result %+v returned beside an error, want the zero result", result)
				}
			})
		}
	}
}

func TestNearestAcceptsTheLongestVectorAndRefusesOneMore(t *testing.T) {
	p := newFieldParent(t)
	ref := p.ref("u1", 1, FieldHash)
	long := make([]float32, nearestMaxVectorBytes/4)
	long[0] = 1
	nearestPutElement(t, p, ref, "a", vectorElement(t, "Vector", long...))
	req := nearestReq(NearestCosine, 1, long...)
	if len(req.Vector) != 16384 {
		t.Fatalf("fixture is %d bytes, want 16384", len(req.Vector))
	}
	result, err := nearestRun(t, p, ref, req)
	if err != nil || len(result.Hits) != 1 || result.Hits[0].Score != 1 {
		t.Fatalf("a 4096-dimension query: result %+v, err %v", result, err)
	}
	req.Vector = float32Bytes(append(long, 0)...)
	if _, err := nearestRun(t, p, ref, req); err == nil {
		t.Fatal("a 4097-dimension query (16388 bytes) was accepted")
	}
}

func TestNearestNonFiniteElementsScoreWorstAndStillCount(t *testing.T) {
	inf := float32(math.Inf(1))
	cases := []struct {
		name   string
		metric NearestMetric
		query  []float32
		bad    []float32
		worst  float64
	}{
		{"nan cosine", NearestCosine, []float32{1, 0}, []float32{float32(math.NaN()), 0}, -1},
		{"inf dot", NearestDot, []float32{1, 0}, []float32{inf, 0}, -math.MaxFloat64},
		{"inf l2", NearestL2, []float32{1, 0}, []float32{inf, 0}, math.MaxFloat64},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := newFieldParent(t)
			ref := p.ref("u1", 1, FieldHash)
			nearestPutElement(t, p, ref, "a", vectorElement(t, "Vector", tc.query...))
			nearestPutElement(t, p, ref, "m", vectorElement(t, "Vector", tc.bad...))
			nearestPutElement(t, p, ref, "z", vectorElement(t, "Vector", tc.query...))
			result, err := nearestRun(t, p, ref, nearestReq(tc.metric, 3, tc.query...))
			if err != nil {
				t.Fatal(err)
			}
			if result.Scanned != 3 || len(result.Hits) != 3 {
				t.Fatalf("scanned %d hits %d, want 3 and 3", result.Scanned, len(result.Hits))
			}
			last := result.Hits[2]
			if last.Entry.ElementKey != "m" || last.Score != tc.worst {
				t.Fatalf("last hit %q score %v, want m scoring %v", last.Entry.ElementKey, last.Score, tc.worst)
			}
		})
	}
}
