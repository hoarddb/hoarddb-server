package server

import (
	"fmt"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// updateMayDeepen decides which update results are measured for depth. It was
// a rule over the request's depth as the depth scan measured it; build step 9
// asks Validate whether the request is deeper than a threshold instead (owner
// ruling (a)). These tests hold the two to the same answer, with the request's
// depth known from how its bytes were built rather than measured.

// oldUpdateMayDeepenRule is updateMayDeepen as it was before step 9, over a
// request of known depth: a request the scan refused (deeper than its request
// limit, or not BSON) measures every result.
func oldUpdateMayDeepenRule(segments, requestDepth int, wellFormed bool) bool {
	if !wellFormed || requestDepth > requestDepthLimit(transport.OpUpdate) {
		return true
	}
	return segments+1+max(requestDepth-2, 0) > maxDocumentDepth
}

// requestWithSegments is an update whose longest path has exactly segments
// segments, or no path at all for 0.
func requestWithSegments(segments int) UpdateRequest {
	if segments == 0 {
		return UpdateRequest{Database: "app", Bucket: "b", Key: "k"}
	}
	path := strings.TrimSuffix(strings.Repeat("a.", segments), ".")
	return UpdateRequest{Database: "app", Bucket: "b", Key: "k", Set: map[string]interface{}{path: int32(1)}}
}

// holdKnownDepth checks raw nests exactly depth levels, with the library's
// Validate alone, so the oracle's depth is the bytes' depth.
func holdKnownDepth(t *testing.T, raw []byte, depth int) {
	t.Helper()
	if depth > bson.MaxNestingCeiling {
		// Past the ceiling only "deeper than the ceiling" can be checked.
		if _, err := bson.Validate(raw, bson.Limits{Depth: bson.MaxNestingCeiling}); err == nil {
			t.Fatalf("setup: a %d-level request is accepted at the %d-level ceiling", depth, bson.MaxNestingCeiling)
		}
		return
	}
	if _, err := bson.Validate(raw, bson.Limits{Depth: depth}); err != nil {
		t.Fatalf("setup: a %d-level request is refused at %d levels: %v", depth, depth, err)
	}
	if depth > 1 {
		if _, err := bson.Validate(raw, bson.Limits{Depth: depth - 1}); err == nil {
			t.Fatalf("setup: a %d-level request is accepted at %d levels", depth, depth-1)
		}
	}
}

// TestUpdateMayDeepenMatchesTheDepthRule runs every request depth from 1 to
// past the library's ceiling, every path length from 0 to 101 segments, as
// documents, arrays and code-with-scope scopes, plus malformed and missing
// encodings and a $rename, and compares updateMayDeepen with the old rule.
func TestUpdateMayDeepenMatchesTheDepthRule(t *testing.T) {
	compared, deepens := 0, 0
	for _, kind := range []string{"doc", "array", "scope"} {
		for requestDepth := 1; requestDepth <= 205; requestDepth++ {
			encoded := nestedChain(requestDepth, kind)
			holdKnownDepth(t, encoded, requestDepth)
			for segments := 0; segments <= 101; segments++ {
				want := oldUpdateMayDeepenRule(segments, requestDepth, true)
				if got := updateMayDeepen(requestWithSegments(segments), encoded); got != want {
					t.Errorf("%s, request depth %d, %d segments: updateMayDeepen = %t, the rule says %t", kind, requestDepth, segments, got, want)
				}
				compared++
				if want {
					deepens++
				}
			}
		}
	}

	malformed := nestedChain(10, "doc")
	malformed = malformed[:len(malformed)-1]
	for segments := 0; segments <= 101; segments++ {
		request := requestWithSegments(segments)
		for name, encoded := range map[string][]byte{"malformed": malformed, "missing": nil} {
			if !updateMayDeepen(request, encoded) {
				t.Errorf("%s encoding, %d segments: updateMayDeepen = false, want true (a request that is not measured measures every result)", name, segments)
			}
			compared++
		}
		// A boolean byte of 2 is the one input the two readings part on: the scan
		// stepped over it and measured one level, Validate refuses it, and a
		// refusal measures every result. The answer may only grow more careful.
		// encoded is bson.Marshal's output, which never writes such a byte.
		if oldUpdateMayDeepenRule(segments, 1, true) && !updateMayDeepen(request, badBooleanDocument()) {
			t.Errorf("bad boolean, %d segments: updateMayDeepen = false where the rule says true", segments)
		}
		compared++
		renamed := request
		renamed.Rename = map[string]string{"a": "b"}
		if !updateMayDeepen(renamed, nestedChain(2, "doc")) {
			t.Errorf("$rename, %d segments: updateMayDeepen = false, want true", segments)
		}
		compared++
	}
	if deepens == 0 || deepens == compared {
		t.Fatalf("the cases do not split: %d of %d deepen", deepens, compared)
	}
	t.Logf("%d comparisons, %d of the depth-rule rows deepen", compared, deepens)
}

// TestUpdateMayDeepenOnEncodedRequests compares the two on requests encoded as
// newUpdateGuard encodes them: a $set value nested values levels deep sits in
// a request of values+2 levels (the request, the $set map).
func TestUpdateMayDeepenOnEncodedRequests(t *testing.T) {
	for _, segments := range []int{1, 2, 50, 97, 98, 99, 100} {
		for _, values := range []int{0, 1, 2, 3, 50, 97, 98, 99, 100, 101} {
			name := fmt.Sprintf("%d segments, value %d levels", segments, values)
			path := strings.TrimSuffix(strings.Repeat("a.", segments), ".")
			var value interface{} = int32(1)
			if values > 0 {
				value = deepClientDocument(values)
			}
			request := UpdateRequest{Database: "app", Bucket: "b", Key: "k", Set: map[string]interface{}{path: value}}
			encoded, err := bson.Marshal(request)
			if err != nil {
				t.Fatalf("%s: encoding the request: %v", name, err)
			}
			requestDepth := values + 2
			holdKnownDepth(t, encoded, requestDepth)
			if got, want := updateMayDeepen(request, encoded), oldUpdateMayDeepenRule(segments, requestDepth, true); got != want {
				t.Errorf("%s: updateMayDeepen = %t, the rule says %t", name, got, want)
			}
		}
	}
}
