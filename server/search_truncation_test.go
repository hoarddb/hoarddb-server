package server

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// searchCase is one search under test: the field its filter names decides the
// path (City has an index; Tier has none, so that search walks the bucket).
type searchCase struct {
	name     string
	field    string
	wantPath string
}

var searchPaths = []searchCase{
	{"an indexed search", "City", "City_1"},
	{"a full-scan search", "Tier", "none — full scan"},
}

var searchFilterValue = map[string]string{"City": "Oslo", "Tier": "gold"}

// hasMoreOnWire reads has_more from the answer as it is sent, in BSON and in
// JSON, so the test pins the wire name and not only a Go field.
func hasMoreOnWire(t *testing.T, answer *SearchResponse) bool {
	t.Helper()
	encoded, err := bson.Marshal(answer)
	if err != nil {
		t.Fatalf("marshalling the answer: %v", err)
	}
	flag, err := bson.Raw(encoded).LookupErr("has_more")
	if err != nil {
		t.Fatalf("the search answer carries no has_more in BSON: %v", err)
	}
	return flag.Boolean()
}

func searchOslo(t *testing.T, s *Server, tc searchCase, limit int) *SearchResponse {
	t.Helper()
	answer, err := s.Search(SearchRequest{Database: "app", Bucket: "cities",
		Filter: map[string]interface{}{tc.field: searchFilterValue[tc.field]}, Limit: limit})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if answer.IndexUsed != tc.wantPath {
		t.Fatalf("index_used %q, want %q (the test must cover this path)", answer.IndexUsed, tc.wantPath)
	}
	return answer
}

func insertOsloDocuments(t *testing.T, s *Server, count int) {
	t.Helper()
	for i := 1; i <= count; i++ {
		mustInsert(t, s, "app", "cities", fmt.Sprintf("k%05d", i), map[string]interface{}{"City": "Oslo", "Tier": "gold"})
	}
}

// TestSearchSaysWhenItWasTruncated is BUG-0517's failing test: total is the
// number returned, so has_more is the only signal that a search was cut at its
// limit. It must be true only when a real further match exists.
func TestSearchSaysWhenItWasTruncated(t *testing.T) {
	for _, tc := range searchPaths {
		for _, row := range []struct {
			name        string
			matches     int
			limit       int
			wantResults int
			wantHasMore bool
		}{
			{"more matches than the limit", 5, 3, 3, true},
			{"exactly as many matches as the limit", 3, 3, 3, false},
			{"fewer matches than the limit", 2, 3, 2, false},
			{"limit 0 means no limit", 5, 0, 5, false},
			{"one match past the limit", 4, 3, 3, true},
		} {
			t.Run(tc.name+"/"+row.name, func(t *testing.T) {
				s := newTestServer(t)
				createUniqueBucket(t, s, "app", "cities", "City")
				insertOsloDocuments(t, s, row.matches)
				answer := searchOslo(t, s, tc, row.limit)
				if len(answer.Results) != row.wantResults || answer.Total != row.wantResults {
					t.Errorf("%d results, total %d; want both %d (total is the number returned)", len(answer.Results), answer.Total, row.wantResults)
				}
				if got := hasMoreOnWire(t, answer); got != row.wantHasMore {
					t.Errorf("has_more = %v, want %v", got, row.wantHasMore)
				}
			})
		}
	}
}

// TestSearchHasMoreIsNotSetByAnOrphanOrStaleEntry is the subtle case: an index
// entry just past the limit that the residual filter rejects is not a further
// match, so has_more stays false. A real match behind it sets it.
func TestSearchHasMoreIsNotSetByAnOrphanOrStaleEntry(t *testing.T) {
	for _, row := range []struct {
		name        string
		plant       func(t *testing.T, s *Server)
		wantHasMore bool
	}{
		{"an orphan entry past the limit", func(t *testing.T, s *Server) {
			plantEntry(t, s, servedBucket(t, s, "app", "cities"), 0, []interface{}{"Oslo"}, "k00003zz-ghost")
		}, false},
		{"a stale entry past the limit", func(t *testing.T, s *Server) {
			mustInsert(t, s, "app", "cities", "k00004", map[string]interface{}{"City": "Bergen"})
			plantEntry(t, s, servedBucket(t, s, "app", "cities"), 0, []interface{}{"Oslo"}, "k00004")
		}, false},
		{"an orphan entry and then a real match", func(t *testing.T, s *Server) {
			plantEntry(t, s, servedBucket(t, s, "app", "cities"), 0, []interface{}{"Oslo"}, "k00003zz-ghost")
			mustInsert(t, s, "app", "cities", "k00009", map[string]interface{}{"City": "Oslo"})
		}, true},
	} {
		t.Run(row.name, func(t *testing.T) {
			s := newTestServer(t)
			createUniqueBucket(t, s, "app", "cities", "City")
			insertOsloDocuments(t, s, 3)
			row.plant(t, s)
			answer := searchOslo(t, s, searchPaths[0], 3)
			if len(answer.Results) != 3 || answer.Total != 3 {
				t.Errorf("%d results, total %d; want 3 and 3", len(answer.Results), answer.Total)
			}
			if got := hasMoreOnWire(t, answer); got != row.wantHasMore {
				t.Errorf("has_more = %v, want %v", got, row.wantHasMore)
			}
		})
	}
}

// TestSearchHasMoreOnEveryStoreTypeSearchAnswers: SEARCH on a fifo, lifo,
// btree or heap bucket is a full scan (BUG-0516), and it says when it was cut.
func TestSearchHasMoreOnEveryStoreTypeSearchAnswers(t *testing.T) {
	for _, storeType := range []BucketType{BucketTypeFIFO, BucketTypeLIFO, BucketTypeBTree, BucketTypeHeap, BucketTypeHash} {
		t.Run(string(storeType), func(t *testing.T) {
			s := newTestServer(t)
			createTestBucket(t, s, "app", "cities", storeType)
			for i := 1; i <= 4; i++ {
				document := map[string]interface{}{"Tier": "gold"}
				if storeType == BucketTypeHeap {
					pushed, err := s.Push(PushRequest{Database: "app", Bucket: "cities", Priority: int64(i), Data: document})
					if err != nil || !pushed.Success {
						t.Fatalf("Push(%d) = %+v, %v", i, pushed, err)
					}
					continue
				}
				insertScanDoc(t, s, "app", "cities", fmt.Sprintf("k%05d", i), document)
			}
			for _, row := range []struct {
				limit       int
				wantResults int
				wantHasMore bool
			}{{3, 3, true}, {4, 4, false}, {0, 4, false}} {
				answer := searchOslo(t, s, searchPaths[1], row.limit)
				if len(answer.Results) != row.wantResults {
					t.Errorf("limit %d: %d results, want %d", row.limit, len(answer.Results), row.wantResults)
				}
				if got := hasMoreOnWire(t, answer); got != row.wantHasMore {
					t.Errorf("limit %d: has_more = %v, want %v", row.limit, got, row.wantHasMore)
				}
			}
		})
	}
}

// TestSearchTotalStaysTheNumberReturned pins the decision that total keeps its
// meaning (the number returned) next to has_more: the match count is not
// computed, because that is O(matches).
func TestSearchTotalStaysTheNumberReturned(t *testing.T) {
	for _, tc := range searchPaths {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestServer(t)
			createUniqueBucket(t, s, "app", "cities", "City")
			insertOsloDocuments(t, s, 5)
			answer := searchOslo(t, s, tc, 3)
			if len(answer.Results) != 3 || answer.Total != 3 {
				t.Errorf("limit 3 of 5 matches: %d results, total %d; want 3 and 3", len(answer.Results), answer.Total)
			}
		})
	}
}

// TestSearchLookPastTheLimitIsBounded shows the one-past-the-limit look does
// not grow with the matches: limit 3 visits 4 documents whether 5 or 5,000
// match.
func TestSearchLookPastTheLimitIsBounded(t *testing.T) {
	for _, tc := range searchPaths {
		t.Run(tc.name, func(t *testing.T) {
			visitsFor := func(matches int) int64 {
				s := newTestServer(t)
				createUniqueBucket(t, s, "app", "cities", "City")
				insertOsloDocuments(t, s, matches)
				var visits atomic.Int64
				hook := func(method string) {
					if method == queryMethodSearch {
						visits.Add(1)
					}
				}
				queryVisitHook.Store(&hook)
				defer queryVisitHook.Store(nil)
				if !hasMoreOnWire(t, searchOslo(t, s, tc, 3)) {
					t.Fatalf("%d matches at limit 3: has_more false", matches)
				}
				return visits.Load()
			}
			few, many := visitsFor(5), visitsFor(5000)
			if few != 4 || many != 4 {
				t.Errorf("visits for limit 3: %d with 5 matches, %d with 5000; want 4 (the limit plus one) for both", few, many)
			}
		})
	}
}
