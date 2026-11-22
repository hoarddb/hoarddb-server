package server

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/transport"
)

func regexFilter(pattern string) map[string]interface{} {
	return map[string]interface{}{"Name": map[string]interface{}{"$regex": pattern}}
}

func costTestDoc() map[string]interface{} {
	return map[string]interface{}{"Name": strings.Repeat("abcdefghij", 100)}
}

// TestFilterCostIsRefusedBeforeEvaluation proves that a filter whose work is
// far larger than its size is refused by name, with the limit and the fix,
// and that the refusal does not itself cost the work it prevents.
func TestFilterCostIsRefusedBeforeEvaluation(t *testing.T) {
	var alternation strings.Builder
	for i := 0; alternation.Len() < 360*1024; i++ {
		fmt.Fprintf(&alternation, "w%dx|", i)
	}
	orClauses := make([]interface{}, 0, 200)
	for i := 0; i < 200; i++ {
		orClauses = append(orClauses, map[string]interface{}{"Age": float64(i)})
	}
	members := make([]interface{}, 0, 5000)
	for i := 0; i < 5000; i++ {
		members = append(members, float64(i))
	}
	nested := map[string]interface{}{"Age": float64(1)}
	for i := 0; i < 40; i++ {
		nested = map[string]interface{}{"$or": []interface{}{nested}}
	}

	cases := []struct {
		label  string
		filter map[string]interface{}
		names  string
	}{
		{"a very long pattern", regexFilter(alternation.String()), "4096"},
		{"a short pattern with a huge program", regexFilter(strings.Repeat("a{1000}", 500)), "512"},
		{"too many $or clauses", map[string]interface{}{"$or": orClauses}, "$or"},
		{"too many $in members", map[string]interface{}{"Age": map[string]interface{}{"$in": members}}, "$in"},
		{"too deep a nesting", nested, "levels deep"},
	}
	for _, tc := range cases {
		t.Run(tc.label, func(t *testing.T) {
			start := time.Now()
			var lastErr error
			for i := 0; i < 50; i++ {
				_, lastErr = compileFilter(tc.filter).matches(costTestDoc())
			}
			if elapsed := time.Since(start); elapsed > 2*time.Second {
				t.Fatalf("50 evaluations took %v; the filter must be refused before any work", elapsed)
			}
			if lastErr == nil {
				t.Fatal("the filter was accepted; it must be refused with the limit named")
			}
			message := lastErr.Error()
			if !strings.Contains(message, tc.names) {
				t.Fatalf("error %q does not name %q", message, tc.names)
			}
			if len(message) > 1024 {
				t.Fatalf("error is %d bytes; it must not echo the filter", len(message))
			}
		})
	}
}

// TestRegexIsCompiledOncePerQuery proves a $regex is compiled when the query
// is, not again for every document it examines.
func TestRegexIsCompiledOncePerQuery(t *testing.T) {
	compiled := compileFilter(regexFilter("(abc|def|ghi)+j"))
	doc := costTestDoc()
	allocations := testing.AllocsPerRun(100, func() {
		if _, err := compiled.matches(doc); err != nil {
			t.Fatal(err)
		}
	})
	if allocations > 8 {
		t.Fatalf("one document cost %.0f allocations; the pattern is being recompiled per document", allocations)
	}
}

// TestFilterWithinLimitsStillMatches guards the other side: ordinary filters
// at the edge of the limits keep working.
func TestFilterWithinLimitsStillMatches(t *testing.T) {
	ordinary := map[string]string{
		"prefix":                 `^abc`,
		"contains":               `fghij`,
		"email shape":            `^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$`,
		"case-insensitive words": `(?i)\b(error|warning|fatal|timeout)\b|abcdefghij`,
		"date shape":             `^\d{4}-\d{2}-\d{2}`,
		"500-byte literal":       strings.Repeat("z", 499) + "j",
	}
	for label, pattern := range ordinary {
		matched, err := compileFilter(regexFilter(pattern)).matches(costTestDoc())
		if err != nil {
			t.Errorf("%s (%q) was refused: %v", label, pattern, err)
		}
		_ = matched
	}
	members := make([]interface{}, 0, 1000)
	for i := 0; i < 1000; i++ {
		members = append(members, float64(i))
	}
	matched, err := compileFilter(map[string]interface{}{"Age": map[string]interface{}{"$in": members}}).matches(map[string]interface{}{"Age": float64(7)})
	if err != nil || !matched {
		t.Fatalf("a 1000-member $in = %v, %v; want true, nil", matched, err)
	}
}

// TestFilterTotalsAreEnforced proves the two whole-filter limits each go red
// when raised: the regex steps of one filter, and its clause count.
func TestFilterTotalsAreEnforced(t *testing.T) {
	// Each of these patterns is about 400 steps: under the per-pattern limit.
	pattern := strings.Repeat("a", 400)
	regexClauses := func(count int) map[string]interface{} {
		clauses := make([]interface{}, 0, count)
		for i := 0; i < count; i++ {
			clauses = append(clauses, map[string]interface{}{fmt.Sprintf("F%d", i): map[string]interface{}{"$regex": pattern}})
		}
		return map[string]interface{}{"$or": clauses}
	}
	if _, err := compileFilter(regexClauses(5)).matches(map[string]interface{}{}); err != nil {
		t.Fatalf("5 patterns of 400 steps (2000 in all) were refused: %v", err)
	}
	if _, err := compileFilter(regexClauses(6)).matches(map[string]interface{}{}); err == nil || !strings.Contains(err.Error(), "2048") {
		t.Fatalf("6 patterns of 400 steps (2400 in all) returned %v; want a refusal naming 2048", err)
	}

	clauses := func(count int) map[string]interface{} {
		filter := make(map[string]interface{}, count)
		for i := 0; i < count; i++ {
			filter[fmt.Sprintf("F%d", i)] = float64(i)
		}
		return filter
	}
	if _, err := compileFilter(clauses(1000)).matches(map[string]interface{}{}); err != nil {
		t.Fatalf("1000 clauses were refused: %v", err)
	}
	if _, err := compileFilter(clauses(1001)).matches(map[string]interface{}{}); err == nil || !strings.Contains(err.Error(), "1000") {
		t.Fatalf("1001 clauses returned %v; want a refusal naming 1000", err)
	}
}

// TestMatchWorkIsBoundedPerDocument proves the cost of a match is charged by
// the pattern's size times the field's length, and cumulatively across every
// $regex one document meets.
func TestMatchWorkIsBoundedPerDocument(t *testing.T) {
	boundedClass := "[a-z]{0,50}b" // about 100 steps
	longField := func(length int) map[string]interface{} {
		return map[string]interface{}{"Name": strings.Repeat("a", length)}
	}
	orOf := func(count int, pattern string) map[string]interface{} {
		clauses := make([]interface{}, 0, count)
		for i := 0; i < count; i++ {
			clauses = append(clauses, regexFilter(pattern))
		}
		return map[string]interface{}{"$or": clauses}
	}
	cases := []struct {
		label   string
		filter  map[string]interface{}
		doc     map[string]interface{}
		refused bool
	}{
		{"one 100-step pattern on 1,000 bytes", regexFilter(boundedClass), longField(1000), false},
		{"one 100-step pattern on 20,000 bytes", regexFilter(boundedClass), longField(20000), true},
		{"8 small patterns on 1,000 bytes", orOf(8, "a.*b"), longField(1000), false},
		{"30 100-step patterns on 1,000 bytes, charged together", orOf(30, boundedClass), longField(1000), true},
		{"a pattern at the per-pattern limit on 10,000 bytes", regexFilter("[a-z]{0,250}b"), longField(10000), true},
	}
	for _, tc := range cases {
		t.Run(tc.label, func(t *testing.T) {
			start := time.Now()
			_, err := compileFilter(tc.filter).matches(tc.doc)
			elapsed := time.Since(start)
			if tc.refused && (err == nil || !strings.Contains(err.Error(), "work limit")) {
				t.Fatalf("returned %v; want a refusal naming the work limit", err)
			}
			if !tc.refused && err != nil {
				t.Fatalf("was refused: %v", err)
			}
			// The refusal above is the accounting; the clock is only a sanity
			// check, and the race detector slows a match past any fixed bound.
			if !raceEnabled && elapsed > 50*time.Millisecond {
				t.Fatalf("took %v for one document; the budget is about 11 ms", elapsed)
			}
		})
	}
}

// TestEveryFilteredReadRefusesAnOversizedPattern proves the limit holds on each
// read that evaluates a filter, not only in the matcher.
func TestEveryFilteredReadRefusesAnOversizedPattern(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "users", BucketTypeHash)
	for i := 0; i < 3; i++ {
		insertScanDoc(t, s, "app", "users", fmt.Sprintf("user-%d", i), map[string]interface{}{"Name": strings.Repeat("abcdefghij", 100)})
	}
	oversized := regexFilter(strings.Repeat("w1234x|", 50000))

	reads := map[string]func() error{
		"scan": func() error {
			_, err := s.Scan(ScanRequest{Database: "app", Bucket: "users", Filter: oversized})
			return err
		},
		"sorted scan": func() error {
			_, err := s.Scan(ScanRequest{Database: "app", Bucket: "users", Filter: oversized, Sort: map[string]int{"Name": 1}})
			return err
		},
		"search": func() error {
			_, err := s.Search(SearchRequest{Database: "app", Bucket: "users", Filter: oversized})
			return err
		},
		"count": func() error {
			_, err := s.Count(CountRequest{Database: "app", Bucket: "users", Filter: oversized})
			return err
		},
		"distinct": func() error {
			_, err := s.Distinct(DistinctRequest{Database: "app", Bucket: "users", Field: "Name", Filter: oversized})
			return err
		},
	}
	for label, read := range reads {
		t.Run(label, func(t *testing.T) {
			start := time.Now()
			err := read()
			if err == nil || !strings.Contains(err.Error(), "4096") {
				t.Fatalf("%s with a 350 KB pattern returned %v; want an error naming the 4096-byte limit", label, err)
			}
			if elapsed := time.Since(start); elapsed > time.Second {
				t.Fatalf("%s took %v to refuse", label, elapsed)
			}
		})
	}
}

// TestFilterRefusalReachesTheWireAsInvalidArgument keeps a refused filter from
// being reported as a server fault.
func TestFilterRefusalReachesTheWireAsInvalidArgument(t *testing.T) {
	_, err := compileFilter(regexFilter(strings.Repeat("w1234x|", 5000))).matches(costTestDoc())
	if code := codeFor(err); code != transport.ErrInvalidArgument {
		t.Fatalf("a refused filter maps to %v; want INVALID_ARGUMENT", code)
	}
}

// TestSearchHitsShareOnePerDocumentBudget proves the work budget holds on the
// search path, where a flat filter of several fields has no $or to carry it:
// each field's regex is cheap, together they are not.
func TestSearchHitsShareOnePerDocumentBudget(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "users", BucketTypeHash)
	document := map[string]interface{}{}
	filter := map[string]interface{}{}
	for i := 0; i < 18; i++ {
		field := fmt.Sprintf("F%d", i)
		document[field] = strings.Repeat("a", 1000)
		filter[field] = map[string]interface{}{"$regex": "[a-z]{0,50}b"} // about 100 steps
	}
	insertScanDoc(t, s, "app", "users", "user-1", document)

	_, err := s.Search(SearchRequest{Database: "app", Bucket: "users", Filter: filter})
	if err == nil || !strings.Contains(err.Error(), "work limit") {
		t.Fatalf("a search of 18 regexes on 1,000-byte fields returned %v; want a refusal naming the work limit", err)
	}

	for field := range filter {
		if field != "F0" {
			delete(filter, field)
		}
	}
	if _, err := s.Search(SearchRequest{Database: "app", Bucket: "users", Filter: filter}); err != nil {
		t.Fatalf("a search of one such regex was refused: %v", err)
	}
}

// TestMatchWorkIsChargedInBytes proves a field is charged by its bytes, which
// is what the scan costs, and not by its characters.
func TestMatchWorkIsChargedInBytes(t *testing.T) {
	clauses := make([]interface{}, 0, 8)
	for i := 0; i < 8; i++ {
		clauses = append(clauses, regexFilter("[a-z]{0,50}b")) // about 100 steps each
	}
	filter := map[string]interface{}{"$or": clauses}

	// 1,000 characters of 2 bytes each: 8 x 102 steps x 2,000 bytes is over the
	// budget; counted as characters it would be half of that, and under it.
	if _, err := compileFilter(filter).matches(map[string]interface{}{"Name": strings.Repeat("é", 1000)}); err == nil || !strings.Contains(err.Error(), "2000 bytes") {
		t.Fatalf("1,000 two-byte characters returned %v; want a refusal that counts 2000 bytes", err)
	}
	if _, err := compileFilter(filter).matches(map[string]interface{}{"Name": strings.Repeat("a", 1000)}); err != nil {
		t.Fatalf("1,000 one-byte characters were refused: %v", err)
	}
}

// TestLiteralPatternsAreChargedAsTheyRun proves only a literal anchored at the
// start is charged as the prefix check Go runs it as. A literal found anywhere
// else is verified by the program at every candidate position, which on
// self-overlapping text costs about its length times the field's, so it keeps
// its full charge, as does a case-insensitive one.
func TestLiteralPatternsAreChargedAsTheyRun(t *testing.T) {
	cases := []struct {
		label   string
		pattern string
		field   string
		refused bool
	}{
		{"a start-anchored literal on 1 MB", "^foo", strings.Repeat("a", 1<<20), false},
		{"a start-and-end-anchored literal on 1 MB", "^foo$", strings.Repeat("a", 1<<20), false},
		{"a start-anchored 500-byte literal on 4 KB", "^" + strings.Repeat("a", 499) + "b", strings.Repeat("a", 4000), false},
		{"an unanchored literal on 400 KB", "foo", strings.Repeat("a", 400<<10), false},
		{"an unanchored literal on 1 MB", "foo", strings.Repeat("a", 1<<20), true},
		{"an unanchored 500-byte literal on 4 KB", strings.Repeat("z", 500), strings.Repeat("a", 4000), true},
		{"a case-insensitive literal on 2 MB", "(?i)foo", strings.Repeat("a", 2<<20), true},
		{"a literal followed by a wildcard on 2 MB", "foo.*bar", strings.Repeat("a", 2<<20), true},
		{"a self-overlapping literal on 1.4 MB", strings.Repeat("a", 499) + "b", strings.Repeat("a", 1400000), true},
		{"an end-anchored-only literal on 1.4 MB", strings.Repeat("a", 500) + "$", strings.Repeat("a", 1400000), true},
		{"an end-anchored-only short literal on 1.4 MB", "foo$", strings.Repeat("a", 1400000), true},
	}
	for _, tc := range cases {
		t.Run(tc.label, func(t *testing.T) {
			doc := map[string]interface{}{"Name": tc.field}
			start := time.Now()
			_, err := compileFilter(regexFilter(tc.pattern)).matches(doc)
			elapsed := time.Since(start)
			if tc.refused && (err == nil || !strings.Contains(err.Error(), "work limit")) {
				t.Fatalf("%q on %d bytes returned %v; want a refusal naming the work limit", tc.pattern, len(tc.field), err)
			}
			if !tc.refused && err != nil {
				t.Fatalf("%q on %d bytes was refused: %v", tc.pattern, len(tc.field), err)
			}
			if !raceEnabled && elapsed > 50*time.Millisecond {
				t.Fatalf("%q on %d bytes took %v for one document", tc.pattern, len(tc.field), elapsed)
			}
		})
	}
}

// TestAnOverLimitFilterIsRefusedOnAnEmptyBucket proves the static limits are
// answered when the filter is compiled, as the user limits page says, and not
// at the first document: an empty bucket must refuse the query with the same
// text a bucket with documents answers (BUG-0690). The per-document regex work
// budget is deliberately not part of this: it is charged by what a document
// holds, so it can only be reached by a document.
func TestAnOverLimitFilterIsRefusedOnAnEmptyBucket(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "emptyq", BucketTypeHash)
	createTestBucket(t, s, "app", "populated", BucketTypeHash)
	insertScanDoc(t, s, "app", "populated", "doc-1", map[string]interface{}{"Name": "abc", "Age": float64(1)})

	members := make([]interface{}, 0, maxFilterInMembers+1)
	for i := 0; i <= maxFilterInMembers; i++ {
		members = append(members, float64(i))
	}
	filters := map[string]map[string]interface{}{
		"a $regex over the byte limit": regexFilter(strings.Repeat("a", maxRegexPatternBytes+1)),
		"an $in over the member limit": {"Age": map[string]interface{}{"$in": members}},
	}
	reads := map[string]func(database, bucket string, filter map[string]interface{}) error{
		"find": func(database, bucket string, filter map[string]interface{}) error {
			_, err := s.Scan(ScanRequest{Database: database, Bucket: bucket, Filter: filter})
			return err
		},
		"sorted find": func(database, bucket string, filter map[string]interface{}) error {
			_, err := s.Scan(ScanRequest{Database: database, Bucket: bucket, Filter: filter, Sort: map[string]int{"Name": 1}})
			return err
		},
		"update many": func(database, bucket string, filter map[string]interface{}) error {
			_, err := s.Update(UpdateRequest{Database: database, Bucket: bucket, Filter: filter, Multi: true, Set: map[string]interface{}{"Seen": true}})
			return err
		},
		"count": func(database, bucket string, filter map[string]interface{}) error {
			_, err := s.Count(CountRequest{Database: database, Bucket: bucket, Filter: filter})
			return err
		},
		"distinct": func(database, bucket string, filter map[string]interface{}) error {
			_, err := s.Distinct(DistinctRequest{Database: database, Bucket: bucket, Field: "Name", Filter: filter})
			return err
		},
		"search": func(database, bucket string, filter map[string]interface{}) error {
			_, err := s.Search(SearchRequest{Database: database, Bucket: bucket, Filter: filter})
			return err
		},
	}
	for readLabel, read := range reads {
		for filterLabel, filter := range filters {
			t.Run(readLabel+"/"+filterLabel, func(t *testing.T) {
				populated := read("app", "populated", filter)
				if populated == nil {
					t.Fatalf("%s with %s was accepted on a populated bucket; want a refusal", readLabel, filterLabel)
				}
				empty := read("app", "emptyq", filter)
				if empty == nil {
					t.Fatalf("%s with %s on an empty bucket answered no matches; want the refusal a populated bucket gives", readLabel, filterLabel)
				}
				if empty.Error() != populated.Error() {
					t.Fatalf("%s with %s on an empty bucket said %q; on a populated bucket it says %q",
						readLabel, filterLabel, empty.Error(), populated.Error())
				}
				if code := codeFor(empty); code != transport.ErrInvalidArgument {
					t.Fatalf("%s with %s on an empty bucket maps to %v; want INVALID_ARGUMENT", readLabel, filterLabel, code)
				}
			})
		}
	}
}

// TestAnOverLimitFilterIsRefusedBeforeAnIndexIsRead covers the search that an
// index serves, which compiles its filter on a different path than a scan: the
// index of an empty bucket names no document, so a refusal kept until a
// document is read never comes (BUG-0690).
func TestAnOverLimitFilterIsRefusedBeforeAnIndexIsRead(t *testing.T) {
	s := newTestServer(t)
	createIndexedTestBucket(t, s, "app", "emptyidx", []interface{}{"Email"})
	resp, err := s.CreateBucket(CreateBucketRequest{
		Database: "app", Name: "populatedidx", Type: BucketTypeHash,
		Options: map[string]interface{}{"indices": []interface{}{"Email"}},
	})
	if err != nil || !resp.Success {
		t.Fatalf("CreateBucket(populatedidx) = %+v, %v", resp, err)
	}
	insertScanDoc(t, s, "app", "populatedidx", "doc-1", map[string]interface{}{"Email": "a@example.com", "Name": "abc"})

	filter := map[string]interface{}{
		"Email": "a@example.com",
		"Name":  map[string]interface{}{"$regex": strings.Repeat("a", maxRegexPatternBytes+1)},
	}
	for _, bucket := range []string{"emptyidx", "populatedidx"} {
		opened, err := s.resolveBucket("app", bucket)
		if err != nil {
			t.Fatalf("resolving %s: %v", bucket, err)
		}
		if plan, err := opened.idxMgr.PlanSearch(filter); err != nil || plan == nil {
			t.Fatalf("%s: the filter does not choose the Email index (%v, %v); the case would not reach searchByIndex", bucket, plan, err)
		}
	}

	populated := searchRefusal(s, "populatedidx", filter)
	if populated == nil {
		t.Fatal("an over-limit filter was accepted on a populated indexed bucket; want a refusal")
	}
	empty := searchRefusal(s, "emptyidx", filter)
	if empty == nil {
		t.Fatal("an over-limit filter on an empty indexed bucket answered no matches; want the refusal a populated bucket gives")
	}
	if empty.Error() != populated.Error() {
		t.Fatalf("empty indexed bucket said %q; populated one says %q", empty.Error(), populated.Error())
	}
}

func searchRefusal(s *Server, bucket string, filter map[string]interface{}) error {
	_, err := s.Search(SearchRequest{Database: "app", Bucket: bucket, Filter: filter})
	return err
}
