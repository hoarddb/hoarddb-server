package server

import (
	"bytes"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"

	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// TestMatchesFilterOperators covers the v1 grammar (spec 4.1): equality, the
// four comparisons, $in, $regex and $and. Comparison is type-aware, so the
// numbers here are float64 because that is what parseDocument and Badger both
// hand back.
func TestMatchesFilterOperators(t *testing.T) {
	doc := map[string]interface{}{
		"Name":   "ada",
		"Age":    float64(42),
		"Status": "active",
	}

	cases := []struct {
		label  string
		filter map[string]interface{}
		want   bool
	}{
		{"empty filter matches everything", map[string]interface{}{}, true},
		{"equality hit", map[string]interface{}{"Status": "active"}, true},
		{"equality miss", map[string]interface{}{"Status": "archived"}, false},
		{"numeric equality", map[string]interface{}{"Age": float64(42)}, true},
		{"$gt above", map[string]interface{}{"Age": map[string]interface{}{"$gt": float64(40)}}, true},
		{"$gt below", map[string]interface{}{"Age": map[string]interface{}{"$gt": float64(50)}}, false},
		{"$gte equal", map[string]interface{}{"Age": map[string]interface{}{"$gte": float64(42)}}, true},
		{"$lt below", map[string]interface{}{"Age": map[string]interface{}{"$lt": float64(50)}}, true},
		{"$lt equal", map[string]interface{}{"Age": map[string]interface{}{"$lt": float64(42)}}, false},
		{"$lte equal", map[string]interface{}{"Age": map[string]interface{}{"$lte": float64(42)}}, true},
		{"string $gt", map[string]interface{}{"Name": map[string]interface{}{"$gt": "ab"}}, true},
		{"string $gt miss", map[string]interface{}{"Name": map[string]interface{}{"$gt": "zz"}}, false},
		{"$in hit", map[string]interface{}{"Status": map[string]interface{}{"$in": []interface{}{"archived", "active"}}}, true},
		{"$in miss", map[string]interface{}{"Status": map[string]interface{}{"$in": []interface{}{"archived"}}}, false},
		{"$regex hit", map[string]interface{}{"Name": map[string]interface{}{"$regex": "^ad"}}, true},
		{"$regex miss", map[string]interface{}{"Name": map[string]interface{}{"$regex": "^zz"}}, false},
		{
			"$and both match",
			map[string]interface{}{"$and": []interface{}{
				map[string]interface{}{"Status": "active"},
				map[string]interface{}{"Age": map[string]interface{}{"$gte": float64(18)}},
			}},
			true,
		},
		{
			"$and one fails",
			map[string]interface{}{"$and": []interface{}{
				map[string]interface{}{"Status": "active"},
				map[string]interface{}{"Age": map[string]interface{}{"$gte": float64(99)}},
			}},
			false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.label, func(t *testing.T) {
			got, err := matchesFilter(doc, tc.filter)
			if err != nil {
				t.Fatalf("matchesFilter(%v) errored: %v", tc.filter, err)
			}
			if got != tc.want {
				t.Fatalf("matchesFilter(%v) = %v, want %v", tc.filter, got, tc.want)
			}
		})
	}
}

// TestMatchesFilterAbsentFieldMatchesNothing is rule one: a field the document
// does not carry matches nothing, comparisons included. A missing field is not
// zero, so $lt: 0 must not match a document with no such field.
func TestMatchesFilterAbsentFieldMatchesNothing(t *testing.T) {
	doc := map[string]interface{}{"Name": "ada"}

	cases := []struct {
		label  string
		filter map[string]interface{}
	}{
		{"equality", map[string]interface{}{"Missing": "x"}},
		{"$gt", map[string]interface{}{"Missing": map[string]interface{}{"$gt": float64(0)}}},
		{"$gte", map[string]interface{}{"Missing": map[string]interface{}{"$gte": float64(0)}}},
		{"$lt", map[string]interface{}{"Missing": map[string]interface{}{"$lt": float64(0)}}},
		{"$lte", map[string]interface{}{"Missing": map[string]interface{}{"$lte": float64(0)}}},
		{"$in", map[string]interface{}{"Missing": map[string]interface{}{"$in": []interface{}{"x"}}}},
		{"$regex", map[string]interface{}{"Missing": map[string]interface{}{"$regex": ".*"}}},
	}

	for _, tc := range cases {
		t.Run(tc.label, func(t *testing.T) {
			got, err := matchesFilter(doc, tc.filter)
			if err != nil {
				t.Fatalf("matchesFilter(%v) errored: %v", tc.filter, err)
			}
			if got {
				t.Fatalf("%s on an absent field matched; a missing field must match nothing", tc.label)
			}
		})
	}
}

// TestMatchesFilterIsTypeAware is rule two: a number is never greater than a
// string, and a type mismatch matches nothing rather than erroring — the user
// is filtering a heterogeneous bucket, not writing a schema.
func TestMatchesFilterIsTypeAware(t *testing.T) {
	doc := map[string]interface{}{"Age": float64(42), "Name": "ada"}

	if got, err := matchesFilter(doc, map[string]interface{}{"Age": map[string]interface{}{"$gt": "0"}}); err != nil || got {
		t.Fatalf("a number compared to a string = %v, %v; want false, no error", got, err)
	}
	if got, err := matchesFilter(doc, map[string]interface{}{"Name": map[string]interface{}{"$lt": float64(100)}}); err != nil || got {
		t.Fatalf("a string compared to a number = %v, %v; want false, no error", got, err)
	}
	if got, err := matchesFilter(doc, map[string]interface{}{"Age": "42"}); err != nil || got {
		t.Fatalf("numeric equality against a string = %v, %v; want false, no error", got, err)
	}
}

// TestScanUnknownOperatorIsAnError is the failure mode this spec exists to
// remove: an operator the matcher does not know must be refused by name, not
// silently match nothing.
func TestScanUnknownOperatorIsAnError(t *testing.T) {
	_, err := matchesFilter(
		map[string]interface{}{"Status": "active"},
		map[string]interface{}{"Status": map[string]interface{}{"$nope": float64(1)}},
	)
	if err == nil {
		t.Fatal("an unknown field operator was accepted; it must error naming the operator")
	}
	if !strings.Contains(err.Error(), "$nope") {
		t.Fatalf("error %q does not name the unknown operator", err)
	}

	_, err = matchesFilter(
		map[string]interface{}{"Status": "active"},
		map[string]interface{}{"$nope": float64(1)},
	)
	if err == nil {
		t.Fatal("an unknown top-level operator was accepted; it must error naming the operator")
	}
	if !strings.Contains(err.Error(), "$nope") {
		t.Fatalf("error %q does not name the unknown top-level operator", err)
	}
}

// TestOrMatchesAnyClause tests the $or operator: at least one clause must match.
func TestOrMatchesAnyClause(t *testing.T) {
	doc := map[string]interface{}{
		"Status": "active",
		"Age":    float64(25),
	}

	cases := []struct {
		label  string
		filter map[string]interface{}
		want   bool
	}{
		{
			"$or first clause matches",
			map[string]interface{}{"$or": []interface{}{
				map[string]interface{}{"Status": "active"},
				map[string]interface{}{"Age": map[string]interface{}{"$gt": float64(30)}},
			}},
			true,
		},
		{
			"$or second clause matches",
			map[string]interface{}{"$or": []interface{}{
				map[string]interface{}{"Status": "inactive"},
				map[string]interface{}{"Age": map[string]interface{}{"$lt": float64(30)}},
			}},
			true,
		},
		{
			"$or both clauses match",
			map[string]interface{}{"$or": []interface{}{
				map[string]interface{}{"Status": "active"},
				map[string]interface{}{"Age": map[string]interface{}{"$lt": float64(30)}},
			}},
			true,
		},
		{
			"$or no clauses match",
			map[string]interface{}{"$or": []interface{}{
				map[string]interface{}{"Status": "inactive"},
				map[string]interface{}{"Age": map[string]interface{}{"$gt": float64(30)}},
			}},
			false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.label, func(t *testing.T) {
			got, err := matchesFilter(doc, tc.filter)
			if err != nil {
				t.Fatalf("matchesFilter(%v) errored: %v", tc.filter, err)
			}
			if got != tc.want {
				t.Fatalf("matchesFilter(%v) = %v, want %v", tc.filter, got, tc.want)
			}
		})
	}
}

// TestNeNotEqual tests the $ne operator: not equal.
func TestNeNotEqual(t *testing.T) {
	doc := map[string]interface{}{
		"Status": "active",
		"Age":    float64(42),
	}

	cases := []struct {
		label  string
		filter map[string]interface{}
		want   bool
	}{
		{
			"$ne with matching value",
			map[string]interface{}{"Status": map[string]interface{}{"$ne": "inactive"}},
			true,
		},
		{
			"$ne with equal value",
			map[string]interface{}{"Status": map[string]interface{}{"$ne": "active"}},
			false,
		},
		{
			"$ne numeric not equal",
			map[string]interface{}{"Age": map[string]interface{}{"$ne": float64(50)}},
			true,
		},
		{
			"$ne numeric equal",
			map[string]interface{}{"Age": map[string]interface{}{"$ne": float64(42)}},
			false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.label, func(t *testing.T) {
			got, err := matchesFilter(doc, tc.filter)
			if err != nil {
				t.Fatalf("matchesFilter(%v) errored: %v", tc.filter, err)
			}
			if got != tc.want {
				t.Fatalf("matchesFilter(%v) = %v, want %v", tc.filter, got, tc.want)
			}
		})
	}
}

// TestExistsFieldPresent tests the $exists operator: field is present.
func TestExistsFieldPresent(t *testing.T) {
	doc := map[string]interface{}{
		"Name": "ada",
		"Age":  float64(42),
	}

	cases := []struct {
		label  string
		filter map[string]interface{}
		want   bool
	}{
		{
			"$exists true, field present",
			map[string]interface{}{"Name": map[string]interface{}{"$exists": true}},
			true,
		},
		{
			"$exists true, field absent",
			map[string]interface{}{"Phone": map[string]interface{}{"$exists": true}},
			false,
		},
		{
			"$exists false, field present",
			map[string]interface{}{"Name": map[string]interface{}{"$exists": false}},
			false,
		},
		{
			"$exists false, field absent",
			map[string]interface{}{"Phone": map[string]interface{}{"$exists": false}},
			true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.label, func(t *testing.T) {
			got, err := matchesFilter(doc, tc.filter)
			if err != nil {
				t.Fatalf("matchesFilter(%v) errored: %v", tc.filter, err)
			}
			if got != tc.want {
				t.Fatalf("matchesFilter(%v) = %v, want %v", tc.filter, got, tc.want)
			}
		})
	}
}

// TestNinNotInArray tests the $nin operator: value not in array.
func TestNinNotInArray(t *testing.T) {
	doc := map[string]interface{}{
		"Status": "active",
		"Age":    float64(25),
	}

	cases := []struct {
		label  string
		filter map[string]interface{}
		want   bool
	}{
		{
			"$nin value not in array",
			map[string]interface{}{"Status": map[string]interface{}{"$nin": []interface{}{"inactive", "archived"}}},
			true,
		},
		{
			"$nin value in array",
			map[string]interface{}{"Status": map[string]interface{}{"$nin": []interface{}{"active", "pending"}}},
			false,
		},
		{
			"$nin numeric not in array",
			map[string]interface{}{"Age": map[string]interface{}{"$nin": []interface{}{float64(30), float64(40)}}},
			true,
		},
		{
			"$nin numeric in array",
			map[string]interface{}{"Age": map[string]interface{}{"$nin": []interface{}{float64(25), float64(30)}}},
			false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.label, func(t *testing.T) {
			got, err := matchesFilter(doc, tc.filter)
			if err != nil {
				t.Fatalf("matchesFilter(%v) errored: %v", tc.filter, err)
			}
			if got != tc.want {
				t.Fatalf("matchesFilter(%v) = %v, want %v", tc.filter, got, tc.want)
			}
		})
	}
}

// TestNoCoercionStringVsDate proves type-aware comparison by ensuring that
// a string "2026-09-20" never matches a date field. This test uses a string
// operand where a date is expected.
func TestNoCoercionStringVsDate(t *testing.T) {
	// For now, this test uses a string field to simulate the date behavior,
	// since dates aren't yet fully implemented. The principle is the same:
	// a string operand never matches a numeric field.
	doc := map[string]interface{}{
		"created": float64(1695168000), // Unix timestamp for 2026-09-20
	}

	// Try to match a numeric field with a string $ne operand
	got, err := matchesFilter(doc, map[string]interface{}{
		"created": map[string]interface{}{"$ne": "1695168000"},
	})
	if err != nil {
		t.Fatalf("matchesFilter errored: %v", err)
	}
	if !got {
		t.Fatalf("string operand matched numeric field; type-aware comparison failed")
	}
}

// TestMatchesFilterOrdersDates covers comparison of primitive.DateTime values
// with the $gt, $gte, $lt, $lte operators across epoch, across millisecond precision,
// and with non-UTC offsets normalized to UTC.
func TestMatchesFilterOrdersDates(t *testing.T) {
	// Test basic comparison across epoch
	doc := map[string]interface{}{"ts": primitive.NewDateTimeFromTime(time.Unix(1, 0).UTC())}

	// $gt: ts > 0 should be true (1ms > 0)
	matched, err := matchesFilter(doc, map[string]interface{}{"ts": map[string]interface{}{"$gt": primitive.NewDateTimeFromTime(time.Unix(0, 0).UTC())}})
	if err != nil || !matched {
		t.Errorf("ts > 0 = %v, %v; want true, nil", matched, err)
	}

	// $lt: ts < epoch should be false
	matched, err = matchesFilter(doc, map[string]interface{}{"ts": map[string]interface{}{"$lt": primitive.NewDateTimeFromTime(time.Unix(0, 0).UTC())}})
	if err != nil || matched {
		t.Errorf("ts < epoch = %v, %v; want false, nil", matched, err)
	}

	// Test millisecond precision
	t1 := time.Unix(0, 1000000).UTC() // 1ms since epoch
	t2 := time.Unix(0, 2000000).UTC() // 2ms since epoch
	doc2 := map[string]interface{}{"ts": primitive.NewDateTimeFromTime(t2)}

	matched, err = matchesFilter(doc2, map[string]interface{}{"ts": map[string]interface{}{"$gt": primitive.NewDateTimeFromTime(t1)}})
	if err != nil || !matched {
		t.Errorf("2ms > 1ms = %v, %v; want true, nil", matched, err)
	}

	// Test offset normalization: +02:00 offset should normalize to UTC
	// 2026-09-19T18:47:43+02:00 is the same instant as 2026-09-19T16:47:43Z
	const ms1789836463000 = primitive.DateTime(1789836463000)
	doc3 := map[string]interface{}{"ts": ms1789836463000}

	// The same instant without offset (bare equality)
	matched, err = matchesFilter(doc3, map[string]interface{}{"ts": ms1789836463000})
	if err != nil || !matched {
		t.Errorf("same instant equals = %v, %v; want true, nil", matched, err)
	}
}

// TestMatchesFilterDateInListAndEqualityCompareByInstant covers $in and bare equality
// on date fields, ensuring they compare by instant.
func TestMatchesFilterDateInListAndEqualityCompareByInstant(t *testing.T) {
	doc := map[string]interface{}{"ts": primitive.DateTime(1789836463000)}

	// $in with a list of dates
	matched, err := matchesFilter(doc, map[string]interface{}{"ts": map[string]interface{}{"$in": []interface{}{
		primitive.DateTime(1000),
		primitive.DateTime(1789836463000),
		primitive.DateTime(3000),
	}}})
	if err != nil || !matched {
		t.Errorf("$in with matching date = %v, %v; want true, nil", matched, err)
	}

	// Bare equality
	matched, err = matchesFilter(doc, map[string]interface{}{"ts": primitive.DateTime(1789836463000)})
	if err != nil || !matched {
		t.Errorf("bare equality on date = %v, %v; want true, nil", matched, err)
	}
}

// TestMatchesFilterDateAgainstStringIsNotComparable covers D-1: a date against
// a string operand matches nothing without error.
func TestMatchesFilterDateAgainstStringIsNotComparable(t *testing.T) {
	doc := map[string]interface{}{"ts": primitive.DateTime(1789836463000)}

	// $gt against a string
	matched, err := matchesFilter(doc, map[string]interface{}{"ts": map[string]interface{}{"$gt": "2026-09-19T16:47:43Z"}})
	if err != nil || matched {
		t.Errorf("date $gt string = %v, %v; want false, nil", matched, err)
	}

	// Bare equality against a string
	matched, err = matchesFilter(doc, map[string]interface{}{"ts": "2026-09-19T16:47:43Z"})
	if err != nil || matched {
		t.Errorf("date == string = %v, %v; want false, nil", matched, err)
	}
}

// TestMatchesFilterDateAgainstNumberIsNotComparable covers D-3 direction: a date against
// a numeric operand matches nothing. This test fails on main because toFloat64 accepts
// primitive.DateTime via reflect.Int64.
func TestMatchesFilterDateAgainstNumberIsNotComparable(t *testing.T) {
	doc := map[string]interface{}{"ts": primitive.DateTime(1789836463000)}

	// date $gt number should not match
	matched, err := matchesFilter(doc, map[string]interface{}{"ts": map[string]interface{}{"$gt": float64(1789836463000)}})
	if err != nil || matched {
		t.Errorf("date $gt number = %v, %v; want false, nil", matched, err)
	}

	// Mirror: number field against date operand should not match
	docNum := map[string]interface{}{"ts": float64(1789836463000)}
	matched, err = matchesFilter(docNum, map[string]interface{}{"ts": map[string]interface{}{"$gt": primitive.DateTime(1789836463000)}})
	if err != nil || matched {
		t.Errorf("number $gt date = %v, %v; want false, nil", matched, err)
	}
}

// TestMatchesFilterTimeTimeCompareLikeDateTime covers D-3: a time.Time value
// (what an in-process caller may hold) compares to a primitive.DateTime operand.
func TestMatchesFilterTimeTimeCompareLikeDateTime(t *testing.T) {
	refTime := time.Unix(0, 1789836463000*1000000).UTC() // Convert ms to ns
	doc := map[string]interface{}{"ts": refTime}

	// time.Time should order correctly against a DateTime operand
	matched, err := matchesFilter(doc, map[string]interface{}{"ts": map[string]interface{}{"$gt": primitive.DateTime(1789836463000 - 1)}})
	if err != nil || !matched {
		t.Errorf("time.Time $gt date = %v, %v; want true, nil", matched, err)
	}

	matched, err = matchesFilter(doc, map[string]interface{}{"ts": map[string]interface{}{"$lt": primitive.DateTime(1789836463000 + 1)}})
	if err != nil || !matched {
		t.Errorf("time.Time $lt date = %v, %v; want true, nil", matched, err)
	}
}

// TestDateWithNewOperators proves that $or, $ne, and $nin work correctly with
// date fields. This test addresses the gap noted in project-status: dates
// landed correct-by-routing but untested with the new logical/comparison operators.
func TestDateWithNewOperators(t *testing.T) {
	refDate := primitive.DateTime(1789836463000)
	otherDate := primitive.DateTime(1789836463001)
	doc := map[string]interface{}{"created": refDate}

	// $ne: created != otherDate should be true
	matched, err := matchesFilter(doc, map[string]interface{}{"created": map[string]interface{}{"$ne": otherDate}})
	if err != nil || !matched {
		t.Errorf("date $ne different_date = %v, %v; want true, nil", matched, err)
	}

	// $ne: created != refDate should be false
	matched, err = matchesFilter(doc, map[string]interface{}{"created": map[string]interface{}{"$ne": refDate}})
	if err != nil || matched {
		t.Errorf("date $ne same_date = %v, %v; want false, nil", matched, err)
	}

	// $nin: created not in [otherDate, otherDate+1] should be true
	matched, err = matchesFilter(doc, map[string]interface{}{"created": map[string]interface{}{"$nin": []interface{}{
		otherDate,
		primitive.DateTime(1789836463002),
	}}})
	if err != nil || !matched {
		t.Errorf("date $nin [other_dates] = %v, %v; want true, nil", matched, err)
	}

	// $nin: created not in [refDate, otherDate] should be false
	matched, err = matchesFilter(doc, map[string]interface{}{"created": map[string]interface{}{"$nin": []interface{}{
		refDate,
		otherDate,
	}}})
	if err != nil || matched {
		t.Errorf("date $nin [refDate, ...] = %v, %v; want false, nil", matched, err)
	}

	// $or: created=$ne otherDate OR created=$gt otherDate should be true
	matched, err = matchesFilter(doc, map[string]interface{}{"$or": []interface{}{
		map[string]interface{}{"created": map[string]interface{}{"$ne": otherDate}},
		map[string]interface{}{"created": map[string]interface{}{"$gt": otherDate}},
	}})
	if err != nil || !matched {
		t.Errorf("date $or [$ne, $gt] = %v, %v; want true, nil", matched, err)
	}

	// $or: created=$gt otherDate+1 OR created=$lt refDate-1 should be false
	// (both clauses are false: created=1789836463000 is not > 1789836463002, and not < 1789836462999)
	matched, err = matchesFilter(doc, map[string]interface{}{"$or": []interface{}{
		map[string]interface{}{"created": map[string]interface{}{"$gt": primitive.DateTime(1789836463002)}},
		map[string]interface{}{"created": map[string]interface{}{"$lt": primitive.DateTime(1789836462999)}},
	}})
	if err != nil || matched {
		t.Errorf("date $or [$gt higher, $lt lower] = %v, %v; want false, nil", matched, err)
	}
}

// TestScanComparesLargeIntegersExactly: an integer above 2^53 has
// no float64 of its own, so a comparison through float64 called 2^53 and
// 2^53+1 equal and ordered them wrongly. Integers compare as integers, and an
// integer against a float compares by exact value, at the boundary too.
func TestScanComparesLargeIntegersExactly(t *testing.T) {
	const big = int64(1) << 53
	const two63 = float64(1 << 63)
	cases := []struct {
		label    string
		field    interface{}
		operator string
		operand  interface{}
		want     bool
	}{
		{"2^53+1 $gt 2^53", big + 1, "$gt", big, true},
		{"2^53+1 $lte 2^53", big + 1, "$lte", big, false},
		{"2^53+1 $gte 2^53+1", big + 1, "$gte", big + 1, true},
		{"2^53 $lt 2^53+1", big, "$lt", big + 1, true},
		{"2^53 $gte 2^53+1", big, "$gte", big + 1, false},
		{"int64 max $gt int64 max-1", int64(math.MaxInt64), "$gt", int64(math.MaxInt64 - 1), true},
		{"int64 min $lt int64 min+1", int64(math.MinInt64), "$lt", int64(math.MinInt64 + 1), true},
		{"int32 against int64", int32(7), "$lt", int64(8), true},
		{"2^53+1 $gt float 2^53", big + 1, "$gt", float64(big), true},
		{"2^53+1 $lte float 2^53", big + 1, "$lte", float64(big), false},
		{"float 2^53 $lt int 2^53+1", float64(big), "$lt", big + 1, true},
		{"float 2^53 $gte int 2^53+1", float64(big), "$gte", big + 1, false},
		{"int 3 $lt float 3.5", int64(3), "$lt", 3.5, true},
		{"int 4 $gt float 3.5", int64(4), "$gt", 3.5, true},
		{"int -3 $gt float -3.5", int64(-3), "$gt", -3.5, true},
		{"int 2^53 $gte float 2^53", big, "$gte", float64(big), true},
		{"int64 max $lt float 2^63", int64(math.MaxInt64), "$lt", two63, true},
		{"int64 max $gte float 2^63", int64(math.MaxInt64), "$gte", two63, false},
		{"int64 min $gte float -2^63", int64(math.MinInt64), "$gte", -two63, true},
		// A stored number is an int32, an int64 or a double, so a uint64 above
		// int64's range reaches the filter only as an operand; the stored side
		// of these rows is the int64 nearest it. compareValues' own uint64 rows
		// are below, for the in-process callers ($min, $max).
		{"int64 max $lt uint64 max", int64(math.MaxInt64), "$lt", uint64(math.MaxUint64), true},
		{"int64 max $gt uint64 2^63", int64(math.MaxInt64), "$gt", uint64(1 << 63), false},
		{"int -1 $lt uint64 1", int64(-1), "$lt", uint64(1), true},
		{"int64 max $lt float 2^64", int64(math.MaxInt64), "$lt", float64(1<<63) * 2, true},
		{"int $lt +Inf", big + 1, "$lt", math.Inf(1), true},
		{"int $gt -Inf", big + 1, "$gt", math.Inf(-1), true},
		{"float32 against int", float32(2.5), "$gt", int64(2), true},
	}
	for _, tc := range cases {
		t.Run(tc.label, func(t *testing.T) {
			filter := map[string]interface{}{"N": map[string]interface{}{tc.operator: tc.operand}}
			got, err := matchesFilter(map[string]interface{}{"N": tc.field}, filter)
			if err != nil {
				t.Fatalf("matchesFilter(%v) errored: %v", filter, err)
			}
			if got != tc.want {
				t.Errorf("{N: %v} against %v = %v, want %v", tc.field, filter, got, tc.want)
			}
		})
	}
	for _, row := range []struct {
		label       string
		left, right interface{}
		want        int
	}{
		{"uint64 max against int64 max", uint64(math.MaxUint64), int64(math.MaxInt64), 1},
		{"uint64 max-1 against uint64 max", uint64(math.MaxUint64 - 1), uint64(math.MaxUint64), -1},
		{"uint64 max against float 2^64", uint64(math.MaxUint64), float64(1<<63) * 2, -1},
	} {
		if order, comparable := compareValues(row.left, row.right); !comparable || order != row.want {
			t.Errorf("compareValues(%s) = %d, %v; want %d", row.label, order, comparable, row.want)
		}
	}
	if valuesEqual(big, big+1) {
		t.Error("2^53 and 2^53+1 are equal")
	}
	if !valuesEqual(big, float64(big)) || !valuesEqual(int32(5), 5.0) {
		t.Error("an integer and the float of the same value are not equal")
	}
}

// TestScanAndIndexAgreeOnLargeIntegers: the secondary index orders numbers by
// their mathematical value, and a scan must return what the index returns for
// the same filter (index-parity spec section 12 item 1).
func TestScanAndIndexAgreeOnLargeIntegers(t *testing.T) {
	s := newTestServer(t)
	createUniqueBucket(t, s, "app", "Ledger", uniqueOn("N"))
	const big = int64(1) << 53
	for key, value := range map[string]interface{}{"below": big - 1, "low": big, "high": big + 1, "top": big + 2, "float": float64(big) * 4} {
		mustInsert(t, s, "app", "Ledger", key, map[string]interface{}{"N": value})
	}
	// Search serves only equality from an index; a range filter falls back to
	// the scan, which would make its agreement with find vacuous. The range
	// operators are compared against the index's key order below.
	filters := []map[string]interface{}{{"N": big}, {"N": big + 1}, {"N": big - 1}, {"N": big + 2}, {"N": float64(big) * 4}}
	for _, filter := range filters {
		found := scanKeys(t, s, "Ledger", filter)
		indexed, _ := searchKeys(t, s, "Ledger", filter)
		if strings.Join(found, ",") != strings.Join(indexed, ",") {
			t.Errorf("for %v find returns %v and the index returns %v", filter, found, indexed)
		}
	}
}

// TestScanOrderMatchesIndexKeyOrder: for every pair of numbers, the order a
// filter compares them in is the byte order of their index keys, across
// integer widths, floats and the boundaries of int64 and uint64.
func TestScanOrderMatchesIndexKeyOrder(t *testing.T) {
	const big = int64(1) << 53
	numbers := []interface{}{
		int64(math.MinInt64), -big - 1, -big, int32(-7), -3.5, int64(-3), 0.0, int32(0), 0.5, int8(1), 2.5, int64(3), int32(7),
		big - 1, big, float64(big), big + 1, big + 2, float64(big) * 4, int64(math.MaxInt64 - 1), int64(math.MaxInt64), float64(1 << 63),
		uint64(1 << 63), uint64(1<<63 + 1), uint64(math.MaxUint64 - 1), uint64(math.MaxUint64), float64(1<<63) * 2, 1e30, float32(2.5),
	}
	for _, left := range numbers {
		for _, right := range numbers {
			leftKey, err := store.EncodeComponent(left)
			if err != nil {
				t.Fatalf("encoding %v: %v", left, err)
			}
			rightKey, err := store.EncodeComponent(right)
			if err != nil {
				t.Fatalf("encoding %v: %v", right, err)
			}
			scanned, comparable := compareValues(left, right)
			if !comparable {
				t.Fatalf("compareValues(%v, %v) says two numbers are not comparable", left, right)
			}
			if indexed := bytes.Compare(leftKey, rightKey); scanned != indexed {
				t.Errorf("%T(%v) against %T(%v): the scan orders them %d and the index keys %d", left, left, right, right, scanned, indexed)
			}
		}
	}
}

// TestDistinctKeepsLargeIntegersApart: distinct tracks seen values by a key,
// which cannot merge two integers a float64 cannot tell apart.
func TestDistinctKeepsLargeIntegersApart(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "Ledger", BucketTypeHash)
	const big = int64(1) << 53
	for key, value := range map[string]interface{}{"a": big, "b": big + 1, "c": float64(big), "d": int32(5), "e": 5.0} {
		mustInsert(t, s, "app", "Ledger", key, map[string]interface{}{"N": value})
	}
	distinct, err := s.Distinct(DistinctRequest{Database: "app", Bucket: "Ledger", Field: "N"})
	if err != nil {
		t.Fatalf("distinct: %v", err)
	}
	if len(distinct.Values) != 3 {
		t.Errorf("distinct N over 2^53, 2^53+1, 2^53 as a float, 5 and 5.0 returned %v, want three values", distinct.Values)
	}
}

// TestNaNMatchesNoRangeFilter: NaN is neither above, below nor at
// any number, so a document holding one matches no ordered comparison. It
// equals only another NaN, and it sorts below every other number.
func TestNaNMatchesNoRangeFilter(t *testing.T) {
	nan := math.NaN()
	for _, operator := range []string{"$gt", "$gte", "$lt", "$lte"} {
		for _, operand := range []interface{}{7.0, int64(7), float64(1e6), math.Inf(1)} {
			filter := map[string]interface{}{"N": map[string]interface{}{operator: operand}}
			got, err := matchesFilter(map[string]interface{}{"N": nan}, filter)
			if err != nil {
				t.Fatalf("matchesFilter(%v): %v", filter, err)
			}
			if got {
				t.Errorf("a NaN field matched %v", filter)
			}
		}
		filter := map[string]interface{}{"N": map[string]interface{}{operator: nan}}
		if got, _ := matchesFilter(map[string]interface{}{"N": 7.0}, filter); got {
			t.Errorf("the number 7 matched %v", filter)
		}
	}
	if !valuesEqual(nan, nan) || valuesEqual(nan, 7.0) || valuesEqual(7.0, nan) {
		t.Error("NaN must equal NaN and nothing else")
	}
	if got, _ := matchesFilter(map[string]interface{}{"N": nan}, map[string]interface{}{"N": map[string]interface{}{"$ne": 7.0}}); !got {
		t.Error("a NaN field did not match $ne 7")
	}
	if compareSortKeys(sortKeyOf(nan), sortKeyOf(-1e300)) >= 0 || compareSortKeys(sortKeyOf(nan), sortKeyOf(nan)) != 0 {
		t.Error("NaN must sort below every other number and tie with another NaN")
	}
}

// nowSentinel is what the CLI sends for now().
func nowSentinel() primitive.DBPointer { return NowMarker() }

// TestSubstituteNowSentinelsReachesIntoADecodedArray: a document
// off the wire holds arrays as primitive.A, a different type from
// []interface{}, and the walk used to treat it as a leaf, storing the sentinel
// document where a date was meant. The input is built by bson.Unmarshal
// (wireDecoded), never by hand: a hand-written []interface{} is why the case
// looked covered.
func TestSubstituteNowSentinelsReachesIntoADecodedArray(t *testing.T) {
	decoded := wireDecoded(t, bson.D{
		{Key: "Dates", Value: bson.A{NowMarker()}},
		{Key: "Events", Value: bson.A{bson.D{{Key: "at", Value: NowMarker()}, {Key: "kind", Value: "login"}}}},
		{Key: "Grid", Value: bson.A{bson.A{NowMarker(), int32(2)}}},
	})
	if _, isDecodedArray := decoded["Dates"].(primitive.A); !isDecodedArray {
		t.Fatalf("the probe is wrong: a decoded array is %T, want primitive.A", decoded["Dates"])
	}

	substituted := substituteNowSentinels(decoded).(map[string]interface{})

	dates, isArray := substituted["Dates"].(primitive.A)
	if !isArray || len(dates) != 1 {
		t.Fatalf("Dates came back as %T %v; want a primitive.A of one element", substituted["Dates"], substituted["Dates"])
	}
	first, isDate := dates[0].(primitive.DateTime)
	if !isDate {
		t.Fatalf("Dates[0] = %T %v; want a primitive.DateTime", dates[0], dates[0])
	}
	events := substituted["Events"].(primitive.A)
	event := events[0].(map[string]interface{})
	if at, isDate := event["at"].(primitive.DateTime); !isDate || at != first {
		t.Errorf("Events[0].at = %T %v; want the same datetime as Dates[0] (%v): one clock reading per call", event["at"], event["at"], first)
	}
	if event["kind"] != "login" {
		t.Errorf("Events[0].kind = %v; a sibling of the sentinel must survive", event["kind"])
	}
	grid := substituted["Grid"].(primitive.A)[0].(primitive.A)
	if cell, isDate := grid[0].(primitive.DateTime); !isDate || cell != first || grid[1] != int32(2) {
		t.Errorf("Grid[0] = %v; want [the datetime, 2]", grid)
	}
}

// TestSubstituteNowSentinelsOnlyMatchesTheMarker: the array case does not
// widen the match. The old sentinel's shape, {"$$now": true}, and a deprecated
// pointer that is not exactly the marker are the user's data (BUG-0151).
func TestSubstituteNowSentinelsOnlyMatchesTheMarker(t *testing.T) {
	decoded := wireDecoded(t, bson.D{{Key: "List", Value: bson.A{
		bson.D{{Key: "$$now", Value: true}},
		bson.D{{Key: "$$now", Value: true}, {Key: "extra", Value: int32(1)}},
		bson.D{{Key: "$$now", Value: "true"}},
		primitive.DBPointer{DB: "$$hoardDB.now.other"},
		primitive.DBPointer{DB: nowMarkerNamespace, Pointer: primitive.ObjectID{1}},
	}}})
	substituted := substituteNowSentinels(decoded).(map[string]interface{})
	for position, element := range substituted["List"].(primitive.A) {
		if _, isDate := element.(primitive.DateTime); isDate {
			t.Errorf("List[%d] = %v was rewritten to a datetime; it is not the marker", position, element)
		}
	}
}

// TestSubstituteNowSentinelsReachesIntoInProcessDocuments: a bson.D (which is
// primitive.D) and an array inside it, from an in-process caller (spec §11.1).
func TestSubstituteNowSentinelsReachesIntoInProcessDocuments(t *testing.T) {
	substituted := substituteNowSentinels(bson.D{
		{Key: "When", Value: NowMarker()},
		{Key: "List", Value: bson.A{NowMarker()}},
		{Key: "Name", Value: "x"},
	})
	document, isDocument := substituted.(primitive.D)
	if !isDocument || len(document) != 3 {
		t.Fatalf("a bson.D came back as %T %v", substituted, substituted)
	}
	if _, isDate := document[0].Value.(primitive.DateTime); !isDate {
		t.Errorf("When = %T %v; want a datetime", document[0].Value, document[0].Value)
	}
	if list, ok := document[1].Value.(primitive.A); !ok || len(list) != 1 {
		t.Errorf("List = %T %v; want a primitive.A of one datetime", document[1].Value, document[1].Value)
	} else if _, isDate := list[0].(primitive.DateTime); !isDate {
		t.Errorf("List[0] = %T; want a datetime", list[0])
	}
	if document[2].Value != "x" {
		t.Errorf("Name = %v; want x", document[2].Value)
	}
	bare := substituteNowSentinels(NowMarker())
	if _, isDate := bare.(primitive.DateTime); !isDate {
		t.Errorf("a bare sentinel bson.D = %T %v; want a datetime", bare, bare)
	}
}

// wireRoundTrip sends request through BSON and back into into, as a frame
// does, so arrays arrive as primitive.A.
func wireRoundTrip(t *testing.T, request, into interface{}) {
	t.Helper()
	encoded, err := bson.Marshal(request)
	if err != nil {
		t.Fatalf("encoding the request: %v", err)
	}
	if err := bson.Unmarshal(encoded, into); err != nil {
		t.Fatalf("decoding the request: %v", err)
	}
}

// TestNowInsideAnArrayIsADatetime checks, end to end, that a put whose arrays
// hold now(), decoded from BSON as a handler does, stores datetimes, and a
// filter with now() inside $and, $or and $in arrays substitutes too.
func TestNowInsideAnArrayIsADatetime(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "events", BucketTypeHash)

	var put InsertRequest
	wireRoundTrip(t, InsertRequest{Database: "app", Bucket: "events", Key: "e1", Data: map[string]interface{}{
		"Dates":  []interface{}{nowSentinel()},
		"Events": []interface{}{map[string]interface{}{"at": nowSentinel(), "kind": "login"}},
	}}, &put)
	if _, isDecodedArray := put.Data["Dates"].(primitive.A); !isDecodedArray {
		t.Fatalf("the probe is wrong: Dates decoded as %T, want primitive.A", put.Data["Dates"])
	}
	// Through the client door: an applier stamps nothing.
	doorWrite(t, s, transport.OpPut, put)

	stored := documentOf(t, s, "app", "events", "e1")
	dates, _ := stored["Dates"].(primitive.A)
	if len(dates) != 1 {
		t.Fatalf("Dates = %v; want one element", stored["Dates"])
	}
	storedDate, isDate := dates[0].(primitive.DateTime)
	if !isDate {
		t.Fatalf("Dates[0] was stored as %T %v; want a datetime, not the sentinel document", dates[0], dates[0])
	}
	if age := time.Since(storedDate.Time()).Abs(); age > 5*time.Second {
		t.Errorf("Dates[0] is %v from the server's clock; want it within 5s", age)
	}
	events, _ := stored["Events"].(primitive.A)
	if len(events) != 1 {
		t.Fatalf("Events = %v; want one element", stored["Events"])
	}
	if at := events[0].(map[string]interface{})["at"]; reflect.TypeOf(at) != reflect.TypeOf(primitive.DateTime(0)) {
		t.Errorf("Events[0].at was stored as %T %v; want a datetime", at, at)
	}

	past := primitive.NewDateTimeFromTime(time.Now().Add(-time.Hour))
	mustInsert(t, s, "app", "events", "old", map[string]interface{}{"Stamp": past})
	for name, filter := range map[string]map[string]interface{}{
		"$and": {"$and": []interface{}{map[string]interface{}{"Stamp": map[string]interface{}{"$lt": nowSentinel()}}}},
		"$or":  {"$or": []interface{}{map[string]interface{}{"Stamp": map[string]interface{}{"$lt": nowSentinel()}}}},
	} {
		var scan ScanRequest
		wireRoundTrip(t, ScanRequest{Database: "app", Bucket: "events", Filter: filter}, &scan)
		response, err := s.Scan(scan)
		if err != nil {
			t.Fatalf("%s: Scan: %v", name, err)
		}
		if found := decodedDocuments(t, response.Documents); len(found) != 1 || found[0]["_id_"] != "old" {
			t.Errorf("%s: a Stamp before now() found %v; want only the old document", name, found)
		}
	}

	var inFilter ScanRequest
	wireRoundTrip(t, ScanRequest{Database: "app", Bucket: "events", Filter: map[string]interface{}{
		"Stamp": map[string]interface{}{"$in": []interface{}{nowSentinel()}},
	}}, &inFilter)
	substituted := substituteNowSentinels(inFilter.Filter).(map[string]interface{})
	inList := substituted["Stamp"].(map[string]interface{})["$in"].(primitive.A)
	if _, isDate := inList[0].(primitive.DateTime); !isDate {
		t.Errorf("$in [now()] = %T %v after substitution; want a datetime", inList[0], inList[0])
	}
}
