package server

import (
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"reflect"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"

	"github.com/hoarddb/hoarddb-server/transport"
)

// The update half of spec §4.9 (Part 10): every operator takes a path, on an
// undeclared bucket. Each test stores one document under key "a" in
// app/people and updates it.

// pathUpdateServer stores document under key "a" in app/people.
func pathUpdateServer(t *testing.T, document map[string]interface{}) *Server {
	t.Helper()
	s := newTestServer(t)
	createTestBucket(t, s, "app", "people", BucketTypeHash)
	mustInsert(t, s, "app", "people", "a", document)
	return s
}

// updateA applies req to document "a" of app/people.
func updateA(s *Server, req UpdateRequest) error {
	req.Database, req.Bucket, req.Key = "app", "people", "a"
	_, err := s.Update(req)
	return err
}

// storedA is document "a" of app/people as stored, without its key.
func storedA(t *testing.T, s *Server) map[string]interface{} {
	t.Helper()
	document := documentOf(t, s, "app", "people", "a")
	delete(document, "_id_")
	return document
}

// requireStored fails unless document "a" decodes deep-equal to want.
func requireStored(t *testing.T, s *Server, what string, want map[string]interface{}) {
	t.Helper()
	if got := storedA(t, s); !reflect.DeepEqual(got, want) {
		t.Errorf("%s: stored %v, want %v", what, got, want)
	}
}

// requirePathRefusal fails unless err is a path refusal containing every part.
func requirePathRefusal(t *testing.T, what string, err error, parts ...string) {
	t.Helper()
	if err == nil {
		t.Errorf("%s was applied; want a refusal", what)
		return
	}
	if !errors.Is(err, ErrPathRefused) || codeFor(err) != transport.ErrInvalidArgument {
		t.Errorf("%s was refused with %v (code %s); want a path refusal answered INVALID_ARGUMENT", what, err, codeFor(err))
	}
	for _, part := range parts {
		if !strings.Contains(err.Error(), part) {
			t.Errorf("%s was refused with %q; want it to say %q", what, err, part)
		}
	}
}

func TestSetOfANestedMemberChangesItAndAddsNoSiblingField(t *testing.T) {
	s := pathUpdateServer(t, map[string]interface{}{"Address": map[string]interface{}{"City": "Tirana", "Zip": "1001"}})
	if err := updateA(s, UpdateRequest{Set: map[string]interface{}{"Address.City": "Paris"}}); err != nil {
		t.Fatalf("$set Address.City: %v", err)
	}
	requireStored(t, s, "$set Address.City", map[string]interface{}{"Address": map[string]interface{}{"City": "Paris", "Zip": "1001"}})
}

func TestIncAndUnsetReachNestedMembers(t *testing.T) {
	s := pathUpdateServer(t, map[string]interface{}{"Address": map[string]interface{}{"Visits": int32(1), "Zip": "1001"}})
	if err := updateA(s, UpdateRequest{Inc: map[string]int64{"Address.Visits": 2}}); err != nil {
		t.Fatalf("$inc Address.Visits: %v", err)
	}
	if err := updateA(s, UpdateRequest{Unset: []string{"Address.Zip"}}); err != nil {
		t.Fatalf("$unset Address.Zip: %v", err)
	}
	requireStored(t, s, "$inc then $unset", map[string]interface{}{"Address": map[string]interface{}{"Visits": int64(3)}})
}

func TestEveryOperatorTakesAPath(t *testing.T) {
	s := pathUpdateServer(t, map[string]interface{}{"Profile": map[string]interface{}{
		"Tags": primitive.A{"x"}, "Badges": primitive.A{"gold"}, "Score": int32(3), "Low": int32(5), "High": int32(5), "Zip": "1001",
	}})
	if err := updateA(s, UpdateRequest{
		Push:     map[string]interface{}{"Profile.Tags": "y"},
		AddToSet: map[string]interface{}{"Profile.Badges": "gold"},
		Mul:      map[string]float64{"Profile.Score": 2},
		Min:      map[string]interface{}{"Profile.Low": int32(1)},
		Max:      map[string]interface{}{"Profile.High": int32(9)},
		Rename:   map[string]string{"Profile.Zip": "Profile.PostCode"},
	}); err != nil {
		t.Fatalf("every operator on a path: %v", err)
	}
	requireStored(t, s, "every operator on a path", map[string]interface{}{"Profile": map[string]interface{}{
		"Tags": primitive.A{"x", "y"}, "Badges": primitive.A{"gold"}, "Score": int64(6), "Low": int32(1), "High": int32(9), "PostCode": "1001",
	}})
}

func TestMissingIntermediateObjectsAreCreated(t *testing.T) {
	for _, row := range []struct {
		operator string
		req      UpdateRequest
		want     map[string]interface{}
	}{
		{"$set", UpdateRequest{Set: map[string]interface{}{"New.Inner.X": "v"}},
			map[string]interface{}{"New": map[string]interface{}{"Inner": map[string]interface{}{"X": "v"}}}},
		{"$inc", UpdateRequest{Inc: map[string]int64{"New.X": 2}}, map[string]interface{}{"New": map[string]interface{}{"X": int64(2)}}},
		{"$min", UpdateRequest{Min: map[string]interface{}{"New.X": int32(4)}}, map[string]interface{}{"New": map[string]interface{}{"X": int32(4)}}},
		{"$max", UpdateRequest{Max: map[string]interface{}{"New.X": int32(4)}}, map[string]interface{}{"New": map[string]interface{}{"X": int32(4)}}},
		{"$push", UpdateRequest{Push: map[string]interface{}{"New.X": "e"}}, map[string]interface{}{"New": map[string]interface{}{"X": primitive.A{"e"}}}},
		{"$addToSet", UpdateRequest{AddToSet: map[string]interface{}{"New.X": "e"}}, map[string]interface{}{"New": map[string]interface{}{"X": primitive.A{"e"}}}},
		{"$rename target", UpdateRequest{Rename: map[string]string{"Old": "Contact.PostCode"}},
			map[string]interface{}{"Contact": map[string]interface{}{"PostCode": "1001"}}},
		// $mul and $unset do nothing at a missing field, and create nothing.
		{"$mul", UpdateRequest{Mul: map[string]float64{"New.X": 2}}, map[string]interface{}{"Old": "1001"}},
		{"$unset", UpdateRequest{Unset: []string{"New.X"}}, map[string]interface{}{"Old": "1001"}},
	} {
		s := pathUpdateServer(t, map[string]interface{}{"Old": "1001"})
		if err := updateA(s, row.req); err != nil {
			t.Errorf("%s on a missing intermediate: %v", row.operator, err)
			continue
		}
		want := row.want
		if row.operator != "$rename target" && row.operator != "$mul" && row.operator != "$unset" {
			want = maps.Clone(row.want)
			want["Old"] = "1001"
		}
		requireStored(t, s, row.operator+" on a missing intermediate", want)
	}
}

func TestMissingArrayIsNotCreatedByAPosition(t *testing.T) {
	s := pathUpdateServer(t, map[string]interface{}{"Name": "Ana"})
	err := updateA(s, UpdateRequest{Set: map[string]interface{}{"Tags[0].Name": "x"}})
	requirePathRefusal(t, "$set Tags[0].Name on a document without Tags", err, "Tags does not exist, and a missing array is not created by a position")
	requireStored(t, s, "after the refusal", map[string]interface{}{"Name": "Ana"})
}

func TestSetThroughANonObjectIsRefused(t *testing.T) {
	stored := map[string]interface{}{
		"Text": "a string", "Count": int32(7), "Gone": nil, "Lines": primitive.A{map[string]interface{}{"Sku": "A"}},
	}
	for _, row := range []struct {
		path, says string
	}{
		{"Text.City", "Text is a string, not a document, so it has no member City"},
		{"Count.City", "Count is a 32-bit integer, not a document, so it has no member City"},
		{"Gone.City", "Gone is null, not a document, so it has no member City"},
		{"Lines.Sku", "Lines is an array, not a document, so it has no member Sku"},
		{"Text[0]", "Text is a string, not an array, so it has no element 0"},
	} {
		s := pathUpdateServer(t, maps.Clone(stored))
		err := updateA(s, UpdateRequest{Set: map[string]interface{}{row.path: "v"}})
		requirePathRefusal(t, "$set "+row.path, err, row.says, "Nothing was changed.")
		requireStored(t, s, "after refusing $set "+row.path, stored)
	}
}

func TestPositionPastTheEndIsRefused(t *testing.T) {
	stored := map[string]interface{}{"Lines": primitive.A{map[string]interface{}{"Qty": int32(1)}, map[string]interface{}{"Qty": int32(2)}}}
	s := pathUpdateServer(t, maps.Clone(stored))
	for _, req := range []UpdateRequest{
		{Set: map[string]interface{}{"Lines[2].Qty": int32(5)}},
		{Inc: map[string]int64{"Lines[2].Qty": 1}},
		{Set: map[string]interface{}{"Lines[7]": "x"}},
	} {
		err := updateA(s, req)
		requirePathRefusal(t, fmt.Sprintf("%+v", req), err, "Lines has 2 elements (positions 0 to 1), and a position past the end is not added")
	}
	requireStored(t, s, "after refusing positions past the end", stored)
}

func TestPositionInsideTheArrayIsSet(t *testing.T) {
	s := pathUpdateServer(t, map[string]interface{}{"Lines": primitive.A{
		map[string]interface{}{"Qty": int32(1)}, map[string]interface{}{"Qty": int32(2)},
	}, "Scores": primitive.A{primitive.A{int32(1), int32(2)}}})
	if err := updateA(s, UpdateRequest{
		Set: map[string]interface{}{"Lines[1].Qty": int32(9)},
		Inc: map[string]int64{"Lines[0].Qty": 4},
		Max: map[string]interface{}{"Scores[0][1]": int32(8)},
	}); err != nil {
		t.Fatalf("positions inside the arrays: %v", err)
	}
	requireStored(t, s, "positions inside the arrays", map[string]interface{}{"Lines": primitive.A{
		map[string]interface{}{"Qty": int64(5)}, map[string]interface{}{"Qty": int32(9)},
	}, "Scores": primitive.A{primitive.A{int32(1), int32(8)}}})
}

func TestUnsetOfAPositionIsRefused(t *testing.T) {
	stored := map[string]interface{}{"Lines": primitive.A{"a", "b"}}
	s := pathUpdateServer(t, maps.Clone(stored))
	err := updateA(s, UpdateRequest{Unset: []string{"Lines[0]"}})
	requirePathRefusal(t, "$unset Lines[0]", err, "removing an array element would move every element after it")
	requireStored(t, s, "after refusing $unset Lines[0]", stored)
}

func TestUnsetOfAMemberOfAnElementWorks(t *testing.T) {
	s := pathUpdateServer(t, map[string]interface{}{"Lines": primitive.A{map[string]interface{}{"Sku": "A", "Qty": int32(1)}}})
	if err := updateA(s, UpdateRequest{Unset: []string{"Lines[0].Qty", "Lines[4].Qty", "Missing.X"}}); err != nil {
		t.Fatalf("$unset Lines[0].Qty: %v", err)
	}
	requireStored(t, s, "$unset Lines[0].Qty", map[string]interface{}{"Lines": primitive.A{map[string]interface{}{"Sku": "A"}}})
}

func TestUnsetLeavesAnEmptyParent(t *testing.T) {
	s := pathUpdateServer(t, map[string]interface{}{"Address": map[string]interface{}{"Zip": "1001"}})
	if err := updateA(s, UpdateRequest{Unset: []string{"Address.Zip"}}); err != nil {
		t.Fatalf("$unset Address.Zip: %v", err)
	}
	requireStored(t, s, "$unset of the last member", map[string]interface{}{"Address": map[string]interface{}{}})
}

func TestRenameAcrossPaths(t *testing.T) {
	s := pathUpdateServer(t, map[string]interface{}{"Address": map[string]interface{}{"Zip": "1001"}, "Taken": "t", "Other": "o"})
	if err := updateA(s, UpdateRequest{Rename: map[string]string{"Address.Zip": "Contact.PostCode"}}); err != nil {
		t.Fatalf("$rename Address.Zip to Contact.PostCode: %v", err)
	}
	requireStored(t, s, "$rename across paths", map[string]interface{}{
		"Address": map[string]interface{}{}, "Contact": map[string]interface{}{"PostCode": "1001"}, "Taken": "t", "Other": "o",
	})
	// A missing source does nothing; an existing target does nothing.
	if err := updateA(s, UpdateRequest{Rename: map[string]string{"Nothing.Here": "Somewhere.Else", "Other": "Taken"}}); err != nil {
		t.Fatalf("$rename of a missing source and onto an existing target: %v", err)
	}
	requireStored(t, s, "no-op renames", map[string]interface{}{
		"Address": map[string]interface{}{}, "Contact": map[string]interface{}{"PostCode": "1001"}, "Taken": "t", "Other": "o",
	})
}

func TestRenameWithAPositionIsRefused(t *testing.T) {
	stored := map[string]interface{}{"Lines": primitive.A{map[string]interface{}{"Sku": "A"}}}
	s := pathUpdateServer(t, maps.Clone(stored))
	requirePathRefusal(t, "$rename Lines[0].Sku", updateA(s, UpdateRequest{Rename: map[string]string{"Lines[0].Sku": "Sku"}}),
		"$rename moves a named member, and an array position is not a name")
	requirePathRefusal(t, "$rename into Lines[0]", updateA(s, UpdateRequest{Rename: map[string]string{"Name": "Lines[0]"}}),
		"$rename moves a named member, and an array position is not a name")
	requireStored(t, s, "after refusing renames with a position", stored)
}

func TestRenameIntoItselfIsRefused(t *testing.T) {
	stored := map[string]interface{}{"Address": map[string]interface{}{"Zip": "1001"}}
	s := pathUpdateServer(t, maps.Clone(stored))
	requirePathRefusal(t, "$rename Address to Address.Old", updateA(s, UpdateRequest{Rename: map[string]string{"Address": "Address.Old"}}),
		`$rename of "Address" to "Address.Old" moves a field inside itself`)
	requirePathRefusal(t, "$rename Address to Address", updateA(s, UpdateRequest{Rename: map[string]string{"Address": "Address"}}),
		`$rename of "Address" to "Address" moves a field onto itself`)
	requireStored(t, s, "after refusing renames into themselves", stored)
}

// overlapIterations is T-10's loop count: an operator's keys are a Go map, so
// a check that depended on their order would pass some runs and fail others.
const overlapIterations = 200

func TestOverlappingPathsAreRefused(t *testing.T) {
	stored := map[string]interface{}{"Address": map[string]interface{}{"City": "Tirana"}}
	s := pathUpdateServer(t, maps.Clone(stored))
	for _, row := range []struct {
		name string
		req  UpdateRequest
	}{
		{"a strict prefix within $set", UpdateRequest{Set: map[string]interface{}{"Address": map[string]interface{}{"City": "Rome"}, "Address.City": "Paris"}}},
		{"a strict prefix within $unset", UpdateRequest{Unset: []string{"Address.City", "Address"}}},
		{"a strict prefix across operators", UpdateRequest{Set: map[string]interface{}{"Address.City": "Paris"}, Unset: []string{"Address"}}},
		{"a strict prefix across $inc and $set", UpdateRequest{Set: map[string]interface{}{"Address": map[string]interface{}{}}, Inc: map[string]int64{"Address.Visits": 1}}},
		{"a position under a whole array", UpdateRequest{Set: map[string]interface{}{"Lines": primitive.A{}}, Max: map[string]interface{}{"Lines[0]": int32(1)}}},
	} {
		for iteration := 1; iteration <= overlapIterations; iteration++ {
			err := updateA(s, row.req)
			if err == nil || !errors.Is(err, ErrPathRefused) || !strings.Contains(err.Error(), "one is inside the other") {
				t.Fatalf("%s: iteration %d answered %v; want E-16 every time", row.name, iteration, err)
			}
		}
	}
	requireStored(t, s, "after every overlap refusal", stored)
}

// renameOverlapShapes are the three overlapping shapes: a target that is another
// pair's source, a shared target, and a pair inside itself.
var renameOverlapShapes = []struct {
	name   string
	stored map[string]interface{}
	rename map[string]string
	says   string
}{
	{"chain {a:b, b:c}", map[string]interface{}{"a": int32(1)}, map[string]string{"a": "b", "b": "c"},
		`$rename of "a" to "b" and of "b" to "c" both touch "b"`},
	{"swap {a:b, b:a}", map[string]interface{}{"a": int32(1)}, map[string]string{"a": "b", "b": "a"},
		`$rename of "a" to "b" and of "b" to "a" both touch "a"`},
	{"shared target {a:t, x:t}", map[string]interface{}{"a": int32(1), "x": int32(2)}, map[string]string{"a": "t", "x": "t"},
		`$rename of "a" to "t" and of "x" to "t" both touch "t"`},
	{"inside itself {a:a.b}", map[string]interface{}{"a": int32(1)}, map[string]string{"a": "a.b"},
		`$rename of "a" to "a.b" moves a field inside itself`},
	{"a prefix across pairs {a:x, b:a.c}", map[string]interface{}{"a": int32(1), "b": int32(2)}, map[string]string{"a": "x", "b": "a.c"},
		`$rename of "a" to "x" and of "b" to "a.c" both touch "a"`},
}

func TestRenameOverlapShapesAreRefused(t *testing.T) {
	for _, shape := range renameOverlapShapes {
		for iteration := 1; iteration <= overlapIterations; iteration++ {
			doc := maps.Clone(shape.stored)
			err := applyUpdateOperators(doc, "k", UpdateRequest{Database: "app", Bucket: "people", Rename: shape.rename})
			if err == nil || !errors.Is(err, ErrPathRefused) || !strings.Contains(err.Error(), shape.says) {
				t.Fatalf("%s: iteration %d answered %v with %v; want E-16 saying %q every time", shape.name, iteration, err, doc, shape.says)
			}
			if !reflect.DeepEqual(doc, shape.stored) {
				t.Fatalf("%s: iteration %d changed the document to %v before refusing", shape.name, iteration, doc)
			}
		}
	}
}

func TestEqualKeysAcrossOperatorsKeepTheirFixedOrder(t *testing.T) {
	for iteration := 1; iteration <= overlapIterations; iteration++ {
		doc := map[string]interface{}{"a": int32(0), "n": "x"}
		err := applyUpdateOperators(doc, "k", UpdateRequest{
			Set: map[string]interface{}{"a": int64(1), "p.q": int64(1)}, Inc: map[string]int64{"a": 1, "p.q": 2},
			Rename: map[string]string{"n": "m"}, Unset: []string{"n"},
		})
		want := map[string]interface{}{"a": int64(2), "p": map[string]interface{}{"q": int64(3)}}
		if err != nil || !reflect.DeepEqual(doc, want) {
			t.Fatalf("iteration %d: {$set: {a: 1, p.q: 1}, $inc: {a: 1, p.q: 2}, $unset: [n], $rename: {n: m}} = %v, %v; want %v ($set, $unset, $inc ... $rename, in that order)",
				iteration, doc, err, want)
		}
	}
	// P-2's tree: $set then $rename on one key.
	doc := map[string]interface{}{"a": int32(1)}
	if err := applyUpdateOperators(doc, "k", UpdateRequest{Set: map[string]interface{}{"a": int32(5)}, Rename: map[string]string{"a": "b"}}); err != nil ||
		!reflect.DeepEqual(doc, map[string]interface{}{"b": int32(5)}) {
		t.Errorf("$set a then $rename a to b = %v, %v; want {b: 5}", doc, err)
	}
}

func TestUnsetDuplicatesAreANoOp(t *testing.T) {
	s := pathUpdateServer(t, map[string]interface{}{"a": int32(1), "b": int32(2), "c": map[string]interface{}{"d": int32(3)}})
	if err := updateA(s, UpdateRequest{Unset: []string{"a", "a", "c.d", "c.d"}}); err != nil {
		t.Fatalf("$unset with duplicates: %v", err)
	}
	requireStored(t, s, "$unset with duplicates", map[string]interface{}{"b": int32(2), "c": map[string]interface{}{}})
}

func TestMultiUpdateWithAPathChangesEveryMatch(t *testing.T) {
	s := newTestServer(t)
	seedPathDocuments(t, s, "people")
	resp, err := s.Update(UpdateRequest{Database: "app", Bucket: "people", Multi: true,
		Filter: map[string]interface{}{"Address.City": map[string]interface{}{"$exists": true}},
		Set:    map[string]interface{}{"Address.Country": "AL", "Seen.Count": int32(1)}})
	if err != nil || !resp.Success {
		t.Fatalf("multi-update on a path: %+v, %v", resp, err)
	}
	if got := pathFindKeys(t, s, "people", map[string]interface{}{"Address.Country": "AL", "Seen.Count": int32(1)}); !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Errorf("after the multi-update, Address.Country is AL on %v, want [a b]", got)
	}
	// One refused document refuses the whole update: d's Address is a string.
	_, err = s.Update(UpdateRequest{Database: "app", Bucket: "people", Multi: true,
		Filter: map[string]interface{}{"Status": "open"}, Set: map[string]interface{}{"Address.Region": "North"}})
	requirePathRefusal(t, "a multi-update reaching a string Address", err, "Address is a string, not a document")
	if got := pathFindKeys(t, s, "people", map[string]interface{}{"Address.Region": map[string]interface{}{"$exists": true}}); len(got) != 0 {
		t.Errorf("a refused multi-update changed %v", got)
	}
}

// TestNestedRefusalTextsAreExact pins E-14, E-15 and E-16 against spec §6.2.
func TestNestedRefusalTextsAreExact(t *testing.T) {
	stored := map[string]interface{}{
		"Address": "a string", "Lines": primitive.A{int32(0), int32(1), int32(2), int32(3), int32(4)}, "Text": "t",
	}
	for _, row := range []struct {
		req  UpdateRequest
		want string
	}{
		{UpdateRequest{Set: map[string]interface{}{"Address.City": "x"}},
			`$set of "Address.City" in bucket "people" (database "app") was refused: Address is a string, not a document, so it has no member City. Nothing was changed.` + "\n" +
				`To replace it, set the whole value, for example: db.people.update({_id_: "a"}, {$set: {Address: {City: …}}})`},
		{UpdateRequest{Inc: map[string]int64{"Text[3]": 1}},
			`$inc of "Text[3]" in bucket "people" (database "app") was refused: Text is a string, not an array, so it has no element 3. Nothing was changed.` + "\n" +
				`To replace it, set the whole value, for example: db.people.update({_id_: "a"}, {$set: {Text: [ … ]}})`},
		{UpdateRequest{Set: map[string]interface{}{"Lines..Qty": "x"}},
			`$set of "Lines..Qty" in bucket "people" (database "app") was refused: "Lines..Qty" is not a path: it has an empty name (a path is names joined by ".", with positions in brackets). Nothing was changed.` + "\n" +
				`Write a path as names joined by ".", with 0-based positions in brackets, for example Address.City or Lines[3].Qty.`},
		{UpdateRequest{Set: map[string]interface{}{"Lines[7].Qty": "x"}},
			`$set of "Lines[7].Qty" in bucket "people" (database "app") was refused: Lines has 5 elements (positions 0 to 4), and a position past the end is not added. Nothing was changed.` + "\n" +
				`To add an element, use $push, for example: db.people.update({_id_: "a"}, {$push: {Lines: …}})`},
		{UpdateRequest{Unset: []string{"Lines[3]"}},
			`$unset of "Lines[3]" in bucket "people" (database "app") was refused: removing an array element would move every element after it. Nothing was changed.` + "\n" +
				`Set the whole array without that element instead: {$set: {Lines: [ … ]}}`},
		{UpdateRequest{Rename: map[string]string{"Lines[0].Sku": "Sku"}},
			`$rename of "Lines[0].Sku" in bucket "people" (database "app") was refused: $rename moves a named member, and an array position is not a name. Nothing was changed.` + "\n" +
				`Rename members of objects only, for example {$rename: {"Address.Zip": "Address.PostCode"}}`},
		{UpdateRequest{Set: map[string]interface{}{"Tags[0].Name": "x"}},
			`$set of "Tags[0].Name" in bucket "people" (database "app") was refused: Tags does not exist, and a missing array is not created by a position. Nothing was changed.` + "\n" +
				`Create the array first: {$set: {Tags: [ … ]}}`},
		{UpdateRequest{Set: map[string]interface{}{"Address": "x", "Address.City": "y"}},
			`This update to bucket "people" (database "app") was refused: it writes both "Address" and "Address.City", and one is inside the other, so the result would depend on which is applied first. Nothing was changed.` + "\n" +
				`Write one of them: set the whole Address, or set Address.City alone.`},
		{UpdateRequest{Rename: map[string]string{"a": "b", "b": "c"}},
			`This update to bucket "people" (database "app") was refused: $rename of "a" to "b" and of "b" to "c" both touch "b", so the result would depend on which is applied first. Nothing was changed.` + "\n" +
				`Split it into separate updates, or rename to names no other pair uses.`},
		{UpdateRequest{Rename: map[string]string{"Address": "Address.Old"}},
			`This update to bucket "people" (database "app") was refused: $rename of "Address" to "Address.Old" moves a field inside itself. Nothing was changed.` + "\n" +
				`Rename it to a name outside Address, for example Address_Old.`},
	} {
		s := pathUpdateServer(t, maps.Clone(stored))
		err := updateA(s, row.req)
		if err == nil || err.Error() != row.want {
			t.Errorf("refusal of %+v =\n  %v\nwant\n  %s", row.req, err, row.want)
		}
	}
}

// constraintRefusalFrame is the part of an error frame these tests read.
type constraintRefusalFrame struct {
	Code              int    `bson:"code"`
	Message           string `bson:"message"`
	ConstraintRefusal bool   `bson:"constraint_refusal"`
	WriteOutcome      string `bson:"write_outcome"`
}

// TestPathRefusalsCountAsRejectedAtReplay: a refused path update is logged and
// shipped like any refused write (spec T-2). Its refusal depends only on the
// payload and the stored document, so a replay meets it again and counts it
// rejected_by_constraint, not skipped, which pages an operator (spec §6.1).
func TestPathRefusalsCountAsRejectedAtReplay(t *testing.T) {
	recorder := recordClusterLogs(t)
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	createTestHashBucket(t, tc)
	tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: testBucket, Key: "a",
		Data: map[string]interface{}{"Address": "a string"}})

	refused := tc.frame(0, transport.OpUpdate, UpdateRequest{Database: testDatabase, Bucket: testBucket, Key: "a",
		Set: map[string]interface{}{"Address.City": "Paris"}})
	if refused.Opcode != transport.OpError {
		t.Fatalf("$set through a string answered %s, want an error frame", refused.Opcode)
	}
	var answer constraintRefusalFrame
	if err := bson.Unmarshal(refused.Payload, &answer); err != nil {
		t.Fatalf("decoding the refusal: %v", err)
	}
	if transport.ErrorCode(answer.Code) != transport.ErrInvalidArgument || !answer.ConstraintRefusal || answer.WriteOutcome != outcomeNotApplied {
		t.Errorf("the refusal = %+v; want INVALID_ARGUMENT, constraint_refusal and write_outcome not_applied", answer)
	}

	tc.restartNode(ownerIndexOf(t, tc, testBucket))
	replayed := recorder.matching(slog.LevelInfo, "replayed write-ahead log")
	if len(replayed) == 0 {
		t.Fatal("the restarted node logged no replay line")
	}
	last := replayed[len(replayed)-1]
	if last.attrs["rejected_by_constraint"] != "1" || last.attrs["skipped"] != "0" {
		t.Fatalf("replay reported %v, want rejected_by_constraint=1 and skipped=0", last.attrs)
	}
}

// TestPathUpdatesAgreeOnEveryReplica: three copies apply one path update with
// created intermediates and hold the same decoded document, numeric types
// kept. Bytes are not compared: a stored document is the encoding of a Go map,
// whose field order differs between copies (spec §11.11, A-F14).
func TestPathUpdatesAgreeOnEveryReplica(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	createTestHashBucket(t, tc)
	tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: testBucket, Key: "a", WriteConcern: "all",
		Data: map[string]interface{}{"Lines": primitive.A{map[string]interface{}{"Qty": int32(1)}}, "Zip": "1001"}})
	tc.ok(0, transport.OpUpdate, pathUpdateWithConcern{Database: testDatabase, Bucket: testBucket, Key: "a", WriteConcern: "all",
		Set:    map[string]interface{}{"Address.Geo.Lat": 41.3, "Lines[0].Sku": "A-1"},
		Inc:    map[string]int64{"Lines[0].Qty": 2, "Stats.Visits": 1},
		Rename: map[string]string{"Zip": "Address.PostCode"}})
	want := map[string]interface{}{
		"_id_":    "a",
		"Lines":   primitive.A{map[string]interface{}{"Qty": int64(3), "Sku": "A-1"}},
		"Address": map[string]interface{}{"Geo": map[string]interface{}{"Lat": 41.3}, "PostCode": "1001"},
		"Stats":   map[string]interface{}{"Visits": int64(1)},
	}
	held := func(index int) map[string]interface{} {
		return documentOf(t, tc.nodes[index].server.core, testDatabase, testBucket, "a")
	}
	if !eventually(func() bool {
		for index := range tc.nodes {
			if !reflect.DeepEqual(held(index), want) {
				return false
			}
		}
		return true
	}) {
		for index := range tc.nodes {
			t.Errorf("node %d holds %v", index+1, held(index))
		}
		t.Fatalf("the copies differ; want every one to hold %v", want)
	}
}

// pathUpdateWithConcern is an update as a client sends it, with the write concern
// UpdateRequest does not carry.
type pathUpdateWithConcern struct {
	Database     string                 `bson:"database"`
	Bucket       string                 `bson:"bucket"`
	Key          string                 `bson:"key"`
	Set          map[string]interface{} `bson:"set,omitempty"`
	Inc          map[string]int64       `bson:"inc,omitempty"`
	Rename       map[string]string      `bson:"rename,omitempty"`
	WriteConcern string                 `bson:"writeconcern"`
}
