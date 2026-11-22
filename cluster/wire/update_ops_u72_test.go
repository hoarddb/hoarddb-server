package wire

import (
	"context"
	"math"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// u72Node serves a node with the bucket Item and the document i1. Qty is an int, Price a
// double, Tags an array, Label and Alias strings, Spare an int, Seen a datetime.
func u72Node(t *testing.T) *Node {
	t.Helper()
	node, _ := u30iFresh(t)
	ctx := context.Background()
	if _, err := node.CreateBucket(ctx, u30iDB, "Item",
		Field{Name: "Qty", Type: "int"}, Field{Name: "Price", Type: "double"}, Field{Name: "Tags", Type: "array"},
		Field{Name: "Label", Type: "string"}, Field{Name: "Alias", Type: "string"}, Field{Name: "Spare", Type: "int"},
		Field{Name: "Seen", Type: "datetime"}, Field{Name: "Log", Type: "fifoStore"}); err != nil {
		t.Fatal(err)
	}
	if _, err := node.Insert(ctx, u30iDB, "Item", bson.D{{Key: "_id_", Value: "i1"}, {Key: "Label", Value: "a"}}); err != nil {
		t.Fatal(err)
	}
	return node
}

func u72Update(node *Node, req UpdateRequest, options ...WriteOption) (Updated, error) {
	req.Database, req.Bucket, req.ID = u30iDB, "Item", "i1"
	return node.Update(context.Background(), req, options...)
}

func u72Item(t *testing.T, node *Node) bson.Raw {
	t.Helper()
	doc, err := node.Get(context.Background(), u30iDB, "Item", "i1")
	if err != nil {
		t.Fatal(err)
	}
	return bson.Raw(doc.Body)
}

func u72Position(node *Node) any {
	return node.pairs.Snapshot(node.slotOf(u30iDB, "Item", "i1"), 1)
}

// u72Put replaces a field with any value, or removes it when value is nil.
func u72Put(t *testing.T, node *Node, field string, value any) {
	t.Helper()
	req := UpdateRequest{Set: bson.D{{Key: field, Value: value}}}
	if value == nil {
		req = UpdateRequest{Unset: []string{field}}
	}
	if _, err := u72Update(node, req); err != nil {
		t.Fatal(err)
	}
}

// u72Same fails the test unless got is the BSON value of want, type and bytes.
func u72Same(t *testing.T, label string, got bson.RawValue, want any) {
	t.Helper()
	kind, raw, err := bson.MarshalValue(want)
	if err != nil {
		t.Fatal(err)
	}
	if got.Type != kind || string(got.Value) != string(raw) {
		t.Errorf("%s: got %v (%s), want %v (%s)", label, got, got.Type, want, kind)
	}
}

// u72Refused runs req and requires an INVALID_ARGUMENT that says every word in said and
// that nothing was changed, with the pair at the same position and still writable.
func u72Refused(t *testing.T, node *Node, label string, req UpdateRequest, said ...string) {
	t.Helper()
	before := u72Position(node)
	_, err := u72Update(node, req)
	u30iCode(t, err, cluster.CodeInvalidArgument)
	for _, word := range append(said, "Nothing was changed") {
		if !strings.Contains(err.Error(), word) {
			t.Errorf("%s: %q does not say %q", label, err, word)
		}
	}
	if after := u72Position(node); after != before {
		t.Errorf("%s moved the pair from %+v to %+v", label, before, after)
	}
	if _, err := u72Update(node, UpdateRequest{Set: bson.D{{Key: "Alias", Value: "after " + label}}}); err != nil {
		t.Fatalf("%s: the pair did not take a write after the refusal: %v", label, err)
	}
}

// TestU72Mul: [GUARD mulNumbers keeps inc's typing and refuses overflow; a missing field
// takes zero of the factor's type].
func TestU72Mul(t *testing.T) {
	node := u72Node(t)
	for _, tc := range []struct {
		name          string
		stored, by    any
		want          any
		refused, said string
	}{
		{name: "int times int", stored: int32(6), by: int32(7), want: int32(42)},
		{name: "int overflow becomes long", stored: int32(math.MaxInt32), by: int32(2), want: int64(math.MaxInt32) * 2},
		{name: "long times int is long", stored: int64(1 << 40), by: int32(2), want: int64(1 << 41)},
		{name: "int times double is double", stored: int32(3), by: 1.5, want: 4.5},
		{name: "negative", stored: int32(3), by: int32(-2), want: int32(-6)},
		{name: "missing field takes int zero", by: int32(5), want: int32(0)},
		{name: "missing field takes long zero", by: int64(5), want: int64(0)},
		{name: "missing field takes double zero", by: 2.5, want: 0.0},
		{name: "long overflow", stored: int64(math.MaxInt64), by: int32(2), refused: "mul", said: "overflow"},
		{name: "double overflow", stored: 1e308, by: 10.0, refused: "mul", said: "overflow"},
		{name: "stored string", stored: "x", by: int32(2), refused: "mul", said: "string"},
		{name: "stored null", stored: primitive.Null{}, by: int32(2), refused: "mul", said: "null"},
		{name: "factor NaN", stored: int32(1), by: math.NaN(), refused: "mul", said: "Qty"},
		{name: "factor string", stored: int32(1), by: "2", refused: "mul", said: "Qty"},
	} {
		u72Put(t, node, "Qty", tc.stored)
		req := UpdateRequest{Mul: bson.D{{Key: "Qty", Value: tc.by}}}
		if tc.refused != "" {
			u72Refused(t, node, tc.name, req, tc.refused, tc.said)
			continue
		}
		if _, err := u72Update(node, req); err != nil {
			t.Errorf("%s: %v", tc.name, err)
			continue
		}
		u72Same(t, tc.name, u72Item(t, node).Lookup("Qty"), tc.want)
	}
}

// TestU72MinMax: [GUARD compareOrder follows MongoDB's brackets and exact number order; a
// tie keeps the stored value and its type].
func TestU72MinMax(t *testing.T) {
	node := u72Node(t)
	stamp := primitive.Timestamp{T: 5, I: 1}
	for _, tc := range []struct {
		name          string
		stored, with  any
		min, max      any // the result of each operator; nil means the stored value stays
		refused, said string
	}{
		{name: "ints", stored: int32(5), with: int32(3), min: int32(3)},
		{name: "ints larger", stored: int32(5), with: int32(9), max: int32(9)},
		{name: "a tie across types keeps the stored type", stored: int32(5), with: 5.0},
		{name: "long past 2^53 is not rounded", stored: int64(1<<53 + 1), with: float64(1 << 53), min: float64(1 << 53)},
		{name: "int against double", stored: 2.5, with: int32(2), min: int32(2)},
		{name: "strings by bytes", stored: "b", with: "a", min: "a"},
		{name: "strings larger", stored: "b", with: "ab", max: nil, min: "ab"},
		{name: "dates", stored: primitive.DateTime(100), with: primitive.DateTime(50), min: primitive.DateTime(50)},
		{name: "false before true", stored: true, with: false, min: false},
		{name: "timestamps", stored: stamp, with: primitive.Timestamp{T: 5, I: 2}, max: primitive.Timestamp{T: 5, I: 2}},
		{name: "binary by length first", stored: primitive.Binary{Data: []byte{9, 9}}, with: primitive.Binary{Data: []byte{1}}, min: primitive.Binary{Data: []byte{1}}},
		{name: "a number sorts below a string", stored: "z", with: int32(7), min: int32(7)},
		{name: "a string sorts above a number", stored: int32(7), with: "z", max: "z"},
		{name: "null sorts below a number", stored: primitive.Null{}, with: int32(1), max: int32(1)},
		{name: "a bool sorts below a date", stored: primitive.DateTime(1), with: true, min: true},
		{name: "a date sorts below a timestamp", stored: stamp, with: primitive.DateTime(1 << 40), min: primitive.DateTime(1 << 40)},
		{name: "a document is refused", stored: int32(1), with: bson.D{{Key: "a", Value: int32(1)}}, refused: "document"},
		{name: "a stored array is refused", stored: bson.A{int32(1)}, with: int32(1), refused: "array"},
		{name: "NaN is refused", stored: int32(1), with: math.NaN(), refused: "finite"},
	} {
		for _, operator := range []struct {
			word string
			req  func(any) UpdateRequest
			want any
		}{
			{"min", func(v any) UpdateRequest { return UpdateRequest{Min: bson.D{{Key: "Qty", Value: v}}} }, tc.min},
			{"max", func(v any) UpdateRequest { return UpdateRequest{Max: bson.D{{Key: "Qty", Value: v}}} }, tc.max},
		} {
			label := operator.word + " " + tc.name
			u72Put(t, node, "Qty", tc.stored)
			if tc.refused != "" {
				u72Refused(t, node, label, operator.req(tc.with), operator.word, tc.refused)
				continue
			}
			if _, err := u72Update(node, operator.req(tc.with)); err != nil {
				t.Errorf("%s: %v", label, err)
				continue
			}
			want := operator.want
			if want == nil {
				want = tc.stored
			}
			u72Same(t, label, u72Item(t, node).Lookup("Qty"), want)
		}
	}
	u72Put(t, node, "Qty", nil)
	if _, err := u72Update(node, UpdateRequest{Min: bson.D{{Key: "Qty", Value: "first"}}}); err != nil {
		t.Fatal(err)
	}
	u72Same(t, "min on a missing field", u72Item(t, node).Lookup("Qty"), "first")
}

// u72Tags reads the elements of the Tags array of i1.
func u72Tags(t *testing.T, node *Node) []bson.RawValue {
	t.Helper()
	values, err := u72Item(t, node).Lookup("Tags").Array().Values()
	if err != nil {
		t.Fatal(err)
	}
	return values
}

// TestU72Push: [GUARD pushResult appends one value after the last element and creates the
// array when the field is missing].
func TestU72Push(t *testing.T) {
	node := u72Node(t)
	push := func(value any) error {
		_, err := u72Update(node, UpdateRequest{Push: bson.D{{Key: "Tags", Value: value}}})
		return err
	}
	if err := push("red"); err != nil {
		t.Fatal(err)
	}
	if err := push("red"); err != nil {
		t.Fatal(err)
	}
	if err := push(int32(7)); err != nil {
		t.Fatal(err)
	}
	if err := push(bson.D{{Key: "k", Value: "v"}}); err != nil {
		t.Fatal(err)
	}
	if err := push(bson.A{int32(1)}); err != nil {
		t.Fatal(err)
	}
	tags := u72Tags(t, node)
	if len(tags) != 5 {
		t.Fatalf("Tags holds %d elements, want 5 (push never dedupes): %v", len(tags), u72Item(t, node))
	}
	u72Same(t, "first", tags[0], "red")
	u72Same(t, "second", tags[1], "red")
	u72Same(t, "third", tags[2], int32(7))
	if tags[3].Type != bson.TypeEmbeddedDocument || tags[4].Type != bson.TypeArray {
		t.Errorf("the pushed document and array: %v, %v", tags[3], tags[4])
	}
	for _, modifier := range []string{"$each", "$position", "$slice", "$sort"} {
		u72Refused(t, node, "push with "+modifier, UpdateRequest{Push: bson.D{{Key: "Tags", Value: bson.D{{Key: modifier, Value: int32(1)}}}}}, "push", "Tags", modifier)
	}
	u72Put(t, node, "Tags", bson.A{})
	if err := push(int32(1)); err != nil || len(u72Tags(t, node)) != 1 {
		t.Fatalf("push onto an empty array: %v", err)
	}
	u72Put(t, node, "Tags", "text")
	u72Refused(t, node, "push onto a string", UpdateRequest{Push: bson.D{{Key: "Tags", Value: "x"}}}, "push", "string", "array")
	u72Put(t, node, "Tags", primitive.Null{})
	u72Refused(t, node, "push onto a null", UpdateRequest{Push: bson.D{{Key: "Tags", Value: "x"}}}, "push", "null")
	u72Put(t, node, "Tags", nil)
	if err := push("fresh"); err != nil {
		t.Fatal(err)
	}
	if tags := u72Tags(t, node); len(tags) != 1 {
		t.Fatalf("push onto a missing field gave %v", tags)
	}
}

// TestU72PushStopsBeforeAResultOneBatchCannotHold: [GUARD updateStateRefusal bounds the bytes
// an operator writes]. An array that outgrew one batch would be appended and then fail to
// apply, which wedges the pair.
func TestU72PushStopsBeforeAResultOneBatchCannotHold(t *testing.T) {
	node := u72Node(t)
	chunk := strings.Repeat("x", 1<<20)
	for pushed := 0; ; pushed++ {
		if pushed == 40 {
			t.Fatal("40 pushes of 1 MiB were all accepted, so the result is not bounded")
		}
		before := u72Position(node)
		_, err := u72Update(node, UpdateRequest{Push: bson.D{{Key: "Tags", Value: chunk}}})
		if err == nil {
			continue
		}
		u30iCode(t, err, cluster.CodeInvalidArgument)
		if !strings.Contains(err.Error(), "at most") || !strings.Contains(err.Error(), "Nothing was changed") {
			t.Fatalf("the refusal after %d pushes: %q", pushed, err)
		}
		if after := u72Position(node); after != before {
			t.Fatal("the refused push moved the pair")
		}
		break
	}
	if _, err := u72Update(node, UpdateRequest{Unset: []string{"Tags"}}); err != nil {
		t.Fatalf("the pair did not take a write after the refusal: %v", err)
	}
}

// TestU72AddToSet: [GUARD setEqual follows MongoDB: a number equals the same number of
// another type, NaN equals NaN, existing duplicates stay].
func TestU72AddToSet(t *testing.T) {
	node := u72Node(t)
	add := func(value any) error {
		_, err := u72Update(node, UpdateRequest{AddToSet: bson.D{{Key: "Tags", Value: value}}})
		return err
	}
	for _, value := range []any{"red", "red", "blue", int32(1), 1.0, int64(1), true, true, math.NaN(), math.NaN(), math.Inf(1), math.Inf(1), primitive.Null{}, primitive.Null{}} {
		if err := add(value); err != nil {
			t.Fatalf("addToSet %v: %v", value, err)
		}
	}
	tags := u72Tags(t, node)
	if len(tags) != 7 {
		t.Fatalf("Tags = %v, want red, blue, 1, true, NaN, Inf, null", tags)
	}
	u72Same(t, "red", tags[0], "red")
	u72Same(t, "one stays the int32 added first", tags[2], int32(1))
	// A stored duplicate is left alone, and a value that is there is not added again.
	u72Put(t, node, "Tags", bson.A{"a", "a"})
	if err := add("a"); err != nil || len(u72Tags(t, node)) != 2 {
		t.Fatalf("addToSet of a present value: %v, %v", err, u72Tags(t, node))
	}
	if err := add("b"); err != nil || len(u72Tags(t, node)) != 3 {
		t.Fatalf("addToSet of a new value: %v, %v", err, u72Tags(t, node))
	}
	u72Put(t, node, "Tags", "text")
	u72Refused(t, node, "addToSet onto a string", UpdateRequest{AddToSet: bson.D{{Key: "Tags", Value: "x"}}}, "addToSet", "array")
	u72Put(t, node, "Tags", nil)
	u72Refused(t, node, "addToSet of a document", UpdateRequest{AddToSet: bson.D{{Key: "Tags", Value: bson.D{{Key: "a", Value: int32(1)}}}}}, "addToSet", "document", "Tags")
	u72Refused(t, node, "addToSet of an array", UpdateRequest{AddToSet: bson.D{{Key: "Tags", Value: bson.A{int32(1)}}}}, "addToSet", "array")
	if err := add("first"); err != nil || len(u72Tags(t, node)) != 1 {
		t.Fatalf("addToSet onto a missing field: %v", err)
	}
}

// TestU72Rename: [GUARD fillUpdate moves the value and deletes the source; renameTargetRefusal
// and renameOps hold the schema]. SPEC-GAP U72-4.
func TestU72Rename(t *testing.T) {
	node := u72Node(t)
	rename := func(source, target string) UpdateRequest {
		return UpdateRequest{Rename: bson.D{{Key: source, Value: target}}}
	}
	u72Put(t, node, "Qty", int32(9))
	if _, err := u72Update(node, rename("Qty", "Spare")); err != nil {
		t.Fatal(err)
	}
	doc := u72Item(t, node)
	u72Same(t, "Spare after the rename", doc.Lookup("Spare"), int32(9))
	if _, err := doc.LookupErr("Qty"); err == nil {
		t.Errorf("the source survived the rename: %v", doc)
	}
	// A missing source changes nothing, and a target that holds a value is overwritten.
	before := u72Item(t, node)
	if _, err := u72Update(node, rename("Qty", "Spare")); err != nil {
		t.Fatal(err)
	}
	if after := u72Item(t, node); string(after) != string(before) {
		t.Errorf("a rename of a missing source changed the document: %v then %v", before, after)
	}
	u72Put(t, node, "Qty", "text")
	u72Put(t, node, "Label", "from")
	u72Put(t, node, "Alias", "old")
	if _, err := u72Update(node, rename("Label", "Alias")); err != nil {
		t.Fatal(err)
	}
	u72Same(t, "Alias overwritten", u72Item(t, node).Lookup("Alias"), "from")
	// A value of another BSON type moves too: the ring does not enforce declared types on writes.
	u72Put(t, node, "Spare", "odd")
	if _, err := u72Update(node, rename("Spare", "Qty")); err != nil {
		t.Fatal(err)
	}
	u72Same(t, "Qty", u72Item(t, node).Lookup("Qty"), "odd")

	u72Refused(t, node, "another declared type", rename("Qty", "Label"), "rename", "Qty", "Label", "same type")
	u72Refused(t, node, "a store target", rename("Qty", "Log"), "rename", "Log", "push")
	u72Refused(t, node, "an undeclared target", rename("Qty", "Quantity"), "rename", "Quantity", "alter bucket")
	u72Refused(t, node, "itself", rename("Qty", "Qty"), "rename", "Qty")
	u72Refused(t, node, "a target that is also a source", UpdateRequest{Rename: bson.D{{Key: "Qty", Value: "Spare"}, {Key: "Spare", Value: "Qty"}}}, "Spare", "two operations")
	u72Refused(t, node, "a target another operator names", UpdateRequest{Rename: bson.D{{Key: "Qty", Value: "Spare"}}, Set: bson.D{{Key: "Spare", Value: int32(1)}}}, "Spare")
	u72Refused(t, node, "a name that is not a string", UpdateRequest{Rename: bson.D{{Key: "Qty", Value: int32(3)}}}, "rename", "string")
	before2 := u72Position(node)
	_, err := u72Update(node, UpdateRequest{Rename: bson.D{{Key: "Log", Value: "Qty"}}})
	u30iCode(t, err, cluster.CodeFieldType)
	if after := u72Position(node); after != before2 {
		t.Error("a rename of a store field moved the pair")
	}
}
