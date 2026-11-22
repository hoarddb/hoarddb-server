package cli

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/ringclient"
)

// TestUpdateStatementTakesEveryOperator: [GUARD updateDocument turns each operator into the
// client's Update, keys in the order written].
func TestUpdateStatementTakesEveryOperator(t *testing.T) {
	session, fake := u52Session()
	if _, err := u52Run(session, "create database app", "use app", "create bucket Item { Qty int, Tags array, Label string, Alias string }",
		`db.Item.insert({_id_: "i1", Qty: 3, Label: "a"})`); err != nil {
		t.Fatal(err)
	}
	got, err := u52Run(session,
		`db.Item.update({_id_: "i1", $set: {Qty: 4}, $unset: {Label: ""}, $inc: {Qty2: 1}, $mul: {Qty: 2}, $min: {Qty: 1}, $max: {Qty: 9}, $push: {Tags: "x"}, $addToSet: {Tags: "y"}, $rename: {Label: "Alias"}})`)
	if err != nil {
		t.Fatal(err)
	}
	u52Want(t, got, "OK  updated id=i1")
	updates := fake.buckets["app.Item"].updates
	if len(updates) != 1 {
		t.Fatalf("the bucket was sent %d updates, want one", len(updates))
	}
	want := ringclient.Update{
		Set: bson.D{{Key: "Qty", Value: int64(4)}}, Unset: []string{"Label"}, Inc: bson.D{{Key: "Qty2", Value: int64(1)}},
		Mul: bson.D{{Key: "Qty", Value: int64(2)}}, Min: bson.D{{Key: "Qty", Value: int64(1)}}, Max: bson.D{{Key: "Qty", Value: int64(9)}},
		Push: bson.D{{Key: "Tags", Value: "x"}}, AddToSet: bson.D{{Key: "Tags", Value: "y"}}, Rename: bson.D{{Key: "Label", Value: "Alias"}},
	}
	if !reflect.DeepEqual(updates[0], want) {
		t.Fatalf("the statement became %+v, want %+v", updates[0], want)
	}
}

// TestUpdateStatementRefusals: [GUARD the statement's own checks name the fix].
func TestUpdateStatementRefusals(t *testing.T) {
	session, fake := u52Session()
	if _, err := u52Run(session, "create database app", "use app", "create bucket Item { Qty int }", `db.Item.insert({_id_: "i1", Qty: 3})`); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ statement, code, said string }{
		{`db.Item.update({_id_: "i1"})`, "PARSE_ERROR", "$set"},
		{`db.Item.update({_id_: "i1", $bump: {Qty: 1}})`, "PARSE_ERROR", "$addToSet"},
		{`db.Item.update({_id_: "i1", $set: {Qty: 1}, $set: {Qty: 2}})`, "PARSE_ERROR", "twice"},
		{`db.Item.update({_id_: "i1", $mul: {}})`, "PARSE_ERROR", "no field"},
		{`db.Item.update({_id_: "i1", $set: {_id_: "z"}})`, "PARSE_ERROR", "_id_"},
		{`db.Item.update({$set: {Qty: 1}})`, "PARSE_ERROR", "_id_"},
	} {
		_, err := u52Run(session, tc.statement)
		var failed *RingError
		if !errors.As(err, &failed) || failed.Code != tc.code || !strings.Contains(failed.Message, tc.said) {
			t.Errorf("%s: %v, want %s saying %q", tc.statement, err, tc.code, tc.said)
		}
	}
	if len(fake.buckets["app.Item"].updates) != 0 {
		t.Error("a refused statement reached the bucket")
	}
	if _, err := u52Run(session, `db.Item.update({_id_: "ghost", $inc: {Qty: 1}})`); err == nil {
		t.Error("an update of a missing document succeeded")
	}
}
