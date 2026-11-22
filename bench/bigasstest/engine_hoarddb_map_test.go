package main

import (
	"reflect"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/bench/scenarios/model"
)

func marshalRaw(t *testing.T, doc bson.D) bson.Raw {
	t.Helper()
	raw, err := bson.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestRingFieldsKeepOrderAndWidenIntegers(t *testing.T) {
	raw := marshalRaw(t, bson.D{
		{Key: ringIDField, Value: "u1"},
		{Key: "Name", Value: "ann"},
		{Key: "Age", Value: int32(7)},
		{Key: "At", Value: int64(1 << 40)},
	})
	got, err := ringFields(raw)
	if err != nil {
		t.Fatal(err)
	}
	want := []model.Field{{Name: "Name", Value: "ann"}, {Name: "Age", Value: int64(7)}, {Name: "At", Value: int64(1 << 40)}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v want %#v", got, want)
	}
}

func TestRingBodyRoundTripsAVector(t *testing.T) {
	fields := []model.Field{{Name: "text", Value: "hello"}, {Name: "vec", Value: []byte{1, 2, 3, 4}}, {Name: "n", Value: int64(9)}}
	raw, err := bson.Marshal(ringBody(fields))
	if err != nil {
		t.Fatal(err)
	}
	got, err := ringFields(bson.Raw(raw))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, fields) {
		t.Fatalf("got %#v want %#v", got, fields)
	}
}

func TestRingFieldsRefuseUnexpectedTypes(t *testing.T) {
	raw := marshalRaw(t, bson.D{{Key: "flag", Value: true}})
	if _, err := ringFields(raw); err == nil {
		t.Fatal("a boolean member must be refused, the layouts hold none")
	}
}

func TestRingElementCarriesItsStore(t *testing.T) {
	raw := marshalRaw(t, bson.D{{Key: "user", Value: "u2"}})
	ref := StoreRef{Bucket: "User", Owner: "u1", Store: "Followers"}
	got, err := ringElement(ref, "u2", raw)
	if err != nil {
		t.Fatal(err)
	}
	if got.Bucket != "User" || got.Owner != "u1" || got.Store != "Followers" || got.Key != "u2" || len(got.Body) != 1 {
		t.Fatalf("unexpected element %#v", got)
	}
}
