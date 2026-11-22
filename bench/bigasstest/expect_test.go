package main

import (
	"testing"

	"github.com/hoarddb/hoarddb-server/bench/scenarios/model"
)

func TestDatasetMatchesModelCounts(t *testing.T) {
	for _, layout := range benchLayouts {
		ds, err := newDataset(layout, 1, 2)
		if err != nil {
			t.Fatal(err)
		}
		gen, _ := model.New(layout, 1, 2)
		for name, want := range gen.Counts() {
			if got := ds.count(name); got != want {
				t.Errorf("%s %s: dataset holds %d, model counts %d", layout, name, got, want)
			}
		}
	}
}

func TestDatasetAccessors(t *testing.T) {
	ds, err := newDataset(model.Social, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if ids := ds.ids("User"); len(ids) != 40 || ids[0] != "u0" {
		t.Fatalf("users = %d first %v", len(ids), ids[:1])
	}
	followers := ds.elements(StoreRef{"User", "u0", "Followers"})
	for _, e := range followers {
		if e.Key == "" {
			t.Error("a followers element has its follower as key")
		}
	}
	if _, ok := ds.doc("Post", "p0"); !ok {
		t.Error("post p0 missing")
	}
	if ds.fingerprint() == "" || ds.fingerprint() != mustDataset(t, model.Social, 1, 1).fingerprint() {
		t.Error("fingerprint must be stable")
	}
	if ds.fingerprint() == mustDataset(t, model.Social, 2, 1).fingerprint() {
		t.Error("fingerprint must change with the seed")
	}
}

func mustDataset(t *testing.T, layout model.Layout, seed uint64, scale int) *Dataset {
	t.Helper()
	ds, err := newDataset(layout, seed, scale)
	if err != nil {
		t.Fatal(err)
	}
	return ds
}

func TestCanonIgnoresNothingAndDistinguishesTypes(t *testing.T) {
	a := canonBody([]model.Field{{Name: "n", Value: int64(1)}})
	b := canonBody([]model.Field{{Name: "n", Value: "1"}})
	if a == b {
		t.Error("int 1 and string 1 must differ")
	}
}
