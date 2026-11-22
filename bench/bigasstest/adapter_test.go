package main

import (
	"context"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/bench/scenarios/model"
)

func TestOpenEngineWithoutAdapterNamesTheFix(t *testing.T) {
	_, err := openEngine(context.Background(), Config{Engine: "nosuch"})
	if err == nil || !strings.Contains(err.Error(), `engine "nosuch" has no adapter`) {
		t.Fatalf("err = %v, want 'engine \"nosuch\" has no adapter'", err)
	}
}

func TestRegisterRefusesDuplicate(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("registering a name twice must panic at init time")
		}
	}()
	open := func(context.Context, Config) (Engine, error) { return nil, nil }
	register("dup-test", open)
	register("dup-test", open)
}

func TestStoreKindsComeFromTheModelSchema(t *testing.T) {
	cases := []struct {
		layout        model.Layout
		bucket, store string
		want          StoreKind
	}{
		{model.Social, "User", "Feed", FIFO},
		{model.Social, "User", "Notifications", LIFO},
		{model.Social, "User", "Followers", Hash},
		{model.Ledger, "Account", "Expenses", BTree},
	}
	for _, tc := range cases {
		got, err := KindOf(tc.layout, tc.bucket, tc.store)
		if err != nil || got != tc.want {
			t.Errorf("KindOf(%s,%s,%s) = %v, %v; want %v", tc.layout, tc.bucket, tc.store, got, err, tc.want)
		}
	}
	if _, err := KindOf(model.Social, "User", "Nope"); err == nil {
		t.Error("an unknown store must be an error")
	}
}
