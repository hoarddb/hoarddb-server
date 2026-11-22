package ringclient_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/ringclient"
)

// u85Users is a live server with one User bucket and the document ada.
func u85Users(t *testing.T) *ringclient.Bucket {
	t.Helper()
	client, ctx := liveClient(t), liveContext(t)
	if _, err := client.CreateDatabase(ctx, "app", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CreateBucket(ctx, "app", "User", []ringclient.FieldDecl{
		{Name: "Name", Type: "string"}, {Name: "Purchases", Type: "fifoStore"}, {Name: "Photos", Type: "lifoStore"},
		{Name: "Friends", Type: "hashStore"}, {Name: "Recipes", Type: "btreeStore"},
	}); err != nil {
		t.Fatal(err)
	}
	users := client.Bucket("app", "User")
	if _, err := users.Insert(ctx, "ada", ringclient.Doc{"Name": "Ada"}); err != nil {
		t.Fatal(err)
	}
	return users
}

// TestLiveStoreGetReadsOneKey: db.User["ada"].Friends.get("grace") of the CLI grammar.
func TestLiveStoreGetReadsOneKey(t *testing.T) {
	users, ctx := u85Users(t), liveContext(t)
	for _, store := range []string{"Friends", "Recipes"} {
		if _, err := users.Put(ctx, "ada", store, "grace", item("pal")); err != nil {
			t.Fatal(err)
		}
		got, err := users.StoreGet(ctx, "ada", store, "grace")
		if err != nil || itemOf(t, got) != "pal" {
			t.Fatalf("StoreGet of %s = %v, %v", store, got, err)
		}
		if _, err := users.StoreGet(ctx, "ada", store, "linus"); !errors.Is(err, ringclient.ErrNotFound) {
			t.Fatalf("an absent key of %s must be ErrNotFound, got %v", store, err)
		}
		if count, _ := users.StoreCount(ctx, "ada", store); count != 1 {
			t.Fatalf("a keyed read removed from %s: count %d", store, count)
		}
	}
	_, err := users.StoreGet(ctx, "ada", "Purchases", "grace")
	if err == nil || !strings.Contains(err.Error(), "lease_get") {
		t.Fatalf("a keyed read of a queue = %v, want a refusal naming lease_get", err)
	}
	if _, err := users.StoreGet(ctx, "ada", "Friends", ""); err == nil || !strings.Contains(err.Error(), "1 to 1024") {
		t.Fatalf("an empty key = %v, want the client to refuse it naming 1 to 1024", err)
	}
}

func TestLiveStoreListAndPeekReturnKeys(t *testing.T) {
	users, ctx := u85Users(t), liveContext(t)
	for _, key := range []string{"linus", "grace", "ken"} {
		if _, err := users.Put(ctx, "ada", "Friends", key, item(key)); err != nil {
			t.Fatal(err)
		}
	}
	for _, label := range []string{"a", "b"} {
		if _, err := users.Push(ctx, "ada", "Purchases", item(label)); err != nil {
			t.Fatal(err)
		}
	}
	page, err := users.StoreList(ctx, "ada", "Friends", 2, nil)
	if err != nil || len(page.Keys) != 2 || len(page.Elements) != 2 || !page.More || len(page.Priorities) != 0 {
		t.Fatalf("list page = %+v, %v", page, err)
	}
	if fmt.Sprint(page.Keys) != "[grace ken]" || itemOf(t, page.Elements[0]) != "grace" {
		t.Fatalf("keys %v, first element %v", page.Keys, page.Elements[0])
	}
	queue, err := users.StoreList(ctx, "ada", "Purchases", 0, nil)
	if err != nil || len(queue.Elements) != 2 || len(queue.Keys) != 0 {
		t.Fatalf("a queue list = %+v, %v: a queue has no keys", queue, err)
	}
	peeked, err := users.PeekKeyed(ctx, "ada", "Purchases", 1)
	if err != nil || len(peeked.Elements) != 1 || len(peeked.Keys) != 0 || itemOf(t, peeked.Elements[0]) != "a" {
		t.Fatalf("PeekKeyed of a queue = %+v, %v", peeked, err)
	}
}

// TestLivePeekCapIsTheServersOneHundred: [GUARD the client and the server say the same limit]
func TestLivePeekCapIsTheServersOneHundred(t *testing.T) {
	users, ctx := u85Users(t), liveContext(t)
	if _, err := users.Push(ctx, "ada", "Purchases", item("a")); err != nil {
		t.Fatal(err)
	}
	if got, err := users.Peek(ctx, "ada", "Purchases", 100); err != nil || len(got) != 1 {
		t.Fatalf("Peek of 100 = %v, %v", got, err)
	}
	for _, count := range []int{0, 101, 1000, 1001} {
		_, err := users.Peek(ctx, "ada", "Purchases", count)
		if err == nil || !strings.Contains(err.Error(), "ringclient: Peek") || !strings.Contains(err.Error(), "1 to 100") || strings.Contains(err.Error(), "1 to 1000") {
			t.Errorf("Peek of %d = %v, want the client's own refusal naming 1 to 100", count, err)
		}
	}
}

func TestLivePopNTakesSeveralInOrder(t *testing.T) {
	users, ctx := u85Users(t), liveContext(t)
	for _, label := range []string{"1", "2", "3", "4", "5"} {
		if _, err := users.Push(ctx, "ada", "Purchases", item(label)); err != nil {
			t.Fatal(err)
		}
	}
	got, err := users.PopN(ctx, "ada", "Purchases", 3)
	if err != nil || len(got) != 3 || itemOf(t, got[0]) != "1" || itemOf(t, got[2]) != "3" {
		t.Fatalf("PopN 3 = %v, %v", got, err)
	}
	rest, err := users.PopN(ctx, "ada", "Purchases", 10)
	if err != nil || len(rest) != 2 {
		t.Fatalf("PopN 10 of 2 left = %v, %v", rest, err)
	}
	none, err := users.PopN(ctx, "ada", "Purchases", 3)
	if err != nil || len(none) != 0 {
		t.Fatalf("PopN of an empty store = %v, %v, want an empty list and no error", none, err)
	}
	for _, n := range []int{0, 101} {
		if _, err := users.PopN(ctx, "ada", "Purchases", n); err == nil || !strings.Contains(err.Error(), "1 to 100") {
			t.Errorf("PopN %d = %v, want a refusal naming 1 to 100", n, err)
		}
	}
}
