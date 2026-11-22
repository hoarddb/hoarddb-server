package ringclient_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/ringclient"
)

// heapUsers is a live server with a User bucket that has a heapStore Books and a fifoStore Purchases.
func heapUsers(t *testing.T) *ringclient.Bucket {
	t.Helper()
	client, ctx := liveClient(t), liveContext(t)
	if _, err := client.CreateDatabase(ctx, "app", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CreateBucket(ctx, "app", "User", []ringclient.FieldDecl{
		{Name: "Name", Type: "string"}, {Name: "Books", Type: "heapStore"}, {Name: "Purchases", Type: "fifoStore"},
	}); err != nil {
		t.Fatal(err)
	}
	users := client.Bucket("app", "User")
	if _, err := users.Insert(ctx, "ada", ringclient.Doc{"Name": "Ada"}); err != nil {
		t.Fatal(err)
	}
	return users
}

func TestPushHeapAndStoreListReturnKeysAndPriorities(t *testing.T) {
	users, ctx := heapUsers(t), liveContext(t)
	for _, entry := range []struct {
		id       string
		priority int64
	}{{"low", 1}, {"high", 9}, {"mid", 5}} {
		if _, err := users.PushHeap(ctx, "ada", "Books", entry.id, entry.priority, item(entry.id)); err != nil {
			t.Fatalf("PushHeap %s: %v", entry.id, err)
		}
	}
	page, err := users.StoreList(ctx, "ada", "Books", 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(page.Keys) != "[high mid low]" || fmt.Sprint(page.Priorities) != "[9 5 1]" || len(page.Elements) != 3 {
		t.Fatalf("StoreList = keys %v priorities %v elements %d, want [high mid low] [9 5 1] 3", page.Keys, page.Priorities, len(page.Elements))
	}
	if itemOf(t, page.Elements[0]) != "high" {
		t.Fatalf("first element %v", page.Elements[0])
	}
}

func TestPushHeapInABatch(t *testing.T) {
	client, ctx := liveClient(t), liveContext(t)
	if _, err := client.CreateDatabase(ctx, "app", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CreateBucket(ctx, "app", "User", []ringclient.FieldDecl{{Name: "Books", Type: "heapStore"}}); err != nil {
		t.Fatal(err)
	}
	users := client.Bucket("app", "User")
	if _, err := users.Insert(ctx, "ada", ringclient.Doc{}); err != nil {
		t.Fatal(err)
	}
	batch := client.NewBatch()
	queued := users.In(batch)
	for _, entry := range []struct {
		id       string
		priority int64
	}{{"a", 2}, {"b", 7}} {
		if _, err := queued.PushHeap(ctx, "ada", "Books", entry.id, entry.priority, item(entry.id)); err != nil {
			t.Fatalf("queueing %s: %v", entry.id, err)
		}
	}
	if batch.Len() != 2 {
		t.Fatalf("queued %d operations, want 2", batch.Len())
	}
	result, err := batch.Send(ctx)
	if err != nil || result.Outcome != "ok" || len(result.Answers) != 2 {
		t.Fatalf("Send = %+v, %v", result, err)
	}
	page, err := users.StoreList(ctx, "ada", "Books", 0, nil)
	if err != nil || fmt.Sprint(page.Keys) != "[b a]" || fmt.Sprint(page.Priorities) != "[7 2]" {
		t.Fatalf("after the batch: %+v, %v", page, err)
	}
}

// TestPushHeapOnAFifoStoreIsRefusedByTheClientNamingTheFieldType: [the declared type is cached, so a heap push
// to a fifoStore (and a Push to a heapStore) is refused before any frame is sent; the server keeps its own text
// as the authority for a caller that does not go through this client].
func TestPushHeapOnAFifoStoreIsRefusedByTheClientNamingTheFieldType(t *testing.T) {
	users, ctx := heapUsers(t), liveContext(t)
	_, err := users.PushHeap(ctx, "ada", "Purchases", "e1", 3, item("x"))
	if err == nil {
		t.Fatal("a heap push to a fifoStore was accepted")
	}
	var refusal *ringclient.Error
	if errors.As(err, &refusal) {
		t.Fatalf("the refusal came from the server (%v), want the client's own before sending", err)
	}
	for _, want := range []string{"Purchases is a fifoStore, not a heapStore", "PushHeap", "Push"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("PushHeap on a fifoStore: %q lacks %q", err, want)
		}
	}
	_, err = users.Push(ctx, "ada", "Books", item("x"))
	if err == nil || !strings.Contains(err.Error(), "Books is a heapStore, not a fifoStore or lifoStore") || !strings.Contains(err.Error(), "PushHeap") {
		t.Errorf("Push on a heapStore = %v, want the client's refusal naming the type and PushHeap", err)
	}
	if count, _ := users.StoreCount(ctx, "ada", "Purchases"); count != 0 {
		t.Fatalf("the refused push left %d elements in the fifo", count)
	}
}

func TestPushHeapRefusesAnEmptyEntryIDInTheClientWritingNothing(t *testing.T) {
	users, ctx := heapUsers(t), liveContext(t)
	_, err := users.PushHeap(ctx, "ada", "Books", "", 1, item("x"))
	if err == nil || !strings.Contains(err.Error(), "ringclient: building the argument") || !strings.Contains(err.Error(), "1 to 1024") {
		t.Fatalf("an empty entry id = %v, want the client's own refusal (building the argument) naming 1 to 1024", err)
	}
	if count, _ := users.StoreCount(ctx, "ada", "Books"); count != 0 {
		t.Fatalf("the refused push left %d elements", count)
	}
}

// TestPushHeapIdentityIsPriorityAndEntryID (characterization): the same priority and entry id
// overwrite; the same entry id at another priority is a second entry; a get or take by entry id
// reaches the higher priority first.
func TestPushHeapIdentityIsPriorityAndEntryID(t *testing.T) {
	users, ctx := heapUsers(t), liveContext(t)
	push := func(priority int64, label string) {
		t.Helper()
		if _, err := users.PushHeap(ctx, "ada", "Books", "r", priority, item(label)); err != nil {
			t.Fatal(err)
		}
	}
	count := func() int64 {
		t.Helper()
		n, err := users.StoreCount(ctx, "ada", "Books")
		if err != nil {
			t.Fatal(err)
		}
		return int64(n)
	}
	push(4, "four")
	push(8, "eight")
	if got := count(); got != 2 {
		t.Fatalf("same id at two priorities: count %d, want 2", got)
	}
	push(8, "eight-again")
	if got := count(); got != 2 {
		t.Fatalf("same priority and id twice: count %d, want 2 (an overwrite)", got)
	}
	got, err := users.StoreGet(ctx, "ada", "Books", "r")
	if err != nil || itemOf(t, got) != "eight-again" {
		t.Fatalf("StoreGet by entry id = %v, %v, want the higher priority's overwritten element", got, err)
	}
}
