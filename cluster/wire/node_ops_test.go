package wire

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

// u62Fresh is a node with bucket Box { Name, Queue fifoStore, Stack lifoStore,
// Ranked heapStore, Table hashStore } and one document b1.
func u62Fresh(t *testing.T) (*Node, string) {
	t.Helper()
	dir := t.TempDir()
	node := u30iOpen(t, dir)
	u62Schema(t, node)
	return node, dir
}

func u62Schema(t testing.TB, node *Node) {
	t.Helper()
	ctx := context.Background()
	if _, err := node.CreateDatabase(ctx, u30iDB); err != nil {
		t.Fatal(err)
	}
	if _, err := node.CreateBucket(ctx, u30iDB, "Box", Field{Name: "Name", Type: "string"},
		Field{Name: "Queue", Type: "fifoStore"}, Field{Name: "Stack", Type: "lifoStore"},
		Field{Name: "Ranked", Type: "heapStore"}, Field{Name: "Table", Type: "hashStore"}); err != nil {
		t.Fatal(err)
	}
	if _, err := node.Insert(ctx, u30iDB, "Box", bson.D{{Key: "_id_", Value: "b1"}, {Key: "Name", Value: "one"}}); err != nil {
		t.Fatal(err)
	}
}

func u62Target(field string) StoreTarget {
	return StoreTarget{Database: u30iDB, Bucket: "Box", ID: "b1", Field: field}
}

func u62Push(t *testing.T, node *Node, field string, items ...string) {
	t.Helper()
	for _, item := range items {
		if err := node.Push(context.Background(), PushRequest{StoreTarget: u62Target(field), Element: bson.D{{Key: "item", Value: item}}}); err != nil {
			t.Fatalf("Push %s to %s: %v", item, field, err)
		}
	}
}

func u62Item(t *testing.T, element []byte) string {
	t.Helper()
	return bson.Raw(element).Lookup("item").StringValue()
}

func u62Marker(t *testing.T, node *Node, field string) string {
	t.Helper()
	doc, err := node.Get(context.Background(), u30iDB, "Box", "b1")
	if err != nil {
		t.Fatal(err)
	}
	return u30iString(t, doc, field)
}

// u62SeedHeap puts elements into Ranked straight through the store, because the
// argument encoding of a heap push is not specified yet (SPEC-GAP-1).
func u62SeedHeap(t *testing.T, node *Node, priorities map[string]int64) {
	t.Helper()
	ref, err := node.bucket(context.Background(), u30iDB, "Box")
	if err != nil {
		t.Fatal(err)
	}
	fieldID, err := ref.storeField("Ranked")
	if err != nil {
		t.Fatal(err)
	}
	encID, err := store.EncodeComponent("b1")
	if err != nil {
		t.Fatal(err)
	}
	owner := storage.Key(ref.slot(encID), ref.dbID, ref.entry.Decl.BucketID, storage.NSFieldStore, encID)
	heap := store.FieldRef{Owner: owner, FieldID: fieldID, Engine: store.FieldHeap}
	// One batch per element: a put reads the count from its snapshot, so two puts in one
	// batch would count once.
	for item, priority := range priorities {
		raw, err := bson.Marshal(bson.D{{Key: "item", Value: item}})
		if err != nil {
			t.Fatal(err)
		}
		snapshot := node.kv.NewSnapshot()
		batch := node.kv.NewBatch()
		var discarded store.BucketState
		err = store.FieldHeapPush(store.FieldWrite{Snap: snapshot, State: &discarded, Batch: batch}, heap, priority, item, raw)
		if err == nil {
			err = batch.Commit(true)
		}
		batch.Discard()
		snapshot.Discard()
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestU62PopTakesTheNextElementOfEachOrderedStore(t *testing.T) {
	node, _ := u62Fresh(t)
	ctx := context.Background()
	u62Push(t, node, "Queue", "a", "b", "c")
	u62Push(t, node, "Stack", "a", "b", "c")
	u62SeedHeap(t, node, map[string]int64{"low": 1, "high": 9, "mid": 5})
	for _, tc := range []struct {
		field string
		want  []string
	}{
		{"Queue", []string{"a", "b", "c"}},
		{"Stack", []string{"c", "b", "a"}},
		{"Ranked", []string{"high", "mid", "low"}},
	} {
		for _, want := range tc.want {
			popped, err := node.Pop(ctx, PopRequest{StoreTarget: u62Target(tc.field)})
			if err != nil || !popped.Found || u62Item(t, popped.Element) != want {
				t.Fatalf("Pop %s = %+v, %v, want %q", tc.field, popped, err, want)
			}
		}
		if got := u62Marker(t, node, tc.field); !strings.HasSuffix(got, "(0 elements)") {
			t.Fatalf("%s = %q after popping everything", tc.field, got)
		}
	}
}

func TestU62PopOfAnEmptyFieldIsNotAnError(t *testing.T) {
	node, _ := u62Fresh(t)
	popped, err := node.Pop(context.Background(), PopRequest{StoreTarget: u62Target("Queue")})
	if err != nil || popped.Found || popped.Element != nil {
		t.Fatalf("Pop of an empty queue = %+v, %v, want not found and no error", popped, err)
	}
}

func TestU62PopRefusalsNameTheFix(t *testing.T) {
	node, _ := u62Fresh(t)
	ctx := context.Background()
	ghost := u62Target("Queue")
	ghost.ID = "ghost"
	_, err := node.Pop(ctx, PopRequest{StoreTarget: ghost})
	u30iCode(t, err, cluster.CodeNotFound)
	_, err = node.Pop(ctx, PopRequest{StoreTarget: u62Target("Name")})
	u30iCode(t, err, cluster.CodeFieldType)
	_, err = node.Pop(ctx, PopRequest{StoreTarget: u62Target("Table")})
	u30iCode(t, err, cluster.CodeInvalidArgument)
	if !strings.Contains(err.Error(), "hashStore") || !strings.Contains(err.Error(), "Nothing was") {
		t.Fatalf("pop on a hashStore says %q, want the store type and that nothing changed", err)
	}
}

func TestU62RetriedPopReturnsTheSameElementAndRemovesOne(t *testing.T) {
	node, _ := u62Fresh(t)
	ctx := context.Background()
	u62Push(t, node, "Queue", "a", "b", "c")
	request := [16]byte{0x62, 1}
	first, err := node.Pop(ctx, PopRequest{StoreTarget: u62Target("Queue")}, WithRequestID(request))
	if err != nil || u62Item(t, first.Element) != "a" {
		t.Fatalf("first pop = %+v, %v", first, err)
	}
	retry, err := node.Pop(ctx, PopRequest{StoreTarget: u62Target("Queue")}, WithRequestID(request))
	if err != nil || !retry.Deduped || !retry.Found || u62Item(t, retry.Element) != "a" || retry.Pos != first.Pos {
		t.Fatalf("retry = %+v, %v, want the first answer (a) deduped", retry, err)
	}
	if got := u62Marker(t, node, "Queue"); got != "fifoStore (2 elements)" {
		t.Fatalf("Queue = %q, want one removal after the retry", got)
	}
}

func TestU62PopRefusesAnElementTooBigForItsDedupeEntry(t *testing.T) {
	node, _ := u62Fresh(t)
	ctx := context.Background()
	u62Push(t, node, "Queue", strings.Repeat("x", maxResultBytes+100), "small")
	_, err := node.Pop(ctx, PopRequest{StoreTarget: u62Target("Queue")})
	u30iCode(t, err, cluster.CodeResultTooLarge)
	if !strings.Contains(err.Error(), "claim") {
		t.Fatalf("the refusal says %q, want the fix (claim and ack it)", err)
	}
	if got := u62Marker(t, node, "Queue"); got != "fifoStore (2 elements)" {
		t.Fatalf("Queue = %q, want nothing removed by the refusal", got)
	}
}

func TestU62PeekReadsWithoutRemovingOrLogging(t *testing.T) {
	node, _ := u62Fresh(t)
	ctx := context.Background()
	u62Push(t, node, "Queue", "a", "b", "c")
	u62Push(t, node, "Stack", "a", "b", "c")
	slot := node.slotOf(u30iDB, "Box", "b1")
	before := node.pairs.Snapshot(slot, 1)
	for _, tc := range []struct {
		field string
		count int
		want  []string
	}{
		{"Queue", 2, []string{"a", "b"}},
		{"Stack", 2, []string{"c", "b"}},
		{"Queue", 100, []string{"a", "b", "c"}},
	} {
		elements, err := node.Peek(ctx, PeekRequest{StoreTarget: u62Target(tc.field), Count: tc.count})
		if err != nil || strings.Join(u30iItems(t, elements), ",") != strings.Join(tc.want, ",") {
			t.Fatalf("Peek %s %d = %v, %v, want %v", tc.field, tc.count, u30iItems(t, elements), err, tc.want)
		}
	}
	if after := node.pairs.Snapshot(slot, 1); after != before {
		t.Fatalf("pair after peeks = %+v, want %+v: a peek takes no position", after, before)
	}
	if got := u62Marker(t, node, "Queue"); got != "fifoStore (3 elements)" {
		t.Fatalf("Queue = %q after peeks", got)
	}
	elements, err := node.Peek(ctx, PeekRequest{StoreTarget: u62Target("Ranked"), Count: 1})
	if err != nil || len(elements) != 0 {
		t.Fatalf("Peek of an empty heap = %v, %v, want none", elements, err)
	}
}

func TestU62PeekRefusals(t *testing.T) {
	node, _ := u62Fresh(t)
	ctx := context.Background()
	ghost := u62Target("Queue")
	ghost.ID = "ghost"
	_, err := node.Peek(ctx, PeekRequest{StoreTarget: ghost, Count: 1})
	u30iCode(t, err, cluster.CodeNotFound)
	_, err = node.Peek(ctx, PeekRequest{StoreTarget: u62Target("Name"), Count: 1})
	u30iCode(t, err, cluster.CodeFieldType)
	_, err = node.Peek(ctx, PeekRequest{StoreTarget: u62Target("Table"), Count: 1})
	u30iCode(t, err, cluster.CodeInvalidArgument)
	for _, count := range []int{0, -1, 101} {
		_, err = node.Peek(ctx, PeekRequest{StoreTarget: u62Target("Queue"), Count: count})
		u30iCode(t, err, cluster.CodeInvalidArgument)
	}
}

// TestU62PopsRebuildFromTheLog: pops are in the log, so a node whose Badger is lost is
// rebuilt with the same elements gone, a second scan changes nothing, and a retried pop
// still returns the element it removed.
func TestU62PopsRebuildFromTheLog(t *testing.T) {
	dir := t.TempDir()
	node, err := OpenNode(context.Background(), NodeConfig{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	u62Schema(t, node)
	ctx := context.Background()
	u62Push(t, node, "Queue", "a", "b", "c", "d")
	u62Push(t, node, "Stack", "a", "b", "c", "d")
	request := [16]byte{0x62, 2}
	if _, err := node.Pop(ctx, PopRequest{StoreTarget: u62Target("Queue")}, WithRequestID(request)); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"Queue", "Stack"} {
		if _, err := node.Pop(ctx, PopRequest{StoreTarget: u62Target(field)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	loseStoreForTheReplay(t, dir)
	reopened := u30iOpen(t, dir)
	slot := reopened.slotOf(u30iDB, "Box", "b1")
	once := reopened.pairs.Snapshot(slot, 1)
	if err := reopened.tail.Scan(ctx, 1); err != nil {
		t.Fatal(err)
	}
	if twice := reopened.pairs.Snapshot(slot, 1); twice != once {
		t.Fatalf("pair after a second scan = %+v, want %+v", twice, once)
	}
	if got := u62Marker(t, reopened, "Queue"); got != "fifoStore (2 elements)" {
		t.Fatalf("Queue after the rebuild = %q, want 2 elements", got)
	}
	queue, err := reopened.Peek(ctx, PeekRequest{StoreTarget: u62Target("Queue"), Count: 5})
	stack, err2 := reopened.Peek(ctx, PeekRequest{StoreTarget: u62Target("Stack"), Count: 5})
	if err != nil || err2 != nil || fmt.Sprint(u30iItems(t, queue)) != "[c d]" || fmt.Sprint(u30iItems(t, stack)) != "[c b a]" {
		t.Fatalf("after the rebuild Queue = %v (%v), Stack = %v (%v), want [c d] and [c b a]", u30iItems(t, queue), err, u30iItems(t, stack), err2)
	}
	retry, err := reopened.Pop(ctx, PopRequest{StoreTarget: u62Target("Queue")}, WithRequestID(request))
	if err != nil || !retry.Deduped || u62Item(t, retry.Element) != "a" {
		t.Fatalf("retry after the rebuild = %+v, %v, want a deduped", retry, err)
	}
}
