package wire

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

// u63Fresh is a node with bucket Box { Name, Queue fifoStore, Stack lifoStore, Ranked
// heapStore, Table hashStore, Tree btreeStore, Files blobStore } and one document b1.
func u63Fresh(t *testing.T) (*Node, string) {
	t.Helper()
	dir := t.TempDir()
	node := u30iOpen(t, dir)
	ctx := context.Background()
	if _, err := node.CreateDatabase(ctx, u30iDB); err != nil {
		t.Fatal(err)
	}
	if _, err := node.CreateBucket(ctx, u30iDB, "Box", Field{Name: "Name", Type: "string"},
		Field{Name: "Queue", Type: "fifoStore"}, Field{Name: "Stack", Type: "lifoStore"},
		Field{Name: "Ranked", Type: "heapStore"}, Field{Name: "Table", Type: "hashStore"},
		Field{Name: "Tree", Type: "btreeStore"}, Field{Name: "Files", Type: "blobStore"}); err != nil {
		t.Fatal(err)
	}
	if _, err := node.Insert(ctx, u30iDB, "Box", bson.D{{Key: "_id_", Value: "b1"}, {Key: "Name", Value: "one"}}); err != nil {
		t.Fatal(err)
	}
	return node, dir
}

func u63Item(label string) bson.D { return bson.D{{Key: "item", Value: label}} }

func u63Field(t *testing.T, node *Node, field string, engine store.FieldEngine) store.FieldRef {
	t.Helper()
	ref, err := node.bucket(context.Background(), u30iDB, "Box")
	if err != nil {
		t.Fatal(err)
	}
	fieldID, err := ref.storeField(field)
	if err != nil {
		t.Fatal(err)
	}
	encID, err := store.EncodeComponent("b1")
	if err != nil {
		t.Fatal(err)
	}
	owner := storage.Key(ref.slot(encID), ref.dbID, ref.entry.Decl.BucketID, storage.NSFieldStore, encID)
	return store.FieldRef{Owner: owner, FieldID: fieldID, Engine: engine}
}

// u63Stored reads the element under key straight from the store; "" when absent.
func u63Stored(t *testing.T, node *Node, field string, engine store.FieldEngine, key string) string {
	t.Helper()
	snapshot := node.kv.NewSnapshot()
	defer snapshot.Discard()
	element, found, err := store.FieldGet(snapshot, u63Field(t, node, field, engine), key)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		return ""
	}
	return bson.Raw(element).Lookup("item").StringValue()
}

func u63Count(t *testing.T, node *Node, field string) string {
	t.Helper()
	doc, err := node.Get(context.Background(), u30iDB, "Box", "b1")
	if err != nil {
		t.Fatal(err)
	}
	return u30iString(t, doc, field)
}

func u63Position(node *Node) any {
	return node.pairs.Snapshot(node.slotOf(u30iDB, "Box", "b1"), 1)
}

// u63Raw writes a hand-built STORE_VERB through the owner write path.
func u63Raw(node *Node, field string, verb byte, arg []byte) (WriteAnswer, error) {
	ctx := context.Background()
	call, err := node.storeCall(ctx, u62Target(field))
	if err != nil {
		return WriteAnswer{}, err
	}
	opts, err := newWriteOptions(nil)
	if err != nil {
		return WriteAnswer{}, err
	}
	op := call.ref.template()
	op.Class, op.Fields = cluster.OpStoreVerb, []cluster.FieldOp{{FieldID: call.fieldID, Verb: verb, Value: arg}}
	return node.send(ctx, call.ref, call.encID, op, opts)
}

func u63Put(t *testing.T, node *Node, field, key, label string, options ...WriteOption) {
	t.Helper()
	if err := node.Put(context.Background(), PutRequest{StoreTarget: u62Target(field), Key: key, Element: u63Item(label)}, options...); err != nil {
		t.Fatalf("Put %s/%s: %v", field, key, err)
	}
}

func u63HeapPush(t *testing.T, node *Node, entryID string, priority int64) {
	t.Helper()
	req := PushRequest{StoreTarget: u62Target("Ranked"), Element: u63Item(entryID), Priority: priority, EntryID: entryID}
	if err := node.Push(context.Background(), req); err != nil {
		t.Fatalf("Push %s: %v", entryID, err)
	}
}

func u63Take(t *testing.T, node *Node, field, key string, options ...WriteOption) Taken {
	t.Helper()
	taken, err := node.Take(context.Background(), TakeRequest{StoreTarget: u62Target(field), Key: key}, options...)
	if err != nil {
		t.Fatalf("Take %s/%s: %v", field, key, err)
	}
	return taken
}

func u63TakenItem(taken Taken) string {
	if !taken.Found {
		return ""
	}
	return bson.Raw(taken.Element).Lookup("item").StringValue()
}

func TestU63PutSetsAndReplacesAKeyOfHashAndBtree(t *testing.T) {
	node, _ := u63Fresh(t)
	for field, engine := range map[string]store.FieldEngine{"Table": store.FieldHash, "Tree": store.FieldBTree} {
		u63Put(t, node, field, "k1", "first")
		u63Put(t, node, field, "k2", "other")
		if got := u63Stored(t, node, field, engine, "k1"); got != "first" {
			t.Fatalf("%s k1 = %q after the first put", field, got)
		}
		u63Put(t, node, field, "k1", "second")
		if got := u63Stored(t, node, field, engine, "k1"); got != "second" {
			t.Fatalf("%s k1 = %q after a replacing put, want second", field, got)
		}
		if got := u63Count(t, node, field); !strings.HasSuffix(got, "(2 elements)") {
			t.Fatalf("%s = %q, want 2 elements: a replacing put does not grow the store", field, got)
		}
	}
}

func TestU63HeapPushOrdersByPriority(t *testing.T) {
	node, _ := u63Fresh(t)
	u63HeapPush(t, node, "low", 1)
	u63HeapPush(t, node, "high", 9)
	u63HeapPush(t, node, "mid", 5)
	for _, want := range []string{"high", "mid", "low"} {
		popped, err := node.Pop(context.Background(), PopRequest{StoreTarget: u62Target("Ranked")})
		if err != nil || u62Item(t, popped.Element) != want {
			t.Fatalf("Pop = %+v, %v, want %q", popped, err, want)
		}
	}
}

func TestU63HeapPushNeedsAnEntryIdAndAUsablePriority(t *testing.T) {
	node, _ := u63Fresh(t)
	ctx := context.Background()
	before := u63Position(node)
	err := node.Push(ctx, PushRequest{StoreTarget: u62Target("Ranked"), Element: u63Item("x"), Priority: 1})
	u30iCode(t, err, cluster.CodeInvalidArgument)
	if !strings.Contains(err.Error(), `"e"`) || !strings.Contains(err.Error(), "1024") {
		t.Fatalf("push without an entry id says %q, want the field and the limit", err)
	}
	err = node.Push(ctx, PushRequest{StoreTarget: u62Target("Ranked"), Element: u63Item("x"), EntryID: "x", Priority: math.MinInt64})
	u30iCode(t, err, cluster.CodeInvalidArgument)
	if after := u63Position(node); after != before {
		t.Fatalf("pair moved from %+v to %+v on refused pushes", before, after)
	}
}

func TestU63TakeRemovesAndReturnsTheElement(t *testing.T) {
	node, _ := u63Fresh(t)
	for field, engine := range map[string]store.FieldEngine{"Table": store.FieldHash, "Tree": store.FieldBTree} {
		u63Put(t, node, field, "k1", "one")
		u63Put(t, node, field, "k2", "two")
		taken := u63Take(t, node, field, "k1")
		if !taken.Found || u63TakenItem(taken) != "one" || taken.Pos.Zero() {
			t.Fatalf("%s take k1 = %+v", field, taken)
		}
		if got := u63Stored(t, node, field, engine, "k1"); got != "" {
			t.Fatalf("%s k1 = %q after take, want gone", field, got)
		}
		if got := u63Stored(t, node, field, engine, "k2"); got != "two" {
			t.Fatalf("%s k2 = %q, want untouched", field, got)
		}
		if got := u63Count(t, node, field); !strings.HasSuffix(got, "(1 element)") {
			t.Fatalf("%s = %q, want 1 element", field, got)
		}
	}
}

func TestU63TakeOfAnAbsentKeyIsNotAnError(t *testing.T) {
	node, _ := u63Fresh(t)
	for _, field := range []string{"Table", "Tree", "Ranked"} {
		taken := u63Take(t, node, field, "nothing-here")
		if taken.Found || taken.Element != nil || taken.Pos.Zero() {
			t.Fatalf("%s take of an absent key = %+v, want not found with a position", field, taken)
		}
	}
}

func TestU63TakeOfAHeapEntryLeavesTheRestInOrder(t *testing.T) {
	node, _ := u63Fresh(t)
	u63HeapPush(t, node, "low", 1)
	u63HeapPush(t, node, "high", 9)
	u63HeapPush(t, node, "mid", 5)
	taken := u63Take(t, node, "Ranked", "mid")
	if !taken.Found || u63TakenItem(taken) != "mid" {
		t.Fatalf("take mid = %+v", taken)
	}
	if got := u63Count(t, node, "Ranked"); got != "heapStore (2 elements)" {
		t.Fatalf("Ranked = %q, want 2 elements", got)
	}
	for _, want := range []string{"high", "low"} {
		popped, err := node.Pop(context.Background(), PopRequest{StoreTarget: u62Target("Ranked")})
		if err != nil || u62Item(t, popped.Element) != want {
			t.Fatalf("Pop = %+v, %v, want %q", popped, err, want)
		}
	}
}

// TestU63RetriedTakeReturnsTheSameElement: [GUARD the removed element is the dedupe result].
func TestU63RetriedTakeReturnsTheSameElement(t *testing.T) {
	node, _ := u63Fresh(t)
	u63Put(t, node, "Table", "k1", "original")
	request := [16]byte{0x63, 1}
	first := u63Take(t, node, "Table", "k1", WithRequestID(request))
	if u63TakenItem(first) != "original" {
		t.Fatalf("first take = %+v", first)
	}
	u63Put(t, node, "Table", "k1", "newer")
	retry := u63Take(t, node, "Table", "k1", WithRequestID(request))
	if !retry.Deduped || u63TakenItem(retry) != "original" || retry.Pos != first.Pos {
		t.Fatalf("retry = %+v, want the first answer (original) deduped", retry)
	}
	if got := u63Stored(t, node, "Table", store.FieldHash, "k1"); got != "newer" {
		t.Fatalf("k1 = %q after the retry, want the newer put kept: a retry removes nothing", got)
	}
}

func TestU63RetriedTakeOfAnAbsentKeyStaysNotFound(t *testing.T) {
	node, _ := u63Fresh(t)
	request := [16]byte{0x63, 2}
	first := u63Take(t, node, "Table", "k1", WithRequestID(request))
	u63Put(t, node, "Table", "k1", "later")
	retry := u63Take(t, node, "Table", "k1", WithRequestID(request))
	if first.Found || retry.Found || !retry.Deduped {
		t.Fatalf("first = %+v, retry = %+v, want both not found and the retry deduped", first, retry)
	}
	if got := u63Stored(t, node, "Table", store.FieldHash, "k1"); got != "later" {
		t.Fatalf("k1 = %q, want the later put kept", got)
	}
}

// TestU63TakeRefusesAnElementTooBigForItsDedupeEntry: [GUARD over-cap refusal before a position].
func TestU63TakeRefusesAnElementTooBigForItsDedupeEntry(t *testing.T) {
	node, _ := u63Fresh(t)
	big := strings.Repeat("x", maxResultBytes+100)
	u63Put(t, node, "Table", "big", big)
	u63Put(t, node, "Tree", "big", big)
	u63HeapPush(t, node, "first", 9)
	if err := node.Push(context.Background(), PushRequest{StoreTarget: u62Target("Ranked"), Element: u63Item(big), EntryID: "big", Priority: 1}); err != nil {
		t.Fatal(err)
	}
	before := u63Position(node)
	for field, key := range map[string]string{"Table": "big", "Tree": "big", "Ranked": "big"} {
		_, err := node.Take(context.Background(), TakeRequest{StoreTarget: u62Target(field), Key: key})
		u30iCode(t, err, cluster.CodeResultTooLarge)
		if !strings.Contains(err.Error(), "Nothing was removed") {
			t.Fatalf("%s: the refusal says %q, want that nothing was removed", field, err)
		}
	}
	if after := u63Position(node); after != before {
		t.Fatalf("pair moved from %+v to %+v: the refusal must come before a position", before, after)
	}
	if u63Stored(t, node, "Table", store.FieldHash, "big") != big || u63Count(t, node, "Ranked") != "heapStore (2 elements)" {
		t.Fatal("a refused take removed something")
	}
}

func TestU63TakeOfAMissingDocumentIsNotFound(t *testing.T) {
	node, _ := u63Fresh(t)
	ghost := u62Target("Table")
	ghost.ID = "ghost"
	_, err := node.Take(context.Background(), TakeRequest{StoreTarget: ghost, Key: "k"})
	u30iCode(t, err, cluster.CodeNotFound)
	err = node.Put(context.Background(), PutRequest{StoreTarget: ghost, Key: "k", Element: u63Item("x")})
	u30iCode(t, err, cluster.CodeNotFound)
}

// TestU63WrongVerbAndStoreTypePairsAreRefusedBeforeAPosition: [GUARD verb/store pair table].
func TestU63WrongVerbAndStoreTypePairsAreRefusedBeforeAPosition(t *testing.T) {
	node, _ := u63Fresh(t)
	ctx := context.Background()
	element := bson.D{{Key: "item", Value: "x"}}
	cases := []struct {
		name  string
		field string
		do    func(field string) error
		store string
		fits  string
	}{
		{"push to hash", "Table", func(f string) error {
			return node.Push(ctx, PushRequest{StoreTarget: u62Target(f), Element: element})
		}, "hashStore", "put"},
		{"push to btree", "Tree", func(f string) error {
			return node.Push(ctx, PushRequest{StoreTarget: u62Target(f), Element: element})
		}, "btreeStore", "put"},
		{"push to blob", "Files", func(f string) error {
			return node.Push(ctx, PushRequest{StoreTarget: u62Target(f), Element: element})
		}, "blobStore", ""},
		{"put to fifo", "Queue", func(f string) error {
			return node.Put(ctx, PutRequest{StoreTarget: u62Target(f), Key: "k", Element: element})
		}, "fifoStore", "push"},
		{"put to lifo", "Stack", func(f string) error {
			return node.Put(ctx, PutRequest{StoreTarget: u62Target(f), Key: "k", Element: element})
		}, "lifoStore", "push"},
		{"put to heap", "Ranked", func(f string) error {
			return node.Put(ctx, PutRequest{StoreTarget: u62Target(f), Key: "k", Element: element})
		}, "heapStore", "push"},
		{"put to blob", "Files", func(f string) error {
			return node.Put(ctx, PutRequest{StoreTarget: u62Target(f), Key: "k", Element: element})
		}, "blobStore", ""},
		{"take from fifo", "Queue", func(f string) error {
			_, err := node.Take(ctx, TakeRequest{StoreTarget: u62Target(f), Key: "k"})
			return err
		}, "fifoStore", "pop"},
		{"take from lifo", "Stack", func(f string) error {
			_, err := node.Take(ctx, TakeRequest{StoreTarget: u62Target(f), Key: "k"})
			return err
		}, "lifoStore", "pop"},
		{"take from blob", "Files", func(f string) error {
			_, err := node.Take(ctx, TakeRequest{StoreTarget: u62Target(f), Key: "k"})
			return err
		}, "blobStore", ""},
	}
	before := u63Position(node)
	for _, tc := range cases {
		err := tc.do(tc.field)
		u30iCode(t, err, cluster.CodeInvalidArgument)
		if !strings.Contains(err.Error(), "the field is a "+tc.store) || !strings.Contains(err.Error(), tc.fits) || !strings.Contains(err.Error(), "Nothing was") {
			t.Errorf("%s: %q, want the store type %q, the verb %q and that nothing changed", tc.name, err, tc.store, tc.fits)
		}
	}
	if after := u63Position(node); after != before {
		t.Fatalf("pair moved from %+v to %+v on refused pairs", before, after)
	}
}

// TestU63OwnerRefusesWhatTheNodeNeverSends: the owner path checks the pair table and the
// arg itself, so a record built by hand is refused before a position too.
func TestU63OwnerRefusesWhatTheNodeNeverSends(t *testing.T) {
	node, _ := u63Fresh(t)
	element, err := bson.Marshal(u63Item("x"))
	if err != nil {
		t.Fatal(err)
	}
	putArg, err := encodePutArg("k", element)
	if err != nil {
		t.Fatal(err)
	}
	takeArg, err := encodeTakeKeyArg("k")
	if err != nil {
		t.Fatal(err)
	}
	pushArg, err := encodeElementArg(element)
	if err != nil {
		t.Fatal(err)
	}
	before := u63Position(node)
	for name, tc := range map[string]struct {
		field string
		verb  byte
		arg   []byte
	}{
		"take on a fifo":              {"Queue", cluster.VerbTake, takeArg},
		"put on a heap":               {"Ranked", cluster.VerbPut, putArg},
		"push on a hash":              {"Table", cluster.VerbPush, pushArg},
		"old raw element as push arg": {"Queue", cluster.VerbPush, element},
		"put arg given to a push":     {"Queue", cluster.VerbPush, putArg},
		"pop carrying an arg":         {"Queue", cluster.VerbPop, pushArg},
		"put with a take arg":         {"Table", cluster.VerbPut, takeArg},
		"take with an empty arg":      {"Table", cluster.VerbTake, nil},
	} {
		_, err := u63Raw(node, tc.field, tc.verb, tc.arg)
		var coded *cluster.Error
		if !errors.As(err, &coded) || coded.Code != cluster.CodeInvalidArgument {
			t.Errorf("%s: err = %v, want INVALID_ARGUMENT", name, err)
		}
	}
	if after := u63Position(node); after != before {
		t.Fatalf("pair moved from %+v to %+v on refused records", before, after)
	}
	if queued, err := node.Peek(context.Background(), PeekRequest{StoreTarget: u62Target("Queue"), Count: 5}); err != nil || len(queued) != 0 {
		t.Fatalf("Queue holds %d elements (%v), want nothing written", len(queued), err)
	}
}

func TestU63PushToFifoAndLifoStillRoundTrips(t *testing.T) {
	node, _ := u63Fresh(t)
	u62Push(t, node, "Queue", "a", "b")
	u62Push(t, node, "Stack", "a", "b")
	queue, err := node.Peek(context.Background(), PeekRequest{StoreTarget: u62Target("Queue"), Count: 5})
	stack, err2 := node.Peek(context.Background(), PeekRequest{StoreTarget: u62Target("Stack"), Count: 5})
	if err != nil || err2 != nil || fmt.Sprint(u30iItems(t, queue)) != "[a b]" || fmt.Sprint(u30iItems(t, stack)) != "[b a]" {
		t.Fatalf("Queue = %v (%v), Stack = %v (%v)", u30iItems(t, queue), err, u30iItems(t, stack), err2)
	}
}

// TestU63VerbsRebuildFromTheLog: every new verb is in the log, so a node whose Badger is
// lost rebuilds the same stores, a second scan changes nothing, and a retried take still
// returns the element it removed.
func TestU63VerbsRebuildFromTheLog(t *testing.T) {
	node, dir := u63Fresh(t)
	ctx := context.Background()
	for _, field := range []string{"Table", "Tree"} {
		u63Put(t, node, field, "keep", "kept")
		u63Put(t, node, field, "gone", "removed")
		u63Put(t, node, field, "keep", "kept-again")
	}
	u63HeapPush(t, node, "low", 1)
	u63HeapPush(t, node, "high", 9)
	u63HeapPush(t, node, "mid", 5)
	request := [16]byte{0x63, 3}
	heapRequest := [16]byte{0x63, 4}
	first := u63Take(t, node, "Table", "gone", WithRequestID(request))
	u63Take(t, node, "Tree", "gone")
	u63Take(t, node, "Table", "absent")
	heapFirst := u63Take(t, node, "Ranked", "mid", WithRequestID(heapRequest))
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
	for field, engine := range map[string]store.FieldEngine{"Table": store.FieldHash, "Tree": store.FieldBTree} {
		if got := u63Stored(t, reopened, field, engine, "keep"); got != "kept-again" {
			t.Fatalf("%s keep = %q after the rebuild, want kept-again", field, got)
		}
		if got := u63Stored(t, reopened, field, engine, "gone"); got != "" {
			t.Fatalf("%s gone = %q after the rebuild, want removed", field, got)
		}
		if got := u63Count(t, reopened, field); !strings.HasSuffix(got, "(1 element)") {
			t.Fatalf("%s = %q after the rebuild, want 1 element", field, got)
		}
	}
	for _, want := range []string{"high", "low"} {
		popped, err := reopened.Pop(ctx, PopRequest{StoreTarget: u62Target("Ranked")})
		if err != nil || u62Item(t, popped.Element) != want {
			t.Fatalf("Pop after the rebuild = %+v, %v, want %q", popped, err, want)
		}
	}
	retry := u63Take(t, reopened, "Table", "gone", WithRequestID(request))
	heapRetry := u63Take(t, reopened, "Ranked", "mid", WithRequestID(heapRequest))
	if !retry.Deduped || u63TakenItem(retry) != "removed" || retry.Pos != first.Pos {
		t.Fatalf("hash retry after the rebuild = %+v, want removed deduped at %v", retry, first.Pos)
	}
	if !heapRetry.Deduped || u63TakenItem(heapRetry) != "mid" || heapRetry.Pos != heapFirst.Pos {
		t.Fatalf("heap retry after the rebuild = %+v, want mid deduped at %v", heapRetry, heapFirst.Pos)
	}
}

// TestU63ReplayingTheLogTwiceChangesNothing: applying the same records again over a
// rebuilt store leaves the counts and the pair where they were.
func TestU63ReplayingTheLogTwiceChangesNothing(t *testing.T) {
	node, dir := u63Fresh(t)
	u63Put(t, node, "Table", "a", "1")
	u63Put(t, node, "Table", "b", "2")
	u63Take(t, node, "Table", "a")
	u63HeapPush(t, node, "x", 1)
	u63HeapPush(t, node, "y", 2)
	u63Take(t, node, "Ranked", "x")
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	for round := 0; round < 2; round++ {
		if round == 0 {
			loseStoreForTheReplay(t, dir)
		}
		reopened, err := OpenNode(context.Background(), NodeConfig{Dir: dir})
		if err != nil {
			t.Fatal(err)
		}
		if got := u63Count(t, reopened, "Table"); got != "hashStore (1 element)" {
			t.Fatalf("round %d: Table = %q", round, got)
		}
		if got := u63Count(t, reopened, "Ranked"); got != "heapStore (1 element)" {
			t.Fatalf("round %d: Ranked = %q", round, got)
		}
		if err := reopened.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
