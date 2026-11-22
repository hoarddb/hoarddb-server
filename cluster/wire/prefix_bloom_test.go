package wire

import (
	"context"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

// bloomReaderNode opens a ring node on dir with database app and, when it is fresh, a bucket
// with a scalar, a fifoStore and a lifoStore field.
func bloomReaderNode(t *testing.T, dir string, fresh bool) *Node {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	node, err := OpenNode(ctx, NodeConfig{Dir: dir})
	if err != nil {
		t.Fatalf("OpenNode(%s): %v", dir, err)
	}
	if !fresh {
		return node
	}
	if _, err := node.CreateDatabase(ctx, u30iDB); err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}
	if _, err := node.CreateBucket(ctx, u30iDB, "Box",
		Field{Name: "Name", Type: "string"}, Field{Name: "Queue", Type: "fifoStore"}, Field{Name: "Stack", Type: "lifoStore"}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	return node
}

// TestReadersUseTheFilter is the prefix bloom spec's T11: a ring node's get asks the filter
// under the document rule, and a store-field list, a fifo peek and a lifo peek under the store
// rule; each returns the right result from the memtable, after a restart (which flushes the
// memtable into a table), and after an update or a push on top of that table.
// Against: building the IterOptions by hand in readStoredDocument or FieldRef.elementBounds.
func TestReadersUseTheFilter(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	node := bloomReaderNode(t, dir, true)
	closeNode := func() {
		t.Helper()
		if err := node.Close(); err != nil {
			t.Fatalf("closing the node: %v", err)
		}
	}
	queue := StoreTarget{Database: u30iDB, Bucket: "Box", ID: "b1", Field: "Queue"}
	stack := StoreTarget{Database: u30iDB, Bucket: "Box", ID: "b1", Field: "Stack"}
	push := func(target StoreTarget, n int32) {
		t.Helper()
		if err := node.Push(ctx, PushRequest{StoreTarget: target, Element: bson.D{{Key: "n", Value: n}}}); err != nil {
			t.Fatalf("pushing %d to %s: %v", n, target.Field, err)
		}
	}
	numbers := func(elements [][]byte) []int32 {
		out := make([]int32, 0, len(elements))
		for _, element := range elements {
			out = append(out, bson.Raw(element).Lookup("n").Int32())
		}
		return out
	}

	// read runs the four readers, checks what each returns, and requires each to have moved
	// its rule's Iterators and nothing to have been refused.
	read := func(phase, wantName string, wantQueue, wantStack []int32) {
		t.Helper()
		step := func(reader string, moved func(before, after storage.PrefixBloomStats) uint64, run func()) {
			t.Helper()
			before := node.PrefixBloomStats()
			run()
			after := node.PrefixBloomStats()
			if moved(before, after) == 0 {
				t.Fatalf("%s: %s did not ask the filter: its rule's Iterators did not move (%+v, were %+v)", phase, reader, after, before)
			}
			if after.Refused != before.Refused {
				t.Fatalf("%s: %s had its filter refused %d times", phase, reader, after.Refused-before.Refused)
			}
		}
		document := func(before, after storage.PrefixBloomStats) uint64 {
			return after.Document.Iterators - before.Document.Iterators
		}
		store := func(before, after storage.PrefixBloomStats) uint64 {
			return after.Store.Iterators - before.Store.Iterators
		}
		step("get", document, func() {
			doc, err := node.Get(ctx, u30iDB, "Box", "b1")
			if err != nil {
				t.Fatalf("%s: Get: %v", phase, err)
			}
			if got := bson.Raw(doc.Body).Lookup("Name").StringValue(); got != wantName {
				t.Fatalf("%s: Name = %q, want %q", phase, got, wantName)
			}
		})
		step("a store-field list", store, func() {
			page, err := node.ListField(ctx, FieldListRequest{StoreTarget: queue})
			if err != nil {
				t.Fatalf("%s: ListField: %v", phase, err)
			}
			if got := numbers(page.Elements); !slices.Equal(got, wantQueue) {
				t.Fatalf("%s: the list of Queue is %v, want %v", phase, got, wantQueue)
			}
		})
		step("a fifo peek", store, func() {
			peeked, err := node.Peek(ctx, PeekRequest{StoreTarget: queue, Count: 1})
			if err != nil {
				t.Fatalf("%s: Peek of Queue: %v", phase, err)
			}
			if got := numbers(peeked); !slices.Equal(got, wantQueue[:1]) {
				t.Fatalf("%s: the fifo peek is %v, want the oldest, %v", phase, got, wantQueue[:1])
			}
		})
		step("a lifo peek", store, func() {
			peeked, err := node.Peek(ctx, PeekRequest{StoreTarget: stack, Count: 1})
			if err != nil {
				t.Fatalf("%s: Peek of Stack: %v", phase, err)
			}
			if got := numbers(peeked); !slices.Equal(got, wantStack[len(wantStack)-1:]) {
				t.Fatalf("%s: the lifo peek is %v, want the newest, %v", phase, got, wantStack[len(wantStack)-1:])
			}
		})
		// A document that was never written reads as absent through the filter too.
		if _, err := node.Get(ctx, u30iDB, "Box", "nobody"); err == nil {
			t.Fatalf("%s: Get of a document that was never inserted returned one", phase)
		}
	}

	if _, err := node.Insert(ctx, u30iDB, "Box", bson.D{{Key: "_id_", Value: "b1"}, {Key: "Name", Value: "one"}}); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	// Other documents in the same bucket, so b1 is not the only thing a table holds.
	for _, id := range []string{"a0", "z9"} {
		if _, err := node.Insert(ctx, u30iDB, "Box", bson.D{{Key: "_id_", Value: id}, {Key: "Name", Value: id}}); err != nil {
			t.Fatalf("Insert %s: %v", id, err)
		}
	}
	for n := range int32(3) {
		push(queue, n)
		push(stack, 10+n)
	}
	read("in the memtable", "one", []int32{0, 1, 2}, []int32{10, 11, 12})

	closeNode()
	node = bloomReaderNode(t, dir, false)
	read("after a restart", "one", []int32{0, 1, 2}, []int32{10, 11, 12})

	// An update writes the field key alone and a push one element: the newest version of
	// each sits in the memtable, above the table the restart flushed.
	if _, err := node.Update(ctx, UpdateRequest{Database: u30iDB, Bucket: "Box", ID: "b1", Set: bson.D{{Key: "Name", Value: "two"}}}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	push(queue, 3)
	push(stack, 13)
	read("after an update and a push", "two", []int32{0, 1, 2, 3}, []int32{10, 11, 12, 13})

	closeNode()
	node = bloomReaderNode(t, dir, false)
	defer closeNode()
	read("after a second restart", "two", []int32{0, 1, 2, 3}, []int32{10, 11, 12, 13})

	// The second flush put the field-only update into a table without its header. The readers
	// above met the tables the two restarts left (how many depends on compaction, which is why
	// nothing here counts Skipped: TestAbsentPrefixSkipsTablesAndCounts does, on a store that
	// does not compact); none may have been built without the rules.
	stats := node.PrefixBloomStats()
	if stats.Document.Candidates == 0 || stats.Store.Candidates == 0 {
		t.Fatalf("the readers met no table after two restarts: %+v", stats)
	}
	if stats.Document.Unmarked != 0 || stats.Store.Unmarked != 0 {
		t.Fatalf("a table of a format %d directory carries no rule bit: %+v", ringFormat, stats)
	}
}

// TestAllScalarsReadsThroughTheFilter covers the third reader of the prefix bloom spec's §3.3:
// allScalars, the whole-document read behind a refused guard's answer and behind an update or
// delete that returns the document as it was. It walks one document's keys, so it asks the
// filter under the document rule, and it returns the newest value of every key when a field
// was last written into a table that does not hold the document's header.
// Against: building the IterOptions by hand in allScalars.
func TestAllScalarsReadsThroughTheFilter(t *testing.T) {
	dir := t.TempDir()
	open := func() *storage.BadgerKV {
		t.Helper()
		kv, err := storage.OpenBadgerKV(dir, storage.BadgerTuning{MemTableMB: 32}, nil)
		if err != nil {
			t.Fatalf("opening the store: %v", err)
		}
		return kv
	}
	// write commits the pairs and closes the store, which flushes them into one table.
	write := func(pairs map[string]string) {
		t.Helper()
		kv := open()
		batch := kv.NewBatch()
		for key, value := range pairs {
			if err := batch.Set([]byte(key), []byte(value)); err != nil {
				t.Fatalf("adding %x to a batch: %v", key, err)
			}
		}
		if err := batch.Commit(false); err != nil {
			t.Fatalf("committing: %v", err)
		}
		if err := kv.Close(); err != nil {
			t.Fatalf("closing the store: %v", err)
		}
	}
	document := func(letter byte) []byte {
		return store.DocumentPrefix(3, 7, 9, []byte{storage.TagString, letter, 0x00, 0x00})
	}
	prefix := document('m')
	// The header and a field, between two neighbours; then two fields without the header.
	write(map[string]string{
		string(prefix): "header", string(store.FieldKey(prefix, 2)): "old",
		string(document('a')): "low", string(document('z')): "high",
	})
	write(map[string]string{string(store.FieldKey(prefix, 2)): "new", string(store.FieldKey(prefix, 3)): "third"})

	kv := open()
	defer func() {
		if err := kv.Close(); err != nil {
			t.Errorf("closing the store: %v", err)
		}
	}()
	snapshot := kv.NewSnapshot()
	defer snapshot.Discard()
	before := kv.PrefixBloomStats()
	scalars, err := allScalars(snapshot, prefix)
	if err != nil {
		t.Fatalf("allScalars: %v", err)
	}
	after := kv.PrefixBloomStats()
	want := map[uint16][]byte{headerScalarID: []byte("header"), 2: []byte("new"), 3: []byte("third")}
	if !reflect.DeepEqual(scalars, want) {
		t.Fatalf("allScalars returned %v, want %v", scalars, want)
	}
	if moved := after.Document.Iterators - before.Document.Iterators; moved != 1 {
		t.Fatalf("allScalars did not ask the filter: the document rule's Iterators moved by %d, want 1 (%+v, were %+v)", moved, after, before)
	}
	if after.Refused != before.Refused || after.Document.Unmarked != 0 {
		t.Fatalf("allScalars' filter was refused, or met a table without the rule bit: %+v, were %+v", after, before)
	}
}
