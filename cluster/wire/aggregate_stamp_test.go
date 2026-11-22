package wire

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/storage/kvfault"
	"github.com/hoarddb/hoarddb-server/store"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// fixedEngines is a FieldEngineSource with a copy's knowledge only: ids and engines, no
// aggregates. A copy applies the record's stamp and has nothing else to read.
type fixedEngines map[uint16]store.FieldEngine

func (e fixedEngines) FieldEngine(_, _ uint64, fieldID uint16) (store.FieldEngine, error) {
	if engine, ok := e[fieldID]; ok {
		return engine, nil
	}
	return 0, fmt.Errorf("field %d: %w", fieldID, errUndeclaredField)
}

// loggedOps decodes every record the node's own log holds, with the slot and position of each.
func loggedOps(t *testing.T, node *Node) []AppliedRecord {
	t.Helper()
	entries, err := node.wal.ReadFrom(1)
	if err != nil {
		t.Fatal(err)
	}
	var out []AppliedRecord
	for _, entry := range entries {
		slot, db, pos, err := storage.DecodePositionKey(entry.Key)
		if err != nil {
			t.Fatal(err)
		}
		record, err := storage.DecodeWALRecord(entry.Op, entry.Data)
		if err != nil {
			t.Fatal(err)
		}
		op, err := cluster.DecodeOp(record.Body)
		if err != nil {
			t.Fatalf("DecodeOp of seq %d: %v", entry.Seq, err)
		}
		out = append(out, AppliedRecord{Slot: slot, DB: db, Op: op, Pos: cluster.Position(pos), Cursor: entry.Seq,
			RequestID: record.RequestID, OpIndex: record.OpIndex})
	}
	return out
}

// storeRange is every key of one field of l1 including its aggregate keys.
func storeRange(t *testing.T, kv storage.KV, prefix []byte) map[string]string {
	t.Helper()
	snapshot := kv.NewSnapshot()
	defer snapshot.Discard()
	upper := append(bytes.Clone(prefix), 0xFF, 0xFF)
	it := snapshot.NewIterator(storage.IterOptions{Lower: prefix, Upper: upper})
	defer it.Close()
	keys := map[string]string{}
	for it.Seek(nil); it.Valid(); it.Next() {
		value, err := it.Value()
		if err != nil {
			t.Fatal(err)
		}
		keys[string(it.Key())] = string(value)
	}
	return keys
}

// aggregateWorkload drives pushes, puts, pops, a claim and an ack, a replace and a take, the
// verbs that change a sum.
func aggregateWorkload(t *testing.T, node *Node) {
	t.Helper()
	ctx := context.Background()
	for i, currency := range []string{"USD", "EUR", "USD", "GBP"} {
		if err := node.Push(ctx, PushRequest{StoreTarget: aggTarget("Fifo"), Element: expense(int64(10*(i+1)), currency)}); err != nil {
			t.Fatal(err)
		}
		if err := node.Put(ctx, PutRequest{StoreTarget: aggTarget("Tree"), Key: fmt.Sprint("k", i), Element: expense(int64(5*(i+1)), currency)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := node.Put(ctx, PutRequest{StoreTarget: aggTarget("Tree"), Key: "k1", Element: expense(int64(99), "USD")}); err != nil {
		t.Fatal(err)
	}
	if _, err := node.Take(ctx, TakeRequest{StoreTarget: aggTarget("Tree"), Key: "k3"}); err != nil {
		t.Fatal(err)
	}
	if _, err := node.Pop(ctx, PopRequest{StoreTarget: aggTarget("Fifo")}); err != nil {
		t.Fatal(err)
	}
	lease, err := node.Claim(ctx, ClaimRequest{StoreTarget: aggTarget("Fifo"), Count: 2})
	if err != nil {
		t.Fatal(err)
	}
	if err := node.Ack(ctx, AckRequest{StoreTarget: aggTarget("Fifo"), Lease: lease.ID}); err != nil {
		t.Fatal(err)
	}
}

// TestTheNodeWiresTheAggregateStamp: the owner's record of a write to a store with aggregates
// carries the stamp, in id order, and a write to a field with none carries nothing.
func TestTheNodeWiresTheAggregateStamp(t *testing.T) {
	node := aggRig(t)
	if err := node.Push(context.Background(), PushRequest{StoreTarget: aggTarget("Fifo"), Element: expense(int64(1), "USD")}); err != nil {
		t.Fatal(err)
	}
	records := loggedOps(t, node)
	last := records[len(records)-1].Op
	if last.Class != cluster.OpStoreVerb || len(last.Aggregates) != 3 {
		t.Fatalf("the push's record carries %d aggregates (class %#02x), want 3", len(last.Aggregates), last.Class)
	}
	for i := 1; i < len(last.Aggregates); i++ {
		if last.Aggregates[i-1].AggID >= last.Aggregates[i].AggID {
			t.Fatalf("the stamp is not in id order: %+v", last.Aggregates)
		}
	}
	for _, record := range records {
		if record.Op.Class == cluster.OpInsert && len(record.Op.Aggregates) != 0 {
			t.Fatalf("an insert carries an aggregate stamp: %+v", record.Op.Aggregates)
		}
	}
}

// TestCopyAndOwnerHoldIdenticalAggregateKeys: the owner's log, applied with no declaration
// and no aggregate source, leaves every key under the stores, the aggregate keys included,
// byte for byte as the owner holds them.
func TestCopyAndOwnerHoldIdenticalAggregateKeys(t *testing.T) {
	node := aggRig(t)
	aggregateWorkload(t, node)
	engines := fixedEngines{}
	for _, field := range []string{"Fifo", "Stack", "Ranked", "Tree", "Table"} {
		ref, err := node.bucket(context.Background(), u30iDB, "Ledger")
		if err != nil {
			t.Fatal(err)
		}
		fieldID, err := ref.storeField(field)
		if err != nil {
			t.Fatal(err)
		}
		engine, err := declaredEngines{node: node}.FieldEngine(ref.dbID, ref.entry.Decl.BucketID, fieldID)
		if err != nil {
			t.Fatal(err)
		}
		engines[fieldID] = engine
	}
	copyKV := kvfault.New()
	applier := NewDocumentApplier(copyKV, nil).WithFieldEngines(engines)
	for _, record := range loggedOps(t, node) {
		if record.Op.BucketID == store.BucketsBucketID {
			continue // declarations are the owner's; a copy gets them through its own stream
		}
		batch := copyKV.NewBatch()
		if err := applier.Fill(batch, record); err != nil {
			t.Fatalf("a copy applying position %s: %v", record.Pos, err)
		}
		if err := batch.Commit(false); err != nil {
			t.Fatal(err)
		}
	}
	checked := 0
	for _, field := range []string{"Fifo", "Tree"} {
		prefix := aggPrefix(t, node, field)
		owner, copied := storeRange(t, node.kv, prefix), storeRange(t, copyKV, prefix)
		aggregateKeys := 0
		for key := range owner {
			if key[len(prefix)] == store.InnerAggregate {
				aggregateKeys++
			}
		}
		if aggregateKeys == 0 {
			t.Fatalf("%s: the owner holds no aggregate keys, so the comparison proves nothing", field)
		}
		if len(owner) != len(copied) {
			t.Fatalf("%s: owner holds %d keys, copy %d", field, len(owner), len(copied))
		}
		for key, value := range owner {
			if got, ok := copied[key]; !ok || got != value {
				t.Fatalf("%s: key %x differs: owner %x, copy %x (present %v)", field, key, value, got, ok)
			}
		}
		checked += aggregateKeys
	}
	t.Logf("compared %d aggregate keys", checked)
}

// TestACopyAppliesFromTheRecordNotTheDeclaration: an applier with no aggregate source maintains
// exactly the stamp it is given, so a copy with a cold or different declaration cannot compute
// another list.
func TestACopyAppliesFromTheRecordNotTheDeclaration(t *testing.T) {
	node := aggRig(t)
	if err := node.Push(context.Background(), PushRequest{StoreTarget: aggTarget("Stack"), Element: expense(int64(4), "USD")}); err != nil {
		t.Fatal(err)
	}
	records := loggedOps(t, node)
	last := records[len(records)-1]
	if len(last.Op.Aggregates) != 1 {
		t.Fatalf("want one stamped aggregate, got %+v", last.Op.Aggregates)
	}
	ref, err := node.bucket(context.Background(), u30iDB, "Ledger")
	if err != nil {
		t.Fatal(err)
	}
	fieldID, err := ref.storeField("Stack")
	if err != nil {
		t.Fatal(err)
	}
	// A different stamp is applied as given, whatever the declaration says.
	last.Op.Aggregates = []cluster.AggregateStamp{{AggID: 9, Kind: 1, Field: "Amount"}}
	copyKV := kvfault.New()
	applier := NewDocumentApplier(copyKV, nil).WithFieldEngines(fixedEngines{fieldID: store.FieldLIFO})
	batch := copyKV.NewBatch()
	if err := applier.Fill(batch, last); err != nil {
		t.Fatal(err)
	}
	if err := batch.Commit(false); err != nil {
		t.Fatal(err)
	}
	prefix := aggPrefix(t, node, "Stack")
	snapshot := copyKV.NewSnapshot()
	defer snapshot.Discard()
	if _, err := snapshot.Get(store.AggregateKey(prefix, 9, nil)); err != nil {
		t.Fatalf("the stamped aggregate 9 was not written: %v", err)
	}
	if _, err := snapshot.Get(store.AggregateKey(prefix, 1, nil)); !errors.Is(err, storage.ErrKeyNotFound) {
		t.Fatalf("the declared aggregate 1 was written although the record did not stamp it: %v", err)
	}
}

// TestAggregateKeysGoWithTheDocumentDelete: the keys sit under the store's prefix, so the
// range sweep that finishes a document delete lists them with the elements and the state.
func TestAggregateKeysGoWithTheDocumentDelete(t *testing.T) {
	node := aggRig(t)
	aggregateWorkload(t, node)
	ref, err := node.bucket(context.Background(), u30iDB, "Ledger")
	if err != nil {
		t.Fatal(err)
	}
	encID, err := store.EncodeComponent("l1")
	if err != nil {
		t.Fatal(err)
	}
	doc := DocumentKey{Ref: storage.BucketRef{Slot: ref.slot(encID), DatabaseID: ref.dbID, BucketID: ref.entry.Decl.BucketID}, EncID: encID}
	snapshot := node.kv.NewSnapshot()
	defer snapshot.Discard()
	swept, exhausted, err := nextSweepKeys(snapshot, doc, sweepLimits{keys: 1 << 20, bytes: 1 << 40})
	if err != nil || !exhausted {
		t.Fatalf("nextSweepKeys = exhausted %v, %v", exhausted, err)
	}
	listed := map[string]bool{}
	for _, key := range swept {
		listed[string(key)] = true
	}
	for _, field := range []string{"Fifo", "Tree"} {
		prefix := aggPrefix(t, node, field)
		aggregates := 0
		for key := range storeRange(t, node.kv, prefix) {
			if !listed[key] {
				t.Fatalf("%s: key %x under the store is not in the sweep", field, key)
			}
			if key[len(prefix)] == store.InnerAggregate {
				aggregates++
			}
		}
		if aggregates == 0 {
			t.Fatalf("%s holds no aggregate keys, so the test proves nothing", field)
		}
	}
}

// TestADedupedRetryDoesNotApplyTheAggregateAgain: a push sent twice under one request id is
// applied once, so its sum is added once.
func TestADedupedRetryDoesNotApplyTheAggregateAgain(t *testing.T) {
	node := aggRig(t)
	ctx := context.Background()
	id := aggID(t, node, "Stack", 1, "")
	request := PushRequest{StoreTarget: aggTarget("Stack"), Element: expense(int64(8), "USD")}
	option := WithRequestID([16]byte{9, 9, 9})
	for range 2 {
		if err := node.Push(ctx, request, option); err != nil {
			t.Fatal(err)
		}
	}
	if sum := aggPlainSum(t, node, "Stack", id); sum.Whole != 8 {
		t.Fatalf("sum after one request sent twice = %+v, want 8", sum)
	}
}

// TestTheStampIsTheAdmittedDeclarations: the owner stamps and dry-runs from the declaration the write
// was admitted with, read once. A cache that moves between admission and the stamp (an eviction and a
// refetch, or an alter) never changes the list the record carries.
func TestTheStampIsTheAdmittedDeclarations(t *testing.T) {
	fx := newFixture(t)
	fx.schema.decl.Fields = machine.FieldTable{NextFieldID: 5, Entries: []machine.FieldEntry{{ID: 1, Type: "any", Name: "_id_"}, {ID: 2, Type: "string", Name: "Name"},
		{ID: 3, Type: "int", Name: "Age"}, {ID: 4, Type: "fifoStore", Name: "Posts"}}}
	admitted := machine.AggregateTable{Sets: []machine.AggregateSet{{FieldID: 4, NextID: 2, Entries: []machine.AggregateEntry{{ID: 1, Kind: machine.AggSum, Field: "Amount"}}}}}
	fx.schema.decl.Aggregates = admitted
	fx.w = NewWriter(WriterConfig{
		Topology: fx.topo, Pairs: fx.pairs, Log: fx.wal, KV: fx.kv,
		Applier: NewDocumentApplier(fx.kv, pairRecorder{}).WithFieldEngines(fixedEngines{4: store.FieldFIFO}), Clock: fixedClock{}, Lease: fx.lease,
		Dedupe: fx.dedupe, Schema: fx.schema, Replication: fx.repl, WCTimeout: time.Second,
		DedupeWindow: machine.WindowConfig{Window: 8192, MaxResult: 1024},
	})
	fx.mustWrite(testSlot, "p1")
	element, err := bson.Marshal(bson.D{{Key: "Amount", Value: int64(5)}})
	if err != nil {
		t.Fatal(err)
	}
	arg, err := ElementArg(element)
	if err != nil {
		t.Fatal(err)
	}
	encID, _ := store.EncodeComponent("p1")
	push := cluster.Op{Class: cluster.OpStoreVerb, BucketID: testBucket, DeclVersion: testSeen, SchemaVersionSeen: testSeen, EncID: encID,
		Fields: []cluster.FieldOp{{FieldID: 4, Verb: cluster.VerbPush, Value: arg}}}
	// After the owner's admission under the pair lock has read the declaration, every later read sees another list.
	views := 0
	fx.schema.beforeView = func(n int, s *fakeSchema) {
		views++
		if views > 2 { // the first read is the outer admission, the second the owner's own under the pair lock
			s.decl.Aggregates = machine.AggregateTable{Sets: []machine.AggregateSet{{FieldID: 4, NextID: 3, Entries: []machine.AggregateEntry{
				{ID: 1, Kind: machine.AggSum, Field: "Amount"}, {ID: 2, Kind: machine.AggCountBy, By: "Status"}}}}}
		}
	}
	if _, err := fx.w.Write(context.Background(), WriteRequest{Slot: testSlot, DBID: testDB, Op: push, RequestID: [16]byte{9, 9}, Deadline: time.Second}); err != nil {
		t.Fatal(err)
	}
	entries := fx.logged()
	record, err := storage.DecodeWALRecord(entries[len(entries)-1].Op, entries[len(entries)-1].Data)
	if err != nil {
		t.Fatal(err)
	}
	op, err := cluster.DecodeOp(record.Body)
	if err != nil {
		t.Fatal(err)
	}
	if len(op.Aggregates) != 1 || op.Aggregates[0].AggID != 1 {
		t.Fatalf("the record carries %+v, want exactly the admitted declaration's one aggregate", op.Aggregates)
	}
}
