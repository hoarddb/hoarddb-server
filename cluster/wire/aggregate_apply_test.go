package wire

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// aggRig is a node with bucket Ledger { Name, Fifo, Stack, Ranked, Tree, Table } where every
// store sums Amount and the tree and the fifo also group by Currency, and one document l1.
func aggRig(t *testing.T) *Node {
	t.Helper()
	node := u30iOpen(t, t.TempDir())
	ctx := context.Background()
	if _, err := node.CreateDatabase(ctx, u30iDB); err != nil {
		t.Fatal(err)
	}
	sums := []AggregateDecl{{Kind: "sum", Field: "Amount"}}
	both := []AggregateDecl{{Kind: "sum", Field: "Amount"}, {Kind: "sum by", Field: "Amount", By: "Currency"}, {Kind: "count by", By: "Currency"}}
	var decls []AggregateDecl
	for store, list := range map[string][]AggregateDecl{"Fifo": both, "Stack": sums, "Ranked": sums, "Tree": both, "Table": sums} {
		for _, decl := range list {
			decl.Store = store
			decls = append(decls, decl)
		}
	}
	fields := []Field{{Name: "Name", Type: "string"}, {Name: "Fifo", Type: "fifoStore"}, {Name: "Stack", Type: "lifoStore"},
		{Name: "Ranked", Type: "heapStore"}, {Name: "Tree", Type: "btreeStore"}, {Name: "Table", Type: "hashStore"}}
	if _, err := node.CreateBucketWithAggregates(ctx, u30iDB, "Ledger", fields, decls); err != nil {
		t.Fatal(err)
	}
	if _, err := node.Insert(ctx, u30iDB, "Ledger", bson.D{{Key: "_id_", Value: "l1"}, {Key: "Name", Value: "one"}}); err != nil {
		t.Fatal(err)
	}
	return node
}

func aggTarget(field string) StoreTarget {
	return StoreTarget{Database: u30iDB, Bucket: "Ledger", ID: "l1", Field: field}
}

func expense(amount any, currency string) bson.D {
	return bson.D{{Key: "Amount", Value: amount}, {Key: "Currency", Value: currency}}
}

// aggPrefix is the store prefix of one field of l1.
func aggPrefix(t *testing.T, node *Node, field string) []byte {
	t.Helper()
	ref, err := node.bucket(context.Background(), u30iDB, "Ledger")
	if err != nil {
		t.Fatal(err)
	}
	fieldID, err := ref.storeField(field)
	if err != nil {
		t.Fatal(err)
	}
	encID, err := store.EncodeComponent("l1")
	if err != nil {
		t.Fatal(err)
	}
	return store.StorePrefix(ref.slot(encID), ref.dbID, ref.entry.Decl.BucketID, encID, fieldID)
}

// aggRead returns the raw value under an aggregate key and whether it exists. A store's
// aggregate ids are minted in declaration order per field, so 1 is its plain sum.
func aggRead(t *testing.T, node *Node, field string, id uint8, by any) ([]byte, bool) {
	t.Helper()
	var byKey []byte
	if by != nil {
		var err error
		if byKey, err = store.EncodeComponent(by); err != nil {
			t.Fatal(err)
		}
	}
	snapshot := node.kv.NewSnapshot()
	defer snapshot.Discard()
	raw, err := snapshot.Get(store.AggregateKey(aggPrefix(t, node, field), id, byKey))
	if errors.Is(err, storage.ErrKeyNotFound) {
		return nil, false
	}
	if err != nil {
		t.Fatal(err)
	}
	return raw, true
}

// aggPlainSum reads the plain sum of a field, 0 when the store never held an element.
func aggPlainSum(t *testing.T, node *Node, field string, id uint8) store.AggNumber {
	t.Helper()
	raw, ok := aggRead(t, node, field, id, nil)
	if !ok {
		return store.AggNumber{}
	}
	sum, _, err := store.DecodeSumValue(nil, raw)
	if err != nil {
		t.Fatal(err)
	}
	return sum
}

// aggIDs finds the id of an aggregate by walking the declaration, so a test never assumes
// the map order of aggRig's declarations.
func aggID(t *testing.T, node *Node, field string, kind uint8, by string) uint8 {
	t.Helper()
	ref, err := node.bucket(context.Background(), u30iDB, "Ledger")
	if err != nil {
		t.Fatal(err)
	}
	fieldID, err := ref.storeField(field)
	if err != nil {
		t.Fatal(err)
	}
	set, ok := ref.entry.Decl.Aggregates.Set(fieldID)
	if !ok {
		t.Fatalf("field %s keeps no aggregates", field)
	}
	for _, entry := range set.Entries {
		if entry.Kind == kind && entry.By == by {
			return entry.ID
		}
	}
	t.Fatalf("field %s has no aggregate of kind %d by %q", field, kind, by)
	return 0
}

func TestSumIsMaintainedByPushAndPop(t *testing.T) {
	node := aggRig(t)
	ctx := context.Background()
	for _, field := range []string{"Fifo", "Stack", "Ranked"} {
		id := aggID(t, node, field, 1, "")
		for i, amount := range []int64{10, 20, 30, 40} {
			req := PushRequest{StoreTarget: aggTarget(field), Element: expense(amount, "USD")}
			if field == "Ranked" {
				req.EntryID, req.Priority = string(rune('a'+i)), int64(i+1)
			}
			if err := node.Push(ctx, req); err != nil {
				t.Fatalf("%s push: %v", field, err)
			}
		}
		if sum := aggPlainSum(t, node, field, id); sum.Whole != 100 {
			t.Fatalf("%s sum after four pushes = %+v, want 100", field, sum)
		}
		popped, err := node.Pop(ctx, PopRequest{StoreTarget: aggTarget(field)})
		if err != nil || !popped.Found {
			t.Fatalf("%s pop: %+v, %v", field, popped, err)
		}
		removed := bson.Raw(popped.Element).Lookup("Amount").Int64()
		if sum := aggPlainSum(t, node, field, id); sum.Whole != 100-removed {
			t.Fatalf("%s sum after a pop of %d = %+v, want %d", field, removed, sum, 100-removed)
		}
		many, err := node.PopN(ctx, PopNRequest{StoreTarget: aggTarget(field), Count: 10})
		if err != nil || len(many.Elements) != 3 {
			t.Fatalf("%s pop(10): %d elements, %v; want 3", field, len(many.Elements), err)
		}
		if sum := aggPlainSum(t, node, field, id); sum.Whole != 0 {
			t.Fatalf("%s sum of an emptied store = %+v, want 0", field, sum)
		}
	}
}

func TestPutReplaceSubtractsOldAddsNew(t *testing.T) {
	node := aggRig(t)
	ctx := context.Background()
	sumID := aggID(t, node, "Tree", 1, "")
	put := func(key string, amount int64, currency string) {
		t.Helper()
		if err := node.Put(ctx, PutRequest{StoreTarget: aggTarget("Tree"), Key: key, Element: expense(amount, currency)}); err != nil {
			t.Fatal(err)
		}
	}
	put("e1", 100, "USD")
	put("e2", 50, "EUR")
	put("e1", 70, "USD") // a replaced expense counts once, at its new amount
	if sum := aggPlainSum(t, node, "Tree", sumID); sum.Whole != 120 {
		t.Fatalf("sum = %+v after a replace, want 70 + 50", sum)
	}
	put("e2", 50, "USD") // the same amount moves to another currency
	byID := aggID(t, node, "Tree", 2, "Currency")
	if _, ok := aggRead(t, node, "Tree", byID, "EUR"); ok {
		t.Fatal("EUR still holds a key after its only element moved to USD")
	}
	raw, ok := aggRead(t, node, "Tree", byID, "USD")
	if !ok {
		t.Fatal("no USD key")
	}
	if sum, _, n, err := store.DecodeSumByValue(nil, raw); err != nil || sum.Whole != 120 || n != 2 {
		t.Fatalf("USD = %+v over %d, %v; want 120 over 2", sum, n, err)
	}
}

func TestHeapPushOverAnExistingEntryReplacesItsContribution(t *testing.T) {
	node := aggRig(t)
	id := aggID(t, node, "Ranked", 1, "")
	for range 2 {
		if err := node.Push(context.Background(), PushRequest{StoreTarget: aggTarget("Ranked"), Element: expense(int64(9), "USD"), EntryID: "job", Priority: 3}); err != nil {
			t.Fatal(err)
		}
	}
	if sum := aggPlainSum(t, node, "Ranked", id); sum.Whole != 9 {
		t.Fatalf("sum = %+v after the same entry was pushed twice, want 9", sum)
	}
}

func TestSumByAndCountByCreateAndDeleteKeysPerDistinctValue(t *testing.T) {
	node := aggRig(t)
	ctx := context.Background()
	for i, currency := range []string{"USD", "EUR", "USD", "GBP", "USD"} {
		if err := node.Put(ctx, PutRequest{StoreTarget: aggTarget("Tree"), Key: string(rune('a' + i)), Element: expense(int64(10), currency)}); err != nil {
			t.Fatal(err)
		}
	}
	byID, countID := aggID(t, node, "Tree", 2, "Currency"), aggID(t, node, "Tree", 3, "Currency")
	for currency, want := range map[string]uint64{"USD": 3, "EUR": 1, "GBP": 1} {
		raw, ok := aggRead(t, node, "Tree", countID, currency)
		if !ok {
			t.Fatalf("no count key for %s", currency)
		}
		if n, err := store.DecodeCountByValue(nil, raw); err != nil || n != want {
			t.Fatalf("count by %s = %d, %v; want %d", currency, n, err, want)
		}
	}
	if _, err := node.Take(ctx, TakeRequest{StoreTarget: aggTarget("Tree"), Key: "b"}); err != nil {
		t.Fatal(err)
	}
	for _, id := range []uint8{byID, countID} {
		if _, ok := aggRead(t, node, "Tree", id, "EUR"); ok {
			t.Fatalf("aggregate %d still has an EUR key after the only EUR element was taken", id)
		}
	}
}

func TestAggregateWriteIsRefusedBeforeAPositionWhenTheFieldIsMissing(t *testing.T) {
	node := aggRig(t)
	ctx := context.Background()
	before := u63Position(node)
	err := node.Push(ctx, PushRequest{StoreTarget: aggTarget("Stack"), Element: bson.D{{Key: "Other", Value: int64(1)}}})
	var refusal *cluster.Error
	if !errors.As(err, &refusal) || refusal.Code != cluster.CodeInvalidArgument || !strings.Contains(refusal.Message, `"Amount"`) {
		t.Fatalf("push without the summed field = %v, want an INVALID_ARGUMENT naming Amount", err)
	}
	if after := u63Position(node); after != before {
		t.Fatalf("the refused write took a position: %v then %v", before, after)
	}
	err = node.Push(ctx, PushRequest{StoreTarget: aggTarget("Fifo"), Element: bson.D{{Key: "Amount", Value: "12"}, {Key: "Currency", Value: "USD"}}})
	if !errors.As(err, &refusal) || refusal.Code != cluster.CodeInvalidArgument {
		t.Fatalf("push of a string amount = %v, want INVALID_ARGUMENT", err)
	}
}

func TestIntegerOverflowRefusesTheWrite(t *testing.T) {
	node := aggRig(t)
	ctx := context.Background()
	big := int64(1) << 62
	if err := node.Push(ctx, PushRequest{StoreTarget: aggTarget("Stack"), Element: expense(big, "USD")}); err != nil {
		t.Fatal(err)
	}
	err := node.Push(ctx, PushRequest{StoreTarget: aggTarget("Stack"), Element: expense(int64(1), "USD")})
	var refusal *cluster.Error
	if !errors.As(err, &refusal) || refusal.Code != cluster.CodeInvalidArgument || !strings.Contains(refusal.Message, "integer minor units") {
		t.Fatalf("push past 2^62 = %v, want a refusal that names integer minor units", err)
	}
	if sum := aggPlainSum(t, node, "Stack", aggID(t, node, "Stack", 1, "")); sum.Whole != big {
		t.Fatalf("sum = %+v after a refused push, want unchanged", sum)
	}
}

// pairFlags is the replay flags of the pair that holds l1.
func pairFlags(node *Node) uint8 {
	snapshot := node.pairs.Snapshot(node.slotOf(u30iDB, "Ledger", "l1"), 1)
	return uint8(snapshot.Flags & flagReplayNeeded)
}

// TestAnAckNeverOverflowsAnAdmittedSum: a bound on the sum alone admits -B, -B, +B, +B, +B, +B (B = 2^61) whose
// first two elements, acked, leave 2^63; the absolute-sum bound refuses the fifth push, so every
// admitted store acks cleanly and the pair is never fenced. Ints and doubles.
func TestAnAckNeverOverflowsAnAdmittedSum(t *testing.T) {
	for name, value := range map[string]func(int64) any{"int": func(v int64) any { return v }, "double": func(v int64) any { return float64(v) }} {
		t.Run(name, func(t *testing.T) {
			node := aggRig(t)
			ctx := context.Background()
			half := int64(1) << 60
			pushed := 0
			for _, amount := range []int64{-half, -half, half, half, half, half} {
				err := node.Push(ctx, PushRequest{StoreTarget: aggTarget("Fifo"), Element: expense(value(amount), "USD")})
				if err != nil {
					break
				}
				pushed++
			}
			if pushed != 4 {
				t.Fatalf("%d elements admitted, want 4: the fifth would take the absolute values past 2^62", pushed)
			}
			lease, err := node.Claim(ctx, ClaimRequest{StoreTarget: aggTarget("Fifo"), Count: 2})
			if err != nil || len(lease.Elements) != 2 {
				t.Fatalf("claim: %d, %v", len(lease.Elements), err)
			}
			if err := node.Ack(ctx, AckRequest{StoreTarget: aggTarget("Fifo"), Lease: lease.ID}); err != nil {
				t.Fatalf("ack of two admitted elements: %v", err)
			}
			if flags := pairFlags(node); flags != 0 {
				t.Fatalf("the pair needs a replay after the ack (flags %#x)", flags)
			}
			if err := node.Push(ctx, PushRequest{StoreTarget: aggTarget("Stack"), Element: expense(value(1), "USD")}); err != nil {
				t.Fatalf("a push to another store of the same document after the ack: %v", err)
			}
		})
	}
}

// TestTheHeadOfAQueueIsAlwaysPoppable: whatever was admitted, the head can be popped.
func TestTheHeadOfAQueueIsAlwaysPoppable(t *testing.T) {
	node := aggRig(t)
	ctx := context.Background()
	half := int64(1) << 60
	for _, amount := range []int64{-half, half, half} {
		if err := node.Push(ctx, PushRequest{StoreTarget: aggTarget("Fifo"), Element: expense(amount, "USD")}); err != nil {
			t.Fatal(err)
		}
	}
	for range 3 {
		if popped, err := node.Pop(ctx, PopRequest{StoreTarget: aggTarget("Fifo")}); err != nil || !popped.Found {
			t.Fatalf("pop: %+v, %v", popped, err)
		}
	}
}

func TestDoubleSwitchesTheTagForGood(t *testing.T) {
	node := aggRig(t)
	ctx := context.Background()
	id := aggID(t, node, "Stack", 1, "")
	push := func(amount any) {
		t.Helper()
		if err := node.Push(ctx, PushRequest{StoreTarget: aggTarget("Stack"), Element: expense(amount, "USD")}); err != nil {
			t.Fatal(err)
		}
	}
	push(int64(2))
	if sum := aggPlainSum(t, node, "Stack", id); sum.Float || sum.Whole != 2 {
		t.Fatalf("integer sum = %+v", sum)
	}
	push(0.5)
	if sum := aggPlainSum(t, node, "Stack", id); !sum.Float || sum.Real != 2.5 {
		t.Fatalf("after a double = %+v, want the float 2.5", sum)
	}
	if _, err := node.Pop(ctx, PopRequest{StoreTarget: aggTarget("Stack")}); err != nil { // removes the 0.5
		t.Fatal(err)
	}
	if sum := aggPlainSum(t, node, "Stack", id); !sum.Float || math.Abs(sum.Real-2) > 1e-9 {
		t.Fatalf("after the double left = %+v, want the float 2 (the tag never narrows)", sum)
	}
}

func TestAggregateBatchBoundIsRefusedNamingTheBytes(t *testing.T) {
	node := aggRig(t)
	ctx := context.Background()
	// The estimate is what a write is charged; a bound under it must refuse before a position.
	applier := NewDocumentApplier(node.kv, nil)
	ref := store.FieldRef{Owner: []byte("owner"), FieldID: 3, Engine: store.FieldFIFO}
	text := applier.aggregateBatchRefusal(ref, int(node.kv.MaxBatchBytes()))
	if !strings.Contains(text, "AGGREGATE_BATCH") || !strings.Contains(text, "bytes") {
		t.Fatalf("an impossible batch = %q, want AGGREGATE_BATCH naming the bytes", text)
	}
	if text := applier.aggregateBatchRefusal(ref, 3); text != "" {
		t.Fatalf("three keys refused: %q", text)
	}
	_ = ctx
}

func TestAckSubtractsTheLeasedElements(t *testing.T) {
	node := aggRig(t)
	ctx := context.Background()
	id := aggID(t, node, "Fifo", 1, "")
	for _, amount := range []int64{10, 20, 30, 40} {
		if err := node.Push(ctx, PushRequest{StoreTarget: aggTarget("Fifo"), Element: expense(amount, "USD")}); err != nil {
			t.Fatal(err)
		}
	}
	lease, err := node.Claim(ctx, ClaimRequest{StoreTarget: aggTarget("Fifo"), Count: 3})
	if err != nil || len(lease.Elements) != 3 {
		t.Fatalf("claim: %d elements, %v", len(lease.Elements), err)
	}
	if sum := aggPlainSum(t, node, "Fifo", id); sum.Whole != 100 {
		t.Fatalf("sum with 3 elements leased = %+v, want the leased elements still summed (100)", sum)
	}
	if err := node.Ack(ctx, AckRequest{StoreTarget: aggTarget("Fifo"), Lease: lease.ID}); err != nil {
		t.Fatal(err)
	}
	if sum := aggPlainSum(t, node, "Fifo", id); sum.Whole != 40 {
		t.Fatalf("sum after the ack = %+v, want 40 (an acked message stays summed for ever otherwise)", sum)
	}
	byCount := aggID(t, node, "Fifo", 3, "Currency")
	raw, ok := aggRead(t, node, "Fifo", byCount, "USD")
	if n, err := store.DecodeCountByValue(nil, raw); !ok || err != nil || n != 1 {
		t.Fatalf("count by USD after the ack = %d, %v, %v; want 1", n, ok, err)
	}
}

// TestADeadLetteredEntryLeavesTheSum: a message released MaxDeliveries times goes to the dead
// list without an ack, and it must leave the sum there.
func TestADeadLetteredEntryLeavesTheSum(t *testing.T) {
	node := aggRig(t)
	ctx := context.Background()
	id := aggID(t, node, "Fifo", 1, "")
	for _, amount := range []int64{7, 5} {
		if err := node.Push(ctx, PushRequest{StoreTarget: aggTarget("Fifo"), Element: expense(amount, "USD")}); err != nil {
			t.Fatal(err)
		}
	}
	for range store.MaxDeliveries {
		lease, err := node.Claim(ctx, ClaimRequest{StoreTarget: aggTarget("Fifo"), Count: 1})
		if err != nil || lease.ID == ([16]byte{}) {
			t.Fatalf("claim: %+v, %v", lease, err)
		}
		if _, err := node.Release(ctx, ReleaseRequest{StoreTarget: aggTarget("Fifo"), Lease: lease.ID}); err != nil {
			t.Fatal(err)
		}
	}
	if sum := aggPlainSum(t, node, "Fifo", id); sum.Whole != 5 {
		t.Fatalf("sum after the first message died = %+v, want 5 (7 left the sum when it was dead-lettered)", sum)
	}
}

// declAggregateSets is the number of aggregate sets in the stored declaration of Ledger.
func declAggregateSets(t *testing.T, node *Node) int {
	t.Helper()
	dbID, err := node.database(u30iDB)
	if err != nil {
		t.Fatal(err)
	}
	decl, _, found, err := node.fetcher.Read(context.Background(), dbID, "Ledger")
	if err != nil || !found {
		t.Fatalf("reading the declaration: found %v, %v", found, err)
	}
	return len(decl.Aggregates.Sets)
}

// TestAlterDroppingAStoreWithAggregatesPrunesItsSet: dropping a store field that keeps aggregates
// must not brick the bucket: a second alter and a drop bucket both succeed afterwards, and the dead
// field's set is gone from field 12 once the alter that dropped it has been folded.
func TestAlterDroppingAStoreWithAggregatesPrunesItsSet(t *testing.T) {
	node := aggRig(t)
	ctx := context.Background()
	if declAggregateSets(t, node) != 5 {
		t.Fatalf("the rig keeps %d sets, want 5", declAggregateSets(t, node))
	}
	if _, err := node.AlterBucket(ctx, u30iDB, "Ledger", AlterChange{Drop: []string{"Fifo"}}); err != nil {
		t.Fatalf("dropping a store with aggregates: %v", err)
	}
	if err := node.Push(ctx, PushRequest{StoreTarget: aggTarget("Stack"), Element: expense(int64(1), "USD")}); err != nil {
		t.Fatalf("a push to a store that kept its aggregates: %v", err)
	}
	if _, err := node.AlterBucket(ctx, u30iDB, "Ledger", AlterChange{Drop: []string{"Stack"}}); err != nil {
		t.Fatalf("a second alter after dropping a store with aggregates: %v", err)
	}
	if got := declAggregateSets(t, node); got != 4 {
		t.Fatalf("%d aggregate sets remain after two store drops, want 4 (the fold prunes the first dropped field's set; the second is still pending)", got)
	}
	if err := node.DropBucket(ctx, u30iDB, "Ledger"); err != nil {
		t.Fatalf("drop bucket after dropping stores with aggregates: %v", err)
	}
}

// TestTheRetireSweepTakesTheRetiredFieldsAggregateKeys: the sweep judges a key by the field id after
// the document id, so the 0x06 keys of a dropped store go with its elements and a live store's stay.
func TestTheRetireSweepTakesTheRetiredFieldsAggregateKeys(t *testing.T) {
	node := aggRig(t)
	if err := node.Push(context.Background(), PushRequest{StoreTarget: aggTarget("Fifo"), Element: expense(int64(3), "USD")}); err != nil {
		t.Fatal(err)
	}
	ref, err := node.bucket(context.Background(), u30iDB, "Ledger")
	if err != nil {
		t.Fatal(err)
	}
	fifoID, _ := ref.storeField("Fifo")
	stackID, _ := ref.storeField("Stack")
	prefix := aggPrefix(t, node, "Fifo")
	byKey, _ := store.EncodeComponent("USD")
	for _, key := range [][]byte{store.AggregateKey(prefix, 1, nil), store.AggregateKey(prefix, 2, byKey)} {
		if got, err := keyCarriesField(key, fifoID); err != nil || !got {
			t.Fatalf("an aggregate key of Fifo is not taken by a sweep of Fifo: %v, %v", got, err)
		}
		if got, err := keyCarriesField(key, stackID); err != nil || got {
			t.Fatalf("an aggregate key of Fifo is taken by a sweep of Stack: %v, %v", got, err)
		}
	}
}

// TestALongByValueIsRefusedBeforeAPosition: a by value whose key would pass the store's key limit
// (65000 bytes) used to pass the dry run, which only reads the key, and fail the apply after the append,
// fencing the pair for good. A by value is capped at 1024 encoded bytes and refused at check 6a.
func TestALongByValueIsRefusedBeforeAPosition(t *testing.T) {
	node := aggRig(t)
	ctx := context.Background()
	before := u63Position(node)
	for _, size := range []int{1025, 70000} {
		err := node.Push(ctx, PushRequest{StoreTarget: aggTarget("Fifo"), Element: expense(int64(1), strings.Repeat("x", size))})
		var refusal *cluster.Error
		if !errors.As(err, &refusal) || refusal.Code != cluster.CodeInvalidArgument || !strings.Contains(refusal.Message, "AGGREGATE_FIELD") || !strings.Contains(refusal.Message, "1024") {
			t.Fatalf("a by value of %d bytes = %v, want an AGGREGATE_FIELD refusal naming the 1024-byte cap", size, err)
		}
	}
	if after := u63Position(node); after != before {
		t.Fatal("a refused write took a position")
	}
	if flags := pairFlags(node); flags != 0 {
		t.Fatalf("the pair needs a replay (flags %#x)", flags)
	}
	if err := node.Push(ctx, PushRequest{StoreTarget: aggTarget("Fifo"), Element: expense(int64(1), strings.Repeat("x", 1000))}); err != nil {
		t.Fatalf("a by value of 1000 bytes: %v", err)
	}
	if err := node.Push(ctx, PushRequest{StoreTarget: aggTarget("Stack"), Element: expense(int64(1), "USD")}); err != nil {
		t.Fatalf("a later write to another store: %v", err)
	}
}

// TestAnAggregateKeyIsNeverLongerThanItsCharge: the batch charge is an upper bound. The longest by key
// the cap admits, with the longest value, costs no more than aggregateKeyBytes charges for it.
func TestAnAggregateKeyIsNeverLongerThanItsCharge(t *testing.T) {
	prefix := store.StorePrefix(1, 1, 1, bytes.Repeat([]byte{1}, 1030), 4)
	by := bytes.Repeat([]byte{0x41}, store.AggregateByCap)
	key := store.AggregateKey(prefix, 255, by)
	value := store.EncodeSumByValue(store.AggNumber{Whole: 1}, store.AggNumber{Whole: 1}, 1)
	if cost := int64(len(key) + len(value) + batchEntryOverhead); cost > aggregateKeyBytes(len(prefix)) {
		t.Fatalf("a longest aggregate key costs %d bytes in a batch and is charged %d", cost, aggregateKeyBytes(len(prefix)))
	}
}

// TestAnAckOfEightByAggregatesWithLongValuesIsAdmittedAndApplies: a lease of the most elements, each with
// eight by values at the cap, acks inside the batch bound the charge reserved, and leaves the pair clean.
func TestAnAckOfEightByAggregatesWithLongValuesIsAdmittedAndApplies(t *testing.T) {
	node := u30iOpen(t, t.TempDir())
	ctx := context.Background()
	if _, err := node.CreateDatabase(ctx, u30iDB); err != nil {
		t.Fatal(err)
	}
	var decls []AggregateDecl
	for i := 0; i < 8; i++ {
		decls = append(decls, AggregateDecl{Store: "Wide", Kind: "count by", By: fmt.Sprint("C", i)})
	}
	if _, err := node.CreateBucketWithAggregates(ctx, u30iDB, "Ledger", []Field{{Name: "Name", Type: "string"}, {Name: "Wide", Type: "fifoStore"}}, decls); err != nil {
		t.Fatal(err)
	}
	if _, err := node.Insert(ctx, u30iDB, "Ledger", bson.D{{Key: "_id_", Value: "l1"}, {Key: "Name", Value: "one"}}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		element := bson.D{}
		for c := 0; c < 8; c++ {
			element = append(element, bson.E{Key: fmt.Sprint("C", c), Value: fmt.Sprintf("%0*d", 1000, i)})
		}
		if err := node.Push(ctx, PushRequest{StoreTarget: aggTarget("Wide"), Element: element}); err != nil {
			t.Fatalf("push %d: %v", i, err)
		}
	}
	lease, err := node.Claim(ctx, ClaimRequest{StoreTarget: aggTarget("Wide"), Count: 100})
	if err != nil || len(lease.Elements) == 0 {
		t.Fatalf("claim: %d elements, %v", len(lease.Elements), err)
	}
	if err := node.Ack(ctx, AckRequest{StoreTarget: aggTarget("Wide"), Lease: lease.ID}); err != nil {
		t.Fatalf("ack of %d elements: %v", len(lease.Elements), err)
	}
	if flags := pairFlags(node); flags != 0 {
		t.Fatalf("the pair needs a replay after the ack (flags %#x)", flags)
	}
}

// TestAReplaceAtTheBoundIsAdmittedAndApplies: a put or heap push that replaces an element of -2^62 with
// one of +2^62 is inside the bound only if the old element leaves before the new one enters, in the dry run
// and in the apply alike. Either order wrong refuses the write at 6a or, worse, fails the apply after the append.
func TestAReplaceAtTheBoundIsAdmittedAndApplies(t *testing.T) {
	node := aggRig(t)
	ctx := context.Background()
	big := int64(1) << 62
	if err := node.Put(ctx, PutRequest{StoreTarget: aggTarget("Tree"), Key: "k", Element: expense(-big, "USD")}); err != nil {
		t.Fatal(err)
	}
	if err := node.Put(ctx, PutRequest{StoreTarget: aggTarget("Tree"), Key: "k", Element: expense(big, "USD")}); err != nil {
		t.Fatalf("a put replacing -2^62 with 2^62: %v", err)
	}
	if sum := aggPlainSum(t, node, "Tree", aggID(t, node, "Tree", 1, "")); sum.Whole != big {
		t.Fatalf("Tree sum %d after the replace, want 2^62", sum.Whole)
	}
	push := func(amount int64) error {
		return node.Push(ctx, PushRequest{StoreTarget: aggTarget("Ranked"), Element: expense(amount, "USD"), EntryID: "e", Priority: 1})
	}
	if err := push(-big); err != nil {
		t.Fatal(err)
	}
	if err := push(big); err != nil {
		t.Fatalf("a heap push replacing -2^62 with 2^62: %v", err)
	}
	if sum := aggPlainSum(t, node, "Ranked", aggID(t, node, "Ranked", 1, "")); sum.Whole != big {
		t.Fatalf("Ranked sum %d after the replace, want 2^62", sum.Whole)
	}
	if flags := pairFlags(node); flags != 0 {
		t.Fatalf("the pair needs a replay (flags %#x)", flags)
	}
}

// TestDescribeBetweenDropAndFoldListsNoDroppedStore: after an alter drops a store with aggregates and
// before the next alter folds the table, describe lists the aggregates of the stores that remain and none
// under an empty store name.
func TestDescribeBetweenDropAndFoldListsNoDroppedStore(t *testing.T) {
	node := aggRig(t)
	ctx := context.Background()
	if _, err := node.AlterBucket(ctx, u30iDB, "Ledger", AlterChange{Drop: []string{"Fifo"}}); err != nil {
		t.Fatal(err)
	}
	ref, err := node.bucket(ctx, u30iDB, "Ledger")
	if err != nil {
		t.Fatal(err)
	}
	listed := replyAggregates(ref.table, ref.entry.Decl.Aggregates)
	if len(listed) == 0 {
		t.Fatal("describe lists no aggregate at all")
	}
	for _, aggregate := range listed {
		if aggregate.Store == "" || aggregate.Store == "Fifo" {
			t.Fatalf("describe lists %+v for a store that is no longer declared", aggregate)
		}
	}
}

// TestAnAggregateRefusalNamesTheElement: the refusal of an element an aggregate cannot sum says which element
// it was, not only the path, so the writer can find the document that carries it.
func TestAnAggregateRefusalNamesTheElement(t *testing.T) {
	node := aggRig(t)
	ctx := context.Background()
	err := node.Push(ctx, PushRequest{StoreTarget: aggTarget("Stack"), Element: bson.D{{Key: "Name", Value: "no amount here"}}})
	var refusal *cluster.Error
	if !errors.As(err, &refusal) || !strings.Contains(refusal.Message, "AGGREGATE_FIELD") {
		t.Fatalf("a push with no Amount = %v, want an AGGREGATE_FIELD refusal", err)
	}
	if !strings.Contains(refusal.Message, "no amount here") || !strings.Contains(refusal.Message, `"Amount"`) {
		t.Fatalf("the refusal %q does not name the element and the Amount path", refusal.Message)
	}
}

// TestAPopThatWouldOverflowTheSumIsRefusedBeforeAPosition is spec error 8's sequence. A push that would take the
// absolute values past 2^62 is refused before a position, so the pop that follows finds the store as it was: the
// bound keeps every removal inside the range, and the pop of the admitted element returns it and zeroes the sum.
func TestAPopThatWouldOverflowTheSumIsRefusedBeforeAPosition(t *testing.T) {
	node := aggRig(t)
	ctx := context.Background()
	big := int64(1) << 62
	if err := node.Push(ctx, PushRequest{StoreTarget: aggTarget("Stack"), Element: expense(big, "USD")}); err != nil {
		t.Fatal(err)
	}
	before := u63Position(node)
	err := node.Push(ctx, PushRequest{StoreTarget: aggTarget("Stack"), Element: expense(int64(-5), "USD")})
	var refusal *cluster.Error
	if !errors.As(err, &refusal) || !strings.Contains(refusal.Message, "AGGREGATE_RANGE") {
		t.Fatalf("a push that takes the absolute values past 2^62 = %v, want AGGREGATE_RANGE", err)
	}
	if after := u63Position(node); after != before {
		t.Fatalf("a refused push took a position: %d then %d", before, after)
	}
	popped, err := node.Pop(ctx, PopRequest{StoreTarget: aggTarget("Stack")})
	if err != nil || !popped.Found {
		t.Fatalf("pop after a refused push: %+v, %v", popped, err)
	}
	if sum := aggPlainSum(t, node, "Stack", aggID(t, node, "Stack", 1, "")); sum.Whole != 0 {
		t.Fatalf("sum after the pop = %+v, want 0", sum)
	}
}
