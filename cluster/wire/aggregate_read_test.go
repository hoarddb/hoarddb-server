package wire

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/store"

	"github.com/hoarddb/hoarddb-bson/bson"
)

func aggRead2(t *testing.T, node *Node, field string, read RingAggregateRead, limit int, after []byte) (AggregateAnswer, error) {
	t.Helper()
	call, err := node.fieldCall(context.Background(), aggTarget(field), "aggregate")
	if err != nil {
		t.Fatal(err)
	}
	return node.aggregateOfRead(context.Background(), call, read, limit, after)
}

type byRow struct {
	By  any   `bson:"by"`
	Sum any   `bson:"sum"`
	N   int64 `bson:"n"`
}

func decodeValues(t *testing.T, answer AggregateAnswer) []byRow {
	t.Helper()
	var doc struct {
		Values []byRow `bson:"values"`
	}
	if err := bson.Unmarshal(answer.Document, &doc); err != nil {
		t.Fatal(err)
	}
	return doc.Values
}

func TestSumReadsOneKeyAndAnEmptyStoreIsZero(t *testing.T) {
	node := aggRig(t)
	sum := RingAggregateRead{Kind: AggregateReadSum, Field: "Amount"}
	answer, err := aggRead2(t, node, "Stack", sum, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	var doc struct{ Sum int64 }
	if err := bson.Unmarshal(answer.Document, &doc); err != nil || doc.Sum != 0 {
		t.Fatalf("an empty store's sum = %+v, %v; want 0", doc, err)
	}
	for _, amount := range []int64{3, 4} {
		if err := node.Push(context.Background(), PushRequest{StoreTarget: aggTarget("Stack"), Element: expense(amount, "USD")}); err != nil {
			t.Fatal(err)
		}
	}
	answer, _ = aggRead2(t, node, "Stack", sum, 0, nil)
	if err := bson.Unmarshal(answer.Document, &doc); err != nil || doc.Sum != 7 {
		t.Fatalf("sum = %+v, %v; want 7", doc, err)
	}
}

func TestSumByAndCountByAreInKeyOrderAndPaged(t *testing.T) {
	node := aggRig(t)
	ctx := context.Background()
	for i, currency := range []string{"USD", "EUR", "GBP", "CHF", "JPY"} {
		if err := node.Put(ctx, PutRequest{StoreTarget: aggTarget("Tree"), Key: fmt.Sprint("k", i), Element: expense(int64(10+i), currency)}); err != nil {
			t.Fatal(err)
		}
	}
	read := RingAggregateRead{Kind: AggregateReadSumBy, Field: "Amount", By: "Currency"}
	var seen []string
	var after []byte
	for page := 0; page < 10; page++ {
		answer, err := aggRead2(t, node, "Tree", read, 2, after)
		if err != nil {
			t.Fatal(err)
		}
		for _, row := range decodeValues(t, answer) {
			seen = append(seen, row.By.(string))
		}
		if !answer.More {
			break
		}
		after = answer.Cursor
	}
	if want := []string{"CHF", "EUR", "GBP", "JPY", "USD"}; !reflect.DeepEqual(seen, want) {
		t.Fatalf("paged by values = %v, want key order %v with no repeat or skip", seen, want)
	}
	answer, err := aggRead2(t, node, "Tree", RingAggregateRead{Kind: AggregateReadCountBy, By: "Currency"}, 0, nil)
	if err != nil || len(decodeValues(t, answer)) != 5 {
		t.Fatalf("count by = %v, %v; want 5 rows", decodeValues(t, answer), err)
	}
}

func TestAggregateReadOfAnUndeclaredAggregateNamesTheDeclaration(t *testing.T) {
	node := aggRig(t)
	_, err := aggRead2(t, node, "Stack", RingAggregateRead{Kind: AggregateReadSum, Field: "Amout"}, 0, nil)
	var refusal *cluster.Error
	if !errors.As(err, &refusal) || refusal.Code != cluster.CodeInvalidArgument ||
		!strings.Contains(refusal.Message, "AGGREGATE_NOT_DECLARED") || !strings.Contains(refusal.Message, "sum Amount") {
		t.Fatalf("a typo'd sum = %v, want AGGREGATE_NOT_DECLARED listing the store's aggregates", err)
	}
	if node.Aggregates.Refusals.Load() != 1 {
		t.Fatalf("refusals counted %d, want 1", node.Aggregates.Refusals.Load())
	}
}

// TestAggregateReadAgreesWithAFullScanOfTheElements is the differential gate: random pushes,
// puts, pops and takes, then every aggregate equals what a client computes from the elements.
func TestAggregateReadAgreesWithAFullScanOfTheElements(t *testing.T) {
	node := aggRig(t)
	ctx := context.Background()
	random := rand.New(rand.NewSource(7))
	currencies := []string{"USD", "EUR", "GBP", "CHF"}
	for i := 0; i < 400; i++ {
		amount, currency := int64(random.Intn(2001)-1000), currencies[random.Intn(len(currencies))]
		switch random.Intn(5) {
		case 0, 1:
			if err := node.Push(ctx, PushRequest{StoreTarget: aggTarget("Fifo"), Element: expense(amount, currency)}); err != nil {
				t.Fatal(err)
			}
		case 2:
			if _, err := node.PopN(ctx, PopNRequest{StoreTarget: aggTarget("Fifo"), Count: 1 + random.Intn(3)}); err != nil {
				t.Fatal(err)
			}
		case 3:
			if err := node.Put(ctx, PutRequest{StoreTarget: aggTarget("Tree"), Key: fmt.Sprint("k", random.Intn(20)), Element: expense(amount, currency)}); err != nil {
				t.Fatal(err)
			}
		default:
			if _, err := node.Take(ctx, TakeRequest{StoreTarget: aggTarget("Tree"), Key: fmt.Sprint("k", random.Intn(20))}); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, field := range []string{"Fifo", "Tree"} {
		ref, err := node.bucket(ctx, u30iDB, "Ledger")
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
		snapshot := node.kv.NewSnapshot()
		prefix := aggPrefix(t, node, field)
		fref := store.FieldRef{Owner: prefix[:len(prefix)-2], FieldID: fieldID, Engine: engine}
		var total int64
		bySum, byCount := map[string]int64{}, map[string]int64{}
		entries, _, err := store.FieldList(snapshot, fref, store.FieldListRequest{Limit: 1 << 20, MaxBytes: 1 << 30})
		snapshot.Discard()
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			raw := bson.Raw(entry.Value)
			amount, currency := raw.Lookup("Amount").Int64(), raw.Lookup("Currency").StringValue()
			total += amount
			bySum[currency] += amount
			byCount[currency]++
		}
		answer, err := aggRead2(t, node, field, RingAggregateRead{Kind: AggregateReadSum, Field: "Amount"}, 0, nil)
		if err != nil {
			t.Fatal(err)
		}
		var doc struct{ Sum int64 }
		if err := bson.Unmarshal(answer.Document, &doc); err != nil || doc.Sum != total {
			t.Fatalf("%s: sum %d (%v), a scan of %d elements says %d", field, doc.Sum, err, len(entries), total)
		}
		answer, err = aggRead2(t, node, field, RingAggregateRead{Kind: AggregateReadSumBy, Field: "Amount", By: "Currency"}, 0, nil)
		if err != nil {
			t.Fatal(err)
		}
		rows := decodeValues(t, answer)
		if len(rows) != len(bySum) {
			t.Fatalf("%s: %d by values, a scan finds %d", field, len(rows), len(bySum))
		}
		for _, row := range rows {
			currency := row.By.(string)
			if row.Sum.(int64) != bySum[currency] || row.N != byCount[currency] {
				t.Fatalf("%s %s: sum %v over %d, a scan says %d over %d", field, currency, row.Sum, row.N, bySum[currency], byCount[currency])
			}
		}
	}
}

func TestDescribeListsEveryAggregateAndItsValue(t *testing.T) {
	node := aggRig(t)
	if err := node.Put(context.Background(), PutRequest{StoreTarget: aggTarget("Tree"), Key: "a", Element: expense(int64(5), "USD")}); err != nil {
		t.Fatal(err)
	}
	answer, err := aggRead2(t, node, "Tree", RingAggregateRead{Kind: AggregateReadDescribe}, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	text := bson.Raw(answer.Document).String()
	for _, want := range []string{"sum Amount", "sum Amount by Currency", "count by Currency", "ready"} {
		if !strings.Contains(text, want) {
			t.Fatalf("describe %s lacks %q", text, want)
		}
	}
}

// TestAnAggregateReadRoundTripsTheRingCodec: class 0x8B, its form and the reply member.
func TestAnAggregateReadRoundTripsTheRingCodec(t *testing.T) {
	encID, _ := store.EncodeComponent("l1")
	read := RingRead{Class: RingClassAggregate, BucketID: 9, EncID: encID, FieldID: 3, Limit: 50, After: []byte{1, 2},
		Aggregate: &RingAggregateRead{Kind: AggregateReadSumBy, Field: "Amount", By: "Currency"}}
	raw, err := EncodeRingKeyed(RingKeyedRequest{Read: &read})
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeRingKeyed(raw)
	if err != nil || !reflect.DeepEqual(*got.Read, read) {
		t.Fatalf("decoded %+v, %v; want %+v", got.Read, err, read)
	}
	bad := read
	bad.Aggregate = &RingAggregateRead{Kind: AggregateReadSum}
	if _, err := EncodeRingKeyed(RingKeyedRequest{Read: &bad}); err == nil {
		t.Fatal("a sum with no field was encoded")
	}
	other := read
	other.Class = RingClassStoreCount
	if _, err := EncodeRingKeyed(RingKeyedRequest{Read: &other}); err == nil {
		t.Fatal("an aggregate member on a count was encoded")
	}
	reply := RingReply{Found: ringTrue(), Aggregate: []byte{5, 0, 0, 0, 0}, More: true, Cursor: []byte{1}}
	encoded, err := EncodeRingReply(reply)
	if err != nil {
		t.Fatal(err)
	}
	if back, err := DecodeRingReply(encoded); err != nil || !reflect.DeepEqual(back.Aggregate, reply.Aggregate) {
		t.Fatalf("reply round trip = %+v, %v", back, err)
	}
}

// TestACorruptStoredSumSurfacesAnErrorNamingTheAggregate: a sum key whose bytes are not a sum must fail the
// read with the aggregate's declared text in the message, so the operator knows which declaration to inspect.
func TestACorruptStoredSumSurfacesAnErrorNamingTheAggregate(t *testing.T) {
	node := aggRig(t)
	ctx := context.Background()
	if err := node.Push(ctx, PushRequest{StoreTarget: aggTarget("Stack"), Element: expense(int64(3), "USD")}); err != nil {
		t.Fatal(err)
	}
	batch := node.kv.NewBatch()
	if err := batch.Set(store.AggregateKey(aggPrefix(t, node, "Stack"), 1, nil), []byte{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	if err := batch.Commit(true); err != nil {
		t.Fatal(err)
	}
	_, err := aggRead2(t, node, "Stack", RingAggregateRead{Kind: AggregateReadSum, Field: "Amount"}, 0, nil)
	if err == nil {
		t.Fatal("a corrupt stored sum read without an error")
	}
	if !strings.Contains(err.Error(), `"sum Amount"`) {
		t.Fatalf("the error %q does not name the aggregate \"sum Amount\"", err)
	}
}

// TestAggregateReadsAreCountedPerKind: the node counts each aggregate read under its kind, so the per-kind
// counter the spec names can say which reads a node serves. Fifo declares all three kinds.
func TestAggregateReadsAreCountedPerKind(t *testing.T) {
	node := aggRig(t)
	reads := []RingAggregateRead{
		{Kind: AggregateReadSum, Field: "Amount"},
		{Kind: AggregateReadSumBy, Field: "Amount", By: "Currency"},
		{Kind: AggregateReadCountBy, By: "Currency"},
		{Kind: AggregateReadCountBy, By: "Currency"},
	}
	for _, read := range reads {
		if _, err := aggRead2(t, node, "Fifo", read, 0, nil); err != nil {
			t.Fatalf("read %+v: %v", read, err)
		}
	}
	want := [4]uint64{0, 1, 1, 2}
	for kind, count := range want {
		if got := node.Aggregates.ReadsByKind[kind].Load(); got != count {
			t.Errorf("reads of kind %d counted %d, want %d", kind, got, count)
		}
	}
	if got := node.Aggregates.Reads(); got != 4 {
		t.Errorf("total reads %d, want 4", got)
	}
}
