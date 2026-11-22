package wire

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage/kvfault"
	"github.com/hoarddb/hoarddb-server/store"
)

// incRig is aggRig (the Ledger bucket, whose Table and Tree keep sums) plus Meter, a bucket whose
// hashStore Hits and btreeStore Daily keep none, so an element of them may hold any numbers.
func incRig(t *testing.T) *Node {
	t.Helper()
	node := aggRig(t)
	ctx := context.Background()
	fields := []Field{{Name: "Name", Type: "string"}, {Name: "Hits", Type: "hashStore"}, {Name: "Daily", Type: "btreeStore"}, {Name: "Queue", Type: "fifoStore"}}
	if _, err := node.CreateBucket(ctx, u30iDB, "Meter", fields...); err != nil {
		t.Fatal(err)
	}
	if _, err := node.Insert(ctx, u30iDB, "Meter", bson.D{{Key: "_id_", Value: "m1"}, {Key: "Name", Value: "one"}}); err != nil {
		t.Fatal(err)
	}
	return node
}

func meterTarget(field string) StoreTarget {
	return StoreTarget{Database: u30iDB, Bucket: "Meter", ID: "m1", Field: field}
}

// incTermOf is the term for one path and a Go number, in the typed form incElement takes.
func incTermOf(t *testing.T, path string, amount any) incTerm {
	t.Helper()
	raw := u63Doc(t, bson.D{{Key: "x", Value: amount}})
	value := bson.Raw(raw).Lookup("x")
	return incTerm{Path: strings.Split(path, "."), Amount: append([]byte{byte(value.Type)}, value.Value...)}
}

// sameDoc reports whether raw holds exactly want, field for field and byte for byte.
func sameDoc(t *testing.T, raw []byte, want bson.D) bool {
	t.Helper()
	return string(raw) == string(u63Doc(t, want))
}

func mustInc(t *testing.T, node *Node, req IncRequest, options ...WriteOption) Incremented {
	t.Helper()
	got, err := node.Inc(context.Background(), req, options...)
	if err != nil {
		t.Fatalf("inc %s.%s key %q: %v", req.Bucket, req.Field, req.Key, err)
	}
	return got
}

func storedElement(t *testing.T, node *Node, target StoreTarget, key string) (bson.Raw, bool) {
	t.Helper()
	got, err := node.GetField(context.Background(), StoreGetRequest{StoreTarget: target, Key: key})
	if err != nil {
		t.Fatal(err)
	}
	return bson.Raw(got.Element), got.Found
}

// TestIncElementUsesTheUpdateIncArithmetic: [GUARD: adding with Go + instead of incResult]. An int32 at
// its top widens to an int64, a double plus an int is a double, a path under a new parent creates the
// parent, and the fields a term does not name keep their bytes and their order.
func TestIncElementUsesTheUpdateIncArithmetic(t *testing.T) {
	old := u63Doc(t, bson.D{{Key: "a", Value: "keep"}, {Key: "n", Value: int32(math.MaxInt32)}, {Key: "s", Value: 1.5}, {Key: "z", Value: bson.D{{Key: "q", Value: int32(1)}}}})
	got, err := incElement(old, []incTerm{incTermOf(t, "n", int32(1)), incTermOf(t, "s", int32(1)), incTermOf(t, "d.n", int32(5)), incTermOf(t, "z.q", int32(2))})
	if err != nil {
		t.Fatal(err)
	}
	want := u63Doc(t, bson.D{{Key: "a", Value: "keep"}, {Key: "n", Value: int64(math.MaxInt32) + 1}, {Key: "s", Value: 2.5}, {Key: "z", Value: bson.D{{Key: "q", Value: int32(3)}}}, {Key: "d", Value: bson.D{{Key: "n", Value: int32(5)}}}})
	if string(got) != string(want) {
		t.Fatalf("incElement = %v, want %v", bson.Raw(got), bson.Raw(want))
	}
	// the same sum update's $inc computes
	widened, err := incResult(append([]byte{0x10}, binary.LittleEndian.AppendUint32(nil, math.MaxInt32)...), append([]byte{0x10}, binary.LittleEndian.AppendUint32(nil, 1)...))
	if err != nil || widened[0] != 0x12 {
		t.Fatalf("incResult = %x, %v", widened, err)
	}
	if value := bson.Raw(got).Lookup("n"); value.Type != 0x12 || string(append([]byte{byte(value.Type)}, value.Value...)) != string(widened) {
		t.Fatalf("n is %v, want update's widened result %x", value, widened)
	}
	absent, err := incElement(nil, []incTerm{incTermOf(t, "t", int32(1)), incTermOf(t, "s", 30.5)})
	if err != nil || string(absent) != string(u63Doc(t, bson.D{{Key: "t", Value: int32(1)}, {Key: "s", Value: 30.5}})) {
		t.Fatalf("incElement over an absent element = %v, %v", bson.Raw(absent), err)
	}
}

// TestIncCreatesAnAbsentElement: [GUARD: refusing an absent key]. The first inc creates the element with
// the increments as its values and the store counts it once; the second adds to it and the count stays 1.
func TestIncCreatesAnAbsentElement(t *testing.T) {
	node := incRig(t)
	ctx := context.Background()
	mustInc(t, node, IncRequest{StoreTarget: meterTarget("Hits"), Key: "2026-11-05T14", Increments: bson.D{{Key: "t", Value: int32(1)}, {Key: "s", Value: int32(30)}}})
	element, found := storedElement(t, node, meterTarget("Hits"), "2026-11-05T14")
	if !found || !sameDoc(t, element, bson.D{{Key: "t", Value: int32(1)}, {Key: "s", Value: int32(30)}}) {
		t.Fatalf("created element = %v (found %v)", element, found)
	}
	mustInc(t, node, IncRequest{StoreTarget: meterTarget("Hits"), Key: "2026-11-05T14", Increments: bson.D{{Key: "t", Value: int32(1)}, {Key: "s", Value: int32(-5)}}})
	element, _ = storedElement(t, node, meterTarget("Hits"), "2026-11-05T14")
	if !sameDoc(t, element, bson.D{{Key: "t", Value: int32(2)}, {Key: "s", Value: int32(25)}}) {
		t.Fatalf("incremented element = %v", element)
	}
	if count, err := node.CountField(ctx, meterTarget("Hits")); err != nil || count != 1 {
		t.Fatalf("Hits holds %d elements (%v), want 1: the second inc must not count a new element", count, err)
	}
	mustInc(t, node, IncRequest{StoreTarget: meterTarget("Hits"), Key: "other", Increments: bson.D{{Key: "t", Value: int32(1)}}})
	if count, _ := node.CountField(ctx, meterTarget("Hits")); count != 2 {
		t.Fatalf("Hits holds %d elements, want 2", count)
	}
}

// TestIncCreatesABtreeElementKeyedByADatetimeString: [GUARD: restricting the key, ruling Q1].
func TestIncCreatesABtreeElementKeyedByADatetimeString(t *testing.T) {
	node := incRig(t)
	for _, key := range []string{"2026-11-05T14:00:00Z", "2026-11-05T13:00:00Z", "2026-11-05T14:00:00Z"} {
		mustInc(t, node, IncRequest{StoreTarget: meterTarget("Daily"), Key: key, Increments: bson.D{{Key: "n", Value: int32(1)}}})
	}
	page, err := node.ListField(context.Background(), FieldListRequest{StoreTarget: meterTarget("Daily"), Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Keys) != 2 || page.Keys[0] != "2026-11-05T13:00:00Z" || page.Keys[1] != "2026-11-05T14:00:00Z" {
		t.Fatalf("the btreeStore keys = %v, want the two hours in key order", page.Keys)
	}
	if element, _ := storedElement(t, node, meterTarget("Daily"), "2026-11-05T14:00:00Z"); !sameDoc(t, element, bson.D{{Key: "n", Value: int32(2)}}) {
		t.Fatalf("the hour incremented twice holds %v", element)
	}
}

// refusedBeforeAPosition runs req and requires an INVALID_ARGUMENT that took no log position and left key as it was.
func refusedBeforeAPosition(t *testing.T, node *Node, req IncRequest, wantText string) {
	t.Helper()
	before, _ := storedElement(t, node, req.StoreTarget, req.Key)
	logged := node.wal.LastSequence()
	_, err := node.Inc(context.Background(), req)
	var refused *cluster.Error
	if !errors.As(err, &refused) || refused.Code != cluster.CodeInvalidArgument {
		t.Fatalf("inc = %v, want an INVALID_ARGUMENT refusal", err)
	}
	if !strings.Contains(err.Error(), wantText) {
		t.Fatalf("the refusal reads %q, want it to contain %q", err.Error(), wantText)
	}
	if node.wal.LastSequence() != logged {
		t.Fatalf("the refused inc took a log position (%d records, was %d): after the append the pair would fence itself", node.wal.LastSequence(), logged)
	}
	after, _ := storedElement(t, node, req.StoreTarget, req.Key)
	if string(before) != string(after) {
		t.Fatalf("a refused inc changed the element: %v then %v", before, after)
	}
}

// TestIncOnAStringPathIsRefusedBeforeAPosition: [GUARD: refusing after the append].
func TestIncOnAStringPathIsRefusedBeforeAPosition(t *testing.T) {
	node := incRig(t)
	if err := node.Put(context.Background(), PutRequest{StoreTarget: meterTarget("Hits"), Key: "k", Element: bson.D{{Key: "n", Value: "seven"}, {Key: "m", Value: int32(1)}}}); err != nil {
		t.Fatal(err)
	}
	refusedBeforeAPosition(t, node, IncRequest{StoreTarget: meterTarget("Hits"), Key: "k", Increments: bson.D{{Key: "m", Value: int32(1)}, {Key: "n", Value: int32(1)}}}, `"n"`)
	// a path under a number is a number where a document is wanted
	refusedBeforeAPosition(t, node, IncRequest{StoreTarget: meterTarget("Hits"), Key: "k", Increments: bson.D{{Key: "m.deeper", Value: int32(1)}}}, `"m.deeper"`)
}

// TestIncOverflowIsRefused: [GUARD: wrapping at MaxInt64]. The refusal is W-I2 and the text names the key and the path.
func TestIncOverflowIsRefused(t *testing.T) {
	node := incRig(t)
	if err := node.Put(context.Background(), PutRequest{StoreTarget: meterTarget("Hits"), Key: "k", Element: bson.D{{Key: "n", Value: int64(math.MaxInt64)}}}); err != nil {
		t.Fatal(err)
	}
	refusedBeforeAPosition(t, node, IncRequest{StoreTarget: meterTarget("Hits"), Key: "k", Increments: bson.D{{Key: "n", Value: int32(1)}}}, "overflow")
}

// TestIncKeepsDeclaredAggregatesExact: [GUARD: skipping remove-old and add-new, so a sum drifts by the old
// value per inc]. Table keeps sum Amount; Tree keeps sum Amount by Currency and count by Currency.
func TestIncKeepsDeclaredAggregatesExact(t *testing.T) {
	node := incRig(t)
	ctx := context.Background()
	tableSum := aggID(t, node, "Table", 1, "")
	for _, step := range []struct {
		key    string
		amount int64
		want   int64
	}{{"a", 5, 5}, {"a", 7, 12}, {"b", 3, 15}, {"a", -2, 13}} {
		mustInc(t, node, IncRequest{StoreTarget: aggTarget("Table"), Key: step.key, Increments: bson.D{{Key: "Amount", Value: step.amount}}})
		if sum := aggPlainSum(t, node, "Table", tableSum); sum.Whole != step.want {
			t.Fatalf("after inc %q by %d the sum is %d, want %d", step.key, step.amount, sum.Whole, step.want)
		}
	}
	if err := node.Put(ctx, PutRequest{StoreTarget: aggTarget("Tree"), Key: "e1", Element: expense(int64(10), "USD")}); err != nil {
		t.Fatal(err)
	}
	mustInc(t, node, IncRequest{StoreTarget: aggTarget("Tree"), Key: "e1", Increments: bson.D{{Key: "Amount", Value: int64(5)}}})
	byID := aggID(t, node, "Tree", 2, "Currency")
	raw, ok := aggRead(t, node, "Tree", byID, "USD")
	if !ok {
		t.Fatal("the sum by USD vanished")
	}
	if sum, _, n, err := store.DecodeSumByValue(nil, raw); err != nil || sum.Whole != 15 || n != 1 {
		t.Fatalf("sum by USD = %d over %d (%v), want 15 over 1: the old element was not subtracted", sum.Whole, n, err)
	}
}

// TestIncThatLeavesASummedPathMissingIsRefused: [GUARD: creating {n: 1} in a store that sums Amount].
func TestIncThatLeavesASummedPathMissingIsRefused(t *testing.T) {
	node := incRig(t)
	refusedBeforeAPosition(t, node, IncRequest{StoreTarget: aggTarget("Table"), Key: "k", Increments: bson.D{{Key: "n", Value: int32(1)}}}, "AGGREGATE_FIELD")
	if sum := aggPlainSum(t, node, "Table", aggID(t, node, "Table", 1, "")); sum.Whole != 0 {
		t.Fatalf("the refused inc left a sum of %d", sum.Whole)
	}
}

// TestIncIsSerialisedUnderThePairSection: [GUARD: reading the element outside the section]. 100 goroutines
// add 1 to one element: the final value is 100, on the pipelined RF 1 path and on the locked path an RF 2
// database takes.
func TestIncIsSerialisedUnderThePairSection(t *testing.T) {
	for _, path := range []string{"pipelined", "locked"} {
		t.Run(path, func(t *testing.T) {
			node := incRig(t)
			if path == "locked" {
				node.writer.cfg.Advancer = lockedOnlyAdvancer{node.writer.cfg.Advancer}
			}
			ref, err := node.bucket(context.Background(), u30iDB, "Meter")
			if err != nil {
				t.Fatal(err)
			}
			dbRow, _ := node.writer.cfg.Topology.Database(ref.dbID)
			if _, pipelined := node.writer.pipelinedAdvancer(dbRow); pipelined != (path == "pipelined") {
				t.Fatalf("the %s run took pipelined = %v", path, pipelined)
			}
			var wait sync.WaitGroup
			for range 100 {
				wait.Add(1)
				go func() {
					defer wait.Done()
					if _, err := node.Inc(context.Background(), IncRequest{StoreTarget: meterTarget("Hits"), Key: "c", Increments: bson.D{{Key: "n", Value: int32(1)}}}); err != nil {
						t.Errorf("inc: %v", err)
					}
				}()
			}
			wait.Wait()
			element, found := storedElement(t, node, meterTarget("Hits"), "c")
			if !found || !sameDoc(t, element, bson.D{{Key: "n", Value: int32(100)}}) {
				t.Fatalf("after 100 concurrent incs the element is %v (found %v), want n = 100", element, found)
			}
			if count, _ := node.CountField(context.Background(), meterTarget("Hits")); count != 1 {
				t.Fatalf("Hits holds %d elements, want 1", count)
			}
		})
	}
}

// lockedOnlyAdvancer hides PipelineAdvancer, so every write takes the locked path an RF 2 database takes.
type lockedOnlyAdvancer struct{ PairAdvancer }

// TestIncWithReturnAfter: the element as applied rides in the answer, and a retry with the same request id
// is deduped and returns that same element without adding again.
func TestIncWithReturnAfter(t *testing.T) {
	node := incRig(t)
	id := [16]byte{9, 9, 1}
	first := mustInc(t, node, IncRequest{StoreTarget: meterTarget("Hits"), Key: "k", Increments: bson.D{{Key: "n", Value: int32(4)}}, ReturnAfter: true}, WithRequestID(id))
	if !sameDoc(t, first.Element, bson.D{{Key: "n", Value: int32(4)}}) {
		t.Fatalf("return after = %v", bson.Raw(first.Element))
	}
	retry := mustInc(t, node, IncRequest{StoreTarget: meterTarget("Hits"), Key: "k", Increments: bson.D{{Key: "n", Value: int32(4)}}, ReturnAfter: true}, WithRequestID(id))
	if !retry.Deduped || string(retry.Element) != string(first.Element) {
		t.Fatalf("retry = %+v, want the deduped answer carrying %v", retry, bson.Raw(first.Element))
	}
	plain := mustInc(t, node, IncRequest{StoreTarget: meterTarget("Hits"), Key: "k", Increments: bson.D{{Key: "n", Value: int32(1)}}})
	if len(plain.Element) != 0 {
		t.Fatalf("an inc without return answered %v", bson.Raw(plain.Element))
	}
	if element, _ := storedElement(t, node, meterTarget("Hits"), "k"); !sameDoc(t, element, bson.D{{Key: "n", Value: int32(5)}}) {
		t.Fatalf("after the deduped retry and one more inc the element is %v, want n = 5", element)
	}
}

// TestCopyAndOwnerAgreeAfterInc: [GUARD: a copy that needs a result the record does not carry]. The owner's
// log, applied to a second store from the records alone, leaves every key under the stores, the aggregate
// keys included, as the owner holds them.
func TestCopyAndOwnerAgreeAfterInc(t *testing.T) {
	node := incRig(t)
	ctx := context.Background()
	for i := range 6 {
		mustInc(t, node, IncRequest{StoreTarget: aggTarget("Table"), Key: fmt.Sprintf("k%d", i%3), Increments: bson.D{{Key: "Amount", Value: int64(i + 1)}}})
	}
	if err := node.Put(ctx, PutRequest{StoreTarget: aggTarget("Tree"), Key: "e1", Element: expense(int64(10), "USD")}); err != nil {
		t.Fatal(err)
	}
	mustInc(t, node, IncRequest{StoreTarget: aggTarget("Tree"), Key: "e1", Increments: bson.D{{Key: "Amount", Value: int32(5)}}, ReturnAfter: true})
	if err := node.Put(ctx, PutRequest{StoreTarget: aggTarget("Tree"), Key: "e2", Element: expense(int64(1), "EUR")}); err != nil {
		t.Fatal(err)
	}
	mustInc(t, node, IncRequest{StoreTarget: aggTarget("Tree"), Key: "e2", Increments: bson.D{{Key: "Amount", Value: 2.5}}})
	engines := fixedEngines{}
	ref, err := node.bucket(ctx, u30iDB, "Ledger")
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"Table", "Tree"} {
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
	for i, record := range loggedOps(t, node) {
		if record.Op.BucketID != ref.entry.Decl.BucketID || record.Op.Class != cluster.OpStoreVerb {
			continue // the copy needs the stores' records only
		}
		// fillApplied is the one apply a copy's commit calls (BUG-0749): the keys, the inc's recorded
		// result and the dedupe entry in one batch. The ordinal counts the record, as the pair's does.
		record.Ordinal = uint64(i + 1)
		batch := copyKV.NewBatch()
		if _, err := fillApplied(batch, applier, record, machine.WindowConfig{Window: 8192, MaxResult: maxResultBytes}); err != nil {
			t.Fatalf("a copy applying position %s: %v", record.Pos, err)
		}
		if err := batch.Commit(false); err != nil {
			t.Fatal(err)
		}
	}
	incs := 0
	for _, record := range loggedOps(t, node) {
		if record.Op.Class == cluster.OpStoreVerb && record.Op.Fields[0].Verb == cluster.VerbInc {
			incs++
		}
	}
	if incs != 8 {
		t.Fatalf("the log holds %d inc records, want 8", incs)
	}
	for _, field := range []string{"Table", "Tree"} {
		prefix := aggPrefix(t, node, field)
		owner, copied := storeRange(t, node.kv, prefix), storeRange(t, copyKV, prefix)
		aggregateKeys := 0
		for key := range owner {
			if key[len(prefix)] == store.InnerAggregate {
				aggregateKeys++
			}
		}
		if aggregateKeys == 0 || len(owner) != len(copied) {
			t.Fatalf("%s: owner holds %d keys (%d aggregate), copy %d", field, len(owner), aggregateKeys, len(copied))
		}
		for key, want := range owner {
			if got, ok := copied[key]; !ok || got != want {
				t.Fatalf("%s: key %x differs: owner %x, copy %x (present %v)", field, key, want, got, ok)
			}
		}
	}
}

// TestIncRefusesAnElementPutWouldRefuse: [GUARD: inc growing an element past what put takes]. Many incs of
// new paths could each be accepted and the element grow without bound, so the element an inc would write
// is held to the bound a put of the same element has (maxVerbArgBytes), refused before a position.
func TestIncRefusesAnElementPutWouldRefuse(t *testing.T) {
	node := incRig(t)
	nearly := strings.Repeat("x", maxVerbArgBytes-300)
	if err := node.Put(context.Background(), PutRequest{StoreTarget: meterTarget("Hits"), Key: "k", Element: bson.D{{Key: "pad", Value: nearly}}}); err != nil {
		t.Fatalf("setup: put of an element just under the bound: %v", err)
	}
	refusedBeforeAPosition(t, node, IncRequest{StoreTarget: meterTarget("Hits"), Key: "k", Increments: bson.D{{Key: strings.Repeat("p", 400), Value: int32(1)}}}, fmt.Sprintf("%d bytes", maxVerbArgBytes))
}

// TestIncReturnAfterTooBigForADedupeEntryGetsIncAdvice: [GUARD: pop and take advice on an inc]. An inc that
// asks for an element larger than a dedupe entry holds is refused before a position, and the text says what
// an inc can do (drop the return and read it with get), not to claim and ack.
func TestIncReturnAfterTooBigForADedupeEntryGetsIncAdvice(t *testing.T) {
	node := incRig(t)
	big := strings.Repeat("x", maxResultBytes+100)
	if err := node.Put(context.Background(), PutRequest{StoreTarget: meterTarget("Hits"), Key: "k", Element: bson.D{{Key: "n", Value: int32(1)}, {Key: "pad", Value: big}}}); err != nil {
		t.Fatal(err)
	}
	logged := node.wal.LastSequence()
	_, err := node.Inc(context.Background(), IncRequest{StoreTarget: meterTarget("Hits"), Key: "k", Increments: bson.D{{Key: "n", Value: int32(1)}}, ReturnAfter: true})
	u30iCode(t, err, cluster.CodeResultTooLarge)
	for _, want := range []string{"nothing was written", "run inc without return and read it with get"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal reads %q, want it to contain %q", err, want)
		}
	}
	if strings.Contains(err.Error(), "claim") || strings.Contains(err.Error(), "pop or take") {
		t.Errorf("the refusal gives pop and take advice for an inc: %q", err)
	}
	if node.wal.LastSequence() != logged {
		t.Fatal("the refused inc took a log position")
	}
}

// TestIncCountsItsOutcomesNodeWide: [GUARD: spec section 7, hoarddb_store_inc_total{outcome}]. The counts are
// node-scoped atomics, one per outcome, never per bucket.
func TestIncCountsItsOutcomesNodeWide(t *testing.T) {
	node := incRig(t)
	id := [16]byte{7, 7, 1}
	req := IncRequest{StoreTarget: meterTarget("Hits"), Key: "k", Increments: bson.D{{Key: "n", Value: int32(1)}}}
	mustInc(t, node, req, WithRequestID(id))
	mustInc(t, node, req, WithRequestID(id)) // a retry: deduped
	mustInc(t, node, req)
	if err := node.Put(context.Background(), PutRequest{StoreTarget: meterTarget("Hits"), Key: "s", Element: bson.D{{Key: "n", Value: "seven"}}}); err != nil {
		t.Fatal(err)
	}
	refusedBeforeAPosition(t, node, IncRequest{StoreTarget: meterTarget("Hits"), Key: "s", Increments: bson.D{{Key: "n", Value: int32(1)}}}, `"n"`)
	if got := [3]uint64{node.Incs.Applied.Load(), node.Incs.Deduped.Load(), node.Incs.Refused.Load()}; got != [3]uint64{2, 1, 1} {
		t.Fatalf("inc outcomes applied, deduped, refused = %v, want [2 1 1]", got)
	}
}
