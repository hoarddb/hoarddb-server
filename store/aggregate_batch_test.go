package store

import (
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/storage/kvfault"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"
)

var aggTestPrefix = []byte("store-prefix")

func aggTestSpecs() []AggregateSpec {
	return []AggregateSpec{
		{ID: 1, Kind: 1, Field: []string{"Amount"}},
		{ID: 2, Kind: 2, Field: []string{"Amount"}, By: []string{"Currency"}},
		{ID: 3, Kind: 3, By: []string{"Currency"}},
	}
}

func aggExpense(t *testing.T, amount any, currency any) []byte {
	t.Helper()
	raw, err := bson.Marshal(bson.D{{Key: "Amount", Value: amount}, {Key: "Currency", Value: currency}})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// aggApply runs fn over a batch against kv and commits it.
func aggApply(t *testing.T, kv storage.KV, specs []AggregateSpec, fn func(*AggregateBatch)) {
	t.Helper()
	snap := kv.NewSnapshot()
	defer snap.Discard()
	batch := kv.NewBatch()
	agg := NewAggregateBatch(snap, batch, aggTestPrefix, specs)
	fn(agg)
	if err := agg.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := batch.Commit(false); err != nil {
		t.Fatal(err)
	}
}

func aggSum(t *testing.T, kv storage.KV, id uint8, by []byte) (AggNumber, uint64, bool) {
	t.Helper()
	snap := kv.NewSnapshot()
	defer snap.Discard()
	key := AggregateKey(aggTestPrefix, id, by)
	raw, err := snap.Get(key)
	if errors.Is(err, storage.ErrKeyNotFound) {
		return AggNumber{}, 0, false
	}
	if err != nil {
		t.Fatal(err)
	}
	if by == nil {
		sum, _, err := DecodeSumValue(key, raw)
		if err != nil {
			t.Fatal(err)
		}
		return sum, 0, true
	}
	if id == 3 {
		n, err := DecodeCountByValue(key, raw)
		if err != nil {
			t.Fatal(err)
		}
		return AggNumber{}, n, true
	}
	sum, _, n, err := DecodeSumByValue(key, raw)
	if err != nil {
		t.Fatal(err)
	}
	return sum, n, true
}

func aggBy(t *testing.T, value any) []byte {
	t.Helper()
	encoded, err := EncodeComponent(value)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

// TestFiveRemovalsOfOneByValueLeaveOneCorrectKey: deltas are summed in memory, because the
// snapshot never shows the batch's own earlier writes.
func TestFiveRemovalsOfOneByValueLeaveOneCorrectKey(t *testing.T) {
	kv := kvfault.New()
	aggApply(t, kv, aggTestSpecs(), func(agg *AggregateBatch) {
		for i := 0; i < 8; i++ {
			if err := agg.Add(aggExpense(t, int64(10), "USD"), 1); err != nil {
				t.Fatal(err)
			}
		}
	})
	aggApply(t, kv, aggTestSpecs(), func(agg *AggregateBatch) {
		for i := 0; i < 5; i++ {
			if err := agg.Add(aggExpense(t, int64(10), "USD"), -1); err != nil {
				t.Fatal(err)
			}
		}
	})
	usd := aggBy(t, "USD")
	if sum, _, ok := aggSum(t, kv, 1, nil); !ok || sum.Whole != 30 || sum.Float {
		t.Fatalf("plain sum = %+v, %v; want 30", sum, ok)
	}
	if sum, n, ok := aggSum(t, kv, 2, usd); !ok || sum.Whole != 30 || n != 3 {
		t.Fatalf("sum by USD = %+v n %d, %v; want 30 over 3", sum, n, ok)
	}
	if _, n, ok := aggSum(t, kv, 3, usd); !ok || n != 3 {
		t.Fatalf("count by USD = %d, %v; want 3", n, ok)
	}
}

// TestByKeyIsDeletedWhenItsCountReturnsToZero: a currency nobody holds is not listed.
func TestByKeyIsDeletedWhenItsCountReturnsToZero(t *testing.T) {
	kv := kvfault.New()
	aggApply(t, kv, aggTestSpecs(), func(agg *AggregateBatch) {
		_ = agg.Add(aggExpense(t, int64(5), "EUR"), 1)
		_ = agg.Add(aggExpense(t, int64(7), "USD"), 1)
	})
	aggApply(t, kv, aggTestSpecs(), func(agg *AggregateBatch) {
		if err := agg.Add(aggExpense(t, int64(5), "EUR"), -1); err != nil {
			t.Fatal(err)
		}
	})
	eur := aggBy(t, "EUR")
	for _, id := range []uint8{2, 3} {
		if _, _, ok := aggSum(t, kv, id, eur); ok {
			t.Fatalf("aggregate %d still holds a key for a by value with no elements", id)
		}
	}
	if sum, _, ok := aggSum(t, kv, 1, nil); !ok || sum.Whole != 7 {
		t.Fatalf("plain sum = %+v, %v; want 7", sum, ok)
	}
	aggApply(t, kv, aggTestSpecs(), func(agg *AggregateBatch) { _ = agg.Add(aggExpense(t, int64(7), "USD"), -1) })
	if sum, _, ok := aggSum(t, kv, 1, nil); !ok || sum.Whole != 0 {
		t.Fatalf("an emptied store's plain sum = %+v, %v; want a key holding 0, never deleted", sum, ok)
	}
}

// TestDecimalElementIsRefused: a decimal has no key encoding and no sum, and is refused
// with its path, not summed as zero.
func TestDecimalElementIsRefused(t *testing.T) {
	kv := kvfault.New()
	dec := primitive.NewDecimal128(0x3040000000000000, 1250)
	raw := aggExpense(t, dec, "USD")
	snap := kv.NewSnapshot()
	defer snap.Discard()
	agg := NewAggregateBatch(snap, kv.NewBatch(), aggTestPrefix, aggTestSpecs())
	var refusal *AggregateError
	if err := agg.Add(raw, 1); !errors.As(err, &refusal) || refusal.Path != "Amount" || refusal.Overflow {
		t.Fatalf("Add(decimal) = %v, want an *AggregateError on path Amount", err)
	}
	for name, element := range map[string][]byte{
		"missing field": mustRaw(t, bson.D{{Key: "Currency", Value: "USD"}}),
		"string amount": aggExpense(t, "12", "USD"),
		"missing by":    mustRaw(t, bson.D{{Key: "Amount", Value: int64(1)}}),
		"object by":     aggExpense(t, int64(1), bson.D{{Key: "x", Value: 1}}),
		"nan":           aggExpense(t, math.NaN(), "USD"),
	} {
		if err := NewAggregateBatch(snap, kv.NewBatch(), aggTestPrefix, aggTestSpecs()).Add(element, 1); !errors.As(err, &refusal) {
			t.Errorf("%s: Add = %v, want an *AggregateError", name, err)
		}
	}
}

func mustRaw(t *testing.T, doc bson.D) []byte {
	t.Helper()
	raw, err := bson.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// TestRefusedAddChangesNothing: an element whose by value is bad fails the whole Add, so the
// plain sum is not half updated.
func TestRefusedAddChangesNothing(t *testing.T) {
	kv := kvfault.New()
	aggApply(t, kv, aggTestSpecs(), func(agg *AggregateBatch) {
		_ = agg.Add(aggExpense(t, int64(5), "USD"), 1)
		if err := agg.Add(aggExpense(t, int64(100), bson.D{{Key: "x", Value: 1}}), 1); err == nil {
			t.Error("an object by value was accepted")
		}
	})
	if sum, _, _ := aggSum(t, kv, 1, nil); sum.Whole != 5 {
		t.Fatalf("plain sum = %d after a refused Add, want 5", sum.Whole)
	}
}

// TestTheAbsoluteSumBoundKeepsEveryRemovalInRange: an element past 2^62 is refused, and so is an add
// that takes the absolute values past 2^62 even when it lowers the sum; and with the shape of the counterexample
// to a bound on the sum alone (two negatives, then positives until refused; at 2^61 the old rule admitted
// them and acking the negatives left 2^63) every admitted store can lose
// its elements in any order without a refusal.
func TestTheAbsoluteSumBoundKeepsEveryRemovalInRange(t *testing.T) {
	kv := kvfault.New()
	specs := []AggregateSpec{{ID: 1, Kind: 1, Field: []string{"Amount"}}}
	half := int64(1) << 60
	var refusal *AggregateError
	snap := kv.NewSnapshot()
	if err := CheckAggregates(snap, aggTestPrefix, specs, [][]byte{aggExpense(t, 1<<62+1, "x")}, nil); !errors.As(err, &refusal) || !refusal.Overflow {
		t.Fatalf("an element past 2^62 = %v, want an overflow refusal", err)
	}
	snap.Discard()
	var admitted []int64
	for _, amount := range []int64{-half, -half, half, half, half, half} {
		snap := kv.NewSnapshot()
		err := CheckAggregates(snap, aggTestPrefix, specs, [][]byte{aggExpense(t, amount, "x")}, nil)
		snap.Discard()
		if err != nil {
			if !errors.As(err, &refusal) || !refusal.Overflow {
				t.Fatalf("add %d: %v, want an overflow refusal or success", amount, err)
			}
			break
		}
		aggApply(t, kv, specs, func(agg *AggregateBatch) { _ = agg.Add(aggExpense(t, amount, "x"), 1) })
		admitted = append(admitted, amount)
	}
	if len(admitted) != 4 {
		t.Fatalf("admitted %v: the absolute values add to 2^62 after four elements of 2^60, so the fifth must be refused", admitted)
	}
	// remove in the order that raises the sum most: the two negatives first
	for _, amount := range admitted {
		aggApply(t, kv, specs, func(agg *AggregateBatch) {
			if err := agg.Add(aggExpense(t, amount, "x"), -1); err != nil {
				t.Fatalf("removing %d: %v, a removal of an admitted element must never fail", amount, err)
			}
		})
	}
	if sum, abs, _ := aggSumAbs(t, kv); sum.Whole != 0 || abs.Whole != 0 {
		t.Fatalf("after every removal sum %+v abs %+v, want 0 and 0", sum, abs)
	}
}

func aggSumAbs(t *testing.T, kv storage.KV) (AggNumber, AggNumber, bool) {
	t.Helper()
	snap := kv.NewSnapshot()
	defer snap.Discard()
	key := AggregateKey(aggTestPrefix, 1, nil)
	raw, err := snap.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	sum, abs, err := DecodeSumValue(key, raw)
	if err != nil {
		t.Fatal(err)
	}
	return sum, abs, true
}

// TestAByValueOverTheCapIsRefusedOnEveryAdd: the cap is checked where the by value is read, so an add
// (push, put replace, heap replace) and the dry run refuse it alike, and one at the cap is taken.
func TestAByValueOverTheCapIsRefusedOnEveryAdd(t *testing.T) {
	kv := kvfault.New()
	snap := kv.NewSnapshot()
	defer snap.Discard()
	var refusal *AggregateError
	long := aggExpense(t, int64(1), strings.Repeat("c", AggregateByCap))
	if err := NewAggregateBatch(snap, nil, aggTestPrefix, aggTestSpecs()).Add(long, 1); !errors.As(err, &refusal) || refusal.Overflow {
		t.Fatalf("a by value over the cap = %v, want a non-overflow *AggregateError", err)
	}
	if err := CheckAggregates(snap, aggTestPrefix, aggTestSpecs(), [][]byte{long}, nil); !errors.As(err, &refusal) {
		t.Fatalf("the dry run = %v, want the same refusal", err)
	}
	atCap := aggExpense(t, int64(1), strings.Repeat("c", AggregateByCap-3))
	if err := NewAggregateBatch(snap, nil, aggTestPrefix, aggTestSpecs()).Add(atCap, 1); err != nil {
		t.Fatalf("a by value at the cap = %v", err)
	}
}

// TestAnIntRemovalOfAnElementThatNeverEnteredIsReported: only a double's abs is clamped at zero (rounding
// drift); for ints a negative abs means an element left that was never added, which is a broken invariant
// and fails loudly instead of letting a later removal wrap.
func TestAnIntRemovalOfAnElementThatNeverEnteredIsReported(t *testing.T) {
	kv := kvfault.New()
	snap := kv.NewSnapshot()
	defer snap.Discard()
	specs := []AggregateSpec{{ID: 1, Kind: 1, Field: []string{"Amount"}}}
	var refusal *AggregateError
	if err := NewAggregateBatch(snap, nil, aggTestPrefix, specs).Add(aggExpense(t, int64(5), "x"), -1); !errors.As(err, &refusal) || !refusal.Overflow {
		t.Fatalf("removing an int that never entered = %v, want an *AggregateError", err)
	}
	if err := NewAggregateBatch(snap, nil, aggTestPrefix, specs).Add(aggExpense(t, 0.5, "x"), -1); err != nil {
		t.Fatalf("a double removal inside rounding drift = %v, want it clamped", err)
	}
}
