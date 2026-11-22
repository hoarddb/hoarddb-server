package store

import (
	"bytes"
	"testing"
)

// TestAnAggregateKeyIsNotAnElement pins the reader rule: an aggregate key sits
// under a field store at inner 0x06, and parseFieldKey and FieldValueIsDocument
// must skip it, not decode it as an element. Without that, a dump or describe
// of any bucket that holds an aggregate key fails.
func TestAnAggregateKeyIsNotAnElement(t *testing.T) {
	owner, err := EncodeComponent("d1")
	if err != nil {
		t.Fatal(err)
	}
	layout := FieldLayout{5: FieldBTree}
	rest := append(append([]byte(nil), owner...), 0x00, 0x05, InnerAggregate, 0x01)
	_, isElement, err := parseFieldKey(rest, layout)
	if err != nil || isElement {
		t.Fatalf("parseFieldKey = isElement %v, err %v; want a key that is not an element and no error", isElement, err)
	}
	isDocument, err := FieldValueIsDocument(rest, layout)
	if err != nil || isDocument {
		t.Fatalf("FieldValueIsDocument = %v, %v; want false, nil", isDocument, err)
	}
}

// TestAggregateKeysSortByByValue pins that a by value is an order-preserving
// component: 9 sorts before 10, a numeric by and its float twin are one key,
// and AggregateRange covers one aggregate's keys and no other aggregate's.
func TestAggregateKeysSortByByValue(t *testing.T) {
	prefix := []byte("store-prefix")
	keyFor := func(aggID uint8, by interface{}) []byte {
		t.Helper()
		encoded, err := EncodeComponent(by)
		if err != nil {
			t.Fatal(err)
		}
		return AggregateKey(prefix, aggID, encoded)
	}
	nine, ten := keyFor(1, int64(9)), keyFor(1, int64(10))
	if bytes.Compare(nine, ten) >= 0 {
		t.Fatalf("key for by 9 sorts at or after key for by 10: %x vs %x", nine, ten)
	}
	if !bytes.Equal(keyFor(1, int64(5)), keyFor(1, float64(5))) {
		t.Fatalf("int64 5 and float64 5 are two keys: %x vs %x", keyFor(1, int64(5)), keyFor(1, float64(5)))
	}
	rng := AggregateRange(prefix, 1)
	for _, key := range [][]byte{AggregateKey(prefix, 1, nil), nine, ten} {
		if bytes.Compare(key, rng.Lower) < 0 || bytes.Compare(key, rng.Upper) >= 0 {
			t.Fatalf("key %x is outside AggregateRange(prefix, 1) [%x, %x)", key, rng.Lower, rng.Upper)
		}
	}
	other := keyFor(2, int64(9))
	if bytes.Compare(other, rng.Lower) >= 0 && bytes.Compare(other, rng.Upper) < 0 {
		t.Fatalf("aggregate 2's key %x is inside AggregateRange(prefix, 1)", other)
	}
}
