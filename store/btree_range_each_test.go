package store

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"testing"

	"github.com/hoarddb/hoarddb-server/storage/kvfault"
)

// TestBTreeRangeEachMatchesRange holds RangeEach to Range: over random bands
// of both key types and limits 0, 1 and n, the callback walk visits exactly
// Range's entries in the same order, and an error from the callback comes back
// as itself, unwrapped, so a caller's message reaches the client unchanged
// (query-pipelining spec §9 hazard 8).
func TestBTreeRangeEachMatchesRange(t *testing.T) {
	const keysPerType = 60
	bt := NewBTreeStore(kvfault.New(), 1, 1, 1, new(OwedApplies))
	for i := 0; i < keysPerType; i++ {
		stringKey := fmt.Sprintf("k%03d", i*3)
		if err := bt.Put(ApplyInfo{}, stringKey, map[string]interface{}{"n": i}, uint64(2*i+1)); err != nil {
			t.Fatalf("Put(%s): %v", stringKey, err)
		}
		numberKey := int64(i*3 - 90)
		if err := bt.Put(ApplyInfo{}, numberKey, map[string]interface{}{"n": i}, uint64(2*i+2)); err != nil {
			t.Fatalf("Put(%d): %v", numberKey, err)
		}
	}

	random := rand.New(rand.NewSource(20260926))
	randomBound := func(numeric bool) interface{} {
		if random.Intn(4) == 0 {
			return nil
		}
		if numeric {
			return int64(random.Intn(keysPerType*3+20) - 100)
		}
		return fmt.Sprintf("k%03d", random.Intn(keysPerType*3+10))
	}
	for trial := 0; trial < 300; trial++ {
		numeric := trial%2 == 1
		low, high := randomBound(numeric), randomBound(numeric)
		limit := []int{0, 1, 1 + random.Intn(keysPerType)}[trial%3]

		want, rangeErr := bt.Range(low, high, limit)
		var got []BTreeEntry
		eachErr := bt.RangeEach(low, high, limit, func(key, value []byte) error {
			got = append(got, BTreeEntry{Key: bytes.Clone(key), Value: bytes.Clone(value)})
			return nil
		})
		if (rangeErr == nil) != (eachErr == nil) {
			t.Fatalf("Range(%v, %v, %d) error %v, RangeEach error %v", low, high, limit, rangeErr, eachErr)
		}
		if len(got) != len(want) {
			t.Fatalf("Range(%v, %v, %d) returned %d entries, RangeEach visited %d", low, high, limit, len(want), len(got))
		}
		for i := range want {
			if !bytes.Equal(got[i].Key, want[i].Key) || !bytes.Equal(got[i].Value, want[i].Value) {
				t.Fatalf("Range(%v, %v, %d) entry %d: Range has %q, RangeEach visited %q", low, high, limit, i, want[i].Key, got[i].Key)
			}
		}
	}

	sentinel := errors.New("the visitor's own refusal")
	visited := 0
	err := bt.RangeEach(nil, nil, 0, func(_, _ []byte) error {
		visited++
		if visited == 3 {
			return sentinel
		}
		return nil
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("RangeEach returned %v, want the callback's error", err)
	}
	if err.Error() != sentinel.Error() {
		t.Fatalf("RangeEach wrapped the callback's error: %q, want %q unchanged", err.Error(), sentinel.Error())
	}
	if visited != 3 {
		t.Fatalf("RangeEach visited %d entries after the callback refused at the 3rd, want it to stop there", visited)
	}
}
