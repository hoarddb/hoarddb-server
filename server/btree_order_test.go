package server

import (
	"context"
	"reflect"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"

	"github.com/hoarddb/hoarddb-server/storage/kvfault"
	"github.com/hoarddb/hoarddb-server/store"
)

// Step 10 of the shared-storage build: btree keys are spec §3.8's encoding
// (T-23). The keys here are typed, which the store takes and the server's
// verbs, whose keys are strings, do not produce; the order and refusal rules
// are the store's, so the store is driven directly.

// putNumbered writes a document {"n": n} under each key, in order.
func putNumbered(t *testing.T, bt *store.BTreeStore, keys ...interface{}) {
	t.Helper()
	for n, key := range keys {
		if err := bt.Put(store.ApplyInfo{}, key, map[string]interface{}{"n": n}, 0); err != nil {
			t.Fatalf("put %v (%T): %v", key, key, err)
		}
	}
}

// documentNumbers is each entry's "n", in the order returned.
func documentNumbers(t *testing.T, entries []store.BTreeEntry) []int32 {
	t.Helper()
	numbers := make([]int32, 0, len(entries))
	for _, entry := range entries {
		var doc struct {
			N int32 `bson:"n"`
		}
		if err := bson.Unmarshal(entry.Value, &doc); err != nil {
			t.Fatalf("decoding the entry under %q: %v", entry.Key, err)
		}
		numbers = append(numbers, doc.N)
	}
	return numbers
}

// TestBTreeKeysAreTypeDistinct is T-23(a): int64(1), "1" and true are three
// keys and three documents, not one slot.
func TestBTreeKeysAreTypeDistinct(t *testing.T) {
	bt := store.NewBTreeStore(kvfault.New(), 1, 1, 1, new(store.OwedApplies))
	putNumbered(t, bt, int64(1), "1", true)
	if n, err := bt.Len(); err != nil || n != 3 {
		t.Fatalf("Len = %d (%v), want 3: the three keys collided", n, err)
	}
	for want, key := range []interface{}{int64(1), "1", true} {
		raw, err := bt.Get(key)
		if err != nil || raw == nil {
			t.Fatalf("get %v (%T): %v, found %v", key, key, err, raw != nil)
		}
		var doc struct {
			N int32 `bson:"n"`
		}
		if err := bson.Unmarshal(raw, &doc); err != nil || int(doc.N) != want {
			t.Fatalf("get %v (%T) returned document %d, want %d", key, key, doc.N, want)
		}
	}
}

// TestBTreeNegativeFloatsAscend is T-23(b): negative floats sort below
// positive ones and in numeric order, from Range and from First and Last.
func TestBTreeNegativeFloatsAscend(t *testing.T) {
	bt := store.NewBTreeStore(kvfault.New(), 1, 1, 1, new(store.OwedApplies))
	// Written out of order: 0.5, -0.5, 0, -2.5, -1.0 are documents 0..4.
	putNumbered(t, bt, 0.5, -0.5, 0.0, -2.5, -1.0)
	entries, err := bt.Range(nil, nil, 0)
	if err != nil {
		t.Fatalf("range: %v", err)
	}
	if got, want := documentNumbers(t, entries), []int32{3, 4, 1, 2, 0}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Range returned documents %v (keys %q), want -2.5, -1.0, -0.5, 0, 0.5 = %v", got, keysOf(entries), want)
	}
	first, err := bt.First()
	if err != nil || string(first.Key) != "-2.5" {
		t.Fatalf("First = %q (%v), want -2.5", first.Key, err)
	}
	last, err := bt.Last()
	if err != nil || string(last.Key) != "0.5" {
		t.Fatalf("Last = %q (%v), want 0.5", last.Key, err)
	}
}

func keysOf(entries []store.BTreeEntry) []string {
	keys := make([]string, 0, len(entries))
	for _, entry := range entries {
		keys = append(keys, string(entry.Key))
	}
	return keys
}

// TestBTreeRangeMatchesCompareValues is T-23(c): a numeric range returns
// exactly the documents the scan filter's compareValues admits, boundary
// values included, whatever numeric spelling each key was written in.
func TestBTreeRangeMatchesCompareValues(t *testing.T) {
	keys := []interface{}{int64(-10), -2.5, int32(-1), 0.0, int64(1), 1.5, uint64(2), 3.0, int64(7)}
	bt := store.NewBTreeStore(kvfault.New(), 1, 1, 1, new(store.OwedApplies))
	putNumbered(t, bt, keys...)
	for _, bounds := range []struct{ low, high interface{} }{
		{int64(-1), 2.0}, {-2.5, int64(1)}, {int64(-100), int64(100)}, {1.5, 1.5}, {int64(4), int64(6)},
	} {
		entries, err := bt.Range(bounds.low, bounds.high, 0)
		if err != nil {
			t.Fatalf("range [%v, %v]: %v", bounds.low, bounds.high, err)
		}
		want := []int32{}
		for n, key := range keys {
			aboveLow, ok := compareValues(key, bounds.low)
			if !ok {
				t.Fatalf("compareValues cannot compare %v and %v", key, bounds.low)
			}
			belowHigh, _ := compareValues(key, bounds.high)
			if aboveLow >= 0 && belowHigh <= 0 {
				want = append(want, int32(n))
			}
		}
		if got := documentNumbers(t, entries); !reflect.DeepEqual(got, want) {
			t.Fatalf("range [%v, %v] returned documents %v, compareValues admits %v", bounds.low, bounds.high, got, want)
		}
	}
}

// TestBTreeAcceptsAnObjectIDKey: an ObjectID `_id_` is a btree key (conventions
// §5, tag 0x70). The document put under it reads back with its own value.
func TestBTreeAcceptsAnObjectIDKey(t *testing.T) {
	bt := store.NewBTreeStore(kvfault.New(), 1, 1, 1, new(store.OwedApplies))
	oid := primitive.NewObjectID()
	if err := bt.Put(store.ApplyInfo{}, oid, map[string]interface{}{"n": 1}, 0); err != nil {
		t.Fatalf("Put with an ObjectID key: %v", err)
	}
	raw, err := bt.Get(oid)
	if err != nil {
		t.Fatalf("Get with an ObjectID key: %v", err)
	}
	var doc bson.M
	if err := bson.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("decoding the stored document: %v", err)
	}
	if n, ok := doc["n"]; !ok || n != int32(1) {
		t.Fatalf("stored document = %v, want n = 1", doc)
	}
	if n, lenErr := bt.Len(); lenErr != nil || n != 1 {
		t.Fatalf("after one Put Len = %d (%v), want 1", n, lenErr)
	}
}

// TestBTreeRangeOverNumberLikeStringKeys: every key a client can
// write is a string, so a range over keys that look like numbers is asked in the
// type they are stored in, and answers the keys between the bounds in byte
// order. The bounds used to be read as numbers, which asked the question in a
// type no key has: "2".."5" found nothing and "2".. found everything.
func TestBTreeRangeOverNumberLikeStringKeys(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "nums", BucketTypeBTree)
	for _, key := range []string{"1", "2", "3", "5", "10", "20", "abc", "b"} {
		stored, err := s.InsertCtx(context.Background(), InsertRequest{Database: "app", Bucket: "nums", Key: key, Data: map[string]interface{}{"k": key}})
		if err != nil || !stored.Success {
			t.Fatalf("insert %q = %+v, %v", key, stored, err)
		}
	}

	cases := []struct {
		low, high string
		want      []string
	}{
		{"2", "5", []string{"2", "20", "3", "5"}},
		{"1", "20", []string{"1", "10", "2", "20"}},
		{"2", "", []string{"2", "20", "3", "5", "abc", "b"}},
		{"", "5", []string{"1", "10", "2", "20", "3", "5"}},
		{"abc", "b", []string{"abc", "b"}},
		{"1", "abc", []string{"1", "10", "2", "20", "3", "5", "abc"}},
	}
	for _, tc := range cases {
		answer, err := s.RangeQuery(BTreeRangeRequest{Database: "app", Bucket: "nums", Low: tc.low, High: tc.high})
		if err != nil {
			t.Errorf("range %q..%q: %v", tc.low, tc.high, err)
			continue
		}
		got := make([]string, 0, len(answer.Entries))
		for _, entry := range answer.Entries {
			got = append(got, entry.Key)
		}
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("range %q..%q = %v, want %v", tc.low, tc.high, got, tc.want)
		}
	}
}
