package store

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/storage/kvfault"
)

// resourceKV counts what a nearest walk asks of the snapshot: point reads (the fetch of the kept
// elements) in calls and bytes, and iterators opened (one per page). onIterator runs after each
// iterator is opened.
type resourceKV struct {
	storage.KV
	getBytes   atomic.Int64
	iterators  atomic.Int64
	onIterator func(opened int64)
}

func (k *resourceKV) NewSnapshot() storage.KVSnapshot {
	return resourceSnapshot{KVSnapshot: k.KV.NewSnapshot(), counter: k}
}

type resourceSnapshot struct {
	storage.KVSnapshot
	counter *resourceKV
}

func (s resourceSnapshot) Get(key []byte) ([]byte, error) {
	value, err := s.KVSnapshot.Get(key)
	s.counter.getBytes.Add(int64(len(value)))
	return value, err
}

func (s resourceSnapshot) NewIterator(opts storage.IterOptions) storage.KVIterator {
	it := s.KVSnapshot.NewIterator(opts)
	opened := s.counter.iterators.Add(1)
	if s.counter.onIterator != nil {
		s.counter.onIterator(opened)
	}
	return it
}

// bigVectorElement is an element holding a 2-dimension vector and padding bytes of filler.
func bigVectorElement(t *testing.T, padding int, values ...float32) []byte {
	t.Helper()
	out, err := bson.Marshal(bson.D{
		{Key: "Vector", Value: primitive.Binary{Subtype: 0x00, Data: float32Bytes(values...)}},
		{Key: "Pad", Value: primitive.Binary{Subtype: 0x00, Data: make([]byte, padding)}},
	})
	if err != nil {
		t.Fatalf("marshalling a large element: %v", err)
	}
	return out
}

func resourceFixture(t *testing.T, elements, padding int) (*fieldParent, *resourceKV, FieldRef, int) {
	t.Helper()
	kv := &resourceKV{KV: kvfault.New()}
	p := &fieldParent{t: t, kv: kv, hash: NewHashStore(kv, fieldTestSlot, 1, fieldTestID, new(OwedApplies))}
	ref := p.ref("u1", 1, FieldHash)
	var size int
	for i := 0; i < elements; i++ {
		element := bigVectorElement(t, padding, float32(i+1), 1)
		size = len(element)
		nearestPutElement(t, p, ref, fmt.Sprintf("k%03d", i), element)
	}
	return p, kv, ref, size
}

// TestNearestCopiesOnlyTheWinnersOut is the memory bound: the walk keeps no element bytes, and
// the k winners' values are fetched once, at the end, so the point reads total at most k elements
// however many large elements the field holds.
func TestNearestCopiesOnlyTheWinnersOut(t *testing.T) {
	const elements, k, padding = 30, 3, 1 << 20
	p, kv, ref, size := resourceFixture(t, elements, padding)
	kv.getBytes.Store(0)
	result, err := nearestRun(t, p, ref, NearestRequest{Metric: NearestDot, K: k, Field: "Vector", Vector: float32Bytes(1, 0), MaxElements: 1000})
	if err != nil {
		t.Fatalf("nearest: %v", err)
	}
	if len(result.Hits) != k {
		t.Fatalf("%d hits, want %d", len(result.Hits), k)
	}
	for i, hit := range result.Hits {
		if want := fmt.Sprintf("k%03d", elements-1-i); hit.Entry.ElementKey != want || len(hit.Entry.Value) != size {
			t.Fatalf("hit %d = key %q with %d value bytes, want %q with %d", i, hit.Entry.ElementKey, len(hit.Entry.Value), want, size)
		}
	}
	// The winners' values come from point reads; the slack is the field's small state record.
	if got := kv.getBytes.Load(); got < int64(k*size) || got > int64(k*size+4096) {
		t.Fatalf("point reads fetched %d bytes, want k*size = %d plus the state record", got, k*size)
	}
}

// TestNearestRefusesAnOversizeReplyBeforeFetchingAValue is the charge: sizes come from the walk,
// so a reply over the ceiling is refused with no kept value copied.
func TestNearestRefusesAnOversizeReplyBeforeFetchingAValue(t *testing.T) {
	const elements, k, padding = 12, 5, 1 << 20
	p, kv, ref, size := resourceFixture(t, elements, padding)
	kv.getBytes.Store(0)
	req := NearestRequest{Metric: NearestDot, K: k, Field: "Vector", Vector: float32Bytes(1, 0), MaxElements: 1000, MaxReplyBytes: 3 * size, ReplyOverhead: 8}
	result, err := nearestRun(t, p, ref, req)
	var tooBig *NearestReplyTooLargeError
	if !errors.As(err, &tooBig) || tooBig.Hits != k || tooBig.Max != 3*size || tooBig.Bytes < k*size {
		t.Fatalf("err = %v, want NearestReplyTooLargeError for %d hits over %d bytes", err, k, 3*size)
	}
	// Only the field's small state record may be read by point; no element value.
	if got := kv.getBytes.Load(); got > 4096 {
		t.Fatalf("%d value bytes were fetched before the refusal, want only the state record", got)
	}
	if len(result.Hits) != 0 {
		t.Fatalf("hits returned with a refusal: %d", len(result.Hits))
	}
	req.MaxReplyBytes = k * (size + 8 + 64)
	if _, err := nearestRun(t, p, ref, req); err != nil {
		t.Fatalf("a reply under the ceiling must pass: %v", err)
	}
}

// TestNearestStopsWhenTheContextIsCancelled: a cancel after the first page ends the walk at the
// next page boundary with the context error, and nothing partial.
func TestNearestStopsWhenTheContextIsCancelled(t *testing.T) {
	old := nearestPageEntries
	nearestPageEntries = 2
	t.Cleanup(func() { nearestPageEntries = old })
	p, kv, ref, _ := resourceFixture(t, 20, 16)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	kv.iterators.Store(0)
	kv.onIterator = func(opened int64) {
		if opened == 1 {
			cancel()
		}
	}
	snap := p.kv.NewSnapshot()
	defer snap.Discard()
	result, err := FieldNearest(ctx, snap, ref, NearestRequest{Metric: NearestDot, K: 3, Field: "Vector", Vector: float32Bytes(1, 0), MaxElements: 1000})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if got := kv.iterators.Load(); got != 1 {
		t.Fatalf("%d pages were read, want 1 (the cancel came during the first)", got)
	}
	if len(result.Hits) != 0 {
		t.Fatalf("hits returned with a cancel: %d", len(result.Hits))
	}
}

// TestNearestWalkRetainsNoElementBytes forces a collection while the walk is on its last pages, with
// k elements of 1 MiB kept: live memory must stay near one page, not grow by the kept elements.
func TestNearestWalkRetainsNoElementBytes(t *testing.T) {
	old := nearestPageEntries
	nearestPageEntries = 2
	t.Cleanup(func() { nearestPageEntries = old })
	const elements, k, padding = 40, 20, 1 << 20
	p, kv, ref, _ := resourceFixture(t, elements, padding)
	var base runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&base)
	var growth int64
	kv.onIterator = func(opened int64) {
		if opened != elements/2-1 { // the second to last page: every kept slot has been filled
			return
		}
		var now runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&now)
		growth = int64(now.HeapAlloc) - int64(base.HeapAlloc)
	}
	// A rising score keeps replacing the root, the worst case for retention.
	if _, err := nearestRun(t, p, ref, NearestRequest{Metric: NearestDot, K: k, Field: "Vector", Vector: float32Bytes(1, 0), MaxElements: 1000}); err != nil {
		t.Fatalf("nearest: %v", err)
	}
	if growth > 8<<20 {
		t.Fatalf("live heap grew %d MiB during the walk with %d kept elements of 1 MiB, want under 8 MiB", growth>>20, k)
	}
}
