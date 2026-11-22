package storage_test

import (
	"bytes"
	"crypto/rand"
	"errors"
	"fmt"
	mathrand "math/rand/v2"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/storage"
)

// boundsEntry is one stored key as an unbounded walk sees it.
type boundsEntry struct {
	key     []byte
	value   []byte
	deleted bool
}

// TestBoundedIteratorMatchesAnUnboundedWalk defends the correctness
// side: passing the bounds' shared prefix to Badger must change the cost of a
// read and nothing it returns. Over a store of many tables across levels, with
// older versions, delete markers and a live memtable, every combination of
// bound shape, direction, IncludeDeleted and Seek target must return the same
// keys and values as an unbounded walk filtered in Go.
//
// The two walks are two iterators of one snapshot, and a snapshot fixes the
// live keys only. Badger's compaction drops a delete marker, with the versions
// under it, once the marker is at or below the oldest open read and no deeper
// table can hold the key, so an IncludeDeleted walk opened after a compaction
// finished shows fewer markers than one opened before it. The store is built
// with compactions still running, so on a mismatch with IncludeDeleted the
// unbounded walk is taken again (refreshUnboundedWalk) and both sides are
// compared afresh; a walk without IncludeDeleted must match at once.
func TestBoundedIteratorMatchesAnUnboundedWalk(t *testing.T) {
	if testing.Short() {
		t.Skip("writes ~40 MB to build a store with tens of tables")
	}
	kv := buildMixedStore(t)
	t.Cleanup(func() { _ = kv.Close() })
	t.Logf("the store holds %d tables", kv.TableCount())
	if kv.TableCount() < 10 {
		t.Fatalf("the store holds %d tables; the guard needs many to prune any", kv.TableCount())
	}

	snapshot := kv.NewSnapshot()
	defer snapshot.Discard()
	everything := walkAll(t, snapshot)
	if len(everything) == 0 {
		t.Fatal("the unbounded walk found nothing")
	}

	for _, shape := range boundShapes(everything) {
		for _, reverse := range []bool{false, true} {
			for _, includeDeleted := range []bool{false, true} {
				for _, seek := range seekTargets(shape, everything) {
					name := fmt.Sprintf("%s/reverse=%v/includeDeleted=%v/seek=%x", shape.name, reverse, includeDeleted, seek)
					opts := storage.IterOptions{Lower: shape.lower, Upper: shape.upper, Reverse: reverse, IncludeDeleted: includeDeleted}
					want := expectedWalk(everything, opts, seek)
					got := boundedWalk(t, snapshot, opts, seek)
					for deadline := time.Now().Add(markerSettleTimeout); includeDeleted && !sameEntries(got, want) && time.Now().Before(deadline); {
						everything = refreshUnboundedWalk(t, snapshot, everything)
						want = expectedWalk(everything, opts, seek)
						got = boundedWalk(t, snapshot, opts, seek)
					}
					if !sameEntries(got, want) {
						t.Fatalf("%s: got %d entries, want %d\n got  %s\n want %s", name, len(got), len(want), describe(got), describe(want))
					}
				}
			}
		}
	}
}

// markerSettleTimeout bounds how long an IncludeDeleted comparison is retried
// while compactions drop delete markers under the open snapshot. The store's
// compactions finish in about a second; a bounded walk that loses a marker for
// another reason never agrees and fails once this has passed.
const markerSettleTimeout = 30 * time.Second

// refreshUnboundedWalk takes the unbounded walk again and checks it against
// the previous one: every live key and value must be unchanged, and the only
// difference allowed is that delete markers have gone. Anything else is a
// snapshot showing two different stores, which no retry may hide.
func refreshUnboundedWalk(t *testing.T, snapshot storage.KVSnapshot, previous []boundsEntry) []boundsEntry {
	t.Helper()
	refreshed := walkAll(t, snapshot)
	next := 0
	for _, entry := range previous {
		if next < len(refreshed) && bytes.Equal(refreshed[next].key, entry.key) {
			if refreshed[next].deleted != entry.deleted || !bytes.Equal(refreshed[next].value, entry.value) {
				t.Fatalf("key %x changed under an open snapshot between two unbounded walks", entry.key)
			}
			next++
			continue
		}
		if !entry.deleted {
			t.Fatalf("live key %x vanished under an open snapshot between two unbounded walks", entry.key)
		}
	}
	if next != len(refreshed) {
		t.Fatalf("key %x appeared under an open snapshot between two unbounded walks", refreshed[next].key)
	}
	return refreshed
}

type boundShape struct {
	name         string
	lower, upper []byte
}

func prefixEndOf(prefix []byte) []byte {
	end := append([]byte(nil), prefix...)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xFF {
			end[i]++
			return end[:i+1]
		}
	}
	return nil
}

func boundShapes(everything []boundsEntry) []boundShape {
	var shapes []boundShape
	add := func(name string, lower, upper []byte) {
		shapes = append(shapes, boundShape{name: name, lower: lower, upper: upper})
	}
	// 700 holds no key; 255 and 65535 make prefixEnd carry into a shorter key.
	for _, id := range []uint64{5, 6, 255, 256, 257, 300, 599, 700, 65535} {
		bucket := storage.Scope(0, 1, id)
		queue := storage.Key(0, 1, id, storage.NSQueue)
		add(fmt.Sprintf("bucket %d by Scope", id), bucket, storage.Scope(0, 1, id+1))
		add(fmt.Sprintf("bucket %d by prefixEnd", id), bucket, prefixEndOf(bucket))
		add(fmt.Sprintf("bucket %d namespace", id), queue, prefixEndOf(queue))
		add(fmt.Sprintf("bucket %d namespace and first byte", id), storage.Key(0, 1, id, storage.NSQueue, []byte{0, 0, 0}),
			prefixEndOf(storage.Key(0, 1, id, storage.NSQueue, []byte{0, 0, 0})))
		add(fmt.Sprintf("bucket %d from the middle", id), storage.Key(0, 1, id, storage.NSQueue, []byte{0, 0, 0, 7}), storage.Scope(0, 1, id+1))
		add(fmt.Sprintf("bucket %d up to the middle", id), bucket, storage.Key(0, 1, id, storage.NSQueue, []byte{0, 0, 0, 7}))
		add(fmt.Sprintf("bucket %d slot 1", id), storage.Scope(1, 1, id), storage.Scope(1, 1, id+1))
	}
	add("two buckets", storage.Scope(0, 1, 5), storage.Scope(0, 1, 7))
	add("three hundred buckets", storage.Scope(0, 1, 5), storage.Scope(0, 1, 305))
	add("one slot", []byte{0, 0}, []byte{0, 1})
	add("two slots, no shared byte", []byte{0, 0}, []byte{1, 0})
	add("across slots, no shared prefix", storage.Scope(0, 1, 5), storage.Scope(300, 1, 5))
	add("lower only", storage.Scope(0, 1, 300), nil)
	add("upper only", nil, storage.Scope(0, 1, 300))
	add("unbounded", nil, nil)
	add("empty non-nil bounds are bounds, not open", []byte{}, []byte{})
	// An exact stored key as either bound: a raw bucket prefix is stored for the ids below.
	add("Upper is a stored key", storage.Scope(0, 1, 5), storage.Scope(0, 1, 6))
	add("Lower is a stored key", storage.Scope(0, 1, 6), storage.Scope(0, 1, 7))
	middle := everything[len(everything)/2].key
	add("Lower and Upper are stored keys", everything[len(everything)/3].key, middle)
	add("from a stored key to the end of its bucket", middle, storage.Scope(binaryUint16(middle), 1, binaryUint64(middle)+1))
	return shapes
}

func binaryUint16(key []byte) uint16 { return uint16(key[0])<<8 | uint16(key[1]) }

func binaryUint64(key []byte) uint64 {
	var id uint64
	for _, b := range key[10:18] {
		id = id<<8 | uint64(b)
	}
	return id
}

func seekTargets(shape boundShape, everything []boundsEntry) [][]byte {
	targets := [][]byte{nil, {}, {0x00}, {0xFF, 0xFF}}
	if shape.lower != nil {
		targets = append(targets, shape.lower, append(append([]byte(nil), shape.lower...), 0x00))
	}
	if shape.upper != nil {
		targets = append(targets, shape.upper)
	}
	inRange := expectedWalk(everything, storage.IterOptions{Lower: shape.lower, Upper: shape.upper, IncludeDeleted: true}, nil)
	for _, fraction := range []int{3, 2} {
		if len(inRange) >= fraction {
			targets = append(targets, inRange[len(inRange)/fraction].key)
		}
	}
	if len(inRange) > 0 {
		last := inRange[len(inRange)-1].key
		targets = append(targets, last, append(append([]byte(nil), last...), 0x00))
	}
	return targets
}

// expectedWalk filters the unbounded list the way a bounded walk must: by the
// half-open range, by liveness, in the walk's direction, from the Seek target.
func expectedWalk(everything []boundsEntry, opts storage.IterOptions, seek []byte) []boundsEntry {
	var inRange []boundsEntry
	for _, entry := range everything {
		if opts.Lower != nil && bytes.Compare(entry.key, opts.Lower) < 0 {
			continue
		}
		if opts.Upper != nil && bytes.Compare(entry.key, opts.Upper) >= 0 {
			continue
		}
		if entry.deleted && !opts.IncludeDeleted {
			continue
		}
		inRange = append(inRange, entry)
	}
	if opts.Reverse {
		for left, right := 0, len(inRange)-1; left < right; left, right = left+1, right-1 {
			inRange[left], inRange[right] = inRange[right], inRange[left]
		}
	}
	if len(seek) == 0 {
		return inRange
	}
	var from []boundsEntry
	for _, entry := range inRange {
		keep := bytes.Compare(entry.key, seek) >= 0
		if opts.Reverse {
			keep = bytes.Compare(entry.key, seek) <= 0
		}
		if keep {
			from = append(from, entry)
		}
	}
	return from
}

func boundedWalk(t *testing.T, snapshot storage.KVSnapshot, opts storage.IterOptions, seek []byte) []boundsEntry {
	t.Helper()
	iterator := snapshot.NewIterator(opts)
	defer iterator.Close()
	var walked []boundsEntry
	for iterator.Seek(seek); iterator.Valid(); iterator.Next() {
		entry := boundsEntry{key: append([]byte(nil), iterator.Key()...)}
		value, err := iterator.Value()
		switch {
		case errors.Is(err, storage.ErrKeyNotFound):
			entry.deleted = true
		case err != nil:
			t.Fatalf("reading the value of %x: %v", entry.key, err)
		default:
			entry.value = value
		}
		walked = append(walked, entry)
	}
	// A walk an unreadable block ended looks like a short range otherwise.
	if err := iterator.Err(); err != nil {
		t.Fatalf("the walk from %x with %+v ended on an error after %d entries: %v", seek, opts, len(walked), err)
	}
	return walked
}

func walkAll(t *testing.T, snapshot storage.KVSnapshot) []boundsEntry {
	t.Helper()
	return boundedWalk(t, snapshot, storage.IterOptions{IncludeDeleted: true}, nil)
}

func sameEntries(got, want []boundsEntry) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if !bytes.Equal(got[i].key, want[i].key) || got[i].deleted != want[i].deleted || !bytes.Equal(got[i].value, want[i].value) {
			return false
		}
	}
	return true
}

func describe(entries []boundsEntry) string {
	const shown = 4
	var out []string
	for i, entry := range entries {
		if i == shown {
			out = append(out, "...")
			break
		}
		out = append(out, fmt.Sprintf("%x", entry.key))
	}
	return fmt.Sprint(out)
}

// buildMixedStore writes tens of megabytes in three generations, closing the
// store between them so older versions and delete markers sit in tables at
// several levels, and leaves the last generation in the memtable.
func buildMixedStore(t *testing.T) *storage.BadgerKV {
	t.Helper()
	dir := t.TempDir()
	open := func() *storage.BadgerKV {
		kv, err := storage.OpenBadgerKVSmallTables(dir, 8)
		if err != nil {
			t.Fatalf("opening the store: %v", err)
		}
		return kv
	}
	random := mathrand.New(mathrand.NewPCG(244, 244))
	value := func() []byte {
		stored := make([]byte, 1000)
		if _, err := rand.Read(stored); err != nil {
			t.Fatalf("filling a value: %v", err)
		}
		return stored
	}
	commit := func(batch storage.KVBatch) {
		if err := batch.Commit(false); err != nil {
			t.Fatalf("committing: %v", err)
		}
	}
	var keys [][]byte
	kv := open()
	for id := uint64(1); id <= 600; id++ {
		batch := kv.NewBatch()
		for _, namespace := range []byte{storage.NSQueue, 0x01, 0x02} {
			for position := byte(0); position < 20; position++ {
				key := storage.Key(0, 1, id, namespace, []byte{0, 0, 0, position})
				keys = append(keys, key)
				mustSet(t, batch, key, value())
			}
		}
		commit(batch)
	}
	for _, id := range []uint64{5, 255, 300} {
		batch := kv.NewBatch()
		for position := byte(0); position < 20; position++ {
			key := storage.Key(1, 1, id, storage.NSQueue, []byte{0, 0, 0, position})
			keys = append(keys, key)
			mustSet(t, batch, key, value())
		}
		key := storage.Key(300, 1, id, storage.NSQueue, []byte{0, 0, 0, 1})
		keys = append(keys, key)
		mustSet(t, batch, key, value())
		commit(batch)
	}
	// A key that is exactly a bucket's prefix is what a reverse walk meets
	// first when it seeks to the next bucket's prefix.
	batch := kv.NewBatch()
	for _, id := range []uint64{5, 6, 7, 255, 256, 300, 306} {
		key := storage.Scope(0, 1, id)
		keys = append(keys, key)
		mustSet(t, batch, key, value())
	}
	commit(batch)
	if err := kv.Close(); err != nil {
		t.Fatalf("closing the first generation: %v", err)
	}

	mutate := func(generation string, overwrite, remove int) {
		// The store commits at most ~1.2 MB atomically at this memtable size.
		const keysPerBatch = 500
		batch := kv.NewBatch()
		for written, index := range random.Perm(len(keys))[:overwrite+remove] {
			if written > 0 && written%keysPerBatch == 0 {
				commit(batch)
				batch = kv.NewBatch()
			}
			if overwrite > 0 {
				overwrite--
				mustSet(t, batch, keys[index], value())
				continue
			}
			if err := batch.Delete(keys[index]); err != nil {
				t.Fatalf("%s: deleting %x: %v", generation, keys[index], err)
			}
		}
		commit(batch)
	}
	kv = open()
	mutate("second generation", len(keys)/10, len(keys)/10)
	if err := kv.Close(); err != nil {
		t.Fatalf("closing the second generation: %v", err)
	}
	kv = open()
	// Badger flushes a closed memtable as one big level-0 table; compaction
	// is what cuts it into the 1 MiB tables this guard needs to prune.
	for deadline := time.Now().Add(30 * time.Second); kv.TableCount() < 20 && time.Now().Before(deadline); {
		time.Sleep(250 * time.Millisecond)
	}
	mutate("third generation", len(keys)/20, len(keys)/20)
	return kv
}

func mustSet(t *testing.T, batch storage.KVBatch, key, value []byte) {
	t.Helper()
	if err := batch.Set(key, value); err != nil {
		t.Fatalf("setting %x: %v", key, err)
	}
}
