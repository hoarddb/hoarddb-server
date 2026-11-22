// Package kvtest is the contract every storage.KV implementation passes. Each
// implementation's own test calls Run with a constructor, so the Badger store
// and the in-memory fault store are held to one set of assertions.
package kvtest

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/hoarddb/hoarddb-server/storage"
)

// powerLosser is implemented by stores that can simulate a power loss. Only
// the fault store does: Badger has no way to un-commit a batch.
type powerLosser interface {
	PowerLoss()
}

// Run runs the storage.KV contract against stores built by open. open is
// called once per subtest and must return an empty store; the subtest closes it.
func Run(t *testing.T, open func(t *testing.T) storage.KV) {
	t.Helper()
	contract := []struct {
		name string
		run  func(t *testing.T, kv storage.KV)
	}{
		{"TestBatchIsAtomic", testBatchIsAtomic},
		{"TestSnapshotIsolation", testSnapshotIsolation},
		{"TestIteratorStaysInBounds", testIteratorStaysInBounds},
		{"TestIteratorVisitsCountDeleteMarkers", testIteratorVisitsCountDeleteMarkers},
		{"TestIteratorReturnsTheNewestLiveVersion", testIteratorReturnsTheNewestLiveVersion},
		{"TestIteratorIncludeDeletedStopsAtEveryKey", testIteratorIncludeDeletedStopsAtEveryKey},
	}
	for _, clause := range contract {
		t.Run(clause.name, func(t *testing.T) {
			kv := open(t)
			t.Cleanup(func() {
				if err := kv.Close(); err != nil {
					t.Errorf("closing store: %v", err)
				}
			})
			clause.run(t, kv)
		})
	}

	// The power-loss clause is registered only for a store that can lose
	// power; skipping it elsewhere would report a pass nobody ran.
	probe := open(t)
	_, canLosePower := probe.(powerLosser)
	if err := probe.Close(); err != nil {
		t.Fatalf("closing probe store: %v", err)
	}
	if canLosePower {
		t.Run("TestFaultStoreDiscardsUnsyncedBatches", func(t *testing.T) {
			kv := open(t)
			t.Cleanup(func() {
				if err := kv.Close(); err != nil {
					t.Errorf("closing store: %v", err)
				}
			})
			testFaultStoreDiscardsUnsyncedBatches(t, kv, kv.(powerLosser))
		})
	}
}

func commitOrFail(t *testing.T, kv storage.KV, sync bool, sets map[string]string, deletes []string) {
	t.Helper()
	batch := kv.NewBatch()
	for key, value := range sets {
		if err := batch.Set([]byte(key), []byte(value)); err != nil {
			t.Fatalf("batch set %q: %v", key, err)
		}
	}
	for _, key := range deletes {
		if err := batch.Delete([]byte(key)); err != nil {
			t.Fatalf("batch delete %q: %v", key, err)
		}
	}
	if err := batch.Commit(sync); err != nil {
		t.Fatalf("batch commit: %v", err)
	}
}

// present reports whether snapshot serves key, failing on any error other
// than storage.ErrKeyNotFound.
func present(t *testing.T, snapshot storage.KVSnapshot, key string) bool {
	t.Helper()
	_, err := snapshot.Get([]byte(key))
	if errors.Is(err, storage.ErrKeyNotFound) {
		return false
	}
	if err != nil {
		t.Fatalf("get %q: %v", key, err)
	}
	return true
}

func testBatchIsAtomic(t *testing.T, kv storage.KV) {
	const setCount, deleteCount = 50, 20
	doomed := make(map[string]string, deleteCount)
	var doomedKeys []string
	for i := 0; i < deleteCount; i++ {
		key := fmt.Sprintf("doomed-%03d", i)
		doomed[key] = "v"
		doomedKeys = append(doomedKeys, key)
	}
	commitOrFail(t, kv, false, doomed, nil)

	before := kv.NewSnapshot()
	defer before.Discard()

	batch := kv.NewBatch()
	var setKeys []string
	for i := 0; i < setCount; i++ {
		key := fmt.Sprintf("fresh-%03d", i)
		setKeys = append(setKeys, key)
		if err := batch.Set([]byte(key), []byte("v")); err != nil {
			t.Fatalf("batch set %q: %v", key, err)
		}
	}
	for _, key := range doomedKeys {
		if err := batch.Delete([]byte(key)); err != nil {
			t.Fatalf("batch delete %q: %v", key, err)
		}
	}

	uncommitted := kv.NewSnapshot()
	for _, key := range setKeys {
		if present(t, uncommitted, key) {
			t.Fatalf("an uncommitted batch's key %q is visible", key)
		}
	}
	for _, key := range doomedKeys {
		if !present(t, uncommitted, key) {
			t.Fatalf("an uncommitted batch's delete of %q is visible", key)
		}
	}
	uncommitted.Discard()

	if err := batch.Commit(false); err != nil {
		t.Fatalf("commit: %v", err)
	}

	after := kv.NewSnapshot()
	defer after.Discard()
	for _, key := range setKeys {
		if present(t, before, key) {
			t.Fatalf("snapshot taken before the commit sees its key %q", key)
		}
		if !present(t, after, key) {
			t.Fatalf("snapshot taken after the commit misses its key %q", key)
		}
	}
	for _, key := range doomedKeys {
		if !present(t, before, key) {
			t.Fatalf("snapshot taken before the commit misses %q, deleted by it", key)
		}
		if present(t, after, key) {
			t.Fatalf("snapshot taken after the commit still sees %q, deleted by it", key)
		}
	}

	// A batch larger than the store takes in one commit must fail whole. A
	// store that split it into two commits (Badger's WriteBatch does) would
	// report success and leave half of it written.
	const valueBytes = 64 << 10
	value := bytes.Repeat([]byte{0xAB}, valueBytes)
	oversizeCount := int(kv.MaxBatchBytes()/valueBytes) + 2
	oversize := kv.NewBatch()
	for i := 0; i < oversizeCount; i++ {
		// Set may refuse the key that crosses the bound; the refusal must
		// then stick, so it is Commit's error that the clause asserts.
		_ = oversize.Set([]byte(fmt.Sprintf("oversize-%05d", i)), value)
	}
	err := oversize.Commit(false)
	if !errors.Is(err, storage.ErrBatchTooLarge) {
		t.Fatalf("committing %d bytes against a %d-byte bound returned %v, want storage.ErrBatchTooLarge",
			oversizeCount*valueBytes, kv.MaxBatchBytes(), err)
	}
	final := kv.NewSnapshot()
	defer final.Discard()
	for i := 0; i < oversizeCount; i++ {
		key := fmt.Sprintf("oversize-%05d", i)
		if present(t, final, key) {
			t.Fatalf("a refused oversize batch left %q written", key)
		}
	}
}

func testSnapshotIsolation(t *testing.T, kv storage.KV) {
	commitOrFail(t, kv, false, map[string]string{"k": "old"}, nil)

	snapshot := kv.NewSnapshot()
	defer snapshot.Discard()

	commitOrFail(t, kv, false, map[string]string{"k": "new", "later": "v"}, nil)

	got, err := snapshot.Get([]byte("k"))
	if err != nil {
		t.Fatalf("get k from the earlier snapshot: %v", err)
	}
	if string(got) != "old" {
		t.Fatalf("earlier snapshot reads k = %q, want the value it was opened over, %q", got, "old")
	}
	if present(t, snapshot, "later") {
		t.Fatalf("earlier snapshot sees a key committed after it was opened")
	}
	iterator := snapshot.NewIterator(storage.IterOptions{})
	var keys []string
	for iterator.Seek(nil); iterator.Valid(); iterator.Next() {
		keys = append(keys, string(iterator.Key()))
	}
	iterator.Close()
	if len(keys) != 1 || keys[0] != "k" {
		t.Fatalf("earlier snapshot's iterator returns %v, want [k]", keys)
	}

	fresh := kv.NewSnapshot()
	defer fresh.Discard()
	got, err = fresh.Get([]byte("k"))
	if err != nil || string(got) != "new" {
		t.Fatalf("fresh snapshot reads k = %q, %v, want %q", got, err, "new")
	}
	if _, err := fresh.Get([]byte("absent")); !errors.Is(err, storage.ErrKeyNotFound) {
		t.Fatalf("get of an absent key returned %v, want storage.ErrKeyNotFound", err)
	}
}

func testIteratorStaysInBounds(t *testing.T, kv storage.KV) {
	lower := []byte{0x10}
	upper := []byte{0x20}
	all := [][]byte{
		{0x0F, 0xFF}, // just below Lower
		{0x10},       // Lower itself: inside
		{0x10, 0x00},
		{0x1F, 0xFF}, // just below Upper: inside
		{0x20},       // Upper itself: outside, the bound is exclusive
		{0x20, 0x00},
	}
	inside := [][]byte{{0x10}, {0x10, 0x00}, {0x1F, 0xFF}}

	batch := kv.NewBatch()
	for _, key := range all {
		if err := batch.Set(key, []byte("v")); err != nil {
			t.Fatalf("set %x: %v", key, err)
		}
	}
	if err := batch.Commit(false); err != nil {
		t.Fatalf("commit: %v", err)
	}
	snapshot := kv.NewSnapshot()
	defer snapshot.Discard()

	walk := func(reverse bool, seek []byte) [][]byte {
		iterator := snapshot.NewIterator(storage.IterOptions{Lower: lower, Upper: upper, Reverse: reverse})
		defer iterator.Close()
		var keys [][]byte
		for iterator.Seek(seek); iterator.Valid(); iterator.Next() {
			keys = append(keys, append([]byte(nil), iterator.Key()...))
		}
		return keys
	}
	reversed := [][]byte{inside[2], inside[1], inside[0]}

	cases := []struct {
		name    string
		reverse bool
		seek    []byte
		want    [][]byte
	}{
		{"forward from the start", false, nil, inside},
		{"forward from below Lower", false, []byte{0x00}, inside},
		{"forward from inside", false, []byte{0x10, 0x01}, inside[2:]},
		{"forward from Upper", false, upper, nil},
		{"reverse from the end", true, nil, reversed},
		{"reverse from Upper, exclusive", true, upper, reversed},
		{"reverse from above Upper", true, []byte{0xFF}, reversed},
		{"reverse from inside", true, []byte{0x10, 0x01}, reversed[1:]},
		{"reverse from below Lower", true, []byte{0x0F, 0xFF}, nil},
	}
	for _, tc := range cases {
		got := walk(tc.reverse, tc.seek)
		if len(got) != len(tc.want) {
			t.Fatalf("%s: keys %x, want %x", tc.name, got, tc.want)
		}
		for i := range got {
			if !bytes.Equal(got[i], tc.want[i]) {
				t.Fatalf("%s: keys %x, want %x", tc.name, got, tc.want)
			}
		}
	}
}

func testIteratorVisitsCountDeleteMarkers(t *testing.T, kv storage.KV) {
	const keyCount = 1000
	keyAt := func(i int) []byte { return []byte(fmt.Sprintf("q-%05d", i)) }

	writes := kv.NewBatch()
	for i := 0; i < keyCount; i++ {
		if err := writes.Set(keyAt(i), []byte("v")); err != nil {
			t.Fatalf("set: %v", err)
		}
	}
	if err := writes.Commit(false); err != nil {
		t.Fatalf("commit sets: %v", err)
	}
	deletes := kv.NewBatch()
	for i := 0; i < keyCount; i++ {
		if err := deletes.Delete(keyAt(i)); err != nil {
			t.Fatalf("delete: %v", err)
		}
	}
	if err := deletes.Commit(false); err != nil {
		t.Fatalf("commit deletes: %v", err)
	}

	snapshot := kv.NewSnapshot()
	defer snapshot.Discard()
	iterator := snapshot.NewIterator(storage.IterOptions{Lower: []byte("q-"), Upper: []byte("q.")})
	defer iterator.Close()
	iterator.Seek(nil)
	if iterator.Valid() {
		t.Fatalf("iterator over %d deleted keys is positioned at %q, want no live key", keyCount, iterator.Key())
	}
	if visits := iterator.Visits(); visits < keyCount {
		t.Fatalf("Visits() = %d after walking %d delete markers, want at least %d: the walk the reclaimer's budget charges was not counted",
			visits, keyCount, keyCount)
	}
}

// testIteratorIncludeDeletedStopsAtEveryKey: with IncludeDeleted the iterator
// stops at a delete marker as at a live key, so a walk can end inside a run of
// markers after a bounded number of visits — the reclaimer's step (spec
// §5.2.3 step 3) — and says which keys are markers through ErrKeyNotFound.
func testIteratorIncludeDeletedStopsAtEveryKey(t *testing.T, kv storage.KV) {
	commitOrFail(t, kv, false, map[string]string{"m-1": "a", "m-2": "b", "m-3": "c", "m-4": "d"}, nil)
	commitOrFail(t, kv, false, nil, []string{"m-1", "m-3"})

	snapshot := kv.NewSnapshot()
	defer snapshot.Discard()
	iterator := snapshot.NewIterator(storage.IterOptions{Lower: []byte("m-"), Upper: []byte("m."), IncludeDeleted: true})
	defer iterator.Close()

	type stop struct {
		key  string
		live string
	}
	var stops []stop
	for iterator.Seek(nil); iterator.Valid(); iterator.Next() {
		stored, err := iterator.Value()
		switch {
		case errors.Is(err, storage.ErrKeyNotFound):
			stops = append(stops, stop{key: string(iterator.Key())})
		case err != nil:
			t.Fatalf("value of %q: %v", iterator.Key(), err)
		default:
			stops = append(stops, stop{key: string(iterator.Key()), live: string(stored)})
		}
	}
	want := []stop{{key: "m-1"}, {key: "m-2", live: "b"}, {key: "m-3"}, {key: "m-4", live: "d"}}
	if fmt.Sprint(stops) != fmt.Sprint(want) {
		t.Fatalf("an IncludeDeleted walk stopped at %v, want every key with the markers read as not found: %v", stops, want)
	}

	// The first stop is the first marker, after visiting only it: the walk did
	// not pass the run on its way to a live key.
	first := snapshot.NewIterator(storage.IterOptions{Lower: []byte("m-"), Upper: []byte("m."), IncludeDeleted: true})
	defer first.Close()
	first.Seek([]byte("m-1"))
	if !first.Valid() || string(first.Key()) != "m-1" {
		t.Fatalf("a seek onto a marker with IncludeDeleted did not stop at it")
	}
	if visits := first.Visits(); visits > 2 {
		t.Fatalf("stopping at one marker visited %d versions, want at most its own two", visits)
	}
}

func testFaultStoreDiscardsUnsyncedBatches(t *testing.T, kv storage.KV, store powerLosser) {
	commitOrFail(t, kv, true, map[string]string{"A": "a"}, nil)
	commitOrFail(t, kv, false, map[string]string{"B": "b"}, nil)
	commitOrFail(t, kv, false, map[string]string{"C": "c"}, nil)

	store.PowerLoss()

	snapshot := kv.NewSnapshot()
	defer snapshot.Discard()
	if !present(t, snapshot, "A") {
		t.Fatalf("a synced batch was lost to the power loss")
	}
	for _, key := range []string{"B", "C"} {
		if present(t, snapshot, key) {
			t.Fatalf("unsynced batch %q survived the power loss", key)
		}
	}

	// A Sync makes every earlier unsynced batch durable.
	commitOrFail(t, kv, false, map[string]string{"D": "d"}, nil)
	if err := kv.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
	commitOrFail(t, kv, false, map[string]string{"E": "e"}, nil)
	store.PowerLoss()
	afterSync := kv.NewSnapshot()
	defer afterSync.Discard()
	if !present(t, afterSync, "D") {
		t.Fatalf("a batch made durable by Sync was lost to the power loss")
	}
	if present(t, afterSync, "E") {
		t.Fatalf("a batch committed unsynced after Sync survived the power loss")
	}
}

// testIteratorReturnsTheNewestLiveVersion walks keys with several versions in
// both directions. A walk that sees every version must still return one entry
// per key, carrying its newest value — Badger yields a key's versions newest
// first going forward and oldest first in reverse — and must hide a key whose
// newest version is a delete even when older live versions remain.
func testIteratorReturnsTheNewestLiveVersion(t *testing.T, kv storage.KV) {
	commitOrFail(t, kv, false, map[string]string{"a": "a1", "b": "b1", "c": "c1", "d": "d1"}, nil)
	commitOrFail(t, kv, false, map[string]string{"b": "b2", "d": "d2"}, []string{"c"})
	commitOrFail(t, kv, false, map[string]string{"b": "b3"}, nil)

	snapshot := kv.NewSnapshot()
	defer snapshot.Discard()
	walk := func(reverse bool) []string {
		iterator := snapshot.NewIterator(storage.IterOptions{Reverse: reverse})
		defer iterator.Close()
		var pairs []string
		for iterator.Seek(nil); iterator.Valid(); iterator.Next() {
			stored, err := iterator.Value()
			if err != nil {
				t.Fatalf("value of %q: %v", iterator.Key(), err)
			}
			pairs = append(pairs, string(iterator.Key())+"="+string(stored))
		}
		return pairs
	}
	forward := fmt.Sprint(walk(false))
	if forward != "[a=a1 b=b3 d=d2]" {
		t.Fatalf("forward walk = %s, want [a=a1 b=b3 d=d2]", forward)
	}
	reverse := fmt.Sprint(walk(true))
	if reverse != "[d=d2 b=b3 a=a1]" {
		t.Fatalf("reverse walk = %s, want [d=d2 b=b3 a=a1]", reverse)
	}
}
