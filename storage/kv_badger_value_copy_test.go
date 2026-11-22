package storage_test

import (
	"bytes"
	"fmt"
	"runtime"
	"testing"

	"github.com/hoarddb/hoarddb-server/storage"
)

// TestBadgerIteratorCopiesNoValueTheWalkDoesNotRead defends the second
// half: a walk that steps over keys without calling Value — a scan's skip
// prefix, a count — must not copy every value it passes. Before the fix
// settle copied each live value eagerly, so stepping over 1,000 values of
// 4 KiB allocated at least 4 MiB in either direction.
func TestBadgerIteratorCopiesNoValueTheWalkDoesNotRead(t *testing.T) {
	const (
		keyCount   = 1000
		valueBytes = 4 << 10
	)
	kv, err := storage.OpenBadgerKV(t.TempDir(), storage.BadgerTuning{MemTableMB: 32}, nil)
	if err != nil {
		t.Fatalf("opening the Badger store: %v", err)
	}
	t.Cleanup(func() { _ = kv.Close() })

	value := bytes.Repeat([]byte("v"), valueBytes)
	batch := kv.NewBatch()
	for i := range keyCount {
		if err := batch.Set(fmt.Appendf(nil, "k-%04d", i), value); err != nil {
			t.Fatalf("staging key %d: %v", i, err)
		}
	}
	if err := batch.Commit(false); err != nil {
		t.Fatalf("committing %d keys: %v", keyCount, err)
	}

	snapshot := kv.NewSnapshot()
	defer snapshot.Discard()
	for _, reverse := range []bool{false, true} {
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		iterator := snapshot.NewIterator(storage.IterOptions{Lower: []byte("k-"), Upper: []byte("k."), Reverse: reverse})
		stepped := 0
		for iterator.Seek(nil); iterator.Valid(); iterator.Next() {
			stepped++
		}
		iterator.Close()
		runtime.ReadMemStats(&after)

		if stepped != keyCount {
			t.Fatalf("reverse=%v: the walk stepped over %d keys, want %d", reverse, stepped, keyCount)
		}
		copied := after.TotalAlloc - before.TotalAlloc
		if limit := uint64(keyCount * valueBytes / 8); copied > limit {
			t.Errorf("reverse=%v: stepping over %d keys of %d-byte values without reading one allocated %d bytes, want at most %d: values the walk never read were copied",
				reverse, keyCount, valueBytes, copied, limit)
		}
	}
}
