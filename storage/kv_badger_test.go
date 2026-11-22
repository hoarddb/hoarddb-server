package storage_test

import (
	"bytes"
	"crypto/rand"
	"errors"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/storage/kvtest"
)

func TestBadgerKVContract(t *testing.T) {
	kvtest.Run(t, func(t *testing.T) storage.KV {
		kv, err := storage.OpenBadgerKV(t.TempDir(), storage.BadgerTuning{MemTableMB: 32}, nil)
		if err != nil {
			t.Fatalf("opening the Badger store: %v", err)
		}
		return kv
	})
}

// TestBadgerBatchRefusalIsTypedAndSaysWhichBoundWasReached defends against a regression: the
// server words its refusal from the typed error, so the bounds must be on it and
// the entry-count case must be told apart from the byte case.
func TestBadgerBatchRefusalIsTypedAndSaysWhichBoundWasReached(t *testing.T) {
	kv, err := storage.OpenBadgerKV(t.TempDir(), storage.BadgerTuning{MemTableMB: 32}, nil)
	if err != nil {
		t.Fatalf("opening the Badger store: %v", err)
	}
	t.Cleanup(func() { _ = kv.Close() })

	tests := []struct {
		name        string
		valueBytes  int
		wantEntries bool
	}{
		{"large values run out of bytes", 64 << 10, false},
		{"tiny values run out of entries", 1, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			batch := kv.NewBatch()
			value := bytes.Repeat([]byte{0xAB}, tt.valueBytes)
			var refusal error
			for i := 0; refusal == nil && i < 1_000_000; i++ {
				refusal = batch.Set([]byte(fmt.Sprintf("k-%07d", i)), value)
			}
			var tooLarge *storage.BatchTooLargeError
			if !errors.As(refusal, &tooLarge) {
				t.Fatalf("refusal = %v, want a *storage.BatchTooLargeError", refusal)
			}
			if !errors.Is(refusal, storage.ErrBatchTooLarge) {
				t.Errorf("errors.Is(%v, ErrBatchTooLarge) = false", refusal)
			}
			if tooLarge.Bytes != kv.MaxBatchBytes() || tooLarge.Entries <= 0 || len(tooLarge.Key) == 0 {
				t.Errorf("refusal = %+v, want Bytes %d, a positive Entries and the key", tooLarge, kv.MaxBatchBytes())
			}
			if tooLarge.EntryCountReached != tt.wantEntries {
				t.Errorf("EntryCountReached = %v, want %v", tooLarge.EntryCountReached, tt.wantEntries)
			}
			_ = batch.Commit(false)
		})
	}
}

// TestBoundedIteratorCostIsIndependentOfTableCount defends against a regression: a bounded
// iterator over one bucket must not copy the table list of every level, take a
// reference on every SST and open an iterator on every level-0 table. Before
// the fix the same bounded read allocated 2.3x the bytes with 274 tables
// against one.
func TestBoundedIteratorCostIsIndependentOfTableCount(t *testing.T) {
	if testing.Short() {
		t.Skip("writes ~200 MB to build a store with hundreds of tables")
	}
	const (
		wantedBucket  = 7_000_000
		keysPerBucket = 20
		reads         = 2000
		minTables     = 100
	)
	dir := t.TempDir()
	open := func() *storage.BadgerKV {
		kv, err := storage.OpenBadgerKVSmallTables(dir, 32)
		if err != nil {
			t.Fatalf("opening the store: %v", err)
		}
		return kv
	}
	writeBucket := func(kv *storage.BadgerKV, id uint64) {
		batch := kv.NewBatch()
		value := make([]byte, 1000)
		for position := 0; position < keysPerBucket; position++ {
			if _, err := rand.Read(value); err != nil {
				t.Fatalf("filling a value: %v", err)
			}
			key := storage.Key(0, 1, id, storage.NSQueue, []byte{0, 0, 0, byte(position)})
			if err := batch.Set(key, value); err != nil {
				t.Fatalf("setting key %x: %v", key, err)
			}
		}
		if err := batch.Commit(false); err != nil {
			t.Fatalf("committing bucket %d: %v", id, err)
		}
	}
	allocatedPerRead := func(kv *storage.BadgerKV) float64 {
		prefix := storage.Scope(0, 1, wantedBucket)
		bounds := storage.IterOptions{Lower: prefix, Upper: storage.Scope(0, 1, wantedBucket+1), Reverse: true}
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		for run := 0; run < reads; run++ {
			snapshot := kv.NewSnapshot()
			iterator := snapshot.NewIterator(bounds)
			iterator.Seek(nil)
			if !iterator.Valid() {
				t.Fatalf("the bucket's range is empty")
			}
			iterator.Close()
			snapshot.Discard()
		}
		runtime.ReadMemStats(&after)
		return float64(after.TotalAlloc-before.TotalAlloc) / reads
	}

	kv := open()
	writeBucket(kv, wantedBucket)
	if err := kv.Close(); err != nil {
		t.Fatalf("closing the one-bucket store: %v", err)
	}
	kv = open()
	oneTable := allocatedPerRead(kv)
	oneTableCount := kv.TableCount()
	if err := kv.Close(); err != nil {
		t.Fatalf("closing: %v", err)
	}

	kv = open()
	for id := uint64(1); id <= 10_000; id++ {
		writeBucket(kv, id)
	}
	if err := kv.Close(); err != nil {
		t.Fatalf("closing the full store: %v", err)
	}
	kv = open()
	t.Cleanup(func() { _ = kv.Close() })
	time.Sleep(15 * time.Second) // lets Badger's compactors settle, so they do not allocate during the measurement
	manyTables := allocatedPerRead(kv)
	tableCount := kv.TableCount()
	if tableCount < minTables {
		t.Fatalf("the store holds %d tables, want at least %d for the read to be told apart", tableCount, minTables)
	}

	ratio := manyTables / oneTable
	t.Logf("bounded iterator: %.0f B/read with %d tables, %.0f B/read with %d tables (%.2fx)",
		oneTable, oneTableCount, manyTables, tableCount, ratio)
	if ratio > 1.5 {
		t.Errorf("a bounded read allocates %.2fx the bytes with %d tables as with %d; want at most 1.5x",
			ratio, tableCount, oneTableCount)
	}
}

// TestRangeWalkCopyCount walks a band without reading a key or a value and
// asserts the walk copies neither: a copy exists only to keep bytes alive
// after the inner iterator advances, and a walk that never asks for them has
// nothing to keep. Before the fix settle copied every visited key into a
// fresh buffer, so this walk allocated about one buffer per key in either
// direction. The name is the one BUG-0097's acceptance criteria propose.
func TestRangeWalkCopyCount(t *testing.T) {
	const keyCount = 1000
	kv, err := storage.OpenBadgerKV(t.TempDir(), storage.BadgerTuning{MemTableMB: 32}, nil)
	if err != nil {
		t.Fatalf("opening the Badger store: %v", err)
	}
	t.Cleanup(func() { _ = kv.Close() })

	batch := kv.NewBatch()
	for i := range keyCount {
		if err := batch.Set(fmt.Appendf(nil, "k-%05d", i), []byte("v")); err != nil {
			t.Fatalf("staging key %d: %v", i, err)
		}
	}
	if err := batch.Commit(false); err != nil {
		t.Fatalf("committing %d keys: %v", keyCount, err)
	}

	snapshot := kv.NewSnapshot()
	defer snapshot.Discard()
	for _, reverse := range []bool{false, true} {
		bounds := storage.IterOptions{Lower: []byte("k-"), Upper: []byte("k."), Reverse: reverse}
		allocs := testing.AllocsPerRun(5, func() {
			iterator := snapshot.NewIterator(bounds)
			walked := 0
			for iterator.Seek(nil); iterator.Valid(); iterator.Next() {
				walked++
			}
			iterator.Close()
			if walked != keyCount {
				t.Fatalf("reverse=%v: the walk stepped over %d keys, want %d", reverse, walked, keyCount)
			}
		})
		if allocs > keyCount/10 {
			t.Errorf("reverse=%v: walking %d keys without reading one allocated %.0f times, want at most %d: keys the walk never asked for were copied",
				reverse, keyCount, allocs, keyCount/10)
		}
	}
}

// BenchmarkRangeWalkAllocs is the walk half of a Range with nothing read: the
// cost of stepping over a band of keys. BUG-0097's before/after numbers are in
// the bug file.
func BenchmarkRangeWalkAllocs(b *testing.B) {
	const keyCount = 1000
	kv, err := storage.OpenBadgerKV(b.TempDir(), storage.BadgerTuning{MemTableMB: 32}, nil)
	if err != nil {
		b.Fatalf("opening the Badger store: %v", err)
	}
	defer func() { _ = kv.Close() }()
	batch := kv.NewBatch()
	for i := range keyCount {
		if err := batch.Set(fmt.Appendf(nil, "k-%05d", i), []byte("v")); err != nil {
			b.Fatalf("staging key %d: %v", i, err)
		}
	}
	if err := batch.Commit(false); err != nil {
		b.Fatalf("committing %d keys: %v", keyCount, err)
	}
	snapshot := kv.NewSnapshot()
	defer snapshot.Discard()
	bounds := storage.IterOptions{Lower: []byte("k-"), Upper: []byte("k.")}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		iterator := snapshot.NewIterator(bounds)
		for iterator.Seek(nil); iterator.Valid(); iterator.Next() {
		}
		iterator.Close()
	}
}

// BenchmarkBoundedIteratorBuildAndClose is the cost of the bounded iterator: one
// bounded reverse read of a 20-key bucket, iterator built, positioned and
// closed, in a store of a few hundred tables.
func BenchmarkBoundedIteratorBuildAndClose(b *testing.B) {
	dir := b.TempDir()
	kv, err := storage.OpenBadgerKVSmallTables(dir, 32)
	if err != nil {
		b.Fatalf("opening the store: %v", err)
	}
	value := make([]byte, 1000)
	for id := uint64(1); id <= 20000; id++ {
		batch := kv.NewBatch()
		for position := 0; position < 20; position++ {
			if _, err := rand.Read(value); err != nil {
				b.Fatalf("filling a value: %v", err)
			}
			if err := batch.Set(storage.Key(0, 1, id, storage.NSQueue, []byte{0, 0, 0, byte(position)}), value); err != nil {
				b.Fatalf("setting a key: %v", err)
			}
		}
		if err := batch.Commit(false); err != nil {
			b.Fatalf("committing bucket %d: %v", id, err)
		}
	}
	if err := kv.Close(); err != nil {
		b.Fatalf("closing: %v", err)
	}
	if kv, err = storage.OpenBadgerKVSmallTables(dir, 32); err != nil {
		b.Fatalf("reopening: %v", err)
	}
	defer func() { _ = kv.Close() }()
	time.Sleep(15 * time.Second) // compaction settles
	b.Logf("%d tables", kv.TableCount())

	bounds := storage.IterOptions{Lower: storage.Scope(0, 1, 10000), Upper: storage.Scope(0, 1, 10001), Reverse: true}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		snapshot := kv.NewSnapshot()
		iterator := snapshot.NewIterator(bounds)
		iterator.Seek(nil)
		iterator.Close()
		snapshot.Discard()
	}
}
