package wire

import (
	"fmt"
	mathrand "math/rand/v2"
	"testing"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

// BenchmarkReadStoredDocument seeds 50,000 documents of a header and three
// fields across 20 reopened generations, then reads random documents with the
// real readStoredDocument. "unbounded" hides the bounds from Badger (what the
// read did before it passed any); "bounded" is the read as written.
func BenchmarkReadStoredDocument(b *testing.B) {
	dir := b.TempDir()
	random := mathrand.New(mathrand.NewPCG(7, 7))
	order := random.Perm(50000)
	value := make([]byte, 100)
	open := func() *storage.BadgerKV {
		kv, err := storage.OpenBadgerKV(dir, storage.BadgerTuning{MemTableMB: 8}, nil)
		if err != nil {
			b.Fatal(err)
		}
		return kv
	}
	slotOf := func(id int) uint16 { return uint16(id * 7919 % 64) }
	bucketOf := func(id int) uint64 { return uint64(1 + id%50) }
	encoded := make([][]byte, len(order))
	for id := range encoded {
		component, err := store.EncodeComponent(fmt.Sprintf("user-%d", id))
		if err != nil {
			b.Fatal(err)
		}
		encoded[id] = component
	}
	kv := open()
	const generations = 20
	for generation := 0; generation < generations; generation++ {
		chunk := order[generation*len(order)/generations : (generation+1)*len(order)/generations]
		for start := 0; start < len(chunk); start += 250 {
			batch := kv.NewBatch()
			for _, id := range chunk[start:min(start+250, len(chunk))] {
				prefix := store.DocumentPrefix(slotOf(id), testDB, bucketOf(id), encoded[id])
				for i := range value {
					value[i] = byte(random.UintN(256))
				}
				if err := batch.Set(prefix, value); err != nil {
					b.Fatal(err)
				}
				for field := uint16(2); field <= 4; field++ {
					if err := batch.Set(store.FieldKey(prefix, field), value); err != nil {
						b.Fatal(err)
					}
				}
			}
			if err := batch.Commit(false); err != nil {
				b.Fatal(err)
			}
		}
		if generation < generations-1 {
			if err := kv.Close(); err != nil {
				b.Fatal(err)
			}
			kv = open()
		}
	}
	b.Cleanup(func() { _ = kv.Close() })
	// Both modes read the same documents before any timing, so neither pays for the cold
	// block and table caches: the first sub-benchmark would otherwise look slower than it is.
	readOne := func(snapshot storage.KVSnapshot, reader *mathrand.Rand, b *testing.B) {
		id := reader.IntN(len(order))
		req := ReadRequest{Slot: slotOf(id), DBID: testDB, BucketID: bucketOf(id), EncID: encoded[id]}
		doc, live, err := readStoredDocument(snapshot, req, "")
		if err != nil || !live || len(doc.fields) != 3 {
			b.Fatalf("read of user-%d = %d fields, live %v, %v", id, len(doc.fields), live, err)
		}
	}
	snapshotFor := func(mode string) storage.KVSnapshot {
		snapshot := kv.NewSnapshot()
		if mode == "unbounded" {
			snapshot = unboundedSnapshot{snapshot}
		}
		return snapshot
	}
	modes := []string{"unbounded", "bounded"}
	const warmReads = 20000
	for pass := 0; pass < 2; pass++ {
		for _, mode := range modes {
			snapshot := snapshotFor(mode)
			reader := mathrand.New(mathrand.NewPCG(1, 1))
			for range warmReads {
				readOne(snapshot, reader, b)
			}
			snapshot.Discard()
		}
	}
	for _, mode := range modes {
		b.Run(mode, func(b *testing.B) {
			snapshot := snapshotFor(mode)
			defer snapshot.Discard()
			reader := mathrand.New(mathrand.NewPCG(1, 1))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				readOne(snapshot, reader, b)
			}
		})
	}
}
