package storage_test

import (
	"bytes"
	"encoding/binary"
	mathrand "math/rand/v2"
	"testing"

	"github.com/hoarddb/hoarddb-server/storage"
)

// docReadFixture seeds a store of many small tables: 50,000 documents of a
// header and three fields, spread over 50 buckets and 64 slots, written in a
// shuffled order across generations so the tables overlap the way a node's do.
func docReadFixture(b *testing.B) (*storage.BadgerKV, int) {
	b.Helper()
	dir := b.TempDir()
	random := mathrand.New(mathrand.NewPCG(7, 7))
	order := random.Perm(50000)
	// Random bytes: Snappy shrinks a repeated value to nothing and the store would hold few tables.
	value := make([]byte, 100)
	kv, err := storage.OpenBadgerKVSmallTables(dir, 8)
	if err != nil {
		b.Fatal(err)
	}
	const generations = 20
	for generation := 0; generation < generations; generation++ {
		chunk := order[generation*len(order)/generations : (generation+1)*len(order)/generations]
		for start := 0; start < len(chunk); start += 500 {
			batch := kv.NewBatch()
			for _, id := range chunk[start:min(start+500, len(chunk))] {
				prefix := docReadPrefix(id)
				for i := range value {
					value[i] = byte(random.UintN(256))
				}
				for _, key := range [][]byte{prefix, append(bytes.Clone(prefix), 0, 2), append(bytes.Clone(prefix), 0, 3), append(bytes.Clone(prefix), 0, 4)} {
					if err := batch.Set(key, value); err != nil {
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
			if kv, err = storage.OpenBadgerKVSmallTables(dir, 8); err != nil {
				b.Fatal(err)
			}
		}
	}
	b.Cleanup(func() { _ = kv.Close() })
	return kv, kv.TableCount()
}

func docReadPrefix(id int) []byte {
	enc := binary.BigEndian.AppendUint64([]byte{0x10}, uint64(id))
	return storage.Key(uint16(id*7919%64), 1, uint64(1+id%50), storage.NSDocuments, enc)
}

// BenchmarkDocumentPrefixRead is the read a document get does: one iterator,
// one Seek to the document's prefix, walk its keys, read each value. Unbounded
// is the old call (empty options); bounded passes the prefix as Lower and Upper.
func BenchmarkDocumentPrefixRead(b *testing.B) {
	kv, tables := docReadFixture(b)
	b.Logf("the store holds %d tables", tables)
	if tables < 8 {
		b.Fatalf("the store holds %d tables; the comparison needs 8 or more", tables)
	}
	modes := []struct {
		name    string
		options func(prefix []byte) storage.IterOptions
	}{
		{"unbounded", func([]byte) storage.IterOptions { return storage.IterOptions{} }},
		{"bounded", func(prefix []byte) storage.IterOptions {
			return storage.IterOptions{Lower: prefix, Upper: prefixEndOf(prefix)}
		}},
	}
	readOne := func(snapshot storage.KVSnapshot, random *mathrand.Rand, options func([]byte) storage.IterOptions) {
		prefix := docReadPrefix(random.IntN(50000))
		iterator := snapshot.NewIterator(options(prefix))
		keys := 0
		for iterator.Seek(prefix); iterator.Valid(); iterator.Next() {
			if !bytes.HasPrefix(iterator.Key(), prefix) {
				break
			}
			if _, err := iterator.Value(); err != nil {
				b.Fatal(err)
			}
			keys++
		}
		iterator.Close()
		if keys != 4 {
			b.Fatalf("read %d keys of a document, want 4", keys)
		}
	}
	// Both modes read the same documents before any timing, so neither pays for the cold
	// block and table caches: the first sub-benchmark would otherwise look slower than it is.
	const warmReads = 20000
	for pass := 0; pass < 2; pass++ {
		for _, mode := range modes {
			snapshot := kv.NewSnapshot()
			random := mathrand.New(mathrand.NewPCG(1, 1))
			for range warmReads {
				readOne(snapshot, random, mode.options)
			}
			snapshot.Discard()
		}
	}
	for _, mode := range modes {
		b.Run(mode.name, func(b *testing.B) {
			snapshot := kv.NewSnapshot()
			defer snapshot.Discard()
			random := mathrand.New(mathrand.NewPCG(1, 1))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				readOne(snapshot, random, mode.options)
			}
		})
	}
}
