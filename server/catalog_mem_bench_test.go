package server

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/hoarddb/hoarddb-server/store"
)

// BenchmarkCatalogHeapPerEntry measures what one open bucket costs a node in
// memory, which is the per-bucket cost the shared storage instance did not
// remove: every bucket on a node is a record in its database's table, and a
// bucket that declares indexes or store fields also its declaration. Since
// the catalog-gc swap no engine or index manager is resident between
// operations.
//
// It answers what the retired layout raised and this one cannot. VER-17's
// descriptors, mappings and address space were per-instance and are gone — no
// server path opens a Badger of its own (storage.OpenBadger has no caller
// outside the retired density harness) — but the catalog is still O(buckets) resident
// on every node. That figure is what a density claim has to be stated against.
//
// Three shapes are measured because they differ, and the copy has to know
// which one it is quoting: an empty bucket, one declared index, one declared
// store field (GAP-22 nested buckets).
//
//	go test -run XXX -bench BenchmarkCatalogHeapPerEntry -benchtime=1s -memprofile=catalog.mem ./server/
//	go tool pprof -sample_index=inuse_space -top catalog.mem
//
// Nothing here asserts a threshold. the shared-storage-build record Step 24
// names 1 KB per catalog entry as the ceiling to record and stop at; the
// deliverable here is the measurement, and the number is read from -benchmem
// and published where the other density evidence lives.
func BenchmarkCatalogHeapPerEntry(b *testing.B) {
	shapes := []struct {
		name    string
		indexes []store.IndexSpec
		stores  []catalogStore
	}{
		{name: "empty"},
		{
			name:    "indexed",
			indexes: []store.IndexSpec{{ID: 0, Name: "idx_user_id", Fields: []store.IndexKeyPart{{Field: "user_id"}}}},
		},
		{
			name:   "nested",
			stores: []catalogStore{{Name: "items", ID: 0, Engine: "fifo", Parent: 0}},
		},
	}

	for _, shape := range shapes {
		b.Run(shape.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			// The bucket count is fixed, not b.N: one op builds a whole
			// catalog, so letting b.N drive the count would make the run's
			// work quadratic in the iteration count and the per-op figure
			// meaningless. 10^4 is Step 24's lowest density point.
			for i := 0; i < b.N; i++ {
				held := buildCatalogForBench(b, shape.indexes, shape.stores, catalogBenchBuckets)
				// The catalog has to stay reachable: this reports resident
				// cost, and a map the collector may drop measures nothing.
				holdCatalogForBench(held)
			}
		})
	}
}

// catalogBenchBuckets is the catalog size each op builds. It is a constant
// rather than a flag because the figure this benchmark produces is a cost per
// entry, and that only means something at a stated catalog size.
const catalogBenchBuckets = 10_000

// catalogHeapCeilingBytes is the per-entry resident cost Step 24 names as the
// point to record and stop at (N-24). A catalog above it means the published
// density figure has to account for it rather than be quoted as if buckets
// were free.
const catalogHeapCeilingBytes = 1024

// TestCatalogHeapPerEntry is the measurement the benchmark cannot make.
// -benchmem reports bytes allocated, which includes everything the build
// discards; a density claim needs bytes resident, so this forces a GC and
// reads the live heap (Step 24: "force a GC, record runtime.MemStats.HeapInuse
// per entry with GOGC at its default"; residentHeapAfterGC says why it reads
// HeapAlloc now).
//
// It asserts only Step 24's stop condition. It does not assert a target: the
// figure is published in the private design records, and this exists so the ceiling
// cannot be crossed unnoticed.
func TestCatalogHeapPerEntry(t *testing.T) {
	if testing.Short() {
		t.Skip("builds a 10^4-bucket catalog; skipped under -short")
	}

	shapes := []struct {
		name    string
		indexes []store.IndexSpec
		stores  []catalogStore
	}{
		{name: "empty"},
		{
			name:    "indexed",
			indexes: []store.IndexSpec{{ID: 0, Name: "idx_user_id", Fields: []store.IndexKeyPart{{Field: "user_id"}}}},
		},
		{
			name:   "nested",
			stores: []catalogStore{{Name: "items", ID: 0, Engine: "fifo", Parent: 0}},
		},
	}

	for _, shape := range shapes {
		t.Run(shape.name, func(t *testing.T) {
			// The previous shape's catalog has to go before the baseline, or
			// it is still resident and this shape's figure carries it.
			releaseCatalogForBench()
			baseline := residentHeapAfterGC(t, 0)

			held := buildCatalogForBench(t, shape.indexes, shape.stores, catalogBenchBuckets)
			holdCatalogForBench(held)

			got := residentHeapAfterGC(t, held.count)
			perEntry := (float64(got) - float64(baseline)) / float64(held.count)
			t.Logf("catalog of %d %s buckets: %.0f bytes resident, %.0f bytes per entry",
				held.count, shape.name, float64(got)-float64(baseline), perEntry)

			if perEntry > catalogHeapCeilingBytes {
				t.Errorf("a catalog entry costs %.0f resident bytes, over the %d-byte ceiling Step 24 names; the published density figure has to account for it (shared-storage-build, Step 24)",
					perEntry, catalogHeapCeilingBytes)
			}
		})
	}
}

// residentHeapAfterGC returns HeapAlloc with the collector forced first, which
// is what makes the figure the live bytes rather than merely allocated ones.
// It is HeapAlloc and not HeapInuse because a catalog of records is small
// enough that the spans an earlier shape's garbage left in use outweigh it:
// HeapInuse measured less with 10,000 buckets held than with none. kept is
// passed so the caller can name what is being held across the collection.
func residentHeapAfterGC(t *testing.T, kept int) uint64 {
	t.Helper()

	runtime.GC()
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	t.Logf("HeapAlloc with %d buckets held: %d bytes", kept, stats.HeapAlloc)
	return stats.HeapAlloc
}

// benchmarkCatalogSink holds the catalog a run built, so both the benchmark
// and the test measure an open catalog rather than one the collector has
// already reclaimed. The two accessors exist because a plain assignment reads
// as ineffectual to the linter, which is right: the write is what keeps the
// allocation alive, and the value is never read here.
var benchmarkCatalogSink benchCatalog

// holdCatalogForBench keeps a catalog reachable across a collection.
func holdCatalogForBench(catalog benchCatalog) {
	benchmarkCatalogSink = catalog
	runtime.KeepAlive(catalog)
}

// releaseCatalogForBench drops the catalog the previous shape held, so the
// next shape's baseline is taken without it.
func releaseCatalogForBench() {
	benchmarkCatalogSink = benchCatalog{}
	runtime.GC()
}

// benchCatalog is what one database's registry holds once its buckets are
// created: the table of records, and the declarations of the flagged ones.
type benchCatalog struct {
	db    *inMemoryDB
	decls *declTable
	count int
}

// buildCatalogForBench builds one database's catalog of count buckets, each
// held the way a create leaves it: a record with its slot, type and id, and,
// for a bucket that declares indexes or store fields, its declaration in the
// table. No engine or index manager is resident: a handle builds them per
// operation.
func buildCatalogForBench(tb testing.TB, indexes []store.IndexSpec, stores []catalogStore, count int) benchCatalog {
	tb.Helper()

	held := benchCatalog{db: newInMemoryDB(), decls: new(declTable), count: count}
	for i := 0; i < count; i++ {
		claimed, err := held.db.table.claim(fmt.Sprintf("bucket-%09d", i), BucketTypeHash, 0, false)
		if err != nil {
			tb.Fatalf("claiming bucket %d: %v", i, err)
		}
		rec := claimed.rec
		id := uint64(i + 1)
		rec.mu.Lock()
		rec.slot = uint16(i % 4096)
		if len(indexes) > 0 || len(stores) > 0 {
			decl := &bucketDecl{}
			if len(indexes) > 0 {
				decl.indexSpecs.store(append([]store.IndexSpec(nil), indexes...))
			}
			if len(stores) > 0 {
				decl.fieldStores.Store(&storeFieldSet{stores: append([]catalogStore(nil), stores...), next: 1})
			}
			held.decls.put(id, decl)
			rec.setFlag(recHasDecl, true)
		}
		// id is stored last and exactly once, as an attach does: it publishes
		// the fields written above it.
		rec.id.Store(id)
		rec.mu.Unlock()
	}
	return held
}
