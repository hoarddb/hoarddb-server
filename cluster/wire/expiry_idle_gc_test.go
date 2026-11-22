package wire

import (
	"fmt"
	"runtime"
	"runtime/metrics"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/store"
)

// idleBucketsTracedObjects writes the catalog rows of count idle buckets into a fresh store and
// returns the heap objects the collector traces that they leave behind. expiring declares both
// expiry forms on each bucket.
func idleBucketsTracedObjects(t *testing.T, count int, expiring bool) float64 {
	t.Helper()
	kv := openKV(t, t.TempDir())
	defer func() {
		if err := kv.Close(); err != nil {
			t.Errorf("closing the store: %v", err)
		}
	}()
	read := func() uint64 {
		runtime.GC()
		runtime.GC()
		samples := []metrics.Sample{{Name: "/gc/heap/objects:objects"}}
		metrics.Read(samples)
		return samples[0].Value.Uint64()
	}
	before := read()
	batch := kv.NewBatch()
	for i := range count {
		decl := machine.Declaration{BucketID: uint64(i + 1), State: machine.StateActive, Version: 1, Fields: machine.FieldTable{
			NextFieldID: 5,
			Entries:     []machine.FieldEntry{{ID: 1, Type: "any", Name: "_id_"}, {ID: 2, Type: "datetime", Name: "ExpiresAt"}, {ID: 3, Type: "fifoStore", Name: "Log"}},
		}}
		if expiring {
			decl.Expire = machine.ExpireTable{BucketField: 2, Stores: []machine.ExpireStore{{FieldID: 3, Path: "ts"}}}
		}
		stored, err := store.EncodeDeclaration(decl)
		if err != nil {
			t.Fatal(err)
		}
		encID, err := store.EncodeComponent(fmt.Sprintf("bucket-%08d", i))
		if err != nil {
			t.Fatal(err)
		}
		prefix := store.DocumentPrefix(1, 1, machine.SystemBucketID, encID)
		for fieldID, value := range stored {
			if err := batch.Set(store.FieldKey(prefix, fieldID), value); err != nil {
				t.Fatal(err)
			}
		}
		if i%1000 == 999 {
			if err := batch.Commit(false); err != nil {
				t.Fatal(err)
			}
			batch = kv.NewBatch()
		}
	}
	if err := batch.Commit(true); err != nil {
		t.Fatal(err)
	}
	return float64(read()-before) / float64(count)
}

// TestIdleExpiringBucketsAddNoGCTracedObjects is the ring's form of the server's
// TestIdleBucketsAddNoGCTracedObjects (small-bucket principle, 10^5 buckets): declaring expiry on a
// bucket adds under a tenth of a heap object per idle bucket on top of what the same bucket costs
// without it. LIMIT: it measures the stored catalog rows only. It does not measure resident engine state
// (the declaration cache is bounded by cluster.decl_cache_entries and holds only buckets in use, so an idle
// bucket has no resident expiry state; TestIdleExpiringBucketsAddNoResidentBytes pins the sweeper side).
func TestIdleExpiringBucketsAddNoGCTracedObjects(t *testing.T) {
	if testing.Short() {
		t.Skip("writes 200,000 catalog rows; skipped under -short")
	}
	const buckets = 100_000
	plain := idleBucketsTracedObjects(t, buckets, false)
	expiring := idleBucketsTracedObjects(t, buckets, true)
	t.Logf("%.4f traced objects per plain idle bucket, %.4f per expiring one (%d buckets)", plain, expiring, buckets)
	if added := expiring - plain; added >= 0.1 {
		t.Errorf("an idle expiring bucket adds %.4f heap objects the collector traces over a plain one; the limit is 0.1", added)
	}
}
