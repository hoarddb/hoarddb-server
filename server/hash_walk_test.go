package server

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/hoarddb/hoarddb-server/storage"
)

// hashWalkBucketSizes are the hash bucket sizes the walk benchmarks measure:
// the scale the db-comparison bench and the earlier walk fixes used, and five times it.
var hashWalkBucketSizes = []int{1000, 5000}

// BenchmarkHashWalk measures the four server paths that walk every document of
// a hash bucket — an unqualified Scan, a filtered Count, Distinct and a
// multi-document Update — over buckets of hashWalkBucketSizes keys.
// The update's filter matches one document, so the walk that finds it, not
// the write of the match, is what the benchmark times.
func BenchmarkHashWalk(b *testing.B) {
	for _, keyCount := range hashWalkBucketSizes {
		s := newBenchServer(b)
		seedIndexedBand(b, s, "app", "band", BucketTypeHash, keyCount)

		b.Run(fmt.Sprintf("scan/keys=%d", keyCount), func(b *testing.B) {
			request := ScanRequest{Database: "app", Bucket: "band"}
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				scanResponse, err := s.Scan(request)
				if err != nil {
					b.Fatalf("Scan: %v", err)
				}
				if scanResponse.Total != int64(keyCount) {
					b.Fatalf("Scan total = %d, want %d", scanResponse.Total, keyCount)
				}
			}
		})

		b.Run(fmt.Sprintf("count/keys=%d", keyCount), func(b *testing.B) {
			request := CountRequest{Database: "app", Bucket: "band", Filter: map[string]interface{}{"group": scanBandGroup}}
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				countResponse, err := s.Count(request)
				if err != nil {
					b.Fatalf("Count: %v", err)
				}
				if countResponse.Count != int64(keyCount) {
					b.Fatalf("Count = %d, want %d", countResponse.Count, keyCount)
				}
			}
		})

		b.Run(fmt.Sprintf("distinct/keys=%d", keyCount), func(b *testing.B) {
			request := DistinctRequest{Database: "app", Bucket: "band", Field: "group"}
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				distinctResponse, err := s.Distinct(request)
				if err != nil {
					b.Fatalf("Distinct: %v", err)
				}
				if len(distinctResponse.Values) != 1 {
					b.Fatalf("Distinct returned %d values, want 1", len(distinctResponse.Values))
				}
			}
		})

		b.Run(fmt.Sprintf("updateMultiple/keys=%d", keyCount), func(b *testing.B) {
			request := UpdateRequest{
				Database: "app",
				Bucket:   "band",
				Multi:    true,
				Filter:   map[string]interface{}{"label": "k0000042"},
				Inc:      map[string]int64{"touched": 1},
			}
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				updateResponse, err := s.Update(request)
				if err != nil || !updateResponse.Success {
					b.Fatalf("Update = %+v, %v", updateResponse, err)
				}
			}
		})
	}
}

// storeCallCounts counts the store calls an engine makes through a countingKV.
type storeCallCounts struct {
	// snapshots is every NewSnapshot an engine asked for: a Badger snapshot on
	// the node's store, the shared reader's snapshot on a read view.
	snapshots atomic.Int64
	// pointReads is every Get on a snapshot the engine was handed.
	pointReads atomic.Int64
	// iteratorSteps is every position an iterator the engine opened moved to:
	// each Seek and each Next.
	iteratorSteps atomic.Int64
}

func (c *storeCallCounts) reset() {
	c.snapshots.Store(0)
	c.pointReads.Store(0)
	c.iteratorSteps.Store(0)
}

// countingKV passes every call to the store it wraps and counts the engine's
// snapshots and point reads.
type countingKV struct {
	storage.KV
	counts *storeCallCounts
}

func (k countingKV) NewSnapshot() storage.KVSnapshot {
	k.counts.snapshots.Add(1)
	return countingSnapshot{KVSnapshot: k.KV.NewSnapshot(), counts: k.counts}
}

type countingSnapshot struct {
	storage.KVSnapshot
	counts *storeCallCounts
}

func (s countingSnapshot) Get(key []byte) ([]byte, error) {
	s.counts.pointReads.Add(1)
	return s.KVSnapshot.Get(key)
}

func (s countingSnapshot) NewIterator(opts storage.IterOptions) storage.KVIterator {
	return countingIterator{KVIterator: s.KVSnapshot.NewIterator(opts), counts: s.counts}
}

type countingIterator struct {
	storage.KVIterator
	counts *storeCallCounts
}

func (it countingIterator) Seek(key []byte) {
	it.counts.iteratorSteps.Add(1)
	it.KVIterator.Seek(key)
}

func (it countingIterator) Next() {
	it.counts.iteratorSteps.Add(1)
	it.KVIterator.Next()
}

// countedBucket returns engines for the served bucket database.name built
// over kv wrapped in a countingKV, the way buildEngines builds them for
// the bucket itself (kv is the node's store) or for a read view (kv is a
// snapshotKV).
func countedBucket(t *testing.T, s *Server, database, name string, kv storage.KV, counts *storeCallCounts) *inMemoryBucket {
	t.Helper()
	served, err := s.resolveBucket(database, name)
	if err != nil {
		t.Fatalf("resolving %s.%s: %v", database, name, err)
	}
	counted := &inMemoryBucket{rec: served.rec, epoch: served.epoch, bucketType: served.bucketType, slot: served.slot, databaseID: storage.LegacyDatabaseID("app"), fieldTypes: served.fieldTypes}
	counted.decl.Store(served.decl.Load())
	counted.id.Store(served.id.Load())
	if err := s.buildEngines(counted, countingKV{KV: kv, counts: counts}, counted.slot, counted.id.Load()); err != nil {
		t.Fatalf("building counted engines for %s.%s: %v", database, name, err)
	}
	return counted
}

// TestHashWalksOpenOneSnapshot holds the four server paths that walk a whole
// hash bucket to one snapshot and no per-key read, whatever the bucket's size.
// Scan, Count and Distinct read a view of one snapshot, where each
// per-key read was a point lookup on it; a multi-document Update walks the
// bucket itself, where each was a fresh snapshot — twice per match.
func TestHashWalksOpenOneSnapshot(t *testing.T) {
	const keyCount = 50
	s := newTestServer(t)
	createTestBucket(t, s, "app", "walked", BucketTypeHash)
	for seq := 0; seq < keyCount; seq++ {
		insertScanDoc(t, s, "app", "walked", fmt.Sprintf("k%03d", seq), map[string]interface{}{"seq": int32(seq), "group": "all"})
	}
	matchAll := map[string]interface{}{"group": "all"}

	// pointReads is what a site may read besides its iterator, whatever the
	// bucket's size: an unfiltered scan reads the bucket's state document
	// once for Total instead of counting by a full walk.
	readSites := []struct {
		name       string
		pointReads int64
		walk       func(view *inMemoryBucket) (int64, error)
	}{
		{"scan", 1, func(view *inMemoryBucket) (int64, error) {
			scanResponse, err := scanRawPage(view, ScanRequest{Database: "app", Bucket: "walked"}, 0, defaultScanLimit, testBudget(queryMethodScan), testWalk(queryMethodScan))
			if err != nil {
				return 0, err
			}
			return scanResponse.Total, nil
		}},
		{"count", 0, func(view *inMemoryBucket) (int64, error) {
			return countFilteredEntries(view, matchAll, testWalk(queryMethodCount))
		}},
		{"distinct", 0, func(view *inMemoryBucket) (int64, error) {
			values, err := distinctValues(view, DistinctRequest{Database: "app", Bucket: "walked", Field: "seq"}, testBudget(queryMethodDistinct), testWalk(queryMethodDistinct))
			return int64(len(values)), err
		}},
	}
	for _, site := range readSites {
		t.Run(site.name, func(t *testing.T) {
			snap := s.kv.NewSnapshot()
			defer snap.Discard()
			counts := &storeCallCounts{}
			view := countedBucket(t, s, "app", "walked", snapshotKV{snap: snap}, counts)

			counts.reset()
			walked, err := site.walk(view)
			if err != nil {
				t.Fatalf("%s: %v", site.name, err)
			}
			if walked != keyCount {
				t.Fatalf("%s saw %d documents, want %d", site.name, walked, keyCount)
			}
			if got := counts.snapshots.Load(); got != 1 {
				t.Errorf("%s over %d keys asked for %d snapshots, want 1 for the whole walk", site.name, keyCount, got)
			}
			if got := counts.pointReads.Load(); got != site.pointReads {
				t.Errorf("%s over %d keys made %d point reads, want %d: one iterator reads every document", site.name, keyCount, got, site.pointReads)
			}
		})
	}

	t.Run("updateMultiple", func(t *testing.T) {
		counts := &storeCallCounts{}
		live := countedBucket(t, s, "app", "walked", s.kv, counts)

		counts.reset()
		// updateMultiple's caller holds the bucket's lock (lockBucket).
		live.rec.mu.Lock()
		updateResponse, err := s.updateMultiple(context.Background(), live, UpdateRequest{
			Database: "app", Bucket: "walked", Multi: true, Filter: matchAll, Inc: map[string]int64{"touched": 1},
		})
		live.rec.mu.Unlock()
		if err != nil || !updateResponse.Success {
			t.Fatalf("updateMultiple = %+v, %v", updateResponse, err)
		}
		if want := multiUpdateMessage(keyCount); updateResponse.Message != want {
			t.Fatalf("updateMultiple message = %q, want %q", updateResponse.Message, want)
		}
		// One snapshot for the walk and one for the apply that writes every
		// match in a batch.
		if got := counts.snapshots.Load(); got != 2 {
			t.Errorf("a multi-update of %d matches opened %d snapshots, want 2: one for the walk, one for the write", keyCount, got)
		}

		for seq := 0; seq < keyCount; seq++ {
			key := fmt.Sprintf("k%03d", seq)
			found, err := s.Find(FindRequest{Database: "app", Bucket: "walked", Key: key})
			if err != nil || !found.Found {
				t.Fatalf("Find(%q) = %+v, %v", key, found, err)
			}
			if touched := found.Data.Lookup("touched").AsInt64(); touched != 1 {
				t.Errorf("%s has touched = %d after the update, want 1", key, touched)
			}
		}
	})
}
