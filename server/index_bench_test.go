package server

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The index and TTL measurements (GAP-14 build doc Step 21). A cost claim
// arrives with the experiment that settles it. The benchmarks run under
// -bench; the measurements that seed a large bucket run only when
// HOARDDB_INDEX_MEASUREMENTS is set, and log their numbers:
//
//	HOARDDB_INDEX_MEASUREMENTS=1 go test -run 'TestMeasure' -v ./server/
//	go test -run '^$' -bench 'BenchmarkInsertWithIndexes|BenchmarkSearch' ./server/

// measurementSize reads the document count a measurement seeds, skipping the
// test unless measurements are asked for.
func measurementSize(t *testing.T, fallback int) int {
	t.Helper()
	if os.Getenv("HOARDDB_INDEX_MEASUREMENTS") == "" {
		t.Skip("set HOARDDB_INDEX_MEASUREMENTS=1 to run the index measurements")
	}
	if size, err := strconv.Atoi(os.Getenv("HOARDDB_INDEX_MEASUREMENTS_DOCS")); err == nil && size > 0 {
		return size
	}
	return fallback
}

// seedDocuments writes count documents through batch inserts of 1000.
func seedDocuments(tb testing.TB, s *Server, database, bucket string, count int, document func(i int) map[string]interface{}) {
	tb.Helper()
	for start := 0; start < count; start += 1000 {
		var batch []map[string]interface{}
		for i := start; i < start+1000 && i < count; i++ {
			doc := document(i)
			doc["_id_"] = fmt.Sprintf("d%08d", i)
			batch = append(batch, doc)
		}
		if resp, err := s.BatchInsert(BatchInsertRequest{Database: database, Bucket: bucket, Data: batch}); err != nil || !resp.Success {
			tb.Fatalf("seeding at %d: %+v, %v", start, resp, err)
		}
	}
}

// BenchmarkInsertWithIndexes: insert throughput with 0, 1, 3 and 12 indexes,
// and with one unique index — the per-write cost §6.2 reasons about (one
// decode, k entries, one bounded seek per unique index).
func BenchmarkInsertWithIndexes(b *testing.B) {
	declarations := []struct {
		name    string
		indices []interface{}
	}{
		{"0-indexes", nil},
		{"1-index", []interface{}{"f0"}},
		{"1-unique", []interface{}{map[string]interface{}{"field": "f0", "unique": true}}},
		{"3-indexes", []interface{}{"f0", "f1", "f2"}},
		{"12-indexes", []interface{}{"f0", "f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9", "f10", "f11"}},
	}
	for _, declaration := range declarations {
		b.Run(declaration.name, func(b *testing.B) {
			s := newIndexBenchServer(b)
			options := map[string]interface{}{}
			if declaration.indices != nil {
				options["indices"] = declaration.indices
			}
			if resp, err := s.CreateBucket(CreateBucketRequest{Database: "bench", Name: "b", Type: BucketTypeHash, Options: options}); err != nil || !resp.Success {
				b.Fatalf("CreateBucket = %+v, %v", resp, err)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				doc := map[string]interface{}{}
				for f := 0; f < 12; f++ {
					doc[fmt.Sprintf("f%d", f)] = fmt.Sprintf("value-%d-%d", f, i)
				}
				if resp, err := s.Insert(InsertRequest{Database: "bench", Bucket: "b", Key: fmt.Sprintf("k%09d", i), Data: doc}); err != nil || !resp.Success {
					b.Fatalf("Insert = %+v, %v", resp, err)
				}
			}
		})
	}
}

// BenchmarkSearchCompoundIndex: search on (A, B) served by a compound index
// against the same search served by a single-field index on A with B
// re-applied to each hit — the path the intersection of two single-field
// indexes was replaced by.
func BenchmarkSearchCompoundIndex(b *testing.B) {
	for _, shape := range []struct {
		name    string
		indices []interface{}
	}{
		{"compound-A-B", []interface{}{map[string]interface{}{"fields": []interface{}{"A", "B"}}}},
		{"single-A-residual-B", []interface{}{"A"}},
	} {
		b.Run(shape.name, func(b *testing.B) {
			s := newIndexBenchServer(b)
			if resp, err := s.CreateBucket(CreateBucketRequest{Database: "bench", Name: "b", Type: BucketTypeHash,
				Options: map[string]interface{}{"indices": shape.indices}}); err != nil || !resp.Success {
				b.Fatalf("CreateBucket = %+v, %v", resp, err)
			}
			seedDocuments(b, s, "bench", "b", 20000, func(i int) map[string]interface{} {
				return map[string]interface{}{"A": fmt.Sprintf("a%02d", i%20), "B": fmt.Sprintf("b%03d", i%1000)}
			})
			filter := map[string]interface{}{"A": "a07", "B": "b007"}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				resp, err := s.Search(SearchRequest{Database: "bench", Bucket: "b", Filter: filter})
				if err != nil || resp.Total != 20 {
					b.Fatalf("Search = %+v, %v", resp, err)
				}
			}
		})
	}
}

// newIndexBenchServer is newTestServer for a benchmark.
func newIndexBenchServer(b *testing.B) *Server {
	b.Helper()
	cfg, err := config.Load("")
	if err != nil {
		b.Fatalf("config.Load: %v", err)
	}
	cfg.Storage.DataDir = b.TempDir()
	cfg.Server.ReplicationFactor = 1
	qs, err := NewService(cfg, discardLogger())
	if err != nil {
		b.Fatalf("NewService: %v", err)
	}
	b.Cleanup(func() { _ = qs.Stop() })
	if _, err := qs.core.CreateDatabase("bench"); err != nil {
		b.Fatalf("CreateDatabase: %v", err)
	}
	return qs.core
}

// TestMeasureUpgradeRebuildRate: the upgrade rebuild's documents per second
// (Step 4), over a bucket seeded under a pre-build index.
func TestMeasureUpgradeRebuildRate(t *testing.T) {
	count := measurementSize(t, 1_000_000)
	dataDir := t.TempDir()
	cs := startCatalogServerAt(t, dataDir)
	s := cs.core
	if _, err := s.CreateDatabase("app"); err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}
	if resp, err := s.CreateBucket(CreateBucketRequest{Database: "app", Name: "users", Type: BucketTypeHash,
		Options: map[string]interface{}{"indices": []interface{}{"Email"}}}); err != nil || !resp.Success {
		t.Fatalf("CreateBucket = %+v, %v", resp, err)
	}
	seedDocuments(t, s, "app", "users", count, func(i int) map[string]interface{} {
		if i%3 == 0 {
			return map[string]interface{}{"n": i}
		}
		return map[string]interface{}{"n": i, "Email": fmt.Sprintf("u%d@example.com", i)}
	})
	writeCatalogIndices(t, s, "app", "users", []bson.M{{"field": "Email", "type": "equality"}})
	cs.stop(t)
	reopened := startCatalogServerAt(t, dataDir)
	started := time.Now()
	if err := reopened.qs.LoadCatalog(); err != nil {
		t.Fatalf("LoadCatalog: %v", err)
	}
	elapsed := time.Since(started)
	if stored := storedCatalogIndices(t, reopened.core, "app", "users"); stored[0]["version"] != int32(2) {
		t.Fatalf("the index was not rebuilt: %v", stored)
	}
	t.Logf("MEASURE upgrade rebuild: %d documents in %v = %.0f documents/s (start blocked for the duration)",
		count, elapsed, float64(count)/elapsed.Seconds())
}

// TestMeasureSweepDrainRateStandalone: the sweep's sustained drain rate on
// one node (Step 17).
func TestMeasureSweepDrainRateStandalone(t *testing.T) {
	count := measurementSize(t, 20_000)
	s := newTestServer(t)
	now := time.Now()
	createUniqueBucket(t, s, "app", "sessions", ttlDeclaration("CreatedAt", 3600))
	seedDocuments(t, s, "app", "sessions", count, func(int) map[string]interface{} {
		return map[string]interface{}{"CreatedAt": primitive.NewDateTimeFromTime(now.Add(-2 * time.Hour))}
	})
	started := time.Now()
	deleted := 0
	for {
		pass := s.sweepTTL(context.Background(), now)
		deleted += pass.deleted
		if !pass.exhausted {
			break
		}
	}
	elapsed := time.Since(started)
	if left := countOf(t, s, "sessions"); left != 0 {
		t.Fatalf("%d documents left after draining", left)
	}
	t.Logf("MEASURE sweep drain, standalone: %d expiries in %v = %.0f documents/s", deleted, elapsed, float64(deleted)/elapsed.Seconds())
}

// TestMeasureSweepDrainRateRF3: the same on the owner of a three-node RF 3
// cluster, where each expiry is appended, applied and shipped.
func TestMeasureSweepDrainRateRF3(t *testing.T) {
	count := measurementSize(t, 5_000)
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	now := time.Now()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: "sessions", Type: BucketTypeHash,
		Options: map[string]interface{}{"indices": []interface{}{ttlDeclaration("CreatedAt", 3600)}}})
	for start := 0; start < count; start += 500 {
		var batch []map[string]interface{}
		for i := start; i < start+500 && i < count; i++ {
			batch = append(batch, map[string]interface{}{"_id_": fmt.Sprintf("s%07d", i),
				"CreatedAt": primitive.NewDateTimeFromTime(now.Add(-2 * time.Hour))})
		}
		tc.ok(0, transport.OpBatchPut, BatchInsertRequest{Database: testDatabase, Bucket: "sessions", Data: batch})
	}
	owner, err := tc.nodes[0].server.core.cluster.Owner(placementOf(testDatabase, "sessions"))
	if err != nil {
		t.Fatalf("Owner: %v", err)
	}
	ownerCore := tc.nodes[nodeIndexByAddr(t, tc.addrs, owner.ID)].server.core
	started := time.Now()
	deleted := 0
	for {
		pass := ownerCore.sweepTTL(context.Background(), now)
		deleted += pass.deleted
		if !pass.exhausted {
			break
		}
	}
	elapsed := time.Since(started)
	t.Logf("MEASURE sweep drain, RF 3 owner: %d expiries in %v = %.0f documents/s", deleted, elapsed, float64(deleted)/elapsed.Seconds())
}

// TestMeasureStoreAfterMassExpiry: whether the node store shrinks after every
// document of a bucket expires, and on what horizon (spec §6.6, Q6). It
// samples BadgerKV.Size across the horizon; nothing here forces a compaction.
func TestMeasureStoreAfterMassExpiry(t *testing.T) {
	count := measurementSize(t, 200_000)
	s := newTestServer(t)
	kv, ok := s.kv.(*storage.BadgerKV)
	if !ok {
		t.Skipf("the node store is %T, not Badger", s.kv)
	}
	sample := func(label string) {
		lsm, vlog, err := kv.Size()
		if err != nil {
			t.Fatalf("Size: %v", err)
		}
		t.Logf("MEASURE store size %s: lsm %d B, vlog %d B", label, lsm, vlog)
	}
	now := time.Now()
	createUniqueBucket(t, s, "app", "sessions", ttlDeclaration("CreatedAt", 3600))
	sample("empty")
	seedDocuments(t, s, "app", "sessions", count, func(i int) map[string]interface{} {
		return map[string]interface{}{"CreatedAt": primitive.NewDateTimeFromTime(now.Add(-2 * time.Hour)), "Payload": fmt.Sprintf("%0200d", i)}
	})
	time.Sleep(time.Minute)
	sample(fmt.Sprintf("after %d documents (+60 s)", count))
	for {
		if pass := s.sweepTTL(context.Background(), now); !pass.exhausted {
			break
		}
	}
	for _, wait := range []time.Duration{0, time.Minute, 2 * time.Minute} {
		time.Sleep(wait)
		sample(fmt.Sprintf("after the mass expiry (+%v)", wait))
	}
}
