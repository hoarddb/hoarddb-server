package server

import (
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"testing"

	"github.com/hoarddb/hoarddb-server/config"
)

// BenchmarkStoreTypes compares the six store engines through the same service
// layer the QUIC handlers call, so what it measures is the engine's own cost:
// no transport, no cluster routing, no client.
//
// Run it on real disk, not tmpfs — the queue and replicated paths fsync, and
// /tmp on this host is a tmpfs where fsync is nearly free:
//
//	TMPDIR=/path/on/a/real/disk go test -run XXX -bench BenchmarkStore -benchtime=1s ./server/
//
// Semantics differ between types and the numbers must be read with that in
// mind: Find is a keyed lookup, Peek is "look at the next queue/heap element",
// and for blob every entry is a file. docs/s on the batch benchmark is the
// only column directly comparable across all five batchable types.

var benchStoreTypes = []struct {
	name  string
	btype BucketType
}{
	{"hash", BucketTypeHash},
	{"btree", BucketTypeBTree},
	{"fifo", BucketTypeFIFO},
	{"lifo", BucketTypeLIFO},
	{"heap", BucketTypeHeap},
	{"blob", BucketTypeBlob},
}

func newBenchServer(b *testing.B) *Server {
	b.Helper()

	cfg, err := config.Load("")
	if err != nil {
		b.Fatalf("loading default config: %v", err)
	}
	cfg.Storage.DataDir = b.TempDir()
	cfg.Server.ReplicationFactor = 1

	qs, err := NewService(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		b.Fatalf("creating server: %v", err)
	}
	b.Cleanup(func() {
		if err := qs.Stop(); err != nil {
			b.Logf("stopping server: %v", err)
		}
	})
	return qs.core
}

func benchDoc(key string) map[string]interface{} {
	return map[string]interface{}{
		"_id_":     key,
		"key":      key,
		"username": key,
		"email":    key + "@test.com",
	}
}

// BenchmarkStoreSingleInsert writes one document per operation: the cost of a
// single keyed write, which is where the per-operation overheads (a Badger
// transaction, a segment append plus fsync) show up without batching to hide
// them.
func BenchmarkStoreSingleInsert(b *testing.B) {
	for _, st := range benchStoreTypes {
		b.Run(st.name, func(b *testing.B) {
			s := newBenchServer(b)
			createTestBucket(b, s, "app", "b", st.btype)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				key := fmt.Sprintf("k%08d", i)
				if _, err := s.Insert(InsertRequest{
					Database: "app",
					Bucket:   "b",
					Key:      key,
					Data:     benchDoc(key),
				}); err != nil {
					b.Fatalf("insert: %v", err)
				}
			}
		})
	}
}

// BenchmarkStoreBatchInsert writes 1000 documents per operation, which is the
// shape the benchmark harness uses. Blob buckets have no batch path (each blob
// is a file), so blob is absent here rather than measured differently.
func BenchmarkStoreBatchInsert(b *testing.B) {
	const batch = 1000
	for _, st := range benchStoreTypes {
		if st.btype == BucketTypeBlob {
			continue
		}
		b.Run(st.name, func(b *testing.B) {
			s := newBenchServer(b)
			createTestBucket(b, s, "app", "b", st.btype)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				docs := make([]map[string]interface{}, batch)
				for j := range docs {
					key := fmt.Sprintf("k%09d", i*batch+j)
					docs[j] = benchDoc(key)
				}
				if _, err := s.BatchInsert(BatchInsertRequest{
					Database: "app",
					Bucket:   "b",
					Data:     docs,
				}); err != nil {
					b.Fatalf("batch insert: %v", err)
				}
			}
			if elapsed := b.Elapsed().Seconds(); elapsed > 0 {
				b.ReportMetric(float64(b.N*batch)/elapsed, "docs/s")
			}
		})
	}
}

// BenchmarkStoreRead reads from a pre-loaded bucket: a keyed Find where the
// type supports keys, a Peek where it does not.
func BenchmarkStoreRead(b *testing.B) {
	const docs = 5000
	for _, st := range benchStoreTypes {
		b.Run(st.name, func(b *testing.B) {
			s := newBenchServer(b)
			createTestBucket(b, s, "app", "b", st.btype)

			for i := 0; i < docs; i++ {
				key := fmt.Sprintf("k%08d", i)
				if _, err := s.Insert(InsertRequest{
					Database: "app",
					Bucket:   "b",
					Key:      key,
					Data:     benchDoc(key),
				}); err != nil {
					b.Fatalf("preload insert: %v", err)
				}
			}

			randKey := rand.New(rand.NewSource(1))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				switch st.btype {
				case BucketTypeHash, BucketTypeBTree, BucketTypeBlob:
					key := fmt.Sprintf("k%08d", randKey.Intn(docs))
					resp, err := s.Find(FindRequest{Database: "app", Bucket: "b", Key: key})
					if err != nil {
						b.Fatalf("find: %v", err)
					}
					if !resp.Found {
						b.Fatalf("find %s: document missing", key)
					}
				default:
					resp, err := s.Peek(PeekRequest{Database: "app", Bucket: "b"})
					if err != nil {
						b.Fatalf("peek: %v", err)
					}
					if !resp.Found {
						b.Fatalf("peek: empty %s bucket", st.name)
					}
				}
			}
		})
	}
}
