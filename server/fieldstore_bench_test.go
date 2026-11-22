package server

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

// Nested buckets step 16 (spec §15.1; S-3). None of these numbers is a
// performance claim until reviewed.

// payloadOf is an element document of about size bytes.
func payloadOf(size int) map[string]interface{} {
	return map[string]interface{}{"pad": strings.Repeat("x", max(size-20, 1))}
}

// fillField pushes n elements of size bytes onto document key's Comments
// through the store, one apply each: the state a push reads is the
// snapshot's, so a batch holds one push.
func fillField(tb testing.TB, s *Server, key string, n, size int) {
	tb.Helper()
	b := lookupBucket(s, "app", "Post")
	owner, err := store.FieldOwnerPrefix(b.slot, b.databaseID, b.id.Load(), key)
	if err != nil {
		tb.Fatalf("owner: %v", err)
	}
	comments, _ := b.storeFields().find("Comments", 0)
	ref := store.FieldRef{Owner: owner, FieldID: uint16(comments.ID), Engine: store.FieldFIFO}
	element, _ := bson.Marshal(payloadOf(size))
	for range n {
		_, err := store.Apply(store.ApplyRequest{KV: s.kv, Slot: b.slot, DatabaseID: b.databaseID, ID: b.id.Load(), Owed: b.hashStore.Carry(),
			Op: func(snap storage.KVSnapshot, st *store.BucketState, batch storage.KVBatch) error {
				_, err := store.FieldPush(store.FieldWrite{Snap: snap, State: st, Batch: batch}, ref, element)
				return err
			}})
		if err != nil {
			tb.Fatalf("filling %s: %v", key, err)
		}
	}
}

// benchServer is newTestServer for a benchmark.
func benchServer(tb testing.TB) *Server {
	tb.Helper()
	cfg, err := config.Load("")
	if err != nil {
		tb.Fatalf("loading default config: %v", err)
	}
	cfg.Storage.DataDir = tb.TempDir()
	cfg.Server.ReplicationFactor = 1
	qs, err := NewService(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		tb.Fatalf("creating server: %v", err)
	}
	tb.Cleanup(func() { _ = qs.Stop() })
	return qs.core
}

// BenchmarkFieldQueueVsBucketQueue is spec §15.1's own request: one push
// and one pop of a 100-byte element on a fifo field and on a fifo bucket,
// each already holding n elements.
func BenchmarkFieldQueueVsBucketQueue(b *testing.B) {
	for _, n := range []int{100, 10_000, 1_000_000} {
		b.Run(fmt.Sprintf("field/n=%d", n), func(b *testing.B) {
			s := benchServer(b)
			seedBenchPost(b, s)
			fillField(b, s, "p1", n, 100)
			ctx := context.Background()
			push := FieldRequest{Database: "app", Bucket: "Post", Key: "p1", Field: "Comments", Data: payloadOf(100)}
			pop := FieldRequest{Database: "app", Bucket: "Post", Key: "p1", Field: "Comments"}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, err := s.FieldPushCtx(ctx, push); err != nil {
					b.Fatal(err)
				}
				if _, err := s.FieldPopCtx(ctx, FieldPopRequest{FieldRequest: pop}); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(fmt.Sprintf("bucket/n=%d", n), func(b *testing.B) {
			s := benchServer(b)
			createTestBucket(b, s, "app", "Queue", BucketTypeFIFO)
			queue := lookupBucket(s, "app", "Queue").queueStore
			element, _ := bson.Marshal(payloadOf(100))
			for filled := 0; filled < n; filled += 1000 {
				batch := make([][]byte, 0, 1000)
				for range min(1000, n-filled) {
					batch = append(batch, element)
				}
				if err := queue.PushBatch(store.ApplyInfo{}, batch); err != nil {
					b.Fatal(err)
				}
			}
			ctx := context.Background()
			push := PushRequest{Database: "app", Bucket: "Queue", Data: payloadOf(100)}
			pop := PopRequest{Database: "app", Bucket: "Queue"}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, err := s.PushCtx(ctx, push); err != nil {
					b.Fatal(err)
				}
				if _, err := s.PopCtx(ctx, pop); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// seedBenchPost creates app.Post declaring Comments (fifo) with p1.
func seedBenchPost(tb testing.TB, s *Server) {
	tb.Helper()
	if _, err := s.CreateDatabase("app"); err != nil {
		tb.Fatal(err)
	}
	resp, err := s.CreateBucket(CreateBucketRequest{Database: "app", Name: "Post", Type: BucketTypeHash,
		Options: storesOption(StoreDeclaration{Name: "Comments", Engine: "fifoStore"})})
	if err != nil || !resp.Success {
		tb.Fatalf("creating Post: %+v, %v", resp, err)
	}
	if _, err := s.Insert(InsertRequest{Database: "app", Bucket: "Post", Key: "p1", Data: map[string]interface{}{"title": "p1"}}); err != nil {
		tb.Fatal(err)
	}
}

// BenchmarkDeleteCascade is a document delete holding n field elements, and
// the unconditional cascade's cost on a bucket that declares no store (S-3).
func BenchmarkDeleteCascade(b *testing.B) {
	for _, n := range []int{0, 100, 10_000} {
		b.Run(fmt.Sprintf("elements=%d", n), func(b *testing.B) {
			s := benchServer(b)
			seedBenchPost(b, s)
			ctx := context.Background()
			b.ReportAllocs()
			for i := range b.N {
				b.StopTimer()
				key := fmt.Sprintf("d%d", i)
				if _, err := s.Insert(InsertRequest{Database: "app", Bucket: "Post", Key: key, Data: map[string]interface{}{"title": key}}); err != nil {
					b.Fatal(err)
				}
				fillField(b, s, key, n, 100)
				b.StartTimer()
				if resp, err := s.DeleteCtx(ctx, DeleteRequest{Database: "app", Bucket: "Post", Key: key}); err != nil || !resp.Success {
					b.Fatalf("deleting %s: %+v, %v", key, resp, err)
				}
			}
		})
	}
	b.Run("no-declared-stores", func(b *testing.B) {
		s := benchServer(b)
		createTestBucket(b, s, "app", "Plain", BucketTypeHash)
		ctx := context.Background()
		b.ReportAllocs()
		for i := range b.N {
			b.StopTimer()
			key := fmt.Sprintf("d%d", i)
			if _, err := s.Insert(InsertRequest{Database: "app", Bucket: "Plain", Key: key, Data: map[string]interface{}{"title": key}}); err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
			if resp, err := s.DeleteCtx(ctx, DeleteRequest{Database: "app", Bucket: "Plain", Key: key}); err != nil || !resp.Success {
				b.Fatalf("deleting %s: %+v, %v", key, resp, err)
			}
		}
	})
}

// refusedAtKey reads the key a batch refusal names: "reached at key <hex>".
var refusedAtKey = regexp.MustCompile(`reached at key ([0-9a-f]+)`)

// TestMeasureCascadeCeiling finds the largest document, in elements of one
// fifo field, whose delete fits one batch, for 100-byte and 1-KiB elements
// (S-3). An over-full instance's refusal names the key at which the bound was
// reached, which places the ceiling within a few elements (the batch also
// holds the document, its version and the instance state); instances just
// below it are then deleted to find the exact largest. Skipped unless
// HOARDDB_BENCH=1; the number goes to Hermes.
func TestMeasureCascadeCeiling(t *testing.T) {
	if os.Getenv("HOARDDB_BENCH") != "1" {
		t.Skip("measurement: set HOARDDB_BENCH=1 to run")
	}
	for _, size := range []int{100, 1024} {
		s := benchServer(t)
		seedBenchPost(t, s)
		const overFull = 200_000
		fillField(t, s, "p1", overFull, size)
		_, err := s.deleteDocument(context.Background(), lookupBucket(s, "app", "Post"), DeleteRequest{Database: "app", Bucket: "Post", Key: "p1"})
		if !errors.Is(err, storage.ErrBatchTooLarge) {
			t.Fatalf("%d-byte elements: deleting %d = %v; want ErrBatchTooLarge (raise overFull)", size, overFull, err)
		}
		match := refusedAtKey.FindStringSubmatch(err.Error())
		if match == nil {
			t.Fatalf("the refusal names no key: %v", err)
		}
		key, _ := hex.DecodeString(match[1])
		refusedAt := int(binary.BigEndian.Uint64(key[len(key)-8:]))
		largest := -1
		for n := refusedAt - 4; n <= refusedAt; n++ {
			probe := benchServer(t)
			seedBenchPost(t, probe)
			fillField(t, probe, "p1", n, size)
			resp, err := probe.DeleteCtx(context.Background(), DeleteRequest{Database: "app", Bucket: "Post", Key: "p1"})
			if err != nil {
				t.Fatalf("deleting %d elements: %v", n, err)
			}
			if resp.Success {
				largest = n
			}
		}
		t.Logf("%d-byte elements: a document whose one fifo field holds %d elements deletes, %d is refused (the refusal of an over-full instance names position %d); batch bound %d bytes",
			size, largest, largest+1, refusedAt, s.kv.MaxBatchBytes())
	}
}
