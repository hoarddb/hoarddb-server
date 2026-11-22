package ringclient_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/ringclient"
	"github.com/hoarddb/hoarddb-server/server"
)

// The live benchmarks drive the real request path: the TLS listener, one pipelined
// connection from the ring client, the ring handler and the node. Many callers share the
// one connection, so the server runs many frames at once, as it does under load. They
// exist to measure the per-frame cost of dispatching (goroutine start, stack growth), and
// are compared with themselves on two commits, never with another engine.

// benchLiveBucket starts the real server in process and returns a bucket with benchKeys
// documents written.
func benchLiveBucket(b *testing.B, benchKeys int) *ringclient.Bucket {
	b.Helper()
	reserved, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	addr := reserved.Addr().String()
	if err := reserved.Close(); err != nil {
		b.Fatal(err)
	}
	cfg := config.DefaultConfig()
	cfg.Storage.DataDir = b.TempDir()
	cfg.Server.Listen = "127.0.0.1:0"
	cfg.TCP.Listen = addr
	service, err := server.NewService(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		b.Fatal(err)
	}
	if err := service.SeedRootUser("root", "live-bench-password"); err != nil {
		b.Fatal(err)
	}
	if err := service.Start(); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = service.Stop() })

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	b.Cleanup(cancel)
	client, err := ringclient.Dial(ctx, ringclient.Options{
		Addr: addr, User: "root", Password: "live-bench-password", Insecure: true,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = client.Close() })
	if _, err := client.CreateDatabase(ctx, "app", 0); err != nil {
		b.Fatal(err)
	}
	if _, err := client.CreateBucket(ctx, "app", "User", []ringclient.FieldDecl{
		{Name: "Name", Type: "string"}, {Name: "Age", Type: "int"},
	}); err != nil {
		b.Fatal(err)
	}
	users := client.Bucket("app", "User")
	for i := 0; i < benchKeys; i++ {
		if _, err := users.Upsert(ctx, fmt.Sprintf("u%05d", i), ringclient.Doc{"Name": "Ada", "Age": int64(i)}); err != nil {
			b.Fatal(err)
		}
	}
	return users
}

// BenchmarkLiveKeyedRead is one keyed read per op, from 8 callers per P on one connection.
func BenchmarkLiveKeyedRead(b *testing.B) {
	const benchKeys = 1000
	users := benchLiveBucket(b, benchKeys)
	ctx := context.Background()
	var caller atomic.Int64
	b.SetParallelism(8)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		next := int(caller.Add(1)) * 7919
		for pb.Next() {
			if _, err := users.Get(ctx, fmt.Sprintf("u%05d", next%benchKeys)); err != nil {
				b.Error(err)
				return
			}
			next++
		}
	})
}

// BenchmarkLiveKeyedWrite is one keyed upsert per op, from 8 callers per P on one
// connection, each caller on its own keys.
func BenchmarkLiveKeyedWrite(b *testing.B) {
	const benchKeys = 1000
	users := benchLiveBucket(b, benchKeys)
	ctx := context.Background()
	var caller atomic.Int64
	b.SetParallelism(8)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		next := int(caller.Add(1)) * 7919
		for pb.Next() {
			if _, err := users.Upsert(ctx, fmt.Sprintf("u%05d", next%benchKeys), ringclient.Doc{"Name": "Ada L", "Age": int64(next)}); err != nil {
				b.Error(err)
				return
			}
			next++
		}
	})
}
