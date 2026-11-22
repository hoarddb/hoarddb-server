package server

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"unsafe"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
)

// The enforcement's cost per write (rule 11; build doc TF-6 step 6.1): what
// an insert pays for the check on a bucket that declares nothing, and on one
// that declares five of a 20-field document's fields.

// enforcementServer is a standalone node holding app.plain, undeclared, and
// app.typed, declaring the first five of benchFields in strict.
func enforcementServer(tb testing.TB) *Server {
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
	tb.Cleanup(func() {
		if err := qs.Stop(); err != nil {
			tb.Logf("stopping server: %v", err)
		}
	})
	s := qs.core
	if _, err := s.CreateDatabase("app"); err != nil {
		tb.Fatalf("CreateDatabase: %v", err)
	}
	fields := bson.A{}
	for _, field := range benchFields[:5] {
		fields = append(fields, bson.M{"name": field.name, "type": field.word})
	}
	for name, options := range map[string]map[string]interface{}{"plain": nil, "typed": {"fields": fields}} {
		resp, err := s.CreateBucket(CreateBucketRequest{Database: "app", Name: name, Type: BucketTypeHash, Options: options})
		if err != nil || !resp.Success {
			tb.Fatalf("creating %s: %+v %v", name, resp, err)
		}
	}
	return s
}

var enforcementSink error

// BenchmarkInsertFieldTypeCheck is the check a handler runs, on the bucket
// it holds: the declaration's load and the nil test on an undeclared bucket,
// and the five-field check of a conforming document on a typed one.
func BenchmarkInsertFieldTypeCheck(b *testing.B) {
	s := enforcementServer(b)
	document := benchDocument(b)
	ctx := context.Background()
	for _, name := range []string{"plain", "typed"} {
		bucket := servedBucketTB(b, s, "app", name)
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				enforcementSink = s.refuseMistyped(ctx, bucket.declaredFieldTypes(), bucket, "app", name, document)
			}
		})
	}
}

// BenchmarkInsertDeclaredVsUndeclared is a whole insert of the 20-field
// document, each under a new key, into the undeclared and the typed bucket:
// the check against everything else an insert does.
func BenchmarkInsertDeclaredVsUndeclared(b *testing.B) {
	s := enforcementServer(b)
	for _, name := range []string{"plain", "typed"} {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for i := range b.N {
				document := benchDocument(b)
				resp, err := s.Insert(InsertRequest{Database: "app", Bucket: name, Key: fmt.Sprintf("k%d", i), Data: document})
				if err != nil || !resp.Success {
					b.Fatalf("insert: %+v %v", resp, err)
				}
			}
		})
	}
}

// TestUndeclaredBucketPaysNoEnforcementAllocation (spec §9.1, §9.2): the
// handler's check allocates nothing on a bucket that declares nothing, nor
// for a conforming document in a typed one (the one-document slice the check
// walks stays on the stack).
func TestUndeclaredBucketPaysNoEnforcementAllocation(t *testing.T) {
	s := enforcementServer(t)
	document := benchDocument(t)
	ctx := context.Background()
	plain, typed := servedBucket(t, s, "app", "plain"), servedBucket(t, s, "app", "typed")
	if allocations := minAllocs(7, 1000, func() {
		enforcementSink = s.refuseMistyped(ctx, plain.declaredFieldTypes(), plain, "app", "plain", document)
	}); allocations != 0 {
		t.Errorf("the check on an undeclared bucket allocated %v times per write, want 0", allocations)
	}
	if allocations := minAllocs(7, 1000, func() {
		enforcementSink = s.refuseMistyped(ctx, typed.declaredFieldTypes(), typed, "app", "typed", document)
	}); allocations != 0 {
		t.Errorf("the check of a conforming document allocated %v times per write, want 0", allocations)
	}
}

// TestTypedBucketHolderIsTwoPointers (rule 11): every declared bucket holds
// one holder, so what TF-6 adds for the rare unenforceable declaration (its
// recorded fields and the validator's refusal) sits behind one pointer that
// is nil for every other bucket, and the holder stays in the 16-byte size
// class: 16 MB at a million declared buckets, not 48 MB.
func TestTypedBucketHolderIsTwoPointers(t *testing.T) {
	if size := unsafe.Sizeof(bucketFieldTypes{}); size != 16 {
		t.Errorf("a declared bucket's holder is %d bytes; want 16, the schema and one pointer for an unenforceable declaration", size)
	}
}

// servedBucketTB is servedBucket for a benchmark.
func servedBucketTB(tb testing.TB, s *Server, database, name string) *inMemoryBucket {
	tb.Helper()
	bucket, err := s.resolveBucket(database, name)
	if err != nil {
		tb.Fatalf("resolving %s/%s: %v", database, name, err)
	}
	return bucket
}
