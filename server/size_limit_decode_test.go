package server

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"

	"github.com/hoarddb/hoarddb-server/transport"
)

// The size limits are read from a write the way its handler decodes it, so
// what the door measures is what the handler stores.

const (
	decodedDocumentLimit = 64 << 10
	decodedBlobCap       = 32 << 10
)

// decodedLimitServer is a standalone node holding one bucket of every type the
// guarded writes reach, and Post.p1 with a fifoStore, a hashStore and a
// blobStore field.
func decodedLimitServer(t *testing.T) (*Server, transport.RequestHandler) {
	t.Helper()
	s := newTestServer(t)
	s.config.Storage.MaxDocumentBytes = decodedDocumentLimit
	s.config.Storage.MaxBlobBytes = decodedBlobCap
	if resp := createWithStores(t, s, "app", "Post", BucketTypeHash,
		StoreDeclaration{Name: "Comments", Engine: "fifoStore"},
		StoreDeclaration{Name: "Tags", Engine: "hashStore"},
		StoreDeclaration{Name: "Photos", Engine: "blobStore"}); !resp.Success {
		t.Fatalf("creating Post: %s", resp.Message)
	}
	if resp, err := s.Insert(InsertRequest{Database: "app", Bucket: "Post", Key: "p1", Data: map[string]interface{}{"title": "p1"}}); err != nil || !resp.Success {
		t.Fatalf("inserting p1: %+v, %v", resp, err)
	}
	for name, kind := range decodedLimitBuckets {
		if resp, err := s.CreateBucket(CreateBucketRequest{Database: "app", Name: name, Type: kind}); err != nil || !resp.Success {
			t.Fatalf("creating %s: %+v, %v", name, resp, err)
		}
	}
	return s, NewHandler(s)
}

var decodedLimitBuckets = map[string]BucketType{
	"docs": BucketTypeHash, "sorted": BucketTypeBTree, "jobs": BucketTypeFIFO,
	"stack": BucketTypeLIFO, "ranked": BucketTypeHeap, "files": BucketTypeBlob,
}

// storedCounts is how many entries each bucket and each of Post.p1's three
// fields holds.
func storedCounts(t *testing.T, s *Server) map[string]int64 {
	t.Helper()
	counts := make(map[string]int64, len(decodedLimitBuckets)+3)
	for name := range decodedLimitBuckets {
		counted, err := s.Count(CountRequest{Database: "app", Bucket: name})
		if err != nil {
			t.Fatalf("counting %s: %v", name, err)
		}
		counts[name] = counted.Count
	}
	for _, field := range []string{"Comments", "Tags", "Photos"} {
		counts["Post.p1."+field] = fieldLen(t, s, "p1", field)
	}
	return counts
}

func filledDocument(size int) bson.D {
	return bson.D{{Key: "body", Value: strings.Repeat("x", size)}}
}

func filledBinary(size int) primitive.Binary {
	return primitive.Binary{Data: make([]byte, size)}
}

// decodedLimitWrite is one guarded write: its opcode, its payload, and the
// text its refusal must carry.
type decodedLimitWrite struct {
	name    string
	op      transport.Opcode
	payload bson.D
	refusal string
}

// guardedWrites is every guarded opcode with its measured field named name
// (data) and contentName (a blobStore element's content).
func guardedWrites(name, contentName string) []decodedLimitWrite {
	over := filledDocument(decodedDocumentLimit + 1024)
	overCap := filledDocument(decodedBlobCap + 1024)
	const documentLimit, blobCap = "storage.max_document_bytes", "storage.max_blob_bytes"
	const blobBucketVerb = "a blob bucket is written with put"
	app := bson.E{Key: "database", Value: "app"}
	return []decodedLimitWrite{
		{"put", transport.OpPut, bson.D{app, {Key: "bucket", Value: "docs"}, {Key: "key", Value: "k"}, {Key: name, Value: over}}, documentLimit},
		{"btree insert", transport.OpBTreeInsert, bson.D{app, {Key: "bucket", Value: "sorted"}, {Key: "key", Value: "k"}, {Key: name, Value: over}}, documentLimit},
		{"fifo push", transport.OpFIFOPush, bson.D{app, {Key: "bucket", Value: "jobs"}, {Key: name, Value: over}}, documentLimit},
		{"lifo push", transport.OpLIFOPush, bson.D{app, {Key: "bucket", Value: "stack"}, {Key: name, Value: over}}, documentLimit},
		{"heap push", transport.OpHeapPush, bson.D{app, {Key: "bucket", Value: "ranked"}, {Key: name, Value: over}, {Key: "priority", Value: int64(1)}}, documentLimit},
		{"batch put", transport.OpBatchPut, bson.D{app, {Key: "bucket", Value: "docs"}, {Key: name, Value: bson.A{over}}}, documentLimit},
		{"field push", transport.OpFieldPush, bson.D{app, {Key: "bucket", Value: "Post"}, {Key: "key", Value: "p1"}, {Key: "field", Value: "Comments"}, {Key: name, Value: over}}, documentLimit},
		{"field put", transport.OpFieldPut, bson.D{app, {Key: "bucket", Value: "Post"}, {Key: "key", Value: "p1"}, {Key: "field", Value: "Tags"}, {Key: "element", Value: "t1"}, {Key: name, Value: over}}, documentLimit},
		{"field put content", transport.OpFieldPut, bson.D{app, {Key: "bucket", Value: "Post"}, {Key: "key", Value: "p1"}, {Key: "field", Value: "Photos"}, {Key: "element", Value: "ph"}, {Key: contentName, Value: filledBinary(decodedBlobCap + 1)}}, blobCap},
		{"blob write", transport.OpBlobWrite, bson.D{app, {Key: "bucket", Value: "files"}, {Key: "key", Value: "f"}, {Key: name, Value: filledBinary(decodedBlobCap + 1)}}, blobCap},
		// A document PUT into a blob bucket is refused as the wrong verb (BD-7), at any size.
		{"document put into a blob bucket", transport.OpPut, bson.D{app, {Key: "bucket", Value: "files"}, {Key: "key", Value: "d"}, {Key: name, Value: overCap}}, blobBucketVerb},
		{"blob write over the cap to the blob bucket", transport.OpBlobWrite, bson.D{app, {Key: "bucket", Value: "files"}, {Key: "key", Value: "d"}, {Key: name, Value: filledBinary(decodedBlobCap + 1024)}}, blobCap},
	}
}

func TestSizeLimitsMeasureTheDecodedPayload(t *testing.T) {
	var writes []decodedLimitWrite
	for spelling, names := range [][2]string{{"data", "content"}, {"Data", "Content"}, {"DATA", "CONTENT"}, {"dAtA", "cOnTeNt"}} {
		for _, write := range guardedWrites(names[0], names[1]) {
			write.name = fmt.Sprintf("%s #%d", write.name, spelling)
			writes = append(writes, write)
		}
	}
	writes = append(writes, repeatedFieldWrites()...)
	writes = append(writes, textValuedBlobWrites()...)
	writes = append(writes, decodedScopeWrites()...)

	s, handler := decodedLimitServer(t)
	for _, write := range writes {
		before := storedCounts(t, s)
		frame, err := handler(context.Background(), transport.NewFrame(write.op, mustMarshalBSON(t, write.payload)))
		if err != nil {
			t.Fatalf("%s: handler error %v", write.name, err)
		}
		if frame.Opcode != transport.OpError {
			t.Errorf("%s: answered %s, want a refusal naming %s", write.name, frame.Opcode, write.refusal)
		} else if message := wireRefusal(t, frame); !strings.Contains(message, write.refusal) {
			t.Errorf("%s: refusal %.300q does not name %s", write.name, message, write.refusal)
		}
		for store, after := range storedCounts(t, s) {
			if after != before[store] {
				t.Errorf("%s: a refused write changed %s from %d to %d entries", write.name, store, before[store], after)
			}
		}
	}
}

// repeatedFieldWrites send the measured field twice, a small value first:
// what is measured must be what the handler keeps.
func repeatedFieldWrites() []decodedLimitWrite {
	small := filledDocument(16)
	over := filledDocument(decodedDocumentLimit + 1024)
	app := bson.E{Key: "database", Value: "app"}
	return []decodedLimitWrite{
		{"repeated put", transport.OpPut, bson.D{app, {Key: "bucket", Value: "docs"}, {Key: "key", Value: "k"}, {Key: "data", Value: small}, {Key: "Data", Value: over}}, "storage.max_document_bytes"},
		{"repeated push", transport.OpFIFOPush, bson.D{app, {Key: "bucket", Value: "jobs"}, {Key: "data", Value: small}, {Key: "DATA", Value: over}}, "storage.max_document_bytes"},
		{"repeated batch", transport.OpBatchPut, bson.D{app, {Key: "bucket", Value: "docs"}, {Key: "data", Value: bson.A{small}}, {Key: "Data", Value: bson.A{over}}}, "storage.max_document_bytes"},
		{"repeated field push", transport.OpFieldPush, bson.D{app, {Key: "bucket", Value: "Post"}, {Key: "key", Value: "p1"}, {Key: "field", Value: "Comments"}, {Key: "data", Value: small}, {Key: "Data", Value: over}}, "storage.max_document_bytes"},
		{"repeated blob write", transport.OpBlobWrite, bson.D{app, {Key: "bucket", Value: "files"}, {Key: "key", Value: "f"}, {Key: "data", Value: filledBinary(16)}, {Key: "Data", Value: filledBinary(decodedBlobCap + 1)}}, "storage.max_blob_bytes"},
		{"repeated field content", transport.OpFieldPut, bson.D{app, {Key: "bucket", Value: "Post"}, {Key: "key", Value: "p1"}, {Key: "field", Value: "Photos"}, {Key: "element", Value: "ph"}, {Key: "content", Value: filledBinary(16)}, {Key: "Content", Value: filledBinary(decodedBlobCap + 1)}}, "storage.max_blob_bytes"},
		// Two parts that are each under the limit, which the handler keeps as one document.
		{"repeated put, both parts kept", transport.OpPut, bson.D{app, {Key: "bucket", Value: "docs"}, {Key: "key", Value: "k"},
			{Key: "data", Value: bson.D{{Key: "a", Value: strings.Repeat("x", decodedDocumentLimit/2+512)}}},
			{Key: "Data", Value: bson.D{{Key: "b", Value: strings.Repeat("x", decodedDocumentLimit/2+512)}}}}, "storage.max_document_bytes"},
	}
}

// textValuedBlobWrites carry blob bytes as a BSON string, which the handlers'
// []byte fields accept.
func textValuedBlobWrites() []decodedLimitWrite {
	text := strings.Repeat("x", decodedBlobCap+1)
	app := bson.E{Key: "database", Value: "app"}
	return []decodedLimitWrite{
		{"blob write as text", transport.OpBlobWrite, bson.D{app, {Key: "bucket", Value: "files"}, {Key: "key", Value: "f"}, {Key: "data", Value: text}}, "storage.max_blob_bytes"},
		{"field content as text", transport.OpFieldPut, bson.D{app, {Key: "bucket", Value: "Post"}, {Key: "key", Value: "p1"}, {Key: "field", Value: "Photos"}, {Key: "element", Value: "ph"}, {Key: "content", Value: text}}, "storage.max_blob_bytes"},
	}
}

func oneKiBPutFrame(b *testing.B) *transport.Frame {
	b.Helper()
	payload, err := bson.Marshal(InsertRequest{Database: "app", Bucket: "docs", Key: "k", Data: map[string]interface{}{"body": strings.Repeat("x", 1<<10)}})
	if err != nil {
		b.Fatal(err)
	}
	return transport.NewFrame(transport.OpPut, payload)
}

// BenchmarkSizeCheckOnAOneKiBPut is the size check's cost on the common write.
func BenchmarkSizeCheckOnAOneKiBPut(b *testing.B) {
	s := &Server{}
	frame := oneKiBPutFrame(b)
	b.ReportAllocs()
	for b.Loop() {
		if _, err := s.refuseOversizedWrite(frame); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkSizeCheckOnAOneMiBBlobWrite is the size check's cost on a blob
// write.
func BenchmarkSizeCheckOnAOneMiBBlobWrite(b *testing.B) {
	s := &Server{}
	payload, err := bson.Marshal(BlobWriteRequest{Database: "app", Bucket: "files", Key: "f", Data: make([]byte, 1<<20)})
	if err != nil {
		b.Fatal(err)
	}
	frame := transport.NewFrame(transport.OpBlobWrite, payload)
	b.ReportAllocs()
	for b.Loop() {
		if _, err := s.refuseOversizedWrite(frame); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPutHandlerDecodeOfAOneKiBPut is the handler's own decode of the
// same write, for scale.
func BenchmarkPutHandlerDecodeOfAOneKiBPut(b *testing.B) {
	frame := oneKiBPutFrame(b)
	b.ReportAllocs()
	for b.Loop() {
		var req InsertRequest
		if err := bson.Unmarshal(frame.Payload, &req); err != nil {
			b.Fatal(err)
		}
	}
}

// decodedScopeWrites name the bucket and the key the way the handler reads
// them: the blob write needs the bucket to know it is a blob, and the
// replication arithmetic needs the key's length.
func decodedScopeWrites() []decodedLimitWrite {
	return []decodedLimitWrite{
		{"blob write to the blob bucket, scope as decoded", transport.OpBlobWrite, bson.D{{Key: "Database", Value: "app"}, {Key: "BUCKET", Value: "files"}, {Key: "key", Value: "d"}, {Key: "data", Value: filledBinary(decodedBlobCap + 1024)}}, "storage.max_blob_bytes"},
		{"blob write, key as decoded", transport.OpBlobWrite, bson.D{{Key: "database", Value: "app"}, {Key: "bucket", Value: "files"}, {Key: "Key", Value: strings.Repeat("k", 17<<20)}, {Key: "data", Value: filledBinary(1 << 10)}}, "replicate"},
	}
}
