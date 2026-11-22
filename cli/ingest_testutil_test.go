package cli

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// ingestTestServer is a live server plus an authenticated CLI on it, and the
// address and credentials the legacy-server tests need to dial the same server.
type ingestTestServer struct {
	cli      *CLI
	addr     string
	user     string
	password string
	svc      *server.Service
}

const ingestTestPassword = "ingest-e2e-password"

// newIngestTestServer is newDumpTestServer with a hook on the configuration,
// so a test can shorten the TTL sweep. It copies newDumpTestServer rather
// than editing it (ingest-build.md step 0).
func newIngestTestServer(t testing.TB, mutate func(*config.Config)) *ingestTestServer {
	t.Helper()

	cfg := config.DefaultConfig()
	cfg.Storage.DataDir = t.TempDir()
	cfg.Server.Listen = "127.0.0.1:0"
	cfg.TCP.Listen = freeAddress(t)
	if mutate != nil {
		mutate(cfg)
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc, err := server.NewService(cfg, logger)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	if err := svc.SeedRootUser("admin", ingestTestPassword); err != nil {
		t.Fatalf("SeedRootUser: %v", err)
	}
	if err := svc.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = svc.Stop() })

	c := NewCLI(
		WithAddress(cfg.TCP.Listen),
		WithInsecureTLS(true),
		WithCredentials("admin", ingestTestPassword),
		WithTerminal(io.Discard),
	)
	if err := c.Connect(); err != nil {
		t.Fatalf("connect to %s: %v", cfg.TCP.Listen, err)
	}
	t.Cleanup(c.Close)
	return &ingestTestServer{cli: c, addr: cfg.TCP.Listen, user: "admin", password: ingestTestPassword, svc: svc}
}

// call sends one request on the server's admin connection and returns the
// reply frame, failing the test on a transport error.
func (s *ingestTestServer) call(t *testing.T, op transport.Opcode, req, dst interface{}) *transport.Frame {
	t.Helper()
	return callOn(t, s.cli, op, req, dst)
}

func callOn(t *testing.T, c *CLI, op transport.Opcode, req, dst interface{}) *transport.Frame {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	frame, err := c.handler.conn.SendRequestBSON(ctx, op, req, dst)
	if err != nil {
		t.Fatalf("%s: %v", op, err)
	}
	return frame
}

// errorFrameText renders an OpError frame's code and message.
func errorFrameText(t *testing.T, frame *transport.Frame) string {
	t.Helper()
	var refusal server.ErrorResponse
	if err := bson.Unmarshal(frame.Payload, &refusal); err != nil {
		t.Fatalf("decoding error frame: %v", err)
	}
	return fmt.Sprintf("code %d: %s", refusal.Code, refusal.Message)
}

// errorFrameCode is an OpError frame's code.
func errorFrameCode(t *testing.T, frame *transport.Frame) transport.ErrorCode {
	t.Helper()
	var refusal server.ErrorResponse
	if err := bson.Unmarshal(frame.Payload, &refusal); err != nil {
		t.Fatalf("decoding error frame: %v", err)
	}
	return transport.ErrorCode(refusal.Code)
}

func (s *ingestTestServer) createDatabase(t *testing.T, name string) {
	t.Helper()
	var resp server.CreateDatabaseResponse
	frame := s.call(t, transport.OpCreateDB, &server.CreateDatabaseRequest{Name: name}, &resp)
	if frame.Opcode == transport.OpError {
		t.Fatalf("create database %q: %s", name, errorFrameText(t, frame))
	}
}

func (s *ingestTestServer) createBucket(t *testing.T, database, name string, bucketType server.BucketType, options map[string]interface{}) {
	t.Helper()
	var resp server.CreateBucketResponse
	frame := s.call(t, transport.OpCreateBucket, &server.CreateBucketRequest{
		Database: database, Name: name, Type: bucketType, Options: options,
	}, &resp)
	if frame.Opcode == transport.OpError || !resp.Success {
		t.Fatalf("create bucket %s.%s: %v %s", database, name, resp, errorFrameText(t, frame))
	}
}

// makeBucket creates database and bucket, tolerating an existing database.
func (s *ingestTestServer) makeBucket(t *testing.T, database, name string, bucketType server.BucketType) {
	t.Helper()
	var resp server.CreateDatabaseResponse
	s.call(t, transport.OpCreateDB, &server.CreateDatabaseRequest{Name: database}, &resp)
	s.createBucket(t, database, name, bucketType, nil)
}

// statsOf returns the OpBucketStats answer: the decoded response and the
// frame (an OpError frame for a missing bucket or database).
func (s *ingestTestServer) statsOf(t *testing.T, database, bucket string) (server.BucketStatsResponse, *transport.Frame) {
	t.Helper()
	var resp server.BucketStatsResponse
	frame := s.call(t, transport.OpBucketStats, &server.BucketStatsRequest{Database: database, Bucket: bucket}, &resp)
	return resp, frame
}

// entries returns the bucket's entry count from OpBucketStats.
func (s *ingestTestServer) entries(t *testing.T, database, bucket string) int64 {
	t.Helper()
	stats, frame := s.statsOf(t, database, bucket)
	if frame.Opcode == transport.OpError {
		t.Fatalf("stats %s.%s: %s", database, bucket, errorFrameText(t, frame))
	}
	return stats.Entries
}

// batchPut sends one OpBatchPut and returns the decoded reply and the frame.
func (s *ingestTestServer) batchPut(t *testing.T, database, bucket string, docs []map[string]interface{}) (server.BatchInsertResponse, *transport.Frame) {
	t.Helper()
	var resp server.BatchInsertResponse
	frame := s.call(t, transport.OpBatchPut, &server.BatchInsertRequest{Database: database, Bucket: bucket, Data: docs}, &resp)
	return resp, frame
}

// batchUpsert is batchPut as an OpBatchUpsert: a batch that repeats an _id_
// and means the last one wins.
func (s *ingestTestServer) batchUpsert(t *testing.T, database, bucket string, docs []map[string]interface{}) (server.BatchInsertResponse, *transport.Frame) {
	t.Helper()
	var resp server.BatchInsertResponse
	frame := s.call(t, transport.OpBatchUpsert, &server.BatchInsertRequest{Database: database, Bucket: bucket, Data: docs}, &resp)
	return resp, frame
}

// scanAll returns every document in a keyed bucket, as stored.
func (s *ingestTestServer) scanAll(t *testing.T, database, bucket string) []bson.Raw {
	t.Helper()
	var resp server.ScanResponse
	frame := s.call(t, transport.OpScan, &server.ScanRequest{Database: database, Bucket: bucket, Limit: 100000}, &resp)
	if frame.Opcode == transport.OpError {
		t.Fatalf("scan %s.%s: %s", database, bucket, errorFrameText(t, frame))
	}
	return resp.Documents
}

// getDocument reads one keyed document; found is false for a miss.
func (s *ingestTestServer) getDocument(t *testing.T, database, bucket, key string) (bson.Raw, bool) {
	t.Helper()
	var resp server.FindResponse
	frame := s.call(t, transport.OpGet, &server.FindRequest{Database: database, Bucket: bucket, Key: key}, &resp)
	if frame.Opcode == transport.OpError {
		t.Fatalf("get %s.%s/%s: %s", database, bucket, key, errorFrameText(t, frame))
	}
	return resp.Data, resp.Found
}

// bsonDocumentBytes marshals doc to the bytes a .bson file holds.
func bsonDocumentBytes(t *testing.T, doc interface{}) []byte {
	t.Helper()
	encoded, err := bson.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal fixture document: %v", err)
	}
	return encoded
}

// bsonStream concatenates documents the way a .bson file does.
func bsonStream(t *testing.T, docs ...interface{}) []byte {
	t.Helper()
	var stream bytes.Buffer
	for _, doc := range docs {
		stream.Write(bsonDocumentBytes(t, doc))
	}
	return stream.Bytes()
}

// requireSameValue fails the test unless got holds the same BSON value as want:
// the same type and, for a leaf, the same bytes. Embedded documents are
// compared field by field and NOT in field order (the write path does not
// preserve it, ingest-spec.md §2.4); arrays are compared element by element in
// order, because array order is preserved.
func requireSameValue(t *testing.T, path string, want, got bson.RawValue) {
	t.Helper()
	if want.Type != got.Type {
		t.Errorf("%s: BSON type %s, want %s", path, got.Type, want.Type)
		return
	}
	switch want.Type {
	case bson.TypeEmbeddedDocument:
		wantDoc, gotDoc := want.Document(), got.Document()
		wantFields, err := wantDoc.Elements()
		if err != nil {
			t.Fatalf("%s: reading expected document: %v", path, err)
		}
		gotFields, err := gotDoc.Elements()
		if err != nil {
			t.Fatalf("%s: reading stored document: %v", path, err)
		}
		if len(wantFields) != len(gotFields) {
			t.Errorf("%s: %d field(s), want %d", path, len(gotFields), len(wantFields))
		}
		for _, wantField := range wantFields {
			gotField, lookupErr := gotDoc.LookupErr(wantField.Key())
			if lookupErr != nil {
				t.Errorf("%s.%s: missing from the stored document", path, wantField.Key())
				continue
			}
			requireSameValue(t, path+"."+wantField.Key(), wantField.Value(), gotField)
		}
	case bson.TypeArray:
		wantValues, err := want.Array().Values()
		if err != nil {
			t.Fatalf("%s: reading expected array: %v", path, err)
		}
		gotValues, err := got.Array().Values()
		if err != nil {
			t.Fatalf("%s: reading stored array: %v", path, err)
		}
		if len(wantValues) != len(gotValues) {
			t.Errorf("%s: %d element(s), want %d", path, len(gotValues), len(wantValues))
			return
		}
		for index := range wantValues {
			requireSameValue(t, fmt.Sprintf("%s[%d]", path, index), wantValues[index], gotValues[index])
		}
	default:
		if !bytes.Equal(want.Value, got.Value) {
			t.Errorf("%s: value bytes % x, want % x", path, got.Value, want.Value)
		}
	}
}

// nineteenFieldDocument is the document ingest-spec.md §2.4 measured: every
// BSON value type a database user is likely to have, plus a nested document and
// an array holding a document, 19 fields with _id_. It is returned as an
// ordered bson.D (the expected value) and as the map the write path receives.
func nineteenFieldDocument(key string) (bson.D, map[string]interface{}) {
	ordered := bson.D{
		{Key: "_id_", Value: key},
		{Key: "text", Value: "héllo"},
		{Key: "oid", Value: primitive.ObjectID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}},
		{Key: "when", Value: primitive.NewDateTimeFromTime(time.Date(2021, 3, 4, 5, 6, 7, 0, time.UTC))},
		{Key: "small", Value: int32(-7)},
		{Key: "large", Value: int64(1) << 40},
		{Key: "ratio", Value: 3.25},
		{Key: "money", Value: primitive.NewDecimal128(0x3040000000000000, 12345)},
		{Key: "uuid", Value: primitive.Binary{Subtype: 4, Data: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}}},
		{Key: "blob", Value: primitive.Binary{Subtype: 0, Data: []byte{0, 1, 2, 255}}},
		{Key: "stamp", Value: primitive.Timestamp{T: 99, I: 3}},
		{Key: "low", Value: primitive.MinKey{}},
		{Key: "high", Value: primitive.MaxKey{}},
		{Key: "script", Value: primitive.JavaScript("return 1")},
		{Key: "pattern", Value: primitive.Regex{Pattern: "^a.*z$", Options: "i"}},
		{Key: "nothing", Value: nil},
		{Key: "flag", Value: true},
		{Key: "nested", Value: bson.D{{Key: "x", Value: int32(1)}, {Key: "y", Value: bson.D{{Key: "deep", Value: "yes"}}}}},
		{Key: "items", Value: bson.A{bson.D{{Key: "p", Value: int32(1)}, {Key: "q", Value: "two"}}, int32(3)}},
	}
	sent := make(map[string]interface{}, len(ordered))
	for _, element := range ordered {
		sent[element.Key] = element.Value
	}
	return ordered, sent
}

// freeAddress picks an address nothing is listening on, for a test.TB.
func freeAddress(tb testing.TB) string {
	tb.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("finding a free port: %v", err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		tb.Fatalf("releasing the probe listener: %v", err)
	}
	return address
}
