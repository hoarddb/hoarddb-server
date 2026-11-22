package server

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/x/bsonx/bsoncore"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/transport"
)

// GAP-36: one document has an explicit size limit, enforced where a client's
// write enters, and never where a replica or a replay applies it.

const documentLimitAdvice = `A list that grows belongs in a nested store field; a large value belongs in a blob bucket, and a large file on your own storage behind a pointer document — see "Large file stores" in the docs.`

// docOfEncodedSize is a document whose BSON encoding is exactly size bytes:
// the header, one string element named "body" and the terminator add 16.
func docOfEncodedSize(size int) map[string]interface{} {
	return map[string]interface{}{"body": strings.Repeat("x", size-16)}
}

// documentLimitServer is a standalone node with hash bucket "app.docs", queue
// "app.jobs" and heap "app.ranked", and the given document limit (0 leaves the
// configured default).
func documentLimitServer(t *testing.T, limitBytes int) (*Server, transport.RequestHandler) {
	t.Helper()
	s := newTestServer(t)
	if limitBytes != 0 {
		s.config.Storage.MaxDocumentBytes = limitBytes
	}
	handler := NewHandler(s)
	handlerSend(t, handler, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
	for name, kind := range map[string]BucketType{"docs": BucketTypeHash, "jobs": BucketTypeFIFO, "ranked": BucketTypeHeap} {
		// docs is whole: its tests send multi-updates through the door, which a spread bucket refuses (build step 12c).
		handlerSend(t, handler, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: name, Type: kind,
			Options: map[string]interface{}{"placement": "whole"}})
	}
	return s, handler
}

func sendFrame(t *testing.T, handler transport.RequestHandler, op transport.Opcode, req any) *transport.Frame {
	t.Helper()
	frame, err := handler(context.Background(), transport.NewFrame(op, mustMarshalBSON(t, req)))
	if err != nil {
		t.Fatalf("%s: handler error %v", op, err)
	}
	return frame
}

func wireRefusal(t *testing.T, frame *transport.Frame) string {
	t.Helper()
	if frame.Opcode != transport.OpError {
		t.Fatalf("answered %s, want an error frame", frame.Opcode)
	}
	var refusal ErrorResponse
	if err := bson.Unmarshal(frame.Payload, &refusal); err != nil {
		t.Fatalf("decoding the refusal: %v", err)
	}
	if refusal.Code != int(transport.ErrInvalidArgument) {
		t.Errorf("refusal code = %d, want %d (INVALID_ARGUMENT)", refusal.Code, transport.ErrInvalidArgument)
	}
	return refusal.Message
}

func putLimitDocument(t *testing.T, handler transport.RequestHandler, key string, data map[string]interface{}) *transport.Frame {
	t.Helper()
	return sendFrame(t, handler, transport.OpPut, InsertRequest{Database: "app", Bucket: "docs", Key: key, Data: data})
}

func TestDocumentAtTheLimitIsAcceptedAndOneByteOverIsRefused(t *testing.T) {
	const limit = 1 << 20
	_, handler := documentLimitServer(t, limit)

	if frame := putLimitDocument(t, handler, "at-limit", docOfEncodedSize(limit)); frame.Opcode == transport.OpError {
		t.Fatalf("a document of exactly the limit was refused: %.300s", frame.Payload)
	}
	got := wireRefusal(t, putLimitDocument(t, handler, "over-limit", docOfEncodedSize(limit+1)))
	want := fmt.Sprintf(`Document "over-limit" is %d bytes; this server's limit is %d bytes per document (storage.max_document_bytes, maximum 30 MiB). %s`,
		limit+1, limit, documentLimitAdvice)
	if got != want {
		t.Errorf("refusal text:\n got: %s\nwant: %s", got, want)
	}
}

func TestDocumentLimitDefaultsTo24MiB(t *testing.T) {
	_, handler := documentLimitServer(t, 0)

	if frame := putLimitDocument(t, handler, "twenty-four", docOfEncodedSize(24<<20)); frame.Opcode == transport.OpError {
		t.Fatalf("a 24 MiB document was refused on a default node: %.300s", frame.Payload)
	}
	got := wireRefusal(t, putLimitDocument(t, handler, "big", docOfEncodedSize(25<<20)))
	want := `Document "big" is 25.0 MiB; this server's limit is 24 MiB per document (storage.max_document_bytes, maximum 30 MiB). ` + documentLimitAdvice
	if got != want {
		t.Errorf("refusal text:\n got: %s\nwant: %s", got, want)
	}
}

func TestRaisedDocumentLimitTakesEffect(t *testing.T) {
	s, handler := documentLimitServer(t, 0)

	putLimitDocument(t, handler, "warm", docOfEncodedSize(1<<10))
	if frame := putLimitDocument(t, handler, "twenty-nine", docOfEncodedSize(29<<20)); frame.Opcode != transport.OpError {
		t.Fatal("a 29 MiB document was accepted at the default limit")
	}
	s.config.Storage.MaxDocumentBytes = config.MaxDocumentBytesCeiling
	if frame := putLimitDocument(t, handler, "twenty-nine", docOfEncodedSize(29<<20)); frame.Opcode == transport.OpError {
		t.Fatalf("a 29 MiB document was refused after the limit was raised to 30 MiB: %.300s", frame.Payload)
	}
}

// The server-minted _id_ is not the client's bytes: a document at the limit
// stays at the limit however its key is chosen.
func TestDocumentLimitDoesNotCountTheKeyTheServerAdds(t *testing.T) {
	const limit = 1 << 20
	_, handler := documentLimitServer(t, limit)

	frame := sendFrame(t, handler, transport.OpPut, InsertRequest{Database: "app", Bucket: "docs", Data: docOfEncodedSize(limit)})
	if frame.Opcode == transport.OpError {
		t.Fatalf("a keyless document of exactly the limit was refused: %.300s", frame.Payload)
	}
}

func TestPushesAndBatchesAreLimitedPerDocument(t *testing.T) {
	const limit = 1 << 20
	_, handler := documentLimitServer(t, limit)
	over := docOfEncodedSize(limit + 1)

	for _, tt := range []struct {
		name string
		op   transport.Opcode
		req  any
	}{
		{"fifo push", transport.OpFIFOPush, PushRequest{Database: "app", Bucket: "jobs", Data: over}},
		{"heap push", transport.OpHeapPush, PushRequest{Database: "app", Bucket: "ranked", Data: over, Priority: 1}},
	} {
		if message := wireRefusal(t, sendFrame(t, handler, tt.op, tt.req)); !strings.Contains(message, "storage.max_document_bytes") {
			t.Errorf("%s: refusal %q does not name the key", tt.name, message)
		}
	}

	batch := BatchInsertRequest{Database: "app", Bucket: "docs", Data: []map[string]interface{}{
		{"_id_": "small-1", "n": 1},
		{"_id_": "the-big-one", "body": strings.Repeat("x", limit)},
		{"_id_": "small-2", "n": 2},
	}}
	message := wireRefusal(t, sendFrame(t, handler, transport.OpBatchPut, batch))
	if !strings.Contains(message, `"the-big-one"`) || strings.Contains(message, "small-") {
		t.Errorf("batch refusal %q must name the oversized document and only it", message)
	}
	found := sendFrame(t, handler, transport.OpGet, FindRequest{Database: "app", Bucket: "docs", Key: "small-1"})
	if found.Opcode != transport.OpError {
		var resp FindResponse
		if err := bson.Unmarshal(found.Payload, &resp); err == nil && resp.Found {
			t.Error("a refused batch stored its small documents")
		}
	}
}

func TestFieldStoreElementIsLimitedPerDocument(t *testing.T) {
	const limit = 1 << 20
	s := newPostServer(t)
	s.config.Storage.MaxDocumentBytes = limit
	handler := NewHandler(s)

	push := func(size int) *transport.Frame {
		req := fieldReq("p1", "Comments")
		req.Data = docOfEncodedSize(size)
		return sendFrame(t, handler, transport.OpFieldPush, req)
	}
	if frame := push(limit); frame.Opcode == transport.OpError {
		t.Fatalf("an element of exactly the limit was refused: %.300s", frame.Payload)
	}
	if message := wireRefusal(t, push(limit+1)); !strings.Contains(message, "storage.max_document_bytes") {
		t.Errorf("refusal %q does not name the key", message)
	}
}

// An update that grows a document past the limit is refused, and the document
// is as it was.
func TestUpdateThatGrowsADocumentPastTheLimitIsRefusedAndChangesNothing(t *testing.T) {
	const limit = 1 << 20
	s, handler := documentLimitServer(t, limit)
	putLimitDocument(t, handler, "grows", map[string]interface{}{"body": strings.Repeat("x", 600<<10)})

	update := UpdateRequest{Database: "app", Bucket: "docs", Key: "grows",
		Set: map[string]interface{}{"more": strings.Repeat("y", 600<<10)}}
	message := wireRefusal(t, sendFrame(t, handler, transport.OpUpdate, update))
	for _, want := range []string{`Document "grows" would be`, "after this update", "was not changed", "storage.max_document_bytes"} {
		if !strings.Contains(message, want) {
			t.Errorf("refusal %q does not contain %q", message, want)
		}
	}
	found, err := s.Find(FindRequest{Database: "app", Bucket: "docs", Key: "grows"})
	if err != nil || !found.Found {
		t.Fatalf("Find: %+v, %v", found, err)
	}
	if _, present := bson.Raw(found.Data).Lookup("more").StringValueOK(); present {
		t.Error("a refused update changed the document")
	}

	small := UpdateRequest{Database: "app", Bucket: "docs", Key: "grows", Set: map[string]interface{}{"more": "fits"}}
	if frame := sendFrame(t, handler, transport.OpUpdate, small); frame.Opcode == transport.OpError {
		t.Fatalf("an update that stays under the limit was refused: %.300s", frame.Payload)
	}
}

func TestMultiUpdateThatGrowsADocumentPastTheLimitIsRefusedWhole(t *testing.T) {
	const limit = 1 << 20
	s, handler := documentLimitServer(t, limit)
	putLimitDocument(t, handler, "small", map[string]interface{}{"kind": "a"})
	putLimitDocument(t, handler, "large", map[string]interface{}{"kind": "a", "body": strings.Repeat("x", 600<<10)})

	update := UpdateRequest{Database: "app", Bucket: "docs", Multi: true, Filter: map[string]interface{}{"kind": "a"},
		Set: map[string]interface{}{"more": strings.Repeat("y", 600<<10)}}
	message := wireRefusal(t, sendFrame(t, handler, transport.OpUpdate, update))
	if !strings.Contains(message, `"large"`) {
		t.Errorf("refusal %q does not name the document that would overflow", message)
	}
	found, err := s.Find(FindRequest{Database: "app", Bucket: "docs", Key: "small"})
	if err != nil || !found.Found {
		t.Fatalf("Find: %+v, %v", found, err)
	}
	if _, present := bson.Raw(found.Data).Lookup("more").StringValueOK(); present {
		t.Error("a refused multi-update changed a document")
	}
}

// A lowered limit is a limit on what enters, not on what is already stored
// (cluster-settings spec §9.6, T-23; BUG-0224): a document left over it stays
// readable, and an update that does not grow it is accepted even while the
// result is still over, so it can be trimmed in steps. Growth is refused, and
// a delete is accepted.
func TestShrinkingUpdateOfAnOverLimitDocumentIsAccepted(t *testing.T) {
	t.Run("single update", shrinkingSingleUpdateOfOverLimitDocuments)
	t.Run("multi-update", shrinkingMultiUpdateOfOverLimitDocuments)
}

func shrinkingSingleUpdateOfOverLimitDocuments(t *testing.T) {
	const seeded, lowered = 2 << 20, 1 << 20
	s, handler := documentLimitServer(t, seeded)
	overLimit := func() map[string]interface{} {
		return map[string]interface{}{"kind": "over", "a": strings.Repeat("x", 1500<<10), "b": strings.Repeat("y", 400<<10)}
	}
	for _, key := range []string{"trimmed", "rewritten", "grows", "doomed"} {
		if frame := putLimitDocument(t, handler, key, overLimit()); frame.Opcode == transport.OpError {
			t.Fatalf("seeding %q under the %d-byte limit: %.300s", key, seeded, frame.Payload)
		}
	}
	s.config.Storage.MaxDocumentBytes = lowered
	stored := func(key string) bson.Raw {
		t.Helper()
		found, err := s.Find(FindRequest{Database: "app", Bucket: "docs", Key: key})
		if err != nil || !found.Found {
			t.Fatalf("Find %q after the limit was lowered: %+v, %v", key, found, err)
		}
		return bson.Raw(found.Data)
	}
	stored("trimmed")

	trim := UpdateRequest{Database: "app", Bucket: "docs", Key: "trimmed", Unset: []string{"b"}}
	if frame := sendFrame(t, handler, transport.OpUpdate, trim); frame.Opcode == transport.OpError {
		t.Errorf("an update that shrank an over-limit document from 1.9 MiB to 1.5 MiB was refused: %.300s", frame.Payload)
	}
	if _, present := stored("trimmed").Lookup("b").StringValueOK(); present {
		t.Error("the shrinking update did not change the document")
	}

	sameSize := UpdateRequest{Database: "app", Bucket: "docs", Key: "rewritten", Set: map[string]interface{}{"b": strings.Repeat("z", 400<<10)}}
	if frame := sendFrame(t, handler, transport.OpUpdate, sameSize); frame.Opcode == transport.OpError {
		t.Errorf("an update that left an over-limit document the same size was refused: %.300s", frame.Payload)
	}
	if b, _ := stored("rewritten").Lookup("b").StringValueOK(); !strings.HasPrefix(b, "z") {
		t.Error("the same-size update did not change the document")
	}

	grow := UpdateRequest{Database: "app", Bucket: "docs", Key: "grows", Set: map[string]interface{}{"c": "one more field"}}
	message := wireRefusal(t, sendFrame(t, handler, transport.OpUpdate, grow))
	for _, want := range []string{`Document "grows" would be`, "after this update", "was not changed", "storage.max_document_bytes"} {
		if !strings.Contains(message, want) {
			t.Errorf("refusal %q does not contain %q", message, want)
		}
	}
	if _, present := stored("grows").Lookup("c").StringValueOK(); present {
		t.Error("a refused growing update changed the document")
	}

	if frame := sendFrame(t, handler, transport.OpDelete, DeleteRequest{Database: "app", Bucket: "docs", Key: "doomed"}); frame.Opcode == transport.OpError {
		t.Errorf("deleting an over-limit document was refused: %.300s", frame.Payload)
	}
	if found, err := s.Find(FindRequest{Database: "app", Bucket: "docs", Key: "doomed"}); err != nil || found.Found {
		t.Errorf("the over-limit document survived its delete: %+v, %v", found, err)
	}
}

// The multi-update path holds each match to the same rule, and a refusal of
// one match still refuses the whole call.
func shrinkingMultiUpdateOfOverLimitDocuments(t *testing.T) {
	const seeded, lowered = 2 << 20, 1 << 20
	s, handler := documentLimitServer(t, seeded)
	putLimitDocument(t, handler, "shrinks-1", map[string]interface{}{"kind": "all-shrink", "a": strings.Repeat("x", 1500<<10), "b": strings.Repeat("y", 400<<10)})
	putLimitDocument(t, handler, "shrinks-2", map[string]interface{}{"kind": "all-shrink", "a": strings.Repeat("x", 1500<<10), "b": strings.Repeat("y", 400<<10)})
	putLimitDocument(t, handler, "shrinks", map[string]interface{}{"kind": "mixed", "a": strings.Repeat("x", 1500<<10), "b": strings.Repeat("y", 400<<10)})
	putLimitDocument(t, handler, "grows", map[string]interface{}{"kind": "mixed", "a": strings.Repeat("x", 1900<<10)})
	s.config.Storage.MaxDocumentBytes = lowered
	field := func(key, name string) (string, bool) {
		t.Helper()
		found, err := s.Find(FindRequest{Database: "app", Bucket: "docs", Key: key})
		if err != nil || !found.Found {
			t.Fatalf("Find %q: %+v, %v", key, found, err)
		}
		return bson.Raw(found.Data).Lookup(name).StringValueOK()
	}

	shrinkAll := UpdateRequest{Database: "app", Bucket: "docs", Multi: true, Filter: map[string]interface{}{"kind": "all-shrink"},
		Set: map[string]interface{}{"b": "short"}}
	if frame := sendFrame(t, handler, transport.OpUpdate, shrinkAll); frame.Opcode == transport.OpError {
		t.Errorf("a multi-update that shrank every over-limit match was refused: %.300s", frame.Payload)
	}
	for _, key := range []string{"shrinks-1", "shrinks-2"} {
		if b, _ := field(key, "b"); b != "short" {
			t.Errorf("%q: the shrinking multi-update did not change it", key)
		}
	}

	// The same $set shrinks "shrinks" (its b was 400 KiB) and grows "grows"
	// (it had no b).
	mixed := UpdateRequest{Database: "app", Bucket: "docs", Multi: true, Filter: map[string]interface{}{"kind": "mixed"},
		Set: map[string]interface{}{"b": strings.Repeat("z", 1<<10)}}
	message := wireRefusal(t, sendFrame(t, handler, transport.OpUpdate, mixed))
	if !strings.Contains(message, `Document "grows" would be`) {
		t.Errorf("refusal %q does not name the document that grew", message)
	}
	for _, key := range []string{"shrinks", "grows"} {
		if b, _ := field(key, "b"); strings.HasPrefix(b, "z") {
			t.Errorf("%q: a refused multi-update changed it", key)
		}
	}
}

// The limit an update is held to is the one the entering node stamps, not one
// the client names.
func TestUpdateIgnoresALimitTheClientNames(t *testing.T) {
	const limit = 1 << 20
	_, handler := documentLimitServer(t, limit)
	putLimitDocument(t, handler, "grows", map[string]interface{}{"body": strings.Repeat("x", 600<<10)})

	payload := mustMarshalBSON(t, UpdateRequest{Database: "app", Bucket: "docs", Key: "grows",
		Set: map[string]interface{}{"more": strings.Repeat("y", 600<<10)}})
	payload = append(payload[:len(payload)-1], bsoncore.AppendInt64Element(nil, "max_document_bytes", config.MaxDocumentBytesCeiling)...)
	payload = append(payload, 0)
	payload[0], payload[1], payload[2], payload[3] = byte(len(payload)), byte(len(payload)>>8), byte(len(payload)>>16), byte(len(payload)>>24)

	frame, err := handler(context.Background(), transport.NewFrame(transport.OpUpdate, payload))
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	wireRefusal(t, frame)
}

// A document whose key would push its replication frame past one frame is
// refused at the door rather than accepted and never shipped.
func TestDocumentWhoseKeyMakesItsReplicationFrameTooLargeIsRefused(t *testing.T) {
	_, handler := documentLimitServer(t, 0)

	message := wireRefusal(t, putLimitDocument(t, handler, strings.Repeat("k", 17<<20), docOfEncodedSize(1<<10)))
	if !strings.Contains(message, "replicate") || !strings.Contains(message, "key") {
		t.Errorf("a %d-byte refusal does not say the key cannot be replicated: %.200q", len(message), message)
	}
}

// The replica applies what its owner accepted, whatever its own limit says,
// and an update the owner accepted is applied by the replica because the
// entry carries the limit it was accepted under.
func TestReplicaAppliesADocumentItsOwnerAcceptedWhateverItsOwnLimit(t *testing.T) {
	tc := newTestClusterWith(t, 2, 2, func(index int, cfg *config.Config) {
		if index == 1 {
			cfg.Storage.MaxDocumentBytes = 64 << 10
		}
	})
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})

	tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: testBucket, Key: "big", Data: docOfEncodedSize(200 << 10)})
	tc.ok(0, transport.OpUpdate, UpdateRequest{Database: testDatabase, Bucket: testBucket, Key: "big",
		Set: map[string]interface{}{"more": strings.Repeat("y", 100<<10)}})

	for node := 0; node < 2; node++ {
		frame := tc.ok(node, transport.OpGet, FindRequest{Database: testDatabase, Bucket: testBucket, Key: "big"})
		var resp FindResponse
		if err := bson.Unmarshal(frame.Payload, &resp); err != nil || !resp.Found {
			t.Fatalf("node %d does not hold the document: %+v, %v", node+1, resp, err)
		}
		if _, ok := bson.Raw(resp.Data).Lookup("more").StringValueOK(); !ok {
			t.Fatalf("node %d (limit %d) did not apply the owner's accepted update", node+1, map[bool]int{true: 64 << 10, false: config.DefaultMaxDocumentBytes}[node == 1])
		}
	}

	refused := tc.frame(1, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: testBucket, Key: "via-replica", Data: docOfEncodedSize(200 << 10)})
	if refused.Opcode != transport.OpError {
		t.Fatal("the node with the 64 KiB limit accepted a 200 KiB client write")
	}
}

// The ceiling is honest: a document of exactly 30 MiB is written, replicated
// and read back on the replica through the real envelope and framing.
func TestThirtyMiBDocumentReplicatesDumpsAndRestores(t *testing.T) {
	if testing.Short() {
		t.Skip("moves several 30 MiB frames")
	}
	tc := newTestClusterWith(t, 2, 2, func(_ int, cfg *config.Config) {
		cfg.Storage.MaxDocumentBytes = config.MaxDocumentBytesCeiling
	})
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})

	tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: testBucket, Key: "thirty", Data: docOfEncodedSize(config.MaxDocumentBytesCeiling)})
	for node := 0; node < 2; node++ {
		frame := tc.ok(node, transport.OpGet, FindRequest{Database: testDatabase, Bucket: testBucket, Key: "thirty"})
		var resp FindResponse
		if err := bson.Unmarshal(frame.Payload, &resp); err != nil || !resp.Found {
			t.Fatalf("node %d does not hold the 30 MiB document: found=%v err=%v", node+1, resp.Found, err)
		}
	}
}

func TestThirtyMiBDocumentRoundTripsThroughDumpAndRestore(t *testing.T) {
	s, handler := documentLimitServer(t, config.MaxDocumentBytesCeiling)
	if frame := putLimitDocument(t, handler, "thirty", docOfEncodedSize(config.MaxDocumentBytesCeiling)); frame.Opcode == transport.OpError {
		t.Fatalf("a 30 MiB document was refused at a 30 MiB limit: %.300s", frame.Payload)
	}

	ctx := rootContext()
	manifest, raw := dumpBucketRaw(t, s, ctx, "app", "docs", 1)
	restoreBucketRaw(t, s, ctx, restoreRequestFromManifest(manifest, int64(len(raw)), true), raw, 1)

	found, err := s.Find(FindRequest{Database: "app", Bucket: "docs", Key: "thirty"})
	if err != nil || !found.Found {
		t.Fatalf("Find after restore: %+v, %v", found, err)
	}
	if body, _ := bson.Raw(found.Data).Lookup("body").StringValueOK(); len(body) != config.MaxDocumentBytesCeiling-16 {
		t.Errorf("restored body is %d bytes, want %d", len(body), config.MaxDocumentBytesCeiling-16)
	}
}

// A restore is an ordinary write: a dump from a node with a higher limit is
// refused, with the same text, by a node whose limit is lower.
func TestRestoreIsHeldToTheNodesDocumentLimit(t *testing.T) {
	source, sourceHandler := documentLimitServer(t, 0)
	putLimitDocument(t, sourceHandler, "big", docOfEncodedSize(2<<20))
	ctx := rootContext()
	manifest, raw := dumpBucketRaw(t, source, ctx, "app", "docs", 1)

	target, _ := documentLimitServer(t, 1<<20)
	_, done, message := restoreBucketRaw(t, target, ctx, restoreRequestFromManifest(manifest, int64(len(raw)), true), raw, 1)
	if done != nil {
		message += done.Message
	}
	t.Logf("restore answered %q, done=%+v", message, done)
	if !strings.Contains(message, "storage.max_document_bytes") {
		t.Errorf("restore refusal %q does not name the limit", message)
	}
}
