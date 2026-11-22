package server

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/x/bsonx/bsoncore"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

// BUG-0526: a document a client sends may nest at most 100 levels, counted as
// the drivers count it (the root document is level 1, each embedded document
// or array adds one). The BSON corpus fixes the count: document/nested-depth-100
// and array/nested-depth-100 are accepted, reject/depth-101-* refused.

// nestingRefusalPhrase is the part of the refusal every site shares.
const nestingRefusalPhrase = "levels deep"

// nestedDocumentBytes is a document nested to depth: a chain of embedded
// documents under "a", or a root document holding an array under "a" with
// arrays under "0" inside it. It is built from bytes, as the corpus builds its
// depth vectors (cmd/gen-testvectors, bsonNestedBytes), so its depth does not
// depend on an encoder; without rootElements it is the corpus's bytes.
// rootElements are written into the root ahead of "a".
func nestedDocumentBytes(depth int, arrays bool, rootElements ...[]byte) []byte {
	container := bsoncore.BuildDocument(nil)
	for level := depth - 1; level >= 2; level-- {
		if arrays {
			container = bsoncore.BuildDocument(nil, bsoncore.AppendArrayElement(nil, "0", container))
		} else {
			container = bsoncore.BuildDocument(nil, bsoncore.AppendDocumentElement(nil, "a", container))
		}
	}
	elements := append([][]byte{}, rootElements...)
	if arrays {
		elements = append(elements, bsoncore.AppendArrayElement(nil, "a", container))
	} else {
		elements = append(elements, bsoncore.AppendDocumentElement(nil, "a", container))
	}
	return bsoncore.BuildDocument(nil, elements...)
}

// wireString and wireDocument are request elements, written from bytes so a
// row's payload carries the nested document exactly as built.
func wireString(key, text string) []byte { return bsoncore.AppendStringElement(nil, key, text) }

func wireDocument(key string, document []byte) []byte {
	return bsoncore.AppendDocumentElement(nil, key, document)
}

// wireDocumentList is an array element holding documents.
func wireDocumentList(key string, documents ...[]byte) []byte {
	members := make([][]byte, 0, len(documents))
	for position, document := range documents {
		members = append(members, bsoncore.AppendDocumentElement(nil, fmt.Sprint(position), document))
	}
	return bsoncore.AppendArrayElement(nil, key, bsoncore.BuildDocument(nil, members...))
}

func wireRequest(elements ...[]byte) []byte { return bsoncore.BuildDocument(nil, elements...) }

// depthAnswer is what a site answered one nested document.
type depthAnswer struct {
	accepted bool
	// refusal is the refusal's text, "" when accepted.
	refusal string
	// named reports a refusal that is the depth bound's: INVALID_ARGUMENT,
	// worded with nestingRefusalPhrase, and not_applied for a data write.
	named bool
}

// answerOfFrame reads a handler's answer: an error frame is a refusal, and so
// is an answer whose success member is false.
func answerOfFrame(t *testing.T, op transport.Opcode, frame *transport.Frame) depthAnswer {
	t.Helper()
	if frame.Opcode != transport.OpError {
		if success, present := bson.Raw(frame.Payload).Lookup("success").BooleanOK(); present && !success {
			message, _ := bson.Raw(frame.Payload).Lookup("message").StringValueOK()
			return depthAnswer{refusal: "answered success false: " + message}
		}
		return depthAnswer{accepted: true}
	}
	var refusal ErrorResponse
	if err := bson.Unmarshal(frame.Payload, &refusal); err != nil {
		t.Fatalf("%s: decoding the refusal: %v", op, err)
	}
	named := refusal.Code == int(transport.ErrInvalidArgument) && strings.Contains(refusal.Message, nestingRefusalPhrase)
	if isDataWrite(op) && refusal.WriteOutcome != outcomeNotApplied {
		named = false
	}
	return depthAnswer{refusal: fmt.Sprintf("code %d, write_outcome %q: %s", refusal.Code, refusal.WriteOutcome, refusal.Message), named: named}
}

// depthSite is one decode site of BUG-0526's table, driven through the real
// handler or function.
type depthSite struct {
	name string
	// send delivers the nested document through the site, under a key no
	// other row uses.
	send func(t *testing.T, key string, document []byte) depthAnswer
	// holds reports whether the document a send delivered under key is
	// stored; nil for a read, which stores nothing.
	holds func(t *testing.T, key string) bool
	// refusesAnyway marks a site whose fixture refuses every push for a reason
	// of its own; its depth-100 row checks only that the refusal is not the
	// depth bound's.
	refusesAnyway bool
}

// nestingFixture is a standalone node with database app and one bucket per
// store type, a bucket Post with store fields Comments (fifo) and Tags (hash)
// and its document p1, and a document "base" in docs for the update to change.
type nestingFixture struct {
	s       *Server
	handler transport.RequestHandler
}

func newNestingFixture(t *testing.T) nestingFixture {
	t.Helper()
	s := newTestServer(t)
	handler := NewHandler(s)
	handlerSend(t, handler, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
	for name, kind := range map[string]BucketType{
		"docs": BucketTypeHash, "tree": BucketTypeBTree, "jobs": BucketTypeFIFO,
		"stack": BucketTypeLIFO, "ranked": BucketTypeHeap, "files": BucketTypeBlob,
	} {
		// whole: the update cases send multi-updates through the door, which a spread bucket refuses (build step 12c).
		handlerSend(t, handler, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: name, Type: kind,
			Options: map[string]interface{}{"placement": "whole"}})
	}
	handlerSend(t, handler, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: "Post", Type: BucketTypeHash,
		Options: map[string]interface{}{"stores": []interface{}{
			map[string]interface{}{"name": "Comments", "engine": "fifo"},
			map[string]interface{}{"name": "Tags", "engine": "hash"},
		}}})
	handlerSend(t, handler, transport.OpPut, InsertRequest{Database: "app", Bucket: "Post", Key: "p1", Data: map[string]interface{}{"title": "p1"}})
	return nestingFixture{s: s, handler: handler}
}

func (f nestingFixture) send(t *testing.T, op transport.Opcode, payload []byte) depthAnswer {
	t.Helper()
	frame, err := f.handler(context.Background(), transport.NewFrame(op, payload))
	if err != nil {
		t.Fatalf("%s: the handler returned a Go error, which resets the connection: %v", op, err)
	}
	return answerOfFrame(t, op, frame)
}

func (f nestingFixture) count(t *testing.T, bucket string) int64 {
	t.Helper()
	counted, err := f.s.Count(CountRequest{Database: "app", Bucket: bucket})
	if err != nil {
		t.Fatalf("counting %s: %v", bucket, err)
	}
	return counted.Count
}

func (f nestingFixture) found(t *testing.T, bucket, key string) bool {
	t.Helper()
	found, err := f.s.Find(FindRequest{Database: "app", Bucket: bucket, Key: key})
	if err != nil {
		t.Fatalf("reading %s/%s: %v", bucket, key, err)
	}
	return found.Found
}

func (f nestingFixture) fieldLength(t *testing.T, field string) int64 {
	t.Helper()
	resp, err := f.s.FieldLen(FieldRequest{Database: "app", Bucket: "Post", Key: "p1", Field: field})
	if err != nil {
		t.Fatalf("FieldLen(p1, %s): %v", field, err)
	}
	return resp.Len
}

// documentSites are sites 1 to 8: every client frame passes NewHandler, and on
// a standalone node fencedDispatch's resolveNowAtTheDoor (site 8) decodes the
// write again on its way to the handler.
func (f nestingFixture) documentSites() []depthSite {
	keyed := func(bucket string) func(t *testing.T, key string) bool {
		return func(t *testing.T, key string) bool { return f.found(t, bucket, key) }
	}
	counted := func(bucket string) (func(t *testing.T, key string, document []byte) depthAnswer, func(t *testing.T, key string) bool) {
		// A push names no key: the row counts the bucket around the send.
		before := map[string]int64{}
		send := func(t *testing.T, key string, document []byte) depthAnswer {
			before[key] = f.count(t, bucket)
			op := map[string]transport.Opcode{"jobs": transport.OpFIFOPush, "stack": transport.OpLIFOPush, "ranked": transport.OpHeapPush}[bucket]
			return f.send(t, op, wireRequest(wireString("database", "app"), wireString("bucket", bucket), wireDocument("data", document)))
		}
		holds := func(t *testing.T, key string) bool { return f.count(t, bucket) != before[key] }
		return send, holds
	}
	fifoSend, fifoHolds := counted("jobs")
	lifoSend, lifoHolds := counted("stack")
	heapSend, heapHolds := counted("ranked")
	fieldLengths := map[string]int64{}
	filtered := func(op transport.Opcode, bucket string, extra ...[]byte) func(t *testing.T, key string, document []byte) depthAnswer {
		return func(t *testing.T, key string, document []byte) depthAnswer {
			elements := append([][]byte{wireString("database", "app"), wireString("bucket", bucket)}, extra...)
			return f.send(t, op, wireRequest(append(elements, wireDocument("filter", document))...))
		}
	}
	return []depthSite{
		{name: "1, 8: PUT", send: func(t *testing.T, key string, document []byte) depthAnswer {
			return f.send(t, transport.OpPut, wireRequest(wireString("database", "app"), wireString("bucket", "docs"), wireString("key", key), wireDocument("data", document)))
		}, holds: keyed("docs")},
		{name: "1: BATCH_PUT", send: func(t *testing.T, key string, document []byte) depthAnswer {
			return f.send(t, transport.OpBatchPut, wireRequest(wireString("database", "app"), wireString("bucket", "docs"),
				wireDocumentList("data", withRootKey(t, document, key))))
		}, holds: keyed("docs")},
		{name: "1: BTREE_INSERT", send: func(t *testing.T, key string, document []byte) depthAnswer {
			return f.send(t, transport.OpBTreeInsert, wireRequest(wireString("database", "app"), wireString("bucket", "tree"), wireString("key", key), wireDocument("data", document)))
		}, holds: keyed("tree")},
		{name: "2: FIFO_PUSH", send: fifoSend, holds: fifoHolds},
		{name: "2: LIFO_PUSH", send: lifoSend, holds: lifoHolds},
		{name: "2: HEAP_PUSH", send: heapSend, holds: heapHolds},
		{name: "3: FIELD_PUSH", send: func(t *testing.T, key string, document []byte) depthAnswer {
			fieldLengths[key] = f.fieldLength(t, "Comments")
			return f.send(t, transport.OpFieldPush, wireRequest(wireString("database", "app"), wireString("bucket", "Post"), wireString("key", "p1"),
				wireString("field", "Comments"), wireDocument("data", document)))
		}, holds: func(t *testing.T, key string) bool { return f.fieldLength(t, "Comments") != fieldLengths[key] }},
		{name: "3: FIELD_PUT", send: func(t *testing.T, key string, document []byte) depthAnswer {
			fieldLengths[key] = f.fieldLength(t, "Tags")
			return f.send(t, transport.OpFieldPut, wireRequest(wireString("database", "app"), wireString("bucket", "Post"), wireString("key", "p1"),
				wireString("field", "Tags"), wireString("element", key), wireDocument("data", document)))
		}, holds: func(t *testing.T, key string) bool { return f.fieldLength(t, "Tags") != fieldLengths[key] }},
		// The document is the $set map: setting a to what the document holds
		// under a gives the stored document the document's own depth.
		{name: "4: UPDATE $set", send: func(t *testing.T, key string, document []byte) depthAnswer {
			handlerSend(t, f.handler, transport.OpPut, InsertRequest{Database: "app", Bucket: "docs", Key: key, Data: map[string]interface{}{"n": 1}})
			return f.send(t, transport.OpUpdate, wireRequest(wireString("database", "app"), wireString("bucket", "docs"), wireString("key", key), wireDocument("set", document)))
		}, holds: func(t *testing.T, key string) bool {
			found, err := f.s.Find(FindRequest{Database: "app", Bucket: "docs", Key: key})
			if err != nil || !found.Found {
				t.Fatalf("reading the updated document %s: %+v, %v", key, found, err)
			}
			_, present := bson.Raw(found.Data).LookupErr("a")
			return present == nil
		}},
		{name: "5: SCAN filter", send: filtered(transport.OpScan, "docs")},
		{name: "5: SEARCH filter", send: filtered(transport.OpSearch, "docs")},
		{name: "5: COUNT filter", send: filtered(transport.OpCount, "docs")},
		{name: "5: DISTINCT filter", send: filtered(transport.OpDistinct, "docs", wireString("field", "n"))},
		{name: "6: BLOB_WRITE metadata", send: func(t *testing.T, key string, document []byte) depthAnswer {
			return f.send(t, transport.OpBlobWrite, wireRequest(wireString("database", "app"), wireString("bucket", "files"), wireString("key", key),
				bsoncore.AppendBinaryElement(nil, "data", 0, []byte("blob")), wireDocument("metadata", document)))
		}, holds: func(t *testing.T, key string) bool {
			read, err := f.s.BlobRead(BlobReadRequest{Database: "app", Bucket: "files", Key: key})
			return err == nil && read.Found
		}},
		// A declaration request decodes its options, declarations included, into
		// Go values before maxFieldTypeDepth is applied to them.
		{name: "7: CREATE_BUCKET options", send: func(t *testing.T, key string, document []byte) depthAnswer {
			return f.send(t, transport.OpCreateBucket, wireRequest(wireString("database", "app"), wireString("name", key), wireString("type", string(BucketTypeHash)),
				wireDocument("options", document)))
		}, holds: func(t *testing.T, key string) bool { return f.s.bucketStoreOpen("app", key) }},
	}
}

// withRootKey writes _id_ into a nested document's root, for a batch, whose
// documents carry their own keys.
func withRootKey(t *testing.T, document []byte, key string) []byte {
	t.Helper()
	elements, err := bsoncore.Document(document).Elements()
	if err != nil {
		t.Fatalf("the test's own nested document does not read: %v", err)
	}
	rebuilt := [][]byte{wireString("_id_", key)}
	for _, element := range elements {
		rebuilt = append(rebuilt, element)
	}
	return bsoncore.BuildDocument(nil, rebuilt...)
}

// replicatedPushSite is site 9: a peer's push, carrying a client's put as its
// entry, answered by a node whose cluster knows only its token. Nothing is
// listened on or dialled.
func replicatedPushSite(t *testing.T) depthSite {
	t.Helper()
	token, err := GenerateClusterToken()
	if err != nil {
		t.Fatalf("generating a cluster token: %v", err)
	}
	node := &Server{}
	node.cluster = &Cluster{s: node, token: ClusterToken{Token: token}, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	return depthSite{name: "9: REPL_PUSH entry", refusesAnyway: true, send: func(t *testing.T, key string, document []byte) depthAnswer {
		entry := ReplEntry{Seq: 1, Op: int32(transport.OpPut), Bucket: "docs",
			Payload: wireRequest(wireString("database", "app"), wireString("bucket", "docs"), wireString("key", key), wireDocument("data", document))}
		push := mustMarshalBSON(t, ReplPushRequest{Token: token, FromNode: "peer", Entries: []ReplEntry{entry}})
		frame, err := handleReplPush(context.Background(), transport.NewFrame(transport.OpReplPush, push), node)
		if err != nil {
			t.Fatalf("REPL_PUSH: %v", err)
		}
		var answer ReplPushResponse
		if err := bson.Unmarshal(frame.Payload, &answer); err != nil {
			t.Fatalf("decoding the REPL_ACK: %v", err)
		}
		if answer.Success {
			return depthAnswer{accepted: true}
		}
		return depthAnswer{refusal: answer.Error, named: strings.Contains(answer.Error, nestingRefusalPhrase)}
	}}
}

// TestNestingDepthBoundIsRefusedAtEveryDecodeSite drives each decode site with
// documents and arrays nested to exactly 100 levels, which every site must
// take, and to 101, which every site must refuse with the depth bound's own
// refusal, storing nothing.
//
// The replicated push is answered by a node with no store and no stream, so a
// push it does not refuse for depth it refuses for that (refusesAnyway).
func TestNestingDepthBoundIsRefusedAtEveryDecodeSite(t *testing.T) {
	f := newNestingFixture(t)
	sites := append(f.documentSites(), replicatedPushSite(t))
	for _, site := range sites {
		for _, arrays := range []bool{false, true} {
			shape := "documents"
			if arrays {
				shape = "arrays"
			}
			slug := strings.NewReplacer(" ", "-", ":", "", ",", "", "$", "").Replace(strings.ToLower(site.name))

			atBound := fmt.Sprintf("at-bound-%s-%s", slug, shape)
			answer := site.send(t, atBound, nestedDocumentBytes(maxNestingDepthUnderTest, arrays))
			switch {
			case site.refusesAnyway:
				if answer.named {
					t.Errorf("site %s, %s nested %d deep: refused for its depth: %s", site.name, shape, maxNestingDepthUnderTest, answer.refusal)
				}
			case !answer.accepted:
				t.Errorf("site %s, %s nested %d deep: refused, want accepted: %s", site.name, shape, maxNestingDepthUnderTest, answer.refusal)
			case site.holds != nil && !site.holds(t, atBound):
				t.Errorf("site %s, %s nested %d deep: accepted but not stored", site.name, shape, maxNestingDepthUnderTest)
			}

			overBound := fmt.Sprintf("over-bound-%s-%s", slug, shape)
			answer = site.send(t, overBound, nestedDocumentBytes(maxNestingDepthUnderTest+1, arrays))
			if !answer.named {
				what := "accepted"
				if !answer.accepted {
					what = "refused, but not by the depth bound: " + answer.refusal
				}
				t.Errorf("site %s, %s nested %d deep: %s", site.name, shape, maxNestingDepthUnderTest+1, what)
			}
			if site.holds != nil && site.holds(t, overBound) {
				t.Errorf("site %s, %s nested %d deep: the document was stored", site.name, shape, maxNestingDepthUnderTest+1)
			}
		}
	}
}

// maxNestingDepthUnderTest is the drivers' bound, written out here so the test
// states the number it holds the server to.
const maxNestingDepthUnderTest = 100

// TestDocumentStoredAtTheBoundReadsBackUnchanged: the deepest document a
// client may send is stored byte for byte as sent.
func TestDocumentStoredAtTheBoundReadsBackUnchanged(t *testing.T) {
	f := newNestingFixture(t)
	for _, arrays := range []bool{false, true} {
		key := fmt.Sprintf("unchanged-%t", arrays)
		sent := nestedDocumentBytes(maxNestingDepthUnderTest, arrays)
		if answer := f.send(t, transport.OpPut, wireRequest(wireString("database", "app"), wireString("bucket", "docs"), wireString("key", key), wireDocument("data", sent))); !answer.accepted {
			t.Fatalf("arrays=%t: the document at the bound was refused: %s", arrays, answer.refusal)
		}
		found, err := f.s.Find(FindRequest{Database: "app", Bucket: "docs", Key: key})
		if err != nil || !found.Found {
			t.Fatalf("arrays=%t: reading it back: %+v, %v", arrays, found, err)
		}
		storedValue, err := bson.Raw(found.Data).LookupErr("a")
		if err != nil {
			t.Fatalf("arrays=%t: the stored document has no a: %v", arrays, err)
		}
		sentValue := bson.Raw(sent).Lookup("a")
		if storedValue.Type != sentValue.Type || string(storedValue.Value) != string(sentValue.Value) {
			t.Errorf("arrays=%t: the stored document's a differs from the one sent", arrays)
		}
	}
}

// TestUpdateThatWouldNestPastTheBoundIsRefused: a request within the bound can
// still move a stored subtree deeper ($rename into a longer path), and the
// stored document must stay within the bound for every later read to decode
// it. The update is refused and the document is as it was.
func TestUpdateThatWouldNestPastTheBoundIsRefused(t *testing.T) {
	f := newNestingFixture(t)
	// {a: <99 levels>} is 100 deep; moved under b.c it would be 101.
	stored := nestedDocumentBytes(maxNestingDepthUnderTest, false)
	if answer := f.send(t, transport.OpPut, wireRequest(wireString("database", "app"), wireString("bucket", "docs"), wireString("key", "moved"), wireDocument("data", stored))); !answer.accepted {
		t.Fatalf("storing the document at the bound: %s", answer.refusal)
	}
	for _, update := range []UpdateRequest{
		{Database: "app", Bucket: "docs", Key: "moved", Rename: map[string]string{"a": "b.c"}},
		{Database: "app", Bucket: "docs", Key: "moved", Rename: map[string]string{"a": "b.c"}, Multi: true, Filter: map[string]interface{}{"_id_": "moved"}},
	} {
		answer := f.send(t, transport.OpUpdate, mustMarshalBSON(t, update))
		if !answer.named {
			t.Errorf("multi=%t: a rename that nests the document 101 deep was not refused by the depth bound: accepted=%t %s", update.Multi, answer.accepted, answer.refusal)
		}
		found, err := f.s.Find(FindRequest{Database: "app", Bucket: "docs", Key: "moved"})
		if err != nil || !found.Found {
			t.Fatalf("reading the document back: %+v, %v", found, err)
		}
		if _, err := bson.Raw(found.Data).LookupErr("a"); err != nil {
			t.Errorf("multi=%t: the refused update changed the document: a is gone", update.Multi)
		}
	}
	shallow := UpdateRequest{Database: "app", Bucket: "docs", Key: "moved", Rename: map[string]string{"a": "b"}}
	if answer := f.send(t, transport.OpUpdate, mustMarshalBSON(t, shallow)); !answer.accepted {
		t.Errorf("a rename that keeps the document at 100 levels was refused: %s", answer.refusal)
	}
}

// TestReplayPassesOverAnEntryNestedPastTheBound: a logged entry is a client's
// frame, and replay decodes it as the handler does. One nested past the bound
// is passed over as an entry that did not apply; one at the bound is applied.
func TestReplayPassesOverAnEntryNestedPastTheBound(t *testing.T) {
	s := newTestServer(t)
	w, err := storage.OpenWAL(filepath.Join(t.TempDir(), "wal"), nil)
	if err != nil {
		t.Fatalf("opening the WAL: %v", err)
	}
	defer func() {
		if err := w.Close(); err != nil {
			t.Errorf("closing the WAL: %v", err)
		}
	}()
	attachWALToStartedServer(s, w)
	logEntry := func(op transport.Opcode, bucket, key string, payload []byte) {
		t.Helper()
		if _, err := w.Append(bucket, byte(op), []byte(key), payload); err != nil {
			t.Fatalf("logging %s: %v", op, err)
		}
	}
	logEntry(transport.OpCreateDB, "", "", mustMarshalBSON(t, CreateDatabaseRequest{Name: "app"}))
	logEntry(transport.OpCreateBucket, "docs", "", mustMarshalBSON(t, CreateBucketRequest{Database: "app", Name: "docs", Type: BucketTypeHash}))
	for key, depth := range map[string]int{"at-bound": maxNestingDepthUnderTest, "over-bound": maxNestingDepthUnderTest + 1} {
		logEntry(transport.OpPut, "docs", key, wireRequest(wireString("database", "app"), wireString("bucket", "docs"), wireString("key", key),
			wireDocument("data", nestedDocumentBytes(depth, false))))
	}

	if err := s.replayWAL(context.Background()); err != nil {
		t.Fatalf("replaying the log: %v", err)
	}
	for key, want := range map[string]bool{"at-bound": true, "over-bound": false} {
		found, err := s.Find(FindRequest{Database: "app", Bucket: "docs", Key: key})
		if err != nil {
			t.Fatalf("reading %s: %v", key, err)
		}
		if found.Found != want {
			t.Errorf("after replay, %s is stored: %t, want %t", key, found.Found, want)
		}
	}
}

// TestRestoreRefusesADocumentNestedPastTheBound: a dump's documents are
// decoded one by one before they are written back; one past the bound is
// refused by the depth bound, one at it decodes.
func TestRestoreRefusesADocumentNestedPastTheBound(t *testing.T) {
	for _, arrays := range []bool{false, true} {
		if _, err := decodeRestoreDocument(nestedDocumentBytes(maxNestingDepthUnderTest, arrays)); err != nil {
			t.Errorf("arrays=%t: a dumped document at the bound was refused: %v", arrays, err)
		}
		_, err := decodeRestoreDocument(nestedDocumentBytes(maxNestingDepthUnderTest+1, arrays))
		if err == nil || !strings.Contains(err.Error(), nestingRefusalPhrase) {
			t.Errorf("arrays=%t: a dumped document 101 deep: %v, want the depth bound's refusal", arrays, err)
		}
	}
}
